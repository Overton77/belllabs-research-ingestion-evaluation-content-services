"""PostgreSQL adapters for safe macro snapshots and semantic forks (RRM-006, migration 0024).

Every query runs with `belllabs.request_scope` set, so forced row-level security confines
reads and writes to the caller's scope. Snapshots and reuse decisions are insert-only; the
fork materialization writes its reuse decisions, its lineage-journal record and the fork
row's materialization payload in one transaction.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from app.application.runtime.postgres_stage3_kernel_repository import (
    append_lineage_in_transaction,
)
from app.application.runtime.run_forks import (
    FamilyHeadRecord,
    ForkOfRun,
    ForkSourceFacts,
    LinkedRunRecord,
    fork_marker_of,
)
from app.application.runtime.runtime_lineage import PersistedExecutionLineage
from app.domain.control_plane.canonical import stable_json_digest, stable_json_dump
from app.domain.run_control.errors import IdempotencyConflict
from app.domain.run_control.forks import (
    ForkLineageManifest,
    ForkRejected,
    ForkReuseDecision,
    RunForkRequest,
    RunSnapshotManifest,
    fork_request_fingerprint,
)


async def _scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute("SELECT set_config('belllabs.request_scope', $1, true)", request_scope)


def _load(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _dump(value: Any) -> str:
    return json.dumps(stable_json_dump(value), sort_keys=True, separators=(",", ":"))


class PostgresRunSnapshotRepository:
    """Insert-only, content-addressed run snapshots (`run_snapshot_manifests`)."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get(self, request_scope: str, snapshot_id: str) -> RunSnapshotManifest | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT manifest FROM belllabs_control.run_snapshot_manifests
                WHERE request_scope = $1 AND snapshot_id = $2
                """,
                request_scope,
                snapshot_id,
            )
        return RunSnapshotManifest.model_validate(_load(payload)) if payload else None

    async def put(self, snapshot: RunSnapshotManifest) -> RunSnapshotManifest:
        async with self._pool.acquire() as connection, connection.transaction():
            await _scope(connection, snapshot.request_scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"run-snapshot:{snapshot.request_scope}:{snapshot.snapshot_id}",
            )
            prior = await connection.fetchrow(
                """
                SELECT snapshot_digest, manifest FROM belllabs_control.run_snapshot_manifests
                WHERE request_scope = $1 AND snapshot_id = $2
                """,
                snapshot.request_scope,
                snapshot.snapshot_id,
            )
            if prior is not None:
                if prior["snapshot_digest"] != snapshot.snapshot_digest:
                    raise ForkRejected(
                        "snapshot_digest_conflict",
                        "the same run version and boundary produced a different snapshot",
                        reasons=(prior["snapshot_digest"], snapshot.snapshot_digest),
                    )
                return RunSnapshotManifest.model_validate(_load(prior["manifest"]))
            await connection.execute(
                """
                INSERT INTO belllabs_control.run_snapshot_manifests (
                    request_scope, snapshot_id, schema_version, source_run_id,
                    execution_epoch, family, boundary_kind, projection_version,
                    snapshot_digest, manifest, taken_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11)
                """,
                snapshot.request_scope,
                snapshot.snapshot_id,
                snapshot.schema_version,
                snapshot.source_run_id,
                snapshot.execution_epoch,
                snapshot.family,
                snapshot.boundary_kind,
                snapshot.projection_version,
                snapshot.snapshot_digest,
                _dump(snapshot),
                snapshot.taken_at,
            )
            return snapshot


class PostgresForkSourceReader:
    """Family head, linked-run and semantic-binding authority, read in one snapshot."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def read_fork_source(self, request_scope: str, run_id: str) -> ForkSourceFacts | None:
        async with self._pool.acquire() as connection:
            async with connection.transaction(readonly=True, isolation="repeatable_read"):
                await _scope(connection, request_scope)
                version = await connection.fetchval(
                    """
                    SELECT version FROM belllabs_control.workflow_runs
                    WHERE request_scope = $1 AND run_id = $2
                    """,
                    request_scope,
                    run_id,
                )
                if version is None:
                    return None
                heads = await connection.fetch(
                    """
                    SELECT family_kind, family_version, mutation_fingerprint, mutation
                    FROM belllabs_control.family_admission_heads
                    WHERE request_scope = $1 AND run_id = $2
                    ORDER BY family_kind
                    """,
                    request_scope,
                    run_id,
                )
                links = await connection.fetch(
                    """
                    SELECT link.link_id, link.child_run_id, terminal.status
                    FROM belllabs_control.run_composition_links AS link
                    LEFT JOIN belllabs_control.linked_child_terminal_records AS terminal
                      ON terminal.link_id = link.link_id
                    WHERE link.request_scope = $1 AND link.parent_run_id = $2
                    ORDER BY link.link_id
                    """,
                    request_scope,
                    run_id,
                )
                binding = await connection.fetchrow(
                    """
                    SELECT blueprint_digest, binding_digest
                    FROM belllabs_control.workflow_semantic_input_bindings
                    WHERE request_scope = $1 AND run_id = $2
                    """,
                    request_scope,
                    run_id,
                )
        return ForkSourceFacts(
            run_version=int(version),
            family_heads=tuple(
                FamilyHeadRecord(
                    family_kind=row["family_kind"],
                    family_version=int(row["family_version"]),
                    mutation_fingerprint=row["mutation_fingerprint"],
                    mutation=_load(row["mutation"]),
                )
                for row in heads
            ),
            linked_runs=tuple(
                LinkedRunRecord(
                    link_id=row["link_id"],
                    child_run_id=row["child_run_id"],
                    terminal_status=row["status"],
                )
                for row in links
            ),
            blueprint_digest=binding["blueprint_digest"] if binding is not None else None,
            semantic_input_binding_digest=(
                binding["binding_digest"] if binding is not None else None
            ),
        )


class PostgresForkMaterializationStore:
    """Reuse decisions, fork lineage, and the materialization record, atomically."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def record_materialization(
        self,
        request: RunForkRequest,
        lineage: ForkLineageManifest,
        execution_lineage: PersistedExecutionLineage,
    ) -> ForkLineageManifest:
        scope = request.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _scope(connection, scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"fork:{scope}:{request.request_id}",
            )
            row = await connection.fetchrow(
                """
                SELECT request_digest, materialization_payload
                FROM belllabs_control.runtime_fork_requests
                WHERE request_scope = $1 AND request_id = $2 AND schema_version IS NOT NULL
                FOR UPDATE
                """,
                scope,
                request.request_id,
            )
            if row is None or row["request_digest"] != fork_request_fingerprint(request):
                raise LookupError("fork reservation is unavailable")
            if row["materialization_payload"] is not None:
                persisted = ForkLineageManifest.model_validate(
                    _load(row["materialization_payload"])
                )
                if persisted != lineage:
                    raise IdempotencyConflict("fork materialization has conflicting lineage")
                return persisted
            for decision in request.reuse_decisions:
                candidate = decision.candidate
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.run_fork_reuse_decisions (
                        request_scope, fork_request_id, derived_run_id, derived_unit_key,
                        source_run_id, source_unit_key, schema_version, decision, reason,
                        result_manifest_ref, result_manifest_digest, decision_digest,
                        decision_payload, recorded_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14)
                    """,
                    scope,
                    request.request_id,
                    decision.derived_run_id,
                    decision.derived_unit_key,
                    decision.source_run_id,
                    decision.source_unit_key,
                    decision.schema_version,
                    decision.decision,
                    decision.reason,
                    candidate.result_manifest_ref if candidate is not None else None,
                    candidate.result_manifest_digest if candidate is not None else None,
                    stable_json_digest(decision),
                    _dump(decision),
                    request.requested_at,
                )
            await append_lineage_in_transaction(connection, execution_lineage)
            await connection.execute(
                """
                UPDATE belllabs_control.runtime_fork_requests
                SET materialization_payload = $3::jsonb
                WHERE request_scope = $1 AND request_id = $2
                """,
                scope,
                request.request_id,
                _dump(lineage),
            )
            return lineage

    async def get_materialization(
        self, request_scope: str, request_id: str
    ) -> ForkLineageManifest | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT materialization_payload FROM belllabs_control.runtime_fork_requests
                WHERE request_scope = $1 AND request_id = $2 AND schema_version IS NOT NULL
                """,
                request_scope,
                request_id,
            )
        return ForkLineageManifest.model_validate(_load(payload)) if payload else None

    async def fork_of_run(self, request_scope: str, derived_run_id: str) -> ForkOfRun | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT request_id, materialization_payload IS NOT NULL AS materialized
                FROM belllabs_control.runtime_fork_requests
                WHERE request_scope = $1 AND target_run_id = $2
                """,
                request_scope,
                derived_run_id,
            )
        if row is None:
            return None
        return ForkOfRun(fork_request_id=row["request_id"], materialized=row["materialized"])

    async def fork_marker(self, request_scope: str, run_id: str) -> str | None:
        """The fork request id named by the run's admission transition (version 1), if any."""

        async with self._pool.acquire() as connection, connection.transaction():
            await _scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT t.transition
                FROM belllabs_control.lifecycle_transitions t
                JOIN belllabs_control.workflow_runs r ON r.run_id = t.run_id
                WHERE r.request_scope = $1 AND t.run_id = $2 AND t.resulting_version = 1
                """,
                request_scope,
                run_id,
            )
        if payload is None:
            return None
        return fork_marker_of(tuple(_load(payload).get("evidence_refs", ())))

    async def get_reuse_decision(
        self, request_scope: str, derived_run_id: str, derived_unit_key: str
    ) -> ForkReuseDecision | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT decision_payload FROM belllabs_control.run_fork_reuse_decisions
                WHERE request_scope = $1 AND derived_run_id = $2 AND derived_unit_key = $3
                """,
                request_scope,
                derived_run_id,
                derived_unit_key,
            )
        return ForkReuseDecision.model_validate(_load(payload)) if payload else None

    async def list_reuse_decisions(
        self, request_scope: str, request_id: str
    ) -> tuple[ForkReuseDecision, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT decision_payload FROM belllabs_control.run_fork_reuse_decisions
                WHERE request_scope = $1 AND fork_request_id = $2
                ORDER BY derived_unit_key
                """,
                request_scope,
                request_id,
            )
        return tuple(
            ForkReuseDecision.model_validate(_load(row["decision_payload"])) for row in rows
        )
