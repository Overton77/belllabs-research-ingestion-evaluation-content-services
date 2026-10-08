"""PostgreSQL adapters for safe macro snapshots and semantic forks on mission_control.

A sealed run snapshot is a canonical ``continuation_checkpoint`` with a ``valid``
checkpoint validation and its boundary detail in support ``run_snapshot``. The fork
materialization writes its reuse decisions, its execution-lineage record and the fork
saga's materialization payload in one transaction. Every statement runs with the
composite scope applied and filters on all three scope columns.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from mission_control.adapters.postgres.orchestration.orchestration_binding_repository import (
    semantic_binding_row,
)
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.adapters.postgres.runtime.stage3_kernel_repository import (
    append_lineage_in_transaction,
)
from mission_control.application.recovery.run_forks import (
    FamilyHeadRecord,
    ForkOfRun,
    ForkSourceFacts,
    LinkedRunRecord,
    fork_marker_of,
)
from mission_control.application.recovery.runtime_lineage import PersistedExecutionLineage
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import stable_json_digest, stable_json_dump
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.policies.forks import (
    ForkLineageManifest,
    ForkRejected,
    ForkReuseDecision,
    RunForkRequest,
    RunSnapshotManifest,
    fork_request_fingerprint,
)

SNAPSHOT_ACTOR = "mission-control-run-snapshots"


def _load(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _dump(value: Any) -> str:
    return json.dumps(stable_json_dump(value), sort_keys=True, separators=(",", ":"))


class PostgresRunSnapshotRepository:
    """Insert-only, content-addressed sealed run snapshots."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def get(self, request_scope: str, snapshot_id: str) -> RunSnapshotManifest | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT manifest FROM mission_control.continuation_checkpoint
                WHERE {SCOPE} AND checkpoint_key = $4
                """,
                *args,
                snapshot_id,
            )
        return RunSnapshotManifest.model_validate(_load(payload)) if payload else None

    async def latest(self, request_scope: str, run_id: str) -> RunSnapshotManifest | None:
        """FT-F4: the newest sealed Snapshot of a run (highest version, then latest taken)."""

        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT checkpoint.manifest
                FROM mission_control.run_snapshot snapshot
                JOIN mission_control.continuation_checkpoint checkpoint
                  ON checkpoint.installation_id = snapshot.installation_id
                 AND checkpoint.application_id = snapshot.application_id
                 AND checkpoint.tenant_id = snapshot.tenant_id
                 AND checkpoint.checkpoint_id = snapshot.checkpoint_id
                WHERE {scoped("snapshot")} AND snapshot.source_run_key = $4
                ORDER BY snapshot.projection_version DESC, snapshot.taken_at DESC
                LIMIT 1
                """,
                *args,
                run_id,
            )
        return RunSnapshotManifest.model_validate(_load(payload)) if payload else None

    async def put(self, snapshot: RunSnapshotManifest) -> RunSnapshotManifest:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, snapshot.request_scope)
            await mc.advisory_lock(
                connection, f"run-snapshot:{snapshot.request_scope}:{snapshot.snapshot_id}"
            )
            prior = await connection.fetchrow(
                f"""
                SELECT manifest_digest, manifest FROM mission_control.continuation_checkpoint
                WHERE {SCOPE} AND checkpoint_key = $4
                """,
                *args,
                snapshot.snapshot_id,
            )
            if prior is not None:
                if prior["manifest_digest"] != snapshot.snapshot_digest:
                    raise ForkRejected(
                        "snapshot_digest_conflict",
                        "the same run version and boundary produced a different snapshot",
                        reasons=(prior["manifest_digest"], snapshot.snapshot_digest),
                    )
                return RunSnapshotManifest.model_validate(_load(prior["manifest"]))
            run = await mc.require_run(connection, args, snapshot.source_run_id)
            checkpoint_id = uuid7()
            await connection.execute(
                """
                INSERT INTO mission_control.continuation_checkpoint (
                    installation_id, application_id, tenant_id, checkpoint_id, run_id,
                    checkpoint_key, activation_id, attempt_id, predecessor_checkpoint_id,
                    manifest_ref, manifest_digest, contract_version, runtime_checkpoint_ref,
                    sandbox_snapshot_ref, source_revision_digest, source_binding_digest,
                    source_input_digest, execution_epoch, execution_generation, event_frontier,
                    harness_format, manifest, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, NULL, $6, $7, $8, NULL, NULL, NULL,
                        $9, $10, $11, 1, $12, $13, $14::jsonb, $15, $16)
                """,
                *args,
                checkpoint_id,
                run["run_id"],
                snapshot.snapshot_id,
                snapshot.snapshot_digest,
                snapshot.schema_version,
                snapshot.semantic_input_binding_digest,
                snapshot.input_manifest.digest,
                snapshot.execution_epoch,
                f"run-version:{snapshot.projection_version}",
                snapshot.family,
                _dump(snapshot),
                snapshot.taken_at,
                SNAPSHOT_ACTOR,
            )
            await connection.execute(
                """
                INSERT INTO mission_control.checkpoint_validation (
                    installation_id, application_id, tenant_id, checkpoint_validation_id,
                    checkpoint_id, status, reason, decided_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, 'valid', $6, $7, $7, $8)
                """,
                *args,
                uuid7(),
                checkpoint_id,
                f"sealed at {snapshot.boundary_kind}:{snapshot.boundary_ref}"[:1024],
                snapshot.taken_at,
                SNAPSHOT_ACTOR,
            )
            await connection.execute(
                """
                INSERT INTO mission_control.run_snapshot (
                    installation_id, application_id, tenant_id, run_snapshot_id, snapshot_key,
                    checkpoint_id, schema_version, source_run_key, execution_epoch, family,
                    boundary_kind, projection_version, snapshot_digest, taken_at, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $14, $15)
                """,
                *args,
                uuid7(),
                snapshot.snapshot_id,
                checkpoint_id,
                snapshot.schema_version,
                snapshot.source_run_id,
                snapshot.execution_epoch,
                snapshot.family,
                snapshot.boundary_kind,
                snapshot.projection_version,
                snapshot.snapshot_digest,
                snapshot.taken_at,
                SNAPSHOT_ACTOR,
            )
            return snapshot


class PostgresForkSourceReader:
    """Family head, linked-run and semantic-binding authority, read in one snapshot."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def read_fork_source(self, request_scope: str, run_id: str) -> ForkSourceFacts | None:
        async with self._pool.acquire() as connection:
            async with connection.transaction(readonly=True, isolation="repeatable_read"):
                args = await mc.begin(connection, request_scope)
                run = await mc.run_row(connection, args, run_id)
                if run is None:
                    return None
                heads = await connection.fetch(
                    f"""
                    SELECT family_kind, family_version, mutation_fingerprint, mutation
                    FROM mission_control.family_admission_head
                    WHERE {SCOPE} AND run_key = $4
                    ORDER BY family_kind
                    """,
                    *args,
                    run_id,
                )
                links = await connection.fetch(
                    f"""
                    SELECT link.link_key, link.child_run_key, terminal.status
                    FROM mission_control.run_composition_link AS link
                    LEFT JOIN mission_control.linked_child_terminal AS terminal
                      ON terminal.installation_id = link.installation_id
                     AND terminal.application_id = link.application_id
                     AND terminal.tenant_id = link.tenant_id
                     AND terminal.link_key = link.link_key
                    WHERE {scoped("link")} AND link.parent_run_key = $4
                    ORDER BY link.link_key
                    """,
                    *args,
                    run_id,
                )
                binding = await semantic_binding_row(connection, args, run_id)
        manifest = _load(binding["manifest"]) if binding is not None else None
        return ForkSourceFacts(
            run_version=int(run["version"]),
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
                    link_id=row["link_key"],
                    child_run_id=row["child_run_key"],
                    terminal_status=row["status"],
                )
                for row in links
            ),
            blueprint_digest=manifest["blueprint_digest"] if manifest is not None else None,
            semantic_input_binding_digest=(
                binding["manifest_digest"] if binding is not None else None
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
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"fork:{scope}:{request.request_id}")
            row = await connection.fetchrow(
                f"""
                SELECT request_digest, materialization_payload FROM mission_control.fork_request
                WHERE {SCOPE} AND request_key = $4
                FOR UPDATE
                """,
                *args,
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
                    INSERT INTO mission_control.fork_reuse_decision (
                        installation_id, application_id, tenant_id, fork_reuse_decision_id,
                        fork_request_key, derived_run_key, derived_unit_key, source_run_key,
                        source_unit_key, schema_version, decision, reason, result_manifest_ref,
                        result_manifest_digest, decision_digest, decision_payload, recorded_at,
                        created_at, created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                            $16::jsonb, $17, $17, $18)
                    """,
                    *args,
                    uuid7(),
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
                    request.actor_id,
                )
            await append_lineage_in_transaction(connection, execution_lineage)
            # FT-F4: the fork edge in the Mission Graph (`mission_relationship` kind `fork`).
            source = await mc.require_run(connection, args, request.source_run_id)
            derived = await mc.require_run(connection, args, request.derived_run_id)
            await connection.execute(
                """
                INSERT INTO mission_control.mission_relationship (
                    installation_id, application_id, tenant_id, relationship_id,
                    relationship_key, source_run_id, target_run_id, kind, invocation_ref,
                    grant_ref, projected_output_policy, detail, version, updated_at, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, 'fork', $8, NULL, '{}'::jsonb, $9::jsonb,
                        1, $10, $10, $11)
                ON CONFLICT (installation_id, application_id, tenant_id, relationship_key)
                DO NOTHING
                """,
                *args,
                uuid7(),
                f"fork:{request.request_id}",
                source["run_id"],
                derived["run_id"],
                f"fork-request:{request.request_id}",
                _dump(
                    {
                        "fork_request_id": request.request_id,
                        "snapshot_id": request.snapshot_id,
                        "snapshot_digest": request.snapshot_digest,
                        "patch_digest": request.patch.patch_digest,
                    }
                ),
                request.requested_at,
                request.actor_id,
            )
            await connection.execute(
                f"""
                UPDATE mission_control.fork_request
                SET materialization_payload = $5::jsonb
                WHERE {SCOPE} AND request_key = $4
                """,
                *args,
                request.request_id,
                _dump(lineage),
            )
            return lineage

    async def get_materialization(
        self, request_scope: str, request_id: str
    ) -> ForkLineageManifest | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT materialization_payload FROM mission_control.fork_request
                WHERE {SCOPE} AND request_key = $4
                """,
                *args,
                request_id,
            )
        return ForkLineageManifest.model_validate(_load(payload)) if payload else None

    async def lineage_of_run(
        self, request_scope: str, run_id: str
    ) -> tuple[ForkLineageManifest, ...]:
        """FT-F4: materialized forks a run is the source or the target of."""

        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT materialization_payload FROM mission_control.fork_request
                WHERE {SCOPE} AND (source_run_key = $4 OR target_run_key = $4)
                  AND materialization_payload IS NOT NULL
                ORDER BY requested_at, request_key
                """,
                *args,
                run_id,
            )
        return tuple(
            ForkLineageManifest.model_validate(_load(row["materialization_payload"]))
            for row in rows
        )

    async def fork_of_run(self, request_scope: str, derived_run_id: str) -> ForkOfRun | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await connection.fetchrow(
                f"""
                SELECT request_key, materialization_payload IS NOT NULL AS materialized
                FROM mission_control.fork_request
                WHERE {SCOPE} AND target_run_key = $4
                """,
                *args,
                derived_run_id,
            )
        if row is None:
            return None
        return ForkOfRun(fork_request_id=row["request_key"], materialized=row["materialized"])

    async def fork_marker(self, request_scope: str, run_id: str) -> str | None:
        """The fork request id named by the run's admission transition (version 1), if any."""

        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT transition FROM mission_control.run_lifecycle_transition
                WHERE {SCOPE} AND run_key = $4 AND resulting_version = 1
                """,
                *args,
                run_id,
            )
        if payload is None:
            return None
        return fork_marker_of(tuple(_load(payload).get("evidence_refs", ())))

    async def get_reuse_decision(
        self, request_scope: str, derived_run_id: str, derived_unit_key: str
    ) -> ForkReuseDecision | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await connection.fetchval(
                f"""
                SELECT decision_payload FROM mission_control.fork_reuse_decision
                WHERE {SCOPE} AND derived_run_key = $4 AND derived_unit_key = $5
                """,
                *args,
                derived_run_id,
                derived_unit_key,
            )
        return ForkReuseDecision.model_validate(_load(payload)) if payload else None

    async def list_reuse_decisions(
        self, request_scope: str, request_id: str
    ) -> tuple[ForkReuseDecision, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT decision_payload FROM mission_control.fork_reuse_decision
                WHERE {SCOPE} AND fork_request_key = $4
                ORDER BY derived_unit_key
                """,
                *args,
                request_id,
            )
        return tuple(
            ForkReuseDecision.model_validate(_load(row["decision_payload"])) for row in rows
        )
