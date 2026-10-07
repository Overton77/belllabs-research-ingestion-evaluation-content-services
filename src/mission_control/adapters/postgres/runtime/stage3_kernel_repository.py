"""RLS-scoped persistence for the durable runtime kernel on mission_control.

Kept here: the immutable execution-lineage journal helper used by fork materialization,
durable decision requests (canonical ``human_task``) and responses (canonical
``human_resolution``), and the semantic fork saga (canonical ``recovery_request`` of kind
``fork`` plus support ``fork_request``, and ``fork_lineage`` once accepted).

Retired with their tests-only callers (no bootstrap/composition or application-service
construction): the standalone lineage provenance reader, the generic resource lease
journal, the runtime incident repair/reconciliation repository and the retention
deletion repository.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, Literal

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.application.recovery.runtime_decisions import DurableDecisionRecord
from mission_control.application.recovery.runtime_lineage import PersistedExecutionLineage
from mission_control.application.recovery.runtime_recovery import ForkAdmission
from mission_control.contracts.identities import parse_request_scope, uuid7
from mission_control.domain.authoring.canonical import stable_json_digest
from mission_control.domain.graph_runtime.kernel import DecisionRequest, DecisionResponse
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.policies.forks import (
    RunForkReceipt,
    RunForkRequest,
    fork_request_fingerprint,
)

RETENTION_DAYS = 90
FORK_ACTION = "mc.run.fork"
DECISION_KIND_PREFIX = "runtime_decision:"
_FORK_STATE = {
    "reserved": "admitted",
    "admitting": "admitted",
    "admitted": "running",
    "copying": "running",
    "accepted": "completed",
}


async def append_lineage_in_transaction(
    connection: asyncpg.Connection, lineage: PersistedExecutionLineage
) -> PersistedExecutionLineage:
    """Append one immutable lineage record and its edges in the caller's transaction.

    The caller has applied the scope. A fork materialization appends its lineage in the
    same transaction as its reuse decisions (RRM-006).
    """

    scope = lineage.envelope.request_scope
    parsed = parse_request_scope(scope)
    args = mc.scope_args(parsed)
    await mc.advisory_lock(connection, f"lineage:{scope}:{lineage.lineage_id}")
    prior = await connection.fetchrow(
        f"""
        SELECT lineage_payload FROM mission_control.execution_lineage_record
        WHERE {SCOPE} AND lineage_key = $4
        """,
        *args,
        lineage.lineage_id,
    )
    if prior is not None:
        persisted = PersistedExecutionLineage.model_validate(_json(prior["lineage_payload"]))
        if persisted != lineage:
            raise IdempotencyConflict("lineage identity was reused with conflicting facts")
        return persisted
    digest_owner = await connection.fetchval(
        f"""
        SELECT lineage_key FROM mission_control.execution_lineage_record
        WHERE {SCOPE} AND lineage_digest = $4
        """,
        *args,
        lineage.lineage_digest,
    )
    if digest_owner is not None:
        raise IdempotencyConflict("lineage digest is already bound to another identity")
    parent_id = lineage.envelope.parent_lineage_id
    if parent_id is not None:
        parent_exists = await connection.fetchval(
            f"""
            SELECT 1 FROM mission_control.execution_lineage_record
            WHERE {SCOPE} AND lineage_key = $4
            """,
            *args,
            parent_id,
        )
        if parent_exists is None:
            raise ValueError("lineage parent must be persisted before its child")
    await connection.execute(
        """
        INSERT INTO mission_control.execution_lineage_record (
            installation_id, application_id, tenant_id, execution_lineage_record_id,
            lineage_key, run_key, execution_epoch, lineage_digest, result_manifest_ref,
            lineage_payload, recorded_at, retain_until, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $11, $13)
        """,
        *args,
        uuid7(),
        lineage.lineage_id,
        lineage.envelope.belllabs_run_id,
        lineage.envelope.execution_epoch,
        lineage.lineage_digest,
        lineage.envelope.result_manifest_ref,
        _dump(lineage),
        lineage.recorded_at,
        lineage.retain_until,
        mc.WRITER_REF,
    )
    for parent_edge in lineage.parent_edges:
        await connection.execute(
            """
            INSERT INTO mission_control.execution_lineage_edge (
                installation_id, application_id, tenant_id, execution_lineage_edge_id,
                lineage_key, parent_identity_key, child_identity_key, relationship,
                edge_digest, recorded_at, created_at, created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, $11)
            """,
            *args,
            uuid7(),
            lineage.lineage_id,
            parent_edge.parent.canonical_key,
            parent_edge.child.canonical_key,
            parent_edge.relationship,
            # Set-free edge: value-identical to the former JSON-mode digest (RRM-015).
            stable_json_digest(parent_edge),
            lineage.recorded_at,
            mc.WRITER_REF,
        )
    return lineage


class PostgresDecisionRepository:
    """Durable decision request/response journal with conflicting-replay rejection."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create(self, request: DecisionRequest) -> DurableDecisionRecord:
        scope = request.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"decision:{scope}:{request.decision_id}")
            prior = await _decision_row(connection, args, request.decision_id, lock=True)
            if prior is not None:
                persisted_request = DecisionRequest.model_validate(_json(prior["request_packet"]))
                if persisted_request != request:
                    raise IdempotencyConflict("decision identity has conflicting intent")
                return _decision_record(prior)
            await connection.execute(
                """
                INSERT INTO mission_control.human_task (
                    installation_id, application_id, tenant_id, human_task_id, task_key,
                    target_ref, kind, request_packet_ref, assignee_scope, deadline_at,
                    on_timeout, lifecycle, request_packet, version, updated_at, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, 'open', $11::jsonb, 1,
                        $12, $12, $13)
                """,
                *args,
                uuid7(),
                DECISION_KIND_PREFIX + request.decision_id,
                f"binding:{request.binding_id}",
                DECISION_KIND_PREFIX + request.decision_type,
                f"{request.schema_ref}@{request.request_digest}",
                request.policy_ref,
                request.expires_at,
                _dump(request),
                request.requested_at,
                mc.WRITER_REF,
            )
            return DurableDecisionRecord(request=request)

    async def get(
        self,
        request_scope: str,
        decision_id: str,
    ) -> DurableDecisionRecord | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await _decision_row(connection, args, decision_id)
        return _decision_record(row) if row is not None else None

    async def answer(
        self,
        request: DecisionRequest,
        response: DecisionResponse,
    ) -> DurableDecisionRecord:
        scope = request.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"decision:{scope}:{request.decision_id}")
            prior = await _decision_row(connection, args, request.decision_id, lock=True)
            if prior is None:
                raise LookupError("durable decision request not found")
            persisted_request = DecisionRequest.model_validate(_json(prior["request_packet"]))
            if persisted_request != request:
                raise LookupError("durable decision request not found")
            if prior["answer"] is not None:
                persisted_response = DecisionResponse.model_validate(_json(prior["answer"]))
                if persisted_response != response:
                    raise IdempotencyConflict("decision already has a different response")
                return DurableDecisionRecord(
                    request=persisted_request,
                    status="answered",
                    response=persisted_response,
                )
            await connection.execute(
                """
                INSERT INTO mission_control.human_resolution (
                    installation_id, application_id, tenant_id, resolution_id, human_task_id,
                    actor_ref, answer, answer_ref, answer_digest, expected_task_version,
                    decided_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11, $11, $6)
                """,
                *args,
                uuid7(),
                prior["human_task_id"],
                response.actor_ref,
                _dump(response),
                f"decision-response:{response.response_id}",
                response.response_digest,
                prior["version"],
                response.decided_at,
            )
            await connection.execute(
                f"""
                UPDATE mission_control.human_task
                SET lifecycle = 'resolved', version = version + 1, updated_at = $5
                WHERE {SCOPE} AND human_task_id = $4 AND version = $6
                """,
                *args,
                prior["human_task_id"],
                response.decided_at,
                prior["version"],
            )
            return DurableDecisionRecord(
                request=persisted_request,
                status="answered",
                response=response,
            )


class PostgresForkRepository:
    """Durable fork saga state (v2: snapshot, patch, derived run) for process-loss recovery.

    The advisory guard, the admission and materialization claims and idempotency are kept;
    `reserve()` resolves the sealed source snapshot (a scoped foreign key). Idempotency
    compares the fork intent without its request times (`fork_request_fingerprint`), so a
    later replay continues the persisted intent.
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @asynccontextmanager
    async def guard(self, request: RunForkRequest) -> AsyncIterator[None]:
        key = f"fork-execution:{request.request_scope}:{request.request_id}"
        async with self._pool.acquire() as connection:
            await connection.execute("SELECT pg_advisory_lock(hashtextextended($1, 0))", key)
            try:
                yield
            finally:
                await connection.execute(
                    "SELECT pg_advisory_unlock(hashtextextended($1, 0))",
                    key,
                )

    async def reserve(self, request: RunForkRequest) -> bool:
        scope = request.request_scope
        digest = fork_request_fingerprint(request)
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"fork:{scope}:{request.request_id}")
            prior = await connection.fetchrow(
                f"""
                SELECT request_digest FROM mission_control.fork_request
                WHERE {SCOPE} AND (request_key = $4 OR idempotency_key = $5)
                FOR UPDATE
                """,
                *args,
                request.request_id,
                request.idempotency_key,
            )
            if prior is not None:
                if prior["request_digest"] != digest:
                    raise IdempotencyConflict("fork identity has conflicting intent")
                return False
            snapshot = await connection.fetchrow(
                f"""
                SELECT s.snapshot_digest, s.checkpoint_id, run.run_id, s.projection_version
                FROM mission_control.run_snapshot s
                JOIN mission_control.mission_run run
                  ON run.installation_id = s.installation_id
                 AND run.application_id = s.application_id
                 AND run.tenant_id = s.tenant_id AND run.run_key = s.source_run_key
                WHERE {scoped("s")} AND s.snapshot_key = $4 AND s.source_run_key = $5
                """,
                *args,
                request.snapshot_id,
                request.source_run_id,
            )
            if snapshot is None:
                raise LookupError("fork source snapshot is unavailable")
            if snapshot["snapshot_digest"] != request.snapshot_digest:
                raise IdempotencyConflict("fork names another digest of its source snapshot")
            recovery_id = uuid7()
            await connection.execute(
                """
                INSERT INTO mission_control.recovery_request (
                    installation_id, application_id, tenant_id, recovery_id, source_run_id,
                    source_attempt_id, source_checkpoint_id, kind, actor_ref, action,
                    request_key, payload_digest, expected_source_version, expected_generation,
                    state, target_run_id, reason_ref, result_ref, detail, version, updated_at,
                    created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, NULL, $6, 'fork', $7, $8, $9, $10, $11, NULL,
                        'admitted', NULL, NULL, NULL, $12::jsonb, 1, $13, $13, $7)
                """,
                *args,
                recovery_id,
                snapshot["run_id"],
                snapshot["checkpoint_id"],
                request.actor_id,
                FORK_ACTION,
                request.request_id,
                digest,
                snapshot["projection_version"],
                _dump(
                    {
                        "derived_run_key": request.derived_run_id,
                        "patch_digest": request.patch.patch_digest,
                        "reason": request.reason,
                    }
                ),
                request.requested_at,
            )
            await connection.execute(
                """
                INSERT INTO mission_control.fork_request (
                    installation_id, application_id, tenant_id, fork_request_id, request_key,
                    recovery_id, idempotency_key, schema_version, request_digest,
                    request_payload, status, source_run_key, source_snapshot_key, patch_digest,
                    target_run_key, requested_at, updated_at, retain_until, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, 'reserved', $11, $12,
                        $13, $14, $15, $15, $16, $15, $17)
                """,
                *args,
                uuid7(),
                request.request_id,
                recovery_id,
                request.idempotency_key,
                request.schema_version,
                digest,
                _dump(request),
                request.source_run_id,
                request.snapshot_id,
                request.patch.patch_digest,
                request.derived_run_id,
                request.requested_at,
                request.requested_at + timedelta(days=RETENTION_DAYS),
                request.actor_id,
            )
            return True

    async def get_request(self, request_scope: str, request_id: str) -> RunForkRequest | None:
        payload = await self._column(request_scope, request_id, "request_payload")
        return RunForkRequest.model_validate(_json(payload)) if payload else None

    async def get(self, request_scope: str, request_id: str) -> RunForkReceipt | None:
        payload = await self._column(request_scope, request_id, "receipt_payload")
        return RunForkReceipt.model_validate(_json(payload)) if payload else None

    async def get_admission(
        self,
        request_scope: str,
        request_id: str,
    ) -> ForkAdmission | None:
        payload = await self._column(request_scope, request_id, "admission_payload")
        return ForkAdmission.model_validate(_json(payload)) if payload else None

    async def _column(
        self,
        request_scope: str,
        request_id: str,
        column: Literal["request_payload", "receipt_payload", "admission_payload"],
    ) -> Any:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await connection.fetchval(
                f"""
                SELECT {column} FROM mission_control.fork_request
                WHERE {SCOPE} AND request_key = $4
                """,
                *args,
                request_id,
            )

    async def claim_admission(self, request: RunForkRequest) -> bool:
        return await self._transition(request, from_status="reserved", to_status="admitting")

    async def release_admission_claim(self, request: RunForkRequest) -> None:
        await self._transition(request, from_status="admitting", to_status="reserved")

    async def claim_copy(self, request: RunForkRequest) -> bool:
        return await self._transition(request, from_status="admitted", to_status="copying")

    async def release_copy_claim(self, request: RunForkRequest) -> None:
        await self._transition(request, from_status="copying", to_status="admitted")

    async def _transition(
        self, request: RunForkRequest, *, from_status: str, to_status: str
    ) -> bool:
        scope = request.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"fork:{scope}:{request.request_id}")
            recovery_id = await connection.fetchval(
                f"""
                UPDATE mission_control.fork_request
                SET status = $6, updated_at = $7
                WHERE {SCOPE} AND request_key = $4 AND status = $5
                RETURNING recovery_id
                """,
                *args,
                request.request_id,
                from_status,
                to_status,
                request.requested_at,
            )
            if recovery_id is None:
                return False
            await _set_recovery_state(connection, args, recovery_id, to_status, request)
            return True

    async def record_admission(
        self,
        request: RunForkRequest,
        admission: ForkAdmission,
    ) -> ForkAdmission:
        scope = request.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"fork:{scope}:{request.request_id}")
            row = await _fork_row(connection, args, request.request_id)
            if row is None or row["request_digest"] != fork_request_fingerprint(request):
                raise LookupError("fork reservation is unavailable")
            if row["admission_payload"] is not None:
                persisted = ForkAdmission.model_validate(_json(row["admission_payload"]))
                if persisted != admission:
                    raise IdempotencyConflict("fork admission has conflicting identities")
                return persisted
            if row["status"] != "admitting":
                raise IdempotencyConflict("fork admission was not atomically claimed")
            await connection.execute(
                f"""
                UPDATE mission_control.fork_request
                SET admission_payload = $5::jsonb, status = 'admitted', updated_at = $6
                WHERE {SCOPE} AND request_key = $4
                """,
                *args,
                request.request_id,
                _dump(admission),
                request.requested_at,
            )
            await _set_recovery_state(connection, args, row["recovery_id"], "admitted", request)
            return admission

    async def record(self, request: RunForkRequest, receipt: RunForkReceipt) -> RunForkReceipt:
        scope = request.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, scope)
            await mc.advisory_lock(connection, f"fork:{scope}:{request.request_id}")
            row = await _fork_row(connection, args, request.request_id)
            if row is None or row["request_digest"] != fork_request_fingerprint(request):
                raise LookupError("fork reservation is unavailable")
            if row["receipt_payload"] is not None:
                persisted = RunForkReceipt.model_validate(_json(row["receipt_payload"]))
                if persisted != receipt:
                    raise IdempotencyConflict("fork receipt has conflicting identities")
                return persisted
            if row["status"] != "copying":
                raise IdempotencyConflict("fork materialization was not atomically claimed")
            await connection.execute(
                f"""
                UPDATE mission_control.fork_request
                SET receipt_payload = $5::jsonb, status = 'accepted', updated_at = $6
                WHERE {SCOPE} AND request_key = $4
                """,
                *args,
                request.request_id,
                _dump(receipt),
                receipt.recorded_at,
            )
            await _accept_fork(connection, args, row, request, receipt)
            return receipt


async def _decision_row(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    decision_id: str,
    *,
    lock: bool = False,
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT task.human_task_id, task.request_packet, task.lifecycle, task.version,
               resolution.answer
        FROM mission_control.human_task task
        LEFT JOIN mission_control.human_resolution resolution
          ON resolution.installation_id = task.installation_id
         AND resolution.application_id = task.application_id
         AND resolution.tenant_id = task.tenant_id
         AND resolution.human_task_id = task.human_task_id
        WHERE {scoped("task")} AND task.task_key = $4 AND task.kind LIKE $5
        """
        + (" FOR UPDATE OF task" if lock else ""),
        *args,
        DECISION_KIND_PREFIX + decision_id,
        DECISION_KIND_PREFIX + "%",
    )


def _decision_record(row: asyncpg.Record) -> DurableDecisionRecord:
    response = (
        DecisionResponse.model_validate(_json(row["answer"])) if row["answer"] is not None else None
    )
    status = {"open": "pending", "resolved": "answered"}.get(row["lifecycle"], row["lifecycle"])
    return DurableDecisionRecord(
        request=DecisionRequest.model_validate(_json(row["request_packet"])),
        status=status,
        response=response,
    )


async def _fork_row(
    connection: asyncpg.Connection, args: tuple[Any, ...], request_id: str
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT request_digest, admission_payload, receipt_payload, status, recovery_id,
               source_snapshot_key
        FROM mission_control.fork_request
        WHERE {SCOPE} AND request_key = $4
        FOR UPDATE
        """,
        *args,
        request_id,
    )


async def _set_recovery_state(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    recovery_id: Any,
    saga_status: str,
    request: RunForkRequest,
) -> None:
    await connection.execute(
        f"""
        UPDATE mission_control.recovery_request
        SET state = $5, version = version + 1, updated_at = $6
        WHERE {SCOPE} AND recovery_id = $4
        """,
        *args,
        recovery_id,
        _FORK_STATE[saga_status],
        request.requested_at,
    )


async def _accept_fork(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    row: asyncpg.Record,
    request: RunForkRequest,
    receipt: RunForkReceipt,
) -> None:
    """The accepted fork: completed recovery request and immutable fork lineage."""

    target = await mc.run_row(connection, args, receipt.target_run_id)
    snapshot = await connection.fetchrow(
        f"""
        SELECT s.checkpoint_id, run.run_id AS source_run_id
        FROM mission_control.run_snapshot s
        JOIN mission_control.mission_run run
          ON run.installation_id = s.installation_id AND run.application_id = s.application_id
         AND run.tenant_id = s.tenant_id AND run.run_key = s.source_run_key
        WHERE {scoped("s")} AND s.snapshot_key = $4
        """,
        *args,
        row["source_snapshot_key"],
    )
    await connection.execute(
        f"""
        UPDATE mission_control.recovery_request
        SET state = 'completed', target_run_id = $5, result_ref = $6, version = version + 1,
            updated_at = $7
        WHERE {SCOPE} AND recovery_id = $4
        """,
        *args,
        row["recovery_id"],
        target["run_id"] if target is not None else None,
        receipt.admission_ref,
        receipt.recorded_at,
    )
    if target is None or snapshot is None:
        return
    await connection.execute(
        """
        INSERT INTO mission_control.fork_lineage (
            installation_id, application_id, tenant_id, fork_lineage_id, source_run_id,
            target_run_id, source_checkpoint_id, source_checkpoint_digest,
            copied_artifact_manifest_digest, budget_admission_ref, grant_admission_ref, reason,
            detail, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NULL, $11, $12::jsonb, $13, $14)
        ON CONFLICT (installation_id, application_id, tenant_id, target_run_id) DO NOTHING
        """,
        *args,
        uuid7(),
        snapshot["source_run_id"],
        target["run_id"],
        snapshot["checkpoint_id"],
        receipt.snapshot_digest,
        receipt.lineage.reuse_manifest_digest,
        receipt.admission_ref,
        request.reason,
        _dump(receipt.lineage),
        receipt.recorded_at,
        request.actor_id,
    )


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
