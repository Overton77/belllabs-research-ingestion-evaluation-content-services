"""PostgreSQL authority of async children (migrations 0016 and 0021).

Reservation and admission, the per-child submission fence (REQ-CP-DA-008), the lifecycle
mirror used by inspection (REQ-CP-RUN-011), every observed provider run with its usage
disposition (REQ-CP-DA-011), typed in_doubt incidents in `runtime_reconciliation_incidents`,
and the immutable command and fact ledgers. Every statement runs under the request-scope RLS
setting; production pools connect as `belllabs_control_runtime`.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid5

import asyncpg

from app.application.async_subagents.inspection import AsyncChildLineageView
from app.application.async_subagents.service import (
    AsyncSubagentError,
    AsyncSubagentSpawnRequest,
)
from app.domain.operation_execution.async_subagent_reconciliation import (
    ASYNC_SUBAGENT_INCIDENT_TYPE,
    AsyncProviderRunRecord,
    AsyncSubagentIncident,
    AsyncSubagentReconciliationDecision,
)
from app.domain.operation_execution.contracts import (
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    AsyncSubagentMessage,
)

INCIDENT_ACTOR = "belllabs-async-subagent-service"
INCIDENT_RETENTION = timedelta(days=3650)


class PostgresAsyncSubagentAuthority:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def reserve_and_admit(
        self, request: AsyncSubagentSpawnRequest, child_execution_id: str, link_id: str
    ) -> None:
        now = request.requested_at
        command_id = str(
            uuid5(NAMESPACE_URL, f"async-admit:{request.request_scope}:{child_execution_id}")
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request.request_scope)
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_authority
                (request_scope, child_execution_id, parent_run_id, parent_operation_id, link_id,
                 contract_id, contract_digest, reservation_id, dependency_class,
                 execution_generation, created_at, updated_at, lifecycle, lifecycle_updated_at,
                 parent_binding_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$11,'admitted',$11,$12)
                ON CONFLICT (request_scope, child_execution_id) DO NOTHING""",
                request.request_scope,
                child_execution_id,
                request.parent_run_id,
                request.parent_operation_id,
                link_id,
                request.contract.contract_id,
                request.contract.contract_digest,
                request.reservation_id,
                request.dependency_class.value,
                request.execution_generation,
                now,
                request.parent_binding_id,
            )
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_commands
                (command_id, request_scope, child_execution_id, command_kind, payload, recorded_at)
                VALUES ($1,$2,$3,'admit',$4::jsonb,$5) ON CONFLICT (command_id) DO NOTHING""",
                command_id,
                request.request_scope,
                child_execution_id,
                json.dumps(
                    {
                        "reservation_id": request.reservation_id,
                        "link_id": link_id,
                        "parent_binding_id": request.parent_binding_id,
                        "graph_id": request.contract.graph_id,
                        "graph_revision": request.contract.graph_revision,
                        "graph_binding_digest": request.contract.graph_binding_digest,
                    }
                ),
                now,
            )

    # ------------------------------------------------------------------ submission fence

    async def acquire_submission_fence(
        self,
        request_scope: str,
        child_execution_id: str,
        *,
        holder: str,
        lease_expires_at: datetime,
        now: datetime,
    ) -> int | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """UPDATE belllabs_control.async_subagent_authority
                SET submission_fence = submission_fence + 1, submission_holder = $3,
                    submission_lease_expires_at = $4, updated_at = $5
                WHERE request_scope = $1 AND child_execution_id = $2
                  AND (submission_holder IS NULL OR submission_lease_expires_at <= $5)
                RETURNING submission_fence""",
                request_scope,
                child_execution_id,
                holder,
                lease_expires_at,
                now,
            )
            if row is not None:
                return int(row["submission_fence"])
            exists = await connection.fetchval(
                """SELECT 1 FROM belllabs_control.async_subagent_authority
                WHERE request_scope = $1 AND child_execution_id = $2""",
                request_scope,
                child_execution_id,
            )
            if exists is None:
                raise AsyncSubagentError("async child authority row is missing")
            return None

    async def release_submission_fence(
        self, request_scope: str, child_execution_id: str, fence: int
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """UPDATE belllabs_control.async_subagent_authority
                SET submission_holder = NULL, submission_lease_expires_at = NULL, updated_at = $4
                WHERE request_scope = $1 AND child_execution_id = $2 AND submission_fence = $3""",
                request_scope,
                child_execution_id,
                fence,
                datetime.now(UTC),
            )

    # ------------------------------------------------------------------ lifecycle mirror

    async def record_execution_state(
        self, request_scope: str, execution: AsyncSubagentExecution
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """UPDATE belllabs_control.async_subagent_authority
                SET lifecycle = $3, provider_thread_id = $4, provider_run_id = $5,
                    in_doubt_reason = $6, incident_id = $7, lifecycle_updated_at = $8,
                    updated_at = $8
                WHERE request_scope = $1 AND child_execution_id = $2""",
                request_scope,
                execution.child_execution_id,
                execution.lifecycle.value,
                execution.provider_thread_id,
                execution.provider_run_id,
                execution.in_doubt_reason,
                execution.incident_id,
                execution.updated_at,
            )

    async def record_provider_run(self, request_scope: str, record: AsyncProviderRunRecord) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_provider_runs
                (request_scope, child_execution_id, provider_run_id, schema_version,
                 provider_thread_id, disposition, provider_status, usage_attribution,
                 attributed_amounts, pending_amounts, record_payload, observed_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,$12)
                ON CONFLICT (request_scope, child_execution_id, provider_run_id) DO UPDATE
                SET disposition = EXCLUDED.disposition, provider_status = EXCLUDED.provider_status,
                    usage_attribution = EXCLUDED.usage_attribution,
                    attributed_amounts = EXCLUDED.attributed_amounts,
                    pending_amounts = EXCLUDED.pending_amounts,
                    record_payload = EXCLUDED.record_payload, observed_at = EXCLUDED.observed_at
                WHERE belllabs_control.async_subagent_provider_runs.disposition = 'bound'
                   OR EXCLUDED.disposition <> 'bound'""",
                request_scope,
                record.child_execution_id,
                record.provider_run_id,
                record.schema_version,
                record.provider_thread_id,
                record.disposition,
                record.provider_status,
                record.usage.attribution,
                json.dumps(record.usage.attributed_amounts),
                json.dumps(record.usage.pending_amounts),
                json.dumps(record.model_dump(mode="json")),
                record.observed_at,
            )

    async def cancelled_provider_run_ids(
        self, request_scope: str, child_execution_id: str
    ) -> frozenset[str]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """SELECT provider_run_id FROM belllabs_control.async_subagent_provider_runs
                   WHERE request_scope = $1 AND child_execution_id = $2
                     AND disposition IN ('duplicate_cancelled', 'orphaned_cancelled')""",
                request_scope,
                child_execution_id,
            )
            return frozenset(str(row["provider_run_id"]) for row in rows)

    # ------------------------------------------------------------------ incidents

    async def open_incident(self, incident: AsyncSubagentIncident) -> AsyncSubagentIncident:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, incident.request_scope)
            await connection.execute(
                """INSERT INTO belllabs_control.runtime_reconciliation_incidents (
                    incident_id, request_scope, incident_type, severity, status,
                    identity_digest, actor_ref, reason, evidence_refs, incident_payload,
                    version, recorded_at, updated_at, retain_until
                ) VALUES ($1,$2,$3,'error',$4,$5,$6,$7,$8::jsonb,$9::jsonb,1,$10,$10,$11)
                ON CONFLICT (request_scope, incident_type, identity_digest) DO NOTHING""",
                incident.incident_id,
                incident.request_scope,
                ASYNC_SUBAGENT_INCIDENT_TYPE,
                incident.status,
                incident.identity_digest,
                INCIDENT_ACTOR,
                incident.reason,
                json.dumps(list(incident.candidate_run_ids)),
                json.dumps(incident.model_dump(mode="json")),
                incident.recorded_at,
                incident.recorded_at + INCIDENT_RETENTION,
            )
            stored = await _incident(
                connection, incident.request_scope, incident.child_execution_id
            )
        assert stored is not None
        return stored

    async def get_incident(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentIncident | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            return await _incident(connection, request_scope, child_execution_id)

    async def resolve_incident(self, incident: AsyncSubagentIncident) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, incident.request_scope)
            current = await _incident(
                connection, incident.request_scope, incident.child_execution_id
            )
            if current is None or current.revision != incident.revision:
                raise AsyncSubagentError("no open incident matches the resolution")
            if current.status == "resolved":
                if current.decision_id != incident.decision_id:
                    raise AsyncSubagentError("the incident is already resolved by another decision")
                return
            await connection.execute(
                """UPDATE belllabs_control.runtime_reconciliation_incidents
                SET status = 'resolved', incident_payload = $4::jsonb, version = version + 1,
                    updated_at = $5
                WHERE request_scope = $1 AND incident_type = $2 AND identity_digest = $3""",
                incident.request_scope,
                ASYNC_SUBAGENT_INCIDENT_TYPE,
                incident.identity_digest,
                json.dumps(incident.model_dump(mode="json")),
                datetime.now(UTC),
            )

    async def record_reconciliation_decision(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: AsyncSubagentReconciliationDecision,
        *,
        decision_id: str,
        adopted_run_id: str | None,
        reason: str,
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_commands
                (command_id, request_scope, child_execution_id, command_kind, payload, recorded_at)
                VALUES ($1,$2,$3,$4,$5::jsonb,$6) ON CONFLICT (command_id) DO NOTHING""",
                decision_id,
                request_scope,
                child_execution_id,
                decision,
                json.dumps({"adopted_run_id": adopted_run_id, "reason": reason}),
                datetime.now(UTC),
            )

    # ------------------------------------------------------------------ ledgers

    async def record_fact(
        self, request_scope: str, child_execution_id: str, fact_kind: str, fact_ref: str
    ) -> None:
        fact_id = str(
            uuid5(
                NAMESPACE_URL,
                f"async-fact:{request_scope}:{child_execution_id}:{fact_kind}:{fact_ref}",
            )
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_facts
                (fact_id, request_scope, child_execution_id, fact_kind, fact_ref, recorded_at)
                VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (fact_id) DO NOTHING""",
                fact_id,
                request_scope,
                child_execution_id,
                fact_kind,
                fact_ref,
                datetime.now(UTC),
            )

    async def append_message(self, request_scope: str, message: AsyncSubagentMessage) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_messages
                (message_id, request_scope, child_execution_id, direction,
                 target_sequence, receipt, payload, recorded_at)
                VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8) ON CONFLICT (message_id) DO NOTHING""",
                message.message_id,
                request_scope,
                message.child_execution_id,
                message.direction,
                message.target_sequence,
                message.receipt,
                json.dumps(message.model_dump(mode="json")),
                message.created_at,
            )

    async def request_cancellation(
        self, request_scope: str, child_execution_id: str, reason: str
    ) -> None:
        await self._command(
            request_scope, child_execution_id, "cancel", {"reason": reason}, cancellation=True
        )

    async def decide_result(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: Literal["admit", "conditionally_admit", "reject", "defer"],
        manifest_digest: str,
    ) -> None:
        await self._command(
            request_scope,
            child_execution_id,
            "result_decision",
            {"decision": decision, "manifest_digest": manifest_digest},
            decision=decision,
            manifest_digest=manifest_digest,
        )

    async def settle(
        self, request_scope: str, child_execution_id: str, settlement_ref: str
    ) -> None:
        await self._command(
            request_scope,
            child_execution_id,
            "settle",
            {"settlement_ref": settlement_ref},
            settlement_ref=settlement_ref,
        )

    # ------------------------------------------------------------------ inspection

    async def list_children(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[AsyncChildLineageView, ...]:
        """REQ-CP-RUN-011: read-only child lineage of one parent run, from authority rows."""

        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """SELECT a.*, c.payload AS admit_payload,
                          r.payload AS reconciliation_payload, r.command_kind AS reconciliation_kind
                   FROM belllabs_control.async_subagent_authority a
                   LEFT JOIN belllabs_control.async_subagent_commands c
                     ON c.request_scope = a.request_scope
                    AND c.child_execution_id = a.child_execution_id AND c.command_kind = 'admit'
                   LEFT JOIN belllabs_control.async_subagent_commands r
                     ON r.request_scope = a.request_scope
                    AND r.child_execution_id = a.child_execution_id
                    AND r.command_kind IN ('adopt_provider_run', 'orphan_child')
                   WHERE a.request_scope = $1 AND a.parent_run_id = $2
                   ORDER BY a.created_at, a.child_execution_id""",
                request_scope,
                parent_run_id,
            )
            views: list[AsyncChildLineageView] = []
            for row in rows:
                runs = await connection.fetch(
                    """SELECT record_payload FROM belllabs_control.async_subagent_provider_runs
                       WHERE request_scope = $1 AND child_execution_id = $2
                       ORDER BY observed_at, provider_run_id""",
                    request_scope,
                    row["child_execution_id"],
                )
                admit = _json(row["admit_payload"])
                views.append(
                    AsyncChildLineageView(
                        request_scope=request_scope,
                        child_execution_id=row["child_execution_id"],
                        parent_run_id=row["parent_run_id"],
                        parent_operation_id=row["parent_operation_id"],
                        parent_binding_id=row["parent_binding_id"]
                        or admit.get("parent_binding_id"),
                        contract_id=row["contract_id"],
                        contract_digest=row["contract_digest"],
                        graph_id=admit.get("graph_id"),
                        graph_revision=admit.get("graph_revision"),
                        graph_binding_digest=admit.get("graph_binding_digest"),
                        lifecycle=AsyncSubagentLifecycle(row["lifecycle"]),
                        provider_thread_id=row["provider_thread_id"],
                        provider_run_id=row["provider_run_id"],
                        submission_fence=int(row["submission_fence"]),
                        submission_holder=row["submission_holder"],
                        in_doubt_reason=row["in_doubt_reason"],
                        incident_id=row["incident_id"],
                        reconciliation_decision=row["reconciliation_kind"],
                        result_decision=row["result_decision"],
                        settlement_ref=row["settlement_ref"],
                        provider_runs=tuple(
                            AsyncProviderRunRecord.model_validate(_json(item["record_payload"]))
                            for item in runs
                        ),
                        updated_at=row["updated_at"],
                    )
                )
            return tuple(views)

    async def _command(
        self,
        request_scope: str,
        child_execution_id: str,
        kind: str,
        payload: dict[str, object],
        *,
        cancellation: bool = False,
        decision: str | None = None,
        manifest_digest: str | None = None,
        settlement_ref: str | None = None,
    ) -> None:
        command_id = str(
            uuid5(
                NAMESPACE_URL,
                f"async-command:{request_scope}:{child_execution_id}:{kind}:{payload}",
            )
        )
        now = datetime.now(UTC)
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                """UPDATE belllabs_control.async_subagent_authority SET
                cancellation_requested = cancellation_requested OR $3,
                result_decision = COALESCE($4, result_decision),
                result_manifest_digest = COALESCE($5, result_manifest_digest),
                settlement_ref = COALESCE($6, settlement_ref), updated_at = $7
                WHERE request_scope=$1 AND child_execution_id=$2""",
                request_scope,
                child_execution_id,
                cancellation,
                decision,
                manifest_digest,
                settlement_ref,
                now,
            )
            await connection.execute(
                """INSERT INTO belllabs_control.async_subagent_commands
                (command_id, request_scope, child_execution_id, command_kind, payload, recorded_at)
                VALUES ($1,$2,$3,$4,$5::jsonb,$6) ON CONFLICT (command_id) DO NOTHING""",
                command_id,
                request_scope,
                child_execution_id,
                kind,
                json.dumps(payload),
                now,
            )


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute("SELECT set_config('belllabs.request_scope', $1, true)", request_scope)


async def _incident(
    connection: asyncpg.Connection, request_scope: str, child_execution_id: str
) -> AsyncSubagentIncident | None:
    row = await connection.fetchrow(
        """SELECT incident_payload FROM belllabs_control.runtime_reconciliation_incidents
           WHERE request_scope = $1 AND incident_type = $2
             AND incident_payload->>'child_execution_id' = $3
           ORDER BY COALESCE((incident_payload->>'revision')::bigint, 1) DESC LIMIT 1""",
        request_scope,
        ASYNC_SUBAGENT_INCIDENT_TYPE,
        child_execution_id,
    )
    if row is None:
        return None
    return AsyncSubagentIncident.model_validate(_json(row["incident_payload"]))


def _json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    loaded = json.loads(value)
    return loaded if isinstance(loaded, dict) else {}
