"""PostgreSQL authority of async children on mission_control.

An async child is a canonical ``subordinate_execution`` under its parent run, with the
exact admission and per-child submission fence (REQ-CP-DA-008) in support
``subordinate_admission``. Admission, cancellation, result and settlement commands are
canonical ``command`` rows; observed facts are ``native_observation`` rows; every observed
provider run keeps its usage disposition (REQ-CP-DA-011) in support
``async_provider_run``; typed in_doubt incidents are ``reconciliation_case`` rows.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg

from mission_control.adapters.postgres.operations.checkpoint_lineage import (
    insert_incident,
    resolve_incident_row,
)
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.application.subordinates.inspection import AsyncChildLineageView
from mission_control.application.subordinates.service import (
    AsyncSubagentError,
    AsyncSubagentSpawnRequest,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.async_subagent_reconciliation import (
    ASYNC_SUBAGENT_INCIDENT_TYPE,
    AsyncProviderRunRecord,
    AsyncSubagentIncident,
    AsyncSubagentReconciliationDecision,
)
from mission_control.domain.execution.contracts import (
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    AsyncSubagentMessage,
)

INCIDENT_ACTOR = "mission-control-async-subagent-service"
FACT_RETENTION = timedelta(days=3650)
_DEPENDENCY = {
    "required_blocking": "required",
    "degradable_blocking": "degradable",
    "nonblocking": "nonblocking",
    "advisory": "nonblocking",
}
_RESULT_ADMISSION = {
    "admit": "admitted",
    "conditionally_admit": "admitted",
    "reject": "rejected",
    "defer": "pending",
}


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
            args = await mc.begin(connection, request.request_scope)
            await mc.advisory_lock(
                connection, f"async-admit:{request.request_scope}:{child_execution_id}"
            )
            run = await mc.require_run(connection, args, request.parent_run_id)
            subordinate_id = await _subordinate_id(connection, args, child_execution_id)
            if subordinate_id is None:
                subordinate_id = uuid7()
                await connection.execute(
                    """
                    INSERT INTO mission_control.subordinate_execution (
                        installation_id, application_id, tenant_id, subordinate_id, run_id,
                        subordinate_key, parent_activation_id, parent_attempt_id, execution_kind,
                        dependency_class, binding_ref, binding_digest, dependency_policy,
                        native_task_ref, native_thread_ref, generation, desired_lifecycle,
                        observed_lifecycle, output_refs, result_admission, deadline_at, detail,
                        version, updated_at, created_at, created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, NULL, NULL, 'async', $7, $8, $9, $10::jsonb,
                            NULL, NULL, $11, 'running', 'admitted', '{}', 'pending', NULL,
                            $12::jsonb, 1, $13, $13, $14)
                    """,
                    *args,
                    subordinate_id,
                    run["run_id"],
                    child_execution_id,
                    _DEPENDENCY[request.dependency_class.value],
                    request.contract.contract_id,
                    request.contract.contract_digest,
                    mc.dump(
                        {
                            "dependency_class": request.dependency_class.value,
                            "reservation_id": request.reservation_id,
                        }
                    ),
                    request.execution_generation,
                    mc.dump(
                        {
                            "parent_operation_id": request.parent_operation_id,
                            "parent_binding_id": request.parent_binding_id,
                            "link_id": link_id,
                        }
                    ),
                    now,
                    INCIDENT_ACTOR,
                )
                await connection.execute(
                    """
                    INSERT INTO mission_control.subordinate_admission (
                        installation_id, application_id, tenant_id, subordinate_admission_id,
                        subordinate_key, subordinate_id, parent_run_key, parent_operation_key,
                        parent_binding_key, link_key, contract_key, contract_digest,
                        reservation_key, dependency_class, execution_generation, lifecycle,
                        lifecycle_updated_at, cancellation_requested, submission_fence,
                        updated_at, created_at, created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
                            'admitted', $16, false, 0, $16, $16, $17)
                    """,
                    *args,
                    uuid7(),
                    child_execution_id,
                    subordinate_id,
                    request.parent_run_id,
                    request.parent_operation_id,
                    request.parent_binding_id,
                    link_id,
                    request.contract.contract_id,
                    request.contract.contract_digest,
                    request.reservation_id,
                    request.dependency_class.value,
                    request.execution_generation,
                    now,
                    INCIDENT_ACTOR,
                )
            await _insert_command(
                connection,
                args,
                command_key=command_id,
                subordinate_id=subordinate_id,
                kind="admit",
                payload={
                    "reservation_id": request.reservation_id,
                    "link_id": link_id,
                    "parent_binding_id": request.parent_binding_id,
                    "graph_id": request.contract.graph_id,
                    "graph_revision": request.contract.graph_revision,
                    "graph_binding_digest": request.contract.graph_binding_digest,
                },
                recorded_at=now,
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
            args = await mc.begin(connection, request_scope)
            row = await connection.fetchrow(
                f"""
                UPDATE mission_control.subordinate_admission
                SET submission_fence = submission_fence + 1, submission_holder = $5,
                    submission_lease_expires_at = $6, updated_at = $7
                WHERE {SCOPE} AND subordinate_key = $4
                  AND (submission_holder IS NULL OR submission_lease_expires_at <= $7)
                RETURNING submission_fence
                """,
                *args,
                child_execution_id,
                holder,
                lease_expires_at,
                now,
            )
            if row is not None:
                return int(row["submission_fence"])
            if await _subordinate_id(connection, args, child_execution_id) is None:
                raise AsyncSubagentError("async child authority row is missing")
            return None

    async def release_submission_fence(
        self, request_scope: str, child_execution_id: str, fence: int
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await connection.execute(
                f"""
                UPDATE mission_control.subordinate_admission
                SET submission_holder = NULL, submission_lease_expires_at = NULL, updated_at = $6
                WHERE {SCOPE} AND subordinate_key = $4 AND submission_fence = $5
                """,
                *args,
                child_execution_id,
                fence,
                datetime.now(UTC),
            )

    # ------------------------------------------------------------------ lifecycle mirror

    async def record_execution_state(
        self, request_scope: str, execution: AsyncSubagentExecution
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await connection.execute(
                f"""
                UPDATE mission_control.subordinate_admission
                SET lifecycle = $5, provider_thread_key = $6, provider_run_key = $7,
                    in_doubt_reason = $8, incident_key = $9, lifecycle_updated_at = $10,
                    updated_at = $10
                WHERE {SCOPE} AND subordinate_key = $4
                """,
                *args,
                execution.child_execution_id,
                execution.lifecycle.value,
                execution.provider_thread_id,
                execution.provider_run_id,
                execution.in_doubt_reason,
                execution.incident_id,
                execution.updated_at,
            )
            await connection.execute(
                f"""
                UPDATE mission_control.subordinate_execution
                SET observed_lifecycle = $5, native_thread_ref = $6, native_task_ref = $7,
                    version = version + 1, updated_at = $8
                WHERE {SCOPE} AND subordinate_key = $4
                """,
                *args,
                execution.child_execution_id,
                execution.lifecycle.value,
                execution.provider_thread_id,
                execution.provider_run_id,
                execution.updated_at,
            )

    async def record_provider_run(self, request_scope: str, record: AsyncProviderRunRecord) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await connection.execute(
                """
                INSERT INTO mission_control.async_provider_run (
                    installation_id, application_id, tenant_id, async_provider_run_id,
                    subordinate_key, provider_run_key, schema_version, provider_thread_key,
                    disposition, provider_status, usage_attribution, attributed_amounts,
                    pending_amounts, record_payload, observed_at, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13::jsonb,
                        $14::jsonb, $15, $15, $16)
                ON CONFLICT (installation_id, application_id, tenant_id, subordinate_key,
                             provider_run_key) DO UPDATE
                SET disposition = EXCLUDED.disposition, provider_status = EXCLUDED.provider_status,
                    usage_attribution = EXCLUDED.usage_attribution,
                    attributed_amounts = EXCLUDED.attributed_amounts,
                    pending_amounts = EXCLUDED.pending_amounts,
                    record_payload = EXCLUDED.record_payload, observed_at = EXCLUDED.observed_at
                WHERE mission_control.async_provider_run.disposition = 'bound'
                   OR EXCLUDED.disposition <> 'bound'
                """,
                *args,
                uuid7(),
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
                INCIDENT_ACTOR,
            )

    async def cancelled_provider_run_ids(
        self, request_scope: str, child_execution_id: str
    ) -> frozenset[str]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT provider_run_key FROM mission_control.async_provider_run
                WHERE {SCOPE} AND subordinate_key = $4
                  AND disposition IN ('duplicate_cancelled', 'orphaned_cancelled')
                """,
                *args,
                child_execution_id,
            )
            return frozenset(str(row["provider_run_key"]) for row in rows)

    # ------------------------------------------------------------------ incidents

    async def open_incident(self, incident: AsyncSubagentIncident) -> AsyncSubagentIncident:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, incident.request_scope)
            await insert_incident(
                connection,
                args,
                incident_type=ASYNC_SUBAGENT_INCIDENT_TYPE,
                identity_digest=incident.identity_digest,
                target_ref=incident.child_execution_id,
                reason=incident.reason,
                status=incident.status,
                payload=incident.model_dump(mode="json"),
                recorded_at=incident.recorded_at,
                actor_ref=INCIDENT_ACTOR,
            )
            stored = await _incident(connection, args, incident.child_execution_id)
        assert stored is not None
        return stored

    async def get_incident(
        self, request_scope: str, child_execution_id: str
    ) -> AsyncSubagentIncident | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await _incident(connection, args, child_execution_id)

    async def resolve_incident(self, incident: AsyncSubagentIncident) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, incident.request_scope)
            current = await _incident(connection, args, incident.child_execution_id)
            if current is None or current.revision != incident.revision:
                raise AsyncSubagentError("no open incident matches the resolution")
            if current.status == "resolved":
                if current.decision_id != incident.decision_id:
                    raise AsyncSubagentError("the incident is already resolved by another decision")
                return
            await resolve_incident_row(
                connection,
                args,
                incident_type=ASYNC_SUBAGENT_INCIDENT_TYPE,
                identity_digest=incident.identity_digest,
                payload=incident.model_dump(mode="json"),
                updated_at=datetime.now(UTC),
            )

    async def claim_reconciliation_decision(
        self,
        request_scope: str,
        child_execution_id: str,
        decision: AsyncSubagentReconciliationDecision,
        *,
        decision_id: str,
        adopted_run_id: str | None,
        reason: str,
    ) -> bool:
        """Insert the child's single decision command (unique per child); replay-safe."""

        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            subordinate_id = await _require_subordinate(connection, args, child_execution_id)
            await _insert_command(
                connection,
                args,
                command_key=decision_id,
                subordinate_id=subordinate_id,
                kind=decision,
                payload={"adopted_run_id": adopted_run_id, "reason": reason},
                recorded_at=datetime.now(UTC),
            )
            holder = await connection.fetchval(
                f"""
                SELECT command_key FROM mission_control.command
                WHERE {SCOPE} AND subordinate_id = $4
                  AND command_kind IN ('adopt_provider_run', 'orphan_child')
                """,
                *args,
                subordinate_id,
            )
            return bool(holder == decision_id)

    async def list_provider_runs(
        self, request_scope: str, child_execution_id: str
    ) -> tuple[AsyncProviderRunRecord, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return await _provider_runs(connection, args, child_execution_id)

    async def record_fact(
        self, request_scope: str, child_execution_id: str, fact_kind: str, fact_ref: str
    ) -> None:
        fact_id = str(
            uuid5(
                NAMESPACE_URL,
                f"async-fact:{request_scope}:{child_execution_id}:{fact_kind}:{fact_ref}",
            )
        )
        now = datetime.now(UTC)
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            child = await connection.fetchrow(
                f"""
                SELECT subordinate_id, execution_generation
                FROM mission_control.subordinate_admission
                WHERE {SCOPE} AND subordinate_key = $4
                """,
                *args,
                child_execution_id,
            )
            if child is None:
                raise AsyncSubagentError("async child authority row is missing")
            await connection.execute(
                """
                INSERT INTO mission_control.native_observation (
                    installation_id, application_id, tenant_id, native_observation_id,
                    harness_execution_id, subordinate_id, native_event_key, cursor, generation,
                    received_at, payload, payload_ref, retain_until, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, NULL, $5, $6, NULL, $7, $8, $9::jsonb, NULL, $10, $8, $11)
                ON CONFLICT (installation_id, application_id, tenant_id, native_event_key)
                DO NOTHING
                """,
                *args,
                uuid7(),
                child["subordinate_id"],
                fact_id,
                child["execution_generation"],
                now,
                mc.dump(
                    {
                        "contract": "mc.async-child-fact/1",
                        "child_execution_id": child_execution_id,
                        "fact_kind": fact_kind,
                        "fact_ref": fact_ref,
                    }
                ),
                now + FACT_RETENTION,
                INCIDENT_ACTOR,
            )

    async def append_message(self, request_scope: str, message: AsyncSubagentMessage) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await connection.execute(
                """
                INSERT INTO mission_control.subordinate_message (
                    installation_id, application_id, tenant_id, subordinate_message_id,
                    message_key, subordinate_key, direction, target_sequence, receipt,
                    message_contract, payload, recorded_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, $12, $13)
                ON CONFLICT (installation_id, application_id, tenant_id, message_key) DO NOTHING
                """,
                *args,
                uuid7(),
                message.message_id,
                message.child_execution_id,
                message.direction,
                message.target_sequence,
                message.receipt,
                message.schema_version,
                json.dumps(message.model_dump(mode="json")),
                message.created_at,
                INCIDENT_ACTOR,
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

    async def list_child_ids(self, request_scope: str, parent_binding_id: str) -> tuple[str, ...]:
        """RRM-008: the children one parent operation binding spawned, in admission order."""

        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT subordinate_key FROM mission_control.subordinate_admission
                WHERE {SCOPE} AND parent_binding_key = $4
                ORDER BY created_at, subordinate_key
                """,
                *args,
                parent_binding_id,
            )
        return tuple(str(row["subordinate_key"]) for row in rows)

    async def list_children(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[AsyncChildLineageView, ...]:
        """REQ-CP-RUN-011: read-only child lineage of one parent run, from authority rows."""

        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT a.*, c.payload AS admit_payload,
                       r.payload AS reconciliation_payload, r.command_kind AS reconciliation_kind
                FROM mission_control.subordinate_admission a
                LEFT JOIN mission_control.command c
                  ON c.installation_id = a.installation_id
                 AND c.application_id = a.application_id AND c.tenant_id = a.tenant_id
                 AND c.subordinate_id = a.subordinate_id AND c.command_kind = 'admit'
                LEFT JOIN mission_control.command r
                  ON r.installation_id = a.installation_id
                 AND r.application_id = a.application_id AND r.tenant_id = a.tenant_id
                 AND r.subordinate_id = a.subordinate_id
                 AND r.command_kind IN ('adopt_provider_run', 'orphan_child')
                WHERE {scoped("a")} AND a.parent_run_key = $4
                ORDER BY a.created_at, a.subordinate_key
                """,
                *args,
                parent_run_id,
            )
            views: list[AsyncChildLineageView] = []
            for row in rows:
                runs = await _provider_runs(connection, args, row["subordinate_key"])
                admit = _json(row["admit_payload"])
                views.append(
                    AsyncChildLineageView(
                        request_scope=request_scope,
                        child_execution_id=row["subordinate_key"],
                        parent_run_id=row["parent_run_key"],
                        parent_operation_id=row["parent_operation_key"],
                        parent_binding_id=row["parent_binding_key"]
                        or admit.get("parent_binding_id"),
                        contract_id=row["contract_key"],
                        contract_digest=row["contract_digest"],
                        graph_id=admit.get("graph_id"),
                        graph_revision=admit.get("graph_revision"),
                        graph_binding_digest=admit.get("graph_binding_digest"),
                        lifecycle=AsyncSubagentLifecycle(row["lifecycle"]),
                        provider_thread_id=row["provider_thread_key"],
                        provider_run_id=row["provider_run_key"],
                        submission_fence=int(row["submission_fence"]),
                        submission_holder=row["submission_holder"],
                        in_doubt_reason=row["in_doubt_reason"],
                        incident_id=row["incident_key"],
                        reconciliation_decision=row["reconciliation_kind"],
                        result_decision=row["result_decision"],
                        settlement_ref=row["settlement_ref"],
                        provider_runs=runs,
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
            args = await mc.begin(connection, request_scope)
            subordinate_id = await _require_subordinate(connection, args, child_execution_id)
            await connection.execute(
                f"""
                UPDATE mission_control.subordinate_admission SET
                    cancellation_requested = cancellation_requested OR $5,
                    result_decision = COALESCE($6, result_decision),
                    result_manifest_digest = COALESCE($7, result_manifest_digest),
                    settlement_ref = COALESCE($8, settlement_ref), updated_at = $9
                WHERE {SCOPE} AND subordinate_key = $4
                """,
                *args,
                child_execution_id,
                cancellation,
                decision,
                manifest_digest,
                settlement_ref,
                now,
            )
            await connection.execute(
                f"""
                UPDATE mission_control.subordinate_execution SET
                    desired_lifecycle = CASE WHEN $5 THEN 'cancelled' ELSE desired_lifecycle END,
                    result_admission = COALESCE($6, result_admission),
                    version = version + 1, updated_at = $7
                WHERE {SCOPE} AND subordinate_id = $4
                """,
                *args,
                subordinate_id,
                cancellation,
                _RESULT_ADMISSION.get(decision) if decision is not None else None,
                now,
            )
            await _insert_command(
                connection,
                args,
                command_key=command_id,
                subordinate_id=subordinate_id,
                kind=kind,
                payload=payload,
                recorded_at=now,
            )


async def _subordinate_id(
    connection: asyncpg.Connection, args: tuple[Any, ...], child_execution_id: str
) -> UUID | None:
    return await connection.fetchval(
        f"""
        SELECT subordinate_id FROM mission_control.subordinate_admission
        WHERE {SCOPE} AND subordinate_key = $4
        """,
        *args,
        child_execution_id,
    )


async def _require_subordinate(
    connection: asyncpg.Connection, args: tuple[Any, ...], child_execution_id: str
) -> UUID:
    value = await _subordinate_id(connection, args, child_execution_id)
    if value is None:
        raise AsyncSubagentError("async child authority row is missing")
    return value


async def _insert_command(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    command_key: str,
    subordinate_id: UUID,
    kind: str,
    payload: dict[str, object],
    recorded_at: datetime,
) -> None:
    """An accepted child command (idempotent by key; one reconciliation decision per child)."""

    await connection.execute(
        f"""
        INSERT INTO mission_control.command (
            installation_id, application_id, tenant_id, command_id, command_key, mission_id,
            run_id, activation_id, subordinate_id, target_generation, target_version,
            command_kind, payload, payload_digest, payload_ref, deadline_at, lifecycle, outcome,
            requested_by_actor_ref, version, updated_at, created_at, created_by_actor_ref
        )
        SELECT $1, $2, $3, $4, $5, run.mission_id, run.run_id, NULL, sub.subordinate_id,
               sub.generation, NULL, $7, $8::jsonb, $9, NULL, NULL, 'accepted', NULL, $10, 1,
               $11, $11, $10
        FROM mission_control.subordinate_execution sub
        JOIN mission_control.mission_run run
          ON run.installation_id = sub.installation_id
         AND run.application_id = sub.application_id
         AND run.tenant_id = sub.tenant_id AND run.run_id = sub.run_id
        WHERE {scoped("sub")} AND sub.subordinate_id = $6
        ON CONFLICT DO NOTHING
        """,
        *args,
        uuid7(),
        command_key,
        subordinate_id,
        kind,
        mc.dump(payload),
        sha256_digest(payload),
        INCIDENT_ACTOR,
        recorded_at,
    )


async def _provider_runs(
    connection: asyncpg.Connection, args: tuple[Any, ...], child_execution_id: str
) -> tuple[AsyncProviderRunRecord, ...]:
    rows = await connection.fetch(
        f"""
        SELECT record_payload FROM mission_control.async_provider_run
        WHERE {SCOPE} AND subordinate_key = $4
        ORDER BY observed_at, provider_run_key
        """,
        *args,
        child_execution_id,
    )
    return tuple(
        AsyncProviderRunRecord.model_validate(_json(row["record_payload"])) for row in rows
    )


async def _incident(
    connection: asyncpg.Connection, args: tuple[Any, ...], child_execution_id: str
) -> AsyncSubagentIncident | None:
    payload = await connection.fetchval(
        f"""
        SELECT detail FROM mission_control.reconciliation_case
        WHERE {SCOPE} AND target_kind = $4 AND target_ref = $5
        ORDER BY COALESCE((detail->>'revision')::bigint, 1) DESC LIMIT 1
        """,
        *args,
        ASYNC_SUBAGENT_INCIDENT_TYPE,
        child_execution_id,
    )
    if payload is None:
        return None
    return AsyncSubagentIncident.model_validate(_json(payload))


def _json(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    loaded = json.loads(value)
    return loaded if isinstance(loaded, dict) else {}
