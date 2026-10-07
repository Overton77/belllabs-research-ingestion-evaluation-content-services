"""`PostgresAsyncSubagentAuthority` on mission_control under the production runtime role.

Every repository call runs as a restricted login of `mission_control_runtime`, so forced
RLS and the least-privilege grants are what is exercised: the submission fence with lease
takeover, the lifecycle mirror on the canonical subordinate execution, provider-run
records with pending usage, typed in_doubt incidents (canonical reconciliation cases),
decisions (canonical commands) and the inspection read.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
from mission_control.adapters.postgres.scope import apply_scope
from mission_control.domain.execution.async_subagent_reconciliation import (
    AsyncProviderRunRecord,
    AsyncSubagentIncident,
    async_subagent_incident_id,
)
from mission_control.domain.execution.contracts import (
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    AsyncSubagentUsage,
)
from tests.acceptance.control_plane.test_wp_cp_045 import request as spawn_request
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows
from tests.integration.postgres.test_checkpoint_lineage_postgres import RUNTIME_ROLE, admit_run

pytestmark = pytest.mark.common_db


@pytest.mark.asyncio
async def test_runtime_role_fences_submission_and_records_in_doubt_lineage(
    common_db: CommonDatabase,
) -> None:
    runtime = await common_db.pool(max_size=4)
    scope = common_db.scope()
    try:
        run_id = await admit_run(runtime, common_db)
        authority = PostgresAsyncSubagentAuthority(runtime)
        async with runtime.acquire() as connection:
            assert await connection.fetchval(
                "SELECT pg_has_role(current_user, $1, 'MEMBER')", RUNTIME_ROLE
            )

        base = spawn_request().model_copy(update={"request_scope": scope, "parent_run_id": run_id})
        child_id = f"child-{uuid4().hex[:12]}"
        await authority.reserve_and_admit(base, child_id, f"link-{child_id}")
        await authority.reserve_and_admit(base, child_id, f"link-{child_id}")  # idempotent

        now = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
        first = await authority.acquire_submission_fence(
            scope,
            child_id,
            holder="worker-1",
            lease_expires_at=now + timedelta(seconds=60),
            now=now,
        )
        assert first == 1
        # A live lease blocks a second submitter; an expired lease is taken over by fence 2.
        assert (
            await authority.acquire_submission_fence(
                scope,
                child_id,
                holder="worker-2",
                lease_expires_at=now + timedelta(seconds=120),
                now=now + timedelta(seconds=10),
            )
            is None
        )
        second = await authority.acquire_submission_fence(
            scope,
            child_id,
            holder="worker-2",
            lease_expires_at=now + timedelta(seconds=180),
            now=now + timedelta(seconds=61),
        )
        assert second == 2
        await authority.release_submission_fence(scope, child_id, 1)  # stale: no effect
        await authority.release_submission_fence(scope, child_id, 2)
        third = await authority.acquire_submission_fence(
            scope,
            child_id,
            holder="worker-3",
            lease_expires_at=now + timedelta(seconds=240),
            now=now + timedelta(seconds=62),
        )
        assert third == 3

        execution = AsyncSubagentExecution(
            child_execution_id=child_id,
            contract_id=base.contract.contract_id,
            contract_digest=base.contract.contract_digest,
            parent_run_id=run_id,
            parent_operation_id=base.parent_operation_id,
            parent_binding_id=base.parent_binding_id,
            execution_generation=1,
            objective_ref=base.objective_ref,
            context_slice_ref=base.context_slice_ref,
            reservation_id=base.reservation_id,
            lifecycle=AsyncSubagentLifecycle.IN_DOUBT,
            in_doubt_reason="multiple_provider_runs",
            incident_id=async_subagent_incident_id(scope, child_id, 1),
            created_at=now,
            updated_at=now,
        )
        incident = AsyncSubagentIncident(
            incident_id=execution.incident_id or "",
            request_scope=scope,
            parent_run_id=run_id,
            parent_binding_id=base.parent_binding_id,
            child_execution_id=child_id,
            reason="multiple_provider_runs",
            provider_thread_id=child_id,
            candidate_run_ids=("run-a", "run-b"),
            recorded_at=now,
        )
        stored = await authority.open_incident(incident)
        assert stored == await authority.open_incident(incident)  # idempotent open
        assert stored.status == "operator_required"
        await authority.record_execution_state(scope, execution)
        for run_id_, disposition in (("run-a", "bound"), ("run-b", "duplicate_cancelled")):
            await authority.record_provider_run(
                scope,
                AsyncProviderRunRecord(
                    child_execution_id=child_id,
                    provider_thread_id=child_id,
                    provider_run_id=run_id_,
                    disposition=disposition,  # type: ignore[arg-type]
                    provider_status="running" if disposition == "bound" else "interrupted",
                    usage=AsyncSubagentUsage(
                        provider_run_id=run_id_,
                        attribution="pending",
                        pending_amounts={"tokens.total": 3},
                    ),
                    observed_at=now,
                ),
            )
        assert await authority.claim_reconciliation_decision(
            scope,
            child_id,
            "adopt_provider_run",
            decision_id="decision-1",
            adopted_run_id="run-a",
            reason="operator",
        )
        # The child holds exactly one decision command: a replay is accepted, another decision
        # is refused (unique partial index of migration 0021).
        assert await authority.claim_reconciliation_decision(
            scope,
            child_id,
            "adopt_provider_run",
            decision_id="decision-1",
            adopted_run_id="run-a",
            reason="replay",
        )
        assert not await authority.claim_reconciliation_decision(
            scope,
            child_id,
            "orphan_child",
            decision_id="decision-2",
            adopted_run_id=None,
            reason="too late",
        )
        listed = await authority.list_provider_runs(scope, child_id)
        assert [record.provider_run_id for record in listed] == ["run-a", "run-b"]
        await authority.resolve_incident(
            stored.model_copy(
                update={
                    "status": "resolved",
                    "resolution": "adopt_provider_run",
                    "decision_id": "decision-1",
                    "adopted_run_id": "run-a",
                }
            )
        )
        resolved = await authority.get_incident(scope, child_id)
        assert resolved is not None and resolved.status == "resolved"
        assert resolved.adopted_run_id == "run-a"
        with pytest.raises(Exception, match="already resolved"):
            await authority.resolve_incident(
                stored.model_copy(
                    update={
                        "status": "resolved",
                        "resolution": "orphan_child",
                        "decision_id": "decision-2",
                    }
                )
            )
        bound = execution.model_copy(
            update={
                "lifecycle": AsyncSubagentLifecycle.RUNNING,
                "provider_thread_id": child_id,
                "provider_run_id": "run-a",
                "in_doubt_reason": None,
                "incident_id": None,
                "updated_at": now + timedelta(seconds=5),
            }
        )
        await authority.record_execution_state(scope, bound)
        await authority.record_fact(scope, child_id, "lifecycle", "running")
        await authority.record_fact(scope, child_id, "lifecycle", "running")

        views = await authority.list_children(scope, run_id)
        assert [view.child_execution_id for view in views] == [child_id]
        view = views[0]
        assert view.lifecycle == AsyncSubagentLifecycle.RUNNING
        assert view.provider_run_id == "run-a"
        assert view.submission_fence == 3 and view.submission_holder == "worker-3"
        assert view.graph_id == base.contract.graph_id
        assert view.graph_revision == base.contract.graph_revision
        assert view.graph_binding_digest == base.contract.graph_binding_digest
        assert view.parent_binding_id == base.parent_binding_id
        assert view.reconciliation_decision == "adopt_provider_run"
        assert {record.provider_run_id: record.disposition for record in view.provider_runs} == {
            "run-a": "bound",
            "run-b": "duplicate_cancelled",
        }
        assert all(record.usage.attribution == "pending" for record in view.provider_runs)

        # Forced RLS hides other scopes; the runtime role cannot delete ledgers.
        async with runtime.acquire() as connection:
            async with connection.transaction():
                await apply_scope(connection, common_db.scope("tenant-2"))
                assert (
                    await connection.fetchval(
                        "SELECT count(*) FROM mission_control.async_provider_run"
                    )
                    == 0
                )
            for table in ("async_provider_run", "native_observation", "subordinate_execution"):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await apply_scope(connection, scope)
                        await connection.execute(f"DELETE FROM mission_control.{table}")
        kinds = await owner_rows(
            common_db,
            """SELECT c.command_kind FROM mission_control.command c
               JOIN mission_control.subordinate_execution s
                 USING (installation_id, application_id, tenant_id, subordinate_id)
               WHERE s.subordinate_key = $1 ORDER BY c.created_at, c.command_kind""",
            child_id,
        )
        assert [row["command_kind"] for row in kinds] == ["admit", "adopt_provider_run"]
        lineage = await owner_rows(
            common_db,
            """SELECT s.observed_lifecycle, s.native_task_ref, r.run_key,
                      (SELECT count(*) FROM mission_control.native_observation o
                       WHERE o.subordinate_id = s.subordinate_id) AS facts
               FROM mission_control.subordinate_execution s
               JOIN mission_control.mission_run r USING (installation_id, application_id,
                                                         tenant_id, run_id)
               WHERE s.subordinate_key = $1""",
            child_id,
        )
        assert [tuple(row.values()) for row in lineage] == [("running", "run-a", run_id, 1)]
    finally:
        await runtime.close()
