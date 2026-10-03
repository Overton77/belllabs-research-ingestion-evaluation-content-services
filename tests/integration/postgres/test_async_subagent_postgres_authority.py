"""Migration 0021 and `PostgresAsyncSubagentAuthority` under the production runtime role.

Opt-in through `TEST_APPLICATION_POSTGRES_DSN` (disposable stack only). Every repository call
runs as `belllabs_control_runtime`, so forced RLS and the least-privilege grants are what is
exercised: the submission fence with lease takeover, the lifecycle mirror, provider-run
records with pending usage, typed in_doubt incidents, decisions and the inspection read.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.async_subagents.async_subagents import (
    PostgresAsyncSubagentAuthority,
)
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
from tests.integration.postgres.test_checkpoint_lineage_postgres import (
    RUNTIME_ROLE,
    _assume_runtime_role,
    admit_run,
    require_disposable_postgres,
    reset_application_schema,
)


@pytest.mark.asyncio
async def test_runtime_role_fences_submission_and_records_in_doubt_lineage(
    test_application_postgres_dsn: str,
) -> None:
    require_disposable_postgres(test_application_postgres_dsn)
    owner = await asyncpg.create_pool(dsn=test_application_postgres_dsn, min_size=1, max_size=2)
    runtime = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn, min_size=1, max_size=4, setup=_assume_runtime_role
    )
    try:
        await reset_application_schema(owner)
        async with owner.acquire() as connection:
            versions = {
                row["version"]
                for row in await connection.fetch(
                    "SELECT version FROM belllabs_control.schema_migrations"
                )
            }
        assert "0021_async_subagent_submission_fence_v1.sql" in versions
        run_id = await admit_run(owner)
        authority = PostgresAsyncSubagentAuthority(runtime)
        async with runtime.acquire() as connection:
            assert await connection.fetchval("SELECT current_user") == RUNTIME_ROLE

        base = spawn_request().model_copy(
            update={"request_scope": "tenant-1", "parent_run_id": run_id}
        )
        child_id = f"child-{uuid4().hex[:12]}"
        await authority.reserve_and_admit(base, child_id, f"link-{child_id}")
        await authority.reserve_and_admit(base, child_id, f"link-{child_id}")  # idempotent

        now = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)
        first = await authority.acquire_submission_fence(
            "tenant-1",
            child_id,
            holder="worker-1",
            lease_expires_at=now + timedelta(seconds=60),
            now=now,
        )
        assert first == 1
        # A live lease blocks a second submitter; an expired lease is taken over by fence 2.
        assert (
            await authority.acquire_submission_fence(
                "tenant-1",
                child_id,
                holder="worker-2",
                lease_expires_at=now + timedelta(seconds=120),
                now=now + timedelta(seconds=10),
            )
            is None
        )
        second = await authority.acquire_submission_fence(
            "tenant-1",
            child_id,
            holder="worker-2",
            lease_expires_at=now + timedelta(seconds=180),
            now=now + timedelta(seconds=61),
        )
        assert second == 2
        await authority.release_submission_fence("tenant-1", child_id, 1)  # stale: no effect
        await authority.release_submission_fence("tenant-1", child_id, 2)
        third = await authority.acquire_submission_fence(
            "tenant-1",
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
            incident_id=async_subagent_incident_id("tenant-1", child_id, 1),
            created_at=now,
            updated_at=now,
        )
        incident = AsyncSubagentIncident(
            incident_id=execution.incident_id or "",
            request_scope="tenant-1",
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
        await authority.record_execution_state("tenant-1", execution)
        for run_id_, disposition in (("run-a", "bound"), ("run-b", "duplicate_cancelled")):
            await authority.record_provider_run(
                "tenant-1",
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
            "tenant-1",
            child_id,
            "adopt_provider_run",
            decision_id="decision-1",
            adopted_run_id="run-a",
            reason="operator",
        )
        # The child holds exactly one decision command: a replay is accepted, another decision
        # is refused (unique partial index of migration 0021).
        assert await authority.claim_reconciliation_decision(
            "tenant-1",
            child_id,
            "adopt_provider_run",
            decision_id="decision-1",
            adopted_run_id="run-a",
            reason="replay",
        )
        assert not await authority.claim_reconciliation_decision(
            "tenant-1",
            child_id,
            "orphan_child",
            decision_id="decision-2",
            adopted_run_id=None,
            reason="too late",
        )
        listed = await authority.list_provider_runs("tenant-1", child_id)
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
        resolved = await authority.get_incident("tenant-1", child_id)
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
        await authority.record_execution_state("tenant-1", bound)
        await authority.record_fact("tenant-1", child_id, "lifecycle", "running")
        await authority.record_fact("tenant-1", child_id, "lifecycle", "running")

        views = await authority.list_children("tenant-1", run_id)
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
                await connection.execute(
                    "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
                )
                assert (
                    await connection.fetchval(
                        "SELECT count(*) FROM belllabs_control.async_subagent_provider_runs"
                    )
                    == 0
                )
            for table in ("async_subagent_provider_runs", "async_subagent_facts"):
                with pytest.raises(asyncpg.InsufficientPrivilegeError):
                    async with connection.transaction():
                        await connection.execute(
                            "SELECT set_config('belllabs.request_scope', 'tenant-1', true)"
                        )
                        await connection.execute(f"DELETE FROM belllabs_control.{table}")
        async with owner.acquire() as connection:
            kinds = await connection.fetch(
                """SELECT command_kind FROM belllabs_control.async_subagent_commands
                   WHERE child_execution_id = $1 ORDER BY recorded_at""",
                child_id,
            )
        assert [row["command_kind"] for row in kinds] == ["admit", "adopt_provider_run"]
    finally:
        await runtime.close()
        await owner.close()
