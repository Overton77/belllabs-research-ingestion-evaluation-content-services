"""MP-07 x MP-11 on real services: a claude unit's native permission request bound to a durable
approval Human Task in PostgreSQL, resolved by a reviewer while the production
`OperationWorkflow` runs on the real local Temporal server.

SCHEMA: the scratch database is the released common migration chain (0001..0033, component
1.2.0); `approval_correlation` comes from migration 0033 section 1, exactly as an installation
receives it (the same database fixture as `tests/integration/postgres/test_mp11_*`).

Everything on the Mission Control side is production code: `PostgresApprovalTaskRepository`,
`PostgresApprovalCorrelationRepository`, `PostgresStopFenceRepository`,
`PostgresApprovalContextProbe` (generation from the MP-06 session owner, policy digest from
`harness_execution`), `ApprovalBroker`, `HumanTaskService`, `LaneTurnService`,
`OperationWorkflow`. The SDK client is the MP-07 FIXTURE: no Claude Code runs, nothing is paid.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import asyncpg
import pytest
from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

from mission_control.adapters.claude.approvals import BrokerPermissionBinding
from mission_control.adapters.claude.compose import broker_permissions
from mission_control.adapters.claude.harness import approval_policy_digest
from mission_control.adapters.postgres.approvals.context import PostgresApprovalContextProbe
from mission_control.adapters.postgres.approvals.correlations import (
    PostgresApprovalCorrelationRepository,
)
from mission_control.adapters.postgres.approvals.tasks import PostgresApprovalTaskRepository
from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.application.execution.approvals import ApprovalTaskView
from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.domain.execution.contracts import OperationWorkflowResult
from mission_control.domain.policies.contracts import ActorContext
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.integration.temporal.test_mp07_claude_lane_turns import (
    WORKER,
    claude_unit,
    operation_workers,
    queues,
    replayed,
    start_operation,
    temporal_client,
    workflow_request,
)
from tests.unit.approvals.fixtures import answer
from tests.unit.claude.fixtures import PROFILE

pytestmark = pytest.mark.common_db

REVIEWER = "owner"
TOOL_USE = "toolu_sub_bash_31"


async def _released(database: CommonDatabase) -> None:
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        table = await connection.fetchval(
            "SELECT to_regclass('mission_control.approval_correlation') IS NOT NULL"
        )
    finally:
        await connection.close()
    assert table, "migration 0033 (approval_correlation) is part of the installed chain"


def _broker(pool: asyncpg.Pool) -> tuple[ApprovalBroker, PostgresApprovalTaskRepository]:
    tasks = PostgresApprovalTaskRepository(pool)
    broker = ApprovalBroker(
        tasks,
        PostgresApprovalCorrelationRepository(pool),
        probe=PostgresApprovalContextProbe(pool),
        connection_ref=f"{WORKER}#1",
        fences=PostgresStopFenceRepository(pool),
        poll_seconds=0.05,
    )
    return broker, tasks


async def _open_task(
    tasks: PostgresApprovalTaskRepository, scope: str, run_key: str
) -> ApprovalTaskView:
    async with asyncio.timeout(60):
        while True:
            found = await tasks.list_tasks(scope, run_id=run_key, lifecycle="open")
            if found:
                return found[0]
            await asyncio.sleep(0.05)


async def test_a_permission_request_waits_on_a_postgres_approval_task_and_the_reviewer_allows(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    await _released(common_db)
    client = await temporal_client()
    lane_queue, workflow_queue = queues()
    pool = await common_db.pool("mission_control_runtime", max_size=8)
    try:
        broker, tasks = _broker(pool)
        port: BrokerPermissionBinding = broker_permissions(
            broker, wait_seconds=30, reviewers=(REVIEWER,), segment_budget_s=60
        )
        stack, service, _frames, _states = await claude_unit(
            pool,
            common_db,
            tmp_path,
            script="subagent_task",
            lane_queue=lane_queue,
            permissions=port,
        )
        identity = LaneExecutionIdentity.of(stack.operation, PROFILE, 1)
        scope, run_key = identity.request_scope, identity.run_key
        human_tasks = HumanTaskService(
            PostgresHumanTaskRepository(pool),
            request_scope=scope,
            approvals=tasks,
            approval_wake=broker.hub,
        )
        request = workflow_request(stack.operation)
        async with operation_workers(client, stack, service, workflow_queue):
            handle = await start_operation(client, request, workflow_queue)
            # A reviewer (the API process in production) resolves the persisted task while
            # the SDK callback waits inside the `lane.turn` activity.
            task = await _open_task(tasks, scope, run_key)
            await human_tasks.resolve(
                task.human_task_id,
                answer(task, request_id="review-1"),
                ActorContext(actor_id=REVIEWER, permissions=frozenset({f"reviewer:{REVIEWER}"})),
            )
            result: OperationWorkflowResult = await asyncio.wait_for(handle.result(), timeout=180)
            await replayed(handle)

        assert result.disposition == "completed"
        fixture_client = stack.factory.last
        assert fixture_client.permission_calls == [("Bash", True)]
        (allowed,) = fixture_client.permission_results
        assert isinstance(allowed, PermissionResultAllow)

        # The task in PostgreSQL carries the MP-11 Claude mapping.
        stored = await tasks.get_task(scope, task.human_task_id)
        assert stored is not None and stored.lifecycle == "resolved"
        binding = stored.packet.binding
        assert stored.packet.origin == "provider_permission"
        assert binding.native.tool_call_ref == TOOL_USE
        assert binding.native.connection_scoped is False
        assert binding.replay_strategy == "reissue_native_request"
        assert binding.harness_execution_id == str(identity.harness_execution_id)

        # Equality proof: what the lane bound is exactly what the production probe reads
        # back from `harness_execution` (else the broker would have replied
        # `policy_changed` / `stale_generation` and the tool would have been denied).
        probe = await PostgresApprovalContextProbe(pool).current(
            scope, run_id=run_key, harness_execution_id=str(identity.harness_execution_id)
        )
        assert probe.granted
        assert probe.policy_digest == binding.policy_digest
        assert probe.policy_digest == approval_policy_digest(stack.operation)
        assert probe.generation == binding.generation == 1

        (outcome,) = port.outcomes
        assert outcome.status == "approved" and outcome.reply.reason == "human_decision"
        correlation = await PostgresApprovalCorrelationRepository(pool).get_correlation(
            scope, outcome.correlation_id
        )
        assert correlation is not None and correlation.state == "answered"
        assert correlation.connection_ref == f"{WORKER}#1"
    finally:
        await pool.close()


async def test_an_unanswered_request_expires_its_correlation_and_the_task_stays_pending(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    await _released(common_db)
    client = await temporal_client()
    lane_queue, workflow_queue = queues()
    pool = await common_db.pool("mission_control_runtime", max_size=8)
    try:
        broker, tasks = _broker(pool)
        port = broker_permissions(
            broker, wait_seconds=1, reviewers=(REVIEWER,), segment_budget_s=60
        )
        stack, service, _frames, _states = await claude_unit(
            pool,
            common_db,
            tmp_path,
            script="subagent_task",
            lane_queue=lane_queue,
            permissions=port,
        )
        identity = LaneExecutionIdentity.of(stack.operation, PROFILE, 1)
        request = workflow_request(stack.operation)
        async with operation_workers(client, stack, service, workflow_queue):
            handle = await start_operation(client, request, workflow_queue)
            await asyncio.wait_for(handle.result(), timeout=180)
            await replayed(handle)
        (denied,) = stack.factory.last.permission_results
        assert isinstance(denied, PermissionResultDeny) and denied.interrupt
        (outcome,) = port.outcomes
        assert outcome.status == "expired" and outcome.reply.reason == "wait_expired"
        (task,) = await tasks.list_tasks(identity.request_scope, run_id=identity.run_key)
        assert task.lifecycle == "open", "a callback timeout never resolves the durable task"
        correlation = await PostgresApprovalCorrelationRepository(pool).get_correlation(
            identity.request_scope, outcome.correlation_id
        )
        assert correlation is not None and correlation.state == "expired"
    finally:
        await pool.close()
