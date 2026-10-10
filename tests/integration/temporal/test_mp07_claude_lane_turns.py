"""MP-07 production routing on the real local Temporal server (127.0.0.1:7233) and the
disposable PostgreSQL 17 (`MISSION_CONTROL_TEST_ADMIN_DSN`, scratch `mct_*` databases).

A claude unit runs through the production `OperationWorkflow` (`mc.operation.v1` segment
loop, `SEGMENT_LOOP_PATCH`): `OperationWorkflowRequest.activity_task_queue` routes its
`lane.turn` segments to `provider_binding.task_queue`, where the production
`OperationExecutionActivities` run `LaneTurnService` over `PostgresFrameRepository` and
`PostgresLaneExecutionStateStore` (runtime role) with the FIXTURE SDK client
(`tests/unit/claude/fixtures.py`: no Claude Code runs, nothing is paid for). Every history is
replayed with `Replayer` against `OperationWorkflow`.

The MP-07 test-local segment loop (`mp07_segment_loop.py`) only stood in for this workflow
while the queue delta was missing; it proved nothing the production workflow does not, and
was removed.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Replayer, Worker

from mission_control.adapters.claude.permissions import PermissionBindingPort
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.execution_state import PostgresLaneExecutionStateStore
from mission_control.adapters.temporal.operation_activities import (
    OperationExecutionActivities,
    parse_operation_result,
)
from mission_control.adapters.temporal.registration.activities import agent_cognitive_activities
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import (
    SEGMENT_LOOP_PATCH,
    OperationWorkflow,
)
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
)
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
    OperationWorkflowResult,
)
from mission_control.domain.execution.lane_turns import LaneSegmentBounds
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.temporal_history import patch_ids
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.unit.claude.fixtures import (
    PROFILE,
    SESSION_ID,
    ClaudeStack,
    claude_operation,
    claude_stack,
)

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")
WORKER = "mp07-worker-a"
# Three frames per segment: the full run spans several `lane.turn` activities, each one
# reattaching the live session this process holds (the SDK subprocess outlives a segment).
BOUNDS = LaneSegmentBounds(
    max_frames=3,
    max_duration_s=30,
    start_to_close_s=60,
    heartbeat_timeout_s=5,
    status_poll_limit=3,
    status_poll_interval_s=1,
    busy_wait_s=30,
)


async def temporal_client() -> Client:
    try:
        return await Client.connect(ADDRESS, namespace=NAMESPACE)
    except RuntimeError as error:
        pytest.fail(f"the real local Temporal server at {ADDRESS} is required: {error}")


async def claude_unit(
    pool: asyncpg.Pool,
    db: CommonDatabase,
    tmp_path: Path,
    *,
    script: str,
    lane_queue: str,
    permissions: PermissionBindingPort | None = None,
) -> tuple[ClaudeStack, LaneTurnService, PostgresFrameRepository, PostgresLaneExecutionStateStore]:
    """An admitted unit attempt in PostgreSQL whose operation is a claude unit bound to
    `lane_queue` (its `provider_binding.task_queue`), and the production lane-turn service."""

    admitted = await admit_unit_attempt(pool, db)
    unit = admitted.unit
    payload = claude_operation(task_queue=lane_queue).model_dump(mode="python")
    payload.update(
        request_scope=unit.request_scope,
        identity=OperationAttemptIdentity(
            run_id=admitted.run_key,
            operation_id=unit.semantic_operation_id,
            operation_attempt=unit.semantic_attempt,
        ),
        runtime_unit=unit,
        idempotency_key=f"mp07:{unit.unit_key}",
    )
    operation = OperationExecutionRequest.model_validate(payload)
    stack = claude_stack(tmp_path, script=script, operation=operation, permissions=permissions)
    frames = PostgresFrameRepository(pool)
    states = PostgresLaneExecutionStateStore(pool)
    service = LaneTurnService(
        lanes=stack.lanes.service._lanes,
        boundary=stack.lanes.boundary,
        frames=frames,
        states=states,
        frame_reader=frames,
        sessions=WorkerSessionManager(owner_ref=WORKER, min_lease=timedelta(seconds=1)),
    )
    return stack, service, frames, states


def workflow_request(operation: OperationExecutionRequest) -> OperationWorkflowRequest:
    return OperationWorkflowRequest.model_validate(
        {
            "semantic_attempt_id": operation.identity.semantic_key,
            "operation_kind": "bound_operation",
            "operation": operation,
            "segments": BOUNDS,
        }
    )


@asynccontextmanager
async def operation_workers(
    client: Client, stack: ClaudeStack, service: LaneTurnService, workflow_queue: str
) -> AsyncIterator[None]:
    """The production workflow on its queue; the lane activities on the binding's queue."""

    assert stack.operation.provider_binding is not None
    lane_queue = stack.operation.provider_binding.task_queue
    assert lane_queue is not None
    activities = OperationExecutionActivities(
        stack.lanes.boundary, worker_identity=WORKER, lane_turns=service
    )
    async with (
        Worker(
            client,
            task_queue=workflow_queue,
            workflows=[OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ),
        Worker(client, task_queue=lane_queue, activities=agent_cognitive_activities(activities)),
    ):
        yield


async def start_operation(
    client: Client, request: OperationWorkflowRequest, workflow_queue: str
) -> WorkflowHandle[Any, Any]:
    return await client.start_workflow(
        OperationWorkflow.run,
        request,
        id=f"mp07-claude-{uuid4().hex[:10]}",
        task_queue=workflow_queue,
        execution_timeout=timedelta(minutes=5),
    )


async def replayed(handle: WorkflowHandle[Any, Any]) -> Any:
    history = await handle.fetch_history()
    await Replayer(
        workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
    ).replay_workflow(history)
    return history


def scheduled(history: Any) -> list[str]:
    return [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


def lane_queues(history: Any) -> set[str]:
    return {
        event.activity_task_scheduled_event_attributes.task_queue.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    }


def queues() -> tuple[str, str]:
    return f"mp07-lane-{uuid4().hex[:8]}", f"mp07-wf-{uuid4().hex[:8]}"


@pytest.mark.common_db
async def test_a_claude_unit_runs_through_the_production_operation_workflow(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    client = await temporal_client()
    lane_queue, workflow_queue = queues()
    pool = await common_db.pool("mission_control_runtime")
    try:
        stack, service, frames, states = await claude_unit(
            pool, common_db, tmp_path, script="full_run", lane_queue=lane_queue
        )
        request = workflow_request(stack.operation)
        # The production routing: the claude unit's segments go to its binding's queue.
        assert request.segment_driven and request.activity_task_queue == lane_queue
        async with operation_workers(client, stack, service, workflow_queue):
            handle = await start_operation(client, request, workflow_queue)
            result: OperationWorkflowResult = await asyncio.wait_for(handle.result(), timeout=180)
            history = await replayed(handle)

        assert result.disposition == "completed"
        settled = parse_operation_result(result.result)
        assert settled.status == "completed"
        names = scheduled(history)
        assert names and set(names) == {"lane.turn"} and len(names) >= 3, "segments of three"
        assert lane_queues(history) == {lane_queue}
        assert SEGMENT_LOOP_PATCH in patch_ids(history)
        client_fixture = stack.factory.last
        assert len(stack.factory.clients) == 1, "the live session outlived every segment"
        assert len(client_fixture.sent) == 1, "segments reattach; the turn is sent once"
        assert client_fixture.disconnected, "end_session closed the SDK client"
        identity = LaneExecutionIdentity.of(stack.operation, PROFILE, 1)
        stored = await frames.frames_for_execution(
            identity.request_scope, identity.harness_execution_id, 1, limit=10_000
        )
        ordinals = [frame.arrival_ordinal for frame in stored]
        assert ordinals == list(range(1, len(ordinals) + 1)), "no duplicate frame across segments"
        kinds = [frame.kind for frame in stored]
        assert kinds[0] is FrameKind.SESSION_INIT and kinds[-1] is FrameKind.RUN_RESULT
        assert FrameKind.HOOK_RESULT in kinds and FrameKind.TOOL_CALL_COMPLETED in kinds
        assert {frame.native_session_ref for frame in stored} == {SESSION_ID}
        state = await states.load(identity.request_scope, identity.harness_execution_id)
        assert state is not None and state.native_session_ref == SESSION_ID
        assert state.native_turn_ref == client_fixture.sent[0][0]
        assert state.owner is not None and state.owner.owner_ref == WORKER
        send = state.dispatch("send", f"{identity.harness_execution_id}:1:turn:1")
        assert send is not None and send.phase == "acknowledged"
        create = state.dispatch("create", f"{identity.harness_execution_id}:1:turn:1")
        assert create is not None and (create.phase, create.native_ref) == (
            "acknowledged",
            SESSION_ID,
        )
        (lease,) = stack.workspace.registry.values()
        assert lease.released and state.bridge_state_root is not None
    finally:
        await pool.close()


@pytest.mark.common_db
async def test_a_provider_error_result_settles_failed_capacity_through_the_operation_workflow(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    client = await temporal_client()
    lane_queue, workflow_queue = queues()
    pool = await common_db.pool("mission_control_runtime")
    try:
        stack, service, _frames, _states = await claude_unit(
            pool, common_db, tmp_path, script="error_result", lane_queue=lane_queue
        )
        request = workflow_request(stack.operation)
        async with operation_workers(client, stack, service, workflow_queue):
            handle = await start_operation(client, request, workflow_queue)
            result: OperationWorkflowResult = await asyncio.wait_for(handle.result(), timeout=180)
            history = await replayed(handle)
        assert result.disposition == "failed"
        settled = parse_operation_result(result.result)
        assert settled.status == "failed"
        assert set(scheduled(history)) == {"lane.turn"}
        assert lane_queues(history) == {lane_queue}
        assert "capacity" in (settled.failure_code or "") + (settled.failure_message or "")
    finally:
        await pool.close()


def test_the_operation_request_routes_a_claude_unit_to_its_binding_queue() -> None:
    """`activity_task_queue` reads `provider_binding.task_queue` (the integrated delta); a
    claude unit carries no Cursor binding and no native placement."""

    operation = claude_operation(task_queue="mp07-lane-queue")
    request = workflow_request(operation)
    assert operation.cursor_binding is None and operation.native_placement is None
    assert operation.provider_binding is not None
    assert request.activity_task_queue == "mp07-lane-queue"
