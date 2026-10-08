"""FT-G5: the recorded-shape cloud fixture through the real adapter, reducer and Temporal.

The synthetic SSE fixture (`fixtures/cloud/run_stream.sse`, hand-authored from the documented
Cloud Agents API v1 shapes) is replayed by an `httpx.MockTransport` fake through
`CursorCloudHarness`, `lane.turn` and the C2 reducer, and once end to end through the real
`OperationWorkflow` segment loop on the time-skipping Temporal server with a dropped stream
that resumes with `Last-Event-ID`. No Cursor API is called.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from mission_control.adapters.temporal.operation_activities import (
    OperationExecutionActivities,
    parse_operation_result,
)
from mission_control.adapters.temporal.registration.activities import agent_cognitive_activities
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.frames.reducer import DeriveContext, derive
from mission_control.domain.execution.contracts import OperationWorkflowRequest
from mission_control.domain.execution.lane_turns import LaneTurnRequest
from mission_control.domain.execution.lanes import LaneSegmentBounds
from mission_control.domain.frames.contracts import LaneProfile
from mission_control.domain.frames.facts import ToolEffectFact, TurnCompletedFact
from tests.fixtures.cursor_cloud import cloud_stack
from tests.fixtures.lane_turns import LANE_QUEUE, RecordingSignals, lane_stack

WORKFLOW_QUEUE = "ft-g5-operation-workflows"


async def test_the_cloud_fixture_replays_through_the_adapter_and_reducer(tmp_path: Path) -> None:
    stack = cloud_stack(tmp_path)
    lanes = lane_stack(stack.harness, operation=stack.operation)
    identity = LaneExecutionIdentity.of(stack.operation, "cursor_cloud", 1)
    request = LaneTurnRequest(operation=stack.operation, lane_profile="cursor_cloud", generation=1)
    result = await lanes.service.turn(request, RecordingSignals(lanes.frames))
    assert result.done
    frames = list(lanes.frames._executions[identity.harness_execution_id].frames.values())
    facts = derive(
        [frame for frame in frames if frame.closing],
        DeriveContext(lane=LaneProfile.CURSOR_CLOUD, current_generation=1),
    )
    tools = {fact.tool_call_ref for fact in facts if isinstance(fact, ToolEffectFact)}
    assert tools == {"call-c1", "call-c2"}
    assert any(isinstance(fact, TurnCompletedFact) for fact in facts)
    assert all("cursor-test-key" not in frame.body_excerpt for frame in frames)


async def test_the_cloud_lane_runs_through_the_operation_workflow_segment_loop(
    tmp_path: Path,
) -> None:
    try:
        env = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with env:
        stack = cloud_stack(tmp_path, api_changes={"cut_after": "4"})
        lanes = lane_stack(stack.harness, operation=stack.operation)
        activities = OperationExecutionActivities(
            lanes.boundary, worker_identity="ft-g5-worker", lane_turns=lanes.service
        )
        request = OperationWorkflowRequest(
            semantic_attempt_id=stack.operation.identity.semantic_key,
            operation_kind="bound_operation",
            operation=stack.operation,
            segments=LaneSegmentBounds(
                max_frames=100, max_duration_s=30, start_to_close_s=60, heartbeat_timeout_s=10
            ),
        )
        async with (
            Worker(
                env.client,
                task_queue=WORKFLOW_QUEUE,
                workflows=[OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
            ),
            Worker(
                env.client, task_queue=LANE_QUEUE, activities=agent_cognitive_activities(activities)
            ),
        ):
            handle = await env.client.start_workflow(
                OperationWorkflow.run, request, id="ft-g5-cloud", task_queue=WORKFLOW_QUEUE
            )
            result = await asyncio.wait_for(handle.result(), timeout=120)
            history = await handle.fetch_history()
            await Replayer(
                workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
            ).replay_workflow(history)

    assert result.disposition == "completed"
    assert parse_operation_result(result.result).status == "completed"
    scheduled = [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]
    # The dropped stream ended segment 1 at id 4; segment 2 resumed with Last-Event-ID.
    assert scheduled == ["lane.turn", "lane.turn"]
    assert stack.api.stream_requests == [None, "4"]
    assert len(stack.api.creates) == 1 and len(stack.api.archived) == 1
