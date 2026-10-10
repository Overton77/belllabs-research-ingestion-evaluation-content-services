"""MP-08 on the real local Temporal server (127.0.0.1:7233) and PostgreSQL 17 (55433).

The real `OperationWorkflow` schedules the real `lane.turn` / `lane.status` / `lane.cancel`
activities over `LaneTurnService`, whose frames and lane state live in a disposable
PostgreSQL 17 database (runtime role, forced RLS) and whose lane is the `codex` harness over
the FIXTURE app-server (`tests/unit/codex/fixture_app_server.py`: in-process, no CLI, no
login, no model turn). Every history is replayed with `Replayer`. Nothing here proves live
Codex behaviour; it proves the lane drives the shared lifecycle on real services.

- a full codex turn settles `completed` through `lane.turn` segments, its frames in the
  Native Event Store (PostgreSQL) with one `run_result`, and its history replays;
- an app-server that dies mid-turn ends the segment; the next `lane.turn` segment relaunches,
  `thread/resume`s and completes from thread history without a second `turn/start`;
- a lane with a bounded segment crosses segments (`done=False`, cursor) and completes.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Replayer, Worker

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.execution_state import PostgresLaneExecutionStateStore
from mission_control.adapters.temporal.operation_activities import (
    OperationExecutionActivities,
    parse_operation_result,
)
from mission_control.adapters.temporal.registration.activities import agent_cognitive_activities
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
)
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
)
from mission_control.domain.execution.lanes import LaneSegmentBounds
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.lane_turns import lane_stack
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.unit.codex.fixture_app_server import FixtureLauncher, load_script
from tests.unit.codex.support import CodexStack, build_harness, codex_binding, codex_operation

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")
BOUNDS = LaneSegmentBounds(
    max_frames=50,
    max_duration_s=30,
    start_to_close_s=60,
    heartbeat_timeout_s=3,
    status_poll_limit=3,
    status_poll_interval_s=1,
    busy_wait_s=30,
)


async def _client() -> Client:
    try:
        return await Client.connect(ADDRESS, namespace=NAMESPACE)
    except RuntimeError as error:
        pytest.fail(f"the real local Temporal server at {ADDRESS} is required: {error}")


def _request(
    operation: OperationExecutionRequest, bounds: LaneSegmentBounds = BOUNDS
) -> OperationWorkflowRequest:
    return OperationWorkflowRequest.model_validate(
        {
            "semantic_attempt_id": operation.identity.semantic_key,
            "operation_kind": "bound_operation",
            "operation": operation,
            "segments": bounds,
        }
    )


def _workflow_worker(client: Client, queue: str) -> Worker:
    return Worker(
        client,
        task_queue=queue,
        workflows=[OperationWorkflow],
        workflow_runner=coordinator_workflow_runner(),
    )


def _activity_worker(client: Client, queue: str, boundary: Any, service: LaneTurnService) -> Worker:
    activities = OperationExecutionActivities(
        boundary, worker_identity=service.sessions.owner_ref, lane_turns=service
    )
    return Worker(
        client,
        task_queue=queue,
        activities=agent_cognitive_activities(activities),
        graceful_shutdown_timeout=timedelta(0),
    )


async def _start(
    client: Client, request: OperationWorkflowRequest, queue: str, name: str
) -> WorkflowHandle[Any, Any]:
    return await client.start_workflow(
        OperationWorkflow.run,
        request,
        id=f"{name}-{uuid4().hex[:10]}",
        task_queue=queue,
        execution_timeout=timedelta(minutes=5),
    )


async def _replayed(handle: WorkflowHandle[Any, Any]) -> Any:
    history = await handle.fetch_history()
    await Replayer(
        workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
    ).replay_workflow(history)
    return history


def _scheduled(history: Any) -> list[str]:
    return [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


async def _pg_codex_unit(
    pool: Any, db: CommonDatabase, tmp_path: Path, *scripts: str, lane_queue: str
) -> tuple[CodexStack, PostgresFrameRepository, PostgresLaneExecutionStateStore]:
    """A codex-bound operation of a real admitted unit, its frames and lane state in PG."""

    admitted = await admit_unit_attempt(pool, db)
    unit = admitted.unit
    payload = codex_operation(codex_binding(task_queue=lane_queue)).model_dump(mode="python")
    payload.update(
        request_scope=unit.request_scope,
        identity=OperationAttemptIdentity(
            run_id=admitted.run_key,
            operation_id=unit.semantic_operation_id,
            operation_attempt=unit.semantic_attempt,
        ),
        runtime_unit=unit,
        idempotency_key=f"mp08:{unit.unit_key}",
    )
    operation = OperationExecutionRequest.model_validate(payload)
    launcher = FixtureLauncher(scripts=[load_script(name) for name in scripts])
    harness, leaser, artifacts, auth = build_harness(tmp_path, launcher)
    lanes = lane_stack(harness, operation=operation)
    stack = CodexStack(
        harness=harness,
        launcher=launcher,
        leaser=leaser,
        artifacts=artifacts,
        auth=auth,
        rig=None,
        operation=operation,
        lanes=lanes,
    )
    return stack, PostgresFrameRepository(pool), PostgresLaneExecutionStateStore(pool)


def _service(
    stack: CodexStack,
    frames: PostgresFrameRepository,
    states: PostgresLaneExecutionStateStore,
    owner: str,
) -> LaneTurnService:
    return LaneTurnService(
        lanes=stack.lanes.service._lanes,
        boundary=stack.lanes.boundary,
        frames=frames,
        states=states,
        frame_reader=frames,
        sessions=WorkerSessionManager(owner_ref=owner, min_lease=timedelta(seconds=1)),
    )


@pytest.mark.common_db
async def test_a_codex_turn_settles_through_lane_turn_segments_on_real_services(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    client = await _client()
    lane_queue, workflow_queue = f"mp08-lane-{uuid4().hex[:8]}", f"mp08-wf-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        stack, frames, states = await _pg_codex_unit(
            pool, common_db, tmp_path, "turn_full", lane_queue=lane_queue
        )
        service = _service(stack, frames, states, "worker-a")
        async with (
            _workflow_worker(client, workflow_queue),
            _activity_worker(client, lane_queue, stack.lanes.boundary, service),
        ):
            handle = await _start(client, _request(stack.operation), workflow_queue, "mp08-full")
            result = await asyncio.wait_for(handle.result(), timeout=180)
            history = await _replayed(handle)

        assert result.disposition == "completed"
        settled = parse_operation_result(result.result)
        assert settled.status == "completed"
        assert _scheduled(history) == ["lane.turn"]
        server = stack.launcher.server
        assert [m for m, _p in server.records].count("turn/start") == 1
        assert server.steer_by_turn_start == 0
        identity = LaneExecutionIdentity.of(stack.operation, "codex", 1)
        state = await states.load(identity.request_scope, identity.harness_execution_id)
        assert state is not None and state.native_session_ref == "thr-0001"
        assert state.native_turn_ref == "turn-0002"
        stored = await frames.frames_for_execution(
            identity.request_scope, identity.harness_execution_id, 1, limit=1_000
        )
        kinds = [frame.kind for frame in stored]
        assert kinds.count(FrameKind.RUN_RESULT) == 1 and FrameKind.TURN_ENDED in kinds
        assert FrameKind.UNKNOWN in kinds, "unmapped app-server methods are stored as unknown"
        assert kinds.count(FrameKind.MESSAGE_DELTA) == 2
        assert all(frame.lane_profile.value == "codex" for frame in stored)
        env = dict(stack.launcher.launches[0].spec.env)
        assert set(env) == {"PATH", "HOME", "CODEX_HOME"}, "no credential reached the child"
    finally:
        await pool.close()


@pytest.mark.common_db
async def test_an_app_server_that_dies_mid_turn_is_reattached_by_the_next_segment(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    client = await _client()
    lane_queue, workflow_queue = f"mp08-lane-{uuid4().hex[:8]}", f"mp08-wf-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        stack, frames, states = await _pg_codex_unit(
            pool, common_db, tmp_path, "turn_disconnect", lane_queue=lane_queue
        )
        service = _service(stack, frames, states, "worker-a")
        identity = LaneExecutionIdentity.of(stack.operation, "codex", 1)
        async with (
            _workflow_worker(client, workflow_queue),
            _activity_worker(client, lane_queue, stack.lanes.boundary, service),
        ):
            handle = await _start(
                client, _request(stack.operation), workflow_queue, "mp08-disconnect"
            )
            await asyncio.wait_for(stack.launcher.launched.wait(), 60)
            await asyncio.wait_for(stack.launcher.server.held.wait(), 60)
            # The segment is observing live (its first tool frame reached PostgreSQL)...
            for _ in range(600):
                stored = await frames.frames_for_execution(
                    identity.request_scope, identity.harness_execution_id, 1, limit=1_000
                )
                if any(frame.tool_call_ref == "item-cmd-5" for frame in stored):
                    break
                await asyncio.sleep(0.1)
            else:
                pytest.fail("the first tool frame never reached the Native Event Store")
            # ... and now the app-server dies; the turn finishes "offline" on the fixture disk.
            stack.launcher.server.release.set()
            result = await asyncio.wait_for(handle.result(), timeout=180)
            history = await _replayed(handle)

        assert result.disposition == "completed"
        assert parse_operation_result(result.result).status == "completed"
        scheduled = _scheduled(history)
        assert scheduled.count("lane.turn") >= 2, "the next segment reattached"
        assert len(stack.launcher.launches) == 2, "one relaunch over the same lease"
        first, second = stack.launcher.launches
        assert first.spec.cwd == second.spec.cwd
        assert "thread/resume" in [m for m, _p in second.server.records]
        assert "turn/start" not in [m for m, _p in second.server.records], "never re-sent"
        stored = await frames.frames_for_execution(
            identity.request_scope, identity.harness_execution_id, 1, limit=1_000
        )
        assert sum(frame.kind == FrameKind.RUN_RESULT for frame in stored) == 1
        assert len({frame.provider_key for frame in stored}) == len(stored)
        state = await states.load(identity.request_scope, identity.harness_execution_id)
        assert state is not None and state.native_turn_ref == "turn-0002"
    finally:
        await pool.close()


@pytest.mark.common_db
async def test_bounded_segments_cross_an_activity_boundary_and_complete(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    client = await _client()
    lane_queue, workflow_queue = f"mp08-lane-{uuid4().hex[:8]}", f"mp08-wf-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        stack, frames, states = await _pg_codex_unit(
            pool, common_db, tmp_path, "turn_full", lane_queue=lane_queue
        )
        service = _service(stack, frames, states, "worker-a")
        bounds = BOUNDS.model_copy(update={"max_frames": 3})
        async with (
            _workflow_worker(client, workflow_queue),
            _activity_worker(client, lane_queue, stack.lanes.boundary, service),
        ):
            handle = await _start(
                client, _request(stack.operation, bounds), workflow_queue, "mp08-segments"
            )
            result = await asyncio.wait_for(handle.result(), timeout=180)
            history = await _replayed(handle)

        assert result.disposition == "completed"
        assert _scheduled(history).count("lane.turn") >= 2
        assert [m for m, _p in stack.launcher.server.records].count("turn/start") == 1
        assert len(stack.launcher.launches) == 1, "segments reuse the retained session"
        identity = LaneExecutionIdentity.of(stack.operation, "codex", 1)
        stored = await frames.frames_for_execution(
            identity.request_scope, identity.harness_execution_id, 1, limit=1_000
        )
        assert len({frame.provider_key for frame in stored}) == len(stored), "no duplicate"
        assert sum(frame.kind == FrameKind.RUN_RESULT for frame in stored) == 1
    finally:
        await pool.close()
