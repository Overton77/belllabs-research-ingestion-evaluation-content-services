"""MP-09 on the real local Temporal server (127.0.0.1:7233, `make temporal-up`) and a
disposable PostgreSQL 17 (127.0.0.1:55433, `MISSION_CONTROL_TEST_ADMIN_DSN`).

The real `OperationWorkflow` segment loop drives the real `cursor_cloud` harness over the
FIXTURE Cloud Agents API (`tests/fixtures/cursor_cloud.FakeCloudApi` behind
`httpx.MockTransport`, a bare git remote) with frames, lane state, the MP-06 dispatch journal
and session ownership in PostgreSQL (runtime role, forced RLS); every history is replayed with
`Replayer`. No Cursor API is called and nothing here proves live provider behaviour.

- V08/V21: segment 1 ends on a dropped stream; segment 2 resumes with `Last-Event-ID`, meets
  `410 stream_expired`, records the expiry as its own frame, reconciles a run that is still
  running from the run record and settles from its terminal state. One agent, one create,
  frames stored once, the journal acknowledged, the provider workspace lease released with
  its branch snapshot.
- MP-05/06: a `429` on the create is `ProviderCapacityLimited`: the workflow waits on a
  Temporal timer (`mp05-capacity-wait`) and the next segment creates exactly once.
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import asyncpg
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
from mission_control.adapters.temporal.workflows.operation import (
    CAPACITY_WAIT_PATCH,
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
)
from mission_control.domain.execution.lanes import CursorExecutionBinding, LaneSegmentBounds
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.cursor_cloud import CloudStack, cloud_stack
from tests.fixtures.lane_turns import LaneStack, lane_stack
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.temporal_history import patch_ids
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")
PROFILE = "cursor_cloud"
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


@dataclass
class PgCloudUnit:
    operation: OperationExecutionRequest
    stack: LaneStack
    cloud: CloudStack
    frames: PostgresFrameRepository
    states: PostgresLaneExecutionStateStore

    @property
    def identity(self) -> LaneExecutionIdentity:
        return LaneExecutionIdentity.of(self.operation, PROFILE, 1)

    def service(self, owner: str) -> LaneTurnService:
        return LaneTurnService(
            lanes=self.stack.service._lanes,
            boundary=self.stack.boundary,
            frames=self.frames,
            states=self.states,
            frame_reader=self.frames,
            sessions=WorkerSessionManager(owner_ref=owner, min_lease=timedelta(seconds=1)),
        )


async def pg_cloud_unit(
    pool: asyncpg.Pool,
    db: CommonDatabase,
    tmp_path: Path,
    *,
    task_queue: str,
    api_changes: dict[str, Any],
) -> PgCloudUnit:
    """A `cursor_cloud` operation bound to one admitted runtime unit of a real run in
    PostgreSQL, over the fixture Cloud API (its binding re-sealed with the test queue)."""

    admitted = await admit_unit_attempt(pool, db)
    unit = admitted.unit
    cloud = cloud_stack(tmp_path, api_changes=api_changes)
    base = cloud.operation.cursor_binding
    assert base is not None
    binding = CursorExecutionBinding.sealed(
        **{**base.model_dump(mode="python", exclude={"binding_digest"}), "task_queue": task_queue}
    )
    payload = cloud.operation.model_dump(mode="python")
    payload.update(
        request_scope=unit.request_scope,
        identity=OperationAttemptIdentity(
            run_id=admitted.run_key,
            operation_id=unit.semantic_operation_id,
            operation_attempt=unit.semantic_attempt,
        ),
        runtime_unit=unit,
        idempotency_key=f"mp09:{unit.unit_key}",
        cursor_binding=binding,
    )
    operation = OperationExecutionRequest.model_validate(payload)
    stack = lane_stack(cloud.harness, operation=operation)
    return PgCloudUnit(
        operation=operation,
        stack=stack,
        cloud=cloud,
        frames=PostgresFrameRepository(pool),
        states=PostgresLaneExecutionStateStore(pool),
    )


def _request(operation: OperationExecutionRequest) -> OperationWorkflowRequest:
    return OperationWorkflowRequest.model_validate(
        {
            "semantic_attempt_id": operation.identity.semantic_key,
            "operation_kind": "bound_operation",
            "operation": operation,
            "segments": BOUNDS,
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


async def _run(
    unit: PgCloudUnit, client: Client, lane_queue: str, workflow_queue: str, name: str
) -> tuple[Any, Any]:
    service = unit.service(f"mp09-worker-{uuid4().hex[:6]}")
    async with (
        _workflow_worker(client, workflow_queue),
        _activity_worker(client, lane_queue, unit.stack.boundary, service),
    ):
        handle = await _start(client, _request(unit.operation), workflow_queue, name)
        result = await asyncio.wait_for(handle.result(), timeout=180)
        history = await _replayed(handle)
    return result, history


@pytest.mark.common_db
async def test_the_cloud_segment_loop_resumes_after_a_dropped_stream_and_a_retention_expiry(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    client = await _client()
    lane_queue, workflow_queue = f"mp09-lane-{uuid4().hex[:8]}", f"mp09-wf-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        unit = await pg_cloud_unit(
            pool,
            common_db,
            tmp_path,
            task_queue=lane_queue,
            # Three RUNNING reads: segment 1 reconciles once after the cut, segment 2 reads once
            # on reattach, and the expiry poll sees the third before the record turns final.
            api_changes={"cut_after": "4", "expire_after": "4", "running_reads": 3},
        )
        result, history = await _run(unit, client, lane_queue, workflow_queue, "mp09-v21")
        api = unit.cloud.api
        scope, heid = unit.identity.request_scope, unit.identity.harness_execution_id

        assert result.disposition == "completed"
        assert parse_operation_result(result.result).status == "completed"
        assert _scheduled(history) == ["lane.turn", "lane.turn"], "cut, then one resume"
        assert api.stream_requests == [None, "4"], "resumed from the durable cursor; one 410"
        assert len(api.creates) == 1 and len(api.agents) == 1
        assert api.archived == list(api.agents)
        assert api.run_reads >= 4, "the running run was reconciled from the record"

        frames = await unit.frames.frames_for_execution(scope, heid, 1, limit=1_000)
        keys = [frame.provider_key for frame in frames]
        assert keys and len(keys) == len(set(keys)), "frames stored once in PostgreSQL"
        assert sum(key.startswith("sse-expired:") for key in keys) == 1
        assert sum(key.startswith("run-status:") for key in keys) == 1
        finals = [frame for frame in frames if frame.kind == FrameKind.RUN_RESULT]
        assert len(finals) == 1 and finals[0].raw_kind == "run.final"
        assert '"stream":"expired"' in finals[0].body_excerpt.replace(" ", "")

        state = await unit.states.load(scope, heid)
        assert state is not None and state.owner is not None
        assert state.native_session_ref == next(iter(api.agents))
        assert state.native_turn_ref == api.run_id
        assert state.provider_cursor is not None and state.usage_disposition is not None
        create = state.dispatch("create", f"{heid}:1:turn:1")
        send = state.dispatch("send", f"{heid}:1:turn:1")
        assert create is not None and create.phase == "acknowledged" and create.attempts == 1
        assert send is not None and send.phase == "acknowledged" and send.attempts == 1
        assert send.native_ref == api.run_id

        assert unit.cloud.leases is not None
        (lease,) = unit.cloud.leases._leases.values()
        assert lease.released and lease.snapshot_ref is not None
        assert lease.snapshot_ref.startswith("branch:") and lease.cleanup_status == "not_required"
    finally:
        await pool.close()


@pytest.mark.common_db
async def test_a_429_on_create_waits_on_a_temporal_timer_and_then_creates_once(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    client = await _client()
    lane_queue, workflow_queue = f"mp09-lane-{uuid4().hex[:8]}", f"mp09-wf-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        unit = await pg_cloud_unit(
            pool,
            common_db,
            tmp_path,
            task_queue=lane_queue,
            api_changes={"rate_limited_creates": 1, "retry_after_s": 2},
        )
        result, history = await _run(unit, client, lane_queue, workflow_queue, "mp09-429")
        api = unit.cloud.api
        scope, heid = unit.identity.request_scope, unit.identity.harness_execution_id

        assert result.disposition == "completed"
        assert parse_operation_result(result.result).status == "completed"
        assert api.rate_limit_refusals == 1 and len(api.creates) == 2 and len(api.agents) == 1
        assert CAPACITY_WAIT_PATCH in patch_ids(history), "the MP-05 wait, not a retry loop"
        timers = [e for e in history.events if e.HasField("timer_started_event_attributes")]
        assert timers, "the Retry-After reset was waited out on a Temporal timer"
        assert _scheduled(history) == ["lane.turn", "lane.turn"]
        state = await unit.states.load(scope, heid)
        assert state is not None
        create = state.dispatch("create", f"{heid}:1:turn:1")
        assert create is not None and create.phase == "acknowledged" and create.attempts == 2
        assert create.native_ref == next(iter(api.agents))
    finally:
        await pool.close()
