"""MP-08 x MP-12 on the real local Temporal server (127.0.0.1:7233) and a disposable
PostgreSQL 17: a codex unit continued into a fresh thread by the real `OperationWorkflow`.

The real `lane.*` and `continuation.*` activities run over `LaneTurnService` and the
continuation service; the continuation ledger (transfer rows, sealed checkpoint, the
continuation packet), the lane state and the frames live in PostgreSQL under the runtime role.
The lane is the `codex` harness over the FIXTURE app-server (no CLI, no login, no model turn),
continued through `codex_continuation_registration`: the snapshot port freezes the lease and
its git patch, the hydrator starts a fresh thread on a fresh app-server. The worker is lost
(an activity raises) right after three persisted phases; exactly one target is activated, the
continuation turn is sent once to that fresh thread, and the history replays.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from temporalio.client import Client
from temporalio.worker import Replayer, Worker

from mission_control.adapters.codex.continuation import (
    CodexWorkspaceSnapshots,
    codex_continuation_registration,
)
from mission_control.adapters.postgres.context.continuation_repository import (
    PostgresCheckpointRepository,
    PostgresContinuationRepository,
)
from mission_control.adapters.postgres.context.selection_repository import (
    PostgresContextSelectionRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.lanes.execution_state import PostgresLaneExecutionStateStore
from mission_control.adapters.temporal.activities.continuation import ContinuationActivities
from mission_control.adapters.temporal.operation_activities import (
    OperationExecutionActivities,
    parse_operation_result,
)
from mission_control.adapters.temporal.registration.activities import agent_cognitive_activities
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import (
    CONTINUATION_PATCH,
    OperationWorkflow,
)
from mission_control.application.context.continuation import (
    ContinuationService,
    InMemoryMailbox,
    RunControlContinuationEvents,
)
from mission_control.application.context.facts import OperationFactsCapture
from mission_control.application.context.hydrators import LaneHydratorRegistry
from mission_control.application.context.lane_continuation import (
    LaneContinuationCoordinator,
    LaneStateActivation,
)
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.context.phases import ContinuationPhaseService
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
)
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.domain.context.phases import ContinuationPhase
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
)
from mission_control.domain.execution.lanes import LaneSegmentBounds
from tests.fixtures.continuation import FakeStaging, NoArtifacts, StepClock, trigger
from tests.fixtures.lane_turns import lane_stack
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.mp12_lanes import Crashes
from tests.fixtures.temporal_history import patch_ids
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.integration.postgres.test_continuation_postgres import continuation_actor
from tests.unit.codex.fixture_app_server import FixtureLauncher, load_script
from tests.unit.codex.support import build_harness, codex_binding, codex_operation

pytestmark = pytest.mark.common_db

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")
SOURCE = "thr-0001"
BOUNDS = LaneSegmentBounds(
    max_frames=50,
    max_duration_s=30,
    start_to_close_s=120,
    heartbeat_timeout_s=5,
    status_poll_limit=3,
    status_poll_interval_s=1,
    busy_wait_s=30,
)
CRASH_AT = frozenset(
    {ContinuationPhase.SNAPSHOTTED, ContinuationPhase.HYDRATED, ContinuationPhase.ACTIVATED}
)


async def _client() -> Client:
    try:
        return await Client.connect(ADDRESS, namespace=NAMESPACE)
    except RuntimeError as error:
        pytest.fail(f"the real local Temporal server at {ADDRESS} is required: {error}")


def _scheduled(history: Any) -> list[str]:
    return [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


async def test_a_codex_unit_continues_into_a_fresh_thread_on_real_services(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
) -> None:
    client = await _client()
    lane_queue, workflow_queue = f"mp08c-lane-{uuid4().hex[:8]}", f"mp08c-wf-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        unit = admitted.unit
        scope = unit.request_scope
        payload = codex_operation(codex_binding(task_queue=lane_queue)).model_dump(mode="python")
        payload.update(
            request_scope=scope,
            identity=OperationAttemptIdentity(
                run_id=admitted.run_key,
                operation_id=unit.semantic_operation_id,
                operation_attempt=unit.semantic_attempt,
            ),
            runtime_unit=unit,
            idempotency_key=f"mp08c:{unit.unit_key}",
        )
        operation = OperationExecutionRequest.model_validate(payload)
        launcher = FixtureLauncher(
            scripts=[load_script("turn_full")], launch_scripts={1: [load_script("turn_full")]}
        )
        harness, _leaser, _artifacts, _auth = build_harness(tmp_path, launcher)
        stack = lane_stack(harness, operation=operation)
        frames = PostgresFrameRepository(pool)
        states = PostgresLaneExecutionStateStore(pool)
        staging = FakeStaging()
        snapshots = CodexWorkspaceSnapshots(harness, staging)
        selections = PostgresContextSelectionRepository(pool)
        transfers = PostgresContinuationRepository(pool)
        checkpoints = PostgresCheckpointRepository(pool)
        service = ContinuationService(
            transfers=transfers,
            checkpoints=checkpoints,
            packets=ContextPackService(
                artifacts=NoArtifacts(), selections=selections, staging=staging
            ),
            packet_reader=selections,
            snapshots=snapshots,
            events=RunControlContinuationEvents(
                admitted.run_control, actor=continuation_actor(), clock=StepClock()
            ),
            mailbox=InMemoryMailbox(),
            clock=StepClock(),
        )
        registry = LaneHydratorRegistry()
        registry.register(codex_continuation_registration(harness, staging))
        crashes = Crashes(at=set(CRASH_AT))
        phases = ContinuationPhaseService(
            service,
            lanes=registry,
            facts=OperationFactsCapture(),
            activation=LaneStateActivation(states),
            after_persist=crashes,
        )
        coordinator = LaneContinuationCoordinator(
            transfers, checkpoints=checkpoints, packets=selections
        )
        lane_turns = LaneTurnService(
            lanes=stack.service._lanes,
            boundary=stack.boundary,
            frames=frames,
            states=states,
            frame_reader=frames,
            sessions=WorkerSessionManager(owner_ref=f"worker-{uuid4().hex[:6]}"),
            continuations=coordinator,
        )
        activities = ContinuationActivities(
            lambda _scope: service,
            registry,
            phases=lambda _scope: phases,
            coordinators=lambda _scope: coordinator,
            states=states,
            frames=frames,
        )
        operations = OperationExecutionActivities(
            stack.boundary, worker_identity=lane_turns.sessions.owner_ref, lane_turns=lane_turns
        )
        identity = LaneExecutionIdentity.of(operation, "codex", 1)
        requested = await service.request(
            trigger(ref="command://codex-continue-1"),
            request_scope=scope,
            run_key=identity.run_key,
            activation_key=identity.activation_key,
            logical_execution_id=identity.activation_key,
            lane_profile="codex",
            source_session_ref=SOURCE,
        )
        assert requested.phase == ContinuationPhase.REQUESTED

        async with (
            Worker(
                client,
                task_queue=workflow_queue,
                workflows=[OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
            ),
            Worker(
                client,
                task_queue=lane_queue,
                activities=[*agent_cognitive_activities(operations), *activities.all()],
                graceful_shutdown_timeout=timedelta(0),
            ),
        ):
            handle = await client.start_workflow(
                OperationWorkflow.run,
                OperationWorkflowRequest.model_validate(
                    {
                        "semantic_attempt_id": operation.identity.semantic_key,
                        "operation_kind": "bound_operation",
                        "operation": operation,
                        "segments": BOUNDS,
                    }
                ),
                id=f"mp08-continuation-{uuid4().hex[:10]}",
                task_queue=workflow_queue,
                execution_timeout=timedelta(minutes=5),
            )
            result = await asyncio.wait_for(handle.result(), timeout=240)
            history = await handle.fetch_history()
            await Replayer(
                workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
            ).replay_workflow(history)

        assert result.disposition == "completed"
        assert parse_operation_result(result.result).status == "completed"
        assert crashes.at == set(), "every chosen phase crashed once"
        activated = list(await transfers.for_run(scope, admitted.run_key))
        assert len(activated) == 1 and activated[0].activated, "exactly one activated target"
        transfer = activated[0]
        target = transfer.target_session_ref
        assert target is not None and target != SOURCE
        assert transfer.target_turn_no == 2 and transfer.released
        assert len(launcher.launches) == 2, "one fresh app-server for the target, never more"
        source_methods = [m for m, _p in launcher.launches[0].server.records]
        target_methods = [m for m, _p in launcher.launches[1].server.records]
        assert source_methods.count("turn/start") == 1
        assert target_methods.count("thread/start") == 1 and target_methods.count("turn/start") == 1
        assert not {"thread/resume", "thread/fork"} & {*source_methods, *target_methods}
        (sent,) = [
            "".join(item.get("text", "") for item in params.get("input", []))
            for method, params in launcher.launches[1].server.records
            if method == "turn/start"
        ]
        assert "purpose: continuation" in sent
        heid = identity.harness_execution_id
        state = await states.load(scope, heid)
        assert state is not None and state.native_session_ref == target
        turn_two = state.dispatch("send", f"{heid}:1:turn:2")
        assert turn_two is not None and turn_two.phase == "acknowledged"
        assert CONTINUATION_PATCH in patch_ids(history)
        scheduled = _scheduled(history)
        assert scheduled.count("lane.turn") == 2
        assert "continuation.pending" in scheduled
    finally:
        await pool.close()
