"""MP-07 x MP-12 (V13) on the real local Temporal server (127.0.0.1:7233) and a disposable
PostgreSQL 17: a `claude_agent_sdk` unit continued into a fresh session by the real
`OperationWorkflow` (the Claude twin of
`tests/integration/codex/test_codex_continuation_temporal.py`).

The real `lane.*` and `continuation.*` activities run over `LaneTurnService` and the
continuation service; the continuation ledger (transfer rows, sealed checkpoint, the
continuation packet), the lane state and the frames live in PostgreSQL under the runtime role.
The lane is the Claude harness over the FIXTURE SDK client (`tests/unit/claude/fixtures.py`: no
Claude Code process, no login, no model turn), continued through
`claude_continuation_registration`: the snapshot port freezes the lease, the hydrator opens a
fresh SDK session (no `resume`, no conversation fork). The worker is lost (an activity raises)
right after three persisted phases; exactly one target is activated, the continuation turn is
sent once to that fresh session, and the history replays.
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

from mission_control.adapters.claude.continuation import (
    ClaudeWorkspaceSnapshots,
    claude_continuation_registration,
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
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.mp12_lanes import Crashes
from tests.fixtures.temporal_history import patch_ids
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.integration.postgres.test_continuation_postgres import continuation_actor
from tests.unit.claude.fixtures import (
    SESSION_ID,
    FixtureScript,
    claude_operation,
    claude_stack,
)

pytestmark = pytest.mark.common_db

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")
TARGET = "sess-fixture-claude-0002"
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


@pytest.mark.parametrize(
    "crash_at",
    [frozenset(), CRASH_AT],
    ids=["no-loss", "loss-after-snapshotted-hydrated-activated"],
)
async def test_a_claude_unit_continues_into_a_fresh_session_on_real_services(
    common_db: CommonDatabase,  # noqa: F811
    tmp_path: Path,
    crash_at: frozenset[ContinuationPhase],
) -> None:
    client = await _client()
    lane_queue, workflow_queue = f"mp07c-lane-{uuid4().hex[:8]}", f"mp07c-wf-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        unit = admitted.unit
        scope = unit.request_scope
        payload = claude_operation(task_queue=lane_queue).model_dump(mode="python")
        payload.update(
            request_scope=scope,
            identity=OperationAttemptIdentity(
                run_id=admitted.run_key,
                operation_id=unit.semantic_operation_id,
                operation_attempt=unit.semantic_attempt,
            ),
            runtime_unit=unit,
            idempotency_key=f"mp07c:{unit.unit_key}",
        )
        operation = OperationExecutionRequest.model_validate(payload)
        stack = claude_stack(tmp_path, script="continuation_source", operation=operation)
        # The fresh target session replays the target script. The activated target is adopted
        # from the pending handover this process holds (no resume); should a later process
        # ever resume it by id, that client replays the target script too (the fixture
        # factory would otherwise replay its base, the source script, for a resumed client).
        stack.factory.fresh_scripts.append(FixtureScript.load("continuation_target"))
        base_create = stack.factory.create

        def create(options: Any, *, environment: Any) -> Any:
            if options.resume == TARGET:
                saved, stack.factory.script = (
                    stack.factory.script,
                    FixtureScript.load("continuation_target"),
                )
                try:
                    return base_create(options, environment=environment)
                finally:
                    stack.factory.script = saved
            return base_create(options, environment=environment)

        stack.factory.create = create  # type: ignore[method-assign]
        harness = stack.harness
        frames = PostgresFrameRepository(pool)
        states = PostgresLaneExecutionStateStore(pool)
        staging = FakeStaging()
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
            snapshots=ClaudeWorkspaceSnapshots(harness, staging),
            events=RunControlContinuationEvents(
                admitted.run_control, actor=continuation_actor(), clock=StepClock()
            ),
            mailbox=InMemoryMailbox(),
            clock=StepClock(),
        )
        registry = LaneHydratorRegistry()
        registry.register(claude_continuation_registration(harness, staging))
        crashes = Crashes(at=set(crash_at))
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
            lanes=stack.lanes.service._lanes,
            boundary=stack.lanes.boundary,
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
            stack.lanes.boundary,
            worker_identity=lane_turns.sessions.owner_ref,
            lane_turns=lane_turns,
        )
        identity = LaneExecutionIdentity.of(operation, "claude_agent_sdk", 1)
        requested = await service.request(
            trigger(ref="command://claude-continue-1"),
            request_scope=scope,
            run_key=identity.run_key,
            activation_key=identity.activation_key,
            logical_execution_id=identity.activation_key,
            lane_profile="claude_agent_sdk",
            source_session_ref=SESSION_ID,
        )
        assert requested.phase == ContinuationPhase.REQUESTED

        async def release_the_source_turn() -> None:
            source = await stack.factory.connected(within_s=120)
            await source.held.wait()
            source.release.set()

        releaser = asyncio.create_task(release_the_source_turn())
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
                id=f"mp07-continuation-{uuid4().hex[:10]}",
                task_queue=workflow_queue,
                execution_timeout=timedelta(minutes=5),
            )
            result = await asyncio.wait_for(handle.result(), timeout=150)
            history = await handle.fetch_history()
            await Replayer(
                workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
            ).replay_workflow(history)
        await asyncio.wait_for(releaser, timeout=5)

        assert result.disposition == "completed"
        assert parse_operation_result(result.result).status == "completed"
        assert crashes.at == set(), "every chosen phase crashed once"
        activated = list(await transfers.for_run(scope, admitted.run_key))
        assert len(activated) == 1 and activated[0].activated, "exactly one activated target"
        transfer = activated[0]
        assert transfer.target_session_ref == TARGET and transfer.released
        # The target is a fresh SDK session (never a resume of the source, never a
        # conversation fork); after the loss at `activated` it is reattached by its own id.
        targets = [item for item in stack.factory.clients if item.script.session_id == TARGET]
        assert targets and targets[0].options.resume is None
        assert {item.options.resume for item in targets} <= {None, TARGET}
        # The hydrated session itself is adopted: no second connection resumes the target.
        assert len(targets) == 1
        assert all(item.options.fork_session is False for item in targets)
        sent_to_target = [text for item in targets for _uuid, text in item.sent]
        assert len(sent_to_target) == 1, "the continuation turn is sent once"
        assert "- purpose: continuation" in sent_to_target[0]
        heid = identity.harness_execution_id
        state = await states.load(scope, heid)
        assert state is not None and state.native_session_ref == TARGET
        assert CONTINUATION_PATCH in patch_ids(history)
        scheduled = _scheduled(history)
        assert scheduled.count("lane.turn") >= 2
        assert "continuation.pending" in scheduled
    finally:
        await pool.close()
