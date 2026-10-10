"""MP-12 on the real local Temporal server (127.0.0.1:7233) and a disposable PostgreSQL 17.

The real `OperationWorkflow` drives the real `lane.*` and `continuation.*` activities; the
continuation ledger (transfer rows, sealed checkpoints, the continuation packet) and, in
the crash drill, the lane state and frames live in PostgreSQL under the runtime role.
Providers are labelled fixtures (`tests/fixtures/mp12_lanes.py`); nothing here proves live
provider behaviour.

- V13: a worker is lost after *every* persisted continuation phase (two drills covering the
  seven phases). Each drill ends with exactly one activated target generation whose packet,
  workspace and materialization digests match what was sealed; the continuation turn is sent
  once to the recorded target; the `mp12-operation-continuation` marker is in the history and
  the history replays.
- V12: a GoalDirected run whose first executor is continued (two turns, a transfer) and
  whose family rolls over through Continue-As-New between its iterations: the goal iteration
  and agent-run counters, the rollover count and the reservations are exactly those of a run
  without a continuation; the continuation ledger counts one transfer; the family history
  carries the `mp12-goal-drain-before-continue-as-new` marker and every run replays.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from typing import Any, cast
from uuid import uuid4

import pytest
from temporalio.client import Client, WorkflowHandle
from temporalio.worker import Replayer, Worker

from mission_control.adapters.operations.conformance import ConformanceAuthority
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
from mission_control.adapters.temporal.workflows.goal_directed import (
    CONTINUE_AS_NEW_DRAIN_PATCH,
    GoalDirectedWorkflow,
)
from mission_control.adapters.temporal.workflows.operation import (
    CONTINUATION_PATCH,
    OperationWorkflow,
)
from mission_control.application.context.continuation import (
    ContinuationService,
    ContinuationTransfer,
    InMemoryMailbox,
    RunControlContinuationEvents,
)
from mission_control.application.context.facts import OperationFactsCapture
from mission_control.application.context.hydrators import (
    LaneContinuationRegistration,
    LaneHydratorRegistry,
)
from mission_control.application.context.lane_continuation import (
    LaneContinuationCoordinator,
    LaneStateActivation,
)
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.context.phases import (
    ContinuationPhaseService,
    materialization_digest,
)
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
)
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.contracts.canonical import canonical_digest
from mission_control.domain.authoring.contracts import GoalDirectedBlueprint
from mission_control.domain.authoring.fixtures import GENERIC_GOAL_DIRECTED
from mission_control.domain.context.phases import PHASE_ORDER, ContinuationPhase
from mission_control.domain.execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
)
from mission_control.domain.execution.lanes import LaneSegmentBounds
from mission_control.domain.programs.contracts import GoalDirectedRunInput
from mission_control.domain.programs.goal_directed_runtime import (
    GoalOperationDispatch,
    GoalOperationPreparationRequest,
)
from tests.fixtures.continuation import (
    FakeStaging,
    MemorySnapshots,
    NoArtifacts,
    StepClock,
    trigger,
)
from tests.fixtures.lane_turns import (
    LaneStack,
    cursor_binding,
    cursor_operation,
    lane_stack,
    scripted_frames,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.mp12_lanes import SOURCE_SESSION, ContinuingLane, Crashes, FakeHydrator
from tests.fixtures.temporal_history import patch_ids
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.integration.postgres.test_continuation_postgres import continuation_actor
from tests.integration.temporal.test_wp_bp_020_temporal import FakeGoalDirectedActivities
from tests.integration.temporal.test_wp_bp_020_temporal import _run_input as goal_input

pytestmark = pytest.mark.common_db

ADDRESS = os.environ.get("MC_TEMPORAL_TEST_ADDRESS", "127.0.0.1:7233")
NAMESPACE = os.environ.get("MC_TEMPORAL_TEST_NAMESPACE", "default")
BOUNDS = LaneSegmentBounds(
    max_frames=50,
    max_duration_s=30,
    start_to_close_s=120,
    heartbeat_timeout_s=5,
    status_poll_limit=3,
    status_poll_interval_s=1,
    busy_wait_s=30,
)
FILES = {
    SOURCE_SESSION: {
        "/inputs/sources/source_manifest.json": '{"records": 180}',
        "/outputs/evidence_map.md": "# draft",
    }
}


async def _client() -> Client:
    try:
        return await Client.connect(ADDRESS, namespace=NAMESPACE)
    except RuntimeError as error:
        pytest.fail(f"the real local Temporal server at {ADDRESS} is required: {error}")


def _request(operation: OperationExecutionRequest) -> OperationWorkflowRequest:
    return OperationWorkflowRequest.model_validate(
        {
            "semantic_attempt_id": operation.identity.semantic_key,
            "operation_kind": "bound_operation",
            "operation": operation,
            "segments": BOUNDS,
        }
    )


def _scheduled(history: Any) -> list[str]:
    return [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


async def _replayed(handle: WorkflowHandle[Any, Any], workflows: list[type[Any]]) -> Any:
    history = await handle.fetch_history()
    await Replayer(
        workflows=workflows, workflow_runner=coordinator_workflow_runner()
    ).replay_workflow(history)
    return history


class Composition:
    """The MP-12 worker composition over one scope: ledger in PostgreSQL, lane fixtures."""

    def __init__(
        self,
        pool: Any,
        run_control: Any,
        scope: str,
        *,
        stack: LaneStack,
        frames: Any,
        states: Any,
        crashes: Crashes | None = None,
    ) -> None:
        self.scope = scope
        self.staging = FakeStaging()
        self.snapshots = MemorySnapshots(
            self.staging, {key: dict(value) for key, value in FILES.items()}
        )
        self.selections = PostgresContextSelectionRepository(pool)
        self.transfers = PostgresContinuationRepository(pool)
        self.checkpoints = PostgresCheckpointRepository(pool)
        self.mailbox = InMemoryMailbox()
        self.hydrator = FakeHydrator()
        self.service = ContinuationService(
            transfers=self.transfers,
            checkpoints=self.checkpoints,
            packets=ContextPackService(
                artifacts=NoArtifacts(), selections=self.selections, staging=self.staging
            ),
            packet_reader=self.selections,
            snapshots=self.snapshots,
            events=RunControlContinuationEvents(
                run_control, actor=continuation_actor(), clock=StepClock()
            ),
            mailbox=self.mailbox,
            clock=StepClock(),
        )
        registry = LaneHydratorRegistry(
            {
                "cursor_local": LaneContinuationRegistration(
                    "cursor_local",
                    lambda scope: self.hydrator,
                    snapshots=lambda scope: self.snapshots,
                )
            }
        )
        self.phases = ContinuationPhaseService(
            self.service,
            lanes=registry,
            facts=OperationFactsCapture(),
            activation=LaneStateActivation(states),
            after_persist=crashes,
        )
        self.coordinator = LaneContinuationCoordinator(
            self.transfers, checkpoints=self.checkpoints, packets=self.selections
        )
        self.lane_turns = LaneTurnService(
            lanes=stack.service._lanes,
            boundary=stack.boundary,
            frames=frames,
            states=states,
            frame_reader=frames,
            sessions=WorkerSessionManager(owner_ref=f"worker-{uuid4().hex[:6]}"),
            continuations=self.coordinator,
        )
        self.activities = ContinuationActivities(
            lambda scope: self.service,
            registry,
            phases=lambda scope: self.phases,
            coordinators=lambda scope: self.coordinator,
            states=states,
            frames=frames,
        )
        self.operations = OperationExecutionActivities(
            stack.boundary,
            worker_identity=self.lane_turns.sessions.owner_ref,
            lane_turns=self.lane_turns,
        )

    def worker(self, client: Client, queue: str) -> Worker:
        return Worker(
            client,
            task_queue=queue,
            activities=[*agent_cognitive_activities(self.operations), *self.activities.all()],
            graceful_shutdown_timeout=timedelta(0),
        )

    async def request(self, identity: LaneExecutionIdentity, ref: str) -> ContinuationTransfer:
        return await self.service.request(
            trigger(ref=ref),
            request_scope=self.scope,
            run_key=identity.run_key,
            activation_key=identity.activation_key,
            logical_execution_id=identity.activation_key,
            lane_profile="cursor_local",
            source_session_ref=SOURCE_SESSION,
        )

    async def activated(self, run_key: str) -> list[ContinuationTransfer]:
        return [
            item for item in await self.transfers.for_run(self.scope, run_key) if item.activated
        ]

    async def digests_match(self, transfer: ContinuationTransfer) -> None:
        """The digests the target received are the digests that were sealed and prepared."""

        assert transfer.checkpoint_id is not None and transfer.workspace_snapshot_ref is not None
        stored = await self.checkpoints.get(self.scope, transfer.run_key, transfer.checkpoint_id)
        assert stored is not None and stored.checkpoint.valid
        packet_id = stored.checkpoint.context_packet_ref.removeprefix("context_packet:").rsplit(
            "#", 1
        )[0]
        packet = await self.selections.get(packet_id, request_scope=self.scope)
        assert packet is not None and packet.target.purpose.value == "continuation"
        snapshot = self.snapshots.snapshots[transfer.workspace_snapshot_ref]
        assert transfer.packet_digest == packet.packet_digest
        assert transfer.workspace_manifest_digest == snapshot.manifest_digest
        assert transfer.materialization_digest == materialization_digest(packet, snapshot)
        assert transfer.restored_manifest_digest == canonical_digest(
            dict(sorted(snapshot.manifest.items()))
        )
        assert stored.checkpoint.workspace_snapshot_ref == transfer.workspace_snapshot_ref
        assert [item.phase for item in transfer.phases] == list(PHASE_ORDER[1:]), "no repeats"
        assert transfer.released and transfer.target_generation == 2


# --- V13: a worker lost after every persisted phase ------------------------------------------

DRILLS = (
    frozenset(
        {
            ContinuationPhase.FROZEN,
            ContinuationPhase.SEALED,
            ContinuationPhase.HYDRATED,
            ContinuationPhase.ACTIVATED,
        }
    ),
    frozenset(
        {
            ContinuationPhase.SNAPSHOTTED,
            ContinuationPhase.TARGET_PREPARED,
            ContinuationPhase.VERIFIED,
        }
    ),
)


@pytest.mark.parametrize("crash_at", DRILLS, ids=("drill-a", "drill-b"))
async def test_a_worker_lost_after_each_phase_activates_exactly_one_target(
    common_db: CommonDatabase,  # noqa: F811
    crash_at: frozenset[ContinuationPhase],
) -> None:
    client = await _client()
    lane_queue, workflow_queue = f"mp12-lane-{uuid4().hex[:8]}", f"mp12-wf-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        unit = admitted.unit
        scope = unit.request_scope
        payload = cursor_operation(binding=cursor_binding(task_queue=lane_queue)).model_dump(
            mode="python"
        )
        payload.update(
            request_scope=scope,
            identity=OperationAttemptIdentity(
                run_id=admitted.run_key,
                operation_id=unit.semantic_operation_id,
                operation_attempt=unit.semantic_attempt,
            ),
            runtime_unit=unit,
            idempotency_key=f"mp12:{unit.unit_key}",
        )
        operation = OperationExecutionRequest.model_validate(payload)
        lane = ContinuingLane(frames=scripted_frames(), missing=())
        stack = lane_stack(lane, operation=operation)
        frames = PostgresFrameRepository(pool)
        states = PostgresLaneExecutionStateStore(pool)
        crashes = Crashes(at=set(crash_at))
        composition = Composition(
            pool,
            admitted.run_control,
            scope,
            stack=stack,
            frames=frames,
            states=states,
            crashes=crashes,
        )
        identity = LaneExecutionIdentity.of(operation, "cursor_local", 1)
        requested = await composition.request(identity, "command://continue-1")
        assert requested.phase == ContinuationPhase.REQUESTED

        workflow_worker = Worker(
            client,
            task_queue=workflow_queue,
            workflows=[OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        )
        async with workflow_worker, composition.worker(client, lane_queue):
            handle = await client.start_workflow(
                OperationWorkflow.run,
                _request(operation),
                id=f"mp12-v13-{uuid4().hex[:10]}",
                task_queue=workflow_queue,
                execution_timeout=timedelta(minutes=5),
            )
            result = await asyncio.wait_for(handle.result(), timeout=240)
            history = await _replayed(handle, [OperationWorkflow])

        assert result.disposition == "completed"
        assert parse_operation_result(result.result).status == "completed"
        assert crashes.at == set(), "every chosen phase crashed once"
        activated = await composition.activated(admitted.run_key)
        assert len(activated) == 1, "exactly one activated target generation"
        transfer = activated[0]
        assert transfer.transfer_id == requested.transfer_id
        await composition.digests_match(transfer)
        assert transfer.target_session_ref == "agent-fake-2"
        assert transfer.target_turn_no == 2
        assert len(composition.hydrator.requests) == 1, "the recorded hydration never repeats"
        heid = identity.harness_execution_id
        assert lane.sends == [f"{heid}:1:turn:1", f"{heid}:1:turn:2"], "one continuation turn"
        state = await states.load(scope, heid)
        assert state is not None and state.native_session_ref == "agent-fake-2"
        turn_two = state.dispatch("send", f"{heid}:1:turn:2")
        assert turn_two is not None and turn_two.phase == "acknowledged"
        assert "purpose: continuation" in lane.staged_text[f"continuation:{transfer.transfer_id}"]
        assert CONTINUATION_PATCH in patch_ids(history)
        scheduled = _scheduled(history)
        assert scheduled.count("lane.turn") == 2
        assert "continuation.pending" in scheduled
        assert scheduled.count("continuation.advance") >= 4
        assert len(await composition.transfers.for_run(scope, admitted.run_key)) == 1
    finally:
        await pool.close()


# --- V12: a continued executor leaves the GoalDirected counters untouched ---------------------


class _IterationAuthority(ConformanceAuthority):
    """FIXTURE: accepts every iteration's claim of one run (reservation ids and run
    versions differ per claim); the real authority is run control, not exercised here."""

    async def verify(self, request: OperationExecutionRequest) -> None:
        if (
            request.identity.run_id != self.accepted_run_id
            or request.effective_configuration_digest != self.configuration_digest
        ):
            raise ValueError("operation authority binding is not accepted")


class ContinuedExecutorGoal(FakeGoalDirectedActivities):
    """FIXTURE family boundaries whose executors run on the MP-12 lane stack (segment-driven
    `cursor_local` units); the verifier stays the fake native operation."""

    def __init__(self, queue: str, lane_queue: str, executors: dict[int, Any]) -> None:
        super().__init__(complete_at_iteration=2)
        self.queue = queue
        self.lane_queue = lane_queue
        self.executors = executors
        self.executor_preparations: list[tuple[int, dict[str, int]]] = []

    async def _prepare(self, request: GoalOperationPreparationRequest) -> GoalOperationDispatch:
        dispatch = await super()._prepare(request)
        if request.operation_role == "executor":
            self.executor_preparations.append((request.goal_iteration, dict(request.reservation)))
            template = self.executors[request.goal_iteration]
            # The interpreter binds the result to the claim's session, workspace and
            # reservation; the family consumes the settlement at the claim's run version.
            operation = template.model_copy(
                update={
                    "session_id": request.session_id,
                    "workspace": template.workspace.model_copy(
                        update={"workspace_id": request.workspace_id}
                    ),
                    "budget_reservation_id": request.reservation_id,
                    "budget_limits": dict(request.reservation),
                    "run_control_revision": request.expected_run_version,
                }
            )
            workflow_request = OperationWorkflowRequest.model_validate(
                {
                    "semantic_attempt_id": operation.identity.semantic_key,
                    "execution_generation": request.execution_generation,
                    "operation_kind": "bound_operation",
                    "operation": operation,
                    "segments": BOUNDS,
                }
            )
            return dispatch.model_copy(update={"workflow_request": workflow_request})
        from mission_control.domain.execution.contracts import NativeOperationExecutionPlacement

        operation = dispatch.workflow_request.operation.model_copy(
            update={
                "native_placement": NativeOperationExecutionPlacement.create(
                    placement_id="native.mp12.verifier",
                    revision=1,
                    task_queue=self.queue,
                    qualification_refs=("QUAL-MP12-FIXTURE",),
                )
            }
        )
        return dispatch.model_copy(
            update={
                "workflow_request": dispatch.workflow_request.model_copy(
                    update={"operation": operation}
                )
            }
        )

    @property
    def functions(self) -> list[object]:
        return [
            self.prepare_executor,
            self.prepare_verifier,
            self.execute_operation,
            self.cancel_operation,
            self.reconcile,
            self.lifecycle,
        ]


async def test_a_continued_executor_and_a_rollover_leave_the_goal_counters_intact(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    from mission_control.application.execution.harness.state import InMemoryLaneExecutionStateStore
    from mission_control.application.frames.sink import InMemoryFrameStore
    from tests.fixtures.lane_turns import ACTIVATION_UUID, RUN_UUID

    client = await _client()
    lane_queue, family_queue = f"mp12-lane-{uuid4().hex[:8]}", f"mp12-goal-{uuid4().hex[:8]}"
    pool = await common_db.pool("mission_control_runtime")
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        scope = admitted.unit.request_scope
        run_key = admitted.run_key

        def executor(iteration: int) -> OperationExecutionRequest:
            payload = cursor_operation(binding=cursor_binding(task_queue=lane_queue)).model_dump(
                mode="python"
            )
            payload.update(
                request_scope=scope,
                identity=OperationAttemptIdentity(
                    run_id=run_key, operation_id=f"goal-executor-{iteration}", operation_attempt=1
                ),
                idempotency_key=f"mp12-goal:{iteration}",
            )
            return OperationExecutionRequest.model_validate(payload)

        executors = {1: executor(1), 2: executor(2)}
        lane = ContinuingLane(frames=scripted_frames(), missing=())
        stack = lane_stack(lane, operation=executors[1])
        # FIXTURE authority: each iteration's claim brings its own reservation and run
        # version; the in-memory boundary accepts the run and configuration digest only.
        stack.boundary._authority = _IterationAuthority(
            accepted_run_id=run_key,
            configuration_digest=executors[1].effective_configuration_digest,
            control_revision=executors[1].run_control_revision,
            reservation_id=executors[1].budget_reservation_id,
        )
        frames = InMemoryFrameStore()
        frames.register_run(
            scope,
            run_key,
            RUN_UUID,
            {
                executors[1].identity.operation_id: ACTIVATION_UUID,
                executors[2].identity.operation_id: uuid4(),
            },
        )
        states = InMemoryLaneExecutionStateStore()
        composition = Composition(
            pool, admitted.run_control, scope, stack=stack, frames=frames, states=states
        )
        first = LaneExecutionIdentity.of(executors[1], "cursor_local", 1)
        requested = await composition.request(first, "command://continue-goal-1")

        family = ContinuedExecutorGoal(family_queue, lane_queue, executors)
        blueprint = GoalDirectedBlueprint.model_validate(
            {**GENERIC_GOAL_DIRECTED.model_dump(mode="python"), "max_iterations": 3}
        )
        run_input: GoalDirectedRunInput = replace(
            goal_input(blueprint=blueprint, run_id=run_key),
            request_scope=scope,
            continue_as_new_iterations=2,
        )
        family_worker = Worker(
            client,
            task_queue=family_queue,
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            activities=cast(list[Callable[..., Any]], family.functions),
            workflow_runner=coordinator_workflow_runner(),
        )
        async with family_worker, composition.worker(client, lane_queue):
            handle = await client.start_workflow(
                GoalDirectedWorkflow.run,
                run_input,
                id=f"mp12-v12-{uuid4().hex[:10]}",
                task_queue=family_queue,
                execution_timeout=timedelta(minutes=6),
            )
            result = await asyncio.wait_for(handle.result(), timeout=300)
            runs = [run async for run in client.list_workflows(f'WorkflowId = "{handle.id}"')]
            histories = []
            for run in sorted(runs, key=lambda item: item.start_time):
                histories.append(
                    await _replayed(
                        client.get_workflow_handle(handle.id, run_id=run.run_id),
                        [GoalDirectedWorkflow, OperationWorkflow],
                    )
                )

        # Two goal iterations, two agent runs, no session rollover: a continuation and a
        # Continue-As-New inside the run moved none of the family's counters.
        assert result.status == "stopping"
        assert (result.goal_iterations, result.agent_runs, result.rollover_count) == (2, 2, 0)
        assert [item[0] for item in family.executor_preparations] == [1, 2]
        reservations = {tuple(sorted(item[1].items())) for item in family.executor_preparations}
        assert len(reservations) == 1, "the reservation is the blueprint's, never reset"
        assert family.lifecycle_kinds.count("terminalize") == 1
        activated = await composition.activated(run_key)
        assert [item.transfer_id for item in activated] == [requested.transfer_id]
        await composition.digests_match(activated[0])
        assert activated[0].ledger.transfers == 1
        heid_one = first.harness_execution_id
        heid_two = LaneExecutionIdentity.of(executors[2], "cursor_local", 1).harness_execution_id
        assert lane.sends == [
            f"{heid_one}:1:turn:1",
            f"{heid_one}:1:turn:2",
            f"{heid_two}:1:turn:1",
        ], "one continuation turn in iteration 1; iteration 2 is an ordinary fresh session"
        assert len(histories) == 2, "one Continue-As-New between the iterations"
        assert CONTINUE_AS_NEW_DRAIN_PATCH in patch_ids(histories[0])
        assert CONTINUATION_PATCH not in patch_ids(histories[0]), "the family never continued"
    finally:
        await pool.close()
