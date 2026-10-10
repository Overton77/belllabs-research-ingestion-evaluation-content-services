"""MP-12 in the lane turn service (in-memory stores, FIXTURE lanes): pressure observations
that never invent an occupancy, native compaction as a qualified-only path, the
continuation boundary, the fence of an in-flight transfer, the continuation turn on the
activated target, the fenced-handover decision and the settlement after a failed target."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from mission_control.application.context.facts import FactsContext, OperationFactsCapture
from mission_control.application.context.hydrators import (
    LaneContinuationRegistration,
    LaneHydratorRegistry,
)
from mission_control.application.context.lane_continuation import (
    LaneContinuationCoordinator,
    LaneStateActivation,
)
from mission_control.application.context.lane_support import ContinuationInFlight
from mission_control.application.context.phases import ContinuationPhaseService
from mission_control.application.execution.harness.controls import SessionHandover
from mission_control.application.execution.harness.dispatch import (
    DispatchRecord,
    instruction_digest,
)
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
)
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.domain.context.checkpoint import ContinuationTriggerKind
from mission_control.domain.context.phases import ContinuationPhase
from mission_control.domain.context.pressure import ContextOccupancy, ContextPressurePolicy
from mission_control.domain.execution.lane_turns import LaneTurnRequest
from mission_control.domain.execution.lanes import (
    LaneSegmentBounds,
    SessionHandle,
    UsageReport,
)
from mission_control.domain.frames.contracts import FrameKind
from mission_control.domain.policies.stop_fence import StopFence
from tests.fixtures.continuation import build_service, trigger
from tests.fixtures.lane_turns import (
    SCOPE,
    RecordingSignals,
    ScriptedSessionLane,
    cursor_operation,
    lane_stack,
    scripted_frames,
)
from tests.fixtures.mp12_lanes import CompactingFixtureLane, ContinuingLane, FakeHydrator

BOUNDS = LaneSegmentBounds(max_frames=50, max_duration_s=5, heartbeat_timeout_s=3)
SOURCE = "agent-fake-1"
FILES = {SOURCE: {"/outputs/evidence_map.md": "# draft"}}
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


@dataclass
class Stack:
    lane: Any
    service: LaneTurnService
    stack: Any
    wired: dict[str, Any]
    coordinator: LaneContinuationCoordinator
    phases: ContinuationPhaseService
    hydrator: FakeHydrator
    operation: Any
    identity: LaneExecutionIdentity

    def request(self, **changes: Any) -> LaneTurnRequest:
        values: dict[str, Any] = {
            "operation": self.operation,
            "lane_profile": "cursor_local",
            "generation": 1,
            "segment": BOUNDS,
        }
        values.update(changes)
        return LaneTurnRequest.model_validate(values)

    def signals(self) -> RecordingSignals:
        return RecordingSignals(self.stack.frames, self.identity.harness_execution_id)

    async def frames(self) -> list[Any]:
        return list(
            await self.stack.frames.frames_for_execution(
                SCOPE, self.identity.harness_execution_id, 1, limit=10_000
            )
        )

    async def frame_kinds(self) -> list[tuple[str, str]]:
        return [(frame.raw_kind, frame.kind.value) for frame in await self.frames()]

    async def pending(self) -> Any:
        return await self.coordinator.pending(
            SCOPE, self.identity.run_key, source_session_ref=SOURCE
        )

    async def request_continuation(self) -> Any:
        return await self.wired["service"].request(
            trigger(),
            request_scope=SCOPE,
            run_key=self.identity.run_key,
            activation_key=self.identity.activation_key,
            logical_execution_id=self.identity.activation_key,
            lane_profile="cursor_local",
            source_session_ref=SOURCE,
        )

    async def drive(self, transfer_id: str) -> Any:
        state = await self.stack.states.load(SCOPE, self.identity.harness_execution_id)
        context = FactsContext(
            operation=self.operation,
            harness_execution_id=str(self.identity.harness_execution_id),
            generation=1,
            turn_no=1,
            lane_state=state,
            frames=tuple(await self.frames()),
        )
        return await self.phases.drive(transfer_id, context, request_scope=SCOPE)


def build(
    lane: Any,
    *,
    policy: ContextPressurePolicy | None = None,
    fences: InMemoryStopFenceRepository | None = None,
    hydrator: FakeHydrator | None = None,
) -> Stack:
    operation = cursor_operation()
    stack = lane_stack(lane, operation=operation)
    identity = LaneExecutionIdentity.of(operation, "cursor_local", 1)
    wired = build_service(session_files=FILES)
    hydrator = hydrator or FakeHydrator()
    registry = LaneHydratorRegistry(
        {
            "cursor_local": LaneContinuationRegistration(
                "cursor_local", lambda scope: hydrator, snapshots=lambda scope: wired["snapshots"]
            )
        }
    )
    phases = ContinuationPhaseService(
        wired["service"],
        lanes=registry,
        facts=OperationFactsCapture(),
        activation=LaneStateActivation(stack.states),
    )
    coordinator = LaneContinuationCoordinator(
        wired["store"],
        policy=policy,
        checkpoints=wired["service"].checkpoint_store,
        packets=wired["selections"],
    )
    service = LaneTurnService(
        lanes=stack.service._lanes,
        boundary=stack.boundary,
        frames=stack.frames,
        states=stack.states,
        frame_reader=stack.frames,
        sessions=WorkerSessionManager(owner_ref="worker-a"),
        fences=fences,
        continuations=coordinator,
    )
    return Stack(lane, service, stack, wired, coordinator, phases, hydrator, operation, identity)


@pytest.mark.asyncio
async def test_pressure_is_observed_as_unknown_and_a_complete_turn_settles_as_before() -> None:
    lane = ContinuingLane(frames=scripted_frames(), missing=())
    s = build(lane)
    result = await s.service.turn(s.request(), s.signals())
    assert result.done and result.operation_result is not None
    assert result.operation_result["status"] == "completed"
    kinds = await s.frame_kinds()
    assert ("mc.context_pressure", "status") in kinds
    pressure = next(frame for frame in await s.frames() if frame.raw_kind == "mc.context_pressure")
    assert '"ratio":"unknown"' in pressure.body_excerpt
    assert '"basis":"unknown"' in pressure.body_excerpt
    assert await s.pending() is None, "no pressure trigger without work to continue"


@pytest.mark.asyncio
async def test_hard_pressure_with_work_left_on_an_unqualified_lane_seals_a_continuation() -> None:
    lane = ContinuingLane(
        frames=scripted_frames(),
        occupancy=ContextOccupancy.measured(90_000, 100_000, "provider_context_window"),
    )
    s = build(lane)
    result = await s.service.turn(s.request(), s.signals())
    assert not result.done and not result.busy
    assert result.closing_facts is not None and result.closing_facts.native_status == "finished"
    assert result.native.session_ref == SOURCE and result.native.turn_ref == "run-fake-1"
    pending = await s.pending()
    assert (
        pending is not None and pending.trigger.kind == ContinuationTriggerKind.CONTEXT_HEALTH_HARD
    )
    assert pending.phase == ContinuationPhase.REQUESTED
    assert "end_session" not in lane.calls, "the source turn is not settled at the boundary"
    kinds = [raw for raw, _kind in await s.frame_kinds()]
    assert "mc.context_pressure" in kinds and "mc.native_compaction_unavailable" in kinds
    assert "custom.mc.before_compaction" not in kinds, "unqualified: never compacted natively"
    assert s.service.sessions.live(SCOPE, s.identity.harness_execution_id) is not None


@pytest.mark.asyncio
async def test_soft_pressure_compacts_only_where_qualified_then_continues_the_session() -> None:
    lane = CompactingFixtureLane(
        frames=scripted_frames(),
        occupancy=ContextOccupancy.measured(75_000, 100_000, "provider_context_window"),
        occupancy_after=ContextOccupancy.measured(30_000, 100_000, "provider_context_window"),
        describe_matrix=ScriptedSessionLane()
        .describe()
        .model_copy(update={"compaction_control": "native"}),
    )
    s = build(lane)
    result = await s.service.turn(s.request(), s.signals())
    assert result.done and result.operation_result is not None, "no transfer: the session goes on"
    assert len(lane.compactions) == 1 and lane.compactions[0].epoch == 1
    kinds = await s.frame_kinds()
    assert ("custom.mc.before_compaction", "before_compaction") in kinds
    assert ("custom.mc.after_compaction", "after_compaction") in kinds
    remeasured = [frame for frame in await s.frames() if frame.raw_kind == "mc.context_pressure"]
    assert len(remeasured) == 2 and '"level":"none"' in remeasured[-1].body_excerpt
    assert await s.pending() is None
    # The same lane without the qualified control never compacts.
    unqualified = CompactingFixtureLane(
        frames=scripted_frames(),
        occupancy=ContextOccupancy.measured(75_000, 100_000, "provider_context_window"),
    )
    u = build(unqualified)
    boundary = await u.service.turn(u.request(), u.signals())
    assert not boundary.done and unqualified.compactions == []
    pending = await u.pending()
    assert pending is not None
    assert pending.trigger.kind == ContinuationTriggerKind.CONTEXT_HEALTH_SOFT


@pytest.mark.asyncio
async def test_a_failed_native_compaction_falls_back_to_the_sealed_checkpoint() -> None:
    lane = CompactingFixtureLane(
        frames=scripted_frames(),
        occupancy=ContextOccupancy.measured(75_000, 100_000, "provider_context_window"),
        compact_fails=True,
        describe_matrix=ScriptedSessionLane()
        .describe()
        .model_copy(update={"compaction_control": "native"}),
    )
    s = build(lane)
    result = await s.service.turn(s.request(), s.signals())
    assert not result.done
    kinds = [raw for raw, _kind in await s.frame_kinds()]
    assert "custom.mc.before_compaction" in kinds and "mc.native_compaction_failed" in kinds
    assert "custom.mc.after_compaction" not in kinds, "a failed compaction advances no epoch"
    assert await s.pending() is not None


@pytest.mark.asyncio
async def test_a_requested_continuation_runs_the_phases_then_the_next_turn_hits_the_target() -> (
    None
):
    lane = ContinuingLane(frames=scripted_frames())
    s = build(lane)
    transfer = await s.request_continuation()
    boundary = await s.service.turn(s.request(), s.signals())
    assert not boundary.done and boundary.closing_facts is not None
    assert lane.sends == [f"{s.identity.harness_execution_id}:1:turn:1"]
    activated = await s.drive(transfer.transfer_id)
    assert activated.activated and activated.target_session_ref == "agent-fake-2"
    # The next turn goes to the activated target, journaled under its own key.
    continuation = s.request(turn_no=2, instruction_ref=f"continuation:{transfer.transfer_id}")
    result = await s.service.turn(continuation, s.signals())
    assert result.done and result.operation_result is not None
    assert result.native.session_ref == "agent-fake-2"
    assert lane.sends == [
        f"{s.identity.harness_execution_id}:1:turn:1",
        f"{s.identity.harness_execution_id}:1:turn:2",
    ]
    assert "reattach" in lane.calls
    staged = lane.staged_text[f"continuation:{transfer.transfer_id}"]
    assert "purpose: continuation" in staged
    state = await s.stack.states.load(SCOPE, s.identity.harness_execution_id)
    assert state is not None and state.native_session_ref == "agent-fake-2"
    record = state.dispatch("send", f"{s.identity.harness_execution_id}:1:turn:2")
    assert record is not None and record.phase == "acknowledged"


@pytest.mark.asyncio
async def test_a_frozen_session_refuses_every_new_dispatch_until_activation() -> None:
    lane = ContinuingLane(frames=scripted_frames())
    s = build(lane)
    transfer = await s.request_continuation()
    await s.service.turn(s.request(), s.signals())
    state = await s.stack.states.load(SCOPE, s.identity.harness_execution_id)
    context = FactsContext(
        operation=s.operation,
        harness_execution_id=str(s.identity.harness_execution_id),
        generation=1,
        turn_no=1,
        lane_state=state,
    )
    frozen = await s.phases.advance(transfer.transfer_id, context, request_scope=SCOPE)
    assert frozen.phase == ContinuationPhase.FROZEN and frozen.fencing
    with pytest.raises(ContinuationInFlight) as refused:
        await s.service.turn(s.request(turn_no=2), s.signals())
    assert refused.value.transfer_id == transfer.transfer_id
    assert lane.sends == [f"{s.identity.harness_execution_id}:1:turn:1"], "nothing new was sent"


@pytest.mark.asyncio
async def test_a_continuation_that_ends_without_a_target_lets_the_source_turn_settle() -> None:
    lane = ContinuingLane(frames=scripted_frames(), missing=())
    s = build(lane, hydrator=FakeHydrator(corrupt=True))
    transfer = await s.request_continuation()
    boundary = await s.service.turn(s.request(), s.signals())
    assert not boundary.done
    with pytest.raises(Exception, match="CHECKPOINT_INVALID"):
        await s.drive(transfer.transfer_id)
    failed = await s.wired["store"].get(SCOPE, transfer.transfer_id)
    assert failed is not None and failed.ended and not failed.activated
    resumed = await s.service.turn(s.request(phase="resume", cursor=boundary.cursor), s.signals())
    assert resumed.done and resumed.operation_result is not None
    assert resumed.operation_result["status"] == "completed"
    assert resumed.native.session_ref == SOURCE, "the source generation settled"
    assert lane.sends == [f"{s.identity.harness_execution_id}:1:turn:1"], "never re-sent"
    assert "end_session" in lane.calls


# --- the MP-06 open item: a fenced or ambiguous handover ---------------------------------------


@dataclass
class HandoverLane(ScriptedSessionLane):
    """FIXTURE: the FT-G4 handover path (a hydrated agent offered by the lane itself)."""

    offered: SessionHandover | None = None
    completed: list[str] = field(default_factory=list)

    async def pending_handover(self, harness_execution_id: str) -> SessionHandover | None:
        return self.offered

    async def complete_handover(self, harness_execution_id: str, transfer_id: str) -> None:
        self.completed.append(transfer_id)
        self.offered = None

    def stage_turn(self, harness_execution_id: str, instruction_ref: str, text: str) -> None:
        return None


def _handover(heid: str) -> SessionHandover:
    return SessionHandover(
        transfer_id="t-legacy",
        session=SessionHandle(
            lane_profile="cursor_local",
            harness_execution_id=heid,
            generation=1,
            native_session_ref="agent-fake-2",
        ),
        instruction_ref="continuation:t-legacy",
        source_session_ref=SOURCE,
    )


@pytest.mark.asyncio
async def test_a_handover_the_stop_fence_denies_settles_cancelled() -> None:
    operation = cursor_operation()
    fences = InMemoryStopFenceRepository()
    lane = HandoverLane(frames=scripted_frames())
    s = build(lane, fences=fences)
    lane.offered = _handover(str(s.identity.harness_execution_id))
    await fences.persist(
        StopFence(
            request_scope=operation.request_scope,
            run_id=operation.identity.run_id,
            generation=1,
            command_id="cancel-now",
            reason="operator immediate cancel",
            requested_at=NOW,
        )
    )
    # The first turn's own send is fenced too: nothing reaches the provider at all, so the
    # unit settles cancelled before the handover is even reached.
    first = await s.service.turn(s.request(), s.signals())
    assert first.done and first.operation_result is not None
    assert first.operation_result["status"] == "cancelled"
    assert lane.sends == []


@pytest.mark.asyncio
async def test_a_fence_that_lands_between_the_source_turn_and_the_handover_cancels_the_unit() -> (
    None
):
    operation = cursor_operation()
    fences = InMemoryStopFenceRepository()
    lane = HandoverLane(frames=scripted_frames())
    s = build(lane, fences=fences)
    heid = str(s.identity.harness_execution_id)
    lane.offered = _handover(heid)
    fence = StopFence(
        request_scope=operation.request_scope,
        run_id=operation.identity.run_id,
        generation=1,
        command_id="cancel-now",
        reason="operator immediate cancel",
        requested_at=NOW,
    )

    original_send = lane.send_turn

    async def fenced_after_first(request: Any) -> Any:
        handle = await original_send(request)
        if len(lane.sends) == 1:
            await fences.persist(fence)  # the fence lands while the first turn runs
        return handle

    lane.send_turn = fenced_after_first  # type: ignore[method-assign]
    result = await s.service.turn(s.request(), s.signals())
    assert result.done and result.operation_result is not None
    assert result.operation_result["status"] == "cancelled"
    assert result.closing_facts is not None
    assert result.closing_facts.error_code == "cancelled_by_command"
    assert lane.sends == [f"{heid}:1:turn:1"], "the handover was never sent"
    assert lane.completed == [], "a fenced handover is not completed"
    state = await s.stack.states.load(SCOPE, s.identity.harness_execution_id)
    assert state is not None and state.native_session_ref == SOURCE
    handover_key = f"{heid}:1:continuation:t-legacy"
    record = state.dispatch("send", handover_key)
    assert record is not None and (record.phase, record.reason) == ("declined", "stop_fenced")


@pytest.mark.asyncio
async def test_an_ambiguous_handover_dispatch_still_parks_in_doubt() -> None:
    lane = HandoverLane(frames=scripted_frames())
    s = build(lane)
    heid = str(s.identity.harness_execution_id)
    lane.offered = _handover(heid)
    assert s.operation.cursor_binding is not None
    owner = await s.stack.states.claim_owner(
        SCOPE,
        s.identity.harness_execution_id,
        owner_ref="worker-a",
        generation=1,
        now=NOW,
        lease=timedelta(hours=1),
    )
    # A journaled `intended` handover send from a lost attempt that cannot be reconciled.
    await s.stack.states.intend_dispatch(
        SCOPE,
        s.identity.harness_execution_id,
        DispatchRecord(
            kind="send",
            idempotency_key=f"{heid}:1:continuation:t-legacy",
            expected_generation=1,
            instruction_digest=instruction_digest(
                kind="send",
                instruction_ref="continuation:t-legacy",
                binding_digest=s.operation.cursor_binding.binding_digest,
                turn_no=2,
            ),
            owner_ref=owner.owner_ref,
            owner_epoch=owner.epoch,
            intended_at=NOW,
        ),
        owner=owner,
    )
    result = await s.service.turn(s.request(), s.signals())
    assert result.done and result.operation_result is not None
    assert result.operation_result["status"] == "in_doubt"
    assert result.closing_facts is not None
    assert result.closing_facts.error_code == "native_turn_lost"
    assert lane.sends == [f"{heid}:1:turn:1"], "an ambiguous handover is never re-sent"


def test_fixture_usage_report_is_not_an_occupancy() -> None:
    report = UsageReport(disposition="settled", input_tokens=10, output_tokens=5, total_tokens=15)
    assert ContextOccupancy.unknown("cumulative usage is not occupancy").ratio is None
    assert report.total_tokens == 15
    assert FrameKind.STATUS.value == "status"
