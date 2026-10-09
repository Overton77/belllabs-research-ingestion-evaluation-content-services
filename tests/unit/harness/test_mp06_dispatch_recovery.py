"""MP-06 (OVE-69): fenced session ownership, the native dispatch journal and interventions.

In-memory stores with the production transition rules (`dispatch.py`) and scripted fake
lanes (`tests/fixtures/mp06_lanes.py`, labelled fixtures). The PostgreSQL store and the
real-Temporal crash/race scenarios are in `tests/integration/{postgres,temporal}`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from mission_control.application.execution.harness.describe import (
    CURSOR_LOCAL_DESCRIBE,
    DECLARED_LANE_MATRICES,
)
from mission_control.application.execution.harness.dispatch import (
    DispatchConflict,
    DispatchRecord,
    SessionOwnedElsewhere,
    SessionOwner,
    StaleSessionOwner,
    SteerResult,
    claim_ownership,
    intend,
    resolve,
)
from mission_control.application.execution.harness.inject import resolve_interrupt_mode
from mission_control.application.execution.harness.lane_turns import (
    LaneExecutionIdentity,
    LaneTurnService,
)
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.execution.lane_turns import (
    LANE_COMMAND_SEMANTICS,
    LANE_PAUSE_SEMANTICS,
    ClosingFacts,
    LaneCancelRequest,
    LaneStatusRequest,
    LaneTurnRequest,
    NativeRefs,
    pause_decision_for,
)
from mission_control.domain.execution.lanes import LaneSegmentBounds, TurnHandle
from mission_control.domain.policies.mailbox import MailboxState
from mission_control.domain.policies.stop_fence import EffectAdmission, StopFence
from tests.fixtures.lane_turns import (
    LaneStack,
    RecordingSignals,
    ScriptedSessionLane,
    cursor_operation,
    lane_stack,
    scripted_frames,
)
from tests.fixtures.mp06_lanes import (
    LossyReceiptLane,
    NeverAcceptedLane,
    ReconcilingLossyLane,
)

T0 = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
BOUNDS = LaneSegmentBounds(max_frames=50, heartbeat_timeout_s=3)


@dataclass
class Clock:
    now: datetime = T0

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


def service(
    stack: LaneStack,
    owner: str,
    *,
    clock: Clock | None = None,
    fences: InMemoryStopFenceRepository | None = None,
    **extra: Any,
) -> LaneTurnService:
    return LaneTurnService(
        lanes=stack.service._lanes,
        boundary=stack.boundary,
        frames=stack.frames,
        states=stack.states,
        clock=clock or (lambda: datetime.now(UTC)),
        sessions=WorkerSessionManager(owner_ref=owner, min_lease=timedelta(seconds=1)),
        fences=fences,
        **extra,
    )


def turn_request(operation: OperationExecutionRequest | None = None, **changes: Any) -> Any:
    return LaneTurnRequest.model_validate(
        {
            "operation": operation or cursor_operation(),
            "lane_profile": "cursor_local",
            "generation": 1,
            "segment": BOUNDS,
            **changes,
        }
    )


def identity(operation: OperationExecutionRequest | None = None) -> LaneExecutionIdentity:
    return LaneExecutionIdentity.of(operation or cursor_operation(), "cursor_local", 1)


def send_key(operation: OperationExecutionRequest | None = None) -> str:
    return f"{identity(operation).harness_execution_id}:1:turn:1"


# --- pure transition rules ------------------------------------------------------------------


def _record(owner: SessionOwner, *, digest: str = "sha256:a") -> DispatchRecord:
    return DispatchRecord(
        kind="send",
        idempotency_key="k1",
        expected_generation=1,
        instruction_digest=digest,
        owner_ref=owner.owner_ref,
        owner_epoch=owner.epoch,
        intended_at=T0,
    )


def test_a_live_foreign_lease_refuses_and_an_expired_one_is_taken_over_with_a_new_epoch() -> None:
    lease = timedelta(seconds=10)
    a = claim_ownership(None, owner_ref="a", generation=1, now=T0, lease=lease)
    assert (a.epoch, a.owner_ref) == (1, "a")
    renewed = claim_ownership(a, owner_ref="a", generation=1, now=T0 + lease / 2, lease=lease)
    assert renewed.epoch == 1 and renewed.lease_expires_at > a.lease_expires_at
    with pytest.raises(SessionOwnedElsewhere):
        claim_ownership(renewed, owner_ref="b", generation=1, now=T0 + lease, lease=lease)
    b = claim_ownership(renewed, owner_ref="b", generation=1, now=T0 + 2 * lease, lease=lease)
    assert (b.epoch, b.previous_owner_ref) == (2, "a")
    stale_generation = b.model_copy(update={"generation": 2})
    with pytest.raises(StaleSessionOwner):
        claim_ownership(stale_generation, owner_ref="b", generation=1, now=T0, lease=lease)


def test_an_intended_dispatch_is_never_fresh_again_until_declined_or_not_received() -> None:
    owner = claim_ownership(None, owner_ref="a", generation=1, now=T0, lease=timedelta(seconds=5))
    first = intend(None, _record(owner), owner)
    assert first.fresh and first.record.attempts == 1
    again = intend(first.record, _record(owner), owner)
    assert not again.fresh, "a retry of an unacknowledged send must reconcile, not resend"
    with pytest.raises(DispatchConflict):
        intend(first.record, _record(owner, digest="sha256:b"), owner)
    declined = resolve(first.record, outcome="declined", owner=owner, at=T0, reason="busy")
    retried = intend(declined, _record(owner), owner)
    assert retried.fresh and retried.record.attempts == 2
    acked = resolve(retried.record, outcome="acknowledged", owner=owner, at=T0, native_ref="t1")
    assert not intend(acked, _record(owner), owner).fresh
    with pytest.raises(DispatchConflict):
        resolve(acked, outcome="acknowledged", owner=owner, at=T0, native_ref="t2")
    parked = resolve(
        intend(None, _record(owner), owner).record, outcome="in_doubt", owner=owner, at=T0
    )
    with pytest.raises(DispatchConflict):
        resolve(parked, outcome="not_received", owner=owner, at=T0)


# --- V07: process dies after the native send, before the local receipt ------------------------


async def test_a_lost_receipt_is_reconciled_by_idempotency_key_and_never_resent() -> None:
    lane = ReconcilingLossyLane(frames=scripted_frames(), loss="raise")
    stack = lane_stack(lane)
    turns = service(stack, "worker-a")
    signals = RecordingSignals(stack.frames)
    with pytest.raises(ConnectionError):
        await turns.turn(turn_request(), signals)
    state = await stack.states.load(identity().request_scope, identity().harness_execution_id)
    assert state is not None and state.native_turn_ref is None
    record = state.dispatch("send", send_key())
    assert record is not None and record.phase == "intended"

    result = await turns.turn(turn_request(), signals)  # Temporal's retry of the attempt

    assert result.done and result.operation_result is not None
    assert result.operation_result["status"] == "completed"
    assert lane.native_sends == [send_key()], "one native send; the retry reconciled it"
    assert lane.ledger.accepted == {send_key(): "run-fake-1"}
    assert lane.lookups == [f"send:{send_key()}"]
    state = await stack.states.load(identity().request_scope, identity().harness_execution_id)
    assert state is not None and state.native_turn_ref == "run-fake-1"
    acked = state.dispatch("send", send_key())
    assert acked is not None and acked.phase == "acknowledged" and acked.attempts == 1


async def test_an_unreconcilable_lost_receipt_parks_in_doubt_and_blocks_any_resend() -> None:
    lane = LossyReceiptLane(frames=scripted_frames(), loss="raise")
    stack = lane_stack(lane)
    turns = service(stack, "worker-a")
    signals = RecordingSignals(stack.frames)
    with pytest.raises(ConnectionError):
        await turns.turn(turn_request(), signals)

    parked = await turns.turn(turn_request(), signals)
    again = await turns.turn(turn_request(), signals)
    cancel = await turns.cancel(
        LaneCancelRequest.model_validate(
            {"operation": cursor_operation(), "lane_profile": "cursor_local", "generation": 1}
        )
    )

    assert parked.closing_facts is not None
    assert parked.closing_facts.native_status == "in_doubt"
    assert parked.closing_facts.error_code == "send_dispatch_ambiguous"
    assert parked.operation_result is not None and parked.operation_result["status"] == "in_doubt"
    assert again.closing_facts is not None and again.closing_facts.native_status == "in_doubt"
    assert lane.native_sends == [send_key()], "no blind resend, ever"
    # A cancel cannot claim "never sent" for an ambiguous dispatch.
    assert cancel.receipt.native_status == "unknown" and not cancel.settled
    assert cancel.operation_result is not None
    assert cancel.operation_result["status"] == "in_doubt"


async def test_an_authoritative_not_received_allows_exactly_one_more_send() -> None:
    lane = NeverAcceptedLane(frames=scripted_frames(), loss="raise")
    stack = lane_stack(lane)
    turns = service(stack, "worker-a")
    signals = RecordingSignals(stack.frames)
    with pytest.raises(ConnectionError):
        await turns.turn(turn_request(), signals)
    assert lane.ledger.accepted == {}

    result = await turns.turn(turn_request(), signals)

    assert result.operation_result is not None
    assert result.operation_result["status"] == "completed"
    assert lane.native_sends == [send_key()] and lane.ledger.accepted == {send_key(): "run-fake-1"}
    state = await stack.states.load(identity().request_scope, identity().harness_execution_id)
    assert state is not None
    record = state.dispatch("send", send_key())
    assert record is not None and record.phase == "acknowledged" and record.attempts == 2


# --- V08 and ownership: a resumed segment never resends; a stale owner cannot settle -----------


async def test_a_takeover_fences_the_old_owner_out_of_journal_writes_and_settlement() -> None:
    clock = Clock()
    lane = ScriptedSessionLane(frames=scripted_frames())
    stack = lane_stack(lane)
    a = service(stack, "worker-a", clock=clock)
    b = service(stack, "worker-b", clock=clock)
    small = BOUNDS.model_copy(update={"max_frames": 2})
    first = await a.turn(turn_request(segment=small), RecordingSignals(stack.frames))
    assert not first.done and lane.sends == [send_key()]
    scope, heid = identity().request_scope, identity().harness_execution_id
    live = a.sessions.live(scope, heid)
    assert live is not None and live.turn is not None, "segment end retains the live session"
    a_owner = live.owner

    # B cannot take a live lease; after it expires B takes over with a higher epoch.
    with pytest.raises(SessionOwnedElsewhere):
        await b.turn(
            turn_request(phase="resume", cursor=first.cursor), RecordingSignals(stack.frames)
        )
    clock.advance(a.sessions.lease_for(BOUNDS.heartbeat_timeout_s).total_seconds() + 1)
    b_owner = await b.sessions.claim(
        stack.states, scope, heid, generation=1, now=clock(), heartbeat_timeout_s=3
    )
    assert b_owner.epoch == a_owner.epoch + 1

    # The fenced-out owner can neither journal, nor settle, nor reclaim B's live lease.
    with pytest.raises(StaleSessionOwner):
        await stack.states.intend_dispatch(scope, heid, _record(a_owner), owner=a_owner)
    with pytest.raises(StaleSessionOwner):
        await a._settle(
            turn_request(),
            stack.service._lanes.admit("cursor_local"),  # type: ignore[arg-type]
            identity(),
            TurnHandle(session=live.turn.session, turn_no=1, native_turn_ref="run-fake-1"),
            ClosingFacts(native_status="finished"),
            attempt=None,
            native=NativeRefs(turn_ref="run-fake-1"),
            owner=a_owner,
        )
    assert await stack.boundary.lane_settlement(cursor_operation()) is None
    with pytest.raises(SessionOwnedElsewhere):
        await a.turn(
            turn_request(phase="resume", cursor=first.cursor), RecordingSignals(stack.frames)
        )

    finished = await b.turn(
        turn_request(phase="resume", cursor=first.cursor), RecordingSignals(stack.frames)
    )
    assert finished.done and finished.operation_result is not None
    assert finished.operation_result["status"] == "completed"
    assert lane.sends == [send_key()], "the resumed observation never resent the prompt"
    assert "reattach" in lane.calls
    assert b.sessions.live(scope, heid) is None, "settlement releases the session"


async def test_local_control_from_a_worker_that_does_not_own_the_session_is_refused() -> None:
    lane = ScriptedSessionLane(frames=scripted_frames())
    stack = lane_stack(lane)
    a = service(stack, "worker-a")
    b = service(stack, "worker-b")
    small = BOUNDS.model_copy(update={"max_frames": 2})
    await a.turn(turn_request(segment=small), RecordingSignals(stack.frames))
    payload = {"operation": cursor_operation(), "lane_profile": "cursor_local", "generation": 1}
    with pytest.raises(SessionOwnedElsewhere):
        await b.status(LaneStatusRequest.model_validate(payload))
    with pytest.raises(SessionOwnedElsewhere):
        await b.cancel(LaneCancelRequest.model_validate(payload))
    assert lane.cancels == []
    # The owner itself routes the control to its session.
    observed = await a.status(LaneStatusRequest.model_validate(payload))
    assert observed.status == "running"
    # A provider-hosted session is reachable from any worker.
    hosted = SessionOwner(
        owner_ref="worker-a",
        epoch=1,
        generation=1,
        lease_expires_at=T0 + timedelta(hours=1),
        claimed_at=T0,
    )
    b.sessions.check_control(hosted, placement="cloud", now=T0)


# --- V06: Stop Fence vs governed dispatch -----------------------------------------------------


async def test_a_stop_fence_before_the_first_send_settles_cancelled_without_any_send() -> None:
    lane = ScriptedSessionLane(frames=scripted_frames())
    stack = lane_stack(lane)
    fences = InMemoryStopFenceRepository()
    operation = cursor_operation()
    run_id, scope = operation.identity.run_id, operation.request_scope
    pre = EffectAdmission(request_scope=scope, run_id=run_id, generation=1, effect_ref="call-0")
    assert (await fences.admit_effect(pre)).allowed
    await fences.persist(
        StopFence(
            request_scope=scope,
            run_id=run_id,
            generation=1,
            command_id="cancel-now",
            reason="operator stop",
            requested_at=datetime.now(UTC),
        )
    )
    turns = service(stack, "worker-a", fences=fences)

    result = await turns.turn(turn_request(), RecordingSignals(stack.frames))

    assert lane.sends == [], "a post-fence send is a denied governed effect"
    assert result.operation_result is not None
    assert result.operation_result["status"] == "cancelled"
    post = EffectAdmission(request_scope=scope, run_id=run_id, generation=1, effect_ref="call-1")
    assert not (await fences.admit_effect(post)).allowed
    assert (await fences.admit_effect(pre)).allowed, "a pre-fence effect keeps its disposition"
    state = await stack.states.load(identity().request_scope, identity().harness_execution_id)
    assert state is not None
    # Even the session create is a governed dispatch: no provider session was started.
    created = state.dispatch("create", send_key())
    assert created is not None and (created.phase, created.reason) == ("declined", "stop_fenced")
    assert state.dispatch("send", send_key()) is None and "start" not in lane.calls


async def test_a_cancel_acknowledgement_records_its_own_timestamp() -> None:
    lane = ScriptedSessionLane(frames=scripted_frames())
    stack = lane_stack(lane)
    fences = InMemoryStopFenceRepository()
    operation = cursor_operation()
    turns = service(stack, "worker-a", fences=fences)
    small = BOUNDS.model_copy(update={"max_frames": 2})
    await turns.turn(turn_request(segment=small), RecordingSignals(stack.frames))
    requested = datetime.now(UTC)
    await fences.persist(
        StopFence(
            request_scope=operation.request_scope,
            run_id=operation.identity.run_id,
            generation=1,
            command_id="cancel-now",
            reason="operator stop",
            requested_at=requested,
        )
    )
    cancelled = await turns.cancel(
        LaneCancelRequest.model_validate(
            {
                "operation": operation,
                "lane_profile": "cursor_local",
                "generation": 1,
                "urgency": "immediate",
                "command_id": "cancel-now",
            }
        )
    )
    await fences.record_milestone(
        operation.request_scope, operation.identity.run_id, None, "settled"
    )
    report = await fences.report(operation.request_scope, operation.identity.run_id)
    assert cancelled.settled and report is not None
    assert report.requested_at == requested
    assert report.provider_acknowledged_at is not None and report.settled_at is not None
    assert report.requested_at <= report.fence_persisted_at <= report.provider_acknowledged_at
    assert report.provider_acknowledged_at <= report.settled_at


# --- explicit steering vs cancel-and-replace (per-profile LANE_COMMAND_SEMANTICS) -------------


class _Steers:
    async def steer(self, turn: TurnHandle, *, instruction_ref: str) -> SteerResult:
        return SteerResult(outcome="applied", target_turn_ref=turn.native_turn_ref)


def test_the_interrupt_mode_follows_the_frozen_per_profile_semantics_with_no_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for profile, describe in DECLARED_LANE_MATRICES.items():
        expected = LANE_COMMAND_SEMANTICS["interrupt_and_inject"][profile]
        assert resolve_interrupt_mode(profile, describe, object()) == expected
    assert resolve_interrupt_mode(
        "codex_cloud", DECLARED_LANE_MATRICES["codex_cloud"], _Steers()
    ) == ("unsupported")
    # A describe that disagrees with the frozen table refuses (no silent weaker mode).
    assert (
        resolve_interrupt_mode("cursor_local", DECLARED_LANE_MATRICES["codex_cloud"], object())
        == "unsupported"
    )
    steering = CURSOR_LOCAL_DESCRIBE.model_copy(
        update={
            "delivery_semantics": {
                **CURSOR_LOCAL_DESCRIBE.delivery_semantics,
                "interrupt_and_inject": "cooperative_inject",
            }
        }
    )
    monkeypatch.setitem(
        LANE_COMMAND_SEMANTICS["interrupt_and_inject"], "cursor_local", "cooperative_inject"
    )
    assert resolve_interrupt_mode("cursor_local", steering, object()) == "unsupported"
    assert resolve_interrupt_mode("cursor_local", steering, _Steers()) == "cooperative_inject"
    assert resolve_interrupt_mode("cursor_local", CURSOR_LOCAL_DESCRIBE, _Steers()) == (
        "unsupported"
    )


def test_pause_discloses_boundary_or_tool_gate_and_hosted_cancel_stays_unsupported() -> None:
    for profile, semantics in LANE_PAUSE_SEMANTICS.items():
        running = pause_decision_for(profile, semantics, turn_in_flight=True)
        idle = pause_decision_for(profile, semantics, turn_in_flight=False)
        if semantics == "unsupported":
            assert not running.accepted and running.reason_code == "unsupported_control"
            assert "run boundary" in running.detail
            assert idle.accepted and idle.detail == "applied at the run boundary"
        else:
            assert running.accepted and running.delivery_semantics == semantics
        for decision in (running, idle):
            assert "frozen" not in decision.detail.lower()
    assert LANE_COMMAND_SEMANTICS["cancel"]["claude_cloud"] == "unsupported"
    assert LANE_COMMAND_SEMANTICS["cancel"]["codex_cloud"] == "unsupported"


# --- V05: a steer racing the native turn's completion -----------------------------------------


@dataclass
class _SteeringLane(ScriptedSessionLane):
    """FIXTURE: steers natively; `stale` simulates the turn completing first."""

    stale: bool = True
    steered: list[tuple[str | None, str]] = field(default_factory=list)

    async def steer(self, turn: TurnHandle, *, instruction_ref: str) -> SteerResult:
        self.steered.append((turn.native_turn_ref, instruction_ref))
        self.released.set()
        if self.stale:
            return SteerResult(outcome="stale_target", detail="turn completed first")
        return SteerResult(outcome="applied", target_turn_ref=turn.native_turn_ref)


@pytest.mark.parametrize("stale", [True, False])
async def test_a_steer_lands_on_the_exact_turn_or_is_requeued_as_stale_target(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any, stale: bool
) -> None:
    from tests.fixtures.cursor_controls import FAST, _admitted_unit, _compose

    monkeypatch.setitem(
        LANE_COMMAND_SEMANTICS["interrupt_and_inject"], "cursor_local", "cooperative_inject"
    )
    describe = CURSOR_LOCAL_DESCRIBE.model_copy(
        update={
            "delivery_semantics": {
                **CURSOR_LOCAL_DESCRIBE.delivery_semantics,
                "interrupt_and_inject": "cooperative_inject",
            }
        }
    )
    lane = _SteeringLane(
        frames=scripted_frames(), hold_after=2, describe_matrix=describe, stale=stale
    )
    run_control, repository, run_id, unit, changes = await _admitted_unit((), None)
    operation = OperationExecutionRequest.model_validate(
        {**cursor_operation().model_dump(mode="python"), **changes}
    )
    stack = _compose(
        None,
        lane,
        InMemoryFrameStore(),
        operation,
        run_control=run_control,
        repository=repository,
        run_id=run_id,
        unit=unit,
        settings=FAST,
        profile="cursor_local",
    )
    turns = service(stack.lanes, "worker-a", mailbox=stack.mailbox, injections=stack.injections)
    await stack.command("interrupt_and_inject", "Use release/2.3.")

    result = await turns.turn(turn_request(operation), RecordingSignals(stack.lanes.frames))

    assert result.operation_result is not None
    assert result.operation_result["status"] == "completed"
    assert lane.steered and lane.steered[0][0] == "run-fake-1", "only the active turn is steered"
    assert lane.cancels == [] and len(lane.sends) == 1, "steering never cancels or replaces"
    (entry,) = [e for e in await stack.entries() if e.kind == "interrupt_and_inject"]
    if stale:
        assert len(lane.steered) == 1
        assert entry.state == MailboxState.QUEUED, "requeued for the next boundary, same command"
    else:
        assert entry.state in {MailboxState.CONSUMED, MailboxState.DELIVERED}
