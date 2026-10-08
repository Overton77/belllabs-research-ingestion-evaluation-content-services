"""FT-G2: `lane.turn`, `lane.status` and `lane.cancel` over a scripted Session Lane.

Persist-before-heartbeat, a worker lost between a persisted frame and its heartbeat, segment
bounds, the never-send-twice resume, `wait_then_send`, a lost native turn, the requested
cancel (and only it) reaching the provider, and settlement from closing facts.
"""

from __future__ import annotations

import asyncio

import pytest

from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.execution.harness.registry import LaneNotQualified, LaneRegistry
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import (
    LANE_COMMAND_SEMANTICS,
    LaneCancelRequest,
    LaneSegmentBounds,
    LaneStatusRequest,
    LaneTurnRequest,
)
from tests.fixtures.lane_turns import (
    RecordingSignals,
    ScriptedSessionLane,
    SimulatedWorkerLoss,
    cursor_operation,
    lane_stack,
    scripted_frames,
)


def _turn(**changes: object) -> LaneTurnRequest:
    fields: dict[str, object] = {
        "operation": cursor_operation(),
        "lane_profile": "cursor_local",
        "generation": 1,
        **changes,
    }
    return LaneTurnRequest.model_validate(fields)


def _identity() -> LaneExecutionIdentity:
    return LaneExecutionIdentity.of(cursor_operation(), "cursor_local", 1)


async def test_frames_persist_before_each_heartbeat_and_the_turn_settles_once() -> None:
    stack = lane_stack()
    signals = RecordingSignals(stack.frames, _identity().harness_execution_id)
    result = await stack.service.turn(_turn(), signals)

    assert result.done and result.closing_facts is not None
    assert result.frames_persisted == len(scripted_frames())
    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "completed" and settled.output_text == "done"
    # Every heartbeat names exactly the frames already stored.
    assert signals.beats and all(persisted == stored for _c, persisted, stored in signals.beats)
    assert [beat[0] for beat in signals.beats] == [str(i) for i in range(len(scripted_frames()))]
    assert stack.lane.sends == [f"{_identity().harness_execution_id}:1:turn:1"]
    # Native identity first, then observation; usage upgraded from the provider.
    state = await stack.states.load(_identity().request_scope, _identity().harness_execution_id)
    assert state is not None and state.native_turn_ref == "run-fake-1"
    assert result.closing_facts.usage.disposition == "settled"
    assert result.closing_facts.cost_disposition == "estimated"  # no provider cost reported
    assert "artifact://fake/patch.diff" in settled.output_refs
    assert stack.lane.calls.index("send_turn") < stack.lane.calls.index("observe:None")
    (settlement,) = stack.budget.settlements.values()
    assert settlement[1].amounts == {"tokens.total": 15}, "bound dimensions settle from usage"


async def test_a_worker_lost_between_persist_and_heartbeat_resumes_without_gap_or_duplicate() -> (
    None
):
    stack = lane_stack()
    heid = _identity().harness_execution_id
    crashing = RecordingSignals(stack.frames, heid, fail_on=4)
    with pytest.raises(SimulatedWorkerLoss):
        await stack.service.turn(_turn(), crashing)
    stored_after_crash = len(stack.frames._executions[heid].frames)
    assert stored_after_crash == 4 and crashing.beats[-1][0] == "3"

    # Temporal retries the activity (phase still `start`), with a stale heartbeat hint.
    retry = RecordingSignals(stack.frames, heid, prior="1")
    result = await stack.service.turn(_turn(), retry)

    assert result.done
    frames = stack.frames._executions[heid].frames
    ordinals = sorted(frame.arrival_ordinal for frame in frames.values())
    assert ordinals == list(range(1, len(scripted_frames()) + 1)), "no gap, no duplicate"
    assert len(stack.lane.sends) == 1, "a resume never sends the turn again"
    assert stack.lane.calls.count("start") == 1 and "reattach" in stack.lane.calls
    # The persisted frames (not the heartbeat hint) decided where observation resumed.
    assert "observe:3" in stack.lane.calls


async def test_a_segment_returns_at_its_bound_and_the_next_one_resumes() -> None:
    stack = lane_stack()
    heid = _identity().harness_execution_id
    bounds = LaneSegmentBounds(max_frames=3, max_duration_s=60, start_to_close_s=120)
    first = await stack.service.turn(_turn(segment=bounds), RecordingSignals(stack.frames, heid))
    assert not first.done and first.cursor == "2" and first.frames_persisted == 3

    second = await stack.service.turn(
        _turn(segment=bounds, phase="resume", cursor=first.cursor, segment_no=2),
        RecordingSignals(stack.frames, heid),
    )
    assert not second.done and second.cursor == "5"
    third = await stack.service.turn(
        _turn(segment=bounds, phase="resume", cursor=second.cursor, segment_no=3),
        RecordingSignals(stack.frames, heid),
    )
    assert third.done and len(stack.lane.sends) == 1
    state = await stack.states.load(_identity().request_scope, heid)
    assert state is not None and state.provider_cursor == "8"


async def test_a_silent_provider_ends_the_segment_at_its_duration() -> None:
    lane = ScriptedSessionLane(frames=scripted_frames(), hold_after=2)
    stack = lane_stack(lane)
    heid = _identity().harness_execution_id
    bounds = LaneSegmentBounds(max_duration_s=1, start_to_close_s=5, heartbeat_timeout_s=3)
    result = await stack.service.turn(_turn(segment=bounds), RecordingSignals(stack.frames, heid))
    assert not result.done and result.cursor == "2"
    assert stack.lane.cancels == [], "a segment bound never cancels the provider"


async def test_busy_send_waits_and_sends_nothing() -> None:
    stack = lane_stack(ScriptedSessionLane(frames=scripted_frames(), busy_sends=1))
    result = await stack.service.turn(_turn(), RecordingSignals(stack.frames))
    assert result.busy and not result.done and stack.lane.sends == []
    again = await stack.service.turn(_turn(), RecordingSignals(stack.frames))
    assert again.done and len(stack.lane.sends) == 1


async def test_capacity_exhausted_settles_failed_capacity_without_sending() -> None:
    stack = lane_stack()
    result = await stack.service.turn(
        _turn(capacity_exhausted=True), RecordingSignals(stack.frames)
    )
    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert settled.status == "failed" and settled.failure_code == "capacity"
    assert stack.lane.sends == []


async def test_a_lost_native_turn_is_in_doubt_and_never_resent() -> None:
    stack = lane_stack()
    heid = _identity().harness_execution_id
    bounds = LaneSegmentBounds(max_frames=2, max_duration_s=60, start_to_close_s=120)
    first = await stack.service.turn(_turn(segment=bounds), RecordingSignals(stack.frames, heid))
    assert not first.done
    stack.lane.lose_turn = True
    lost = await stack.service.turn(
        _turn(segment=bounds, phase="resume", cursor=first.cursor, segment_no=2),
        RecordingSignals(stack.frames, heid),
    )
    assert lost.done and lost.closing_facts is not None
    assert lost.closing_facts.native_status == "in_doubt"
    result = OperationExecutionResult.model_validate(lost.operation_result)
    assert result.status == "in_doubt" and result.failure_code == "native_turn_lost"
    assert len(stack.lane.sends) == 1


@pytest.mark.parametrize("requested", [True, False])
async def test_only_a_requested_cancel_reaches_the_provider(requested: bool) -> None:
    lane = ScriptedSessionLane(frames=scripted_frames(), hold_after=1)
    stack = lane_stack(lane)
    signals = RecordingSignals(stack.frames, _identity().harness_execution_id, cancel=requested)
    task = asyncio.create_task(stack.service.turn(_turn(), signals))
    await asyncio.wait_for(lane.held.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # A worker shutdown, pause or reset cancels the task too; only a cancel stops the run.
    assert lane.cancels == (["command"] if requested else [])


async def test_lane_cancel_settles_cancelled_once_the_provider_is_terminal() -> None:
    lane = ScriptedSessionLane(frames=scripted_frames(), hold_after=1)
    stack = lane_stack(lane)
    heid = _identity().harness_execution_id
    task = asyncio.create_task(stack.service.turn(_turn(), RecordingSignals(stack.frames, heid)))
    await asyncio.wait_for(lane.held.wait(), timeout=5)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    payload = {"operation": cursor_operation(), "lane_profile": "cursor_local", "generation": 1}
    status = await stack.service.status(LaneStatusRequest.model_validate(payload))
    assert status.status == "running" and not status.terminal
    cancelled = await stack.service.cancel(LaneCancelRequest.model_validate(payload))
    assert cancelled.settled and cancelled.receipt.acknowledged
    settled = OperationExecutionResult.model_validate(cancelled.operation_result)
    assert settled.status == "cancelled"
    again = await stack.service.cancel(LaneCancelRequest.model_validate(payload))
    assert again.settled and again.receipt.already_terminal, "lane.cancel is idempotent"
    after = await stack.service.status(LaneStatusRequest.model_validate(payload))
    assert after.settled and after.terminal


async def test_cancel_before_any_send_settles_without_provider_work() -> None:
    stack = lane_stack()
    await stack.boundary.admit_lane_session(cursor_operation())
    payload = {"operation": cursor_operation(), "lane_profile": "cursor_local", "generation": 1}
    cancelled = await stack.service.cancel(LaneCancelRequest.model_validate(payload))
    assert cancelled.settled and stack.lane.cancels == []


async def test_in_doubt_after_the_status_bound_records_no_settlement() -> None:
    stack = lane_stack()
    payload = {
        "operation": cursor_operation(),
        "lane_profile": "cursor_local",
        "generation": 1,
        "in_doubt": True,
    }
    outcome = await stack.service.cancel(LaneCancelRequest.model_validate(payload))
    result = OperationExecutionResult.model_validate(outcome.operation_result)
    assert result.status == "in_doubt" and result.failure_code == "lane_status_unresolved"
    assert await stack.boundary.lane_settlement(cursor_operation()) is None


async def test_a_settled_attempt_returns_its_settlement_without_provider_work() -> None:
    stack = lane_stack()
    first = await stack.service.turn(_turn(), RecordingSignals(stack.frames))
    calls = list(stack.lane.calls)
    replay = await stack.service.turn(_turn(), RecordingSignals(stack.frames))
    assert replay.done and replay.operation_result == first.operation_result
    assert stack.lane.calls == calls


async def test_an_unqualified_lane_is_refused_without_policy() -> None:
    stack = lane_stack()
    strict = LaneRegistry([stack.lane], allow_unqualified=False)
    stack.service._lanes = strict
    with pytest.raises(LaneNotQualified):
        await stack.service.turn(_turn(), RecordingSignals(stack.frames))
    assert stack.lane.calls == []


def test_command_semantics_match_every_declared_lane_matrix() -> None:
    for describe in DECLARED_LANE_MATRICES.values():
        for command, semantics in LANE_COMMAND_SEMANTICS.items():
            assert describe.delivery_semantics[command] == semantics


def test_segment_bounds_end_before_the_activity_times_out() -> None:
    with pytest.raises(ValueError, match="start-to-close"):
        LaneSegmentBounds(max_duration_s=60, start_to_close_s=60)


def test_a_turn_names_the_operation_lane() -> None:
    with pytest.raises(ValueError, match="lane profile"):
        _turn(lane_profile="cursor_cloud")
    with pytest.raises(ValueError, match="lane"):
        LaneTurnRequest.model_validate(
            {"operation": cursor_operation(), "lane_profile": "deep_agents", "generation": 1}
        )
