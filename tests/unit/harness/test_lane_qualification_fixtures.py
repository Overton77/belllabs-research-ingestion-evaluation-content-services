"""FT-G6: the Cursor lane qualification fixture suite, replayed offline under `make check`.

Every case of SPEC-07 "Qualification fixtures" for both profiles, replayed through the real
adapter (`CursorLocalHarness` over the replaying bridge, `CursorCloudHarness` over the fake
Cloud Agents API and a bare git remote), `lane.turn` (`LaneTurnService` on a governed in-memory
boundary) and the C2 reducer (`derive`):

| case | cursor_local | cursor_cloud |
| --- | --- | --- |
| full | `full_run.jsonl` | `run_stream.sse` + `run_record.json` |
| error | `error_run.jsonl` | `error_stream.sse` + `error_record.json` |
| cancelled (no command) | `cancelled_run.jsonl` | `cancelled_stream.sse`, `cancelled_record` |
| busy | `AgentBusy` at the send | `409 agent_busy` at a later turn |
| conflict | `run_not_cancellable`, deduped send | `409 agent_id_conflict`, `run_not_cancellable` |
| expired | `expired_run.jsonl` | `410 stream_expired` → `GET .../runs/{id}` |
| hook deny | `hook_deny.jsonl` (kernel hooks) | `hook_deny_stream.sse` (catalog hook in the VM) |

The fixtures are synthetic (hand-authored from research/cursor-platform.md, first line marked
`synthetic`), scrubbed of secrets and e-mail addresses; no network, no credentials, no paid
call. The owner's live drill (`make lane-qualify`) records real ones (docs/qualification/lanes).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.cursor.frames import scrub
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.frames.reducer import DeriveContext, derive
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import LaneCancelRequest, LaneTurnRequest
from mission_control.domain.execution.lanes import CancelTurnRequest, SendTurnRequest, TurnHandle
from mission_control.domain.frames.contracts import LaneProfile
from mission_control.domain.frames.facts import (
    ExecutionOutcomeFact,
    ToolEffectFact,
    TurnCompletedFact,
)
from tests.fixtures.cursor_cloud import cloud_stack
from tests.fixtures.cursor_controls import harness_fields
from tests.fixtures.cursor_local import local_stack
from tests.fixtures.lane_turns import RecordingSignals, lane_stack

FIXTURES = Path(__file__).resolve().parents[2] / "integration" / "cursor" / "fixtures"
SECRET = "sk-fixture-secret"

# case -> (fixture, launcher changes, settled status, failure code, native status)
LOCAL = {
    "full": ("full_run", {}, "completed", None, "finished"),
    "error": ("error_run", {}, "failed", "provider_error", "error"),
    "cancelled": ("cancelled_run", {}, "failed", "infrastructure", "cancelled"),
    "expired": ("expired_run", {}, "failed", "timeout", "expired"),
    "hook_deny": ("hook_deny", {}, "completed", None, "finished"),
}
# case -> (stream, record, api changes, settled status, failure code, native status)
CLOUD = {
    "full": ("run_stream", "run_record", {}, "completed", None, "finished"),
    "error": ("error_stream", "error_record", {}, "failed", "provider_error", "error"),
    "cancelled": (
        "cancelled_stream",
        "cancelled_record",
        {},
        "failed",
        "infrastructure",
        "cancelled",
    ),
    "expired_stream": (
        "run_stream",
        "run_record",
        {"cut_after": "4", "expire_after": "4"},
        "completed",
        None,
        "finished",
    ),
    "hook_deny": ("hook_deny_stream", "run_record", {}, "completed", None, "finished"),
}


def _request(operation: Any, profile: str, **changes: Any) -> LaneTurnRequest:
    return LaneTurnRequest.model_validate(
        {"operation": operation, "lane_profile": profile, "generation": 1, **changes}
    )


def _frames(lanes: Any, operation: Any, profile: str) -> list[Any]:
    heid = LaneExecutionIdentity.of(operation, profile, 1).harness_execution_id
    return sorted(
        lanes.frames._executions[heid].frames.values(), key=lambda frame: frame.arrival_ordinal
    )


async def _until_done(lanes: Any, operation: Any, profile: str) -> Any:
    result = await lanes.service.turn(_request(operation, profile), RecordingSignals(lanes.frames))
    segment = 1
    while not result.done:
        segment += 1
        assert segment < 10, "the fixture never closed"
        result = await lanes.service.turn(
            _request(operation, profile, phase="resume", cursor=result.cursor, segment_no=segment),
            RecordingSignals(lanes.frames),
        )
    return result


def _common_assertions(lanes: Any, operation: Any, profile: str) -> list[Any]:
    frames = _frames(lanes, operation, profile)
    assert len({frame.provider_key for frame in frames}) == len(frames), "a frame stored twice"
    assert all(SECRET not in frame.body_excerpt for frame in frames)
    assert all("@example" not in frame.body_excerpt for frame in frames)
    return frames


# --- fixture hygiene ---------------------------------------------------------------------------


def test_every_committed_fixture_is_synthetic_scrubbed_and_secret_free() -> None:
    paths = sorted(FIXTURES.rglob("*.*"))
    assert {path.suffix for path in paths} == {".jsonl", ".sse", ".json"}
    for path in paths:
        text = path.read_text("utf-8")
        first = text.splitlines()[0]
        # Hand-authored fixtures say `synthetic`; a live drill's scrubbed ones say `recorded`.
        marked = "synthetic" in first or "recorded" in first
        assert marked or json.loads(text).get("synthetic") is True, path.name
        assert "@" not in text.replace("@localhost", ""), f"an e-mail-like value in {path.name}"
        for marker in ("sk-", "cursor_api_key", "Bearer ", "CURSOR_API_KEY="):
            assert marker not in text, f"{marker!r} in {path.name}"
        if path.suffix == ".jsonl":
            records = [json.loads(line) for line in text.splitlines() if line.strip()]
            assert scrub(records, secrets=(SECRET,)) == records, path.name


# --- cursor_local -----------------------------------------------------------------------------


@pytest.mark.parametrize("case", sorted(LOCAL))
async def test_cursor_local_fixture_replays_through_lane_turn(tmp_path: Path, case: str) -> None:
    fixture, launcher, status, failure, native = LOCAL[case]
    stack = local_stack(tmp_path, fixture, launcher_changes=launcher)
    lanes = lane_stack(stack.harness, frames=stack.frames, operation=stack.operation)
    result = await _until_done(lanes, stack.operation, "cursor_local")

    assert result.closing_facts is not None
    assert result.closing_facts.native_status == native
    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert (settled.status, settled.failure_code) == (status, failure)
    frames = _common_assertions(lanes, stack.operation, "cursor_local")
    facts = derive(
        [frame for frame in frames if frame.closing],
        DeriveContext(lane=LaneProfile.CURSOR_LOCAL, current_generation=1),
    )
    kinds = {type(fact) for fact in facts}
    assert ToolEffectFact in kinds
    if case in {"error", "cancelled", "expired"}:
        assert ExecutionOutcomeFact in kinds
    if case == "hook_deny":
        refused = {(event, item.decision.value) for event, _x, item in stack.launcher.hook_results}
        assert ("preToolUse", "deny") in refused
        assert stack.intents.intents() == ()
    assert len(stack.launcher.sends) == 1


async def test_cursor_local_busy_waits_then_sends_once(tmp_path: Path) -> None:
    stack = local_stack(tmp_path, launcher_changes={"busy_sends": 1})
    lanes = lane_stack(stack.harness, frames=stack.frames, operation=stack.operation)
    busy = await lanes.service.turn(
        _request(stack.operation, "cursor_local"), RecordingSignals(lanes.frames)
    )
    assert busy.busy and not busy.done and stack.launcher.sends == []
    done = await lanes.service.turn(
        _request(stack.operation, "cursor_local"), RecordingSignals(lanes.frames)
    )
    assert done.done and len(stack.launcher.sends) == 1
    # Past the wait bound the attempt fails `capacity` without a send.
    other = local_stack(tmp_path / "bound", launcher_changes={"busy_sends": 5})
    bound = lane_stack(other.harness, frames=other.frames, operation=other.operation)
    exhausted = await bound.service.turn(
        _request(other.operation, "cursor_local", capacity_exhausted=True),
        RecordingSignals(bound.frames),
    )
    settled = OperationExecutionResult.model_validate(exhausted.operation_result)
    assert (settled.status, settled.failure_code) == ("failed", "capacity")
    assert other.launcher.sends == []


async def test_cursor_local_conflicts_are_idempotent(tmp_path: Path) -> None:
    stack = local_stack(tmp_path)
    lanes = lane_stack(stack.harness, frames=stack.frames, operation=stack.operation)
    await _until_done(lanes, stack.operation, "cursor_local")
    # `lane.cancel` on a settled unit: no provider call, the settlement stands.
    receipt = await lanes.service.cancel(
        LaneCancelRequest.model_validate(
            {"operation": stack.operation, "lane_profile": "cursor_local", "generation": 1}
        )
    )
    assert receipt.receipt.already_terminal and stack.launcher.cancel_calls == 0
    # The bridge deduplicates a repeated send by its idempotency key (a lost response).
    bridge = await stack.launcher.launch(workspace=tmp_path, state_root=tmp_path / "state")
    first = await bridge.send("agent", "text", idempotency_key="k-1")
    again = await bridge.send("agent", "text", idempotency_key="k-1")
    assert first == again and stack.launcher.sends.count(("k-1", "text")) == 1


# --- cursor_cloud -----------------------------------------------------------------------------


@pytest.mark.parametrize("case", sorted(CLOUD))
async def test_cursor_cloud_fixture_replays_through_lane_turn(tmp_path: Path, case: str) -> None:
    stream, record, api, status, failure, native = CLOUD[case]
    stack = cloud_stack(tmp_path, stream=stream, record=record, api_changes=dict(api))
    lanes = lane_stack(stack.harness, operation=stack.operation)
    result = await _until_done(lanes, stack.operation, "cursor_cloud")

    assert result.closing_facts is not None
    assert result.closing_facts.native_status == native
    settled = OperationExecutionResult.model_validate(result.operation_result)
    assert (settled.status, settled.failure_code) == (status, failure)
    frames = _common_assertions(lanes, stack.operation, "cursor_cloud")
    facts = derive(
        [frame for frame in frames if frame.closing],
        DeriveContext(lane=LaneProfile.CURSOR_CLOUD, current_generation=1),
    )
    kinds = {type(fact) for fact in facts}
    if case != "cancelled":
        assert ToolEffectFact in kinds or TurnCompletedFact in kinds
    if case == "expired_stream":
        # Resumed with the run-scoped Last-Event-ID, then the run record after `410`.
        assert stack.api.stream_requests == [None, "4"]
        assert any(frame.raw_kind == "run.final" for frame in frames)
    if case == "hook_deny":
        failed = [
            fact
            for fact in facts
            if isinstance(fact, ToolEffectFact) and fact.tool_call_ref == "call-d1"
        ]
        assert failed and failed[0].status == "failed"
    # One agent and one run were created; the agent was archived after the artifacts.
    assert len(stack.api.creates) == 1 and stack.api.archived


async def test_cursor_cloud_busy_is_wait_then_send_and_capacity_after_the_bound(
    tmp_path: Path,
) -> None:
    stack = cloud_stack(tmp_path, api_changes={"busy": True})
    lanes = lane_stack(stack.harness, operation=stack.operation)
    await _until_done(lanes, stack.operation, "cursor_cloud")
    # The unit settled; a later turn on the same agent meets `409 agent_busy`: nothing sent.
    stack2 = cloud_stack(tmp_path / "two", api_changes={"busy": True})
    harness = stack2.harness
    heid = str(LaneExecutionIdentity.of(stack2.operation, "cursor_cloud", 1).harness_execution_id)
    from tests.fixtures.cursor_controls import started_session

    _heid, session = await started_session(stack2)
    assert _heid == heid
    busy = await harness.send_turn(
        SendTurnRequest(
            **harness_fields(stack2.operation, heid, "cursor_cloud"),
            session=session,
            turn_no=2,
            instruction_ref="turn-2",
        )
    )
    assert busy.status == "busy" and busy.native_turn_ref is None
    capacity = lane_stack(harness, operation=stack2.operation)
    exhausted = await capacity.service.turn(
        _request(stack2.operation, "cursor_cloud", capacity_exhausted=True),
        RecordingSignals(capacity.frames),
    )
    settled = OperationExecutionResult.model_validate(exhausted.operation_result)
    assert (settled.status, settled.failure_code) == ("failed", "capacity")


async def test_cursor_cloud_conflicts_reattach_and_never_double_cancel(tmp_path: Path) -> None:
    from tests.fixtures.cursor_controls import started_session

    stack = cloud_stack(tmp_path)
    heid, session = await started_session(stack)
    # A retried create meets `409 agent_id_conflict` and reattaches to the same agent.
    from mission_control.domain.execution.lanes import PreparedSession, StartRequest

    fields = harness_fields(stack.operation, heid, "cursor_cloud")
    again = await stack.harness.start(
        StartRequest(
            **fields,
            prepared=PreparedSession(
                lane_profile="cursor_cloud", harness_execution_id=heid, generation=1
            ),
        )
    )
    assert again.native_session_ref == session.native_session_ref
    assert len(stack.api.agents) == 1 and len(stack.api.creates) == 2
    turn = TurnHandle(session=session, turn_no=1, native_turn_ref=stack.api.run_id)
    first = await stack.harness.cancel_turn(CancelTurnRequest(**fields, turn=turn, reason="cmd"))
    second = await stack.harness.cancel_turn(CancelTurnRequest(**fields, turn=turn, reason="cmd"))
    assert first.acknowledged and not first.already_terminal
    assert second.acknowledged and second.already_terminal  # `409 run_not_cancellable`
    assert stack.api.cancelled == [stack.api.run_id]
