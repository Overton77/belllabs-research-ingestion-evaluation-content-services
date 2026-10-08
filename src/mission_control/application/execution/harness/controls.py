"""Session-lane controls beside the turn (SPEC-07 section 7, SPEC-06; FT-G4).

What `lane.turn` needs to synthesize the SPEC-07 section 7 controls on a Session Lane that has
no native steer, pause or fork (Cursor):

- **Uncertain effects** (`open_tool_calls`): a `tool_call{status: running}` frame of the
  interrupted turn without a completion (a closing tool frame, or an `after_tool` /
  `after_tool_failure` hook for the same call) is an Uncertain Effect; `cancel_and_replace`
  never sends the replacement turn while one is open.
- **Usage of a cancelled turn** (`usage_from_frames`): the last `TurnEndedUpdate` (or `usage`)
  frame of the turn, recorded as `estimated` (never zero when the provider reported tokens).
- **Pause** (`pause_decision`): a lane whose describe says `pause: unsupported` rejects a pause
  while a turn runs (a typed `unsupported_control` rejection) and applies it at the run
  boundary only (no new segment starts until resume).
- Optional lane capabilities: `TurnTextStaging` (the text of a turn the lane sends later,
  addressed by its instruction ref: replacement and continuation turns) and `SessionHandover`
  (a continuation hydrated a fresh agent; the next turn goes to it).

Pure application code: no provider SDK, no Temporal.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

from mission_control.domain.execution.lane_turns import (
    UNSUPPORTED_CONTROL,
    ControlDecision,
    pause_decision_for,
)
from mission_control.domain.execution.lanes import LaneDescribe, SessionHandle, UsageReport
from mission_control.domain.frames.body import frame_body_object
from mission_control.domain.frames.contracts import FrameKind, ProviderFrame

_STARTED = frozenset({FrameKind.TOOL_CALL_STARTED})
_CLOSED = frozenset({FrameKind.TOOL_CALL_COMPLETED, FrameKind.TOOL_CALL_FAILED})
_CLOSING_HOOK_EVENTS = frozenset({"after_tool", "after_tool_failure", "after_shell"})
_USAGE_KINDS = frozenset({FrameKind.TURN_ENDED, FrameKind.USAGE})


def _body(frame: ProviderFrame) -> dict[str, Any]:
    return frame_body_object(frame.body_excerpt, frame.body_bytes)


def open_tool_calls(frames: Iterable[ProviderFrame], turn_ref: str | None) -> tuple[str, ...]:
    """Tool calls the turn `turn_ref` started and nothing closed (Uncertain Effects)."""

    started: dict[str, None] = {}
    closed: set[str] = set()
    for frame in frames:
        call = frame.tool_call_ref
        if call is None:
            continue
        if frame.kind in _STARTED:
            if turn_ref is None or frame.native_turn_ref in {None, turn_ref}:
                started.setdefault(call, None)
        elif frame.kind in _CLOSED:
            closed.add(call)
        elif frame.kind == FrameKind.HOOK_RESULT:
            if _body(frame).get("event") in _CLOSING_HOOK_EVENTS:
                closed.add(call)
    return tuple(call for call in started if call not in closed)


def _tokens(usage: Mapping[str, Any]) -> tuple[int, int, int]:
    def read(*keys: str) -> int:
        for key in keys:
            value = usage.get(key)
            if isinstance(value, int | float):
                return int(value)
        return 0

    inputs = read("input_tokens", "inputTokens")
    outputs = read("output_tokens", "outputTokens")
    total = read("total_tokens", "totalTokens") or inputs + outputs
    return inputs, outputs, total


def usage_from_frames(frames: Iterable[ProviderFrame], turn_ref: str | None) -> UsageReport:
    """The last reported usage of the turn (`TurnEndedUpdate`, `usage`), as `estimated`."""

    last: Mapping[str, Any] | None = None
    for frame in frames:
        if frame.kind not in _USAGE_KINDS:
            continue
        if turn_ref is not None and frame.native_turn_ref not in {None, turn_ref}:
            continue
        body = _body(frame)
        usage = body.get("usage") if isinstance(body.get("usage"), dict) else body
        if isinstance(usage, dict) and any(_tokens(usage)):
            last = usage
    if last is None:
        return UsageReport(disposition="unknown")
    inputs, outputs, total = _tokens(last)
    return UsageReport(
        disposition="estimated", input_tokens=inputs, output_tokens=outputs, total_tokens=total
    )


def pause_decision(describe: LaneDescribe, *, turn_in_flight: bool) -> ControlDecision:
    """SPEC-07 section 7 `pause` for a lane's describe (see `pause_decision_for`)."""

    semantics = describe.delivery_semantics.get("pause", "unsupported")
    if describe.controls.get("pause") not in {"native", "emulated"}:
        semantics = "unsupported"
    return pause_decision_for(describe.lane_profile, semantics, turn_in_flight=turn_in_flight)


ControlCell = Literal["native", "emulated", "unsupported", "unqualified"]


@runtime_checkable
class TurnTextStaging(Protocol):
    """A lane that sends a later turn's text by reference (replacement, continuation)."""

    def stage_turn(self, harness_execution_id: str, instruction_ref: str, text: str) -> None: ...


@dataclass(frozen=True)
class SessionHandover:
    """A continuation hydrated a fresh agent: the next turn is sent to `session`."""

    transfer_id: str
    session: SessionHandle
    instruction_ref: str
    source_session_ref: str


@runtime_checkable
class SessionHandoverLane(Protocol):
    async def pending_handover(self, harness_execution_id: str) -> SessionHandover | None: ...

    async def complete_handover(self, harness_execution_id: str, transfer_id: str) -> None: ...


__all__ = [
    "UNSUPPORTED_CONTROL",
    "ControlCell",
    "ControlDecision",
    "SessionHandover",
    "SessionHandoverLane",
    "TurnTextStaging",
    "open_tool_calls",
    "pause_decision",
    "usage_from_frames",
]
