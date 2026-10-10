"""Common lifecycle projection over provider frames (SPEC-04 "Streams and envelopes").

Every provider frame keeps its native `raw_kind` and bounded original payload; this module
adds only the small stable vocabulary a dashboard can rely on across lanes:

| normalized fact         | from                                                     |
| ----------------------- | -------------------------------------------------------- |
| `execution.started`     | the parent's `session_init`                              |
| `execution.ended`       | the parent's `run_result`                                |
| `tool.started`          | `tool_call_started`                                      |
| `tool.completed`        | `tool_call_completed`                                    |
| `tool.failed`           | `tool_call_failed`                                       |
| `approval.pending`      | `approval_requested`                                     |
| `approval.resolved`     | `approval_resolved`                                      |
| `compaction.observed`   | `after_compaction`                                       |
| `usage`                 | `usage`                                                  |
| `subordinate.started`   | lineage: the first frame of a child (complete lineage)   |
| `subordinate.ended`     | lineage: the child's own end evidence                    |

A frame of a provider subagent never yields `execution.*`: a child's session or result is
not the parent execution's. `unknown`, delta, status, heartbeat and hook frames yield no
normalized fact; they stay visible with their native kind. The projection is a read view: it
never changes which frames are closing and never feeds the reducer.

Usage attribution tells a consumer how a `usage` frame relates to the parent's authoritative
turn totals so a recursive view never double counts (`lineage.usage_rule`): a parent frame is
counted in the parent turn; a subordinate frame is either already folded into the parent turn
(`folded`) or not attributable to any total (`unattributable`; Claude reports subagent tokens
only inside provider-inclusive cost), and in neither case may it be added again.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Final

from mission_control.application.frames.lineage import counts_usage_frame, usage_rule
from mission_control.application.frames.lineage_stream import Transition
from mission_control.domain.frames.contracts import FrameKind, ProviderFrame

PROJECTION_VERSION: Final = "mc.frame_projection.v1"

_ANY_FRAME: Final[dict[FrameKind, str]] = {
    FrameKind.TOOL_CALL_STARTED: "tool.started",
    FrameKind.TOOL_CALL_COMPLETED: "tool.completed",
    FrameKind.TOOL_CALL_FAILED: "tool.failed",
    FrameKind.APPROVAL_REQUESTED: "approval.pending",
    FrameKind.APPROVAL_RESOLVED: "approval.resolved",
    FrameKind.AFTER_COMPACTION: "compaction.observed",
    FrameKind.USAGE: "usage",
}
_PARENT_ONLY: Final[dict[FrameKind, str]] = {
    FrameKind.SESSION_INIT: "execution.started",
    FrameKind.RUN_RESULT: "execution.ended",
}
_TRANSITIONS: Final[dict[str, str]] = {
    "started": "subordinate.started",
    "ended": "subordinate.ended",
}
NORMALIZED_FACTS: Final[tuple[str, ...]] = (
    *_PARENT_ONLY.values(),
    *_ANY_FRAME.values(),
    *_TRANSITIONS.values(),
)


def normalized_facts(
    frame: ProviderFrame, transitions: Iterable[Transition] = ()
) -> tuple[str, ...]:
    facts: list[str] = []
    if frame.subordinate_ref is None and frame.kind in _PARENT_ONLY:
        facts.append(_PARENT_ONLY[frame.kind])
    if frame.kind in _ANY_FRAME:
        facts.append(_ANY_FRAME[frame.kind])
    if frame.subordinate_ref is not None:
        facts.extend(_TRANSITIONS[item] for item in transitions if item in _TRANSITIONS)
    return tuple(facts)


def usage_attribution(frame: ProviderFrame) -> dict[str, Any] | None:
    """How a `usage` frame relates to the parent's turn totals (None for other kinds)."""

    if frame.kind != FrameKind.USAGE:
        return None
    if frame.subordinate_ref is None:
        return {"scope": "parent", "counted_in_parent_turn": True, "add_to_parent": False}
    rule = usage_rule(frame.lane_profile, "provider_subagent")
    return {
        "scope": "subordinate",
        "tokens": rule.tokens,
        "cost": rule.cost,
        "counted_in_parent_turn": counts_usage_frame(frame.lane_profile, frame.subordinate_ref),
        # Folded usage is already in the parent turn; unattributable usage is in no total
        # (or only inside a provider-inclusive cost). Either way, never add it again.
        "add_to_parent": False,
    }


def projection(frame: ProviderFrame, transitions: Iterable[Transition] = ()) -> dict[str, Any]:
    """The payload additions of one frame envelope."""

    body: dict[str, Any] = {
        "projection": PROJECTION_VERSION,
        "normalized": list(normalized_facts(frame, transitions)),
        "known_kind": frame.kind != FrameKind.UNKNOWN,
    }
    usage = usage_attribution(frame)
    if usage is not None:
        body["usage_attribution"] = usage
    return body


__all__ = [
    "NORMALIZED_FACTS",
    "PROJECTION_VERSION",
    "normalized_facts",
    "projection",
    "usage_attribution",
]
