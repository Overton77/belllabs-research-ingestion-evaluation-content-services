"""Per-stream cursors and envelopes over the frozen `mc.stream_envelope.v1` (SPEC-04).

Each stream keeps its own cursor domain and positions are never compared across streams:

- `mission_events`: `position = str(mission_seq)` (the reducer journal's per-mission seq);
- `provider_frames`: `position = "<harness_execution_id>:<arrival_ordinal>"` with the
  execution generation in `StreamCursor.generation`; a cursor from an older generation is
  `STALE_GENERATION` (the Native Event Store rejects stale writers the same way).

Parsing a client cursor checks it against the stream, target execution and the server's
high-watermark, and fails with a `STREAM_ERROR_CODES` code rather than guessing. Provider
frame envelopes carry the frame's `SubordinateRef` when lineage knows the child, so a
dashboard can group provider subagents without reading message text.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from uuid import UUID

from mission_control.application.frames.lineage import SubordinateNode
from mission_control.application.frames.provider_mapping import (
    CLAUDE_TASK_ID_PREFIX,
    CLAUDE_TASK_PREFIX,
    CODEX_THREAD_PREFIX,
)
from mission_control.domain.frames.contracts import FrameKind, ProviderFrame, native_event_ref
from mission_control.domain.subscriptions.streams import (
    StreamCursor,
    StreamEnvelope,
    StreamErrorCode,
    StreamFilters,
    StreamScope,
    SubordinateRef,
)

DELTA_KINDS = frozenset(
    {FrameKind.MESSAGE_DELTA, FrameKind.THINKING_DELTA, FrameKind.TOOL_CALL_DELTA}
)
_TOOL_KINDS = frozenset(
    {
        FrameKind.TOOL_CALL_STARTED,
        FrameKind.TOOL_CALL_DELTA,
        FrameKind.TOOL_CALL_COMPLETED,
        FrameKind.TOOL_CALL_FAILED,
    }
)


class StreamCursorError(ValueError):
    def __init__(self, code: StreamErrorCode, detail: str) -> None:
        super().__init__(detail)
        self.code: StreamErrorCode = code
        self.detail = detail


@dataclass(frozen=True)
class FramePosition:
    harness_execution_id: UUID
    generation: int
    arrival_ordinal: int


def frame_cursor(frame: ProviderFrame) -> StreamCursor:
    return StreamCursor(
        stream="provider_frames",
        position=f"{frame.harness_execution_id}:{frame.arrival_ordinal}",
        generation=frame.generation,
    )


def parse_frame_cursor(
    cursor: StreamCursor,
    *,
    harness_execution_id: UUID,
    current_generation: int,
    high_watermark: int,
) -> FramePosition:
    """A client's provider-frame cursor for one execution, or a stream error code."""

    if cursor.stream != "provider_frames":
        raise StreamCursorError("SCOPE_MISMATCH", "cursor belongs to another stream")
    execution, _sep, ordinal = cursor.position.rpartition(":")
    try:
        execution_id = UUID(execution)
        position = int(ordinal)
    except ValueError as error:
        raise StreamCursorError("CURSOR_EXPIRED", "unreadable provider frame cursor") from error
    if execution_id != harness_execution_id:
        raise StreamCursorError("SCOPE_MISMATCH", "cursor names another execution")
    if cursor.generation is None or cursor.generation < current_generation:
        raise StreamCursorError("STALE_GENERATION", "cursor is from an older generation")
    if cursor.generation > current_generation or position > high_watermark:
        raise StreamCursorError("CURSOR_AHEAD", "cursor is past the server high-watermark")
    if position < 0:
        raise StreamCursorError("CURSOR_EXPIRED", "negative provider frame cursor")
    return FramePosition(execution_id, cursor.generation, position)


def mission_cursor(seq: int) -> StreamCursor:
    return StreamCursor(stream="mission_events", position=str(seq))


def parse_mission_cursor(cursor: StreamCursor, *, high_watermark: int) -> int:
    if cursor.stream != "mission_events":
        raise StreamCursorError("SCOPE_MISMATCH", "cursor belongs to another stream")
    try:
        seq = int(cursor.position)
    except ValueError as error:
        raise StreamCursorError("CURSOR_EXPIRED", "unreadable mission event cursor") from error
    if seq < 0:
        raise StreamCursorError("CURSOR_EXPIRED", "negative mission event cursor")
    if seq > high_watermark:
        raise StreamCursorError("CURSOR_AHEAD", "cursor is past the server high-watermark")
    return seq


def frame_passes(frame: ProviderFrame, filters: StreamFilters) -> bool:
    if filters.exclude_deltas and frame.kind in DELTA_KINDS:
        return False
    if filters.tool_detail == "none" and frame.kind in _TOOL_KINDS:
        return False
    return not filters.kinds or frame.kind.value in filters.kinds


def subordinate_index(
    nodes: tuple[SubordinateNode, ...],
) -> dict[tuple[str, int, str], SubordinateRef]:
    """(execution ref, generation, frame `subordinate_ref`) -> the lineage ref."""

    index: dict[tuple[str, int, str], SubordinateRef] = {}
    for node in nodes:
        ref = node.ref
        if ref.kind != "provider_subagent" or ref.generation is None:
            continue
        keys = {
            CODEX_THREAD_PREFIX + (ref.native_child_ref or ""),
            CLAUDE_TASK_PREFIX + (ref.spawn_correlation or ""),
            CLAUDE_TASK_ID_PREFIX + (ref.native_child_ref or ""),
            ref.native_child_ref or "",
        }
        for key in keys - {"", CODEX_THREAD_PREFIX, CLAUDE_TASK_PREFIX, CLAUDE_TASK_ID_PREFIX}:
            index.setdefault((ref.parent_execution_ref, ref.generation, key), ref)
    return index


def frame_envelope(
    frame: ProviderFrame,
    *,
    scope: StreamScope,
    mission_ref: str | None = None,
    run_ref: str | None = None,
    subordinates: Mapping[tuple[str, int, str], SubordinateRef] | None = None,
    tool_detail: str = "summary",
) -> StreamEnvelope:
    """One provider frame as a `provider_frames` envelope (summary inline, body by ref)."""

    execution_ref = f"harness_execution:{frame.harness_execution_id}"
    subordinate = None
    if frame.subordinate_ref is not None:
        subordinate = (subordinates or {}).get(
            (execution_ref, frame.generation, frame.subordinate_ref)
        ) or SubordinateRef(
            kind="provider_subagent",
            parent_execution_ref=execution_ref,
            native_parent_ref=frame.native_session_ref,
            native_child_ref=frame.subordinate_ref,
            generation=frame.generation,
        )
    payload: dict[str, object] = {
        "raw_kind": frame.raw_kind,
        "closing": frame.closing,
        "native_event_ref": native_event_ref(frame.frame_id),
        "body_digest": frame.body_digest,
        "body_bytes": frame.body_bytes,
        "native_turn_ref": frame.native_turn_ref,
        "tool_call_ref": frame.tool_call_ref,
    }
    if tool_detail == "full" or frame.kind not in _TOOL_KINDS:
        payload["body_excerpt"] = frame.body_excerpt
    observed = frame.observed_at.isoformat()
    return StreamEnvelope(
        stream="provider_frames",
        event_id=str(frame.frame_id),
        scope=scope,
        mission_ref=mission_ref,
        run_ref=run_ref or f"run:{frame.run_id}",
        execution_ref=execution_ref,
        generation=frame.generation,
        cursor=frame_cursor(frame),
        occurred_at=(frame.provider_timestamp or frame.observed_at).isoformat(),
        recorded_at=observed,
        kind=frame.kind.value,
        payload=payload,
        correlation_id=frame.native_turn_ref,
        subordinate_ref=subordinate,
    )


__all__ = [
    "DELTA_KINDS",
    "FramePosition",
    "StreamCursorError",
    "frame_cursor",
    "frame_envelope",
    "frame_passes",
    "mission_cursor",
    "parse_frame_cursor",
    "parse_mission_cursor",
    "subordinate_index",
]
