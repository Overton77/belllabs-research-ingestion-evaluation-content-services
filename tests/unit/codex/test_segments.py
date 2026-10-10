"""MP-08 failure B and unresolved 7: segment cursors over the FIXTURE app-server.

- One app-server event can yield several frames (`turn/completed` -> TURN_ENDED and the
  terminal RUN_RESULT). `LaneTurnService` may end a segment between them (`max_frames`); the
  next segment must re-read the event, never skip the rest of it (the livelock the recovery
  session reproduced on real Temporal + PostgreSQL). Every bound from 1 frame up completes
  with exactly one RUN_RESULT.
- A turn already finished whose completion the cursor passed resynchronizes from history
  instead of waiting on a stream that will never deliver it again.
- A new turn's first observation starts at its own `turn/start`, not at the start of the
  connection's buffer (earlier turns' frames are not re-emitted to consume the budget).
- A history resync after a relaunch pages with history cursors under a tiny frame budget.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mission_control.adapters.codex import frames as framing
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lanes import LaneSegmentBounds
from mission_control.domain.frames.contracts import FrameKind
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.codex.drive import drive_segments, observed, sent, started
from tests.unit.codex.support import codex_stack


def _bounds(max_frames: int) -> LaneSegmentBounds:
    return LaneSegmentBounds(
        max_frames=max_frames, max_duration_s=5, start_to_close_s=60, heartbeat_timeout_s=3
    )


@pytest.mark.parametrize("max_frames", list(range(1, 15)))
async def test_every_frame_bound_completes_with_exactly_one_run_result(
    tmp_path: Path, max_frames: int
) -> None:
    stack = codex_stack(tmp_path, "turn_full")
    result, results = await asyncio.wait_for(
        drive_segments(stack, _bounds(max_frames), limit=40), timeout=120
    )
    assert result.done, f"max_frames={max_frames}: not settled after {len(results)} segments"
    assert OperationExecutionResult.model_validate(result.operation_result).status == "completed"
    frames = stack.frames()
    assert sum(frame.kind == FrameKind.RUN_RESULT for frame in frames) == 1
    assert sum(frame.kind == FrameKind.TURN_ENDED for frame in frames) == 1
    assert len({frame.provider_key for frame in frames}) == len(frames), "no duplicate stored"
    server = stack.launcher.server
    assert [m for m, _p in server.records].count("turn/start") == 1
    assert len(stack.launcher.launches) == 1, "segments reuse the retained session"
    if max_frames < len(frames):
        assert len(results) >= 2, "the bound actually crossed segments"


async def test_the_segment_boundary_inside_turn_completed_is_reread(tmp_path: Path) -> None:
    """The exact failure-B boundary: a segment ending on TURN_ENDED resumes before the event."""

    stack = codex_stack(tmp_path, "turn_full")
    session = await started(stack)
    turn = await sent(stack, session)
    frames = await observed(stack, turn)
    ended, final = frames[-2], frames[-1]
    assert (ended.raw_kind, final.raw_kind) == ("turn/completed", "turn/completed.result")
    assert final.terminal and not ended.terminal
    before = framing.parse_cursor(ended.cursor)
    after = framing.parse_cursor(final.cursor)
    assert before is not None and after is not None
    assert (before.seq, before.taken, before.resume_after) == (after.seq, 1, after.seq - 1), (
        "all but the last frame of an event resume before it"
    )
    # RUN_RESULT was yielded after TURN_ENDED: only the last yielded frame names a resume
    # point; an earlier key defers to the service's last-taken cursor.
    assert stack.harness.resume_cursor(final.provider_key) == final.cursor
    assert stack.harness.resume_cursor(ended.provider_key) is None
    # A segment cut right after TURN_ENDED re-reads `turn/completed`: TURN_ENDED again
    # (deduped by the store) and then the terminal RUN_RESULT.
    again = await asyncio.wait_for(observed(stack, turn, after=ended.cursor), timeout=5)
    assert [frame.raw_kind for frame in again] == ["turn/completed.result"]
    assert again[-1].terminal
    # The diagnosed `seq - 1` cursor re-reads the whole event (TURN_ENDED deduped).
    legacy = framing.composite_cursor(before.turn_id, before.epoch, before.seq - 1)
    whole = await asyncio.wait_for(observed(stack, turn, after=legacy), timeout=5)
    assert [frame.raw_kind for frame in whole] == ["turn/completed", "turn/completed.result"]


async def test_a_cursor_past_a_finished_turn_resynchronizes_instead_of_waiting(
    tmp_path: Path,
) -> None:
    stack = codex_stack(tmp_path, "turn_full")
    session = await started(stack)
    turn = await sent(stack, session)
    frames = await observed(stack, turn)
    assert frames[-1].terminal
    # The cursor is past the completion (e.g. RUN_RESULT persisted, settlement lost): the
    # connection will never deliver `turn/completed` again, so history supplies it.
    replayed = await asyncio.wait_for(observed(stack, turn, after=frames[-1].cursor), timeout=5)
    assert replayed and replayed[-1].terminal
    assert replayed[-1].provider_key == frames[-1].provider_key
    reads = [p for m, p in stack.launcher.server.records if m == "thread/read"]
    assert any(p.get("includeTurns") for p in reads), "resynchronized from thread history"
    parsed = framing.parse_cursor(replayed[-1].cursor)
    assert parsed is not None and parsed.history is not None


async def test_a_new_turn_is_observed_from_its_turn_start_not_the_whole_buffer(
    tmp_path: Path,
) -> None:
    stack = codex_stack(tmp_path, "turn_full", "turn_full")
    session = await started(stack)
    first = await sent(stack, session, "turn:1")
    assert (await observed(stack, first))[-1].terminal
    second = await sent(stack, session, "turn:2", turn_no=2)
    assert second.native_turn_ref not in {None, first.native_turn_ref}
    frames = await observed(stack, second)
    assert frames[-1].terminal
    assert not any(frame.raw_kind == "thread/started" for frame in frames)
    earlier = [frame for frame in frames if frame.native_turn_ref == first.native_turn_ref]
    # At most the first turn's own completion (its terminal event is never marked taken).
    assert {frame.raw_kind for frame in earlier} <= {"turn/completed", "turn/completed.result"}
    assert not any(frame.terminal for frame in earlier), "never the other turn's terminal"
    assert all(
        frame.native_turn_ref in {first.native_turn_ref, second.native_turn_ref}
        for frame in frames
        if frame.native_turn_ref is not None
    )


@pytest.mark.parametrize("max_frames", [1, 2, 3])
async def test_a_relaunch_resync_pages_history_under_a_tiny_budget(
    tmp_path: Path, max_frames: int
) -> None:
    stack = codex_stack(tmp_path, "turn_disconnect")
    task = asyncio.create_task(
        stack.lanes.service.turn(
            stack.turn(segment=_bounds(50)), RecordingSignals(stack.lanes.frames)
        )
    )
    await asyncio.wait_for(stack.launcher.launched.wait(), 10)
    await asyncio.wait_for(stack.launcher.server.held.wait(), 10)
    for _ in range(500):
        if any(frame.tool_call_ref == "item-cmd-5" for frame in stack.frames()):
            break
        await asyncio.sleep(0.01)
    stack.launcher.server.release.set()  # the app-server dies mid-turn
    first = await asyncio.wait_for(task, 30)
    assert not first.done
    await asyncio.wait_for(stack.launcher.launches[0].task, timeout=5)
    result = first
    segments = 1
    while not result.done and segments < 40:
        result = await asyncio.wait_for(
            stack.lanes.service.turn(
                stack.turn(
                    segment=_bounds(max_frames),
                    phase="resume",
                    cursor=result.cursor,
                    segment_no=result.segment_no + 1,
                ),
                RecordingSignals(stack.lanes.frames),
            ),
            timeout=30,
        )
        segments += 1
    assert result.done, f"not settled after {segments} segments"
    assert OperationExecutionResult.model_validate(result.operation_result).status == "completed"
    frames = stack.frames()
    assert sum(frame.kind == FrameKind.RUN_RESULT for frame in frames) == 1
    assert len({frame.provider_key for frame in frames}) == len(frames)
    assert len(stack.launcher.launches) == 2, "one relaunch, then segments reuse it"
    assert "turn/start" not in [m for m, _p in stack.launcher.launches[1].server.records]


def test_cursor_forms_round_trip() -> None:
    live = framing.composite_cursor("turn-1", "ab12", 7)
    history = framing.history_cursor("turn-1", "ab12", 3)
    assert framing.parse_cursor(live) == framing.ParsedCursor("turn-1", "ab12", 7, None)
    assert framing.parse_cursor(history) == framing.ParsedCursor("turn-1", "ab12", -1, 3)
    partial = framing.composite_cursor("turn-1", "ab12", 7, 1)
    assert partial == "turn-1@ab12:7/1"
    parsed = framing.parse_cursor(partial)
    assert parsed == framing.ParsedCursor("turn-1", "ab12", 7, None, 1)
    assert parsed is not None and parsed.resume_after == 6
    assert framing.composite_cursor("t", "e", -1) == "t@e:0"
    broken_cursors = (
        None,
        "",
        "no-at",
        "t@e",
        "t@e:",
        "t@e:x",
        "t@e:h",
        "t@:3",
        "@e:3",
        "t@e:-2",
        "t@e:3/0",
        "t@e:h3/1",
        "t@e:0/1",
        "t@e:3/x",
    )
    for broken in broken_cursors:
        assert framing.parse_cursor(broken) is None, broken
