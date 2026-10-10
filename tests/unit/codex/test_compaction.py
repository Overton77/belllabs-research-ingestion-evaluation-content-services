"""MP-08 x MP-12: the codex lane's `CompactingLane` and `ContextOccupancyLane`.

FIXTURE app-server (no Codex): `compact(CompactionRequest)` runs `thread/compact/start` only
on an idle thread and waits for the server's own report; `context_occupancy` reads the last
`thread/tokenUsage/updated` against its `modelContextWindow` (never the cumulative total;
`unknown` without one, and after a compaction until a new usage update); both are reached
through the real `LaneTurnService` safe boundary with a continuation coordinator (soft
pressure -> native compaction). What only the owner-run drill can verify is listed in
`CodexLocalHarness.compact` and `frames.occupancy_from_usage`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from mission_control.adapters.codex import frames as framing
from mission_control.adapters.codex.protocol import ThreadTokenUsage, TokenUsageBreakdown
from mission_control.application.context.lane_continuation import LaneContinuationCoordinator
from mission_control.application.context.lane_support import (
    CompactingLane,
    CompactionRequest,
    ContextOccupancyLane,
)
from mission_control.application.execution.harness.lane_turns import LaneTurnService
from mission_control.application.execution.harness.sessions import WorkerSessionManager
from mission_control.domain.execution.lanes import LaneSegmentBounds
from tests.fixtures.continuation import build_service
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.codex.drive import observed, sent, started
from tests.unit.codex.fixture_app_server import FixtureLauncher
from tests.unit.codex.support import build_harness, codex_operation, codex_stack

BOUNDS = LaneSegmentBounds(max_frames=200, max_duration_s=20, heartbeat_timeout_s=3)


def test_the_harness_is_a_compacting_and_occupancy_lane(tmp_path: Path) -> None:
    harness, *_rest = build_harness(tmp_path, FixtureLauncher())
    assert isinstance(harness, CompactingLane)
    assert isinstance(harness, ContextOccupancyLane)
    assert harness.describe().compaction_control == "native"


def test_occupancy_is_the_last_call_against_the_window_never_the_total() -> None:
    usage = ThreadTokenUsage(
        last=TokenUsageBreakdown(input_tokens=1_000, output_tokens=200, total_tokens=1_200),
        total=TokenUsageBreakdown(input_tokens=90_000, output_tokens=9_000, total_tokens=99_000),
        model_context_window=10_000,
    )
    occupancy = framing.occupancy_from_usage(usage)
    assert occupancy.known and occupancy.used_tokens == 1_200
    assert occupancy.window_tokens == 10_000 and occupancy.source == "provider_context_window"
    windowless = framing.occupancy_from_usage(
        usage.model_copy(update={"model_context_window": None})
    )
    assert not windowless.known and "modelContextWindow" in windowless.reason
    assert not framing.occupancy_from_usage(None).known


async def test_compaction_is_refused_mid_turn_then_reported_on_an_idle_thread(
    tmp_path: Path,
) -> None:
    stack = codex_stack(tmp_path, "turn_hold", "turn_full")
    harness = stack.harness
    session = await started(stack)
    before = await harness.context_occupancy(stack.heid, session, None)
    assert before is not None and not before.known, "no usage yet: unknown, never invented"
    turn = await sent(stack, session)
    server = stack.launcher.server
    await asyncio.wait_for(server.held.wait(), timeout=5)
    request = CompactionRequest(
        harness_execution_id=stack.heid, session=session, turn=turn, epoch=1
    )
    refused = await harness.compact(request)
    assert not refused.completed and "active" in refused.detail and server.compactions == 0

    server.release.set()
    assert (await observed(stack, turn))[-1].terminal
    measured = await harness.context_occupancy(stack.heid, session, turn)
    assert measured is not None and measured.known
    assert (measured.used_tokens, measured.window_tokens) == (90, 272_000)

    receipt = await harness.compact(request)
    assert receipt.completed and server.compactions == 1
    epoch = stack.launcher.launches[0].connection.epoch
    assert receipt.native_ref is not None and receipt.native_ref.startswith(f"thr-0001@{epoch}:")
    assert receipt.occupancy_after is None, "no usage update followed the compaction"
    stale = await harness.context_occupancy(stack.heid, session, turn)
    assert stale is not None and not stale.known and "compacted" in stale.reason

    # The next turn's observation still carries the compaction the provider reported between
    # the turns, and its own usage makes the occupancy known again.
    second = await sent(stack, session, "turn:2", turn_no=2)
    frames = await observed(stack, second)
    raw = [frame.raw_kind for frame in frames]
    assert "thread/compacted" in raw
    assert any(
        frame.raw_kind == "item/completed" and frame.body["item"]["type"] == "contextCompaction"
        for frame in frames
    )
    again = await harness.context_occupancy(stack.heid, session, second)
    assert again is not None and again.known and again.used_tokens == 160


async def test_soft_pressure_runs_the_native_compaction_through_lane_turn_service(
    tmp_path: Path,
) -> None:
    base = codex_operation()
    workspace = base.workspace.model_copy(update={"exclusive_write_paths": ("/outputs/report.md",)})
    operation = codex_operation(workspace=workspace.model_dump(mode="python"))
    stack = codex_stack(tmp_path, "turn_pressure", operation=operation)
    wired = build_service()
    coordinator = LaneContinuationCoordinator(
        wired["store"],
        checkpoints=wired["service"].checkpoint_store,
        packets=wired["selections"],
    )
    service = LaneTurnService(
        lanes=stack.lanes.service._lanes,
        boundary=stack.lanes.boundary,
        frames=stack.lanes.frames,
        states=stack.lanes.states,
        frame_reader=stack.lanes.frames,
        sessions=WorkerSessionManager(owner_ref="worker-a"),
        continuations=coordinator,
    )
    result = await service.turn(stack.turn(segment=BOUNDS), RecordingSignals(stack.lanes.frames))

    server = stack.launcher.server
    assert server.compactions == 1, "the lane's explicit compaction ran"
    stored = stack.frames()
    by_raw = {frame.raw_kind: frame for frame in stored}
    assert "custom.mc.before_compaction" in by_raw and "custom.mc.after_compaction" in by_raw
    after = by_raw["custom.mc.after_compaction"]
    assert '"epoch":1' in after.body_excerpt and "thr-0001@" in after.body_excerpt
    pressure = [frame for frame in stored if frame.raw_kind == "mc.context_pressure"]
    assert len(pressure) == 2
    assert '"level":"soft"' in pressure[0].body_excerpt
    assert "provider_context_window" in pressure[0].body_excerpt
    # Remeasured after the compaction: no fresh usage, so `unknown`, never the stale 77%.
    assert '"ratio":"unknown"' in pressure[1].body_excerpt
    assert "mc.native_compaction_failed" not in by_raw
    # Declared outputs are missing (FT-G4): the turn finished, the operation is not accepted.
    assert result.done and result.closing_facts is not None
    assert result.closing_facts.missing_outputs == ("outputs/report.md",)
