"""Small drivers for the MP-08 codex tests: direct harness calls and `lane.turn` segments
over the FIXTURE app-server (nothing here is a provider)."""

from __future__ import annotations

from typing import Any

from mission_control.application.execution.harness.lane_turns import harness_scope
from mission_control.domain.execution.lane_turns import LaneTurnResult
from mission_control.domain.execution.lanes import (
    LaneFrame,
    LaneSegmentBounds,
    ObserveRequest,
    PrepareRequest,
    SendTurnRequest,
    SessionHandle,
    StartRequest,
    TurnHandle,
)
from tests.fixtures.lane_turns import RecordingSignals
from tests.unit.codex.support import CodexStack


def fields(stack: CodexStack, key: str = "test") -> dict[str, Any]:
    return {
        "scope": harness_scope(stack.operation.request_scope),
        "lane_profile": "codex",
        "harness_execution_id": stack.heid,
        "binding_digest": stack.operation.effective_configuration_digest,
        "idempotency_key": f"{stack.heid}:1:{key}",
        "generation": 1,
    }


async def started(stack: CodexStack) -> SessionHandle:
    """Stage, prepare and start directly on the harness (no `lane.turn`)."""

    harness = stack.harness
    harness.stage(stack.heid, stack.operation)
    values = fields(stack)
    prepared = await harness.prepare(
        PrepareRequest(
            **values,
            run_id=stack.operation.identity.run_id,
            operation_id=stack.operation.identity.operation_id,
            attempt_no=1,
        )
    )
    return await harness.start(StartRequest(**values, prepared=prepared))


async def sent(
    stack: CodexStack, session: SessionHandle, key: str = "turn:1", turn_no: int = 1
) -> TurnHandle:
    return await stack.harness.send_turn(
        SendTurnRequest(
            **fields(stack, key),
            session=session,
            turn_no=turn_no,
            instruction_ref=f"operation:{stack.operation.identity.semantic_key}:{key}",
        )
    )


async def observed(
    stack: CodexStack, turn: TurnHandle, *, after: str | None = None, max_frames: int = 200
) -> list[LaneFrame]:
    """Observe until the terminal frame (or the bound)."""

    frames: list[LaneFrame] = []
    async for frame in stack.harness.observe(
        ObserveRequest(**fields(stack), turn=turn, after=after, max_frames=max_frames)
    ):
        frames.append(frame)
        if frame.terminal:
            break
    return frames


async def drive_segments(
    stack: CodexStack, bounds: LaneSegmentBounds, *, limit: int = 80
) -> tuple[LaneTurnResult, list[LaneTurnResult]]:
    """`lane.turn` segments as the workflow schedules them, until done (at most `limit`)."""

    results: list[LaneTurnResult] = []
    result = await stack.lanes.service.turn(
        stack.turn(segment=bounds), RecordingSignals(stack.lanes.frames)
    )
    results.append(result)
    while not result.done and len(results) < limit:
        result = await stack.lanes.service.turn(
            stack.turn(
                segment=bounds,
                phase="resume",
                cursor=result.cursor,
                segment_no=result.segment_no + 1,
            ),
            RecordingSignals(stack.lanes.frames),
        )
        results.append(result)
    return result, results


__all__ = ["drive_segments", "fields", "observed", "sent", "started"]
