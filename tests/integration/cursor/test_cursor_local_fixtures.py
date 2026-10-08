"""FT-G3: recorded-shape fixtures replayed through the real `cursor_local` adapter and reducer.

Each fixture under `fixtures/local/` (hand-authored from the documented SDK and hook shapes,
marked synthetic until FT-G6 records scrubbed real runs) is replayed by a fake bridge through
`CursorLocalHarness`, `lane.turn` (`LaneTurnService`), the kernel hook callback and the C2
reducer (`derive`). Every fixture settles once, stores no frame twice, keeps secrets out of
frames, and yields the facts its run implies.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.frames.reducer import DeriveContext, derive
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import LaneTurnRequest
from mission_control.domain.frames.contracts import LaneProfile
from mission_control.domain.frames.facts import (
    CompactionObservedFact,
    ExecutionOutcomeFact,
    ToolEffectFact,
    TurnCompletedFact,
)
from tests.fixtures.cursor_local import local_stack
from tests.fixtures.lane_turns import RecordingSignals, lane_stack

EXPECTED = {
    "full_run": ("completed", "finished", {ToolEffectFact, TurnCompletedFact}),
    "error_run": ("failed", "error", {ToolEffectFact, ExecutionOutcomeFact}),
    "hook_deny": ("completed", "finished", {ToolEffectFact}),
    "precompact": ("failed", "finished", {CompactionObservedFact, TurnCompletedFact}),
}


@pytest.mark.parametrize("fixture", sorted(EXPECTED))
async def test_fixture_replays_through_the_adapter_lane_turn_and_reducer(
    tmp_path: Path, fixture: str
) -> None:
    stack = local_stack(tmp_path, fixture)
    lanes = lane_stack(stack.harness, frames=stack.frames, operation=stack.operation)
    identity = LaneExecutionIdentity.of(stack.operation, "cursor_local", 1)
    request = LaneTurnRequest(operation=stack.operation, lane_profile="cursor_local", generation=1)
    result = await lanes.service.turn(request, RecordingSignals(stack.frames))
    status, native, fact_types = EXPECTED[fixture]
    assert result.closing_facts is not None and result.closing_facts.native_status == native
    settled = OperationExecutionResult.model_validate(result.operation_result)
    # precompact ran past the operation's token budget: it fails `budget_exceeded`.
    assert settled.status == status
    for _event, expected, decision in stack.launcher.hook_results:
        assert decision.decision.value == expected
    frames = list(stack.frames._executions[identity.harness_execution_id].frames.values())
    assert len({frame.provider_key for frame in frames}) == len(frames)
    assert all("sk-fixture-secret" not in frame.body_excerpt for frame in frames)
    facts = derive(
        [frame for frame in frames if frame.closing],
        DeriveContext(lane=LaneProfile.CURSOR_LOCAL, current_generation=1),
    )
    assert fact_types <= {type(fact) for fact in facts}
    # Replaying the same segment again stores nothing new and sends nothing.
    before = len(frames)
    again = await lanes.service.turn(request, RecordingSignals(stack.frames))
    assert again.done and len(stack.launcher.sends) == 1
    assert len(stack.frames._executions[identity.harness_execution_id].frames) == before
