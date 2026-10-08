"""FT-G6: captured `mc.operation.v1` lane histories replay on the current worker code.

`histories/ft_lanes/` holds the histories the FT-G2 and FT-G4 time-skipping suites recorded
(`MC_CAPTURE_LANE_HISTORIES=1 pytest tests/integration/temporal/test_lane_turn.py`): Cursor
units as `lane.turn` segments, a segment resumed after a lost worker, the cancel Update with
`lane.cancel`, carried command ids, a status poll parked `in_doubt`, `wait_then_send` after a
busy agent, Deep Agents through `lane.turn` (settled and cancelled), continue-as-new (both
runs), a pause refused mid-run and a boundary pause held until resume, `cancel_and_replace`
inside one segment, a forked unit restored from a snapshot, and a continuation handed to a
hydrated agent. Each replays with `Replayer` against today's `OperationWorkflow`, so any change
to the segment loop that alters a recorded command sequence fails here (and needs a
`workflow.patched` marker).
"""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import (
    LANE_PAUSE_PATCH,
    SEGMENT_LOOP_PATCH,
    OperationWorkflow,
)
from tests.fixtures.temporal_history import patch_ids

HISTORIES = Path(__file__).resolve().parent / "histories" / "ft_lanes"
EXPECTED = {
    "ft-g2-busy",
    "ft-g2-cancel-update",
    "ft-g2-carried-commands",
    "ft-g2-continue-as-new-first",
    "ft-g2-continue-as-new-latest",
    "ft-g2-crash-resume",
    "ft-g2-deep-agents",
    "ft-g2-deep-agents-cancel",
    "ft-g2-in-doubt",
    "ft-g2-segments",
    "ft-g4-continuation",
    "ft-g4-fork",
    "ft-g4-inject",
    "ft-g4-pause-boundary",
    "ft-g4-pause-mid-run",
}


def _history(path: Path) -> WorkflowHistory:
    return WorkflowHistory.from_json(path.stem, path.read_text("utf-8"))


def _scheduled(history: WorkflowHistory) -> list[str]:
    return [
        event.activity_task_scheduled_event_attributes.activity_type.name
        for event in history.events
        if event.HasField("activity_task_scheduled_event_attributes")
    ]


def test_the_capture_covers_every_lane_scenario() -> None:
    assert {path.stem for path in HISTORIES.glob("*.json")} == EXPECTED


@pytest.mark.asyncio
@pytest.mark.parametrize("name", sorted(EXPECTED))
async def test_a_captured_lane_history_replays_on_the_current_worker(name: str) -> None:
    history = _history(HISTORIES / f"{name}.json")
    await Replayer(
        workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
    ).replay_workflow(history)
    markers = patch_ids(history)
    scheduled = _scheduled(history)
    if name.startswith("ft-g2-deep-agents"):
        assert scheduled and set(scheduled) <= {"lane.turn", "lane.cancel"}
    else:
        assert SEGMENT_LOOP_PATCH in markers
        assert "operation.execute" not in scheduled, "a Cursor unit never runs operation.execute"
    # The boundary-pause marker exists only where a pause held a segment.
    assert (LANE_PAUSE_PATCH in markers) is (name == "ft-g4-pause-boundary")
