"""RRM-021 replay compatibility: StageGraph histories captured before the baseline settlement.

The fixtures under `tests/fixtures/histories/rrm021_pre_change/` were captured by running the
StageGraph family workflow of integration base `0d0c184` (before RRM-021) on the
time-skipping dev server with the RRM-008 harness and a non-empty baseline in the family
input: a StageGraph run that completed, and one that was cancelled (the RRM-008 saga). They
carry no `rrm-021-settle-stagegraph-baseline` marker, so replaying them against the current
workflow proves that the patch keeps the recorded command sequence intact: the
`stagegraph.settle_baseline` activity is never issued on replay.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import (
    SETTLE_BASELINE_PATCH,
    StageGraphWorkflow,
)
from tests.fixtures.temporal_history import patch_ids, scheduled_activity_inputs

HISTORIES = Path(__file__).resolve().parents[2] / "fixtures" / "histories" / "rrm021_pre_change"
CASES = (
    ("stagegraph_baseline_completed.run1.json", "family/pre-baseline-completed/1"),
    ("stagegraph_baseline_cancelled.run1.json", "family/pre-baseline-cancelled/1"),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "workflow_id"), CASES)
async def test_pre_change_stagegraph_history_replays_without_the_baseline_settlement(
    name: str, workflow_id: str
) -> None:
    history = WorkflowHistory.from_json(workflow_id, (HISTORIES / name).read_text("utf-8"))
    assert len(history.events) > 10
    assert SETTLE_BASELINE_PATCH not in patch_ids(history), "pre-change"
    assert scheduled_activity_inputs(history, "stagegraph.settle_baseline") == []
    started = scheduled_activity_inputs(history, "stagegraph.initialize")[0]
    assert started["request_scope"], "the captured family input carries its run binding"
    await Replayer(
        workflows=[StageGraphWorkflow, OperationWorkflow],
        workflow_runner=coordinator_workflow_runner(),
    ).replay_workflow(history)
