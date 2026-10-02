"""RRM-007 replay compatibility: histories captured with the integration-base code replay here.

The fixtures under `tests/fixtures/histories/rrm007_pre_change/` were captured by running
the family workflows of integration base `d46f548` (before RRM-007) on the time-skipping
dev server with the WP-BP-010/020 fixtures: a StageGraph whose declared wait was released
by the then-raw `satisfy_wait` signal and that continued-as-new (two runs), a plain
StageGraph any-join run, a two-iteration GoalDirected run, and a GoalDirected run that
failed with the then non-retryable `goal_paused` error. Replaying them against the current
workflow code proves that the `workflow.patched` markers (`rrm-007-governed-waits`,
`rrm-007-declared-waits`, `rrm-007-quiescence`, `rrm-007-durable-goal-pause`) keep every
pre-RRM-007 command sequence intact.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow

HISTORIES = Path(__file__).resolve().parents[2] / "fixtures" / "histories" / "rrm007_pre_change"
CASES = (
    ("stagegraph_wait_signal_continue_as_new.run1.json", "family/pre-wait/1", StageGraphWorkflow),
    ("stagegraph_wait_signal_continue_as_new.run2.json", "family/pre-wait/1", StageGraphWorkflow),
    ("stagegraph_any_join.run1.json", "family/pre-plain/1", StageGraphWorkflow),
    ("goal_directed_two_iterations.run1.json", "family/pre-goal/1", GoalDirectedWorkflow),
    (
        "goal_directed_policy_pause_failure.run1.json",
        "family/pre-goal-paused/1",
        GoalDirectedWorkflow,
    ),
)


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "workflow_id", "family"), CASES)
async def test_pre_change_family_history_replays_against_the_current_code(
    name: str, workflow_id: str, family: type
) -> None:
    history = WorkflowHistory.from_json(workflow_id, (HISTORIES / name).read_text("utf-8"))
    assert len(history.events) > 10
    patches = {
        event.marker_recorded_event_attributes.marker_name
        for event in history.events
        if event.HasField("marker_recorded_event_attributes")
    }
    assert not any(marker.startswith("rrm-007") for marker in patches), "captured pre-RRM-007"
    await Replayer(
        workflows=[family, OperationWorkflow], workflow_runner=coordinator_workflow_runner()
    ).replay_workflow(history)
