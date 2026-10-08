"""FT-G7: histories recorded before the temporalio 1.34 upgrade replay on the upgraded SDK.

`histories/temporalio_1_30/` holds executions captured by running the Temporal
integration suite of base `e19f376` (pre-upgrade code) on `temporalio==1.30.0`, through
a capture plugin that fetched every root, family, operation and linked-run history before
the test server shut down: a StageGraph root, family and operation and a GoalDirected
family (after Continue-As-New) and operations, all started under the `required` Search
Attribute policy; an operation started directly under `required` (it upserts its own
attributes); the RRM-008 operation cancellation sagas; and a linked run (parent,
observer and child root). The older captures under `tests/fixtures/histories/` replay too.

None of them carries the `ft-g7-mc-visibility` marker, so replaying them against the
current workflows proves the upgrade and the FT-G7 visibility writes keep the recorded
command sequences: no new upsert, marker or child-start change is issued on replay.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from mission_control.adapters.temporal.linked_run_workflow import (
    LinkedRunObserverWorkflow,
    LinkedRunWorkflow,
)
from mission_control.adapters.temporal.search_attributes import MC_VISIBILITY_PATCH
from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from mission_control.adapters.temporal.workflows.goal_directed import GoalDirectedWorkflow
from mission_control.adapters.temporal.workflows.operation import OperationWorkflow
from mission_control.adapters.temporal.workflows.stagegraph import StageGraphWorkflow
from tests.fixtures.temporal_history import patch_ids
from tests.integration.temporal.test_linked_runs import FixtureChildWorkflow

UPGRADE = Path(__file__).resolve().parent / "histories" / "temporalio_1_30"
FIXTURES = Path(__file__).resolve().parents[2] / "fixtures" / "histories"
PRODUCTION_WORKFLOWS = [
    BellLabsRunWorkflow,
    StageGraphWorkflow,
    GoalDirectedWorkflow,
    OperationWorkflow,
    LinkedRunWorkflow,
    LinkedRunObserverWorkflow,
]


def _cases() -> list[Path]:
    return sorted([*UPGRADE.glob("*.json"), *FIXTURES.glob("*/*.json")])


def _history(path: Path) -> WorkflowHistory:
    return WorkflowHistory.from_json(path.stem, path.read_text("utf-8"))


def _workflow_type(history: WorkflowHistory) -> str:
    started = history.events[0].workflow_execution_started_event_attributes
    return started.workflow_type.name


def test_the_upgrade_capture_covers_every_family() -> None:
    types = {_workflow_type(_history(path)) for path in UPGRADE.glob("*.json")}
    assert types == {
        "belllabs.run.v1",
        "belllabs.stagegraph",
        "belllabs.goal-directed",
        "belllabs.operation.v2",
        "belllabs.linked-run",
        "belllabs.linked-run-observer",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("path", _cases(), ids=lambda path: f"{path.parent.name}/{path.stem}")
async def test_pre_upgrade_history_replays_on_the_upgraded_sdk(path: Path) -> None:
    history = _history(path)
    assert MC_VISIBILITY_PATCH not in patch_ids(history), "captured before FT-G7"
    if path.name.startswith("linked_run_") and _workflow_type(history) == "belllabs.run.v1":
        # The linked-run test's child root is a fixture workflow, not the production root.
        replayer = Replayer(workflows=[FixtureChildWorkflow])
    else:
        replayer = Replayer(
            workflows=PRODUCTION_WORKFLOWS, workflow_runner=coordinator_workflow_runner()
        )
    await replayer.replay_workflow(history)
