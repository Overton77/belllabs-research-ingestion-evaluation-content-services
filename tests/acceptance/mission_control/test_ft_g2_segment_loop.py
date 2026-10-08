"""FT-G2 acceptance: the Deep Agents parity scenario through the `lane.turn` segment loop.

The production composition with `MISSION_CONTROL_LANE_SEGMENT_LOOP=true` runs the RRM-009
GoalDirected scenario (real DeepAgents, saver/store, journals, family settlement) unchanged;
every operation workflow schedules `lane.turn` instead of `operation.execute`, carries the
`ft-g2-segment-loop` marker, and replays.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from temporalio.worker import Replayer
from tests.acceptance.mission_control.test_postgres_runtime_parity import (
    test_goal_directed_runs_two_iterations_through_the_production_composition as goal_scenario,
)
from tests.fixtures.mission_control_production_stack import open_postgres_production_stack
from tests.fixtures.rrm009_production_harness import ProductionStack
from tests.fixtures.temporal_history import patch_ids

from mission_control.adapters.temporal.workflow_sandbox import coordinator_workflow_runner
from mission_control.adapters.temporal.workflows.operation import (
    SEGMENT_LOOP_PATCH,
    OperationWorkflow,
)

pytestmark = pytest.mark.common_db


@pytest.fixture
async def stack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[ProductionStack]:
    async with open_postgres_production_stack(
        root=tmp_path,
        monkeypatch=monkeypatch,
        extra_environment={"MISSION_CONTROL_LANE_SEGMENT_LOOP": "true"},
    ) as production:
        yield production


@pytest.mark.asyncio
async def test_goal_directed_parity_runs_every_operation_through_lane_turn(
    stack: ProductionStack,
) -> None:
    await goal_scenario(stack)
    operations = [
        item
        async for item in stack.client.list_workflows(
            "WorkflowType = 'belllabs.operation.v2' OR WorkflowType = 'mc.operation.v1'"
        )
    ]
    assert len(operations) == 4, [item.id for item in operations]
    replayer = Replayer(
        workflows=[OperationWorkflow], workflow_runner=coordinator_workflow_runner()
    )
    for item in operations:
        history = await stack.client.get_workflow_handle(item.id).fetch_history()
        scheduled = [
            event.activity_task_scheduled_event_attributes.activity_type.name
            for event in history.events
            if event.HasField("activity_task_scheduled_event_attributes")
        ]
        assert scheduled == ["lane.turn"], (item.id, scheduled)
        assert SEGMENT_LOOP_PATCH in patch_ids(history)
        await replayer.replay_workflow(history)
