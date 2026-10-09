from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from mission_control.adapters.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from mission_control.adapters.temporal.workflows.goal_directed import GoalDirectedWorkflow
from mission_control.adapters.temporal.workflows.human_gate import HumanGateWorkflow
from mission_control.adapters.temporal.workflows.linked_run import (
    LinkedRunObserverWorkflow,
    LinkedRunWorkflow,
)
from mission_control.adapters.temporal.workflows.mission_run import MissionRunWorkflow
from mission_control.adapters.temporal.workflows.operation import (
    MissionOperationWorkflow,
    OperationWorkflow,
)
from mission_control.adapters.temporal.workflows.stagegraph import StageGraphWorkflow

WORKFLOW_TYPES: tuple[type[Any], ...] = (
    BellLabsRunWorkflow,
    MissionRunWorkflow,
    MissionOperationWorkflow,
    StageGraphWorkflow,
    GoalDirectedWorkflow,
    OperationWorkflow,
    LinkedRunWorkflow,
    LinkedRunObserverWorkflow,
    # MP-10: `mc.human_gate.v1`, started by both families on their own queue.
    HumanGateWorkflow,
)


def registered_workflows() -> Sequence[type[Any]]:
    """Return the one authoritative versioned workflow registry."""

    return WORKFLOW_TYPES


def coordinator_workflows(family: str) -> Sequence[type[Any]]:
    if family == "StageGraph":
        return (
            BellLabsRunWorkflow,
            MissionRunWorkflow,
            StageGraphWorkflow,
            OperationWorkflow,
            MissionOperationWorkflow,
            HumanGateWorkflow,
        )
    if family == "GoalDirected":
        return (
            BellLabsRunWorkflow,
            MissionRunWorkflow,
            GoalDirectedWorkflow,
            OperationWorkflow,
            MissionOperationWorkflow,
            HumanGateWorkflow,
        )
    raise ValueError(f"undeclared BellLabs workflow family: {family}")
