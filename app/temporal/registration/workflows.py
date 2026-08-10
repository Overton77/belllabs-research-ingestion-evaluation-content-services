from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.linked_run import LinkedRunObserverWorkflow, LinkedRunWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow

WORKFLOW_TYPES: tuple[type[Any], ...] = (
    BellLabsRunWorkflow,
    StageGraphWorkflow,
    GoalDirectedWorkflow,
    OperationWorkflow,
    LinkedRunWorkflow,
    LinkedRunObserverWorkflow,
)


def registered_workflows() -> Sequence[type[Any]]:
    """Return the one authoritative versioned workflow registry."""

    return WORKFLOW_TYPES


def coordinator_workflows(family: str) -> Sequence[type[Any]]:
    if family == "StageGraph":
        return (BellLabsRunWorkflow, StageGraphWorkflow, OperationWorkflow)
    if family == "GoalDirected":
        return (BellLabsRunWorkflow, GoalDirectedWorkflow, OperationWorkflow)
    raise ValueError(f"undeclared BellLabs workflow family: {family}")
