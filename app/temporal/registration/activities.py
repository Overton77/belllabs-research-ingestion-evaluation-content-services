from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

ActivityCallable = Callable[..., Any]


@dataclass(frozen=True)
class ActivityRegistry:
    """Concrete activity instances grouped by logical worker isolation class."""

    coordinator_family: tuple[ActivityCallable, ...] = ()
    agent_cognitive: tuple[ActivityCallable, ...] = ()
    ingestion_io: tuple[ActivityCallable, ...] = ()
    sandbox_external_job: tuple[ActivityCallable, ...] = ()
    verification_reconciliation: tuple[ActivityCallable, ...] = ()

    def for_queue_class(self, queue_class: str) -> Sequence[ActivityCallable]:
        try:
            return getattr(self, queue_class)
        except AttributeError as error:
            raise ValueError(f"undeclared BellLabs activity queue class: {queue_class}") from error


def coordinator_activities(family: str, activities: Any) -> Sequence[ActivityCallable]:
    """Select only the declared activity surface for one coordinator family worker."""

    if family == "StageGraph":
        return (
            activities.execute_operation,
            activities.evaluate_workflow,
            activities.apply_lifecycle_command,
            activities.materialize_workflow_result,
        )
    if family == "GoalDirected":
        return (
            activities.execute_iteration,
            activities.prepare_handoff,
            activities.verify_iteration,
            activities.apply_lifecycle_command,
            activities.materialize_workflow_result,
        )
    raise ValueError(f"undeclared BellLabs activity family: {family}")


def agent_cognitive_activities(activities: Any) -> Sequence[ActivityCallable]:
    """Select the sole family-neutral cognitive operation activity."""

    return (activities.execute,)
