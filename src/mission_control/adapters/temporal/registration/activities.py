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
            activities.initialize,
            activities.admit_operation,
            activities.decide_result,
            activities.apply_cycle,
            activities.complete_stagegraph,
            # RRM-021: releases the admitted baseline reservation before terminalization.
            activities.settle_baseline,
            # RRM-007: the family boundary's run-control facts (waits, quiescence, applied
            # boundary commands), bound to the current run version by the activity.
            activities.apply_boundary_command,
        )
    if family == "GoalDirected":
        return (
            activities.execute_iteration,
            activities.prepare_handoff,
            activities.verify_iteration,
            activities.apply_lifecycle_command,
            activities.materialize_workflow_result,
            activities.apply_boundary_command,
        )
    raise ValueError(f"undeclared BellLabs activity family: {family}")


def agent_cognitive_activities(activities: Any) -> Sequence[ActivityCallable]:
    """Select the family-neutral cognitive operation activities.

    `operation.execute` runs one Activity attempt of a unit; `operation.cancel` (RRM-008,
    REQ-CP-EXEC-008) settles a unit the cancellation saga reached, never dispatching
    cognition. Both are served by the same `OperationExecutionActivities` instance, and so
    are the lane activities when a lane turn service is composed (FT-G2).
    """

    lanes = getattr(activities, "lane_activities", None)
    # FT-G2: `lane.turn`, `lane.status` and `lane.cancel` share the operation's queue.
    return (activities.execute, activities.cancel, *(lanes() if callable(lanes) else ()))
