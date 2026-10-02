"""Map family semantic locations onto `CON-CP-RUNTIME-UNIT-V1` (REQ-CP-EXEC-013)."""

from __future__ import annotations

from typing import Literal

from app.domain.graph_runtime.identities import (
    GoalDirectedUnitLocation,
    RuntimeUnitIdentity,
    StageGraphUnitLocation,
)
from app.domain.orchestration.contracts import StageExecutionIdentity


def stage_runtime_unit(request_scope: str, identity: StageExecutionIdentity) -> RuntimeUnitIdentity:
    candidate = identity.candidate
    return RuntimeUnitIdentity(
        request_scope=request_scope,
        belllabs_run_id=identity.run_id,
        execution_epoch=identity.execution_epoch,
        family="stage_graph",
        unit_kind="stage_operation",
        semantic_operation_id=identity.operation_id,
        semantic_attempt=identity.semantic_attempt,
        location=StageGraphUnitLocation(
            stage_id=candidate.stage_id,
            mapped_instance_id=candidate.mapped_instance_id,
            workflow_cycle_ordinal=candidate.workflow_cycle_ordinal,
            stage_cycle_ordinal=candidate.stage_cycle_ordinal,
            operation_slot_id=candidate.operation_slot_id,
        ),
    )


def goal_runtime_unit(
    *,
    request_scope: str,
    run_id: str,
    execution_epoch: int,
    operation_id: str,
    operation_attempt: int,
    goal_iteration: int,
    goal_revision_id: str,
    operation_role: Literal["executor", "verifier"],
    agent_run: int,
    session_generation: int,
) -> RuntimeUnitIdentity:
    return RuntimeUnitIdentity(
        request_scope=request_scope,
        belllabs_run_id=run_id,
        execution_epoch=execution_epoch,
        family="goal_directed",
        unit_kind="goal_executor" if operation_role == "executor" else "goal_verifier",
        semantic_operation_id=operation_id,
        semantic_attempt=operation_attempt,
        location=GoalDirectedUnitLocation(
            goal_iteration=goal_iteration,
            goal_revision_id=goal_revision_id,
            operation_role=operation_role,
            agent_run=agent_run,
            session_generation=session_generation,
        ),
    )


def goal_unit_workspace_root(unit: RuntimeUnitIdentity | None) -> str | None:
    """The role-scoped root under which a GoalDirected unit binds its compiled slots.

    REQ-CP-DA-013 requires exact exclusive writable slots; the GoalDirected interpreter also
    requires executor and verifier writable paths to be disjoint (REQ-BP-GD-004: an
    independently bound verifier with its own workspace). So a
    GoalDirected operation binds every compiled workspace slot under
    `/goal/{goal_iteration}/{operation_role}`. The root is derived from the digest-bound unit
    identity only, never chosen by the operation, so the run-control authority can recompute
    and verify it. StageGraph units (and requests without a unit) have no root.
    """

    if unit is None or not isinstance(unit.location, GoalDirectedUnitLocation):
        return None
    location = unit.location
    return f"/goal/{location.goal_iteration}/{location.operation_role}"


def goal_operation_id(goal_iteration: int, operation_role: str) -> str:
    """The semantic operation ID of a GoalDirected executor or verifier operation."""

    return f"goal-iteration/{goal_iteration}/{operation_role}"


def goal_unit_operation_id(unit: RuntimeUnitIdentity) -> str | None:
    """The operation ID a GoalDirected unit's location implies (`None` for other units)."""

    if not isinstance(unit.location, GoalDirectedUnitLocation):
        return None
    return goal_operation_id(unit.location.goal_iteration, unit.location.operation_role)
