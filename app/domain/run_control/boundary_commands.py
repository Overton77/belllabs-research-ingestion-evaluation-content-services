"""Boundary commands: the pure rules behind governed interventions (RRM-007).

`CON-CP-WORKFLOW-MESSAGE-V1` (AMD-RRM-001): run control accepts a pause, resume, wait
release, cancel or `reconcile_unit` command idempotently, bound to scope, target, expected
version and generation, and records a receipt for every later transition
(`accepted -> delivered -> applied`, `rejected` from `accepted` or `delivered`). Acceptance
is not delivery; delivery is not application. These helpers decide, from the projection
alone, which boundary a command targets and how its receipts are shaped; nothing here
touches a transport or a store.
"""

from __future__ import annotations

from datetime import datetime

from app.domain.control_plane.canonical import contract_fingerprint
from app.domain.run_control.contracts import (
    BOUNDARY_COMMAND_KINDS,
    EXECUTION_SEQUENCE_SPACE,
    FAMILY_BOUNDARY_COMMAND_KINDS,
    BoundaryCommandReceipt,
    BoundaryCommandRecord,
    BoundaryRejectionReason,
    BoundaryTarget,
    CancelAction,
    LifecycleCommand,
    PauseAction,
    ReceiptState,
    ReconcileUnitAction,
    ResumeAction,
    RunProjection,
    SatisfyWaitAction,
)

RUN_CONTROL_RECORDER = "run_control"
BoundaryAction = PauseAction | ResumeAction | SatisfyWaitAction | CancelAction | ReconcileUnitAction

# Reducer rejection codes mapped onto the closed rejection-reason set. Anything else that
# the reducer rejects at acceptance is `not_applicable` (the command cannot apply to the
# run as it stands).
_REJECTION_REASONS: dict[str, BoundaryRejectionReason] = {
    "stale_run_version": "stale_version",
    "run_is_terminal": "terminal_run",
    "unauthorized_command": "unauthorized",
    "invalid_pause_authority": "unauthorized",
    "invalid_resume_authority": "unauthorized",
    "budget_hard_cap_exceeded": "insufficient_budget",
    "insufficient_budget": "insufficient_budget",
    "stale_target": "stale_target",
    "stale_generation": "stale_generation",
}


def is_boundary_command(action: object) -> bool:
    return getattr(action, "kind", None) in BOUNDARY_COMMAND_KINDS


def is_family_boundary_command(action: object) -> bool:
    return getattr(action, "kind", None) in FAMILY_BOUNDARY_COMMAND_KINDS


def rejection_reason_for(reason_code: str) -> BoundaryRejectionReason:
    return _REJECTION_REASONS.get(reason_code, "not_applicable")


def boundary_target_for(projection: RunProjection, action: BoundaryAction) -> BoundaryTarget:
    """The boundary a command targets, from the run's declared execution target."""

    if isinstance(action, ReconcileUnitAction):
        return BoundaryTarget(
            kind="unit",
            target_ref=f"{action.unit_key}:gen:{action.execution_generation}",
            execution_epoch=(
                projection.execution_target.execution_epoch
                if projection.execution_target is not None
                else 1
            ),
            execution_generation=action.execution_generation,
            sequence_space=f"unit:{action.unit_key}:gen:{action.execution_generation}",
        )
    target = projection.execution_target
    if target is None:
        return BoundaryTarget(kind="run_control")
    if isinstance(action, CancelAction):
        return BoundaryTarget(
            kind="root" if target.root_workflow_id is not None else "family",
            target_ref=target.root_workflow_id or target.family_workflow_id,
            root_workflow_id=target.root_workflow_id,
            family_workflow_id=target.family_workflow_id,
            execution_epoch=target.execution_epoch,
            execution_generation=target.execution_generation,
            sequence_space=EXECUTION_SEQUENCE_SPACE,
        )
    return BoundaryTarget(
        kind="family",
        target_ref=target.family_workflow_id,
        root_workflow_id=target.root_workflow_id,
        family_workflow_id=target.family_workflow_id,
        execution_epoch=target.execution_epoch,
        execution_generation=target.execution_generation,
        sequence_space=EXECUTION_SEQUENCE_SPACE,
    )


def boundary_command_record(
    command: LifecycleCommand,
    *,
    target: BoundaryTarget,
    target_sequence: int,
    accepted_run_version: int,
) -> BoundaryCommandRecord:
    action = command.action
    if not is_boundary_command(action):
        raise ValueError("only boundary commands get a boundary command record")
    return BoundaryCommandRecord(
        command_id=command.command_id,
        idempotency_issuer=command.idempotency_issuer,
        request_scope=command.request_scope,
        run_id=command.run_id,
        kind=action.kind,
        action=action,
        target=target,
        target_sequence=target_sequence,
        accepted_run_version=accepted_run_version,
        payload_digest=contract_fingerprint(action),
        actor_id=command.actor.actor_id,
        correlation_id=command.correlation_id,
        recorded_at=command.occurred_at,
    )


def receipt(
    command: BoundaryCommandRecord | LifecycleCommand,
    *,
    ordinal: int,
    state: ReceiptState,
    recorded_by: str,
    recorded_at: datetime,
    rejection_reason: BoundaryRejectionReason | None = None,
    detail: str = "",
    transport_ref: str | None = None,
    applied_run_version: int | None = None,
    boundary_state: dict[str, object] | None = None,
) -> BoundaryCommandReceipt:
    return BoundaryCommandReceipt(
        command_id=command.command_id,
        run_id=command.run_id,
        request_scope=command.request_scope,
        ordinal=ordinal,
        state=state,
        recorded_by=recorded_by,
        rejection_reason=rejection_reason,
        detail=detail,
        transport_ref=transport_ref,
        applied_run_version=applied_run_version,
        boundary_state=dict(boundary_state or {}),
        recorded_at=recorded_at,
    )


def run_control_boundary_receipts(
    command: LifecycleCommand,
    *,
    target: BoundaryTarget,
    resulting_run_version: int,
) -> tuple[BoundaryCommandReceipt, ...]:
    """The receipts run control records in one commit when it accepts a boundary command.

    - Run control is itself the boundary (no execution target, or a cancel without a root
      execution to deliver to): `accepted`, `delivered` and `applied` together.
    - A family-targeted command: `accepted` only; delivery and application follow.
    - A cancel with a target: `accepted`; `applied` when the terminal outcome is recorded.
    - `reconcile_unit`: `accepted`; the hint delivery and the operation boundary follow.
    """

    when = command.occurred_at
    accepted = receipt(
        command,
        ordinal=1,
        state=ReceiptState.ACCEPTED,
        recorded_by=RUN_CONTROL_RECORDER,
        recorded_at=when,
    )
    if target.kind != "run_control":
        return (accepted,)
    delivered = receipt(
        command,
        ordinal=2,
        state=ReceiptState.DELIVERED,
        recorded_by=RUN_CONTROL_RECORDER,
        detail="run control is the target boundary: no root execution has started",
        recorded_at=when,
    )
    if isinstance(command.action, CancelAction):
        # Cancellation is applied only when the reducer records the terminal outcome.
        return (accepted, delivered)
    return (
        accepted,
        delivered,
        receipt(
            command,
            ordinal=3,
            state=ReceiptState.APPLIED,
            recorded_by=RUN_CONTROL_RECORDER,
            applied_run_version=resulting_run_version,
            recorded_at=when,
        ),
    )
