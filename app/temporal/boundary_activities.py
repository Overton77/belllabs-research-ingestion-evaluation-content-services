"""The family boundaries' run-control facts as one Temporal activity body (RRM-007).

Both families register `<family>.apply_boundary_command` over this function. A request
either applies a lifecycle action (`apply_boundary_command`, `set_wait`,
`observe_quiescence`, a policy `pause`) or records that the boundary could not apply a
delivered command (`rejection_reason`). The application service binds the current run
version, so version drift from pending commands never fails a family (REQ-CP-RUN-004).
"""

from __future__ import annotations

from datetime import UTC, datetime

from temporalio.exceptions import ApplicationError

from app.application.run_control.boundary_interventions import (
    BoundaryApplicationRejected,
    BoundaryCommandApplicationService,
    BoundaryFactStale,
)
from app.domain.orchestration.contracts import (
    BoundaryLifecycleOutcome,
    BoundaryLifecycleRequest,
)
from app.domain.run_control.boundary_commands import receipt as boundary_receipt
from app.domain.run_control.contracts import CommandStatus, ReceiptState
from app.domain.run_control.errors import ReceiptTransitionRejected


async def apply_boundary_fact(
    service: BoundaryCommandApplicationService, request: BoundaryLifecycleRequest
) -> BoundaryLifecycleOutcome:
    if not all((request.run_id, request.request_scope, request.idempotency_issuer)):
        raise ApplicationError(
            "boundary fact is missing its run-scoped binding",
            type="boundary_fact_unbound",
            non_retryable=True,
        )
    when = request.occurred_at or datetime.now(UTC)
    if request.rejection_reason:
        return await _record_rejection(service, request, when)
    try:
        result = await service.execute(
            request_scope=request.request_scope,
            run_id=request.run_id,
            command_id=request.command_id,
            idempotency_issuer=request.idempotency_issuer,
            correlation_id=request.correlation_id or request.command_id,
            action=request.action,
            reason=request.reason,
            evidence_refs=request.evidence_refs,
            occurred_at=when,
        )
    except BoundaryApplicationRejected as error:
        raise ApplicationError(
            str(error), type="boundary_fact_rejected", non_retryable=True
        ) from error
    except BoundaryFactStale as error:
        # Retryable: no result was stored, so the next attempt binds the new version.
        raise ApplicationError(str(error), type="boundary_fact_stale") from error
    except ReceiptTransitionRejected as error:
        # F8: the ledger refused the transition (for example the command was already closed
        # by the terminal outcome): a deterministic outcome, not a workflow failure.
        return await _current_outcome(service, request, reason_code=error.code)
    receipt_state, sequence = "", 0
    if request.boundary_command_id:
        receipt_state, sequence = await service.boundary_receipt_state(
            request.request_scope,
            request.run_id,
            request.boundary_command_issuer,
            request.boundary_command_id,
        )
    return BoundaryLifecycleOutcome(
        accepted=result.status == CommandStatus.ACCEPTED,
        status=result.status.value,
        reason_code=result.reason_code,
        resulting_run_version=result.resulting_run_version,
        phase=result.phase.value,
        receipt_state=receipt_state,
        target_sequence=sequence,
    )


async def _record_rejection(
    service: BoundaryCommandApplicationService,
    request: BoundaryLifecycleRequest,
    when: datetime,
) -> BoundaryLifecycleOutcome:
    """The boundary could not apply the delivered command: terminal `rejected` receipt."""

    status = await service.run_control.get_boundary_command(
        request.request_scope,
        request.run_id,
        request.boundary_command_issuer,
        request.boundary_command_id,
    )
    if status is None:
        raise ApplicationError(
            f"boundary command {request.boundary_command_id} is unknown to run control",
            type="boundary_command_not_found",
            non_retryable=True,
        )
    try:
        if status.state == ReceiptState.ACCEPTED:
            status = await service.record_receipt(
                request.request_scope,
                boundary_receipt(
                    status.command,
                    ordinal=1,
                    state=ReceiptState.DELIVERED,
                    recorded_by=request.boundary_ref,
                    detail="delivery acknowledged by the applying boundary",
                    transport_ref=request.boundary_ref,
                    recorded_at=when,
                ),
            )
        if status.state == ReceiptState.DELIVERED:
            status = await service.record_receipt(
                request.request_scope,
                boundary_receipt(
                    status.command,
                    ordinal=1,
                    state=ReceiptState.REJECTED,
                    recorded_by=request.boundary_ref,
                    rejection_reason=request.rejection_reason,  # type: ignore[arg-type]
                    detail=request.reason,
                    transport_ref=request.boundary_ref,
                    recorded_at=when,
                ),
            )
    except ReceiptTransitionRejected as error:
        return await _current_outcome(service, request, reason_code=error.code)
    run = await service.run_control.get_run(request.request_scope, request.run_id)
    return BoundaryLifecycleOutcome(
        accepted=False,
        status="rejected",
        reason_code=request.rejection_reason,
        resulting_run_version=run.version,
        phase=run.phase.value,
        receipt_state=status.state.value,
        target_sequence=status.command.target_sequence,
    )


async def _current_outcome(
    service: BoundaryCommandApplicationService,
    request: BoundaryLifecycleRequest,
    *,
    reason_code: str,
) -> BoundaryLifecycleOutcome:
    """The ledger's current state for the command, reported without applying anything."""

    run = await service.run_control.get_run(request.request_scope, request.run_id)
    receipt_state, sequence = "", 0
    if request.boundary_command_id:
        receipt_state, sequence = await service.boundary_receipt_state(
            request.request_scope,
            request.run_id,
            request.boundary_command_issuer,
            request.boundary_command_id,
        )
    return BoundaryLifecycleOutcome(
        accepted=False,
        status="rejected",
        reason_code=reason_code,
        resulting_run_version=run.version,
        phase=run.phase.value,
        receipt_state=receipt_state,
        target_sequence=sequence,
    )
