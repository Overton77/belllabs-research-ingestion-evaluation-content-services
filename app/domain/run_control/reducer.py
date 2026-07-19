from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from app.domain.control_plane.canonical import sha256_digest
from app.domain.run_control.contracts import (
    AcceptedObligationEvidence,
    AcceptedOutputEvidence,
    AcceptFinalizationPlanAction,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetLedgerEntry,
    BudgetLedgerKind,
    BudgetState,
    CancelAction,
    CommandResult,
    CommandStatus,
    ContinuationProposal,
    DecideContinuationAction,
    DomainEventEnvelope,
    LifecycleCommand,
    LifecycleTransitionRecord,
    PauseAction,
    ProposeContinuationAction,
    RecordFinalizationResultAction,
    RecordObligationEvidenceAction,
    RecordOutputEvidenceAction,
    RecordReadinessAction,
    RecordUsageAction,
    ReserveBudgetAction,
    ResumeAction,
    RunOutcome,
    RunPhase,
    RunProjection,
    SatisfyWaitAction,
    SettlePendingUsageAction,
    SetWaitAction,
    StartAction,
    TerminalizeAction,
)


class ReductionRejected(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class Reduction:
    projection: RunProjection
    budget: BudgetState
    transition: LifecycleTransitionRecord
    result: CommandResult
    ledger_entries: tuple[BudgetLedgerEntry, ...]
    events: tuple[DomainEventEnvelope, ...]


ACTION_PERMISSIONS: dict[str, str] = {
    "start": "workflow_run.start",
    "set_wait": "workflow_run.observe_wait",
    "satisfy_wait": "workflow_run.observe_wait",
    "pause": "workflow_run.pause",
    "resume": "workflow_run.resume",
    "cancel": "workflow_run.cancel",
    "reserve_budget": "workflow_run.reserve_budget",
    "record_usage": "workflow_run.report_usage",
    "settle_pending_usage": "workflow_run.settle_usage",
    "propose_continuation": "workflow_run.propose_continuation",
    "decide_continuation": "workflow_run.decide_continuation",
    "accept_finalization_plan": "workflow_run.accept_finalization",
    "record_finalization_result": "workflow_run.record_finalization",
    "record_obligation_evidence": "workflow_run.accept_obligation_evidence",
    "record_output_evidence": "workflow_run.accept_output_evidence",
    "terminalize": "workflow_run.terminalize",
    "record_readiness": "workflow_run.decide_readiness",
}


def reduce_lifecycle(
    projection: RunProjection,
    budget: BudgetState,
    command: LifecycleCommand,
    command_fingerprint: str,
) -> Reduction:
    if command.run_id != projection.run_id:
        raise ReductionRejected("run_identity_mismatch", "command targets another run")
    if command.expected_run_version != projection.version:
        raise ReductionRejected(
            "stale_run_version",
            f"expected version {command.expected_run_version}, "
            f"current version is {projection.version}",
        )
    permission = ACTION_PERMISSIONS[command.action.kind]
    if permission not in command.actor.permissions:
        raise ReductionRejected("unauthorized_command", f"missing permission: {permission}")
    if projection.phase == RunPhase.TERMINAL and not isinstance(
        command.action, RecordReadinessAction
    ):
        raise ReductionRejected("run_is_terminal", "terminal lifecycle state is immutable")

    action = command.action
    phase = projection.phase
    waits = list(projection.active_waits)
    pauses = list(projection.active_pauses)
    resumes = list(projection.resume_decisions)
    outcome = projection.terminal_outcome
    readiness = list(projection.readiness)
    pending_proposals = list(projection.pending_continuation_proposals)
    accepted_proposals = set(projection.accepted_continuation_proposals)
    finalization_plan = projection.finalization_plan
    finalization_output_refs = projection.finalization_output_refs
    finalization_omission_reason = projection.finalization_omission_reason
    obligation_evidence = list(projection.accepted_obligation_evidence)
    output_evidence = list(projection.accepted_output_evidence)
    evidence_frontier_digest = projection.evidence_frontier_digest
    next_budget = budget
    ledger: list[BudgetLedgerEntry] = []
    event_type = f"workflow_run.{action.kind}"

    if isinstance(action, StartAction):
        if phase != RunPhase.PENDING:
            raise ReductionRejected("invalid_phase", "only a pending run can start")
        phase = RunPhase.ACTIVE
    elif isinstance(action, SetWaitAction):
        if phase not in {RunPhase.ACTIVE, RunPhase.WAITING}:
            raise ReductionRejected("invalid_phase", "waits apply only to active or waiting runs")
        if any(item.condition_id == action.condition.condition_id for item in waits):
            raise ReductionRejected("wait_exists", "wait condition identity already exists")
        waits.append(action.condition)
        phase = _progress_phase(action.runnable_work_remains, waits, pauses)
    elif isinstance(action, SatisfyWaitAction):
        if not any(item.condition_id == action.condition_id for item in waits):
            raise ReductionRejected("wait_not_found", "wait condition is not active")
        waits = [item for item in waits if item.condition_id != action.condition_id]
        phase = _progress_phase(action.runnable_work_remains, waits, pauses)
    elif isinstance(action, PauseAction):
        if phase not in {RunPhase.ACTIVE, RunPhase.WAITING, RunPhase.PAUSED}:
            raise ReductionRejected("invalid_phase", "run cannot be paused from its current phase")
        if action.decision.authority_ref not in command.actor.authority_refs:
            raise ReductionRejected("invalid_pause_authority", "pause authority was not granted")
        if any(item.decision_id == action.decision.decision_id for item in pauses):
            raise ReductionRejected("pause_exists", "pause decision identity already exists")
        pauses.append(action.decision)
        phase = _progress_phase(action.runnable_work_remains, waits, pauses)
    elif isinstance(action, ResumeAction):
        pause = next(
            (item for item in pauses if item.decision_id == action.decision.pause_decision_id),
            None,
        )
        if pause is None:
            raise ReductionRejected("pause_not_found", "resume must reference an active pause")
        if action.decision.authority_ref not in command.actor.authority_refs:
            raise ReductionRejected("invalid_resume_authority", "resume authority was not granted")
        pauses.remove(pause)
        resumes.append(action.decision)
        phase = _progress_phase(action.runnable_work_remains, waits, pauses)
    elif isinstance(action, CancelAction):
        phase = RunPhase.CANCELLING
    elif isinstance(action, ReserveBudgetAction):
        next_budget, entry = _reserve(next_budget, action, command)
        ledger.append(entry)
        pending_proposals = _add_soft_limit_proposal(
            projection.run_id, next_budget, pending_proposals
        )
    elif isinstance(action, RecordUsageAction):
        next_budget, entries = _record_usage(next_budget, action, command)
        ledger.extend(entries)
        pending_proposals = _add_soft_limit_proposal(
            projection.run_id, next_budget, pending_proposals
        )
    elif isinstance(action, SettlePendingUsageAction):
        next_budget, entries = _settle_pending(next_budget, action, command)
        ledger.extend(entries)
        pending_proposals = _add_soft_limit_proposal(
            projection.run_id, next_budget, pending_proposals
        )
    elif isinstance(action, ProposeContinuationAction):
        _validate_continuation_dimensions(next_budget, action.proposal)
        if any(item.proposal_id == action.proposal.proposal_id for item in pending_proposals):
            raise ReductionRejected("continuation_exists", "continuation proposal already exists")
        pending_proposals.append(action.proposal)
    elif isinstance(action, DecideContinuationAction):
        proposal = next(
            (item for item in pending_proposals if item.proposal_id == action.proposal_id),
            None,
        )
        if proposal is None:
            raise ReductionRejected(
                "continuation_not_found", "continuation proposal is not pending"
            )
        if action.authority_ref not in command.actor.authority_refs:
            raise ReductionRejected(
                "invalid_continuation_authority", "continuation decision authority was not granted"
            )
        pending_proposals.remove(proposal)
        if action.accepted:
            accepted_proposals.add(proposal.proposal_id)
            if action.approved_reservation:
                reservation = ReserveBudgetAction(
                    reservation_id=f"continuation:{proposal.proposal_id}",
                    amounts=action.approved_reservation,
                )
                next_budget, entry = _reserve(next_budget, reservation, command)
                ledger.append(entry)
    elif isinstance(action, AcceptFinalizationPlanAction):
        if finalization_plan is not None:
            raise ReductionRejected(
                "finalization_plan_exists", "a finalization plan was already accepted"
            )
        if action.plan.budget_reservation_id not in next_budget.reservations:
            raise ReductionRejected(
                "finalization_budget_missing",
                "finalization requires a dedicated existing reservation",
            )
        if action.plan.deadline <= command.occurred_at:
            raise ReductionRejected(
                "finalization_deadline_elapsed", "finalization deadline has elapsed"
            )
        if action.plan.eligible_evidence_frontier_digest != projection.evidence_frontier_digest:
            raise ReductionRejected(
                "finalization_evidence_mismatch",
                "finalization plan must freeze the current evidence frontier",
            )
        finalization_plan = action.plan
    elif isinstance(action, RecordFinalizationResultAction):
        if finalization_plan is None or action.plan_id != finalization_plan.plan_id:
            raise ReductionRejected(
                "finalization_plan_mismatch", "result does not match the accepted plan"
            )
        if action.operation not in finalization_plan.permitted_operations:
            raise ReductionRejected(
                "finalization_operation_forbidden",
                "operation is outside the accepted finalization plan",
            )
        if action.evidence_frontier_digest != finalization_plan.eligible_evidence_frontier_digest:
            raise ReductionRejected(
                "finalization_evidence_mismatch",
                "finalization cannot use evidence beyond its frozen frontier",
            )
        if command.occurred_at > finalization_plan.deadline and action.omission_reason is None:
            raise ReductionRejected(
                "finalization_timed_out",
                "late finalization may record only a typed omission",
            )
        finalization_output_refs = action.output_refs
        finalization_omission_reason = action.omission_reason
    elif isinstance(action, RecordObligationEvidenceAction):
        if finalization_plan is not None:
            raise ReductionRejected(
                "evidence_frontier_frozen",
                "new obligation evidence is forbidden after finalization freezes the frontier",
            )
        if action.evidence.accepted_by_authority_ref not in command.actor.authority_refs:
            raise ReductionRejected(
                "invalid_evidence_authority",
                "obligation evidence acceptance authority was not granted",
            )
        obligation_evidence = [
            item
            for item in obligation_evidence
            if item.obligation_ref != action.evidence.obligation_ref
        ] + [action.evidence]
        evidence_frontier_digest = _evidence_frontier(
            projection.input_manifest.digest,
            obligation_evidence,
            output_evidence,
        )
    elif isinstance(action, RecordOutputEvidenceAction):
        if finalization_plan is not None:
            raise ReductionRejected(
                "evidence_frontier_frozen",
                "new output evidence is forbidden after finalization freezes the frontier",
            )
        if action.evidence.accepted_by_authority_ref not in command.actor.authority_refs:
            raise ReductionRejected(
                "invalid_output_authority",
                "output evidence acceptance authority was not granted",
            )
        output_evidence = [
            item for item in output_evidence if item.output_ref != action.evidence.output_ref
        ] + [action.evidence]
        evidence_frontier_digest = _evidence_frontier(
            projection.input_manifest.digest,
            obligation_evidence,
            output_evidence,
        )
    elif isinstance(action, TerminalizeAction):
        outcome = _terminal_outcome(projection, next_budget, action)
        phase = RunPhase.TERMINAL
    elif isinstance(action, RecordReadinessAction):
        if phase != RunPhase.TERMINAL:
            raise ReductionRejected(
                "readiness_before_terminal", "readiness decisions do not terminalize a run"
            )
        readiness.append(action.decision)
    else:  # pragma: no cover - discriminated contracts make this unreachable
        raise AssertionError(f"unsupported action: {type(action).__name__}")

    version = projection.version + 1
    next_projection = projection.model_copy(
        update={
            "version": version,
            "phase": phase,
            "active_waits": tuple(waits),
            "active_pauses": tuple(pauses),
            "resume_decisions": tuple(resumes),
            "terminal_outcome": outcome,
            "readiness": tuple(readiness),
            "pending_continuation_proposals": tuple(pending_proposals),
            "accepted_continuation_proposals": frozenset(accepted_proposals),
            "finalization_plan": finalization_plan,
            "finalization_output_refs": finalization_output_refs,
            "finalization_omission_reason": finalization_omission_reason,
            "accepted_obligation_evidence": tuple(obligation_evidence),
            "accepted_output_evidence": tuple(output_evidence),
            "evidence_frontier_digest": evidence_frontier_digest,
            "updated_at": command.occurred_at,
        }
    )
    next_projection = RunProjection.model_validate(next_projection.model_dump(mode="python"))
    transition = LifecycleTransitionRecord(
        transition_id=_stable_id("transition", projection.run_id, str(version)),
        run_id=projection.run_id,
        command_id=command.command_id,
        prior_version=projection.version,
        resulting_version=version,
        prior_phase=projection.phase,
        resulting_phase=phase,
        prior_projection=projection,
        resulting_projection=next_projection,
        actor=command.actor,
        reason=command.reason,
        evidence_refs=command.evidence_refs,
        occurred_at=command.occurred_at,
        correlation_id=command.correlation_id,
        causation_id=command.causation_id,
    )
    result = CommandResult(
        command_id=command.command_id,
        idempotency_issuer=command.idempotency_issuer,
        run_id=projection.run_id,
        command_fingerprint=command_fingerprint,
        status=CommandStatus.ACCEPTED,
        resulting_run_version=version,
        phase=phase,
        terminal_outcome=outcome,
        reason_code="accepted",
        reason="lifecycle command accepted",
        recorded_at=command.occurred_at,
    )
    event = DomainEventEnvelope(
        event_id=_stable_id("event", projection.run_id, str(version), event_type),
        event_type=event_type,
        aggregate_id=projection.run_id,
        aggregate_version=version,
        sequence=1,
        occurred_at=command.occurred_at,
        recorded_at=command.occurred_at,
        actor=command.actor,
        correlation_id=command.correlation_id,
        causation_id=command.causation_id or command.command_id,
        payload={
            "command_id": command.command_id,
            "prior_phase": projection.phase.value,
            "resulting_phase": phase.value,
            "terminal_outcome": outcome.value if outcome else None,
        },
    )
    return Reduction(
        projection=next_projection,
        budget=next_budget,
        transition=transition,
        result=result,
        ledger_entries=tuple(ledger),
        events=(event,),
    )


def _progress_phase(
    runnable_work_remains: bool, waits: Sequence[object], pauses: Sequence[object]
) -> RunPhase:
    if runnable_work_remains:
        return RunPhase.ACTIVE
    if pauses:
        return RunPhase.PAUSED
    if waits:
        return RunPhase.WAITING
    return RunPhase.ACTIVE


def _limits(state: BudgetState) -> dict[str, BudgetDimensionLimit]:
    return {item.dimension: item for item in state.limits}


def _validate_amounts(state: BudgetState, amounts: dict[str, int]) -> None:
    limits = _limits(state)
    for dimension, amount in amounts.items():
        if amount < 0:
            raise ReductionRejected("invalid_budget_amount", "budget amounts cannot be negative")
        limit = limits.get(dimension)
        if limit is None:
            raise ReductionRejected(
                "undeclared_budget_dimension", f"undeclared dimension: {dimension}"
            )
        if limit.applicability == BudgetApplicability.NOT_APPLICABLE:
            raise ReductionRejected(
                "budget_not_applicable", f"dimension is not applicable: {dimension}"
            )


def _enforce_hard_caps(state: BudgetState) -> None:
    for limit in state.limits:
        if limit.hard_cap is None:
            continue
        total = (
            state.reserved.get(limit.dimension, 0)
            + state.consumed.get(limit.dimension, 0)
            + state.pending_settlement.get(limit.dimension, 0)
        )
        if total > limit.hard_cap:
            raise ReductionRejected(
                "budget_hard_cap_exceeded",
                f"hard cap exceeded independently for {limit.dimension}",
            )


def _reserve(
    state: BudgetState, action: ReserveBudgetAction, command: LifecycleCommand
) -> tuple[BudgetState, BudgetLedgerEntry]:
    _validate_amounts(state, action.amounts)
    if action.reservation_id in state.reservations:
        raise ReductionRejected("reservation_exists", "reservation identity already exists")
    reserved = dict(state.reserved)
    for dimension, amount in action.amounts.items():
        reserved[dimension] = reserved.get(dimension, 0) + amount
    reservations = dict(state.reservations)
    reservations[action.reservation_id] = dict(action.amounts)
    updated = state.model_copy(update={"reserved": reserved, "reservations": reservations})
    _enforce_hard_caps(updated)
    return updated, _ledger_entry(
        state, BudgetLedgerKind.RESERVATION, action.reservation_id, action.amounts, command
    )


def _record_usage(
    state: BudgetState, action: RecordUsageAction, command: LifecycleCommand
) -> tuple[BudgetState, tuple[BudgetLedgerEntry, ...]]:
    all_amounts = {
        **action.actual_amounts,
        **{
            dimension: max(
                action.actual_amounts.get(dimension, 0),
                action.pending_external_amounts.get(dimension, 0),
                action.release_amounts.get(dimension, 0),
            )
            for dimension in (
                action.actual_amounts.keys()
                | action.pending_external_amounts.keys()
                | action.release_amounts.keys()
            )
        },
    }
    _validate_amounts(state, all_amounts)
    if action.usage_id in state.usage_ids:
        raise ReductionRejected("usage_exists", "usage identity already exists")
    if action.reservation_id is None or action.reservation_id not in state.reservations:
        raise ReductionRejected(
            "reservation_required", "observed usage must reconcile an existing reservation"
        )
    consumed = dict(state.consumed)
    pending = dict(state.pending_settlement)
    reserved = dict(state.reserved)
    reservations = dict(state.reservations)
    reservation = reservations[action.reservation_id]
    draw: dict[str, int] = {}
    for dimension in all_amounts:
        actual = action.actual_amounts.get(dimension, 0)
        pending_amount = action.pending_external_amounts.get(dimension, 0)
        release = action.release_amounts.get(dimension, 0)
        draw[dimension] = actual + pending_amount + release
        if release > max(reservation.get(dimension, 0) - actual - pending_amount, 0):
            raise ReductionRejected(
                "release_exceeds_reservation",
                f"release exceeds unused reservation for {dimension}",
            )
        consumed[dimension] = consumed.get(dimension, 0) + actual
        pending[dimension] = pending.get(dimension, 0) + pending_amount
        reserved_draw = min(draw[dimension], reservation.get(dimension, 0))
        reserved[dimension] = reserved.get(dimension, 0) - reserved_draw
    remainder = {
        dimension: amount - min(draw.get(dimension, 0), amount)
        for dimension, amount in reservation.items()
        if amount - min(draw.get(dimension, 0), amount) > 0
    }
    if remainder:
        reservations[action.reservation_id] = remainder
    else:
        reservations.pop(action.reservation_id, None)
    updated = state.model_copy(
        update={
            "consumed": consumed,
            "pending_settlement": pending,
            "reserved": reserved,
            "reservations": reservations,
            "usage_ids": state.usage_ids | {action.usage_id},
        }
    )
    entries = [
        _ledger_entry(
            state, BudgetLedgerKind.CONSUMPTION, action.usage_id, action.actual_amounts, command
        )
    ]
    if action.pending_external_amounts:
        entries.append(
            _ledger_entry(
                state,
                BudgetLedgerKind.PENDING_SETTLEMENT,
                action.usage_id,
                action.pending_external_amounts,
                command,
            )
        )
    if action.release_amounts:
        entries.append(
            _ledger_entry(
                state,
                BudgetLedgerKind.RELEASE,
                action.usage_id,
                action.release_amounts,
                command,
            )
        )
    return updated, tuple(entries)


def _settle_pending(
    state: BudgetState, action: SettlePendingUsageAction, command: LifecycleCommand
) -> tuple[BudgetState, tuple[BudgetLedgerEntry, ...]]:
    _validate_amounts(state, action.actual_amounts | action.pending_release_amounts)
    if action.settlement_id in state.settlement_ids:
        raise ReductionRejected("settlement_exists", "settlement identity already exists")
    pending = dict(state.pending_settlement)
    consumed = dict(state.consumed)
    for dimension in action.actual_amounts.keys() | action.pending_release_amounts.keys():
        actual = action.actual_amounts.get(dimension, 0)
        release = action.pending_release_amounts.get(dimension, 0)
        if actual + release > pending.get(dimension, 0):
            raise ReductionRejected(
                "settlement_exceeds_pending", f"settlement exceeds pending amount for {dimension}"
            )
        pending[dimension] = pending.get(dimension, 0) - actual - release
        consumed[dimension] = consumed.get(dimension, 0) + actual
    updated = state.model_copy(
        update={
            "pending_settlement": pending,
            "consumed": consumed,
            "settlement_ids": state.settlement_ids | {action.settlement_id},
        }
    )
    entries = [
        _ledger_entry(
            state, BudgetLedgerKind.SETTLEMENT, action.settlement_id, action.actual_amounts, command
        )
    ]
    if action.pending_release_amounts:
        entries.append(
            _ledger_entry(
                state,
                BudgetLedgerKind.RELEASE,
                action.settlement_id,
                action.pending_release_amounts,
                command,
            )
        )
    return updated, tuple(entries)


def _validate_continuation_dimensions(state: BudgetState, proposal: ContinuationProposal) -> None:
    declared = {item.dimension for item in state.limits}
    if not proposal.triggered_dimensions <= declared:
        raise ReductionRejected(
            "undeclared_budget_dimension", "continuation references undeclared dimensions"
        )
    _validate_amounts(state, proposal.requested_reservation)


def _add_soft_limit_proposal(
    run_id: str,
    state: BudgetState,
    proposals: list[ContinuationProposal],
) -> list[ContinuationProposal]:
    existing_dimensions = {
        dimension for proposal in proposals for dimension in proposal.triggered_dimensions
    }
    triggered = {
        limit.dimension
        for limit in state.limits
        if limit.soft_limit is not None
        and limit.dimension not in existing_dimensions
        and (
            state.reserved.get(limit.dimension, 0)
            + state.consumed.get(limit.dimension, 0)
            + state.pending_settlement.get(limit.dimension, 0)
        )
        >= limit.soft_limit
    }
    if not triggered:
        return proposals
    proposal_id = _stable_id("continuation", run_id, *sorted(triggered))
    return proposals + [
        ContinuationProposal(
            proposal_id=proposal_id,
            triggered_dimensions=frozenset(triggered),
            action="reduce_effort",
            reason="one or more independent budget soft limits were reached",
        )
    ]


def _terminal_outcome(
    projection: RunProjection, budget: BudgetState, action: TerminalizeAction
) -> RunOutcome:
    proposal = action.proposal
    if projection.phase == RunPhase.PENDING:
        raise ReductionRejected("invalid_phase", "a pending run cannot terminalize")
    if projection.active_pauses and projection.phase != RunPhase.CANCELLING:
        raise ReductionRejected(
            "active_pause",
            "non-cancellation terminalization requires every pause to be resumed",
        )
    if proposal.obligation_revision != projection.obligation_revision:
        raise ReductionRejected(
            "stale_obligation_revision", "proposal obligation revision is stale"
        )
    if proposal.evidence_frontier_digest != projection.evidence_frontier_digest:
        raise ReductionRejected("stale_evidence_frontier", "proposal evidence frontier is stale")
    evidence_payload = [
        item.model_dump(mode="json")
        for item in sorted(
            projection.accepted_obligation_evidence,
            key=lambda item: item.obligation_ref,
        )
    ]
    if proposal.accepted_obligation_evidence_digest != sha256_digest(evidence_payload):
        raise ReductionRejected(
            "obligation_evidence_mismatch",
            "proposal does not bind the current accepted obligation evidence",
        )
    accepted_refs = {item.obligation_ref for item in projection.accepted_obligation_evidence}
    required_accepted = projection.required_obligation_refs <= accepted_refs
    if proposal.required_obligations_accepted != required_accepted:
        raise ReductionRejected(
            "obligation_acceptance_mismatch",
            "proposal obligation acceptance does not match authoritative evidence",
        )
    if proposal.pending_wait_or_link_ids or projection.active_waits:
        raise ReductionRejected("unresolved_terminal_dependencies", "terminal dependencies remain")
    if proposal.finalization_plan != projection.finalization_plan:
        raise ReductionRejected(
            "finalization_plan_mismatch",
            "terminalization must bind the accepted finalization plan exactly",
        )
    authoritative_outputs = {item.output_ref for item in projection.accepted_output_evidence} | set(
        projection.finalization_output_refs
    )
    if set(proposal.valid_output_refs) != authoritative_outputs:
        raise ReductionRejected(
            "terminal_output_mismatch",
            "terminalization outputs must equal authoritatively accepted outputs",
        )
    if projection.finalization_plan is not None and (
        proposal.output_omission_reason != projection.finalization_omission_reason
    ):
        raise ReductionRejected(
            "finalization_omission_mismatch",
            "terminalization omission must match the recorded finalization result",
        )
    if projection.finalization_plan is not None and (
        not projection.finalization_output_refs
        and projection.finalization_omission_reason is None
        and proposal.output_omission_reason is None
    ):
        raise ReductionRejected(
            "finalization_incomplete",
            "finalization must record bounded outputs or a typed omission",
        )
    budget_is_settled = not any(budget.reserved.values()) and not any(
        budget.pending_settlement.values()
    )
    if not proposal.budget_settled or not budget_is_settled:
        raise ReductionRejected("budget_not_settled", "budget reservations or charges remain")
    if projection.phase == RunPhase.CANCELLING:
        if not proposal.cancellation_settled:
            raise ReductionRejected("cancellation_not_settled", "cancellation effects remain")
        return RunOutcome.CANCELLED
    if proposal.execution_failure_refs:
        return RunOutcome.FAILED
    if not proposal.required_obligations_accepted:
        return RunOutcome.FAILED
    if proposal.degradable_failures:
        if not proposal.valid_output_refs:
            return RunOutcome.FAILED
        return RunOutcome.PARTIALLY_COMPLETED
    return RunOutcome.COMPLETED


def _ledger_entry(
    state: BudgetState,
    kind: BudgetLedgerKind,
    idempotency_id: str,
    amounts: dict[str, int],
    command: LifecycleCommand,
) -> BudgetLedgerEntry:
    return BudgetLedgerEntry(
        entry_id=_stable_id("ledger", state.account_id, kind.value, idempotency_id),
        account_id=state.account_id,
        run_id=state.run_id,
        kind=kind,
        idempotency_id=idempotency_id,
        amounts=amounts,
        occurred_at=command.occurred_at,
        parent_account_id=state.parent_account_id,
    )


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))


def _evidence_frontier(
    input_manifest_digest: str,
    obligation_evidence: list[AcceptedObligationEvidence],
    output_evidence: list[AcceptedOutputEvidence],
) -> str:
    return sha256_digest(
        {
            "input_manifest_digest": input_manifest_digest,
            "obligation_evidence": [
                item.model_dump(mode="json")
                for item in sorted(obligation_evidence, key=lambda item: item.obligation_ref)
            ],
            "output_evidence": [
                item.model_dump(mode="json")
                for item in sorted(output_evidence, key=lambda item: item.output_ref)
            ],
        }
    )
