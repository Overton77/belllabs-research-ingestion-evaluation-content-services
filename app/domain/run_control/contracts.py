from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    model_validator,
)

from app.domain.control_plane.contracts import ExactDefinitionRef, RunInputManifestRef

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    @model_validator(mode="before")
    @classmethod
    def bound_persisted_payloads(cls, value: object) -> object:
        nodes = 0

        def inspect(item: object, depth: int = 0) -> None:
            nonlocal nodes
            nodes += 1
            if nodes > 20_000 or depth > 32:
                raise ValueError("run-control payload exceeds structural limits")
            if isinstance(item, str) and len(item) > 8_192:
                raise ValueError("run-control strings cannot exceed 8192 characters")
            if isinstance(item, int) and not -(2**63) <= item < 2**63:
                raise ValueError("run-control integers must fit signed 64-bit storage")
            if isinstance(item, dict):
                if len(item) > 1_024:
                    raise ValueError("run-control mappings cannot exceed 1024 entries")
                for key, nested in item.items():
                    inspect(key, depth + 1)
                    inspect(nested, depth + 1)
            elif isinstance(item, list | tuple | set | frozenset):
                if len(item) > 1_024:
                    raise ValueError("run-control collections cannot exceed 1024 entries")
                for nested in item:
                    inspect(nested, depth + 1)

        inspect(value)
        return value


class RunPhase(StrEnum):
    PENDING = "pending"
    ACTIVE = "active"
    WAITING = "waiting"
    PAUSED = "paused"
    CANCELLING = "cancelling"
    TERMINAL = "terminal"


class RunOutcome(StrEnum):
    COMPLETED = "completed"
    PARTIALLY_COMPLETED = "partially_completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class DecisionStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"


class CommandStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    STALE = "stale"


class BudgetApplicability(StrEnum):
    BOUNDED = "bounded"
    UNBOUNDED = "unbounded"
    NOT_APPLICABLE = "not_applicable"


class BudgetLedgerKind(StrEnum):
    RESERVATION = "reservation"
    CONSUMPTION = "consumption"
    RELEASE = "release"
    PENDING_SETTLEMENT = "pending_settlement"
    SETTLEMENT = "settlement"
    ADJUSTMENT = "adjustment"


class ReadinessStatus(StrEnum):
    READY = "ready"
    CONDITIONALLY_READY = "conditionally_ready"
    NOT_READY = "not_ready"


class ConsumerApplyStatus(StrEnum):
    APPLIED = "applied"
    DUPLICATE = "duplicate"
    GAP = "gap"


class ActorContext(Contract):
    actor_id: str = Field(min_length=1)
    authority_refs: frozenset[str] = Field(default_factory=frozenset)
    permissions: frozenset[str] = Field(default_factory=frozenset)


class BudgetDimensionLimit(Contract):
    dimension: str = Field(min_length=1, pattern=r"^[a-z][a-z0-9_.:-]*$")
    applicability: BudgetApplicability
    soft_limit: int | None = Field(default=None, ge=0)
    hard_cap: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def limits_match_applicability(self) -> BudgetDimensionLimit:
        if self.applicability == BudgetApplicability.BOUNDED:
            if self.hard_cap is None:
                raise ValueError("bounded budget dimensions require a hard cap")
            if self.soft_limit is not None and self.soft_limit > self.hard_cap:
                raise ValueError("soft limit cannot exceed hard cap")
        elif self.soft_limit is not None or self.hard_cap is not None:
            raise ValueError("unbounded and not-applicable dimensions cannot carry limits")
        return self


class BudgetEnvelope(Contract):
    dimensions: tuple[BudgetDimensionLimit, ...] = Field(min_length=1)
    baseline_reservations: dict[str, int] = Field(default_factory=dict)
    parent_account_id: str | None = None

    @model_validator(mode="after")
    def dimensions_are_complete_and_unique(self) -> BudgetEnvelope:
        limits = {item.dimension: item for item in self.dimensions}
        if len(limits) != len(self.dimensions):
            raise ValueError("budget dimension declarations must be unique")
        for dimension, amount in self.baseline_reservations.items():
            if amount < 0 or dimension not in limits:
                raise ValueError("baseline reservations require declared dimensions")
            limit = limits[dimension]
            if limit.applicability == BudgetApplicability.NOT_APPLICABLE:
                raise ValueError("not-applicable dimensions cannot be reserved")
            if limit.hard_cap is not None and amount > limit.hard_cap:
                raise ValueError("baseline reservation exceeds hard cap")
        return self


class RunRequest(Contract):
    schema_version: Literal["1"] = "1"
    request_scope: str = Field(min_length=1)
    idempotency_issuer: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    actor: ActorContext
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    workflow_type_ref: ExactDefinitionRef
    input_manifest: RunInputManifestRef
    budget_envelope: BudgetEnvelope
    requested_at: AwareDatetime
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = None
    parent_run_id: str | None = None
    sponsorship_ref: str = Field(min_length=1)
    approval_refs: tuple[str, ...] = ()
    delegation_authority_refs: frozenset[str] = Field(default_factory=frozenset)
    admission_evidence_refs: tuple[str, ...] = ()

    @model_validator(mode="after")
    def parent_binding_is_complete(self) -> RunRequest:
        has_parent_run = self.parent_run_id is not None
        has_parent_account = self.budget_envelope.parent_account_id is not None
        if has_parent_run != has_parent_account:
            raise ValueError("parent_run_id and budget parent_account_id must be provided together")
        return self


class VerifiedRunConfiguration(Contract):
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    workflow_type_ref: ExactDefinitionRef
    input_manifest: RunInputManifestRef
    effective_budget_ceilings: dict[str, int]
    max_concurrency: int = Field(ge=1)
    input_admission_contract: str = Field(min_length=1)
    invariant_refs: frozenset[str] = Field(min_length=1)
    obligation_revision: str = Field(min_length=1)
    required_obligation_refs: frozenset[str] = Field(default_factory=frozenset)


class AdmissionDecision(Contract):
    schema_version: Literal["1"] = "1"
    request_scope: str
    idempotency_issuer: str
    request_id: str
    request_fingerprint: str = Field(pattern=DIGEST_PATTERN)
    status: DecisionStatus
    run_id: str | None = None
    reason_code: str
    reason: str
    recorded_at: AwareDatetime

    @model_validator(mode="after")
    def accepted_decisions_have_runs(self) -> AdmissionDecision:
        if (self.status == DecisionStatus.ACCEPTED) != (self.run_id is not None):
            raise ValueError("only accepted admission decisions have a run id")
        return self


class WaitCondition(Contract):
    condition_id: str = Field(min_length=1)
    kind: Literal["dependency", "timer", "approval", "resource", "budget", "external_result"]
    scope: frozenset[str] = Field(min_length=1)
    verification_ref: str = Field(min_length=1)
    timeout_policy_ref: str = Field(min_length=1)


class PauseDecision(Contract):
    decision_id: str = Field(min_length=1)
    scope: frozenset[str] = Field(min_length=1)
    reason: str = Field(min_length=1)
    authority_ref: str = Field(min_length=1)
    reconsideration_conditions: tuple[str, ...] = ()


class ResumeDecision(Contract):
    decision_id: str = Field(min_length=1)
    pause_decision_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    authority_ref: str = Field(min_length=1)


class OutputReadinessDecision(Contract):
    decision_id: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    status: ReadinessStatus
    output_refs: tuple[str, ...]
    reason: str = Field(min_length=1)
    decided_at: AwareDatetime


class FinalizationPlan(Contract):
    plan_id: str = Field(min_length=1)
    eligible_evidence_frontier_digest: str = Field(pattern=DIGEST_PATTERN)
    permitted_operations: frozenset[
        Literal["assemble_existing_output", "validate_existing_output", "write_omission_report"]
    ]
    budget_reservation_id: str = Field(min_length=1)
    side_effect_allowlist: frozenset[str] = Field(default_factory=frozenset)
    deadline: AwareDatetime
    omission_reason_contract: str = Field(min_length=1)


class TerminalizationProposal(Contract):
    proposal_id: str = Field(min_length=1)
    obligation_revision: str = Field(min_length=1)
    evidence_frontier_digest: str = Field(pattern=DIGEST_PATTERN)
    accepted_obligation_evidence_digest: str = Field(pattern=DIGEST_PATTERN)
    proposing_execution_binding_ref: str = Field(min_length=1)
    required_obligations_accepted: bool
    degradable_failures: tuple[str, ...] = ()
    valid_output_refs: tuple[str, ...] = ()
    cancellation_settled: bool = False
    budget_settled: bool
    pending_wait_or_link_ids: tuple[str, ...] = ()
    proposed_at: AwareDatetime
    finalization_plan: FinalizationPlan | None = None
    output_omission_reason: str | None = None


class ContinuationProposal(Contract):
    proposal_id: str = Field(min_length=1)
    triggered_dimensions: frozenset[str] = Field(min_length=1)
    action: Literal[
        "continue_unchanged",
        "reduce_effort",
        "skip_degradable_work",
        "request_additional_reservation",
        "terminate",
    ]
    requested_reservation: dict[str, int] = Field(default_factory=dict)
    reason: str = Field(min_length=1)


class StartAction(Contract):
    kind: Literal["start"] = "start"


class SetWaitAction(Contract):
    kind: Literal["set_wait"] = "set_wait"
    condition: WaitCondition
    runnable_work_remains: bool


class SatisfyWaitAction(Contract):
    kind: Literal["satisfy_wait"] = "satisfy_wait"
    condition_id: str = Field(min_length=1)
    verification_evidence_ref: str = Field(min_length=1)
    runnable_work_remains: bool = True


class PauseAction(Contract):
    kind: Literal["pause"] = "pause"
    decision: PauseDecision
    runnable_work_remains: bool


class ResumeAction(Contract):
    kind: Literal["resume"] = "resume"
    decision: ResumeDecision
    runnable_work_remains: bool = True


class CancelAction(Contract):
    kind: Literal["cancel"] = "cancel"


class ReserveBudgetAction(Contract):
    kind: Literal["reserve_budget"] = "reserve_budget"
    reservation_id: str = Field(min_length=1)
    amounts: dict[str, int]
    parent_reservation_id: str | None = None


class RecordUsageAction(Contract):
    kind: Literal["record_usage"] = "record_usage"
    usage_id: str = Field(min_length=1)
    actual_amounts: dict[str, int]
    reservation_id: str | None = None
    release_amounts: dict[str, int] = Field(default_factory=dict)
    pending_external_amounts: dict[str, int] = Field(default_factory=dict)


class SettlePendingUsageAction(Contract):
    kind: Literal["settle_pending_usage"] = "settle_pending_usage"
    settlement_id: str = Field(min_length=1)
    actual_amounts: dict[str, int]
    pending_release_amounts: dict[str, int] = Field(default_factory=dict)


class ProposeContinuationAction(Contract):
    kind: Literal["propose_continuation"] = "propose_continuation"
    proposal: ContinuationProposal


class DecideContinuationAction(Contract):
    kind: Literal["decide_continuation"] = "decide_continuation"
    proposal_id: str = Field(min_length=1)
    accepted: bool
    approved_reservation: dict[str, int] = Field(default_factory=dict)
    authority_ref: str = Field(min_length=1)


class TerminalizeAction(Contract):
    kind: Literal["terminalize"] = "terminalize"
    proposal: TerminalizationProposal


class AcceptFinalizationPlanAction(Contract):
    kind: Literal["accept_finalization_plan"] = "accept_finalization_plan"
    plan: FinalizationPlan


class RecordFinalizationResultAction(Contract):
    kind: Literal["record_finalization_result"] = "record_finalization_result"
    plan_id: str = Field(min_length=1)
    operation: Literal[
        "assemble_existing_output", "validate_existing_output", "write_omission_report"
    ]
    evidence_frontier_digest: str = Field(pattern=DIGEST_PATTERN)
    output_refs: tuple[str, ...] = ()
    omission_reason: str | None = None

    @model_validator(mode="after")
    def result_or_omission_is_present(self) -> RecordFinalizationResultAction:
        if not self.output_refs and self.omission_reason is None:
            raise ValueError("finalization must record outputs or an omission reason")
        return self


class AcceptedObligationEvidence(Contract):
    obligation_ref: str = Field(min_length=1)
    evidence_digest: str = Field(pattern=DIGEST_PATTERN)
    accepted_by_authority_ref: str = Field(min_length=1)


class RecordObligationEvidenceAction(Contract):
    kind: Literal["record_obligation_evidence"] = "record_obligation_evidence"
    evidence: AcceptedObligationEvidence


class AcceptedOutputEvidence(Contract):
    output_ref: str = Field(min_length=1)
    evidence_digest: str = Field(pattern=DIGEST_PATTERN)
    accepted_by_authority_ref: str = Field(min_length=1)


class RecordOutputEvidenceAction(Contract):
    kind: Literal["record_output_evidence"] = "record_output_evidence"
    evidence: AcceptedOutputEvidence


class RecordReadinessAction(Contract):
    kind: Literal["record_readiness"] = "record_readiness"
    decision: OutputReadinessDecision


LifecycleAction = Annotated[
    StartAction
    | SetWaitAction
    | SatisfyWaitAction
    | PauseAction
    | ResumeAction
    | CancelAction
    | ReserveBudgetAction
    | RecordUsageAction
    | SettlePendingUsageAction
    | ProposeContinuationAction
    | DecideContinuationAction
    | AcceptFinalizationPlanAction
    | RecordFinalizationResultAction
    | RecordObligationEvidenceAction
    | RecordOutputEvidenceAction
    | TerminalizeAction
    | RecordReadinessAction,
    Field(discriminator="kind"),
]


class LifecycleCommand(Contract):
    schema_version: Literal["1"] = "1"
    command_id: str = Field(min_length=1)
    idempotency_issuer: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    expected_run_version: int = Field(ge=1)
    actor: ActorContext
    action: LifecycleAction
    reason: str = Field(min_length=1)
    evidence_refs: tuple[str, ...] = ()
    occurred_at: AwareDatetime
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = None


class RunProjection(Contract):
    schema_version: Literal["1"] = "1"
    run_id: str
    request_scope: str
    idempotency_issuer: str
    request_id: str
    version: int = Field(ge=1)
    phase: RunPhase
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    workflow_type_ref: ExactDefinitionRef
    input_manifest: RunInputManifestRef
    active_waits: tuple[WaitCondition, ...] = ()
    active_pauses: tuple[PauseDecision, ...] = ()
    resume_decisions: tuple[ResumeDecision, ...] = ()
    terminal_outcome: RunOutcome | None = None
    readiness: tuple[OutputReadinessDecision, ...] = ()
    obligation_revision: str
    required_obligation_refs: frozenset[str] = Field(default_factory=frozenset)
    accepted_obligation_evidence: tuple[AcceptedObligationEvidence, ...] = ()
    accepted_output_evidence: tuple[AcceptedOutputEvidence, ...] = ()
    evidence_frontier_digest: str = Field(pattern=DIGEST_PATTERN)
    accepted_continuation_proposals: frozenset[str] = Field(default_factory=frozenset)
    pending_continuation_proposals: tuple[ContinuationProposal, ...] = ()
    finalization_plan: FinalizationPlan | None = None
    finalization_output_refs: tuple[str, ...] = ()
    finalization_omission_reason: str | None = None
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def terminal_axes_are_consistent(self) -> RunProjection:
        if (self.phase == RunPhase.TERMINAL) != (self.terminal_outcome is not None):
            raise ValueError("terminal outcome exists exactly when phase is terminal")
        return self


class BudgetState(Contract):
    account_id: str
    run_id: str
    parent_account_id: str | None = None
    limits: tuple[BudgetDimensionLimit, ...]
    reserved: dict[str, int] = Field(default_factory=dict)
    consumed: dict[str, int] = Field(default_factory=dict)
    pending_settlement: dict[str, int] = Field(default_factory=dict)
    reservations: dict[str, dict[str, int]] = Field(default_factory=dict)
    usage_ids: frozenset[str] = Field(default_factory=frozenset)
    settlement_ids: frozenset[str] = Field(default_factory=frozenset)


class BudgetLedgerEntry(Contract):
    entry_id: str
    account_id: str
    run_id: str
    kind: BudgetLedgerKind
    idempotency_id: str
    amounts: dict[str, int]
    occurred_at: AwareDatetime
    parent_account_id: str | None = None


class LifecycleTransitionRecord(Contract):
    schema_version: Literal["1"] = "1"
    transition_id: str
    run_id: str
    command_id: str
    prior_version: int
    resulting_version: int
    prior_phase: RunPhase | None
    resulting_phase: RunPhase
    prior_projection: RunProjection | None
    resulting_projection: RunProjection
    actor: ActorContext
    reason: str
    evidence_refs: tuple[str, ...]
    occurred_at: AwareDatetime
    correlation_id: str
    causation_id: str | None = None


class CommandResult(Contract):
    schema_version: Literal["1"] = "1"
    command_id: str
    idempotency_issuer: str
    run_id: str
    command_fingerprint: str = Field(pattern=DIGEST_PATTERN)
    status: CommandStatus
    resulting_run_version: int
    phase: RunPhase
    terminal_outcome: RunOutcome | None = None
    reason_code: str
    reason: str
    recorded_at: AwareDatetime


class DomainEventEnvelope(Contract):
    schema_version: Literal["1"] = "1"
    event_id: str
    event_type: str = Field(min_length=1)
    aggregate_type: Literal["workflow_run"] = "workflow_run"
    aggregate_id: str
    aggregate_version: int = Field(ge=1)
    sequence: int = Field(ge=1)
    is_version_final: bool = True
    occurred_at: AwareDatetime
    recorded_at: AwareDatetime
    actor: ActorContext
    correlation_id: str
    causation_id: str | None = None
    payload: dict[str, object]


class OutboxCursor(Contract):
    position: int = Field(ge=1)
    recorded_at: AwareDatetime
    aggregate_id: str
    aggregate_version: int = Field(ge=1)
    sequence: int = Field(ge=1)


class OutboxRecord(Contract):
    envelope: DomainEventEnvelope
    cursor: OutboxCursor
    delivery_attempts: int = Field(default=0, ge=0)
    delivered_at: AwareDatetime | None = None


class ConsumerCursor(Contract):
    consumer_id: str
    aggregate_id: str
    last_aggregate_version: int = Field(ge=0)
    last_sequence: int = Field(default=0, ge=0)
    last_version_final: bool = False


class ConsumerApplyResult(Contract):
    status: ConsumerApplyStatus
    cursor: ConsumerCursor
    expected_version: int
    observed_version: int
