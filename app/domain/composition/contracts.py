from __future__ import annotations

from enum import StrEnum
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.domain.control_plane.contracts import (
    CompileInvocation,
    ExactDefinitionRef,
)
from app.domain.run_control.contracts import ActorContext, BudgetEnvelope


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RunDependencyClass(StrEnum):
    REQUIRED_BLOCKING = "required_blocking"
    DEGRADABLE_BLOCKING = "degradable_blocking"
    DEGRADABLE_NONBLOCKING = "degradable_nonblocking"
    DETACHED_ADVISORY = "detached_advisory"


class LinkedRunRequest(Contract):
    request_scope: str = Field(min_length=1)
    parent_run_id: str = Field(min_length=1)
    slot_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    request_revision: int = Field(ge=1)
    target_workflow_type_ref: ExactDefinitionRef
    compilation: CompileInvocation
    child_budget: BudgetEnvelope
    dependency_class: RunDependencyClass
    purpose: str = Field(min_length=1)
    actor: ActorContext
    requested_at: AwareDatetime
    authority_request_refs: frozenset[str] = Field(default_factory=frozenset)
    permission_assessment_refs: tuple[str, ...] = ()
    causation_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)


class RunCompositionLink(Contract):
    link_id: str = Field(min_length=1)
    request_identity: str = Field(min_length=1)
    request_fingerprint: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    request_scope: str = Field(min_length=1)
    parent_run_id: str = Field(min_length=1)
    child_run_id: str = Field(min_length=1)
    slot_id: str = Field(min_length=1)
    request_revision: int = Field(ge=1)
    target_workflow_type_ref: ExactDefinitionRef
    child_effective_configuration_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    dependency_class: RunDependencyClass
    dependency_revision: int = Field(default=1, ge=1)
    linked_budget_account_id: str = Field(min_length=1)
    result_admission_policy: str = Field(min_length=1)
    cancellation_policy: Literal["request_cancel", "allow_continue"]
    created_at: AwareDatetime


class DependencyAssessment(Contract):
    affected_obligation_refs: tuple[str, ...] = ()
    affected_artifact_refs: tuple[str, ...] = ()
    affected_output_refs: tuple[str, ...] = ()
    affected_evaluation_refs: tuple[str, ...] = ()
    readiness_reassessment_required: bool
    reason: str = Field(min_length=1)


class RunDependencyRevision(Contract):
    revision_id: str = Field(min_length=1)
    link_id: str = Field(min_length=1)
    revision: int = Field(ge=2)
    prior_dependency_class: RunDependencyClass
    dependency_class: RunDependencyClass
    assessment: DependencyAssessment
    authority_ref: str = Field(min_length=1)
    decided_by: str = Field(min_length=1)
    decided_at: AwareDatetime


class ResultEvidenceAssessment(Contract):
    intended_purpose_satisfied: bool
    exact_version_compatible: bool
    ready: bool
    provenance_valid: bool
    permissions_valid: bool
    evaluation_evidence_valid: bool
    evidence_refs: tuple[str, ...] = ()

    @property
    def fully_admissible(self) -> bool:
        return all(
            (
                self.intended_purpose_satisfied,
                self.exact_version_compatible,
                self.ready,
                self.provenance_valid,
                self.permissions_valid,
                self.evaluation_evidence_valid,
            )
        )


class LinkedRunResultAdmissionDecision(Contract):
    decision_id: str = Field(min_length=1)
    link_id: str = Field(min_length=1)
    parent_run_id: str = Field(min_length=1)
    child_run_id: str = Field(min_length=1)
    exact_output_ref: str = Field(min_length=1)
    outcome: Literal["admit", "conditionally_admit", "reject", "defer"]
    assessment: ResultEvidenceAssessment
    condition_refs: tuple[str, ...] = ()
    late_result: bool = False
    authority_ref: str = Field(min_length=1)
    decided_by: str = Field(min_length=1)
    decided_at: AwareDatetime
    reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_outcome(self) -> LinkedRunResultAdmissionDecision:
        if self.outcome == "admit" and not self.assessment.fully_admissible:
            raise ValueError("admit requires all exact result-admission checks to pass")
        if self.outcome == "conditionally_admit" and not self.condition_refs:
            raise ValueError("conditional admission requires explicit conditions")
        if self.late_result and self.outcome in {"admit", "conditionally_admit"}:
            raise ValueError("late results cannot mutate a terminal parent")
        return self


class LinkedRunDependencyDisposition(Contract):
    link_id: str
    blocks_parent_completion: bool
    wait_required: bool
    degradation_required_on_failure: bool
    parent_may_complete: bool


class LinkedRunCancellationRequest(Contract):
    cancellation_request_id: str
    link_id: str
    child_run_id: str
    requested: bool
    reason: str


class LinkedChildResultObservation(Contract):
    link: RunCompositionLink
    status: Literal["completed", "failed", "cancelled", "timed_out"]
    exact_output_refs: tuple[str, ...] = ()
    failure_ref: str | None = None
    observed_at: AwareDatetime


class LinkedChildTerminalRecord(Contract):
    terminal_record_id: str
    link_id: str
    child_run_id: str
    status: Literal["completed", "failed", "cancelled", "timed_out"]
    exact_output_refs: tuple[str, ...] = ()
    failure_ref: str | None = None
    observed_at: AwareDatetime


class LinkedChildResolution(Contract):
    link_id: str
    child_status: Literal["completed", "failed", "cancelled", "timed_out"]
    failure_ref: str | None = None
    disposition: Literal[
        "admitted",
        "conditionally_admitted",
        "deferred",
        "rejected",
        "degraded",
        "failed",
    ]
    decision_ids: tuple[str, ...] = ()
    admitted_output_refs: tuple[str, ...] = ()
    reason: str = Field(min_length=1)


class LinkedResultAdmissionProposal(Contract):
    outcome: Literal["admit", "conditionally_admit", "reject", "defer"]
    assessment: ResultEvidenceAssessment
    condition_refs: tuple[str, ...] = ()
    reason: str = Field(min_length=1)


class LinkedRunExecutionBinding(Contract):
    link: RunCompositionLink
    effective_dependency_class: RunDependencyClass
    dependency_revision_id: str | None = None


class LinkedRunContinuationState(Contract):
    """Compact application state carried through Continue-As-New."""

    run_id: str
    next_execution_epoch: int = Field(ge=2)
    workflow_cycle: int = Field(ge=0)
    semantic_counters: dict[str, int] = Field(default_factory=dict)
    pending_wait_ids: tuple[str, ...] = ()
    link_ids: tuple[str, ...] = ()
    accepted_dependency_revision_ids: tuple[str, ...] = ()
    accepted_result_decision_ids: tuple[str, ...] = ()
    reservation_ids: tuple[str, ...] = ()
    authority_ref: str = Field(min_length=1)
