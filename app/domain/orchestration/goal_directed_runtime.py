from __future__ import annotations

from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from app.domain.control_plane.contracts import GoalDirectedBlueprint
from app.domain.operation_execution.contracts import (
    OperationWorkflowRequest,
    OperationWorkflowResult,
)
from app.domain.operation_execution.delegation import (
    AsyncDelegationBoundary,
    classify_async_delegation,
)
from app.domain.orchestration.contracts import (
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoff,
    GoalRevision,
    GoalVerificationResult,
    GoalVerifierDecision,
)
from app.domain.run_control.contracts import Contract
from app.domain.run_control.family_admission import AtomicFamilyMutation


class GoalFamilyDecisionMutation(AtomicFamilyMutation):
    """GoalDirected decision committed with one reducer-authorized lifecycle command."""

    family_kind: Literal["goal_directed"] = "goal_directed"
    mutation_kind: Literal["decision"] = "decision"
    goal_revision_id: str = Field(min_length=1, max_length=512)
    goal_revision_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    goal_iteration: int = Field(ge=1)
    operation_role: Literal["executor", "verifier"]
    operation_request_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    semantic_input_binding_ref: str = Field(min_length=1, max_length=512)
    handoff_ref: str | None = Field(default=None, max_length=512)
    verification_ref: str | None = Field(default=None, max_length=512)
    convergence_action: Literal[
        "continue",
        "reduce_effort",
        "skip_degradable",
        "revise",
        "repair",
        "pause",
        "escalate",
        "fork",
        "linked_run",
        "control_revision",
        "new_run",
        "complete",
        "partial_or_fail",
        "fail",
    ]


class GoalOperationPreparationRequest(Contract):
    schema_version: Literal["belllabs.goal-operation-preparation.v1"] = (
        "belllabs.goal-operation-preparation.v1"
    )
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    effective_configuration_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    semantic_input_binding_ref: str = Field(min_length=1)
    goal_revision_id: str = Field(min_length=1)
    goal_revision_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    goal_revision: GoalRevision
    goal_iteration: int = Field(ge=1)
    operation_role: Literal["executor", "verifier"]
    operation_attempt: int = Field(ge=1)
    execution_generation: int = Field(ge=1)
    expected_run_version: int = Field(ge=1)
    expected_family_version: int = Field(ge=0)
    reservation_id: str = Field(min_length=1)
    reservation: dict[str, int] = Field(min_length=1)
    session_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    read_workspace_id: str | None = Field(default=None, min_length=1)
    handoff_ref: str | None = None
    handoff: GoalHandoff | None = None
    verifier_input_refs: tuple[str, ...] = ()
    decided_at: AwareDatetime

    @model_validator(mode="after")
    def exact_revision(self) -> GoalOperationPreparationRequest:
        if (
            self.goal_revision.revision_id != self.goal_revision_id
            or self.goal_revision.canonical_digest != self.goal_revision_digest
        ):
            raise ValueError("operation preparation revision does not match its exact reference")
        if (self.handoff_ref is None) != (self.handoff is None):
            raise ValueError("operation preparation handoff reference and content must be atomic")
        if self.handoff is not None and (
            self.handoff_ref != self.handoff.handoff_id
            or self.handoff.run_id != self.run_id
            or self.handoff.goal_revision_id != self.goal_revision_id
        ):
            raise ValueError("operation preparation handoff does not match its exact reference")
        if self.operation_role == "verifier":
            if self.read_workspace_id is None:
                raise ValueError("verifier preparation requires its executor read workspace")
        elif self.read_workspace_id is not None:
            raise ValueError("executor preparation cannot mount a prior read workspace")
        return self


class GoalOperationDispatch(Contract):
    schema_version: Literal["belllabs.goal-operation-dispatch.v1"] = (
        "belllabs.goal-operation-dispatch.v1"
    )
    workflow_request: OperationWorkflowRequest
    operation_binding_ref: str = Field(min_length=1)
    operation_request_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    resulting_run_version: int = Field(ge=1)
    resulting_family_version: int = Field(ge=1)


class GoalOperationReconciliationRequest(Contract):
    schema_version: Literal["belllabs.goal-operation-reconciliation.v1"] = (
        "belllabs.goal-operation-reconciliation.v1"
    )
    request_scope: str = Field(min_length=1)
    goal_revision_id: str = Field(min_length=1)
    operation_role: Literal["executor", "verifier"]
    operation_binding_ref: str = Field(min_length=1)
    required_output_contract_refs: tuple[str, ...] = Field(min_length=1)
    operation_request: OperationWorkflowRequest
    claim: GoalExecutionClaim
    executor_result: GoalExecutionResult | None = None
    operation_result: OperationWorkflowResult
    remaining_iterations: int = Field(ge=0)
    protected_fact_classes: tuple[str, ...] = ()
    context_selection_policy_ref: str | None = Field(default=None, min_length=1)
    context_compaction_policy_ref: str | None = Field(default=None, min_length=1)
    workspace_ref_class: str | None = Field(default=None, min_length=1)
    compaction_failure_action: Literal[
        "retry", "fresh_from_handoff", "pause", "escalate"
    ] | None = None
    verifier_policy_binding_ref: str | None = Field(default=None, min_length=1)
    verifier_rubric_ref: str | None = Field(default=None, min_length=1)
    verifier_rubric_version: int | None = Field(default=None, ge=1)
    acceptance_contract_ref: str | None = Field(default=None, min_length=1)
    acceptance_version: int | None = Field(default=None, ge=1)
    recorded_at: AwareDatetime

    @model_validator(mode="after")
    def verifier_authority_is_exact(self) -> GoalOperationReconciliationRequest:
        values = (
            self.verifier_policy_binding_ref,
            self.verifier_rubric_ref,
            self.verifier_rubric_version,
            self.acceptance_contract_ref,
            self.acceptance_version,
        )
        if self.operation_role == "verifier":
            if any(value is None for value in values):
                raise ValueError("verifier reconciliation requires its frozen policy authority")
            if (
                self.executor_result is None
                or self.executor_result.identity != self.claim.identity
            ):
                raise ValueError(
                    "verifier reconciliation requires the exact admitted executor result"
                )
            if self.compaction_failure_action is not None:
                raise ValueError(
                    "verifier reconciliation cannot carry a compaction failure action"
                )
            if (
                self.protected_fact_classes
                or self.context_selection_policy_ref is not None
                or self.context_compaction_policy_ref is not None
                or self.workspace_ref_class is not None
            ):
                raise ValueError("verifier reconciliation cannot carry handoff authority")
        elif any(value is not None for value in values):
            raise ValueError("executor reconciliation cannot carry verifier policy authority")
        elif self.executor_result is not None:
            raise ValueError("executor reconciliation cannot carry an executor result")
        elif self.compaction_failure_action is None:
            raise ValueError(
                "executor reconciliation requires its frozen compaction failure action"
            )
        elif (
            not self.protected_fact_classes
            or self.context_selection_policy_ref is None
            or self.context_compaction_policy_ref is None
            or self.workspace_ref_class is None
        ):
            raise ValueError("executor reconciliation requires frozen handoff authority")
        return self


class GoalHandoffDraft(Contract):
    """Model-authored handoff content; application authority binds its identity."""

    schema_version: Literal["belllabs.goal-handoff-draft.v1"] = (
        "belllabs.goal-handoff-draft.v1"
    )
    accepted_fact_refs: tuple[str, ...] = Field(min_length=1)
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    artifact_refs: tuple[str, ...] = ()
    attempted_tactics: tuple[str, ...] = ()
    rejected_tactics: tuple[tuple[str, str], ...] = ()
    unresolved_obligations: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    effect_frontier_refs: tuple[str, ...] = ()
    pending_liability_refs: tuple[str, ...] = ()
    context_selection_refs: tuple[str, ...] = Field(min_length=1)
    compaction_decision_ref: str = Field(min_length=1)
    compaction_status: Literal["accepted", "failed"] = "accepted"
    compaction_attempt: int = Field(default=1, ge=1)
    compaction_failure_ref: str = ""
    continuation_instructions: str = Field(min_length=1)

    @model_validator(mode="after")
    def compaction_failure_is_exact(self) -> GoalHandoffDraft:
        if (self.compaction_status == "failed") != bool(self.compaction_failure_ref):
            raise ValueError(
                "failed handoff compaction requires exactly one failure reference"
            )
        return self


class GoalExecutorObservation(Contract):
    """Cognitive executor output with no lifecycle, identity, or usage authority."""

    schema_version: Literal["belllabs.goal-executor-observation.v1"] = (
        "belllabs.goal-executor-observation.v1"
    )
    disposition: Literal["completed", "failed", "blocked"]
    output_refs: tuple[str, ...] = ()
    completion_claim: bool = False
    blocker_class: str = ""
    authority_breach_ref: str = ""
    hard_budget_exhausted_dimensions: tuple[str, ...] = ()
    irrecoverable_failure_ref: str = ""
    accepted_fact_refs: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    effect_frontier_refs: tuple[str, ...] = ()
    pending_liability_refs: tuple[str, ...] = ()
    handoff: GoalHandoffDraft | None = None
    output_contract_ref: str = Field(min_length=1)


class GoalVerifierObservation(Contract):
    """Cognitive verifier output; application authority binds applicability."""

    schema_version: Literal["belllabs.goal-verifier-observation.v1"] = (
        "belllabs.goal-verifier-observation.v1"
    )
    decision: GoalVerifierDecision
    progress_made: bool
    accepted_obligation_refs: tuple[str, ...] = ()
    findings: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    unmet_obligations: tuple[str, ...] = ()
    obligation_applicability: tuple[tuple[str, bool], ...] = ()
    blocker_class: str = ""
    authority_breach_ref: str = ""
    hard_budget_exhausted_dimensions: tuple[str, ...] = ()
    soft_budget_dimensions: tuple[str, ...] = ()
    irrecoverable_failure_ref: str = ""
    proposed_revision: GoalRevision | None = None
    scope_expansion_route: Literal[
        "control_revision", "fork", "linked_run", "new_run"
    ] | None = None
    route_ref: str = ""
    effect_refs: tuple[str, ...] = ()
    output_contract_ref: str = Field(min_length=1)


class GoalOperationReconciliationResult(Contract):
    schema_version: Literal["belllabs.goal-operation-reconciliation-result.v1"] = (
        "belllabs.goal-operation-reconciliation-result.v1"
    )
    operation_role: Literal["executor", "verifier"]
    execution_result: GoalExecutionResult | None = None
    verification_result: GoalVerificationResult | None = None
    detail_ref: str = Field(min_length=1)

    @model_validator(mode="after")
    def exact_role_result(self) -> GoalOperationReconciliationResult:
        if self.operation_role == "executor":
            if self.execution_result is None or self.verification_result is not None:
                raise ValueError("executor reconciliation requires only an execution result")
        elif self.verification_result is None or self.execution_result is not None:
            raise ValueError("verifier reconciliation requires only a verification result")
        return self


class GoalAsyncSubgoalRouting(Contract):
    subgoal_class: str = Field(min_length=1)
    route: Literal["subordinate", "operation", "linked_run"]
    reason_code: str = Field(min_length=1)


def route_goal_async_subgoal(
    blueprint: GoalDirectedBlueprint,
    *,
    subgoal_class: str,
    boundary: AsyncDelegationBoundary,
) -> GoalAsyncSubgoalRouting:
    """Apply the sole canonical classifier within the frozen GoalDirected envelope."""

    if subgoal_class not in blueprint.allowed_async_subgoal_classes:
        raise ValueError("async subgoal class is outside the frozen GoalDirected blueprint")
    decision = classify_async_delegation(boundary)
    return GoalAsyncSubgoalRouting(
        subgoal_class=subgoal_class,
        route=decision.route,
        reason_code=decision.reason_code,
    )


__all__ = [
    "GoalAsyncSubgoalRouting",
    "GoalFamilyDecisionMutation",
    "GoalExecutorObservation",
    "GoalHandoffDraft",
    "GoalOperationDispatch",
    "GoalOperationPreparationRequest",
    "GoalOperationReconciliationRequest",
    "GoalOperationReconciliationResult",
    "GoalVerifierObservation",
    "route_goal_async_subgoal",
]
