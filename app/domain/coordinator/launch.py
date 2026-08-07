from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    CompileInvocation,
    ExactDefinitionRef,
)
from app.domain.graph_runtime.definitions import RunPlanV3, UnavailableStageSurface
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetEnvelope,
    RunOutcome,
    RunRequest,
)

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BlueprintFamily(StrEnum):
    STAGE_GRAPH = "StageGraph"
    GOAL_DIRECTED = "GoalDirected"


class LaunchTicketState(StrEnum):
    PREPARED = "prepared"
    CONSUMED = "consumed"
    EXPIRED = "expired"
    INVALIDATED = "invalidated"


class LaunchContractError(RuntimeError):
    code = "launch_contract_error"

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class LaunchAuthorizationError(LaunchContractError):
    code = "launch_authorization_failed"


class LaunchIdempotencyConflict(LaunchContractError):
    code = "launch_idempotency_conflict"


class LaunchTicketUnavailable(LaunchContractError):
    code = "launch_ticket_unavailable"


class LaunchTicketNotFound(LaunchContractError):
    code = "launch_ticket_not_found"


class LaunchDecision(Contract):
    subject: str = Field(min_length=1)
    outcome: Literal["accepted", "degraded", "unavailable", "forbidden"]
    reason: str = Field(min_length=1)
    evidence_refs: tuple[str, ...] = ()


class RunAdmissionSpec(Contract):
    actor: ActorContext
    budget_envelope: BudgetEnvelope
    requested_at: AwareDatetime
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = None
    parent_run_id: str | None = None
    sponsorship_ref: str = Field(min_length=1)
    approval_refs: tuple[str, ...] = ()
    delegation_authority_refs: frozenset[str] = Field(default_factory=frozenset)
    admission_evidence_refs: tuple[str, ...] = ()


class WorkflowLaunchProposal(Contract):
    request_scope: str = Field(min_length=1)
    tenant_scope: str = Field(min_length=1)
    compilation: CompileInvocation
    admission: RunAdmissionSpec
    initial_goal: str | None = Field(default=None, repr=False)
    selected_asset_refs: tuple[ExactDefinitionRef, ...] = ()
    authority_decisions: tuple[LaunchDecision, ...] = ()
    availability_decisions: tuple[LaunchDecision, ...] = ()
    policy_snapshot_digest: str = Field(pattern=DIGEST_PATTERN)
    environment_snapshot_digest: str = Field(pattern=DIGEST_PATTERN)
    idempotency_issuer: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)

    @model_validator(mode="after")
    def proposal_bindings_are_consistent(self) -> WorkflowLaunchProposal:
        if self.compilation.context.authority_scope != self.request_scope:
            raise ValueError("compilation authority scope differs from launch request scope")
        if self.compilation.context.actor_id != self.admission.actor.actor_id:
            raise ValueError("compilation actor differs from admission actor")
        if self.compilation.context.authority_subject_id != self.admission.actor.actor_id:
            raise ValueError("compilation authority subject differs from admission actor")
        if self.idempotency_issuer != self.admission.actor.actor_id:
            raise ValueError("launch idempotency issuer must be the admitted actor")
        if self.admission.approval_refs != tuple(sorted(set(self.admission.approval_refs))):
            raise ValueError("approval references must be unique and sorted")
        if len(set(self.selected_asset_refs)) != len(self.selected_asset_refs):
            raise ValueError("selected exact asset references must be unique")
        return self

    @property
    def digest(self) -> str:
        return sha256_digest(self.model_dump(mode="json"))


class LaunchRequestContext(Contract):
    caller_id: str = Field(min_length=1)
    tenant_scope: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    approval_refs: tuple[str, ...] = ()
    policy_snapshot_digest: str = Field(pattern=DIGEST_PATTERN)
    environment_snapshot_digest: str = Field(pattern=DIGEST_PATTERN)
    observed_at: AwareDatetime

    @model_validator(mode="after")
    def approval_refs_are_canonical(self) -> LaunchRequestContext:
        if self.approval_refs != tuple(sorted(set(self.approval_refs))):
            raise ValueError("approval references must be unique and sorted")
        return self


class AdmissionPreviewDecision(Contract):
    accepted: bool
    reason_code: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class SemanticBindingPlan(Contract):
    """Frozen provider-authored semantic inputs before admission assigns a run id."""

    plan_ref: str = Field(min_length=1)
    blueprint_family: BlueprintFamily
    exact_input_refs: tuple[str, ...] = ()
    payload: dict[str, Any] = Field(repr=False)
    plan_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def plan_content_matches_digest(self) -> SemanticBindingPlan:
        content = self.model_dump(mode="json", exclude={"plan_digest"})
        if sha256_digest(content) != self.plan_digest:
            raise ValueError("semantic binding plan digest mismatch")
        if self.exact_input_refs != tuple(sorted(set(self.exact_input_refs))):
            raise ValueError("semantic binding plan input references must be unique and sorted")
        return self

    @classmethod
    def create(
        cls,
        *,
        plan_ref: str,
        blueprint_family: BlueprintFamily,
        exact_input_refs: tuple[str, ...],
        payload: dict[str, Any],
    ) -> SemanticBindingPlan:
        values = {
            "plan_ref": plan_ref,
            "blueprint_family": blueprint_family,
            "exact_input_refs": tuple(sorted(set(exact_input_refs))),
            "payload": payload,
        }
        return cls(**values, plan_digest=sha256_digest(values))


class PreparedLaunchTicket(Contract):
    ticket_id: str = Field(min_length=1)
    caller_id: str = Field(min_length=1)
    tenant_scope: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    state: LaunchTicketState = LaunchTicketState.PREPARED
    prepared_at: AwareDatetime
    expires_at: AwareDatetime
    proposal_digest: str = Field(pattern=DIGEST_PATTERN)
    workflow_type_ref: ExactDefinitionRef
    blueprint_ref: ExactDefinitionRef
    blueprint_family: BlueprintFamily
    initial_goal: str | None = Field(default=None, repr=False)
    initial_goal_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    run_request_digest: str = Field(pattern=DIGEST_PATTERN)
    resolved_asset_refs: tuple[ExactDefinitionRef, ...] = ()
    authority_decisions: tuple[LaunchDecision, ...] = ()
    availability_decisions: tuple[LaunchDecision, ...] = ()
    approval_refs: tuple[str, ...] = ()
    policy_snapshot_digest: str = Field(pattern=DIGEST_PATTERN)
    environment_snapshot_digest: str = Field(pattern=DIGEST_PATTERN)
    semantic_binding_plan_ref: str | None = None
    semantic_binding_plan_digest: str | None = Field(
        default=None,
        pattern=DIGEST_PATTERN,
    )
    semantic_binding_plan: SemanticBindingPlan | None = Field(
        default=None,
        repr=False,
    )
    runtime_run_plan: RunPlanV3 | None = Field(default=None, repr=False)
    runtime_unavailable_surfaces: tuple[UnavailableStageSurface, ...] = Field(
        default=(),
        repr=False,
    )
    warnings: tuple[str, ...] = ()
    launchable: bool
    idempotency_issuer: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    frozen_run_request: RunRequest
    consumed_run_id: str | None = None
    consumed_at: AwareDatetime | None = None
    invalidation_reason: str | None = None

    @model_validator(mode="after")
    def ticket_is_internally_consistent(self) -> PreparedLaunchTicket:
        if self.expires_at <= self.prepared_at:
            raise ValueError("launch ticket expiry must follow preparation")
        if self.blueprint_family == BlueprintFamily.STAGE_GRAPH:
            if self.initial_goal is not None or self.initial_goal_digest is not None:
                raise ValueError("StageGraph launch tickets cannot contain an initial goal")
        else:
            if self.initial_goal is None or not self.initial_goal.strip():
                raise ValueError("GoalDirected launch tickets require a non-empty initial goal")
            if sha256_digest(self.initial_goal) != self.initial_goal_digest:
                raise ValueError("GoalDirected initial goal does not match its frozen digest")
        expected_run_digest = sha256_digest(self.frozen_run_request)
        if expected_run_digest != self.run_request_digest:
            raise ValueError("frozen Run Request does not match the launch ticket digest")
        if self.frozen_run_request.request_scope != self.request_scope:
            raise ValueError("frozen Run Request belongs to a different request scope")
        if (
            self.frozen_run_request.idempotency_issuer != self.idempotency_issuer
            or self.frozen_run_request.request_id != self.idempotency_key
        ):
            raise ValueError("frozen Run Request has a different idempotency identity")
        if self.frozen_run_request.effective_configuration_digest != (
            self.effective_configuration_digest
        ):
            raise ValueError("frozen Run Request has a different ERC digest")
        if self.frozen_run_request.workflow_type_ref != self.workflow_type_ref:
            raise ValueError("frozen Run Request has a different Workflow Type")
        if self.semantic_binding_plan is None:
            if (
                self.semantic_binding_plan_ref is not None
                or self.semantic_binding_plan_digest is not None
            ):
                raise ValueError("semantic binding plan metadata is incomplete")
        elif (
            self.semantic_binding_plan.plan_ref != self.semantic_binding_plan_ref
            or self.semantic_binding_plan.plan_digest
            != self.semantic_binding_plan_digest
            or self.semantic_binding_plan.blueprint_family != self.blueprint_family
        ):
            raise ValueError(
                "frozen semantic binding plan differs from launch ticket metadata"
            )
        if self.runtime_run_plan is not None:
            if self.runtime_run_plan.effective_run_configuration_digest != (
                self.effective_configuration_digest
            ):
                raise ValueError("runtime RunPlan has a different ERC digest")
            if (
                self.runtime_run_plan.semantic_binding_ref
                != self.semantic_binding_plan_ref
            ):
                raise ValueError("runtime RunPlan has a different semantic binding plan")
            if self.runtime_run_plan.workflow_implementation_ref not in self.resolved_asset_refs:
                raise ValueError("runtime RunPlan implementation is not a resolved asset")
        if self.runtime_unavailable_surfaces and self.launchable:
            raise ValueError("unavailable required runtime surfaces cannot be launchable")
        if self.state == LaunchTicketState.CONSUMED:
            if self.consumed_run_id is None or self.consumed_at is None:
                raise ValueError("consumed launch tickets require run identity and timestamp")
        elif self.consumed_run_id is not None or self.consumed_at is not None:
            raise ValueError("only consumed launch tickets may carry a consumed run")
        if self.state == LaunchTicketState.INVALIDATED and not self.invalidation_reason:
            raise ValueError("invalidated launch tickets require a reason")
        return self

    def public_view(self) -> PublicPreparedLaunchTicket:
        return PublicPreparedLaunchTicket.model_validate(
            self.model_dump(
                mode="json",
                exclude={
                    "initial_goal",
                    "frozen_run_request",
                    "semantic_binding_plan",
                    "runtime_run_plan",
                    "runtime_unavailable_surfaces",
                },
            )
        )


class PublicPreparedLaunchTicket(Contract):
    ticket_id: str
    caller_id: str
    tenant_scope: str
    request_scope: str
    state: LaunchTicketState
    prepared_at: AwareDatetime
    expires_at: AwareDatetime
    proposal_digest: str = Field(pattern=DIGEST_PATTERN)
    workflow_type_ref: ExactDefinitionRef
    blueprint_ref: ExactDefinitionRef
    blueprint_family: BlueprintFamily
    initial_goal_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    run_request_digest: str = Field(pattern=DIGEST_PATTERN)
    resolved_asset_refs: tuple[ExactDefinitionRef, ...] = ()
    authority_decisions: tuple[LaunchDecision, ...] = ()
    availability_decisions: tuple[LaunchDecision, ...] = ()
    approval_refs: tuple[str, ...] = ()
    policy_snapshot_digest: str = Field(pattern=DIGEST_PATTERN)
    environment_snapshot_digest: str = Field(pattern=DIGEST_PATTERN)
    semantic_binding_plan_ref: str | None = None
    semantic_binding_plan_digest: str | None = Field(
        default=None,
        pattern=DIGEST_PATTERN,
    )
    warnings: tuple[str, ...] = ()
    launchable: bool
    idempotency_issuer: str
    idempotency_key: str
    consumed_run_id: str | None = None
    consumed_at: AwareDatetime | None = None
    invalidation_reason: str | None = None


class WorkflowSubmission(Contract):
    workflow_id: str = Field(min_length=1)
    temporal_run_id: str | None = None


class WorkflowLaunchHandle(Contract):
    run_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    workflow_type_ref: ExactDefinitionRef
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    blueprint_ref: ExactDefinitionRef
    blueprint_family: BlueprintFamily
    phase: str = Field(min_length=1)
    result_resource_uri: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    workflow_id: str = Field(min_length=1)
    temporal_run_id: str | None = None


class StageGraphResultDetails(Contract):
    family: Literal[BlueprintFamily.STAGE_GRAPH] = BlueprintFamily.STAGE_GRAPH
    execution_epoch: int = Field(ge=1)
    # The initial StageGraph pass is cycle zero. This value only advances when
    # the whole-workflow evaluator requests another cycle.
    workflow_cycles: int = Field(ge=0)
    stage_cycles: dict[str, int] = Field(default_factory=dict)
    operation_attempts: dict[str, int] = Field(default_factory=dict)
    output_refs: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    reused_output_refs: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    schedule_trace: tuple[str, ...] = ()


class GoalDirectedResultDetails(Contract):
    family: Literal[BlueprintFamily.GOAL_DIRECTED] = BlueprintFamily.GOAL_DIRECTED
    execution_epoch: int = Field(ge=1)
    stop_reason: str = Field(min_length=1)
    final_verifier_action: str = Field(min_length=1)
    goal_iterations: int = Field(ge=0)
    agent_runs: int = Field(ge=0)
    rollover_count: int = Field(ge=0)
    active_revision_id: str = Field(min_length=1)
    accepted_revision_ids: tuple[str, ...] = ()
    handoff_checkpoints: tuple[dict[str, Any], ...] = ()
    execution_results: tuple[dict[str, Any], ...] = ()
    verification_results: tuple[dict[str, Any], ...] = ()


WorkflowFamilyResult = StageGraphResultDetails | GoalDirectedResultDetails


class WorkflowResultRecord(Contract):
    run_id: str = Field(min_length=1)
    tenant_scope: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    blueprint_family: BlueprintFamily
    terminal_outcome: RunOutcome
    output_contract_results: dict[str, Any] = Field(default_factory=dict)
    artifact_refs: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    degradations: tuple[str, ...] = ()
    operation_binding_refs: tuple[str, ...] = ()
    usage_summary: dict[str, int] = Field(default_factory=dict)
    family_result: WorkflowFamilyResult
    completed_at: AwareDatetime

    @model_validator(mode="after")
    def family_result_matches_blueprint(self) -> WorkflowResultRecord:
        if self.family_result.family != self.blueprint_family:
            raise ValueError("typed family result differs from the launched blueprint family")
        return self


class TerminalWorkflowCompletion(Contract):
    """Authoritative terminal execution facts passed to application completion."""

    run_id: str = Field(min_length=1)
    tenant_scope: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    blueprint_family: BlueprintFamily
    terminal_outcome: RunOutcome
    output_contract_results: dict[str, Any] = Field(default_factory=dict)
    artifact_refs: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    degradations: tuple[str, ...] = ()
    operation_binding_refs: tuple[str, ...] = ()
    usage_summary: dict[str, int] = Field(default_factory=dict)
    family_result: WorkflowFamilyResult
    completed_at: AwareDatetime

    @model_validator(mode="after")
    def family_result_matches_blueprint(self) -> TerminalWorkflowCompletion:
        if self.family_result.family != self.blueprint_family:
            raise ValueError("completion family differs from the launched blueprint family")
        return self


class WorkflowResultView(Contract):
    run_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    phase: str = Field(min_length=1)
    result: WorkflowResultRecord | None = None

    @model_validator(mode="after")
    def completed_views_have_results(self) -> WorkflowResultView:
        if (self.phase == "terminal") != (self.result is not None):
            raise ValueError("a typed result exists exactly for a terminal result view")
        return self


def validate_launch_context(
    *,
    caller_id: str,
    tenant_scope: str,
    request_scope: str,
    approval_refs: tuple[str, ...],
    policy_snapshot_digest: str,
    environment_snapshot_digest: str,
    context: LaunchRequestContext,
) -> None:
    checks = (
        (caller_id == context.caller_id, "launch ticket belongs to another caller"),
        (tenant_scope == context.tenant_scope, "launch ticket belongs to another tenant"),
        (request_scope == context.request_scope, "launch ticket belongs to another scope"),
        (approval_refs == context.approval_refs, "launch approval snapshot changed"),
        (
            policy_snapshot_digest == context.policy_snapshot_digest,
            "launch policy snapshot changed",
        ),
        (
            environment_snapshot_digest == context.environment_snapshot_digest,
            "launch environment snapshot changed",
        ),
    )
    for valid, message in checks:
        if not valid:
            raise LaunchAuthorizationError(message)


def is_expired(expires_at: datetime, observed_at: datetime) -> bool:
    return observed_at >= expires_at
