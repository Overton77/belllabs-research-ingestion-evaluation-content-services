from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import Field, model_validator

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import OperationWorkflowRequest
from app.domain.run_control.contracts import RunOutcome
from app.domain.run_control.family_admission import AtomicFamilyMutation

StageStatus = Literal[
    "structurally_unavailable",
    "blocked",
    "ready",
    "reserved",
    "running",
    "waiting",
    "paused",
    "completed",
    "degraded",
    "failed",
    "cancelled",
    "skipped",
    "invalidated",
]


@dataclass(frozen=True)
class ExecutionIdentity:
    run_id: str
    execution_epoch: int = 1


WorkflowFamily = Literal["StageGraph", "GoalDirected"]


@dataclass(frozen=True)
class WorkflowMessage:
    """Reference-only command delivered durably to a workflow execution."""

    message_id: str
    sequence: int
    kind: Literal["control", "fact", "result", "cancel"]
    payload_ref: str
    execution_generation: int = 1

    def __post_init__(self) -> None:
        if not self.message_id or not self.payload_ref:
            raise ValueError("workflow messages require stable identities and payload refs")
        if self.sequence < 1 or self.execution_generation < 1:
            raise ValueError("workflow message sequence and generation must be positive")


@dataclass(frozen=True)
class WorkflowMessageReceipt:
    message_id: str
    sequence: int
    status: Literal["accepted", "duplicate", "stale_generation", "gap"]
    technical_segment: int


@dataclass(frozen=True)
class RunContinuityState:
    """Compact semantic state carried across technical history segments."""

    execution_epoch: int = 1
    technical_segment: int = 1
    execution_generation: int = 1
    family_workflow_id: str = ""
    active_operation_ids: tuple[str, ...] = ()
    pending_message_ids: tuple[str, ...] = ()
    message_receipts: tuple[WorkflowMessageReceipt, ...] = ()
    last_message_sequence: int = 0
    reservation_balances: dict[str, int] = field(default_factory=dict)
    linked_run_ids: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if min(self.execution_epoch, self.technical_segment, self.execution_generation) < 1:
            raise ValueError("continuity identities must be positive")
        message_ids = tuple(receipt.message_id for receipt in self.message_receipts)
        if len(message_ids) != len(set(message_ids)):
            raise ValueError("continuity receipts require unique message identities")
        accepted_sequences = tuple(
            receipt.sequence
            for receipt in self.message_receipts
            if receipt.status == "accepted"
        )
        if accepted_sequences != tuple(sorted(set(accepted_sequences))):
            raise ValueError("accepted continuity receipts must be unique and ordered")
        if accepted_sequences and accepted_sequences[-1] > self.last_message_sequence:
            raise ValueError("receipt frontier exceeds the message sequence frontier")

    def next_technical_segment(self) -> RunContinuityState:
        return replace(self, technical_segment=self.technical_segment + 1)


@dataclass(frozen=True)
class BellLabsRunInput:
    schema_version: Literal["belllabs.temporal-root.v1"]
    run_id: str
    request_scope: str
    effective_configuration_digest: str
    workflow_type_digest: str
    family: WorkflowFamily
    family_input: dict[str, Any]
    family_task_queue: str
    continuity: RunContinuityState = field(default_factory=RunContinuityState)
    continue_as_new_event_threshold: int = 10_000
    force_continue_as_new: bool = False

    def __post_init__(self) -> None:
        if not all(
            (
                self.run_id,
                self.request_scope,
                self.effective_configuration_digest,
                self.workflow_type_digest,
                self.family_task_queue,
            )
        ):
            raise ValueError("root input requires exact identities, digests, and task queue")
        if self.continuity.execution_epoch < 1:
            raise ValueError("execution epoch must be positive")
        if self.continue_as_new_event_threshold < 1:
            raise ValueError("Continue-As-New threshold must be positive")

    @property
    def workflow_id(self) -> str:
        return f"belllabs-run/{self.run_id}"

    @property
    def family_workflow_id(self) -> str:
        return f"family/{self.run_id}/{self.continuity.execution_epoch}"


@dataclass(frozen=True)
class BellLabsRunResult:
    run_id: str
    execution_epoch: int
    technical_segment: int
    family: WorkflowFamily
    family_result: dict[str, Any]
    message_receipts: tuple[WorkflowMessageReceipt, ...] = ()


@dataclass(frozen=True)
class SemanticForkRequest:
    source_run_id: str
    new_run_id: str
    request_scope: str
    snapshot_ref: str
    effective_configuration_digest: str


@dataclass(frozen=True)
class SemanticForkResult:
    new_run_id: str
    execution_epoch: Literal[1]
    technical_segment: Literal[1]
    snapshot_ref: str
    active_operation_ids: tuple[()] = ()
    pending_message_ids: tuple[()] = ()


def create_semantic_fork(request: SemanticForkRequest) -> SemanticForkResult:
    """Create isolated fork identity; live execution state is deliberately not copied."""

    if request.source_run_id == request.new_run_id:
        raise ValueError("semantic fork requires a new BellLabs run identity")
    if not request.snapshot_ref or not request.effective_configuration_digest:
        raise ValueError("semantic fork requires an admitted semantic snapshot and ERC")
    return SemanticForkResult(
        new_run_id=request.new_run_id,
        execution_epoch=1,
        technical_segment=1,
        snapshot_ref=request.snapshot_ref,
    )


class DependencyDisposition(StrEnum):
    UNRESOLVED = "unresolved"
    FULFILLED = "fulfilled"
    DEGRADED = "degraded"
    OMITTED = "omitted"
    FAILED = "failed"
    CANCELLED = "cancelled"
    INVALID = "invalid"


class JoinDisposition(StrEnum):
    SATISFIED = "satisfied"
    PENDING = "pending"
    IMPOSSIBLE = "impossible"


class ResultDecision(StrEnum):
    ADMIT = "admit"
    REJECT = "reject"
    QUARANTINE = "quarantine"


@dataclass(frozen=True, order=True)
class StageCandidateIdentity:
    stage_id: str
    mapped_instance_presence: int
    mapped_instance_id: str
    workflow_cycle_ordinal: int
    stage_cycle_ordinal: int
    operation_slot_id: str

    def __post_init__(self) -> None:
        if self.mapped_instance_presence not in {0, 1}:
            raise ValueError("mapped-instance presence must be zero or one")
        if (self.mapped_instance_presence == 0) != (
            self.mapped_instance_id == "NO_MAPPED_INSTANCE"
        ):
            raise ValueError("absent mappings require the typed NO_MAPPED_INSTANCE sentinel")
        if min(self.workflow_cycle_ordinal, self.stage_cycle_ordinal) < 0:
            raise ValueError("semantic cycle ordinals cannot be negative")

    @property
    def semantic_prefix(self) -> str:
        mapped = (
            "none"
            if self.mapped_instance_presence == 0
            else self.mapped_instance_id
        )
        return (
            f"stage:{self.stage_id}:mapped:{mapped}:"
            f"workflow-cycle:{self.workflow_cycle_ordinal}:"
            f"stage-cycle:{self.stage_cycle_ordinal}:slot:{self.operation_slot_id}"
        )


@dataclass(frozen=True)
class CandidateOrderingKey:
    priority: int
    identity: StageCandidateIdentity

    def as_tuple(self) -> tuple[object, ...]:
        identity = self.identity
        return (
            self.priority,
            identity.stage_id.encode("utf-8"),
            identity.mapped_instance_presence,
            (
                b""
                if identity.mapped_instance_presence == 0
                else identity.mapped_instance_id.encode("utf-8")
            ),
            identity.workflow_cycle_ordinal,
            identity.stage_cycle_ordinal,
            identity.operation_slot_id.encode("utf-8"),
        )


@dataclass(frozen=True)
class StageExecutionIdentity:
    run_id: str
    execution_epoch: int
    candidate: StageCandidateIdentity
    semantic_attempt: int
    execution_generation: int = 1

    @property
    def semantic_key(self) -> str:
        return (
            f"{self.run_id}:execution-epoch:{self.execution_epoch}:"
            f"{self.candidate.semantic_prefix}:semantic-attempt:{self.semantic_attempt}"
        )


@dataclass(frozen=True)
class DependencyProjection:
    dependency_id: str
    generation: int = 1
    disposition: DependencyDisposition = DependencyDisposition.UNRESOLVED
    evidence_refs: tuple[str, ...] = ()
    supersedes_generation: int | None = None


@dataclass(frozen=True)
class FairnessCursorState:
    group_ring_cursor: int = 0
    candidate_cursors: dict[str, CandidateOrderingKey | None] = field(default_factory=dict)


@dataclass(frozen=True)
class ProducerLiability:
    semantic_attempt_id: str
    reservation_id: str
    reserved_amounts: dict[str, int] = field(default_factory=dict)
    child_closed_or_quiesced: bool = False
    reservations_and_usage_settled: bool = False
    effects_settled: bool = False
    cancellation_reconciled: bool = False
    result_decision: ResultDecision | None = None

    @property
    def closed(self) -> bool:
        return (
            self.child_closed_or_quiesced
            and self.reservations_and_usage_settled
            and self.effects_settled
            and self.cancellation_reconciled
            and self.result_decision is not None
        )


@dataclass(frozen=True)
class StageInstanceProjection:
    candidate: StageCandidateIdentity
    status: StageStatus = "blocked"
    semantic_attempt: int = 0
    admitted_operation_request_ref: str | None = None
    frozen_input_refs: tuple[str, ...] = ()
    output_refs: tuple[str, ...] = ()
    obligation_evidence_refs: tuple[str, ...] = ()
    wait_condition_id: str | None = None
    pause_decision_id: str | None = None


@dataclass(frozen=True)
class AcceptedResultFact:
    identity: StageExecutionIdentity
    operation_result: dict[str, object]
    accepted_at_order: int


@dataclass(frozen=True)
class StageGraphAcceptedProjection:
    identity: ExecutionIdentity
    family_version: int
    run_version: int
    workflow_cycle_ordinal: int = 0
    stages: dict[str, StageInstanceProjection] = field(default_factory=dict)
    dependencies: dict[str, DependencyProjection] = field(default_factory=dict)
    fairness: FairnessCursorState = field(default_factory=FairnessCursorState)
    producer_liabilities: dict[str, ProducerLiability] = field(default_factory=dict)
    accepted_results: tuple[AcceptedResultFact, ...] = ()
    accepted_obligation_evidence: frozenset[str] = frozenset()
    invalidated_stage_ids: frozenset[str] = frozenset()

    @property
    def digest(self) -> str:
        payload = asdict(self)
        payload.pop("run_version")
        return sha256_digest(payload)


@dataclass(frozen=True)
class StageOperationAdmissionProposal:
    ordering_key: CandidateOrderingKey
    identity: StageExecutionIdentity
    operation_request_key: str
    exact_operation_request_ref: str
    reservation_id: str
    reservation: dict[str, int]
    frozen_input_refs: tuple[str, ...]
    selected_ring_index: int
    next_fairness: FairnessCursorState


@dataclass(frozen=True)
class StageResultObservation:
    identity: StageExecutionIdentity
    operation_result: dict[str, object]
    child_closed_or_quiesced: bool
    reservations_and_usage_settled: bool
    effects_settled: bool
    cancellation_reconciled: bool
    accepted_order: int


@dataclass(frozen=True)
class LateResultFacts:
    consumer_already_admitted: bool = False
    dependency_terminally_disposed: bool = False
    producer_invalidated: bool = False
    generation_superseded: bool = False
    evidence_invalid: bool = False
    run_cancelling: bool = False
    terminalization_started: bool = False
    run_terminal: bool = False


@dataclass(frozen=True)
class ResultDispositionProposal:
    identity: StageExecutionIdentity
    decision: ResultDecision
    dependency_dispositions: dict[str, DependencyDisposition]
    matched_veto: str | None = None
    matched_rule_id: str | None = None
    quarantine_reason: str | None = None


@dataclass(frozen=True)
class WorkflowInvalidationProposal:
    next_workflow_cycle_ordinal: int
    invalidation_frontier: tuple[str, ...]
    invalidated_stage_ids: tuple[str, ...]
    reused_output_refs: dict[str, tuple[str, ...]]
    next_objective: str


@dataclass(frozen=True)
class StageGraphCompletionProposal:
    required_obligations_accepted: bool
    pending_dependency_ids: tuple[str, ...]
    open_producer_liability_ids: tuple[str, ...]
    valid_output_refs: tuple[str, ...]

    @property
    def can_terminalize(self) -> bool:
        return (
            self.required_obligations_accepted
            and not self.pending_dependency_ids
            and not self.open_producer_liability_ids
        )


class StageGraphDecisionMutation(AtomicFamilyMutation):
    family_kind: Literal["stagegraph"] = "stagegraph"
    mutation_kind: Literal["decision_committed"] = "decision_committed"
    decision_kind: Literal[
        "operation_admitted",
        "result_decided",
        "wait_decided",
        "cycle_decided",
        "completion_proposed",
    ]
    prior_projection_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    next_projection_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    decision_payload: dict[str, object]

    @model_validator(mode="after")
    def decision_changes_projection(self) -> StageGraphDecisionMutation:
        if self.prior_projection_digest == self.next_projection_digest:
            raise ValueError("StageGraph decisions must advance the accepted projection")
        return self


@dataclass(frozen=True)
class StageOperationRequest:
    proposal: StageOperationAdmissionProposal
    operation: OperationWorkflowRequest


@dataclass(frozen=True)
class StageOperationResult:
    observation: StageResultObservation
    proposal: ResultDispositionProposal | None = None


@dataclass(frozen=True)
class StageGraphInitializeRequest:
    run_id: str
    request_scope: str
    expected_run_version: int
    initial_projection: StageGraphAcceptedProjection
    occurred_at: datetime
    idempotency_issuer: str
    correlation_id: str


@dataclass(frozen=True)
class StageGraphInitializeResult:
    accepted: bool
    projection: StageGraphAcceptedProjection
    reason_code: str


@dataclass(frozen=True)
class StageGraphAdmissionActivityRequest:
    run_id: str
    request_scope: str
    projection: StageGraphAcceptedProjection
    proposal: StageOperationAdmissionProposal
    operation: OperationWorkflowRequest | None
    blueprint: dict[str, object]
    effective_max_concurrency: int
    occurred_at: datetime
    idempotency_issuer: str
    correlation_id: str


@dataclass(frozen=True)
class StageGraphAdmissionActivityResult:
    accepted: bool
    projection: StageGraphAcceptedProjection
    operation: OperationWorkflowRequest | None
    reason_code: str


@dataclass(frozen=True)
class StageGraphResultActivityRequest:
    run_id: str
    request_scope: str
    projection: StageGraphAcceptedProjection
    observation: StageResultObservation
    late_facts: LateResultFacts
    blueprint: dict[str, object]
    effective_max_concurrency: int
    occurred_at: datetime
    idempotency_issuer: str
    correlation_id: str


@dataclass(frozen=True)
class StageGraphResultActivityResult:
    accepted: bool
    projection: StageGraphAcceptedProjection
    proposal: ResultDispositionProposal
    reason_code: str


@dataclass(frozen=True)
class StageGraphCompletionActivityRequest:
    run_id: str
    request_scope: str
    projection: StageGraphAcceptedProjection
    proposal: StageGraphCompletionProposal
    workflow_type_digest: str
    occurred_at: datetime
    idempotency_issuer: str
    correlation_id: str


@dataclass(frozen=True)
class StageGraphCompletionActivityResult:
    accepted: bool
    terminal_outcome: RunOutcome | None
    resulting_run_version: int
    reason_code: str


@dataclass(frozen=True)
class WorkflowEvaluationRequest:
    run_id: str
    workflow_cycle_ordinal: int
    objective: str
    current_output_refs: dict[str, tuple[str, ...]]
    request_scope: str
    effective_configuration_digest: str
    blueprint_digest: str


@dataclass(frozen=True)
class WorkflowEvaluationResult:
    action: Literal["accept", "cycle", "fail"]
    evaluation_ref: str
    invalidation_frontier: tuple[str, ...] = ()
    next_objective: str = ""
    evaluation_contract_ref: str = ""
    objective_contract_ref: str = ""


@dataclass(frozen=True)
class LifecycleCommandRequest:
    command_id: str
    expected_run_version: int
    action: dict[str, Any]
    reason: str
    evidence_refs: tuple[str, ...] = ()
    occurred_at: datetime | None = None
    run_id: str = ""
    request_scope: str = ""
    effective_configuration_digest: str = ""
    idempotency_issuer: str = ""
    correlation_id: str = ""
    blueprint_digest: str = ""


@dataclass(frozen=True)
class LifecycleCommandOutcome:
    accepted: bool
    resulting_run_version: int
    phase: str
    reason_code: str
    evidence_frontier_digest: str = ""
    obligation_revision: str = ""
    accepted_obligation_evidence_digest: str = ""
    required_obligations_accepted: bool = False
    workflow_type_digest: str = ""
    terminal_outcome: RunOutcome | None = None


@dataclass(frozen=True)
class StageGraphRunInput:
    run_id: str
    request_scope: str
    effective_configuration_digest: str
    workflow_type_digest: str
    blueprint_digest: str
    blueprint: dict[str, Any]
    initial_run_version: int = 1
    execution_epoch: int = 1
    max_concurrency: int = 1
    task_timeout_seconds: int = 30
    orchestration_authority_ref: str = "orchestration-authority"
    lifecycle_idempotency_issuer: str = "stagegraph-worker"
    correlation_id: str = ""
    baseline_reservation: dict[str, int] = field(default_factory=dict)
    semantic_input_binding_ref: str = ""
    tenant_scope: str = ""
    materialize_typed_result: bool = False
    durable_operation_children: Literal[True] = True
    operation_requests: dict[str, OperationWorkflowRequest] = field(default_factory=dict)
    initial_projection: StageGraphAcceptedProjection | None = None
    continue_as_new_event_threshold: int = 10_000
    force_continue_as_new: bool = False


@dataclass(frozen=True)
class StageGraphRunResult:
    run_id: str
    workflow_cycles: int
    execution_epoch: int
    family_version: int
    output_refs: dict[str, tuple[str, ...]]
    reused_output_refs: dict[str, tuple[str, ...]]
    schedule_trace: tuple[str, ...]
    completion_proposal: StageGraphCompletionProposal


GoalVerifierAction = Literal[
    "continue",
    "repair",
    "degrade",
    "stop",
    "fork",
    "escalate",
    "verified_completion",
]

GoalStopReason = Literal[
    "authority_breach",
    "hard_budget_exhausted",
    "verified_completion",
    "irrecoverable_failure",
    "no_progress",
    "repeated_blocker",
    "iteration_limit",
    "degraded",
    "verifier_stop",
    "fork_requested",
    "escalation_requested",
]

GoalExecutionStatus = Literal[
    "ready",
    "executing",
    "awaiting_verification",
    "terminal",
]


@dataclass(frozen=True)
class GoalIterationIdentity:
    run_id: str
    goal_iteration: int
    goal_revision_id: str
    execution_epoch: int

    @property
    def semantic_key(self) -> str:
        return (
            f"{self.run_id}:execution-epoch:{self.execution_epoch}:"
            f"goal-iteration:{self.goal_iteration}:revision:{self.goal_revision_id}"
        )


@dataclass(frozen=True)
class GoalAgentRunIdentity:
    iteration: GoalIterationIdentity
    agent_run: int
    session_generation: int

    @property
    def semantic_key(self) -> str:
        return (
            f"{self.iteration.semantic_key}:agent-run:{self.agent_run}:"
            f"session-generation:{self.session_generation}"
        )


@dataclass(frozen=True)
class GoalRevision:
    revision_id: str
    revision: int
    parent_revision_id: str | None
    protected_scope_digest: str
    objective: str
    evidence_refs: tuple[str, ...]
    unmet_obligations: tuple[str, ...]
    author: str
    deciding_authority: str
    applicability: Literal["next_iteration", "remaining_run"]
    tactics: tuple[str, ...] = ()
    subgoals: tuple[str, ...] = ()
    coverage_emphasis: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.revision_id or self.revision < 1:
            raise ValueError("goal revisions require an identity and positive revision")
        if (self.revision == 1) != (self.parent_revision_id is None):
            raise ValueError("only the initial Goal Revision omits a parent")
        if not self.protected_scope_digest or not self.objective:
            raise ValueError("goal revisions require a protected scope and objective")
        if not self.author or not self.deciding_authority:
            raise ValueError("goal revisions require author and deciding authority")


@dataclass(frozen=True)
class GoalHandoffCheckpoint:
    checkpoint_id: str
    agent_run_identity: GoalAgentRunIdentity
    goal_revision_id: str
    protected_scope_digest: str
    instructions: str
    state_refs: tuple[str, ...] = ()
    artifact_refs: tuple[str, ...] = ()
    workspace_ref: str = ""

    def __post_init__(self) -> None:
        if not self.checkpoint_id or not self.instructions:
            raise ValueError("goal handoff checkpoints require identity and instructions")


@dataclass(frozen=True)
class GoalExecutionClaim:
    identity: GoalAgentRunIdentity
    idempotency_key: str
    operation_class: str
    objective: str
    protected_scope_digest: str
    reservation_id: str
    reservation: dict[str, int]
    session_mode: Literal["reuse", "fresh", "fresh_from_handoff"]
    session_id: str
    workspace_mode: Literal["shared", "fresh", "fresh_from_snapshot"]
    workspace_namespace: str
    snapshot_mode: Literal["none", "on_rollover", "every_iteration", "on_failure"]
    prior_checkpoint_id: str = ""
    fresh_agent_token_threshold: int = 0
    handoff_token_reserve: int = 0
    token_budget_remaining: int = 0
    request_scope: str = ""
    semantic_input_binding_ref: str = ""
    effective_configuration_digest: str = ""
    blueprint_digest: str = ""


@dataclass(frozen=True)
class GoalExecutionResult:
    identity: GoalAgentRunIdentity
    disposition: Literal["completed", "failed", "blocked"]
    output_refs: tuple[str, ...] = ()
    completion_claim: bool = False
    actual_usage: dict[str, int] = field(default_factory=dict)
    blocker_class: str = ""
    authority_breach_ref: str = ""
    hard_budget_exhausted_dimensions: tuple[str, ...] = ()
    irrecoverable_failure_ref: str = ""
    handoff_checkpoint: GoalHandoffCheckpoint | None = None
    temporal_activity_attempt: int = 1
    output_contract_ref: str = ""


@dataclass(frozen=True)
class GoalHandoffRequest:
    claim: GoalExecutionClaim
    execution_result: GoalExecutionResult
    protected_scope_digest: str
    verification_ref: str = ""
    unmet_obligations: tuple[str, ...] = ()
    fallback: bool = False
    failure_reason: str = ""


@dataclass(frozen=True)
class GoalHandoffResult:
    checkpoint: GoalHandoffCheckpoint
    actual_usage: dict[str, int] = field(default_factory=dict)
    fallback_used: bool = False
    output_contract_ref: str = ""


@dataclass(frozen=True)
class GoalVerificationRequest:
    claim: GoalExecutionClaim
    execution_result: GoalExecutionResult
    verifier_ref: str
    acceptance_contract_ref: str
    accepted_output_refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class GoalVerificationResult:
    identity: GoalAgentRunIdentity
    action: GoalVerifierAction
    verification_ref: str
    verifier_ref: str
    acceptance_contract_ref: str
    progress_made: bool
    evidence_refs: tuple[str, ...] = ()
    unmet_obligations: tuple[str, ...] = ()
    blocker_class: str = ""
    authority_breach_ref: str = ""
    hard_budget_exhausted_dimensions: tuple[str, ...] = ()
    irrecoverable_failure_ref: str = ""
    proposed_revision: GoalRevision | None = None
    actual_usage: dict[str, int] = field(default_factory=dict)
    output_contract_ref: str = ""


@dataclass(frozen=True)
class GoalDirectedRunInput:
    run_id: str
    request_scope: str
    effective_configuration_digest: str
    blueprint_digest: str
    blueprint: dict[str, Any]
    protected_scope_digest: str
    initial_revision: GoalRevision
    initial_run_version: int = 1
    execution_epoch: int = 1
    task_timeout_seconds: int = 300
    orchestration_authority_ref: str = "orchestration-authority"
    lifecycle_idempotency_issuer: str = "goal-directed-worker"
    correlation_id: str = ""
    baseline_reservation: dict[str, int] = field(default_factory=dict)
    required_obligation_refs: tuple[str, ...] = ()
    semantic_input_binding_ref: str = ""
    tenant_scope: str = ""
    materialize_typed_result: bool = False
    durable_operation_children: bool = False


@dataclass(frozen=True)
class GoalDirectedExecutionState:
    run_id: str
    execution_epoch: int
    protected_scope_digest: str
    active_revision: GoalRevision
    accepted_revisions: tuple[GoalRevision, ...]
    next_goal_iteration: int = 1
    next_agent_run: int = 1
    session_generation: int = 1
    session_token_usage: int = 0
    workspace_generation: int = 1
    status: GoalExecutionStatus = "ready"
    active_claim: GoalExecutionClaim | None = None
    pending_result: GoalExecutionResult | None = None
    execution_results: tuple[GoalExecutionResult, ...] = ()
    verification_results: tuple[GoalVerificationResult, ...] = ()
    handoff_checkpoints: tuple[GoalHandoffCheckpoint, ...] = ()
    output_refs: tuple[str, ...] = ()
    no_progress_iterations: int = 0
    repeated_blocker_count: int = 0
    last_blocker_class: str = ""
    rollover_count: int = 0
    next_session_mode: Literal["reuse", "fresh", "fresh_from_handoff"] = "reuse"
    degraded: bool = False
    stop_reason: GoalStopReason | None = None
    final_action: GoalVerifierAction | None = None
    request_scope: str = ""
    semantic_input_binding_ref: str = ""
    effective_configuration_digest: str = ""
    blueprint_digest: str = ""


@dataclass(frozen=True)
class GoalDirectedRunResult:
    run_id: str
    execution_epoch: int
    status: Literal["terminal"]
    stop_reason: GoalStopReason
    final_action: GoalVerifierAction
    goal_iterations: int
    agent_runs: int
    rollover_count: int
    active_revision_id: str
    accepted_revision_ids: tuple[str, ...]
    output_refs: tuple[str, ...]
    handoff_checkpoints: tuple[GoalHandoffCheckpoint, ...]
    execution_results: tuple[GoalExecutionResult, ...]
    verification_results: tuple[GoalVerificationResult, ...]
