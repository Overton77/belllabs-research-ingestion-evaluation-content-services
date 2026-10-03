from __future__ import annotations

from dataclasses import asdict, dataclass, field, replace
from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import Field, model_validator

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import OperationWorkflowRequest
from mission_control.domain.policies.contracts import ExecutionTarget, RunOutcome
from mission_control.domain.policies.family_admission import AtomicFamilyMutation
from mission_control.domain.programs.search_attributes import SearchAttributePolicy

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
    # RRM-007 (F7): on a `duplicate`, the status the root cached for this message, so a
    # transport never treats a cached gap or stale result as a delivery.
    cached_status: Literal["accepted", "duplicate", "stale_generation", "gap"] = "accepted"


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
            receipt.sequence for receipt in self.message_receipts if receipt.status == "accepted"
        )
        if accepted_sequences != tuple(sorted(set(accepted_sequences))):
            raise ValueError("accepted continuity receipts must be unique and ordered")
        if accepted_sequences and accepted_sequences[-1] > self.last_message_sequence:
            raise ValueError("receipt frontier exceeds the message sequence frontier")

    def next_technical_segment(self) -> RunContinuityState:
        return replace(self, technical_segment=self.technical_segment + 1)


# --- Boundary commands at a family boundary (RRM-007, CON-CP-WORKFLOW-MESSAGE-V1) -------

# Family-level execution generation: the root continuity's generation, which nothing
# advances today. A delivery for another generation is `stale_generation`.
FAMILY_EXECUTION_GENERATION = 1
FamilyBoundaryCommandKind = Literal["pause", "resume", "satisfy_wait"]
BoundaryAckStatus = Literal["delivered", "duplicate", "stale_generation", "stale_target", "gap"]


@dataclass(frozen=True)
class BoundaryCommandDelivery:
    """One accepted command as the delivery service hands it to a family boundary.

    `payload` is the exact run-control action (JSON) and `payload_digest` its fingerprint,
    so the boundary applies exactly what run control accepted.
    """

    command_id: str
    kind: FamilyBoundaryCommandKind
    target_sequence: int
    execution_epoch: int
    execution_generation: int
    accepted_run_version: int
    payload: dict[str, Any]
    payload_digest: str
    # The accepting principal's issuer: with `command_id` the exact command identity (F5).
    idempotency_issuer: str = ""

    def __post_init__(self) -> None:
        if not self.command_id or not self.payload_digest or self.target_sequence < 1:
            raise ValueError("boundary deliveries require an identity, digest and sequence")
        if self.execution_epoch < 1 or self.execution_generation < 1:
            raise ValueError("boundary delivery epoch and generation must be positive")


@dataclass(frozen=True)
class BoundaryCommandAck:
    """The family boundary's Update return value: evidence of delivery, never of application."""

    command_id: str
    status: BoundaryAckStatus
    technical_segment: int
    detail: str = ""


CancelAckStatus = Literal["delivered", "duplicate", "stale_generation", "stale_target"]


@dataclass(frozen=True)
class CancelDelivery:
    """RRM-008 (REQ-CP-EXEC-008 step 2): one accepted cancel as the delivery service hands
    it to the root and then to the family, through the dedicated `deliver_cancel` Update.

    A cancel is sequenced in its own `cancel` space (RRM-007 N1): it never takes a place in
    the root's contiguous `execution` sequence, so it can neither open a gap there nor be
    blocked by one.
    """

    command_id: str
    idempotency_issuer: str
    target_sequence: int
    execution_epoch: int
    execution_generation: int
    accepted_run_version: int
    payload_digest: str
    reason: str = ""

    def __post_init__(self) -> None:
        if not self.command_id or not self.payload_digest or self.target_sequence < 1:
            raise ValueError("cancel deliveries require an identity, digest and sequence")
        if self.execution_epoch < 1 or self.execution_generation < 1:
            raise ValueError("cancel delivery epoch and generation must be positive")


@dataclass(frozen=True)
class CancelAck:
    """The root's or family's `deliver_cancel` return value: evidence that the journaled
    cancellation intent reached that execution, never that the run is cancelled."""

    command_id: str
    status: CancelAckStatus
    technical_segment: int
    detail: str = ""


@dataclass(frozen=True)
class FamilyPause:
    """A pause the family boundary applied; scope strings follow `PauseDecision.scope`."""

    decision_id: str
    scope: tuple[str, ...]


@dataclass(frozen=True)
class BoundaryLifecycleRequest:
    """A family boundary's run-control fact, issued through its application activity.

    `action` is the exact lifecycle action (JSON); the activity binds the current run
    version itself, so a family never fails on version drift caused by pending commands.
    """

    command_id: str
    action: dict[str, Any]
    reason: str
    boundary_ref: str
    occurred_at: datetime | None = None
    run_id: str = ""
    request_scope: str = ""
    idempotency_issuer: str = ""
    correlation_id: str = ""
    evidence_refs: tuple[str, ...] = ()
    # The delivered command this fact applies or rejects (empty for waits and quiescence).
    boundary_command_id: str = ""
    boundary_command_issuer: str = ""
    # Set when the boundary could not apply the delivered command; no action is executed.
    rejection_reason: str = ""


@dataclass(frozen=True)
class BoundaryLifecycleOutcome:
    accepted: bool
    status: str
    reason_code: str
    resulting_run_version: int
    phase: str
    # The boundary command's receipt state after this fact (`applied`, `rejected`, ...) and
    # its target sequence, so a boundary that applied its own command keeps its contiguity.
    receipt_state: str = ""
    target_sequence: int = 0


@dataclass(frozen=True)
class GoalPausedState:
    """REQ-BP-GD-011: the durable paused state a GoalDirected family binds at a boundary."""

    pause_decision_id: str
    command_id: str
    active_revision_id: str
    next_goal_iteration: int
    session_generation: int
    next_session_mode: Literal["reuse", "fresh", "fresh_from_handoff"]
    handoff_ref: str
    effect_frontier_refs: tuple[str, ...]
    held_reservation_ids: tuple[str, ...]
    released_reservation_ids: tuple[str, ...] = ()
    next_iteration_reservation: dict[str, int] = field(default_factory=dict)
    paused_at_run_version: int = 0


@dataclass(frozen=True)
class BellLabsRunInput:
    schema_version: Literal["belllabs.temporal-root.v1", "mc.mission_run.v1"]
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
    # REQ-CP-EXEC-015: carried in the input (and through Continue-As-New); absent means
    # `disabled`, so every captured history replays unchanged.
    search_attribute_policy: SearchAttributePolicy = "disabled"
    # REQ-CP-EXEC-015 / REQ-CP-EXEC-012 (RRM-006): the source run of a fork root; it sets
    # `BellLabsParentRunId` on the root only. Absent for ordinary roots and every history.
    parent_run_id: str | None = None

    def __post_init__(self) -> None:
        if self.parent_run_id is not None and (
            not self.parent_run_id or self.parent_run_id == self.run_id
        ):
            raise ValueError("a fork root's parent run must be another BellLabs run")
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
        if self.schema_version == "mc.mission_run.v1":
            from mission_control.contracts.identities import mission_root_id

            return mission_root_id(self.request_scope, self.run_id)
        return f"belllabs-run/{self.run_id}"

    @property
    def family_workflow_id(self) -> str:
        if self.schema_version == "mc.mission_run.v1":
            return f"{self.workflow_id}/family/{self.continuity.execution_epoch}"
        return f"family/{self.run_id}/{self.continuity.execution_epoch}"

    def validate_mission_binding(self) -> None:
        """New roots cannot launch family input from another admitted authority."""
        if self.schema_version != "mc.mission_run.v1":
            raise ValueError("Mission Control requires its scoped root envelope")
        _ = self.workflow_id  # validates canonical scope and run identity
        if self.family not in {"StageGraph", "GoalDirected"}:
            raise ValueError("unsupported Mission Control workflow family")
        expected = {
            "run_id": self.run_id,
            "request_scope": self.request_scope,
            "effective_configuration_digest": self.effective_configuration_digest,
            "workflow_type_digest": self.workflow_type_digest,
        }
        if any(self.family_input.get(key) != value for key, value in expected.items()):
            raise ValueError(
                "family input differs from admitted root identity or immutable binding"
            )
        if self.family_input.get("execution_epoch", 1) != self.continuity.execution_epoch:
            raise ValueError("family execution epoch differs from root continuity")


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
        mapped = "none" if self.mapped_instance_presence == 0 else self.mapped_instance_id
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
            f"{self.run_id}:operation:execution-epoch:{self.execution_epoch}:"
            f"{self.candidate.semantic_prefix}:attempt:{self.semantic_attempt}"
        )

    @property
    def operation_id(self) -> str:
        return f"execution-epoch:{self.execution_epoch}:{self.candidate.semantic_prefix}"

    @property
    def stage_id(self) -> str:
        return self.candidate.stage_id

    @property
    def workflow_cycle(self) -> int:
        return self.candidate.workflow_cycle_ordinal

    @property
    def stage_cycle(self) -> int:
        return self.candidate.stage_cycle_ordinal

    @property
    def operation_attempt(self) -> int:
        return self.semantic_attempt


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
    objective_override: str | None = None


@dataclass(frozen=True)
class AcceptedResultFact:
    identity: StageExecutionIdentity
    operation_result: dict[str, Any]
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
    objective_override: str | None = None


@dataclass(frozen=True)
class StageResultObservation:
    identity: StageExecutionIdentity
    operation_result: dict[str, Any]
    child_closed_or_quiesced: bool
    reservations_and_usage_settled: bool
    effects_settled: bool
    cancellation_reconciled: bool
    accepted_order: int
    operation_disposition: Literal["completed", "cancelled", "failed", "in_doubt"] = "completed"


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
class StageInvalidationProposal:
    stage_id: str
    prior_stage_cycle_ordinal: int
    next_stage_cycle_ordinal: int
    invalidated_stage_ids: tuple[str, ...]
    reused_output_refs: dict[str, tuple[str, ...]]
    unmet_obligation_refs: tuple[str, ...]
    accepted_evidence_refs: tuple[str, ...]
    allowed_input_refs: tuple[str, ...]
    prior_result_refs: tuple[str, ...]
    next_objective: str


@dataclass(frozen=True)
class StageGraphCompletionProposal:
    required_obligations_accepted: bool
    pending_dependency_ids: tuple[str, ...]
    open_producer_liability_ids: tuple[str, ...]
    valid_output_refs: tuple[str, ...]
    # RRM-008 (REQ-CP-EXEC-008 step 7): a cancellation completion. Every producer liability
    # is closed; unresolved dependencies are cancelled, not pending; the reducer decides the
    # `cancelled` outcome once budgets and effects are settled.
    cancelled: bool = False

    @property
    def can_terminalize(self) -> bool:
        if self.open_producer_liability_ids:
            return False
        if self.cancelled:
            return True
        return self.required_obligations_accepted and not self.pending_dependency_ids


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
    """Bound input for a native StageGraph semantic operation handler."""

    identity: StageExecutionIdentity
    idempotency_key: str
    objective: str
    input_refs: tuple[str, ...]
    reservation_id: str
    reservation: dict[str, int]
    workspace_namespace: str
    request_scope: str = ""
    semantic_input_binding_ref: str = ""
    effective_configuration_digest: str = ""
    blueprint_digest: str = ""
    cycle_evaluation_contract_ref: str = ""
    cycle_objective_contract_ref: str = ""


@dataclass(frozen=True)
class StageOperationResult:
    """Typed native handler observation; family authority still decides admission."""

    identity: StageExecutionIdentity
    disposition: Literal["completed", "skipped", "failed", "waiting", "paused"]
    output_refs: tuple[str, ...] = ()
    evaluation: Literal["accept", "cycle", "degrade", "escalate"] = "accept"
    evaluation_ref: str = ""
    next_objective: str = ""
    evaluation_contract_ref: str = ""
    objective_contract_ref: str = ""
    wait_condition_id: str = ""
    pause_decision_id: str = ""
    handoff_ref: str = ""
    temporal_activity_attempt: int = 1
    actual_usage: dict[str, int] = field(default_factory=dict)
    pending_external_usage: dict[str, int] = field(default_factory=dict)
    output_contract_ref: str = ""


@dataclass(frozen=True)
class StageGraphInitializeRequest:
    run_id: str
    request_scope: str
    expected_run_version: int
    initial_projection: StageGraphAcceptedProjection
    occurred_at: datetime
    idempotency_issuer: str
    correlation_id: str
    # RRM-007: the family declares the execution that applies boundary commands.
    execution_target: ExecutionTarget | None = None


@dataclass(frozen=True)
class StageGraphInitializeResult:
    accepted: bool
    projection: StageGraphAcceptedProjection
    reason_code: str
    # RRM-008: the run phase after the start fact; `cancelling` when the cancel was accepted
    # before the family started, so the family runs the saga at once.
    phase: str = ""


@dataclass(frozen=True)
class StageGraphAdmissionActivityRequest:
    run_id: str
    request_scope: str
    projection: StageGraphAcceptedProjection
    proposal: StageOperationAdmissionProposal
    operation: OperationWorkflowRequest | None
    blueprint: dict[str, Any]
    effective_max_concurrency: int
    occurred_at: datetime
    idempotency_issuer: str
    correlation_id: str
    semantic_input_binding_ref: str = ""
    effective_configuration_digest: str = ""


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
    blueprint: dict[str, Any]
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
class StageGraphCycleActivityRequest:
    run_id: str
    request_scope: str
    projection: StageGraphAcceptedProjection
    invalidation_frontier: tuple[str, ...]
    next_objective: str
    evaluation_ref: str
    evaluation_contract_ref: str
    objective_contract_ref: str
    blueprint: dict[str, Any]
    effective_max_concurrency: int
    occurred_at: datetime
    idempotency_issuer: str
    correlation_id: str
    cycle_scope: Literal["stage", "workflow"] = "workflow"
    stage_id: str | None = None


@dataclass(frozen=True)
class StageGraphCycleActivityResult:
    accepted: bool
    projection: StageGraphAcceptedProjection
    proposal: WorkflowInvalidationProposal | StageInvalidationProposal
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
class StageGraphBaselineSettlementRequest:
    """RRM-021 (REQ-CP-RUN-006): release the run's admitted baseline reservation."""

    run_id: str
    request_scope: str
    occurred_at: datetime
    idempotency_issuer: str
    correlation_id: str
    baseline_reservation: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class StageGraphBaselineSettlementResult:
    accepted: bool
    resulting_run_version: int
    reason_code: str


@dataclass(frozen=True)
class WorkflowEvaluationRequest:
    run_id: str
    workflow_cycle: int
    objective: str
    current_output_refs: dict[str, tuple[str, ...]]
    request_scope: str
    effective_configuration_digest: str
    blueprint_digest: str
    execution_lineage: tuple[StageOperationResult, ...] = ()
    semantic_input_binding_ref: str = ""
    evaluation_contract_ref: str = ""
    objective_contract_ref: str = ""


@dataclass(frozen=True)
class WorkflowEvaluationResult:
    action: Literal["accept", "cycle", "fail"]
    evaluation_ref: str
    invalidation_frontier: tuple[str, ...] = ()
    next_objective: str = ""
    evaluation_contract_ref: str = ""
    objective_contract_ref: str = ""
    output_contract_ref: str = ""


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
    search_attribute_policy: SearchAttributePolicy = "disabled"
    technical_segment: int = 1
    # RRM-007 (REQ-CP-EXEC-011): boundary state carried across Continue-As-New. Satisfied
    # and declared wait identities, applied pauses, delivered-but-unapplied commands and
    # the applied-command cache all survive the technical segment.
    satisfied_wait_ids: tuple[str, ...] = ()
    declared_wait_ids: tuple[str, ...] = ()
    active_pauses: tuple[FamilyPause, ...] = ()
    pending_boundary_commands: tuple[BoundaryCommandDelivery, ...] = ()
    applied_boundary_command_ids: tuple[str, ...] = ()
    quiescent: bool = False
    last_delivered_sequence: int = 0
    # RRM-008: see `GoalDirectedRunInput`.
    cancellation_retry_seconds: int = 30
    cancel_requested: bool = False


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


GoalVerifierDecision = Literal["accepted", "rejected", "revision_required", "repair_required"]
GoalOperationRole = Literal["executor", "verifier"]
GoalExecutionStatus = Literal[
    "ready",
    "executing",
    "awaiting_verification",
    "waiting",
    "paused",
    "stopping",
]
GoalConvergenceAction = Literal[
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
GoalConvergenceReason = Literal[
    "authority_breach",
    "hard_budget_exhausted",
    "verified_completion",
    "irrecoverable_failure",
    "no_progress",
    "repeated_blocker",
    "iteration_limit",
    "soft_budget_response",
    "bounded_revision",
    "repair_requested",
    "continue",
    "scope_expansion",
    "compaction_failure",
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
    schema_version: Literal["belllabs.goal-revision.v1"]
    revision_id: str
    revision: int
    parent_revision_id: str | None
    canonical_digest: str
    envelope_digest: str
    objective: str
    tactical_changes: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    unmet_obligations: tuple[str, ...]
    proposer: str
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
        if not self.canonical_digest or not self.envelope_digest or not self.objective:
            raise ValueError("goal revisions require canonical and envelope digests")
        if not self.proposer or not self.deciding_authority:
            raise ValueError("goal revisions require proposer and deciding authority")


@dataclass(frozen=True)
class GoalHandoff:
    schema_version: Literal["belllabs.goal-handoff.v1"]
    handoff_id: str
    handoff_digest: str
    run_id: str
    execution_epoch: int
    goal_revision_id: str
    source_iteration: GoalIterationIdentity
    accepted_fact_refs: tuple[str, ...]
    evidence_refs: tuple[str, ...]
    artifact_refs: tuple[str, ...] = ()
    attempted_tactics: tuple[str, ...] = ()
    rejected_tactics: tuple[tuple[str, str], ...] = ()
    unresolved_obligations: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    effect_frontier_refs: tuple[str, ...] = ()
    pending_liability_refs: tuple[str, ...] = ()
    consumed_budget: dict[str, int] = field(default_factory=dict)
    reserved_budget: dict[str, int] = field(default_factory=dict)
    remaining_budget: dict[str, int] = field(default_factory=dict)
    remaining_iterations: int = 0
    protected_context_facts: tuple[tuple[str, str], ...] = ()
    context_selection_policy_ref: str = ""
    context_compaction_policy_ref: str = ""
    context_selection_refs: tuple[str, ...] = ()
    compaction_decision_ref: str = ""
    compaction_status: Literal["accepted", "failed"] = "accepted"
    compaction_attempt: int = 1
    compaction_failure_ref: str = ""
    workspace_refs: tuple[str, ...] = ()
    snapshot_refs: tuple[str, ...] = ()
    source_document_digests: tuple[str, ...] = ()
    source_binding_digests: tuple[str, ...] = ()
    continuation_instructions: str = ""

    def __post_init__(self) -> None:
        if not self.handoff_id or not self.handoff_digest or not self.continuation_instructions:
            raise ValueError(
                "goal handoffs require identity, digest, and continuation instructions"
            )
        if self.run_id != self.source_iteration.run_id:
            raise ValueError("goal handoff run does not match its source iteration")
        if self.execution_epoch != self.source_iteration.execution_epoch:
            raise ValueError("goal handoff epoch does not match its source iteration")
        if self.goal_revision_id != self.source_iteration.goal_revision_id:
            raise ValueError("goal handoff revision does not match its source iteration")
        if self.remaining_iterations < 0:
            raise ValueError("goal handoff remaining iterations cannot be negative")
        if self.compaction_attempt < 1:
            raise ValueError("goal handoff compaction attempt must be positive")
        if (self.compaction_status == "failed") != bool(self.compaction_failure_ref):
            raise ValueError("failed handoff compaction requires exactly one failure reference")


@dataclass(frozen=True)
class GoalExecutionClaim:
    identity: GoalAgentRunIdentity
    idempotency_key: str
    operation_class: str
    objective: str
    envelope_digest: str
    goal_revision_digest: str
    reservation_id: str
    reservation: dict[str, int]
    session_mode: Literal["reuse", "fresh", "fresh_from_handoff"]
    session_id: str
    workspace_mode: Literal["shared", "fresh", "fresh_from_snapshot"]
    workspace_namespace: str
    snapshot_mode: Literal["none", "on_rollover", "every_iteration", "on_failure"]
    prior_handoff_ref: str = ""
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
    operation_identity: str
    operation_binding_ref: str
    session_id: str
    workspace_id: str
    writable_paths: tuple[str, ...]
    output_refs: tuple[str, ...] = ()
    completion_claim: bool = False
    actual_usage: dict[str, int] = field(default_factory=dict)
    blocker_class: str = ""
    authority_breach_ref: str = ""
    hard_budget_exhausted_dimensions: tuple[str, ...] = ()
    irrecoverable_failure_ref: str = ""
    accepted_fact_refs: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    effect_frontier_refs: tuple[str, ...] = ()
    pending_liability_refs: tuple[str, ...] = ()
    handoff: GoalHandoff | None = None
    output_contract_ref: str = ""


@dataclass(frozen=True)
class GoalVerificationRequest:
    executor_claim: GoalExecutionClaim
    execution_result: GoalExecutionResult
    verifier_operation_identity: str
    verifier_binding_ref: str
    verifier_session_id: str
    verifier_workspace_id: str
    verifier_writable_paths: tuple[str, ...]
    rubric_ref: str
    rubric_version: int
    acceptance_contract_ref: str
    acceptance_version: int
    admitted_output_refs: tuple[str, ...]
    admitted_evidence_refs: tuple[str, ...]
    required_obligation_refs: tuple[str, ...]
    stale_frontier_digest: str


@dataclass(frozen=True)
class GoalVerificationResult:
    schema_version: Literal["belllabs.goal-verification.v1"]
    verification_id: str
    verification_digest: str
    executor_identity: GoalAgentRunIdentity
    verifier_operation_identity: str
    verifier_binding_ref: str
    verifier_policy_binding_ref: str
    verifier_session_id: str
    verifier_workspace_id: str
    verifier_writable_paths: tuple[str, ...]
    decision: GoalVerifierDecision
    verification_ref: str
    rubric_ref: str
    rubric_version: int
    acceptance_contract_ref: str
    acceptance_version: int
    progress_made: bool
    accepted_obligation_refs: tuple[str, ...] = ()
    findings: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    admitted_executor_output_refs: tuple[str, ...] = ()
    admitted_executor_evidence_refs: tuple[str, ...] = ()
    unmet_obligations: tuple[str, ...] = ()
    obligation_applicability: tuple[tuple[str, bool], ...] = ()
    stale_frontier_digest: str = ""
    blocker_class: str = ""
    authority_breach_ref: str = ""
    hard_budget_exhausted_dimensions: tuple[str, ...] = ()
    soft_budget_dimensions: tuple[str, ...] = ()
    irrecoverable_failure_ref: str = ""
    proposed_revision: GoalRevision | None = None
    scope_expansion_route: Literal["control_revision", "fork", "linked_run", "new_run"] | None = (
        None
    )
    route_ref: str = ""
    actual_usage: dict[str, int] = field(default_factory=dict)
    effect_refs: tuple[str, ...] = ()
    output_contract_ref: str = ""


@dataclass(frozen=True)
class GoalConvergenceFacts:
    authority_breach: bool = False
    hard_budget_exhausted: bool = False
    all_required_obligations_verified: bool = False
    irrecoverable_failure: bool = False
    no_progress_threshold_reached: bool = False
    repeated_blocker_threshold_reached: bool = False
    iteration_limit_reached: bool = False
    soft_budget_response_required: bool = False
    bounded_revision: GoalRevision | None = None
    repair_requested: bool = False
    scope_expansion_route: Literal["control_revision", "fork", "linked_run", "new_run"] | None = (
        None
    )


@dataclass(frozen=True)
class GoalConvergenceProposal:
    proposal_id: str
    action: GoalConvergenceAction
    reason: GoalConvergenceReason
    goal_revision_id: str
    source_iteration: GoalIterationIdentity
    verification_ref: str
    evidence_refs: tuple[str, ...] = ()
    route_ref: str = ""


@dataclass(frozen=True)
class GoalTerminalizationProposal:
    proposal_id: str
    expected_run_version: int
    goal_revision_id: str
    verifier_decision_ref: str
    obligation_evidence_refs: tuple[str, ...]
    output_refs: tuple[str, ...]
    degradation_refs: tuple[str, ...]
    blocker_refs: tuple[str, ...]
    budget_state_digest: str
    effect_frontier_digest: str
    stale_frontier_digest: str
    effects_settled: bool
    proposed_outcome: Literal["complete", "partial_or_fail", "fail"]


@dataclass(frozen=True)
class GoalContinuationState:
    active_revision: GoalRevision
    accepted_revisions: tuple[GoalRevision, ...]
    next_goal_iteration: int
    next_agent_run: int
    session_generation: int
    session_token_usage: int
    workspace_generation: int
    handoffs: tuple[GoalHandoff, ...]
    output_refs: tuple[str, ...]
    no_progress_iterations: int
    repeated_blocker_count: int
    last_blocker_class: str
    rollover_count: int
    next_session_mode: Literal["reuse", "fresh", "fresh_from_handoff"]
    completed_goal_iterations: int = 0
    completed_agent_runs: int = 0
    lineage_digest: str = ""
    # RRM-007 (REQ-BP-GD-011): a durable pause survives Continue-As-New.
    paused: GoalPausedState | None = None


@dataclass(frozen=True)
class GoalDirectedRunInput:
    run_id: str
    request_scope: str
    effective_configuration_digest: str
    blueprint_digest: str
    blueprint: dict[str, Any]
    envelope_digest: str
    initial_revision: GoalRevision
    initial_run_version: int = 1
    execution_epoch: int = 1
    task_timeout_seconds: int = 300
    orchestration_authority_ref: str = "orchestration-authority"
    lifecycle_idempotency_issuer: str = "goal-directed-worker"
    correlation_id: str = ""
    baseline_reservation: dict[str, int] = field(default_factory=dict)
    required_obligation_refs: tuple[str, ...] = ()
    required_output_contract_refs: tuple[str, ...] = ()
    semantic_input_binding_ref: str = ""
    family_version: int = 0
    technical_segment: int = 1
    continuation_handoff: GoalHandoff | None = None
    continuation_state: GoalContinuationState | None = None
    tenant_scope: str = ""
    materialize_typed_result: bool = False
    durable_operation_children: bool = False
    search_attribute_policy: SearchAttributePolicy = "disabled"
    # RRM-007: delivered-but-unapplied commands and the applied-command cache carried across
    # Continue-As-New; the segment length is configurable for forced-continuation proofs.
    pending_boundary_commands: tuple[BoundaryCommandDelivery, ...] = ()
    applied_boundary_command_ids: tuple[str, ...] = ()
    continue_as_new_iterations: int = 20
    # Forces one continuation at the next iteration boundary (also while paused).
    force_continue_as_new: bool = False
    last_delivered_sequence: int = 0
    # RRM-008: the first wait before the cancellation saga proposes terminalization again
    # when a liability remains (doubles up to one hour); the hint signal wakes it earlier.
    cancellation_retry_seconds: int = 30
    # RRM-008 (REQ-CP-EXEC-011): a delivered cancel carried across Continue-As-New.
    cancel_requested: bool = False
    # Fresh Mission Control roots bind the admitted Workflow Type independently of
    # the blueprint. Older inputs omit it; scoped admission fills the exact digest.
    workflow_type_digest: str = ""


@dataclass(frozen=True)
class GoalDirectedExecutionState:
    run_id: str
    execution_epoch: int
    envelope_digest: str
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
    handoffs: tuple[GoalHandoff, ...] = ()
    output_refs: tuple[str, ...] = ()
    no_progress_iterations: int = 0
    repeated_blocker_count: int = 0
    last_blocker_class: str = ""
    rollover_count: int = 0
    next_session_mode: Literal["reuse", "fresh", "fresh_from_handoff"] = "reuse"
    degraded: bool = False
    convergence_proposal: GoalConvergenceProposal | None = None
    terminalization_proposal: GoalTerminalizationProposal | None = None
    request_scope: str = ""
    semantic_input_binding_ref: str = ""
    effective_configuration_digest: str = ""
    blueprint_digest: str = ""
    completed_goal_iterations: int = 0
    completed_agent_runs: int = 0
    lineage_digest: str = ""


@dataclass(frozen=True)
class GoalDirectedRunResult:
    run_id: str
    execution_epoch: int
    # RRM-008: `cancelled` when the family completed the cancellation saga (no convergence
    # proposal; the reducer recorded the `cancelled` outcome).
    status: Literal["stopping", "cancelled"]
    convergence_proposal: GoalConvergenceProposal | None
    terminalization_proposal: GoalTerminalizationProposal | None
    goal_iterations: int
    agent_runs: int
    rollover_count: int
    active_revision_id: str
    accepted_revision_ids: tuple[str, ...]
    # Lineage: every iteration's executor outputs. The promoted run outputs are
    # `terminalization_proposal.output_refs` (REQ-BP-GD-004, RRM-019).
    output_refs: tuple[str, ...]
    handoffs: tuple[GoalHandoff, ...]
    execution_results: tuple[GoalExecutionResult, ...]
    verification_results: tuple[GoalVerificationResult, ...]
    lineage_digest: str = ""
