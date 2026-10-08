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

from mission_control.domain.authoring.canonical import canonical_json, sha256_digest
from mission_control.domain.authoring.contracts import ExactDefinitionRef, RunInputManifestRef
from mission_control.domain.frames.facts import FrameFact
from mission_control.domain.graph_runtime.identities import UNIT_KEY_PATTERN, QualifiedCheckpointKey

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
MAX_AUTHORITY_BATCH_BYTES = 65_536
MAX_AUTHORITY_BATCH_IDENTITY_SUMMARY_BYTES = 32_768
MAX_LIFECYCLE_COMMAND_BYTES = 65_536


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
                    if isinstance(key, str):
                        normalized = key.lower().replace("-", "_")
                        secret_keys = {
                            "secret",
                            "password",
                            "api_key",
                            "access_token",
                            "refresh_token",
                            "authorization",
                            "cookie",
                            "patient_id",
                            "phi",
                            "raw_content",
                            "raw_output",
                        }
                        if normalized in secret_keys or normalized.endswith(
                            ("_password", "_api_key", "_access_token", "_refresh_token")
                        ):
                            raise ValueError(
                                "run-control records cannot contain raw secrets, PHI, or content"
                            )
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


class EffectDisposition(StrEnum):
    CLAIMED = "claimed"
    PENDING = "pending"
    AMBIGUOUS = "ambiguous"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class EffectSettlementOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AsyncChildDependencyClass(StrEnum):
    REQUIRED_BLOCKING = "required_blocking"
    DEGRADABLE_BLOCKING = "degradable_blocking"
    NONBLOCKING = "nonblocking"
    ADVISORY = "advisory"
    # Source compatibility for the WP-CP-020 authority tests; persisted V1 values are canonical.
    REQUIRED = "required_blocking"
    DEGRADABLE = "degradable_blocking"
    OPTIONAL = "nonblocking"


class AsyncChildDecisionOutcome(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    DEFERRED = "deferred"


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
    # `operator_reconciliation` (AMD-RRM-001, REQ-CP-RUN-007): an `in_doubt` runtime unit
    # waits for the typed `reconcile_unit` command; it is never resolved by re-execution.
    kind: Literal[
        "dependency",
        "timer",
        "approval",
        "resource",
        "budget",
        "external_result",
        "operator_reconciliation",
    ]
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
    expected_run_version: int = Field(ge=1)
    workflow_type_digest: str = Field(pattern=DIGEST_PATTERN)
    obligation_revision: str = Field(min_length=1)
    evidence_frontier_digest: str = Field(pattern=DIGEST_PATTERN)
    accepted_obligation_evidence_digest: str = Field(pattern=DIGEST_PATTERN)
    proposing_execution_binding_ref: str = Field(min_length=1)
    required_obligations_accepted: bool
    execution_failure_refs: tuple[str, ...] = ()
    degradable_failures: tuple[str, ...] = ()
    valid_output_refs: tuple[str, ...] = ()
    cancellation_settled: bool = False
    budget_settled: bool
    effects_settled: bool
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


class ExecutionTarget(Contract):
    """The exact macro execution that applies boundary commands for a run (RRM-007).

    The family declares it in its `start` fact. While a target is bound, an accepted
    pause, resume or wait release is a pending command delivered to that boundary; its
    phase effect is recorded only by the boundary's `applied` fact (REQ-CP-RUN-004). With no
    target bound, run control is itself the boundary (`CON-CP-WORKFLOW-MESSAGE-V1`).
    """

    family: Literal["StageGraph", "GoalDirected"]
    family_workflow_id: str = Field(min_length=1, max_length=512)
    root_workflow_id: str | None = Field(default=None, min_length=1, max_length=512)
    execution_epoch: int = Field(default=1, ge=1)
    execution_generation: int = Field(default=1, ge=1)


class StartAction(Contract):
    kind: Literal["start"] = "start"
    execution_target: ExecutionTarget | None = None


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


def _normal_urgency(value: object) -> bool:
    return value == "normal"


class CancelAction(Contract):
    kind: Literal["cancel"] = "cancel"
    # FT-F3: `immediate` is admitted only after its Stop Fence is persisted. `normal` is
    # left out of dumps and digests, so every cancel recorded before FT-F3 keeps its
    # payload digest.
    urgency: Literal["normal", "immediate"] = Field(default="normal", exclude_if=_normal_urgency)


class ReserveBudgetAction(Contract):
    kind: Literal["reserve_budget"] = "reserve_budget"
    reservation_id: str = Field(min_length=1)
    amounts: dict[str, int]
    parent_reservation_id: str | None = None


class RecordUsageAction(Contract):
    kind: Literal["record_usage"] = "record_usage"
    usage_id: str = Field(min_length=1)
    authority_ref: str | None = Field(default=None, min_length=1)
    actual_amounts: dict[str, int]
    reservation_id: str | None = None
    release_amounts: dict[str, int] = Field(default_factory=dict)
    pending_external_amounts: dict[str, int] = Field(default_factory=dict)


class SettlePendingUsageAction(Contract):
    kind: Literal["settle_pending_usage"] = "settle_pending_usage"
    settlement_id: str = Field(min_length=1)
    usage_id: str = Field(min_length=1)
    actual_amounts: dict[str, int]
    pending_release_amounts: dict[str, int] = Field(default_factory=dict)
    # Attributed usage above the originating pending amounts (REQ-CP-RUN-009: a provider fact
    # is recorded in full). It is consumed on top of the exact pending reconciliation.
    overage_amounts: dict[str, int] = Field(default_factory=dict)


class ClaimEffectAction(Contract):
    kind: Literal["claim_effect"] = "claim_effect"
    effect_id: str = Field(min_length=1)
    effect_kind: str = Field(min_length=1)
    operation_ref: str = Field(min_length=1)
    provider_idempotency_key: str = Field(min_length=1)
    reservation_id: str = Field(min_length=1)
    claim_payload_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)


class ObserveEffectAction(Contract):
    kind: Literal["observe_effect"] = "observe_effect"
    effect_id: str = Field(min_length=1)
    observation_id: str = Field(min_length=1)
    disposition: Literal[
        EffectDisposition.PENDING,
        EffectDisposition.AMBIGUOUS,
        EffectDisposition.SUCCEEDED,
        EffectDisposition.FAILED,
        EffectDisposition.CANCELLED,
    ]
    provider_effect_ref: str | None = Field(default=None, min_length=1)
    evidence_refs: tuple[str, ...] = ()


class SettleEffectAction(Contract):
    kind: Literal["settle_effect"] = "settle_effect"
    effect_id: str = Field(min_length=1)
    settlement_id: str = Field(min_length=1)
    observation_id: str = Field(min_length=1)
    outcome: EffectSettlementOutcome
    usage_settlement_ref: str = Field(min_length=1)
    evidence_refs: tuple[str, ...] = ()


class RegisterAsyncChildAction(Contract):
    kind: Literal["register_async_child"] = "register_async_child"
    child_execution_id: str = Field(min_length=1)
    parent_operation_ref: str = Field(min_length=1)
    dependency_class: AsyncChildDependencyClass
    reservation_id: str = Field(min_length=1)


class RecordAsyncChildFactAction(Contract):
    kind: Literal["record_async_child_fact"] = "record_async_child_fact"
    fact_id: str = Field(min_length=1)
    child_execution_id: str = Field(min_length=1)
    fact_kind: Literal["lifecycle", "result", "cancellation", "settlement"]
    lifecycle_status: str | None = Field(default=None, min_length=1)
    result_manifest_ref: str | None = Field(default=None, min_length=1)
    evidence_refs: tuple[str, ...] = ()


class DecideAsyncChildFactAction(Contract):
    kind: Literal["decide_async_child_fact"] = "decide_async_child_fact"
    decision_id: str = Field(min_length=1)
    fact_id: str = Field(min_length=1)
    outcome: AsyncChildDecisionOutcome
    authority_ref: str = Field(min_length=1)
    reason: str = Field(min_length=1)


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


UnitReconciliationDecisionKind = Literal[
    "accept_descendant",
    "abandon_unit",
    "start_new_generation",
]


def operator_reconciliation_condition_id(
    unit_key: str, execution_generation: int, revision: int = 1
) -> str:
    """The `operator_reconciliation` wait of one incident revision of a unit generation."""

    base = f"operator-reconciliation:{unit_key}:gen:{execution_generation}"
    return base if revision == 1 else f"{base}:revision:{revision}"


class ReconcileUnitAction(Contract):
    """`reconcile_unit` (`CON-CP-LIFECYCLE-V1`, AMD-RRM-001): the privileged, typed operator
    decision for one `in_doubt` runtime-unit generation (REQ-CP-DA-018, REQ-CP-RUN-007)."""

    kind: Literal["reconcile_unit"] = "reconcile_unit"
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    incident_id: str = Field(min_length=1, max_length=256)
    decision: UnitReconciliationDecisionKind
    # `accept_descendant` names the exact stamped root-namespace descendant to adopt.
    accepted_checkpoint: QualifiedCheckpointKey | None = None

    @model_validator(mode="after")
    def accepted_checkpoint_matches_decision(self) -> ReconcileUnitAction:
        if (self.decision == "accept_descendant") != (self.accepted_checkpoint is not None):
            raise ValueError("only accept_descendant names an exact accepted checkpoint")
        if self.accepted_checkpoint is not None and not self.accepted_checkpoint.is_root:
            raise ValueError("an accepted descendant must be a root-namespace checkpoint")
        return self


class UnitReconciliationDecision(Contract):
    """An accepted `reconcile_unit` decision, held by the run projection as authority."""

    decision_id: str = Field(min_length=1)
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    incident_id: str = Field(min_length=1, max_length=256)
    decision: UnitReconciliationDecisionKind
    accepted_checkpoint: QualifiedCheckpointKey | None = None
    actor_id: str = Field(min_length=1)
    decided_at: AwareDatetime


# --- Boundary commands and receipts (RRM-007, CON-CP-WORKFLOW-MESSAGE-V1) -----------------

BoundaryCommandKind = Literal[
    "pause",
    "resume",
    "satisfy_wait",
    "cancel",
    "reconcile_unit",
    "queue_instruction",
    "add_context",
]
BOUNDARY_COMMAND_KINDS: frozenset[str] = frozenset(
    {
        "pause",
        "resume",
        "satisfy_wait",
        "cancel",
        "reconcile_unit",
        "queue_instruction",
        "add_context",
    }
)
# FT-F1 (SPEC-06): the mailbox-bound kinds. They are family-applicable, but the family pulls
# them from the Run's command mailbox at its next boundary (StageGraph admission, GoalDirected
# iteration) instead of receiving them through the root's `execution` transport.
MAILBOX_COMMAND_KINDS: frozenset[str] = frozenset({"queue_instruction", "add_context"})
# The kinds a family boundary applies; `cancel` is applied by the terminal outcome
# (RRM-008 owns its delivery) and `reconcile_unit` by the operation boundary.
FAMILY_BOUNDARY_COMMAND_KINDS: frozenset[str] = frozenset({"pause", "resume", "satisfy_wait"})
BoundaryTargetKind = Literal["run_control", "root", "family", "unit"]
# The root's contiguous message sequence: only commands the transport delivers root-first
# (operator pause/resume/satisfy_wait) take a place in it. Cancels (`cancel`, delivered by
# RRM-008 on its own path) and commands a family issues to itself (`boundary:<family>`)
# have their own spaces, so they can never open a gap at the root (review N1).
EXECUTION_SEQUENCE_SPACE = "execution"
CANCEL_SEQUENCE_SPACE = "cancel"


def self_issued_sequence_space(family_workflow_id: str) -> str:
    return f"boundary:{family_workflow_id}"


def mailbox_sequence_space(generation: int) -> str:
    """FT-F1: queued content is sequenced per Generation, never in the boundary spaces, so a
    queued instruction can never open a gap in the root's `execution` sequence."""

    return f"mailbox:{generation}"


class ReceiptState(StrEnum):
    """The receipt ledger of a Command (workflow-types/09 five-state vocabulary, SPEC-06).

    `accepted -> delivered -> applied` (or `rejected`) is the RRM-007 chain of the boundary
    commands; FT-F1 adds `queued` (a mailbox-bound command waiting for its boundary),
    `observed` (the lane reported the Delivery Report), and the terminal outcomes `expired`
    (superseded by a cancel or a stale Generation) and `failed` (the carrying turn failed).
    `applied`, `rejected`, `expired` and `failed` are `completed` with that outcome.
    """

    ACCEPTED = "accepted"
    QUEUED = "queued"
    DELIVERED = "delivered"
    OBSERVED = "observed"
    APPLIED = "applied"
    REJECTED = "rejected"
    EXPIRED = "expired"
    FAILED = "failed"


BoundaryRejectionReason = Literal[
    "stale_target",
    "stale_generation",
    "stale_version",
    "not_applicable",
    "terminal_run",
    "superseded",
    "unauthorized",
    "insufficient_budget",
]
# A command rejected at acceptance has `rejected` as its only receipt.
RECEIPT_TRANSITIONS: dict[ReceiptState | None, frozenset[ReceiptState]] = {
    None: frozenset({ReceiptState.ACCEPTED, ReceiptState.REJECTED}),
    ReceiptState.ACCEPTED: frozenset(
        {ReceiptState.QUEUED, ReceiptState.DELIVERED, ReceiptState.REJECTED}
    ),
    # FT-F1: a mailbox-bound command waits `queued` until its boundary takes it, or expires.
    ReceiptState.QUEUED: frozenset(
        {ReceiptState.DELIVERED, ReceiptState.REJECTED, ReceiptState.EXPIRED}
    ),
    ReceiptState.DELIVERED: frozenset(
        {
            ReceiptState.OBSERVED,
            ReceiptState.APPLIED,
            ReceiptState.REJECTED,
            ReceiptState.EXPIRED,
            ReceiptState.FAILED,
        }
    ),
    ReceiptState.OBSERVED: frozenset(
        {ReceiptState.APPLIED, ReceiptState.REJECTED, ReceiptState.EXPIRED, ReceiptState.FAILED}
    ),
    ReceiptState.APPLIED: frozenset(),
    ReceiptState.REJECTED: frozenset(),
    ReceiptState.EXPIRED: frozenset(),
    ReceiptState.FAILED: frozenset(),
}
# `completed` in the five-state vocabulary: the receipt states that end a command.
COMPLETED_RECEIPT_STATES: frozenset[ReceiptState] = frozenset(
    {ReceiptState.APPLIED, ReceiptState.REJECTED, ReceiptState.EXPIRED, ReceiptState.FAILED}
)


MailboxBoundary = Literal["next_turn", "next_iteration"]
# `workspace` (FT-F4) is kernel-issued only: a fork's Snapshot restore for the derived Run's
# first packet; public `add_context` payloads cannot name it.
MailboxExpand = Literal["inline", "reference", "materialize", "auto", "workspace"]
# The content ref of a bounded inline text: the text lives in the mailbox entry only, never in
# the command record, which binds it by digest.
MAILBOX_INLINE_REF_PREFIX = "mailbox-inline:"


class MailboxCommandAction(Contract):
    """FT-F1: the shape `queue_instruction` and `add_context` share (SPEC-06 Contracts).

    The command binds its content by `content_digest`: an artifact reference, or a bounded
    inline text whose bytes are stored only in the Run's mailbox entry (`content_ref` is then
    `mailbox-inline:<digest>`). `generation` is the Run Generation the entry targets; a later
    Generation expires it (`stale_generation`), never redirects it.
    """

    boundary: MailboxBoundary = "next_turn"
    generation: int = Field(ge=1)
    content_ref: str = Field(min_length=1, max_length=2048)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    media_type: str = Field(default="text/markdown", min_length=1, max_length=128)
    content_bytes: int = Field(ge=0)
    inline: bool = False
    node_key: str | None = Field(default=None, min_length=1, max_length=256)
    deadline: AwareDatetime | None = None

    @model_validator(mode="after")
    def inline_ref_names_its_digest(self) -> MailboxCommandAction:
        expected = f"{MAILBOX_INLINE_REF_PREFIX}{self.content_digest}"
        if self.inline != (self.content_ref == expected):
            raise ValueError("inline content is referenced exactly as mailbox-inline:<digest>")
        return self


class QueueInstructionAction(MailboxCommandAction):
    """Rendered into the next packet as an `admitted_input` instruction (data, not authority)."""

    kind: Literal["queue_instruction"] = "queue_instruction"


class AddContextAction(MailboxCommandAction):
    """A packet item with its own expansion hint for the Context Packer."""

    kind: Literal["add_context"] = "add_context"
    expand: MailboxExpand = "auto"


DeliveryObservedOutcome = Literal["delivered", "applied", "rejected", "emulated", "unknown"]


class DeliveryNativeRefs(Contract):
    """Native identity of what carried a Command on its lane (never a credential)."""

    lane_profile: str = Field(min_length=1, max_length=64)
    session_ref: str | None = Field(default=None, max_length=512)
    turn_ref: str | None = Field(default=None, max_length=512)
    cancelled_turn_ref: str | None = Field(default=None, max_length=512)
    replacement_turn_ref: str | None = Field(default=None, max_length=512)


class DeliveryReport(Contract):
    """SPEC-06 Delivery Report: the semantics requested at admission (the lane's `describe`)
    and the semantics the lane actually delivered, with its native refs."""

    command_id: str = Field(min_length=1)
    requested_semantics: str | None = Field(default=None, max_length=64)
    delivered_semantics: str = Field(min_length=1, max_length=64)
    emulation_note: str | None = Field(default=None, max_length=512)
    native_refs: DeliveryNativeRefs
    observed_outcome: DeliveryObservedOutcome
    # FT-F2 (cancel_and_replace): effect ids settled or still pending before the replacement.
    settled_effect_ids: tuple[str, ...] = ()
    pending_effect_ids: tuple[str, ...] = ()
    replacement_generation: int | None = Field(default=None, ge=1)
    recorded_at: AwareDatetime


def _no_delivery_report(value: object) -> bool:
    return value is None


class BoundaryTarget(Contract):
    kind: BoundaryTargetKind
    # Root and family workflow identities for an execution target; the unit generation
    # reference (`{unit_key}:gen:{n}`) for an operation boundary; empty for run control.
    target_ref: str = ""
    root_workflow_id: str | None = None
    family_workflow_id: str | None = None
    execution_epoch: int = Field(default=1, ge=1)
    execution_generation: int = Field(default=1, ge=1)
    # Commands routed through one root share one contiguous sequence space.
    sequence_space: str = EXECUTION_SEQUENCE_SPACE


BoundaryCommandAction = Annotated[
    PauseAction
    | ResumeAction
    | SatisfyWaitAction
    | CancelAction
    | ReconcileUnitAction
    | QueueInstructionAction
    | AddContextAction,
    Field(discriminator="kind"),
]


class BoundaryCommandRecord(Contract):
    """One immutable, authorized boundary command as run control accepted or rejected it."""

    schema_version: Literal["belllabs.boundary-command.v1"] = "belllabs.boundary-command.v1"
    command_id: str = Field(min_length=1)
    idempotency_issuer: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    kind: BoundaryCommandKind
    # The exact action as accepted; `payload_digest` is its order-stable fingerprint.
    action: BoundaryCommandAction
    target: BoundaryTarget
    # 0 means the command was rejected before it was sequenced; it never reached a target.
    target_sequence: int = Field(ge=0)
    accepted_run_version: int = Field(ge=1)
    payload_digest: str = Field(pattern=DIGEST_PATTERN)
    actor_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    recorded_at: AwareDatetime


class BoundaryCommandReceipt(Contract):
    """One durable receipt transition of a boundary command, ordered by `ordinal`."""

    schema_version: Literal["belllabs.boundary-receipt.v1"] = "belllabs.boundary-receipt.v1"
    command_id: str = Field(min_length=1)
    # The command identity is (issuer, command_id): two principals may reuse an ID (F5).
    idempotency_issuer: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    ordinal: int = Field(ge=1)
    state: ReceiptState
    recorded_by: str = Field(min_length=1)
    rejection_reason: BoundaryRejectionReason | None = None
    detail: str = Field(default="", max_length=1024)
    # The transport acknowledgement (for example the family's technical segment) or the
    # boundary reference that applied the command.
    transport_ref: str | None = Field(default=None, max_length=512)
    # The run-control version whose transition applied the command (applied receipts).
    applied_run_version: int | None = Field(default=None, ge=1)
    # The boundary state the application bound (for example a durable paused state).
    boundary_state: dict[str, object] = Field(default_factory=dict)
    # FT-F1: the lane's Delivery Report for this transition (left out of dumps while absent,
    # so every receipt recorded before FT-F1 keeps its bytes).
    delivery_report: DeliveryReport | None = Field(default=None, exclude_if=_no_delivery_report)
    recorded_at: AwareDatetime

    @model_validator(mode="after")
    def rejection_reason_matches_state(self) -> BoundaryCommandReceipt:
        if (self.state == ReceiptState.REJECTED) != (self.rejection_reason is not None):
            raise ValueError("exactly a rejected receipt carries a typed rejection reason")
        if self.state != ReceiptState.APPLIED and self.applied_run_version is not None:
            raise ValueError("only an applied receipt binds the applying run version")
        return self


class BoundaryCommandStatus(Contract):
    """Read model: one command with its ordered receipts and current state."""

    command: BoundaryCommandRecord
    receipts: tuple[BoundaryCommandReceipt, ...] = Field(min_length=1)

    @property
    def state(self) -> ReceiptState:
        return self.receipts[-1].state

    @model_validator(mode="after")
    def receipts_follow_the_state_machine(self) -> BoundaryCommandStatus:
        previous: ReceiptState | None = None
        for index, receipt in enumerate(self.receipts, start=1):
            if (
                receipt.ordinal != index
                or receipt.command_id != self.command.command_id
                or receipt.idempotency_issuer != self.command.idempotency_issuer
                or receipt.state not in RECEIPT_TRANSITIONS[previous]
            ):
                raise ValueError("boundary receipts must follow the closed receipt state machine")
            previous = receipt.state
        return self


def next_receipt_state(current: ReceiptState | None, requested: ReceiptState) -> bool:
    """Whether `requested` is a legal next receipt after `current`."""

    return requested in RECEIPT_TRANSITIONS[current]


FamilyBoundaryAction = Annotated[
    PauseAction | ResumeAction | SatisfyWaitAction,
    Field(discriminator="kind"),
]


class ApplyBoundaryCommandAction(Contract):
    """The family boundary's `applied` fact for one accepted, delivered command.

    The reducer applies the nested command exactly as the boundary saw it and records the
    phase effect (REQ-CP-RUN-004). `resume_reservation` is the next unit of work the
    boundary must be able to reserve again (REQ-BP-GD-011): a resume that cannot re-reserve
    is rejected `insufficient_budget` and the run stays paused.
    """

    kind: Literal["apply_boundary_command"] = "apply_boundary_command"
    command_id: str = Field(min_length=1)
    # The accepting principal's idempotency issuer: with `command_id` the exact command.
    command_issuer: str = Field(min_length=1)
    action: FamilyBoundaryAction
    boundary_ref: str = Field(min_length=1, max_length=512)
    runnable_work_remains: bool
    resume_reservation: dict[str, int] = Field(default_factory=dict)
    boundary_state: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def reservation_only_for_resume(self) -> ApplyBoundaryCommandAction:
        if self.resume_reservation and not isinstance(self.action, ResumeAction):
            raise ValueError("only a resume application probes a re-reservation")
        return self


# Family boundary facts whose stale result is never persisted (F2): the boundary binds the
# current version and retries at the new one, so a race can never strand a command.
BOUNDARY_FACT_KINDS: frozenset[str] = frozenset(
    {"apply_boundary_command", "observe_quiescence", "set_wait"}
)


MAX_FRAME_FACTS_PER_ACTION = 128


class FrameFactCursor(Contract):
    """SPEC-03: the last frame arrival ordinal whose facts this run has applied."""

    harness_execution_id: str = Field(min_length=1, max_length=64)
    generation: int = Field(ge=1)
    through_ordinal: int = Field(ge=1)


class ApplyFrameFactsAction(Contract):
    """SPEC-03 (C2): mission state from closing provider frames of one harness execution.

    Facts are derived only from persisted closing frames of the execution's current
    generation, in arrival order; `through_ordinal` is the last frame they cover. The
    reducer records the cursor so a fact is applied exactly once, and writes one mission
    event per fact whose `source.native_event_ref` points at its frame.
    """

    kind: Literal["apply_frame_facts"] = "apply_frame_facts"
    harness_execution_id: str = Field(min_length=1, max_length=64)
    generation: int = Field(ge=1)
    lane_profile: str = Field(min_length=1, max_length=64)
    activation_id: str = Field(min_length=1, max_length=64)
    attempt_no: int = Field(ge=1)
    through_ordinal: int = Field(ge=1)
    facts: tuple[FrameFact, ...] = Field(min_length=1, max_length=MAX_FRAME_FACTS_PER_ACTION)

    @model_validator(mode="after")
    def facts_belong_to_this_execution(self) -> ApplyFrameFactsAction:
        ordinals = [fact.arrival_ordinal for fact in self.facts]
        if ordinals != sorted(ordinals) or ordinals[-1] > self.through_ordinal:
            raise ValueError("frame facts must be in arrival order and covered by the cursor")
        for fact in self.facts:
            if (
                str(fact.harness_execution_id) != self.harness_execution_id
                or fact.generation != self.generation
            ):
                raise ValueError("frame facts belong to exactly one execution generation")
        return self


class ObserveQuiescenceAction(Contract):
    """The family boundary's fact that admissible work ran out (or returned) under the
    currently applied scoped waits and pauses, so the aggregate phase stays accurate."""

    kind: Literal["observe_quiescence"] = "observe_quiescence"
    boundary_ref: str = Field(min_length=1, max_length=512)
    runnable_work_remains: bool


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


class AcceptedOperationSettlementEvidence(Contract):
    settlement_id: str = Field(min_length=1)
    settlement_payload_digest: str = Field(pattern=DIGEST_PATTERN)
    accepted_by_authority_ref: str = Field(min_length=1)


class RecordOperationSettlementEvidenceAction(Contract):
    kind: Literal["record_operation_settlement_evidence"] = "record_operation_settlement_evidence"
    evidence: AcceptedOperationSettlementEvidence


AuthoritySettlementAction = Annotated[
    RecordUsageAction
    | SettlePendingUsageAction
    | ObserveEffectAction
    | SettleEffectAction
    | RecordObligationEvidenceAction
    | RecordOutputEvidenceAction
    | RecordOperationSettlementEvidenceAction,
    Field(discriminator="kind"),
]
AUTHORITY_SETTLEMENT_ACTION_TYPES = (
    RecordUsageAction,
    SettlePendingUsageAction,
    ObserveEffectAction,
    SettleEffectAction,
    RecordObligationEvidenceAction,
    RecordOutputEvidenceAction,
    RecordOperationSettlementEvidenceAction,
)


class ApplyAuthorityBatchAction(Contract):
    kind: Literal["apply_authority_batch"] = "apply_authority_batch"
    actions: tuple[AuthoritySettlementAction, ...] = Field(min_length=1, max_length=64)

    @model_validator(mode="after")
    def actions_are_unique_and_canonically_ordered(self) -> ApplyAuthorityBatchAction:
        if not self.actions or len(self.actions) > 64:
            raise ValueError("authority batch must contain between 1 and 64 actions")
        if any(type(action) not in AUTHORITY_SETTLEMENT_ACTION_TYPES for action in self.actions):
            raise ValueError("authority batch contains a forbidden or non-exact nested action")
        keys = [_authority_settlement_order_key(action) for action in self.actions]
        if len(set(keys)) != len(keys):
            raise ValueError("authority batch action identities must be unique")
        if keys != sorted(keys):
            raise ValueError("authority batch actions must use canonical deterministic ordering")
        effect_settlements = [
            action.effect_id for action in self.actions if isinstance(action, SettleEffectAction)
        ]
        if len(set(effect_settlements)) != len(effect_settlements):
            raise ValueError("authority batch may settle each effect at most once")
        obligation_refs = [
            action.evidence.obligation_ref
            for action in self.actions
            if isinstance(action, RecordObligationEvidenceAction)
        ]
        output_refs = [
            action.evidence.output_ref
            for action in self.actions
            if isinstance(action, RecordOutputEvidenceAction)
        ]
        if len(set(obligation_refs)) != len(obligation_refs):
            raise ValueError("authority batch obligation references must be unique")
        if len(set(output_refs)) != len(output_refs):
            raise ValueError("authority batch output references must be unique")
        settlement_refs = [
            action.evidence.settlement_id
            for action in self.actions
            if isinstance(action, RecordOperationSettlementEvidenceAction)
        ]
        if len(set(settlement_refs)) != len(settlement_refs):
            raise ValueError("authority batch settlement evidence identities must be unique")
        if len(canonical_json(self)) > MAX_AUTHORITY_BATCH_BYTES:
            raise ValueError(
                f"authority batch exceeds {MAX_AUTHORITY_BATCH_BYTES} serialized bytes"
            )
        if (
            len(canonical_json(self.canonical_identity_summary()))
            > MAX_AUTHORITY_BATCH_IDENTITY_SUMMARY_BYTES
        ):
            raise ValueError("authority batch identity summary exceeds its serialized byte limit")
        return self

    def canonical_identity_summary(self) -> tuple[dict[str, str], ...]:
        """Return bounded digests that let consumers match every nested authority identity."""

        return tuple(_authority_settlement_identity(action) for action in self.actions)


def _authority_settlement_order_key(action: AuthoritySettlementAction) -> tuple[int, str, str]:
    if isinstance(action, RecordUsageAction):
        return (0, action.usage_id, "")
    if isinstance(action, SettlePendingUsageAction):
        return (1, action.settlement_id, "")
    if isinstance(action, ObserveEffectAction):
        return (2, action.effect_id, action.observation_id)
    if isinstance(action, SettleEffectAction):
        return (3, action.effect_id, action.settlement_id)
    if isinstance(action, RecordObligationEvidenceAction):
        return (4, action.evidence.obligation_ref, "")
    if isinstance(action, RecordOutputEvidenceAction):
        return (5, action.evidence.output_ref, "")
    return (6, action.evidence.settlement_id, "")


def _authority_settlement_identity(action: AuthoritySettlementAction) -> dict[str, str]:
    if isinstance(action, RecordUsageAction):
        return {
            "action_kind": action.kind,
            "usage_id_digest": sha256_digest(action.usage_id),
        }
    if isinstance(action, SettlePendingUsageAction):
        return {
            "action_kind": action.kind,
            "settlement_id_digest": sha256_digest(action.settlement_id),
        }
    if isinstance(action, ObserveEffectAction):
        return {
            "action_kind": action.kind,
            "effect_id_digest": sha256_digest(action.effect_id),
            "observation_id_digest": sha256_digest(action.observation_id),
        }
    if isinstance(action, SettleEffectAction):
        return {
            "action_kind": action.kind,
            "effect_id_digest": sha256_digest(action.effect_id),
            "observation_id_digest": sha256_digest(action.observation_id),
            "settlement_id_digest": sha256_digest(action.settlement_id),
            "usage_settlement_ref_digest": sha256_digest(action.usage_settlement_ref),
        }
    if isinstance(action, RecordObligationEvidenceAction):
        return {
            "action_kind": action.kind,
            "obligation_ref_digest": sha256_digest(action.evidence.obligation_ref),
        }
    if isinstance(action, RecordOutputEvidenceAction):
        return {
            "action_kind": action.kind,
            "output_ref_digest": sha256_digest(action.evidence.output_ref),
        }
    return {
        "action_kind": action.kind,
        "settlement_id_digest": sha256_digest(action.evidence.settlement_id),
    }


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
    | ClaimEffectAction
    | ObserveEffectAction
    | SettleEffectAction
    | RegisterAsyncChildAction
    | RecordAsyncChildFactAction
    | DecideAsyncChildFactAction
    | ProposeContinuationAction
    | DecideContinuationAction
    | AcceptFinalizationPlanAction
    | RecordFinalizationResultAction
    | RecordObligationEvidenceAction
    | RecordOutputEvidenceAction
    | RecordOperationSettlementEvidenceAction
    | ApplyAuthorityBatchAction
    | TerminalizeAction
    | RecordReadinessAction
    | ReconcileUnitAction
    | ApplyBoundaryCommandAction
    | ObserveQuiescenceAction
    | ApplyFrameFactsAction
    | QueueInstructionAction
    | AddContextAction,
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

    @model_validator(mode="after")
    def serialized_command_is_bounded(self) -> LifecycleCommand:
        if len(canonical_json(self)) > MAX_LIFECYCLE_COMMAND_BYTES:
            raise ValueError(
                f"lifecycle command exceeds {MAX_LIFECYCLE_COMMAND_BYTES} serialized bytes"
            )
        return self


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
    accepted_operation_settlement_evidence: tuple[AcceptedOperationSettlementEvidence, ...] = ()
    evidence_frontier_digest: str = Field(pattern=DIGEST_PATTERN)
    accepted_continuation_proposals: frozenset[str] = Field(default_factory=frozenset)
    pending_continuation_proposals: tuple[ContinuationProposal, ...] = ()
    finalization_plan: FinalizationPlan | None = None
    finalization_output_refs: tuple[str, ...] = ()
    finalization_omission_reason: str | None = None
    async_children: tuple[AsyncChildAuthorityState, ...] = ()
    # AMD-RRM-001: accepted `reconcile_unit` decisions, one per in_doubt unit generation.
    unit_reconciliations: tuple[UnitReconciliationDecision, ...] = ()
    # RRM-007: the macro execution that applies boundary commands, declared at `start`.
    execution_target: ExecutionTarget | None = None
    # SPEC-03 (C2): frame-fact cursors per harness execution generation; omitted from JSON
    # dumps while empty so runs without provider frames keep their persisted form.
    frame_fact_cursors: tuple[FrameFactCursor, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def terminal_axes_are_consistent(self) -> RunProjection:
        if (self.phase == RunPhase.TERMINAL) != (self.terminal_outcome is not None):
            raise ValueError("terminal outcome exists exactly when phase is terminal")
        return self


class UsageRecord(Contract):
    usage_id: str = Field(min_length=1)
    reservation_id: str = Field(min_length=1)
    authority_ref: str | None = Field(default=None, min_length=1)
    actual_amounts: dict[str, int]
    release_amounts: dict[str, int] = Field(default_factory=dict)
    pending_external_amounts: dict[str, int] = Field(default_factory=dict)


class UsageSettlementRecord(Contract):
    settlement_id: str = Field(min_length=1)
    usage_id: str = Field(min_length=1)
    reservation_id: str = Field(min_length=1)
    authority_ref: str | None = Field(default=None, min_length=1)
    settled_amounts: dict[str, int]
    released_amounts: dict[str, int] = Field(default_factory=dict)
    source_pending_amounts: dict[str, int] = Field(default_factory=dict)
    provenance_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def provenance_is_exact(self) -> UsageSettlementRecord:
        expected = sha256_digest(
            {
                "settlement_id": self.settlement_id,
                "usage_id": self.usage_id,
                "reservation_id": self.reservation_id,
                "authority_ref": self.authority_ref,
                "settled_amounts": self.settled_amounts,
                "released_amounts": self.released_amounts,
                "source_pending_amounts": self.source_pending_amounts,
            }
        )
        if self.provenance_digest != expected:
            raise ValueError("usage settlement provenance digest mismatch")
        return self


class BudgetState(Contract):
    schema_version: Literal["1"] = "1"
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
    usage_records: dict[str, UsageRecord] = Field(default_factory=dict)
    usage_settlements: dict[str, UsageSettlementRecord] = Field(default_factory=dict)
    outstanding_usage_ids: frozenset[str] = Field(default_factory=frozenset)
    usage_settlement_effect_refs: dict[str, str] = Field(default_factory=dict)


class BudgetLedgerEntry(Contract):
    schema_version: Literal["1"] = "1"
    entry_id: str
    account_id: str
    run_id: str
    kind: BudgetLedgerKind
    idempotency_id: str
    amounts: dict[str, int]
    occurred_at: AwareDatetime
    parent_account_id: str | None = None


class EffectObservation(Contract):
    schema_version: Literal["1"] = "1"
    observation_id: str = Field(min_length=1)
    disposition: EffectDisposition
    provider_effect_ref: str | None = Field(default=None, min_length=1)
    evidence_refs: tuple[str, ...] = ()
    observed_at: AwareDatetime


class EffectSettlement(Contract):
    schema_version: Literal["1"] = "1"
    settlement_id: str = Field(min_length=1)
    observation_id: str = Field(min_length=1)
    outcome: EffectSettlementOutcome
    usage_settlement_ref: str = Field(min_length=1)
    evidence_refs: tuple[str, ...] = ()
    settled_at: AwareDatetime


class ConsequentialEffectClaim(Contract):
    schema_version: Literal["1"] = "1"
    effect_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    effect_kind: str = Field(min_length=1)
    operation_ref: str = Field(min_length=1)
    provider_idempotency_key: str = Field(min_length=1)
    reservation_id: str = Field(min_length=1)
    disposition: EffectDisposition = EffectDisposition.CLAIMED
    claimed_at: AwareDatetime
    observations: tuple[EffectObservation, ...] = ()
    settlement: EffectSettlement | None = None

    @model_validator(mode="after")
    def settlement_matches_disposition(self) -> ConsequentialEffectClaim:
        terminal = {
            EffectDisposition.SUCCEEDED,
            EffectDisposition.FAILED,
            EffectDisposition.CANCELLED,
        }
        if (self.settlement is not None) != (self.disposition in terminal):
            raise ValueError("only a settled effect has a terminal disposition")
        if self.settlement is not None and self.settlement.outcome.value != self.disposition.value:
            raise ValueError("effect settlement outcome must match its disposition")
        return self


class EffectLedgerState(Contract):
    schema_version: Literal["1"] = "1"
    run_id: str = Field(min_length=1)
    claims: dict[str, ConsequentialEffectClaim] = Field(default_factory=dict)

    @model_validator(mode="after")
    def claim_keys_are_exact(self) -> EffectLedgerState:
        if any(
            key != claim.effect_id or claim.run_id != self.run_id
            for key, claim in self.claims.items()
        ):
            raise ValueError("effect ledger keys and run identities must match claims")
        return self


class EffectLedgerEntry(Contract):
    schema_version: Literal["1"] = "1"
    entry_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    effect_id: str = Field(min_length=1)
    kind: Literal["claim", "observation", "settlement"]
    idempotency_id: str = Field(min_length=1)
    record: ConsequentialEffectClaim | EffectObservation | EffectSettlement
    occurred_at: AwareDatetime


class AsyncChildObservedFact(Contract):
    schema_version: Literal["1"] = "1"
    fact_id: str = Field(min_length=1)
    fact_kind: Literal["lifecycle", "result", "cancellation", "settlement"]
    lifecycle_status: str | None = Field(default=None, min_length=1)
    result_manifest_ref: str | None = Field(default=None, min_length=1)
    evidence_refs: tuple[str, ...] = ()
    observed_at: AwareDatetime


class AsyncChildFactDecision(Contract):
    schema_version: Literal["1"] = "1"
    decision_id: str = Field(min_length=1)
    fact_id: str = Field(min_length=1)
    outcome: AsyncChildDecisionOutcome
    authority_ref: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    decided_at: AwareDatetime


class AsyncChildAuthorityState(Contract):
    schema_version: Literal["1"] = "1"
    child_execution_id: str = Field(min_length=1)
    parent_operation_ref: str = Field(min_length=1)
    dependency_class: AsyncChildDependencyClass
    reservation_id: str = Field(min_length=1)
    facts: tuple[AsyncChildObservedFact, ...] = ()
    decisions: tuple[AsyncChildFactDecision, ...] = ()


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
