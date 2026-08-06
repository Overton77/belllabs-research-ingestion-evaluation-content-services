from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.definitions import ContentAddressedRef
from app.domain.graph_runtime.identities import (
    DIGEST_PATTERN,
    AgentRunKey,
    AgentThreadKey,
    AsyncTaskKey,
    BellLabsRunKey,
    DeploymentIdentity,
    ExecutionEpochKey,
    GoalHandoffCheckpointKey,
    LangGraphCheckpointKey,
    RuntimeTransportAttemptKey,
    SemanticOperationAttemptKey,
)


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RuntimeExecutionStatus(StrEnum):
    SUBMITTING = "submitting"
    ACCEPTED = "accepted"
    RUNNING = "running"
    WAITING = "waiting"
    PAUSED = "paused"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class AttemptDisposition(StrEnum):
    CREATED = "created"
    ACCEPTED = "accepted"
    RUNNING = "running"
    AMBIGUOUS = "ambiguous"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ActorRef(Contract):
    actor_id: str = Field(min_length=1)
    actor_type: Literal["user", "service", "operator", "runtime"]
    authority_ref: str = Field(min_length=1)


class Correlation(Contract):
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = None
    trace_parent_ref: str | None = None


class GraphExecutionSubmission(Contract):
    submission_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    request_digest: str = Field(pattern=DIGEST_PATTERN)
    epoch: ExecutionEpochKey
    expected_belllabs_version: int = Field(ge=1)
    run_plan_ref: ContentAddressedRef
    run_plan_digest: str = Field(pattern=DIGEST_PATTERN)
    graph_assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    input_manifest_ref: str = Field(min_length=1)
    actor: ActorRef
    correlation: Correlation
    submitted_at: AwareDatetime

    @model_validator(mode="after")
    def submission_digest_matches_intent(self) -> GraphExecutionSubmission:
        if self.run_plan_ref.kind.value != "run_plan":
            raise ValueError("graph execution submissions require an exact RunPlan reference")
        content = self.model_dump(mode="json", exclude={"request_digest"})
        if sha256_digest(content) != self.request_digest:
            raise ValueError("graph execution submission request digest mismatch")
        return self


class GraphExecutionReceipt(Contract):
    submission_id: str
    request_digest: str = Field(pattern=DIGEST_PATTERN)
    epoch: ExecutionEpochKey
    status: Literal["accepted", "existing", "reconciliation_required"]
    binding_id: str = Field(min_length=1)
    agent_thread: AgentThreadKey | None = None
    agent_run: AgentRunKey | None = None
    accepted_at: AwareDatetime

    @model_validator(mode="after")
    def accepted_provider_run_requires_thread(self) -> GraphExecutionReceipt:
        if self.agent_run is not None and self.agent_thread is None:
            raise ValueError("an Agent Server run requires its explicitly bound thread")
        return self


class RuntimeExecutionBinding(Contract):
    binding_id: str = Field(min_length=1)
    epoch: ExecutionEpochKey
    submission_id: str = Field(min_length=1)
    submission_idempotency_key: str = Field(min_length=1)
    submission_digest: str = Field(pattern=DIGEST_PATTERN)
    run_plan_digest: str = Field(pattern=DIGEST_PATTERN)
    graph_assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    runtime_provider: Literal["legacy_temporal", "langgraph_agent_server"]
    deployment: DeploymentIdentity | None = None
    agent_thread: AgentThreadKey | None = None
    active: bool = True
    status: RuntimeExecutionStatus = RuntimeExecutionStatus.SUBMITTING
    version: int = Field(default=1, ge=1)
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def provider_facts_match_runtime(self) -> RuntimeExecutionBinding:
        graph = self.runtime_provider == "langgraph_agent_server"
        if graph != (self.deployment is not None and self.agent_thread is not None):
            raise ValueError(
                "LangGraph bindings require qualified deployment and thread identities"
            )
        return self


class RuntimeExecutionAttempt(Contract):
    attempt_key: RuntimeTransportAttemptKey
    binding_id: str = Field(min_length=1)
    disposition: AttemptDisposition
    agent_run: AgentRunKey | None = None
    provider_request_digest: str = Field(pattern=DIGEST_PATTERN)
    provider_metadata: dict[str, Any] = Field(default_factory=dict)
    started_at: AwareDatetime
    heartbeat_at: AwareDatetime | None = None
    lease_expires_at: AwareDatetime | None = None
    finished_at: AwareDatetime | None = None
    failure_code: str | None = None

    @field_validator("provider_metadata")
    @classmethod
    def provider_metadata_has_no_sensitive_values(
        cls, value: dict[str, Any]
    ) -> dict[str, Any]:
        _reject_sensitive_payload(value)
        return value


class RuntimeExecutionProjection(Contract):
    binding: RuntimeExecutionBinding
    attempts: tuple[RuntimeExecutionAttempt, ...]
    latest_checkpoint: LangGraphCheckpointKey | None = None
    pending_interrupt_ids: tuple[str, ...] = ()
    pending_async_task_ids: tuple[str, ...] = ()
    reconciliation_reason: str | None = None


class InterventionBase(Contract):
    command_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    request_digest: str = Field(pattern=DIGEST_PATTERN)
    epoch: ExecutionEpochKey
    expected_belllabs_version: int = Field(ge=1)
    expected_checkpoint: LangGraphCheckpointKey | None = None
    actor: ActorRef
    reason: str = Field(min_length=1, max_length=4_000)
    correlation: Correlation
    requested_at: AwareDatetime

    @model_validator(mode="after")
    def intervention_digest_matches_intent(self) -> InterventionBase:
        content = self.model_dump(mode="json", exclude={"request_digest"})
        if sha256_digest(content) != self.request_digest:
            raise ValueError("runtime intervention request digest mismatch")
        return self


class AppendInputIntervention(InterventionBase):
    kind: Literal["append_input"] = "append_input"
    input_manifest_ref: str = Field(min_length=1)
    input_digest: str = Field(pattern=DIGEST_PATTERN)


class SatisfyWaitIntervention(InterventionBase):
    kind: Literal["satisfy_wait"] = "satisfy_wait"
    wait_condition_id: str = Field(min_length=1)
    satisfaction_ref: str = Field(min_length=1)


class ResumePauseIntervention(InterventionBase):
    kind: Literal["resume_pause"] = "resume_pause"
    pause_decision_id: str = Field(min_length=1)


class RespondToInterruptIntervention(InterventionBase):
    kind: Literal["respond_to_interrupt"] = "respond_to_interrupt"
    interrupt_request_id: str = Field(min_length=1)
    response_schema_ref: ContentAddressedRef
    response_payload_ref: str = Field(min_length=1)
    response_digest: str = Field(pattern=DIGEST_PATTERN)


class UpdateAsyncTaskIntervention(InterventionBase):
    kind: Literal["update_async_task"] = "update_async_task"
    async_task: AsyncTaskKey
    update_kind: Literal["accept_result", "reject_result", "request_status"]
    result_manifest_ref: str | None = None


class CancelAsyncTaskIntervention(InterventionBase):
    kind: Literal["cancel_async_task"] = "cancel_async_task"
    async_task: AsyncTaskKey


class CancelRunIntervention(InterventionBase):
    kind: Literal["cancel_run"] = "cancel_run"
    cancellation_mode: Literal["graceful", "immediate"] = "graceful"


class ForkFromCheckpointIntervention(InterventionBase):
    kind: Literal["fork_from_checkpoint"] = "fork_from_checkpoint"
    source_checkpoint: LangGraphCheckpointKey
    new_belllabs_run: BellLabsRunKey
    fork_input_manifest_ref: str = Field(min_length=1)

    @model_validator(mode="after")
    def fork_stays_in_request_scope(self) -> ForkFromCheckpointIntervention:
        if self.new_belllabs_run.request_scope != self.epoch.request_scope:
            raise ValueError("fork targets cannot cross request scope")
        return self


class PrivilegedOperatorReconcileIntervention(InterventionBase):
    kind: Literal["privileged_operator_reconcile"] = "privileged_operator_reconcile"
    repair_authorization_ref: str = Field(min_length=1)
    reconciliation_action: Literal[
        "bind_observed_run",
        "mark_attempt_ambiguous",
        "admit_observed_checkpoint",
        "close_orphan_task",
    ]
    evidence_refs: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def privileged_repairs_require_operator_actor(
        self,
    ) -> PrivilegedOperatorReconcileIntervention:
        if self.actor.actor_type != "operator":
            raise ValueError("privileged reconciliation requires an operator actor")
        return self


RuntimeIntervention = Annotated[
    AppendInputIntervention
    | SatisfyWaitIntervention
    | ResumePauseIntervention
    | RespondToInterruptIntervention
    | UpdateAsyncTaskIntervention
    | CancelAsyncTaskIntervention
    | CancelRunIntervention
    | ForkFromCheckpointIntervention
    | PrivilegedOperatorReconcileIntervention,
    Field(discriminator="kind"),
]


class InterventionReceipt(Contract):
    command_id: str
    status: Literal["accepted", "existing", "stale", "rejected", "reconciliation_required"]
    binding_id: str = Field(min_length=1)
    resulting_belllabs_version: int = Field(ge=1)
    reason_code: str = Field(min_length=1)
    recorded_at: AwareDatetime


class DurableInterruptEnvelope(Contract):
    interrupt_request_id: str = Field(min_length=1)
    binding_id: str = Field(min_length=1)
    epoch: ExecutionEpochKey
    checkpoint: LangGraphCheckpointKey
    interrupt_kind: str = Field(min_length=1)
    request_schema_ref: ContentAddressedRef
    redacted_request_summary: dict[str, Any]
    request_digest: str = Field(pattern=DIGEST_PATTERN)
    status: Literal["pending", "answered", "expired", "cancelled"]
    requested_at: AwareDatetime
    expires_at: AwareDatetime | None = None

    @field_validator("redacted_request_summary")
    @classmethod
    def interrupt_summary_has_no_sensitive_values(
        cls, value: dict[str, Any]
    ) -> dict[str, Any]:
        _reject_sensitive_payload(value)
        return value


class DurableInterruptResponse(Contract):
    interrupt_request_id: str
    response_id: str = Field(min_length=1)
    response_schema_ref: ContentAddressedRef
    response_payload_ref: str = Field(min_length=1)
    response_digest: str = Field(pattern=DIGEST_PATTERN)
    actor: ActorRef
    decided_at: AwareDatetime


class RuntimeAsyncTaskProjection(Contract):
    task: AsyncTaskKey
    binding_id: str = Field(min_length=1)
    parent_epoch: ExecutionEpochKey
    status: Literal[
        "submitted",
        "running",
        "waiting",
        "completed",
        "failed",
        "cancel_requested",
        "cancelled",
        "orphaned",
        "reconciliation_required",
    ]
    request_digest: str = Field(pattern=DIGEST_PATTERN)
    result_manifest_ref: str | None = None
    lease_expires_at: AwareDatetime | None = None
    heartbeat_at: AwareDatetime | None = None
    version: int = Field(ge=1)
    created_at: AwareDatetime
    updated_at: AwareDatetime


class BellLabsStreamEvent(Contract):
    schema_version: Literal["belllabs.stream.v2"] = "belllabs.stream.v2"
    event_id: str = Field(min_length=1)
    outbox_position: int = Field(ge=1)
    event_type: str = Field(min_length=1)
    epoch: ExecutionEpochKey
    binding_id: str = Field(min_length=1)
    aggregate_version: int = Field(ge=1)
    sequence: int = Field(ge=1)
    payload_ref: str | None = None
    redacted_payload: dict[str, Any] = Field(default_factory=dict)
    occurred_at: AwareDatetime

    @field_validator("redacted_payload")
    @classmethod
    def stream_payload_has_no_sensitive_values(
        cls, value: dict[str, Any]
    ) -> dict[str, Any]:
        _reject_sensitive_payload(value)
        return value


class ForkRequest(Contract):
    request_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    source_epoch: ExecutionEpochKey
    source_checkpoint: LangGraphCheckpointKey
    target_run: BellLabsRunKey
    run_plan_digest: str = Field(pattern=DIGEST_PATTERN)
    actor: ActorRef
    reason: str = Field(min_length=1)
    requested_at: AwareDatetime


class ForkReceipt(Contract):
    request_id: str
    source_epoch: ExecutionEpochKey
    target_epoch: ExecutionEpochKey
    target_thread: AgentThreadKey
    status: Literal["accepted", "existing"]
    recorded_at: AwareDatetime


class RedactedCheckpointSummary(Contract):
    checkpoint: LangGraphCheckpointKey
    binding_id: str = Field(min_length=1)
    epoch: ExecutionEpochKey
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    graph_assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    status: str = Field(min_length=1)
    pending_interrupt_ids: tuple[str, ...] = ()
    output_refs: tuple[str, ...] = ()
    summary_digest: str = Field(pattern=DIGEST_PATTERN)
    observed_at: AwareDatetime


class RuntimeCapabilityReadiness(Contract):
    capability_id: str = Field(min_length=1)
    maturity: Literal["stable", "beta", "preview", "entitlement_dependent", "policy_disabled"]
    enabled: bool
    ready: bool
    reason: str = Field(min_length=1)
    fallback: str = Field(min_length=1)


class GraphRuntimeHealth(Contract):
    runtime_provider: Literal["legacy_temporal", "langgraph_agent_server"]
    deployment: DeploymentIdentity | None = None
    status: Literal["ready", "degraded", "unavailable"]
    capabilities: tuple[RuntimeCapabilityReadiness, ...]
    observed_at: AwareDatetime


class ProviderNeutralAttemptMetadata(Contract):
    attempt_key: RuntimeTransportAttemptKey
    semantic_attempt: SemanticOperationAttemptKey | None = None
    provider: str = Field(min_length=1)
    provider_attempt_id: str | None = None
    idempotency_supported: bool
    consequential: bool
    retry_class: Literal["safe", "claim_then_reconcile", "non_retryable"]
    usage: dict[str, int] = Field(default_factory=dict)

    @model_validator(mode="after")
    def consequential_retry_is_safe(self) -> ProviderNeutralAttemptMetadata:
        if self.consequential and not self.idempotency_supported and self.retry_class == "safe":
            raise ValueError("consequential non-idempotent providers cannot be blindly retried")
        return self


class SubagentContextSlice(Contract):
    context_assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    source_entry_ids: tuple[str, ...]
    protected_atoms_digest: str = Field(pattern=DIGEST_PATTERN)
    maximum_bytes: int = Field(ge=0)
    slice_digest: str = Field(pattern=DIGEST_PATTERN)


class SubagentResultManifest(Contract):
    result_id: str = Field(min_length=1)
    async_task: AsyncTaskKey | None = None
    output_refs: tuple[str, ...]
    evidence_refs: tuple[str, ...] = ()
    usage: dict[str, int] = Field(default_factory=dict)
    context_slice_digest: str = Field(pattern=DIGEST_PATTERN)
    result_digest: str = Field(pattern=DIGEST_PATTERN)


class ContextReconstructionResult(Contract):
    context_assembly_ref: ContentAddressedRef
    reconstructed_entry_ids: tuple[str, ...]
    missing_entry_ids: tuple[str, ...] = ()
    tombstoned_entry_ids: tuple[str, ...] = ()
    contradiction_groups: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    reconstruction_digest: str = Field(pattern=DIGEST_PATTERN)
    complete: bool


class GoalHandoffReference(Contract):
    checkpoint: GoalHandoffCheckpointKey
    artifact_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)


class BellLabsSuccessEnvelope[T](Contract):
    schema_version: Literal["belllabs.api.v2"] = "belllabs.api.v2"
    request_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    data: T


class BellLabsErrorDetail(Contract):
    code: str = Field(min_length=1)
    message: str = Field(min_length=1)
    retryable: bool = False
    field: str | None = None


class BellLabsErrorEnvelope(Contract):
    schema_version: Literal["belllabs.api.v2"] = "belllabs.api.v2"
    request_id: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    error: BellLabsErrorDetail


def _reject_sensitive_payload(value: object) -> None:
    sensitive = ("secret", "token", "password", "credential", "api_key", "apikey", "phi")

    def inspect(item: object) -> None:
        if isinstance(item, dict):
            for key, nested in item.items():
                normalized = str(key).lower().replace("-", "_")
                reference_only = normalized.endswith(("_ref", "_refs", "_digest", "_id"))
                if any(fragment in normalized for fragment in sensitive) and not reference_only:
                    raise ValueError("runtime payloads may contain sensitive references only")
                inspect(nested)
        elif isinstance(item, list | tuple):
            for nested in item:
                inspect(nested)

    inspect(value)
