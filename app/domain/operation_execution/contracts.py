from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import ExactDefinitionRef, SecretRef

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PromptTrustClass(StrEnum):
    SYSTEM_AUTHORITY = "system_authority"
    AUTHORED_INSTRUCTION = "authored_instruction"
    ADMITTED_INPUT = "admitted_input"
    UNTRUSTED_CONTENT = "untrusted_content"


class OperationAttemptIdentity(Contract):
    run_id: str = Field(min_length=1)
    operation_id: str = Field(min_length=1)
    operation_attempt: int = Field(ge=1)

    @property
    def semantic_key(self) -> str:
        return f"{self.run_id}:operation:{self.operation_id}:attempt:{self.operation_attempt}"


class PromptSegment(Contract):
    source_ref: str = Field(min_length=1)
    source_revision: int = Field(ge=1)
    trust_class: PromptTrustClass
    content: str = Field(max_length=100_000)
    rendered_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def rendered_content_matches_digest(self) -> PromptSegment:
        if sha256_digest(self.content) != self.rendered_digest:
            raise ValueError("prompt segment content does not match its rendered digest")
        return self


class ModelPolicy(Contract):
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    reasoning_effort: Literal["minimal", "low", "medium", "high"] | None = None
    verbosity: Literal["low", "medium", "high"] | None = None
    max_turns: int = Field(default=3, ge=1, le=50)
    fallback_models: tuple[str, ...] = ()


class ToolBinding(Contract):
    tool_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    schema_digest: str = Field(pattern=DIGEST_PATTERN)
    approval_policy: Literal["never", "always", "policy"] = "policy"
    configuration: dict[str, object] = Field(default_factory=dict)


class GuardrailBinding(Contract):
    guardrail_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    implementation_digest: str = Field(pattern=DIGEST_PATTERN)
    stage: Literal["input", "output"]


class StructuredOutputBinding(Contract):
    schema_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    schema_digest: str = Field(pattern=DIGEST_PATTERN)
    strict: bool = True


class MCPServerBinding(Contract):
    server_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    transport: Literal["stdio", "sse", "streamable_http"]
    endpoint_ref: str = Field(min_length=1)
    allowed_tools: frozenset[str]
    schema_digest: str = Field(pattern=DIGEST_PATTERN)
    timeout_seconds: int = Field(default=30, ge=1)
    max_retries: int = Field(default=2, ge=0)
    approval_policy: Literal["never", "always", "policy"] = "policy"


class ImmutableAssetBinding(Contract):
    ref: ExactDefinitionRef
    manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    mount_path: str = Field(min_length=1)


class WorkspaceMount(Contract):
    logical_path: str = Field(min_length=1)
    durable_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    read_only: Literal[True] = True


class WorkspaceContract(Contract):
    namespace_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    template_ref: ExactDefinitionRef
    workflow_contract_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    slot_bindings: tuple[WorkspaceSlotBinding, ...] = ()
    exclusive_write_paths: tuple[str, ...]
    read_mounts: tuple[WorkspaceMount, ...] = ()
    network_policy: Literal["none", "allowlisted"] = "none"
    runtime_digest: str = Field(pattern=DIGEST_PATTERN)
    image_digest: str = Field(pattern=DIGEST_PATTERN)
    package_digest: str = Field(pattern=DIGEST_PATTERN)
    environment_digest: str = Field(pattern=DIGEST_PATTERN)
    restore_snapshot_id: str | None = None

    @field_validator("exclusive_write_paths")
    @classmethod
    def unique_writable_paths(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if not value or len(value) != len(set(value)):
            raise ValueError("workspace requires unique exclusive writable paths")
        return value

    @model_validator(mode="after")
    def compiled_slots_match_legacy_projection(self) -> WorkspaceContract:
        if not self.slot_bindings:
            return self
        paths = [slot.logical_path for slot in self.slot_bindings]
        if any(
            _workspace_paths_overlap(left, right)
            for index, left in enumerate(paths)
            for right in paths[index + 1 :]
        ):
            raise ValueError("compiled workspace slot paths cannot overlap")
        writable = tuple(
            slot.logical_path for slot in self.slot_bindings if slot.access == "exclusive_write"
        )
        if set(writable) != set(self.exclusive_write_paths):
            raise ValueError("compiled workspace slots do not match writable path projection")
        if self.workflow_contract_digest is None:
            raise ValueError("compiled workspace slots require the workflow contract digest")
        return self


class CapabilityGrant(Contract):
    capabilities: frozenset[str]
    tool_ids: frozenset[str] = Field(default_factory=frozenset)
    mcp_server_ids: frozenset[str] = Field(default_factory=frozenset)
    data_scope_refs: frozenset[str] = Field(default_factory=frozenset)
    network_hosts: frozenset[str] = Field(default_factory=frozenset)


class AgentDefinition(Contract):
    definition_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    name: str = Field(min_length=1, max_length=128)
    description: str = Field(min_length=1, max_length=2_000)
    instructions: str = Field(min_length=1, max_length=100_000)
    model_policy: ModelPolicy
    tools: tuple[ToolBinding, ...] = ()
    mcp_servers: tuple[MCPServerBinding, ...] = ()
    skills: tuple[ImmutableAssetBinding, ...] = ()
    plugins: tuple[ImmutableAssetBinding, ...] = ()
    capability_grant: CapabilityGrant
    output_schema: StructuredOutputBinding | None = None
    guardrails: tuple[GuardrailBinding, ...] = ()
    requested_workflow_type_ref: ExactDefinitionRef | None = None

    @model_validator(mode="after")
    def capabilities_cover_agent_bindings(self) -> AgentDefinition:
        if not {tool.tool_id for tool in self.tools} <= self.capability_grant.tool_ids:
            raise ValueError("agent tool binding exceeds its capability grant")
        if (
            not {server.server_id for server in self.mcp_servers}
            <= self.capability_grant.mcp_server_ids
        ):
            raise ValueError("agent MCP binding exceeds its capability grant")
        return self


class DelegationBinding(Contract):
    mode: Literal["handoff", "task_subagent"]
    agent: AgentDefinition
    tool_name: str | None = Field(default=None, pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    tool_description: str | None = Field(default=None, max_length=2_000)
    needs_approval: bool = False
    budget_limits: dict[str, int] = Field(default_factory=dict)
    child_workspace_id: str = Field(min_length=1)
    child_namespace_id: str = Field(min_length=1)
    read_mounts: tuple[WorkspaceMount, ...] = ()

    @model_validator(mode="after")
    def task_subagent_has_tool_identity(self) -> DelegationBinding:
        if self.mode == "task_subagent" and (
            self.tool_name is None or self.tool_description is None
        ):
            raise ValueError("task subagents require an exact tool name and description")
        if self.mode == "handoff" and (
            self.tool_name is not None or self.tool_description is not None
        ):
            raise ValueError("handoffs cannot declare an agent-tool identity")
        return self


class DelegationCeiling(Contract):
    allowed_modes: frozenset[Literal["handoff", "task_subagent"]] = frozenset()
    max_depth: int = Field(default=0, ge=0, le=10)
    max_concurrency: int = Field(default=0, ge=0, le=100)
    max_delegations: int = Field(default=0, ge=0, le=1_000)
    allowed_models: frozenset[str] = frozenset()
    tool_ids: frozenset[str] = frozenset()
    mcp_server_ids: frozenset[str] = frozenset()
    data_scope_refs: frozenset[str] = frozenset()
    network_hosts: frozenset[str] = frozenset()
    budget_limits: dict[str, int] = Field(default_factory=dict)


class UnsupportedPolicy(Contract):
    policy: str = Field(min_length=1)
    required: bool = True
    authored_degradation: str | None = None

    @model_validator(mode="after")
    def required_policy_cannot_silently_degrade(self) -> UnsupportedPolicy:
        if self.required and self.authored_degradation is not None:
            raise ValueError("required policies cannot declare degradation")
        return self


class OperationExecutionRequest(Contract):
    identity: OperationAttemptIdentity
    request_scope: str = Field(min_length=1)
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    run_control_revision: int = Field(ge=1)
    operation_contract_ref: str = Field(min_length=1)
    prompt_segments: tuple[PromptSegment, ...] = Field(min_length=1)
    model_policy: ModelPolicy
    tools: tuple[ToolBinding, ...] = ()
    mcp_servers: tuple[MCPServerBinding, ...] = ()
    skills: tuple[ImmutableAssetBinding, ...] = ()
    plugins: tuple[ImmutableAssetBinding, ...] = ()
    output_schema: StructuredOutputBinding | None = None
    guardrails: tuple[GuardrailBinding, ...] = ()
    delegations: tuple[DelegationBinding, ...] = ()
    delegation_ceiling: DelegationCeiling = Field(default_factory=DelegationCeiling)
    session_id: str | None = Field(default=None, min_length=1, max_length=256)
    agent_profile_ref: ExactDefinitionRef
    capability_grant: CapabilityGrant
    workspace: WorkspaceContract
    secret_refs: tuple[SecretRef, ...] = ()
    unsupported_policies: tuple[UnsupportedPolicy, ...] = ()
    budget_reservation_id: str = Field(min_length=1)
    budget_limits: dict[str, int]
    tracing_policy_ref: str = Field(min_length=1)
    sensitive_data_policy_ref: str = Field(min_length=1)
    snapshot_policy_ref: str = Field(min_length=1)
    prior_binding_id: str | None = None
    requested_at: AwareDatetime
    idempotency_key: str = Field(min_length=1)

    @model_validator(mode="after")
    def capabilities_cover_exact_bindings(self) -> OperationExecutionRequest:
        if not {tool.tool_id for tool in self.tools} <= self.capability_grant.tool_ids:
            raise ValueError("tool binding exceeds the operation capability grant")
        if (
            not {server.server_id for server in self.mcp_servers}
            <= self.capability_grant.mcp_server_ids
        ):
            raise ValueError("MCP binding exceeds the operation capability grant")
        ceiling = self.delegation_ceiling
        if len(self.delegations) > ceiling.max_delegations:
            raise ValueError("delegations exceed the operation delegation count ceiling")
        if self.delegations and ceiling.max_depth < 1:
            raise ValueError("delegations exceed the operation delegation depth ceiling")
        if self.delegations and ceiling.max_concurrency < 1:
            raise ValueError("delegations exceed the operation concurrency ceiling")
        for delegation in self.delegations:
            agent = delegation.agent
            if delegation.mode not in ceiling.allowed_modes:
                raise ValueError("delegation mode exceeds the operation delegation ceiling")
            if agent.model_policy.model not in ceiling.allowed_models:
                raise ValueError("delegate model exceeds the operation delegation ceiling")
            if not agent.capability_grant.capabilities <= self.capability_grant.capabilities:
                raise ValueError("delegate capabilities exceed operation authority")
            if not agent.capability_grant.tool_ids <= (
                self.capability_grant.tool_ids & ceiling.tool_ids
            ):
                raise ValueError("delegate tools exceed intersected delegation authority")
            if not agent.capability_grant.mcp_server_ids <= (
                self.capability_grant.mcp_server_ids & ceiling.mcp_server_ids
            ):
                raise ValueError("delegate MCP servers exceed intersected delegation authority")
            if not agent.capability_grant.data_scope_refs <= (
                self.capability_grant.data_scope_refs & ceiling.data_scope_refs
            ):
                raise ValueError("delegate data scope exceeds intersected delegation authority")
            if not agent.capability_grant.network_hosts <= (
                self.capability_grant.network_hosts & ceiling.network_hosts
            ):
                raise ValueError("delegate network access exceeds intersected delegation authority")
            if any(
                amount > self.budget_limits.get(dimension, 0)
                or amount > ceiling.budget_limits.get(dimension, 0)
                for dimension, amount in delegation.budget_limits.items()
            ):
                raise ValueError("delegate budget exceeds intersected delegation authority")
        return self


class OperationExecutionBinding(Contract):
    binding_id: str = Field(min_length=1)
    semantic_attempt_key: str = Field(min_length=1)
    request_fingerprint: str = Field(pattern=DIGEST_PATTERN)
    request_scope: str
    run_id: str
    operation_id: str
    operation_attempt: int = Field(ge=1)
    prior_binding_id: str | None = None
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    run_control_revision: int = Field(ge=1)
    operation_contract_ref: str
    prompt_sources: tuple[tuple[str, int, PromptTrustClass, str], ...]
    model_policy: ModelPolicy
    tools: tuple[ToolBinding, ...]
    mcp_servers: tuple[MCPServerBinding, ...]
    skills: tuple[ImmutableAssetBinding, ...]
    plugins: tuple[ImmutableAssetBinding, ...]
    output_schema: StructuredOutputBinding | None = None
    guardrails: tuple[GuardrailBinding, ...] = ()
    delegations: tuple[DelegationBinding, ...] = ()
    delegation_ceiling: DelegationCeiling = Field(default_factory=DelegationCeiling)
    session_id: str | None = None
    agent_profile_ref: ExactDefinitionRef
    capability_grant: CapabilityGrant
    workspace: WorkspaceContract
    secret_refs: tuple[SecretRef, ...]
    budget_reservation_id: str
    budget_limits: dict[str, int]
    tracing_policy_ref: str
    sensitive_data_policy_ref: str
    snapshot_policy_ref: str
    applied_degradations: tuple[str, ...] = ()
    side_effect_key: str
    bound_at: AwareDatetime


class MaterializedWorkspace(Contract):
    workspace_id: str
    provider: str
    runtime_digest: str = Field(pattern=DIGEST_PATTERN)
    image_digest: str = Field(pattern=DIGEST_PATTERN)
    mount_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    namespace_id: str | None = None
    manifest_revision: int | None = Field(default=None, ge=1)
    materialization_manifest: WorkspaceMaterializationManifest | None = None


class RuntimeInvocation(Contract):
    binding: OperationExecutionBinding
    prompt_segments: tuple[PromptSegment, ...]
    workspace: MaterializedWorkspace
    resolved_secret_names: tuple[str, ...] = ()


class RuntimeUsage(Contract):
    amounts: dict[str, int] = Field(default_factory=dict)
    pending_external_amounts: dict[str, int] = Field(default_factory=dict)


class RuntimeResult(Contract):
    output_text: str
    structured_output: dict[str, object] | None = None
    output_refs: tuple[str, ...] = ()
    usage: RuntimeUsage = Field(default_factory=RuntimeUsage)
    provider_run_id: str | None = None
    event_payloads: tuple[dict[str, object], ...] = ()


class RuntimeEventEnvelope(Contract):
    schema_version: Literal["1"] = "1"
    event_id: str = Field(min_length=1)
    event_type: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    binding_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    operation_id: str = Field(min_length=1)
    sequence: int = Field(ge=1)
    occurred_at: AwareDatetime
    durable: bool = True
    payload: dict[str, object] = Field(default_factory=dict)


class RuntimeApprovalRequest(Contract):
    approval_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    binding_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    operation_id: str = Field(min_length=1)
    tool_name: str = Field(min_length=1)
    arguments_digest: str = Field(pattern=DIGEST_PATTERN)
    argument_names: tuple[str, ...] = ()
    requested_at: AwareDatetime
    expires_at: AwareDatetime


class RuntimeApprovalDecision(Contract):
    approval_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    binding_id: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)
    decision: Literal["approved", "rejected"]
    reason: str | None = Field(default=None, max_length=2_000)
    decided_at: AwareDatetime


class OperationSettlement(Contract):
    settlement_id: str
    binding_id: str
    status: Literal["completed", "failed", "cancelled", "timed_out"]
    output_text: str = ""
    structured_output: dict[str, object] | None = None
    output_refs: tuple[str, ...] = ()
    usage: RuntimeUsage = Field(default_factory=RuntimeUsage)
    provider_run_id: str | None = None
    event_payloads: tuple[dict[str, object], ...] = ()
    failure_code: str | None = None
    failure_message: str | None = None
    settled_at: AwareDatetime


class OperationExecutionResult(Contract):
    binding_id: str
    semantic_attempt_key: str
    status: Literal["completed", "failed", "cancelled", "timed_out", "in_doubt"]
    output_text: str = ""
    structured_output: dict[str, object] | None = None
    output_refs: tuple[str, ...] = ()
    usage: RuntimeUsage = Field(default_factory=RuntimeUsage)
    failure_code: str | None = None
    failure_message: str | None = None


class ArtifactCheckEvidence(Contract):
    check_id: str = Field(min_length=1)
    required: bool = True
    outcome: Literal["passed", "failed", "not_applicable"]
    evidence_ref: str = Field(min_length=1)


class ArtifactPromotionDeclaration(Contract):
    namespace_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    output_slot: str = Field(min_length=1)
    logical_path: str = Field(min_length=1)
    owner: WorkspaceOwner
    candidate_id: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    media_type: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    permission_ref: str = Field(min_length=1)
    permission_outcome: Literal[
        "allowed", "allowed_with_conditions", "requires_review", "unknown", "prohibited"
    ]
    output_contract_ref: str = Field(min_length=1)
    checks: tuple[ArtifactCheckEvidence, ...] = ()
    requested_at: AwareDatetime

    @field_validator("checks")
    @classmethod
    def check_identities_are_unique(
        cls, value: tuple[ArtifactCheckEvidence, ...]
    ) -> tuple[ArtifactCheckEvidence, ...]:
        identities = [item.check_id for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("artifact check identities must be unique")
        return value


class ArtifactPromotionRequest(ArtifactPromotionDeclaration):
    request_scope: str = Field(min_length=1)
    binding_id: str = Field(min_length=1)
    requested_at: AwareDatetime


class ArtifactPromotionState(StrEnum):
    CANDIDATE = "candidate"
    PAYLOAD_STAGED = "payload_staged"
    METADATA_COMMITTED = "metadata_committed"
    ADMITTED = "admitted"
    REJECTED = "rejected"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class ArtifactMetadataRevision(Contract):
    promotion_id: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    intent_key: str = Field(min_length=1)
    promotion_identity: str = Field(pattern=DIGEST_PATTERN)
    revision: int = Field(ge=1)
    state: ArtifactPromotionState
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    semantic_attempt_key: str = Field(min_length=1)
    producer_binding_id: str = Field(min_length=1)
    namespace_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    output_slot: str = Field(min_length=1)
    logical_path: str = Field(min_length=1)
    owner: WorkspaceOwner
    candidate_id: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    media_type: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    permission_ref: str = Field(min_length=1)
    permission_outcome: Literal[
        "allowed", "allowed_with_conditions", "requires_review", "unknown", "prohibited"
    ]
    output_contract_ref: str = Field(min_length=1)
    checks: tuple[ArtifactCheckEvidence, ...] = ()
    object_ref: str | None = None
    manifest_revision: int | None = Field(default=None, ge=1)
    durable_reference: str | None = None
    reason: str | None = None
    recorded_at: AwareDatetime


class PromotedArtifact(Contract):
    artifact_id: str
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    object_ref: str
    metadata_revision: int = Field(ge=1)
    manifest_revision: int = Field(ge=1)
    durable_reference: str = Field(min_length=1)
    status: Literal["admitted"]


class ArtifactPromotionPlan(Contract):
    namespace_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    output_slot: str = Field(min_length=1)
    logical_path: str = Field(min_length=1)
    owner: WorkspaceOwner
    permission_ref: str = Field(min_length=1)
    permission_outcome: Literal[
        "allowed", "allowed_with_conditions", "requires_review", "unknown", "prohibited"
    ]
    output_contract_ref: str = Field(min_length=1)
    checks: tuple[ArtifactCheckEvidence, ...] = ()


class CapturedWorkspaceCandidate(Contract):
    namespace_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    output_slot: str = Field(min_length=1)
    logical_path: str = Field(min_length=1)
    owner: WorkspaceOwner
    candidate_id: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    media_type: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)


class GenericArtifactWorkflowRequest(Contract):
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    operation: OperationExecutionRequest
    promotion: ArtifactPromotionPlan

    @model_validator(mode="after")
    def operation_and_promotion_share_run_workspace(
        self,
    ) -> GenericArtifactWorkflowRequest:
        if (
            self.operation.request_scope != self.request_scope
            or self.operation.identity.run_id != self.run_id
            or self.operation.workspace.namespace_id != self.promotion.namespace_id
            or self.operation.workspace.workspace_id != self.promotion.workspace_id
        ):
            raise ValueError("generic artifact workflow inputs do not share one run workspace")
        return self


class GenericArtifactWorkflowResult(Contract):
    workflow_id: str = Field(min_length=1)
    operation: OperationExecutionResult
    artifact: PromotedArtifact


class WorkspaceOwnerKind(StrEnum):
    RUN = "run"
    STAGE = "stage"
    CYCLE = "cycle"
    ITERATION = "iteration"
    EVALUATOR = "evaluator"
    AGENT = "agent"
    DELEGATE = "delegate"


class WorkspaceOwner(Contract):
    kind: WorkspaceOwnerKind
    owner_id: str = Field(min_length=1)
    parent_owner_id: str | None = None

    @model_validator(mode="after")
    def delegates_require_parent(self) -> WorkspaceOwner:
        if self.kind == WorkspaceOwnerKind.DELEGATE and self.parent_owner_id is None:
            raise ValueError("delegate workspace owners require an explicit parent")
        return self


class WorkspaceSlotBinding(Contract):
    slot_name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    logical_path: str = Field(pattern=r"^/[A-Za-z0-9._/-]+$")
    access: Literal["read_only", "exclusive_write"]
    owner: WorkspaceOwner
    durable_ref: str | None = None
    content_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def mounted_inputs_are_complete(self) -> WorkspaceSlotBinding:
        if self.access == "read_only":
            if self.durable_ref is None or self.content_digest is None:
                raise ValueError("read-only slots require a durable reference and digest")
        elif self.durable_ref is not None or self.content_digest is not None:
            raise ValueError("writable slots cannot declare a durable input")
        if ".." in self.logical_path.split("/"):
            raise ValueError("logical workspace paths cannot traverse parents")
        return self


class DurableInputManifestEntry(Contract):
    kind: Literal["durable_input"] = "durable_input"
    entry_id: str = Field(min_length=1)
    slot_name: str = Field(min_length=1)
    logical_path: str = Field(min_length=1)
    owner: WorkspaceOwner
    durable_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    read_only: Literal[True] = True


class LocalCandidateManifestEntry(Contract):
    kind: Literal["local_candidate"] = "local_candidate"
    entry_id: str = Field(min_length=1)
    slot_name: str = Field(min_length=1)
    logical_path: str = Field(min_length=1)
    owner: WorkspaceOwner
    candidate_id: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    media_type: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)


class PromotedArtifactManifestEntry(Contract):
    kind: Literal["promoted_artifact"] = "promoted_artifact"
    entry_id: str = Field(min_length=1)
    slot_name: str = Field(min_length=1)
    logical_path: str = Field(min_length=1)
    owner: WorkspaceOwner
    candidate_id: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    artifact_metadata_revision: int = Field(ge=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)


class StaleManifestEntry(Contract):
    kind: Literal["stale"] = "stale"
    entry_id: str = Field(min_length=1)
    slot_name: str = Field(min_length=1)
    logical_path: str = Field(min_length=1)
    owner: WorkspaceOwner
    superseded_entry_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)


WorkspaceMaterializationEntry = Annotated[
    DurableInputManifestEntry
    | LocalCandidateManifestEntry
    | PromotedArtifactManifestEntry
    | StaleManifestEntry,
    Field(discriminator="kind"),
]


class WorkspaceMaterializationManifest(Contract):
    manifest_id: str = Field(min_length=1)
    namespace_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    template_ref: ExactDefinitionRef
    workflow_contract_digest: str = Field(pattern=DIGEST_PATTERN)
    slots: tuple[WorkspaceSlotBinding, ...] = Field(min_length=1)
    entries: tuple[WorkspaceMaterializationEntry, ...]
    prior_manifest_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    created_at: AwareDatetime

    @model_validator(mode="after")
    def governed_paths_are_unique(self) -> WorkspaceMaterializationManifest:
        paths = [entry.logical_path for entry in self.entries if entry.kind != "stale"]
        if len(paths) != len(set(paths)):
            raise ValueError("current manifest entries require unique logical paths")
        return self


class WorkspaceMaterializationRequest(Contract):
    namespace_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    template_ref: ExactDefinitionRef
    workflow_contract_digest: str = Field(pattern=DIGEST_PATTERN)
    slots: tuple[WorkspaceSlotBinding, ...] = Field(min_length=1)
    runtime_digest: str = Field(pattern=DIGEST_PATTERN)
    image_digest: str = Field(pattern=DIGEST_PATTERN)
    created_at: AwareDatetime

    @model_validator(mode="after")
    def slots_are_unique_and_exclusive(self) -> WorkspaceMaterializationRequest:
        names = [slot.slot_name for slot in self.slots]
        paths = [slot.logical_path for slot in self.slots]
        if len(names) != len(set(names)) or len(paths) != len(set(paths)):
            raise ValueError("workspace slot names and logical paths must be unique")
        if any(
            _workspace_paths_overlap(left, right)
            for index, left in enumerate(paths)
            for right in paths[index + 1 :]
        ):
            raise ValueError("workspace slot paths cannot overlap")
        writable_owners = [
            (slot.logical_path, slot.owner.owner_id)
            for slot in self.slots
            if slot.access == "exclusive_write"
        ]
        if len(writable_owners) != len(set(writable_owners)):
            raise ValueError("writable workspace slots require one owner")
        return self


def _workspace_paths_overlap(left: str, right: str) -> bool:
    normalized_left = left.rstrip("/")
    normalized_right = right.rstrip("/")
    return (
        normalized_left == normalized_right
        or normalized_left.startswith(normalized_right + "/")
        or normalized_right.startswith(normalized_left + "/")
    )


class SnapshotCreationReason(StrEnum):
    REPRODUCIBILITY = "reproducibility"
    DEBUGGING = "debugging"
    RESUMPTION = "resumption"
    AUDIT = "audit"
    FAILURE = "failure"
    CYCLE = "cycle"


class SnapshotCapabilityShape(Contract):
    """Non-secret historical shape; never an authority grant."""

    capabilities: frozenset[str] = frozenset()
    tool_ids: frozenset[str] = frozenset()
    mcp_server_ids: frozenset[str] = frozenset()
    data_scope_refs: frozenset[str] = frozenset()
    network_hosts: frozenset[str] = frozenset()
    network_policy: Literal["none", "allowlisted"] = "none"


class SnapshotRetention(Contract):
    policy_ref: str = Field(min_length=1)
    retain_until: AwareDatetime | None = None
    deletion_protected: bool = False


class SnapshotPayloadAddress(Contract):
    object_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    size_bytes: int = Field(ge=0)
    media_type: str = Field(min_length=1)


class SandboxSnapshotCreateRequest(Contract):
    snapshot_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    source_namespace_id: str = Field(min_length=1)
    source_workspace_id: str = Field(min_length=1)
    parent_snapshot_id: str | None = Field(default=None, min_length=1)
    provider: str = Field(min_length=1)
    reason: SnapshotCreationReason
    producer_binding_id: str = Field(min_length=1)
    snapshot_policy_ref: str = Field(min_length=1)
    runtime_digest: str = Field(pattern=DIGEST_PATTERN)
    image_digest: str = Field(pattern=DIGEST_PATTERN)
    package_digest: str = Field(pattern=DIGEST_PATTERN)
    environment_digest: str = Field(pattern=DIGEST_PATTERN)
    workspace_contract_digest: str = Field(pattern=DIGEST_PATTERN)
    mount_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    capability_shape: SnapshotCapabilityShape
    retention: SnapshotRetention
    created_at: AwareDatetime


class SandboxSnapshotCapture(Contract):
    provider_snapshot_id: str = Field(min_length=1)
    filesystem_digest: str = Field(pattern=DIGEST_PATTERN)
    content_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    payload: bytes = Field(repr=False, max_length=268_435_456)
    media_type: str = Field(default="application/x-tar", min_length=1)


class SandboxSnapshot(Contract):
    snapshot_id: str = Field(min_length=1)
    creation_identity: str = Field(pattern=DIGEST_PATTERN)
    request_scope: str = Field(min_length=1)
    source_namespace_id: str = Field(min_length=1)
    source_workspace_id: str = Field(min_length=1)
    parent_snapshot_id: str | None = None
    provider: str = Field(min_length=1)
    provider_snapshot_id: str = Field(min_length=1)
    filesystem_digest: str = Field(pattern=DIGEST_PATTERN)
    content_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    payload: SnapshotPayloadAddress
    runtime_digest: str = Field(pattern=DIGEST_PATTERN)
    image_digest: str = Field(pattern=DIGEST_PATTERN)
    package_digest: str = Field(pattern=DIGEST_PATTERN)
    environment_digest: str = Field(pattern=DIGEST_PATTERN)
    workspace_contract_digest: str = Field(pattern=DIGEST_PATTERN)
    mount_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    reason: SnapshotCreationReason
    producer_binding_id: str = Field(min_length=1)
    snapshot_policy_ref: str = Field(min_length=1)
    capability_shape: SnapshotCapabilityShape
    retention: SnapshotRetention
    created_at: AwareDatetime


class SnapshotCloneRequest(Contract):
    snapshot_id: str = Field(min_length=1)
    clone_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    target_namespace_id: str = Field(min_length=1)
    target_workspace_id: str = Field(min_length=1)
    binding_id: str = Field(min_length=1)
    runtime_digest: str = Field(pattern=DIGEST_PATTERN)
    image_digest: str = Field(pattern=DIGEST_PATTERN)
    package_digest: str = Field(pattern=DIGEST_PATTERN)
    environment_digest: str = Field(pattern=DIGEST_PATTERN)
    workspace_contract_digest: str = Field(pattern=DIGEST_PATTERN)
    target_mount_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    capability_shape: SnapshotCapabilityShape
    requested_at: AwareDatetime


class SnapshotCloneRecord(Contract):
    clone_id: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    parent_workspace_id: str = Field(min_length=1)
    target_namespace_id: str = Field(min_length=1)
    target_workspace_id: str = Field(min_length=1)
    binding_id: str = Field(min_length=1)
    resources: ReacquiredRuntimeResources
    created_at: AwareDatetime


class ReacquiredRuntimeResources(Contract):
    secret_names: tuple[str, ...] = ()
    credential_names: tuple[str, ...] = ()
    lease_names: tuple[str, ...] = ()
    mcp_connection_names: tuple[str, ...] = ()
    socket_names: tuple[str, ...] = ()


class SnapshotCloneResult(Contract):
    clone_id: str
    workspace: MaterializedWorkspace
    parent_snapshot_id: str
    parent_workspace_id: str
    resources: ReacquiredRuntimeResources = Field(default_factory=ReacquiredRuntimeResources)
    credentials_reresolved: Literal[True] = True
    external_leases_reresolved: Literal[True] = True
    live_resources_restored: tuple[()] = ()
    artifact_promotion_required: Literal[True] = True
