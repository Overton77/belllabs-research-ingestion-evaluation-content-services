from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import ExactDefinitionRef, SecretRef
from app.domain.graph_runtime.identities import DIGEST_PATTERN, GraphIdentity, SubagentProfileKey


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RuntimeDefinitionKind(StrEnum):
    GRAPH_ASSEMBLY = "graph_assembly"
    AGENT_HARNESS = "agent_harness"
    MIDDLEWARE_STACK = "middleware_stack"
    CONTEXT_POLICY = "context_policy"
    CONTEXT_ASSEMBLY = "context_assembly"
    DELEGATION_POLICY = "delegation_policy"
    MCP_SERVER = "mcp_server"
    PROMPT_CONTEXT = "prompt_context"
    INTERPRETER_PROFILE = "interpreter_profile"
    SANDBOX_PROFILE = "sandbox_profile"
    EXECUTION_ENVIRONMENT = "execution_environment"
    EVALUATION_PROFILE = "evaluation_profile"
    CAPABILITY_MANIFEST = "capability_manifest"
    STATE_SCHEMA = "state_schema"
    REDUCER_REGISTRY = "reducer_registry"
    OPERATION_REGISTRY = "operation_registry"
    RUN_PLAN = "run_plan"


class ContentAddressedRef(Contract):
    kind: RuntimeDefinitionKind
    logical_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    schema_version: str = Field(min_length=1, max_length=64)
    digest: str = Field(pattern=DIGEST_PATTERN)
    content_uri: str | None = Field(default=None, min_length=1)


class RuntimeDefinition(Contract):
    schema_version: Literal["belllabs.graph-runtime.v1"] = "belllabs.graph-runtime.v1"
    logical_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    title: str = Field(min_length=1, max_length=256)
    description: str = Field(min_length=1, max_length=4_000)

    @property
    def digest(self) -> str:
        return sha256_digest(self.model_dump(mode="json"))


class MiddlewareBinding(Contract):
    middleware_id: str = Field(pattern=r"^[a-z][a-z0-9._:-]*$")
    implementation_ref: ContentAddressedRef
    phase: Literal["before_agent", "before_model", "wrap_model", "after_model", "after_agent"]
    core_capability: Literal[
        "planning",
        "filesystem",
        "subagents",
        "summarization",
        "cache",
        "human_in_the_loop",
        "tool_policy",
        "custom",
    ]
    configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    conflicts_with: frozenset[str] = Field(default_factory=frozenset)


class MiddlewareStackDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.MIDDLEWARE_STACK] = (
        RuntimeDefinitionKind.MIDDLEWARE_STACK
    )
    ordered_middleware: tuple[MiddlewareBinding, ...]

    @model_validator(mode="after")
    def reject_duplicates_and_conflicts(self) -> MiddlewareStackDefinition:
        identities = [item.middleware_id for item in self.ordered_middleware]
        if len(identities) != len(set(identities)):
            raise ValueError("middleware identities must be unique")
        core = [
            item.core_capability
            for item in self.ordered_middleware
            if item.core_capability != "custom"
        ]
        if len(core) != len(set(core)):
            raise ValueError("core middleware capabilities cannot be installed twice")
        selected = set(identities)
        for item in self.ordered_middleware:
            conflicts = item.conflicts_with & selected
            if conflicts:
                raise ValueError(
                    f"middleware {item.middleware_id} conflicts with {sorted(conflicts)}"
                )
        return self


class AgentHarnessDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.AGENT_HARNESS] = RuntimeDefinitionKind.AGENT_HARNESS
    harness_kind: Literal["langchain_agent", "deep_agent", "compiled_graph", "pure_operation"]
    package_lock_digest: str = Field(pattern=DIGEST_PATTERN)
    model_profile_ref: ExactDefinitionRef | None = None
    prompt_context_ref: ContentAddressedRef
    middleware_stack_ref: ContentAddressedRef
    tool_schema_refs: tuple[ContentAddressedRef, ...] = ()
    skill_manifest_refs: tuple[ContentAddressedRef, ...] = ()
    filesystem_backend: Literal["none", "state", "store", "sandbox"]
    default_tools_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def harness_backend_matches_skills(self) -> AgentHarnessDefinition:
        if self.skill_manifest_refs and self.filesystem_backend == "none":
            raise ValueError("skill-enabled harnesses require an explicit filesystem backend")
        if self.harness_kind == "deep_agent" and self.default_tools_digest is None:
            raise ValueError("Deep Agents harnesses require an inspected default tool digest")
        return self


class ContextSourceRule(Contract):
    source_kind: Literal[
        "admitted_input",
        "artifact",
        "evidence",
        "catalog",
        "procedural_store",
        "prior_checkpoint_summary",
    ]
    authority: Literal["authoritative", "evidence", "derived", "procedural_only"]
    max_items: int = Field(ge=0)
    max_bytes: int = Field(ge=0)
    retention_policy_ref: str = Field(min_length=1)
    sensitivity_policy_ref: str = Field(min_length=1)


class ContextPolicyDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.CONTEXT_POLICY] = RuntimeDefinitionKind.CONTEXT_POLICY
    source_rules: tuple[ContextSourceRule, ...]
    protected_atom_kinds: frozenset[str]
    contradiction_policy_ref: str = Field(min_length=1)
    tombstone_policy_ref: str = Field(min_length=1)
    approval_policy_ref: str = Field(min_length=1)
    maximum_context_bytes: int = Field(ge=1)
    store_scientific_authority: Literal[False] = False
    store_approval_authority: Literal[False] = False
    store_budget_authority: Literal[False] = False
    store_terminality_authority: Literal[False] = False

    @field_validator("source_rules")
    @classmethod
    def context_source_kinds_are_unique(
        cls, value: tuple[ContextSourceRule, ...]
    ) -> tuple[ContextSourceRule, ...]:
        kinds = [rule.source_kind for rule in value]
        if len(kinds) != len(set(kinds)):
            raise ValueError("context source rules must be unique by source kind")
        return value


class ContextManifestEntry(Contract):
    entry_id: str = Field(min_length=1)
    source_kind: str = Field(min_length=1)
    source_ref: str = Field(min_length=1)
    source_digest: str = Field(pattern=DIGEST_PATTERN)
    authority: Literal["authoritative", "evidence", "derived", "procedural_only"]
    byte_count: int = Field(ge=0)
    tombstoned: bool = False
    contradiction_group: str | None = None
    approval_ref: str | None = None


class ContextAssemblySpec(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.CONTEXT_ASSEMBLY] = (
        RuntimeDefinitionKind.CONTEXT_ASSEMBLY
    )
    policy_ref: ContentAddressedRef
    ordered_entries: tuple[ContextManifestEntry, ...]
    protected_atoms_digest: str = Field(pattern=DIGEST_PATTERN)
    context_assembly_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def assembly_digest_matches_content(self) -> ContextAssemblySpec:
        content = _model_content(self, exclude={"context_assembly_digest"})
        if sha256_digest(content) != self.context_assembly_digest:
            raise ValueError("context assembly digest mismatch")
        if sum(item.byte_count for item in self.ordered_entries if not item.tombstoned) < 0:
            raise ValueError("context byte count cannot be negative")
        return self

    @classmethod
    def create(cls, **values: object) -> ContextAssemblySpec:
        draft = cast(Any, cls).model_construct(
            **values,
            context_assembly_digest=DIGEST_PLACEHOLDER,
        )
        digest = sha256_digest(_model_content(draft, exclude={"context_assembly_digest"}))
        return cls(**values, context_assembly_digest=digest)


class SyncDictionarySubagent(Contract):
    kind: Literal["dictionary_agent"] = "dictionary_agent"
    profile: SubagentProfileKey
    harness_ref: ContentAddressedRef
    tool_name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    tool_description: str = Field(min_length=1, max_length=2_000)


class SyncCompiledGraphSubagent(Contract):
    kind: Literal["compiled_graph"] = "compiled_graph"
    profile: SubagentProfileKey
    graph_assembly_ref: ContentAddressedRef


SynchronousSubagentDefinition = Annotated[
    SyncDictionarySubagent | SyncCompiledGraphSubagent,
    Field(discriminator="kind"),
]


class AsyncSubagentDefinition(Contract):
    kind: Literal["async_remote_graph"] = "async_remote_graph"
    profile: SubagentProfileKey
    graph_id: str = Field(min_length=1)
    deployment_endpoint_ref: str = Field(min_length=1)
    runtime_policy_ref: ContentAddressedRef
    dedicated_child_thread: Literal[True] = True
    headers_secret_ref: SecretRef | None = None
    lifecycle_policy_ref: str = Field(min_length=1)


class DelegationModePolicy(Contract):
    mode: Literal["sync_subagent", "dynamic_interpreter", "async_subagent", "linked_run"]
    enabled: bool = False
    maturity: Literal["stable", "beta", "preview", "policy_disabled"]
    fallback_mode: Literal["sync_subagent", "async_subagent", "linked_run", "reject"]
    max_concurrency: int = Field(ge=0)
    max_depth: int = Field(ge=0)
    capacity_policy_ref: str = Field(min_length=1)
    result_admission_policy_ref: str = Field(min_length=1)


class DelegationPolicyDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.DELEGATION_POLICY] = (
        RuntimeDefinitionKind.DELEGATION_POLICY
    )
    continuity_mode: Literal[
        "isolated_context",
        "bounded_context_slice",
        "snapshot_continuity",
        "linked_run_contract",
    ]
    modes: tuple[DelegationModePolicy, ...]
    synchronous_subagents: tuple[SynchronousSubagentDefinition, ...] = ()
    asynchronous_subagents: tuple[AsyncSubagentDefinition, ...] = ()

    @model_validator(mode="after")
    def delegation_modes_are_complete_and_distinct(self) -> DelegationPolicyDefinition:
        names = [item.mode for item in self.modes]
        required = {"sync_subagent", "dynamic_interpreter", "async_subagent", "linked_run"}
        if set(names) != required or len(names) != len(required):
            raise ValueError("all four delegation modes require distinct policies")
        enabled = {item.mode for item in self.modes if item.enabled}
        if self.asynchronous_subagents and "async_subagent" not in enabled:
            raise ValueError("async subagent definitions require the async mode to be enabled")
        if self.synchronous_subagents and "sync_subagent" not in enabled:
            raise ValueError("sync subagent definitions require the sync mode to be enabled")
        return self


class MCPServerDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.MCP_SERVER] = RuntimeDefinitionKind.MCP_SERVER
    transport: Literal["stdio", "sse", "streamable_http"]
    endpoint_ref: str = Field(min_length=1)
    tool_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    allowed_tools: frozenset[str]
    session_policy: Literal["stateless", "per_operation", "per_thread"]
    elicitation_policy: Literal["deny", "typed_intervention"]
    auth_secret_refs: tuple[SecretRef, ...] = ()
    timeout_seconds: int = Field(ge=1)
    max_retries: int = Field(ge=0)


class PromptContextBinding(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.PROMPT_CONTEXT] = RuntimeDefinitionKind.PROMPT_CONTEXT
    ordered_prompt_refs: tuple[ExactDefinitionRef, ...]
    rendered_prompt_digest: str = Field(pattern=DIGEST_PATTERN)
    context_policy_ref: ContentAddressedRef
    context_assembly_ref: ContentAddressedRef


class InterpreterProfileDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.INTERPRETER_PROFILE] = (
        RuntimeDefinitionKind.INTERPRETER_PROFILE
    )
    interpreter_kind: Literal["none", "native_tool", "quickjs_call", "quickjs_ptc"]
    package_lock_digest: str = Field(pattern=DIGEST_PATTERN)
    memory_limit_bytes: int = Field(ge=0)
    timeout_seconds: int = Field(ge=0)
    max_calls: int = Field(ge=0)
    subagents_allowed: bool = False
    enabled: bool = False
    fallback_ref: ContentAddressedRef | None = None

    @model_validator(mode="after")
    def disabled_experimental_interpreter_has_fallback(self) -> InterpreterProfileDefinition:
        if self.interpreter_kind in {"quickjs_call", "quickjs_ptc"} and not self.enabled:
            if self.fallback_ref is None:
                raise ValueError("disabled experimental interpreters require an exact fallback")
        return self


class SandboxSnapshotPolicy(Contract):
    capture: Literal["never", "on_interrupt", "on_failure", "always"]
    retention_policy_ref: str = Field(min_length=1)
    deletion_policy_ref: str = Field(min_length=1)
    restore_requires_reauthorization: Literal[True] = True


class SandboxProfileDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.SANDBOX_PROFILE] = RuntimeDefinitionKind.SANDBOX_PROFILE
    provider: Literal["langsmith_sandbox", "legacy_local_docker", "none"]
    runtime_image_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    package_lock_digest: str = Field(pattern=DIGEST_PATTERN)
    network_policy_ref: str = Field(min_length=1)
    mount_policy_ref: str = Field(min_length=1)
    credential_policy_ref: str = Field(min_length=1)
    snapshot_policy: SandboxSnapshotPolicy
    enabled: bool = False


class ExecutionEnvironmentDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.EXECUTION_ENVIRONMENT] = (
        RuntimeDefinitionKind.EXECUTION_ENVIRONMENT
    )
    environment_id: str = Field(min_length=1)
    deployment_ref: str = Field(min_length=1)
    deployment_revision: str = Field(min_length=1)
    package_lock_digest: str = Field(pattern=DIGEST_PATTERN)
    feature_manifest_ref: ContentAddressedRef
    sandbox_profile_ref: ContentAddressedRef
    region: str | None = None
    required_secret_refs: tuple[SecretRef, ...] = ()


class EvaluationProfileDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.EVALUATION_PROFILE] = (
        RuntimeDefinitionKind.EVALUATION_PROFILE
    )
    evaluator_refs: tuple[ExactDefinitionRef, ...]
    dataset_refs: tuple[str, ...] = ()
    gate_thresholds: dict[str, float] = Field(default_factory=dict)
    online_evaluators_enabled: Literal[False] = False
    trace_policy_ref: str = Field(min_length=1)


class CapabilityMaturityRecord(Contract):
    capability_id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    maturity: Literal["stable", "beta", "preview", "entitlement_dependent", "policy_disabled"]
    required_for_migration: bool
    feature_flag: str = Field(pattern=r"^[A-Z][A-Z0-9_]*$")
    enabled: bool = False
    fallback: str = Field(min_length=1)
    promotion_gate: str | None = None

    @model_validator(mode="after")
    def nonstable_capability_requires_gate_when_enabled(self) -> CapabilityMaturityRecord:
        if self.enabled and self.maturity != "stable" and not self.promotion_gate:
            raise ValueError("enabled non-stable capabilities require a passed promotion gate")
        return self


class CapabilityManifestDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.CAPABILITY_MANIFEST] = (
        RuntimeDefinitionKind.CAPABILITY_MANIFEST
    )
    capabilities: tuple[CapabilityMaturityRecord, ...]

    @field_validator("capabilities")
    @classmethod
    def capability_ids_are_unique(
        cls, value: tuple[CapabilityMaturityRecord, ...]
    ) -> tuple[CapabilityMaturityRecord, ...]:
        ids = [item.capability_id for item in value]
        if len(ids) != len(set(ids)):
            raise ValueError("capability identities must be unique")
        return value


class GraphAssemblyDefinition(RuntimeDefinition):
    kind: Literal[RuntimeDefinitionKind.GRAPH_ASSEMBLY] = RuntimeDefinitionKind.GRAPH_ASSEMBLY
    graph: GraphIdentity
    graph_factory_ref: ContentAddressedRef
    state_schema_ref: ContentAddressedRef
    reducer_registry_ref: ContentAddressedRef
    operation_registry_ref: ContentAddressedRef
    harness_ref: ContentAddressedRef
    context_policy_ref: ContentAddressedRef
    delegation_policy_ref: ContentAddressedRef
    execution_environment_ref: ContentAddressedRef
    evaluation_profile_ref: ContentAddressedRef
    capability_manifest_ref: ContentAddressedRef
    checkpoint_compatibility_key: str = Field(min_length=1)
    prohibited_state_fields: frozenset[str] = frozenset(
        {"secrets", "credentials", "checkpoint_body", "raw_private_corpus"}
    )
    maximum_state_bytes: int = Field(ge=1)

    @model_validator(mode="after")
    def graph_digest_matches_definition(self) -> GraphAssemblyDefinition:
        content = _model_content(self, exclude={"graph"})
        if self.graph.graph_assembly_digest != sha256_digest(content):
            raise ValueError("graph assembly digest does not match the frozen assembly")
        return self

    @classmethod
    def create(
        cls,
        *,
        graph_family: Literal["StageGraph", "GoalDirected", "deep_agent", "operation"],
        graph_id: str,
        **values: object,
    ) -> GraphAssemblyDefinition:
        placeholder = GraphIdentity(
            graph_family=graph_family,
            graph_id=graph_id,
            graph_assembly_digest=DIGEST_PLACEHOLDER,
        )
        draft = cast(Any, cls).model_construct(**values, graph=placeholder)
        digest = sha256_digest(_model_content(draft, exclude={"graph"}))
        graph = placeholder.model_copy(update={"graph_assembly_digest": digest})
        return cls(**values, graph=graph)


class NativeOperationImplementation(Contract):
    kind: Literal["native_operation"] = "native_operation"
    operation_contract_ref: str = Field(min_length=1)
    implementation_ref: ContentAddressedRef


class HarnessOperationImplementation(Contract):
    kind: Literal["agent_harness"] = "agent_harness"
    operation_contract_ref: str = Field(min_length=1)
    harness_ref: ContentAddressedRef


class CompiledGraphOperationImplementation(Contract):
    kind: Literal["compiled_graph"] = "compiled_graph"
    operation_contract_ref: str = Field(min_length=1)
    graph_assembly_ref: ContentAddressedRef


OperationImplementation = Annotated[
    NativeOperationImplementation
    | HarnessOperationImplementation
    | CompiledGraphOperationImplementation,
    Field(discriminator="kind"),
]


class StageImplementationBinding(Contract):
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    implementation: OperationImplementation


class GraphAssemblySpec(Contract):
    schema_version: Literal["belllabs.graph-assembly-spec.v1"] = (
        "belllabs.graph-assembly-spec.v1"
    )
    graph_assembly_ref: ContentAddressedRef
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    reducer_registry_digest: str = Field(pattern=DIGEST_PATTERN)
    operation_registry_digest: str = Field(pattern=DIGEST_PATTERN)
    stage_implementations: tuple[StageImplementationBinding, ...]
    compatibility_manifest_digest: str = Field(pattern=DIGEST_PATTERN)

    @field_validator("stage_implementations")
    @classmethod
    def stages_are_unique(
        cls, value: tuple[StageImplementationBinding, ...]
    ) -> tuple[StageImplementationBinding, ...]:
        stages = [item.stage_id for item in value]
        if len(stages) != len(set(stages)):
            raise ValueError("stage implementation bindings must be unique")
        return value


class RunPlan(Contract):
    schema_version: Literal["belllabs.run-plan.v2"] = "belllabs.run-plan.v2"
    plan_id: str = Field(min_length=1)
    effective_run_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    semantic_binding_ref: str = Field(min_length=1)
    workflow_implementation_ref: ExactDefinitionRef
    graph_assembly: GraphAssemblySpec
    harness_ref: ContentAddressedRef
    delegation_policy_ref: ContentAddressedRef
    context_assembly_ref: ContentAddressedRef
    execution_environment_ref: ContentAddressedRef
    capability_manifest_ref: ContentAddressedRef
    evaluation_profile_ref: ContentAddressedRef
    alias_evidence_digest: str = Field(pattern=DIGEST_PATTERN)
    plan_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def plan_digest_matches_frozen_content(self) -> RunPlan:
        content = _model_content(self, exclude={"plan_digest"})
        if sha256_digest(content) != self.plan_digest:
            raise ValueError("RunPlan digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> RunPlan:
        draft = cast(Any, cls).model_construct(
            **values,
            plan_digest=DIGEST_PLACEHOLDER,
        )
        digest = sha256_digest(_model_content(draft, exclude={"plan_digest"}))
        return cls(**values, plan_digest=digest)


class StageCapabilityRequirement(Contract):
    """Provider-neutral immutable requirement for one executable stage variant."""

    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    variant_name: str = Field(default="default", pattern=r"^[a-z][a-z0-9_-]*$")
    operation_contract_ref: str = Field(min_length=1)
    required_capability_ids: frozenset[str] = frozenset()
    optional_capability_ids: frozenset[str] = frozenset()
    input_contract_ref: str = Field(min_length=1)
    output_contract_ref: str = Field(min_length=1)
    context_purpose: str = Field(min_length=1)
    effect_class: Literal[
        "pure", "read_only", "idempotent_effect", "consequential_effect"
    ]
    delegation_modes_allowed: frozenset[Literal["sync", "async", "linked_run"]] = frozenset()
    resource_class_ref: str = Field(min_length=1)
    verification_contract_ref: str = Field(min_length=1)
    degradation_contract_ref: str = Field(min_length=1)
    speculation_policy_ref: str = Field(min_length=1)

    @model_validator(mode="after")
    def optional_capabilities_cannot_duplicate_required(self) -> StageCapabilityRequirement:
        if self.required_capability_ids & self.optional_capability_ids:
            raise ValueError("a capability cannot be both required and optional")
        if self.effect_class in {"idempotent_effect", "consequential_effect"} and (
            self.speculation_policy_ref != "policy:speculation:disabled"
        ):
            raise ValueError("effectful stages must keep speculation disabled")
        return self


class ExecutionResourceEnvelope(Contract):
    tenant_limit_ref: str = Field(min_length=1)
    workflow_run_slots: int = Field(ge=0)
    stage_slots: int = Field(ge=0)
    operation_worker_slots: int = Field(ge=0)
    model_call_slots: int = Field(ge=0)
    tool_call_slots: int = Field(ge=0)
    mcp_call_slots: int = Field(ge=0)
    sync_subagent_slots: int = Field(ge=0)
    async_child_slots: int = Field(ge=0)
    linked_run_slots: int = Field(ge=0)
    provider_quota_refs: tuple[str, ...] = ()
    budget_reservation_refs: tuple[str, ...] = ()
    deadline: str = Field(min_length=1)
    lease_ttl: str = Field(min_length=1)
    resumption_reserve: int = Field(ge=0)
    release_policy: str = Field(min_length=1)


class UnavailableStageSurface(Contract):
    """Fail-closed prediction for one required stage capability surface."""

    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    variant_name: str = Field(default="default", pattern=r"^[a-z][a-z0-9_-]*$")
    capability_id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    reason_code: Literal[
        "authority_denied",
        "capability_unavailable",
        "maturity_not_promoted",
        "feature_disabled",
        "readiness_unavailable",
    ]
    maturity: Literal[
        "stable", "beta", "preview", "entitlement_dependent", "policy_disabled"
    ]
    fallback: str = Field(min_length=1)
    detail: str = Field(min_length=1)


class OperationAssemblySpec(Contract):
    schema_version: Literal["belllabs.operation-assembly.v2"] = "belllabs.operation-assembly.v2"
    operation_assembly_id: str = Field(min_length=1)
    operation_contract_ref: str = Field(min_length=1)
    implementation_kind: Literal[
        "native", "agent_harness", "compiled_graph", "async_child", "linked_run"
    ]
    implementation_ref: ContentAddressedRef
    model_policy_ref: ContentAddressedRef
    prompt_manifest_ref: ContentAddressedRef
    middleware_manifest_ref: ContentAddressedRef
    tool_manifest_ref: ContentAddressedRef
    mcp_manifest_ref: ContentAddressedRef
    skill_manifest_ref: ContentAddressedRef
    context_assembly_ref: ContentAddressedRef
    delegation_policy_ref: ContentAddressedRef
    synchronous_subagent_refs: tuple[ContentAddressedRef, ...] = ()
    async_subagent_target_refs: tuple[ContentAddressedRef, ...] = ()
    workspace_policy_ref: ContentAddressedRef
    sandbox_profile_ref: ContentAddressedRef
    verifier_ref: ContentAddressedRef
    resource_envelope_ref: ContentAddressedRef
    effect_policy_ref: ContentAddressedRef
    fallback_policy_ref: ContentAddressedRef
    trace_redaction_policy_ref: ContentAddressedRef
    capability_manifest_ref: ContentAddressedRef
    compatibility_manifest_ref: ContentAddressedRef
    operation_assembly_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def assembly_digest_matches_content(self) -> OperationAssemblySpec:
        if sha256_digest(_model_content(self, exclude={"operation_assembly_digest"})) != (
            self.operation_assembly_digest
        ):
            raise ValueError("operation assembly digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> OperationAssemblySpec:
        draft = cast(Any, cls).model_construct(
            **values, operation_assembly_digest=DIGEST_PLACEHOLDER
        )
        digest = sha256_digest(_model_content(draft, exclude={"operation_assembly_digest"}))
        return cls(**values, operation_assembly_digest=digest)


class StageExecutionBinding(Contract):
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    variant_name: str = Field(default="default", pattern=r"^[a-z][a-z0-9_-]*$")
    stage_requirement_ref: ContentAddressedRef
    operation_assembly_ref: ContentAddressedRef
    operation_assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    input_projection_ref: str = Field(min_length=1)
    output_projection_ref: str = Field(min_length=1)
    resource_envelope_ref: ContentAddressedRef
    compatibility_key: str = Field(min_length=1)


class ExecutionLineageEnvelope(Contract):
    request_scope: str = Field(min_length=1)
    belllabs_run_id: str = Field(min_length=1)
    execution_epoch: int = Field(ge=1)
    workflow_implementation_ref: str = Field(min_length=1)
    graph_assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    workflow_cycle: int = Field(ge=0)
    stage_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]*$")
    stage_cycle: int | None = Field(default=None, ge=0)
    semantic_operation_attempt_id: str | None = None
    runtime_attempt_id: str | None = None
    operation_binding_id: str | None = None
    operation_assembly_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    agent_invocation_id: str | None = None
    parent_lineage_id: str | None = None
    delegation_mode: Literal["sync", "async", "linked_run"] | None = None
    child_task_id: str | None = None
    child_thread_id: str | None = None
    child_run_id: str | None = None
    effect_claim_ids: tuple[str, ...] = ()
    input_manifest_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    context_manifest_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    result_manifest_ref: str | None = None
    evidence_refs: tuple[str, ...] = ()
    usage_settlement_refs: tuple[str, ...] = ()
    trace_ref: str | None = None


class GraphAssemblySpecV2(Contract):
    """Versioned replacement; GraphAssemblySpec v1 remains readable unchanged."""

    schema_version: Literal["belllabs.graph-assembly-spec.v2"] = (
        "belllabs.graph-assembly-spec.v2"
    )
    graph_assembly_ref: ContentAddressedRef
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    reducer_registry_digest: str = Field(pattern=DIGEST_PATTERN)
    operation_registry_digest: str = Field(pattern=DIGEST_PATTERN)
    stage_requirements: tuple[StageCapabilityRequirement, ...]
    stage_execution_bindings: tuple[StageExecutionBinding, ...]
    compatibility_manifest_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def requirements_and_bindings_are_one_to_one(self) -> GraphAssemblySpecV2:
        requirement_keys = {(item.stage_id, item.variant_name) for item in self.stage_requirements}
        binding_keys = {
            (item.stage_id, item.variant_name) for item in self.stage_execution_bindings
        }
        if len(requirement_keys) != len(self.stage_requirements):
            raise ValueError("stage capability requirements must be unique")
        if len(binding_keys) != len(self.stage_execution_bindings):
            raise ValueError("stage execution bindings must be unique")
        if requirement_keys != binding_keys:
            raise ValueError("every stage requirement requires exactly one execution binding")
        return self


class RunPlanV3(Contract):
    """v3 removes ambiguous global runtime defaults in favor of per-stage bindings."""

    schema_version: Literal["belllabs.run-plan.v3"] = "belllabs.run-plan.v3"
    plan_id: str = Field(min_length=1)
    effective_run_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    semantic_binding_ref: str = Field(min_length=1)
    workflow_implementation_ref: ExactDefinitionRef
    graph_assembly: GraphAssemblySpecV2
    alias_evidence_digest: str = Field(pattern=DIGEST_PATTERN)
    plan_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def plan_digest_matches_frozen_content(self) -> RunPlanV3:
        if sha256_digest(_model_content(self, exclude={"plan_digest"})) != self.plan_digest:
            raise ValueError("RunPlan v3 digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> RunPlanV3:
        draft = cast(Any, cls).model_construct(**values, plan_digest=DIGEST_PLACEHOLDER)
        digest = sha256_digest(_model_content(draft, exclude={"plan_digest"}))
        return cls(**values, plan_digest=digest)


DIGEST_PLACEHOLDER = "sha256:" + "0" * 64


def _model_content(
    model: BaseModel,
    *,
    exclude: set[str],
) -> dict[str, object]:
    return {
        field_name: getattr(model, field_name)
        for field_name in type(model).model_fields
        if field_name not in exclude
    }
