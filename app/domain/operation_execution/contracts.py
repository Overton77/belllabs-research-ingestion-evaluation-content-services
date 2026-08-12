from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import ExactDefinitionRef, SecretRef

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
PLACEHOLDER_DIGEST = "sha256:" + "0" * 64
MAX_OPERATION_WORKFLOW_PAYLOAD_BYTES = 2_000_000
MAX_OPERATION_SEMANTIC_ATTEMPT_ID_LENGTH = 512
MAX_NATIVE_PLACEMENT_ID_LENGTH = 255
MAX_TEMPORAL_TASK_QUEUE_LENGTH = 255
MAX_EFFECT_FRONTIER_ITEMS = 1_024
MAX_EFFECT_FRONTIER_ITEM_LENGTH = 2_048
MAX_ACTIVE_ASYNC_CHILDREN = 1_024
MAX_ASYNC_CHILD_ID_LENGTH = 512

EffectFrontierItem = Annotated[
    str,
    Field(min_length=1, max_length=MAX_EFFECT_FRONTIER_ITEM_LENGTH),
]
AsyncChildId = Annotated[
    str,
    Field(min_length=1, max_length=MAX_ASYNC_CHILD_ID_LENGTH),
]


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


def workspace_durable_reference(namespace_id: str, workspace_id: str) -> str:
    """Return the opaque local durable reference for a governed workspace."""

    identity = sha256_digest(
        {"workspace_id": workspace_id, "namespace_id": namespace_id}
    ).removeprefix("sha256:")
    return f"workspace://{identity}"


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


class CognitiveChannelContributor(StrEnum):
    BASE = "base"
    MIDDLEWARE = "middleware"
    WORKFLOW_TYPE = "workflow_type"
    OPERATION_ROLE = "operation_role"


class CognitiveReducer(StrEnum):
    REPLACE = "replace"
    MERGE_BY_KEY = "merge_by_key"
    APPEND_UNIQUE_BY_ID = "append_unique_by_id"


class CognitiveChannelDefinition(Contract):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    value_kind: Literal["object", "map", "append_list", "string_list"]
    value_schema_ref: str = Field(min_length=1)
    reducer: CognitiveReducer
    sensitivity: Literal["public", "internal", "restricted"] = "internal"


class CognitiveChannelPack(Contract):
    schema_version: Literal["belllabs.cognitive-channel-pack.v1"] = (
        "belllabs.cognitive-channel-pack.v1"
    )
    logical_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    revision: int = Field(ge=1)
    contributor: CognitiveChannelContributor
    channels: tuple[CognitiveChannelDefinition, ...] = Field(min_length=1)
    digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_pack(self, info: ValidationInfo) -> CognitiveChannelPack:
        names = [channel.name for channel in self.channels]
        if len(names) != len(set(names)):
            raise ValueError("cognitive channel pack names must be unique")
        built_ins = {"messages", "files", "todos", "structured_response"}
        if built_ins & set(names):
            raise ValueError("Deep Agents built-in channels must be inherited, not redeclared")
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"digest"})) != self.digest
        ):
            raise ValueError("cognitive channel pack digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> CognitiveChannelPack:
        payload = dict(values)
        payload.pop("digest", None)
        payload.setdefault("schema_version", "belllabs.cognitive-channel-pack.v1")
        draft = cls.model_validate(
            {**payload, "digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"digest"})
        return cls(**complete, digest=sha256_digest(complete))


class CognitiveChannelPackRef(Contract):
    logical_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    revision: int = Field(ge=1)
    digest: str = Field(pattern=DIGEST_PATTERN)
    contributor: CognitiveChannelContributor


class CognitiveRuntimeField(Contract):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    value_kind: Literal["string", "integer", "boolean", "string_list", "string_map"]
    required: bool = True
    sensitivity: Literal["public", "internal", "restricted"] = "internal"
    reference_only: bool = False


class CognitiveRuntimeContextPack(Contract):
    schema_version: Literal["belllabs.cognitive-context-pack.v1"] = (
        "belllabs.cognitive-context-pack.v1"
    )
    logical_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    revision: int = Field(ge=1)
    contributor: CognitiveChannelContributor
    fields: tuple[CognitiveRuntimeField, ...] = Field(min_length=1)
    digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_pack(self, info: ValidationInfo) -> CognitiveRuntimeContextPack:
        names = [field.name for field in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("cognitive runtime-context pack fields must be unique")
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"digest"})) != self.digest
        ):
            raise ValueError("cognitive runtime-context pack digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> CognitiveRuntimeContextPack:
        payload = dict(values)
        payload.pop("digest", None)
        payload.setdefault("schema_version", "belllabs.cognitive-context-pack.v1")
        draft = cls.model_validate(
            {**payload, "digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"digest"})
        return cls(**complete, digest=sha256_digest(complete))


class SubagentStateSlice(Contract):
    slice_id: str = Field(min_length=1)
    channel_names: frozenset[str]
    include_parent_messages: Literal[False] = False


class SubagentContextSlice(Contract):
    slice_id: str = Field(min_length=1)
    field_names: frozenset[str]
    include_secret_fields: Literal[False] = False


class CognitiveStateSchema(Contract):
    schema_version: Literal["belllabs.cognitive-state-schema.v1"] = (
        "belllabs.cognitive-state-schema.v1"
    )
    schema_id: str = Field(min_length=1)
    pack_refs: tuple[CognitiveChannelPackRef, ...] = Field(min_length=1)
    channels: tuple[CognitiveChannelDefinition, ...] = Field(min_length=3)
    reducer_registry_digest: str = Field(pattern=DIGEST_PATTERN)
    deepagents_version: Literal["0.7.5"] = "0.7.5"
    subagent_slices: tuple[SubagentStateSlice, ...] = ()
    schema_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_schema(self, info: ValidationInfo) -> CognitiveStateSchema:
        names = [channel.name for channel in self.channels]
        if len(names) != len(set(names)):
            raise ValueError("effective cognitive state channel collision")
        minimum = {"artifact_index", "context_manifest", "child_result_index"}
        if not minimum <= set(names):
            raise ValueError("effective cognitive state schema omits a BellLabs base channel")
        if len({item.slice_id for item in self.subagent_slices}) != len(self.subagent_slices):
            raise ValueError("subagent state slice identities must be unique")
        if any(not item.channel_names <= set(names) for item in self.subagent_slices):
            raise ValueError("subagent state slice contains an unknown channel")
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"schema_digest"}))
            != self.schema_digest
        ):
            raise ValueError("cognitive state schema digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> CognitiveStateSchema:
        payload = dict(values)
        payload.pop("schema_digest", None)
        payload.setdefault("schema_version", "belllabs.cognitive-state-schema.v1")
        payload.setdefault("deepagents_version", "0.7.5")
        payload.setdefault("subagent_slices", ())
        draft = cls.model_validate(
            {**payload, "schema_digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"schema_digest"})
        return cls(**complete, schema_digest=sha256_digest(complete))


class CognitiveRuntimeContextSchema(Contract):
    schema_version: Literal["belllabs.cognitive-context-schema.v1"] = (
        "belllabs.cognitive-context-schema.v1"
    )
    schema_id: str = Field(min_length=1)
    pack_refs: tuple[CognitiveChannelPackRef, ...] = Field(min_length=1)
    fields: tuple[CognitiveRuntimeField, ...] = Field(min_length=1)
    subagent_slices: tuple[SubagentContextSlice, ...] = ()
    schema_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_schema(self, info: ValidationInfo) -> CognitiveRuntimeContextSchema:
        names = [field.name for field in self.fields]
        if len(names) != len(set(names)):
            raise ValueError("effective cognitive runtime-context field collision")
        forbidden = {"api_key", "password", "secret", "token", "credential"}
        for field in self.fields:
            normalized = field.name.lower()
            if any(fragment in normalized for fragment in forbidden) and not field.reference_only:
                raise ValueError("sensitive cognitive context fields must contain references only")
        if len({item.slice_id for item in self.subagent_slices}) != len(self.subagent_slices):
            raise ValueError("subagent context slice identities must be unique")
        if any(not item.field_names <= set(names) for item in self.subagent_slices):
            raise ValueError("subagent context slice contains an unknown field")
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"schema_digest"}))
            != self.schema_digest
        ):
            raise ValueError("cognitive runtime-context schema digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> CognitiveRuntimeContextSchema:
        payload = dict(values)
        payload.pop("schema_digest", None)
        payload.setdefault("schema_version", "belllabs.cognitive-context-schema.v1")
        payload.setdefault("subagent_slices", ())
        draft = cls.model_validate(
            {**payload, "schema_digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"schema_digest"})
        return cls(**complete, schema_digest=sha256_digest(complete))


class DeepAgentModelComponent(Contract):
    ref: ExactDefinitionRef
    provider: Literal["openai"]
    model_name: str = Field(min_length=1)
    settings: dict[str, object] = Field(default_factory=dict)

    @model_validator(mode="after")
    def exact_model_ref(self) -> DeepAgentModelComponent:
        if self.ref.kind.value != "model":
            raise ValueError("Deep Agent model component requires an exact model ref")
        return self


class DeepAgentMiddlewareComponent(Contract):
    ref: ExactDefinitionRef
    order: int = Field(ge=0)
    contributed_channels: tuple[CognitiveChannelDefinition, ...] = ()

    @model_validator(mode="after")
    def exact_middleware_ref(self) -> DeepAgentMiddlewareComponent:
        if self.ref.kind.value != "middleware":
            raise ValueError("Deep Agent middleware component requires an exact middleware ref")
        return self


class DeepAgentToolComponent(Contract):
    ref: ExactDefinitionRef
    tool_name: str = Field(min_length=1)
    schema_digest: str = Field(pattern=DIGEST_PATTERN)
    attachment_target: str = Field(min_length=1)


class DeepAgentSkillComponent(Contract):
    ref: ExactDefinitionRef
    skill_name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    bundle_digest: str = Field(pattern=DIGEST_PATTERN)
    skill_md_digest: str = Field(pattern=DIGEST_PATTERN)
    mount_root: str = Field(pattern=r"^/.*[^/]$")
    attachment_target: str = Field(min_length=1)

    @model_validator(mode="after")
    def exact_skill_ref(self) -> DeepAgentSkillComponent:
        if self.ref.kind.value != "skill":
            raise ValueError("Deep Agent skill component requires an exact Skill ref")
        return self


class DeepAgentMCPToolComponent(Contract):
    tool_name: str = Field(min_length=1)
    schema_digest: str = Field(pattern=DIGEST_PATTERN)


class DeepAgentMCPServerComponent(Contract):
    ref: ExactDefinitionRef
    server_name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    transport: Literal["stdio", "sse", "streamable_http"]
    endpoint: str | None = None
    command: str | None = None
    arguments: tuple[str, ...] = ()
    credential_refs: tuple[SecretRef, ...] = ()
    tools: tuple[DeepAgentMCPToolComponent, ...] = Field(min_length=1)
    schema_digest: str = Field(pattern=DIGEST_PATTERN)
    attachment_target: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_transport(self) -> DeepAgentMCPServerComponent:
        if self.ref.kind.value != "mcp_server":
            raise ValueError("Deep Agent MCP component requires an exact MCP server ref")
        if self.transport == "stdio":
            if not self.command or self.endpoint is not None:
                raise ValueError("stdio MCP binding requires only command/arguments")
        elif not self.endpoint or self.command is not None or self.arguments:
            raise ValueError("remote MCP binding requires only an endpoint")
        names = [tool.tool_name for tool in self.tools]
        if len(names) != len(set(names)):
            raise ValueError("MCP tool names must be unique per exact server binding")
        return self


class DeepAgentSandboxComponent(Contract):
    ref: ExactDefinitionRef
    backend: Literal["langsmith", "daytona", "docker", "state"]
    runtime_digest: str = Field(pattern=DIGEST_PATTERN)
    snapshot_ref: str | None = None
    credential_refs: tuple[SecretRef, ...] = ()
    idle_ttl_seconds: int = Field(default=300, ge=60, le=86_400)

    @model_validator(mode="after")
    def exact_sandbox_ref(self) -> DeepAgentSandboxComponent:
        if self.ref.kind.value != "sandbox_profile":
            raise ValueError("Deep Agent sandbox component requires an exact sandbox profile ref")
        return self


class SyncSubagentProfile(Contract):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    description: str = Field(min_length=1)
    system_prompt_ref: ExactDefinitionRef
    model: DeepAgentModelComponent
    tool_refs: tuple[ExactDefinitionRef, ...] = ()
    skill_refs: tuple[ExactDefinitionRef, ...] = ()
    state_slice_id: str = Field(min_length=1)
    context_slice_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    writable_paths: tuple[str, ...] = Field(min_length=1)
    budget_limits: dict[str, int] = Field(default_factory=dict)


class AsyncSubagentDependencyClass(StrEnum):
    REQUIRED_BLOCKING = "required_blocking"
    DEGRADABLE_BLOCKING = "degradable_blocking"
    NONBLOCKING = "nonblocking"
    ADVISORY = "advisory"


class AsyncSubagentLifecycle(StrEnum):
    PROPOSED = "proposed"
    ADMITTED = "admitted"
    SUBMITTED = "submitted"
    RUNNING = "running"
    WAITING = "waiting"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ORPHANED = "orphaned"


class AsyncSubagentContract(Contract):
    """Immutable BellLabs ceiling for one permitted background subordinate."""

    schema_version: Literal["belllabs.async-subagent-contract.v1"] = (
        "belllabs.async-subagent-contract.v1"
    )
    contract_id: str = Field(min_length=1)
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    description: str = Field(min_length=1)
    graph_id: str = Field(min_length=1)
    agent_protocol_url: str = Field(min_length=1)
    objective_schema_ref: str = Field(min_length=1)
    result_schema_ref: str = Field(min_length=1)
    context_slice_id: str = Field(min_length=1)
    state_slice_id: str = Field(min_length=1)
    capability_ceiling: CapabilityGrant
    authority_refs: tuple[str, ...] = Field(min_length=1)
    budget_limits: dict[str, int] = Field(min_length=1)
    dependency_classes: frozenset[AsyncSubagentDependencyClass] = Field(min_length=1)
    timeout_seconds: int = Field(ge=1, le=604_800)
    cancellation_propagation: Literal["required", "best_effort", "none"]
    late_result_policy: Literal["quarantine", "record_only", "eligible_if_parent_open"]
    fallback_policy: Literal["fail_parent", "degrade", "continue_without_result"]
    result_admission_policy_ref: str = Field(min_length=1)
    contract_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_digest(self, info: ValidationInfo) -> AsyncSubagentContract:
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"contract_digest"}))
            != self.contract_digest
        ):
            raise ValueError("async subagent contract digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> AsyncSubagentContract:
        payload = dict(values)
        payload.pop("contract_digest", None)
        payload.setdefault("schema_version", "belllabs.async-subagent-contract.v1")
        draft = cls.model_validate(
            {**payload, "contract_digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"contract_digest"})
        return cls(**complete, contract_digest=sha256_digest(complete))


class AsyncSubagentResultManifest(Contract):
    schema_version: Literal["belllabs.async-subagent-result.v1"] = (
        "belllabs.async-subagent-result.v1"
    )
    manifest_id: str = Field(min_length=1)
    child_execution_id: str = Field(min_length=1)
    execution_generation: int = Field(ge=1)
    output_refs: tuple[str, ...] = ()
    evidence_refs: tuple[str, ...] = ()
    usage_ref: str = Field(min_length=1)
    checkpoint_ref: str = Field(min_length=1)
    effect_refs: tuple[str, ...] = ()
    completed_at: AwareDatetime
    manifest_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_digest(self, info: ValidationInfo) -> AsyncSubagentResultManifest:
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"manifest_digest"}))
            != self.manifest_digest
        ):
            raise ValueError("async subagent result manifest digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> AsyncSubagentResultManifest:
        payload = dict(values)
        payload.pop("manifest_digest", None)
        payload.setdefault("schema_version", "belllabs.async-subagent-result.v1")
        draft = cls.model_validate(
            {**payload, "manifest_digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"manifest_digest"})
        return cls(**complete, manifest_digest=sha256_digest(complete))


class AsyncSubagentExecution(Contract):
    schema_version: Literal["belllabs.async-subagent-execution.v1"] = (
        "belllabs.async-subagent-execution.v1"
    )
    child_execution_id: str = Field(min_length=1)
    contract_id: str = Field(min_length=1)
    contract_digest: str = Field(pattern=DIGEST_PATTERN)
    parent_run_id: str = Field(min_length=1)
    parent_operation_id: str = Field(min_length=1)
    parent_binding_id: str = Field(min_length=1)
    execution_generation: int = Field(ge=1)
    objective_ref: str = Field(min_length=1)
    context_slice_ref: str = Field(min_length=1)
    reservation_id: str = Field(min_length=1)
    lifecycle: AsyncSubagentLifecycle
    provider_thread_id: str | None = Field(default=None, min_length=1)
    provider_run_id: str | None = Field(default=None, min_length=1)
    result_manifest: AsyncSubagentResultManifest | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def provider_binding_follows_submission(self) -> AsyncSubagentExecution:
        bound = self.provider_thread_id is not None and self.provider_run_id is not None
        if (self.provider_thread_id is None) != (self.provider_run_id is None):
            raise ValueError("provider thread and run identities bind together")
        if (
            self.lifecycle in {AsyncSubagentLifecycle.PROPOSED, AsyncSubagentLifecycle.ADMITTED}
            and bound
        ):
            raise ValueError("provider identity cannot precede BellLabs admission")
        if self.lifecycle == AsyncSubagentLifecycle.COMPLETED and self.result_manifest is None:
            raise ValueError("completed async child requires a typed result manifest")
        return self


class AsyncSubagentMessage(Contract):
    schema_version: Literal["belllabs.async-subagent-message.v1"] = (
        "belllabs.async-subagent-message.v1"
    )
    message_id: str = Field(min_length=1)
    child_execution_id: str = Field(min_length=1)
    direction: Literal["parent_to_child", "child_to_parent"]
    target_sequence: int = Field(ge=1)
    correlation_id: str = Field(min_length=1)
    causation_id: str | None = Field(default=None, min_length=1)
    payload_ref: str = Field(min_length=1)
    context_authority: Literal["instruction", "admitted_context", "untrusted_observation"]
    expires_at: AwareDatetime | None = None
    supersedes_message_id: str | None = Field(default=None, min_length=1)
    receipt: Literal[
        "accepted", "claimed", "provider_applied", "checkpoint_committed", "terminal_rejected"
    ] = "accepted"
    created_at: AwareDatetime


class ParentAsyncSubagentLink(Contract):
    schema_version: Literal["belllabs.parent-async-subagent-link.v1"] = (
        "belllabs.parent-async-subagent-link.v1"
    )
    link_id: str = Field(min_length=1)
    child_execution_id: str = Field(min_length=1)
    parent_run_id: str = Field(min_length=1)
    parent_operation_id: str = Field(min_length=1)
    dependency_class: AsyncSubagentDependencyClass
    timeout_at: AwareDatetime
    cancellation_propagation: Literal["required", "best_effort", "none"]
    late_result_policy: Literal["quarantine", "record_only", "eligible_if_parent_open"]
    fallback_policy: Literal["fail_parent", "degrade", "continue_without_result"]
    result_admission_policy_ref: str = Field(min_length=1)
    messages: tuple[AsyncSubagentMessage, ...] = ()
    cancellation_requested: bool = False
    cancellation_reason: str | None = None
    result_decision: Literal["admit", "conditionally_admit", "reject", "defer"] | None = None
    admitted_manifest_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    settled: bool = False
    created_at: AwareDatetime
    updated_at: AwareDatetime


class DeepAgentProfile(Contract):
    schema_version: Literal["belllabs.deep-agent-profile.v1"] = "belllabs.deep-agent-profile.v1"
    logical_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    revision: int = Field(ge=1)
    framework_family: Literal["deepagents"] = "deepagents"
    model: DeepAgentModelComponent
    structured_output_ref: ExactDefinitionRef | None = None
    prompt_refs: tuple[ExactDefinitionRef, ...] = Field(min_length=1)
    context_assembly_ref: ExactDefinitionRef
    middleware: tuple[DeepAgentMiddlewareComponent, ...] = ()
    backend_ref: ExactDefinitionRef
    store_ref: ExactDefinitionRef
    checkpointer_ref: ExactDefinitionRef
    tools: tuple[DeepAgentToolComponent, ...] = ()
    mcp_servers: tuple[DeepAgentMCPServerComponent, ...] = ()
    skills: tuple[DeepAgentSkillComponent, ...] = ()
    memory_refs: tuple[ExactDefinitionRef, ...] = ()
    sandbox: DeepAgentSandboxComponent
    sync_subagents: tuple[SyncSubagentProfile, ...] = ()
    async_subagents: tuple[AsyncSubagentContract, ...] = ()
    async_subagent_policy_refs: tuple[ExactDefinitionRef, ...] = ()
    delegation_ceiling: DelegationCeiling = Field(default_factory=DelegationCeiling)
    hitl_policy_ref: ExactDefinitionRef | None = None
    limits: dict[str, int] = Field(default_factory=dict)
    tracing_policy_ref: ExactDefinitionRef
    cognitive_state_pack_refs: tuple[CognitiveChannelPackRef, ...] = Field(min_length=1)
    cognitive_context_pack_refs: tuple[CognitiveChannelPackRef, ...] = Field(min_length=1)
    compatible_placement_ids: frozenset[str] = Field(min_length=1)
    profile_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_profile(self, info: ValidationInfo) -> DeepAgentProfile:
        orders = [item.order for item in self.middleware]
        if orders != sorted(orders) or len(orders) != len(set(orders)):
            raise ValueError("Deep Agent middleware order must be exact and collision-free")
        names = [item.name for item in self.sync_subagents]
        if len(names) != len(set(names)):
            raise ValueError("synchronous subagent names must be unique")
        async_names = [item.name for item in self.async_subagents]
        if len(async_names) != len(set(async_names)):
            raise ValueError("asynchronous subagent names must be unique")
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"profile_digest"}))
            != self.profile_digest
        ):
            raise ValueError("Deep Agent profile digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> DeepAgentProfile:
        payload = dict(values)
        payload.pop("profile_digest", None)
        payload.setdefault("schema_version", "belllabs.deep-agent-profile.v1")
        payload.setdefault("framework_family", "deepagents")
        draft = cls.model_validate(
            {**payload, "profile_digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"profile_digest"})
        return cls(**complete, profile_digest=sha256_digest(complete))


class DeepAgentExecutionPlacementProfile(Contract):
    schema_version: Literal["belllabs.deep-agent-placement.v1"] = "belllabs.deep-agent-placement.v1"
    logical_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    revision: int = Field(ge=1)
    placement: Literal["local_in_worker", "remote_langsmith_deployment"]
    deepagents_version: Literal["0.7.5"] = "0.7.5"
    python_runtime: str = Field(min_length=1)
    package_versions: dict[str, str] = Field(min_length=1)
    deployment_id: str | None = None
    task_queue: str = Field(min_length=1)
    checkpoint_behavior: Literal["local_checkpointer", "remote_managed"]
    cancellation_behavior: Literal["cooperative", "remote_reconcile"]
    streaming_behavior: Literal["state_updates", "messages"]
    message_injection_behavior: Literal["invoke_only", "remote_thread"]
    reconnect_behavior: Literal["checkpoint_resume", "remote_run_reconnect"]
    sandbox_backends: frozenset[Literal["langsmith", "daytona", "docker", "state"]]
    trace_behavior: Literal["langsmith"] = "langsmith"
    qualification_refs: tuple[str, ...] = Field(min_length=1)
    silent_fallback: Literal[False] = False
    placement_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_placement(self, info: ValidationInfo) -> DeepAgentExecutionPlacementProfile:
        if self.placement == "local_in_worker" and self.deployment_id is not None:
            raise ValueError("local Deep Agent placement cannot declare a remote deployment")
        if self.placement == "remote_langsmith_deployment" and not self.deployment_id:
            raise ValueError("remote Deep Agent placement requires an exact deployment identity")
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"placement_digest"}))
            != self.placement_digest
        ):
            raise ValueError("Deep Agent placement digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> DeepAgentExecutionPlacementProfile:
        payload = dict(values)
        payload.pop("placement_digest", None)
        payload.setdefault("schema_version", "belllabs.deep-agent-placement.v1")
        payload.setdefault("deepagents_version", "0.7.5")
        payload.setdefault("trace_behavior", "langsmith")
        payload.setdefault("silent_fallback", False)
        draft = cls.model_validate(
            {**payload, "placement_digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"placement_digest"})
        return cls(**complete, placement_digest=sha256_digest(complete))


class DeepAgentAttachmentRecord(Contract):
    component_kind: Literal["model", "middleware", "tool", "mcp", "skill", "sandbox"]
    component_digest: str = Field(pattern=DIGEST_PATTERN)
    attachment_target: str = Field(min_length=1)
    status: Literal["intended", "resolved"] = "intended"


class DeepAgentExecutionBinding(Contract):
    schema_version: Literal["belllabs.deep-agent-binding.v1"] = "belllabs.deep-agent-binding.v1"
    binding_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    operation_id: str = Field(min_length=1)
    operation_attempt: int = Field(ge=1)
    execution_generation: int = Field(ge=1)
    erc_digest: str = Field(pattern=DIGEST_PATTERN)
    control_revision: int = Field(ge=1)
    profile_id: str = Field(min_length=1)
    profile_revision: int = Field(ge=1)
    profile_digest: str = Field(pattern=DIGEST_PATTERN)
    placement_id: str = Field(min_length=1)
    placement_revision: int = Field(ge=1)
    placement_digest: str = Field(pattern=DIGEST_PATTERN)
    placement: Literal["local_in_worker", "remote_langsmith_deployment"]
    task_queue: str = Field(min_length=1)
    checkpoint_behavior: Literal["local_checkpointer", "remote_managed"]
    cancellation_behavior: Literal["cooperative", "remote_reconcile"]
    streaming_behavior: Literal["state_updates", "messages"]
    message_injection_behavior: Literal["invoke_only", "remote_thread"]
    reconnect_behavior: Literal["checkpoint_resume", "remote_run_reconnect"]
    model: DeepAgentModelComponent
    backend_ref: ExactDefinitionRef
    store_ref: ExactDefinitionRef
    checkpointer_ref: ExactDefinitionRef
    middleware: tuple[DeepAgentMiddlewareComponent, ...] = ()
    tools: tuple[DeepAgentToolComponent, ...] = ()
    mcp_servers: tuple[DeepAgentMCPServerComponent, ...] = ()
    skills: tuple[DeepAgentSkillComponent, ...] = ()
    sandbox: DeepAgentSandboxComponent
    sync_subagents: tuple[SyncSubagentProfile, ...] = ()
    async_subagents: tuple[AsyncSubagentContract, ...] = ()
    cognitive_state_schema: CognitiveStateSchema
    cognitive_context_schema: CognitiveRuntimeContextSchema
    cognitive_context_values: dict[str, object]
    initial_artifact_index: dict[str, object] = Field(default_factory=dict)
    initial_context_manifest: dict[str, object]
    initial_child_result_index: tuple[dict[str, object], ...] = ()
    workspace: WorkspaceContract
    capability_grant: CapabilityGrant
    reservation_id: str = Field(min_length=1)
    authority_refs: tuple[str, ...] = Field(min_length=1)
    redaction_policy_ref: str = Field(min_length=1)
    package_versions: dict[str, str] = Field(min_length=1)
    intended_attachments: tuple[DeepAgentAttachmentRecord, ...] = Field(min_length=1)
    applied_degradations: tuple[str, ...] = ()
    silent_fallback: Literal[False] = False
    binding_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_binding(self, info: ValidationInfo) -> DeepAgentExecutionBinding:
        context_fields = {field.name: field for field in self.cognitive_context_schema.fields}
        if set(self.cognitive_context_values) != set(context_fields):
            raise ValueError("Deep Agent context values do not exactly match the frozen schema")
        for name, field in context_fields.items():
            value = self.cognitive_context_values[name]
            if (
                field.reference_only
                and isinstance(value, str)
                and not (value.startswith("ref:") or value.startswith("handle:"))
            ):
                raise ValueError("reference-only cognitive context value contains material")
        contributed = {
            channel.name: channel
            for component in self.middleware
            for channel in component.contributed_channels
        }
        effective = {channel.name: channel for channel in self.cognitive_state_schema.channels}
        if any(effective.get(name) != channel for name, channel in contributed.items()):
            raise ValueError("middleware cognitive channels drift from the frozen state schema")
        if self.skills and not {"skills_metadata", "skills_load_errors"} <= set(effective):
            raise ValueError("Skills middleware channels are absent from the frozen state schema")
        attachment_keys = [
            (item.component_kind, item.component_digest, item.attachment_target)
            for item in self.intended_attachments
        ]
        if len(attachment_keys) != len(set(attachment_keys)):
            raise ValueError("Deep Agent attachment collision")
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"binding_digest"}))
            != self.binding_digest
        ):
            raise ValueError("Deep Agent execution binding digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> DeepAgentExecutionBinding:
        payload = dict(values)
        payload.pop("binding_digest", None)
        payload.setdefault("schema_version", "belllabs.deep-agent-binding.v1")
        payload.setdefault("silent_fallback", False)
        draft = cls.model_validate(
            {**payload, "binding_digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"binding_digest"})
        return cls(**complete, binding_digest=sha256_digest(complete))


class NativeOperationExecutionPlacement(Contract):
    """Content-addressed placement authority for a native operation activity."""

    schema_version: Literal["belllabs.native-operation-placement.v1"] = (
        "belllabs.native-operation-placement.v1"
    )
    placement_id: str = Field(
        max_length=MAX_NATIVE_PLACEMENT_ID_LENGTH,
        pattern=r"^[a-z0-9][a-z0-9._:-]*$",
    )
    revision: int = Field(ge=1)
    execution_runtime: Literal["native"] = "native"
    task_queue: str = Field(
        min_length=1,
        max_length=MAX_TEMPORAL_TASK_QUEUE_LENGTH,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._/-]*$",
    )
    qualification_refs: tuple[str, ...] = Field(min_length=1, max_length=64)
    placement_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def validate_digest(self, info: ValidationInfo) -> NativeOperationExecutionPlacement:
        if (
            not (info.context or {}).get("allow_placeholder_digest")
            and sha256_digest(self.model_dump(mode="python", exclude={"placement_digest"}))
            != self.placement_digest
        ):
            raise ValueError("native operation placement digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> NativeOperationExecutionPlacement:
        payload = dict(values)
        payload.pop("placement_digest", None)
        payload.setdefault("schema_version", "belllabs.native-operation-placement.v1")
        payload.setdefault("execution_runtime", "native")
        draft = cls.model_validate(
            {**payload, "placement_digest": PLACEHOLDER_DIGEST},
            context={"allow_placeholder_digest": True},
        )
        complete = draft.model_dump(mode="python", exclude={"placement_digest"})
        return cls(**complete, placement_digest=sha256_digest(complete))


class OperationExecutionRequest(Contract):
    identity: OperationAttemptIdentity
    request_scope: str = Field(min_length=1)
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    run_control_revision: int = Field(ge=1)
    operation_contract_ref: str = Field(min_length=1)
    prompt_segments: tuple[PromptSegment, ...] = Field(min_length=1, max_length=64)
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
    execution_runtime: Literal["native", "deep_agent"] = "native"
    native_placement: NativeOperationExecutionPlacement | None = None
    deep_agent_binding: DeepAgentExecutionBinding | None = None
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
        if (self.execution_runtime == "deep_agent") != (self.deep_agent_binding is not None):
            raise ValueError(
                "deep_agent execution requires exactly one canonical Deep Agent binding"
            )
        if (self.execution_runtime == "native") != (self.native_placement is not None):
            raise ValueError(
                "native execution requires exactly one canonical native placement"
            )
        if self.deep_agent_binding is not None:
            deep_binding = self.deep_agent_binding
            if (
                deep_binding.run_id != self.identity.run_id
                or deep_binding.operation_id != self.identity.operation_id
                or deep_binding.operation_attempt != self.identity.operation_attempt
                or deep_binding.erc_digest != self.effective_configuration_digest
                or deep_binding.control_revision != self.run_control_revision
                or deep_binding.workspace != self.workspace
                or deep_binding.capability_grant != self.capability_grant
                or deep_binding.reservation_id != self.budget_reservation_id
            ):
                raise ValueError(
                    "Deep Agent binding does not match the operation authority envelope"
                )
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
    execution_runtime: Literal["native", "deep_agent"] = "native"
    native_placement: NativeOperationExecutionPlacement | None = None
    deep_agent_binding: DeepAgentExecutionBinding | None = None
    side_effect_key: str
    bound_at: AwareDatetime

    @model_validator(mode="after")
    def exact_runtime_placement(self) -> OperationExecutionBinding:
        if (self.execution_runtime == "deep_agent") != (self.deep_agent_binding is not None):
            raise ValueError("Deep Agent binding runtime placement is not exact")
        if (self.execution_runtime == "native") != (self.native_placement is not None):
            raise ValueError("native binding runtime placement is not exact")
        return self


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


class OperationWorkflowRequest(Contract):
    """Typed durable wrapper for exactly one semantic operation attempt."""

    schema_version: Literal["belllabs.operation-workflow.v2"] = "belllabs.operation-workflow.v2"
    semantic_attempt_id: str = Field(
        min_length=1,
        max_length=MAX_OPERATION_SEMANTIC_ATTEMPT_ID_LENGTH,
    )
    execution_generation: int = Field(default=1, ge=1)
    operation_kind: Literal["bound_operation"]
    operation: OperationExecutionRequest
    timeout_seconds: int = Field(default=300, ge=1, le=86_400)
    message_cursor: int = Field(default=0, ge=0)
    effect_frontier: tuple[EffectFrontierItem, ...] = Field(
        default=(),
        max_length=MAX_EFFECT_FRONTIER_ITEMS,
    )
    active_async_child_ids: tuple[AsyncChildId, ...] = Field(
        default=(),
        max_length=MAX_ACTIVE_ASYNC_CHILDREN,
    )

    @model_validator(mode="after")
    def exact_bound_operation(self) -> OperationWorkflowRequest:
        if self.semantic_attempt_id != self.operation.identity.semantic_key:
            raise ValueError(
                "operation workflow semantic attempt must match the bound execution request"
            )
        binding = self.operation.deep_agent_binding
        if (
            binding is not None
            and self.execution_generation != binding.execution_generation
        ):
            raise ValueError(
                "operation workflow generation must match the exact Deep Agent binding"
            )
        if len(set(self.active_async_child_ids)) != len(self.active_async_child_ids):
            raise ValueError("active async child identities must be unique")
        if (
            len(self.model_dump_json().encode("utf-8"))
            > MAX_OPERATION_WORKFLOW_PAYLOAD_BYTES
        ):
            raise ValueError("operation workflow payload exceeds 2,000,000 bytes")
        return self

    @property
    def activity_task_queue(self) -> str:
        binding = self.operation.deep_agent_binding
        if binding is not None:
            return binding.task_queue
        placement = self.operation.native_placement
        if placement is None:
            raise ValueError("operation execution has no exact activity placement")
        return placement.task_queue

    @property
    def workflow_id(self) -> str:
        return f"operation/{self.semantic_attempt_id}"


class OperationWorkflowResult(Contract):
    schema_version: Literal["belllabs.operation-result.v1"] = "belllabs.operation-result.v1"
    semantic_attempt_id: str
    execution_generation: int = Field(ge=1)
    disposition: Literal["completed", "cancelled", "failed", "in_doubt"]
    result: dict[str, object] = Field(default_factory=dict)
    message_cursor: int = Field(ge=0)
    effect_frontier: tuple[EffectFrontierItem, ...] = Field(
        default=(),
        max_length=MAX_EFFECT_FRONTIER_ITEMS,
    )
    active_async_child_ids: tuple[AsyncChildId, ...] = Field(
        default=(),
        max_length=MAX_ACTIVE_ASYNC_CHILDREN,
    )


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
