from __future__ import annotations

from enum import StrEnum
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from mission_control.domain.authoring.contracts import CatalogPayloadRef, ExactDefinitionRef
from mission_control.domain.capabilities.hooks import HookEvent, HookInterpreter
from mission_control.domain.capabilities.host_support import LaneProfile
from mission_control.domain.capabilities.subagents import SubagentProfile


class HarnessContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AgentHost(StrEnum):
    CURSOR = "cursor"
    CODEX = "codex"
    CLAUDE_CODE = "claude_code"
    AGENT_FRAMEWORK = "agent_framework"

    @property
    def lane_profile(self) -> LaneProfile:
        """The lane profile whose native files this Agent Host reads (SPEC-01)."""
        return HOST_LANE_PROFILE[self]


HOST_LANE_PROFILE: dict[AgentHost, LaneProfile] = {
    AgentHost.CURSOR: LaneProfile.CURSOR_LOCAL,
    AgentHost.CODEX: LaneProfile.CODEX,
    AgentHost.CLAUDE_CODE: LaneProfile.CLAUDE_AGENT_SDK,
    AgentHost.AGENT_FRAMEWORK: LaneProfile.DEEP_AGENTS,
}


class ComponentKind(StrEnum):
    PLUGIN = "plugin"
    MCP_SERVER = "mcp_server"
    SKILL = "skill"
    AGENT_COMPONENT = "agent_component"
    SANDBOX_SNAPSHOT = "sandbox_snapshot"
    WORKSPACE_SETUP = "workspace_setup"
    DIFF_CODEC = "diff_codec"
    HOOK_SCRIPT = "hook_script"
    SUBAGENT_PROFILE = "subagent_profile"


class TrustStage(StrEnum):
    QUARANTINED = "quarantined"
    REVIEWED = "reviewed"
    QUALIFIED = "qualified"
    ACCEPTED = "accepted"


class OperatingSystem(StrEnum):
    LINUX = "linux"
    WINDOWS = "windows"
    MACOS = "macos"


class Architecture(StrEnum):
    AMD64 = "amd64"
    ARM64 = "arm64"


class DiffCodec(StrEnum):
    V4A_FREEFORM = "v4a_freeform"
    V4A_STRUCTURED = "v4a_structured"
    OPERATIONS_STRUCTURED = "operations_structured"
    EXACT_STRING_EDIT = "exact_string_edit"


class ComponentCoordinate(HarnessContract):
    component_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._:/-]*$")
    version: str = Field(min_length=1, max_length=128)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class ComponentDescription(HarnessContract):
    title: str = Field(min_length=1)
    summary: str = Field(min_length=1)
    capabilities: frozenset[str] = Field(default_factory=frozenset)
    tags: frozenset[str] = Field(default_factory=frozenset)
    biotech_domains: frozenset[str] = Field(default_factory=frozenset)
    limitations: tuple[str, ...] = ()


class SourcePin(HarnessContract):
    registry: Literal[
        "belllabs",
        "mcp_official",
        "smithery",
        "skills_sh",
        "npm",
        "pypi",
        "oci",
        "git",
    ]
    locator: str = Field(min_length=1)
    upstream_identity: str = Field(min_length=1)
    upstream_version: str = Field(min_length=1)
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    evidence_ref: str = Field(min_length=1)

    @field_validator("locator")
    @classmethod
    def locator_has_no_credentials(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("source locators cannot contain credentials")
        return value


class PayloadPlacement(HarnessContract):
    payload_ref: CatalogPayloadRef
    target_path: str = Field(min_length=1)
    executable: bool = False

    @field_validator("target_path")
    @classmethod
    def target_is_virtual_relative_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or ".." in normalized.split("/"):
            raise ValueError("payload target paths must be workspace-relative")
        return normalized


class RuntimeRequirement(HarnessContract):
    executable: str = Field(pattern=r"^[A-Za-z0-9_.+-]+$")
    version_constraint: str = Field(min_length=1)
    version_arguments: tuple[str, ...] = ("--version",)
    required_at: Literal["materialization", "startup", "tool_call"] = "startup"


class HostCompatibility(HarnessContract):
    host: AgentHost
    adapter_version: str = Field(min_length=1)
    operating_systems: frozenset[OperatingSystem] = Field(min_length=1)
    architectures: frozenset[Architecture] = Field(min_length=1)
    project_config_path: str = Field(min_length=1)
    skill_root: str | None = None

    @field_validator("project_config_path", "skill_root")
    @classmethod
    def paths_are_relative(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or ".." in normalized.split("/"):
            raise ValueError("host projection paths must be project-relative")
        return normalized


class ReadinessProbe(HarnessContract):
    kind: Literal[
        "mcp_initialize_and_list_tools",
        "http_health",
        "command_exit_zero",
        "skill_manifest_load",
        "filesystem_assertion",
    ]
    timeout_seconds: int = Field(ge=1, le=300)
    attempts: int = Field(default=1, ge=1, le=10)
    command: tuple[str, ...] = ()
    url: str | None = None
    expected_schema_digest: str | None = Field(
        default=None,
        pattern=r"^sha256:[0-9a-f]{64}$",
    )

    @model_validator(mode="after")
    def probe_shape_matches_kind(self) -> ReadinessProbe:
        if self.kind == "http_health" and self.url is None:
            raise ValueError("HTTP readiness probes require a URL")
        if self.kind == "command_exit_zero" and not self.command:
            raise ValueError("command readiness probes require command tokens")
        if self.kind == "mcp_initialize_and_list_tools" and self.expected_schema_digest is None:
            raise ValueError("MCP readiness probes require an expected schema digest")
        return self


class SecretEnvironmentBinding(HarnessContract):
    environment_name: str = Field(pattern=r"^[A-Z][A-Z0-9_]*$")
    secret_ref: str = Field(min_length=1)
    required: bool = True


class MCPRuntimeBinding(HarnessContract):
    server_name: str = Field(pattern=r"^[a-zA-Z0-9_-]+$")
    transport: Literal["stdio", "streamable_http", "sse"]
    command: str | None = None
    arguments: tuple[str, ...] = ()
    url: str | None = None
    working_directory: str | None = None
    secret_environment: tuple[SecretEnvironmentBinding, ...] = ()
    allowed_tools: frozenset[str] = Field(min_length=1)
    schema_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    readiness_probe: ReadinessProbe

    @model_validator(mode="after")
    def transport_has_one_launch_mode(self) -> MCPRuntimeBinding:
        if self.transport == "stdio":
            if not self.command or self.url is not None:
                raise ValueError("stdio MCP bindings require only a command")
        elif not self.url or self.command is not None or self.arguments:
            raise ValueError("remote MCP bindings require only a URL")
        if self.readiness_probe.expected_schema_digest not in {None, self.schema_digest}:
            raise ValueError("MCP readiness schema digest must match the runtime binding")
        names = [item.environment_name for item in self.secret_environment]
        if len(names) != len(set(names)):
            raise ValueError("MCP secret environment names must be unique")
        return self


class SkillLoadBinding(HarnessContract):
    skill_name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    manifest_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    required_executables: frozenset[str] = Field(default_factory=frozenset)
    readiness_probe: ReadinessProbe

    @model_validator(mode="after")
    def uses_skill_probe(self) -> SkillLoadBinding:
        if self.readiness_probe.kind != "skill_manifest_load":
            raise ValueError("skills require a skill_manifest_load readiness probe")
        return self


class SandboxSnapshotPreview(HarnessContract):
    snapshot_id: str = Field(pattern=r"^sandbox:[a-z0-9][a-z0-9._:-]*$")
    snapshot_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    operating_system: OperatingSystem
    architecture: Architecture
    runtimes: dict[str, str] = Field(default_factory=dict)
    executables: dict[str, str] = Field(default_factory=dict)
    network_policy: str = Field(min_length=1)
    filesystem_policy: str = Field(min_length=1)
    image_ref: str | None = None
    provisioning_recipe_ref: CatalogPayloadRef
    preview_ref: CatalogPayloadRef


class WorkspaceMount(HarnessContract):
    source: str = Field(min_length=1)
    target: str = Field(min_length=1)
    mode: Literal["read_only", "read_write", "copy_on_write"]


class WorkspaceSetup(HarnessContract):
    setup_id: str = Field(pattern=r"^workspace:[a-z0-9][a-z0-9._:-]*$")
    description: str = Field(min_length=1)
    sandbox_snapshot_id: str = Field(pattern=r"^sandbox:[a-z0-9][a-z0-9._:-]*$")
    mounts: tuple[WorkspaceMount, ...] = ()
    initialization_commands: tuple[tuple[str, ...], ...] = ()
    required_environment: frozenset[str] = Field(default_factory=frozenset)
    verification_commands: tuple[tuple[str, ...], ...] = ()


class DiffCodecMetrics(HarnessContract):
    syntax_validity: float = Field(ge=0, le=1)
    exact_apply_rate: float = Field(ge=0, le=1)
    stale_context_recovery: float = Field(ge=0, le=1)
    unintended_change_rate: float = Field(ge=0, le=1)
    median_latency_ms: int = Field(ge=0)
    median_output_tokens: int = Field(ge=0)


class DiffCodecQualification(HarnessContract):
    qualification_id: str = Field(pattern=r"^diff-qualification:[a-z0-9][a-z0-9._:-]*$")
    model_pattern: str = Field(min_length=1)
    codec: DiffCodec
    engine_version: str = Field(min_length=1)
    evaluation_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    metrics: DiffCodecMetrics
    minimum_exact_apply_rate: float = Field(ge=0, le=1)
    maximum_unintended_change_rate: float = Field(ge=0, le=1)
    promoted: bool = False

    @property
    def passes_gate(self) -> bool:
        return (
            self.metrics.exact_apply_rate >= self.minimum_exact_apply_rate
            and self.metrics.unintended_change_rate <= self.maximum_unintended_change_rate
        )


class AgentComponentBinding(HarnessContract):
    framework: str = Field(min_length=1)
    framework_version: str = Field(min_length=1)
    import_path: str = Field(min_length=1)
    factory_symbol: str = Field(min_length=1)
    metadata_ref: CatalogPayloadRef


class HookScriptBinding(HarnessContract):
    hook_id: str = Field(min_length=1)
    events: tuple[HookEvent, ...] = Field(min_length=1)
    interpreter: HookInterpreter
    entrypoint: str = Field(min_length=1)
    timeout_seconds: int = Field(default=30, ge=1, le=600)
    fail_closed: bool = False
    matcher: str | None = None


class PluginBinding(HarnessContract):
    """A plugin release expands into exact member releases in position order."""

    members: tuple[ComponentCoordinate, ...] = Field(min_length=1)
    optional_digests: frozenset[str] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def members_are_unique(self) -> PluginBinding:
        digests = [member.digest for member in self.members]
        if len(digests) != len(set(digests)):
            raise ValueError("plugin member releases must be unique")
        if not self.optional_digests <= set(digests):
            raise ValueError("optional plugin members must be members")
        return self


class AgenticComponentRelease(HarnessContract):
    coordinate: ComponentCoordinate
    kind: ComponentKind
    description: ComponentDescription
    definition_ref: ExactDefinitionRef | None = None
    source: SourcePin
    payloads: tuple[PayloadPlacement, ...] = ()
    runtime_requirements: tuple[RuntimeRequirement, ...] = ()
    compatibility: tuple[HostCompatibility, ...] = ()
    trust_stage: TrustStage
    license: str | None = None
    mcp: MCPRuntimeBinding | None = None
    skill: SkillLoadBinding | None = None
    sandbox: SandboxSnapshotPreview | None = None
    workspace: WorkspaceSetup | None = None
    diff_qualification: DiffCodecQualification | None = None
    agent_component: AgentComponentBinding | None = None
    hook_script: HookScriptBinding | None = None
    subagent_profile: SubagentProfile | None = None
    plugin: PluginBinding | None = None

    @model_validator(mode="after")
    def exactly_one_kind_binding(self) -> AgenticComponentRelease:
        bindings = {
            ComponentKind.MCP_SERVER: self.mcp,
            ComponentKind.SKILL: self.skill,
            ComponentKind.SANDBOX_SNAPSHOT: self.sandbox,
            ComponentKind.WORKSPACE_SETUP: self.workspace,
            ComponentKind.DIFF_CODEC: self.diff_qualification,
            ComponentKind.AGENT_COMPONENT: self.agent_component,
            ComponentKind.HOOK_SCRIPT: self.hook_script,
            ComponentKind.SUBAGENT_PROFILE: self.subagent_profile,
            ComponentKind.PLUGIN: self.plugin,
        }
        populated = [kind for kind, value in bindings.items() if value is not None]
        if self.kind in bindings and bindings[self.kind] is None:
            raise ValueError(f"{self.kind.value} releases require their typed binding")
        if any(kind != self.kind for kind in populated):
            raise ValueError("component release contains a binding for another component kind")
        if (
            self.kind in {ComponentKind.MCP_SERVER, ComponentKind.SKILL}
            and self.definition_ref is None
        ):
            raise ValueError("governed MCP and skill releases require an exact definition ref")
        return self


class ComponentQuery(HarnessContract):
    text: str | None = None
    kinds: frozenset[ComponentKind] = Field(default_factory=frozenset)
    host: AgentHost | None = None
    operating_system: OperatingSystem | None = None
    architecture: Architecture | None = None
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    minimum_trust_stage: TrustStage = TrustStage.REVIEWED
    limit: int = Field(default=20, ge=1, le=100)


class MaterializationRequest(HarnessContract):
    request_id: str = Field(pattern=r"^materialize:[a-z0-9][a-z0-9._:-]*$")
    component_digests: tuple[str, ...] = Field(min_length=1)
    host: AgentHost
    operating_system: OperatingSystem
    architecture: Architecture
    workspace_root: str = Field(min_length=1)
    model_id: str | None = None
    dry_run: bool = False


class GeneratedFile(HarnessContract):
    path: str = Field(min_length=1)
    media_type: str = Field(min_length=1)
    content: str
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class MaterializationStep(HarnessContract):
    ordinal: int = Field(ge=1)
    kind: Literal[
        "retrieve",
        "verify",
        "stage",
        "inject_secrets",
        "configure_host",
        "provision_workspace",
        "start_server",
        "probe_readiness",
        "seal_snapshot",
    ]
    component_digest: str | None = None
    description: str = Field(min_length=1)
    command: tuple[str, ...] = ()
    secret_refs: tuple[str, ...] = ()


class MaterializationPlan(HarnessContract):
    plan_id: str = Field(pattern=r"^plan:sha256:[0-9a-f]{64}$")
    request: MaterializationRequest
    releases: tuple[ComponentCoordinate, ...]
    generated_files: tuple[GeneratedFile, ...]
    steps: tuple[MaterializationStep, ...]
    selected_diff_qualification: DiffCodecQualification | None = None

    @model_validator(mode="after")
    def steps_are_contiguous(self) -> MaterializationPlan:
        if [step.ordinal for step in self.steps] != list(range(1, len(self.steps) + 1)):
            raise ValueError("materialization step ordinals must be contiguous")
        return self


class ReadinessReceipt(HarnessContract):
    plan_id: str
    component_digest: str
    status: Literal["ready", "failed", "quarantined"]
    observed_schema_digest: str | None = None
    checked_at: str
    evidence_ref: CatalogPayloadRef
