"""Production launch inputs of a manifest run (MP-02, SPEC-05 "Mapping", ADR-0029).

Lowering pins every resolved capability by exact ref and leaves the lane execution binding to
the semantic input binding at launch. :class:`ManifestLaunchInputAuthor` is that binding's
production author: from the admitted Effective Run Configuration and the run's committed
``MissionDefinition@1`` it resolves, per lowered stage or Goal Loop role, the effective node
environment against the deployment's operator-reviewed ``mc.manifest_launch_bindings.v1``
(Deep Agent profile scaffold and placement, model and sandbox profile components, catalog
capability components) and the components the deployment actually serves. Nothing is
fabricated: a lane, model, sandbox, credential or capability without an exact deployment
binding fails before any provider dispatch with :class:`ManifestLaunchBindingError`, which
names the manifest pointer that selected it. Credentials are ``SecretRef`` names only.

Templates persist once under ``semantic-input:manifest:{run_id}`` in the immutable template
stores; a later call (a duplicate chain delivery, a retried start) reuses the frozen templates
and never authors different ones.

MP-02 for the local provider lanes: a ``claude_agent_sdk`` or ``codex`` stage or Goal Loop role
of a ``mission/v2`` definition resolves against the ``providers`` section of
``mc.manifest_launch_bindings.v2`` (``provider_launch``) into a sealed ``mc.execution_binding.v2``
on a template whose ``execution_runtime``/``lane_profile``/``provider_binding`` route it to the
lane's task queue. Hosted profiles (``claude_cloud``, ``codex_cloud``) are refused at their lane
pointer (evidence-blocked, MP-18/19).

The Cursor lanes: a ``cursor_local`` or ``cursor_cloud`` stage or Goal Loop role of a
``mission/v1`` or ``mission/v2`` definition resolves against ``providers.cursor_local`` /
``providers.cursor_cloud`` (``cursor_launch``) into a sealed ``mc.cursor_binding.v1`` routed to
the lane's task queue, pinning the projection digests the Cursor harness re-renders at
``prepare``. What that binding cannot carry (hook scripts, plugins, other kinds, model settings)
is refused at its pointer.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mission_control.application.authoring.cursor_launch import (
    CURSOR_PROFILES,
    CURSOR_PROJECTED_KINDS,
    CursorCloudLane,
    CursorLocalLane,
    cursor_projection,
    cursor_projection_digests,
    cursor_rows_order,
)
from mission_control.application.authoring.manifest_submit import (
    LaunchInputPort,
    ManifestStartUnavailable,
)
from mission_control.application.authoring.provider_launch import (
    HOSTED_PROVIDER_PROFILES,
    LAUNCH_BINDINGS_SCHEMA_V2,
    LOCAL_PROVIDER_PROFILES,
    MODEL_PROVIDER,
    ProjectionRowsPort,
    ProviderLaneBinding,
    ProviderLanes,
    WorkspaceProvision,
    binding_capability_pins,
    capability_pin_key,
    materialization_digest,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.chains.relay import ChainIntent
from mission_control.application.execution.approvals_coverage import (
    GateCoverageRequirement,
    admit_gate_coverage,
)
from mission_control.application.execution.harness.describe import declared_matrix
from mission_control.application.execution.service import RunControlService
from mission_control.application.programs.human_gates import (
    DEFAULT_GOAL_REVIEWERS,
    goal_human_review,
    stagegraph_human_gates,
)
from mission_control.application.programs.service import (
    GoalDirectedLaunchService,
    StageGraphLaunchService,
    WorkflowLaunchDispatcher,
)
from mission_control.application.programs.stage_inputs import stagegraph_authored_inputs
from mission_control.application.workspaces.errors import WorkspaceError
from mission_control.application.workspaces.policy import admit_policy, policy_pin
from mission_control.domain.agentic_components.projection import ResolvedCapability
from mission_control.domain.authoring.canonical import (
    sha256_digest,
    stable_json_digest,
    stable_json_dump,
)
from mission_control.domain.authoring.contracts import (
    AuthorityCeiling,
    DefinitionKind,
    EffectiveRunConfiguration,
    ExactDefinitionRef,
    GoalDirectedBlueprint,
    SecretRef,
    StageGraphBlueprint,
    WorkflowWorkspaceContract,
)
from mission_control.domain.authoring.manifest import Behavior
from mission_control.domain.authoring.manifest_v2 import ContinuationPolicy, RequiredFeatures
from mission_control.domain.authoring.mission_definition import (
    DefinitionCapability,
    DefinitionNode,
    EnvironmentSelections,
    MissionDefinition,
)
from mission_control.domain.execution.bindings import (
    BindingBudgets,
    ClaudeSdkOptions,
    CodexAppServerOptions,
    ModelPin,
    ProviderExecutionBinding,
    WorkflowRequirements,
)
from mission_control.domain.execution.contracts import (
    CapabilityGrant,
    CognitiveRuntimeContextSchema,
    CognitiveStateSchema,
    DeepAgentExecutionPlacementProfile,
    DeepAgentMCPServerComponent,
    DeepAgentModelComponent,
    DeepAgentProfile,
    DeepAgentSandboxComponent,
    DeepAgentSkillComponent,
    DeepAgentToolComponent,
    ModelPolicy,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    PromptSegment,
    PromptTrustClass,
    StructuredOutputBinding,
    WorkspaceContract,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from mission_control.domain.execution.lane_requirements import RequirementSet, admit_requirements
from mission_control.domain.execution.lanes import (
    LANE_OF_PROFILE,
    RUNTIME_OF_LANE,
    CursorBudgets,
    CursorCloudOptions,
    CursorExecutionBinding,
    CursorProjections,
    CursorWorkspace,
)
from mission_control.domain.execution.materialization import compile_deep_agent_execution_binding
from mission_control.domain.programs.contracts import StageAuthoredInput
from mission_control.domain.programs.human_gate import HumanGateSpec

LAUNCH_BINDINGS_SCHEMA: Literal["mc.manifest_launch_bindings.v1"] = "mc.manifest_launch_bindings.v1"
DEEP_AGENTS_LANE = "deep_agents"
PROVIDER_PROFILES = frozenset((*LOCAL_PROVIDER_PROFILES, *HOSTED_PROVIDER_PROFILES))
CURSOR_LANE_PROFILES = frozenset(CURSOR_PROFILES)
# Manifest capability kinds a Cursor binding can project (`cursor_launch.CURSOR_PROJECTED_KINDS`).
_CURSOR_MANIFEST_KINDS = frozenset({"skill_bundle", "mcp_server", "subagent_profile"})
GOAL_ROLES: tuple[Literal["executor", "verifier"], ...] = ("executor", "verifier")
TEMPLATE_REVISION = 1
"""Templates are frozen per run; the runtime rebinds revision, identity and reservation."""
_CAPABILITY_GROUPS = (
    "capabilities",
    "workspace.skills",
    "workspace.context",
    "agents",
    "hooks",
    "plugins",
)
_MODEL_SETTINGS: dict[str, str] = {
    "temperature": "temperature",
    "reasoning.effort": "reasoning_effort",
    "verbosity": "verbosity",
    "max_output_tokens": "max_completion_tokens",
    "service_tier": "service_tier",
}
_REASONING = ("minimal", "low", "medium", "high")
_VERBOSITY = ("low", "medium", "high")


def binding_ref(run_id: str) -> str:
    """The semantic input binding of a manifest run (one per run, frozen at first launch)."""

    return f"semantic-input:manifest:{run_id}"


def template_key(stage_id: str, slot_id: str, variant_id: str) -> str:
    """The StageGraph template key the stage service resolves an operation slot by."""

    return f"{stage_id}/{slot_id}/{variant_id}"


class ManifestLaunchBindingError(ManifestStartUnavailable):
    """A launch input the manifest selected has no exact deployment binding."""

    code = "launch_binding_unavailable"

    def __init__(self, pointer: str, message: str) -> None:
        self.pointer = pointer
        self.reason = message
        super().__init__(f"{pointer}: {message}")


# --- The deployment file (`mc.manifest_launch_bindings.v1`) -------------------------------


class _File(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DeepAgentScaffold(_File):
    """The reviewed profile and placement a manifest node's components are bound into.

    The scaffold carries no MCP server, Skill, tool or subagent: those come only from what
    the manifest selected (``capabilities``) and the deployment serves.
    """

    profile: DeepAgentProfile
    placement: DeepAgentExecutionPlacementProfile
    state_schema: CognitiveStateSchema
    context_schema: CognitiveRuntimeContextSchema
    context_values: dict[str, object]
    initial_context_manifest: dict[str, object]
    authority_refs: tuple[str, ...] = Field(min_length=1)
    redaction_policy_ref: str = Field(min_length=1)
    agent_profile_ref: ExactDefinitionRef
    tracing_policy_ref: str = Field(min_length=1)
    sensitive_data_policy_ref: str = Field(min_length=1)
    snapshot_policy_ref: str = Field(min_length=1)
    workspace: WorkspaceProvision
    goal_output_schemas: dict[Literal["executor", "verifier"], StructuredOutputBinding] = Field(
        default_factory=dict
    )

    @model_validator(mode="after")
    def scaffold_selects_nothing(self) -> DeepAgentScaffold:
        profile = self.profile
        if (
            profile.mcp_servers
            or profile.skills
            or profile.tools
            or profile.sync_subagents
            or profile.async_subagents
        ):
            raise ValueError(
                "the Deep Agent scaffold carries no MCP server, Skill, tool or subagent; "
                "manifest capabilities bind them"
            )
        if self.placement.logical_id not in profile.compatible_placement_ids:
            raise ValueError("the scaffold profile is not compatible with its placement")
        return self


class CapabilityComponentBinding(_File):
    """The exact component a deployment serves for one catalog capability revision.

    ``pinned`` names a capability-pin entry (MCP ``server_id``, Skill ``skill_name`` or tool
    ``tool_name``) the bootstrap resolves into the inline component.
    """

    catalog_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    kind: Literal["mcp_server", "skill", "tool"]
    pinned: str | None = Field(default=None, min_length=1)
    mcp_server: DeepAgentMCPServerComponent | None = None
    skill: DeepAgentSkillComponent | None = None
    tool: DeepAgentToolComponent | None = None

    @model_validator(mode="after")
    def one_component(self) -> CapabilityComponentBinding:
        inline = {
            "mcp_server": self.mcp_server,
            "skill": self.skill,
            "tool": self.tool,
        }
        present = [name for name, value in inline.items() if value is not None]
        if self.pinned is None and present != [self.kind]:
            raise ValueError(f"a {self.kind} binding needs exactly its inline component")
        if self.pinned is not None and present and present != [self.kind]:
            raise ValueError(f"a pinned {self.kind} binding cannot carry another component")
        return self

    @property
    def resolved(self) -> bool:
        return getattr(self, self.kind) is not None


class ManifestLaunchBindings(_File):
    """``mc.manifest_launch_bindings.v1``, or ``.v2`` = v1 plus the ``providers`` section of
    the local provider lanes (``provider_launch.ProviderLanes``). A v1 file reads unchanged."""

    schema_version: Literal["mc.manifest_launch_bindings.v1", "mc.manifest_launch_bindings.v2"] = (
        LAUNCH_BINDINGS_SCHEMA
    )
    deep_agents: DeepAgentScaffold
    model_profiles: dict[str, DeepAgentModelComponent] = Field(default_factory=dict)
    sandbox_profiles: dict[str, DeepAgentSandboxComponent] = Field(default_factory=dict)
    capabilities: dict[str, CapabilityComponentBinding] = Field(default_factory=dict)
    providers: ProviderLanes | None = Field(default=None, exclude_if=lambda value: value is None)

    @model_validator(mode="after")
    def providers_need_v2(self) -> ManifestLaunchBindings:
        if self.providers is not None and self.schema_version != LAUNCH_BINDINGS_SCHEMA_V2:
            raise ValueError(
                f"a providers section needs schema_version {LAUNCH_BINDINGS_SCHEMA_V2}"
            )
        return self

    @classmethod
    def load(cls, path: Path) -> ManifestLaunchBindings:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"manifest launch bindings are unavailable: {path}") from error
        return cls.model_validate(payload)


@dataclass(frozen=True)
class ServedComponents:
    """Digests of the components the executing workers can materialize.

    Pinned components (capability pins) plus those a deployment registers beside them;
    keyed like ``ExactComponentRegistry`` (Skills by bundle digest).
    """

    models: frozenset[str] = frozenset()
    sandboxes: frozenset[str] = frozenset()
    checkpointers: frozenset[str] = frozenset()
    stores: frozenset[str] = frozenset()
    mcp_servers: frozenset[str] = frozenset()
    skills: frozenset[str] = frozenset()
    tools: frozenset[str] = frozenset()


# --- Resolution (pure) ----------------------------------------------------------------------


@dataclass(frozen=True)
class NodeLaunchBinding:
    """One node role's resolved lane inputs, with the pointer that selected each."""

    model: DeepAgentModelComponent
    sandbox: DeepAgentSandboxComponent
    secret_refs: tuple[SecretRef, ...]
    reasoning_effort: Literal["minimal", "low", "medium", "high"] | None
    verbosity: Literal["low", "medium", "high"] | None
    mcp_servers: tuple[DeepAgentMCPServerComponent, ...] = ()
    skills: tuple[DeepAgentSkillComponent, ...] = ()
    tools: tuple[DeepAgentToolComponent, ...] = ()


@dataclass(frozen=True)
class AdmittedLaunch:
    """What the admitted configuration fixes about the templates of one run."""

    request_scope: str
    run_id: str
    configuration_digest: str
    blueprint: StageGraphBlueprint | GoalDirectedBlueprint
    workspace_ref: ExactDefinitionRef
    workspace_contract: WorkflowWorkspaceContract
    authority: AuthorityCeiling
    operation_binding_refs: frozenset[ExactDefinitionRef]
    compiled_at: datetime

    @classmethod
    def from_configuration(
        cls, request_scope: str, run_id: str, configuration: EffectiveRunConfiguration
    ) -> AdmittedLaunch:
        workspace_ref = next(
            (
                ref
                for ref in configuration.source_refs
                if ref.kind == DefinitionKind.WORKSPACE_TEMPLATE
            ),
            None,
        )
        if workspace_ref is None:
            raise ValueError("the admitted configuration names no workspace template")
        return cls(
            request_scope=request_scope,
            run_id=run_id,
            configuration_digest=configuration.digest,
            blueprint=configuration.selected_blueprint,
            workspace_ref=workspace_ref,
            workspace_contract=configuration.workflow_workspace_contract,
            authority=configuration.effective_authority,
            operation_binding_refs=configuration.runtime_profile.operation_binding_refs,
            compiled_at=configuration.context.compiled_at,
        )


def _authored_value(environment: Any, field: str) -> Any:
    value: Any = environment.authored() if environment is not None else {}
    for part in field.split("."):
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


def _source(
    definition: MissionDefinition,
    path: Sequence[DefinitionNode],
    field: str,
    *,
    verifier: bool = False,
) -> str:
    """The manifest pointer that set ``field`` of the node's effective environment."""

    suffix = "/environment/" + field.replace(".", "/")
    node = path[-1]
    if (
        verifier
        and node.verifier_environment is not None
        and _authored_value(node.verifier_environment, field)
        != _authored_value(node.environment, field)
    ):
        return f"{node.pointer}/verifier{suffix}"
    for item in reversed(path):
        if any(
            origin == "overlay" and (key == field or key.startswith(field + "."))
            for key, origin in item.field_provenance.items()
        ):
            return f"{item.pointer}{suffix}"
    return f"{definition.manifest_pointer}{suffix}"


def _finite_number(value: object) -> Decimal | None:
    """A manifest number; the committed definition stores decimals as JSON strings."""

    if isinstance(value, bool) or not isinstance(value, int | float | Decimal | str):
        return None
    try:
        number = Decimal(str(value))
    except InvalidOperation:
        return None
    return number if number.is_finite() else None


def _flatten(settings: Mapping[str, Any], prefix: str = "") -> dict[str, Any]:
    flat: dict[str, Any] = {}
    for key, value in settings.items():
        name = f"{prefix}{key}"
        if isinstance(value, Mapping):
            flat.update(_flatten(value, f"{name}."))
        else:
            flat[name] = value
    return flat


class ManifestLaunchResolver:
    """Resolve node environments against the deployment bindings (pure, no I/O)."""

    def __init__(
        self,
        bindings: ManifestLaunchBindings,
        served: ServedComponents,
        *,
        provider_secret_env: Mapping[str, str],
    ) -> None:
        profile = bindings.deep_agents.profile
        if profile.checkpointer_ref.digest not in served.checkpointers:
            raise ValueError("the scaffold checkpointer is not served by this deployment")
        if profile.store_ref.digest not in served.stores:
            raise ValueError("the scaffold store is not served by this deployment")
        unresolved = sorted(
            name for name, item in bindings.capabilities.items() if not item.resolved
        )
        if unresolved:
            raise ValueError(f"pinned capability bindings are unresolved: {unresolved}")
        self._bindings = bindings
        self._served = served
        self._secrets = dict(provider_secret_env)

    @property
    def bindings(self) -> ManifestLaunchBindings:
        return self._bindings

    # -- one node role ------------------------------------------------------------------------

    def node(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        *,
        verifier: bool,
        operation_binding_refs: frozenset[ExactDefinitionRef],
    ) -> NodeLaunchBinding:
        node = path[-1]
        environment = (
            node.verifier_environment
            if verifier and node.verifier_environment is not None
            else node.environment
        )
        lane = environment.lane or node.lane
        if str(lane) != DEEP_AGENTS_LANE:
            raise ManifestLaunchBindingError(
                _source(definition, path, "lane", verifier=verifier),
                f"lane {lane} has no production launch author; manifest runs launch on "
                f"{DEEP_AGENTS_LANE} only",
            )
        model, effort, verbosity = self._model(definition, path, environment, verifier=verifier)
        sandbox = self._sandbox(definition, path, environment, verifier=verifier)
        mcp, skills, tools = self._capabilities(
            definition, path, verifier=verifier, operation_binding_refs=operation_binding_refs
        )
        model_pointer = _source(definition, path, "model.profile", verifier=verifier)
        secret_env = self._secrets.get(model.provider)
        if not secret_env:
            raise ManifestLaunchBindingError(
                model_pointer,
                f"no credential reference is declared for model provider {model.provider} "
                "(MANIFEST_PROVIDER_SECRET_ENV)",
            )
        secrets = (SecretRef(provider="environment", key=secret_env), *sandbox.credential_refs)
        return NodeLaunchBinding(
            model=model,
            sandbox=sandbox,
            secret_refs=tuple(dict.fromkeys(secrets)),
            reasoning_effort=effort,
            verbosity=verbosity,
            mcp_servers=mcp,
            skills=skills,
            tools=tools,
        )

    def _model(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        environment: Any,
        *,
        verifier: bool,
    ) -> tuple[DeepAgentModelComponent, Any, Any]:
        pointer = _source(definition, path, "model.profile", verifier=verifier)
        selection = environment.model
        if selection is None or selection.profile is None:
            raise ManifestLaunchBindingError(pointer, "no model profile is bound")
        profile = selection.profile
        if isinstance(profile, str):
            component = self._bindings.model_profiles.get(profile)
            if component is None:
                raise ManifestLaunchBindingError(
                    pointer,
                    f"model profile {profile} has no deployment model binding "
                    "(mc.manifest_launch_bindings.v1 model_profiles)",
                )
        else:
            capability = next(
                (
                    item
                    for item in definition.capabilities
                    if item.role == "model" and item.pointer.startswith(pointer)
                ),
                None,
            )
            pin = (capability.resolved or capability.pin) if capability is not None else None
            if pin is None or pin.digest is None:
                raise ManifestLaunchBindingError(
                    pointer, "the model capability is not resolved to an exact catalog revision"
                )
            component = self._bindings.model_profiles.get(pin.capability_id)
            if component is None:
                raise ManifestLaunchBindingError(
                    pointer,
                    f"model {pin.capability_id}@{pin.version} has no deployment model binding",
                )
            if component.ref.digest != pin.digest:
                raise ManifestLaunchBindingError(
                    pointer,
                    f"model {pin.capability_id}@{pin.version} is pinned at {pin.digest} but the "
                    f"deployment binds {component.ref.digest}",
                )
        if component.ref.digest not in self._served.models:
            raise ManifestLaunchBindingError(
                pointer,
                f"model {component.ref.logical_id} ({component.ref.digest}) is not served by "
                "this deployment: no pinned model and no deployment model factory",
            )
        settings, effort, verbosity = self._model_settings(
            definition, path, environment, verifier=verifier
        )
        if settings:
            component = DeepAgentModelComponent(
                ref=component.ref,
                provider=component.provider,
                model_name=component.model_name,
                settings={**component.settings, **settings},
            )
        effort = effort or component.settings.get("reasoning_effort")
        verbosity = verbosity or component.settings.get("verbosity")
        return component, effort, verbosity

    def _model_settings(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        environment: Any,
        *,
        verifier: bool,
    ) -> tuple[dict[str, object], Any, Any]:
        authored = environment.model.settings or {}
        result: dict[str, object] = {}
        for name, value in sorted(_flatten(authored).items()):
            pointer = _source(definition, path, f"model.settings.{name}", verifier=verifier)
            target = _MODEL_SETTINGS.get(name)
            if target is None:
                raise ManifestLaunchBindingError(
                    pointer, f"model setting {name} has no provider mapping"
                )
            if target == "temperature":
                number = _finite_number(value)
                if number is None:
                    raise ManifestLaunchBindingError(pointer, "temperature must be a number")
                result[target] = float(number)
            elif target == "reasoning_effort" and value not in _REASONING:
                raise ManifestLaunchBindingError(
                    pointer, f"reasoning effort must be one of {', '.join(_REASONING)}"
                )
            elif target == "verbosity" and value not in _VERBOSITY:
                raise ManifestLaunchBindingError(
                    pointer, f"verbosity must be one of {', '.join(_VERBOSITY)}"
                )
            elif target == "max_completion_tokens" and (
                not isinstance(value, int) or isinstance(value, bool) or value < 1
            ):
                raise ManifestLaunchBindingError(pointer, "max_output_tokens must be positive")
            else:
                result[target] = value
        return result, result.get("reasoning_effort"), result.get("verbosity")

    def _sandbox(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        environment: Any,
        *,
        verifier: bool,
    ) -> DeepAgentSandboxComponent:
        pointer = _source(definition, path, "sandbox.profile", verifier=verifier)
        selection = environment.sandbox
        if selection is None or selection.profile is None:
            raise ManifestLaunchBindingError(pointer, "no sandbox profile is bound")
        component = self._bindings.sandbox_profiles.get(selection.profile)
        if component is None:
            raise ManifestLaunchBindingError(
                pointer,
                f"sandbox profile {selection.profile} has no deployment sandbox binding "
                "(mc.manifest_launch_bindings.v1 sandbox_profiles)",
            )
        if component.ref.digest not in self._served.sandboxes:
            raise ManifestLaunchBindingError(
                pointer,
                f"sandbox {component.ref.logical_id} is not served by this deployment",
            )
        if component.backend not in self._bindings.deep_agents.placement.sandbox_backends:
            raise ManifestLaunchBindingError(
                pointer,
                f"sandbox backend {component.backend} is not admitted by the Deep Agent placement",
            )
        return component

    def _capabilities(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        *,
        verifier: bool,
        operation_binding_refs: frozenset[ExactDefinitionRef],
    ) -> tuple[
        tuple[DeepAgentMCPServerComponent, ...],
        tuple[DeepAgentSkillComponent, ...],
        tuple[DeepAgentToolComponent, ...],
    ]:
        node = path[-1]
        environment = (
            node.verifier_environment
            if verifier and node.verifier_environment is not None
            else node.environment
        )
        contexts = environment.workspace.context if environment.workspace is not None else None
        for index, entry in enumerate(contexts or ()):
            if isinstance(entry, str):
                raise ManifestLaunchBindingError(
                    f"{_source(definition, path, 'workspace.context', verifier=verifier)}/{index}",
                    f"context profile {entry} has no deployment binding",
                )
        selected: list[DefinitionCapability] = []
        for group in _CAPABILITY_GROUPS:
            source = _source(definition, path, group, verifier=verifier) + "/"
            selected.extend(
                item for item in definition.capabilities if item.pointer.startswith(source)
            )
        pinned = {(ref.logical_id, ref.digest) for ref in operation_binding_refs}
        mcp: dict[str, DeepAgentMCPServerComponent] = {}
        skills: dict[str, DeepAgentSkillComponent] = {}
        tools: dict[str, DeepAgentToolComponent] = {}
        for item in selected:
            pin = item.resolved or item.pin
            if pin is None or pin.digest is None:
                raise ManifestLaunchBindingError(
                    item.pointer, "the capability is not resolved to an exact catalog revision"
                )
            label = f"{item.role} {pin.capability_id}@{pin.version}"
            if (pin.capability_id, pin.digest) not in pinned:
                raise ManifestLaunchBindingError(
                    item.pointer, f"{label} is not pinned by the admitted runtime profile"
                )
            binding = self._bindings.capabilities.get(pin.capability_id)
            if binding is None:
                raise ManifestLaunchBindingError(
                    item.pointer,
                    f"{label} has no deployment component binding (MCP servers, Skills and "
                    "tools bind through mc.manifest_launch_bindings.v1 capabilities; hook "
                    "scripts, subagent profiles, plugins and context bundles have no Deep "
                    "Agents launch binding)",
                )
            if binding.catalog_digest != pin.digest:
                raise ManifestLaunchBindingError(
                    item.pointer,
                    f"{label} is pinned at {pin.digest} but the deployment binds "
                    f"{binding.catalog_digest}",
                )
            if binding.mcp_server is not None:
                self._served_or_fail(item, label, binding.mcp_server.ref.digest, "mcp_servers")
                mcp[binding.mcp_server.ref.digest] = binding.mcp_server
            elif binding.skill is not None:
                self._served_or_fail(item, label, binding.skill.bundle_digest, "skills")
                skills[binding.skill.bundle_digest] = binding.skill
            elif binding.tool is not None:
                self._served_or_fail(item, label, binding.tool.ref.digest, "tools")
                tools[binding.tool.ref.digest] = binding.tool
        return (
            tuple(mcp[key] for key in sorted(mcp)),
            tuple(skills[key] for key in sorted(skills)),
            tuple(tools[key] for key in sorted(tools)),
        )

    def _served_or_fail(
        self, item: DefinitionCapability, label: str, digest: str, kind: str
    ) -> None:
        if digest not in getattr(self._served, kind):
            raise ManifestLaunchBindingError(
                item.pointer, f"{label} binds a component this deployment does not serve"
            )

    # -- one provider node role (claude_agent_sdk, codex) ---------------------------------------

    def provider_node(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        *,
        verifier: bool,
        operation_binding_refs: frozenset[ExactDefinitionRef],
        rows: Mapping[str, ResolvedCapability] | None = None,
    ) -> ProviderNodeLaunch:
        """Resolve one claude/codex node role to its sealed-binding inputs, or fail pointed.

        Every value comes from the committed definition (the v2 selections compile admitted)
        and the deployment's ``providers`` section; nothing is defaulted from another lane.
        """

        node = path[-1]
        environment = (
            node.verifier_environment
            if verifier and node.verifier_environment is not None
            else node.environment
        )
        selections = (
            node.verifier_selections
            if verifier and node.verifier_selections is not None
            else node.selections
        )
        profile = _lane_of(path, verifier=verifier)

        def source(field: str) -> str:
            return _selection_source(definition, path, field, verifier=verifier)

        lane_pointer = source("lane")
        if profile in HOSTED_PROVIDER_PROFILES:
            raise ManifestLaunchBindingError(
                lane_pointer,
                f"lane {profile} compiles structurally but its launch is refused: the "
                "provider-hosted lifecycle is evidence-blocked (MP-18/19, "
                f"docs/qualification/lanes/{profile}/REVALIDATION-2026-10-09.md)",
            )
        providers = self._bindings.providers
        lane = providers.lane(profile) if providers is not None else None
        if lane is None:
            raise ManifestLaunchBindingError(
                lane_pointer,
                f"lane profile {profile} has no deployment binding "
                f"({LAUNCH_BINDINGS_SCHEMA_V2} providers.{profile})",
            )
        if selections is None:
            raise ManifestLaunchBindingError(
                lane_pointer,
                f"lane {profile} needs the mission/v2 environment selections "
                "(auth, execution_environment, workspace policy); the definition carries none",
            )
        model, options = self._provider_model(
            definition, path, environment, lane, profile, verifier=verifier
        )
        if selections.auth is None:
            raise ManifestLaunchBindingError(source("auth"), "no auth profile is selected")
        auth = lane.auth_profiles.get(selections.auth.profile)
        if auth is None:
            raise ManifestLaunchBindingError(
                source("auth.profile"),
                f"auth profile {selections.auth.profile} has no deployment binding "
                f"(providers.{profile}.auth_profiles)",
            )
        placement = selections.execution_environment
        if placement is None:
            raise ManifestLaunchBindingError(
                source("execution_environment"), "no execution_environment is selected"
            )
        if placement.kind != "local_workspace" or placement.profile is None:
            raise ManifestLaunchBindingError(
                source("execution_environment.profile"),
                f"lane {profile} runs on a worker and needs a local_workspace host profile",
            )
        host = lane.host_profiles.get(placement.profile)
        if host is None:
            raise ManifestLaunchBindingError(
                source("execution_environment.profile"),
                f"host profile {placement.profile} has no deployment binding "
                f"(providers.{profile}.host_profiles)",
            )
        try:
            policy = admit_policy(profile, policy_pin(selections.workspace_policy))
        except WorkspaceError as error:
            raise ManifestLaunchBindingError(source("workspace.policy"), error.message) from error
        repo = environment.workspace.repo if environment.workspace is not None else None
        if repo is None:
            raise ManifestLaunchBindingError(
                source("workspace.repo"), f"lane {profile} requires workspace.repo"
            )
        describe = declared_matrix(profile)
        requires = selections.requires or RequiredFeatures()
        refused = [
            *admit_requirements(
                describe,
                RequirementSet(controls=requires.controls, observation=requires.observation),
            ),
            *admit_gate_coverage(
                describe,
                GateCoverageRequirement(
                    approvals=requires.approvals, all_writes_gated=requires.all_writes_gated
                ),
            ),
        ]
        if refused:
            raise ManifestLaunchBindingError(
                source("requires"), "; ".join(item.message for item in refused)
            )
        requirements = WorkflowRequirements(
            controls=requires.controls,
            approvals=requires.approvals,
            observation=requires.observation,
            describe_digest=describe.digest,
        )
        pins, ordered = self._provider_capabilities(
            definition, path, verifier=verifier, refs=operation_binding_refs, rows=rows
        )
        continuation = selections.continuation
        policy_digest = sha256_digest(
            {
                "lane_profile": profile,
                "requirements": stable_json_dump(requirements),
                "workspace_policy": stable_json_dump(policy),
                "continuation": stable_json_dump(continuation) if continuation else None,
                "side_effects": [item.value for item in environment.side_effects or ()],
                "egress": (
                    list(environment.sandbox.egress)
                    if environment.sandbox is not None and environment.sandbox.egress is not None
                    else None
                ),
                "commands_allowed": (
                    [item.value for item in definition.policies.commands_allowed]
                    if definition.policies.commands_allowed is not None
                    else None
                ),
            }
        )
        return ProviderNodeLaunch(
            profile=profile,
            lane=lane,
            lane_pointer=lane_pointer,
            model=model,
            rows=ordered,
            binding_fields={
                "lane_profile": profile,
                "model": model,
                "auth": auth,
                "environment": host,
                "workspace_policy": policy,
                "repo_url": repo.url or repo.path,
                "repo_ref": repo.ref,
                "requirements": requirements,
                "policy_digest": policy_digest,
                "pins": {**lane.pins, **pins},
                "provider_options": options,
                "budgets": _narrowed_budgets(lane.budgets, environment, continuation),
            },
        )

    def _provider_model(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        environment: Any,
        lane: ProviderLaneBinding,
        profile: str,
        *,
        verifier: bool,
    ) -> tuple[ModelPin, ClaudeSdkOptions | CodexAppServerOptions]:
        pointer = _source(definition, path, "model.profile", verifier=verifier)
        selection = environment.model
        if selection is None or selection.profile is None:
            raise ManifestLaunchBindingError(pointer, "no model profile is bound")
        if not isinstance(selection.profile, str):
            raise ManifestLaunchBindingError(
                pointer,
                f"a catalog model capability has no {profile} lane binding; select a model "
                f"profile bound in providers.{profile}.model_profiles",
            )
        model = lane.model_profiles.get(selection.profile)
        if model is None:
            raise ManifestLaunchBindingError(
                pointer,
                f"model profile {selection.profile} has no deployment model binding for lane "
                f"{profile} (providers.{profile}.model_profiles)",
            )
        options = lane.provider_options
        for name, value in sorted(_flatten(selection.settings or {}).items()):
            setting = _source(definition, path, f"model.settings.{name}", verifier=verifier)
            if (
                isinstance(options, CodexAppServerOptions)
                and name == "reasoning.effort"
                and value in _REASONING
            ):
                options = CodexAppServerOptions.model_validate(
                    {**options.model_dump(mode="python"), "reasoning_effort": value}
                )
                continue
            raise ManifestLaunchBindingError(
                setting, f"model setting {name} has no {profile} provider option mapping"
            )
        return model, options

    def _provider_capabilities(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        *,
        verifier: bool,
        refs: frozenset[ExactDefinitionRef],
        rows: Mapping[str, ResolvedCapability] | None,
    ) -> tuple[dict[str, str], tuple[ResolvedCapability, ...]]:
        """The node's projected capabilities: binding pin entries and rows in render order."""

        node = path[-1]
        environment = (
            node.verifier_environment
            if verifier and node.verifier_environment is not None
            else node.environment
        )
        contexts = environment.workspace.context if environment.workspace is not None else None
        for index, entry in enumerate(contexts or ()):
            if isinstance(entry, str):
                pointer = _source(definition, path, "workspace.context", verifier=verifier)
                raise ManifestLaunchBindingError(
                    f"{pointer}/{index}", f"context profile {entry} has no deployment binding"
                )
        selected = _selected_capabilities(definition, path, verifier=verifier)
        pinned = {(ref.logical_id, ref.digest) for ref in refs}
        pins: dict[str, str] = {}
        available: Mapping[str, ResolvedCapability] = rows or {}
        for item in selected:
            pin = item.resolved or item.pin
            if pin is None or pin.digest is None:
                raise ManifestLaunchBindingError(
                    item.pointer, "the capability is not resolved to an exact catalog revision"
                )
            text = f"{pin.capability_id}@{pin.version}#{pin.digest}"
            if (pin.capability_id, pin.digest) not in pinned:
                raise ManifestLaunchBindingError(
                    item.pointer, f"{text} is not pinned by the admitted runtime profile"
                )
            if text not in available:
                raise ManifestLaunchBindingError(
                    item.pointer,
                    f"{text} has no projection row: no catalog projection rows resolver is "
                    "composed for the provider lanes",
                )
            pins[capability_pin_key(item.role, pin.capability_id)] = text
        ordered = tuple(available[text] for text in binding_capability_pins(pins))
        return pins, ordered

    # -- one Cursor node role (cursor_local, cursor_cloud) --------------------------------------

    def cursor_node(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        *,
        verifier: bool,
        operation_binding_refs: frozenset[ExactDefinitionRef],
        rows: Mapping[str, ResolvedCapability] | None = None,
    ) -> CursorNodeLaunch:
        """Resolve one Cursor node role to its ``mc.cursor_binding.v1`` inputs, or fail pointed.

        A ``mission/v2`` definition's selections (auth, execution environment, workspace policy,
        requires) are admitted against ``providers.<profile>``; a ``mission/v1`` definition has
        none: the worker's Cursor credential applies, ``cursor_local`` runs on the worker that
        serves the lane's queue and ``cursor_cloud`` in the deployment's reviewed
        ``v1_environment``. Model, repository, capabilities and budgets bind for both.
        """

        node = path[-1]
        environment = (
            node.verifier_environment
            if verifier and node.verifier_environment is not None
            else node.environment
        )
        selections = (
            node.verifier_selections
            if verifier and node.verifier_selections is not None
            else node.selections
        )
        profile = _lane_of(path, verifier=verifier)

        def source(field: str) -> str:
            return _selection_source(definition, path, field, verifier=verifier)

        lane_pointer = source("lane")
        providers = self._bindings.providers
        lane = providers.cursor(profile) if providers is not None else None
        if lane is None:
            raise ManifestLaunchBindingError(
                lane_pointer,
                f"lane profile {profile} has no deployment binding "
                f"({LAUNCH_BINDINGS_SCHEMA_V2} providers.{profile})",
            )
        model = self._cursor_model(definition, path, environment, lane, profile, verifier=verifier)
        cloud = self._cursor_placement(selections, lane, profile, source, lane_pointer)
        if selections is not None:
            try:
                admit_policy(profile, policy_pin(selections.workspace_policy))
            except WorkspaceError as error:
                raise ManifestLaunchBindingError(
                    source("workspace.policy"), error.message
                ) from error
            describe = declared_matrix(profile)
            requires = selections.requires or RequiredFeatures()
            refused = [
                *admit_requirements(
                    describe,
                    RequirementSet(controls=requires.controls, observation=requires.observation),
                ),
                *admit_gate_coverage(
                    describe,
                    GateCoverageRequirement(
                        approvals=requires.approvals, all_writes_gated=requires.all_writes_gated
                    ),
                ),
            ]
            if refused:
                raise ManifestLaunchBindingError(
                    source("requires"), "; ".join(item.message for item in refused)
                )
        repo = environment.workspace.repo if environment.workspace is not None else None
        repo_pointer = source("workspace.repo")
        # The repository identity the deployment keys: the worker path a cursor_local node must
        # name (compile requires it), the URL a cursor_cloud agent clones.
        identity = (
            None
            if repo is None
            else (repo.path or repo.url)
            if isinstance(lane, CursorLocalLane)
            else repo.url
        )
        if repo is None or identity is None:
            raise ManifestLaunchBindingError(
                repo_pointer, f"lane {profile} requires workspace.repo"
            )
        repository = lane.repositories.get(str(identity))
        if repository is None:
            raise ManifestLaunchBindingError(
                repo_pointer,
                f"repository {identity} is not bound for lane {profile} "
                f"(providers.{profile}.repositories)",
            )
        if repo.ref is not None and repo.ref not in repository.base_refs:
            raise ManifestLaunchBindingError(
                f"{repo_pointer}/ref",
                f"ref {repo.ref} of repository {identity} is not admitted for lane {profile} "
                f"(providers.{profile}.repositories base_refs {list(repository.base_refs)})",
            )
        slots, ordered = self._cursor_capabilities(
            definition, path, profile, verifier=verifier, refs=operation_binding_refs, rows=rows
        )
        continuation = selections.continuation if selections is not None else None
        local = lane if isinstance(lane, CursorLocalLane) else None
        return CursorNodeLaunch(
            profile=profile,
            lane=lane,
            lane_pointer=lane_pointer,
            model=model,
            rows=ordered,
            binding_fields={
                "lane_profile": profile,
                "pins": lane.pins,
                "model_id": model.model_id,
                "mode": lane.mode,
                "workspace": CursorWorkspace(
                    repo_url=repository.repo_url,
                    base_ref=repo.ref or repository.base_refs[0],
                    lease_root=local.lease_root if local is not None else None,
                    branch_prefix=repository.branch_prefix,
                ),
                "inline_subagents": slots["inline_subagents"],
                "disallowed_tools": local.disallowed_tools if local is not None else (),
                "sandbox_enabled": local.sandbox_enabled if local is not None else False,
                "hook_callback": local.hook_callback if local is not None else None,
                "cloud": cloud,
                "budgets": _narrowed_cursor_budgets(lane.budgets, environment, continuation),
                "task_queue": lane.task_queue,
            },
            skills=slots["skills"],
            mcp_servers=slots["mcp_servers"],
        )

    def _cursor_model(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        environment: Any,
        lane: CursorLocalLane | CursorCloudLane,
        profile: str,
        *,
        verifier: bool,
    ) -> ModelPin:
        pointer = _source(definition, path, "model.profile", verifier=verifier)
        selection = environment.model
        if selection is None or selection.profile is None:
            raise ManifestLaunchBindingError(pointer, "no model profile is bound")
        if not isinstance(selection.profile, str):
            raise ManifestLaunchBindingError(
                pointer,
                f"a catalog model capability has no {profile} lane binding; select a model "
                f"profile bound in providers.{profile}.model_profiles",
            )
        model = lane.model_profiles.get(selection.profile)
        if model is None:
            raise ManifestLaunchBindingError(
                pointer,
                f"model profile {selection.profile} has no deployment model binding for lane "
                f"{profile} (providers.{profile}.model_profiles)",
            )
        for name in sorted(_flatten(selection.settings or {})):
            raise ManifestLaunchBindingError(
                _source(definition, path, f"model.settings.{name}", verifier=verifier),
                f"model setting {name} has no {profile} provider option mapping "
                "(mc.cursor_binding.v1 carries the model ID and mode only)",
            )
        return model

    def _cursor_placement(
        self,
        selections: EnvironmentSelections | None,
        lane: CursorLocalLane | CursorCloudLane,
        profile: str,
        source: Any,
        lane_pointer: str,
    ) -> CursorCloudOptions | None:
        """Admit the auth and execution environment selections; the cloud options to seal."""

        if selections is not None:
            if selections.auth is None:
                raise ManifestLaunchBindingError(source("auth"), "no auth profile is selected")
            if selections.auth.profile not in lane.auth_profiles:
                raise ManifestLaunchBindingError(
                    source("auth.profile"),
                    f"auth profile {selections.auth.profile} has no deployment binding "
                    f"(providers.{profile}.auth_profiles)",
                )
        placement = selections.execution_environment if selections is not None else None
        if isinstance(lane, CursorLocalLane):
            if selections is None:
                return None
            if placement is None:
                raise ManifestLaunchBindingError(
                    source("execution_environment"), "no execution_environment is selected"
                )
            if placement.kind != "local_workspace" or placement.profile is None:
                raise ManifestLaunchBindingError(
                    source("execution_environment.profile"),
                    f"lane {profile} runs on a worker and needs a local_workspace host profile",
                )
            if placement.profile not in lane.host_profiles:
                raise ManifestLaunchBindingError(
                    source("execution_environment.profile"),
                    f"host profile {placement.profile} has no deployment binding "
                    f"(providers.{profile}.host_profiles)",
                )
            return None
        if selections is None:
            if lane.v1_environment is None:
                raise ManifestLaunchBindingError(
                    lane_pointer,
                    f"a mission/v1 {profile} node cannot select a provider-hosted environment "
                    f"and providers.{profile} names no v1_environment",
                )
            return lane.environments[lane.v1_environment].options
        if placement is None or placement.environment_ref is None:
            raise ManifestLaunchBindingError(
                source("execution_environment"),
                f"lane {profile} needs a provider_hosted environment_ref",
            )
        bound = lane.environments.get(placement.environment_ref)
        if bound is None:
            raise ManifestLaunchBindingError(
                source("execution_environment.environment_ref"),
                f"environment {placement.environment_ref} has no deployment binding "
                f"(providers.{profile}.environments)",
            )
        expected = bound.environment
        if (
            placement.expected_revision is not None
            and placement.expected_revision != expected.expected_revision
        ):
            raise ManifestLaunchBindingError(
                source("execution_environment.expected_revision"),
                f"environment {placement.environment_ref} is bound at revision "
                f"{expected.expected_revision}, not {placement.expected_revision}",
            )
        if placement.setup is not None and placement.setup.pin != expected.setup_pin:
            raise ManifestLaunchBindingError(
                source("execution_environment.setup.pin"),
                f"environment {placement.environment_ref} is bound with setup "
                f"{expected.setup_pin}, not {placement.setup.pin}",
            )
        return bound.options

    def _cursor_capabilities(
        self,
        definition: MissionDefinition,
        path: Sequence[DefinitionNode],
        profile: str,
        *,
        verifier: bool,
        refs: frozenset[ExactDefinitionRef],
        rows: Mapping[str, ResolvedCapability] | None,
    ) -> tuple[dict[str, tuple[str, ...]], tuple[ResolvedCapability, ...]]:
        """The binding's projected pins by slot and the rows in ``CatalogRows`` order."""

        node = path[-1]
        environment = (
            node.verifier_environment
            if verifier and node.verifier_environment is not None
            else node.environment
        )
        contexts = environment.workspace.context if environment.workspace is not None else None
        for index, entry in enumerate(contexts or ()):
            if isinstance(entry, str):
                pointer = _source(definition, path, "workspace.context", verifier=verifier)
                raise ManifestLaunchBindingError(
                    f"{pointer}/{index}", f"context profile {entry} has no deployment binding"
                )
        pinned = {(ref.logical_id, ref.digest) for ref in refs}
        available: Mapping[str, ResolvedCapability] = rows or {}
        slots: dict[str, list[str]] = {"skills": [], "mcp_servers": [], "inline_subagents": []}
        for item in _selected_capabilities(definition, path, verifier=verifier):
            refused = (
                f"mc.cursor_binding.v1 projects skills, MCP servers and subagent profiles "
                f"only; a {{kind}} has no {profile} launch binding"
            )
            if item.kind is not None and str(item.kind) not in _CURSOR_MANIFEST_KINDS:
                raise ManifestLaunchBindingError(item.pointer, refused.format(kind=str(item.kind)))
            pin = item.resolved or item.pin
            if pin is None or pin.digest is None:
                raise ManifestLaunchBindingError(
                    item.pointer, "the capability is not resolved to an exact catalog revision"
                )
            text = f"{pin.capability_id}@{pin.version}#{pin.digest}"
            if (pin.capability_id, pin.digest) not in pinned:
                raise ManifestLaunchBindingError(
                    item.pointer, f"{text} is not pinned by the admitted runtime profile"
                )
            row = available.get(text)
            if row is None:
                raise ManifestLaunchBindingError(
                    item.pointer,
                    f"{text} has no projection row: no catalog projection rows resolver is "
                    "composed for the Cursor lanes",
                )
            slot = CURSOR_PROJECTED_KINDS.get(row.definition.kind)
            if slot is None:
                raise ManifestLaunchBindingError(
                    item.pointer, refused.format(kind=str(row.definition.kind))
                )
            slots[slot].append(text)
        fixed = {name: tuple(dict.fromkeys(values)) for name, values in slots.items()}
        order = cursor_rows_order(fixed["skills"], fixed["mcp_servers"], fixed["inline_subagents"])
        return fixed, tuple(available[text] for text in order)

    # -- templates ----------------------------------------------------------------------------

    def stage_templates(
        self,
        definition: MissionDefinition,
        admitted: AdmittedLaunch,
        *,
        rows: Mapping[str, ResolvedCapability] | None = None,
    ) -> dict[str, OperationExecutionRequest]:
        blueprint = admitted.blueprint
        if not isinstance(blueprint, StageGraphBlueprint):
            raise ValueError("the admitted blueprint is not a Stage Graph")
        root = definition.program
        children = root.nodes if root.behavior is Behavior.STAGE_GRAPH else (root,)
        paths = {child.key: ((root, child) if child is not root else (root,)) for child in children}
        templates: dict[str, OperationExecutionRequest] = {}
        for stage in blueprint.stages:
            path = paths.get(stage.stage_id)
            if path is None:
                raise ValueError(f"stage {stage.stage_id} has no manifest node")
            node = path[-1]
            objective = _stage_objective(definition, node)
            namespace = f"workspace-namespace:{{run_id}}:stage:{stage.stage_id}"
            workspace_id = f"workspace:{{run_id}}:stage:{stage.stage_id}"
            owner = WorkspaceOwner(
                kind=WorkspaceOwnerKind.STAGE, owner_id=f"stage:{stage.stage_id}"
            )
            if _lane_of(path, verifier=False) in PROVIDER_PROFILES:
                provider = self.provider_node(
                    definition,
                    path,
                    verifier=False,
                    operation_binding_refs=admitted.operation_binding_refs,
                    rows=rows,
                )
                workspace = self._workspace(
                    admitted,
                    namespace=namespace,
                    workspace_id=workspace_id,
                    owner=owner,
                    provision=provider.lane.workspace,
                )
                for slot in stage.operation_slots:
                    for variant in slot.allowed_variants:
                        key = template_key(
                            stage.stage_id, slot.operation_slot_id, variant.operation_variant_id
                        )
                        templates[key] = self._provider_request(
                            admitted,
                            key=key,
                            provider=provider,
                            objective=objective,
                            source=f"input:{node.pointer}",
                            workspace=workspace,
                            operation_contract_ref=variant.operation_contract_ref,
                            output_schema=None,
                        )
                continue
            if _lane_of(path, verifier=False) in CURSOR_LANE_PROFILES:
                cursor = self.cursor_node(
                    definition,
                    path,
                    verifier=False,
                    operation_binding_refs=admitted.operation_binding_refs,
                    rows=rows,
                )
                workspace = self._workspace(
                    admitted,
                    namespace=namespace,
                    workspace_id=workspace_id,
                    owner=owner,
                    provision=cursor.lane.workspace,
                )
                for slot in stage.operation_slots:
                    for variant in slot.allowed_variants:
                        key = template_key(
                            stage.stage_id, slot.operation_slot_id, variant.operation_variant_id
                        )
                        templates[key] = self._cursor_request(
                            admitted,
                            key=key,
                            cursor=cursor,
                            objective=objective,
                            source=f"input:{node.pointer}",
                            workspace=workspace,
                            operation_contract_ref=variant.operation_contract_ref,
                            output_schema=None,
                        )
                continue
            resolved = self.node(
                definition,
                path,
                verifier=False,
                operation_binding_refs=admitted.operation_binding_refs,
            )
            workspace = self._workspace(
                admitted, namespace=namespace, workspace_id=workspace_id, owner=owner
            )
            for slot in stage.operation_slots:
                for variant in slot.allowed_variants:
                    key = template_key(
                        stage.stage_id, slot.operation_slot_id, variant.operation_variant_id
                    )
                    templates[key] = self._request(
                        admitted,
                        key=key,
                        resolved=resolved,
                        objective=objective,
                        source=f"input:{node.pointer}",
                        workspace=workspace,
                        operation_contract_ref=variant.operation_contract_ref,
                        output_schema=None,
                    )
        return templates

    def goal_templates(
        self,
        definition: MissionDefinition,
        admitted: AdmittedLaunch,
        *,
        rows: Mapping[str, ResolvedCapability] | None = None,
    ) -> dict[str, OperationExecutionRequest]:
        blueprint = admitted.blueprint
        if not isinstance(blueprint, GoalDirectedBlueprint):
            raise ValueError("the admitted blueprint is not a Goal Loop")
        root = definition.program
        base_id = blueprint.logical_id.removesuffix(".blueprint")
        templates: dict[str, OperationExecutionRequest] = {}
        for role in GOAL_ROLES:
            verifier = role == "verifier"
            objective = (
                f"Mission {definition.mission_key} Goal Loop {role}: {blueprint.objective_contract}"
            )
            namespace = "workspace-namespace:{run_id}"
            workspace_id = f"workspace:{{run_id}}:{role}"
            owner = WorkspaceOwner(kind=WorkspaceOwnerKind.RUN, owner_id="goal-template")
            output_schema = self._bindings.deep_agents.goal_output_schemas.get(role)
            if _lane_of((root,), verifier=verifier) in PROVIDER_PROFILES:
                provider = self.provider_node(
                    definition,
                    (root,),
                    verifier=verifier,
                    operation_binding_refs=admitted.operation_binding_refs,
                    rows=rows,
                )
                templates[role] = self._provider_request(
                    admitted,
                    key=role,
                    provider=provider,
                    objective=objective,
                    source=f"input:{root.pointer}/{role}",
                    workspace=self._workspace(
                        admitted,
                        namespace=namespace,
                        workspace_id=workspace_id,
                        owner=owner,
                        provision=provider.lane.workspace,
                    ),
                    operation_contract_ref=f"operation:{base_id}:goal-{role}@1",
                    output_schema=output_schema,
                )
                continue
            if _lane_of((root,), verifier=verifier) in CURSOR_LANE_PROFILES:
                cursor = self.cursor_node(
                    definition,
                    (root,),
                    verifier=verifier,
                    operation_binding_refs=admitted.operation_binding_refs,
                    rows=rows,
                )
                templates[role] = self._cursor_request(
                    admitted,
                    key=role,
                    cursor=cursor,
                    objective=objective,
                    source=f"input:{root.pointer}/{role}",
                    workspace=self._workspace(
                        admitted,
                        namespace=namespace,
                        workspace_id=workspace_id,
                        owner=owner,
                        provision=cursor.lane.workspace,
                    ),
                    operation_contract_ref=f"operation:{base_id}:goal-{role}@1",
                    output_schema=output_schema,
                )
                continue
            resolved = self.node(
                definition,
                (root,),
                verifier=verifier,
                operation_binding_refs=admitted.operation_binding_refs,
            )
            templates[role] = self._request(
                admitted,
                key=role,
                resolved=resolved,
                objective=objective,
                source=f"input:{root.pointer}/{role}",
                workspace=self._workspace(
                    admitted, namespace=namespace, workspace_id=workspace_id, owner=owner
                ),
                operation_contract_ref=f"operation:{base_id}:goal-{role}@1",
                output_schema=output_schema,
            )
        return templates

    def _workspace(
        self,
        admitted: AdmittedLaunch,
        *,
        namespace: str,
        workspace_id: str,
        owner: WorkspaceOwner,
        provision: WorkspaceProvision | None = None,
    ) -> WorkspaceContract:
        contract = admitted.workspace_contract
        provision = provision or self._bindings.deep_agents.workspace
        writable = [slot for slot in contract.slots if slot.access == "exclusive_write"]
        if not writable:
            raise ValueError("the admitted workspace contract has no writable slot")
        return WorkspaceContract(
            namespace_id=namespace,
            workspace_id=workspace_id,
            provider=provision.provider,
            template_ref=admitted.workspace_ref,
            workflow_contract_digest=stable_json_digest(contract),
            slot_bindings=tuple(
                WorkspaceSlotBinding(
                    slot_name=slot.name,
                    logical_path=slot.path,
                    access="exclusive_write",
                    owner=owner,
                )
                for slot in writable
            ),
            exclusive_write_paths=tuple(slot.path for slot in writable),
            network_policy=provision.network_policy,
            runtime_digest=provision.runtime_digest,
            image_digest=provision.image_digest,
            package_digest=provision.package_digest,
            environment_digest=provision.environment_digest,
        )

    def _request(
        self,
        admitted: AdmittedLaunch,
        *,
        key: str,
        resolved: NodeLaunchBinding,
        objective: str,
        source: str,
        workspace: WorkspaceContract,
        operation_contract_ref: str,
        output_schema: StructuredOutputBinding | None,
    ) -> OperationExecutionRequest:
        scaffold = self._bindings.deep_agents
        identity = OperationAttemptIdentity(
            run_id=admitted.run_id,
            operation_id=f"manifest-template/{key}",
            operation_attempt=1,
        )
        reservation_id = f"reservation:{admitted.run_id}:manifest-template"
        grant = CapabilityGrant(
            capabilities=admitted.authority.capabilities,
            tool_ids=frozenset(item.tool_name for item in resolved.tools),
            mcp_server_ids=frozenset(item.server_name for item in resolved.mcp_servers),
        )
        profile = DeepAgentProfile.create(
            **{
                **scaffold.profile.model_dump(mode="python", exclude={"profile_digest"}),
                "model": resolved.model,
                "sandbox": resolved.sandbox,
                "mcp_servers": resolved.mcp_servers,
                "skills": resolved.skills,
                "tools": resolved.tools,
            }
        )
        values = dict(scaffold.context_values)
        for name, value in (
            ("run_id", admitted.run_id),
            ("operation_id", identity.operation_id),
            ("operation_attempt", 1),
            ("execution_generation", 1),
        ):
            if name in values:
                values[name] = value
        binding = compile_deep_agent_execution_binding(
            profile=profile,
            placement=scaffold.placement,
            state_schema=scaffold.state_schema,
            context_schema=scaffold.context_schema,
            context_values=values,
            run_id=admitted.run_id,
            operation_id=identity.operation_id,
            operation_attempt=1,
            execution_generation=1,
            erc_digest=admitted.configuration_digest,
            control_revision=TEMPLATE_REVISION,
            workspace=workspace,
            capability_grant=grant,
            reservation_id=reservation_id,
            authority_refs=scaffold.authority_refs,
            redaction_policy_ref=scaffold.redaction_policy_ref,
            initial_context_manifest=scaffold.initial_context_manifest,
        )
        return OperationExecutionRequest(
            identity=identity,
            request_scope=admitted.request_scope,
            effective_configuration_digest=admitted.configuration_digest,
            run_control_revision=TEMPLATE_REVISION,
            operation_contract_ref=operation_contract_ref,
            prompt_segments=(
                PromptSegment(
                    source_ref=source,
                    source_revision=1,
                    trust_class=PromptTrustClass.ADMITTED_INPUT,
                    content=objective,
                    rendered_digest=sha256_digest(objective),
                ),
            ),
            model_policy=ModelPolicy(
                provider=resolved.model.provider,
                model=resolved.model.model_name,
                reasoning_effort=resolved.reasoning_effort,
                verbosity=resolved.verbosity,
            ),
            output_schema=output_schema,
            agent_profile_ref=scaffold.agent_profile_ref,
            capability_grant=grant,
            workspace=workspace,
            secret_refs=resolved.secret_refs,
            execution_runtime="deep_agent",
            native_placement=None,
            deep_agent_binding=binding,
            budget_reservation_id=reservation_id,
            budget_limits=dict(admitted.authority.budgets.dimensions),
            tracing_policy_ref=scaffold.tracing_policy_ref,
            sensitive_data_policy_ref=scaffold.sensitive_data_policy_ref,
            snapshot_policy_ref=scaffold.snapshot_policy_ref,
            requested_at=admitted.compiled_at,
            idempotency_key=f"manifest-template:{admitted.run_id}:{key}",
        )

    def _provider_request(
        self,
        admitted: AdmittedLaunch,
        *,
        key: str,
        provider: ProviderNodeLaunch,
        objective: str,
        source: str,
        workspace: WorkspaceContract,
        operation_contract_ref: str,
        output_schema: StructuredOutputBinding | None,
    ) -> OperationExecutionRequest:
        """A claude/codex template: a sealed ``mc.execution_binding.v2`` routed to its lane."""

        lane = provider.lane
        segments = (
            PromptSegment(
                source_ref=source,
                source_revision=1,
                trust_class=PromptTrustClass.ADMITTED_INPUT,
                content=objective,
                rendered_digest=sha256_digest(objective),
            ),
        )
        try:
            digest = materialization_digest(provider.rows, provider.profile, segments)
        except ValueError as error:
            raise ManifestLaunchBindingError(
                provider.lane_pointer, f"the {provider.profile} host projection is refused: {error}"
            ) from error
        binding = ProviderExecutionBinding.sealed(
            **provider.binding_fields,
            materialization_digest=digest,
            task_queue=lane.task_queue,
        )
        identity = OperationAttemptIdentity(
            run_id=admitted.run_id,
            operation_id=f"manifest-template/{key}",
            operation_attempt=1,
        )
        reservation_id = f"reservation:{admitted.run_id}:manifest-template"
        options = binding.provider_options
        return OperationExecutionRequest(
            identity=identity,
            request_scope=admitted.request_scope,
            effective_configuration_digest=admitted.configuration_digest,
            run_control_revision=TEMPLATE_REVISION,
            operation_contract_ref=operation_contract_ref,
            prompt_segments=segments,
            model_policy=ModelPolicy(
                provider=MODEL_PROVIDER[provider.profile],
                model=provider.model.model_id,
                reasoning_effort=(
                    options.reasoning_effort if isinstance(options, CodexAppServerOptions) else None
                ),
            ),
            output_schema=output_schema,
            agent_profile_ref=lane.agent_profile_ref,
            capability_grant=CapabilityGrant(capabilities=admitted.authority.capabilities),
            workspace=workspace,
            secret_refs=(),
            execution_runtime=RUNTIME_OF_LANE[LANE_OF_PROFILE[provider.profile]],
            native_placement=None,
            lane_profile=binding.lane_profile,
            provider_binding=binding,
            budget_reservation_id=reservation_id,
            budget_limits=dict(admitted.authority.budgets.dimensions),
            tracing_policy_ref=lane.tracing_policy_ref,
            sensitive_data_policy_ref=lane.sensitive_data_policy_ref,
            snapshot_policy_ref=lane.snapshot_policy_ref,
            requested_at=admitted.compiled_at,
            idempotency_key=f"manifest-template:{admitted.run_id}:{key}",
        )

    def _cursor_request(
        self,
        admitted: AdmittedLaunch,
        *,
        key: str,
        cursor: CursorNodeLaunch,
        objective: str,
        source: str,
        workspace: WorkspaceContract,
        operation_contract_ref: str,
        output_schema: StructuredOutputBinding | None,
    ) -> OperationExecutionRequest:
        """A Cursor template: a sealed ``mc.cursor_binding.v1`` routed to its lane queue, whose
        projection digests are those the harness re-renders at ``prepare``."""

        lane = cursor.lane
        segments = (
            PromptSegment(
                source_ref=source,
                source_revision=1,
                trust_class=PromptTrustClass.ADMITTED_INPUT,
                content=objective,
                rendered_digest=sha256_digest(objective),
            ),
        )
        try:
            projection = cursor_projection(cursor.rows, cursor.profile, segments)
        except ValueError as error:
            raise ManifestLaunchBindingError(
                cursor.lane_pointer, f"the {cursor.profile} host projection is refused: {error}"
            ) from error
        digests = cursor_projection_digests(projection)
        binding = CursorExecutionBinding.sealed(
            **cursor.binding_fields,
            projections=CursorProjections(
                **digests, skills=cursor.skills, mcp_servers=cursor.mcp_servers
            ),
        )
        identity = OperationAttemptIdentity(
            run_id=admitted.run_id,
            operation_id=f"manifest-template/{key}",
            operation_attempt=1,
        )
        reservation_id = f"reservation:{admitted.run_id}:manifest-template"
        return OperationExecutionRequest(
            identity=identity,
            request_scope=admitted.request_scope,
            effective_configuration_digest=admitted.configuration_digest,
            run_control_revision=TEMPLATE_REVISION,
            operation_contract_ref=operation_contract_ref,
            prompt_segments=segments,
            model_policy=ModelPolicy(provider="cursor", model=cursor.model.model_id),
            output_schema=output_schema,
            agent_profile_ref=lane.agent_profile_ref,
            capability_grant=CapabilityGrant(capabilities=admitted.authority.capabilities),
            workspace=workspace,
            # The Cursor credential is the serving worker's (`CURSOR_API_KEY`), never the run's.
            secret_refs=(),
            execution_runtime=RUNTIME_OF_LANE[LANE_OF_PROFILE[cursor.profile]],
            native_placement=None,
            lane_profile=binding.lane_profile,
            cursor_binding=binding,
            budget_reservation_id=reservation_id,
            budget_limits=dict(admitted.authority.budgets.dimensions),
            tracing_policy_ref=lane.tracing_policy_ref,
            sensitive_data_policy_ref=lane.sensitive_data_policy_ref,
            snapshot_policy_ref=lane.snapshot_policy_ref,
            requested_at=admitted.compiled_at,
            idempotency_key=f"manifest-template:{admitted.run_id}:{key}",
        )


@dataclass(frozen=True)
class CursorNodeLaunch:
    """One Cursor node role's resolved binding inputs (sealed per template with the
    projection digests of that template's operating contract)."""

    profile: str
    lane: CursorLocalLane | CursorCloudLane
    lane_pointer: str
    model: ModelPin
    rows: tuple[ResolvedCapability, ...]
    binding_fields: dict[str, Any]
    skills: tuple[str, ...]
    mcp_servers: tuple[str, ...]


@dataclass(frozen=True)
class ProviderNodeLaunch:
    """One claude/codex node role's resolved binding inputs (sealed per template)."""

    profile: str
    lane: ProviderLaneBinding
    lane_pointer: str
    model: ModelPin
    rows: tuple[ResolvedCapability, ...]
    binding_fields: dict[str, Any]


def _selected_capabilities(
    definition: MissionDefinition, path: Sequence[DefinitionNode], *, verifier: bool
) -> list[DefinitionCapability]:
    """The capabilities a node role's effective environment selects (by source pointer)."""

    selected: list[DefinitionCapability] = []
    for group in _CAPABILITY_GROUPS:
        prefix = _source(definition, path, group, verifier=verifier) + "/"
        selected.extend(item for item in definition.capabilities if item.pointer.startswith(prefix))
    return selected


def provider_roles(
    definition: MissionDefinition,
) -> tuple[tuple[tuple[DefinitionNode, ...], bool], ...]:
    """The (path, verifier) node roles a manifest run launches on a provider lane profile
    (claude/codex, hosted or local, and the Cursor profiles)."""

    root = definition.program
    roles: list[tuple[tuple[DefinitionNode, ...], bool]] = []
    if root.behavior is Behavior.GOAL_LOOP:
        roles.extend(((root,), verifier) for verifier in (False, True))
    else:
        children = root.nodes if root.behavior is Behavior.STAGE_GRAPH else (root,)
        roles.extend(
            (((root, child) if child is not root else (root,)), False) for child in children
        )
    return tuple(
        (path, verifier)
        for path, verifier in roles
        if _lane_of(path, verifier=verifier) in PROVIDER_PROFILES | CURSOR_LANE_PROFILES
    )


def provider_capability_pins(definition: MissionDefinition) -> tuple[str, ...]:
    """Exact pin texts the provider node roles project (resolved before the pure resolver).

    A Cursor role asks only for the kinds its binding can project; any other selected kind is
    refused at its pointer by the resolver, never resolved as a projection row."""

    texts: dict[str, None] = {}
    for path, verifier in provider_roles(definition):
        cursor = _lane_of(path, verifier=verifier) in CURSOR_LANE_PROFILES
        for item in _selected_capabilities(definition, path, verifier=verifier):
            if cursor and item.kind is not None and str(item.kind) not in _CURSOR_MANIFEST_KINDS:
                continue
            pin = item.resolved or item.pin
            if pin is not None and pin.digest is not None:
                texts[f"{pin.capability_id}@{pin.version}#{pin.digest}"] = None
    return tuple(texts)


def _lane_of(path: Sequence[DefinitionNode], *, verifier: bool) -> str:
    node = path[-1]
    environment = (
        node.verifier_environment
        if verifier and node.verifier_environment is not None
        else node.environment
    )
    return str(environment.lane or node.lane)


_SELECTION_FIELDS: dict[str, str] = {
    "auth": "auth",
    "execution_environment": "execution_environment",
    "workspace.policy": "workspace_policy",
    "continuation": "continuation",
    "requires": "requires",
}


def _selection_value(selections: EnvironmentSelections | None, field: str) -> Any:
    """A v2 selection's value by environment field path (``auth.profile``, ...)."""

    if selections is None:
        return None
    name = _SELECTION_FIELDS.get(field)
    rest: list[str] = []
    if name is None:
        head, _, tail = field.partition(".")
        name = _SELECTION_FIELDS.get(head)
        rest = tail.split(".") if tail else []
    if name is None:
        return None
    value: Any = selections.model_dump(mode="json").get(name)
    for part in rest:
        value = value.get(part) if isinstance(value, dict) else None
    return value


def _selection_source(
    definition: MissionDefinition, path: Sequence[DefinitionNode], field: str, *, verifier: bool
) -> str:
    """The manifest pointer that set a v2 selection (or any field) of a node role."""

    node = path[-1]
    if (
        verifier
        and node.verifier_selections is not None
        and _selection_value(node.verifier_selections, field)
        != _selection_value(node.selections, field)
    ):
        return f"{node.pointer}/verifier/environment/" + field.replace(".", "/")
    return _source(definition, path, field, verifier=verifier)


def _narrowed_budgets(
    budgets: BindingBudgets, environment: Any, continuation: ContinuationPolicy | None
) -> BindingBudgets:
    """The deployment's session budgets, narrowed (never raised) by the node's own limits."""

    update: dict[str, int] = {}
    authored = environment.budget
    if authored is not None and authored.wall_clock is not None:
        update["wall_clock_s"] = min(budgets.wall_clock_s, authored.wall_clock)
    if authored is not None and authored.tokens is not None and authored.tokens > 0:
        ceiling = budgets.token_ceiling
        update["token_ceiling"] = min(ceiling, authored.tokens) if ceiling else authored.tokens
    if continuation is not None and continuation.max_session_turns is not None:
        update["max_turns"] = min(budgets.max_turns, continuation.max_session_turns)
    if not update:
        return budgets
    return BindingBudgets.model_validate({**budgets.model_dump(mode="python"), **update})


def _narrowed_cursor_budgets(
    budgets: CursorBudgets, environment: Any, continuation: ContinuationPolicy | None
) -> CursorBudgets:
    """The deployment's Cursor session budgets, narrowed (never raised) by the node's own
    wall clock and session-turn limits (``mc.cursor_binding.v1`` has no token ceiling)."""

    update: dict[str, int] = {}
    authored = environment.budget
    if authored is not None and authored.wall_clock is not None:
        update["wall_clock_s"] = min(budgets.wall_clock_s, authored.wall_clock)
    if continuation is not None and continuation.max_session_turns is not None:
        update["max_turns"] = min(budgets.max_turns, continuation.max_session_turns)
    if not update:
        return budgets
    return CursorBudgets.model_validate({**budgets.model_dump(mode="python"), **update})


def _stage_objective(definition: MissionDefinition, node: DefinitionNode) -> str:
    descriptions = {goal.key: goal.description for goal in definition.goals}
    descriptions.update({item.key: item.description for item in definition.objectives})
    texts = [descriptions[key] for key in node.objectives if key in descriptions]
    body = node.body.get("objective") or node.body.get("description")
    detail = "; ".join(texts) or (str(body) if body else node.key)
    return f"Mission {definition.mission_key} stage {node.key}: {detail}"


# --- The author (I/O) -----------------------------------------------------------------------


class ManifestRunDefinitions(Protocol):
    async def run_subscriptions(self, request_scope: str, run_key: str) -> dict[str, Any] | None:
        """The run's mission and the committed ``MissionDefinition@1`` (``definition``)."""


class StageTemplateStore(Protocol):
    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        templates: Mapping[str, OperationExecutionRequest],
        recorded_at: datetime,
    ) -> None: ...

    async def list_templates(
        self, *, request_scope: str, semantic_input_binding_ref: str
    ) -> dict[str, OperationExecutionRequest]: ...


class GoalTemplateStore(Protocol):
    async def persist_templates(
        self,
        *,
        request_scope: str,
        semantic_input_binding_ref: str,
        executor: OperationExecutionRequest,
        verifier: OperationExecutionRequest,
        recorded_at: datetime,
    ) -> None: ...

    async def get_template(
        self,
        *,
        semantic_input_binding_ref: str,
        operation_role: str,
        request_scope: str,
        run_id: str,
    ) -> OperationExecutionRequest: ...


def _gate_reviewers(node: Any) -> tuple[str, ...]:
    """Reviewers of a manifest gate node, whether it carries `task` or a `body["task"]`."""

    task = getattr(node, "task", None)
    if task is None:
        task = (getattr(node, "body", None) or {}).get("task")
    if task is None:
        return ()
    if not isinstance(task, dict):
        task = task.model_dump(mode="python", by_alias=True)
    return tuple(str(item) for item in task.get("reviewers", ()) if item)


def _human_gate_nodes(definition: MissionDefinition) -> list[Any]:
    root = definition.program
    children = root.nodes if root.behavior is Behavior.STAGE_GRAPH else (root,)
    return [child for child in children if child.behavior is Behavior.HUMAN_GATE]


class ManifestLaunchInputAuthor:
    """The production ``LaunchInputPort`` of manifest runs (see module doc)."""

    def __init__(
        self,
        *,
        resolver: ManifestLaunchResolver,
        run_control: RunControlService,
        control_plane: ControlPlaneService,
        definitions: ManifestRunDefinitions,
        stage_templates: StageTemplateStore,
        goal_templates: GoalTemplateStore,
        task_timeout_seconds: int = 300,
        projection_rows: ProjectionRowsPort | None = None,
    ) -> None:
        self._resolver = resolver
        # Catalog rows of the capabilities claude/codex nodes project (MP-03); without it a
        # provider node that selects any capability fails at that capability's pointer.
        self._projection_rows = projection_rows
        self._run_control = run_control
        self._control_plane = control_plane
        self._definitions = definitions
        self._stages = stage_templates
        self._goals = goal_templates
        self._timeout = task_timeout_seconds
        self._dispatcher = WorkflowLaunchDispatcher(
            stagegraph=StageGraphLaunchService(run_control, control_plane),
            goal_directed=GoalDirectedLaunchService(run_control, control_plane),
            run_control=run_control,
            control_plane=control_plane,
        )

    async def family_input(
        self,
        *,
        request_scope: str,
        run_id: str,
        family: Literal["StageGraph", "GoalDirected"],
        initial_goal: str | None,
    ) -> dict[str, Any]:
        await self.bind(request_scope, run_id, family=family)
        gates, review = await self._human_control(request_scope, run_id, family)
        # MP-20: a Stage Graph consumer's authored input names and expand modes travel beside
        # the blueprint (no blueprint digest change) to its Context Packet bindings.
        stage_inputs: tuple[StageAuthoredInput, ...] = ()
        if family == "StageGraph":
            info = await self._definitions.run_subscriptions(request_scope, run_id)
            if info is not None:
                stage_inputs = stagegraph_authored_inputs(
                    MissionDefinition.model_validate(info["definition"])
                )
        prepared = await self._dispatcher.prepare(
            request_scope,
            run_id,
            initial_goal=initial_goal if family == "GoalDirected" else None,
            task_timeout_seconds=self._timeout,
            semantic_input_binding_ref=binding_ref(run_id),
            human_gates=gates,
            human_review=review,
            stage_inputs=stage_inputs,
        )
        return asdict(prepared)

    async def _human_control(
        self, request_scope: str, run_id: str, family: Literal["StageGraph", "GoalDirected"]
    ) -> tuple[tuple[HumanGateSpec, ...], HumanGateSpec | None]:
        """MP-10 (SPEC-03): the manifest's `human_gate` nodes become Stage Graph gate stages;
        a Goal Loop whose acceptance requires `human` gets one review activation.

        The manifest schema names reviewers only on `human_gate` tasks, which cannot sit under
        a Goal Loop root, so a Goal Loop review borrows the reviewers its definition declares
        on any human gate, else the `owner` reviewer role (`DEFAULT_GOAL_REVIEWERS`). The
        requirement is never dropped: the review still waits for an attributable resolution
        by a principal holding that role.
        """

        info = await self._definitions.run_subscriptions(request_scope, run_id)
        if info is None:
            return (), None
        definition = MissionDefinition.model_validate(info["definition"])
        if family == "StageGraph":
            return stagegraph_human_gates(definition), None
        reviewers = tuple(
            dict.fromkeys(
                reviewer
                for node in _human_gate_nodes(definition)
                for reviewer in _gate_reviewers(node)
            )
        )
        return (), goal_human_review(definition, reviewers=reviewers or DEFAULT_GOAL_REVIEWERS)

    async def bind(
        self,
        request_scope: str,
        run_id: str,
        *,
        family: Literal["StageGraph", "GoalDirected"],
    ) -> dict[str, OperationExecutionRequest]:
        """Freeze the run's templates under its binding ref (once) and return them."""

        reference = binding_ref(run_id)
        projection = await self._run_control.get_run(request_scope, run_id)
        configuration = await self._control_plane.retrieve_for_admission(
            projection.effective_configuration_digest
        )
        admitted = AdmittedLaunch.from_configuration(request_scope, run_id, configuration)
        expected = (
            "StageGraph" if isinstance(admitted.blueprint, StageGraphBlueprint) else "GoalDirected"
        )
        if family != expected:
            raise ValueError(f"run {run_id} admitted a {expected} blueprint, not {family}")
        frozen = await self._frozen(admitted, reference)
        if frozen is not None:
            return frozen
        info = await self._definitions.run_subscriptions(request_scope, run_id)
        if info is None:
            raise ManifestStartUnavailable(f"run {run_id} was not submitted from a manifest")
        definition = MissionDefinition.model_validate(info["definition"])
        recorded_at = admitted.compiled_at
        rows = await self._rows(definition)
        if isinstance(admitted.blueprint, StageGraphBlueprint):
            templates = self._resolver.stage_templates(definition, admitted, rows=rows)
            await self._stages.persist_templates(
                request_scope=request_scope,
                semantic_input_binding_ref=reference,
                templates=templates,
                recorded_at=recorded_at,
            )
            return templates
        templates = self._resolver.goal_templates(definition, admitted, rows=rows)
        await self._goals.persist_templates(
            request_scope=request_scope,
            semantic_input_binding_ref=reference,
            executor=templates["executor"],
            verifier=templates["verifier"],
            recorded_at=recorded_at,
        )
        return templates

    async def _rows(self, definition: MissionDefinition) -> Mapping[str, ResolvedCapability]:
        pins = provider_capability_pins(definition)
        if not pins or self._projection_rows is None:
            return {}
        return await self._projection_rows(pins)

    async def _frozen(
        self, admitted: AdmittedLaunch, reference: str
    ) -> dict[str, OperationExecutionRequest] | None:
        if isinstance(admitted.blueprint, StageGraphBlueprint):
            keys = {
                template_key(stage.stage_id, slot.operation_slot_id, variant.operation_variant_id)
                for stage in admitted.blueprint.stages
                for slot in stage.operation_slots
                for variant in slot.allowed_variants
            }
            existing = await self._stages.list_templates(
                request_scope=admitted.request_scope, semantic_input_binding_ref=reference
            )
            return existing if keys and keys <= set(existing) else None
        found: dict[str, OperationExecutionRequest] = {}
        for role in GOAL_ROLES:
            try:
                found[role] = await self._goals.get_template(
                    semantic_input_binding_ref=reference,
                    operation_role=role,
                    request_scope=admitted.request_scope,
                    run_id=admitted.run_id,
                )
            except ValueError:
                return None
        return found


class ManifestChainLaunchInputs:
    """The chain relay's launch input port over the manifest launch author."""

    def __init__(self, author: LaunchInputPort) -> None:
        self._author = author

    async def family_input(self, intent: ChainIntent) -> dict[str, Any]:
        if intent.family is None:
            raise ValueError("a start_run intent names the consumer's family")
        return await self._author.family_input(
            request_scope=intent.request_scope,
            run_id=intent.run_key,
            family=intent.family,
            initial_goal=intent.initial_goal,
        )
