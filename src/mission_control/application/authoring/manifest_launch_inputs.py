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

from mission_control.application.authoring.manifest_submit import (
    LaunchInputPort,
    ManifestStartUnavailable,
)
from mission_control.application.authoring.service import ControlPlaneService
from mission_control.application.chains.relay import ChainIntent
from mission_control.application.execution.service import RunControlService
from mission_control.application.programs.human_gates import (
    goal_human_review,
    stagegraph_human_gates,
)
from mission_control.application.programs.service import (
    GoalDirectedLaunchService,
    StageGraphLaunchService,
    WorkflowLaunchDispatcher,
)
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_digest
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
from mission_control.domain.authoring.mission_definition import (
    DefinitionCapability,
    DefinitionNode,
    MissionDefinition,
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
from mission_control.domain.execution.materialization import compile_deep_agent_execution_binding
from mission_control.domain.programs.human_gate import HumanGateSpec

LAUNCH_BINDINGS_SCHEMA: Literal["mc.manifest_launch_bindings.v1"] = "mc.manifest_launch_bindings.v1"
DEEP_AGENTS_LANE = "deep_agents"
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


class WorkspaceProvision(_File):
    """The sandbox workspace every manifest template runs in (provider and image digests)."""

    provider: str = Field(min_length=1)
    runtime_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    image_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    package_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    environment_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    network_policy: Literal["none", "allowlisted"] = "none"


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
    schema_version: Literal["mc.manifest_launch_bindings.v1"] = LAUNCH_BINDINGS_SCHEMA
    deep_agents: DeepAgentScaffold
    model_profiles: dict[str, DeepAgentModelComponent] = Field(default_factory=dict)
    sandbox_profiles: dict[str, DeepAgentSandboxComponent] = Field(default_factory=dict)
    capabilities: dict[str, CapabilityComponentBinding] = Field(default_factory=dict)

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

    # -- templates ----------------------------------------------------------------------------

    def stage_templates(
        self, definition: MissionDefinition, admitted: AdmittedLaunch
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
            resolved = self.node(
                definition,
                path,
                verifier=False,
                operation_binding_refs=admitted.operation_binding_refs,
            )
            workspace = self._workspace(
                admitted,
                namespace=f"workspace-namespace:{{run_id}}:stage:{stage.stage_id}",
                workspace_id=f"workspace:{{run_id}}:stage:{stage.stage_id}",
                owner=WorkspaceOwner(
                    kind=WorkspaceOwnerKind.STAGE, owner_id=f"stage:{stage.stage_id}"
                ),
            )
            objective = _stage_objective(definition, node)
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
        self, definition: MissionDefinition, admitted: AdmittedLaunch
    ) -> dict[str, OperationExecutionRequest]:
        blueprint = admitted.blueprint
        if not isinstance(blueprint, GoalDirectedBlueprint):
            raise ValueError("the admitted blueprint is not a Goal Loop")
        root = definition.program
        base_id = blueprint.logical_id.removesuffix(".blueprint")
        templates: dict[str, OperationExecutionRequest] = {}
        for role in GOAL_ROLES:
            resolved = self.node(
                definition,
                (root,),
                verifier=role == "verifier",
                operation_binding_refs=admitted.operation_binding_refs,
            )
            templates[role] = self._request(
                admitted,
                key=role,
                resolved=resolved,
                objective=(
                    f"Mission {definition.mission_key} Goal Loop {role}: "
                    f"{blueprint.objective_contract}"
                ),
                source=f"input:{root.pointer}/{role}",
                workspace=self._workspace(
                    admitted,
                    namespace="workspace-namespace:{run_id}",
                    workspace_id=f"workspace:{{run_id}}:{role}",
                    owner=WorkspaceOwner(kind=WorkspaceOwnerKind.RUN, owner_id="goal-template"),
                ),
                operation_contract_ref=f"operation:{base_id}:goal-{role}@1",
                output_schema=self._bindings.deep_agents.goal_output_schemas.get(role),
            )
        return templates

    def _workspace(
        self,
        admitted: AdmittedLaunch,
        *,
        namespace: str,
        workspace_id: str,
        owner: WorkspaceOwner,
    ) -> WorkspaceContract:
        contract = admitted.workspace_contract
        provision = self._bindings.deep_agents.workspace
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
    ) -> None:
        self._resolver = resolver
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
        prepared = await self._dispatcher.prepare(
            request_scope,
            run_id,
            initial_goal=initial_goal if family == "GoalDirected" else None,
            task_timeout_seconds=self._timeout,
            semantic_input_binding_ref=binding_ref(run_id),
            human_gates=gates,
            human_review=review,
        )
        return asdict(prepared)

    async def _human_control(
        self, request_scope: str, run_id: str, family: Literal["StageGraph", "GoalDirected"]
    ) -> tuple[tuple[HumanGateSpec, ...], HumanGateSpec | None]:
        """MP-10 (SPEC-03): the manifest's `human_gate` nodes become Stage Graph gate stages;
        a Goal Loop whose acceptance requires `human` gets one review activation.

        The frozen manifest schema names reviewers only on `human_gate` tasks, so a Goal
        Loop review borrows the reviewers its definition declares on any human gate. A Goal
        Loop that requires a human review and names no reviewer anywhere cannot start: the
        requirement would otherwise be dropped silently (a reported manifest-schema gap).
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
        if reviewers:
            return (), goal_human_review(definition, reviewers=reviewers)
        if goal_human_review(definition, reviewers=("unassigned",)) is not None:
            raise ManifestStartUnavailable(
                f"run {run_id}: the Goal Loop acceptance requires a human review but the "
                "manifest names no reviewer (declare a human_gate task with reviewers)"
            )
        return (), None

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
        if isinstance(admitted.blueprint, StageGraphBlueprint):
            templates = self._resolver.stage_templates(definition, admitted)
            await self._stages.persist_templates(
                request_scope=request_scope,
                semantic_input_binding_ref=reference,
                templates=templates,
                recorded_at=recorded_at,
            )
            return templates
        templates = self._resolver.goal_templates(definition, admitted)
        await self._goals.persist_templates(
            request_scope=request_scope,
            semantic_input_binding_ref=reference,
            executor=templates["executor"],
            verifier=templates["verifier"],
            recorded_at=recorded_at,
        )
        return templates

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
