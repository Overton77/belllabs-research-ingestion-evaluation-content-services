"""``MissionDefinition@1``: the typed definition a Mission Manifest lowers to (SPEC-05).

The committed Revision is this definition, never the YAML. :func:`manifest_to_definition`
is pure and deterministic: the same mission block yields the same canonical digest. Capability
``search`` entries are carried as unresolved requests; the compile service (FT-E2) attaches
exact pins before anything is committed, and :attr:`MissionDefinition.is_resolved` tells the
two states apart.
"""

from __future__ import annotations

from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.authoring.canonical import stable_json_digest
from mission_control.domain.authoring.manifest import (
    DEFAULT_SIDE_EFFECTS,
    Acceptance,
    AgentEntry,
    AnyProgramNode,
    Behavior,
    CapabilityEntry,
    CapabilityEntryBase,
    CapabilityKind,
    CapabilityReference,
    CommandKind,
    ContextEntry,
    Environment,
    ExpansionTier,
    GoalLoopNode,
    Governors,
    HookEntry,
    Lane,
    MissionBlock,
    MissionEnvironments,
    MissionManifest,
    NodeEnvironment,
    NotificationDeclaration,
    OutputDeclaration,
    ProgramNodeBase,
    QuorumJoin,
    SideEffect,
    StageGraphNode,
    SubscriptionDeclaration,
    manifest_digest,
    resolve_mission,
)
from mission_control.domain.authoring.manifest_v2 import (
    AuthSelection,
    ContinuationPolicy,
    EnvironmentV2,
    ExecutionEnvironmentSelection,
    RequiredFeatures,
    WorkspacePolicy,
    WorkspaceSelectionV2,
)

MISSION_DEFINITION_SCHEMA_VERSION: Final = "mc.mission_definition.v1"


class DefinitionContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --- Capability requests ---------------------------------------------------------------


class CapabilityPin(DefinitionContract):
    """An exact catalog pin; ``digest`` is absent only for an authored ``@latest``."""

    capability_id: str = Field(min_length=1)
    version: str = Field(min_length=1)
    digest: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")


class CapabilitySearchRequest(DefinitionContract):
    query: str = Field(min_length=1)
    require: tuple[Lane, ...] = ()


CapabilityRole = Literal[
    "capability", "skill", "agent", "hook", "plugin", "context", "model", "assessment"
]


class DefinitionCapability(DefinitionContract):
    """One capability the definition needs, located by its manifest pointer."""

    pointer: str
    role: CapabilityRole
    alias: str | None = None
    kind: CapabilityKind | None = None
    pin: CapabilityPin | None = None
    search: CapabilitySearchRequest | None = None
    resolved: CapabilityPin | None = None
    attributes: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _pin_or_search(self) -> DefinitionCapability:
        if (self.pin is None) == (self.search is None):
            raise ValueError("a capability request is exactly one of pin | search")
        return self

    @property
    def is_exact(self) -> bool:
        exact = self.resolved or self.pin
        return exact is not None and exact.digest is not None


# --- Acceptance ------------------------------------------------------------------------


class AcceptanceExpression(DefinitionContract):
    """Closed typed acceptance AST (ADR-0010): no free text, no code."""

    op: Literal["assessment", "human", "schema", "all", "any"]
    capability_pointer: str | None = None
    threshold: Decimal | None = None
    decision: Literal["review_accept", "approved"] | None = None
    schema_ref: str | None = None
    operands: tuple[AcceptanceExpression, ...] = ()

    @model_validator(mode="after")
    def _shape(self) -> AcceptanceExpression:
        expected = {
            "assessment": self.capability_pointer is not None and self.threshold is not None,
            "human": self.decision is not None,
            "schema": self.schema_ref is not None,
            "all": bool(self.operands),
            "any": bool(self.operands),
        }
        if not expected[self.op]:
            raise ValueError(f"acceptance operator {self.op} is missing its operand")
        return self


# --- Goals, objectives, criteria -------------------------------------------------------


class DefinitionGoal(DefinitionContract):
    key: str
    description: str
    importance: Literal["primary", "secondary", "optional"]


class DefinitionObjective(DefinitionContract):
    key: str
    goal_key: str
    description: str
    parent: str | None = None


class DefinitionCriterion(DefinitionContract):
    key: str
    goal_key: str
    description: str
    evidence: tuple[str, ...]
    acceptance: AcceptanceExpression


class DefinitionInput(DefinitionContract):
    node_key: str
    name: str
    source: Literal["from", "artifact", "value"]
    ref: str | None = None
    value: Any = None
    expand: ExpansionTier
    required: bool


class DefinitionOutput(DefinitionContract):
    node_key: str
    name: str
    schema_ref: str
    required: bool


ENVIRONMENT_SELECTIONS_SCHEMA_VERSION: Final = "mc.environment_selections.v1"


class EnvironmentSelections(DefinitionContract):
    """The resolved ``mission/v2`` selections of one node role's effective Environment.

    ``DefinitionNode.environment`` is declared as the v1 Environment, so a committed
    definition stores (and digests) only its v1 fields; the v2 selections travel here, beside
    it, so the launch author reads exactly what compile admitted. Absent on every
    ``mission/v1`` definition (left out of dumps and digests: v1 digests are unchanged).
    """

    schema_version: Literal["mc.environment_selections.v1"] = ENVIRONMENT_SELECTIONS_SCHEMA_VERSION
    auth: AuthSelection | None = None
    execution_environment: ExecutionEnvironmentSelection | None = None
    workspace_policy: WorkspacePolicy | None = None
    continuation: ContinuationPolicy | None = None
    requires: RequiredFeatures | None = None

    @classmethod
    def of(cls, environment: Environment) -> EnvironmentSelections | None:
        if not isinstance(environment, EnvironmentV2):
            return None
        workspace = environment.workspace
        return cls(
            auth=environment.auth,
            execution_environment=environment.execution_environment,
            workspace_policy=workspace.policy
            if isinstance(workspace, WorkspaceSelectionV2)
            else None,
            continuation=environment.continuation,
            requires=environment.requires,
        )


def _absent(value: object) -> bool:
    return value is None


# An additive field: left out of dumps (`exclude_if`) and of the canonical digests
# (`digest_omit_default`) while it holds its default, so every v1 definition keeps its bytes.
_ADDITIVE: Final[dict[str, Any]] = {"digest_omit_default": True}


class DefinitionNode(DefinitionContract):
    key: str
    behavior: Behavior
    pointer: str
    lane: Lane
    objectives: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    join: Literal["all", "any"] | int = "all"
    environment: Environment
    field_provenance: dict[str, str]
    verifier_environment: Environment | None = None
    inputs: tuple[DefinitionInput, ...] = ()
    outputs: tuple[DefinitionOutput, ...] = ()
    completion: AcceptanceExpression | None = None
    body: dict[str, Any] = Field(default_factory=dict)
    nodes: tuple[DefinitionNode, ...] = ()
    # mission/v2 (MP-01/MP-02): the node's and its verifier's v2 environment selections.
    selections: EnvironmentSelections | None = Field(
        default=None, exclude_if=_absent, json_schema_extra=_ADDITIVE
    )
    verifier_selections: EnvironmentSelections | None = Field(
        default=None, exclude_if=_absent, json_schema_extra=_ADDITIVE
    )


# --- Policies, budget, completion --------------------------------------------------------


class DefinitionPolicies(DefinitionContract):
    side_effects: tuple[SideEffect, ...]
    governors: Governors
    commands_allowed: tuple[CommandKind, ...] | None = None
    subscriptions: tuple[SubscriptionDeclaration, ...] = ()
    notifications: NotificationDeclaration | None = None
    chain_autostart: bool | None = None


class DefinitionBudget(DefinitionContract):
    usd: Decimal | None = None
    tokens: int | None = None
    wall_clock_seconds: int | None = None
    tool_calls: int | None = None


class GoalCompletion(DefinitionContract):
    goal_key: str
    importance: Literal["primary", "secondary", "optional"]
    required: bool
    expression: AcceptanceExpression


class CompletionContract(DefinitionContract):
    mission_acceptance: Literal["all_required_goals_accepted"] = "all_required_goals_accepted"
    goals: tuple[GoalCompletion, ...]


class MissionDefinition(DefinitionContract):
    """``MissionDefinition@1``."""

    schema_version: Literal["mc.mission_definition.v1"] = MISSION_DEFINITION_SCHEMA_VERSION
    mission_key: str
    title: str
    application: Literal["biotech", "ai-engineer"]
    domain_pack: str | None = None
    description: str | None = None
    manifest_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    manifest_pointer: str
    goals: tuple[DefinitionGoal, ...]
    objectives: tuple[DefinitionObjective, ...] = ()
    criteria: tuple[DefinitionCriterion, ...]
    inputs: tuple[DefinitionInput, ...] = ()
    capabilities: tuple[DefinitionCapability, ...] = ()
    program: DefinitionNode
    policies: DefinitionPolicies
    budget: DefinitionBudget
    completion_contract: CompletionContract

    @property
    def is_resolved(self) -> bool:
        """True when every capability request carries an exact digest-pinned target."""

        return all(capability.is_exact for capability in self.capabilities)

    @property
    def digest(self) -> str:
        """Canonical, set-order-stable digest (``stable_json_digest``)."""

        return stable_json_digest(self)


AcceptanceExpression.model_rebuild()
DefinitionNode.model_rebuild()


# ---------------------------------------------------------------------------
# Lowering
# ---------------------------------------------------------------------------

_COMMON_NODE_FIELDS = frozenset(ProgramNodeBase.model_fields) | {"behavior"}


def _capability(
    reference: CapabilityEntryBase | ContextEntry, pointer: str, role: CapabilityRole
) -> DefinitionCapability | None:
    attributes: dict[str, Any] = {}
    if isinstance(reference, CapabilityEntry) and reference.tools is not None:
        attributes["tools"] = list(reference.tools)
    if isinstance(reference, AgentEntry) and reference.overlay is not None:
        attributes["overlay"] = reference.overlay.authored()
    if isinstance(reference, HookEntry):
        attributes["events"] = [event.value for event in reference.events]
        attributes["fail_closed"] = reference.fail_closed
        if reference.matcher is not None:
            attributes["matcher"] = reference.matcher
    if isinstance(reference, ContextEntry):
        if reference.pin is None and reference.search is None:
            return None
        if reference.expand is not None:
            attributes["expand"] = reference.expand.value
        require: tuple[Lane, ...] = ()
    else:
        require = reference.require or ()
    pin: CapabilityPin | None = None
    if reference.pin is not None:
        capability_id, _, rest = reference.pin.partition("@")
        version, _, digest = rest.partition("#")
        pin = CapabilityPin(capability_id=capability_id, version=version, digest=digest or None)
    return DefinitionCapability(
        pointer=pointer,
        role=role,
        alias=reference.as_,
        kind=reference.kind,
        pin=pin,
        search=(
            CapabilitySearchRequest(query=reference.search, require=require)
            if reference.search is not None
            else None
        ),
        attributes=attributes,
    )


def _environment_capabilities(
    environment: Environment | None, pointer: str
) -> list[DefinitionCapability]:
    if environment is None:
        return []
    found: list[DefinitionCapability | None] = []
    if environment.model is not None and isinstance(environment.model.profile, CapabilityReference):
        found.append(_capability(environment.model.profile, f"{pointer}/model/profile", "model"))
    if environment.workspace is not None:
        for index, item in enumerate(environment.workspace.context or ()):
            if isinstance(item, ContextEntry):
                found.append(_capability(item, f"{pointer}/workspace/context/{index}", "context"))
        for index, skill in enumerate(environment.workspace.skills or ()):
            found.append(_capability(skill, f"{pointer}/workspace/skills/{index}", "skill"))
    groups: tuple[tuple[str, tuple[CapabilityEntryBase, ...] | None, CapabilityRole], ...] = (
        ("capabilities", environment.capabilities, "capability"),
        ("agents", environment.agents, "agent"),
        ("hooks", environment.hooks, "hook"),
        ("plugins", environment.plugins, "plugin"),
    )
    for name, entries, role in groups:
        for index, entry in enumerate(entries or ()):
            found.append(_capability(entry, f"{pointer}/{name}/{index}", role))
    return [item for item in found if item is not None]


def _acceptance_expression(
    acceptance: Acceptance, pointer: str, capabilities: list[DefinitionCapability]
) -> AcceptanceExpression:
    if acceptance.assessment is not None:
        capability_pointer = f"{pointer}/assessment/capability"
        capability = _capability(acceptance.assessment.capability, capability_pointer, "assessment")
        if capability is not None:
            capabilities.append(capability)
        return AcceptanceExpression(
            op="assessment",
            capability_pointer=capability_pointer,
            threshold=acceptance.assessment.threshold,
        )
    if acceptance.human is not None:
        return AcceptanceExpression(op="human", decision=acceptance.human)
    if acceptance.schema_ is not None:
        return AcceptanceExpression(op="schema", schema_ref=acceptance.schema_)
    op: Literal["all", "any"] = "all" if acceptance.all_ is not None else "any"
    operands = acceptance.all_ if acceptance.all_ is not None else acceptance.any_
    return AcceptanceExpression(
        op=op,
        operands=tuple(
            _acceptance_expression(item, f"{pointer}/{op}/{index}", capabilities)
            for index, item in enumerate(operands or ())
        ),
    )


def _node_body(node: AnyProgramNode) -> dict[str, Any]:
    authored = node.model_dump(mode="json", by_alias=True, exclude_unset=False)
    body = {
        key: value
        for key, value in sorted(authored.items())
        if key not in _COMMON_NODE_FIELDS and key not in {"nodes", "verifier"}
    }
    if isinstance(node, GoalLoopNode) and node.verifier is not None:
        body["verifier"] = {"independent": node.verifier.independent}
    return body


def _join(node: AnyProgramNode) -> Literal["all", "any"] | int:
    return node.join.quorum if isinstance(node.join, QuorumJoin) else node.join


def _lower_node(
    node: AnyProgramNode,
    pointer: str,
    environments: Mapping[tuple[str, str], NodeEnvironment],
    capabilities: list[DefinitionCapability],
) -> DefinitionNode:
    resolved = environments[(node.key, "node")]
    verifier = environments.get((node.key, "verifier"))
    capabilities.extend(_environment_capabilities(node.environment, f"{pointer}/environment"))
    if isinstance(node, GoalLoopNode) and node.verifier is not None:
        capabilities.extend(
            _environment_capabilities(node.verifier.environment, f"{pointer}/verifier/environment")
        )
    completion = (
        _acceptance_expression(
            node.completion.acceptance, f"{pointer}/completion/acceptance", capabilities
        )
        if node.completion is not None
        else None
    )
    stop = getattr(node, "stop", None)
    body = _node_body(node)
    if isinstance(stop, Acceptance):
        body["stop"] = _acceptance_expression(stop, f"{pointer}/stop", capabilities).model_dump(
            mode="json"
        )
    children: tuple[DefinitionNode, ...] = ()
    if isinstance(node, StageGraphNode):
        children = tuple(
            _lower_node(child, f"{pointer}/nodes/{index}", environments, capabilities)
            for index, child in enumerate(node.nodes)
        )
    return DefinitionNode(
        key=node.key,
        behavior=Behavior(node.behavior),
        pointer=pointer,
        lane=resolved.lane,
        objectives=node.objectives,
        depends_on=node.depends_on,
        join=_join(node),
        environment=resolved.effective_environment,
        field_provenance=resolved.field_provenance,
        verifier_environment=verifier.effective_environment if verifier is not None else None,
        inputs=tuple(_inputs(node)),
        outputs=tuple(_outputs(node.key, node.outputs)),
        completion=completion,
        body=body,
        nodes=children,
        selections=EnvironmentSelections.of(resolved.effective_environment),
        verifier_selections=(
            EnvironmentSelections.of(verifier.effective_environment)
            if verifier is not None
            else None
        ),
    )


def _inputs(node: AnyProgramNode) -> list[DefinitionInput]:
    result: list[DefinitionInput] = []
    for binding in node.inputs:
        if binding.from_ is not None:
            source: Literal["from", "artifact", "value"] = "from"
            ref: str | None = binding.from_
        elif binding.artifact is not None:
            source, ref = "artifact", binding.artifact
        else:
            source, ref = "value", None
        result.append(
            DefinitionInput(
                node_key=node.key,
                name=binding.name,
                source=source,
                ref=ref,
                value=binding.value if source == "value" else None,
                expand=binding.expand,
                required=binding.required,
            )
        )
    return result


def _outputs(node_key: str, outputs: tuple[OutputDeclaration, ...]) -> list[DefinitionOutput]:
    return [
        DefinitionOutput(
            node_key=node_key, name=item.name, schema_ref=item.schema_, required=item.required
        )
        for item in outputs
    ]


def _flatten(node: DefinitionNode) -> list[DefinitionNode]:
    result = [node]
    for child in node.nodes:
        result.extend(_flatten(child))
    return result


def manifest_to_definition(
    block: MissionBlock,
    *,
    pointer: str,
    digest: str,
    environments: MissionEnvironments | None = None,
) -> MissionDefinition:
    """Lower one mission block to ``MissionDefinition@1`` (pure, deterministic).

    Structural blockers are the caller's concern (:func:`resolve_environments`); lowering
    never fails on a mission that parsed, so a report can always show the definition.
    """

    if environments is None:
        environments, _, _ = resolve_mission(block, pointer)
    by_key: dict[tuple[str, str], NodeEnvironment] = {
        (node.node_key, node.role): node for node in environments.nodes
    }
    capabilities = _environment_capabilities(block.environment, f"{pointer}/environment")
    goals: list[DefinitionGoal] = []
    objectives: list[DefinitionObjective] = []
    criteria: list[DefinitionCriterion] = []
    completions: list[GoalCompletion] = []
    for goal_index, goal in enumerate(block.goals):
        goal_pointer = f"{pointer}/goals/{goal_index}"
        goals.append(
            DefinitionGoal(key=goal.key, description=goal.description, importance=goal.importance)
        )
        objectives.extend(
            DefinitionObjective(
                key=item.key, goal_key=goal.key, description=item.description, parent=item.parent
            )
            for item in goal.objectives
        )
        goal_criteria = [
            DefinitionCriterion(
                key=criterion.key,
                goal_key=goal.key,
                description=criterion.description,
                evidence=criterion.evidence,
                acceptance=_acceptance_expression(
                    criterion.acceptance,
                    f"{goal_pointer}/criteria/{criterion_index}/acceptance",
                    capabilities,
                ),
            )
            for criterion_index, criterion in enumerate(goal.criteria)
        ]
        criteria.extend(goal_criteria)
        completions.append(
            GoalCompletion(
                goal_key=goal.key,
                importance=goal.importance,
                required=goal.importance == "primary",
                expression=AcceptanceExpression(
                    op="all", operands=tuple(item.acceptance for item in goal_criteria)
                ),
            )
        )
    program = _lower_node(block.program, f"{pointer}/program", by_key, capabilities)
    mission_environment = block.environment
    budget = mission_environment.budget
    controls = block.controls
    return MissionDefinition(
        mission_key=block.key,
        title=block.title,
        application=block.application,
        domain_pack=block.domain_pack,
        description=block.description,
        manifest_digest=digest,
        manifest_pointer=pointer,
        goals=tuple(goals),
        objectives=tuple(objectives),
        criteria=tuple(criteria),
        inputs=tuple(item for node in _flatten(program) for item in node.inputs),
        capabilities=tuple(capabilities),
        program=program,
        policies=DefinitionPolicies(
            side_effects=mission_environment.side_effects or DEFAULT_SIDE_EFFECTS,
            governors=mission_environment.governors or Governors(),
            commands_allowed=controls.commands_allowed if controls is not None else None,
            subscriptions=controls.subscriptions if controls is not None else (),
            notifications=controls.notifications if controls is not None else None,
            chain_autostart=controls.chain_autostart if controls is not None else None,
        ),
        budget=DefinitionBudget(
            usd=budget.usd if budget is not None else None,
            tokens=budget.tokens if budget is not None else None,
            wall_clock_seconds=budget.wall_clock if budget is not None else None,
            tool_calls=budget.tool_calls if budget is not None else None,
        ),
        completion_contract=CompletionContract(goals=tuple(completions)),
    )


def manifest_to_definitions(
    manifest: MissionManifest, document: dict[str, Any]
) -> tuple[MissionDefinition, ...]:
    """Every mission of a manifest (one, or each chain member) as ``MissionDefinition@1``."""

    digest = manifest_digest(document)
    return tuple(
        manifest_to_definition(block, pointer=pointer, digest=digest)
        for pointer, block in manifest.mission_blocks()
    )
