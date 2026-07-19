from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


class Contract(BaseModel):
    # API payloads arrive as JSON primitives, while executable shape remains extra-forbidden
    # and immutable after validation.
    model_config = ConfigDict(extra="forbid", frozen=True)


class DefinitionKind(StrEnum):
    WORKFLOW_TYPE = "workflow_type"
    BLUEPRINT = "blueprint"
    CONTROL_PROFILE = "control_profile"
    RUNTIME_PROFILE = "runtime_profile"
    WORKSPACE_TEMPLATE = "workspace_template"
    EVALUATION_PROFILE = "evaluation_profile"
    WORKFLOW_CONFIGURATION = "workflow_configuration"
    # Reference-only boundaries until the owning capability specifications land.
    MEMORY_POLICY = "memory_policy"
    AGENT_PROFILE = "agent_profile"
    CAPABILITY_SELECTION = "capability_selection"
    PROMPT = "prompt"
    SKILL = "skill"
    MCP_SERVER = "mcp_server"
    MCP_TOOL = "mcp_tool"
    PLUGIN_PACKAGE = "plugin_package"


class ExactDefinitionRef(Contract):
    kind: DefinitionKind
    logical_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    revision: int = Field(ge=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class AliasRef(Contract):
    kind: DefinitionKind
    logical_id: str = Field(min_length=1)
    alias: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")


class AliasBinding(Contract):
    alias_ref: AliasRef
    target: ExactDefinitionRef
    moved_at: AwareDatetime
    moved_by: str

    @model_validator(mode="after")
    def identity_matches_target(self) -> AliasBinding:
        if (
            self.alias_ref.kind != self.target.kind
            or self.alias_ref.logical_id != self.target.logical_id
        ):
            raise ValueError("alias resolution evidence must match its exact target identity")
        return self


class DefinitionBase(Contract):
    schema_version: Literal["1"] = "1"
    logical_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)


class NamespacedExtension(Contract):
    namespace: str = Field(pattern=r"^[a-z][a-z0-9]*(?:\.[a-z0-9-]+)+$")
    schema_version: str = Field(min_length=1)
    discriminator: str = Field(min_length=1)
    payload: dict[str, object]

    @field_validator("payload")
    @classmethod
    def payload_cannot_embed_secrets(cls, value: dict[str, object]) -> dict[str, object]:
        sensitive_fragments = ("apikey", "credential", "password", "secret", "token")

        def inspect(subject: object) -> None:
            if isinstance(subject, dict):
                for key, item in subject.items():
                    normalized_key = re.sub(r"[^a-z0-9]", "", str(key).lower())
                    if any(fragment in normalized_key for fragment in sensitive_fragments):
                        try:
                            SecretRef.model_validate(item)
                        except (ValueError, TypeError):
                            raise ValueError(
                                "extension payloads may contain typed SecretRef values only"
                            ) from None
                        continue
                    inspect(item)
            elif isinstance(subject, list | tuple):
                for item in subject:
                    inspect(item)

        inspect(value)
        return value


class ExtensionIdentity(Contract):
    namespace: str = Field(pattern=r"^[a-z][a-z0-9]*(?:\.[a-z0-9-]+)+$")
    schema_version: str = Field(min_length=1)
    discriminator: str = Field(min_length=1)


class SecretRef(Contract):
    provider: Literal["aws-secrets-manager", "vault", "environment"]
    key: str = Field(min_length=1)
    version: str | None = None

    @model_validator(mode="before")
    @classmethod
    def forbid_secret_values(cls, value: object) -> object:
        if isinstance(value, dict) and any(
            key.lower() in {"value", "secret", "token", "password"} for key in value
        ):
            raise ValueError("secret values are forbidden; provide a SecretRef")
        return value


class BudgetCeiling(Contract):
    dimensions: dict[str, int] = Field(default_factory=dict)

    @field_validator("dimensions")
    @classmethod
    def validate_dimensions(cls, value: dict[str, int]) -> dict[str, int]:
        if any(not key or amount < 0 for key, amount in value.items()):
            raise ValueError("budget dimensions require names and non-negative ceilings")
        return value


class AuthorityCeiling(Contract):
    capabilities: frozenset[str] = Field(default_factory=frozenset)
    budgets: BudgetCeiling = Field(default_factory=BudgetCeiling)
    max_concurrency: int = Field(default=1, ge=1)


class EnvironmentAvailability(Contract):
    capabilities: frozenset[str] = Field(default_factory=frozenset)
    runtime_bindings: frozenset[str] = Field(default_factory=frozenset)
    secret_refs: tuple[SecretRef, ...] = ()


class AvailabilityRequirement(Contract):
    capability: str = Field(min_length=1)
    when_unavailable: Literal["reject", "degrade", "omit"] = "reject"
    decision_reason: str = Field(min_length=1)


class RunInputManifestRef(Contract):
    manifest_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class WorkspaceSlot(Contract):
    name: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    path: str = Field(min_length=1)
    access: Literal["read_only", "exclusive_write", "shared_write"]
    purpose: str = Field(min_length=1)


class WorkflowWorkspaceContract(Contract):
    slots: tuple[WorkspaceSlot, ...] = ()

    @model_validator(mode="after")
    def unique_slots(self) -> WorkflowWorkspaceContract:
        names = [slot.name for slot in self.slots]
        if len(names) != len(set(names)):
            raise ValueError("workspace slot names must be unique")
        return self


class StageCyclePolicy(Contract):
    """Application-authored bounds for semantic rework of one stage."""

    max_cycles: int = Field(ge=1)
    evaluation_contract_ref: str = Field(min_length=1)
    objective_contract_ref: str = Field(min_length=1)
    reservation: dict[str, int] = Field(default_factory=dict)

    @field_validator("reservation")
    @classmethod
    def validate_reservation(cls, value: dict[str, int]) -> dict[str, int]:
        if any(not dimension or amount < 0 for dimension, amount in value.items()):
            raise ValueError("stage cycle reservations require names and non-negative amounts")
        if value.get("stage.cycles", 0) < 1:
            raise ValueError("stage cycle reservations require at least one stage.cycles unit")
        return value


class WorkflowCyclePolicy(Contract):
    """Bounds whole-workflow evaluation without adding dependency back-edges."""

    max_cycles: int = Field(ge=1)
    evaluation_contract_ref: str = Field(min_length=1)
    objective_contract_ref: str = Field(min_length=1)
    reservation: dict[str, int] = Field(default_factory=dict)

    @field_validator("reservation")
    @classmethod
    def validate_reservation(cls, value: dict[str, int]) -> dict[str, int]:
        if any(not dimension or amount < 0 for dimension, amount in value.items()):
            raise ValueError("workflow cycle reservations require names and non-negative amounts")
        if value.get("workflow.cycles", 0) < 1:
            raise ValueError(
                "workflow cycle reservations require at least one workflow.cycles unit"
            )
        return value


class StageNode(Contract):
    stage_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    depends_on: frozenset[str] = Field(default_factory=frozenset)
    dependency_classes: dict[str, Literal["required", "degradable", "optional", "advisory"]] = (
        Field(default_factory=dict)
    )
    join_policy: Literal["all", "any", "minimum"] = "all"
    minimum_dependencies: int | None = Field(default=None, ge=1)
    completion_class: Literal["required", "degradable", "optional", "advisory"] = "required"
    skip_policy: Literal["never", "when_dependencies_unsatisfied"] = "never"
    fairness_group: str = Field(default="default", pattern=r"^[a-z][a-z0-9_-]*$")
    fairness_priority: int = Field(default=0, ge=0)
    concurrency_slots: int = Field(default=1, ge=1)
    reservation: dict[str, int] = Field(default_factory=dict)
    stage_cycle_policy: StageCyclePolicy | None = None
    obligation_refs: frozenset[str] = Field(default_factory=frozenset)
    output_slots: frozenset[str] = Field(default_factory=frozenset)
    variant_names: frozenset[str] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def validate_execution_policy(self) -> StageNode:
        if not set(self.dependency_classes) <= self.depends_on:
            raise ValueError("dependency classes may reference only declared dependencies")
        if self.join_policy == "minimum":
            if self.minimum_dependencies is None:
                raise ValueError("minimum joins require minimum_dependencies")
            if self.minimum_dependencies > len(self.depends_on):
                raise ValueError("minimum_dependencies exceeds declared dependencies")
        elif self.minimum_dependencies is not None:
            raise ValueError("minimum_dependencies is valid only for minimum joins")
        if any(not dimension or amount < 0 for dimension, amount in self.reservation.items()):
            raise ValueError("stage reservations require names and non-negative amounts")
        return self


class StageGraphBlueprint(DefinitionBase):
    kind: Literal[DefinitionKind.BLUEPRINT] = DefinitionKind.BLUEPRINT
    family: Literal["StageGraph"] = "StageGraph"
    stages: tuple[StageNode, ...] = Field(min_length=1)
    declared_output_slots: frozenset[str] = Field(default_factory=frozenset)
    max_parallel_stages: int = Field(default=1, ge=1)
    workflow_evaluation_contract_ref: str | None = Field(default=None, min_length=1)
    workflow_cycle_policy: WorkflowCyclePolicy | None = None

    @model_validator(mode="after")
    def validate_graph(self) -> StageGraphBlueprint:
        ids = [stage.stage_id for stage in self.stages]
        if len(ids) != len(set(ids)):
            raise ValueError("stage identities must be unique")
        known = set(ids)
        for stage in self.stages:
            if stage.stage_id in stage.depends_on or not stage.depends_on <= known:
                raise ValueError(f"stage {stage.stage_id} has an invalid dependency")
            if stage.concurrency_slots > self.max_parallel_stages:
                raise ValueError(
                    f"stage {stage.stage_id} concurrency slots exceed the graph ceiling"
                )
            if not stage.output_slots <= self.declared_output_slots:
                raise ValueError(f"stage {stage.stage_id} uses an undeclared output slot")
        visiting: set[str] = set()
        visited: set[str] = set()
        dependencies = {stage.stage_id: stage.depends_on for stage in self.stages}

        def visit(stage_id: str) -> None:
            if stage_id in visiting:
                raise ValueError("StageGraph dependency cycle")
            if stage_id in visited:
                return
            visiting.add(stage_id)
            for dependency in dependencies[stage_id]:
                visit(dependency)
            visiting.remove(stage_id)
            visited.add(stage_id)

        for stage_id in ids:
            visit(stage_id)
        if (
            self.workflow_cycle_policy is not None
            and self.workflow_evaluation_contract_ref is not None
            and self.workflow_cycle_policy.evaluation_contract_ref
            != self.workflow_evaluation_contract_ref
        ):
            raise ValueError(
                "workflow cycle policy must use the frozen workflow evaluation contract"
            )
        return self


class GoalDirectedBlueprint(DefinitionBase):
    kind: Literal[DefinitionKind.BLUEPRINT] = DefinitionKind.BLUEPRINT
    family: Literal["GoalDirected"] = "GoalDirected"
    objective_contract: str = Field(min_length=1)
    acceptance_contract: str = Field(min_length=1)
    independent_verification_required: Literal[True] = True
    max_iterations: int = Field(ge=1)
    variant_names: frozenset[str] = Field(default_factory=frozenset)


WorkflowBlueprint = Annotated[
    StageGraphBlueprint | GoalDirectedBlueprint,
    Field(discriminator="family"),
]


class LinkedRunSlotConstraint(Contract):
    slot_id: str = Field(pattern=r"^[a-z][a-z0-9_-]*$")
    allowed_child_workflow_types: frozenset[ExactDefinitionRef]
    dependency_class: Literal[
        "required_blocking",
        "degradable_blocking",
        "degradable_nonblocking",
        "detached_advisory",
    ]
    wait_policy: Literal["wait", "continue"]
    cancellation_policy: Literal["request_cancel", "allow_continue"]
    result_admission_policy: str = Field(min_length=1)
    delegation_ceiling: AuthorityCeiling
    budget_reservation_ceiling: BudgetCeiling

    @field_validator("allowed_child_workflow_types")
    @classmethod
    def child_refs_are_workflow_types(
        cls, value: frozenset[ExactDefinitionRef]
    ) -> frozenset[ExactDefinitionRef]:
        if any(ref.kind != DefinitionKind.WORKFLOW_TYPE for ref in value):
            raise ValueError("linked-run child references must be Workflow Types")
        return value


class WorkflowTypeDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.WORKFLOW_TYPE] = DefinitionKind.WORKFLOW_TYPE
    purpose: str = Field(min_length=1)
    non_goals: frozenset[str] = Field(default_factory=frozenset)
    input_admission_contract: str = Field(min_length=1)
    invariants: frozenset[str] = Field(min_length=1)
    obligations: frozenset[str] = Field(default_factory=frozenset)
    output_contracts: frozenset[str] = Field(default_factory=frozenset)
    allowed_blueprints: frozenset[ExactDefinitionRef] = Field(min_length=1)
    allowed_control_profiles: frozenset[ExactDefinitionRef] = Field(min_length=1)
    allowed_runtime_profiles: frozenset[ExactDefinitionRef] = Field(min_length=1)
    allowed_workspace_templates: frozenset[ExactDefinitionRef] = Field(min_length=1)
    allowed_evaluation_profiles: frozenset[ExactDefinitionRef] = Field(min_length=1)
    allowed_workflow_configurations: frozenset[ExactDefinitionRef] = Field(
        default_factory=frozenset
    )
    authority_ceiling: AuthorityCeiling
    workspace_contract: WorkflowWorkspaceContract
    linked_run_slots: tuple[LinkedRunSlotConstraint, ...] = ()
    required_extensions: tuple[NamespacedExtension, ...] = ()
    allowed_overlay_extensions: frozenset[ExtensionIdentity] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def validate_ref_families(self) -> WorkflowTypeDefinition:
        expected = (
            (self.allowed_blueprints, DefinitionKind.BLUEPRINT),
            (self.allowed_control_profiles, DefinitionKind.CONTROL_PROFILE),
            (self.allowed_runtime_profiles, DefinitionKind.RUNTIME_PROFILE),
            (self.allowed_workspace_templates, DefinitionKind.WORKSPACE_TEMPLATE),
            (self.allowed_evaluation_profiles, DefinitionKind.EVALUATION_PROFILE),
            (
                self.allowed_workflow_configurations,
                DefinitionKind.WORKFLOW_CONFIGURATION,
            ),
        )
        if any(ref.kind != kind for refs, kind in expected for ref in refs):
            raise ValueError("Workflow Type contains a cross-reference of the wrong family")
        slots = [slot.slot_id for slot in self.linked_run_slots]
        if len(slots) != len(set(slots)):
            raise ValueError("linked-run slot identities must be unique")
        return self


class ControlProfileDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.CONTROL_PROFILE] = DefinitionKind.CONTROL_PROFILE
    blueprint_ref: ExactDefinitionRef
    selected_variants: frozenset[str] = Field(default_factory=frozenset)
    authority_ceiling: AuthorityCeiling
    overlayable_fields: frozenset[
        Literal["capabilities", "budgets", "max_concurrency", "variants"]
    ] = frozenset()
    strengthen_only_fields: frozenset[Literal["budgets", "max_concurrency"]] = frozenset()

    @model_validator(mode="after")
    def strengthen_only_fields_are_overlayable(self) -> ControlProfileDefinition:
        if not self.strengthen_only_fields <= self.overlayable_fields:
            raise ValueError("strengthen-only fields must also be declared overlayable")
        return self


class RuntimeProfileDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.RUNTIME_PROFILE] = DefinitionKind.RUNTIME_PROFILE
    binding: str = Field(min_length=1)
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    capability_requirements: tuple[AvailabilityRequirement, ...] = ()
    required_secrets: tuple[SecretRef, ...] = ()
    # TODO(ticket 06/09): operation and agent runtime semantics are separate pinned assets.
    operation_binding_refs: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)


class WorkspaceTemplateDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.WORKSPACE_TEMPLATE] = DefinitionKind.WORKSPACE_TEMPLATE
    slots: tuple[WorkspaceSlot, ...]
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    capability_requirements: tuple[AvailabilityRequirement, ...] = ()

    @model_validator(mode="after")
    def unique_slots(self) -> WorkspaceTemplateDefinition:
        names = [slot.name for slot in self.slots]
        if len(names) != len(set(names)):
            raise ValueError("workspace template slot names must be unique")
        return self


class EvaluationProfileDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.EVALUATION_PROFILE] = DefinitionKind.EVALUATION_PROFILE
    gate_contract_refs: frozenset[str] = Field(min_length=1)
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    capability_requirements: tuple[AvailabilityRequirement, ...] = ()


class WorkflowConfigurationDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.WORKFLOW_CONFIGURATION] = DefinitionKind.WORKFLOW_CONFIGURATION
    workflow_type_logical_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    extensions: tuple[NamespacedExtension, ...] = ()


Definition = (
    WorkflowTypeDefinition
    | StageGraphBlueprint
    | GoalDirectedBlueprint
    | ControlProfileDefinition
    | RuntimeProfileDefinition
    | WorkspaceTemplateDefinition
    | EvaluationProfileDefinition
    | WorkflowConfigurationDefinition
)


class PublishedDefinition(Contract):
    ref: ExactDefinitionRef
    definition: Definition
    published_at: AwareDatetime
    published_by: str
    retired_at: AwareDatetime | None = None


class RunOverlay(Contract):
    requested_capabilities: frozenset[str] | None = None
    budget_ceilings: dict[str, int] | None = None
    max_concurrency: int | None = Field(default=None, ge=1)
    selected_variants: frozenset[str] | None = None
    extensions: tuple[NamespacedExtension, ...] = ()


class OverlayDecisionStatus(StrEnum):
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    DEGRADED = "degraded"
    OMITTED = "omitted"


class OverlayDecision(Contract):
    field: str
    status: OverlayDecisionStatus
    requested: object | None = None
    effective: object | None = None
    reason: str


class CompilationContext(Contract):
    compilation_id: str = Field(min_length=1)
    compiled_at: AwareDatetime
    actor_id: str = Field(min_length=1)
    authority_subject_id: str = Field(min_length=1)
    authority_scope: str = Field(min_length=1)


class CompilationRequest(Contract):
    workflow_type_ref: ExactDefinitionRef
    blueprint_ref: ExactDefinitionRef
    control_profile_ref: ExactDefinitionRef
    runtime_profile_ref: ExactDefinitionRef
    workspace_template_ref: ExactDefinitionRef
    evaluation_profile_ref: ExactDefinitionRef
    workflow_configuration_ref: ExactDefinitionRef | None = None
    input_manifest: RunInputManifestRef
    overlay: RunOverlay = Field(default_factory=RunOverlay)
    caller_authority: AuthorityCeiling
    parent_authority: AuthorityCeiling | None = None
    environment: EnvironmentAvailability
    context: CompilationContext
    alias_evidence: tuple[AliasBinding, ...] = ()


class ResolvedDefinitions(Contract):
    workflow_type: WorkflowTypeDefinition
    blueprint: WorkflowBlueprint
    control_profile: ControlProfileDefinition
    runtime_profile: RuntimeProfileDefinition
    workspace_template: WorkspaceTemplateDefinition
    evaluation_profile: EvaluationProfileDefinition
    workflow_configuration: WorkflowConfigurationDefinition | None = None
    published_records: tuple[PublishedDefinition, ...]


class EffectiveRunConfiguration(Contract):
    schema_version: Literal["1"] = "1"
    compiler_version: str
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    context: CompilationContext
    source_refs: tuple[ExactDefinitionRef, ...]
    alias_evidence: tuple[AliasBinding, ...] = ()
    input_manifest: RunInputManifestRef
    workflow_type: WorkflowTypeDefinition
    selected_blueprint: WorkflowBlueprint
    selected_variants: frozenset[str]
    control_profile: ControlProfileDefinition
    runtime_profile: RuntimeProfileDefinition
    workspace_template: WorkspaceTemplateDefinition
    workflow_workspace_contract: WorkflowWorkspaceContract
    evaluation_profile: EvaluationProfileDefinition
    workflow_specific_configuration: WorkflowConfigurationDefinition | None = None
    effective_authority: AuthorityCeiling
    linked_run_slots: tuple[LinkedRunSlotConstraint, ...]
    extensions: tuple[NamespacedExtension, ...] = ()
    overlay_decisions: tuple[OverlayDecision, ...]


class AuthoringHead(Contract):
    kind: DefinitionKind
    logical_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    draft_revision: int = Field(ge=1)
    published_revision: int = Field(ge=0)
    definition: Definition
    updated_at: AwareDatetime
    updated_by: str

    @model_validator(mode="after")
    def identity_matches_definition(self) -> AuthoringHead:
        if self.definition.kind != self.kind or self.definition.logical_id != self.logical_id:
            raise ValueError("authoring head identity must match its definition")
        return self


class DefinitionSelector(Contract):
    exact: ExactDefinitionRef | None = None
    alias: AliasRef | None = None

    @model_validator(mode="after")
    def exactly_one(self) -> DefinitionSelector:
        if (self.exact is None) == (self.alias is None):
            raise ValueError("provide exactly one exact or alias reference")
        return self


class CompileInvocation(Contract):
    workflow_type: DefinitionSelector
    blueprint: DefinitionSelector
    control_profile: DefinitionSelector
    runtime_profile: DefinitionSelector
    workspace_template: DefinitionSelector
    evaluation_profile: DefinitionSelector
    workflow_configuration: DefinitionSelector | None = None
    input_manifest: RunInputManifestRef
    overlay: RunOverlay = Field(default_factory=RunOverlay)
    caller_authority: AuthorityCeiling
    parent_authority: AuthorityCeiling | None = None
    environment: EnvironmentAvailability
    context: CompilationContext


class PublishRequest(Contract):
    definition: Definition
    actor_id: str = Field(min_length=1)
    published_at: AwareDatetime
    expected_head_revision: int = Field(ge=0)


class SaveDraftRequest(Contract):
    definition: Definition
    actor_id: str = Field(min_length=1)
    updated_at: AwareDatetime
    expected_draft_revision: int = Field(ge=0)


class PublishDraftRequest(Contract):
    kind: DefinitionKind
    logical_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._-]*$")
    actor_id: str = Field(min_length=1)
    published_at: AwareDatetime
    expected_draft_revision: int = Field(ge=1)
    expected_published_revision: int = Field(ge=0)


class MoveAliasRequest(Contract):
    alias: AliasRef
    target: ExactDefinitionRef
    actor_id: str = Field(min_length=1)
    moved_at: AwareDatetime


class RetireRequest(Contract):
    ref: ExactDefinitionRef
    actor_id: str = Field(min_length=1)
    retired_at: AwareDatetime
