from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Literal
from urllib.parse import urlsplit

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
    WORKFLOW_IMPLEMENTATION = "workflow_implementation"
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
    MODEL = "model"
    MIDDLEWARE = "middleware"
    SANDBOX_PROFILE = "sandbox_profile"
    TOOL = "tool"
    DEEP_AGENT_PLACEMENT = "deep_agent_placement"


class DefinitionLifecycleStatus(StrEnum):
    PUBLISHED = "published"
    RETIRED = "retired"


class ImmutablePayloadRef(Contract):
    schema_id: str = Field(min_length=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    media_type: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    uri: str = Field(min_length=1)


class ExactDefinitionRef(Contract):
    kind: DefinitionKind
    logical_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    schema_version: Literal["1"] = "1"
    revision: int = Field(ge=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    lifecycle_status: DefinitionLifecycleStatus = DefinitionLifecycleStatus.PUBLISHED
    payload_ref: ImmutablePayloadRef | None = None


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
    logical_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9._:-]*$")
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)

    @model_validator(mode="before")
    @classmethod
    def reject_embedded_secret_values(cls, value: object) -> object:
        """Definition documents may contain typed references, never credential values."""
        sensitive_keys = {
            "apikey",
            "apitoken",
            "accesstoken",
            "authtoken",
            "credential",
            "credentials",
            "password",
            "secret",
            "secretvalue",
            "token",
        }

        def is_ref(subject: object) -> bool:
            if isinstance(subject, dict):
                keys = set(subject)
                return (
                    "provider" in keys and "key" in keys and keys <= {"provider", "key", "version"}
                )
            if isinstance(subject, list | tuple):
                return all(is_ref(item) for item in subject)
            return False

        def inspect(subject: object) -> None:
            if isinstance(subject, dict):
                for key, item in subject.items():
                    normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
                    if normalized in sensitive_keys:
                        if item in (None, (), [], {}) or is_ref(item):
                            continue
                        raise ValueError(
                            "secret values are forbidden in definitions; use typed SecretRef values"
                        )
                    inspect(item)
            elif isinstance(subject, list | tuple):
                for item in subject:
                    inspect(item)

        inspect(value)
        return value


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
    exact_capabilities: tuple[AvailableCapability, ...] = ()


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


class GoalSessionRolloverPolicy(Contract):
    """Authored context lifecycle for bounded GoalDirected iterations."""

    session_mode: Literal["reuse", "fresh", "fresh_from_handoff"] = "reuse"
    fresh_agent_token_threshold: int = Field(default=100_000, ge=1)
    handoff_token_reserve: int = Field(default=4_000, ge=0)
    rollover_mode: Literal["fresh", "fresh_from_handoff"] = "fresh_from_handoff"


class GoalWorkspaceSnapshotPolicy(Contract):
    """Workspace continuity is independent from model-session continuity."""

    workspace_mode: Literal["shared", "fresh", "fresh_from_snapshot"] = "shared"
    snapshot_mode: Literal[
        "none",
        "on_rollover",
        "every_iteration",
        "on_failure",
    ] = "on_rollover"
    rollback_on_failure: bool = True
    goal_path: str = Field(default="/goal/GOAL.md", min_length=1)
    handoff_path: str = Field(default="/goal/HANDOFF.md", min_length=1)
    checkpoint_path: str = Field(default="/goal/checkpoint.json", min_length=1)

    @field_validator("goal_path", "handoff_path", "checkpoint_path")
    @classmethod
    def goal_paths_are_absolute(cls, value: str) -> str:
        if not value.startswith("/") or ".." in value.split("/"):
            raise ValueError("goal workspace paths must be absolute and cannot traverse parents")
        return value


class GoalConvergencePolicy(Contract):
    max_no_progress_iterations: int = Field(default=3, ge=1)
    max_repeated_blockers: int = Field(default=3, ge=1)


GoalProtectedField = Literal[
    "objective",
    "acceptance",
    "invariants",
    "admitted_inputs",
    "authority",
    "budget",
    "prohibited_work",
]


class GoalProtectedScopePolicy(Contract):
    """Fields a Goal Revision can never mutate inside the current run."""

    protected_fields: frozenset[GoalProtectedField] = frozenset(
        {
            "objective",
            "acceptance",
            "invariants",
            "admitted_inputs",
            "authority",
            "budget",
            "prohibited_work",
        }
    )
    expansion_route: Literal[
        "control_revision",
        "fork",
        "linked_run",
        "new_run",
    ] = "new_run"

    @field_validator("protected_fields")
    @classmethod
    def all_governing_fields_are_protected(
        cls, value: frozenset[GoalProtectedField]
    ) -> frozenset[GoalProtectedField]:
        required = {
            "objective",
            "acceptance",
            "invariants",
            "admitted_inputs",
            "authority",
            "budget",
            "prohibited_work",
        }
        if value != required:
            raise ValueError("GoalDirected revisions must protect the complete launch envelope")
        return value


class GoalDirectedBlueprint(DefinitionBase):
    kind: Literal[DefinitionKind.BLUEPRINT] = DefinitionKind.BLUEPRINT
    family: Literal["GoalDirected"] = "GoalDirected"
    objective_contract: str = Field(min_length=1)
    acceptance_contract: str = Field(min_length=1)
    independent_verification_required: Literal[True] = True
    independent_verifier_ref: str = Field(
        default="verifier:independent-goal-acceptance@1",
        min_length=1,
    )
    allowed_operation_classes: frozenset[str] = frozenset({"goal_iteration"})
    session_policy: GoalSessionRolloverPolicy = Field(default_factory=GoalSessionRolloverPolicy)
    workspace_policy: GoalWorkspaceSnapshotPolicy = Field(
        default_factory=GoalWorkspaceSnapshotPolicy
    )
    convergence_policy: GoalConvergencePolicy = Field(default_factory=GoalConvergencePolicy)
    iteration_reservation: dict[str, int] = Field(default_factory=lambda: {"goal.iterations": 1})
    protected_scope_policy: GoalProtectedScopePolicy = Field(
        default_factory=GoalProtectedScopePolicy
    )
    max_iterations: int = Field(ge=1)
    variant_names: frozenset[str] = Field(default_factory=frozenset)

    @field_validator("allowed_operation_classes")
    @classmethod
    def operation_classes_are_declared(cls, value: frozenset[str]) -> frozenset[str]:
        if not value or any(not item for item in value):
            raise ValueError("GoalDirected requires at least one allowed operation class")
        return value

    @field_validator("iteration_reservation")
    @classmethod
    def iteration_reservation_is_bounded(cls, value: dict[str, int]) -> dict[str, int]:
        if any(not dimension or amount < 0 for dimension, amount in value.items()):
            raise ValueError("goal iteration reservations require names and non-negative amounts")
        if value.get("goal.iterations", 0) < 1:
            raise ValueError("goal iteration reservations require one goal.iterations unit")
        return value


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
    operation_assemblies: tuple[OperationAssemblyDefinition, ...] = ()

    @model_validator(mode="after")
    def unique_assemblies(self) -> RuntimeProfileDefinition:
        identities = [item.assembly_id for item in self.operation_assemblies]
        if len(identities) != len(set(identities)):
            raise ValueError("operation assembly identities must be unique")
        return self


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
    workflow_type_logical_id: str = Field(
        min_length=1,
        pattern=r"^[a-z0-9][a-z0-9._:-]*$",
    )
    extensions: tuple[NamespacedExtension, ...] = ()


class ObligationRealization(Contract):
    obligation_ref: str = Field(min_length=1)
    realization_kind: Literal["stage", "goal_acceptance"]
    realization_ref: str = Field(min_length=1)


class OutputContractRealization(Contract):
    output_contract_ref: str = Field(min_length=1)
    output_slot: str = Field(min_length=1)


class WorkflowImplementationBindingDefinition(DefinitionBase):
    """One approved, exact implementation of a semantic Workflow Type revision."""

    kind: Literal[DefinitionKind.WORKFLOW_IMPLEMENTATION] = DefinitionKind.WORKFLOW_IMPLEMENTATION
    workflow_type_ref: ExactDefinitionRef
    blueprint_ref: ExactDefinitionRef
    control_profile_ref: ExactDefinitionRef
    runtime_profile_ref: ExactDefinitionRef
    workspace_template_ref: ExactDefinitionRef
    evaluation_profile_ref: ExactDefinitionRef
    workflow_configuration_ref: ExactDefinitionRef | None = None
    obligation_realizations: tuple[ObligationRealization, ...] = Field(min_length=1)
    output_contract_realizations: tuple[OutputContractRealization, ...] = Field(min_length=1)
    conformance_evidence_refs: frozenset[str] = Field(min_length=1)
    approval_status: Literal["approved"] = "approved"

    @model_validator(mode="after")
    def validate_ref_families(self) -> WorkflowImplementationBindingDefinition:
        expected = (
            (self.workflow_type_ref, DefinitionKind.WORKFLOW_TYPE),
            (self.blueprint_ref, DefinitionKind.BLUEPRINT),
            (self.control_profile_ref, DefinitionKind.CONTROL_PROFILE),
            (self.runtime_profile_ref, DefinitionKind.RUNTIME_PROFILE),
            (self.workspace_template_ref, DefinitionKind.WORKSPACE_TEMPLATE),
            (self.evaluation_profile_ref, DefinitionKind.EVALUATION_PROFILE),
        )
        if any(ref.kind != kind for ref, kind in expected):
            raise ValueError("Workflow Implementation contains a reference of the wrong family")
        if (
            self.workflow_configuration_ref is not None
            and self.workflow_configuration_ref.kind != DefinitionKind.WORKFLOW_CONFIGURATION
        ):
            raise ValueError("Workflow Implementation configuration reference has the wrong family")
        obligations = [item.obligation_ref for item in self.obligation_realizations]
        outputs = [item.output_contract_ref for item in self.output_contract_realizations]
        if len(obligations) != len(set(obligations)):
            raise ValueError("Workflow Implementation obligation realizations must be unique")
        if len(outputs) != len(set(outputs)):
            raise ValueError("Workflow Implementation output realizations must be unique")
        return self


class CatalogPayloadRef(Contract):
    uri: str = Field(min_length=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    media_type: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)


class SourceProvenance(Contract):
    source: Literal["belllabs", "local", "git", "mcp_registry", "npx_skills"]
    locator: str = Field(min_length=1)
    upstream_identity: str = Field(min_length=1)
    upstream_version: str = Field(min_length=1)
    commit_digest: str | None = Field(default=None, min_length=1)
    license: str | None = Field(default=None, min_length=1)

    @field_validator("locator")
    @classmethod
    def locator_cannot_embed_credentials(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme and (
            parsed.username is not None
            or parsed.password is not None
            or any(
                fragment in key.lower()
                for key in parsed.query.split("&")
                for fragment in ("token", "secret", "password", "api_key", "apikey")
            )
        ):
            raise ValueError("source provenance locators cannot embed credentials")
        return value


class PromptVariable(Contract):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    description: str = Field(min_length=1)
    required: bool = True
    value_schema: dict[str, object] = Field(default_factory=dict)


class PromptDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.PROMPT] = DefinitionKind.PROMPT
    format: Literal["text", "markdown", "messages"]
    template_engine: Literal["none", "jinja2", "format"]
    variables: tuple[PromptVariable, ...] = ()
    body: str | None = None
    payload_ref: CatalogPayloadRef | None = None
    trust_class: Literal["privileged", "reviewed", "untrusted"]
    eval_refs: frozenset[str] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def exactly_one_payload(self) -> PromptDefinition:
        if (self.body is None) == (self.payload_ref is None):
            raise ValueError("Prompt Definition requires exactly one body or payload_ref")
        names = [variable.name for variable in self.variables]
        if len(names) != len(set(names)):
            raise ValueError("Prompt Definition variable names must be unique")
        return self


class SkillFileManifestEntry(Contract):
    path: str = Field(min_length=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    size_bytes: int = Field(ge=0)
    executable: bool = False

    @field_validator("path")
    @classmethod
    def path_is_relative_and_normalized(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or ".." in normalized.split("/"):
            raise ValueError("skill file paths must be normalized relative paths")
        return normalized


class SkillCompatibility(Contract):
    runtimes: frozenset[str] = Field(default_factory=frozenset)
    executables: frozenset[str] = Field(default_factory=frozenset)
    network_capabilities: frozenset[str] = Field(default_factory=frozenset)
    workspace_capabilities: frozenset[str] = Field(default_factory=frozenset)


class SkillDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.SKILL] = DefinitionKind.SKILL
    skill_name: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9-]*$")
    frontmatter: dict[str, object]
    body_summary: str = Field(min_length=1)
    bundle_ref: CatalogPayloadRef
    manifest_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    file_manifest: tuple[SkillFileManifestEntry, ...] = Field(min_length=1)
    required_capabilities: frozenset[str] = Field(default_factory=frozenset)
    compatibility: SkillCompatibility = Field(default_factory=SkillCompatibility)
    source_provenance: SourceProvenance
    review_status: Literal["reviewed", "approved"] = "reviewed"
    maturity: Literal["experimental", "qualified", "accepted"] = "qualified"
    attachment_targets: frozenset[str] = Field(default_factory=lambda: frozenset({"agent.main"}))
    compatible_compiler_versions: frozenset[str] = Field(
        default_factory=lambda: frozenset({"control-plane-definitions/1"})
    )
    conflicts_with: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def validate_manifest(self) -> SkillDefinition:
        paths = [entry.path for entry in self.file_manifest]
        if len(paths) != len(set(paths)):
            raise ValueError("Skill Definition file paths must be unique")
        if "SKILL.md" not in paths:
            raise ValueError("Skill Definition manifest must contain SKILL.md")
        return self


class MCPNetworkRequirement(Contract):
    host: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    protocol: Literal["https", "http", "stdio"]


class MCPServerDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.MCP_SERVER] = DefinitionKind.MCP_SERVER
    transport: Literal["stdio", "streamable_http", "sse"]
    endpoint: str | None = None
    launch_template: tuple[str, ...] | None = None
    credential_refs: tuple[SecretRef, ...] = ()
    allowed_tools: frozenset[str] = Field(default_factory=frozenset)
    approval_policy: dict[str, Literal["never", "always", "conditional"]] = Field(
        default_factory=dict
    )
    network_requirements: tuple[MCPNetworkRequirement, ...] = ()
    schema_snapshot_ref: CatalogPayloadRef
    schema_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    source_provenance: SourceProvenance
    review_status: Literal["reviewed", "approved"] = "reviewed"
    maturity: Literal["experimental", "qualified", "accepted"] = "qualified"
    attachment_targets: frozenset[str] = Field(default_factory=lambda: frozenset({"agent.main"}))
    compatible_compiler_versions: frozenset[str] = Field(
        default_factory=lambda: frozenset({"control-plane-definitions/1"})
    )
    conflicts_with: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def validate_transport_recipe(self) -> MCPServerDefinition:
        if self.transport == "stdio":
            if not self.launch_template or self.endpoint is not None:
                raise ValueError("stdio MCP servers require only a launch_template")
        elif not self.endpoint or self.launch_template is not None:
            raise ValueError("remote MCP servers require only an endpoint")
        if self.endpoint is not None:
            parsed = urlsplit(self.endpoint)
            if (
                parsed.scheme not in {"https", "http"}
                or not parsed.hostname
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError("MCP endpoint must be a sanitized HTTP(S) origin/path")
        if not set(self.approval_policy) <= self.allowed_tools:
            raise ValueError("MCP approval policy may reference only allowed tools")
        return self


class MCPToolDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.MCP_TOOL] = DefinitionKind.MCP_TOOL
    server_ref: ExactDefinitionRef
    tool_name: str = Field(min_length=1)
    input_schema: dict[str, object]
    output_schema: dict[str, object] | None = None
    annotations: dict[str, object] = Field(default_factory=dict)
    schema_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    side_effect_class: Literal["read_only", "bounded_write", "consequential"]
    maturity: Literal["experimental", "qualified", "accepted"] = "qualified"
    attachment_targets: frozenset[str] = Field(default_factory=lambda: frozenset({"agent.main"}))
    compatible_compiler_versions: frozenset[str] = Field(
        default_factory=lambda: frozenset({"control-plane-definitions/1"})
    )
    conflicts_with: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def validate_server_ref(self) -> MCPToolDefinition:
        if self.server_ref.kind != DefinitionKind.MCP_SERVER:
            raise ValueError("MCP Tool server_ref must reference an MCP Server Definition")
        return self


class ModelPolicy(Contract):
    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    settings: dict[str, object] = Field(default_factory=dict)


class CapabilityKind(StrEnum):
    MCP = "mcp"
    SKILL = "skill"
    SANDBOX = "sandbox"
    MODEL = "model"
    MIDDLEWARE = "middleware"
    TOOL = "tool"


class CapabilityRequirement(Contract):
    requirement_id: str = Field(pattern=r"^[a-z][a-z0-9._-]*$")
    capability_kind: CapabilityKind
    allowed_refs: frozenset[ExactDefinitionRef] = Field(min_length=1)
    attachment_target: str = Field(min_length=1)
    required: bool = True
    when_unavailable: Literal["reject", "omit", "degrade"] = "reject"
    degraded_ref: ExactDefinitionRef | None = None

    @model_validator(mode="after")
    def validate_policy(self) -> CapabilityRequirement:
        if self.required and self.when_unavailable != "reject":
            raise ValueError("required capabilities must fail closed")
        if (self.when_unavailable == "degrade") != (self.degraded_ref is not None):
            raise ValueError("degradation requires exactly one authored degraded_ref")
        allowed_kinds = {
            CapabilityKind.MCP: {DefinitionKind.MCP_SERVER, DefinitionKind.MCP_TOOL},
            CapabilityKind.SKILL: {DefinitionKind.SKILL},
            CapabilityKind.SANDBOX: {DefinitionKind.SANDBOX_PROFILE},
            CapabilityKind.MODEL: {DefinitionKind.MODEL},
            CapabilityKind.MIDDLEWARE: {DefinitionKind.MIDDLEWARE},
            CapabilityKind.TOOL: {DefinitionKind.TOOL, DefinitionKind.MCP_TOOL},
        }[self.capability_kind]
        refs = set(self.allowed_refs)
        if self.degraded_ref is not None:
            refs.add(self.degraded_ref)
        if any(ref.kind not in allowed_kinds for ref in refs):
            raise ValueError("capability requirement contains a reference of the wrong family")
        return self


class AvailableCapability(Contract):
    ref: ExactDefinitionRef


class ResolvedCapabilityAttachment(Contract):
    requirement_id: str
    capability_kind: CapabilityKind
    selected_ref: ExactDefinitionRef | None = None
    attachment_target: str
    status: Literal["accepted", "omitted", "degraded"]
    reason: str


class ProfileComponent(Contract):
    slot: str = Field(pattern=r"^[a-z][a-z0-9._-]*$")
    ref: ExactDefinitionRef


class OperationAssemblyDefinition(Contract):
    assembly_id: str = Field(pattern=r"^[a-z][a-z0-9._-]*$")
    deep_agent_profile_ref: ExactDefinitionRef
    placement_ref: ExactDefinitionRef
    capability_requirements: tuple[CapabilityRequirement, ...] = ()

    @model_validator(mode="after")
    def validate_families(self) -> OperationAssemblyDefinition:
        if self.deep_agent_profile_ref.kind != DefinitionKind.AGENT_PROFILE:
            raise ValueError("operation assembly profile must be an Agent Profile revision")
        if self.placement_ref.kind != DefinitionKind.DEEP_AGENT_PLACEMENT:
            raise ValueError("operation assembly placement must be exact")
        ids = [item.requirement_id for item in self.capability_requirements]
        if len(ids) != len(set(ids)):
            raise ValueError("capability requirement identities must be unique per assembly")
        return self


class DeepAgentPlacementDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.DEEP_AGENT_PLACEMENT] = DefinitionKind.DEEP_AGENT_PLACEMENT
    deep_agents_version: str = Field(min_length=1)
    placement: Literal["local_worker", "langsmith_remote"]
    runtime_binding: str = Field(min_length=1)
    sandbox_ref: ExactDefinitionRef

    @model_validator(mode="after")
    def sandbox_is_exact(self) -> DeepAgentPlacementDefinition:
        if self.sandbox_ref.kind != DefinitionKind.SANDBOX_PROFILE:
            raise ValueError("Deep Agent placement requires an exact sandbox profile")
        return self


class CapabilityDefinition(DefinitionBase):
    kind: Literal[
        DefinitionKind.MODEL,
        DefinitionKind.MIDDLEWARE,
        DefinitionKind.SANDBOX_PROFILE,
        DefinitionKind.TOOL,
    ]
    capability_kind: CapabilityKind
    maturity: Literal["experimental", "qualified", "accepted"]
    attachment_targets: frozenset[str] = Field(min_length=1)
    compatible_compiler_versions: frozenset[str] = Field(default_factory=frozenset)
    conflicts_with: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)

    @model_validator(mode="after")
    def capability_family_matches_definition_kind(self) -> CapabilityDefinition:
        expected = {
            DefinitionKind.MODEL: CapabilityKind.MODEL,
            DefinitionKind.MIDDLEWARE: CapabilityKind.MIDDLEWARE,
            DefinitionKind.SANDBOX_PROFILE: CapabilityKind.SANDBOX,
            DefinitionKind.TOOL: CapabilityKind.TOOL,
        }[self.kind]
        if self.capability_kind != expected:
            raise ValueError(
                f"capability_kind must be {expected.value!r} for definition kind "
                f"{self.kind.value!r}"
            )
        return self


class AgentProfileDefinition(DefinitionBase):
    kind: Literal[DefinitionKind.AGENT_PROFILE] = DefinitionKind.AGENT_PROFILE
    prompt_refs: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)
    skill_refs: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)
    mcp_server_refs: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)
    tool_refs: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)
    model_policy: ModelPolicy
    guardrail_refs: frozenset[str] = Field(default_factory=frozenset)
    output_schema_ref: str | None = Field(default=None, min_length=1)
    maximum_capability_request: AuthorityCeiling
    parent_profile_refs: tuple[ExactDefinitionRef, ...] = ()
    components: tuple[ProfileComponent, ...] = ()
    model_ref: ExactDefinitionRef | None = None
    middleware_refs: frozenset[ExactDefinitionRef] = Field(default_factory=frozenset)
    sandbox_profile_ref: ExactDefinitionRef | None = None
    capability_requirements: tuple[CapabilityRequirement, ...] = ()

    @model_validator(mode="after")
    def validate_ref_families(self) -> AgentProfileDefinition:
        expected = (
            (self.prompt_refs, DefinitionKind.PROMPT),
            (self.skill_refs, DefinitionKind.SKILL),
            (self.mcp_server_refs, DefinitionKind.MCP_SERVER),
            (self.tool_refs, DefinitionKind.MCP_TOOL),
        )
        if any(ref.kind != kind for refs, kind in expected for ref in refs):
            raise ValueError("Agent Profile contains a catalog reference of the wrong family")
        if any(ref.kind != DefinitionKind.AGENT_PROFILE for ref in self.parent_profile_refs):
            raise ValueError("Agent Profile parents must be exact Agent Profile revisions")
        if self.model_ref is not None and self.model_ref.kind != DefinitionKind.MODEL:
            raise ValueError("Agent Profile model must be an exact Model revision")
        if any(ref.kind != DefinitionKind.MIDDLEWARE for ref in self.middleware_refs):
            raise ValueError("Agent Profile middleware must be exact revisions")
        if self.sandbox_profile_ref is not None and (
            self.sandbox_profile_ref.kind != DefinitionKind.SANDBOX_PROFILE
        ):
            raise ValueError("Agent Profile sandbox must be an exact revision")
        slots = [component.slot for component in self.components]
        if len(slots) != len(set(slots)):
            raise ValueError("Agent Profile component slots must be unique")
        return self


Definition = (
    WorkflowTypeDefinition
    | WorkflowImplementationBindingDefinition
    | StageGraphBlueprint
    | GoalDirectedBlueprint
    | ControlProfileDefinition
    | RuntimeProfileDefinition
    | WorkspaceTemplateDefinition
    | EvaluationProfileDefinition
    | WorkflowConfigurationDefinition
    | PromptDefinition
    | SkillDefinition
    | MCPServerDefinition
    | MCPToolDefinition
    | AgentProfileDefinition
    | DeepAgentPlacementDefinition
    | CapabilityDefinition
)


class PublishedDefinition(Contract):
    ref: ExactDefinitionRef
    definition: Definition
    published_at: AwareDatetime
    published_by: str
    retired_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def lifecycle_matches_reference(self) -> PublishedDefinition:
        expected = (
            DefinitionLifecycleStatus.RETIRED
            if self.retired_at is not None
            else DefinitionLifecycleStatus.PUBLISHED
        )
        if self.ref.lifecycle_status != expected:
            raise ValueError("definition reference lifecycle status does not match its record")
        return self


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
    implementation_ref: ExactDefinitionRef | None = None
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
    implementation_binding: WorkflowImplementationBindingDefinition | None = None
    blueprint: WorkflowBlueprint
    control_profile: ControlProfileDefinition
    runtime_profile: RuntimeProfileDefinition
    workspace_template: WorkspaceTemplateDefinition
    evaluation_profile: EvaluationProfileDefinition
    workflow_configuration: WorkflowConfigurationDefinition | None = None
    published_records: tuple[PublishedDefinition, ...]
    agent_profiles: tuple[AgentProfileDefinition, ...] = ()


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
    operation_assemblies: tuple[OperationAssemblyDefinition, ...] = ()
    flattened_agent_bindings: tuple[FlattenedDeepAgentBinding, ...] = ()
    capability_attachment_plan: tuple[ResolvedCapabilityAttachment, ...] = ()


class FlattenedDeepAgentBinding(Contract):
    assembly_id: str
    profile_ref: ExactDefinitionRef
    placement_ref: ExactDefinitionRef
    flattened_components: tuple[ProfileComponent, ...]
    prompt_refs: tuple[ExactDefinitionRef, ...] = ()
    skill_refs: tuple[ExactDefinitionRef, ...] = ()
    mcp_server_refs: tuple[ExactDefinitionRef, ...] = ()
    tool_refs: tuple[ExactDefinitionRef, ...] = ()
    model_policy: ModelPolicy
    guardrail_refs: tuple[str, ...] = ()
    output_schema_ref: str | None = None
    maximum_capability_request: AuthorityCeiling
    model_ref: ExactDefinitionRef | None = None
    middleware_refs: tuple[ExactDefinitionRef, ...] = ()
    sandbox_profile_ref: ExactDefinitionRef | None = None


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
    implementation: DefinitionSelector | None = None
    blueprint: DefinitionSelector | None = None
    control_profile: DefinitionSelector | None = None
    runtime_profile: DefinitionSelector | None = None
    workspace_template: DefinitionSelector | None = None
    evaluation_profile: DefinitionSelector | None = None
    workflow_configuration: DefinitionSelector | None = None
    input_manifest: RunInputManifestRef
    overlay: RunOverlay = Field(default_factory=RunOverlay)
    caller_authority: AuthorityCeiling
    parent_authority: AuthorityCeiling | None = None
    environment: EnvironmentAvailability
    context: CompilationContext

    @model_validator(mode="after")
    def select_one_compilation_mode(self) -> CompileInvocation:
        components = (
            self.blueprint,
            self.control_profile,
            self.runtime_profile,
            self.workspace_template,
            self.evaluation_profile,
        )
        if self.implementation is not None:
            if any(item is not None for item in components) or self.workflow_configuration:
                raise ValueError(
                    "implementation selection cannot be mixed with component selectors"
                )
            return self
        if all(item is not None for item in components):
            return self
        if all(item is None for item in components) and self.workflow_configuration is None:
            # The service resolves the Workflow Type's conventional `default` implementation
            # alias after it has resolved the exact Workflow Type identity.
            return self
        raise ValueError(
            "provide an implementation selector, no implementation selectors for the default, "
            "or the complete legacy component selector set"
        )


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
