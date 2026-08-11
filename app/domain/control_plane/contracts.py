from __future__ import annotations

import re
import unicodedata
from enum import StrEnum
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
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


StageGraphIdentifier = Annotated[str, Field(min_length=1, max_length=256)]


def _stagegraph_identifier(value: str) -> str:
    if value != unicodedata.normalize("NFC", value):
        raise ValueError("StageGraph identifiers must already be Unicode NFC")
    if value in {"NO_OWNER_STAGE", "NO_MAPPED_INSTANCE", "BEFORE_FIRST"}:
        raise ValueError("StageGraph identifiers cannot use typed runtime sentinels")
    if any(ord(character) < 32 or character == "\x7f" for character in value):
        raise ValueError("StageGraph identifiers cannot contain control characters")
    return value


class DependencyClass(StrEnum):
    REQUIRED = "required"
    DEGRADABLE = "degradable"
    OPTIONAL = "optional"
    ADVISORY = "advisory"


class JoinKind(StrEnum):
    ALL = "all"
    ANY = "any"
    MINIMUM = "minimum"


class FairnessGroup(Contract):
    group_id: StageGraphIdentifier
    weight: int = Field(ge=1, le=65_535)

    _identifier = field_validator("group_id")(_stagegraph_identifier)


class StageInputSlot(Contract):
    input_slot_id: StageGraphIdentifier
    required: bool = True

    _identifier = field_validator("input_slot_id")(_stagegraph_identifier)


class StageOutputSlot(Contract):
    output_slot_id: StageGraphIdentifier
    output_contract_ref: str = Field(min_length=1)

    _identifier = field_validator("output_slot_id")(_stagegraph_identifier)


class StageObligationSlot(Contract):
    obligation_slot_id: StageGraphIdentifier
    obligation_ref: str = Field(min_length=1)

    _identifier = field_validator("obligation_slot_id")(_stagegraph_identifier)


class WorkflowObligationSlot(Contract):
    obligation_slot_id: StageGraphIdentifier
    obligation_ref: str = Field(min_length=1)

    _identifier = field_validator("obligation_slot_id")(_stagegraph_identifier)


class AllowedOperationVariant(Contract):
    operation_variant_id: StageGraphIdentifier
    operation_contract_ref: str = Field(min_length=1)

    _identifier = field_validator("operation_variant_id")(_stagegraph_identifier)


class StageOperationSlot(Contract):
    operation_slot_id: StageGraphIdentifier
    priority: int = Field(default=0, ge=0)
    concurrency_slots: int = Field(default=1, ge=1)
    reservation: dict[str, int] = Field(default_factory=dict)
    allowed_variants: tuple[AllowedOperationVariant, ...] = Field(min_length=1)
    fallback_sequence: tuple[StageGraphIdentifier, ...] = ()

    _identifier = field_validator("operation_slot_id")(_stagegraph_identifier)

    @field_validator("allowed_variants")
    @classmethod
    def normalize_allowed_variants(
        cls, value: tuple[AllowedOperationVariant, ...]
    ) -> tuple[AllowedOperationVariant, ...]:
        return tuple(
            sorted(value, key=lambda item: item.operation_variant_id.encode("utf-8"))
        )

    @model_validator(mode="after")
    def validate_operation_slot(self) -> StageOperationSlot:
        if any(not dimension or amount < 0 for dimension, amount in self.reservation.items()):
            raise ValueError("operation reservations require names and non-negative amounts")
        identities = [item.operation_variant_id for item in self.allowed_variants]
        if len(identities) != len(set(identities)):
            raise ValueError("operation variant identities must be unique")
        known = set(identities)
        if len(self.fallback_sequence) != len(set(self.fallback_sequence)):
            raise ValueError("operation fallback sequence identities must be unique")
        if not set(self.fallback_sequence) <= known:
            raise ValueError("operation fallback sequence references an unknown variant")
        return self


class StageCyclePolicy(Contract):
    max_cycles: int = Field(ge=1)
    evaluation_contract_ref: str = Field(min_length=1)
    objective_contract_ref: str = Field(min_length=1)
    reservation: dict[str, int] = Field(default_factory=dict)
    stopping_rule_precedence: tuple[StageGraphIdentifier, ...] = ()

    @field_validator("reservation")
    @classmethod
    def validate_reservation(cls, value: dict[str, int]) -> dict[str, int]:
        if any(not dimension or amount < 0 for dimension, amount in value.items()):
            raise ValueError("stage cycle reservations require names and non-negative amounts")
        if value.get("stage.cycles", 0) < 1:
            raise ValueError("stage cycle reservations require at least one stage.cycles unit")
        return value


class WorkflowCyclePolicy(Contract):
    max_cycles: int = Field(ge=1)
    evaluation_contract_ref: str = Field(min_length=1)
    objective_contract_ref: str = Field(min_length=1)
    reservation: dict[str, int] = Field(default_factory=dict)
    stopping_rule_precedence: tuple[StageGraphIdentifier, ...] = ()

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
    stage_id: StageGraphIdentifier
    fairness_group_id: StageGraphIdentifier = "default"
    input_slots: tuple[StageInputSlot, ...] = ()
    output_slots: tuple[StageOutputSlot, ...] = ()
    obligation_slots: tuple[StageObligationSlot, ...] = ()
    operation_slots: tuple[StageOperationSlot, ...] = Field(min_length=1)
    stage_cycle_policy: StageCyclePolicy | None = None

    _identifiers = field_validator("stage_id", "fairness_group_id")(_stagegraph_identifier)

    @field_validator("input_slots")
    @classmethod
    def normalize_inputs(
        cls, value: tuple[StageInputSlot, ...]
    ) -> tuple[StageInputSlot, ...]:
        return tuple(sorted(value, key=lambda item: item.input_slot_id.encode("utf-8")))

    @field_validator("output_slots")
    @classmethod
    def normalize_outputs(
        cls, value: tuple[StageOutputSlot, ...]
    ) -> tuple[StageOutputSlot, ...]:
        return tuple(sorted(value, key=lambda item: item.output_slot_id.encode("utf-8")))

    @field_validator("obligation_slots")
    @classmethod
    def normalize_obligations(
        cls, value: tuple[StageObligationSlot, ...]
    ) -> tuple[StageObligationSlot, ...]:
        return tuple(
            sorted(value, key=lambda item: item.obligation_slot_id.encode("utf-8"))
        )

    @field_validator("operation_slots")
    @classmethod
    def normalize_operations(
        cls, value: tuple[StageOperationSlot, ...]
    ) -> tuple[StageOperationSlot, ...]:
        return tuple(
            sorted(value, key=lambda item: item.operation_slot_id.encode("utf-8"))
        )

    @model_validator(mode="after")
    def normalize_stage_collections(self) -> StageNode:
        collections: tuple[tuple[object, ...], ...] = (
            self.input_slots,
            self.output_slots,
            self.obligation_slots,
            self.operation_slots,
        )
        for collection in collections:
            identities = []
            for item in collection:
                if not isinstance(item, BaseModel):
                    raise ValueError("StageGraph stage collection item must be a model")
                identity_field = next(
                    name for name in type(item).model_fields if name.endswith("_id")
                )
                identities.append(getattr(item, identity_field))
            if len(identities) != len(set(identities)):
                raise ValueError("StageGraph stage collection contains a duplicate identity")
        return self


class StageMapping(Contract):
    stage_id: StageGraphIdentifier
    mapping_id: StageGraphIdentifier

    _identifiers = field_validator("stage_id", "mapping_id")(_stagegraph_identifier)


class StageDependency(Contract):
    dependency_id: StageGraphIdentifier
    consumer_stage_id: StageGraphIdentifier
    join_id: StageGraphIdentifier
    producer_stage_id: StageGraphIdentifier
    producer_output_slot_id: StageGraphIdentifier
    consumer_input_slot_id: StageGraphIdentifier
    dependency_class: DependencyClass

    _identifiers = field_validator(
        "dependency_id",
        "consumer_stage_id",
        "join_id",
        "producer_stage_id",
        "producer_output_slot_id",
        "consumer_input_slot_id",
    )(_stagegraph_identifier)


class SlowSiblingPolicy(Contract):
    triggers: tuple[
        Literal[
            "join_released",
            "deadline_reached",
            "accepted_budget_pressure",
            "cancellation_requested",
        ],
        ...,
    ] = Field(min_length=1)
    execution_action: Literal["continue", "request_cancel"]
    arrival_route: Literal["evaluate_late_result", "quarantine"]

    @field_validator("triggers")
    @classmethod
    def unique_triggers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("slow-sibling trigger precedence cannot contain duplicates")
        return value


class StageJoin(Contract):
    consumer_stage_id: StageGraphIdentifier
    join_id: StageGraphIdentifier
    kind: JoinKind
    minimum: int | None = Field(default=None, ge=1)
    dependency_ids: tuple[StageGraphIdentifier, ...] = ()
    slow_sibling_policy: SlowSiblingPolicy

    _identifiers = field_validator("consumer_stage_id", "join_id")(_stagegraph_identifier)

    @field_validator("dependency_ids")
    @classmethod
    def normalize_dependency_ids(
        cls, value: tuple[str, ...]
    ) -> tuple[str, ...]:
        return tuple(sorted(value, key=lambda item: item.encode("utf-8")))

    @model_validator(mode="after")
    def validate_cardinality(self) -> StageJoin:
        count = len(self.dependency_ids)
        if len(set(self.dependency_ids)) != count:
            raise ValueError("join dependency identities must be unique")
        if self.kind == JoinKind.ANY and count == 0:
            raise ValueError("any joins require at least one non-advisory dependency")
        if self.kind == JoinKind.MINIMUM:
            if self.minimum is None or count == 0 or self.minimum > count:
                raise ValueError("minimum joins require 1 <= minimum <= dependency count")
        elif self.minimum is not None:
            raise ValueError("minimum is valid only for minimum joins")
        return self


class LateResultRule(Contract):
    rule_id: StageGraphIdentifier
    trigger: Literal["consumer_already_admitted"]
    decision: Literal["admit", "reject", "quarantine"]

    _identifier = field_validator("rule_id")(_stagegraph_identifier)


class LateResultPolicy(Contract):
    rules: tuple[LateResultRule, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def complete_and_ordered(self) -> LateResultPolicy:
        identities = [item.rule_id for item in self.rules]
        if len(identities) != len(set(identities)):
            raise ValueError("late-result rule identities must be unique")
        if not any(item.trigger == "consumer_already_admitted" for item in self.rules):
            raise ValueError("late-result policy must cover consumer_already_admitted")
        return self


class ObligationMatrixRow(Contract):
    obligation_scope: Literal["stage", "workflow"]
    owner_stage_id: StageGraphIdentifier | None = None
    obligation_slot_id: StageGraphIdentifier
    evidence_slot_id: StageGraphIdentifier
    required: bool = True

    _identifiers = field_validator(
        "owner_stage_id", "obligation_slot_id", "evidence_slot_id"
    )(
        lambda value: None if value is None else _stagegraph_identifier(value)
    )

    @model_validator(mode="after")
    def owner_matches_scope(self) -> ObligationMatrixRow:
        if (self.obligation_scope == "stage") != (self.owner_stage_id is not None):
            raise ValueError("stage obligations require an owner; workflow obligations forbid one")
        return self


class LinkedRunSlot(Contract):
    linked_run_slot_id: StageGraphIdentifier
    owner_stage_id: StageGraphIdentifier | None = None
    dependency_ids: tuple[StageGraphIdentifier, ...] = ()

    _identifiers = field_validator("linked_run_slot_id", "owner_stage_id")(
        lambda value: None if value is None else _stagegraph_identifier(value)
    )


class StageGraphPolicyDefinition(Contract):
    policy_kind: StageGraphIdentifier
    scope_kind: Literal["workflow", "stage", "join", "operation"]
    scope_id: StageGraphIdentifier
    policy_id: StageGraphIdentifier

    _identifiers = field_validator("policy_kind", "scope_id", "policy_id")(
        _stagegraph_identifier
    )


class StageGraphWait(Contract):
    scope_kind: Literal["workflow", "stage", "operation"]
    scope_id: StageGraphIdentifier
    wait_id: StageGraphIdentifier

    _identifiers = field_validator("scope_id", "wait_id")(_stagegraph_identifier)


class StageGraphLimit(Contract):
    scope_kind: Literal["workflow", "stage", "operation"]
    scope_id: StageGraphIdentifier
    condition_kind: StageGraphIdentifier
    condition_id: StageGraphIdentifier
    limit: int = Field(ge=0)

    _identifiers = field_validator(
        "scope_id", "condition_kind", "condition_id"
    )(_stagegraph_identifier)


class InvalidationReuseDeclaration(Contract):
    scope_kind: Literal["workflow", "stage"]
    scope_id: StageGraphIdentifier
    declaration_kind: Literal["invalidate_descendants", "reuse_immutable"]
    declaration_id: StageGraphIdentifier

    _identifiers = field_validator("scope_id", "declaration_id")(_stagegraph_identifier)


class CapacityCeiling(Contract):
    scope_kind: Literal["workflow", "stage", "operation"]
    scope_id: StageGraphIdentifier
    dimension_kind: StageGraphIdentifier
    dimension_id: StageGraphIdentifier
    amount: int = Field(ge=0)

    _identifiers = field_validator(
        "scope_id", "dimension_kind", "dimension_id"
    )(_stagegraph_identifier)


class CompletionObligationRef(Contract):
    obligation_scope: Literal["stage", "workflow"]
    owner_stage_id: StageGraphIdentifier | None = None
    obligation_slot_id: StageGraphIdentifier

    _identifiers = field_validator("owner_stage_id", "obligation_slot_id")(
        lambda value: None if value is None else _stagegraph_identifier(value)
    )


class StageGraphBlueprint(DefinitionBase):
    kind: Literal[DefinitionKind.BLUEPRINT] = DefinitionKind.BLUEPRINT
    family: Literal["StageGraph"] = "StageGraph"
    contract_version: Literal["CON-BP-STAGEGRAPH-V2"] = "CON-BP-STAGEGRAPH-V2"
    stages: tuple[StageNode, ...] = Field(min_length=1)
    stage_mappings: tuple[StageMapping, ...] = ()
    joins: tuple[StageJoin, ...] = ()
    dependencies: tuple[StageDependency, ...] = ()
    workflow_obligation_slots: tuple[WorkflowObligationSlot, ...] = ()
    obligation_matrix: tuple[ObligationMatrixRow, ...] = ()
    fairness_groups: tuple[FairnessGroup, ...] = ()
    linked_run_slots: tuple[LinkedRunSlot, ...] = ()
    policy_definitions: tuple[StageGraphPolicyDefinition, ...] = ()
    waits: tuple[StageGraphWait, ...] = ()
    cycle_limits: tuple[StageGraphLimit, ...] = ()
    invalidation_reuse_declarations: tuple[InvalidationReuseDeclaration, ...] = ()
    capacity_ceilings: tuple[CapacityCeiling, ...] = ()
    completion_obligations: tuple[CompletionObligationRef, ...] = ()
    late_result_policy: LateResultPolicy
    workflow_evaluation_contract_ref: str | None = Field(default=None, min_length=1)
    workflow_cycle_policy: WorkflowCyclePolicy | None = None

    @model_validator(mode="before")
    @classmethod
    def default_fairness_group(cls, value: Any) -> Any:
        if isinstance(value, dict) and not value.get("fairness_groups"):
            return {
                **value,
                "fairness_groups": ({"group_id": "default", "weight": 1},),
            }
        return value

    @field_validator(
        "stages",
        "stage_mappings",
        "joins",
        "dependencies",
        "workflow_obligation_slots",
        "obligation_matrix",
        "fairness_groups",
        "linked_run_slots",
        "policy_definitions",
        "waits",
        "cycle_limits",
        "invalidation_reuse_declarations",
        "capacity_ceilings",
        "completion_obligations",
    )
    @classmethod
    def normalize_registry_collection(
        cls, value: tuple[Any, ...], info: ValidationInfo
    ) -> tuple[Any, ...]:
        def utf8(item: str | None) -> tuple[int, bytes]:
            return (0, b"") if item is None else (1, item.encode("utf-8"))

        key_functions = {
            "stages": lambda item: (item.stage_id.encode("utf-8"),),
            "stage_mappings": lambda item: (
                item.stage_id.encode("utf-8"),
                item.mapping_id.encode("utf-8"),
            ),
            "joins": lambda item: (
                item.consumer_stage_id.encode("utf-8"),
                item.join_id.encode("utf-8"),
            ),
            "dependencies": lambda item: (
                item.consumer_stage_id.encode("utf-8"),
                item.join_id.encode("utf-8"),
                item.producer_stage_id.encode("utf-8"),
                item.producer_output_slot_id.encode("utf-8"),
                item.dependency_id.encode("utf-8"),
            ),
            "workflow_obligation_slots": lambda item: (
                item.obligation_slot_id.encode("utf-8"),
            ),
            "obligation_matrix": lambda item: (
                item.obligation_scope.encode("utf-8"),
                *utf8(item.owner_stage_id),
                item.obligation_slot_id.encode("utf-8"),
                item.evidence_slot_id.encode("utf-8"),
            ),
            "fairness_groups": lambda item: (item.group_id.encode("utf-8"),),
            "linked_run_slots": lambda item: (
                *utf8(item.owner_stage_id),
                item.linked_run_slot_id.encode("utf-8"),
            ),
            "policy_definitions": lambda item: (
                item.policy_kind.encode("utf-8"),
                item.scope_kind.encode("utf-8"),
                item.scope_id.encode("utf-8"),
                item.policy_id.encode("utf-8"),
            ),
            "waits": lambda item: (
                item.scope_kind.encode("utf-8"),
                item.scope_id.encode("utf-8"),
                item.wait_id.encode("utf-8"),
            ),
            "cycle_limits": lambda item: (
                item.scope_kind.encode("utf-8"),
                item.scope_id.encode("utf-8"),
                item.condition_kind.encode("utf-8"),
                item.condition_id.encode("utf-8"),
            ),
            "invalidation_reuse_declarations": lambda item: (
                item.scope_kind.encode("utf-8"),
                item.scope_id.encode("utf-8"),
                item.declaration_kind.encode("utf-8"),
                item.declaration_id.encode("utf-8"),
            ),
            "capacity_ceilings": lambda item: (
                item.scope_kind.encode("utf-8"),
                item.scope_id.encode("utf-8"),
                item.dimension_kind.encode("utf-8"),
                item.dimension_id.encode("utf-8"),
            ),
            "completion_obligations": lambda item: (
                item.obligation_scope.encode("utf-8"),
                *utf8(item.owner_stage_id),
                item.obligation_slot_id.encode("utf-8"),
            ),
        }
        field_name = info.field_name
        if field_name is None:
            raise ValueError("StageGraph registry normalization requires a field name")
        return tuple(sorted(value, key=key_functions[field_name]))

    @model_validator(mode="after")
    def validate_and_normalize_graph(self) -> StageGraphBlueprint:
        stage_ids = [stage.stage_id for stage in self.stages]
        if len(stage_ids) != len(set(stage_ids)):
            raise ValueError("stage identities must be unique")
        known_stages = set(stage_ids)
        groups = self.fairness_groups
        if not groups and all(stage.fairness_group_id == "default" for stage in self.stages):
            groups = (FairnessGroup(group_id="default", weight=1),)
        if not groups:
            raise ValueError("StageGraph fairness groups cannot be empty")
        group_ids = [group.group_id for group in groups]
        if len(group_ids) != len(set(group_ids)):
            raise ValueError("fairness group identities must be unique")
        if not {stage.fairness_group_id for stage in self.stages} <= set(group_ids):
            raise ValueError("every stage fairness group must have an authored weight")

        stage_by_id = {stage.stage_id: stage for stage in self.stages}
        dependency_ids = [item.dependency_id for item in self.dependencies]
        if len(dependency_ids) != len(set(dependency_ids)):
            raise ValueError("dependency identities must be unique")
        join_keys = [(item.consumer_stage_id, item.join_id) for item in self.joins]
        if len(join_keys) != len(set(join_keys)):
            raise ValueError("join identities must be unique in their consumer stage")
        joins = {(item.consumer_stage_id, item.join_id): item for item in self.joins}
        edges_by_join: dict[tuple[str, str], set[str]] = {}
        structural_dependencies: dict[str, set[str]] = {
            stage_id: set() for stage_id in stage_ids
        }
        for edge in self.dependencies:
            if (
                edge.consumer_stage_id not in known_stages
                or edge.producer_stage_id not in known_stages
                or edge.consumer_stage_id == edge.producer_stage_id
            ):
                raise ValueError("dependency references an invalid producer or consumer")
            join_key = (edge.consumer_stage_id, edge.join_id)
            if join_key not in joins:
                raise ValueError("dependency references an unknown join")
            producer_outputs = {
                item.output_slot_id for item in stage_by_id[edge.producer_stage_id].output_slots
            }
            consumer_inputs = {
                item.input_slot_id for item in stage_by_id[edge.consumer_stage_id].input_slots
            }
            if edge.producer_output_slot_id not in producer_outputs:
                raise ValueError("dependency references an unknown producer output slot")
            if edge.consumer_input_slot_id not in consumer_inputs:
                raise ValueError("dependency references an unknown consumer input slot")
            edges_by_join.setdefault(join_key, set()).add(edge.dependency_id)
            structural_dependencies[edge.consumer_stage_id].add(edge.producer_stage_id)
        for key, join in joins.items():
            actual = edges_by_join.get(key, set())
            if actual != set(join.dependency_ids):
                raise ValueError("join dependency registry does not match declared edges")
            non_advisory = sum(
                1
                for edge in self.dependencies
                if (edge.consumer_stage_id, edge.join_id) == key
                and edge.dependency_class != DependencyClass.ADVISORY
            )
            if join.kind == JoinKind.ANY and non_advisory == 0:
                raise ValueError("any joins require a non-advisory dependency")
            if join.kind == JoinKind.MINIMUM and (
                join.minimum is None or join.minimum > non_advisory
            ):
                raise ValueError("minimum joins exceed non-advisory dependency count")

        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(stage_id: str) -> None:
            if stage_id in visiting:
                raise ValueError("StageGraph structural dependency cycle")
            if stage_id in visited:
                return
            visiting.add(stage_id)
            for dependency in structural_dependencies[stage_id]:
                visit(dependency)
            visiting.remove(stage_id)
            visited.add(stage_id)

        for stage_id in stage_ids:
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
        normalized = {
            "stages": tuple(sorted(self.stages, key=lambda item: item.stage_id.encode("utf-8"))),
            "stage_mappings": tuple(
                sorted(
                    self.stage_mappings,
                    key=lambda item: (
                        item.stage_id.encode("utf-8"),
                        item.mapping_id.encode("utf-8"),
                    ),
                )
            ),
            "joins": tuple(
                sorted(
                    self.joins,
                    key=lambda item: (
                        item.consumer_stage_id.encode("utf-8"),
                        item.join_id.encode("utf-8"),
                    ),
                )
            ),
            "dependencies": tuple(
                sorted(
                    self.dependencies,
                    key=lambda item: (
                        item.consumer_stage_id.encode("utf-8"),
                        item.join_id.encode("utf-8"),
                        item.producer_stage_id.encode("utf-8"),
                        item.producer_output_slot_id.encode("utf-8"),
                        item.dependency_id.encode("utf-8"),
                    ),
                )
            ),
            "workflow_obligation_slots": tuple(
                sorted(
                    self.workflow_obligation_slots,
                    key=lambda item: item.obligation_slot_id.encode("utf-8"),
                )
            ),
            "obligation_matrix": tuple(
                sorted(
                    self.obligation_matrix,
                    key=lambda item: (
                        item.obligation_scope.encode("utf-8"),
                        0 if item.owner_stage_id is None else 1,
                        b"" if item.owner_stage_id is None else item.owner_stage_id.encode("utf-8"),
                        item.obligation_slot_id.encode("utf-8"),
                        item.evidence_slot_id.encode("utf-8"),
                    ),
                )
            ),
            "fairness_groups": tuple(
                sorted(groups, key=lambda item: item.group_id.encode("utf-8"))
            ),
            "linked_run_slots": tuple(
                sorted(
                    self.linked_run_slots,
                    key=lambda item: (
                        0 if item.owner_stage_id is None else 1,
                        b"" if item.owner_stage_id is None else item.owner_stage_id.encode("utf-8"),
                        item.linked_run_slot_id.encode("utf-8"),
                    ),
                )
            ),
            "policy_definitions": tuple(
                sorted(
                    self.policy_definitions,
                    key=lambda item: (
                        item.policy_kind.encode("utf-8"),
                        item.scope_kind.encode("utf-8"),
                        item.scope_id.encode("utf-8"),
                        item.policy_id.encode("utf-8"),
                    ),
                )
            ),
            "waits": tuple(
                sorted(
                    self.waits,
                    key=lambda item: (
                        item.scope_kind.encode("utf-8"),
                        item.scope_id.encode("utf-8"),
                        item.wait_id.encode("utf-8"),
                    ),
                )
            ),
            "cycle_limits": tuple(
                sorted(
                    self.cycle_limits,
                    key=lambda item: (
                        item.scope_kind.encode("utf-8"),
                        item.scope_id.encode("utf-8"),
                        item.condition_kind.encode("utf-8"),
                        item.condition_id.encode("utf-8"),
                    ),
                )
            ),
            "invalidation_reuse_declarations": tuple(
                sorted(
                    self.invalidation_reuse_declarations,
                    key=lambda item: (
                        item.scope_kind.encode("utf-8"),
                        item.scope_id.encode("utf-8"),
                        item.declaration_kind.encode("utf-8"),
                        item.declaration_id.encode("utf-8"),
                    ),
                )
            ),
            "capacity_ceilings": tuple(
                sorted(
                    self.capacity_ceilings,
                    key=lambda item: (
                        item.scope_kind.encode("utf-8"),
                        item.scope_id.encode("utf-8"),
                        item.dimension_kind.encode("utf-8"),
                        item.dimension_id.encode("utf-8"),
                    ),
                )
            ),
            "completion_obligations": tuple(
                sorted(
                    self.completion_obligations,
                    key=lambda item: (
                        item.obligation_scope.encode("utf-8"),
                        0 if item.owner_stage_id is None else 1,
                        b"" if item.owner_stage_id is None else item.owner_stage_id.encode("utf-8"),
                        item.obligation_slot_id.encode("utf-8"),
                    ),
                )
            ),
        }
        for field_name, collection in normalized.items():
            keys = [item.model_dump_json() for item in collection]
            if len(keys) != len(set(keys)):
                raise ValueError(f"{field_name} contains a duplicate complete key")
        return self

    @property
    def declared_output_slots(self) -> frozenset[str]:
        return frozenset(
            slot.output_slot_id for stage in self.stages for slot in stage.output_slots
        )


class GoalSessionRolloverPolicy(Contract):
    """Authored context lifecycle for bounded GoalDirected iterations."""

    session_mode: Literal["reuse", "fresh", "fresh_from_handoff"] = "reuse"
    fresh_agent_token_threshold: int = Field(default=100_000, ge=1)
    handoff_token_reserve: int = Field(default=4_000, ge=0)
    rollover_mode: Literal["fresh", "fresh_from_handoff"] = "fresh_from_handoff"
    context_selection_policy_ref: str = Field(min_length=1)
    context_compaction_policy_ref: str = Field(min_length=1)
    protected_fact_classes: frozenset[str] = Field(min_length=1)
    max_rollovers: int = Field(ge=0)
    compaction_failure_action: Literal["retry", "fresh_from_handoff", "pause", "escalate"]


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
    authority_breach_action: Literal["fail", "escalate"]
    hard_budget_action: Literal["partial_or_fail"] = "partial_or_fail"
    irrecoverable_failure_action: Literal["partial_or_fail"] = "partial_or_fail"
    no_progress_action: Literal["pause", "revise", "escalate", "partial_or_fail"]
    repeated_blocker_action: Literal["pause", "revise", "escalate", "partial_or_fail"]
    iteration_limit_action: Literal["partial_or_fail"] = "partial_or_fail"
    soft_budget_action: Literal["continue", "reduce_effort", "skip_degradable"]


class GoalVerifierPolicy(Contract):
    operation_class: str = Field(min_length=1)
    binding_ref: str = Field(min_length=1)
    rubric_ref: str = Field(min_length=1)
    rubric_version: int = Field(ge=1)
    acceptance_version: int = Field(ge=1)
    output_contract_ref: str = Field(min_length=1)


class GoalHandoffPolicy(Contract):
    schema_version: Literal["belllabs.goal-handoff.v1"] = "belllabs.goal-handoff.v1"
    handoff_required_for_fresh_session: Literal[True] = True
    max_instruction_bytes: int = Field(ge=1, le=65_536)
    allowed_workspace_ref_classes: frozenset[str] = Field(min_length=1)
    allowed_snapshot_ref_classes: frozenset[str] = Field(min_length=1)


GoalProtectedField = Literal[
    "objective",
    "acceptance",
    "invariants",
    "admitted_inputs",
    "authority",
    "budget",
    "prohibited_work",
    "required_outputs",
    "linked_run_permissions",
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
            "required_outputs",
            "linked_run_permissions",
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
            "required_outputs",
            "linked_run_permissions",
        }
        if value != required:
            raise ValueError("GoalDirected revisions must protect the complete launch envelope")
        return value


class GoalDirectedBlueprint(DefinitionBase):
    kind: Literal[DefinitionKind.BLUEPRINT] = DefinitionKind.BLUEPRINT
    family: Literal["GoalDirected"] = "GoalDirected"
    objective_contract: str = Field(min_length=1)
    acceptance_contract: str = Field(min_length=1)
    admitted_input_classes: frozenset[str] = Field(min_length=1)
    authority_ceiling: AuthorityCeiling
    prohibited_work: frozenset[str] = Field(min_length=1)
    required_output_contracts: frozenset[str] = Field(min_length=1)
    required_obligation_refs: frozenset[str] = Field(min_length=1)
    independent_verification_required: Literal[True] = True
    verifier_policy: GoalVerifierPolicy
    allowed_operation_classes: frozenset[str] = Field(min_length=1)
    allowed_async_subgoal_classes: frozenset[str] = Field(min_length=1)
    allowed_linked_run_slot_ids: frozenset[str] = Field(min_length=1)
    session_policy: GoalSessionRolloverPolicy
    handoff_policy: GoalHandoffPolicy
    workspace_policy: GoalWorkspaceSnapshotPolicy = Field(
        default_factory=GoalWorkspaceSnapshotPolicy
    )
    convergence_policy: GoalConvergencePolicy
    iteration_reservation: dict[str, int] = Field(default_factory=lambda: {"goal.iterations": 1})
    protected_scope_policy: GoalProtectedScopePolicy = Field(
        default_factory=GoalProtectedScopePolicy
    )
    max_iterations: int = Field(ge=1)
    variant_names: frozenset[str] = Field(default_factory=frozenset)

    @field_validator(
        "admitted_input_classes",
        "prohibited_work",
        "required_output_contracts",
        "required_obligation_refs",
        "allowed_operation_classes",
        "allowed_async_subgoal_classes",
        "allowed_linked_run_slot_ids",
    )
    @classmethod
    def governed_sets_are_declared(cls, value: frozenset[str]) -> frozenset[str]:
        if not value or any(not item for item in value):
            raise ValueError("GoalDirected governed sets require non-empty values")
        return value

    @field_validator("iteration_reservation")
    @classmethod
    def iteration_reservation_is_bounded(cls, value: dict[str, int]) -> dict[str, int]:
        if any(not dimension or amount < 0 for dimension, amount in value.items()):
            raise ValueError("goal iteration reservations require names and non-negative amounts")
        if value.get("goal.iterations", 0) < 1:
            raise ValueError("goal iteration reservations require one goal.iterations unit")
        return value

    @model_validator(mode="after")
    def envelope_is_complete_and_bounded(self) -> GoalDirectedBlueprint:
        if self.verifier_policy.operation_class in self.allowed_operation_classes:
            raise ValueError(
                "independent verifier operation class must differ from executor classes"
            )
        if any(
            amount > self.authority_ceiling.budgets.dimensions.get(dimension, -1)
            for dimension, amount in self.iteration_reservation.items()
        ):
            raise ValueError("goal iteration reservation exceeds the frozen authority budget")
        if self.session_policy.rollover_mode == "fresh_from_handoff" and (
            not self.handoff_policy.handoff_required_for_fresh_session
        ):
            raise ValueError("fresh-from-handoff rollover requires a typed handoff")
        return self


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
