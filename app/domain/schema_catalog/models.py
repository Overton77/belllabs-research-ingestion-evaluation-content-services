from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Directive(FrozenModel):
    name: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class FieldDefinition(FrozenModel):
    name: str
    description: str | None = None
    type_expression: str
    named_type: str
    nullable: bool
    is_list: bool
    directives: tuple[Directive, ...] = ()


class IndexDeclaration(FrozenModel):
    node_type: str
    directive: Literal["fulltext", "vector"]
    arguments: dict[str, Any]


class ObjectType(FrozenModel):
    name: str
    description: str | None = None
    interfaces: tuple[str, ...] = ()
    directives: tuple[Directive, ...] = ()
    fields: tuple[FieldDefinition, ...] = ()
    identity_candidates: tuple[str, ...] = ()
    search_candidates: tuple[str, ...] = ()
    aliases: tuple[dict[str, Any], ...] = ()
    sdl: str


class InterfaceType(FrozenModel):
    name: str
    description: str | None = None
    directives: tuple[Directive, ...] = ()
    fields: tuple[FieldDefinition, ...] = ()
    sdl: str


class RelationshipEndpoint(FrozenModel):
    element_id: str
    relationship_type: str
    declaring_type: str
    related_type: str
    field_name: str
    direction: Literal["IN", "OUT"]
    properties_type: str | None = None
    field_type: str
    directives: tuple[Directive, ...] = ()
    physical_start_type: str
    physical_end_type: str


class RelationshipType(FrozenModel):
    name: str
    endpoints: tuple[RelationshipEndpoint, ...]
    property_types: tuple[str, ...] = ()


class PhysicalSchemaCatalog(FrozenModel):
    source_ref: str
    source_digest: str
    source_bytes: int
    generator_version: str
    catalog_digest: str
    nodes: dict[str, ObjectType]
    relationship_property_types: dict[str, ObjectType]
    relationships: dict[str, RelationshipType]
    enums: dict[str, tuple[str, ...]]
    unions: dict[str, tuple[str, ...]]
    interfaces: dict[str, InterfaceType]
    other_definitions: dict[str, str]
    fulltext_indexes: tuple[IndexDeclaration, ...]
    vector_indexes: tuple[IndexDeclaration, ...]

    def element_ids(self) -> frozenset[str]:
        values = {f"node:{name}" for name in self.nodes}
        values.update(f"relationship-property:{name}" for name in self.relationship_property_types)
        values.update(f"relationship-type:{name}" for name in self.relationships)
        values.update(f"interface:{name}" for name in self.interfaces)
        values.update(f"enum:{name}" for name in self.enums)
        values.update(f"union:{name}" for name in self.unions)
        values.update(
            endpoint.element_id
            for relationship in self.relationships.values()
            for endpoint in relationship.endpoints
        )
        return frozenset(values)


class Archetype(StrEnum):
    ENTITY = "entity"
    STATE = "state-or-snapshot"
    EVENT = "event-or-occurrence"
    INFORMATION_ARTIFACT = "information-artifact"
    ASSERTION_EVIDENCE = "assertion-or-evidence"
    MEASUREMENT_VALUE = "measurement-or-value"
    RELATIONSHIP_PROPERTIES = "relationship-properties"
    OPERATIONAL_INTERNAL = "operational-or-internal"


class ClosurePolicy(StrEnum):
    NONE = "none"
    INCIDENT_RELATIONSHIPS = "incident-relationships"
    ONE_HOP = "one-hop"


class Maturity(StrEnum):
    ACTIVE = "active"
    PROVISIONAL = "provisional"
    DEPRECATED = "deprecated"


class ModuleSemantics(FrozenModel):
    name: str = Field(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
    purpose: str = Field(min_length=1)
    description: str | None = None
    seed_elements: tuple[str, ...] = ()
    inclusion_rules: tuple[str, ...] = ()
    closure_policy: ClosurePolicy = ClosurePolicy.NONE


class ElementSemantics(FrozenModel):
    description: str = Field(min_length=1)
    aliases: tuple[str, ...] = ()
    synonyms: tuple[str, ...] = ()
    acronyms: tuple[str, ...] = ()
    legacy_names: tuple[str, ...] = ()
    archetypes: tuple[Archetype, ...]
    modules: tuple[str, ...] = ()
    identity_semantics: str | None = None
    temporal_role: str | None = None
    provenance_role: str | None = None
    maturity: Maturity = Maturity.ACTIVE
    replacement_element: str | None = None
    task_cues: tuple[str, ...] = ()
    common_confusions: tuple[str, ...] = ()
    negative_examples: tuple[str, ...] = ()


class SemanticOverlay(FrozenModel):
    overlay_version: Literal["1"] = "1"
    modules: tuple[ModuleSemantics, ...]
    elements: dict[str, ElementSemantics]


class ValidationIssue(FrozenModel):
    code: str
    message: str
    element_id: str | None = None


class DerivedElement(FrozenModel):
    element_id: str
    physical_name: str
    kind: str
    semantics: ElementSemantics | None = None
    neighboring_elements: tuple[str, ...] = ()


class DerivedCatalog(FrozenModel):
    physical: PhysicalSchemaCatalog
    overlay: SemanticOverlay
    elements: dict[str, DerivedElement]
    modules: dict[str, tuple[str, ...]]
    incoming_by_node: dict[str, tuple[str, ...]]
    outgoing_by_node: dict[str, tuple[str, ...]]


class TierZeroNodeMetadata(FrozenModel):
    description: str | None = None
    archetypes: tuple[Archetype, ...] = ()
    modules: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()


class TierZeroCatalog(FrozenModel):
    catalog_digest: str
    source_digest: str
    node_names: tuple[str, ...]
    relationship_property_types: tuple[str, ...]
    node_metadata: dict[str, TierZeroNodeMetadata]
    endpoint_columns: tuple[str, str, str, str]
    relationships: dict[str, tuple[tuple[str, str, str, str], ...]]
    module_definitions: tuple[ModuleSemantics, ...]
