from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SchemaContextSelectionRequest(StrictModel):
    request_id: str
    purpose: Literal["pre_ingestion_graph_reconciliation"]
    intended_operations: tuple[str, ...]
    schema_definition_ref: str
    schema_definition_digest: str
    catalog_digest: str
    report_ref: str
    report_digest: str
    coverage_obligations: tuple[str, ...]
    workspace_ref: str
    created_at: datetime


class PropertyIntentHint(StrictModel):
    node_type: str
    properties: tuple[str, ...]


class SchemaContextSelectionDraft(StrictModel):
    """Semantic selector output before trusted host lineage binding."""

    selected_node_types: tuple[str, ...] = Field(max_length=16)
    selected_relationship_types: tuple[str, ...] = Field(max_length=24)
    property_intent_hints: tuple[PropertyIntentHint, ...] = Field(max_length=20)
    rationale: str = Field(max_length=1200)
    evidence_locators: tuple[str, ...] = Field(max_length=12)
    explicit_exclusions: tuple[str, ...] = Field(max_length=12)
    unresolved_mappings: tuple[str, ...] = Field(max_length=12)
    near_miss_candidates: tuple[str, ...] = Field(max_length=12)


class SchemaContextSelection(StrictModel):
    selection_id: str
    revision: int = Field(ge=1, le=2)
    purpose: Literal["pre_ingestion_graph_reconciliation"]
    schema_definition_ref: str
    schema_definition_digest: str
    catalog_digest: str
    report_ref: str
    report_digest: str
    selected_node_types: tuple[str, ...]
    selected_relationship_types: tuple[str, ...]
    property_intent_hints: tuple[PropertyIntentHint, ...]
    coverage_obligations: tuple[str, ...]
    rationale: str = Field(max_length=6000)
    evidence_locators: tuple[str, ...] = Field(max_length=80)
    explicit_exclusions: tuple[str, ...] = Field(default_factory=tuple, max_length=80)
    unresolved_mappings: tuple[str, ...] = Field(default_factory=tuple, max_length=80)
    near_miss_candidates: tuple[str, ...] = Field(default_factory=tuple, max_length=80)
    parent_selection_id: str | None = None
    created_at: datetime

    @model_validator(mode="after")
    def canonical_membership(self) -> SchemaContextSelection:
        for label, values in (
            ("selected node", self.selected_node_types),
            ("selected relationship", self.selected_relationship_types),
        ):
            if len(set(values)) != len(values):
                raise ValueError(f"duplicate {label} names are forbidden")
            if tuple(sorted(values)) != values:
                raise ValueError(f"{label} names must be canonically sorted")
            if any("." in value for value in values):
                raise ValueError("properties cannot be semantic selection members")
        return self


class SelectionValidationDiagnostic(StrictModel):
    structurally_valid: bool
    errors: tuple[str, ...]
    warnings: tuple[str, ...]
    required_endpoint_nodes: tuple[str, ...]
    validation_digest: str


class SchemaSelectionReview(StrictModel):
    review_id: str
    selection_id: str
    reviewer_role: str
    decision: Literal["accepted", "rejected", "revision_required"]
    structural_valid: bool
    coverage_findings: tuple[str, ...]
    missing_concepts: tuple[str, ...]
    overbroad_selections: tuple[str, ...]
    unjustified_selections: tuple[str, ...]
    temporal_coverage: str = Field(max_length=2000)
    identity_coverage: str = Field(max_length=2000)
    provenance_coverage: str = Field(max_length=2000)
    near_miss_assessment: str = Field(max_length=3000)
    required_revisions: tuple[str, ...]
    rationale: str = Field(max_length=6000)
    created_at: datetime


class AcceptedSchemaContextSelection(StrictModel):
    selection: SchemaContextSelection
    deterministic_validation_digest: str
    independent_review_digest: str
    acceptance_decision: Literal["accepted"]
    accepted_selection_digest: str
    accepted_at: datetime


class ExpandedSchemaSlice(StrictModel):
    selected_node_definitions: dict[str, dict[str, Any]]
    selected_relationship_definitions: dict[str, dict[str, Any]]
    relationship_property_types: dict[str, dict[str, Any]]
    required_enums: dict[str, tuple[str, ...]]
    required_unions: dict[str, tuple[str, ...]]
    implemented_interfaces: dict[str, dict[str, Any]]
    relevant_directives: dict[str, tuple[dict[str, Any], ...]]
    fulltext_declarations: tuple[dict[str, Any], ...]
    vector_declarations: tuple[dict[str, Any], ...]
    identity_candidates: dict[str, tuple[str, ...]]
    selected_sdl: str
    closure_diagnostics: tuple[str, ...]
    accepted_selection_digest: str
    source_schema_digest: str
    expansion_policy_version: str
    expanded_slice_digest: str


class SchemaOperationProjection(StrictModel):
    projection_id: str
    version: str
    purpose: Literal["read_query_reconciliation"]
    source_schema_digest: str
    accepted_selection_digest: str
    expanded_slice_digest: str
    allowed_node_labels: tuple[str, ...]
    allowed_relationship_types: tuple[str, ...]
    allowed_properties_by_label: dict[str, tuple[str, ...]]
    allowed_relationship_properties: dict[str, tuple[str, ...]]
    allowed_traversals: tuple[dict[str, str], ...]
    identity_fields_by_label: dict[str, tuple[str, ...]]
    exact_range_search_capabilities: dict[str, tuple[str, ...]]
    fulltext_capabilities: tuple[dict[str, Any], ...]
    vector_capabilities: tuple[dict[str, Any], ...]
    permitted_query_kinds: tuple[
        Literal[
            "exact_identity",
            "fulltext_search",
            "bounded_neighborhood",
            "entity_details",
            "vector_search",
        ],
        ...,
    ]
    procedure_allowlist: tuple[str, ...]
    default_limit: int = Field(ge=1)
    maximum_limit: int = Field(ge=1)
    maximum_traversal_depth: int = Field(ge=1, le=3)
    timeout_seconds: float = Field(gt=0, le=60)
    result_policy: dict[str, int]
    live_capability_diagnostics: tuple[str, ...]
    projection_digest: str


QueryKind = Literal[
    "exact_identity",
    "fulltext_search",
    "bounded_neighborhood",
    "entity_details",
    "vector_search",
]


class QueryExecutionIntent(StrictModel):
    intent_id: str
    sequence: int = Field(ge=1)
    purpose: Literal["pre_ingestion_graph_reconciliation"]
    query_kind: QueryKind
    projection_id: str
    projection_digest: str
    schema_definition_digest: str
    selection_digest: str
    goal: str = Field(max_length=2000)
    coverage_obligation_ids: tuple[str, ...]
    labels: tuple[str, ...]
    relationship_types: tuple[str, ...]
    parameters: dict[str, Any]
    requested_fields: tuple[str, ...]
    limit: int = Field(ge=1)
    max_depth: int = Field(ge=0)
    stopping_evidence: str = Field(max_length=2000)
    semantic_query_text: str | None = Field(default=None, max_length=2000)
    proposed_cypher: str | None = Field(default=None, max_length=8000)
    created_at: datetime


class QueryExecutionResult(StrictModel):
    result_id: str
    intent_id: str
    intent_digest: str
    query_kind: QueryKind
    status: Literal["succeeded", "rejected", "failed"]
    compiled_cypher: str | None
    redacted_parameters: dict[str, Any]
    columns: tuple[str, ...]
    records: tuple[dict[str, Any], ...]
    record_count: int = Field(ge=0)
    truncated: bool
    elapsed_ms: int = Field(ge=0)
    database: str | None
    server_info: dict[str, str]
    diagnostics: tuple[str, ...]
    error_type: str | None
    started_at: datetime
    finished_at: datetime
    result_digest: str


class SchemaDeploymentAttestation(StrictModel):
    attestation_kind: Literal["test_only"]
    environment: str
    database: str
    schema_definition_ref: str
    deployed_sdl_digest: str
    issuer: Literal["schema-context-selection-sandbox"]
    issued_at: datetime
    production_usable: Literal[False]
    attestation_digest: str


class SchemaCompatibilityDecision(StrictModel):
    compatible: bool
    schema_definition_digest: str
    deployed_sdl_digest: str
    attestation_digest: str
    reason: str
    decided_at: datetime


class IntentResultReference(StrictModel):
    intent_id: str
    result_id: str


class EvidenceAttribute(StrictModel):
    name: str
    value: str


class MatchedExistingEntity(StrictModel):
    entity_type: str
    entity_id: str | None
    name: str | None
    match_method: str
    result_ids: tuple[str, ...]
    attributes: tuple[EvidenceAttribute, ...]


class ExistingRelationshipEvidence(StrictModel):
    source_id: str
    relationship_type: str
    target_id: str
    result_id: str


class GraphReconciliationEvidence(StrictModel):
    reconciliation_question: str
    query_goals: tuple[str, ...]
    intent_result_references: tuple[IntentResultReference, ...]
    matched_existing_entities: tuple[MatchedExistingEntity, ...]
    existing_relationships: tuple[ExistingRelationshipEvidence, ...]
    aliases_used: tuple[str, ...]
    match_method: str
    confidence: str
    unresolved_candidates: tuple[str, ...]
    schema_mismatches: tuple[str, ...]
    legacy_name_mappings: tuple[str, ...]
    query_failures: tuple[str, ...]
    stopping_rationale: str


class ReportGraphReconciliationResult(StrictModel):
    run_id: str
    status: Literal["build_only", "offline", "completed", "selection_rejected", "failed"]
    input_digests: dict[str, str]
    schema_digest: str
    catalog_digest: str
    model: str
    selection_ref: str | None
    review_ref: str | None
    expanded_slice_ref: str | None
    projection_ref: str | None
    compatibility_decision: SchemaCompatibilityDecision | None
    query_result_references: tuple[str, ...]
    reconciliation_evidence: GraphReconciliationEvidence | None
    usage: dict[str, int]
    timings: dict[str, int]
    evaluation_metrics: dict[str, Any]
    artifact_root: str
    warnings: tuple[str, ...]
