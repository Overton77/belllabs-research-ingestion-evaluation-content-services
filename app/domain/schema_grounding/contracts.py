from __future__ import annotations

from typing import Any, Literal

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.domain.schema_context.contracts import (
    ExpandedSchemaSlice,
    GraphReconciliationEvidence,
    IntentResultReference,
    QueryExecutionIntent,
    QueryKind,
    SchemaOperationProjection,
)

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DurableObjectRef(Contract):
    uri: str = Field(min_length=1)
    digest: str = Field(pattern=DIGEST_PATTERN)
    size_bytes: int = Field(ge=0)
    media_type: str = Field(min_length=1)
    version_id: str | None = None


class SchemaCatalogBuildRequest(Contract):
    build_id: str = Field(min_length=1)
    idempotency_key: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    schema_definition_ref: str = Field(min_length=1)
    schema_definition_digest: str = Field(pattern=DIGEST_PATTERN)
    schema_definition_media_type: str = "application/graphql"
    semantic_overlay_ref: str = Field(min_length=1)
    semantic_overlay_revision: str = Field(min_length=1)
    semantic_overlay_digest: str = Field(pattern=DIGEST_PATTERN)
    candidate_seed_ref: str | None = None
    candidate_seed_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    catalog_schema_version: str = Field(min_length=1)
    generator_version: str = Field(min_length=1)
    normalization_policy_version: str = Field(min_length=1)
    publication_target: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)
    authority_ref: str = Field(min_length=1)
    requested_at: AwareDatetime

    @field_validator(
        "schema_definition_ref",
        "semantic_overlay_ref",
        "candidate_seed_ref",
        "publication_target",
    )
    @classmethod
    def durable_references_only(cls, value: str | None) -> str | None:
        if value is None:
            return value
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or (
            len(normalized) > 2
            and normalized[1] == ":"
            and normalized[0].isalpha()
        ):
            raise ValueError("host absolute paths are not durable schema build identity")
        return value

    @model_validator(mode="after")
    def candidate_seed_binding_is_complete(self) -> SchemaCatalogBuildRequest:
        if (self.candidate_seed_ref is None) != (self.candidate_seed_digest is None):
            raise ValueError(
                "candidate seed reference and digest must be supplied together"
            )
        return self


class CatalogResourceRecord(Contract):
    logical_path: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    media_type: str = Field(min_length=1)
    size_bytes: int = Field(ge=0)
    read_only: Literal[True] = True
    profiles: tuple[str, ...] = ()


class SchemaCatalogBuildRecord(Contract):
    build_id: str
    idempotency_key: str
    request_fingerprint: str = Field(pattern=DIGEST_PATTERN)
    request_scope: str
    status: Literal["published", "rejected"]
    schema_definition_ref: str
    schema_definition_digest: str = Field(pattern=DIGEST_PATTERN)
    semantic_overlay_ref: str
    semantic_overlay_revision: str
    semantic_overlay_digest: str = Field(pattern=DIGEST_PATTERN)
    candidate_seed_ref: str | None = None
    candidate_seed_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    catalog_schema_version: str
    parser_generator_version: str
    normalization_policy_version: str
    physical_schema_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    catalog_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    resource_manifest_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    bundle: DurableObjectRef | None = None
    resource_count: int = Field(ge=0)
    total_bytes: int = Field(ge=0)
    tier0_size_bytes: int = Field(ge=0)
    profiles: dict[str, tuple[str, ...]]
    resources: tuple[CatalogResourceRecord, ...]
    validation_decision: Literal["accepted", "rejected"]
    diagnostics: tuple[str, ...] = ()
    predecessor_build_id: str | None = None
    successor_of_catalog_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    publication_target: str
    published_at: AwareDatetime

    @model_validator(mode="after")
    def accepted_publication_is_consistent(self) -> SchemaCatalogBuildRecord:
        if self.status == "published" and self.validation_decision != "accepted":
            raise ValueError("published catalog builds require an accepted validation decision")
        if self.status == "published" and (
            self.physical_schema_digest is None
            or self.catalog_digest is None
            or self.resource_manifest_digest is None
            or self.bundle is None
        ):
            raise ValueError("published catalog builds require complete digest and bundle lineage")
        if self.status == "rejected" and self.validation_decision != "rejected":
            raise ValueError("rejected catalog builds require a rejected validation decision")
        if self.resource_count != len(self.resources):
            raise ValueError("catalog resource count differs from its immutable manifest")
        return self


class SchemaDeploymentManifestRef(Contract):
    manifest_id: str = Field(min_length=1)
    manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    environment: str = Field(min_length=1)
    database: str = Field(min_length=1)
    schema_definition_ref: str = Field(min_length=1)
    deployed_sdl_digest: str = Field(pattern=DIGEST_PATTERN)
    deployment_id: str = Field(min_length=1)
    issuer_authority_ref: str = Field(min_length=1)
    active: bool
    revoked: bool = False
    issued_at: AwareDatetime


class SchemaWorkspaceBindingRef(Contract):
    binding_id: str = Field(min_length=1)
    binding_digest: str = Field(pattern=DIGEST_PATTERN)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    slot_name: str = Field(min_length=1)
    catalog_build_id: str = Field(min_length=1)
    catalog_digest: str = Field(pattern=DIGEST_PATTERN)
    resource_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    profile: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    read_only: bool
    issuer_authority_ref: str = Field(min_length=1)
    materializer_version: str = Field(min_length=1)
    created_at: AwareDatetime


class GraphCapabilityGrant(Contract):
    grant_id: str = Field(min_length=1)
    grant_digest: str = Field(pattern=DIGEST_PATTERN)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    environment: str = Field(min_length=1)
    database: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    admitted: bool
    query_kinds: frozenset[QueryKind] = frozenset()
    allowed_node_labels: frozenset[str] = frozenset()
    allowed_relationship_types: frozenset[str] = frozenset()
    maximum_limit: int = Field(default=0, ge=0)
    maximum_traversal_depth: int = Field(default=0, ge=0, le=3)
    secret_ref: str | None = None
    budget_reservation_id: str | None = None
    sensitive_data_policy_ref: str = Field(min_length=1)
    decided_by_authority_ref: str = Field(min_length=1)
    decided_at: AwareDatetime


class GraphAdmissionRequest(Contract):
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    environment: str = Field(min_length=1)
    database: str = Field(min_length=1)
    deployment_id: str = Field(min_length=1)
    catalog_build_id: str = Field(min_length=1)
    schema_definition_ref: str = Field(min_length=1)
    schema_definition_digest: str = Field(pattern=DIGEST_PATTERN)
    catalog_digest: str = Field(pattern=DIGEST_PATTERN)
    resource_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    projection_id: str = Field(min_length=1)
    projection_digest: str = Field(pattern=DIGEST_PATTERN)
    deployment_manifest: SchemaDeploymentManifestRef | None
    workspace_binding: SchemaWorkspaceBindingRef | None
    graph_capability: GraphCapabilityGrant | None
    requested_at: AwareDatetime


class GraphAdmissionDecision(Contract):
    decision_id: str = Field(min_length=1)
    admitted: bool
    failure_code: Literal[
        "deployment_manifest_missing",
        "deployment_manifest_revoked",
        "schema_deployment_mismatch",
        "workspace_binding_missing",
        "workspace_profile_invalid",
        "projection_purpose_mismatch",
        "graph_capability_denied",
    ] | None = None
    reason: str = Field(min_length=1)
    deployment_manifest_id: str | None = None
    workspace_binding_id: str | None = None
    graph_capability_grant_id: str | None = None
    schema_definition_digest: str = Field(pattern=DIGEST_PATTERN)
    projection_digest: str = Field(pattern=DIGEST_PATTERN)
    decided_at: AwareDatetime

    @model_validator(mode="after")
    def failure_code_matches_decision(self) -> GraphAdmissionDecision:
        if self.admitted == (self.failure_code is not None):
            raise ValueError("graph admission failure code must exist exactly for denial")
        return self


class SchemaContextDerivationResult(Contract):
    derivation_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    purpose: str = Field(min_length=1)
    accepted_selection_digest: str = Field(pattern=DIGEST_PATTERN)
    expanded_slice: ExpandedSchemaSlice
    projection: SchemaOperationProjection
    derived_at: AwareDatetime


class BoundedQueryPlan(Contract):
    plan_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    question: str = Field(min_length=1, max_length=4_000)
    projection_id: str = Field(min_length=1)
    projection_digest: str = Field(pattern=DIGEST_PATTERN)
    intents: tuple[QueryExecutionIntent, ...] = Field(min_length=1, max_length=100)
    created_at: AwareDatetime

    @model_validator(mode="after")
    def intents_are_ordered_and_projection_bound(self) -> BoundedQueryPlan:
        for expected_sequence, intent in enumerate(self.intents, start=1):
            if intent.sequence != expected_sequence:
                raise ValueError("query-plan intent sequence must be contiguous")
            if (
                intent.projection_id != self.projection_id
                or intent.projection_digest != self.projection_digest
            ):
                raise ValueError("query-plan intent belongs to a different projection")
        return self


class SupportingGraphReconciliationRequest(Contract):
    reconciliation_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    question: str = Field(min_length=1, max_length=4_000)
    admission: GraphAdmissionRequest
    projection: SchemaOperationProjection
    intents: tuple[QueryExecutionIntent, ...] = Field(min_length=1, max_length=100)
    maximum_intents: int = Field(ge=1, le=100)
    created_at: AwareDatetime

    @model_validator(mode="after")
    def request_bindings_match(self) -> SupportingGraphReconciliationRequest:
        if self.admission.request_scope != self.request_scope:
            raise ValueError("graph admission belongs to a different request scope")
        if self.admission.run_id != self.run_id:
            raise ValueError("graph admission belongs to a different Workflow Run")
        if self.projection.projection_id != self.admission.projection_id:
            raise ValueError("graph admission is bound to a different projection")
        if self.projection.projection_digest != self.admission.projection_digest:
            raise ValueError("graph admission projection digest mismatch")
        if self.projection.source_schema_digest != (
            self.admission.schema_definition_digest
        ):
            raise ValueError("projection source schema differs from graph admission")
        if self.projection.purpose != self.admission.purpose:
            raise ValueError("projection purpose differs from graph admission")
        return self


class SupportingGraphReconciliationRecord(Contract):
    reconciliation_id: str
    request_digest: str = Field(pattern=DIGEST_PATTERN)
    request_scope: str
    run_id: str
    status: Literal["completed", "rejected", "failed"]
    question: str
    admission_decision: GraphAdmissionDecision
    intent_result_references: tuple[IntentResultReference, ...]
    evidence: GraphReconciliationEvidence | None = None
    successful_count: int = Field(ge=0)
    zero_result_count: int = Field(ge=0)
    rejected_count: int = Field(ge=0)
    failed_count: int = Field(ge=0)
    projection_id: str
    projection_digest: str = Field(pattern=DIGEST_PATTERN)
    workspace_binding_id: str | None = None
    deployment_manifest_id: str | None = None
    graph_capability_grant_id: str | None = None
    completed_at: AwareDatetime


SchemaGroundingRecordType = Literal[
    "catalog_build",
    "catalog_resource",
    "selection_draft",
    "selection_validation",
    "selection_review",
    "accepted_selection",
    "expanded_slice",
    "operation_projection",
    "compatibility_decision",
    "workspace_binding",
    "query_intent",
    "query_result",
    "reconciliation",
    "evaluation",
]


class SchemaGroundingRecordEnvelope(Contract):
    record_type: SchemaGroundingRecordType
    record_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str | None = None
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    payload: dict[str, Any]
    created_at: AwareDatetime
