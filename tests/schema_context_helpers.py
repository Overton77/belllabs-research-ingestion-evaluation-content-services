from __future__ import annotations

from datetime import UTC, datetime

from app.application.schema.schema_catalog import SchemaCatalog, parse_schema_catalog
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    PropertyIntentHint,
    SchemaContextSelection,
    SchemaContextSelectionRequest,
    SchemaSelectionReview,
)
from app.domain.schema_context.validation import accept_selection, validate_selection

SDL = b"""directive @node on OBJECT
directive @id on FIELD_DEFINITION
directive @alias(property: String!) on FIELD_DEFINITION
directive @relationship(type: String!, direction: Direction!) on FIELD_DEFINITION
directive @relationshipProperties on OBJECT
directive @fulltext(indexes: [FulltextInput!]!) on OBJECT
directive @vector(indexes: [VectorInput!]!) on OBJECT
directive @mystery(value: String) on OBJECT
enum Direction { IN OUT }
input FulltextInput { indexName: String!, queryName: String, fields: [String!]! }
input VectorInput { indexName: String!, queryName: String, embeddingProperty: String! }
type OfferProperties @relationshipProperties { source: String }
type Organization @node @mystery(value: "kept")
  @fulltext(indexes: [{
    indexName: "OrganizationName",
    queryName: "organizationsByName",
    fields: ["name"]
  }]) {
  id: ID! @id
  name: String!
  legacyName: String @alias(property: "name")
  products: [Product!]! @relationship(type: "OFFERS", direction: OUT)
}
type Product @node @vector(indexes: [{
  indexName: "ProductEmbedding",
  queryName: "productsByEmbedding",
  embeddingProperty: "embedding"
}]) {
  id: ID! @id
  name: String!
  embedding: [Float!]
  organization: Organization @relationship(type: "OFFERS", direction: IN)
}
"""


def catalog() -> SchemaCatalog:
    return parse_schema_catalog(SDL, "fixture.graphql")


def request(value: SchemaCatalog) -> SchemaContextSelectionRequest:
    return SchemaContextSelectionRequest(
        request_id="request-1",
        purpose="pre_ingestion_graph_reconciliation",
        intended_operations=("read",),
        schema_definition_ref=value.source_ref,
        schema_definition_digest=value.source_digest,
        catalog_digest=value.catalog_digest,
        report_ref="report.md",
        report_digest="sha256:" + "a" * 64,
        coverage_obligations=("organization_identity",),
        workspace_ref="workspace",
        created_at=datetime.now(UTC),
    )


def selection(value: SchemaCatalog) -> SchemaContextSelection:
    return SchemaContextSelection(
        selection_id="selection-1",
        revision=1,
        purpose="pre_ingestion_graph_reconciliation",
        schema_definition_ref=value.source_ref,
        schema_definition_digest=value.source_digest,
        catalog_digest=value.catalog_digest,
        report_ref="report.md",
        report_digest="sha256:" + "a" * 64,
        selected_node_types=("Organization", "Product"),
        selected_relationship_types=("OFFERS",),
        property_intent_hints=(PropertyIntentHint(node_type="Organization", properties=("name",)),),
        coverage_obligations=("organization_identity",),
        rationale=(
            "Organization and Product reconcile the report; OrganizationState maps to "
            "OrganizationSnapshot and ProductState maps to ProductSnapshot."
        ),
        evidence_locators=("report.md#organization",),
        explicit_exclusions=("Document is not required",),
        unresolved_mappings=(),
        near_miss_candidates=(),
        parent_selection_id=None,
        created_at=datetime.now(UTC),
    )


def accepted(value: SchemaCatalog) -> AcceptedSchemaContextSelection:
    draft = selection(value)
    validation = validate_selection(request(value), draft, value)
    review = SchemaSelectionReview(
        review_id="review-1",
        selection_id=draft.selection_id,
        reviewer_role="independent_schema_reviewer",
        decision="accepted",
        structural_valid=True,
        coverage_findings=("covered",),
        missing_concepts=(),
        overbroad_selections=(),
        unjustified_selections=(),
        temporal_coverage="Snapshots explicitly mapped.",
        identity_coverage="IDs and names covered.",
        provenance_coverage="Document excluded for this bounded purpose.",
        near_miss_assessment="No near miss changes membership.",
        required_revisions=(),
        rationale="Accepted independently.",
        created_at=datetime.now(UTC),
    )
    return accept_selection(draft, validation, review)
