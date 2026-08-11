from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.application.schema.schema_catalog import SchemaCatalog
from app.domain.schema_context.canonicalization import sha256_digest
from app.domain.schema_context.contracts import (
    AcceptedSchemaContextSelection,
    SchemaCompatibilityDecision,
    SchemaContextSelection,
    SchemaContextSelectionRequest,
    SchemaDeploymentAttestation,
    SchemaSelectionReview,
    SelectionValidationDiagnostic,
)
from app.domain.schema_context.errors import SchemaSelectionValidationError


def validate_selection(
    request: SchemaContextSelectionRequest,
    selection: SchemaContextSelection,
    catalog: SchemaCatalog,
) -> SelectionValidationDiagnostic:
    errors: list[str] = []
    warnings: list[str] = []
    if selection.purpose != request.purpose:
        errors.append("selection purpose differs from request")
    for field in ("schema_definition_digest", "catalog_digest", "report_digest"):
        if getattr(selection, field) != getattr(request, field):
            errors.append(f"selection {field} differs from request")
    unknown_nodes = sorted(set(selection.selected_node_types) - set(catalog.nodes))
    unknown_relationships = sorted(
        set(selection.selected_relationship_types) - set(catalog.relationships)
    )
    if unknown_nodes:
        errors.append(f"unknown node types: {', '.join(unknown_nodes)}")
    if unknown_relationships:
        errors.append(f"unknown relationship types: {', '.join(unknown_relationships)}")

    required_endpoints: set[str] = set()
    for rel_type in selection.selected_relationship_types:
        relationship = catalog.relationships.get(rel_type)
        if relationship is None:
            continue
        relevant = [
            endpoint
            for endpoint in relationship["endpoints"]
            if endpoint["source"] in selection.selected_node_types
            or endpoint["target"] in selection.selected_node_types
        ]
        if not relevant:
            errors.append(f"relationship {rel_type} has no topology touching selected nodes")
            continue
        for endpoint in relevant:
            required_endpoints.update((endpoint["source"], endpoint["target"]))

    legacy_text = " ".join(
        selection.explicit_exclusions
        + selection.unresolved_mappings
        + selection.near_miss_candidates
        + (selection.rationale,)
    )
    for old, current in (
        ("OrganizationState", "OrganizationSnapshot"),
        ("ProductState", "ProductSnapshot"),
    ):
        if old not in legacy_text or current not in legacy_text:
            warnings.append(f"legacy mapping not explicitly resolved: {old} -> {current}")
    payload = {
        "structurally_valid": not errors,
        "errors": sorted(errors),
        "warnings": sorted(warnings),
        "required_endpoint_nodes": sorted(required_endpoints),
    }
    return SelectionValidationDiagnostic(
        **payload,
        validation_digest=sha256_digest(payload),
    )


def accept_selection(
    selection: SchemaContextSelection,
    validation: SelectionValidationDiagnostic,
    review: SchemaSelectionReview,
    *,
    accepted_at: datetime | None = None,
) -> AcceptedSchemaContextSelection:
    if not validation.structurally_valid:
        raise SchemaSelectionValidationError("structurally invalid selection cannot be accepted")
    if review.selection_id != selection.selection_id or review.decision != "accepted":
        raise SchemaSelectionValidationError("independent review did not accept the selection")
    if not review.structural_valid:
        raise SchemaSelectionValidationError("review marked the selection structurally invalid")
    review_digest = sha256_digest(review.model_dump(mode="json"))
    selection_digest = sha256_digest(selection.model_dump(mode="json"))
    payload = {
        "selection_digest": selection_digest,
        "validation_digest": validation.validation_digest,
        "review_digest": review_digest,
        "decision": "accepted",
    }
    return AcceptedSchemaContextSelection(
        selection=selection,
        deterministic_validation_digest=validation.validation_digest,
        independent_review_digest=review_digest,
        acceptance_decision="accepted",
        accepted_selection_digest=sha256_digest(payload),
        accepted_at=accepted_at or datetime.now(UTC),
    )


def make_test_attestation(
    *,
    environment: str,
    database: str,
    schema_definition_ref: str,
    schema_definition_digest: str,
    issued_at: datetime | None = None,
) -> SchemaDeploymentAttestation:
    timestamp = issued_at or datetime.now(UTC)
    payload: dict[str, Any] = {
        "attestation_kind": "test_only",
        "environment": environment,
        "database": database,
        "schema_definition_ref": schema_definition_ref,
        "deployed_sdl_digest": schema_definition_digest,
        "issuer": "schema-context-selection-sandbox",
        "issued_at": timestamp.isoformat(),
        "production_usable": False,
    }
    return SchemaDeploymentAttestation(
        **payload,
        attestation_digest=sha256_digest(payload),
    )


def decide_compatibility(
    schema_definition_digest: str,
    attestation: SchemaDeploymentAttestation,
    *,
    decided_at: datetime | None = None,
) -> SchemaCompatibilityDecision:
    compatible = (
        attestation.attestation_kind == "test_only"
        and not attestation.production_usable
        and attestation.deployed_sdl_digest == schema_definition_digest
    )
    return SchemaCompatibilityDecision(
        compatible=compatible,
        schema_definition_digest=schema_definition_digest,
        deployed_sdl_digest=attestation.deployed_sdl_digest,
        attestation_digest=attestation.attestation_digest,
        reason=(
            "exact test-only SDL digest match"
            if compatible
            else "deployed SDL digest does not exactly match the catalog source"
        ),
        decided_at=decided_at or datetime.now(UTC),
    )
