from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

from app.application.schema_catalog import DEFAULT_SEMANTIC_OVERLAY, parse_schema_catalog
from app.domain.schema_catalog import (
    CatalogValidationError,
    derive_catalog,
    load_semantic_overlay,
    parse_physical_schema,
    render_tier_zero,
    require_valid_catalog_overlay,
    semantic_overlay_json_schema,
)

SDL = b"""
directive @node on OBJECT
directive @relationship(
  type: String!, direction: Direction!, properties: String
) on FIELD_DEFINITION
directive @relationshipProperties on OBJECT
enum Direction { IN OUT }
type EdgeProps @relationshipProperties { since: String }
type A @node {
  id: ID!
  incomingBs: [B!]! @relationship(type: "LINKS", direction: IN, properties: "EdgeProps")
}
type B @node {
  id: ID!
  outgoingAs: [A!]! @relationship(type: "LINKS", direction: OUT, properties: "EdgeProps")
}
"""


def _overlay() -> bytes:
    return json.dumps(
        {
            "overlay_version": "1",
            "modules": [
                {
                    "name": "example",
                    "purpose": "Exercise deterministic semantic derivation.",
                    "seed_elements": ["node:A"],
                    "closure_policy": "one-hop",
                }
            ],
            "elements": {
                "node:A": {
                    "description": "The first entity.",
                    "aliases": ["alpha"],
                    "archetypes": ["entity"],
                    "modules": ["example"],
                },
                "relationship-property:EdgeProps": {
                    "description": "Properties recorded on LINKS edges.",
                    "archetypes": ["relationship-properties"],
                    "modules": ["example"],
                },
                "relationship-field:A.incomingBs": {
                    "description": "Incoming LINKS declared from A toward B.",
                    "archetypes": ["operational-or-internal"],
                    "modules": ["example"],
                },
            },
        }
    ).encode()


def test_typed_catalog_is_portable_and_models_physical_direction() -> None:
    first = parse_physical_schema(SDL, "C:/one/schema.graphql")
    relocated = parse_physical_schema(SDL, "/different/location/schema.graphql")

    assert first.catalog_digest == relocated.catalog_digest
    assert "EdgeProps" not in first.nodes
    assert "EdgeProps" in first.relationship_property_types
    incoming = next(
        endpoint
        for endpoint in first.relationships["LINKS"].endpoints
        if endpoint.declaring_type == "A"
    )
    assert incoming.physical_start_type == "B"
    assert incoming.physical_end_type == "A"


def test_overlay_validation_and_derivation_use_typed_element_ids() -> None:
    physical = parse_physical_schema(SDL, "fixture.graphql")
    overlay = load_semantic_overlay(_overlay())
    require_valid_catalog_overlay(physical, overlay)
    derived = derive_catalog(physical, overlay)

    assert "node:B" in derived.modules["example"]
    assert "relationship-type:LINKS" in derived.modules["example"]
    assert "relationship-field:A.incomingBs" in derived.incoming_by_node["A"]
    assert "relationship-field:A.incomingBs" in derived.outgoing_by_node["B"]
    assert semantic_overlay_json_schema()["title"] == "SemanticOverlay"


def test_overlay_drift_fails_closed() -> None:
    payload = json.loads(_overlay())
    payload["elements"]["node:Missing"] = payload["elements"].pop("node:A")
    physical = parse_physical_schema(SDL, "fixture.graphql")
    with pytest.raises(CatalogValidationError, match="unknown-element"):
        require_valid_catalog_overlay(physical, load_semantic_overlay(json.dumps(payload)))


def test_governed_trudiagnostic_overlay_validates_against_authoritative_sdl() -> None:
    project_root = Path(__file__).resolve().parents[1]
    authoritative_sdl = (
        project_root.parent / "biotech-kg" / "src" / "schema" / "neo4jbiotechschema.graphql"
    )
    physical = parse_physical_schema(authoritative_sdl.read_bytes(), str(authoritative_sdl))
    overlay = load_semantic_overlay(DEFAULT_SEMANTIC_OVERLAY)

    require_valid_catalog_overlay(physical, overlay)
    assert 10 <= len(overlay.elements) <= 20
    assert overlay.elements["node:LabTest"].modules == ("diagnostics-and-biomarkers",)
    tier_zero = render_tier_zero(derive_catalog(physical, overlay))
    assert len(tier_zero.model_dump_json().encode()) < 50 * 1024


def test_published_schema_reference_matches_authoritative_source() -> None:
    project_root = Path(__file__).resolve().parents[1]
    authoritative_sdl = project_root.parent / "biotech-kg/src/schema/neo4jbiotechschema.graphql"
    reference = json.loads(
        (project_root / "schema-catalog/source-reference.v1.json").read_text(encoding="utf-8")
    )
    source = authoritative_sdl.read_bytes()

    assert reference["sha256"] == sha256(source).hexdigest()
    assert reference["content_length"] == len(source)
    assert reference["key"].startswith(f"schemas/neo4jbiotechschema/sha256/{reference['sha256']}/")
    assert reference["s3_uri"] == f"s3://{reference['bucket']}/{reference['key']}"
    assert reference["controls"] == {
        "public_access_blocked": True,
        "object_ownership": "BucketOwnerEnforced",
        "versioning": "Enabled",
        "default_encryption": "AES256",
    }


def test_legacy_workflow_catalog_digest_is_portable_and_receives_semantics() -> None:
    plain_first = parse_schema_catalog(SDL, "C:/one/schema.graphql")
    plain_second = parse_schema_catalog(SDL, "/other/schema.graphql")
    assert plain_first.catalog_digest == plain_second.catalog_digest

    project_root = Path(__file__).resolve().parents[1]
    authoritative_sdl = (
        project_root.parent / "biotech-kg" / "src" / "schema" / "neo4jbiotechschema.graphql"
    )
    catalog = parse_schema_catalog(
        authoritative_sdl.read_bytes(),
        str(authoritative_sdl),
        semantic_overlay=DEFAULT_SEMANTIC_OVERLAY,
    )
    assert catalog.nodes["LabTest"]["semantic"]["description"].startswith("A laboratory analysis")


def test_compatibility_details_preserve_non_node_semantics() -> None:
    overlay = load_semantic_overlay(_overlay())
    catalog = parse_schema_catalog(SDL, "fixture.graphql", semantic_overlay=overlay)

    assert catalog.nodes["EdgeProps"]["semantic"]["archetypes"] == ["relationship-properties"]
    endpoint = next(
        item
        for item in catalog.relationships["LINKS"]["endpoints"]
        if item["element_id"] == "relationship-field:A.incomingBs"
    )
    assert endpoint["semantic"]["description"].startswith("Incoming LINKS")
