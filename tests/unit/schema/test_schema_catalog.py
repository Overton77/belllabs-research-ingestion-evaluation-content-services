from __future__ import annotations

from pathlib import Path

import pytest

from app.application.schema.schema_catalog import materialize_schema_catalog, parse_schema_catalog
from app.domain.schema_context.errors import SchemaParseError
from tests.schema_context_helpers import SDL


def test_catalog_parses_directives_indexes_aliases_and_is_deterministic(tmp_path: Path) -> None:
    first = parse_schema_catalog(SDL, "fixture.graphql")
    second = parse_schema_catalog(SDL, "fixture.graphql")

    assert first.catalog_digest == second.catalog_digest
    assert first.relationships["OFFERS"]["endpoints"]
    assert first.nodes["Organization"]["identity_candidates"] == ["id"]
    assert first.nodes["Organization"]["aliases"]
    assert first.fulltext_indexes[0]["indexName"] == "OrganizationName"
    assert first.vector_indexes[0]["indexName"] == "ProductEmbedding"
    assert any(item["name"] == "mystery" for item in first.nodes["Organization"]["directives"])

    manifest = materialize_schema_catalog(first, SDL, tmp_path / "schema")
    assert (tmp_path / "schema/cards/nodes/Organization.json").exists()
    assert (tmp_path / "schema/skills/schema-navigation/SKILL.md").exists()
    assert all(item["read_only"] for item in manifest["resources"])
    product = __import__("json").loads(
        (tmp_path / "schema/cards/nodes/Product.json").read_text(encoding="utf-8")
    )
    assert any(item["field"] == "organization" for item in product["incoming_relationships"])
    offers = __import__("json").loads(
        (tmp_path / "schema/cards/relationships/OFFERS.json").read_text(encoding="utf-8")
    )
    assert all(
        "MATCH (a:Organization)-[r:OFFERS]->(b:Product)" in pattern
        for pattern in offers["example_read_patterns"]
    )


def test_source_or_generator_input_changes_catalog_identity() -> None:
    first = parse_schema_catalog(SDL, "fixture.graphql")
    changed = parse_schema_catalog(SDL + b"\ntype Extra { id: ID }\n", "fixture.graphql")
    assert first.catalog_digest != changed.catalog_digest


def test_malformed_sdl_is_typed_error() -> None:
    with pytest.raises(SchemaParseError):
        parse_schema_catalog(b"type Broken {", "broken.graphql")
