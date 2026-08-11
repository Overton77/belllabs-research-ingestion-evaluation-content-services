from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.schema.graph_query import compile_query_intent, validate_query_intent
from app.domain.schema_context.contracts import QueryExecutionIntent
from app.domain.schema_context.errors import QueryIntentRejected
from app.domain.schema_context.expansion import expand_selection
from app.domain.schema_context.projection import build_operation_projection
from tests.schema_context_helpers import accepted, catalog


def _projection():  # type: ignore[no-untyped-def]
    value = catalog()
    admitted = accepted(value)
    expanded = expand_selection(admitted, value)
    return build_operation_projection(
        admitted,
        expanded,
        live_indexes=({"name": "OrganizationName", "state": "ONLINE"},),
    )


def _intent(**updates):  # type: ignore[no-untyped-def]
    projection = _projection()
    values = {
        "intent_id": "intent-1",
        "sequence": 1,
        "purpose": "pre_ingestion_graph_reconciliation",
        "query_kind": "exact_identity",
        "projection_id": projection.projection_id,
        "projection_digest": projection.projection_digest,
        "schema_definition_digest": projection.source_schema_digest,
        "selection_digest": projection.accepted_selection_digest,
        "goal": "Find organization",
        "coverage_obligation_ids": ("organization_identity",),
        "labels": ("Organization",),
        "relationship_types": (),
        "parameters": {"field": "name", "value": "TruDiagnostic"},
        "requested_fields": ("id", "name"),
        "limit": 5,
        "max_depth": 0,
        "stopping_evidence": "Exact identity result",
        "semantic_query_text": None,
        "proposed_cypher": None,
        "created_at": datetime.now(UTC),
    }
    values.update(updates)
    return QueryExecutionIntent(**values), projection


def test_exact_and_neighborhood_queries_compile_deterministically() -> None:
    intent, projection = _intent()
    validate_query_intent(intent, projection)
    cypher, parameters = compile_query_intent(intent)
    assert "MATCH (n:`Organization`)" in cypher
    assert parameters["value"] == "TruDiagnostic"

    neighborhood, projection = _intent(
        intent_id="intent-2",
        query_kind="bounded_neighborhood",
        relationship_types=("OFFERS",),
        max_depth=1,
    )
    validate_query_intent(neighborhood, projection)
    cypher, _ = compile_query_intent(neighborhood)
    assert "[r:`OFFERS`]" in cypher


@pytest.mark.parametrize(
    "updates",
    [
        {"limit": 101},
        {"max_depth": 2},
        {"labels": ("Unknown",)},
        {"requested_fields": ("unknownProperty",)},
        {"relationship_types": ("UNKNOWN_RELATIONSHIP",)},
        {"parameters": {"field": "name", "value": "x", "password": "secret"}},
        {"parameters": {"field": "name", "value": "x", "neo4j_uri": "secret"}},
        {"proposed_cypher": "CREATE (n)"},
        {"proposed_cypher": "MATCH (n) RETURN n"},
    ],
)
def test_intent_rejects_out_of_projection_or_unsafe_requests(updates) -> None:  # type: ignore[no-untyped-def]
    intent, projection = _intent(**updates)
    with pytest.raises(QueryIntentRejected):
        validate_query_intent(intent, projection)


def test_vector_intent_rejects_raw_embedding() -> None:
    intent, projection = _intent(
        query_kind="vector_search",
        parameters={"index": "ProductEmbedding", "embedding": [0.1, 0.2]},
        semantic_query_text="epigenetic product",
    )
    with pytest.raises(QueryIntentRejected):
        validate_query_intent(intent, projection)


def test_fulltext_intent_rejects_an_unobserved_index() -> None:
    intent, projection = _intent(
        query_kind="fulltext_search",
        parameters={"index": "InventedIndex", "query": "TruDiagnostic"},
    )
    with pytest.raises(QueryIntentRejected, match="not admitted and online"):
        validate_query_intent(intent, projection)
