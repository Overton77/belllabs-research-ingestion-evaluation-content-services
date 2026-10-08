"""FT-A3: lexical recall@3 over the seeded catalog with the fixed evaluation set."""

from __future__ import annotations

from typing import Any

import pytest

from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.domain.authoring.contracts import DefinitionKind
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest
from tests.fixtures.capability_search_catalog import (
    DIMENSIONS,
    MODEL_ID,
    DeterministicEmbeddings,
    build_catalog,
    evaluation_cases,
)

TENANT = "mc/00000000-0000-0000-0000-000000000001/biotech/00000000-0000-0000-0000-000000000002"


def request_for(case: dict[str, Any], limit: int = 3) -> CapabilitySearchRequest:
    return CapabilitySearchRequest(
        query=case["query"],
        tenant_scope=TENANT,
        kinds=frozenset(DefinitionKind(kind) for kind in case.get("kinds", ())),
        host_profiles=frozenset(case.get("host_profiles", ())),
        side_effect_classes=frozenset(case.get("side_effect_classes", ())),
        limit=limit,
    )


async def recall_at_3(service: CapabilitySearchService) -> tuple[float, list[str]]:
    cases = evaluation_cases()
    misses: list[str] = []
    hits = 0
    for case in cases:
        response = await service.search(request_for(case, limit=3))
        ids = [hit.exact_ref.logical_id for hit in response.hits if hit.exact_ref]
        broad = await service.search(request_for(case, limit=20))
        every = {hit.exact_ref.logical_id for hit in broad.hits if hit.exact_ref}
        assert not set(case.get("excluded", ())) & every, case["query"]
        if set(case["expected"]) & set(ids):
            hits += 1
        else:
            misses.append(f"{case['query']!r}: got {ids}")
    return hits / len(cases), misses


def test_evaluation_set_is_large_and_spans_kinds() -> None:
    cases = evaluation_cases()
    assert len(cases) >= 25
    expected = {item for case in cases for item in case["expected"]}
    assert any(item.startswith("mcp.") and ".tool." not in item for item in expected)
    assert any(".tool." in item for item in expected)
    assert {"skill.mission-control-coordinator", "prompt.coordinator.propose-workflow"} <= expected


@pytest.mark.asyncio
async def test_lexical_recall_at_3_on_the_seeded_catalog() -> None:
    definitions, search, _ = await build_catalog()
    service = CapabilitySearchService(search=search, definitions=definitions)
    recall, misses = await recall_at_3(service)
    assert recall >= 0.9, misses
    response = await service.search(request_for({"query": "pubmed literature retrieval"}))
    assert response.search_mode == "lexical"


@pytest.mark.asyncio
async def test_hybrid_recall_at_3_with_recorded_embeddings() -> None:
    embeddings = DeterministicEmbeddings()
    definitions, search, _ = await build_catalog(embeddings)
    service = CapabilitySearchService(
        search=search,
        definitions=definitions,
        embeddings=embeddings,
        embedding_model_id=MODEL_ID,
        embedding_dimensions=DIMENSIONS,
    )
    recall, misses = await recall_at_3(service)
    assert recall >= 0.9, misses
    response = await service.search(request_for({"query": "pubmed literature retrieval"}))
    assert response.search_mode == "hybrid"
    assert response.hits[0].rank_provenance is not None
