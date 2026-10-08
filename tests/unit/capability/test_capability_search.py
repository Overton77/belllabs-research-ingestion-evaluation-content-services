"""FT-A3: hybrid search with lexical fallback, filters, provenance and the public API."""

from __future__ import annotations

from typing import Any

import pytest

from mission_control.application.capabilities.capability_search import (
    DEFAULT_TRIGRAM_WEIGHT,
    MAX_CANDIDATES_PER_LIST,
    RRF_K,
    CapabilitySearchService,
    weighted_rrf,
)
from mission_control.application.capabilities.capability_search_repository import (
    word_similarity,
)
from mission_control.bootstrap.catalog import compose_catalog_service, configured_catalog_embeddings
from mission_control.bootstrap.settings import Settings
from mission_control.domain.authoring.contracts import DefinitionKind
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest
from tests.fixtures.capability_search_catalog import (
    DIMENSIONS,
    MODEL_ID,
    DeterministicEmbeddings,
    build_catalog,
)
from tests.fixtures.catalog_http import catalog_harness

SCOPE = "mc/00000000-0000-4000-8000-000000000001/biotech/00000000-0000-4000-8000-000000000002"


def _request(query: str, **values: Any) -> CapabilitySearchRequest:
    return CapabilitySearchRequest(query=query, tenant_scope=SCOPE, **values)


class FailingEmbeddings(DeterministicEmbeddings):
    async def embed_many(self, texts: tuple[str, ...]) -> Any:
        raise RuntimeError("provider unavailable")


def test_fusion_defaults_follow_adr_0025() -> None:
    assert RRF_K == 60
    assert DEFAULT_TRIGRAM_WEIGHT == 0.5
    assert MAX_CANDIDATES_PER_LIST == 60
    with pytest.raises(ValueError):
        weighted_rrf((), (), k=0)


def test_word_similarity_matches_exact_tool_names() -> None:
    assert word_similarity("tavily_search", "mcp.tavily.tool.tavily-search tavily_search") == 1.0
    assert word_similarity("edgar company", "mcp.edgartools.tool.edgar-company edgar_company") > 0.9
    assert word_similarity("weather forecast", "mcp.pubmed pubmed literature") < 0.3


@pytest.mark.asyncio
async def test_lexical_mode_without_route_and_trigram_provenance() -> None:
    definitions, search, _ = await build_catalog()
    service = CapabilitySearchService(search=search, definitions=definitions)
    assert not service.embeddings_configured
    response = await service.search(_request("tavily_search", limit=3))
    assert response.search_mode == "lexical"
    top = response.hits[0]
    assert top.exact_ref is not None and top.exact_ref.logical_id.startswith("mcp.tavily")
    assert top.rank_provenance is not None
    assert top.rank_provenance.trigram_rank is not None
    assert top.rank_provenance.vector_rank is None
    assert top.pin is not None and top.pin.startswith(top.exact_ref.logical_id + "@")


@pytest.mark.asyncio
async def test_failed_query_embedding_degrades_to_lexical() -> None:
    definitions, search, _ = await build_catalog()
    service = CapabilitySearchService(
        search=search,
        definitions=definitions,
        embeddings=FailingEmbeddings(),
        embedding_model_id=MODEL_ID,
        embedding_dimensions=DIMENSIONS,
    )
    response = await service.search(_request("pubmed literature", limit=3))
    assert response.search_mode == "lexical"
    assert response.hits


@pytest.mark.asyncio
async def test_filters_apply_before_ranking() -> None:
    definitions, search, _ = await build_catalog()
    service = CapabilitySearchService(search=search, definitions=definitions)
    cloud = await service.search(
        _request(
            "pubmed literature biomedical",
            kinds=frozenset({DefinitionKind.MCP_SERVER}),
            host_profiles=frozenset({"cursor_cloud"}),
            limit=10,
        )
    )
    ids = {hit.exact_ref.logical_id for hit in cloud.hits if hit.exact_ref}
    assert "mcp.pubmed" not in ids and "mcp.biomcp" not in ids
    assert all("cursor_cloud" in hit.supported_profiles for hit in cloud.hits)
    read_only = await service.search(
        _request("browser", side_effect_classes=frozenset({"read_only"}), limit=20)
    )
    assert all(
        hit.exact_ref is not None
        and "agent-browser-click" not in hit.exact_ref.logical_id
        and hit.exact_ref.logical_id.rsplit(".", 1)[-1] != "firecrawl-interact"
        for hit in read_only.hits
    )
    tools = await service.search(_request("search", kinds=frozenset({DefinitionKind.MCP_TOOL})))
    assert all(hit.kind is DefinitionKind.MCP_TOOL for hit in tools.hits)


@pytest.mark.asyncio
async def test_deferred_projection_then_embed_pending_and_reembed_on_model_change() -> None:
    definitions, search, projector = await build_catalog()
    documents = await search.list_generation("global", projector.projection_generation)
    assert documents and all(document.embedding is None for document in documents)
    embeddings = DeterministicEmbeddings()
    from mission_control.application.capabilities.catalog_projection import CatalogProjector

    embedder = CatalogProjector(
        definitions=definitions,
        search=search,
        embeddings=embeddings,
        embedding_model_id=MODEL_ID,
        embedding_dimensions=DIMENSIONS,
        projection_generation=projector.projection_generation,
    )
    assert await embedder.embed_pending(batch_size=40) == len(documents)
    assert len(embeddings.calls) == -(-len(documents) // 40)
    assert await embedder.embed_pending() == 0
    stale = (await search.list_generation("global", projector.projection_generation))[0]
    await search.upsert(stale.model_copy(update={"embedding_model": "other-model"}))
    assert await embedder.embed_pending() == 1


def test_embedding_route_only_when_profile_and_credential_are_configured() -> None:
    assert configured_catalog_embeddings(Settings(_env_file=None)) is None
    no_key = Settings(
        _env_file=None,
        capability_embedding_profile="embedding.openai.text-embedding-3-small",
        openai_api_key=None,
    )
    assert configured_catalog_embeddings(no_key) is None
    configured = Settings(
        _env_file=None,
        capability_embedding_profile="embedding.openai.text-embedding-3-small",
        openai_api_key="sk-test-not-used",
    )
    assert configured_catalog_embeddings(configured) is not None  # no request is made


def test_compose_catalog_service_always_composes_search() -> None:
    service = compose_catalog_service(
        object(),  # type: ignore[arg-type]  # the pool is only stored, never used here
        request_scope=SCOPE,
        catalog_scope="mc/00000000-0000-4000-8000-000000000001/biotech/catalog",
    )
    assert service.search is not None
    assert not service.search.embeddings_configured


@pytest.mark.asyncio
async def test_public_api_returns_lexical_results_never_503() -> None:
    definitions, search, _ = await build_catalog()
    harness = catalog_harness(
        definitions, CapabilitySearchService(search=search, definitions=definitions)
    )
    body = {
        "query": "pubmed literature retrieval",
        "tenant_scope": harness.tenant_scope,
        "kinds": ["mcp_server"],
        "host_profiles": ["deep_agents"],
        "limit": 3,
    }
    response = harness.client.post(harness.url("search"), json=body)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["search_mode"] == "lexical"
    top = payload["hits"][0]
    assert top["exact_ref"]["logical_id"] == "mcp.pubmed"
    assert top["pin"].startswith("mcp.pubmed@2.10.20#sha256:")
    assert top["host_support"]["profiles"]["cursor_cloud"]["status"] == "unqualified"
    assert set(top["rank_provenance"]) == {
        "lexical_rank",
        "trigram_rank",
        "vector_rank",
        "fused_score",
    }


@pytest.mark.asyncio
async def test_http_and_service_parity_for_the_same_request() -> None:
    definitions, search, _ = await build_catalog()
    service = CapabilitySearchService(search=search, definitions=definitions)
    harness = catalog_harness(definitions, service)
    for query in ("web search", "sec filings", "browser automation", "pubmed", "MeSH"):
        request = CapabilitySearchRequest(query=query, tenant_scope=harness.tenant_scope, limit=5)
        direct = await service.search(request)
        over_http = harness.client.post(
            harness.url("search"), json=request.model_dump(mode="json")
        ).json()
        assert [hit["pin"] for hit in over_http["hits"]] == [hit.pin for hit in direct.hits]
