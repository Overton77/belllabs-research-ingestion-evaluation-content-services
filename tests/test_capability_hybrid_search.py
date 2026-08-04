from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest

from app.application.capability_search import (
    CapabilitySearchService,
    weighted_rrf,
)
from app.application.capability_search_repository import (
    CapabilityEmbedding,
    InMemoryCatalogSearchRepository,
    RankedCapabilityDocument,
)
from app.application.catalog_projection import CatalogProjectionError, CatalogProjector
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.application.postgres_capability_search_repository import _filtered_query
from app.domain.control_plane.contracts import (
    CatalogPayloadRef,
    DefinitionKind,
    ExactDefinitionRef,
    MCPNetworkRequirement,
    MCPServerDefinition,
    MCPToolDefinition,
    PublishedDefinition,
    SourceProvenance,
)
from app.domain.coordinator.contracts import (
    AuthorizationState,
    CapabilitySearchRequest,
    CatalogAssetStatus,
    SearchDocumentMetadata,
)

NOW = datetime(2026, 7, 25, 17, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64


def test_generation_join_qualifies_search_filter_columns() -> None:
    query, _ = _filtered_query(
        CapabilitySearchRequest(
            query="two-provider web research",
            tenant_scope="tenant-1",
        ),
        score_sql="ts_rank_cd(fts, websearch_to_tsquery('english', $1), 32)",
        match_sql="fts @@ websearch_to_tsquery('english', $1)",
        order_sql="branch_score DESC, logical_id, revision",
        tail_args=("two-provider web research", 10),
    )

    assert "documents.tenant_scope IN ('global', $2)" in query
    assert "documents.asset_kind = ANY($3::text[])" in query
    assert "documents.status = ANY($4::text[])" in query
    assert "documents.capability_requirements @> $5::text[]" in query
    assert "$6 = ANY(documents.compatible_runtimes)" in query
    assert "$7 = ANY(documents.operation_classes)" in query
    assert "documents.workflow_type_refs @>" in query


class FakeEmbeddings:
    def __init__(self, *, wrong_digest: bool = False) -> None:
        self.calls: list[str] = []
        self.wrong_digest = wrong_digest

    async def embed(self, text: str) -> CapabilityEmbedding:
        self.calls.append(text)
        normalized = text.casefold()
        if text == "up-to-date internet investigation":
            vector = (1.0, 0.0, 0.0)
        elif "firecrawl_search" in normalized or "firecrawl search" in normalized:
            vector = (1.0, 0.0, 0.0)
        elif "tavily_search" in normalized or "tavily search" in normalized:
            vector = (0.8, 0.2, 0.0)
        else:
            vector = (0.0, 1.0, 0.0)
        digest = f"sha256:{sha256(text.encode()).hexdigest()}"
        if self.wrong_digest:
            digest = "sha256:" + "f" * 64
        return CapabilityEmbedding(
            vector=vector,
            model_id="text-embedding-3-small",
            dimensions=3,
            input_digest=digest,
        )


def server_definition() -> MCPServerDefinition:
    return MCPServerDefinition(
        logical_id="mcp.web-search",
        title="Two-provider web search MCP server",
        description="Provides independent Firecrawl and Tavily search tools.",
        transport="streamable_http",
        endpoint="https://mcp.example.test/mcp",
        allowed_tools=frozenset({"firecrawl_search", "tavily_search"}),
        approval_policy={
            "firecrawl_search": "never",
            "tavily_search": "never",
        },
        network_requirements=(
            MCPNetworkRequirement(
                host="mcp.example.test",
                port=443,
                protocol="https",
            ),
        ),
        schema_snapshot_ref=CatalogPayloadRef(
            uri="s3://catalog/mcp/web-search/schema.json",
            digest=DIGEST,
            media_type="application/json",
            size_bytes=100,
        ),
        schema_digest=DIGEST,
        source_provenance=SourceProvenance(
            source="local",
            locator="https://mcp.example.test/mcp",
            upstream_identity="fixture/web-search",
            upstream_version="1.0.0",
        ),
        review_status="approved",
    )


def tool_definition(
    *,
    logical_id: str,
    title: str,
    description: str,
    tool_name: str,
    server_ref: ExactDefinitionRef,
) -> MCPToolDefinition:
    return MCPToolDefinition(
        logical_id=logical_id,
        title=title,
        description=description,
        server_ref=server_ref,
        tool_name=tool_name,
        input_schema={"type": "object", "properties": {"query": {"type": "string"}}},
        output_schema={"type": "object"},
        schema_digest=DIGEST,
        side_effect_class="read_only",
    )


async def publish_catalog(
    definitions: InMemoryDefinitionRepository,
) -> tuple[PublishedDefinition, PublishedDefinition, PublishedDefinition]:
    server = await definitions.publish(
        server_definition(),
        "publisher",
        NOW,
        0,
    )
    firecrawl = await definitions.publish(
        tool_definition(
            logical_id="mcp.web-search.firecrawl_search",
            title="Firecrawl Search",
            description="Search current web sources with Firecrawl.",
            tool_name="firecrawl_search",
            server_ref=server.ref,
        ),
        "publisher",
        NOW,
        0,
    )
    tavily = await definitions.publish(
        tool_definition(
            logical_id="mcp.web-search.tavily_search",
            title="Tavily Search",
            description="Search current web sources with Tavily.",
            tool_name="tavily_search",
            server_ref=server.ref,
        ),
        "publisher",
        NOW,
        0,
    )
    return server, firecrawl, tavily


@pytest.mark.asyncio
async def test_projector_is_idempotent_and_checks_embedding_claims() -> None:
    definitions = InMemoryDefinitionRepository()
    _server, firecrawl, _tavily = await publish_catalog(definitions)
    search = InMemoryCatalogSearchRepository()
    embeddings = FakeEmbeddings()
    projector = CatalogProjector(
        definitions=definitions,
        search=search,
        embeddings=embeddings,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        projection_generation="test-generation-1",
        clock=lambda: NOW,
    )

    first = await projector.project(
        firecrawl.ref,
        metadata=SearchDocumentMetadata(tags=frozenset({"web", "search"})),
        capability_requirements=frozenset({"web.search.firecrawl"}),
    )
    second = await projector.project(
        firecrawl.ref,
        metadata=SearchDocumentMetadata(tags=frozenset({"web", "search"})),
        capability_requirements=frozenset({"web.search.firecrawl"}),
    )

    assert first.changed is True
    assert second.changed is False
    assert first.document == second.document
    assert len(embeddings.calls) == 1

    bad_projector = CatalogProjector(
        definitions=definitions,
        search=InMemoryCatalogSearchRepository(),
        embeddings=FakeEmbeddings(wrong_digest=True),
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        projection_generation="test-generation-1",
    )
    with pytest.raises(CatalogProjectionError, match="metadata"):
        await bad_projector.project(firecrawl.ref)


@pytest.mark.asyncio
async def test_hybrid_search_uses_weighted_rrf_and_groups_exact_tool_parent() -> None:
    definitions = InMemoryDefinitionRepository()
    server, firecrawl, tavily = await publish_catalog(definitions)
    search = InMemoryCatalogSearchRepository()
    embeddings = FakeEmbeddings()
    projector = CatalogProjector(
        definitions=definitions,
        search=search,
        embeddings=embeddings,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        projection_generation="test-generation-1",
        clock=lambda: NOW,
    )
    for published, capability in (
        (firecrawl, "web.search.firecrawl"),
        (tavily, "web.search.tavily"),
    ):
        await projector.project(
            published.ref,
            capability_requirements=frozenset({"web.search", capability}),
        )

    service = CapabilitySearchService(
        search=search,
        definitions=definitions,
        embeddings=embeddings,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
    )
    response = await service.search(
        CapabilitySearchRequest(
            query="up-to-date internet investigation",
            kinds=frozenset({DefinitionKind.MCP_TOOL}),
            tenant_scope="tenant-1",
            required_capabilities=frozenset({"web.search"}),
            limit=10,
        )
    )

    assert {hit.exact_ref for hit in response.hits} == {firecrawl.ref, tavily.ref}
    assert all(hit.authorization_state == AuthorizationState.SELECTABLE for hit in response.hits)
    assert all(hit.lexical_rank is None for hit in response.hits)
    assert all(hit.semantic_rank is not None for hit in response.hits)
    assert len(response.tool_groups) == 1
    assert response.tool_groups[0].parent_ref == server.ref
    assert response.tool_groups[0].tools == response.hits


@pytest.mark.asyncio
async def test_exact_rehydration_rejects_stale_digest_and_retirement() -> None:
    definitions = InMemoryDefinitionRepository()
    _server, firecrawl, _tavily = await publish_catalog(definitions)
    search = InMemoryCatalogSearchRepository()
    embeddings = FakeEmbeddings()
    projector = CatalogProjector(
        definitions=definitions,
        search=search,
        embeddings=embeddings,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        projection_generation="test-generation-1",
        clock=lambda: NOW,
    )
    projected = await projector.project(firecrawl.ref)
    stale = projected.document.model_copy(
        update={"source_digest": "sha256:" + "0" * 64}
    )
    await search.upsert(stale)
    service = CapabilitySearchService(
        search=search,
        definitions=definitions,
        embeddings=embeddings,
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
    )
    request = CapabilitySearchRequest(
        query="firecrawl search",
        tenant_scope="tenant-1",
        limit=5,
    )
    assert (await service.search(request)).hits == ()

    await projector.project(firecrawl.ref)
    await definitions.retire(firecrawl.ref, "publisher", NOW)
    assert (await service.search(request)).hits == ()
    retired_projection = await projector.project(firecrawl.ref)
    assert retired_projection.document.status == CatalogAssetStatus.RETIRED
    retired = await service.search(
        request.model_copy(
            update={"status_filter": frozenset({CatalogAssetStatus.RETIRED})}
        )
    )
    assert retired.hits[0].authorization_state == AuthorizationState.UNAVAILABLE


@pytest.mark.asyncio
async def test_rrf_uses_rank_not_branch_score_with_k_50() -> None:
    definitions = InMemoryDefinitionRepository()
    _server, firecrawl, tavily = await publish_catalog(definitions)
    search = InMemoryCatalogSearchRepository()
    projector = CatalogProjector(
        definitions=definitions,
        search=search,
        embeddings=FakeEmbeddings(),
        embedding_model_id="text-embedding-3-small",
        embedding_dimensions=3,
        projection_generation="test-generation-1",
        clock=lambda: NOW,
    )
    firecrawl_doc = (await projector.project(firecrawl.ref)).document
    tavily_doc = (await projector.project(tavily.ref)).document
    fused = weighted_rrf(
        (
            RankedCapabilityDocument(document=firecrawl_doc, branch_score=1_000),
            RankedCapabilityDocument(document=tavily_doc, branch_score=10),
        ),
        (
            RankedCapabilityDocument(document=tavily_doc, branch_score=0.9),
            RankedCapabilityDocument(document=firecrawl_doc, branch_score=0.8),
        ),
        k=50,
    )

    assert fused[0].document.logical_id == firecrawl.ref.logical_id
    assert fused[0].fused_score == pytest.approx(1 / 51 + 1 / 52)
    assert fused[1].fused_score == pytest.approx(1 / 52 + 1 / 51)
    assert {item.lexical_rank for item in fused} == {1, 2}
    assert {item.semantic_rank for item in fused} == {1, 2}
