"""FT-A3 on a disposable common database: migration 0026, lexical fallback and hybrid recall.

Vectors come from tests.fixtures.capability_search_catalog.DeterministicEmbeddings, an
offline hashed bag-of-words embedder (no provider is called). They prove storage, nullable
embedding handling, fusion and recall mechanics, not the quality of a production model.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

import asyncpg
import pytest

from mission_control.adapters.postgres.capability.capability_search_generation_repository import (
    PostgresProjectionGenerationRepository,
)
from mission_control.adapters.postgres.capability.capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope_string
from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.application.capabilities.catalog_projection import (
    CatalogProjectionInput,
    CatalogProjector,
)
from mission_control.application.capabilities.catalog_projection_generation import (
    ProjectionGenerationSpec,
    projection_source_set_digest,
)
from mission_control.domain.authoring.contracts import DefinitionKind
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest
from mission_control_db_contract.seeds import _apply_record, load_bundles, order_bundles
from tests.fixtures.capability_search_catalog import (
    DIMENSIONS,
    MODEL_ID,
    SEEDS,
    DeterministicEmbeddings,
    evaluation_cases,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.catalog_common import BIOTECH_CATALOG
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import catalog_writer_pool as catalog_writer_pool
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool

pytestmark = pytest.mark.common_db

NOW = datetime(2026, 10, 7, tzinfo=UTC)
GENERATION = "ft-a3-generation-1"


async def _seed(database: CommonDatabase) -> None:
    target = {
        "installation_id": str(database.installation_id),
        "application_id": database.application_id,
    }
    bundles = order_bundles(
        load_bundles([SEEDS / "common", SEEDS / "biotech", SEEDS / "qualification"]), set()
    )
    bundles = [
        bundle
        for bundle in bundles
        if not any(record["kind"].startswith("storage_") for record in bundle["records"])
    ]
    connection = await asyncpg.connect(database.owner_dsn)
    try:
        for bundle in bundles:
            stats = {"created": 0, "reused": 0, "revoked": 0}
            async with connection.transaction():
                for record in bundle["records"]:
                    await _apply_record(connection, target, bundle, record, stats)
    finally:
        await connection.close()


async def _evaluate(service: CapabilitySearchService, scope: str) -> tuple[float, list[str]]:
    hits, misses = 0, []
    cases = [case for case in evaluation_cases() if "mcp.edgartools" not in str(case)]
    for case in cases:
        request = CapabilitySearchRequest(
            query=case["query"],
            tenant_scope=scope,
            kinds=frozenset(DefinitionKind(kind) for kind in case.get("kinds", ())),
            host_profiles=frozenset(case.get("host_profiles", ())),
            side_effect_classes=frozenset(case.get("side_effect_classes", ())),
            limit=3,
        )
        response = await service.search(request)
        ids = [hit.exact_ref.logical_id for hit in response.hits if hit.exact_ref]
        if set(case["expected"]) & set(ids):
            hits += 1
        else:
            misses.append(f"{case['query']!r}: {ids}")
    return hits / len(cases), misses


@pytest.mark.asyncio
async def test_lexical_projection_then_async_embedding_reaches_recall(
    catalog_db: CommonDatabase, runtime_pool: asyncpg.Pool, catalog_writer_pool: asyncpg.Pool
) -> None:
    await _seed(catalog_db)
    reader_definitions = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    refs = await reader_definitions.list_published_definition_refs()
    assert len(refs) > 50
    scope = "global"
    spec = ProjectionGenerationSpec(
        tenant_scope=scope,
        projection_generation=GENERATION,
        embedding_model_id=MODEL_ID,
        embedding_dimensions=DIMENSIONS,
        search_document_format_version=1,
        selected_kinds=frozenset(ref.kind for ref in refs),
        expected_count=len(refs),
        expected_source_set_digest=projection_source_set_digest(refs),
    )
    generations = PostgresProjectionGenerationRepository(
        catalog_writer_pool, catalog_scope=BIOTECH_CATALOG
    )
    await generations.begin(spec, created_at=NOW)
    writer = PostgresCatalogSearchRepository(catalog_writer_pool, catalog_scope=BIOTECH_CATALOG)
    lexical_projector = CatalogProjector(
        definitions=PostgresDefinitionRepository(
            catalog_writer_pool, catalog_scope=BIOTECH_CATALOG
        ),
        search=writer,
        embeddings=None,
        embedding_model_id=MODEL_ID,
        embedding_dimensions=DIMENSIONS,
        projection_generation=GENERATION,
        clock=lambda: NOW,
    )
    await lexical_projector.project_many(tuple(CatalogProjectionInput(ref=ref) for ref in refs))
    activation = await generations.activate(spec, activated_at=NOW)
    assert activation.activated_count == len(refs)

    async with runtime_pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        nulls = await connection.fetchval(
            "SELECT count(*) FROM mission_control_search.search_document WHERE embedding IS NULL"
        )
        assert nulls == len(refs)
        pubmed = await connection.fetchrow(
            "SELECT host_profiles, side_effect_class, tool_names, name_surface "
            "FROM mission_control_search.search_document WHERE logical_id='mcp.pubmed'"
        )
    assert "cursor_cloud" not in pubmed["host_profiles"]
    assert "deep_agents" in pubmed["host_profiles"]
    assert pubmed["side_effect_class"] == "read_only"
    assert "pubmed_lookup_mesh" in pubmed["tool_names"]
    assert "pubmed_lookup_mesh" in pubmed["name_surface"]

    reader = PostgresCatalogSearchRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    lexical = CapabilitySearchService(search=reader, definitions=reader_definitions)
    tenant = catalog_db.scope("tenant-1")
    response = await lexical.search(
        CapabilitySearchRequest(query="tavily_search", tenant_scope=tenant, limit=3)
    )
    assert response.search_mode == "lexical"
    assert response.hits[0].exact_ref is not None
    assert response.hits[0].rank_provenance is not None
    assert response.hits[0].rank_provenance.vector_rank is None
    names = [hit.exact_ref.logical_id for hit in response.hits if hit.exact_ref]
    assert "mcp.tavily.tool.tavily-search" in names
    trigram = await reader.trigram_search(
        CapabilitySearchRequest(query="tavily_search", tenant_scope=tenant), limit=5
    )
    assert trigram and trigram[0].document.logical_id.startswith("mcp.tavily")
    recall, misses = await _evaluate(lexical, tenant)
    assert recall >= 0.9, misses

    embeddings = DeterministicEmbeddings()
    embedder = CatalogProjector(
        definitions=PostgresDefinitionRepository(
            catalog_writer_pool, catalog_scope=BIOTECH_CATALOG
        ),
        search=writer,
        embeddings=embeddings,
        embedding_model_id=MODEL_ID,
        embedding_dimensions=DIMENSIONS,
        projection_generation=GENERATION,
        clock=lambda: NOW,
    )
    assert await embedder.embed_pending(batch_size=50) == len(refs)
    assert await embedder.embed_pending(batch_size=50) == 0  # recorded model already matches
    hybrid = CapabilitySearchService(
        search=reader,
        definitions=reader_definitions,
        embeddings=embeddings,
        embedding_model_id=MODEL_ID,
        embedding_dimensions=DIMENSIONS,
    )
    response = await hybrid.search(
        CapabilitySearchRequest(query="pubmed literature retrieval", tenant_scope=tenant, limit=3)
    )
    assert response.search_mode == "hybrid"
    assert response.hits[0].exact_ref is not None
    assert response.hits[0].exact_ref.logical_id == "mcp.pubmed"
    assert response.hits[0].pin and response.hits[0].pin.startswith("mcp.pubmed@2.10.20#sha256:")
    recall, misses = await _evaluate(hybrid, tenant)
    assert recall >= 0.9, misses

    filtered = await hybrid.search(
        CapabilitySearchRequest(
            query="literature",
            tenant_scope=tenant,
            kinds=frozenset({DefinitionKind.MCP_SERVER}),
            host_profiles=frozenset({"cursor_cloud"}),
            limit=10,
        )
    )
    assert "mcp.pubmed" not in {hit.exact_ref.logical_id for hit in filtered.hits if hit.exact_ref}


@pytest.mark.asyncio
async def test_migration_0026_constraints(catalog_db: CommonDatabase) -> None:
    connection = await asyncpg.connect(catalog_db.owner_dsn)
    try:
        nullable = await connection.fetchval(
            "SELECT is_nullable FROM information_schema.columns WHERE table_schema="
            "'mission_control_search' AND table_name='search_document' AND column_name='embedding'"
        )
        assert nullable == "YES"
        indexes = {
            row["indexname"]: row["indexdef"]
            for row in await connection.fetch(
                "SELECT indexname, indexdef FROM pg_indexes "
                "WHERE schemaname='mission_control_search' AND tablename='search_document'"
            )
        }
        assert "WHERE (embedding IS NOT NULL)" in indexes["search_document_embedding_idx"]
        assert "gin_trgm_ops" in indexes["search_document_name_trgm_idx"]
        assert "host_profiles" in indexes["search_document_host_profiles_idx"]
        checks = {
            row["conname"]
            for row in await connection.fetch(
                "SELECT conname FROM pg_constraint WHERE conrelid="
                "'mission_control_search.search_document'::regclass"
            )
        }
        assert {
            "search_document_embedding_provenance",
            "search_document_host_profiles_known",
        } <= checks
        assert json.loads(json.dumps(sorted(checks)))  # constraint names are plain text
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_pin_resolution_and_inspection_on_postgres(
    catalog_db: CommonDatabase, runtime_pool: asyncpg.Pool
) -> None:
    """FT-A8: pins resolve through PostgresDefinitionRepository.find_by_pin."""
    from mission_control.application.capabilities.catalog import inspect_pin

    await _seed(catalog_db)
    definitions = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    published = await definitions.find_by_pin("mcp.pubmed", "sha256:" + "0" * 64)
    assert published is None
    listed = {item.ref.logical_id: item for item in await definitions.list_published_definitions()}
    from mission_control.domain.capabilities.catalog_entry import capability_pin

    pin = capability_pin(listed["mcp.pubmed"]).render()
    inspection = await inspect_pin(definitions, pin)
    assert inspection.pin == pin
    assert set(inspection.secret_refs) == {"NCBI_API_KEY", "NCBI_ADMIN_EMAIL", "UNPAYWALL_EMAIL"}
    assert inspection.host_support is not None
    assert inspection.host_support.status("cursor_cloud").value == "unqualified"
