"""Capability search projection in ``mission_control_search`` on the common component.

Vectors here are LOCAL DETERMINISTIC TEST VECTORS that prove storage, activation and
ranking mechanics only. They are not embeddings of the text, come from no model, and are
no evidence that semantic search is available: that requires a real configured model
generation whose model/dimension/source-set checks pass.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

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
from mission_control.application.capabilities.capability_search_repository import (
    CapabilitySearchDocument,
)
from mission_control.application.capabilities.catalog_projection_generation import (
    ProjectionGenerationSpec,
    projection_source_set_digest,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    CapabilityDefinition,
    DefinitionKind,
    PublishedDefinition,
)
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest, CatalogAssetStatus
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.catalog_common import AI_ENGINEER_CATALOG, BIOTECH_CATALOG
from tests.integration.postgres.catalog_common import catalog_db as catalog_db
from tests.integration.postgres.catalog_common import catalog_writer_pool as catalog_writer_pool
from tests.integration.postgres.catalog_common import runtime_pool as runtime_pool

pytestmark = pytest.mark.common_db

NOW = datetime(2026, 10, 3, tzinfo=UTC)
DIMENSIONS = 1536
MECHANICS_MODEL = "test-only:deterministic-mechanics-vector"


def mechanics_vector(axis: int) -> tuple[float, ...]:
    """Deterministic unit basis vector: storage/ranking mechanics only, not an embedding."""
    return tuple(1.0 if index == axis else 0.0 for index in range(DIMENSIONS))


def tool(logical_id: str, description: str) -> CapabilityDefinition:
    return CapabilityDefinition(
        logical_id=logical_id,
        title=logical_id,
        description=description,
        kind="tool",
        capability_kind="tool",
        maturity="qualified",
        attachment_targets=frozenset({"agent.main"}),
    )


def document(
    published: PublishedDefinition, scope: str, generation: str, axis: int
) -> CapabilitySearchDocument:
    text = published.definition.description
    return CapabilitySearchDocument(
        search_document_id=uuid4(),
        tenant_scope=scope,
        asset_kind=published.ref.kind,
        logical_id=published.ref.logical_id,
        revision=published.ref.revision,
        source_digest=published.ref.digest,
        status=CatalogAssetStatus.PUBLISHED,
        title=published.ref.logical_id,
        description=text,
        search_text=text,
        search_text_digest=sha256_digest(text),
        embedding=mechanics_vector(axis),
        embedding_model_id=MECHANICS_MODEL,
        embedding_dimensions=DIMENSIONS,
        search_document_format_version=1,
        mongodb_document_id=f"{published.ref.logical_id}@{published.ref.revision}",
        source_published_at=published.published_at,
        indexed_at=NOW,
        projection_generation=generation,
    )


def spec(
    scope: str, generation: str, published: tuple[PublishedDefinition, ...]
) -> ProjectionGenerationSpec:
    return ProjectionGenerationSpec(
        tenant_scope=scope,
        projection_generation=generation,
        embedding_model_id=MECHANICS_MODEL,
        embedding_dimensions=DIMENSIONS,
        search_document_format_version=1,
        selected_kinds=frozenset({DefinitionKind.TOOL}),
        expected_count=len(published),
        expected_source_set_digest=projection_source_set_digest(
            tuple(item.ref for item in published)
        ),
    )


@pytest.mark.asyncio
async def test_projection_activation_search_admission_and_installation_scope(
    catalog_db: CommonDatabase, runtime_pool: asyncpg.Pool, catalog_writer_pool: asyncpg.Pool
) -> None:
    catalog = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    crawl = await catalog.publish(tool("tool.web-crawl", "crawl public web pages"), "p", NOW, 0)
    search = await catalog.publish(tool("tool.web-search", "search public web pages"), "p", NOW, 0)
    scope = "global"
    generation = "generation-1"
    # Projection writes run as the catalog writer; reads as the runtime role.
    writer_generations = PostgresProjectionGenerationRepository(
        catalog_writer_pool, catalog_scope=BIOTECH_CATALOG
    )
    writer = PostgresCatalogSearchRepository(catalog_writer_pool, catalog_scope=BIOTECH_CATALOG)
    expected = spec(scope, generation, (crawl, search))
    record = await writer_generations.begin(expected, created_at=NOW)
    assert record.state == "building"
    assert await writer_generations.begin(expected, created_at=NOW) == record
    docs = (document(crawl, scope, generation, 0), document(search, scope, generation, 1))
    assert all([await writer.upsert(item) for item in docs])
    assert not await writer.upsert(docs[0])  # identical replay is a no-op
    activation = await writer_generations.activate(expected, activated_at=NOW)
    assert activation.activated_count == 2
    assert activation.activated_source_set_digest == expected.expected_source_set_digest
    generations = PostgresProjectionGenerationRepository(
        runtime_pool, catalog_scope=BIOTECH_CATALOG
    )
    assert await generations.active_for_kind(scope, DefinitionKind.TOOL) == generation

    reader = PostgresCatalogSearchRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    request = CapabilitySearchRequest(
        query="public web", kinds=frozenset({DefinitionKind.TOOL}), tenant_scope=scope
    )
    lexical = await reader.lexical_search(request, limit=10)
    assert {item.document.logical_id for item in lexical} == {"tool.web-crawl", "tool.web-search"}
    semantic = await reader.semantic_search(request, mechanics_vector(1), limit=1)
    assert semantic[0].document.logical_id == "tool.web-search"  # mechanics: nearest basis axis

    # Search authorization respects admission: retired assets drop out unless requested.
    await catalog.retire(crawl.ref, "operator", NOW)
    lexical = await reader.lexical_search(request, limit=10)
    assert {item.document.logical_id for item in lexical} == {"tool.web-search"}
    # Revoked assets never reappear, even when the projection still holds the row.
    async with catalog_writer_pool.acquire() as connection, connection.transaction():
        await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
        await connection.execute(
            "UPDATE mission_control.asset_version SET status='revoked', version_no=version_no+1, "
            "updated_at=now() WHERE asset_id='definition:tool:tool.web-search'"
        )
    assert await reader.lexical_search(request, limit=10) == ()
    every_status = request.model_copy(update={"status_filter": frozenset(CatalogAssetStatus)})
    assert {
        item.document.logical_id for item in await reader.lexical_search(every_status, limit=10)
    } == {"tool.web-crawl"}

    # Installation scoping: the other app's catalog sees no generations or documents.
    foreign = PostgresCatalogSearchRepository(runtime_pool, catalog_scope=AI_ENGINEER_CATALOG)
    assert await foreign.lexical_search(every_status, limit=10) == ()
    assert (
        await PostgresProjectionGenerationRepository(
            runtime_pool, catalog_scope=AI_ENGINEER_CATALOG
        ).get(scope, generation)
        is None
    )
    # A request scope from another installation cannot be paired with this catalog.
    with pytest.raises(ValueError, match="crosses"):
        await reader.lexical_search(
            request.model_copy(update={"tenant_scope": AI_ENGINEER_CATALOG}), limit=1
        )
    # 'global' alone names no installation: fail closed instead of searching everything.
    with pytest.raises(ValueError, match="installation-bound"):
        await PostgresCatalogSearchRepository(runtime_pool).lexical_search(request, limit=1)
    # The tenant request scope of this installation resolves the installation itself.
    scoped = request.model_copy(
        update={
            "tenant_scope": catalog_db.scope("tenant-1"),
            "status_filter": every_status.status_filter,
        }
    )
    hits = await PostgresCatalogSearchRepository(runtime_pool).lexical_search(scoped, limit=10)
    assert {item.document.logical_id for item in hits} == {"tool.web-crawl"}  # 'global' rows


@pytest.mark.asyncio
async def test_activation_verifies_count_digest_and_embedding_contract(
    catalog_db: CommonDatabase, catalog_writer_pool: asyncpg.Pool, runtime_pool: asyncpg.Pool
) -> None:
    catalog = PostgresDefinitionRepository(runtime_pool, catalog_scope=BIOTECH_CATALOG)
    published = await catalog.publish(tool("tool.one", "one tool"), "p", NOW, 0)
    generations = PostgresProjectionGenerationRepository(
        catalog_writer_pool, catalog_scope=BIOTECH_CATALOG
    )
    writer = PostgresCatalogSearchRepository(catalog_writer_pool, catalog_scope=BIOTECH_CATALOG)
    lying = spec("global", "generation-lying", (published, published))
    await generations.begin(lying, created_at=NOW)
    await writer.upsert(document(published, "global", "generation-lying", 0))
    with pytest.raises(asyncpg.RaiseError, match="verification failed"):
        await generations.activate(lying, activated_at=NOW)
    await generations.mark_failed("global", "generation-lying")
    failed = await generations.get("global", "generation-lying")
    assert failed is not None and failed.state == "failed"
    with pytest.raises(RuntimeError, match="not writable"):
        await writer.upsert(document(published, "global", "generation-lying", 0))
    # Dimension is pinned to the legacy 1536 column type; other sizes are rejected.
    wrong = spec("global", "generation-small", (published,)).model_copy(
        update={"embedding_dimensions": 8}
    )
    await generations.begin(wrong, created_at=NOW)
    small = document(published, "global", "generation-small", 0).model_copy(
        update={"embedding": (1.0,) * 8, "embedding_dimensions": 8}
    )
    with pytest.raises(asyncpg.PostgresError):
        await writer.upsert(small)
    # The readonly/inspection role reads but cannot write the projection.
    readonly = await catalog_db.pool("mission_control_readonly")
    try:
        async with readonly.acquire() as connection, connection.transaction():
            await apply_catalog_scope_string(connection, BIOTECH_CATALOG)
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM mission_control_search.projection_generation"
                )
                == 2
            )
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(
                    "UPDATE mission_control_search.projection_generation SET state='failed'"
                )
    finally:
        await readonly.close()
