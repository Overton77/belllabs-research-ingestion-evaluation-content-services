"""Installation-owned catalog composition; requests cannot select credentials or scope."""

from __future__ import annotations

import asyncpg

from mission_control.adapters.postgres.capability.capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.application.agentic_components.repository import AgenticComponentRepository
from mission_control.application.capabilities.capability_search import CapabilitySearchService
from mission_control.application.capabilities.capability_search_repository import (
    CapabilityEmbeddingPort,
)
from mission_control.application.capabilities.catalog import CatalogService
from mission_control.application.capabilities.external_candidate_inspection import (
    ExternalCandidateInspectionService,
)
from mission_control.application.capabilities.external_capability_discovery import (
    ExternalCapabilityDiscoveryService,
)


def compose_catalog_service(
    pool: asyncpg.Pool,
    *,
    request_scope: str,
    catalog_scope: str,
    embeddings: CapabilityEmbeddingPort | None = None,
    embedding_model_id: str = "text-embedding-3-small",
    embedding_dimensions: int = 1536,
    discovery: ExternalCapabilityDiscoveryService | None = None,
    inspection: ExternalCandidateInspectionService | None = None,
    components: AgenticComponentRepository | None = None,
) -> CatalogService:
    definitions = PostgresDefinitionRepository(pool, catalog_scope=catalog_scope)
    search = (
        None
        if embeddings is None
        else CapabilitySearchService(
            search=PostgresCatalogSearchRepository(pool, catalog_scope=catalog_scope),
            definitions=definitions,
            embeddings=embeddings,
            embedding_model_id=embedding_model_id,
            embedding_dimensions=embedding_dimensions,
        )
    )
    return CatalogService(
        request_scope, catalog_scope, definitions, search, discovery, inspection, components
    )
