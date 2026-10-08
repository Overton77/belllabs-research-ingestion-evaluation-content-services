"""Installation-owned catalog composition; requests cannot select credentials or scope.

FT-A3 (ADR-0025): capability search is always composed. With an embedding route it runs
hybrid; without one it runs lexical-only, so the public API never answers 503 for a
missing route.
"""

from __future__ import annotations

import logging
from typing import cast

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
from mission_control.bootstrap.settings import Settings

EMBEDDING_PROFILE = "embedding.openai.text-embedding-3-small"
_LOG = logging.getLogger(__name__)


def configured_catalog_embeddings(settings: Settings) -> CapabilityEmbeddingPort | None:
    """The OpenAI embedding route, only when configuration names its Model Profile.

    Requires ``capability_embedding_profile`` to name ``embedding.openai.text-embedding-3-small``
    and the credential reference to resolve; otherwise search stays lexical.
    """
    if settings.capability_embedding_profile != EMBEDDING_PROFILE:
        return None
    from mission_control.adapters.capabilities.capability_embeddings import (
        CapabilityEmbeddingDependencyError,
        OpenAICapabilityEmbeddingAdapter,
    )

    try:
        # The tracing decorator hides embed_many's signature from protocol checkers.
        return cast(CapabilityEmbeddingPort, OpenAICapabilityEmbeddingAdapter(settings))
    except CapabilityEmbeddingDependencyError:
        _LOG.warning("catalog embedding profile is configured but its credential is not")
        return None


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
    search = CapabilitySearchService(
        search=PostgresCatalogSearchRepository(pool, catalog_scope=catalog_scope),
        definitions=definitions,
        embeddings=embeddings,
        embedding_model_id=embedding_model_id if embeddings is not None else None,
        embedding_dimensions=embedding_dimensions if embeddings is not None else None,
    )
    return CatalogService(
        request_scope, catalog_scope, definitions, search, discovery, inspection, components
    )
