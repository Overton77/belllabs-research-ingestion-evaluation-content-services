"""Hybrid capability search (ADR-0013 extended by ADR-0025, SPEC-01 "Hybrid search").

Scope, admission, kind, lane-profile and side-effect filters apply first; then up to three
ranked lists (full-text, trigram over names, and pgvector when an embedding route works)
are fused by weighted reciprocal rank fusion (k=60, weights 1.0 / 0.5 / 1.0). Without an
embedding route, or when the query embedding fails, the search runs lexical-only and says
so (``search_mode``); it never fails for a missing route. Every hit is re-verified against
the authoritative definition digest before it is returned.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from math import ceil
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.authoring.control_plane_repository import DefinitionRepository
from mission_control.application.capabilities.capability_search_repository import (
    CapabilityEmbeddingPort,
    CapabilitySearchDocument,
    CatalogSearchRepository,
    RankedCapabilityDocument,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import ExactDefinitionRef
from mission_control.domain.authoring.errors import (
    DefinitionNotFound,
    ReferenceMismatch,
)
from mission_control.domain.capabilities.catalog_entry import capability_pin
from mission_control.domain.capabilities.host_support import CapabilityHostSupport
from mission_control.domain.coordinator.contracts import (
    CapabilitySearchHit,
    CapabilitySearchRequest,
    CatalogAssetStatus,
    RankProvenance,
    SelectionFacts,
)
from mission_control.domain.coordinator.policy import evaluate_selection

RRF_K = 60
DEFAULT_LEXICAL_WEIGHT = 1.0
DEFAULT_TRIGRAM_WEIGHT = 0.5
DEFAULT_SEMANTIC_WEIGHT = 1.0
MAX_CANDIDATES_PER_LIST = 60
SearchMode = Literal["hybrid", "lexical"]
_LOG = logging.getLogger(__name__)


class SearchResponseContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MCPToolSearchGroup(SearchResponseContract):
    parent_ref: ExactDefinitionRef
    tools: tuple[CapabilitySearchHit, ...]


class TokenUseMeasurement(SearchResponseContract):
    metric_kind: str = Field(min_length=1)
    character_count: int = Field(ge=0)
    estimated_tokens: int = Field(ge=0)
    method: str = "unicode_characters_divided_by_4_ceiling_v1"


class CapabilitySearchResponse(SearchResponseContract):
    hits: tuple[CapabilitySearchHit, ...]
    tool_groups: tuple[MCPToolSearchGroup, ...] = ()
    token_use: tuple[TokenUseMeasurement, ...] = ()
    search_mode: SearchMode = "lexical"


class CatalogVisibilityPolicy(Protocol):
    def visible(self, tenant_scope: str, ref: ExactDefinitionRef) -> bool: ...

    def allowed(
        self,
        request: CapabilitySearchRequest,
        ref: ExactDefinitionRef,
    ) -> bool: ...


class AllowVisibleCatalogPolicy:
    def visible(self, _tenant_scope: str, _ref: ExactDefinitionRef) -> bool:
        return True

    def allowed(
        self,
        _request: CapabilitySearchRequest,
        _ref: ExactDefinitionRef,
    ) -> bool:
        return True


class CapabilitySearchService:
    """Fuse disposable search ranks, then rehydrate and verify Mongo authority."""

    def __init__(
        self,
        *,
        search: CatalogSearchRepository,
        definitions: DefinitionRepository,
        embeddings: CapabilityEmbeddingPort | None = None,
        embedding_model_id: str | None = None,
        embedding_dimensions: int | None = None,
        visibility: CatalogVisibilityPolicy | None = None,
        lexical_weight: float = DEFAULT_LEXICAL_WEIGHT,
        semantic_weight: float = DEFAULT_SEMANTIC_WEIGHT,
        trigram_weight: float = DEFAULT_TRIGRAM_WEIGHT,
        rrf_k: int = RRF_K,
    ) -> None:
        if lexical_weight < 0 or semantic_weight < 0 or trigram_weight < 0:
            raise ValueError("RRF weights cannot be negative")
        if lexical_weight == 0 and semantic_weight == 0 and trigram_weight == 0:
            raise ValueError("at least one RRF branch must be enabled")
        if rrf_k < 1:
            raise ValueError("RRF k must be positive")
        if embeddings is not None and (not embedding_model_id or not embedding_dimensions):
            raise ValueError("an embedding route needs its model id and dimensions")
        self._search = search
        self._definitions = definitions
        self._embeddings = embeddings
        self._embedding_model_id = embedding_model_id
        self._embedding_dimensions = embedding_dimensions
        self._visibility = visibility or AllowVisibleCatalogPolicy()
        self._lexical_weight = lexical_weight
        self._semantic_weight = semantic_weight
        self._trigram_weight = trigram_weight
        self._rrf_k = rrf_k

    @property
    def embeddings_configured(self) -> bool:
        return self._embeddings is not None

    def max_fused_score(self, mode: SearchMode) -> float:
        """The fused score of a hit ranked first in every list the mode runs."""
        weights = self._lexical_weight + self._trigram_weight
        if mode == "hybrid":
            weights += self._semantic_weight
        return weights / (self._rrf_k + 1)

    async def _query_embedding(self, query: str) -> tuple[float, ...] | None:
        """The query vector, or None when no route is configured or the route fails."""
        if self._embeddings is None:
            return None
        try:
            embedded = await self._embeddings.embed(query)
        except Exception as error:
            _LOG.warning("capability search embedding unavailable; lexical only: %s", error)
            return None
        if (
            embedded.model_id != self._embedding_model_id
            or embedded.dimensions != self._embedding_dimensions
        ):
            raise ValueError("query embedding metadata does not match the search index")
        return embedded.vector

    async def search(
        self,
        request: CapabilitySearchRequest,
    ) -> CapabilitySearchResponse:
        branch_limit = min(request.limit * 2, MAX_CANDIDATES_PER_LIST)
        lexical = await self._search.lexical_search(request, limit=branch_limit)
        trigram = await self._search.trigram_search(request, limit=branch_limit)
        query_embedding = await self._query_embedding(request.query)
        mode: SearchMode = "lexical"
        semantic: tuple[RankedCapabilityDocument, ...] = ()
        if query_embedding is not None:
            mode = "hybrid"
            semantic = await self._search.semantic_search(
                request,
                query_embedding,
                limit=branch_limit,
            )
        fused = weighted_rrf(
            lexical,
            semantic,
            trigram=trigram,
            k=self._rrf_k,
            lexical_weight=self._lexical_weight,
            semantic_weight=self._semantic_weight,
            trigram_weight=self._trigram_weight,
        )

        hits: list[CapabilitySearchHit] = []
        for item in fused:
            document = item.document
            ref = document.exact_ref
            try:
                published = await self._definitions.get(ref)
            except (DefinitionNotFound, ReferenceMismatch):
                # A stale projection cannot become selection evidence.
                continue
            source_verified = (
                published.ref == ref
                and sha256_digest(published.definition) == document.source_digest
            )
            authoritative_status = (
                CatalogAssetStatus.RETIRED
                if published.retired_at is not None
                else CatalogAssetStatus.PUBLISHED
            )
            if authoritative_status not in request.status_filter:
                continue
            host_support = getattr(published.definition, "host_support", None)
            support = host_support if isinstance(host_support, CapabilityHostSupport) else None
            if request.host_profiles and (
                support is None
                or not all(support.supports(profile) for profile in request.host_profiles)
            ):
                # The projection's host filter is stale; the authority decides.
                continue
            decision = evaluate_selection(
                SelectionFacts(
                    exact_ref=ref,
                    lifecycle_status=authoritative_status,
                    tenant_visible=self._visibility.visible(
                        request.tenant_scope,
                        ref,
                    ),
                    policy_allowed=self._visibility.allowed(request, ref),
                    source_digest_verified=source_verified,
                    schema_digest_verified=document.schema_digest_verified,
                    required_capabilities=frozenset(),
                    granted_capabilities=frozenset(),
                    runtime_compatible=True,
                    runtime_available=True,
                )
            )
            hits.append(
                CapabilitySearchHit(
                    exact_ref=ref,
                    kind=ref.kind,
                    title=document.title,
                    summary=document.description,
                    lexical_rank=item.lexical_rank,
                    semantic_rank=item.semantic_rank,
                    fused_rank=item.fused_score,
                    compatibility_summary=document.compatibility_summary,
                    authorization_state=decision.authorization_state,
                    reasons=decision.reasons,
                    source_digest=document.source_digest,
                    indexed_at=document.indexed_at,
                    projection_generation=document.projection_generation,
                    parent_ref=document.parent_ref,
                    pin=capability_pin(published).render(),
                    host_support=support,
                    supported_profiles=support.supported_profiles() if support else (),
                    rank_provenance=RankProvenance(
                        lexical_rank=item.lexical_rank,
                        trigram_rank=item.trigram_rank,
                        vector_rank=item.semantic_rank,
                        fused_score=item.fused_score,
                    ),
                )
            )
            if len(hits) >= request.limit:
                break
        exact_hits = tuple(hits)
        return CapabilitySearchResponse(
            hits=exact_hits,
            tool_groups=_group_tools(exact_hits),
            token_use=search_token_use(request.query, exact_hits),
            search_mode=mode,
        )


class FusedCapabilityDocument(SearchResponseContract):
    document: CapabilitySearchDocument
    lexical_rank: int | None = None
    trigram_rank: int | None = None
    semantic_rank: int | None = None
    fused_score: float


def weighted_rrf(
    lexical: Sequence[RankedCapabilityDocument],
    semantic: Sequence[RankedCapabilityDocument],
    *,
    trigram: Sequence[RankedCapabilityDocument] = (),
    k: int = RRF_K,
    lexical_weight: float = DEFAULT_LEXICAL_WEIGHT,
    semantic_weight: float = DEFAULT_SEMANTIC_WEIGHT,
    trigram_weight: float = DEFAULT_TRIGRAM_WEIGHT,
) -> tuple[FusedCapabilityDocument, ...]:
    """Fuse independent ranks; branch scores never leak into RRF."""
    if k < 1 or lexical_weight < 0 or semantic_weight < 0 or trigram_weight < 0:
        raise ValueError("invalid RRF configuration")
    documents: dict[str, CapabilitySearchDocument] = {}
    ranks: dict[str, dict[str, int]] = {"lexical": {}, "trigram": {}, "semantic": {}}
    scores: dict[str, float] = {}
    for branch, rows, weight in (
        ("lexical", lexical, lexical_weight),
        ("trigram", trigram, trigram_weight),
        ("semantic", semantic, semantic_weight),
    ):
        for rank, row in enumerate(rows, start=1):
            key = str(row.document.search_document_id)
            documents[key] = row.document
            ranks[branch].setdefault(key, rank)
            scores[key] = scores.get(key, 0.0) + weight / (k + rank)
    return tuple(
        FusedCapabilityDocument(
            document=documents[key],
            lexical_rank=ranks["lexical"].get(key),
            trigram_rank=ranks["trigram"].get(key),
            semantic_rank=ranks["semantic"].get(key),
            fused_score=scores[key],
        )
        for key in sorted(
            documents,
            key=lambda item: (
                -scores[item],
                documents[item].logical_id,
                documents[item].revision,
            ),
        )
    )


def _group_tools(
    hits: tuple[CapabilitySearchHit, ...],
) -> tuple[MCPToolSearchGroup, ...]:
    groups: dict[ExactDefinitionRef, list[CapabilitySearchHit]] = {}
    for hit in hits:
        if hit.parent_ref is not None:
            groups.setdefault(hit.parent_ref, []).append(hit)
    return tuple(
        MCPToolSearchGroup(parent_ref=parent, tools=tuple(tools))
        for parent, tools in sorted(
            groups.items(),
            key=lambda item: (
                item[0].logical_id,
                item[0].revision,
                item[0].digest,
            ),
        )
    )


def search_token_use(
    query: str,
    hits: tuple[CapabilitySearchHit, ...],
) -> tuple[TokenUseMeasurement, ...]:
    result_json = json.dumps(
        [hit.model_dump(mode="json") for hit in hits],
        sort_keys=True,
        separators=(",", ":"),
    )
    return (
        _token_measurement("search_query", query),
        _token_measurement("search_results", result_json),
    )


def token_measurement(metric_kind: str, value: object) -> TokenUseMeasurement:
    serialized = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return _token_measurement(metric_kind, serialized)


def _token_measurement(metric_kind: str, text: str) -> TokenUseMeasurement:
    characters = len(text)
    return TokenUseMeasurement(
        metric_kind=metric_kind,
        character_count=characters,
        estimated_tokens=ceil(characters / 4),
    )
