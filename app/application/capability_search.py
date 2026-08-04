from __future__ import annotations

import json
from collections.abc import Sequence
from math import ceil
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.application.capability_search_repository import (
    CapabilityEmbeddingPort,
    CapabilitySearchDocument,
    CatalogSearchRepository,
    RankedCapabilityDocument,
)
from app.application.control_plane_repository import DefinitionRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import ExactDefinitionRef
from app.domain.control_plane.errors import (
    DefinitionNotFound,
    ReferenceMismatch,
)
from app.domain.coordinator.contracts import (
    CapabilitySearchHit,
    CapabilitySearchRequest,
    CatalogAssetStatus,
    SelectionFacts,
)
from app.domain.coordinator.policy import evaluate_selection

RRF_K = 50
DEFAULT_LEXICAL_WEIGHT = 1.0
DEFAULT_SEMANTIC_WEIGHT = 1.0


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
        embeddings: CapabilityEmbeddingPort,
        embedding_model_id: str,
        embedding_dimensions: int,
        visibility: CatalogVisibilityPolicy | None = None,
        lexical_weight: float = DEFAULT_LEXICAL_WEIGHT,
        semantic_weight: float = DEFAULT_SEMANTIC_WEIGHT,
        rrf_k: int = RRF_K,
    ) -> None:
        if lexical_weight < 0 or semantic_weight < 0:
            raise ValueError("RRF weights cannot be negative")
        if lexical_weight == 0 and semantic_weight == 0:
            raise ValueError("at least one RRF branch must be enabled")
        if rrf_k < 1:
            raise ValueError("RRF k must be positive")
        self._search = search
        self._definitions = definitions
        self._embeddings = embeddings
        self._embedding_model_id = embedding_model_id
        self._embedding_dimensions = embedding_dimensions
        self._visibility = visibility or AllowVisibleCatalogPolicy()
        self._lexical_weight = lexical_weight
        self._semantic_weight = semantic_weight
        self._rrf_k = rrf_k

    async def search(
        self,
        request: CapabilitySearchRequest,
    ) -> CapabilitySearchResponse:
        branch_limit = min(request.limit * 2, 100)
        lexical = await self._search.lexical_search(request, limit=branch_limit)
        query_embedding = await self._embeddings.embed(request.query)
        if (
            query_embedding.model_id != self._embedding_model_id
            or query_embedding.dimensions != self._embedding_dimensions
        ):
            raise ValueError("query embedding metadata does not match the search index")
        semantic = await self._search.semantic_search(
            request,
            query_embedding.vector,
            limit=branch_limit,
        )
        fused = weighted_rrf(
            lexical,
            semantic,
            k=self._rrf_k,
            lexical_weight=self._lexical_weight,
            semantic_weight=self._semantic_weight,
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
                )
            )
            if len(hits) >= request.limit:
                break
        exact_hits = tuple(hits)
        return CapabilitySearchResponse(
            hits=exact_hits,
            tool_groups=_group_tools(exact_hits),
            token_use=search_token_use(request.query, exact_hits),
        )


class FusedCapabilityDocument(SearchResponseContract):
    document: CapabilitySearchDocument
    lexical_rank: int | None = None
    semantic_rank: int | None = None
    fused_score: float


def weighted_rrf(
    lexical: Sequence[RankedCapabilityDocument],
    semantic: Sequence[RankedCapabilityDocument],
    *,
    k: int = RRF_K,
    lexical_weight: float = DEFAULT_LEXICAL_WEIGHT,
    semantic_weight: float = DEFAULT_SEMANTIC_WEIGHT,
) -> tuple[FusedCapabilityDocument, ...]:
    """Fuse independent ranks; branch scores never leak into RRF."""
    if k < 1 or lexical_weight < 0 or semantic_weight < 0:
        raise ValueError("invalid RRF configuration")
    documents: dict[str, CapabilitySearchDocument] = {}
    lexical_ranks: dict[str, int] = {}
    semantic_ranks: dict[str, int] = {}
    scores: dict[str, float] = {}
    for rank, row in enumerate(lexical, start=1):
        key = str(row.document.search_document_id)
        documents[key] = row.document
        lexical_ranks.setdefault(key, rank)
        scores[key] = scores.get(key, 0.0) + lexical_weight / (k + rank)
    for rank, row in enumerate(semantic, start=1):
        key = str(row.document.search_document_id)
        documents[key] = row.document
        semantic_ranks.setdefault(key, rank)
        scores[key] = scores.get(key, 0.0) + semantic_weight / (k + rank)
    return tuple(
        FusedCapabilityDocument(
            document=documents[key],
            lexical_rank=lexical_ranks.get(key),
            semantic_rank=semantic_ranks.get(key),
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
