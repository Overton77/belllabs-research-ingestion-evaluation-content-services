from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.coordinator.contracts import (
    CapabilitySearchRequest,
    CatalogAssetStatus,
)

_TOKEN = re.compile(r"[a-z0-9]+(?:[._:-][a-z0-9]+)*")


class SearchRepositoryContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CapabilityEmbedding(SearchRepositoryContract):
    vector: tuple[float, ...] = Field(min_length=1)
    model_id: str = Field(min_length=1)
    dimensions: int = Field(ge=1)
    input_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    @model_validator(mode="after")
    def vector_matches_claimed_dimensions(self) -> CapabilityEmbedding:
        if len(self.vector) != self.dimensions:
            raise ValueError("embedding vector does not match its claimed dimensions")
        if any(not math.isfinite(value) for value in self.vector):
            raise ValueError("embedding vectors must contain only finite values")
        return self


class CapabilityEmbeddingPort(Protocol):
    async def embed(self, text: str) -> CapabilityEmbedding: ...

    async def embed_many(
        self,
        texts: tuple[str, ...],
    ) -> tuple[CapabilityEmbedding, ...]: ...


class CapabilitySearchDocument(SearchRepositoryContract):
    search_document_id: UUID
    tenant_scope: str = Field(min_length=1)
    asset_kind: DefinitionKind
    logical_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    source_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    status: CatalogAssetStatus
    title: str = Field(min_length=1)
    description: str = Field(min_length=1)
    search_text: str = Field(min_length=1)
    search_text_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    embedding: tuple[float, ...] = Field(min_length=1)
    embedding_model_id: str = Field(min_length=1)
    embedding_dimensions: int = Field(ge=1)
    search_document_format_version: int = Field(ge=1)
    parent_ref: ExactDefinitionRef | None = None
    tags: frozenset[str] = frozenset()
    domains: frozenset[str] = frozenset()
    operation_classes: frozenset[str] = frozenset()
    workflow_type_refs: frozenset[ExactDefinitionRef] = frozenset()
    capability_requirements: frozenset[str] = frozenset()
    compatible_runtimes: frozenset[str] = frozenset()
    compatibility_summary: str = "Compatible with the indexed catalog contract."
    schema_digest_verified: bool = True
    mongodb_collection: str = "control_plane_published_definitions"
    mongodb_document_id: str = Field(min_length=1)
    source_published_at: datetime
    indexed_at: datetime
    projection_generation: str = Field(min_length=1)

    @model_validator(mode="after")
    def validate_projection_shape(self) -> CapabilitySearchDocument:
        if len(self.embedding) != self.embedding_dimensions:
            raise ValueError("search document embedding dimensions do not match")
        if self.asset_kind == DefinitionKind.MCP_TOOL:
            if (
                self.parent_ref is None
                or self.parent_ref.kind != DefinitionKind.MCP_SERVER
            ):
                raise ValueError("MCP Tool search rows require an exact MCP Server parent")
        elif self.parent_ref is not None:
            raise ValueError("only MCP Tool search rows may carry a parent")
        if any(ref.kind != DefinitionKind.WORKFLOW_TYPE for ref in self.workflow_type_refs):
            raise ValueError("workflow filters must contain Workflow Type references")
        return self

    @property
    def exact_ref(self) -> ExactDefinitionRef:
        return ExactDefinitionRef(
            kind=self.asset_kind,
            logical_id=self.logical_id,
            revision=self.revision,
            digest=self.source_digest,
        )


class RankedCapabilityDocument(SearchRepositoryContract):
    document: CapabilitySearchDocument
    branch_score: float


class CatalogSearchRepository(Protocol):
    async def get(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
        logical_id: str,
        revision: int,
        *,
        projection_generation: str | None = None,
    ) -> CapabilitySearchDocument | None: ...

    async def upsert(self, document: CapabilitySearchDocument) -> bool:
        """Return True only when the stored projection changed."""
        ...

    async def list_generation(
        self,
        tenant_scope: str,
        projection_generation: str,
        *,
        kinds: frozenset[DefinitionKind] = frozenset(),
    ) -> tuple[CapabilitySearchDocument, ...]: ...

    async def lexical_search(
        self,
        request: CapabilitySearchRequest,
        *,
        limit: int,
    ) -> tuple[RankedCapabilityDocument, ...]: ...

    async def semantic_search(
        self,
        request: CapabilitySearchRequest,
        query_embedding: tuple[float, ...],
        *,
        limit: int,
    ) -> tuple[RankedCapabilityDocument, ...]: ...


class InMemoryCatalogSearchRepository:
    """Deterministic test adapter with the same pre-ranking filters as PostgreSQL."""

    def __init__(self) -> None:
        self._documents: dict[
            tuple[str, DefinitionKind, str, int, str],
            CapabilitySearchDocument,
        ] = {}
        self._active_generations: dict[tuple[str, DefinitionKind], str] = {}
        self._generation_contracts: dict[
            tuple[str, str],
            tuple[str, int, int],
        ] = {}

    async def get(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
        logical_id: str,
        revision: int,
        *,
        projection_generation: str | None = None,
    ) -> CapabilitySearchDocument | None:
        generation = projection_generation or self._active_generations.get(
            (tenant_scope, kind)
        )
        if generation is None:
            candidates = [
                document
                for key, document in self._documents.items()
                if key[:4] == (tenant_scope, kind, logical_id, revision)
            ]
            value = candidates[-1] if candidates else None
        else:
            value = self._documents.get(
                (tenant_scope, kind, logical_id, revision, generation)
            )
        return value.model_copy(deep=True) if value is not None else None

    async def upsert(self, document: CapabilitySearchDocument) -> bool:
        contract = self._generation_contracts.get(
            (document.tenant_scope, document.projection_generation)
        )
        if contract is not None and contract != (
            document.embedding_model_id,
            document.embedding_dimensions,
            document.search_document_format_version,
        ):
            raise RuntimeError(
                "capability projection row violates its generation contract"
            )
        key = (
            document.tenant_scope,
            document.asset_kind,
            document.logical_id,
            document.revision,
            document.projection_generation,
        )
        current = self._documents.get(key)
        if current == document:
            return False
        self._documents[key] = document.model_copy(deep=True)
        return True

    async def list_generation(
        self,
        tenant_scope: str,
        projection_generation: str,
        *,
        kinds: frozenset[DefinitionKind] = frozenset(),
    ) -> tuple[CapabilitySearchDocument, ...]:
        documents = (
            document.model_copy(deep=True)
            for document in self._documents.values()
            if document.tenant_scope == tenant_scope
            and document.projection_generation == projection_generation
            and (not kinds or document.asset_kind in kinds)
        )
        return tuple(
            sorted(
                documents,
                key=lambda document: (
                    document.asset_kind.value,
                    document.logical_id,
                    document.revision,
                    document.source_digest,
                ),
            )
        )

    def activate_generation(
        self,
        tenant_scope: str,
        projection_generation: str,
        kinds: frozenset[DefinitionKind],
    ) -> None:
        for kind in kinds:
            self._active_generations[(tenant_scope, kind)] = projection_generation

    def active_generation(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
    ) -> str | None:
        return self._active_generations.get((tenant_scope, kind))

    def register_generation_contract(
        self,
        tenant_scope: str,
        projection_generation: str,
        *,
        embedding_model_id: str,
        embedding_dimensions: int,
        search_document_format_version: int,
    ) -> None:
        key = (tenant_scope, projection_generation)
        contract = (
            embedding_model_id,
            embedding_dimensions,
            search_document_format_version,
        )
        current = self._generation_contracts.setdefault(key, contract)
        if current != contract:
            raise RuntimeError("projection generation contract changed")

    def active_generations_for_tenant(
        self,
        tenant_scope: str,
    ) -> dict[DefinitionKind, str]:
        return {
            kind: generation
            for (scope, kind), generation in self._active_generations.items()
            if scope == tenant_scope
        }

    async def lexical_search(
        self,
        request: CapabilitySearchRequest,
        *,
        limit: int,
    ) -> tuple[RankedCapabilityDocument, ...]:
        query_terms = _terms(request.query)
        scored = [
            RankedCapabilityDocument(document=document, branch_score=score)
            for document in self._filtered(request)
            if (score := _lexical_score(query_terms, document)) > 0
        ]
        return tuple(
            sorted(
                scored,
                key=lambda item: (
                    -item.branch_score,
                    item.document.logical_id,
                    item.document.revision,
                ),
            )[:limit]
        )

    async def semantic_search(
        self,
        request: CapabilitySearchRequest,
        query_embedding: tuple[float, ...],
        *,
        limit: int,
    ) -> tuple[RankedCapabilityDocument, ...]:
        scored: list[RankedCapabilityDocument] = []
        for document in self._filtered(request):
            if len(document.embedding) != len(query_embedding):
                continue
            score = _cosine_similarity(query_embedding, document.embedding)
            scored.append(
                RankedCapabilityDocument(document=document, branch_score=score)
            )
        return tuple(
            sorted(
                scored,
                key=lambda item: (
                    -item.branch_score,
                    item.document.logical_id,
                    item.document.revision,
                ),
            )[:limit]
        )

    def _filtered(
        self,
        request: CapabilitySearchRequest,
    ) -> tuple[CapabilitySearchDocument, ...]:
        return tuple(
            document
            for document in self._documents.values()
            if self._is_active(document)
            if document.tenant_scope in {"global", request.tenant_scope}
            and (not request.kinds or document.asset_kind in request.kinds)
            and document.status in request.status_filter
            and (
                not request.required_capabilities
                or request.required_capabilities <= document.capability_requirements
            )
            and (
                request.runtime is None
                or request.runtime in document.compatible_runtimes
            )
            and (
                request.operation_class is None
                or request.operation_class in document.operation_classes
            )
            and (
                request.workflow_type_ref is None
                or request.workflow_type_ref in document.workflow_type_refs
            )
        )

    def _is_active(self, document: CapabilitySearchDocument) -> bool:
        generation = self._active_generations.get(
            (document.tenant_scope, document.asset_kind)
        )
        return generation is None or generation == document.projection_generation


def _terms(value: str) -> tuple[str, ...]:
    return tuple(_TOKEN.findall(value.casefold()))


def _lexical_score(
    query_terms: tuple[str, ...],
    document: CapabilitySearchDocument,
) -> float:
    if not query_terms:
        return 0
    logical = document.logical_id.casefold()
    title = document.title.casefold()
    description = document.description.casefold()
    search_text = document.search_text.casefold()
    score = 0.0
    for term in query_terms:
        score += logical.count(term) * 5.0
        score += title.count(term) * 3.0
        score += search_text.count(term) * 1.0
        score += description.count(term) * 0.5
    normalized_query = " ".join(query_terms)
    if normalized_query == logical:
        score += 20.0
    elif normalized_query in logical:
        score += 8.0
    return score


def _cosine_similarity(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    left_norm = math.sqrt(sum(value * value for value in left))
    right_norm = math.sqrt(sum(value * value for value in right))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return sum(a * b for a, b in zip(left, right, strict=True)) / (
        left_norm * right_norm
    )
