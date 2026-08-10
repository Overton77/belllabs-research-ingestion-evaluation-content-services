from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import cast
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict

from app.application.capability_search_repository import (
    CapabilityEmbedding,
    CapabilityEmbeddingPort,
    CapabilitySearchDocument,
    CatalogSearchRepository,
)
from app.application.catalog_projection_metadata import classify_definition
from app.application.control_plane_repository import DefinitionRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    ExactDefinitionRef,
    MCPToolDefinition,
    PublishedDefinition,
)
from app.domain.coordinator.contracts import (
    CatalogAssetStatus,
    SearchDocumentMetadata,
    SearchDocumentSource,
)
from app.domain.coordinator.search_document import (
    SEARCH_DOCUMENT_FORMAT_VERSION,
    RenderedSearchDocument,
    render_search_document,
    search_document_source,
)


class ProjectionContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CatalogProjectionResult(ProjectionContract):
    document: CapabilitySearchDocument
    changed: bool


class CatalogProjectionInput(ProjectionContract):
    ref: ExactDefinitionRef
    metadata: SearchDocumentMetadata | None = None
    operation_classes: frozenset[str] = frozenset()
    workflow_type_refs: frozenset[ExactDefinitionRef] = frozenset()
    capability_requirements: frozenset[str] = frozenset()
    compatible_runtimes: frozenset[str] = frozenset()


class CatalogProjectionError(RuntimeError):
    pass


@dataclass(frozen=True)
class _PreparedProjection:
    request: CatalogProjectionInput
    published: PublishedDefinition
    source: SearchDocumentSource
    rendered: RenderedSearchDocument
    text_digest: str
    status: CatalogAssetStatus
    operation_classes: frozenset[str]
    workflow_type_refs: frozenset[ExactDefinitionRef]
    capability_requirements: frozenset[str]
    compatible_runtimes: frozenset[str]
    embedding: tuple[float, ...] | None


class CatalogProjector:
    """Rehydrate Mongo authority before materializing a disposable search row."""

    def __init__(
        self,
        *,
        definitions: DefinitionRepository,
        search: CatalogSearchRepository,
        embeddings: CapabilityEmbeddingPort,
        embedding_model_id: str,
        embedding_dimensions: int,
        projection_generation: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not embedding_model_id or embedding_dimensions < 1 or not projection_generation:
            raise ValueError("catalog projection configuration is incomplete")
        self._definitions = definitions
        self._search = search
        self._embeddings = embeddings
        self._embedding_model_id = embedding_model_id
        self._embedding_dimensions = embedding_dimensions
        self._projection_generation = projection_generation
        self._clock = clock or (lambda: datetime.now(UTC))

    async def project(
        self,
        ref: ExactDefinitionRef,
        *,
        tenant_scope: str = "global",
        metadata: SearchDocumentMetadata | None = None,
        operation_classes: frozenset[str] = frozenset(),
        workflow_type_refs: frozenset[ExactDefinitionRef] = frozenset(),
        capability_requirements: frozenset[str] = frozenset(),
        compatible_runtimes: frozenset[str] = frozenset(),
    ) -> CatalogProjectionResult:
        return (
            await self.project_many(
                (
                    CatalogProjectionInput(
                        ref=ref,
                        metadata=metadata,
                        operation_classes=operation_classes,
                        workflow_type_refs=workflow_type_refs,
                        capability_requirements=capability_requirements,
                        compatible_runtimes=compatible_runtimes,
                    ),
                ),
                tenant_scope=tenant_scope,
            )
        )[0]

    async def project_many(
        self,
        requests: Sequence[CatalogProjectionInput],
        *,
        tenant_scope: str = "global",
    ) -> tuple[CatalogProjectionResult, ...]:
        if not tenant_scope.strip():
            raise ValueError("catalog projection tenant scope cannot be blank")
        if not requests:
            return ()
        if len({request.ref for request in requests}) != len(requests):
            raise ValueError("catalog projection batch contains duplicate references")

        prepared: list[_PreparedProjection | CatalogProjectionResult] = []
        embedding_inputs: list[str] = []
        embedding_indexes: list[int] = []
        for request in requests:
            item = await self._prepare(request, tenant_scope)
            if isinstance(item, CatalogProjectionResult):
                prepared.append(item)
                continue
            prepared.append(item)
            if item.embedding is None:
                embedding_indexes.append(len(prepared) - 1)
                embedding_inputs.append(item.rendered.search_text)

        if embedding_inputs:
            embedded_items = await self._embed_many(tuple(embedding_inputs))
            if len(embedded_items) != len(embedding_inputs):
                raise CatalogProjectionError("embedding batch response has an invalid item count")
            for prepared_index, embedded in zip(
                embedding_indexes,
                embedded_items,
                strict=True,
            ):
                item = cast(_PreparedProjection, prepared[prepared_index])
                if (
                    embedded.model_id != self._embedding_model_id
                    or embedded.dimensions != self._embedding_dimensions
                    or embedded.input_digest != item.text_digest
                ):
                    raise CatalogProjectionError(
                        "embedding metadata does not match the projection claim"
                    )
                prepared[prepared_index] = _PreparedProjection(
                    **{
                        **item.__dict__,
                        "embedding": embedded.vector,
                    }
                )

        results: list[CatalogProjectionResult] = []
        for item in prepared:
            if isinstance(item, CatalogProjectionResult):
                results.append(item)
                continue
            if item.embedding is None:
                raise CatalogProjectionError("projection embedding is unavailable")
            document = self._document(item, tenant_scope)
            changed = await self._search.upsert(document)
            results.append(CatalogProjectionResult(document=document, changed=changed))
        return tuple(results)

    async def _prepare(
        self,
        request: CatalogProjectionInput,
        tenant_scope: str,
    ) -> _PreparedProjection | CatalogProjectionResult:
        ref = request.ref
        published = await self._definitions.get(ref)
        published_identity = published.ref.model_dump(exclude={"lifecycle_status"})
        requested_identity = ref.model_dump(exclude={"lifecycle_status"})
        if (
            published_identity != requested_identity
            or sha256_digest(published.definition) != ref.digest
        ):
            raise CatalogProjectionError(
                "authoritative definition does not match the projection source reference"
            )
        source = search_document_source(published.definition, request.metadata)
        rendered = render_search_document(source)
        classification = classify_definition(
            published.definition,
            exact_ref=ref,
        )
        operation_classes = request.operation_classes | classification.operation_classes
        workflow_type_refs = request.workflow_type_refs | classification.workflow_type_refs
        capability_requirements = (
            request.capability_requirements | classification.capability_requirements
        )
        compatible_runtimes = request.compatible_runtimes | classification.compatible_runtimes
        if rendered.search_document_format_version != SEARCH_DOCUMENT_FORMAT_VERSION:
            raise CatalogProjectionError("search document format version changed")
        text_digest = _text_digest(rendered.search_text)
        status = (
            CatalogAssetStatus.RETIRED
            if published.retired_at is not None
            else CatalogAssetStatus.PUBLISHED
        )
        current = await self._search.get(
            tenant_scope,
            ref.kind,
            ref.logical_id,
            ref.revision,
            projection_generation=self._projection_generation,
        )
        reusable = current
        if reusable is None:
            reusable = await self._search.get(
                tenant_scope,
                ref.kind,
                ref.logical_id,
                ref.revision,
            )
        embedding_reusable = (
            reusable is not None
            and reusable.source_digest == ref.digest
            and reusable.search_text_digest == text_digest
            and reusable.embedding_model_id == self._embedding_model_id
            and reusable.embedding_dimensions == self._embedding_dimensions
            and reusable.search_document_format_version == SEARCH_DOCUMENT_FORMAT_VERSION
        )
        projection_unchanged = (
            embedding_reusable
            and current is not None
            and current.status == status
            and current.projection_generation == self._projection_generation
            and current.tags == source.tags
            and current.domains == source.domains
            and current.operation_classes == operation_classes
            and current.workflow_type_refs == workflow_type_refs
            and current.capability_requirements == capability_requirements
            and current.compatible_runtimes == compatible_runtimes
        )
        if projection_unchanged and current is not None:
            return CatalogProjectionResult(document=current, changed=False)
        return _PreparedProjection(
            request=request,
            published=published,
            source=source,
            rendered=rendered,
            text_digest=text_digest,
            status=status,
            operation_classes=operation_classes,
            workflow_type_refs=workflow_type_refs,
            capability_requirements=capability_requirements,
            compatible_runtimes=compatible_runtimes,
            embedding=reusable.embedding if embedding_reusable and reusable else None,
        )

    def _document(
        self,
        item: _PreparedProjection,
        tenant_scope: str,
    ) -> CapabilitySearchDocument:
        ref = item.request.ref
        parent_ref = (
            item.published.definition.server_ref
            if isinstance(item.published.definition, MCPToolDefinition)
            else None
        )
        return CapabilitySearchDocument(
            search_document_id=uuid5(
                NAMESPACE_URL,
                (
                    f"belllabs:capability-search:{tenant_scope}:"
                    f"{ref.kind.value}:{ref.logical_id}:{ref.revision}"
                ),
            ),
            tenant_scope=tenant_scope,
            asset_kind=ref.kind,
            logical_id=ref.logical_id,
            revision=ref.revision,
            source_digest=ref.digest,
            status=item.status,
            title=item.source.title,
            description=item.source.description,
            search_text=item.rendered.search_text,
            search_text_digest=item.text_digest,
            embedding=item.embedding or (),
            embedding_model_id=self._embedding_model_id,
            embedding_dimensions=self._embedding_dimensions,
            search_document_format_version=(item.rendered.search_document_format_version),
            parent_ref=parent_ref,
            tags=item.source.tags,
            domains=item.source.domains,
            operation_classes=item.operation_classes,
            workflow_type_refs=item.workflow_type_refs,
            capability_requirements=item.capability_requirements,
            compatible_runtimes=item.compatible_runtimes,
            compatibility_summary=(
                item.source.compatibility_summary or "Compatible with the indexed catalog contract."
            ),
            mongodb_document_id=(f"{ref.kind.value}:{ref.logical_id}:{ref.revision}:{ref.digest}"),
            source_published_at=item.published.published_at,
            indexed_at=self._clock(),
            projection_generation=self._projection_generation,
        )

    async def _embed_many(
        self,
        texts: tuple[str, ...],
    ) -> tuple[CapabilityEmbedding, ...]:
        batch_method = getattr(self._embeddings, "embed_many", None)
        if callable(batch_method):
            method = cast(
                Callable[
                    [tuple[str, ...]],
                    Awaitable[tuple[CapabilityEmbedding, ...]],
                ],
                batch_method,
            )
            return await method(texts)
        return tuple([await self._embeddings.embed(text) for text in texts])

    @property
    def projection_generation(self) -> str:
        return self._projection_generation

    @property
    def embedding_model_id(self) -> str:
        return self._embedding_model_id

    @property
    def embedding_dimensions(self) -> int:
        return self._embedding_dimensions

    @property
    def definitions(self) -> DefinitionRepository:
        return self._definitions

    @property
    def search(self) -> CatalogSearchRepository:
        return self._search


def _text_digest(value: str) -> str:
    return f"sha256:{sha256(value.encode()).hexdigest()}"
