from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from app.application.capability.capability_search_repository import (
    CapabilitySearchDocument,
    RankedCapabilityDocument,
)
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.coordinator.contracts import CapabilitySearchRequest


class PostgresConnection(Protocol):
    async def fetchrow(self, query: str, *args: object) -> Mapping[str, Any] | None: ...

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def execute(self, query: str, *args: object) -> str: ...

    def transaction(self) -> Any: ...


class PostgresAcquireContext(Protocol):
    async def __aenter__(self) -> PostgresConnection: ...

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        traceback: object,
    ) -> None: ...


class PostgresPool(Protocol):
    def acquire(self) -> PostgresAcquireContext: ...


class PostgresCatalogSearchRepository:
    """Supabase/PostgreSQL projection adapter; the injected pool owns connectivity."""

    def __init__(self, pool: PostgresPool) -> None:
        self._pool = pool

    async def get(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
        logical_id: str,
        revision: int,
        *,
        projection_generation: str | None = None,
    ) -> CapabilitySearchDocument | None:
        async with self._pool.acquire() as connection:
            if projection_generation is None:
                row = await connection.fetchrow(
                    """
                    SELECT documents.*
                    FROM capability_search.documents AS documents
                    JOIN capability_search.active_generations AS active
                      ON active.tenant_scope = documents.tenant_scope
                     AND active.asset_kind = documents.asset_kind
                     AND active.projection_generation =
                         documents.projection_generation
                    WHERE documents.tenant_scope = $1
                      AND documents.asset_kind = $2
                      AND documents.logical_id = $3
                      AND documents.revision = $4
                    """,
                    tenant_scope,
                    kind.value,
                    logical_id,
                    revision,
                )
            else:
                row = await connection.fetchrow(
                    """
                    SELECT *
                    FROM capability_search.documents
                    WHERE tenant_scope = $1
                      AND asset_kind = $2
                      AND logical_id = $3
                      AND revision = $4
                      AND projection_generation = $5
                    """,
                    tenant_scope,
                    kind.value,
                    logical_id,
                    revision,
                    projection_generation,
                )
        return _document(row) if row is not None else None

    async def upsert(self, document: CapabilitySearchDocument) -> bool:
        parent = document.parent_ref
        async with self._pool.acquire() as connection:
            generation = await connection.fetchrow(
                """
                SELECT state
                FROM capability_search.generations
                WHERE tenant_scope = $1
                  AND projection_generation = $2
                  AND state IN ('building', 'active')
                  AND embedding_model_id = $3
                  AND embedding_dimensions = $4
                  AND search_document_format_version = $5
                FOR KEY SHARE
                """,
                document.tenant_scope,
                document.projection_generation,
                document.embedding_model_id,
                document.embedding_dimensions,
                document.search_document_format_version,
            )
            if generation is None:
                raise RuntimeError("capability projection generation is missing or not writable")
            row = await connection.fetchrow(
                """
                INSERT INTO capability_search.documents (
                    search_document_id,
                    tenant_scope,
                    asset_kind,
                    logical_id,
                    revision,
                    source_digest,
                    status,
                    title,
                    description,
                    search_text,
                    search_text_digest,
                    embedding,
                    embedding_model_id,
                    embedding_dimensions,
                    search_document_format_version,
                    parent_kind,
                    parent_logical_id,
                    parent_revision,
                    parent_source_digest,
                    mongodb_collection,
                    mongodb_document_id,
                    tags,
                    domains,
                    operation_classes,
                    workflow_type_refs,
                    capability_requirements,
                    compatible_runtimes,
                    compatibility_summary,
                    schema_digest_verified,
                    source_published_at,
                    indexed_at,
                    projection_generation
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    $12::extensions.vector, $13, $14, $15, $16, $17, $18,
                    $19, $20, $21, $22, $23, $24, $25::jsonb, $26, $27,
                    $28, $29, $30, $31, $32
                )
                ON CONFLICT (
                    tenant_scope,
                    asset_kind,
                    logical_id,
                    revision,
                    projection_generation
                )
                DO UPDATE SET
                    search_document_id = EXCLUDED.search_document_id,
                    source_digest = EXCLUDED.source_digest,
                    status = EXCLUDED.status,
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    search_text = EXCLUDED.search_text,
                    search_text_digest = EXCLUDED.search_text_digest,
                    embedding = EXCLUDED.embedding,
                    embedding_model_id = EXCLUDED.embedding_model_id,
                    embedding_dimensions = EXCLUDED.embedding_dimensions,
                    search_document_format_version =
                        EXCLUDED.search_document_format_version,
                    parent_kind = EXCLUDED.parent_kind,
                    parent_logical_id = EXCLUDED.parent_logical_id,
                    parent_revision = EXCLUDED.parent_revision,
                    parent_source_digest = EXCLUDED.parent_source_digest,
                    mongodb_collection = EXCLUDED.mongodb_collection,
                    mongodb_document_id = EXCLUDED.mongodb_document_id,
                    tags = EXCLUDED.tags,
                    domains = EXCLUDED.domains,
                    operation_classes = EXCLUDED.operation_classes,
                    workflow_type_refs = EXCLUDED.workflow_type_refs,
                    capability_requirements = EXCLUDED.capability_requirements,
                    compatible_runtimes = EXCLUDED.compatible_runtimes,
                    compatibility_summary = EXCLUDED.compatibility_summary,
                    schema_digest_verified = EXCLUDED.schema_digest_verified,
                    source_published_at = EXCLUDED.source_published_at,
                    indexed_at = EXCLUDED.indexed_at,
                    projection_generation = EXCLUDED.projection_generation
                WHERE documents IS DISTINCT FROM EXCLUDED
                RETURNING search_document_id
                """,
                document.search_document_id,
                document.tenant_scope,
                document.asset_kind.value,
                document.logical_id,
                document.revision,
                document.source_digest,
                document.status.value,
                document.title,
                document.description,
                document.search_text,
                document.search_text_digest,
                _vector_literal(document.embedding),
                document.embedding_model_id,
                document.embedding_dimensions,
                document.search_document_format_version,
                parent.kind.value if parent else None,
                parent.logical_id if parent else None,
                parent.revision if parent else None,
                parent.digest if parent else None,
                document.mongodb_collection,
                document.mongodb_document_id,
                sorted(document.tags),
                sorted(document.domains),
                sorted(document.operation_classes),
                json.dumps(
                    [
                        ref.model_dump(mode="json")
                        for ref in sorted(
                            document.workflow_type_refs,
                            key=lambda item: (
                                item.logical_id,
                                item.revision,
                                item.digest,
                            ),
                        )
                    ]
                ),
                sorted(document.capability_requirements),
                sorted(document.compatible_runtimes),
                document.compatibility_summary,
                document.schema_digest_verified,
                document.source_published_at,
                document.indexed_at,
                document.projection_generation,
            )
        return row is not None

    async def list_generation(
        self,
        tenant_scope: str,
        projection_generation: str,
        *,
        kinds: frozenset[DefinitionKind] = frozenset(),
    ) -> tuple[CapabilitySearchDocument, ...]:
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT *
                FROM capability_search.documents
                WHERE tenant_scope = $1
                  AND projection_generation = $2
                  AND (
                      cardinality($3::text[]) = 0
                      OR asset_kind = ANY($3::text[])
                  )
                ORDER BY asset_kind, logical_id, revision, source_digest
                """,
                tenant_scope,
                projection_generation,
                [kind.value for kind in sorted(kinds, key=lambda item: item.value)],
            )
        return tuple(_document(row) for row in rows)

    async def lexical_search(
        self,
        request: CapabilitySearchRequest,
        *,
        limit: int,
    ) -> tuple[RankedCapabilityDocument, ...]:
        query, args = _filtered_query(
            request,
            score_sql=("ts_rank_cd(fts, websearch_to_tsquery('english', $1), 32)"),
            match_sql="fts @@ websearch_to_tsquery('english', $1)",
            order_sql="branch_score DESC, logical_id, revision",
            tail_args=(request.query, limit),
        )
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, *args)
        return tuple(
            RankedCapabilityDocument(
                document=_document(row),
                branch_score=float(row["branch_score"]),
            )
            for row in rows
        )

    async def semantic_search(
        self,
        request: CapabilitySearchRequest,
        query_embedding: tuple[float, ...],
        *,
        limit: int,
    ) -> tuple[RankedCapabilityDocument, ...]:
        query, args = _filtered_query(
            request,
            score_sql=("1 - (embedding <=> $1::extensions.vector)"),
            match_sql="TRUE",
            order_sql="embedding <=> $1::extensions.vector, logical_id, revision",
            tail_args=(_vector_literal(query_embedding), limit),
        )
        async with self._pool.acquire() as connection:
            rows = await connection.fetch(query, *args)
        return tuple(
            RankedCapabilityDocument(
                document=_document(row),
                branch_score=float(row["branch_score"]),
            )
            for row in rows
        )


def _filtered_query(
    request: CapabilitySearchRequest,
    *,
    score_sql: str,
    match_sql: str,
    order_sql: str,
    tail_args: tuple[object, int],
) -> tuple[str, tuple[object, ...]]:
    # $1 is branch-specific input and $9 is the branch limit.
    workflow_ref = (
        json.dumps(request.workflow_type_ref.model_dump(mode="json"))
        if request.workflow_type_ref is not None
        else None
    )
    query = f"""
        SELECT documents.*, {score_sql} AS branch_score
        FROM capability_search.documents AS documents
        JOIN capability_search.active_generations AS active
          ON active.tenant_scope = documents.tenant_scope
         AND active.asset_kind = documents.asset_kind
         AND active.projection_generation = documents.projection_generation
        WHERE ({match_sql})
          AND documents.tenant_scope IN ('global', $2)
          AND (
              cardinality($3::text[]) = 0
              OR documents.asset_kind = ANY($3::text[])
          )
          AND documents.status = ANY($4::text[])
          AND (
              cardinality($5::text[]) = 0
              OR documents.capability_requirements @> $5::text[]
          )
          AND (
              $6::text IS NULL
              OR $6 = ANY(documents.compatible_runtimes)
          )
          AND (
              $7::text IS NULL
              OR $7 = ANY(documents.operation_classes)
          )
          AND (
              $8::jsonb IS NULL
              OR documents.workflow_type_refs @> jsonb_build_array($8::jsonb)
          )
        ORDER BY {order_sql}
        LIMIT $9
    """
    return query, (
        tail_args[0],
        request.tenant_scope,
        [kind.value for kind in sorted(request.kinds, key=lambda item: item.value)],
        [status.value for status in sorted(request.status_filter)],
        sorted(request.required_capabilities),
        request.runtime,
        request.operation_class,
        workflow_ref,
        tail_args[1],
    )


def _document(row: Mapping[str, Any]) -> CapabilitySearchDocument:
    parent = None
    if row.get("parent_kind") is not None:
        parent = ExactDefinitionRef(
            kind=DefinitionKind(str(row["parent_kind"])),
            logical_id=str(row["parent_logical_id"]),
            revision=int(row["parent_revision"]),
            digest=str(row["parent_source_digest"]),
        )
    raw_refs = row.get("workflow_type_refs") or []
    if isinstance(raw_refs, str):
        raw_refs = json.loads(raw_refs)
    embedding = row["embedding"]
    if isinstance(embedding, str):
        embedding = tuple(float(item) for item in embedding.strip("[]").split(",") if item)
    return CapabilitySearchDocument(
        search_document_id=row["search_document_id"],
        tenant_scope=str(row["tenant_scope"]),
        asset_kind=DefinitionKind(str(row["asset_kind"])),
        logical_id=str(row["logical_id"]),
        revision=int(row["revision"]),
        source_digest=str(row["source_digest"]),
        status=str(row["status"]),
        title=str(row["title"]),
        description=str(row["description"]),
        search_text=str(row["search_text"]),
        search_text_digest=str(row["search_text_digest"]),
        embedding=tuple(float(item) for item in embedding),
        embedding_model_id=str(row["embedding_model_id"]),
        embedding_dimensions=int(row["embedding_dimensions"]),
        search_document_format_version=int(row["search_document_format_version"]),
        parent_ref=parent,
        tags=frozenset(row.get("tags") or ()),
        domains=frozenset(row.get("domains") or ()),
        operation_classes=frozenset(row.get("operation_classes") or ()),
        workflow_type_refs=frozenset(ExactDefinitionRef.model_validate(item) for item in raw_refs),
        capability_requirements=frozenset(row.get("capability_requirements") or ()),
        compatible_runtimes=frozenset(row.get("compatible_runtimes") or ()),
        compatibility_summary=str(row["compatibility_summary"]),
        schema_digest_verified=bool(row["schema_digest_verified"]),
        mongodb_collection=str(row["mongodb_collection"]),
        mongodb_document_id=str(row["mongodb_document_id"]),
        source_published_at=row["source_published_at"],
        indexed_at=row["indexed_at"],
        projection_generation=str(row["projection_generation"]),
    )


def _vector_literal(vector: tuple[float, ...]) -> str:
    return "[" + ",".join(format(value, ".17g") for value in vector) + "]"
