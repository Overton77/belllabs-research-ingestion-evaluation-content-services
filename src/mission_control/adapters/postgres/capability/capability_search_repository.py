"""Capability search projection over ``mission_control_search`` (common component).

The projection is installation scoped: every statement runs inside an explicit
transaction after ``apply_catalog_scope``. The installation is taken from the trusted
constructor ``catalog_scope`` and/or the Mission Control scope string carried by the
request (``mc/{installation}/{app}/{tenant|catalog}``); a mismatch fails closed and the
legacy ``'global'`` visibility value only means "every scope of this installation".
Search results additionally require the source definition to be an admitted (or, when
requested, retired) ``mission_control.asset_version``; revoked or unadmitted documents are
never returned. Embeddings are never invented here.

FT-A3 (ADR-0013 extended by ADR-0025): embeddings are nullable, so rows rank lexically
until embedded; ``trigram_search`` ranks near-exact names through pg_trgm word similarity
over the generated ``name_surface``; every list filters host profiles and side-effect
classes before ranking.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import Any, Protocol, cast
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.control_plane.catalog_assets import (
    PUBLISHED_DEFINITION_CONTRACT,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope, parse_catalog_scope
from mission_control.application.capabilities.capability_search_repository import (
    TRIGRAM_THRESHOLD,
    CapabilitySearchDocument,
    RankedCapabilityDocument,
)
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.contracts import DefinitionKind, ExactDefinitionRef
from mission_control.domain.coordinator.contracts import CapabilitySearchRequest


class PostgresConnection(Protocol):
    async def fetchrow(self, query: str, *args: object) -> Mapping[str, Any] | None: ...

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> Sequence[Mapping[str, Any]]: ...

    async def execute(self, query: str, *args: object) -> str: ...

    def transaction(self) -> Any: ...

    def is_in_transaction(self) -> bool: ...


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


def installation_of(scope: str) -> tuple[UUID, str] | None:
    """Installation identity named by a Mission Control scope string, if any."""
    if not scope.startswith("mc/"):
        return None
    if scope.endswith("/catalog"):
        return parse_catalog_scope(scope)
    parsed = parse_request_scope(scope)
    return parsed.installation_id, parsed.application_id


class InstallationSearchScope:
    """Resolve and bind the installation partition for projection statements."""

    def __init__(self, pool: PostgresPool, catalog_scope: str | None) -> None:
        self._pool = pool
        self._catalog = parse_catalog_scope(catalog_scope) if catalog_scope else None

    def installation(self, scope: str) -> tuple[UUID, str]:
        named = installation_of(scope)
        if self._catalog is not None and named is not None and named != self._catalog:
            raise ValueError("capability search scope crosses the configured installation")
        resolved = self._catalog or named
        if resolved is None:
            raise ValueError("capability search requires an installation-bound scope")
        return resolved

    @asynccontextmanager
    async def session(self, scope: str) -> AsyncIterator[tuple[PostgresConnection, UUID, str]]:
        installation_id, application_id = self.installation(scope)
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_catalog_scope(
                cast(asyncpg.Connection, connection), installation_id, application_id
            )
            yield connection, installation_id, application_id


class PostgresCatalogSearchRepository:
    """Supabase/PostgreSQL projection adapter; the injected pool owns connectivity."""

    def __init__(self, pool: PostgresPool, *, catalog_scope: str | None = None) -> None:
        self._pool = pool
        self._scope = InstallationSearchScope(pool, catalog_scope)

    async def get(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
        logical_id: str,
        revision: int,
        *,
        projection_generation: str | None = None,
    ) -> CapabilitySearchDocument | None:
        async with self._scope.session(tenant_scope) as (connection, installation, app):
            if projection_generation is None:
                row = await connection.fetchrow(
                    """
                    SELECT documents.*
                    FROM mission_control_search.search_document AS documents
                    JOIN mission_control_search.active_generation AS active
                      ON active.installation_id = documents.installation_id
                     AND active.application_id = documents.application_id
                     AND active.tenant_scope = documents.tenant_scope
                     AND active.asset_kind = documents.asset_kind
                     AND active.projection_generation = documents.projection_generation
                    WHERE documents.installation_id = $5
                      AND documents.application_id = $6
                      AND documents.tenant_scope = $1
                      AND documents.asset_kind = $2
                      AND documents.logical_id = $3
                      AND documents.revision = $4
                    """,
                    tenant_scope,
                    kind.value,
                    logical_id,
                    revision,
                    installation,
                    app,
                )
            else:
                row = await connection.fetchrow(
                    """
                    SELECT *
                    FROM mission_control_search.search_document
                    WHERE installation_id = $6
                      AND application_id = $7
                      AND tenant_scope = $1
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
                    installation,
                    app,
                )
        return _document(row) if row is not None else None

    async def upsert(self, document: CapabilitySearchDocument) -> bool:
        parent = document.parent_ref
        async with self._scope.session(document.tenant_scope) as (connection, installation, app):
            generation = await connection.fetchrow(
                """
                SELECT state
                FROM mission_control_search.projection_generation
                WHERE installation_id = $6
                  AND application_id = $7
                  AND tenant_scope = $1
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
                installation,
                app,
            )
            if generation is None:
                raise RuntimeError("capability projection generation is missing or not writable")
            row = await connection.fetchrow(
                """
                INSERT INTO mission_control_search.search_document AS documents (
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
                    projection_generation,
                    installation_id,
                    application_id,
                    embedding_model,
                    embedding_dims,
                    host_profiles,
                    side_effect_class,
                    aliases,
                    tool_names
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    $12::extensions.vector, $13, $14, $15, $16, $17, $18,
                    $19, $20, $21, $22, $23, $24, $25::jsonb, $26, $27,
                    $28, $29, $30, $31, $32, $33, $34,
                    $35, $36, $37::text[], $38, $39::text[], $40::text[]
                )
                ON CONFLICT (
                    installation_id,
                    application_id,
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
                    embedding_model = EXCLUDED.embedding_model,
                    embedding_dims = EXCLUDED.embedding_dims,
                    host_profiles = EXCLUDED.host_profiles,
                    side_effect_class = EXCLUDED.side_effect_class,
                    aliases = EXCLUDED.aliases,
                    tool_names = EXCLUDED.tool_names
                WHERE (documents.search_document_id, documents.source_digest, documents.status,
                       documents.title, documents.description, documents.search_text,
                       documents.search_text_digest, documents.embedding::text,
                       documents.embedding_model_id, documents.embedding_dimensions,
                       documents.search_document_format_version, documents.parent_kind,
                       documents.parent_logical_id, documents.parent_revision,
                       documents.parent_source_digest, documents.mongodb_collection,
                       documents.mongodb_document_id, documents.tags, documents.domains,
                       documents.operation_classes, documents.workflow_type_refs,
                       documents.capability_requirements, documents.compatible_runtimes,
                       documents.compatibility_summary, documents.schema_digest_verified,
                       documents.source_published_at, documents.indexed_at,
                       documents.embedding_model, documents.embedding_dims,
                       documents.host_profiles, documents.side_effect_class,
                       documents.aliases, documents.tool_names)
                      IS DISTINCT FROM
                      (EXCLUDED.search_document_id, EXCLUDED.source_digest, EXCLUDED.status,
                       EXCLUDED.title, EXCLUDED.description, EXCLUDED.search_text,
                       EXCLUDED.search_text_digest, EXCLUDED.embedding::text,
                       EXCLUDED.embedding_model_id, EXCLUDED.embedding_dimensions,
                       EXCLUDED.search_document_format_version, EXCLUDED.parent_kind,
                       EXCLUDED.parent_logical_id, EXCLUDED.parent_revision,
                       EXCLUDED.parent_source_digest, EXCLUDED.mongodb_collection,
                       EXCLUDED.mongodb_document_id, EXCLUDED.tags, EXCLUDED.domains,
                       EXCLUDED.operation_classes, EXCLUDED.workflow_type_refs,
                       EXCLUDED.capability_requirements, EXCLUDED.compatible_runtimes,
                       EXCLUDED.compatibility_summary, EXCLUDED.schema_digest_verified,
                       EXCLUDED.source_published_at, EXCLUDED.indexed_at,
                       EXCLUDED.embedding_model, EXCLUDED.embedding_dims,
                       EXCLUDED.host_profiles, EXCLUDED.side_effect_class,
                       EXCLUDED.aliases, EXCLUDED.tool_names)
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
                None if document.embedding is None else _vector_literal(document.embedding),
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
                installation,
                app,
                document.embedding_model,
                document.embedding_dims,
                sorted(document.host_profiles),
                document.side_effect_class,
                sorted(document.aliases),
                sorted(document.tool_names),
            )
        return row is not None

    async def list_generation(
        self,
        tenant_scope: str,
        projection_generation: str,
        *,
        kinds: frozenset[DefinitionKind] = frozenset(),
    ) -> tuple[CapabilitySearchDocument, ...]:
        async with self._scope.session(tenant_scope) as (connection, installation, app):
            rows = await connection.fetch(
                """
                SELECT *
                FROM mission_control_search.search_document
                WHERE installation_id = $4
                  AND application_id = $5
                  AND tenant_scope = $1
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
                installation,
                app,
            )
        return tuple(_document(row) for row in rows)

    async def lexical_search(
        self,
        request: CapabilitySearchRequest,
        *,
        limit: int,
    ) -> tuple[RankedCapabilityDocument, ...]:
        installation = self._scope.installation(request.tenant_scope)
        query, args = _filtered_query(
            request,
            score_sql=("ts_rank_cd(fts, websearch_to_tsquery('english', $1), 32)"),
            match_sql="fts @@ websearch_to_tsquery('english', $1)",
            order_sql="branch_score DESC, logical_id, revision",
            tail_args=(request.query, limit),
            installation=installation,
        )
        async with self._scope.session(request.tenant_scope) as (connection, _, _app):
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
        installation = self._scope.installation(request.tenant_scope)
        query, args = _filtered_query(
            request,
            score_sql=("1 - (embedding OPERATOR(extensions.<=>) $1::extensions.vector)"),
            match_sql="documents.embedding IS NOT NULL",
            order_sql=(
                "embedding OPERATOR(extensions.<=>) $1::extensions.vector, logical_id, revision"
            ),
            tail_args=(_vector_literal(query_embedding), limit),
            installation=installation,
        )
        async with self._scope.session(request.tenant_scope) as (connection, _, _app):
            rows = await connection.fetch(query, *args)
        return tuple(
            RankedCapabilityDocument(
                document=_document(row),
                branch_score=float(row["branch_score"]),
            )
            for row in rows
        )

    async def trigram_search(
        self,
        request: CapabilitySearchRequest,
        *,
        limit: int,
    ) -> tuple[RankedCapabilityDocument, ...]:
        """Near-exact names: pg_trgm word similarity of the query against ``name_surface``."""
        installation = self._scope.installation(request.tenant_scope)
        query, args = _filtered_query(
            request,
            score_sql="extensions.word_similarity(lower($1), documents.name_surface)",
            match_sql=(
                f"extensions.word_similarity(lower($1), documents.name_surface) "
                f">= {TRIGRAM_THRESHOLD}"
            ),
            order_sql="branch_score DESC, logical_id, revision",
            tail_args=(" ".join(request.query.split()), limit),
            installation=installation,
        )
        async with self._scope.session(request.tenant_scope) as (connection, _, _app):
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
    installation: tuple[UUID, str] | None = None,
) -> tuple[str, tuple[object, ...]]:
    # $1 is branch-specific input, $9 the branch limit, $10/$11 the installation,
    # $12 the published-definition asset contract used by the admission filter, $13 the
    # required (supported) lane profiles and $14 the allowed side-effect classes.
    workflow_ref = (
        json.dumps(request.workflow_type_ref.model_dump(mode="json"))
        if request.workflow_type_ref is not None
        else None
    )
    query = f"""
        SELECT documents.*, {score_sql} AS branch_score
        FROM mission_control_search.search_document AS documents
        JOIN mission_control_search.active_generation AS active
          ON active.installation_id = documents.installation_id
         AND active.application_id = documents.application_id
         AND active.tenant_scope = documents.tenant_scope
         AND active.asset_kind = documents.asset_kind
         AND active.projection_generation = documents.projection_generation
        WHERE ({match_sql})
          AND documents.installation_id = $10
          AND documents.application_id = $11
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
          AND documents.host_profiles @> $13::text[]
          AND (
              cardinality($14::text[]) = 0
              OR documents.side_effect_class = ANY($14::text[])
          )
          AND EXISTS (
              SELECT 1
              FROM mission_control.asset_version AS asset
              WHERE asset.installation_id = documents.installation_id
                AND asset.application_id = documents.application_id
                AND asset.asset_id =
                    'definition:' || documents.asset_kind || ':' || documents.logical_id
                AND asset.version = documents.revision::text
                AND asset.contract = $12
                AND asset.manifest->'ref'->>'digest' = documents.source_digest
                AND (
                    asset.status = 'admitted'
                    OR (asset.status = 'retired' AND 'retired' = ANY($4::text[]))
                )
          )
        ORDER BY {order_sql}
        LIMIT $9
    """
    installation_id, application_id = installation or (None, None)
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
        installation_id,
        application_id,
        PUBLISHED_DEFINITION_CONTRACT,
        sorted(profile.value for profile in request.host_profiles),
        sorted(request.side_effect_classes),
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
    vector = None if embedding is None else tuple(float(item) for item in embedding)
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
        embedding=vector,
        embedding_model=row.get("embedding_model"),
        embedding_dims=row.get("embedding_dims"),
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
        host_profiles=frozenset(row.get("host_profiles") or ()),
        side_effect_class=row.get("side_effect_class"),
        aliases=frozenset(row.get("aliases") or ()),
        tool_names=frozenset(row.get("tool_names") or ()),
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
