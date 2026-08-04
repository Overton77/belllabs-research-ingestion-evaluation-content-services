from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from app.application.catalog_projection_generation import (
    ProjectionGenerationActivation,
    ProjectionGenerationRecord,
    ProjectionGenerationRepository,
    ProjectionGenerationSpec,
)
from app.application.postgres_capability_search_repository import PostgresPool
from app.domain.control_plane.contracts import DefinitionKind


class PostgresProjectionGenerationRepository(ProjectionGenerationRepository):
    def __init__(self, pool: PostgresPool) -> None:
        self._pool = pool

    async def begin(
        self,
        spec: ProjectionGenerationSpec,
        *,
        created_at: datetime,
    ) -> ProjectionGenerationRecord:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                INSERT INTO capability_search.generations (
                    tenant_scope,
                    projection_generation,
                    embedding_model_id,
                    embedding_dimensions,
                    search_document_format_version,
                    selected_kinds,
                    expected_count,
                    expected_source_set_digest,
                    state,
                    created_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'building', $9)
                ON CONFLICT (tenant_scope, projection_generation) DO NOTHING
                RETURNING *
                """,
                spec.tenant_scope,
                spec.projection_generation,
                spec.embedding_model_id,
                spec.embedding_dimensions,
                spec.search_document_format_version,
                [
                    kind.value
                    for kind in sorted(
                        spec.selected_kinds,
                        key=lambda item: item.value,
                    )
                ],
                spec.expected_count,
                spec.expected_source_set_digest,
                created_at,
            )
            if row is None:
                row = await connection.fetchrow(
                    """
                    SELECT *
                    FROM capability_search.generations
                    WHERE tenant_scope = $1
                      AND projection_generation = $2
                    """,
                    spec.tenant_scope,
                    spec.projection_generation,
                )
        if row is None:
            raise RuntimeError("projection generation could not be created")
        record = _record(row)
        if _spec(record) != spec or record.state not in {"building", "active"}:
            raise RuntimeError("projection generation identity is already in use")
        return record

    async def get(
        self,
        tenant_scope: str,
        projection_generation: str,
    ) -> ProjectionGenerationRecord | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT *
                FROM capability_search.generations
                WHERE tenant_scope = $1
                  AND projection_generation = $2
                """,
                tenant_scope,
                projection_generation,
            )
        return _record(row) if row is not None else None

    async def active_for_kind(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
    ) -> str | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT projection_generation
                FROM capability_search.active_generations
                WHERE tenant_scope = $1
                  AND asset_kind = $2
                """,
                tenant_scope,
                kind.value,
            )
        return str(row["projection_generation"]) if row is not None else None

    async def activate(
        self,
        spec: ProjectionGenerationSpec,
        *,
        activated_at: datetime,
    ) -> ProjectionGenerationActivation:
        del activated_at  # PostgreSQL provides the authoritative commit timestamp.
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                """
                SELECT activated_count, activated_source_set_digest
                FROM capability_search.activate_generation($1, $2, $3, $4)
                """,
                spec.tenant_scope,
                spec.projection_generation,
                spec.expected_count,
                spec.expected_source_set_digest,
            )
            record = await connection.fetchrow(
                """
                SELECT activated_at
                FROM capability_search.generations
                WHERE tenant_scope = $1
                  AND projection_generation = $2
                """,
                spec.tenant_scope,
                spec.projection_generation,
            )
        if row is None or record is None:
            raise RuntimeError("projection generation activation returned no evidence")
        return ProjectionGenerationActivation(
            tenant_scope=spec.tenant_scope,
            projection_generation=spec.projection_generation,
            activated_count=int(row["activated_count"]),
            activated_source_set_digest=str(
                row["activated_source_set_digest"]
            ),
            activated_at=record["activated_at"],
        )

    async def mark_failed(
        self,
        tenant_scope: str,
        projection_generation: str,
    ) -> None:
        async with self._pool.acquire() as connection:
            await connection.fetchrow(
                """
                UPDATE capability_search.generations
                SET state = 'failed'
                WHERE tenant_scope = $1
                  AND projection_generation = $2
                  AND state = 'building'
                RETURNING projection_generation
                """,
                tenant_scope,
                projection_generation,
            )


def _record(row: Mapping[str, Any]) -> ProjectionGenerationRecord:
    return ProjectionGenerationRecord(
        tenant_scope=str(row["tenant_scope"]),
        projection_generation=str(row["projection_generation"]),
        embedding_model_id=str(row["embedding_model_id"]),
        embedding_dimensions=int(row["embedding_dimensions"]),
        search_document_format_version=int(row["search_document_format_version"]),
        selected_kinds=frozenset(
            DefinitionKind(str(kind)) for kind in row["selected_kinds"]
        ),
        expected_count=int(row["expected_count"]),
        expected_source_set_digest=str(row["expected_source_set_digest"]),
        actual_count=(
            int(row["actual_count"]) if row["actual_count"] is not None else None
        ),
        actual_source_set_digest=(
            str(row["actual_source_set_digest"])
            if row["actual_source_set_digest"] is not None
            else None
        ),
        state=str(row["state"]),
        created_at=row["created_at"],
        verified_at=row["verified_at"],
        activated_at=row["activated_at"],
    )


def _spec(record: ProjectionGenerationRecord) -> ProjectionGenerationSpec:
    return ProjectionGenerationSpec.model_validate(
        record.model_dump(
            exclude={
                "state",
                "actual_count",
                "actual_source_set_digest",
                "created_at",
                "verified_at",
                "activated_at",
            }
        )
    )
