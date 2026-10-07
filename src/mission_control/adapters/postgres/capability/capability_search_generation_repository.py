from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any

from mission_control.adapters.postgres.capability.capability_search_repository import (
    InstallationSearchScope,
    PostgresPool,
)
from mission_control.application.capabilities.catalog_projection_generation import (
    ProjectionGenerationActivation,
    ProjectionGenerationRecord,
    ProjectionGenerationRepository,
    ProjectionGenerationSpec,
)
from mission_control.domain.authoring.contracts import DefinitionKind


class PostgresProjectionGenerationRepository(ProjectionGenerationRepository):
    """Generations and active pointers in ``mission_control_search`` (installation scoped)."""

    def __init__(self, pool: PostgresPool, *, catalog_scope: str | None = None) -> None:
        self._pool = pool
        self._scope = InstallationSearchScope(pool, catalog_scope)

    async def begin(
        self,
        spec: ProjectionGenerationSpec,
        *,
        created_at: datetime,
    ) -> ProjectionGenerationRecord:
        async with self._scope.session(spec.tenant_scope) as (connection, installation, app):
            row = await connection.fetchrow(
                """
                INSERT INTO mission_control_search.projection_generation (
                    installation_id,
                    application_id,
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
                VALUES ($10, $11, $1, $2, $3, $4, $5, $6, $7, $8, 'building', $9)
                ON CONFLICT (installation_id, application_id, tenant_scope, projection_generation)
                DO NOTHING
                RETURNING *
                """,
                spec.tenant_scope,
                spec.projection_generation,
                spec.embedding_model_id,
                spec.embedding_dimensions,
                spec.search_document_format_version,
                [kind.value for kind in sorted(spec.selected_kinds, key=lambda item: item.value)],
                spec.expected_count,
                spec.expected_source_set_digest,
                created_at,
                installation,
                app,
            )
            if row is None:
                row = await connection.fetchrow(
                    """
                    SELECT *
                    FROM mission_control_search.projection_generation
                    WHERE installation_id = $3
                      AND application_id = $4
                      AND tenant_scope = $1
                      AND projection_generation = $2
                    """,
                    spec.tenant_scope,
                    spec.projection_generation,
                    installation,
                    app,
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
        async with self._scope.session(tenant_scope) as (connection, installation, app):
            row = await connection.fetchrow(
                """
                SELECT *
                FROM mission_control_search.projection_generation
                WHERE installation_id = $3
                  AND application_id = $4
                  AND tenant_scope = $1
                  AND projection_generation = $2
                """,
                tenant_scope,
                projection_generation,
                installation,
                app,
            )
        return _record(row) if row is not None else None

    async def active_for_kind(
        self,
        tenant_scope: str,
        kind: DefinitionKind,
    ) -> str | None:
        async with self._scope.session(tenant_scope) as (connection, installation, app):
            row = await connection.fetchrow(
                """
                SELECT projection_generation
                FROM mission_control_search.active_generation
                WHERE installation_id = $3
                  AND application_id = $4
                  AND tenant_scope = $1
                  AND asset_kind = $2
                """,
                tenant_scope,
                kind.value,
                installation,
                app,
            )
        return str(row["projection_generation"]) if row is not None else None

    async def activate(
        self,
        spec: ProjectionGenerationSpec,
        *,
        activated_at: datetime,
    ) -> ProjectionGenerationActivation:
        del activated_at  # PostgreSQL provides the authoritative commit timestamp.
        async with self._scope.session(spec.tenant_scope) as (connection, installation, app):
            row = await connection.fetchrow(
                """
                SELECT activated_count, activated_source_set_digest
                FROM mission_control_search.activate_generation($1, $2, $3, $4)
                """,
                spec.tenant_scope,
                spec.projection_generation,
                spec.expected_count,
                spec.expected_source_set_digest,
            )
            record = await connection.fetchrow(
                """
                SELECT activated_at
                FROM mission_control_search.projection_generation
                WHERE installation_id = $3
                  AND application_id = $4
                  AND tenant_scope = $1
                  AND projection_generation = $2
                """,
                spec.tenant_scope,
                spec.projection_generation,
                installation,
                app,
            )
        if row is None or record is None:
            raise RuntimeError("projection generation activation returned no evidence")
        return ProjectionGenerationActivation(
            tenant_scope=spec.tenant_scope,
            projection_generation=spec.projection_generation,
            activated_count=int(row["activated_count"]),
            activated_source_set_digest=str(row["activated_source_set_digest"]),
            activated_at=record["activated_at"],
        )

    async def mark_failed(
        self,
        tenant_scope: str,
        projection_generation: str,
    ) -> None:
        async with self._scope.session(tenant_scope) as (connection, installation, app):
            await connection.fetchrow(
                """
                UPDATE mission_control_search.projection_generation
                SET state = 'failed'
                WHERE installation_id = $3
                  AND application_id = $4
                  AND tenant_scope = $1
                  AND projection_generation = $2
                  AND state = 'building'
                RETURNING projection_generation
                """,
                tenant_scope,
                projection_generation,
                installation,
                app,
            )


def _record(row: Mapping[str, Any]) -> ProjectionGenerationRecord:
    return ProjectionGenerationRecord(
        tenant_scope=str(row["tenant_scope"]),
        projection_generation=str(row["projection_generation"]),
        embedding_model_id=str(row["embedding_model_id"]),
        embedding_dimensions=int(row["embedding_dimensions"]),
        search_document_format_version=int(row["search_document_format_version"]),
        selected_kinds=frozenset(DefinitionKind(str(kind)) for kind in row["selected_kinds"]),
        expected_count=int(row["expected_count"]),
        expected_source_set_digest=str(row["expected_source_set_digest"]),
        actual_count=(int(row["actual_count"]) if row["actual_count"] is not None else None),
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
