from __future__ import annotations

import asyncpg

from mission_control.domain.execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
)
from mission_control.domain.policies.errors import IdempotencyConflict


class PostgresArtifactMetadataRepository:
    """Immutable promotion history, serialized per artifact and intent within a scope."""

    def __init__(self, pool: asyncpg.Pool, *, request_scope: str) -> None:
        if not request_scope.strip():
            raise ValueError("artifact repository requires request scope")
        self._pool = pool
        self._scope = request_scope

    async def _scope_connection(self, connection: asyncpg.Connection) -> None:
        await connection.execute(
            "SELECT set_config('belllabs.request_scope', $1, true)", self._scope
        )

    async def _get(
        self, *, intent_key: str | None = None, artifact_id: str | None = None
    ) -> ArtifactMetadataRevision | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await self._scope_connection(connection)
            payload = await connection.fetchval(
                """SELECT payload FROM belllabs_control.artifact_metadata_revisions
                   WHERE request_scope=$1 AND (($2::text IS NOT NULL AND intent_key=$2)
                     OR ($3::text IS NOT NULL AND artifact_id=$3))
                   ORDER BY revision DESC LIMIT 1""",
                self._scope,
                intent_key,
                artifact_id,
            )
        return ArtifactMetadataRevision.model_validate_json(payload) if payload else None

    async def get_by_intent(self, intent_key: str) -> ArtifactMetadataRevision | None:
        return await self._get(intent_key=intent_key)

    async def get_by_artifact(self, artifact_id: str) -> ArtifactMetadataRevision | None:
        return await self._get(artifact_id=artifact_id)

    async def append(self, revision: ArtifactMetadataRevision) -> ArtifactMetadataRevision:
        if revision.request_scope != self._scope:
            raise ValueError("artifact revision request scope does not match repository")
        try:
            async with self._pool.acquire() as connection, connection.transaction():
                await self._scope_connection(connection)
                for identity in sorted(
                    (
                        f"artifact:{revision.artifact_id}",
                        f"intent:{revision.intent_key}",
                        f"promotion:{revision.promotion_id}",
                    )
                ):
                    await connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                        f"artifact-metadata:{self._scope}:{identity}",
                    )
                rows = await connection.fetch(
                    """SELECT DISTINCT ON (artifact_id) payload
                       FROM belllabs_control.artifact_metadata_revisions
                       WHERE request_scope=$1 AND
                         (intent_key=$2 OR artifact_id=$3 OR promotion_id=$4)
                       ORDER BY artifact_id, revision DESC""",
                    self._scope,
                    revision.intent_key,
                    revision.artifact_id,
                    revision.promotion_id,
                )
                current = None
                for row in rows:
                    candidate = ArtifactMetadataRevision.model_validate_json(row["payload"])
                    if (candidate.artifact_id, candidate.intent_key, candidate.promotion_id) != (
                        revision.artifact_id,
                        revision.intent_key,
                        revision.promotion_id,
                    ):
                        raise IdempotencyConflict("artifact intent or promotion identity conflict")
                    current = candidate
                if current is not None and revision.revision <= current.revision:
                    matching = await connection.fetchval(
                        """SELECT payload FROM belllabs_control.artifact_metadata_revisions
                           WHERE request_scope=$1 AND artifact_id=$2 AND revision=$3""",
                        self._scope,
                        revision.artifact_id,
                        revision.revision,
                    )
                    if matching is not None:
                        prior = ArtifactMetadataRevision.model_validate_json(matching)
                        if prior == revision:
                            return prior
                    raise IdempotencyConflict("artifact metadata revision conflict")
                if revision.revision != (current.revision + 1 if current else 1):
                    raise IdempotencyConflict("artifact metadata revision gap")
                await connection.execute(
                    """INSERT INTO belllabs_control.artifact_metadata_revisions
                       (request_scope, artifact_id, intent_key, promotion_id, revision,
                        state, payload, recorded_at) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)""",
                    self._scope,
                    revision.artifact_id,
                    revision.intent_key,
                    revision.promotion_id,
                    revision.revision,
                    revision.state.value,
                    revision.model_dump_json(),
                    revision.recorded_at,
                )
                return revision
        except asyncpg.UniqueViolationError as error:
            raise IdempotencyConflict("artifact metadata identity collision") from error

    async def _in_state(
        self, state: ArtifactPromotionState
    ) -> tuple[ArtifactMetadataRevision, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await self._scope_connection(connection)
            rows = await connection.fetch(
                """SELECT payload FROM (
                     SELECT DISTINCT ON (artifact_id) artifact_id, state, payload
                     FROM belllabs_control.artifact_metadata_revisions
                     WHERE request_scope=$1 ORDER BY artifact_id, revision DESC
                   ) latest WHERE state=$2 ORDER BY artifact_id""",
                self._scope,
                state.value,
            )
        return tuple(ArtifactMetadataRevision.model_validate_json(row["payload"]) for row in rows)

    async def reconciliation_required(self) -> tuple[ArtifactMetadataRevision, ...]:
        return await self._in_state(ArtifactPromotionState.RECONCILIATION_REQUIRED)

    async def rejected(self) -> tuple[ArtifactMetadataRevision, ...]:
        return await self._in_state(ArtifactPromotionState.REJECTED)
