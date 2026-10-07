from __future__ import annotations

import asyncpg

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.contracts.identities import parse_request_scope, uuid7
from mission_control.domain.execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
)
from mission_control.domain.policies.errors import IdempotencyConflict

_SERVICE_ACTOR = "service:mission-control-artifacts"


class PostgresArtifactMetadataRepository:
    """Immutable promotion history, serialized per artifact and intent within a scope.

    Rows are ``mission_control.artifact_metadata_revision`` support records under the
    tenant request scope (three-column RLS); history is append-only.
    """

    def __init__(self, pool: asyncpg.Pool, *, request_scope: str) -> None:
        if not request_scope.strip():
            raise ValueError("artifact repository requires request scope")
        self._pool = pool
        self._scope = request_scope
        parsed = parse_request_scope(request_scope)
        self._key = (parsed.installation_id, parsed.application_id, parsed.tenant_id)

    async def _scope_connection(self, connection: asyncpg.Connection) -> None:
        await apply_scope(connection, self._scope)

    async def _get(
        self, *, intent_key: str | None = None, artifact_id: str | None = None
    ) -> ArtifactMetadataRevision | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await self._scope_connection(connection)
            payload = await connection.fetchval(
                """SELECT payload FROM mission_control.artifact_metadata_revision
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND (($4::text IS NOT NULL AND intent_key=$4)
                       OR ($5::text IS NOT NULL AND artifact_key=$5))
                   ORDER BY revision DESC LIMIT 1""",
                *self._key,
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
                    """SELECT DISTINCT ON (artifact_key) payload
                       FROM mission_control.artifact_metadata_revision
                       WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                         AND (intent_key=$4 OR artifact_key=$5 OR promotion_key=$6)
                       ORDER BY artifact_key, revision DESC""",
                    *self._key,
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
                        """SELECT payload FROM mission_control.artifact_metadata_revision
                           WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                             AND artifact_key=$4 AND revision=$5""",
                        *self._key,
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
                    """INSERT INTO mission_control.artifact_metadata_revision
                       (installation_id, application_id, tenant_id,
                        artifact_metadata_revision_id, request_scope, artifact_key, intent_key,
                        promotion_key, revision, state, payload, recorded_at, created_at,
                        created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,
                               clock_timestamp(),$13)""",
                    *self._key,
                    uuid7(),
                    self._scope,
                    revision.artifact_id,
                    revision.intent_key,
                    revision.promotion_id,
                    revision.revision,
                    revision.state.value,
                    revision.model_dump_json(),
                    revision.recorded_at,
                    _SERVICE_ACTOR,
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
                     SELECT DISTINCT ON (artifact_key) artifact_key, state, payload
                     FROM mission_control.artifact_metadata_revision
                     WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     ORDER BY artifact_key, revision DESC
                   ) latest WHERE state=$4 ORDER BY artifact_key""",
                *self._key,
                state.value,
            )
        return tuple(ArtifactMetadataRevision.model_validate_json(row["payload"]) for row in rows)

    async def reconciliation_required(self) -> tuple[ArtifactMetadataRevision, ...]:
        return await self._in_state(ArtifactPromotionState.RECONCILIATION_REQUIRED)

    async def rejected(self) -> tuple[ArtifactMetadataRevision, ...]:
        return await self._in_state(ArtifactPromotionState.REJECTED)
