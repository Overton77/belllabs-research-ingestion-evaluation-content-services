from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import asyncpg

from app.application.artifact_promotion import artifact_durable_reference
from app.domain.operation_execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
)
from app.domain.run_control.errors import IdempotencyConflict, RunControlNotFound

FailureHook = Callable[[str], Awaitable[None] | None]


class PostgresArtifactDurableReferenceRepository:
    """Atomically admits one durable reference and its relayable event."""

    def __init__(self, pool: asyncpg.Pool, *, before_commit: FailureHook | None = None) -> None:
        self._pool = pool
        self._before_commit = before_commit

    async def admit(
        self,
        *,
        request_scope: str,
        run_id: str,
        artifact: ArtifactMetadataRevision,
    ) -> str:
        if (
            artifact.state != ArtifactPromotionState.ADMITTED
            or artifact.object_ref is None
            or artifact.manifest_revision is None
        ):
            raise ValueError("PostgreSQL admission requires the exact admitted metadata revision")
        durable_reference = artifact_durable_reference(request_scope, run_id, artifact.artifact_id)
        if artifact.durable_reference != durable_reference:
            raise ValueError("admitted metadata carries a conflicting durable reference")
        event_id = _stable_id("artifact-admitted-event", artifact.artifact_id)
        recorded_at = datetime.now(UTC)
        envelope: dict[str, object] = {
            "schema_version": "1",
            "event_id": event_id,
            "event_type": "artifact.admitted",
            "aggregate_type": "artifact",
            "aggregate_id": artifact.artifact_id,
            "occurred_at": artifact.recorded_at.isoformat(),
            "recorded_at": recorded_at.isoformat(),
            "correlation_id": f"operation:{artifact.semantic_attempt_key}",
            "causation_id": artifact.producer_binding_id,
            "payload": {
                "artifact_id": artifact.artifact_id,
                "run_id": run_id,
                "promotion_id": artifact.promotion_id,
                "metadata_revision": artifact.revision,
                "manifest_revision": artifact.manifest_revision,
                "content_digest": artifact.content_digest,
                "object_ref": artifact.object_ref,
                "durable_reference": durable_reference,
            },
        }
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"artifact:{artifact.artifact_id}",
            )
            run_exists = await connection.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM belllabs_control.workflow_runs
                    WHERE run_id = $1 AND request_scope = $2
                )
                """,
                run_id,
                request_scope,
            )
            if not run_exists:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
            prior = await connection.fetchrow(
                """
                SELECT promotion_id, metadata_revision, manifest_revision,
                       content_digest, object_ref, durable_reference
                FROM belllabs_control.durable_artifact_references
                WHERE artifact_id = $1
                """,
                artifact.artifact_id,
            )
            if prior is not None:
                expected = (
                    artifact.promotion_id,
                    artifact.revision,
                    artifact.manifest_revision,
                    artifact.content_digest,
                    artifact.object_ref,
                    durable_reference,
                )
                observed = tuple(prior)
                if observed != expected:
                    raise IdempotencyConflict("durable artifact reference conflict")
                return str(prior["durable_reference"])
            await connection.execute(
                """
                INSERT INTO belllabs_control.durable_artifact_references
                    (artifact_id, request_scope, run_id, promotion_id,
                     metadata_revision, manifest_revision, content_digest,
                     object_ref, durable_reference, admitted_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                """,
                artifact.artifact_id,
                request_scope,
                run_id,
                artifact.promotion_id,
                artifact.revision,
                artifact.manifest_revision,
                artifact.content_digest,
                artifact.object_ref,
                durable_reference,
                recorded_at,
            )
            await connection.execute(
                """
                INSERT INTO belllabs_control.artifact_reference_outbox
                    (event_id, request_scope, run_id, artifact_id,
                     event_type, envelope, recorded_at)
                VALUES ($1, $2, $3, $4, 'artifact.admitted', $5::jsonb, $6)
                """,
                event_id,
                request_scope,
                run_id,
                artifact.artifact_id,
                json.dumps(envelope),
                recorded_at,
            )
            if self._before_commit is not None:
                result = self._before_commit("artifact_admission")
                if inspect.isawaitable(result):
                    await result
        return durable_reference

    async def get(self, request_scope: str, artifact_id: str) -> str | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            value = await connection.fetchval(
                """
                SELECT durable_reference
                FROM belllabs_control.durable_artifact_references
                WHERE artifact_id = $1
                """,
                artifact_id,
            )
        return str(value) if value is not None else None

    async def pending_events(
        self, request_scope: str, *, limit: int = 100
    ) -> tuple[dict[str, Any], ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT envelope
                FROM belllabs_control.artifact_reference_outbox
                WHERE delivered_at IS NULL
                ORDER BY recorded_at, event_id
                LIMIT $1
                """,
                limit,
            )
        return tuple(_json(row["envelope"]) for row in rows)


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


def _json(value: Any) -> dict[str, Any]:
    parsed = json.loads(value) if isinstance(value, str) else value
    return dict(parsed)


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))
