from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.application.artifacts.artifact_promotion import artifact_durable_reference
from mission_control.contracts.identities import parse_request_scope, uuid7
from mission_control.domain.authoring.identity import stable_id
from mission_control.domain.execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
)
from mission_control.domain.policies.errors import IdempotencyConflict, RunControlNotFound

FailureHook = Callable[[str], Awaitable[None] | None]

ARTIFACT_DESTINATION = "artifact_reference"
_SERVICE_ACTOR = "service:mission-control-artifacts"


class PostgresArtifactDurableReferenceRepository:
    """Atomically registers one content-addressed artifact and its relayable outbox event.

    The durable reference is a canonical ``mission_control.artifact`` row (registered
    once per artifact key, promotion and durable reference) and its ``artifact.admitted``
    event a canonical ``mission_control.outbox`` row, committed in one transaction under
    the tenant request scope. Replays return the existing reference without new rows.
    """

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
        scope = parse_request_scope(request_scope)
        key = (scope.installation_id, scope.application_id, scope.tenant_id)
        event_id = stable_id("artifact-admitted-event", artifact.artifact_id)
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
            await apply_scope(connection, scope)
            await connection.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                f"artifact:{request_scope}:{artifact.artifact_id}",
            )
            run_uuid = await connection.fetchval(
                """SELECT run_id FROM mission_control.mission_run
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND run_key=$4""",
                *key,
                run_id,
            )
            if run_uuid is None:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
            prior = await connection.fetchrow(
                """SELECT detail, content_digest, object_key, producer_run_id
                   FROM mission_control.artifact
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND artifact_key=$4""",
                *key,
                artifact.artifact_id,
            )
            if prior is not None:
                detail = _json(prior["detail"])
                observed = (
                    detail.get("promotion_id"),
                    detail.get("metadata_revision"),
                    detail.get("manifest_revision"),
                    prior["content_digest"],
                    prior["object_key"],
                    detail.get("durable_reference"),
                    prior["producer_run_id"],
                )
                expected = (
                    artifact.promotion_id,
                    artifact.revision,
                    artifact.manifest_revision,
                    artifact.content_digest,
                    artifact.object_ref,
                    durable_reference,
                    run_uuid,
                )
                if observed != expected:
                    raise IdempotencyConflict("durable artifact reference conflict")
                return str(detail["durable_reference"])
            detail_value = {
                "promotion_id": artifact.promotion_id,
                "metadata_revision": artifact.revision,
                "manifest_revision": artifact.manifest_revision,
                "durable_reference": durable_reference,
                "run_key": run_id,
                "candidate_id": artifact.candidate_id,
                "semantic_attempt_key": artifact.semantic_attempt_key,
                "producer_binding_id": artifact.producer_binding_id,
                "output_slot": artifact.output_slot,
                "logical_path": artifact.logical_path,
            }
            try:
                await connection.execute(
                    """INSERT INTO mission_control.artifact
                       (installation_id, application_id, tenant_id, artifact_id, artifact_key,
                        producer_run_id, kind, schema_ref, media_type, content_digest,
                        byte_size, storage_kind, object_key, artifact_version, custody_state,
                        detail, updated_at, created_at, created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,$6,'workspace_output',$7,$8,$9,$10,
                               'object_store',$11,1,'registered',$12::jsonb,$13,$13,$14)""",
                    *key,
                    uuid7(),
                    artifact.artifact_id,
                    run_uuid,
                    artifact.output_contract_ref,
                    artifact.media_type,
                    artifact.content_digest,
                    artifact.size_bytes,
                    artifact.object_ref,
                    json.dumps(detail_value),
                    recorded_at,
                    _SERVICE_ACTOR,
                )
            except asyncpg.UniqueViolationError as error:
                raise IdempotencyConflict("durable artifact reference conflict") from error
            await connection.execute(
                """INSERT INTO mission_control.outbox
                   (installation_id, application_id, tenant_id, outbox_id, delivery_key,
                    destination_kind, event_type, aggregate_key, aggregate_version,
                    aggregate_sequence, payload, delivery_state, attempts, next_attempt_at,
                    version, created_at, created_by_actor_ref)
                   VALUES ($1,$2,$3,$4,$5,$6,'artifact.admitted',$7,1,1,$8::jsonb,'pending',0,
                           $9,1,$9,$10)""",
                *key,
                uuid7(),
                event_id,
                ARTIFACT_DESTINATION,
                f"artifact:{artifact.artifact_id}",
                json.dumps(envelope),
                recorded_at,
                _SERVICE_ACTOR,
            )
            if self._before_commit is not None:
                result = self._before_commit("artifact_admission")
                if inspect.isawaitable(result):
                    await result
        return durable_reference

    async def get(self, request_scope: str, artifact_id: str) -> str | None:
        scope = parse_request_scope(request_scope)
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            value = await connection.fetchval(
                """SELECT detail->>'durable_reference' FROM mission_control.artifact
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND artifact_key=$4""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                artifact_id,
            )
        return str(value) if value is not None else None

    async def pending_events(
        self, request_scope: str, *, limit: int = 100
    ) -> tuple[dict[str, Any], ...]:
        scope = parse_request_scope(request_scope)
        async with self._pool.acquire() as connection, connection.transaction():
            await apply_scope(connection, scope)
            rows = await connection.fetch(
                """SELECT payload FROM mission_control.outbox
                   WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                     AND destination_kind=$4 AND delivered_at IS NULL
                   ORDER BY global_position
                   LIMIT $5""",
                scope.installation_id,
                scope.application_id,
                scope.tenant_id,
                ARTIFACT_DESTINATION,
                limit,
            )
        return tuple(_json(row["payload"]) for row in rows)


def _json(value: Any) -> dict[str, Any]:
    parsed = json.loads(value) if isinstance(value, str) else value
    return dict(parsed)
