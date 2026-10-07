"""Leased, row-locked processing of immutable installation catalog events.

Events are immutable ``catalog-event/1`` catalog records; their processing state is a
``mission_control.catalog_projection_job`` row claimed with ``FOR UPDATE SKIP LOCKED``,
fenced on (lease owner, attempt count, unexpired lease) and poisoned with an immutable
alert. Every transaction binds the installation catalog scope first.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import asyncpg

from mission_control.adapters.postgres.control_plane.catalog_assets import (
    CATALOG_SERVICE_ACTOR,
    insert_projection_job,
    json_value,
)
from mission_control.adapters.postgres.scope import apply_catalog_scope, parse_catalog_scope
from mission_control.application.capabilities.catalog_projection_events import (
    CatalogProjectionEvent,
    ProjectionEventFailure,
    ProjectionEventState,
    bounded_projection_backoff,
    projection_alert,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.authoring.contracts import ExactDefinitionRef


class PostgresProjectionEventRepository:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        catalog_scope: str,
        actor_ref: str = CATALOG_SERVICE_ACTOR,
    ) -> None:
        if not catalog_scope:
            raise ValueError("trusted installation catalog scope required")
        self._installation_id, self._application_id = parse_catalog_scope(catalog_scope)
        self.pool = pool
        self.scope = catalog_scope
        self._actor = actor_ref

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as connection, connection.transaction():
            await apply_catalog_scope(connection, self._installation_id, self._application_id)
            yield connection

    async def seed(self, connection: asyncpg.Connection) -> None:
        """Idempotently enqueue a job for any verified catalog event that has none."""
        rows = await connection.fetch(
            """SELECT r.record_key, r.payload, r.payload_digest
               FROM mission_control.catalog_record AS r
               WHERE r.installation_id=$1 AND r.application_id=$2
                 AND r.contract='catalog-event/1'
                 AND NOT EXISTS (
                     SELECT 1 FROM mission_control.catalog_projection_job AS j
                     WHERE j.installation_id=r.installation_id
                       AND j.application_id=r.application_id
                       AND j.event_key=r.record_key)""",
            self._installation_id,
            self._application_id,
        )
        for row in rows:
            payload = json_value(row["payload"])
            if sha256_digest(payload) != row["payload_digest"]:
                raise ValueError("catalog outbox digest mismatch")
            event = CatalogProjectionEvent.model_validate(payload)
            if event.event_id != row["record_key"] or event.tenant_scope != self.scope:
                raise ValueError("catalog outbox identity mismatch")
            await insert_projection_job(
                connection,
                installation_id=self._installation_id,
                application_id=self._application_id,
                event=stable_json_dump(event),
                actor_ref=self._actor,
                recorded_at=event.created_at,
            )

    async def save(
        self, connection: asyncpg.Connection, event: CatalogProjectionEvent, *, now: datetime
    ) -> None:
        await connection.execute(
            """UPDATE mission_control.catalog_projection_job
               SET state=$4, attempt_count=$5, lease_owner=$6, lease_expires_at=$7,
                   next_attempt_at=$8, payload=$9::jsonb, version=version + 1, updated_at=$10
               WHERE installation_id=$1 AND application_id=$2 AND event_key=$3""",
            self._installation_id,
            self._application_id,
            event.event_id,
            event.state.value,
            event.attempt_count,
            event.lease_owner,
            event.lease_expires_at,
            event.next_attempt_at,
            json.dumps(stable_json_dump(event)),
            now,
        )

    async def claim_batch(
        self, *, owner: str, now: datetime, lease_duration: timedelta, limit: int
    ) -> tuple[CatalogProjectionEvent, ...]:
        if not owner or lease_duration <= timedelta(0) or limit < 1:
            raise ValueError("projection event claim configuration is invalid")
        async with self.transaction() as connection:
            await self.seed(connection)
            rows = await connection.fetch(
                """SELECT payload FROM mission_control.catalog_projection_job
                   WHERE installation_id=$1 AND application_id=$2
                     AND state IN ('pending','retry','processing')
                     AND next_attempt_at <= $3
                     AND (lease_expires_at IS NULL OR lease_expires_at <= $3)
                   ORDER BY next_attempt_at, created_at, event_key
                   LIMIT $4 FOR UPDATE SKIP LOCKED""",
                self._installation_id,
                self._application_id,
                now,
                limit,
            )
            claimed = []
            for row in rows:
                event = _event(row["payload"])
                event = event.model_copy(
                    update={
                        "state": ProjectionEventState.PROCESSING,
                        "attempt_count": event.attempt_count + 1,
                        "lease_owner": owner,
                        "lease_expires_at": now + lease_duration,
                    }
                )
                await self.save(connection, event, now=now)
                claimed.append(event)
            return tuple(claimed)

    async def fenced(
        self,
        connection: asyncpg.Connection,
        event: CatalogProjectionEvent,
        owner: str,
        now: datetime,
    ) -> CatalogProjectionEvent | None:
        if event.tenant_scope != self.scope:
            return None
        row = await connection.fetchrow(
            """SELECT payload FROM mission_control.catalog_projection_job
               WHERE installation_id=$1 AND application_id=$2 AND event_key=$3 FOR UPDATE""",
            self._installation_id,
            self._application_id,
            event.event_id,
        )
        if row is None:
            return None
        current = _event(row["payload"])
        if (
            current.state != ProjectionEventState.PROCESSING
            or current.lease_owner != owner
            or current.attempt_count != event.attempt_count
            or current.lease_expires_at is None
            or current.lease_expires_at <= now
        ):
            return None
        return current

    async def complete(
        self, event: CatalogProjectionEvent, *, owner: str, completed_at: datetime
    ) -> bool:
        async with self.transaction() as connection:
            current = await self.fenced(connection, event, owner, completed_at)
            if current is None:
                return False
            await self.save(connection, _completed(current, completed_at), now=completed_at)
            return True

    async def fail(
        self,
        event: CatalogProjectionEvent,
        *,
        owner: str,
        failed_at: datetime,
        failure: ProjectionEventFailure,
        max_attempts: int,
        base_backoff: timedelta,
        max_backoff: timedelta,
    ) -> CatalogProjectionEvent | None:
        async with self.transaction() as connection:
            current = await self.fenced(connection, event, owner, failed_at)
            if current is None:
                return None
            poison = not failure.retryable or current.attempt_count >= max_attempts
            delay = bounded_projection_backoff(
                current.attempt_count, base=base_backoff, maximum=max_backoff
            )
            updated = current.model_copy(
                update={
                    "state": ProjectionEventState.POISON if poison else ProjectionEventState.RETRY,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "next_attempt_at": failed_at if poison else failed_at + delay,
                    "last_error_code": failure.error_code,
                    "poison_reason": failure.error_code if poison else None,
                }
            )
            await self.save(connection, updated, now=failed_at)
            if poison:
                alert = projection_alert(updated, failure.error_code, failed_at)
                await connection.execute(
                    """INSERT INTO mission_control.catalog_projection_alert
                       (installation_id, application_id, projection_alert_id, alert_key,
                        event_key, error_code, payload, created_at, created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,clock_timestamp(),$8)
                       ON CONFLICT (installation_id, application_id, alert_key) DO NOTHING""",
                    self._installation_id,
                    self._application_id,
                    uuid7(),
                    alert.alert_id,
                    alert.event_id,
                    alert.error_code,
                    json.dumps(stable_json_dump(alert)),
                    self._actor,
                )
            return updated

    async def complete_for_ref(
        self, ref: ExactDefinitionRef, *, tenant_scope: str, completed_at: datetime
    ) -> int:
        if tenant_scope != self.scope:
            raise ValueError("projection scope differs from installation")
        async with self.transaction() as connection:
            await self.seed(connection)
            rows = await connection.fetch(
                """SELECT payload FROM mission_control.catalog_projection_job
                   WHERE installation_id=$1 AND application_id=$2 AND definition_kind=$3
                     AND logical_id=$4 AND revision=$5 AND source_digest=$6
                   FOR UPDATE""",
                self._installation_id,
                self._application_id,
                ref.kind.value,
                ref.logical_id,
                ref.revision,
                ref.digest,
            )
            count = 0
            for row in rows:
                event = _event(row["payload"])
                if event.state in {
                    ProjectionEventState.PENDING,
                    ProjectionEventState.RETRY,
                    ProjectionEventState.POISON,
                } or (
                    event.state == ProjectionEventState.PROCESSING
                    and event.lease_expires_at is not None
                    and event.lease_expires_at <= completed_at
                ):
                    await self.save(connection, _completed(event, completed_at), now=completed_at)
                    count += 1
            return count


def _event(value: object) -> CatalogProjectionEvent:
    return CatalogProjectionEvent.model_validate(json_value(value))


def _completed(event: CatalogProjectionEvent, at: datetime) -> CatalogProjectionEvent:
    return event.model_copy(
        update={
            "state": ProjectionEventState.COMPLETED,
            "completed_at": at,
            "lease_owner": None,
            "lease_expires_at": None,
            "last_error_code": None,
            "poison_reason": None,
        }
    )
