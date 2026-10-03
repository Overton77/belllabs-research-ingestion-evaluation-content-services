"""Scoped, row-locked processing of the immutable catalog publication outbox."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import asyncpg

from mission_control.application.capabilities.catalog_projection_events import (
    CatalogProjectionEvent,
    ProjectionEventFailure,
    ProjectionEventState,
    bounded_projection_backoff,
    projection_alert,
)
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump
from mission_control.domain.authoring.contracts import ExactDefinitionRef


class PostgresProjectionEventRepository:
    def __init__(self, pool: asyncpg.Pool, *, catalog_scope: str) -> None:
        if not catalog_scope:
            raise ValueError("trusted installation catalog scope required")
        self.pool = pool
        self.scope = catalog_scope

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[asyncpg.Connection]:
        async with self.pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('belllabs.catalog_scope',$1,true)", self.scope
            )
            yield connection

    async def seed(self, connection: asyncpg.Connection) -> None:
        rows = await connection.fetch(
            """SELECT identity,payload,payload_digest
            FROM belllabs_control.definition_catalog_records
            WHERE catalog_scope=$1 AND contract='catalog-event/1'""",
            self.scope,
        )
        for row in rows:
            payload = (
                json.loads(row["payload"]) if isinstance(row["payload"], str) else row["payload"]
            )
            if sha256_digest(payload) != row["payload_digest"]:
                raise ValueError("catalog outbox digest mismatch")
            event = CatalogProjectionEvent.model_validate(payload)
            if event.event_id != row["identity"] or event.tenant_scope != self.scope:
                raise ValueError("catalog outbox identity mismatch")
            await connection.execute(
                """INSERT INTO belllabs_control.catalog_projection_processing
                (catalog_scope,event_id,payload) VALUES($1,$2,$3::jsonb)
                ON CONFLICT(catalog_scope,event_id) DO NOTHING""",
                self.scope,
                event.event_id,
                json.dumps(stable_json_dump(event)),
            )

    async def save(self, connection: asyncpg.Connection, event: CatalogProjectionEvent) -> None:
        await connection.execute(
            """UPDATE belllabs_control.catalog_projection_processing
            SET payload=$3::jsonb WHERE catalog_scope=$1 AND event_id=$2""",
            self.scope,
            event.event_id,
            json.dumps(stable_json_dump(event)),
        )

    async def claim_batch(
        self, *, owner: str, now: datetime, lease_duration: timedelta, limit: int
    ) -> tuple[CatalogProjectionEvent, ...]:
        if not owner or lease_duration <= timedelta(0) or limit < 1:
            raise ValueError("projection event claim configuration is invalid")
        async with self.transaction() as connection:
            await self.seed(connection)
            rows = await connection.fetch(
                """SELECT payload FROM belllabs_control.catalog_projection_processing
                WHERE catalog_scope=$1 AND payload->>'state' IN ('pending','retry','processing')
                AND (payload->>'next_attempt_at')::timestamptz <= $2
                AND ((payload->>'lease_expires_at') IS NULL OR
                     (payload->>'lease_expires_at')::timestamptz <= $2)
                ORDER BY (payload->>'next_attempt_at')::timestamptz,
                         (payload->>'created_at')::timestamptz,event_id
                LIMIT $3 FOR UPDATE SKIP LOCKED""",
                self.scope,
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
                await self.save(connection, event)
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
            """SELECT payload FROM belllabs_control.catalog_projection_processing
            WHERE catalog_scope=$1 AND event_id=$2 FOR UPDATE""",
            self.scope,
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
            await self.save(connection, _completed(current, completed_at))
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
            await self.save(connection, updated)
            if poison:
                alert = projection_alert(updated, failure.error_code, failed_at)
                await connection.execute(
                    """INSERT INTO belllabs_control.catalog_projection_alerts
                    (catalog_scope,alert_id,payload) VALUES($1,$2,$3::jsonb)
                    ON CONFLICT(catalog_scope,alert_id) DO NOTHING""",
                    self.scope,
                    alert.alert_id,
                    json.dumps(stable_json_dump(alert)),
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
                """SELECT payload FROM belllabs_control.catalog_projection_processing
                WHERE catalog_scope=$1 AND payload->>'asset_kind'=$2 AND payload->>'logical_id'=$3
                AND (payload->>'revision')::bigint=$4 AND payload->>'source_digest'=$5
                FOR UPDATE""",
                self.scope,
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
                    await self.save(connection, _completed(event, completed_at))
                    count += 1
            return count


def _event(value: object) -> CatalogProjectionEvent:
    return CatalogProjectionEvent.model_validate(
        json.loads(value) if isinstance(value, str) else value
    )


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
