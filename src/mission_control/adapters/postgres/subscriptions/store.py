"""PostgreSQL subscription store (``mission_subscription``, ``subscription_delivery``).

Every call runs in its own transaction under the transaction-local tenant scope (forced
RLS). Relay writes are fenced by the lease owner, so a relay that lost its lease cannot move
a cursor. Dead-lettering writes the receipt, the state change and the
``subscription.dead_lettered`` mission event in one transaction through the canonical
ledger writer.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.application.subscriptions.ports import NewSubscription, SubscriptionNotFound
from mission_control.contracts.identities import parse_request_scope, uuid7
from mission_control.domain.policies.contracts import ActorContext, DomainEventEnvelope
from mission_control.domain.subscriptions.contracts import (
    DEAD_LETTERED_EVENT,
    MissionEventEnvelope,
    Subscription,
    SubscriptionDelivery,
    SubscriptionFilters,
    SubscriptionState,
    SubscriptionTarget,
    node_key_of,
    payload_digest,
)

SCOPE = mc.SCOPE
RELAY_ACTOR = "system:subscription-relay"
_COLUMNS = """
    subscription_id, target_kind, mission_id, run_id, event_types, node_keys, channel,
    cursor_seq, state, failure_count, next_attempt_at, dead_lettered_at, closed_at, version,
    created_at, updated_at, created_by_actor_ref
"""


EVENT_COLUMNS = """
    event_id, mission_id, run_id, activation_id, seq, event_type, event_version, actor_ref,
    happened_at, recorded_at, causation_ref, payload
"""


def envelope_from_row(row: asyncpg.Record, application_id: str) -> MissionEventEnvelope:
    """The reference-only `mc.event.v1` envelope of one `mission_event` row."""

    payload = mc.load(row["payload"])
    return MissionEventEnvelope(
        event_id=row["event_id"],
        application_id=application_id,
        mission_id=row["mission_id"],
        run_id=row["run_id"],
        activation_id=row["activation_id"],
        seq=row["seq"],
        event_type=row["event_type"],
        event_version=row["event_version"],
        actor_ref=row["actor_ref"],
        happened_at=row["happened_at"],
        recorded_at=row["recorded_at"],
        causation_ref=row["causation_ref"],
        node_key=node_key_of(payload),
        payload_ref=(
            f"mc://applications/{application_id}/missions/{row['mission_id']}/events/{row['seq']}"
        ),
        payload_digest=payload_digest(payload),
    )


class SubscriptionLeaseLost(RuntimeError):
    """Another relay owns this subscription now; this relay must stop writing."""


class PostgresSubscriptionStore:
    def __init__(self, pool: asyncpg.Pool, request_scope: str) -> None:
        parse_request_scope(request_scope)
        self._pool = pool
        self._scope = request_scope

    @property
    def request_scope(self) -> str:
        return self._scope

    def _subscription(self, row: asyncpg.Record) -> Subscription:
        return Subscription(
            subscription_id=row["subscription_id"],
            request_scope=self._scope,
            target=SubscriptionTarget(
                kind=row["target_kind"], mission_id=row["mission_id"], run_id=row["run_id"]
            ),
            filters=SubscriptionFilters(
                event_types=tuple(row["event_types"]), node_keys=tuple(row["node_keys"])
            ),
            channel=mc.load(row["channel"]),
            cursor_seq=row["cursor_seq"],
            state=SubscriptionState(row["state"]),
            failure_count=row["failure_count"],
            next_attempt_at=row["next_attempt_at"],
            actor_ref=row["created_by_actor_ref"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            dead_lettered_at=row["dead_lettered_at"],
            closed_at=row["closed_at"],
            version=row["version"],
        )

    async def resolve_target(self, kind: str, target_id: UUID) -> SubscriptionTarget:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            if kind == "run":
                mission_id = await connection.fetchval(
                    f"SELECT mission_id FROM mission_control.mission_run "
                    f"WHERE {SCOPE} AND run_id = $4",
                    *args,
                    target_id,
                )
                if mission_id is None:
                    raise SubscriptionNotFound(str(target_id))
                return SubscriptionTarget(kind="run", mission_id=mission_id, run_id=target_id)
            exists = await connection.fetchval(
                f"SELECT 1 FROM mission_control.mission WHERE {SCOPE} AND mission_id = $4",
                *args,
                target_id,
            )
            if exists is None:
                raise SubscriptionNotFound(str(target_id))
            return SubscriptionTarget(kind="mission", mission_id=target_id)

    async def last_seq(self, mission_id: UUID) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            value = await connection.fetchval(
                f"SELECT last_event_seq FROM mission_control.mission "
                f"WHERE {SCOPE} AND mission_id = $4",
                *args,
                mission_id,
            )
            return int(value or 0)

    async def oldest_seq(self, mission_id: UUID) -> int | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            value = await connection.fetchval(
                f"SELECT min(seq) FROM mission_control.mission_event "
                f"WHERE {SCOPE} AND mission_id = $4",
                *args,
                mission_id,
            )
            return None if value is None else int(value)

    async def create(self, subscription: NewSubscription) -> Subscription:
        channel = subscription.channel
        channel_json = channel.model_dump(mode="json")
        if channel_json.get("kind") == "webhook":
            ref = channel_json["secret_ref"]
            channel_json["secret_ref"] = f"{ref['provider']}:{ref['key']}"
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            row = await connection.fetchrow(
                f"""
                INSERT INTO mission_control.mission_subscription (
                    installation_id, application_id, tenant_id, subscription_id, target_kind,
                    mission_id, run_id, event_types, node_keys, channel_kind, channel,
                    cursor_seq, state, failure_count, next_attempt_at, version, created_at,
                    updated_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12, 'active', 0,
                        $13, 1, $13, $13, $14)
                RETURNING {_COLUMNS}
                """,
                *args,
                uuid7(),
                subscription.target.kind,
                subscription.target.mission_id,
                subscription.target.run_id,
                list(subscription.filters.event_types),
                list(subscription.filters.node_keys),
                channel_json["kind"],
                json.dumps(channel_json, sort_keys=True),
                subscription.cursor_seq,
                subscription.now,
                subscription.actor_ref,
            )
            return self._subscription(row)

    async def get(self, subscription_id: UUID) -> Subscription:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            row = await connection.fetchrow(
                f"SELECT {_COLUMNS} FROM mission_control.mission_subscription "
                f"WHERE {SCOPE} AND subscription_id = $4",
                *args,
                subscription_id,
            )
            if row is None:
                raise SubscriptionNotFound(str(subscription_id))
            return self._subscription(row)

    async def list(
        self, *, mission_id: UUID | None = None, run_id: UUID | None = None
    ) -> tuple[Subscription, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            rows = await connection.fetch(
                f"""
                SELECT {_COLUMNS} FROM mission_control.mission_subscription
                WHERE {SCOPE} AND ($4::uuid IS NULL OR mission_id = $4)
                  AND ($5::uuid IS NULL OR run_id = $5)
                ORDER BY created_at, subscription_id
                """,
                *args,
                mission_id,
                run_id,
            )
            return tuple(self._subscription(row) for row in rows)

    async def set_state(
        self,
        subscription_id: UUID,
        state: SubscriptionState,
        *,
        now: datetime,
        actor_ref: str | None = None,
    ) -> Subscription:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            row = await connection.fetchrow(
                f"""
                UPDATE mission_control.mission_subscription
                SET state = $5,
                    failure_count = CASE WHEN $5 = 'active' THEN 0 ELSE failure_count END,
                    next_attempt_at = CASE WHEN $5 = 'active' THEN $6 ELSE next_attempt_at END,
                    dead_lettered_at = CASE WHEN $5 = 'dead_lettered' THEN $6 ELSE NULL END,
                    closed_at = CASE WHEN $5 = 'closed' THEN $6 ELSE NULL END,
                    closed_by_actor_ref = CASE WHEN $5 = 'closed' THEN $7 ELSE NULL END,
                    lease_owner = NULL, lease_expires_at = NULL,
                    version = version + 1, updated_at = $6
                WHERE {SCOPE} AND subscription_id = $4 AND state <> 'closed'
                RETURNING {_COLUMNS}
                """,
                *args,
                subscription_id,
                state.value,
                now,
                actor_ref,
            )
            if row is None:
                return await self.get(subscription_id)
            return self._subscription(row)

    async def lease_due(
        self, *, now: datetime, owner: str, lease_seconds: int, limit: int
    ) -> tuple[Subscription, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            rows = await connection.fetch(
                f"""
                UPDATE mission_control.mission_subscription
                SET lease_owner = $4, lease_expires_at = $5::timestamptz + $6::interval,
                    version = version + 1, updated_at = $5
                WHERE {SCOPE} AND subscription_id IN (
                    SELECT subscription_id FROM mission_control.mission_subscription
                    WHERE {SCOPE} AND state = 'active' AND channel_kind <> 'stream_ticket'
                      AND next_attempt_at <= $5
                      AND (lease_expires_at IS NULL OR lease_expires_at < $5)
                    ORDER BY next_attempt_at, subscription_id
                    LIMIT $7
                    FOR UPDATE SKIP LOCKED
                )
                RETURNING {_COLUMNS}
                """,
                *args,
                owner,
                now,
                timedelta(seconds=lease_seconds),
                limit,
            )
            return tuple(self._subscription(row) for row in rows)

    async def events_after(
        self, target: SubscriptionTarget, after_seq: int, limit: int
    ) -> tuple[MissionEventEnvelope, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            rows = await connection.fetch(
                f"""
                SELECT {EVENT_COLUMNS}
                FROM mission_control.mission_event
                WHERE {SCOPE} AND mission_id = $4 AND seq > $5
                  AND ($6::uuid IS NULL OR run_id = $6)
                ORDER BY seq
                LIMIT $7
                """,
                *args,
                target.mission_id,
                after_seq,
                target.run_id,
                limit,
            )
            return tuple(envelope_from_row(row, args[1]) for row in rows)

    async def attempts(self, subscription_id: UUID, event_id: UUID) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            value = await connection.fetchval(
                f"SELECT coalesce(max(attempt), 0) FROM mission_control.subscription_delivery "
                f"WHERE {SCOPE} AND subscription_id = $4 AND event_id = $5",
                *args,
                subscription_id,
                event_id,
            )
            return int(value)

    async def _receipt(
        self, connection: asyncpg.Connection, args: tuple[Any, ...], delivery: SubscriptionDelivery
    ) -> None:
        await connection.execute(
            """
            INSERT INTO mission_control.subscription_delivery (
                installation_id, application_id, tenant_id, delivery_id, subscription_id,
                event_id, seq, attempt, status, response_code, latency_ms, error_class,
                recorded_at, created_at, created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13, $14)
            """,
            *args,
            uuid7(),
            delivery.subscription_id,
            delivery.event_id,
            delivery.seq,
            delivery.attempt,
            delivery.status.value,
            delivery.response_code,
            delivery.latency_ms,
            delivery.error_class,
            delivery.recorded_at,
            RELAY_ACTOR,
        )

    async def _fenced_update(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        subscription_id: UUID,
        owner: str,
        assignments: str,
        *values: object,
    ) -> Subscription:
        row = await connection.fetchrow(
            f"""
            UPDATE mission_control.mission_subscription
            SET {assignments}, version = version + 1
            WHERE {SCOPE} AND subscription_id = $4 AND lease_owner = $5 AND state = 'active'
            RETURNING {_COLUMNS}
            """,
            *args,
            subscription_id,
            owner,
            *values,
        )
        if row is None:
            raise SubscriptionLeaseLost(str(subscription_id))
        return self._subscription(row)

    async def record_success(
        self, subscription: Subscription, delivery: SubscriptionDelivery, *, owner: str
    ) -> Subscription:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            await self._receipt(connection, args, delivery)
            return await self._fenced_update(
                connection,
                args,
                subscription.subscription_id,
                owner,
                "cursor_seq = GREATEST(cursor_seq, $6), failure_count = 0, "
                "next_attempt_at = $7, updated_at = $7",
                delivery.seq,
                delivery.recorded_at,
            )

    async def advance_cursor(
        self, subscription: Subscription, cursor_seq: int, *, now: datetime, owner: str
    ) -> Subscription:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            return await self._fenced_update(
                connection,
                args,
                subscription.subscription_id,
                owner,
                "cursor_seq = GREATEST(cursor_seq, $6), updated_at = $7",
                cursor_seq,
                now,
            )

    async def record_failure(
        self,
        subscription: Subscription,
        delivery: SubscriptionDelivery,
        *,
        failure_count: int,
        next_attempt_at: datetime,
        dead_letter: bool,
        owner: str,
    ) -> Subscription:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            await self._receipt(connection, args, delivery)
            if not dead_letter:
                return await self._fenced_update(
                    connection,
                    args,
                    subscription.subscription_id,
                    owner,
                    "failure_count = $6, next_attempt_at = $7, updated_at = $8",
                    failure_count,
                    next_attempt_at,
                    delivery.recorded_at,
                )
            updated = await self._fenced_update(
                connection,
                args,
                subscription.subscription_id,
                owner,
                "failure_count = $6, state = 'dead_lettered', dead_lettered_at = $7, "
                "lease_owner = NULL, lease_expires_at = NULL, updated_at = $7",
                failure_count,
                delivery.recorded_at,
            )
            await self._emit_dead_lettered(connection, args, updated, delivery)
            return updated

    async def _emit_dead_lettered(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        subscription: Subscription,
        delivery: SubscriptionDelivery,
    ) -> None:
        run_key = await connection.fetchval(
            f"""
            SELECT run_key FROM mission_control.mission_run
            WHERE {SCOPE} AND mission_id = $4 AND ($5::uuid IS NULL OR run_id = $5)
            ORDER BY created_at DESC, run_id DESC
            LIMIT 1
            """,
            *args,
            subscription.target.mission_id,
            subscription.target.run_id,
        )
        if run_key is None:
            return  # A mission without a run has no stream position to attribute the event to.
        ordinal = await connection.fetchval(
            f"SELECT count(*) FROM mission_control.subscription_delivery "
            f"WHERE {SCOPE} AND subscription_id = $4 AND status = 'dead_lettered'",
            *args,
            subscription.subscription_id,
        )
        event = DomainEventEnvelope(
            event_id=f"{DEAD_LETTERED_EVENT}:{subscription.subscription_id}:{ordinal}",
            event_type=DEAD_LETTERED_EVENT,
            aggregate_id=f"subscription:{subscription.subscription_id}",
            aggregate_version=int(ordinal),
            sequence=1,
            occurred_at=delivery.recorded_at,
            recorded_at=delivery.recorded_at,
            actor=ActorContext(actor_id=RELAY_ACTOR),
            correlation_id=str(subscription.subscription_id),
            payload={
                "subscription_id": str(subscription.subscription_id),
                "channel_kind": subscription.channel.kind,
                "failure_count": subscription.failure_count,
                "last_event_id": str(delivery.event_id),
                "last_seq": delivery.seq,
                "cursor_seq": subscription.cursor_seq,
            },
        )
        await mc.append_events(
            connection,
            args,
            run_key=run_key,
            commit_key=f"subscription-dead-letter:{subscription.subscription_id}:{ordinal}",
            expected_versions={},
            events=(event,),
            actor_ref=RELAY_ACTOR,
        )

    async def release(self, subscription: Subscription, *, owner: str) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            await connection.execute(
                f"""
                UPDATE mission_control.mission_subscription
                SET lease_owner = NULL, lease_expires_at = NULL, version = version + 1
                WHERE {SCOPE} AND subscription_id = $4 AND lease_owner = $5
                """,
                *args,
                subscription.subscription_id,
                owner,
            )
