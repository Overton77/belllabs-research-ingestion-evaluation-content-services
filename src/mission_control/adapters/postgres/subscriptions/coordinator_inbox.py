"""PostgreSQL coordinator inbox (MP-15) over 0029 subscriptions plus the 0033 tables.

Existing 0029 state is reused as is: the inbox's `mission_subscription` row (channel
`stream_ticket`, `coordinator-inbox:<uuid>`) holds the target, the materialization cursor
(`cursor_seq`), the lifecycle state, and (for webhook callbacks) the lease, failure count,
next attempt and dead-letter disposition; `subscription_delivery` holds the callback receipts
keyed by each notification's anchor event.

Three tables are new: `coordinator_inbox`, `coordinator_notification` and
`coordinator_causation`, released in common migration 0033 (`RELEASED_MIGRATION`). Every call
runs in its own transaction under the transaction-local tenant scope (forced RLS).
Materialization holds the inbox row lock for the whole page loop, so two passes never plan the
same journal page, and notification upserts change only `open` rows: a sealed or suppressed
notification is never rewritten.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Final, Literal
from uuid import UUID

import asyncpg
from pydantic import TypeAdapter

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.subscriptions.store import (
    EVENT_COLUMNS,
    PostgresSubscriptionStore,
    envelope_from_row,
)
from mission_control.application.subscriptions.coordinator import (
    CoordinatorNotification,
    CoordinatorProfile,
    JournalEvent,
    PlanInput,
    extract_facts,
)
from mission_control.application.subscriptions.coordinator_ports import (
    AckOutcome,
    CausationOrigin,
    CausationRecord,
    CoordinatorInbox,
    InboxDelivery,
    MaterializeResult,
    NewInbox,
    Planner,
    PromptRecord,
    PromptState,
)
from mission_control.application.subscriptions.ports import SubscriptionNotFound
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.subscriptions.contracts import (
    Subscription,
    SubscriptionState,
    SubscriptionTarget,
)

SCOPE = mc.SCOPE
CALLBACK_ACTOR: Final = "system:coordinator-callback"
_DELIVERY: TypeAdapter[Any] = TypeAdapter(InboxDelivery)

# The three coordinator tables are released in common migration 0033 (component 1.2.0),
# section 2 of packages/mission-control-db-contract/component/migrations/
# 0033_approvals_coordinator_inbox_lane_describes.sql. That file is the only copy of the DDL.
RELEASED_MIGRATION: Final = (
    "packages/mission-control-db-contract/component/migrations/"
    "0033_approvals_coordinator_inbox_lane_describes.sql"
)

_NOTIFICATION_COLUMNS = """
    n.notification_id, n.inbox_seq, n.state, n.suppressed_reason, n.body, n.sealed_at,
    n.acknowledged_at, n.prompt_request_id, n.prompt_state, n.prompt_request, n.prompt_detail
"""
_INBOX_FROM = """
    FROM mission_control.coordinator_inbox i
    JOIN mission_control.mission_subscription s
      ON s.installation_id = i.installation_id AND s.application_id = i.application_id
     AND s.tenant_id = i.tenant_id AND s.subscription_id = i.subscription_id
"""
_INBOX_COLUMNS = """
    i.subscription_id, i.coordinator_ref, i.coordinator_run_ref, i.profile, i.delivery,
    i.next_inbox_seq, i.acked_inbox_seq, i.created_at, i.updated_at, i.version,
    s.target_kind, s.mission_id, s.run_id, s.state, s.cursor_seq
"""


def _uuid(value: str | None) -> UUID | None:
    if not value:
        return None
    try:
        return UUID(value)
    except ValueError:
        return None


class PostgresCoordinatorInboxStore:
    def __init__(self, pool: asyncpg.Pool, request_scope: str) -> None:
        parse_request_scope(request_scope)
        self._pool = pool
        self._scope = request_scope

    @property
    def request_scope(self) -> str:
        return self._scope

    # -- rows -------------------------------------------------------------------------------

    def _inbox(self, row: asyncpg.Record) -> CoordinatorInbox:
        return CoordinatorInbox(
            subscription_id=row["subscription_id"],
            coordinator_ref=row["coordinator_ref"],
            coordinator_run_ref=row["coordinator_run_ref"],
            target=SubscriptionTarget(
                kind=row["target_kind"], mission_id=row["mission_id"], run_id=row["run_id"]
            ),
            profile=CoordinatorProfile.model_validate(mc.load(row["profile"])),
            delivery=_DELIVERY.validate_python(mc.load(row["delivery"])),
            state=SubscriptionState(row["state"]),
            cursor_seq=row["cursor_seq"],
            next_inbox_seq=row["next_inbox_seq"],
            acked_inbox_seq=row["acked_inbox_seq"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            version=row["version"],
        )

    @staticmethod
    def _notification(row: asyncpg.Record) -> CoordinatorNotification:
        body = dict(mc.load(row["body"]))
        body.update(
            {
                "inbox_seq": row["inbox_seq"],
                "state": row["state"],
                "suppressed_reason": row["suppressed_reason"],
                "sealed_at": row["sealed_at"],
                "acknowledged_at": row["acknowledged_at"],
            }
        )
        return CoordinatorNotification.model_validate(body)

    @staticmethod
    def _prompt(row: asyncpg.Record) -> PromptRecord | None:
        if row["prompt_state"] is None:
            return None
        return PromptRecord(
            notification_id=row["notification_id"],
            request_id=row["prompt_request_id"],
            request=mc.load(row["prompt_request"]) or {},
            state=row["prompt_state"],
            detail=row["prompt_detail"],
        )

    # -- inbox ------------------------------------------------------------------------------

    async def create(self, inbox: NewInbox) -> CoordinatorInbox:
        delivery = inbox.delivery.model_dump(mode="json")
        if delivery.get("kind") == "webhook":
            ref = delivery["secret_ref"]
            delivery["secret_ref"] = f"{ref['provider']}:{ref['key']}"
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            await connection.execute(
                """
                INSERT INTO mission_control.coordinator_inbox (
                    installation_id, application_id, tenant_id, subscription_id,
                    coordinator_ref, coordinator_run_ref, profile, delivery_kind, delivery,
                    prompting, next_inbox_seq, acked_inbox_seq, version, created_at, updated_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9::jsonb, $10, 1, 0, 1, $11, $11,
                        $5)
                """,
                *args,
                inbox.subscription_id,
                inbox.coordinator_ref,
                inbox.coordinator_run_ref,
                inbox.profile.model_dump_json(),
                delivery["kind"],
                json.dumps(delivery, sort_keys=True),
                inbox.profile.prompt_mode != "off",
                inbox.now,
            )
            return await self._get(connection, args, inbox.subscription_id)

    async def _get(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        subscription_id: UUID,
        *,
        lock: bool = False,
    ) -> CoordinatorInbox:
        if lock:
            # Lock first, then read in a new statement: under READ COMMITTED a `FOR UPDATE OF i`
            # that waited re-checks only `i`, while the joined subscription row (the cursor)
            # would still come from the statement's original snapshot.
            locked = await connection.fetchval(
                f"SELECT 1 FROM mission_control.coordinator_inbox "
                f"WHERE {SCOPE} AND subscription_id = $4 FOR UPDATE",
                *args,
                subscription_id,
            )
            if locked is None:
                raise SubscriptionNotFound(str(subscription_id))
        row = await connection.fetchrow(
            f"SELECT {_INBOX_COLUMNS} {_INBOX_FROM} "
            f"WHERE {mc.scoped('i')} AND i.subscription_id = $4",
            *args,
            subscription_id,
        )
        if row is None:
            raise SubscriptionNotFound(str(subscription_id))
        return self._inbox(row)

    async def get(self, subscription_id: UUID) -> CoordinatorInbox:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            return await self._get(connection, args, subscription_id)

    async def active_inboxes(
        self,
        *,
        delivery: Literal["poll", "mcp_session", "webhook"] | None = None,
        prompting: bool = False,
    ) -> tuple[UUID, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            rows = await connection.fetch(
                f"""
                SELECT i.subscription_id {_INBOX_FROM}
                WHERE {mc.scoped("i")} AND s.state <> 'closed'
                  AND ($4::text IS NULL OR i.delivery_kind = $4)
                  AND (NOT $5::boolean OR i.prompting)
                ORDER BY i.created_at, i.subscription_id
                """,
                *args,
                delivery,
                prompting,
            )
            return tuple(row["subscription_id"] for row in rows)

    # -- materialization --------------------------------------------------------------------

    async def _journal(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        target: SubscriptionTarget,
        after_seq: int,
        limit: int,
    ) -> tuple[JournalEvent, ...]:
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
        return tuple(
            JournalEvent(
                envelope=envelope_from_row(row, args[1]),
                facts=extract_facts(mc.load(row["payload"])),
            )
            for row in rows
        )

    async def materialize(
        self, subscription_id: UUID, *, planner: Planner, now: datetime, page: int, max_pages: int
    ) -> MaterializeResult:
        sealed: list[CoordinatorNotification] = []
        suppressed = 0
        pages = 0
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            inbox = await self._get(connection, args, subscription_id, lock=True)
            cursor = inbox.cursor_seq
            next_inbox_seq = inbox.next_inbox_seq
            if inbox.state is SubscriptionState.CLOSED:
                return MaterializeResult((), 0, cursor, 0)
            profile = inbox.profile
            while pages < max_pages:
                events = await self._journal(connection, args, inbox.target, cursor, page)
                open_rows = await connection.fetch(
                    f"SELECT {_NOTIFICATION_COLUMNS} "
                    f"FROM mission_control.coordinator_notification n "
                    f"WHERE {mc.scoped('n')} AND n.subscription_id = $4 AND n.state = 'open'",
                    *args,
                    subscription_id,
                )
                recent_rows = await connection.fetch(
                    f"""
                    SELECT n.run_id, n.sealed_at FROM mission_control.coordinator_notification n
                    WHERE {mc.scoped("n")} AND n.subscription_id = $4
                      AND n.state IN ('pending', 'acknowledged') AND n.sealed_at > $5
                    """,
                    *args,
                    subscription_id,
                    now - profile.rate_window,
                )
                recent: dict[UUID | None, list[datetime]] = {}
                for row in recent_rows:
                    recent.setdefault(row["run_id"], []).append(row["sealed_at"])
                refs = sorted(
                    {
                        str(parsed)
                        for event in events
                        if (parsed := _uuid(event.envelope.causation_ref)) is not None
                    }
                )
                depths: dict[str, int] = {}
                if refs:
                    for row in await connection.fetch(
                        f"SELECT command_request_id, depth "
                        f"FROM mission_control.coordinator_causation "
                        f"WHERE {SCOPE} AND command_request_id = ANY($4::uuid[])",
                        *args,
                        refs,
                    ):
                        depths[str(row["command_request_id"])] = row["depth"]
                result = planner(
                    PlanInput(
                        subscription_id=subscription_id,
                        mission_id=inbox.target.mission_id,
                        profile=profile,
                        cursor_seq=cursor,
                        next_inbox_seq=next_inbox_seq,
                        open=tuple(self._notification(row) for row in open_rows),
                        recent_sealed={run: tuple(items) for run, items in recent.items()},
                        depths=depths,
                        events=events,
                        now=now,
                    )
                )
                for item in result.upserts:
                    written = await self._upsert(connection, args, item, now)
                    if not written and item.state != "open":
                        # A sealed or suppressed notification is never rewritten; a plan that
                        # tries is a replanned page, so abort rather than skew the counters.
                        raise RuntimeError(f"notification {item.notification_id} is already sealed")
                    if item.state == "pending":
                        sealed.append(item)
                    elif item.state == "suppressed":
                        suppressed += 1
                pages += 1
                if result.cursor_seq > cursor:
                    await connection.execute(
                        f"""
                        UPDATE mission_control.mission_subscription
                        SET cursor_seq = GREATEST(cursor_seq, $5), version = version + 1,
                            updated_at = $6
                        WHERE {SCOPE} AND subscription_id = $4
                        """,
                        *args,
                        subscription_id,
                        result.cursor_seq,
                        now,
                    )
                cursor = result.cursor_seq
                next_inbox_seq = result.next_inbox_seq
                if len(events) < page:
                    break
            if next_inbox_seq != inbox.next_inbox_seq:
                await connection.execute(
                    f"""
                    UPDATE mission_control.coordinator_inbox
                    SET next_inbox_seq = $5, version = version + 1, updated_at = $6
                    WHERE {SCOPE} AND subscription_id = $4
                    """,
                    *args,
                    subscription_id,
                    next_inbox_seq,
                    now,
                )
        return MaterializeResult(tuple(sealed), suppressed, cursor, pages)

    async def _upsert(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        item: CoordinatorNotification,
        now: datetime,
    ) -> bool:
        body = item.model_dump_json(
            exclude={"inbox_seq", "state", "suppressed_reason", "sealed_at", "acknowledged_at"}
        )
        # Re-validate the stored shape: the model's invariants hold before anything is written.
        CoordinatorNotification.model_validate(item.model_dump())
        written = await connection.fetchval(
            """
            INSERT INTO mission_control.coordinator_notification (
                installation_id, application_id, tenant_id, notification_id, subscription_id,
                inbox_seq, state, kind, actionable, mission_id, run_id, seq_from, seq_to,
                event_count, anchor_event_id, depth, suppressed_reason, body, opened_at,
                sealed_at, version, created_at, updated_at, created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                    $18::jsonb, $19, $20, 1, $21, $21, 'system:coordinator-inbox')
            ON CONFLICT (notification_id) DO UPDATE
            SET inbox_seq = EXCLUDED.inbox_seq, state = EXCLUDED.state,
                actionable = EXCLUDED.actionable, seq_from = EXCLUDED.seq_from,
                seq_to = EXCLUDED.seq_to, event_count = EXCLUDED.event_count,
                anchor_event_id = EXCLUDED.anchor_event_id, depth = EXCLUDED.depth,
                suppressed_reason = EXCLUDED.suppressed_reason, body = EXCLUDED.body,
                sealed_at = EXCLUDED.sealed_at,
                version = mission_control.coordinator_notification.version + 1,
                updated_at = EXCLUDED.updated_at
            WHERE mission_control.coordinator_notification.state = 'open'
            RETURNING true
            """,
            *args,
            item.notification_id,
            item.subscription_id,
            item.inbox_seq,
            item.state,
            item.kind.value,
            item.actionable,
            item.mission_id,
            item.run_id,
            item.seq_from,
            item.seq_to,
            item.event_count,
            item.anchor_event_id,
            item.depth,
            item.suppressed_reason,
            body,
            item.opened_at,
            item.sealed_at,
            now,
        )
        return bool(written)

    # -- reading and acknowledging ------------------------------------------------------------

    async def notifications(
        self, subscription_id: UUID, *, after_inbox_seq: int, limit: int
    ) -> tuple[CoordinatorNotification, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            rows = await connection.fetch(
                f"""
                SELECT {_NOTIFICATION_COLUMNS}
                FROM mission_control.coordinator_notification n
                WHERE {mc.scoped("n")} AND n.subscription_id = $4 AND n.state = 'pending'
                  AND n.inbox_seq > $5
                ORDER BY n.inbox_seq
                LIMIT $6
                """,
                *args,
                subscription_id,
                after_inbox_seq,
                limit,
            )
            return tuple(self._notification(row) for row in rows)

    async def notification(
        self, subscription_id: UUID, notification_id: UUID
    ) -> CoordinatorNotification | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            row = await connection.fetchrow(
                f"SELECT {_NOTIFICATION_COLUMNS} FROM mission_control.coordinator_notification n "
                f"WHERE {mc.scoped('n')} AND n.subscription_id = $4 AND n.notification_id = $5",
                *args,
                subscription_id,
                notification_id,
            )
            return None if row is None else self._notification(row)

    async def _acknowledge(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        subscription_id: UUID,
        *,
        notification_ids: tuple[UUID, ...],
        through_inbox_seq: int | None,
        actor_ref: str,
        now: datetime,
    ) -> AckOutcome:
        inbox = await self._get(connection, args, subscription_id, lock=True)
        rows = await connection.fetch(
            f"""
            UPDATE mission_control.coordinator_notification
            SET state = 'acknowledged', acknowledged_at = $7, acknowledged_by_actor_ref = $8,
                version = version + 1, updated_at = $7
            WHERE {SCOPE} AND subscription_id = $4 AND state = 'pending'
              AND (notification_id = ANY($5::uuid[]) OR inbox_seq <= $6)
            RETURNING notification_id
            """,
            *args,
            subscription_id,
            list(notification_ids),
            through_inbox_seq if through_inbox_seq is not None else 0,
            now,
            actor_ref,
        )
        acknowledged = {row["notification_id"] for row in rows}
        known = {
            row["notification_id"]
            for row in await connection.fetch(
                f"""
                SELECT notification_id FROM mission_control.coordinator_notification
                WHERE {SCOPE} AND subscription_id = $4 AND notification_id = ANY($5::uuid[])
                  AND state = 'acknowledged'
                """,
                *args,
                subscription_id,
                list(notification_ids),
            )
        }
        first_pending = await connection.fetchval(
            f"""
            SELECT min(inbox_seq) FROM mission_control.coordinator_notification
            WHERE {SCOPE} AND subscription_id = $4 AND state = 'pending'
            """,
            *args,
            subscription_id,
        )
        acked = int(first_pending) - 1 if first_pending is not None else inbox.next_inbox_seq - 1
        if acked > inbox.acked_inbox_seq:
            await connection.execute(
                f"""
                UPDATE mission_control.coordinator_inbox
                SET acked_inbox_seq = GREATEST(acked_inbox_seq, $5), version = version + 1,
                    updated_at = $6
                WHERE {SCOPE} AND subscription_id = $4
                """,
                *args,
                subscription_id,
                acked,
                now,
            )
        return AckOutcome(
            acknowledged=tuple(item for item in notification_ids if item in acknowledged)
            + tuple(sorted(acknowledged - set(notification_ids), key=str)),
            already=tuple(
                item for item in notification_ids if item in known and item not in acknowledged
            ),
            unknown=tuple(item for item in notification_ids if item not in known),
            acked_inbox_seq=max(acked, inbox.acked_inbox_seq),
        )

    async def acknowledge(
        self,
        subscription_id: UUID,
        *,
        notification_ids: tuple[UUID, ...],
        through_inbox_seq: int | None,
        actor_ref: str,
        now: datetime,
    ) -> AckOutcome:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            return await self._acknowledge(
                connection,
                args,
                subscription_id,
                notification_ids=notification_ids,
                through_inbox_seq=through_inbox_seq,
                actor_ref=actor_ref,
                now=now,
            )

    async def mark_delivered(self, subscription_id: UUID, *, inbox_seq: int, now: datetime) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            outcome = await self._acknowledge(
                connection,
                args,
                subscription_id,
                notification_ids=(),
                through_inbox_seq=inbox_seq,
                actor_ref=CALLBACK_ACTOR,
                now=now,
            )
            return outcome.acked_inbox_seq

    # -- causation and prompts --------------------------------------------------------------

    async def record_causation(self, record: CausationRecord) -> CausationRecord:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            await connection.execute(
                """
                INSERT INTO mission_control.coordinator_causation (
                    installation_id, application_id, tenant_id, command_request_id,
                    subscription_id, notification_id, origin, target_run_ref, depth,
                    recorded_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10, 'system:coordinator-inbox')
                ON CONFLICT DO NOTHING
                """,
                *args,
                record.command_request_id,
                record.subscription_id,
                record.notification_id,
                record.origin,
                record.target_run_ref,
                record.depth,
                record.recorded_at,
            )
            row = await connection.fetchrow(
                f"""
                SELECT command_request_id, subscription_id, notification_id, origin,
                       target_run_ref, depth, recorded_at
                FROM mission_control.coordinator_causation
                WHERE {SCOPE} AND command_request_id = $4
                """,
                *args,
                record.command_request_id,
            )
            assert row is not None
            return CausationRecord(
                command_request_id=row["command_request_id"],
                subscription_id=row["subscription_id"],
                notification_id=row["notification_id"],
                origin=row["origin"],
                target_run_ref=row["target_run_ref"],
                depth=row["depth"],
                recorded_at=row["recorded_at"],
            )

    async def causations_since(
        self,
        subscription_id: UUID,
        *,
        origin: CausationOrigin,
        since: datetime,
        target_run_ref: str | None = None,
        exclude_request_id: UUID | None = None,
    ) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            value = await connection.fetchval(
                f"""
                SELECT count(*) FROM mission_control.coordinator_causation
                WHERE {SCOPE} AND subscription_id = $4 AND origin = $5 AND recorded_at > $6
                  AND ($7::text IS NULL OR target_run_ref = $7)
                  AND ($8::uuid IS NULL OR command_request_id <> $8)
                """,
                *args,
                subscription_id,
                origin,
                since,
                target_run_ref,
                exclude_request_id,
            )
            return int(value)

    async def begin_prompt(
        self,
        subscription_id: UUID,
        notification_id: UUID,
        *,
        request_id: UUID,
        request: Mapping[str, Any],
        now: datetime,
    ) -> PromptRecord:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            row = await connection.fetchrow(
                f"""
                UPDATE mission_control.coordinator_notification n
                SET prompt_request_id = $6, prompt_request = $7::jsonb,
                    prompt_state = 'requested', prompt_detail = NULL, prompted_at = $8,
                    version = n.version + 1, updated_at = $8
                WHERE {mc.scoped("n")} AND n.subscription_id = $4 AND n.notification_id = $5
                  AND (n.prompt_state IS NULL OR n.prompt_state = 'failed')
                RETURNING {_NOTIFICATION_COLUMNS}
                """,
                *args,
                subscription_id,
                notification_id,
                request_id,
                json.dumps(dict(request), sort_keys=True),
                now,
            )
            if row is None:
                row = await connection.fetchrow(
                    f"SELECT {_NOTIFICATION_COLUMNS} "
                    f"FROM mission_control.coordinator_notification n "
                    f"WHERE {mc.scoped('n')} AND n.subscription_id = $4 "
                    f"AND n.notification_id = $5",
                    *args,
                    subscription_id,
                    notification_id,
                )
            if row is None:
                raise SubscriptionNotFound(str(notification_id))
            record = self._prompt(row)
            assert record is not None
            return record

    async def finish_prompt(
        self,
        subscription_id: UUID,
        notification_id: UUID,
        *,
        state: PromptState,
        detail: str | None,
        now: datetime,
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            await connection.execute(
                f"""
                UPDATE mission_control.coordinator_notification
                SET prompt_state = $6, prompt_detail = $7, prompted_at = $8,
                    version = version + 1, updated_at = $8
                WHERE {SCOPE} AND subscription_id = $4 AND notification_id = $5
                  AND prompt_state IS DISTINCT FROM 'admitted'
                """,
                *args,
                subscription_id,
                notification_id,
                state,
                detail,
                now,
            )

    async def prompt_candidates(
        self, subscription_id: UUID, *, limit: int
    ) -> tuple[tuple[CoordinatorNotification, PromptRecord | None], ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            rows = await connection.fetch(
                f"""
                SELECT {_NOTIFICATION_COLUMNS}
                FROM mission_control.coordinator_notification n
                WHERE {mc.scoped("n")} AND n.subscription_id = $4 AND n.state = 'pending'
                  AND n.actionable
                  AND (n.prompt_state IS NULL OR n.prompt_state IN ('requested', 'failed'))
                ORDER BY n.inbox_seq
                LIMIT $5
                """,
                *args,
                subscription_id,
                limit,
            )
            return tuple((self._notification(row), self._prompt(row)) for row in rows)

    # -- webhook callbacks --------------------------------------------------------------------

    async def lease_callbacks(
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
                    SELECT s.subscription_id {_INBOX_FROM}
                    WHERE {mc.scoped("s")} AND s.state = 'active'
                      AND i.delivery_kind = 'webhook' AND s.next_attempt_at <= $5
                      AND (s.lease_expires_at IS NULL OR s.lease_expires_at < $5)
                    ORDER BY s.next_attempt_at, s.subscription_id
                    LIMIT $7
                    FOR UPDATE OF s SKIP LOCKED
                )
                RETURNING subscription_id
                """,
                *args,
                owner,
                now,
                timedelta(seconds=lease_seconds),
                limit,
            )
            leased = [row["subscription_id"] for row in rows]
        # Read back through the subscription mapping the FT-F5 store uses.
        store = PostgresSubscriptionStore(self._pool, self._scope)
        return tuple([await store.get(subscription_id) for subscription_id in leased])


__all__ = ["CALLBACK_ACTOR", "RELEASED_MIGRATION", "PostgresCoordinatorInboxStore"]
