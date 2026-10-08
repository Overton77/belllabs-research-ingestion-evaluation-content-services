"""PostgreSQL command mailbox (migration 0029 section F1; FT-F1, SPEC-06).

`insert_mailbox_entry` runs inside the run-control transaction that admits a
`queue_instruction` / `add_context` Command, so the entry and its Command commit together
(with the `command.queued` mission event). `PostgresCommandMailbox` serves the boundary
transitions; each serializes per Run on a transaction-scoped advisory lock and is
idempotent. Rows are never deleted. Inline bodies live only in `content_inline`; the `entry`
document is the reference-only view.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE
from mission_control.application.execution.mailbox import MailboxClaim, queued_event
from mission_control.contracts.identities import uuid7
from mission_control.domain.policies.contracts import DomainEventEnvelope
from mission_control.domain.policies.mailbox import (
    MailboxBoundaryPoint,
    MailboxEntry,
    MailboxState,
    claim_decision,
)

ACTOR_REF = "mission-control-runtime/mailbox"
_COLUMNS = """
    entry_id, run_key, command_id, command_issuer, generation, kind, boundary, node_key,
    content_ref, content_inline, content_digest, media_type, content_bytes, expand,
    admission_sequence, deadline, state, delivery_key, superseded_by, expired_reason,
    accepted_at, delivered_at, consumed_at, expired_at
"""


def _lock_key(request_scope: str, run_key: str) -> str:
    return f"mc.command_mailbox:{request_scope}:{run_key}"


def _entry(row: asyncpg.Record, request_scope: str) -> MailboxEntry:
    return MailboxEntry(
        entry_id=str(row["entry_id"]),
        request_scope=request_scope,
        run_id=row["run_key"],
        command_id=row["command_id"],
        command_issuer=row["command_issuer"],
        kind=row["kind"],
        generation=row["generation"],
        boundary=row["boundary"],
        node_key=row["node_key"],
        content_ref=row["content_ref"],
        content_digest=row["content_digest"],
        media_type=row["media_type"],
        content_bytes=row["content_bytes"],
        content_inline=row["content_inline"],
        expand=row["expand"],
        admission_sequence=row["admission_sequence"],
        deadline=row["deadline"],
        state=MailboxState(row["state"]),
        delivery_key=row["delivery_key"],
        superseded_by=row["superseded_by"],
        expired_reason=row["expired_reason"],
        accepted_at=row["accepted_at"],
        delivered_at=row["delivered_at"],
        consumed_at=row["consumed_at"],
        expired_at=row["expired_at"],
    )


async def insert_mailbox_entry(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    *,
    run: asyncpg.Record,
    command_row_id: UUID,
    entry: MailboxEntry,
) -> None:
    """Write one accepted mailbox command's entry (inside the admitting transaction)."""

    await connection.execute(
        """
        INSERT INTO mission_control.command_mailbox (
            installation_id, application_id, tenant_id, entry_id, run_id, run_key,
            command_row_id, command_id, command_issuer, generation, kind, boundary, node_key,
            content_ref, content_inline, content_digest, media_type, content_bytes, expand,
            admission_sequence, deadline, state, accepted_at, entry, version, updated_at,
            created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19, $20, $21, 'queued', $22, $23::jsonb, 1, $22, $22, $24)
        """,
        *args,
        UUID(entry.entry_id),
        run["run_id"],
        entry.run_id,
        command_row_id,
        entry.command_id,
        entry.command_issuer,
        entry.generation,
        entry.kind,
        entry.boundary,
        entry.node_key,
        entry.content_ref,
        entry.content_inline,
        entry.content_digest,
        entry.media_type,
        entry.content_bytes,
        entry.expand,
        entry.admission_sequence,
        entry.deadline,
        entry.accepted_at,
        mc.dump(entry.reference_view().model_dump(mode="json")),
        ACTOR_REF,
    )
    await mc.append_events(
        connection,
        args,
        run_key=entry.run_id,
        commit_key=f"mailbox-queued:{entry.entry_id}",
        expected_versions={},
        events=(queued_event(entry),),
        actor_ref=ACTOR_REF,
    )


class PostgresCommandMailbox:
    """`CommandMailboxRepository` over `mission_control.command_mailbox`."""

    def __init__(self, pool: asyncpg.Pool, *, actor_ref: str = ACTOR_REF) -> None:
        self._pool = pool
        self._actor_ref = actor_ref

    async def _rows(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        run_key: str,
        *,
        lock: bool = False,
    ) -> list[asyncpg.Record]:
        return list(
            await connection.fetch(
                f"""
                SELECT {_COLUMNS}
                FROM mission_control.command_mailbox
                WHERE {SCOPE} AND run_key = $4
                ORDER BY generation, admission_sequence
                """
                + (" FOR UPDATE" if lock else ""),
                *args,
                run_key,
            )
        )

    async def _update(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        entry: MailboxEntry,
        now: datetime,
    ) -> None:
        await connection.execute(
            f"""
            UPDATE mission_control.command_mailbox
            SET state = $5, delivery_key = $6, superseded_by = $7, expired_reason = $8,
                delivered_at = $9, consumed_at = $10, expired_at = $11, entry = $12::jsonb,
                version = version + 1, updated_at = $13
            WHERE {SCOPE} AND entry_id = $4
            """,
            *args,
            UUID(entry.entry_id),
            entry.state.value,
            entry.delivery_key,
            entry.superseded_by,
            entry.expired_reason,
            entry.delivered_at,
            entry.consumed_at,
            entry.expired_at,
            mc.dump(entry.reference_view().model_dump(mode="json")),
            now,
        )

    async def list_entries(self, request_scope: str, run_id: str) -> tuple[MailboxEntry, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            return tuple(
                _entry(row, request_scope) for row in await self._rows(connection, args, run_id)
            )

    async def claim(
        self,
        request_scope: str,
        run_id: str,
        *,
        delivery_key: str,
        point: MailboxBoundaryPoint,
        now: datetime,
    ) -> MailboxClaim:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, _lock_key(request_scope, run_id))
            run = await mc.run_uuid(connection, args, run_id)
            recorded = await connection.fetchval(
                f"""
                SELECT entry_ids FROM mission_control.command_mailbox_claim
                WHERE {SCOPE} AND run_id = $4 AND delivery_key = $5
                """,
                *args,
                run,
                delivery_key,
            )
            rows = await self._rows(connection, args, run_id, lock=True)
            entries = [_entry(row, request_scope) for row in rows]
            if recorded is not None:
                taken = {str(item) for item in recorded}
                return MailboxClaim(
                    delivery_key=delivery_key,
                    delivered=tuple(item for item in entries if item.entry_id in taken),
                    replay=True,
                )
            delivered: list[MailboxEntry] = []
            expired: list[MailboxEntry] = []
            for entry in entries:
                decision = claim_decision(entry, point, now=now)
                if decision == "claim":
                    updated = entry.model_copy(
                        update={
                            "state": MailboxState.DELIVERED,
                            "delivery_key": delivery_key,
                            "delivered_at": now,
                        }
                    )
                    delivered.append(updated)
                elif decision in {"stale_generation", "deadline_passed"}:
                    updated = entry.model_copy(
                        update={
                            "state": MailboxState.EXPIRED,
                            "expired_reason": decision,
                            "expired_at": now,
                        }
                    )
                    expired.append(updated)
                else:
                    continue
                await self._update(connection, args, updated, now)
            await connection.execute(
                """
                INSERT INTO mission_control.command_mailbox_claim (
                    installation_id, application_id, tenant_id, claim_id, run_id, run_key,
                    delivery_key, generation, boundary_point, entry_ids, claimed_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $11, $12)
                """,
                *args,
                uuid7(),
                run,
                run_id,
                delivery_key,
                point.generation,
                mc.dump(point.model_dump(mode="json")),
                [UUID(item.entry_id) for item in delivered],
                now,
                self._actor_ref,
            )
            return MailboxClaim(
                delivery_key=delivery_key,
                delivered=tuple(delivered),
                expired=tuple(expired),
            )

    async def consume(
        self, request_scope: str, run_id: str, *, delivery_key: str, now: datetime
    ) -> tuple[MailboxEntry, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, _lock_key(request_scope, run_id))
            consumed: list[MailboxEntry] = []
            for row in await self._rows(connection, args, run_id, lock=True):
                entry = _entry(row, request_scope)
                if entry.delivery_key != delivery_key:
                    continue
                if entry.state == MailboxState.DELIVERED:
                    entry = entry.model_copy(
                        update={"state": MailboxState.CONSUMED, "consumed_at": now}
                    )
                    await self._update(connection, args, entry, now)
                if entry.state == MailboxState.CONSUMED:
                    consumed.append(entry)
            return tuple(consumed)

    async def release(
        self, request_scope: str, run_id: str, *, delivery_key: str
    ) -> tuple[MailboxEntry, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, _lock_key(request_scope, run_id))
            now = await connection.fetchval("SELECT clock_timestamp()")
            released: list[MailboxEntry] = []
            for row in await self._rows(connection, args, run_id, lock=True):
                entry = _entry(row, request_scope)
                if entry.delivery_key != delivery_key or entry.state != MailboxState.DELIVERED:
                    continue
                entry = entry.model_copy(
                    update={
                        "state": MailboxState.QUEUED,
                        "delivery_key": None,
                        "delivered_at": None,
                    }
                )
                await self._update(connection, args, entry, now)
                released.append(entry)
            return tuple(released)

    async def supersede(
        self,
        request_scope: str,
        run_id: str,
        *,
        superseded_by: str,
        now: datetime,
    ) -> tuple[MailboxEntry, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, _lock_key(request_scope, run_id))
            superseded: list[MailboxEntry] = []
            for row in await self._rows(connection, args, run_id, lock=True):
                entry = _entry(row, request_scope)
                if entry.state == MailboxState.SUPERSEDED and entry.superseded_by == superseded_by:
                    superseded.append(entry)
                    continue
                if not entry.pending:
                    continue
                entry = entry.model_copy(
                    update={
                        "state": MailboxState.SUPERSEDED,
                        "superseded_by": superseded_by,
                        "expired_reason": "superseded",
                        "expired_at": now,
                    }
                )
                await self._update(connection, args, entry, now)
                superseded.append(entry)
            return tuple(superseded)

    async def append_events(
        self, request_scope: str, run_id: str, events: Sequence[DomainEventEnvelope]
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            for event in events:
                await mc.append_events(
                    connection,
                    args,
                    run_key=run_id,
                    commit_key=f"mailbox-event:{event.event_id}",
                    expected_versions={},
                    events=(event,),
                    actor_ref=self._actor_ref,
                )


__all__ = ["PostgresCommandMailbox", "insert_mailbox_entry"]
