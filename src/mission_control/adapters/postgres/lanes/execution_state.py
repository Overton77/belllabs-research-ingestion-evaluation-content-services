"""PostgreSQL `LaneExecutionStateStore` over `mission_control.harness_execution` (FT-G2).

The row is opened by the frame store (migration 0027) before any lane write; this store
updates the lane columns of migration 0030 (G2 section) under forced RLS. The native
session and turn are written once: a later write naming another value is refused inside
the same row lock, so a resumed segment can never re-point an attempt at another agent.

MP-06: the session owner and the dispatch journal live in the existing `native_identity`
jsonb (runtime UPDATE grant of 0027) under `mc_session_owner` and `mc_dispatch`; they are
merged with `||` under the same `FOR UPDATE` row lock the frame append takes, so a frame
writer's read-modify-write of its own keys and these writes serialize and keep each other.
No new column: the dedicated columns/table are a proposed 0032+ delta (MP-06 handoff).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE, begin
from mission_control.application.execution.harness.dispatch import (
    DispatchClaim,
    DispatchKind,
    DispatchOutcome,
    DispatchRecord,
    SessionOwner,
    assert_holder,
    claim_ownership,
    dispatch_key,
    intend,
    renew_ownership,
    resolve,
)
from mission_control.application.execution.harness.state import (
    LaneExecutionState,
    LaneExecutionUpdate,
    merge_state,
)

_COLUMNS = (
    "native_session_ref",
    "native_turn_ref",
    "provider_cursor",
    "usage_disposition",
    "last_segment_at",
    "cursor_sdk_version",
    "bridge_state_root",
    "cloud_branch",
    "cloud_agent_url",
)
OWNER_KEY = "mc_session_owner"
DISPATCH_KEY = "mc_dispatch"


class HarnessExecutionNotOpened(LookupError):
    """The harness execution row does not exist yet (the frame store opens it)."""


def _identity(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, Mapping) else {}


def _owner(identity: Mapping[str, Any]) -> SessionOwner | None:
    raw = identity.get(OWNER_KEY)
    return SessionOwner.model_validate(raw) if isinstance(raw, Mapping) else None


def _dispatches(identity: Mapping[str, Any]) -> dict[str, DispatchRecord]:
    raw = identity.get(DISPATCH_KEY)
    if not isinstance(raw, Mapping):
        return {}
    return {key: DispatchRecord.model_validate(value) for key, value in raw.items()}


def _state(harness_execution_id: UUID, row: asyncpg.Record) -> LaneExecutionState:
    identity = _identity(row["native_identity"])
    return LaneExecutionState(
        harness_execution_id=harness_execution_id,
        owner=_owner(identity),
        dispatches=_dispatches(identity),
        **{name: row[name] for name in _COLUMNS},
    )


class PostgresLaneExecutionStateStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def _row(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        harness_execution_id: UUID,
        *,
        lock: bool,
    ) -> asyncpg.Record | None:
        return await connection.fetchrow(
            f"""
            SELECT {", ".join(_COLUMNS)}, native_identity FROM mission_control.harness_execution
            WHERE {SCOPE} AND harness_execution_id = $4
            {"FOR UPDATE" if lock else ""}
            """,
            *args,
            harness_execution_id,
        )

    async def _locked(
        self, connection: asyncpg.Connection, request_scope: str, harness_execution_id: UUID
    ) -> tuple[tuple[Any, ...], asyncpg.Record]:
        args = await begin(connection, request_scope)
        row = await self._row(connection, args, harness_execution_id, lock=True)
        if row is None:
            raise HarnessExecutionNotOpened(str(harness_execution_id))
        return args, row

    async def _merge_identity(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        harness_execution_id: UUID,
        patch: Mapping[str, Any],
    ) -> None:
        await connection.execute(
            f"""
            UPDATE mission_control.harness_execution
            SET native_identity = COALESCE(native_identity, '{{}}'::jsonb) || $5::jsonb,
                version = version + 1, updated_at = clock_timestamp()
            WHERE {SCOPE} AND harness_execution_id = $4
            """,
            *args,
            harness_execution_id,
            json.dumps(patch, sort_keys=True),
        )

    async def load(
        self, request_scope: str, harness_execution_id: UUID
    ) -> LaneExecutionState | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            row = await self._row(connection, args, harness_execution_id, lock=False)
        return None if row is None else _state(harness_execution_id, row)

    async def record(
        self, request_scope: str, harness_execution_id: UUID, update: LaneExecutionUpdate
    ) -> LaneExecutionState:
        async with self._pool.acquire() as connection, connection.transaction():
            args, row = await self._locked(connection, request_scope, harness_execution_id)
            merged = merge_state(_state(harness_execution_id, row), update)
            fields = update.fields()
            if fields:
                assignments = ", ".join(
                    f"{name} = ${index}" for index, name in enumerate(fields, start=5)
                )
                await connection.execute(
                    f"""
                    UPDATE mission_control.harness_execution
                    SET {assignments}, version = version + 1, updated_at = clock_timestamp()
                    WHERE {SCOPE} AND harness_execution_id = $4
                    """,
                    *args,
                    harness_execution_id,
                    *fields.values(),
                )
        return merged

    # --- MP-06 session ownership and dispatch journal ---------------------------------------

    async def claim_owner(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        *,
        owner_ref: str,
        generation: int,
        now: datetime,
        lease: timedelta,
    ) -> SessionOwner:
        async with self._pool.acquire() as connection, connection.transaction():
            args, row = await self._locked(connection, request_scope, harness_execution_id)
            current = _owner(_identity(row["native_identity"]))
            owner = claim_ownership(
                current, owner_ref=owner_ref, generation=generation, now=now, lease=lease
            )
            if owner != current:
                await self._merge_identity(
                    connection,
                    args,
                    harness_execution_id,
                    {OWNER_KEY: owner.model_dump(mode="json")},
                )
            return owner

    async def renew_owner(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        owner: SessionOwner,
        *,
        expires_at: datetime,
    ) -> SessionOwner:
        async with self._pool.acquire() as connection, connection.transaction():
            args, row = await self._locked(connection, request_scope, harness_execution_id)
            renewed = renew_ownership(
                _owner(_identity(row["native_identity"])), owner, expires_at=expires_at
            )
            await self._merge_identity(
                connection, args, harness_execution_id, {OWNER_KEY: renewed.model_dump(mode="json")}
            )
            return renewed

    async def assert_owner(
        self, request_scope: str, harness_execution_id: UUID, owner: SessionOwner
    ) -> None:
        # Read under the row lock so a takeover that committed first is always seen.
        async with self._pool.acquire() as connection, connection.transaction():
            _args, row = await self._locked(connection, request_scope, harness_execution_id)
            assert_holder(_owner(_identity(row["native_identity"])), owner)

    async def intend_dispatch(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        record: DispatchRecord,
        *,
        owner: SessionOwner,
    ) -> DispatchClaim:
        async with self._pool.acquire() as connection, connection.transaction():
            args, row = await self._locked(connection, request_scope, harness_execution_id)
            identity = _identity(row["native_identity"])
            assert_holder(_owner(identity), owner)
            journal = _dispatches(identity)
            claim = intend(journal.get(record.key), record, owner)
            if claim.fresh:
                journal[record.key] = claim.record
                await self._write_journal(connection, args, harness_execution_id, journal)
            return claim

    async def resolve_dispatch(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        kind: DispatchKind,
        idempotency_key: str,
        *,
        outcome: DispatchOutcome,
        owner: SessionOwner,
        at: datetime,
        native_ref: str | None = None,
        reason: str | None = None,
    ) -> DispatchRecord:
        async with self._pool.acquire() as connection, connection.transaction():
            args, row = await self._locked(connection, request_scope, harness_execution_id)
            identity = _identity(row["native_identity"])
            assert_holder(_owner(identity), owner)
            journal = _dispatches(identity)
            key = dispatch_key(kind, idempotency_key)
            record = resolve(
                journal.get(key),
                outcome=outcome,
                owner=owner,
                at=at,
                native_ref=native_ref,
                reason=reason,
            )
            if record != journal.get(key):
                journal[key] = record
                await self._write_journal(connection, args, harness_execution_id, journal)
            return record

    async def _write_journal(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        harness_execution_id: UUID,
        journal: Mapping[str, DispatchRecord],
    ) -> None:
        await self._merge_identity(
            connection,
            args,
            harness_execution_id,
            {DISPATCH_KEY: {key: value.model_dump(mode="json") for key, value in journal.items()}},
        )


__all__ = ["HarnessExecutionNotOpened", "PostgresLaneExecutionStateStore"]
