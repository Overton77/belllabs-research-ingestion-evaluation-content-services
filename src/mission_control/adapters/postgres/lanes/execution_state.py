"""PostgreSQL `LaneExecutionStateStore` over `mission_control.harness_execution` (FT-G2).

The row is opened by the frame store (migration 0027) before any lane write; this store
updates the lane columns of migration 0030 (G2 section) under forced RLS. The native
session and turn are written once: a later write naming another value is refused inside
the same row lock, so a resumed segment can never re-point an attempt at another agent.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE, begin
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


class HarnessExecutionNotOpened(LookupError):
    """The harness execution row does not exist yet (the frame store opens it)."""


def _state(harness_execution_id: UUID, row: asyncpg.Record) -> LaneExecutionState:
    return LaneExecutionState(
        harness_execution_id=harness_execution_id,
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
            SELECT {", ".join(_COLUMNS)} FROM mission_control.harness_execution
            WHERE {SCOPE} AND harness_execution_id = $4
            {"FOR UPDATE" if lock else ""}
            """,
            *args,
            harness_execution_id,
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
            args = await begin(connection, request_scope)
            row = await self._row(connection, args, harness_execution_id, lock=True)
            if row is None:
                raise HarnessExecutionNotOpened(str(harness_execution_id))
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


__all__ = ["HarnessExecutionNotOpened", "PostgresLaneExecutionStateStore"]
