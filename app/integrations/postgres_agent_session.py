from __future__ import annotations

import json
from typing import Any

import asyncpg
from agents.items import TResponseInputItem
from agents.memory import Session
from agents.memory.session_settings import SessionSettings

from app.domain.operation_execution.contracts import OperationExecutionBinding


class PostgresAgentSessionFactory:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    def __call__(self, binding: OperationExecutionBinding, session_id: str) -> Session:
        return PostgresAgentSession(
            pool=self._pool,
            request_scope=binding.request_scope,
            session_id=session_id,
            binding_id=binding.binding_id,
        )


class PostgresAgentSession:
    """Supabase/PostgreSQL-backed, tenant-scoped SDK context projection."""

    session_settings: SessionSettings | None = None

    def __init__(
        self,
        *,
        pool: asyncpg.Pool,
        request_scope: str,
        session_id: str,
        binding_id: str,
    ) -> None:
        self._pool = pool
        self._request_scope = request_scope
        self.session_id = session_id
        self._binding_id = binding_id

    async def get_items(self, limit: int | None = None) -> list[TResponseInputItem]:
        query_limit = limit if limit is not None else 10_000
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, self._request_scope)
            rows = await connection.fetch(
                """
                SELECT message_data
                FROM (
                    SELECT message_id, message_data
                    FROM belllabs_control.agent_runtime_messages
                    WHERE request_scope = $1 AND binding_id = $2 AND session_id = $3
                    ORDER BY message_id DESC
                    LIMIT $4
                ) messages
                ORDER BY message_id
                """,
                self._request_scope,
                self._binding_id,
                self.session_id,
                query_limit,
            )
        return [_json(row["message_data"]) for row in rows]

    async def add_items(self, items: list[TResponseInputItem]) -> None:
        if not items:
            return
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, self._request_scope)
            await connection.execute(
                """
                INSERT INTO belllabs_control.agent_runtime_sessions
                    (request_scope, session_id, binding_id)
                VALUES ($1, $2, $3)
                ON CONFLICT (request_scope, binding_id, session_id)
                DO UPDATE SET updated_at = clock_timestamp()
                """,
                self._request_scope,
                self.session_id,
                self._binding_id,
            )
            await connection.executemany(
                """
                INSERT INTO belllabs_control.agent_runtime_messages
                    (request_scope, binding_id, session_id, message_data)
                VALUES ($1, $2, $3, $4::jsonb)
                """,
                [
                    (
                        self._request_scope,
                        self._binding_id,
                        self.session_id,
                        json.dumps(item),
                    )
                    for item in items
                ],
            )

    async def pop_item(self) -> TResponseInputItem | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, self._request_scope)
            payload = await connection.fetchval(
                """
                DELETE FROM belllabs_control.agent_runtime_messages
                WHERE message_id = (
                    SELECT message_id
                    FROM belllabs_control.agent_runtime_messages
                    WHERE request_scope = $1 AND binding_id = $2 AND session_id = $3
                    ORDER BY message_id DESC
                    LIMIT 1
                    FOR UPDATE
                )
                RETURNING message_data
                """,
                self._request_scope,
                self._binding_id,
                self.session_id,
            )
        return _json(payload) if payload is not None else None

    async def clear_session(self) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, self._request_scope)
            await connection.execute(
                """
                DELETE FROM belllabs_control.agent_runtime_sessions
                WHERE request_scope = $1 AND binding_id = $2 AND session_id = $3
                """,
                self._request_scope,
                self._binding_id,
                self.session_id,
            )


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
