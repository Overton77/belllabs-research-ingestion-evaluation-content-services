"""Mission event reads for Transcript materialization (SPEC-03, C3).

Reads the run's canonical `mission_event` rows (by `seq`) under the caller's tenant
scope; forced RLS denies every other scope. The transcript itself is never persisted.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE, begin
from mission_control.application.frames.transcript import MAX_EVENTS, MissionEventRecord


def _payload(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value) if isinstance(value, dict) else {}


class PostgresMissionEventReader:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def run_identity(self, request_scope: str, run_key: str) -> UUID | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            value = await connection.fetchval(
                f"SELECT run_id FROM mission_control.mission_run WHERE {SCOPE} AND run_key = $4",
                *args,
                run_key,
            )
        return value if isinstance(value, UUID) else None

    async def events_for_run(
        self, request_scope: str, run_key: str, *, limit: int = MAX_EVENTS
    ) -> tuple[MissionEventRecord, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT e.seq, e.event_id, e.event_type, e.recorded_at, e.actor_ref, e.payload
                FROM mission_control.mission_event e
                JOIN mission_control.mission_run r
                  ON r.installation_id = e.installation_id
                 AND r.application_id = e.application_id
                 AND r.tenant_id = e.tenant_id AND r.run_id = e.run_id
                WHERE e.installation_id = $1 AND e.application_id = $2 AND e.tenant_id = $3
                  AND r.run_key = $4
                ORDER BY e.seq
                LIMIT $5
                """,
                *args,
                run_key,
                limit,
            )
        return tuple(
            MissionEventRecord(
                seq=int(row["seq"]),
                event_id=str(row["event_id"]),
                event_type=row["event_type"],
                recorded_at=row["recorded_at"],
                actor_ref=row["actor_ref"],
                payload=_payload(row["payload"]),
            )
            for row in rows
        )
