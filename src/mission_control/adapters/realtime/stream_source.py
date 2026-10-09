"""PostgreSQL `StreamSource`: scoped, read-only reads of the journal and the frame store.

Every read applies the transaction-local tenant scope (forced RLS denies other scopes), so a
target of another tenant resolves exactly like an absent one. Mission events are built by
the subscription store's envelope reader (`mc.event.v1`, by reference, no payload bodies)
and frames by the frame repository; this adapter adds target resolution and watermarks.
The existing unique keys serve every query: `(scope, mission_id, seq)` on `mission_event`
and `(harness_execution_id, generation, arrival_ordinal)` on `provider_frame`.
"""

from __future__ import annotations

from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.application.streams.ports import (
    ResolvedTarget,
    StreamTargetNotFound,
)
from mission_control.application.subscriptions.ports import SubscriptionNotFound
from mission_control.domain.frames.contracts import ProviderFrame
from mission_control.domain.subscriptions.contracts import (
    MissionEventEnvelope,
    SubscriptionTarget,
)
from mission_control.domain.subscriptions.streams import StreamTarget

SCOPE = mc.SCOPE


def _uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise StreamTargetNotFound(value) from None


class PostgresStreamSource:
    def __init__(self, pool: asyncpg.Pool, request_scope: str) -> None:
        self._pool = pool
        self._scope = request_scope
        self._events = PostgresSubscriptionStore(pool, request_scope)
        self._frames = PostgresFrameRepository(pool)

    @property
    def request_scope(self) -> str:
        return self._scope

    async def resolve(self, target: StreamTarget) -> ResolvedTarget:
        if target.kind == "mission":
            try:
                resolved = await self._events.resolve_target("mission", _uuid(target.id))
            except SubscriptionNotFound:
                raise StreamTargetNotFound(target.id) from None
            return ResolvedTarget(target=target, mission_id=resolved.mission_id)
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            if target.kind == "run":
                row = await connection.fetchrow(
                    f"SELECT run_id, run_key, mission_id FROM mission_control.mission_run "
                    f"WHERE {SCOPE} AND run_key = $4",
                    *args,
                    target.id,
                )
                execution = None
            elif target.kind == "execution":
                execution = _uuid(target.id)
                row = await connection.fetchrow(
                    """
                    SELECT r.run_id, r.run_key, r.mission_id
                    FROM mission_control.harness_execution h
                    JOIN mission_control.mission_run r
                      ON r.installation_id = h.installation_id
                     AND r.application_id = h.application_id
                     AND r.tenant_id = h.tenant_id AND r.run_id = h.run_id
                    WHERE h.installation_id = $1 AND h.application_id = $2
                      AND h.tenant_id = $3 AND h.harness_execution_id = $4
                    """,
                    *args,
                    execution,
                )
            else:
                raise StreamTargetNotFound(target.id)
        if row is None:
            raise StreamTargetNotFound(target.id)
        return ResolvedTarget(
            target=target,
            mission_id=row["mission_id"],
            run_id=row["run_id"],
            run_key=row["run_key"],
            harness_execution_id=execution,
        )

    async def mission_high_watermark(self, mission_id: UUID) -> int:
        return await self._events.last_seq(mission_id)

    async def mission_low_watermark(self, mission_id: UUID) -> int | None:
        return await self._events.oldest_seq(mission_id)

    async def mission_events(
        self, target: ResolvedTarget, *, after_seq: int, upto_seq: int, limit: int
    ) -> tuple[MissionEventEnvelope, ...]:
        subscription_target = (
            SubscriptionTarget(kind="mission", mission_id=target.mission_id)
            if target.run_id is None
            else SubscriptionTarget(kind="run", mission_id=target.mission_id, run_id=target.run_id)
        )
        events = await self._events.events_after(subscription_target, after_seq, limit)
        return tuple(event for event in events if event.seq <= upto_seq)

    async def frame_generation(self, harness_execution_id: UUID) -> int | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            row = await connection.fetchrow(
                f"SELECT coalesce(generation, 1) AS generation "
                f"FROM mission_control.harness_execution "
                f"WHERE {SCOPE} AND harness_execution_id = $4",
                *args,
                harness_execution_id,
            )
        return None if row is None else int(row["generation"])

    async def frame_high_watermark(self, harness_execution_id: UUID, generation: int) -> int:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            value = await connection.fetchval(
                f"""
                SELECT max(arrival_ordinal) FROM mission_control.provider_frame
                WHERE {SCOPE} AND harness_execution_id = $4 AND generation = $5
                """,
                *args,
                harness_execution_id,
                generation,
            )
        return 0 if value is None else int(value)

    async def frames(
        self,
        harness_execution_id: UUID,
        generation: int,
        *,
        after_ordinal: int,
        upto_ordinal: int,
        limit: int,
    ) -> tuple[ProviderFrame, ...]:
        frames = await self._frames.frames_for_execution(
            self._scope,
            harness_execution_id,
            generation,
            after_ordinal=after_ordinal,
            limit=limit,
        )
        return tuple(frame for frame in frames if frame.arrival_ordinal <= upto_ordinal)


__all__ = ["PostgresStreamSource"]
