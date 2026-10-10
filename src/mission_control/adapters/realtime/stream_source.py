"""PostgreSQL `StreamSource`: scoped, read-only reads of the journal and the frame store.

Every read applies the transaction-local tenant scope (forced RLS denies other scopes), so a
target of another tenant resolves exactly like an absent one. Mission events are built by
the subscription store's envelope reader (`mc.event.v1`, by reference, no payload bodies)
and frames by the frame repository; this adapter adds target resolution and watermarks.
The existing unique keys serve every query: `(scope, mission_id, seq)` on `mission_event`
and `(harness_execution_id, generation, arrival_ordinal)` on `provider_frame`.

A `chain` target resolves by chain id or chain key under the same scope; its members are the
missions its links name, so a chain of another tenant or application is absent like any other
target. The retention probe reads only what retention leaves behind (the frame at a cursor,
the next retained ordinal, the application's `frame_retention_policy` cutoff); no new state.
"""

from __future__ import annotations

from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import scoped
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.application.streams.ports import (
    ChainState,
    ExecutionRef,
    FrameRetentionProbe,
    LinkedMission,
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
DEFAULT_RETAIN_DAYS = 30
_LINKS = """
    SELECT link.link_id, link.link_key, link.from_mission_id, link.to_mission_id,
           link.state, released.run_key AS released_run_key
    FROM mission_control.chain_link link
    LEFT JOIN mission_control.mission_run released
      ON released.installation_id = link.installation_id
     AND released.application_id = link.application_id
     AND released.tenant_id = link.tenant_id
     AND released.run_id = link.released_run_id
    WHERE link.installation_id = $1 AND link.application_id = $2
      AND link.tenant_id = $3 AND {condition}
    ORDER BY link.link_key, link.link_id
    LIMIT 256
"""


def _link(row: asyncpg.Record) -> LinkedMission:
    return LinkedMission(
        link_id=row["link_id"],
        link_key=row["link_key"],
        from_mission_id=row["from_mission_id"],
        to_mission_id=row["to_mission_id"],
        state=row["state"],
        released_run_key=row["released_run_key"],
    )


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
        if target.kind == "chain":
            return await self._resolve_chain(target)
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

    async def _chain_row(
        self, connection: asyncpg.Connection, args: tuple[object, ...], ident: str
    ) -> asyncpg.Record | None:
        try:
            chain_id: UUID | None = UUID(ident)
        except ValueError:
            chain_id = None
        return await connection.fetchrow(
            f"""
            SELECT chain_id, chain_key, lifecycle, phase, terminal_outcome, version
            FROM mission_control.mission_chain
            WHERE {SCOPE} AND (chain_id = $4 OR ($4::uuid IS NULL AND chain_key = $5))
            """,
            *args,
            chain_id,
            ident,
        )

    async def _resolve_chain(self, target: StreamTarget) -> ResolvedTarget:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            chain = await self._chain_row(connection, args, target.id)
            if chain is None:
                raise StreamTargetNotFound(target.id)
            # Members are the missions the links name and that this scope can see; a
            # mission of another tenant or application is never visible here.
            rows = await connection.fetch(
                f"""
                SELECT m.mission_id FROM mission_control.mission m
                WHERE {scoped("m")} AND m.mission_id IN (
                    SELECT unnest(ARRAY[link.from_mission_id, link.to_mission_id])
                    FROM mission_control.chain_link link
                    WHERE link.installation_id = $1 AND link.application_id = $2
                      AND link.tenant_id = $3 AND link.chain_id = $4
                )
                ORDER BY m.mission_id
                """,
                *args,
                chain["chain_id"],
            )
        members = tuple(row["mission_id"] for row in rows)
        if not members:
            raise StreamTargetNotFound(target.id)
        return ResolvedTarget(
            target=target, mission_id=members[0], chain_id=chain["chain_id"], members=members
        )

    async def chain(self, chain_id: UUID) -> ChainState:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            chain = await self._chain_row(connection, args, str(chain_id))
            if chain is None:
                raise StreamTargetNotFound(str(chain_id))
            links = await connection.fetch(
                _LINKS.format(condition="link.chain_id = $4"), *args, chain_id
            )
        return ChainState(
            chain_id=chain["chain_id"],
            chain_key=chain["chain_key"],
            lifecycle=chain["lifecycle"],
            phase=chain["phase"],
            terminal_outcome=chain["terminal_outcome"],
            version=int(chain["version"]),
            links=tuple(_link(row) for row in links),
        )

    async def executions(self, target: ResolvedTarget, *, limit: int) -> tuple[ExecutionRef, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            rows = await connection.fetch(
                """
                SELECT h.harness_execution_id, coalesce(h.generation, 1) AS generation
                FROM mission_control.harness_execution h
                JOIN mission_control.mission_run r
                  ON r.installation_id = h.installation_id
                 AND r.application_id = h.application_id
                 AND r.tenant_id = h.tenant_id AND r.run_id = h.run_id
                WHERE h.installation_id = $1 AND h.application_id = $2 AND h.tenant_id = $3
                  AND r.mission_id = $4 AND ($5::uuid IS NULL OR r.run_id = $5)
                ORDER BY h.created_at, h.harness_execution_id
                LIMIT $6
                """,
                *args,
                target.mission_id,
                target.run_id,
                limit,
            )
        return tuple(
            ExecutionRef(row["harness_execution_id"], int(row["generation"])) for row in rows
        )

    async def frame_retention(
        self, harness_execution_id: UUID, generation: int, *, after_ordinal: int
    ) -> FrameRetentionProbe:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            row = await connection.fetchrow(
                f"""
                SELECT
                  EXISTS (
                    SELECT 1 FROM mission_control.provider_frame
                    WHERE {SCOPE} AND harness_execution_id = $4 AND generation = $5
                      AND arrival_ordinal = $6
                  ) AS present,
                  (SELECT min(arrival_ordinal) FROM mission_control.provider_frame
                    WHERE {SCOPE} AND harness_execution_id = $4 AND generation = $5
                      AND arrival_ordinal > $6) AS next_ordinal,
                  least(
                    (SELECT min(observed_at) FROM mission_control.provider_frame
                      WHERE {SCOPE} AND harness_execution_id = $4 AND generation = $5),
                    (SELECT created_at FROM mission_control.harness_execution
                      WHERE {SCOPE} AND harness_execution_id = $4)
                  ) < clock_timestamp() - make_interval(days => coalesce(
                    (SELECT retain_days FROM mission_control.frame_retention_policy
                      WHERE installation_id = $1 AND application_id = $2),
                    $7)) AS horizon_passed
                """,
                *args,
                harness_execution_id,
                generation,
                after_ordinal,
                DEFAULT_RETAIN_DAYS,
            )
        assert row is not None
        return FrameRetentionProbe(
            cursor_present=bool(row["present"]),
            next_ordinal=None if row["next_ordinal"] is None else int(row["next_ordinal"]),
            horizon_passed=bool(row["horizon_passed"]),
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

    async def linked_missions(self, mission_id: UUID) -> tuple[LinkedMission, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, self._scope)
            rows = await connection.fetch(
                _LINKS.format(condition="link.from_mission_id = $4"), *args, mission_id
            )
        return tuple(_link(row) for row in rows)

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
