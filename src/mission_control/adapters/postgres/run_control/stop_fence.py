"""PostgreSQL Stop Fence store (migration 0029 section F3; FT-F3).

Fence writes and effect admissions of one run serialize on a transaction-scoped advisory
lock keyed by the run's scope and identity, inside one explicit transaction each. So for
every effect id exactly one of these holds: the admission committed before the fence (it
stays admitted; it was dispatched before the stop) or after it (it is denied with
`STOP_FENCED`). Every row is insert-only; the fence is never deleted.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import (
    SCOPE,
    advisory_lock,
    begin,
    run_uuid,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.policies.stop_fence import (
    STOP_FENCED,
    EffectAdmission,
    FenceMilestone,
    FenceVerdict,
    ImmediateCancelReport,
    StopFence,
    fence_verdict,
)

ACTOR_REF = "mission-control-runtime/stop-fence"


def _lock_key(request_scope: str, run_id: str) -> str:
    return f"mc.stop_fence:{request_scope}:{run_id}"


def _fence(row: asyncpg.Record, request_scope: str) -> StopFence:
    return StopFence(
        request_scope=request_scope,
        run_id=row["run_key"],
        generation=row["generation"],
        command_id=row["command_id"],
        reason=row["reason"],
        requested_at=row["requested_at"],
        fenced_at=row["fenced_at"],
    )


class PostgresStopFenceRepository:
    def __init__(self, pool: asyncpg.Pool, *, actor_ref: str = ACTOR_REF) -> None:
        self._pool = pool
        self._actor_ref = actor_ref

    async def _newest(
        self, connection: asyncpg.Connection, args: tuple[Any, ...], run_key: str
    ) -> asyncpg.Record | None:
        return await connection.fetchrow(
            f"""
            SELECT stop_fence_id, run_key, generation, command_id, reason, requested_at,
                   fenced_at
            FROM mission_control.stop_fence
            WHERE {SCOPE} AND run_key = $4
            ORDER BY generation DESC
            LIMIT 1
            """,
            *args,
            run_key,
        )

    async def persist(self, fence: StopFence) -> StopFence:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, fence.request_scope)
            await advisory_lock(connection, _lock_key(fence.request_scope, fence.run_id))
            run = await run_uuid(connection, args, fence.run_id)
            await connection.execute(
                """
                INSERT INTO mission_control.stop_fence (
                    installation_id, application_id, tenant_id, stop_fence_id, run_id,
                    run_key, generation, command_id, reason, requested_at, fenced_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        greatest(clock_timestamp(), $10), $11)
                ON CONFLICT (installation_id, application_id, tenant_id, run_id, generation)
                DO NOTHING
                """,
                *args,
                uuid7(),
                run,
                fence.run_id,
                fence.generation,
                fence.command_id,
                fence.reason,
                fence.requested_at,
                self._actor_ref,
            )
            row = await connection.fetchrow(
                f"""
                SELECT run_key, generation, command_id, reason, requested_at, fenced_at
                FROM mission_control.stop_fence
                WHERE {SCOPE} AND run_id = $4 AND generation = $5
                """,
                *args,
                run,
                fence.generation,
            )
            assert row is not None
            return _fence(row, fence.request_scope)

    async def get(self, request_scope: str, run_id: str) -> StopFence | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            row = await self._newest(connection, args, run_id)
            return _fence(row, request_scope) if row is not None else None

    async def admit_effect(self, admission: EffectAdmission) -> FenceVerdict:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, admission.request_scope)
            await advisory_lock(connection, _lock_key(admission.request_scope, admission.run_id))
            run = await run_uuid(connection, args, admission.run_id)
            prior = await connection.fetchrow(
                """
                SELECT a.decision, f.command_id
                FROM mission_control.stop_fence_effect_admission AS a
                LEFT JOIN mission_control.stop_fence AS f
                  ON f.installation_id = a.installation_id
                 AND f.application_id = a.application_id
                 AND f.tenant_id = a.tenant_id
                 AND f.stop_fence_id = a.stop_fence_id
                WHERE a.installation_id = $1 AND a.application_id = $2 AND a.tenant_id = $3
                  AND a.run_id = $4 AND a.generation = $5 AND a.effect_ref = $6
                """,
                *args,
                run,
                admission.generation,
                admission.effect_ref,
            )
            if prior is not None:
                if prior["decision"] == "allow":
                    return FenceVerdict(decision="allow", effect_ref=admission.effect_ref)
                return FenceVerdict(
                    decision="deny",
                    reason_code=STOP_FENCED,
                    effect_ref=admission.effect_ref,
                    fence_command_id=prior["command_id"],
                    message=f"run is stop-fenced by immediate cancel {prior['command_id']}",
                )
            newest = await self._newest(connection, args, admission.run_id)
            fence = _fence(newest, admission.request_scope) if newest is not None else None
            verdict = fence_verdict(fence, admission)
            await connection.execute(
                """
                INSERT INTO mission_control.stop_fence_effect_admission (
                    installation_id, application_id, tenant_id, effect_admission_id, run_id,
                    run_key, generation, effect_ref, effect_kind, lane_profile, decision,
                    reason_code, stop_fence_id, decided_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
                        clock_timestamp(), $14)
                """,
                *args,
                uuid7(),
                run,
                admission.run_id,
                admission.generation,
                admission.effect_ref,
                admission.effect_kind,
                admission.lane_profile,
                verdict.decision,
                verdict.reason_code,
                newest["stop_fence_id"] if newest is not None and not verdict.allowed else None,
                self._actor_ref,
            )
            return verdict

    async def record_milestone(
        self,
        request_scope: str,
        run_id: str,
        generation: int | None,
        milestone: FenceMilestone,
        *,
        unit_key: str = "",
        recorded_at: datetime | None = None,
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            fence_id = await connection.fetchval(
                f"""
                SELECT stop_fence_id FROM mission_control.stop_fence
                WHERE {SCOPE} AND run_key = $4 AND ($5::integer IS NULL OR generation = $5)
                ORDER BY generation DESC
                LIMIT 1
                """,
                *args,
                run_id,
                generation,
            )
            if fence_id is None:
                return  # a normal cancel has no fence and no Stop Fence report
            await connection.execute(
                """
                INSERT INTO mission_control.stop_fence_milestone (
                    installation_id, application_id, tenant_id, stop_fence_milestone_id,
                    stop_fence_id, milestone, unit_key, recorded_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                ON CONFLICT (installation_id, application_id, tenant_id, stop_fence_id,
                             milestone, unit_key) DO NOTHING
                """,
                *args,
                uuid7(),
                fence_id,
                milestone,
                unit_key,
                recorded_at or datetime.now(UTC),
                self._actor_ref,
            )

    async def report(self, request_scope: str, run_id: str) -> ImmediateCancelReport | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            fence = await self._newest(connection, args, run_id)
            if fence is None:
                return None
            times = await connection.fetchrow(
                f"""
                SELECT min(recorded_at) FILTER (WHERE milestone = 'provider_acknowledged')
                           AS acknowledged,
                       max(recorded_at) FILTER (WHERE milestone = 'settled') AS settled
                FROM mission_control.stop_fence_milestone
                WHERE {SCOPE} AND stop_fence_id = $4
                """,
                *args,
                fence["stop_fence_id"],
            )
        return ImmediateCancelReport(
            command_id=fence["command_id"],
            run_id=run_id,
            generation=fence["generation"],
            requested_at=fence["requested_at"],
            fence_persisted_at=fence["fenced_at"],
            provider_acknowledged_at=times["acknowledged"] if times is not None else None,
            settled_at=times["settled"] if times is not None else None,
        )


__all__ = ["PostgresStopFenceRepository"]
