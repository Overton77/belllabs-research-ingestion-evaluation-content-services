"""Native Event Store retention (SPEC-03 "Retention").

`mission_control.frame_retention_policy` holds one row per application (absent = the
defaults: 30 days, keep closing frames, 8 KiB excerpt cap). `expire` deletes, within one
tenant scope, non-closing frames observed before `now - retain_days` and, only when the
policy says `keep_closing_frames = false`, closing frames too. Mission events are never
touched; a transcript rendered after expiry shows `frame_expired` placeholders.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE, begin
from mission_control.domain.frames.contracts import DEFAULT_EXCERPT_CAP_BYTES

DEFAULT_RETAIN_DAYS = 30


@dataclass(frozen=True)
class FrameRetentionPolicy:
    retain_days: int = DEFAULT_RETAIN_DAYS
    keep_closing_frames: bool = True
    excerpt_cap_bytes: int = DEFAULT_EXCERPT_CAP_BYTES


@dataclass(frozen=True)
class FrameExpiryReport:
    request_scope: str
    cutoff: datetime
    deleted_non_closing: int
    deleted_closing: int
    policy: FrameRetentionPolicy


async def _policy(connection: asyncpg.Connection, args: tuple[object, ...]) -> FrameRetentionPolicy:
    row = await connection.fetchrow(
        """
        SELECT retain_days, keep_closing_frames, excerpt_cap_bytes
        FROM mission_control.frame_retention_policy
        WHERE installation_id = $1 AND application_id = $2
        """,
        args[0],
        args[1],
    )
    if row is None:
        return FrameRetentionPolicy()
    return FrameRetentionPolicy(
        retain_days=int(row["retain_days"]),
        keep_closing_frames=bool(row["keep_closing_frames"]),
        excerpt_cap_bytes=int(row["excerpt_cap_bytes"]),
    )


class PostgresFrameRetention:
    def __init__(
        self, pool: asyncpg.Pool, *, actor_ref: str = "mission-control-frames-expire"
    ) -> None:
        self._pool = pool
        self._actor = actor_ref

    async def policy(self, request_scope: str) -> FrameRetentionPolicy:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            return await _policy(connection, args)

    async def set_policy(self, request_scope: str, policy: FrameRetentionPolicy) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            await connection.execute(
                """
                INSERT INTO mission_control.frame_retention_policy (
                    installation_id, application_id, retain_days, keep_closing_frames,
                    excerpt_cap_bytes, version, updated_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, 1, clock_timestamp(), clock_timestamp(), $6)
                ON CONFLICT (installation_id, application_id) DO UPDATE
                SET retain_days = EXCLUDED.retain_days,
                    keep_closing_frames = EXCLUDED.keep_closing_frames,
                    excerpt_cap_bytes = EXCLUDED.excerpt_cap_bytes,
                    version = mission_control.frame_retention_policy.version + 1,
                    updated_at = EXCLUDED.updated_at
                """,
                args[0],
                args[1],
                policy.retain_days,
                policy.keep_closing_frames,
                policy.excerpt_cap_bytes,
                self._actor,
            )

    async def expire(self, request_scope: str, *, now: datetime) -> FrameExpiryReport:
        if now.tzinfo is None:
            raise ValueError("expiry clock must be timezone-aware")
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            policy = await _policy(connection, args)
            cutoff = now - timedelta(days=policy.retain_days)
            rows = await connection.fetch(
                f"""
                DELETE FROM mission_control.provider_frame
                WHERE {SCOPE} AND observed_at < $4 AND (NOT closing OR NOT $5)
                RETURNING closing
                """,
                *args,
                cutoff,
                policy.keep_closing_frames,
            )
        closing = sum(1 for row in rows if row["closing"])
        return FrameExpiryReport(
            request_scope=request_scope,
            cutoff=cutoff,
            deleted_non_closing=len(rows) - closing,
            deleted_closing=closing,
            policy=policy,
        )
