from __future__ import annotations

import asyncpg

from mission_control.bootstrap.settings import Settings

CAPABILITY_ROLES = (
    "mission_control_runtime",
    "mission_control_family_writer",
    "mission_control_catalog_writer",
    "mission_control_outbox_worker",
    "mission_control_readonly",
)


async def _require_capability_role(connection: asyncpg.Connection, capability: str) -> None:
    """A pool login holds exactly one capability role and no superuser/RLS bypass."""
    identity = await connection.fetchrow(
        """
        SELECT role.rolsuper, role.rolbypassrls,
               ARRAY(
                   SELECT member.rolname FROM pg_catalog.pg_roles member
                   WHERE member.rolname = ANY($1::text[])
                     AND pg_has_role(current_user, member.oid, 'MEMBER')
                   ORDER BY member.rolname
               ) AS capabilities
        FROM pg_catalog.pg_roles role
        WHERE role.rolname = current_user
        """,
        list(CAPABILITY_ROLES),
    )
    if (
        identity is None
        or identity["rolsuper"]
        or identity["rolbypassrls"]
        or list(identity["capabilities"]) != [capability]
    ):
        raise RuntimeError(
            f"PostgreSQL pool identity must be a non-privileged member of exactly {capability}"
        )


async def create_postgres_pool(settings: Settings) -> asyncpg.Pool:
    """Connect to Supabase Postgres without creating application tables or migrations."""
    pool = await asyncpg.create_pool(
        dsn=settings.postgres_dsn,
        min_size=1,
        max_size=4,
        command_timeout=10,
    )
    async with pool.acquire() as connection:
        await connection.fetchval("SELECT 1")
    return pool


async def create_application_postgres_pool(settings: Settings) -> asyncpg.Pool:
    """Connect only to the application-owned PostgreSQL authority."""
    pool = await asyncpg.create_pool(
        dsn=settings.application_postgres_dsn,
        min_size=1,
        max_size=8,
        command_timeout=30,
    )
    try:
        async with pool.acquire() as connection:
            await connection.fetchval("SELECT 1")
            await _require_capability_role(connection, "mission_control_runtime")
    except Exception:
        await pool.close()
        raise
    return pool


async def create_application_family_writer_pool(settings: Settings) -> asyncpg.Pool:
    """Connect with the distinct least-privilege atomic-family repository identity."""

    pool = await asyncpg.create_pool(
        dsn=settings.application_family_writer_postgres_dsn,
        min_size=1,
        max_size=4,
        command_timeout=30,
    )
    try:
        async with pool.acquire() as connection:
            await _require_capability_role(connection, "mission_control_family_writer")
    except Exception:
        await pool.close()
        raise
    return pool
