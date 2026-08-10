from __future__ import annotations

from pathlib import Path

import asyncpg

from app.config import Settings

MIGRATIONS_ROOT = Path(__file__).resolve().parents[1] / "migrations"


def _is_capability_search_migration(path: Path) -> bool:
    return "_capability_search" in path.stem


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
            identity = await connection.fetchrow(
                """
                SELECT role.rolsuper, role.rolbypassrls,
                       pg_has_role(
                           current_user, 'belllabs_control_runtime', 'member'
                       ) AS runtime_member,
                       pg_has_role(
                           current_user, 'belllabs_family_repository_writer', 'member'
                       ) AS family_writer_member
                FROM pg_roles role
                WHERE role.rolname = current_user
                """
            )
            if (
                identity is None
                or identity["rolsuper"]
                or identity["rolbypassrls"]
                or not identity["runtime_member"]
                or identity["family_writer_member"]
            ):
                raise RuntimeError(
                    "application PostgreSQL runtime identity must be a non-privileged "
                    "member of belllabs_control_runtime and must not hold family writer authority"
                )
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
            identity = await connection.fetchrow(
                """
                SELECT role.rolsuper, role.rolbypassrls,
                       pg_has_role(
                           current_user, 'belllabs_control_runtime', 'member'
                       ) AS runtime_member,
                       pg_has_role(
                           current_user, 'belllabs_family_repository_writer', 'member'
                       ) AS family_writer_member
                FROM pg_roles role
                WHERE role.rolname = current_user
                """
            )
            if (
                identity is None
                or identity["rolsuper"]
                or identity["rolbypassrls"]
                or identity["runtime_member"]
                or not identity["family_writer_member"]
            ):
                raise RuntimeError(
                    "family writer PostgreSQL identity must be non-privileged, distinct from "
                    "belllabs_control_runtime, and a member of "
                    "belllabs_family_repository_writer"
                )
    except Exception:
        await pool.close()
        raise
    return pool


async def create_application_migration_pool(settings: Settings) -> asyncpg.Pool:
    """Connect with the application schema-owner identity."""
    return await asyncpg.create_pool(
        dsn=settings.application_migration_postgres_dsn,
        min_size=1,
        max_size=2,
        command_timeout=30,
    )


async def apply_application_migrations(pool: asyncpg.Pool) -> None:
    """Apply ordered, application-owned migrations under one advisory lock."""
    migration_paths = [
        path
        for path in sorted(MIGRATIONS_ROOT.glob("*.sql"))
        if not _is_capability_search_migration(path)
    ]
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock($1)", 0x42454C4C)
        await connection.execute(
            """
            CREATE SCHEMA IF NOT EXISTS belllabs_control;
            CREATE TABLE IF NOT EXISTS belllabs_control.schema_migrations (
                version text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            );
            """
        )
        applied = {
            row["version"]
            for row in await connection.fetch(
                "SELECT version FROM belllabs_control.schema_migrations"
            )
        }
        for path in migration_paths:
            if path.name in applied:
                continue
            await connection.execute(path.read_text(encoding="utf-8"))
            await connection.execute(
                "INSERT INTO belllabs_control.schema_migrations (version) VALUES ($1)",
                path.name,
            )


async def apply_capability_search_migrations(pool: asyncpg.Pool) -> None:
    """Apply ordered disposable Supabase/pgvector projection migrations only."""
    migration_paths = [
        path
        for path in sorted(MIGRATIONS_ROOT.glob("*.sql"))
        if _is_capability_search_migration(path)
    ]
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT pg_advisory_xact_lock($1)", 0x42454C53)
        await connection.execute(
            """
            CREATE SCHEMA IF NOT EXISTS capability_search;
            CREATE TABLE IF NOT EXISTS capability_search.schema_migrations (
                version text PRIMARY KEY,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            );
            """
        )
        applied = {
            row["version"]
            for row in await connection.fetch(
                "SELECT version FROM capability_search.schema_migrations"
            )
        }
        for path in migration_paths:
            if path.name in applied:
                continue
            await connection.execute(path.read_text(encoding="utf-8"))
            await connection.execute(
                """
                INSERT INTO capability_search.schema_migrations (version)
                VALUES ($1)
                """,
                path.name,
            )
