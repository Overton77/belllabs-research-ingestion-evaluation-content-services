from __future__ import annotations

import asyncpg

from app.config import Settings


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
