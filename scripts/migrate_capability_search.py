from __future__ import annotations

import asyncio
import json

from app.config import Settings
from app.integrations.postgres import (
    apply_capability_search_migrations,
    create_postgres_pool,
)


async def _run() -> dict[str, object]:
    pool = await create_postgres_pool(Settings())
    try:
        await apply_capability_search_migrations(pool)
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT version
                FROM capability_search.schema_migrations
                ORDER BY version
                """
            )
        return {
            "authority": "capability_search",
            "applied_versions": [str(row["version"]) for row in rows],
        }
    finally:
        await pool.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run()), sort_keys=True))


if __name__ == "__main__":
    main()
