from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from typing import Any

from app.config import get_settings
from app.integrations.mongodb import create_mongodb
from app.integrations.neo4j import create_neo4j
from app.integrations.postgres import create_postgres_pool
from app.integrations.s3 import s3_client
from app.integrations.supabase import create_supabase
from app.integrations.temporal import create_temporal_client
from app.temporal.coordinator_runtime import (
    coordinator_task_queues,
    coordinator_worker_readiness,
)


async def _check(name: str, operation: Callable[[], Awaitable[Any]]) -> tuple[str, dict[str, Any]]:
    try:
        value = await operation()
        return name, {"ok": True, "detail": value}
    except Exception as exc:
        # Never include exception strings here: connection errors can echo secret-bearing URLs.
        return name, {"ok": False, "error_type": type(exc).__name__}


async def main() -> int:
    settings = get_settings()

    async def mongo() -> str:
        client, database = await create_mongodb(settings)
        await client.close()
        return f"connected:{database.name}"

    async def neo4j() -> str:
        driver = await create_neo4j(settings)
        await driver.close()
        return "connected"

    async def postgres() -> str:
        pool = await create_postgres_pool(settings)
        await pool.close()
        return "connected:no_app_tables_created"

    async def supabase() -> str:
        await create_supabase(settings)
        return "async_client_configured"

    async def temporal() -> str:
        client = await create_temporal_client(settings)
        await client.service_client.check_health()
        return "connected"

    async def coordinator_temporal_workers() -> dict[str, object]:
        if not settings.coordinator_launch_enabled:
            return {
                "enabled": False,
                "families": {
                    "StageGraph": {"available": False, "reason": "launch_disabled"},
                    "GoalDirected": {"available": False, "reason": "launch_disabled"},
                },
            }
        client = await create_temporal_client(settings)
        readiness = await coordinator_worker_readiness(
            client,
            task_queues=coordinator_task_queues(settings.temporal_task_queue),
        )
        families = {
            item.family: {
                "available": item.available,
                "task_queue": item.task_queue,
                "workflow_pollers": item.workflow_pollers,
            }
            for item in readiness
        }
        if not all(item.available for item in readiness):
            raise RuntimeError(
                "both coordinator Temporal workflow families require active pollers"
            )
        return {"enabled": True, "families": families}

    async def s3() -> str:
        async with s3_client(settings) as client:
            response = await client.list_buckets()
        return f"connected:bucket_count={len(response.get('Buckets', []))}"

    results = dict(
        await asyncio.gather(
            _check("mongodb_beanie", mongo),
            _check("neo4j_async", neo4j),
            _check("supabase_postgres", postgres),
            _check("supabase_async", supabase),
            _check("temporal", temporal),
            _check("coordinator_temporal_workers", coordinator_temporal_workers),
            _check("aws_async_s3", s3),
        )
    )
    print(json.dumps(results, indent=2, sort_keys=True))
    return 0 if all(item["ok"] for item in results.values()) else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
