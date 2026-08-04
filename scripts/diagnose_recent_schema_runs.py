from __future__ import annotations

import asyncio
import json

from app.config import get_settings
from app.integrations.postgres import create_application_postgres_pool


def _json_object(value: object) -> dict[str, object]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise TypeError("workflow run projection must decode to a JSON object")
    return value


async def _run() -> list[dict[str, object]]:
    pool = await create_application_postgres_pool(get_settings())
    try:
        async with pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    "SELECT set_config('belllabs.request_scope', $1, true)",
                    "global",
                )
                rows = await connection.fetch(
                    """
                    SELECT run_id, request_scope, idempotency_issuer, request_id,
                           phase, version, updated_at, projection
                    FROM belllabs_control.workflow_runs
                    ORDER BY updated_at DESC
                    LIMIT 20
                    """
                )
        results: list[dict[str, object]] = []
        for row in rows:
            projection = _json_object(row["projection"])
            results.append({
                "run_id": row["run_id"],
                "request_scope": row["request_scope"],
                "idempotency_issuer": row["idempotency_issuer"],
                "request_id": row["request_id"],
                "phase": row["phase"],
                "version": row["version"],
                "updated_at": row["updated_at"].isoformat(),
                "terminal_outcome": projection.get("terminal_outcome"),
                "terminal_reason": projection.get("terminal_reason"),
            })
        return results
    finally:
        await pool.close()


def main() -> None:
    print(json.dumps(asyncio.run(_run()), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
