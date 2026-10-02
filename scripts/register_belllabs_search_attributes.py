"""Register the BellLabs Search Attributes on the Temporal namespace (REQ-CP-EXEC-015).

The idempotent administrative step a deployment runs once per namespace before the API and
the workers start; readiness only verifies (`verify_belllabs_search_attributes`) and never
mutates the namespace. Uses `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE` from Settings.

    uv run --no-sync python scripts/register_belllabs_search_attributes.py
"""

from __future__ import annotations

import asyncio
import json

from app.config import get_settings
from app.integrations.temporal import create_temporal_client
from app.temporal.search_attributes import (
    register_belllabs_search_attributes,
    verify_belllabs_search_attributes,
)


async def main() -> int:
    settings = get_settings()
    client = await create_temporal_client(settings)
    added = await register_belllabs_search_attributes(client, settings.temporal_namespace)
    await verify_belllabs_search_attributes(client, settings.temporal_namespace)
    print(
        json.dumps(
            {
                "namespace": settings.temporal_namespace,
                "added": list(added),
                "verified": True,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
