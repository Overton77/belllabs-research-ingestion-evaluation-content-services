"""Read-only startup validation of the configured Mission Control installation."""

from __future__ import annotations

import asyncio
import json

from mission_control.bootstrap.api import create_app


async def main() -> int:
    try:
        async with asyncio.timeout(30):
            application = create_app()
            async with application.router.lifespan_context(application):
                result: dict[str, object] = {
                    "ready": bool(application.state.mission_control_ready),
                    "storage_mode": "production_common",
                    "production_ready": all(
                        item.readiness.production_ready
                        for item in application.state.mission_control_compositions.values()
                    ),
                    "application_tenants": len(application.state.mission_control_compositions),
                    "schema_changes": False,
                }
    except Exception as exc:
        # Connection errors and validation inputs can contain credential values.
        result = {"ready": False, "error_type": type(exc).__name__}
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
