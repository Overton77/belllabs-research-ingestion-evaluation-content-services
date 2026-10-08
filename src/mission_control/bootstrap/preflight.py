"""Read-only startup validation of the configured Mission Control installation."""

from __future__ import annotations

import asyncio
import json

from mission_control.adapters.temporal.client import (
    TemporalConnectionError,
    resolve_temporal_connection,
)
from mission_control.bootstrap.api import MissionDeployment, create_app, load_deployment
from mission_control.bootstrap.settings import Settings, get_settings


def temporal_targets(
    deployment: MissionDeployment, settings: Settings
) -> list[dict[str, str | bool]]:
    """FT-G7: which Temporal target (local or cloud) each application connects to.

    Reports the target, address, namespace and whether TLS and an API key are used; never
    the key itself.
    """

    targets: list[dict[str, str | bool]] = []
    for item in deployment.applications:
        if item.temporal is None:
            continue
        try:
            summary = resolve_temporal_connection(
                settings, address=item.temporal.address, namespace=item.temporal.namespace
            ).describe()
        except TemporalConnectionError as error:
            summary = {"target": settings.temporal_target, "error": str(error)}
        targets.append({"application_id": item.authentication.binding.application_id, **summary})
    return targets


async def main() -> int:
    try:
        async with asyncio.timeout(30):
            temporal = temporal_targets(load_deployment(), get_settings())
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
                    "temporal": temporal,
                }
    except Exception as exc:
        # Connection errors and validation inputs can contain credential values.
        result = {"ready": False, "error_type": type(exc).__name__}
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ready"] else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
