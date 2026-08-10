from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack

from app.application.coordinator_composition import (
    CoordinatorProductionDependencies,
    ReadOnlyCoordinatorRuntimeReadiness,
    build_production_coordinator_facade,
    load_coordinator_catalog_bindings,
)
from app.application.coordinator_facade import (
    CoordinatorLimits,
    ProductionCoordinatorFacade,
)
from app.application.postgres_capability_search_repository import PostgresPool
from app.config import Settings, get_settings
from app.integrations.mongodb import create_mongodb
from app.integrations.postgres import (
    create_application_postgres_pool,
    create_postgres_pool,
)
from app.mcp.coordinator_server import (
    CoordinatorPrincipal,
    StaticPrincipalResolver,
    create_coordinator_server,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run the BellLabs Coordinator FastMCP server over Streamable HTTP "
            "for local dashboard and Cursor MCP testing."
        )
    )
    settings = get_settings()
    parser.add_argument("--host", default=settings.api_host)
    parser.add_argument("--port", type=int, default=8010)
    parser.add_argument("--path", default="/mcp")
    parser.add_argument("--tenant-scope", default="global")
    parser.add_argument("--request-scope")
    parser.add_argument("--actor-id", default="coordinator-http-dev")
    parser.add_argument(
        "--skip-external-discovery",
        action="store_true",
        help="Disable MCP Registry and npx skills discovery for faster local startup.",
    )
    return parser


async def _build_facade(
    settings: Settings,
    *,
    capability_pool: PostgresPool,
    application_pool: PostgresPool,
    skip_external_discovery: bool,
) -> ProductionCoordinatorFacade:
    coordinator_skill, prompt_bindings = await load_coordinator_catalog_bindings()
    effective_settings = settings.model_copy(
        update={
            "coordinator_launch_enabled": False,
            "external_capability_discovery_enabled": (
                settings.external_capability_discovery_enabled and not skip_external_discovery
            ),
        }
    )
    return build_production_coordinator_facade(
        settings=effective_settings,
        capability_postgres_pool=capability_pool,
        application_postgres_pool=application_pool,
        dependencies=CoordinatorProductionDependencies(
            readiness=ReadOnlyCoordinatorRuntimeReadiness(),
            coordinator_skill=coordinator_skill,
            prompt_bindings=prompt_bindings,
        ),
        limits=CoordinatorLimits(request_timeout_seconds=120),
    )


async def _serve(args: argparse.Namespace) -> None:
    settings = get_settings()
    async with AsyncExitStack() as stack:
        mongo_client, _database = await create_mongodb(settings)
        stack.push_async_callback(mongo_client.close)
        capability_pool = await create_postgres_pool(settings)
        stack.push_async_callback(capability_pool.close)
        application_pool = await create_application_postgres_pool(settings)
        stack.push_async_callback(application_pool.close)
        facade = await _build_facade(
            settings,
            capability_pool=capability_pool,
            application_pool=application_pool,
            skip_external_discovery=args.skip_external_discovery,
        )
        principal = CoordinatorPrincipal(
            actor_id=args.actor_id,
            tenant_scope=args.tenant_scope,
            roles=frozenset({"coordinator_planner", "operator"}),
            permissions=frozenset(
                {
                    "catalog.read",
                    "capability.discover",
                    "workflow.design.validate",
                    "workflow.prepare",
                    "workflow.launch",
                    "workflow.result.read",
                }
            ),
            request_scope=args.request_scope or args.tenant_scope,
        )
        server = create_coordinator_server(
            facade,
            StaticPrincipalResolver(principal),
        )
        await server.run_http_async(
            transport="streamable-http",
            host=args.host,
            port=args.port,
            path=args.path,
            stateless_http=True,
            json_response=True,
            show_banner=True,
        )


def main() -> None:
    asyncio.run(_serve(_parser().parse_args()))


if __name__ == "__main__":
    main()
