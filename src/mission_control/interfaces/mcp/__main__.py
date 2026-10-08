from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack
from typing import cast

import asyncpg

from mission_control.adapters.postgres.capability.capability_search_repository import PostgresPool
from mission_control.adapters.postgres.connections import (
    create_application_postgres_pool,
    create_postgres_pool,
)
from mission_control.adapters.postgres.control_plane.definition_repository import (
    PostgresDefinitionRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.frames.transcript_projection import (
    PostgresTranscriptDocuments,
)
from mission_control.adapters.postgres.frames.transcript_reads import PostgresMissionEventReader
from mission_control.application.coordinator.coordinator_facade import (
    CoordinatorLimits,
    ProductionCoordinatorFacade,
)
from mission_control.application.frames.search import TranscriptSearchService
from mission_control.application.frames.transcript import TranscriptService
from mission_control.bootstrap.coordinator_composition import (
    CoordinatorProductionDependencies,
    ReadOnlyCoordinatorRuntimeReadiness,
    build_production_coordinator_facade,
    load_coordinator_catalog_bindings,
)
from mission_control.bootstrap.settings import Settings, get_settings
from mission_control.contracts.identities import parse_request_scope
from mission_control.interfaces.mcp.coordinator_server import (
    CoordinatorPrincipal,
    StaticPrincipalResolver,
    create_coordinator_server,
)
from mission_control.interfaces.mcp.transcript_tools import ScopedTranscripts


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
    if not settings.mission_control_catalog_scope:
        raise RuntimeError("MISSION_CONTROL_CATALOG_SCOPE must identify the trusted installation")
    coordinator_skill, prompt_bindings = await load_coordinator_catalog_bindings(
        repository=PostgresDefinitionRepository(
            cast(asyncpg.Pool, application_pool),
            catalog_scope=settings.mission_control_catalog_scope,
        ),
    )
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
                    "workflow_run.read",
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
            transcripts=_transcripts(application_pool, principal.request_scope),
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


def _transcripts(application_pool: PostgresPool, request_scope: str) -> ScopedTranscripts | None:
    """SPEC-03 (C3): the transcript of the principal's canonical tenant scope, if any."""

    try:
        parse_request_scope(request_scope)
    except ValueError:
        return None
    pool = cast(asyncpg.Pool, application_pool)
    service = TranscriptService(
        PostgresMissionEventReader(pool),
        PostgresFrameRepository(pool),
        request_scope=request_scope,
    )
    # FT-C4: transcript search over the projection (run list needs Temporal Visibility,
    # which this standalone server does not compose).
    return ScopedTranscripts(
        {request_scope: service},
        searches={
            request_scope: TranscriptSearchService(service, PostgresTranscriptDocuments(pool))
        },
    )


def main() -> None:
    asyncio.run(_serve(_parser().parse_args()))


if __name__ == "__main__":
    main()
