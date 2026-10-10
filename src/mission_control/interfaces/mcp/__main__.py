from __future__ import annotations

import argparse
import asyncio
from contextlib import AsyncExitStack
from typing import cast

import asyncpg

from mission_control.adapters.postgres.approvals.context import PostgresApprovalContextProbe
from mission_control.adapters.postgres.approvals.intents import PostgresGovernedIntentRepository
from mission_control.adapters.postgres.approvals.tasks import PostgresApprovalTaskRepository
from mission_control.adapters.postgres.capability.capability_search_repository import PostgresPool
from mission_control.adapters.postgres.chains.store import PostgresChainReader
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
from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control.stop_fence import PostgresStopFenceRepository
from mission_control.adapters.storage.control_plane_payloads import UnavailablePayloadStore
from mission_control.application.chains.service import ChainInspectionService
from mission_control.application.coordinator.coordinator_facade import (
    CoordinatorLimits,
    ProductionCoordinatorFacade,
)
from mission_control.application.execution.approvals_governed import (
    GovernedEffectService,
    GovernedToolRegistry,
)
from mission_control.application.frames.search import TranscriptSearchService
from mission_control.application.frames.transcript import TranscriptService
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.bootstrap.api import coordinator_inbox_installed
from mission_control.bootstrap.catalog import compose_catalog_service
from mission_control.bootstrap.coordinator_composition import (
    CoordinatorProductionDependencies,
    ReadOnlyCoordinatorRuntimeReadiness,
    build_production_coordinator_facade,
    load_coordinator_catalog_bindings,
)
from mission_control.bootstrap.manifests import compose_manifest_service
from mission_control.bootstrap.settings import Settings, get_settings
from mission_control.bootstrap.subscriptions import (
    compose_coordinator_inbox_service,
    compose_subscription_service,
)
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.extensions import ExtensionRegistry
from mission_control.interfaces.mcp.coordinator_server import (
    CoordinatorPrincipal,
    StaticPrincipalResolver,
    create_coordinator_server,
)
from mission_control.interfaces.mcp.governed_gateway import ScopedGovernedEffects
from mission_control.interfaces.mcp.human_task_tools import ScopedHumanTasks
from mission_control.interfaces.mcp.mission_tools import ScopedChains, ScopedManifests
from mission_control.interfaces.mcp.subscriptions import McpSessionHub, McpSubscriptionBridge
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
            chains=_chains(application_pool, principal.request_scope),
            manifests=_manifests(settings, application_pool, principal.request_scope),
            human_tasks=_human_tasks(application_pool, principal.request_scope),
            governed=_governed(application_pool, principal.request_scope),
            subscriptions=await _subscriptions(application_pool, principal.request_scope),
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


def _human_tasks(application_pool: PostgresPool, request_scope: str) -> ScopedHumanTasks | None:
    """MP-10 (SPEC-03): Human Tasks of the principal's canonical tenant scope. This
    development server has no Temporal client, so a resolution is observed by the gate's
    bounded poll instead of the wake-up signal."""

    try:
        parse_request_scope(request_scope)
    except ValueError:
        return None
    pool = cast(asyncpg.Pool, application_pool)
    service = HumanTaskService(
        PostgresHumanTaskRepository(pool),
        request_scope=request_scope,
        # MP-11: approval-origin tasks resolve through the same service.
        approvals=PostgresApprovalTaskRepository(pool),
    )
    return ScopedHumanTasks({request_scope: service})


def _governed(application_pool: PostgresPool, request_scope: str) -> ScopedGovernedEffects | None:
    """MP-11: prepare/review/execute for Mission-Control-owned tools. This development server
    registers no governed tool, so the gateway governs nothing until one is composed."""

    try:
        parse_request_scope(request_scope)
    except ValueError:
        return None
    pool = cast(asyncpg.Pool, application_pool)
    approvals = PostgresApprovalTaskRepository(pool)
    service = GovernedEffectService(
        PostgresGovernedIntentRepository(pool),
        approvals,
        GovernedToolRegistry(),
        request_scope=request_scope,
        probe=PostgresApprovalContextProbe(pool),
        fences=PostgresStopFenceRepository(pool),
    )
    return ScopedGovernedEffects({request_scope: service})


async def _subscriptions(
    application_pool: PostgresPool, request_scope: str
) -> McpSubscriptionBridge | None:
    """MP-15 (SPEC-04): durable subscriptions plus the coordinator inbox (poll/ack) where the
    database has release 1.2.0. Prompting needs run control, which this server does not
    compose; `stateless_http` sessions cannot receive live hints, so poll is the path."""

    try:
        parse_request_scope(request_scope)
    except ValueError:
        return None
    pool = cast(asyncpg.Pool, application_pool)
    inboxes = (
        {request_scope: compose_coordinator_inbox_service(pool, request_scope)}
        if await coordinator_inbox_installed(pool)
        else None
    )
    return McpSubscriptionBridge(
        {request_scope: compose_subscription_service(pool, request_scope)},
        McpSessionHub(),
        inboxes=inboxes,
    )


def _chains(application_pool: PostgresPool, request_scope: str) -> ScopedChains | None:
    """SPEC-04 (FT-D2): the Mission Chains of the principal's canonical tenant scope."""

    try:
        parse_request_scope(request_scope)
    except ValueError:
        return None
    pool = cast(asyncpg.Pool, application_pool)
    return ScopedChains(
        {
            request_scope: ChainInspectionService(
                PostgresChainReader(pool), request_scope=request_scope
            )
        }
    )


def _manifests(
    settings: Settings, application_pool: PostgresPool, request_scope: str
) -> ScopedManifests | None:
    """SPEC-05 (FT-E2): manifest compile for the principal's scope. Submit and start need run
    control and a launcher; the API composes them, this development server does not."""

    try:
        parse_request_scope(request_scope)
    except ValueError:
        return None
    if not settings.mission_control_catalog_scope:
        return None
    pool = cast(asyncpg.Pool, application_pool)
    catalog = compose_catalog_service(
        pool, request_scope=request_scope, catalog_scope=settings.mission_control_catalog_scope
    )
    service = compose_manifest_service(
        pool,
        request_scope=request_scope,
        catalog=catalog,
        extensions=ExtensionRegistry(),
        payload_store=UnavailablePayloadStore(),
    )
    return ScopedManifests({request_scope: service})


def main() -> None:
    asyncio.run(_serve(_parser().parse_args()))


if __name__ == "__main__":
    main()
