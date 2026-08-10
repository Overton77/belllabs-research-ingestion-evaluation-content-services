from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Request

from app.agent_server.context import AgentPrincipal
from app.agent_server.health import readiness_report
from app.agent_server.runtime_composition import (
    configure_bootstrap_reconciler,
    reset_bootstrap_reconciler,
)
from app.agent_server.tracing import configure_agent_server_tracing
from app.api.dependencies import require_agent_principal
from app.api.graph_runtime_schemas import router as graph_runtime_contract_router
from app.application.postgres_runtime_authority import (
    PostgresBootstrapAuthority,
    PostgresBootstrapDecisionBridge,
)
from app.application.runtime_bootstrap import RuntimeBootstrapReconciler
from app.config import get_settings
from app.integrations.postgres import create_application_postgres_pool

configure_agent_server_tracing()


@asynccontextmanager
async def lifespan(runtime_app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    if not settings.has_application_postgres:

        async def unavailable() -> bool:
            return False

        runtime_app.state.readiness_probes["application_postgres_authority"] = unavailable
        yield
        return
    pool = await create_application_postgres_pool(settings)

    async def authority_ready() -> bool:
        async with pool.acquire() as connection:
            return bool(await connection.fetchval("SELECT 1"))

    runtime_app.state.readiness_probes["application_postgres_authority"] = authority_ready
    configure_bootstrap_reconciler(
        RuntimeBootstrapReconciler(
            PostgresBootstrapAuthority(pool),
            PostgresBootstrapDecisionBridge(pool),
        )
    )
    try:
        yield
    finally:
        reset_bootstrap_reconciler()
        runtime_app.state.readiness_probes.pop("application_postgres_authority", None)
        await pool.close()


app = FastAPI(
    title="BellLabs Agent Server routes",
    version="2.0.0",
    docs_url="/belllabs/docs",
    openapi_url="/belllabs/openapi.json",
    lifespan=lifespan,
)
app.state.readiness_probes = {}


@app.get("/v2/agent-runtime/readiness")
async def readiness(
    request: Request,
    principal: Annotated[AgentPrincipal, Depends(require_agent_principal)],
) -> dict[str, object]:
    del principal
    report = await readiness_report(request.app.state.readiness_probes)
    return report.as_dict()


app.include_router(
    graph_runtime_contract_router,
    dependencies=[Depends(require_agent_principal)],
)
