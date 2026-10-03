from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, Request

from mission_control.adapters.agent_server.async_subagents.auth import verify_bearer
from mission_control.adapters.agent_server.context import AgentPrincipal
from mission_control.adapters.agent_server.deployment import allowed_graphs, profile
from mission_control.adapters.agent_server.health import readiness_report
from mission_control.adapters.agent_server.hosting import hosted_graph
from mission_control.adapters.agent_server.runtime_composition import (
    configure_bootstrap_reconciler,
    reset_bootstrap_reconciler,
)
from mission_control.adapters.agent_server.tracing import configure_agent_server_tracing
from mission_control.adapters.postgres.connections import create_application_postgres_pool
from mission_control.adapters.postgres.runtime.runtime_authority import (
    PostgresBootstrapAuthority,
    PostgresBootstrapDecisionBridge,
)
from mission_control.application.recovery.runtime_bootstrap import RuntimeBootstrapReconciler
from mission_control.bootstrap.settings import get_settings
from mission_control.interfaces.http.dependencies import require_agent_principal
from mission_control.interfaces.http.graph_runtime_schemas import (
    router as graph_runtime_contract_router,
)

configure_agent_server_tracing()


@asynccontextmanager
async def lifespan(runtime_app: FastAPI) -> AsyncIterator[None]:
    profile()  # Invalid server topology is a startup error, not a request fallback.
    if os.environ.get("MISSION_CONTROL_AGENT_AUTHORITY_ENABLED", "0") != "1":
        runtime_app.state.authority_enabled = False
        yield
        return
    runtime_app.state.authority_enabled = True
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


def require_deployment_credential(
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    if verify_bearer(authorization or "") is None:
        raise HTTPException(status_code=401, detail="invalid scope claim")


@app.get("/belllabs/async-subagents/served-graphs")
async def served_graphs(
    _credential: Annotated[None, Depends(require_deployment_credential)],
) -> dict[str, object]:
    registration = hosted_graph()
    return {
        "graphs": [registration.definition.served.model_dump(mode="json")]
        if registration.definition.graph_id in allowed_graphs()
        else []
    }


@app.get("/v2/block-c/qualification")
async def qualification_marker() -> dict[str, str]:
    return {"surface": "block-c-qualification", "side_effects": "none", "profile": profile()}


@app.get("/v2/agent-runtime/readiness")
async def readiness(
    request: Request,
    principal: Annotated[AgentPrincipal, Depends(require_agent_principal)],
) -> dict[str, object]:
    del principal
    report = await readiness_report(request.app.state.readiness_probes)
    result = report.as_dict()
    result["bootstrap_authority_enabled"] = getattr(request.app.state, "authority_enabled", False)
    result["profile"] = profile()
    return result


app.include_router(
    graph_runtime_contract_router,
    dependencies=[Depends(require_agent_principal)],
)
