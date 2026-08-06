from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, Request

from app.agent_server.context import AgentPrincipal
from app.agent_server.health import readiness_report
from app.agent_server.tracing import configure_agent_server_tracing
from app.api.dependencies import require_agent_principal
from app.api.graph_runtime_schemas import router as graph_runtime_contract_router

configure_agent_server_tracing()

app = FastAPI(
    title="BellLabs Agent Server routes",
    version="2.0.0",
    docs_url="/belllabs/docs",
    openapi_url="/belllabs/openapi.json",
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
