"""Custom routes of the dedicated async subagent Agent Server.

`GET /belllabs/async-subagents/served-graphs` reports the exact identity of every hosted graph
(graph ID, revision, binding digest, deepagents version) so a parent verifies the served
identity before its first submission and on every reconnect (REQ-CP-DA-019). It is read-only
and guarded by the same credential reference as the Agent Protocol routes.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException

from app.agent_server.async_subagents.auth import verify_bearer
from app.agent_server.async_subagents.bindings import served_graph_identities

app = FastAPI(
    title="BellLabs async subagent Agent Server routes",
    version="1.0.0",
    docs_url=None,
    openapi_url=None,
)


def require_deployment_credential(
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    if verify_bearer(authorization or "") is None:
        raise HTTPException(status_code=401, detail="invalid scope claim")


@app.get("/belllabs/async-subagents/served-graphs")
async def served_graphs(
    _credential: Annotated[None, Depends(require_deployment_credential)],
) -> dict[str, object]:
    return {"graphs": [item.model_dump(mode="json") for item in served_graph_identities()]}
