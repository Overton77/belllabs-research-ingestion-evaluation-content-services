from __future__ import annotations

from typing import Annotated

from fastapi import Header, HTTPException
from langgraph_sdk import Auth

from app.agent_server.auth import authenticate_bearer_token
from app.agent_server.context import AgentPrincipal


async def require_agent_principal(
    authorization: Annotated[str | None, Header(alias="Authorization")] = None,
) -> AgentPrincipal:
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(status_code=401, detail="bearer token required")
    try:
        return await authenticate_bearer_token(token)
    except Auth.exceptions.HTTPException as error:
        status_code = int(getattr(error, "status_code", 401))
        detail = str(getattr(error, "detail", "invalid bearer token"))
        raise HTTPException(status_code=status_code, detail=detail) from None
