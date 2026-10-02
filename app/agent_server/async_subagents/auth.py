"""Credential-reference auth for the dedicated async subagent Agent Server.

The parent presents the credential named by `AsyncSubagentContract.deployment_credential_ref`
(`environment:BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`) as a bearer token and its request scope
in `x-belllabs-request-scope`. The server compares the token in constant time against its own
`BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`, which Compose resolves from the invoking shell through
the tracked `langgraph.async_subagents.env` (variable references only; no value in the repo).
Threads and runs are scoped by `metadata.request_scope`; assistants are read-only deployment
topology; Store and crons are disabled.
"""

from __future__ import annotations

import hmac
import os
from typing import Any

from langgraph_sdk import Auth

from app.agent_server.context import principal_from_auth_user

TOKEN_ENV = "BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"
SCOPE_HEADER = b"x-belllabs-request-scope"
PARENT_IDENTITY = "belllabs-parent-operation"

auth = Auth()


def verify_bearer(authorization: str) -> bool:
    expected = os.environ.get(TOKEN_ENV, "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or not expected:
        return False
    return hmac.compare_digest(token.encode("utf-8"), expected.encode("utf-8"))


@auth.authenticate
async def authenticate(headers: dict[bytes, bytes]) -> Auth.types.MinimalUserDict:
    try:
        authorization = headers.get(b"authorization", b"").decode("ascii")
        scope = headers.get(SCOPE_HEADER, b"").decode("ascii").strip()
    except UnicodeDecodeError:
        raise Auth.exceptions.HTTPException(
            status_code=401, detail="bearer token required"
        ) from None
    if not verify_bearer(authorization):
        raise Auth.exceptions.HTTPException(status_code=401, detail="invalid bearer token")
    if not scope:
        raise Auth.exceptions.HTTPException(
            status_code=403, detail="x-belllabs-request-scope is required"
        )
    return {
        "identity": PARENT_IDENTITY,
        "is_authenticated": True,
        "permissions": (f"request_scope:{scope}", "role:operator"),
    }


@auth.on
async def default_deny(ctx: Auth.types.AuthContext, value: Any) -> bool:
    del ctx, value
    raise Auth.exceptions.HTTPException(status_code=403, detail="unhandled resource denied")


@auth.on.threads
async def authorize_threads(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> dict[str, str]:
    return _scoped(ctx, value)


@auth.on(resources="runs")
async def authorize_runs(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> dict[str, str]:
    return _scoped(ctx, value)


@auth.on.assistants
async def authorize_assistants(
    ctx: Auth.types.AuthContext, value: dict[str, Any]
) -> dict[str, str]:
    del value
    action = str(getattr(ctx, "action", "")).lower().rsplit(".", maxsplit=1)[-1]
    if action not in {"read", "search"}:
        raise Auth.exceptions.HTTPException(
            status_code=403, detail="mutable assistants are disabled"
        )
    return {}


@auth.on.store()
async def deny_store(ctx: Auth.types.AuthContext, value: Any) -> bool:
    del ctx, value
    raise Auth.exceptions.HTTPException(status_code=403, detail="Store is disabled")


@auth.on.crons
async def deny_crons(ctx: Auth.types.AuthContext, value: Any) -> bool:
    del ctx, value
    raise Auth.exceptions.HTTPException(status_code=403, detail="crons are disabled")


def _scoped(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> dict[str, str]:
    principal = principal_from_auth_user(ctx.user)
    scope = next(iter(principal.request_scopes), "")
    metadata = value.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        raise Auth.exceptions.HTTPException(status_code=403, detail="invalid resource metadata")
    requested = str(metadata.get("request_scope") or scope)
    if requested != scope:
        raise Auth.exceptions.HTTPException(status_code=403, detail="cross-scope access denied")
    metadata["request_scope"] = scope
    return {"request_scope": scope}
