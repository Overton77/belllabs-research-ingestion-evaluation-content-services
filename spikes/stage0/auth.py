"""Disposable local-only Agent Server authentication spike."""

from __future__ import annotations

from typing import Any

from langgraph_sdk import Auth

from local_identity import authenticate_local, load_local_identity, scope_from_identity
from policies import ALLOWED_STORE_PURPOSES, is_allowed_store_value

auth = Auth()


@auth.authenticate
async def authenticate(headers: dict[bytes, bytes]) -> Auth.types.MinimalUserDict:
    try:
        token = headers.get(b"x-stage0-token", b"").decode()
        claimed_scope = headers.get(b"x-request-scope", b"").decode()
    except UnicodeDecodeError:
        raise Auth.exceptions.HTTPException(
            status_code=401,
            detail="invalid local spike identity",
        ) from None
    identity = authenticate_local(token=token, claimed_request_scope=claimed_scope)
    if identity is None:
        raise Auth.exceptions.HTTPException(
            status_code=401,
            detail="invalid local spike identity",
        )
    return {"identity": f"stage0:{identity.request_scope}"}


@auth.on
async def default_deny(ctx: Auth.types.AuthContext, value: Any) -> bool:
    del ctx, value
    raise Auth.exceptions.HTTPException(status_code=403, detail="unhandled resource denied")


@auth.on.threads
async def authorize_threads(
    ctx: Auth.types.AuthContext,
    value: dict[str, Any],
) -> dict[str, str]:
    scope = scope_from_identity(ctx.user.identity)
    metadata = value.setdefault("metadata", {})
    metadata["request_scope"] = scope
    return {"request_scope": scope}


@auth.on.assistants
async def authorize_assistants(
    ctx: Auth.types.AuthContext,
    value: dict[str, Any],
) -> dict[str, str]:
    scope = scope_from_identity(ctx.user.identity)
    metadata = value.setdefault("metadata", {})
    metadata["request_scope"] = scope
    return {"request_scope": scope}


@auth.on.store()
async def authorize_store(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> bool:
    namespace = tuple(value.get("namespace", ()))
    scope = scope_from_identity(ctx.user.identity)
    identity = load_local_identity()
    if (
        identity is None
        or identity.request_scope != scope
        or len(namespace) != 3
        or namespace[0] != scope
        or namespace[1] != identity.environment
        or namespace[2] not in ALLOWED_STORE_PURPOSES
        or (
            "value" in value
            and not is_allowed_store_value(str(namespace[2]), value["value"])
        )
    ):
        raise Auth.exceptions.HTTPException(status_code=403, detail="Store namespace denied")
    return True


@auth.on.crons
async def deny_crons(ctx: Auth.types.AuthContext, value: Any) -> bool:
    del ctx, value
    raise Auth.exceptions.HTTPException(status_code=403, detail="crons disabled in Stage 0")
