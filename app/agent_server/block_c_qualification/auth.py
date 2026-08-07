"""Local-only Block C qualification auth (RSA JWT from env).

This module is selected by ``langgraph.block_c.json`` / ``langgraph.block_c_n1.json``.
The root production ``langgraph.json`` must not reference this path.

The tracked ``langgraph.block_c.env`` contains variable references only;
Compose resolves their values from the invoking shell. Prefer the single-line
``BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64`` form — LangGraph CLI's dict ``env``
path double-indents Compose YAML and breaks on Windows, and multiline PEM must
never be embedded in generated Compose.

Required server env (values never committed):
- ``BELL_LABS_AGENT_AUTH_ISSUER``
- ``BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64`` (ASCII PEM, standard base64) **or**
  ``BELL_LABS_AGENT_AUTH_PUBLIC_KEY`` (PEM; avoid in Compose/env files)

Optional:
- ``BELL_LABS_AGENT_AUTH_AUDIENCE`` (default ``authenticated``)
- ``BELL_LABS_AGENT_AUTH_ALGORITHM`` (default ``RS256``)
"""

from __future__ import annotations

import base64
import binascii
import os
from functools import lru_cache
from typing import Any

from fastmcp.server.auth.providers.jwt import JWTVerifier
from langgraph_sdk import Auth

from app.agent_server.context import (
    AgentPrincipal,
    permissions_for,
    principal_from_auth_user,
    principal_from_claims,
)

auth = Auth()


def resolve_public_key_pem() -> str:
    """Return PEM text from env, preferring single-line base64."""

    public_key = os.environ.get("BELL_LABS_AGENT_AUTH_PUBLIC_KEY", "").strip()
    if public_key:
        return public_key
    public_key_b64 = os.environ.get("BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64", "").strip()
    if not public_key_b64:
        return ""
    try:
        decoded = base64.b64decode(public_key_b64, validate=True)
    except (binascii.Error, ValueError) as error:
        raise RuntimeError(
            "BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64 is not valid standard base64"
        ) from error
    try:
        return decoded.decode("ascii").strip()
    except UnicodeDecodeError as error:
        raise RuntimeError(
            "BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64 did not decode to ASCII PEM"
        ) from error


@lru_cache(maxsize=1)
def _verifier() -> JWTVerifier:
    issuer = os.environ.get("BELL_LABS_AGENT_AUTH_ISSUER", "").strip().rstrip("/")
    public_key = resolve_public_key_pem()
    if not issuer or not public_key:
        raise RuntimeError(
            "Block C qualification auth requires BELL_LABS_AGENT_AUTH_ISSUER and "
            "BELL_LABS_AGENT_AUTH_PUBLIC_KEY_B64 or BELL_LABS_AGENT_AUTH_PUBLIC_KEY "
            "(local RSA; never commit key material)"
        )
    return JWTVerifier(
        public_key=public_key,
        jwks_uri=None,
        issuer=issuer,
        audience=os.environ.get("BELL_LABS_AGENT_AUTH_AUDIENCE", "authenticated"),
        algorithm=os.environ.get("BELL_LABS_AGENT_AUTH_ALGORITHM", "RS256"),
        ssrf_safe=True,
    )


@auth.authenticate
async def authenticate(headers: dict[bytes, bytes]) -> Auth.types.MinimalUserDict:
    try:
        authorization = headers.get(b"authorization", b"").decode("ascii")
    except UnicodeDecodeError:
        authorization = ""
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise Auth.exceptions.HTTPException(status_code=401, detail="bearer token required")
    access_token = await _verifier().verify_token(token)
    if access_token is None:
        raise Auth.exceptions.HTTPException(status_code=401, detail="invalid bearer token")
    try:
        principal = principal_from_claims(dict(access_token.claims))
    except ValueError as error:
        raise Auth.exceptions.HTTPException(status_code=403, detail=str(error)) from None
    if len(principal.request_scopes) != 1:
        raise Auth.exceptions.HTTPException(
            status_code=403,
            detail="Agent Server identities require exactly one request scope",
        )
    return {
        "identity": principal.subject,
        "is_authenticated": True,
        "permissions": permissions_for(principal),
    }


@auth.on
async def default_deny(ctx: Auth.types.AuthContext, value: Any) -> bool:
    del ctx, value
    raise Auth.exceptions.HTTPException(status_code=403, detail="unhandled resource denied")


@auth.on.threads
async def authorize_threads(
    ctx: Auth.types.AuthContext,
    value: dict[str, Any],
) -> dict[str, str]:
    return _authorize_scoped_resource(ctx, value)


@auth.on(resources="runs")
async def authorize_runs(
    ctx: Auth.types.AuthContext,
    value: dict[str, Any],
) -> dict[str, str]:
    return _authorize_scoped_resource(ctx, value)


@auth.on.assistants
async def authorize_assistants(
    ctx: Auth.types.AuthContext,
    value: dict[str, Any],
) -> dict[str, str]:
    principal = principal_from_auth_user(ctx.user)
    _require_role(principal, ctx)
    action = str(getattr(ctx, "action", "")).lower().rsplit(".", maxsplit=1)[-1]
    if action not in {"read", "search"}:
        raise Auth.exceptions.HTTPException(
            status_code=403,
            detail="mutable assistants are disabled",
        )
    del value
    return {}


@auth.on.store()
async def deny_store(ctx: Auth.types.AuthContext, value: Any) -> bool:
    del ctx, value
    raise Auth.exceptions.HTTPException(
        status_code=403,
        detail="Store is disabled in Block C qualification",
    )


@auth.on.crons
async def deny_crons(ctx: Auth.types.AuthContext, value: Any) -> bool:
    del ctx, value
    raise Auth.exceptions.HTTPException(status_code=403, detail="crons are disabled")


def _authorize_scoped_resource(
    ctx: Auth.types.AuthContext,
    value: dict[str, Any],
) -> dict[str, str]:
    principal = principal_from_auth_user(ctx.user)
    _require_role(principal, ctx)
    metadata = value.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        raise Auth.exceptions.HTTPException(status_code=403, detail="invalid resource metadata")
    requested_scope = str(
        metadata.get("request_scope") or value.get("request_scope") or ""
    )
    if not requested_scope:
        requested_scope = next(iter(principal.request_scopes))
    if requested_scope not in principal.request_scopes:
        raise Auth.exceptions.HTTPException(status_code=403, detail="cross-scope access denied")
    metadata["request_scope"] = requested_scope
    return {"request_scope": requested_scope}


def _require_role(principal: AgentPrincipal, ctx: Auth.types.AuthContext) -> None:
    action = str(getattr(ctx, "action", "")).lower()
    write_action = any(
        marker in action
        for marker in ("create", "update", "delete", "put", "run", "cancel", "copy")
    )
    allowed = {"operator", "scheduler"} if write_action else {
        "operator",
        "scheduler",
        "auditor",
    }
    if not principal.roles & allowed:
        raise Auth.exceptions.HTTPException(
            status_code=403,
            detail="authenticated principal lacks runtime permission",
        )
