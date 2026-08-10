from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from fastmcp.server.auth.providers.jwt import JWTVerifier
from langgraph_sdk import Auth
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError

from app.agent_server.context import (
    AgentPrincipal,
    permissions_for,
    principal_from_auth_user,
    principal_from_claims,
)
from app.domain.graph_runtime.contracts import RuntimeExecutionStatus
from app.domain.graph_runtime.identities import DIGEST_PATTERN

auth = Auth()


class RuntimeProjectionStoreValue(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    graph_id: str = Field(min_length=1, max_length=256)
    binding_id: str = Field(min_length=1, max_length=512)
    checkpoint_id: str | None = Field(default=None, min_length=1, max_length=512)
    status: RuntimeExecutionStatus
    digest: str = Field(pattern=DIGEST_PATTERN)
    updated_at: AwareDatetime


class ProceduralMemoryStoreValue(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    summary_ref: str = Field(min_length=1, max_length=1_024)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    source_refs: tuple[str, ...] = Field(max_length=100)
    created_at: AwareDatetime
    expires_at: AwareDatetime | None = None


@lru_cache(maxsize=1)
def _verifier() -> JWTVerifier:
    issuer = _configured_issuer()
    audience = os.environ.get("BELL_LABS_AGENT_AUTH_AUDIENCE", "authenticated")
    if not issuer:
        raise RuntimeError("BELL_LABS_AGENT_AUTH_ISSUER or SUPABASE_URL is required")
    public_key = os.environ.get("BELL_LABS_AGENT_AUTH_PUBLIC_KEY")
    configured_jwks_uri = os.environ.get("BELL_LABS_AGENT_AUTH_JWKS_URI", "").strip()
    return JWTVerifier(
        public_key=public_key,
        jwks_uri=(None if public_key else configured_jwks_uri or f"{issuer}/.well-known/jwks.json"),
        issuer=issuer,
        audience=audience,
        algorithm=os.environ.get("BELL_LABS_AGENT_AUTH_ALGORITHM", "RS256"),
        ssrf_safe=True,
    )


def authentication_is_configured() -> bool:
    return bool(_configured_issuer())


def _configured_issuer() -> str:
    explicit = os.environ.get("BELL_LABS_AGENT_AUTH_ISSUER", "").strip()
    if explicit:
        return explicit.rstrip("/")
    supabase_url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    return f"{supabase_url}/auth/v1" if supabase_url else ""


@auth.authenticate
async def authenticate(headers: dict[bytes, bytes]) -> Auth.types.MinimalUserDict:
    try:
        authorization = headers.get(b"authorization", b"").decode("ascii")
    except UnicodeDecodeError:
        authorization = ""
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise Auth.exceptions.HTTPException(status_code=401, detail="bearer token required")
    principal = await authenticate_bearer_token(token)
    return {
        "identity": principal.subject,
        "is_authenticated": True,
        "permissions": permissions_for(principal),
    }


async def authenticate_bearer_token(token: str) -> AgentPrincipal:
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
    return principal


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
    """Expose only deployment-defined assistants and reject mutable variants.

    The two registered assistants are immutable deployment topology, contain no
    tenant data, and must remain discoverable for schema/Studio inspection.
    Tenant-owned state lives in threads, runs, and Store namespaces. Allowing
    callers to create or mutate assistants would introduce tenant-specific
    configuration that the default assistants cannot be metadata-filtered from,
    so Stage 2 fails that capability closed.
    """

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
async def authorize_store(ctx: Auth.types.AuthContext, value: dict[str, Any]) -> bool:
    principal = principal_from_auth_user(ctx.user)
    _require_role(principal, ctx)
    raw_namespace = value.get("namespace")
    if not isinstance(raw_namespace, list | tuple):
        raise Auth.exceptions.HTTPException(
            status_code=403,
            detail="unscoped Store namespace operations are disabled",
        )
    namespace = tuple(str(item) for item in raw_namespace)
    if (
        len(namespace) != 3
        or namespace[0] not in principal.request_scopes
        or namespace[1] != os.environ.get("BELL_LABS_ENVIRONMENT", "development")
        or namespace[2] not in {"runtime_projection", "procedural_memory"}
        or ("value" in value and not _allowed_store_value(namespace[2], value["value"]))
    ):
        raise Auth.exceptions.HTTPException(status_code=403, detail="Store namespace denied")
    return True


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
    requested_scope = str(metadata.get("request_scope") or value.get("request_scope") or "")
    if not requested_scope:
        if len(principal.request_scopes) != 1:
            raise Auth.exceptions.HTTPException(
                status_code=403,
                detail="request scope must be explicit for multi-scope principals",
            )
        requested_scope = next(iter(principal.request_scopes))
    if requested_scope not in principal.request_scopes:
        raise Auth.exceptions.HTTPException(status_code=403, detail="cross-scope access denied")
    metadata["request_scope"] = requested_scope
    return {"request_scope": requested_scope}


def _require_role(principal: AgentPrincipal, ctx: Auth.types.AuthContext) -> None:
    action = str(getattr(ctx, "action", "")).lower()
    write_action = any(
        marker in action for marker in ("create", "update", "delete", "put", "run", "cancel")
    )
    allowed = (
        {"operator", "scheduler"}
        if write_action
        else {
            "operator",
            "scheduler",
            "auditor",
        }
    )
    if not principal.roles & allowed:
        raise Auth.exceptions.HTTPException(
            status_code=403,
            detail="authenticated principal lacks runtime permission",
        )


def _allowed_store_value(purpose: str, value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    model = (
        RuntimeProjectionStoreValue
        if purpose == "runtime_projection"
        else ProceduralMemoryStoreValue
    )
    try:
        model.model_validate(value)
    except ValidationError:
        return False
    return True
