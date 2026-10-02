"""Scope-bound credential auth for the dedicated async subagent Agent Server (RRM-013 N8, RRM-009).

The parent's `AsyncSubagentContract.deployment_credential_ref`
(`environment:BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`) names the deployment's signing secret,
never a bearer token. The parent presents a **signed scope claim** as its bearer:

    bl1.<base64url(json{"scope", "iat", "exp"})>.<base64url(HMAC-SHA256(secret, "bl1.<body>"))>

The server verifies the signature in constant time against its own secret (Compose resolves
`BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` from the invoking shell through the tracked
`langgraph.async_subagents.env`; no value lives in the repository), takes the request scope
from the claim, and refuses a `x-belllabs-request-scope` header that names another scope.
Scope isolation therefore no longer rests on a client-asserted header under one static
token: a claim is valid for exactly one scope until its expiry. Threads and runs are scoped
by `metadata.request_scope`; assistants are read-only deployment topology; Store and crons
are disabled.
"""

from __future__ import annotations

import base64
import binascii
import hmac
import json
import os
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any

from langgraph_sdk import Auth

from app.agent_server.context import principal_from_auth_user

TOKEN_ENV = "BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN"
SCOPE_HEADER = b"x-belllabs-request-scope"
PARENT_IDENTITY = "belllabs-parent-operation"
CLAIM_PREFIX = "bl1"
DEFAULT_CLAIM_TTL = timedelta(hours=12)
MAX_CLAIM_TTL = timedelta(days=7)

auth = Auth()


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _unb64(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _signature(secret: str, signed: str) -> str:
    return _b64(hmac.new(secret.encode("utf-8"), signed.encode("utf-8"), sha256).digest())


def mint_scope_claim(
    secret: str,
    request_scope: str,
    *,
    now: datetime | None = None,
    ttl: timedelta = DEFAULT_CLAIM_TTL,
) -> str:
    """A bearer bound to one request scope, signed with the deployment secret."""

    if not secret or not request_scope.strip():
        raise ValueError("a scope claim requires the signing secret and a request scope")
    if ttl <= timedelta(0) or ttl > MAX_CLAIM_TTL:
        raise ValueError("scope claim lifetime must be positive and at most seven days")
    issued = now or datetime.now(UTC)
    body = _b64(
        json.dumps(
            {
                "scope": request_scope.strip(),
                "iat": int(issued.timestamp()),
                "exp": int((issued + ttl).timestamp()),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    signed = f"{CLAIM_PREFIX}.{body}"
    return f"{signed}.{_signature(secret, signed)}"


def claim_expiry(token: str) -> datetime | None:
    """The claim's expiry (unverified), so a holder can re-mint before it lapses."""

    parts = token.split(".")
    if len(parts) != 3 or parts[0] != CLAIM_PREFIX:
        return None
    try:
        payload = json.loads(_unb64(parts[1]))
        return datetime.fromtimestamp(int(payload["exp"]), tz=UTC)
    except (ValueError, TypeError, KeyError, binascii.Error):
        return None


def verify_scope_claim(token: str, *, secret: str, now: datetime | None = None) -> str | None:
    """The claim's scope when its signature and lifetime verify, otherwise `None`."""

    if not secret or not token:
        return None
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != CLAIM_PREFIX:
        return None
    signed = f"{parts[0]}.{parts[1]}"
    if not hmac.compare_digest(
        _signature(secret, signed).encode("utf-8"), parts[2].encode("utf-8")
    ):
        return None
    try:
        payload = json.loads(_unb64(parts[1]))
        scope = str(payload["scope"])
        issued = int(payload["iat"])
        expires = int(payload["exp"])
    except (ValueError, TypeError, KeyError, binascii.Error):
        return None
    moment = int((now or datetime.now(UTC)).timestamp())
    if not scope or expires <= issued or moment >= expires or moment < issued - 300:
        return None
    return scope


def verify_bearer(authorization: str, *, now: datetime | None = None) -> str | None:
    """The scope a bearer authorization proves, or `None` when it proves nothing."""

    expected = os.environ.get(TOKEN_ENV, "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token or not expected:
        return None
    return verify_scope_claim(token.strip(), secret=expected, now=now)


@auth.authenticate
async def authenticate(headers: dict[bytes, bytes]) -> Auth.types.MinimalUserDict:
    try:
        authorization = headers.get(b"authorization", b"").decode("ascii")
        asserted = headers.get(SCOPE_HEADER, b"").decode("ascii").strip()
    except UnicodeDecodeError:
        raise Auth.exceptions.HTTPException(
            status_code=401, detail="bearer scope claim required"
        ) from None
    scope = verify_bearer(authorization)
    if scope is None:
        raise Auth.exceptions.HTTPException(status_code=401, detail="invalid scope claim")
    if asserted and asserted != scope:
        raise Auth.exceptions.HTTPException(
            status_code=403, detail="asserted request scope differs from the claim"
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
