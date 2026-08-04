from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from typing import Any

from fastmcp import Context
from fastmcp.server.auth import AccessToken
from fastmcp.server.dependencies import get_access_token

from app.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from app.mcp.coordinator_server import CoordinatorPrincipal


class VerifiedAccessTokenPrincipalResolver:
    """Derive coordinator identity only from FastMCP's verified token context."""

    def __init__(
        self,
        *,
        token_reader: Callable[[], AccessToken | None] = get_access_token,
        tenant_claim: str = "tenant_scope",
        roles_claim: str = "roles",
        permissions_claim: str = "permissions",
    ) -> None:
        self._token_reader = token_reader
        self._tenant_claim = tenant_claim
        self._roles_claim = roles_claim
        self._permissions_claim = permissions_claim

    async def resolve(self, _context: Context) -> CoordinatorPrincipal:
        token = self._token_reader()
        if token is None:
            raise _unauthenticated("an authenticated MCP access token is required")
        if token.expires_at is not None and token.expires_at <= int(
            datetime.now(UTC).timestamp()
        ):
            raise _unauthenticated("the MCP access token has expired")
        claims = token.claims
        actor_id = _nonblank(token.subject) or _claim_text(claims, "sub")
        if actor_id is None:
            actor_id = _nonblank(token.client_id)
        permitted_scopes = _claim_set(claims, "request_scopes")
        tenant_scope = _claim_text(claims, self._tenant_claim)
        request_scope = _claim_text(claims, "request_scope")
        if request_scope is None and len(permitted_scopes) == 1:
            request_scope = next(iter(permitted_scopes))
        if tenant_scope is None and len(permitted_scopes) == 1:
            tenant_scope = next(iter(permitted_scopes))
        if tenant_scope is None:
            raise _unauthenticated(
                "the verified MCP identity must select exactly one tenant scope"
            )
        if request_scope is None:
            request_scope = tenant_scope
        if permitted_scopes and request_scope not in permitted_scopes:
            raise _unauthenticated(
                "the verified MCP request scope is not permitted"
            )
        if actor_id is None:
            raise _unauthenticated("the verified MCP identity has no actor subject")
        roles = _claim_set(claims, self._roles_claim)
        permissions = frozenset(
            {
                *(_normalized(item) for item in token.scopes),
                *_claim_set(claims, self._permissions_claim),
            }
        )
        return CoordinatorPrincipal(
            actor_id=actor_id,
            tenant_scope=tenant_scope,
            roles=roles,
            permissions=permissions,
            request_scope=request_scope,
        )


def _claim_text(claims: Mapping[str, Any], name: str) -> str | None:
    value = claims.get(name)
    return _nonblank(value) if isinstance(value, str) else None


def _claim_set(claims: Mapping[str, Any], name: str) -> frozenset[str]:
    value = claims.get(name)
    if value is None:
        return frozenset()
    if isinstance(value, str):
        raw: Iterable[object] = value.replace(",", " ").split()
    elif isinstance(value, list | tuple | set | frozenset):
        raw = value
    else:
        raise _unauthenticated(
            f"the verified MCP {name} claim has an invalid shape"
        )
    normalized = frozenset(
        item for value in raw if isinstance(value, str) if (item := _normalized(value))
    )
    if len(normalized) != len(tuple(raw)):
        raise _unauthenticated(
            f"the verified MCP {name} claim contains invalid values"
        )
    return normalized


def _nonblank(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = _normalized(value)
    return normalized or None


def _normalized(value: str) -> str:
    return " ".join(value.split())


def _unauthenticated(message: str) -> CoordinatorDomainError:
    return CoordinatorDomainError(CoordinatorErrorCode.UNAUTHENTICATED, message)
