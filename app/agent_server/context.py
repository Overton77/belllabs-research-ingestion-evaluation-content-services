from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class AgentPrincipal:
    subject: str
    request_scopes: frozenset[str]
    roles: frozenset[str]

    def require_scope(self, request_scope: str) -> None:
        if request_scope not in self.request_scopes:
            raise PermissionError("runtime resource is outside the authenticated request scope")


def principal_from_claims(claims: dict[str, Any]) -> AgentPrincipal:
    subject = str(claims.get("sub") or claims.get("client_id") or "").strip()
    app_metadata = claims.get("app_metadata")
    if not isinstance(app_metadata, dict):
        app_metadata = {}
    scopes = _strings(
        claims.get("request_scopes")
        or app_metadata.get("request_scopes")
        or claims.get("request_scope")
    )
    roles = _strings(claims.get("roles") or app_metadata.get("roles") or claims.get("role"))
    if not subject or not scopes or not roles:
        raise ValueError("authenticated identity lacks subject, request scope, or role")
    return AgentPrincipal(
        subject=subject,
        request_scopes=frozenset(scopes),
        roles=frozenset(roles),
    )


def permissions_for(principal: AgentPrincipal) -> tuple[str, ...]:
    return tuple(
        sorted(
            {f"request_scope:{scope}" for scope in principal.request_scopes}
            | {f"role:{role}" for role in principal.roles}
        )
    )


def principal_from_auth_user(user: Any) -> AgentPrincipal:
    permissions = tuple(getattr(user, "permissions", ()) or ())
    return AgentPrincipal(
        subject=str(user.identity),
        request_scopes=frozenset(
            item.removeprefix("request_scope:")
            for item in permissions
            if item.startswith("request_scope:")
        ),
        roles=frozenset(
            item.removeprefix("role:")
            for item in permissions
            if item.startswith("role:")
        ),
    )


def require_runtime_scope(runtime: Any, request_scope: str) -> None:
    server_info = getattr(runtime, "server_info", None)
    if server_info is None:
        raise PermissionError("direct graph execution is disabled")
    user = getattr(server_info, "user", None)
    if user is None:
        raise PermissionError("authenticated Agent Server runtime identity is required")
    permissions = tuple(getattr(user, "permissions", ()) or ())
    if f"request_scope:{request_scope}" not in permissions:
        raise PermissionError("graph input is outside the authenticated request scope")


def _strings(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        candidates: Iterable[object] = value.replace(",", " ").split()
    elif isinstance(value, list | tuple | set | frozenset):
        candidates = value
    else:
        return ()
    return tuple(
        item
        for item in (str(candidate).strip() for candidate in candidates)
        if item
    )
