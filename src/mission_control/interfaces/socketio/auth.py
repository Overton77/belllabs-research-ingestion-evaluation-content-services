"""Socket principal acquisition, reauthorization and expiry (SPEC-04 "Socket contract").

There is no second authentication path: the resolver calls the same principal dependency
the public HTTP API installs (`install_authentication` in `bootstrap/api.py` overrides
`get_mission_principal` with the `MissionTokenVerifier`: issuer, audience, signature,
`exp`, application claim, tenant grant) and then the same `ApplicationRegistry.resolve`
check that `authorize_application` runs before any service is consulted. A socket keeps
its bearer credential in process memory only to reverify it on every subscribe, ack,
replay, command and renewal; the credential's `exp` bounds the connection's lifetime.
"""

from __future__ import annotations

import base64
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException

from mission_control.application.installations.registry import (
    ApplicationRegistry,
    ApplicationScopeDenied,
    InstallationUnavailable,
    VerifiedApplicationIdentity,
)
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    principal_request_scope,
)

PrincipalResolver = Callable[[str, str], MissionPrincipal]


class SocketAuthRejected(PermissionError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


@dataclass(frozen=True)
class SocketCredential:
    principal: MissionPrincipal
    request_scope: str
    expires_at: float | None

    def expired(self, now: float | None = None) -> bool:
        return self.expires_at is not None and self.expires_at <= (now or time.time())

    def same_identity(self, other: SocketCredential) -> bool:
        left, right = self.principal, other.principal
        return (
            left.installation_id == right.installation_id
            and left.application_id == right.application_id
            and left.tenant_id == right.tenant_id
            and left.actor.actor_id == right.actor.actor_id
        )


def token_expiry(token: str) -> float | None:
    """`exp` of a token the verifier has already accepted (its claims equal the payload)."""

    try:
        segment = token.split(".")[1]
        data = base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
        value = json.loads(data).get("exp")
    except (IndexError, ValueError, AttributeError):
        return None
    return float(value) if isinstance(value, int | float) else None


def _code(error: HTTPException) -> str:
    detail: Any = error.detail
    if isinstance(detail, dict) and isinstance(detail.get("code"), str):
        return str(detail["code"])
    return "unauthorized"


def resolver_from_app(app: FastAPI) -> PrincipalResolver:
    """The public API's own principal dependency plus its registry scope check."""

    def resolve(application_id: str, token: str) -> MissionPrincipal:
        override = app.dependency_overrides.get(get_mission_principal)
        if override is None:
            raise SocketAuthRejected("authentication_not_configured")
        try:
            principal = override(application_id=application_id, authorization=f"Bearer {token}")
        except HTTPException as error:
            raise SocketAuthRejected(_code(error)) from None
        if not isinstance(principal, MissionPrincipal):
            raise SocketAuthRejected("authentication_not_configured")
        authorize_principal(app, application_id, principal)
        return principal

    return resolve


def authorize_principal(app: FastAPI, application_id: str, principal: MissionPrincipal) -> None:
    """The checks of `interfaces.http.mission_control.authorize_application`, without a
    request object: the principal's application and a live registry binding."""

    if principal.application_id != application_id:
        raise SocketAuthRejected("application_scope_denied")
    registry = getattr(app.state, "mission_control_registry", None)
    if not isinstance(registry, ApplicationRegistry):
        raise SocketAuthRejected("application_registry_unavailable")
    try:
        registry.resolve(
            application_id,
            VerifiedApplicationIdentity(
                issuer=principal.issuer,
                audiences=principal.audiences,
                application_id=principal.application_id,
                installation_id=principal.installation_id,
                tenant_id=principal.tenant_id,
            ),
        )
    except ApplicationScopeDenied:
        raise SocketAuthRejected("application_scope_denied") from None
    except InstallationUnavailable:
        raise SocketAuthRejected("application_unavailable") from None


def authenticate(resolver: PrincipalResolver, auth: Any) -> tuple[SocketCredential, str]:
    """Validate the Socket.IO `auth` payload `{application_id, token}`."""

    if not isinstance(auth, dict):
        raise SocketAuthRejected("bearer_token_required")
    application_id = auth.get("application_id")
    token = auth.get("token")
    if not isinstance(application_id, str) or not isinstance(token, str) or not token:
        raise SocketAuthRejected("bearer_token_required")
    if len(application_id) > 63 or len(token) > 16_384:
        raise SocketAuthRejected("invalid_token")
    principal = resolver(application_id, token)
    credential = SocketCredential(
        principal=principal,
        request_scope=principal_request_scope(principal),
        expires_at=token_expiry(token),
    )
    return credential, token


__all__ = [
    "PrincipalResolver",
    "SocketAuthRejected",
    "SocketCredential",
    "authenticate",
    "authorize_principal",
    "resolver_from_app",
    "token_expiry",
]
