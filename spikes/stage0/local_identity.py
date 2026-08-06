"""Shared local-only principal mapper for the disposable Agent Server spike."""

from __future__ import annotations

import hmac
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class LocalIdentity:
    token: str
    request_scope: str
    environment: str


def load_local_identity() -> LocalIdentity | None:
    token = os.environ.get("STAGE0_LOCAL_AUTH_TOKEN", "").strip()
    request_scope = os.environ.get("STAGE0_LOCAL_REQUEST_SCOPE", "").strip()
    environment = os.environ.get("STAGE0_LOCAL_ENVIRONMENT", "").strip()
    if not token or not request_scope or not environment:
        return None
    return LocalIdentity(
        token=token,
        request_scope=request_scope,
        environment=environment,
    )


def authenticate_local(
    *,
    token: str,
    claimed_request_scope: str,
) -> LocalIdentity | None:
    """Authenticate against an environment-bound local principal.

    The caller-provided scope is only a consistency check. The returned scope always
    comes from the local environment, never from the request.
    """

    identity = load_local_identity()
    if identity is None:
        return None
    if not hmac.compare_digest(token, identity.token):
        return None
    if not hmac.compare_digest(claimed_request_scope, identity.request_scope):
        return None
    return identity


def scope_from_identity(identity: object) -> str:
    value = str(identity)
    prefix = "stage0:"
    if not value.startswith(prefix) or not value.removeprefix(prefix):
        raise ValueError("invalid Stage 0 identity")
    return value.removeprefix(prefix)
