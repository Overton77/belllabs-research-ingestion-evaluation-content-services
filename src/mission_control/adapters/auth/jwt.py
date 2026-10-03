"""Pinned asymmetric JWT verification and operator-owned grant mapping.

Uses installed joserfc JWT/JWK APIs; no database is selected from unverified claims.
Public JWKS files are deployment inputs, never token-supplied URLs or key material.
"""

from __future__ import annotations

import base64
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast
from uuid import UUID

from joserfc import jwt
from joserfc.errors import JoseError
from joserfc.jwk import KeySet, KeySetSerialization
from pydantic import BaseModel, ConfigDict, Field, model_validator

from mission_control.application.installations.registry import (
    ApplicationBinding,
    VerifiedApplicationIdentity,
)
from mission_control.contracts.json import parse_json_object
from mission_control.domain.policies.contracts import ActorContext


class ActorGrant(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    subject: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)
    tenant_ids: frozenset[UUID] = Field(min_length=1)
    permissions: frozenset[str] = frozenset()
    authority_refs: frozenset[str] = frozenset()
    sponsorship_refs: frozenset[str] = frozenset()
    approval_refs: frozenset[str] = frozenset()


class ApplicationAuthentication(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    binding: ApplicationBinding
    issuer: str = Field(min_length=1)
    audience: str = Field(min_length=1)
    public_jwks_file: Path
    algorithms: frozenset[Literal["RS256", "ES256", "EdDSA"]] = frozenset({"RS256"})
    application_claim: tuple[str, ...] = ("app_metadata", "application_id")
    tenant_claim: tuple[str, ...] = ("app_metadata", "tenant_id")
    grants: tuple[ActorGrant, ...] = ()

    @model_validator(mode="after")
    def trusted_configuration(self) -> ApplicationAuthentication:
        if self.issuer not in self.binding.accepted_issuers:
            raise ValueError("authentication issuer is not in the binding")
        if self.audience not in self.binding.accepted_audiences:
            raise ValueError("authentication audience is not in the binding")
        if not self.algorithms:
            raise ValueError("at least one asymmetric signature algorithm is required")
        for path in (self.application_claim, self.tenant_claim):
            if not path or len(path) > 8 or any(not part for part in path):
                raise ValueError("a bounded nonempty signed claim path is required")
            if "user_metadata" in path:
                raise ValueError("user-editable metadata cannot grant application or tenant scope")
        if len({grant.subject for grant in self.grants}) != len(self.grants):
            raise ValueError("duplicate subject grant")
        return self


@dataclass(frozen=True)
class AuthenticatedActor:
    identity: VerifiedApplicationIdentity
    actor: ActorContext
    sponsorship_refs: frozenset[str]
    approval_refs: frozenset[str]


class MissionAuthenticationRejected(PermissionError):
    def __init__(self, code: str, status_code: int = 401) -> None:
        super().__init__(code)
        self.code = code
        self.status_code = status_code


def _claim(claims: dict[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = claims
    for key in path:
        if not isinstance(current, dict) or key not in current:
            raise MissionAuthenticationRejected("scope_claim_missing", 403)
        current = current[key]
    return current


def _segment(value: str) -> dict[str, Any]:
    try:
        data = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
        return parse_json_object(data)
    except (ValueError, UnicodeError):
        raise MissionAuthenticationRejected("invalid_token") from None


class MissionTokenVerifier:
    def __init__(
        self,
        configurations: tuple[ApplicationAuthentication, ...],
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._clock = clock
        self._configurations: dict[str, ApplicationAuthentication] = {}
        self._keys: dict[str, KeySet] = {}
        for config in configurations:
            config = ApplicationAuthentication.model_validate(config.model_dump(mode="python"))
            application = config.binding.application_id
            if application in self._configurations:
                raise ValueError("duplicate authenticated application")
            document = parse_json_object(config.public_jwks_file.read_bytes())
            keys = document.get("keys")
            if not isinstance(keys, list) or not 1 <= len(keys) <= 32:
                raise ValueError("public JWKS must contain between 1 and 32 keys")
            key_ids: set[str] = set()
            for key in keys:
                if not isinstance(key, dict) or key.get("kty") not in {"RSA", "EC", "OKP"}:
                    raise ValueError("public asymmetric verification keys are required")
                if {"d", "p", "q", "dp", "dq", "qi", "oth", "k"} & key.keys():
                    raise ValueError("public JWKS must not contain private key material")
                kid = key.get("kid")
                if not isinstance(kid, str) or not kid or kid in key_ids:
                    raise ValueError("public keys require unique nonempty key IDs")
                key_ids.add(kid)
                if key.get("use", "sig") != "sig":
                    raise ValueError("verification key must have signature use")
            self._keys[application] = KeySet.import_key_set(cast(KeySetSerialization, document))
            self._configurations[application] = config

    def authenticate(self, application_id: str, token: str) -> AuthenticatedActor:
        config = self._configurations.get(application_id)
        if config is None:
            raise MissionAuthenticationRejected("application_scope_denied", 403)
        if not 1 <= len(token) <= 16_384:
            raise MissionAuthenticationRejected("invalid_token")
        parts = token.split(".")
        if len(parts) != 3 or not all(parts):
            raise MissionAuthenticationRejected("invalid_token")
        header, raw_claims = _segment(parts[0]), _segment(parts[1])
        if (
            not isinstance(header.get("alg"), str)
            or header.get("alg") not in config.algorithms
            or not isinstance(header.get("kid"), str)
            or not isinstance(header.get("typ", "JWT"), str)
            or header.get("typ", "JWT") not in {"JWT", "at+jwt"}
            or set(header) - {"alg", "kid", "typ"}
        ):
            raise MissionAuthenticationRejected("invalid_token")
        now = self._clock()
        try:
            verified = jwt.decode(token, self._keys[application_id], algorithms=config.algorithms)
            claims = verified.claims
            if claims != raw_claims:
                raise ValueError("ambiguous claims")
            jwt.JWTClaimsRegistry(
                now=int(now),
                leeway=0,
                iss={"essential": True, "value": config.issuer},
                aud={"essential": True, "value": config.audience},
                sub={"essential": True},
                exp={"essential": True},
            ).validate(claims)
            for name in ("exp", "nbf", "iat"):
                if name in claims and type(claims[name]) not in {int, float}:
                    raise ValueError("invalid numeric date")
            if claims["exp"] <= now:
                raise ValueError("expired token")
        except (JoseError, ValueError, TypeError, KeyError):
            raise MissionAuthenticationRejected("invalid_token") from None
        if _claim(claims, config.application_claim) != application_id:
            raise MissionAuthenticationRejected("application_scope_denied", 403)
        try:
            tenant = UUID(_claim(claims, config.tenant_claim))
        except (ValueError, TypeError, AttributeError):
            raise MissionAuthenticationRejected("tenant_scope_denied", 403) from None
        grant = next((item for item in config.grants if item.subject == claims["sub"]), None)
        if grant is None or tenant not in grant.tenant_ids:
            raise MissionAuthenticationRejected("tenant_scope_denied", 403)
        audiences = claims["aud"] if isinstance(claims["aud"], list) else [claims["aud"]]
        return AuthenticatedActor(
            identity=VerifiedApplicationIdentity(
                issuer=config.issuer,
                audiences=frozenset(audiences),
                application_id=application_id,
                installation_id=config.binding.installation_id,
                tenant_id=tenant,
            ),
            actor=ActorContext(
                actor_id=grant.actor_id,
                permissions=grant.permissions,
                authority_refs=grant.authority_refs,
            ),
            sponsorship_refs=grant.sponsorship_refs,
            approval_refs=grant.approval_refs,
        )
