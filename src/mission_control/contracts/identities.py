"""Scoped Temporal identities, derived only from authenticated execution bindings."""

from __future__ import annotations

import os
import re
import time
from dataclasses import dataclass
from hashlib import sha256
from urllib.parse import quote
from uuid import UUID


@dataclass(frozen=True)
class RequestScope:
    """Composite installation/application/tenant scope of one authenticated request."""

    installation_id: UUID
    application_id: str
    tenant_id: UUID

    def __str__(self) -> str:
        return f"mc/{self.installation_id}/{self.application_id}/{self.tenant_id}"


def parse_request_scope(request_scope: str) -> RequestScope:
    parts = request_scope.split("/")
    if len(parts) != 4 or parts[0] != "mc":
        raise ValueError("Mission Control requires canonical installation/application/tenant scope")
    _, installation, application, tenant = parts
    try:
        installation_id, tenant_id = UUID(installation), UUID(tenant)
    except ValueError:
        raise ValueError("Mission Control scope UUIDs must be canonical") from None
    if str(installation_id) != installation or str(tenant_id) != tenant:
        raise ValueError("Mission Control scope UUIDs must be canonical")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", application):
        raise ValueError("invalid Mission Control application identity")
    return RequestScope(installation_id, application, tenant_id)


def uuid7(*, unix_ms: int | None = None) -> UUID:
    """RFC 9562 UUIDv7: 48-bit Unix milliseconds, version 7, 74 random bits."""
    milliseconds = time.time_ns() // 1_000_000 if unix_ms is None else unix_ms
    if not 0 <= milliseconds < 1 << 48:
        raise ValueError("UUIDv7 timestamp out of range")
    random_bits = int.from_bytes(os.urandom(10), "big")
    value = (milliseconds << 80) | (0x7 << 76) | ((random_bits >> 62) & 0xFFF) << 64
    value |= (0b10 << 62) | (random_bits & ((1 << 62) - 1))
    return UUID(int=value)


def mission_root_id(request_scope: str, run_id: str) -> str:
    scope = parse_request_scope(request_scope)
    installation, application = scope.installation_id, scope.application_id
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}", run_id):
        raise ValueError("invalid Mission Control run identity")
    return f"mc/{installation}/{application}/run/{run_id}"


def mission_operation_id(request_scope: str, run_id: str, semantic_attempt_id: str) -> str:
    if not semantic_attempt_id:
        raise ValueError("invalid semantic operation identity")
    segment = quote(semantic_attempt_id, safe=":-._~")
    identity = f"{mission_root_id(request_scope, run_id)}/operation/{segment}"
    if len(identity) > 512:
        raise ValueError("scoped operation identity exceeds Temporal identity limit")
    return identity


def mission_linked_observer_id(request_scope: str, parent_run_id: str, link_id: str) -> str:
    if not link_id:
        raise ValueError("linked observer requires an admitted link identity")
    segment = sha256(link_id.encode("utf-8")).hexdigest()
    return f"{mission_root_id(request_scope, parent_run_id)}/linked-observer/{segment}"
