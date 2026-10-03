"""Scoped Temporal identities, derived only from authenticated execution bindings."""

from __future__ import annotations

import re
from hashlib import sha256
from urllib.parse import quote
from uuid import UUID


def mission_root_id(request_scope: str, run_id: str) -> str:
    parts = request_scope.split("/")
    if len(parts) != 4 or parts[0] != "mc":
        raise ValueError("Mission Control requires canonical installation/application/tenant scope")
    _, installation, application, tenant = parts
    if str(UUID(installation)) != installation or str(UUID(tenant)) != tenant:
        raise ValueError("Mission Control scope UUIDs must be canonical")
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,62}", application):
        raise ValueError("invalid Mission Control application identity")
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
