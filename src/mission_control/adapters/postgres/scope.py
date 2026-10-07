"""Transaction-local composite scope for the common mission_control component.

Every business statement runs inside an explicit transaction after one of these
helpers. The values come from verified bindings and authenticated principals,
never from client headers; database policies deny rows when context is absent.
"""

from __future__ import annotations

from uuid import UUID

import asyncpg

from mission_control.contracts.identities import RequestScope, parse_request_scope

SCHEMA = "mission_control"
SEARCH_SCHEMA = "mission_control_search"


def _require_transaction(connection: asyncpg.Connection) -> None:
    if not connection.is_in_transaction():
        raise RuntimeError("mission_control scope requires an explicit transaction")


async def apply_scope(
    connection: asyncpg.Connection, request_scope: str | RequestScope
) -> RequestScope:
    """Bind tenant scope for the current transaction only."""
    scope = (
        request_scope
        if isinstance(request_scope, RequestScope)
        else parse_request_scope(request_scope)
    )
    _require_transaction(connection)
    await connection.execute(
        "SELECT set_config('mc.installation_id', $1, true), "
        "set_config('mc.application_id', $2, true), "
        "set_config('mc.tenant_id', $3, true)",
        str(scope.installation_id),
        scope.application_id,
        str(scope.tenant_id),
    )
    return scope


async def apply_catalog_scope(
    connection: asyncpg.Connection, installation_id: UUID, application_id: str
) -> None:
    """Bind installation catalog scope; tenant context is explicitly cleared."""
    _require_transaction(connection)
    await connection.execute(
        "SELECT set_config('mc.installation_id', $1, true), "
        "set_config('mc.application_id', $2, true), "
        "set_config('mc.tenant_id', '', true)",
        str(installation_id),
        application_id,
    )


def parse_catalog_scope(catalog_scope: str) -> tuple[UUID, str]:
    """Parse ``mc/{installation_uuid}/{application_id}/catalog``."""
    parts = catalog_scope.split("/")
    if len(parts) != 4 or parts[0] != "mc" or parts[3] != "catalog":
        raise ValueError("Mission Control catalog scope must be mc/{installation}/{app}/catalog")
    scope = parse_request_scope(f"mc/{parts[1]}/{parts[2]}/{parts[1]}")
    return scope.installation_id, scope.application_id


async def apply_catalog_scope_string(connection: asyncpg.Connection, catalog_scope: str) -> None:
    installation_id, application_id = parse_catalog_scope(catalog_scope)
    await apply_catalog_scope(connection, installation_id, application_id)


async def apply_actor(connection: asyncpg.Connection, actor_ref: str) -> None:
    if not actor_ref:
        raise ValueError("actor reference cannot be empty")
    _require_transaction(connection)
    await connection.execute("SELECT set_config('mc.actor_ref', $1, true)", actor_ref)


def scope_values(scope: RequestScope) -> tuple[UUID, str, UUID]:
    return scope.installation_id, scope.application_id, scope.tenant_id
