from __future__ import annotations

import asyncpg

from mission_control.adapters.postgres.scope import apply_scope
from mission_control.contracts.identities import parse_request_scope, uuid7
from mission_control.domain.execution.contracts import (
    WorkspaceMaterializationManifest,
    WorkspaceMaterializationRequest,
)
from mission_control.domain.execution.errors import WorkspaceSlotConflict
from mission_control.domain.execution.materialization import (
    legacy_workspace_reservation_token,
    same_workspace_manifest,
    slot_ownership_boundary,
    verify_workspace_manifest,
    workspace_reservation_token,
)
from mission_control.domain.policies.errors import IdempotencyConflict

_SERVICE_ACTOR = "service:mission-control-workspaces"


class PostgresWorkspaceManifestRepository:
    """Scoped, append-only workspace lineage and transactional writable reservations.

    Support records ``mission_control.workspace_manifest`` and
    ``mission_control.workspace_slot_reservation`` under the tenant request scope.
    """

    def __init__(self, pool: asyncpg.Pool, *, request_scope: str) -> None:
        if not request_scope.strip():
            raise ValueError("workspace repository requires request scope")
        self._pool = pool
        self._scope = request_scope
        parsed = parse_request_scope(request_scope)
        self._key = (parsed.installation_id, parsed.application_id, parsed.tenant_id)

    async def _scope_connection(self, connection: asyncpg.Connection) -> None:
        await apply_scope(connection, self._scope)

    async def _lock(self, connection: asyncpg.Connection, namespace_id: str) -> None:
        await self._scope_connection(connection)
        await connection.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
            f"workspace:{self._scope}:{namespace_id}",
        )

    async def reserve_writable_slots(self, request: WorkspaceMaterializationRequest) -> None:
        token = workspace_reservation_token(request)
        accepted = {token, legacy_workspace_reservation_token(request)}
        async with self._pool.acquire() as connection, connection.transaction():
            await self._lock(connection, request.namespace_id)
            for slot in request.slots:
                if slot.access != "exclusive_write":
                    continue
                boundary = slot_ownership_boundary(slot.logical_path)
                prior = await connection.fetchrow(
                    """SELECT workspace_key, owner_key, reservation_token
                       FROM mission_control.workspace_slot_reservation
                       WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                         AND namespace_key=$4 AND logical_path=$5""",
                    *self._key,
                    request.namespace_id,
                    boundary,
                )
                if prior is not None:
                    if (prior["workspace_key"], prior["owner_key"]) != (
                        request.workspace_id,
                        slot.owner.owner_id,
                    ) or prior["reservation_token"] not in accepted:
                        raise WorkspaceSlotConflict(
                            f"writable slot {slot.logical_path} is owned by another workspace"
                        )
                    continue
                await connection.execute(
                    """INSERT INTO mission_control.workspace_slot_reservation
                       (installation_id, application_id, tenant_id, slot_reservation_id,
                        namespace_key, logical_path, workspace_key, owner_key,
                        reservation_token, reserved_at, created_at, created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,clock_timestamp(),$11)""",
                    *self._key,
                    uuid7(),
                    request.namespace_id,
                    boundary,
                    request.workspace_id,
                    slot.owner.owner_id,
                    token,
                    request.created_at,
                    _SERVICE_ACTOR,
                )

    async def _current(
        self, connection: asyncpg.Connection, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest | None:
        rows = await connection.fetch(
            """SELECT payload FROM mission_control.workspace_manifest
               WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                 AND namespace_key=$4 AND workspace_key=$5
               ORDER BY revision DESC LIMIT 2""",
            *self._key,
            namespace_id,
            workspace_id,
        )
        if not rows:
            return None
        current = WorkspaceMaterializationManifest.model_validate_json(rows[0]["payload"])
        verify_workspace_manifest(current)
        if current.revision > 1:
            if len(rows) != 2:
                raise IdempotencyConflict("workspace manifest lineage is incomplete")
            prior = WorkspaceMaterializationManifest.model_validate_json(rows[1]["payload"])
            verify_workspace_manifest(prior)
            if (
                prior.revision != current.revision - 1
                or prior.manifest_digest != current.prior_manifest_digest
            ):
                raise IdempotencyConflict("workspace manifest lineage is incomplete")
        return current

    async def get_current(
        self, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await self._scope_connection(connection)
            return await self._current(connection, namespace_id, workspace_id)

    async def append(
        self, manifest: WorkspaceMaterializationManifest
    ) -> WorkspaceMaterializationManifest:
        verify_workspace_manifest(manifest)
        try:
            async with self._pool.acquire() as connection, connection.transaction():
                await self._lock(connection, manifest.namespace_id)
                matching = await connection.fetchval(
                    """SELECT payload FROM mission_control.workspace_manifest
                       WHERE installation_id=$1 AND application_id=$2 AND tenant_id=$3
                         AND manifest_key=$4""",
                    *self._key,
                    manifest.manifest_id,
                )
                if matching is not None:
                    prior = WorkspaceMaterializationManifest.model_validate_json(matching)
                    if same_workspace_manifest(prior, manifest):
                        return prior
                    raise IdempotencyConflict("workspace manifest identity conflict")
                current = await self._current(
                    connection, manifest.namespace_id, manifest.workspace_id
                )
                if current is None:
                    if manifest.revision != 1 or manifest.prior_manifest_digest is not None:
                        raise IdempotencyConflict("first workspace manifest must be revision one")
                elif (
                    manifest.revision != current.revision + 1
                    or manifest.prior_manifest_digest != current.manifest_digest
                ):
                    raise IdempotencyConflict("workspace manifest lineage conflict")
                await connection.execute(
                    """INSERT INTO mission_control.workspace_manifest
                       (installation_id, application_id, tenant_id, workspace_manifest_id,
                        namespace_key, workspace_key, revision, manifest_key, manifest_digest,
                        prior_manifest_digest, payload, manifest_created_at, created_at,
                        created_by_actor_ref)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,
                               clock_timestamp(),$13)""",
                    *self._key,
                    uuid7(),
                    manifest.namespace_id,
                    manifest.workspace_id,
                    manifest.revision,
                    manifest.manifest_id,
                    manifest.manifest_digest,
                    manifest.prior_manifest_digest,
                    manifest.model_dump_json(),
                    manifest.created_at,
                    _SERVICE_ACTOR,
                )
                return manifest
        except asyncpg.UniqueViolationError as error:
            raise IdempotencyConflict("workspace manifest identity collision") from error
