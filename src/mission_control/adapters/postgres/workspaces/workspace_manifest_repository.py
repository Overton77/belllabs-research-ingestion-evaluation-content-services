from __future__ import annotations

import asyncpg

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


class PostgresWorkspaceManifestRepository:
    """Scoped, append-only workspace lineage and transactional writable reservations."""

    def __init__(self, pool: asyncpg.Pool, *, request_scope: str) -> None:
        if not request_scope.strip():
            raise ValueError("workspace repository requires request scope")
        self._pool = pool
        self._scope = request_scope

    async def _scope_connection(self, connection: asyncpg.Connection) -> None:
        await connection.execute(
            "SELECT set_config('belllabs.request_scope', $1, true)", self._scope
        )

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
                    """SELECT workspace_id, owner_id, reservation_token
                       FROM belllabs_control.workspace_slot_reservations
                       WHERE request_scope=$1 AND namespace_id=$2 AND logical_path=$3""",
                    self._scope,
                    request.namespace_id,
                    boundary,
                )
                if prior is not None:
                    if (prior["workspace_id"], prior["owner_id"]) != (
                        request.workspace_id,
                        slot.owner.owner_id,
                    ) or prior["reservation_token"] not in accepted:
                        raise WorkspaceSlotConflict(
                            f"writable slot {slot.logical_path} is owned by another workspace"
                        )
                    continue
                await connection.execute(
                    """INSERT INTO belllabs_control.workspace_slot_reservations
                       (request_scope, namespace_id, logical_path, workspace_id,
                        owner_id, reservation_token, reserved_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)""",
                    self._scope,
                    request.namespace_id,
                    boundary,
                    request.workspace_id,
                    slot.owner.owner_id,
                    token,
                    request.created_at,
                )

    async def _current(
        self, connection: asyncpg.Connection, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest | None:
        rows = await connection.fetch(
            """SELECT payload FROM belllabs_control.workspace_manifests
               WHERE request_scope=$1 AND namespace_id=$2 AND workspace_id=$3
               ORDER BY revision DESC LIMIT 2""",
            self._scope,
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
                    """SELECT payload FROM belllabs_control.workspace_manifests
                       WHERE request_scope=$1 AND manifest_id=$2""",
                    self._scope,
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
                    """INSERT INTO belllabs_control.workspace_manifests
                       (request_scope, namespace_id, workspace_id, revision, manifest_id,
                        manifest_digest, prior_manifest_digest, payload, created_at)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)""",
                    self._scope,
                    manifest.namespace_id,
                    manifest.workspace_id,
                    manifest.revision,
                    manifest.manifest_id,
                    manifest.manifest_digest,
                    manifest.prior_manifest_digest,
                    manifest.model_dump_json(),
                    manifest.created_at,
                )
                return manifest
        except asyncpg.UniqueViolationError as error:
            raise IdempotencyConflict("workspace manifest identity collision") from error
