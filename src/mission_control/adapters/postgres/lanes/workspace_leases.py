"""PostgreSQL `WorkspaceLeaseStore` over `mission_control.workspace_lease` (FT-G3).

The 0003 table holds every file-based lane lease: `lease_key` is
`<lane>:<harness_execution_id>:<generation>`, `native_workspace_ref` the worktree path,
`fencing_token` the generation fence, `patch_ref` the stored patch artifact, and `detail` the
secret-free lease facts (run, attempt, base ref, repository, release time). A lease is written
once (first wins) and released once, after its patch is stored.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE, begin
from mission_control.application.execution.harness.leases import WorkspaceLease

ACTOR_REF = "mission-control-runtime/workspace-lease"
_COLUMNS = (
    "workspace_lease_id, lease_key, native_workspace_ref, base_commit, fencing_token, "
    "lease_expires_at, patch_ref, desired_state, detail"
)


def _lease(row: asyncpg.Record, request_scope: str) -> WorkspaceLease:
    detail = row["detail"]
    detail = json.loads(detail) if isinstance(detail, str) else dict(detail)
    released_at = detail.get("released_at")
    return WorkspaceLease(
        lease_id=row["workspace_lease_id"],
        request_scope=request_scope,
        lease_key=row["lease_key"],
        lane_profile=detail["lane_profile"],
        harness_execution_id=UUID(detail["harness_execution_id"]),
        generation=int(detail["generation"]),
        run_id=detail["run_id"],
        attempt_no=int(detail["attempt_no"]),
        path=row["native_workspace_ref"],
        repository=detail.get("repository"),
        base_ref=detail["base_ref"],
        base_commit=row["base_commit"],
        fence=int(row["fencing_token"]),
        expires_at=row["lease_expires_at"],
        released_at=datetime.fromisoformat(released_at) if released_at else None,
        patch_artifact_ref=row["patch_ref"],
        snapshot_ref=detail.get("snapshot_ref"),
    )


class PostgresWorkspaceLeaseStore:
    def __init__(self, pool: asyncpg.Pool, *, actor_ref: str = ACTOR_REF) -> None:
        self._pool = pool
        self._actor = actor_ref

    async def _fetch(
        self, connection: asyncpg.Connection, args: tuple[Any, ...], lease_id: UUID
    ) -> asyncpg.Record | None:
        return await connection.fetchrow(
            f"SELECT {_COLUMNS} FROM mission_control.workspace_lease "
            f"WHERE {SCOPE} AND workspace_lease_id = $4",
            *args,
            lease_id,
        )

    async def record(self, lease: WorkspaceLease) -> WorkspaceLease:
        detail = {
            "lane_profile": lease.lane_profile,
            "harness_execution_id": str(lease.harness_execution_id),
            "generation": lease.generation,
            "run_id": lease.run_id,
            "attempt_no": lease.attempt_no,
            "base_ref": lease.base_ref,
            "repository": lease.repository,
        }
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, lease.request_scope)
            await connection.execute(
                """
                INSERT INTO mission_control.workspace_lease (
                    installation_id, application_id, tenant_id, workspace_lease_id, attempt_id,
                    lease_key, owner_ref, provider_kind, native_workspace_ref, profile_digest,
                    snapshot_digest, lease_expires_at, fencing_token, desired_state,
                    observed_state, cleanup_status, repository_allowlist_ref, base_commit,
                    patch_ref, detail, version, updated_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, NULL, $5, $6, $7, $8, NULL, NULL, $9, $10, 'active',
                        'active', 'pending', NULL, $11, NULL, $12::jsonb, 1, clock_timestamp(),
                        clock_timestamp(), $13)
                ON CONFLICT (installation_id, application_id, tenant_id, lease_key) DO NOTHING
                """,
                *args,
                lease.lease_id,
                lease.lease_key,
                f"harness_execution:{lease.harness_execution_id}",
                lease.lane_profile,
                lease.path,
                lease.expires_at,
                lease.fence,
                lease.base_commit,
                json.dumps(detail, sort_keys=True),
                self._actor,
            )
            row = await self._fetch(connection, args, lease.lease_id)
        if row is None:
            raise LookupError(f"workspace lease {lease.lease_id} was not recorded")
        return _lease(row, lease.request_scope)

    async def get(self, request_scope: str, lease_id: UUID) -> WorkspaceLease | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            row = await self._fetch(connection, args, lease_id)
        return None if row is None else _lease(row, request_scope)

    async def release(
        self,
        request_scope: str,
        lease_id: UUID,
        *,
        patch_artifact_ref: str | None,
        released_at: datetime,
        snapshot_ref: str | None = None,
    ) -> WorkspaceLease:
        released = {"released_at": released_at.isoformat()}
        if snapshot_ref is not None:
            released["snapshot_ref"] = snapshot_ref
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            await connection.execute(
                f"""
                UPDATE mission_control.workspace_lease
                SET desired_state = 'released', observed_state = 'released',
                    cleanup_status = 'completed', patch_ref = $5,
                    detail = detail || $6::jsonb,
                    version = version + 1, updated_at = clock_timestamp()
                WHERE {SCOPE} AND workspace_lease_id = $4 AND desired_state = 'active'
                """,
                *args,
                lease_id,
                patch_artifact_ref,
                json.dumps(released, sort_keys=True),
            )
            row = await self._fetch(connection, args, lease_id)
        if row is None:
            raise LookupError(f"workspace lease {lease_id} is not recorded")
        return _lease(row, request_scope)

    async def sandbox_snapshot_refs(self, request_scope: str, run_id: str) -> tuple[str, ...]:
        """FT-G4 `LaneSnapshotRefReader`: the snapshot frozen when the run's newest released
        lease ended its session (a fork restores the workspace from it)."""

        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            ref = await connection.fetchval(
                f"""
                SELECT detail->>'snapshot_ref' FROM mission_control.workspace_lease
                WHERE {SCOPE} AND detail->>'run_id' = $4 AND detail ? 'snapshot_ref'
                    AND desired_state = 'released'
                ORDER BY updated_at DESC, workspace_lease_id DESC
                LIMIT 1
                """,
                *args,
                run_id,
            )
        return (str(ref),) if ref else ()


__all__ = ["PostgresWorkspaceLeaseStore"]
