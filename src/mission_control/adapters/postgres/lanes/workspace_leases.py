"""PostgreSQL `WorkspaceLeaseStore` / `WorkspaceLeaseLedger` over
`mission_control.workspace_lease` (FT-G3; MP-04).

The 0003 table holds every file-based lane lease: `lease_key` is
`<lane>:<harness_execution_id>:<generation>`, `native_workspace_ref` the worktree path,
`fencing_token` the fence, `patch_ref` the stored patch artifact, and `detail` the secret-free
lease facts (run, attempt, base ref, repository, release time). A FT-G3 lease is written once
(first wins) and released once, after its patch is stored.

MP-04 slot leases use the same table and the existing runtime grants (no new column): the slot,
branch, policy, allocator and snapshot record live in `detail`. An acquisition serializes on a
transaction-scoped advisory lock per (scope, slot), refuses a live competing holder or a
superseded generation, fences out an expired holder (`observed_state = 'lost'`) and issues the
next fence. Every later write is an UPDATE guarded by `desired_state = 'active' AND
fencing_token = <fence>`: under READ COMMITTED a write racing a takeover re-evaluates that
guard after the takeover commits and matches no row, so it is refused, never applied.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control.canonical import SCOPE, advisory_lock, begin
from mission_control.application.execution.harness.leases import (
    LeaseCleanupStatus,
    WorkspaceLease,
    admission_refusal,
    issue_fence,
)
from mission_control.application.workspaces.errors import StaleWorkspaceLease
from mission_control.domain.execution.bindings import WorkspacePolicyPin, WorkspaceSnapshot

ACTOR_REF = "mission-control-runtime/workspace-lease"
_COLUMNS = (
    "workspace_lease_id, lease_key, native_workspace_ref, base_commit, fencing_token, "
    "lease_expires_at, patch_ref, desired_state, observed_state, cleanup_status, detail"
)


def _lease(row: asyncpg.Record, request_scope: str) -> WorkspaceLease:
    detail = row["detail"]
    detail = json.loads(detail) if isinstance(detail, str) else dict(detail)
    released_at = detail.get("released_at")
    policy = detail.get("policy")
    snapshot = detail.get("workspace_snapshot")
    lost_to = detail.get("lost_to")
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
        slot=detail.get("workspace_slot"),
        branch=detail.get("branch"),
        policy=WorkspacePolicyPin.model_validate(policy) if policy else None,
        allocator_ref=detail.get("allocator_ref"),
        observed_state=row["observed_state"],
        cleanup_status=row["cleanup_status"],
        workspace_snapshot=WorkspaceSnapshot.model_validate(snapshot) if snapshot else None,
        input_snapshot_ref=detail.get("input_snapshot_ref"),
        lost_to=UUID(lost_to) if lost_to else None,
    )


def _detail(lease: WorkspaceLease) -> dict[str, Any]:
    detail: dict[str, Any] = {
        "lane_profile": lease.lane_profile,
        "harness_execution_id": str(lease.harness_execution_id),
        "generation": lease.generation,
        "run_id": lease.run_id,
        "attempt_no": lease.attempt_no,
        "base_ref": lease.base_ref,
        "repository": lease.repository,
    }
    if lease.slot is not None:
        detail["workspace_slot"] = lease.slot
    if lease.branch is not None:
        detail["branch"] = lease.branch
    if lease.policy is not None:
        detail["policy"] = lease.policy.model_dump(mode="json")
    if lease.allocator_ref is not None:
        detail["allocator_ref"] = lease.allocator_ref
    return detail


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

    async def _insert(
        self, connection: asyncpg.Connection, args: tuple[Any, ...], lease: WorkspaceLease
    ) -> None:
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
            json.dumps(_detail(lease), sort_keys=True),
            self._actor,
        )

    async def record(self, lease: WorkspaceLease) -> WorkspaceLease:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, lease.request_scope)
            await self._insert(connection, args, lease)
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

    # --- MP-04 fenced ledger ------------------------------------------------------------------

    async def _slot(
        self, connection: asyncpg.Connection, args: tuple[Any, ...], scope: str, slot: str
    ) -> tuple[WorkspaceLease, ...]:
        rows = await connection.fetch(
            f"SELECT {_COLUMNS} FROM mission_control.workspace_lease "
            f"WHERE {SCOPE} AND detail->>'workspace_slot' = $4 "
            "ORDER BY fencing_token, created_at",
            *args,
            slot,
        )
        return tuple(_lease(row, scope) for row in rows)

    async def acquire(self, claim: WorkspaceLease, *, now: datetime) -> WorkspaceLease:
        if claim.slot is None:
            raise ValueError("a fenced acquisition names its workspace slot")
        scope = claim.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, scope)
            await advisory_lock(connection, f"workspace_lease:{scope}:{claim.slot}")
            existing = await self._fetch(connection, args, claim.lease_id)
            if existing is not None:
                stored = _lease(existing, scope)
                if stored.released:
                    raise StaleWorkspaceLease(claim.lease_key, "the holder already released it")
                return stored
            slot_leases = await self._slot(connection, args, scope, claim.slot)
            takeover, refusal = admission_refusal(claim, slot_leases, now)
            if refusal is not None:
                raise refusal
            if takeover is not None:
                await connection.execute(
                    f"""
                    UPDATE mission_control.workspace_lease
                    SET desired_state = 'released', observed_state = 'lost',
                        detail = detail || $6::jsonb,
                        version = version + 1, updated_at = clock_timestamp()
                    WHERE {SCOPE} AND workspace_lease_id = $4 AND desired_state = 'active'
                        AND fencing_token = $5
                    """,
                    *args,
                    takeover.lease_id,
                    takeover.fence,
                    json.dumps(
                        {"released_at": now.isoformat(), "lost_to": str(claim.lease_id)},
                        sort_keys=True,
                    ),
                )
            lease = claim.model_copy(update={"fence": issue_fence(claim, slot_leases)})
            await self._insert(connection, args, lease)
            row = await self._fetch(connection, args, lease.lease_id)
        if row is None:
            raise LookupError(f"workspace lease {claim.lease_id} was not recorded")
        return _lease(row, scope)

    async def _fenced_update(
        self,
        request_scope: str,
        lease_id: UUID,
        fence: int,
        assignments: str,
        values: tuple[Any, ...],
    ) -> WorkspaceLease:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            updated = await connection.fetchval(
                f"""
                UPDATE mission_control.workspace_lease
                SET {assignments}, version = version + 1, updated_at = clock_timestamp()
                WHERE {SCOPE} AND workspace_lease_id = $4 AND desired_state = 'active'
                    AND fencing_token = $5
                RETURNING workspace_lease_id
                """,
                *args,
                lease_id,
                fence,
                *values,
            )
            row = await self._fetch(connection, args, lease_id)
        if row is None:
            raise LookupError(f"workspace lease {lease_id} is not recorded")
        stored = _lease(row, request_scope)
        if updated is None:
            raise StaleWorkspaceLease(
                stored.lease_key,
                f"fence {fence} does not hold it (state {stored.observed_state}, "
                f"fence {stored.fence})",
            )
        return stored

    async def renew(
        self, request_scope: str, lease_id: UUID, *, fence: int, expires_at: datetime
    ) -> WorkspaceLease:
        return await self._fenced_update(
            request_scope, lease_id, fence, "lease_expires_at = $6", (expires_at,)
        )

    async def record_snapshot(
        self,
        request_scope: str,
        lease_id: UUID,
        *,
        fence: int,
        snapshot: WorkspaceSnapshot,
        snapshot_ref: str,
        input_snapshot: bool = False,
    ) -> WorkspaceLease:
        recorded: dict[str, Any] = (
            {"input_snapshot_ref": snapshot_ref, "input_snapshot": snapshot.model_dump(mode="json")}
            if input_snapshot
            else {
                "snapshot_ref": snapshot_ref,
                "workspace_snapshot": snapshot.model_dump(mode="json"),
            }
        )
        return await self._fenced_update(
            request_scope,
            lease_id,
            fence,
            "detail = detail || $6::jsonb",
            (json.dumps(recorded, sort_keys=True),),
        )

    async def release_fenced(
        self,
        request_scope: str,
        lease_id: UUID,
        *,
        fence: int,
        patch_artifact_ref: str | None,
        snapshot_ref: str | None,
        released_at: datetime,
        cleanup_status: LeaseCleanupStatus,
    ) -> WorkspaceLease:
        released: dict[str, Any] = {"released_at": released_at.isoformat()}
        if snapshot_ref is not None:
            released["snapshot_ref"] = snapshot_ref
        return await self._fenced_update(
            request_scope,
            lease_id,
            fence,
            "desired_state = 'released', observed_state = 'released', patch_ref = $6, "
            "cleanup_status = $7, detail = detail || $8::jsonb",
            (patch_artifact_ref, cleanup_status, json.dumps(released, sort_keys=True)),
        )

    async def mark_cleanup(
        self, request_scope: str, lease_id: UUID, *, status: LeaseCleanupStatus
    ) -> WorkspaceLease:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            updated = await connection.fetchval(
                f"""
                UPDATE mission_control.workspace_lease
                SET cleanup_status = $5, version = version + 1, updated_at = clock_timestamp()
                WHERE {SCOPE} AND workspace_lease_id = $4 AND desired_state = 'released'
                RETURNING workspace_lease_id
                """,
                *args,
                lease_id,
                status,
            )
            row = await self._fetch(connection, args, lease_id)
        if row is None:
            raise LookupError(f"workspace lease {lease_id} is not recorded")
        stored = _lease(row, request_scope)
        if updated is None:
            raise StaleWorkspaceLease(stored.lease_key, "an active lease is not cleaned up")
        return stored

    async def slot_leases(self, request_scope: str, slot: str) -> tuple[WorkspaceLease, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await begin(connection, request_scope)
            return await self._slot(connection, args, request_scope, slot)

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
