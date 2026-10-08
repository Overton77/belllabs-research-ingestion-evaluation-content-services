"""Workspace leases of file-based lanes (SPEC-07 section 5.1 and Persistence; FT-G3).

A Workspace lease is the directory one harness execution generation works in (a git worktree
of the target repository at the binding's base ref, or an initialized empty repository). It is
fenced, expires with the attempt and is released only after its patch artifact is stored. The
production store writes `mission_control.workspace_lease` (migration 0003; runtime grants in
0030, G3 section); the lease key is `<lane>:<harness_execution_id>:<generation>`.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Protocol
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

_LEASE_NAMESPACE = UUID("6b1a7c42-93d4-4e0f-a1b8-2f5c7d9e3a61")


class WorkspaceLease(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    lease_id: UUID
    request_scope: str = Field(min_length=1)
    lease_key: str = Field(min_length=1)
    lane_profile: str = Field(min_length=1)
    harness_execution_id: UUID
    generation: int = Field(ge=1)
    run_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=1)
    path: str = Field(min_length=1)
    repository: str | None = None
    base_ref: str = Field(min_length=1)
    base_commit: str = Field(min_length=1)
    fence: int = Field(ge=0)
    expires_at: AwareDatetime
    released_at: AwareDatetime | None = None
    patch_artifact_ref: str | None = None
    # FT-G4: the lane snapshot (`cursor-snapshot:<ref>`) frozen at session end; a fork of the
    # run restores it (RunSnapshotManifest.sandbox_snapshot_refs).
    snapshot_ref: str | None = None

    @property
    def released(self) -> bool:
        return self.released_at is not None


def lease_identity(
    lane_profile: str, harness_execution_id: UUID, generation: int
) -> tuple[UUID, str]:
    key = f"{lane_profile}:{harness_execution_id}:{generation}"
    return uuid5(_LEASE_NAMESPACE, key), key


class LeaseConflict(RuntimeError):
    """A lease key is already recorded with different content."""


class WorkspaceLeaseStore(Protocol):
    async def record(self, lease: WorkspaceLease) -> WorkspaceLease:
        """Insert the lease once; a repeated record returns the stored lease (first wins)."""
        ...

    async def get(self, request_scope: str, lease_id: UUID) -> WorkspaceLease | None: ...

    async def release(
        self,
        request_scope: str,
        lease_id: UUID,
        *,
        patch_artifact_ref: str | None,
        released_at: datetime,
        snapshot_ref: str | None = None,
    ) -> WorkspaceLease: ...


class InMemoryWorkspaceLeaseStore:
    def __init__(self) -> None:
        self._leases: dict[tuple[str, UUID], WorkspaceLease] = {}
        self._lock = asyncio.Lock()

    async def record(self, lease: WorkspaceLease) -> WorkspaceLease:
        async with self._lock:
            key = (lease.request_scope, lease.lease_id)
            stored = self._leases.get(key)
            if stored is None:
                self._leases[key] = lease
                return lease
            return stored

    async def get(self, request_scope: str, lease_id: UUID) -> WorkspaceLease | None:
        return self._leases.get((request_scope, lease_id))

    async def release(
        self,
        request_scope: str,
        lease_id: UUID,
        *,
        patch_artifact_ref: str | None,
        released_at: datetime,
        snapshot_ref: str | None = None,
    ) -> WorkspaceLease:
        async with self._lock:
            stored = self._leases[(request_scope, lease_id)]
            if stored.released:
                return stored
            released = stored.model_copy(
                update={
                    "released_at": released_at,
                    "patch_artifact_ref": patch_artifact_ref,
                    "snapshot_ref": snapshot_ref,
                }
            )
            self._leases[(request_scope, lease_id)] = released
            return released

    async def sandbox_snapshot_refs(self, request_scope: str, run_id: str) -> tuple[str, ...]:
        """The newest released lease snapshot of the run (`LaneSnapshotRefReader`)."""

        released = sorted(
            (
                lease
                for (scope, _), lease in self._leases.items()
                if scope == request_scope
                and lease.run_id == run_id
                and lease.released_at is not None
                and lease.snapshot_ref is not None
            ),
            key=lambda lease: lease.released_at or lease.expires_at,
        )
        return (released[-1].snapshot_ref,) if released and released[-1].snapshot_ref else ()


__all__ = [
    "InMemoryWorkspaceLeaseStore",
    "LeaseConflict",
    "WorkspaceLease",
    "WorkspaceLeaseStore",
    "lease_identity",
]
