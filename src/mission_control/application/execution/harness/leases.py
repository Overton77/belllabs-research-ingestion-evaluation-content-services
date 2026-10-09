"""Workspace leases of file-based lanes (SPEC-07 section 5.1 and Persistence; FT-G3; MP-04).

A Workspace lease is the directory one harness execution generation works in (a git worktree
of the target repository at the binding's base ref, or an initialized empty repository). It is
fenced, expires with the attempt and is released only after its patch artifact is stored. The
production store writes `mission_control.workspace_lease` (migration 0003; runtime grants in
0030, G3 section); the lease key is `<lane>:<harness_execution_id>:<generation>`.

MP-04 generalizes the record to every local and provider-workspace lane: a lease belongs to a
*workspace slot* (the writable workspace it may write, derived from the reuse policy), and a
`WorkspaceLeaseLedger` acquires a slot exclusively and fences every later write by the fence
token it issued. A lower generation, a released holder or a fence older than the slot's
current one is rejected (`StaleWorkspaceLease`); a live holder makes a competing acquisition
fail (`WorkspaceLeaseHeld`). The FT-G3 first-wins `record`/`release` remain for the Cursor lane.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from mission_control.application.workspaces.errors import (
    StaleWorkspaceLease,
    WorkspaceLeaseHeld,
)
from mission_control.domain.execution.bindings import WorkspacePolicyPin, WorkspaceSnapshot

_LEASE_NAMESPACE = UUID("6b1a7c42-93d4-4e0f-a1b8-2f5c7d9e3a61")

LeaseObservedState = Literal["pending", "active", "released", "lost", "unknown"]
LeaseCleanupStatus = Literal["not_required", "pending", "completed", "failed"]


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
    # MP-04: the writable workspace this lease holds (None for FT-G3 per-generation leases),
    # its unique branch, the policy it was allocated under and the allocator that owns it.
    slot: str | None = Field(default=None, min_length=1, max_length=512)
    branch: str | None = Field(default=None, min_length=1, max_length=256)
    policy: WorkspacePolicyPin | None = None
    allocator_ref: str | None = Field(default=None, min_length=1, max_length=256)
    observed_state: LeaseObservedState = "active"
    cleanup_status: LeaseCleanupStatus = "pending"
    workspace_snapshot: WorkspaceSnapshot | None = None
    input_snapshot_ref: str | None = None
    lost_to: UUID | None = None

    @property
    def released(self) -> bool:
        return self.released_at is not None

    def expired(self, now: datetime) -> bool:
        return self.expires_at <= now


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


class WorkspaceLeaseLedger(WorkspaceLeaseStore, Protocol):
    """Exclusive, fenced slot leases (MP-04). Every write names the fence it holds."""

    async def acquire(self, claim: WorkspaceLease, *, now: datetime) -> WorkspaceLease:
        """Acquire `claim.slot` for the claim's holder; returns the stored lease with the fence
        it was issued. The same holder re-acquiring an active lease gets it back unchanged."""
        ...

    async def renew(
        self, request_scope: str, lease_id: UUID, *, fence: int, expires_at: datetime
    ) -> WorkspaceLease: ...

    async def record_snapshot(
        self,
        request_scope: str,
        lease_id: UUID,
        *,
        fence: int,
        snapshot: WorkspaceSnapshot,
        snapshot_ref: str,
        input_snapshot: bool = False,
    ) -> WorkspaceLease: ...

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
    ) -> WorkspaceLease: ...

    async def mark_cleanup(
        self, request_scope: str, lease_id: UUID, *, status: LeaseCleanupStatus
    ) -> WorkspaceLease: ...

    async def slot_leases(self, request_scope: str, slot: str) -> tuple[WorkspaceLease, ...]:
        """Every lease of the slot, oldest fence first."""
        ...


def issue_fence(claim: WorkspaceLease, slot_leases: tuple[WorkspaceLease, ...]) -> int:
    """The next fence of a slot: above every fence it issued, never below the generation."""

    highest = max((lease.fence for lease in slot_leases), default=0)
    return max(highest + 1, claim.generation)


def admission_refusal(
    claim: WorkspaceLease, slot_leases: tuple[WorkspaceLease, ...], now: datetime
) -> tuple[WorkspaceLease | None, Exception | None]:
    """Decide a slot acquisition from the slot's leases (shared by every ledger).

    Returns `(takeover, None)` when the claim may proceed, where `takeover` is an expired
    active lease the claim fences out, or `(None, error)` when it must be refused.
    """

    for lease in slot_leases:
        if (
            lease.harness_execution_id == claim.harness_execution_id
            and lease.lane_profile == claim.lane_profile
            and lease.generation > claim.generation
        ):
            return None, StaleWorkspaceLease(
                claim.lease_key,
                f"generation {claim.generation} is superseded by generation {lease.generation}",
            )
    active = [lease for lease in slot_leases if not lease.released]
    for lease in active:
        if not lease.expired(now):
            return None, WorkspaceLeaseHeld(claim.slot or claim.lease_key, lease.lease_key)
    return (active[-1] if active else None), None


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
                    "observed_state": "released",
                    "cleanup_status": "completed",
                }
            )
            self._leases[(request_scope, lease_id)] = released
            return released

    # --- MP-04 fenced ledger ------------------------------------------------------------------

    def _slot(self, request_scope: str, slot: str) -> tuple[WorkspaceLease, ...]:
        return tuple(
            sorted(
                (
                    lease
                    for (scope, _), lease in self._leases.items()
                    if scope == request_scope and lease.slot == slot
                ),
                key=lambda lease: lease.fence,
            )
        )

    async def acquire(self, claim: WorkspaceLease, *, now: datetime) -> WorkspaceLease:
        if claim.slot is None:
            raise ValueError("a fenced acquisition names its workspace slot")
        async with self._lock:
            key = (claim.request_scope, claim.lease_id)
            existing = self._leases.get(key)
            if existing is not None:
                if existing.released:
                    raise StaleWorkspaceLease(claim.lease_key, "the holder already released it")
                return existing
            slot_leases = self._slot(claim.request_scope, claim.slot)
            takeover, refusal = admission_refusal(claim, slot_leases, now)
            if refusal is not None:
                raise refusal
            if takeover is not None:
                self._leases[(takeover.request_scope, takeover.lease_id)] = takeover.model_copy(
                    update={
                        "released_at": now,
                        "observed_state": "lost",
                        "lost_to": claim.lease_id,
                    }
                )
            lease = claim.model_copy(
                update={
                    "fence": issue_fence(claim, slot_leases),
                    "observed_state": "active",
                    "released_at": None,
                }
            )
            self._leases[key] = lease
            return lease

    def _current(self, request_scope: str, lease_id: UUID, fence: int) -> WorkspaceLease:
        stored = self._leases.get((request_scope, lease_id))
        if stored is None:
            raise LookupError(f"workspace lease {lease_id} is not recorded")
        if stored.released:
            raise StaleWorkspaceLease(stored.lease_key, f"it is {stored.observed_state}")
        if stored.fence != fence:
            raise StaleWorkspaceLease(
                stored.lease_key, f"fence {fence} is not the current fence {stored.fence}"
            )
        return stored

    async def renew(
        self, request_scope: str, lease_id: UUID, *, fence: int, expires_at: datetime
    ) -> WorkspaceLease:
        async with self._lock:
            stored = self._current(request_scope, lease_id, fence)
            renewed = stored.model_copy(update={"expires_at": expires_at})
            self._leases[(request_scope, lease_id)] = renewed
            return renewed

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
        async with self._lock:
            stored = self._current(request_scope, lease_id, fence)
            update: dict[str, object] = (
                {"input_snapshot_ref": snapshot_ref}
                if input_snapshot
                else {"workspace_snapshot": snapshot, "snapshot_ref": snapshot_ref}
            )
            recorded = stored.model_copy(update=update)
            self._leases[(request_scope, lease_id)] = recorded
            return recorded

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
        async with self._lock:
            stored = self._current(request_scope, lease_id, fence)
            released = stored.model_copy(
                update={
                    "released_at": released_at,
                    "patch_artifact_ref": patch_artifact_ref,
                    "snapshot_ref": snapshot_ref or stored.snapshot_ref,
                    "observed_state": "released",
                    "cleanup_status": cleanup_status,
                }
            )
            self._leases[(request_scope, lease_id)] = released
            return released

    async def mark_cleanup(
        self, request_scope: str, lease_id: UUID, *, status: LeaseCleanupStatus
    ) -> WorkspaceLease:
        async with self._lock:
            stored = self._leases.get((request_scope, lease_id))
            if stored is None:
                raise LookupError(f"workspace lease {lease_id} is not recorded")
            if not stored.released:
                raise StaleWorkspaceLease(stored.lease_key, "an active lease is not cleaned up")
            marked = stored.model_copy(update={"cleanup_status": status})
            self._leases[(request_scope, lease_id)] = marked
            return marked

    async def slot_leases(self, request_scope: str, slot: str) -> tuple[WorkspaceLease, ...]:
        return self._slot(request_scope, slot)

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
    "LeaseCleanupStatus",
    "LeaseConflict",
    "LeaseObservedState",
    "StaleWorkspaceLease",
    "WorkspaceLease",
    "WorkspaceLeaseHeld",
    "WorkspaceLeaseLedger",
    "WorkspaceLeaseStore",
    "admission_refusal",
    "issue_fence",
    "lease_identity",
]
