"""Provider-neutral workspace allocation, snapshot, release and cleanup (SPEC-01 "Session
ownership" steps 1 and 8; SPEC-02 "Worktrees and snapshots"; MP-04).

Mission Control allocates the workspace; the agent does not choose whether isolation exists.
`WorkspaceAllocator.allocate` admits the policy for the lane profile, resolves the base commit
in a dedicated clone (never the developer's checkout), refuses a dirty input unless the policy
snapshots it, acquires the workspace slot exclusively through the fenced ledger and only then
materializes the worktree on the slot's unique branch. Every later write (snapshot record,
renewal, release) carries the lease's fence; a holder that was fenced out is refused.

A lease is released only after artifact custody: the caller passes the snapshot it stored.
Cleanup removes only a released lease this allocator owns, that is the newest of its slot,
whose snapshot was registered and whose worktree is still exactly what that snapshot froze;
`retain` never removes, a released lease without custody keeps its failed work, and a dirty
worktree is never force-deleted.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.execution.harness.leases import (
    WorkspaceLease,
    WorkspaceLeaseLedger,
    lease_identity,
)
from mission_control.application.workspaces.errors import (
    CHECKPOINT_INVALID,
    WORKSPACE_CUSTODY_MISSING,
    WORKSPACE_CUSTODY_STALE,
    WORKSPACE_DIRTY_INPUT,
    WORKSPACE_NOT_OWNED,
    WORKSPACE_POLICY_UNSUPPORTED,
    StaleWorkspaceLease,
    WorkspaceError,
)
from mission_control.application.workspaces.policy import (
    BRANCH_SNAPSHOT_PROFILES,
    admit_policy,
    contained,
    reject_nested_root,
    slot_branch,
    slot_of,
    slot_path,
)
from mission_control.application.workspaces.ports import SourceState, WorkspaceBackend
from mission_control.application.workspaces.snapshots import (
    CapturedSnapshot,
    RestoreReceipt,
    SnapshotArtifacts,
    branch_snapshot,
    load_manifest,
    manifest_ref_of,
)
from mission_control.domain.execution.bindings import WorkspacePolicyPin, WorkspaceSnapshot
from mission_control.domain.execution.lanes import LaneProfileName


def _utc_now() -> datetime:
    return datetime.now(UTC)


class AllocationRequest(BaseModel):
    """One holder's request for a workspace (one generation of one harness execution)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1)
    lane_profile: LaneProfileName
    harness_execution_id: UUID
    generation: int = Field(ge=1)
    run_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=1)
    policy: WorkspacePolicyPin
    repository: str | None = None
    base_ref: str = Field(min_length=1)
    # The FT-G3 Cursor lane keeps its own per-generation path and a detached HEAD.
    path: str | None = None
    detached: bool = False


class SharedWorkspaceGrant(BaseModel):
    """A `shared_checkout` participant's access to its parent's leased worktree; valid while
    the parent lease is active under the same fence."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str
    parent_lease_id: UUID
    parent_fence: int
    path: str
    participant: str = Field(min_length=1)
    read_only: bool


CleanupOutcome = Literal["removed", "retained", "not_required", "already_removed"]


class WorkspaceAllocator:
    def __init__(
        self,
        *,
        ledger: WorkspaceLeaseLedger,
        backend: WorkspaceBackend,
        root: Path,
        allocator_ref: str,
        clock: Callable[[], datetime] = _utc_now,
        lease_ttl: timedelta = timedelta(hours=4),
    ) -> None:
        self._ledger = ledger
        self._backend = backend
        self._root = root
        self._allocator_ref = allocator_ref
        self._clock = clock
        self._ttl = lease_ttl

    @property
    def root(self) -> Path:
        return self._root

    # --- allocation -----------------------------------------------------------------------

    async def allocate(
        self, request: AllocationRequest, *, artifacts: SnapshotArtifacts | None = None
    ) -> WorkspaceLease:
        policy = admit_policy(request.lane_profile, request.policy)
        if policy.mode == "provider_workspace":
            raise WorkspaceError(
                WORKSPACE_POLICY_UNSUPPORTED,
                "a provider workspace is recorded with allocate_provider_workspace",
            )
        if policy.mode == "shared_checkout":
            raise WorkspaceError(
                WORKSPACE_POLICY_UNSUPPORTED,
                "a shared_checkout participant joins its parent's lease with join_shared",
            )
        return await self._allocate_worktree(request, policy, artifacts)

    async def _allocate_worktree(
        self,
        request: AllocationRequest,
        policy: WorkspacePolicyPin,
        artifacts: SnapshotArtifacts | None,
    ) -> WorkspaceLease:
        local_source = _local_path(request.repository)
        await asyncio.to_thread(reject_nested_root, self._root, local_source)
        slot = slot_of(
            policy,
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            run_id=request.run_id,
        )
        source = await self._backend.prepare_source(request.repository, request.base_ref)
        self._admit_source(source, policy)
        prior = _retained(await self._ledger.slot_leases(request.request_scope, slot))
        if prior is not None:
            path, branch, base_commit = Path(prior.path), prior.branch, prior.base_commit
        else:
            path = (
                Path(request.path)
                if request.path is not None
                else slot_path(self._root, request.request_scope, slot)
            )
            branch = None if request.detached else slot_branch(request.request_scope, slot)
            base_commit = source.base_commit
        path = await asyncio.to_thread(contained, self._root, path)
        lease_id, key = lease_identity(
            request.lane_profile, request.harness_execution_id, request.generation
        )
        now = self._clock()
        claim = WorkspaceLease(
            lease_id=lease_id,
            request_scope=request.request_scope,
            lease_key=key,
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            run_id=request.run_id,
            attempt_no=request.attempt_no,
            path=str(path),
            repository=request.repository,
            base_ref=request.base_ref,
            base_commit=base_commit,
            fence=0,
            expires_at=now + self._ttl,
            slot=slot,
            branch=branch,
            policy=policy,
            allocator_ref=self._allocator_ref,
        )
        held = await self._ledger.get(request.request_scope, lease_id)
        reacquired = held is not None and not held.released
        lease = await self._ledger.acquire(claim, now=now)
        if (reacquired or prior is not None) and await asyncio.to_thread(Path(lease.path).is_dir):
            await self._backend.verify(lease)
            return lease
        try:
            await self._backend.materialize(lease, source)
        except BaseException:
            await self._ledger.release_fenced(
                lease.request_scope,
                lease.lease_id,
                fence=lease.fence,
                patch_artifact_ref=None,
                snapshot_ref=None,
                released_at=self._clock(),
                cleanup_status="not_required",
            )
            raise
        if source.dirty:
            lease = await self._carry_dirty_input(lease, source, artifacts)
        return lease

    @staticmethod
    def _admit_source(source: SourceState, policy: WorkspacePolicyPin) -> None:
        if not source.dirty:
            return
        if policy.dirty_input == "reject":
            raise WorkspaceError(
                WORKSPACE_DIRTY_INPUT,
                f"the source checkout has {source.dirty_entries} uncommitted or untracked "
                "entries; commit them or declare dirty_input: snapshot",
            )
        if source.checkout_head != source.base_commit:
            raise WorkspaceError(
                WORKSPACE_DIRTY_INPUT,
                "a dirty checkout is snapshotted only onto its own HEAD; the requested base "
                f"{source.base_commit} is not the checkout HEAD {source.checkout_head}",
            )

    async def _carry_dirty_input(
        self, lease: WorkspaceLease, source: SourceState, artifacts: SnapshotArtifacts | None
    ) -> WorkspaceLease:
        if artifacts is None or source.checkout is None:
            raise WorkspaceError(
                WORKSPACE_DIRTY_INPUT, "snapshotting a dirty input needs artifact custody"
            )
        captured = await self._backend.capture(
            Path(source.checkout),
            lease=lease,
            artifacts=artifacts,
            name=f"workspace/{lease.lease_id}/input",
        )
        if captured.manifest is None:
            raise WorkspaceError(WORKSPACE_DIRTY_INPUT, "the dirty input produced no manifest")
        await self._backend.restore(
            captured.manifest, lease, artifacts, snapshot_ref=captured.snapshot_ref
        )
        return await self._ledger.record_snapshot(
            lease.request_scope,
            lease.lease_id,
            fence=lease.fence,
            snapshot=captured.snapshot,
            snapshot_ref=captured.snapshot_ref,
            input_snapshot=True,
        )

    async def allocate_provider_workspace(
        self, request: AllocationRequest, *, branch: str, base_commit: str
    ) -> WorkspaceLease:
        """Record a provider-hosted workspace (the provider isolates it; Mission Control holds
        the fenced record of its branch and base commit)."""

        policy = admit_policy(request.lane_profile, request.policy)
        if policy.mode != "provider_workspace":
            raise WorkspaceError(
                WORKSPACE_POLICY_UNSUPPORTED, f"{request.lane_profile} needs provider_workspace"
            )
        slot = slot_of(
            policy,
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            run_id=request.run_id,
        )
        lease_id, key = lease_identity(
            request.lane_profile, request.harness_execution_id, request.generation
        )
        now = self._clock()
        claim = WorkspaceLease(
            lease_id=lease_id,
            request_scope=request.request_scope,
            lease_key=key,
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            run_id=request.run_id,
            attempt_no=request.attempt_no,
            path=f"provider:{request.lane_profile}:{branch}",
            repository=request.repository,
            base_ref=request.base_ref,
            base_commit=base_commit,
            fence=0,
            expires_at=now + self._ttl,
            slot=slot,
            branch=branch,
            policy=policy,
            allocator_ref=self._allocator_ref,
        )
        return await self._ledger.acquire(claim, now=now)

    async def join_shared(
        self, parent: WorkspaceLease, *, participant: str, read_only: bool = False
    ) -> SharedWorkspaceGrant:
        """A `shared_checkout` participant joins the parent's worktree under its fence."""

        if parent.policy is None or parent.policy.mode != "shared_checkout":
            raise WorkspaceError(
                WORKSPACE_POLICY_UNSUPPORTED,
                "the parent lease does not declare the shared_checkout coordination model",
            )
        current = await self._current(parent)
        return SharedWorkspaceGrant(
            request_scope=current.request_scope,
            parent_lease_id=current.lease_id,
            parent_fence=current.fence,
            path=current.path,
            participant=participant,
            read_only=read_only,
        )

    async def verify_grant(self, grant: SharedWorkspaceGrant) -> WorkspaceLease:
        stored = await self._ledger.get(grant.request_scope, grant.parent_lease_id)
        if stored is None or stored.released or stored.fence != grant.parent_fence:
            raise StaleWorkspaceLease(
                str(grant.parent_lease_id), "the parent lease of this shared checkout moved on"
            )
        return stored

    async def allocate_shared_parent(
        self, request: AllocationRequest, *, artifacts: SnapshotArtifacts | None = None
    ) -> WorkspaceLease:
        """The parent of a `shared_checkout`: an exclusive worktree lease whose policy lets
        declared participants join it."""

        policy = admit_policy(request.lane_profile, request.policy)
        if policy.mode != "shared_checkout":
            raise WorkspaceError(WORKSPACE_POLICY_UNSUPPORTED, "the policy is not shared_checkout")
        return await self._allocate_worktree(request, policy, artifacts)

    # --- fenced writes ----------------------------------------------------------------------

    async def _current(self, lease: WorkspaceLease) -> WorkspaceLease:
        stored = await self._ledger.get(lease.request_scope, lease.lease_id)
        if stored is None:
            raise LookupError(f"workspace lease {lease.lease_id} is not recorded")
        if stored.released or stored.fence != lease.fence:
            raise StaleWorkspaceLease(
                lease.lease_key, f"the slot moved on (state {stored.observed_state})"
            )
        return stored

    async def renew(self, lease: WorkspaceLease) -> WorkspaceLease:
        return await self._ledger.renew(
            lease.request_scope,
            lease.lease_id,
            fence=lease.fence,
            expires_at=self._clock() + self._ttl,
        )

    async def snapshot(
        self,
        lease: WorkspaceLease,
        *,
        artifacts: SnapshotArtifacts,
        exclude: Sequence[str] = (),
    ) -> CapturedSnapshot:
        """Freeze the leased worktree and record the snapshot under the lease's fence."""

        current = await self._current(lease)
        if current.policy is not None and current.policy.mode == "provider_workspace":
            raise WorkspaceError(
                WORKSPACE_POLICY_UNSUPPORTED,
                "a provider workspace is snapshotted from its branch head "
                "(record_provider_snapshot)",
            )
        captured = await self._backend.capture(
            Path(current.path),
            lease=current,
            artifacts=artifacts,
            name=f"workspace/{current.lease_id}/f{current.fence}",
            exclude=exclude,
        )
        await self._ledger.record_snapshot(
            current.request_scope,
            current.lease_id,
            fence=current.fence,
            snapshot=captured.snapshot,
            snapshot_ref=captured.snapshot_ref,
        )
        return captured

    async def record_provider_snapshot(
        self, lease: WorkspaceLease, *, head_commit: str
    ) -> CapturedSnapshot:
        """Record the head commit the caller actually read back from the provider's branch."""

        current = await self._current(lease)
        if current.lane_profile not in BRANCH_SNAPSHOT_PROFILES or current.branch is None:
            raise WorkspaceError(
                WORKSPACE_POLICY_UNSUPPORTED,
                f"{current.lane_profile} does not publish a branch snapshot",
            )
        captured = branch_snapshot(
            lane_profile=cast(LaneProfileName, current.lane_profile),
            branch=current.branch,
            head_commit=head_commit,
            base_commit=current.base_commit,
            producer_lease_id=current.lease_id,
            producer_generation=current.generation,
            captured_at=self._clock(),
        )
        await self._ledger.record_snapshot(
            current.request_scope,
            current.lease_id,
            fence=current.fence,
            snapshot=captured.snapshot,
            snapshot_ref=captured.snapshot_ref,
        )
        return captured

    async def restore(
        self,
        snapshot: WorkspaceSnapshot,
        snapshot_ref: str,
        target: WorkspaceLease,
        *,
        artifacts: SnapshotArtifacts,
    ) -> RestoreReceipt:
        """Re-apply a frozen snapshot into a fresh lease (a fork or a continuation target)."""

        manifest_ref = manifest_ref_of(snapshot_ref)
        if manifest_ref is None:
            raise WorkspaceError(
                CHECKPOINT_INVALID,
                f"{snapshot_ref} is not a local workspace snapshot; a provider branch "
                "snapshot is restored by starting the provider lane at its head",
            )
        current = await self._current(target)
        manifest = await load_manifest(snapshot, manifest_ref, artifacts)
        if current.base_commit != manifest.base_commit:
            raise WorkspaceError(
                CHECKPOINT_INVALID,
                f"the target lease is at {current.base_commit}, the snapshot at "
                f"{manifest.base_commit}",
            )
        return await self._backend.restore(manifest, current, artifacts, snapshot_ref=snapshot_ref)

    async def release(
        self, lease: WorkspaceLease, *, custody: CapturedSnapshot | None
    ) -> WorkspaceLease:
        """Release after custody (`custody` is the snapshot the caller stored); `None`
        releases the slot but keeps the worktree as failed work."""

        current = await self._current(lease)
        provider = current.policy is not None and current.policy.mode == "provider_workspace"
        if custody is not None and current.snapshot_ref != custody.snapshot_ref:
            raise WorkspaceError(
                WORKSPACE_CUSTODY_MISSING, "the custody snapshot was not recorded on this lease"
            )
        released = await self._ledger.release_fenced(
            current.request_scope,
            current.lease_id,
            fence=current.fence,
            patch_artifact_ref=(
                custody.snapshot.patch_artifact_ref if custody is not None else None
            ),
            snapshot_ref=custody.snapshot_ref if custody is not None else None,
            released_at=self._clock(),
            cleanup_status="not_required" if provider else "pending",
        )
        policy = current.policy
        if (
            custody is not None
            and policy is not None
            and policy.cleanup == "immediate"
            and not provider
        ):
            await self.cleanup(released, artifacts=None, verified=custody)
            refreshed = await self._ledger.get(released.request_scope, released.lease_id)
            return refreshed or released
        return released

    # --- cleanup ----------------------------------------------------------------------------

    async def cleanup(
        self,
        lease: WorkspaceLease,
        *,
        artifacts: SnapshotArtifacts | None,
        verified: CapturedSnapshot | None = None,
    ) -> CleanupOutcome:
        """Remove an owned, released, custody-registered worktree that nobody reuses."""

        stored = await self._ledger.get(lease.request_scope, lease.lease_id)
        if stored is None:
            raise LookupError(f"workspace lease {lease.lease_id} is not recorded")
        if stored.allocator_ref != self._allocator_ref:
            raise WorkspaceError(
                WORKSPACE_NOT_OWNED, f"lease {stored.lease_key} belongs to {stored.allocator_ref}"
            )
        if not stored.released:
            raise StaleWorkspaceLease(stored.lease_key, "an active lease is not cleaned up")
        if stored.cleanup_status == "not_required":
            return "not_required"
        if stored.cleanup_status == "completed":
            return "already_removed"
        if stored.policy is not None and stored.policy.cleanup == "retain":
            return "retained"
        if stored.observed_state == "lost":
            raise WorkspaceError(
                WORKSPACE_NOT_OWNED, "a fenced-out lease's worktree belongs to its successor"
            )
        if stored.slot is not None:
            newest = (await self._ledger.slot_leases(stored.request_scope, stored.slot))[-1]
            if newest.lease_id != stored.lease_id:
                raise WorkspaceError(
                    WORKSPACE_NOT_OWNED,
                    f"the slot's worktree was handed on to {newest.lease_key}",
                )
        if stored.workspace_snapshot is None or stored.snapshot_ref is None:
            raise WorkspaceError(
                WORKSPACE_CUSTODY_MISSING,
                "no snapshot was registered for this lease; its work is preserved",
            )
        await asyncio.to_thread(contained, self._root, Path(stored.path))
        if verified is not None and verified.snapshot_ref == stored.snapshot_ref:
            manifest = verified.manifest
        else:
            manifest_ref = manifest_ref_of(stored.snapshot_ref)
            if artifacts is None or manifest_ref is None:
                raise WorkspaceError(
                    WORKSPACE_CUSTODY_MISSING, "the registered snapshot cannot be read back"
                )
            manifest = await load_manifest(stored.workspace_snapshot, manifest_ref, artifacts)
        if manifest is None:
            raise WorkspaceError(WORKSPACE_CUSTODY_MISSING, "the snapshot carries no manifest")
        if await asyncio.to_thread(Path(stored.path).exists) and not await self._backend.matches(
            stored, manifest
        ):
            raise WorkspaceError(
                WORKSPACE_CUSTODY_STALE,
                "the worktree changed after its snapshot was registered; it is not removed",
            )
        await self._backend.remove(stored)
        await self._ledger.mark_cleanup(stored.request_scope, stored.lease_id, status="completed")
        return "removed"


def _local_path(repository: str | None) -> Path | None:
    if repository is None or "://" in repository or repository.startswith("git@"):
        return None
    return Path(repository)


def _retained(slot_leases: tuple[WorkspaceLease, ...]) -> WorkspaceLease | None:
    """The slot's newest lease whose worktree still exists for reuse."""

    if not slot_leases:
        return None
    newest = slot_leases[-1]
    if newest.cleanup_status in {"completed", "not_required"}:
        return None
    return newest


__all__ = [
    "AllocationRequest",
    "CleanupOutcome",
    "SharedWorkspaceGrant",
    "WorkspaceAllocator",
]
