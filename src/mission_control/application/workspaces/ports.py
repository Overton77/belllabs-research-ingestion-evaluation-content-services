"""Ports of provider-neutral workspace allocation (MP-04).

`WorkspaceBackend` is the filesystem/VCS side (the git implementation lives with the lane
adapters); the lease ledger is `application.execution.harness.leases.WorkspaceLeaseLedger`;
artifact custody is `application.workspaces.snapshots.SnapshotArtifacts`.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.execution.harness.leases import WorkspaceLease
from mission_control.application.workspaces.snapshots import (
    CapturedSnapshot,
    RestoreReceipt,
    SnapshotArtifacts,
    WorkspaceArtifactManifest,
)


class SourceState(BaseModel):
    """The resolved input of an allocation: the base commit in the dedicated clone and, for
    a local working checkout, whether it carries uncommitted or untracked changes."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    repository: str | None
    base_ref: str = Field(min_length=1)
    base_commit: str = Field(min_length=7)
    clone: str | None = None
    checkout: str | None = None
    checkout_head: str | None = None
    dirty_entries: int = Field(default=0, ge=0)

    @property
    def dirty(self) -> bool:
        return self.dirty_entries > 0


class WorkspaceBackend(Protocol):
    async def prepare_source(self, repository: str | None, base_ref: str) -> SourceState:
        """Fetch the repository into its dedicated clone and resolve `base_ref`; inspect a
        local working checkout without modifying it."""
        ...

    async def materialize(self, lease: WorkspaceLease, source: SourceState) -> None:
        """Create the lease's worktree at `lease.base_commit` on `lease.branch`; refuse an
        existing branch or path rather than reusing it."""
        ...

    async def verify(self, lease: WorkspaceLease) -> None:
        """Prove an existing worktree is the lease's: inside the root, its own repository
        top level, on the lease's branch."""
        ...

    async def capture(
        self,
        root: Path,
        *,
        lease: WorkspaceLease,
        artifacts: SnapshotArtifacts,
        name: str,
        exclude: Sequence[str] = (),
    ) -> CapturedSnapshot:
        """Freeze `root` (a leased worktree or a source checkout) without modifying it."""
        ...

    async def restore(
        self,
        manifest: WorkspaceArtifactManifest,
        lease: WorkspaceLease,
        artifacts: SnapshotArtifacts,
        *,
        snapshot_ref: str,
    ) -> RestoreReceipt:
        """Re-apply a snapshot into a fresh lease at its base commit and verify it on disk."""
        ...

    async def matches(self, lease: WorkspaceLease, manifest: WorkspaceArtifactManifest) -> bool:
        """Whether the worktree is still exactly what the manifest froze."""
        ...

    async def remove(self, lease: WorkspaceLease) -> None:
        """Remove the lease's worktree and its branch from the dedicated clone."""
        ...


__all__ = ["SourceState", "WorkspaceBackend"]
