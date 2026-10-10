"""Git worktree workspace leases for `cursor_local` (SPEC-07 section 5.1; FT-G3; MP-04).

`prepare` leases a Workspace: a detached `git worktree` of the target repository at the
binding's base ref (Mission 3), or an initialized repository with one empty base commit
(research missions). The lease row records path, base commit, fence and expiry. At the end of
the session the patch (`git diff --binary` against the base commit, untracked files included,
Mission Control's own projections and packet excluded) is captured and stored before the lease
is released. Git runs in worker threads (`asyncio.to_thread`) so every event loop works.

MP-04: a repository-backed lease is allocated by the provider-neutral `WorkspaceAllocator`
under `CURSOR_LOCAL_POLICY` (one lease per holder generation): the worktree comes from a
dedicated clone under the lease root (never added to the developer's checkout), its path is
checked inside the root after symlink resolution, a dirty local checkout is refused, and the
slot is acquired and released through the fenced ledger.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final, cast
from uuid import UUID

from mission_control.adapters.workspaces.git_workspaces import GitWorkspaceBackend
from mission_control.application.execution.harness.leases import (
    WorkspaceLease,
    WorkspaceLeaseLedger,
    lease_identity,
)
from mission_control.application.workspaces.policy import contained
from mission_control.application.workspaces.service import AllocationRequest, WorkspaceAllocator
from mission_control.domain.execution.bindings import WorkspacePolicyPin
from mission_control.domain.execution.lanes import LaneProfileName

# The FT-G3 behaviour expressed as a policy: one worktree per holder generation, removed after
# its patch and snapshot are stored.
CURSOR_LOCAL_POLICY: Final = WorkspacePolicyPin(
    mode="managed_worktree",
    reuse="none",
    dirty_input="reject",
    cleanup="retain_until_artifacts_registered",
)

BASE_COMMIT_MESSAGE = "mission control workspace base"
# Never part of the agent's patch: Mission Control writes them into the lease itself.
ALWAYS_EXCLUDED = (".mission", "inputs", "outputs")
_IDENTITY = ("-c", "user.name=Mission Control", "-c", "user.email=mission-control@localhost")


class GitWorkspaceError(RuntimeError):
    """A git command on a workspace lease failed (stderr excerpt only, never secrets)."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _environment() -> dict[str, str]:
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("GIT_") and not name.startswith("CURSOR_")
    }
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env


def git(*args: str, cwd: Path, check: bool = True) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        env=_environment(),
        check=False,
    )
    if check and completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()[:400]
        raise GitWorkspaceError(f"git {args[0]} failed: {detail}")
    return completed.stdout.decode("utf-8", "replace")


def git_bytes(*args: str, cwd: Path) -> bytes:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, env=_environment(), check=False
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()[:400]
        raise GitWorkspaceError(f"git {args[0]} failed: {detail}")
    return completed.stdout


def _excludes(paths: Sequence[str]) -> list[str]:
    seen = dict.fromkeys((*ALWAYS_EXCLUDED, *paths))
    return [f":(exclude){path}" for path in seen if path]


@dataclass(frozen=True)
class CapturedPatch:
    diff: bytes
    untracked: tuple[str, ...]

    @property
    def empty(self) -> bool:
        return not self.diff


class GitWorktreeLeaser:
    def __init__(
        self,
        store: WorkspaceLeaseLedger,
        *,
        lease_root: Path,
        clock: Callable[[], datetime] = _utc_now,
        lease_ttl: timedelta = timedelta(hours=4),
        policy: WorkspacePolicyPin = CURSOR_LOCAL_POLICY,
    ) -> None:
        self._store = store
        self._root = lease_root
        self._clock = clock
        self._ttl = lease_ttl
        self._policy = policy
        self._backend = GitWorkspaceBackend(lease_root)
        owner = hashlib.sha256(str(lease_root.expanduser().resolve()).encode()).hexdigest()
        self._allocator = WorkspaceAllocator(
            ledger=store,
            backend=self._backend,
            root=lease_root,
            allocator_ref=f"git-worktree-leaser:{owner[:16]}",
            clock=clock,
            lease_ttl=lease_ttl,
        )

    @property
    def allocator(self) -> WorkspaceAllocator:
        return self._allocator

    def lease_path(self, run_id: str, attempt_no: int, generation: int) -> Path:
        run = hashlib.sha256(run_id.encode("utf-8")).hexdigest()[:16]
        return self._root / run / f"a{attempt_no}-g{generation}"

    async def find(
        self,
        *,
        request_scope: str,
        lane_profile: str,
        harness_execution_id: UUID,
        generation: int,
    ) -> WorkspaceLease | None:
        lease_id, _key = lease_identity(lane_profile, harness_execution_id, generation)
        return await self._store.get(request_scope, lease_id)

    async def acquire(
        self,
        *,
        request_scope: str,
        lane_profile: str,
        harness_execution_id: UUID,
        generation: int,
        run_id: str,
        attempt_no: int,
        repository: str | None,
        base_ref: str,
    ) -> WorkspaceLease:
        path = self.lease_path(run_id, attempt_no, generation)
        if repository is not None:
            return await self._allocator.allocate(
                AllocationRequest(
                    request_scope=request_scope,
                    lane_profile=cast(LaneProfileName, lane_profile),
                    harness_execution_id=harness_execution_id,
                    generation=generation,
                    run_id=run_id,
                    attempt_no=attempt_no,
                    policy=self._policy,
                    repository=repository,
                    base_ref=base_ref,
                    path=str(path),
                    detached=True,
                )
            )
        lease_id, key = lease_identity(lane_profile, harness_execution_id, generation)
        existing = await self._store.get(request_scope, lease_id)
        if existing is not None and not existing.released:
            if await asyncio.to_thread(Path(existing.path).is_dir):
                return existing
        base_commit = await asyncio.to_thread(self._initialize, path)
        lease = WorkspaceLease(
            lease_id=lease_id,
            request_scope=request_scope,
            lease_key=key,
            lane_profile=lane_profile,
            harness_execution_id=harness_execution_id,
            generation=generation,
            run_id=run_id,
            attempt_no=attempt_no,
            path=str(path),
            repository=None,
            base_ref=base_ref,
            base_commit=base_commit,
            fence=generation,
            expires_at=self._clock() + self._ttl,
        )
        return await self._store.record(lease)

    def _initialize(self, path: Path) -> str:
        """A research lease: an initialized repository with one empty base commit."""

        target = contained(self._root, path)
        if (target / ".git").exists():
            # A re-lease after a lost worker: the repository is still there; reuse it.
            return git("rev-parse", "HEAD", cwd=target).strip()
        target.mkdir(parents=True, exist_ok=True)
        git("init", "--quiet", cwd=target)
        git(
            *_IDENTITY,
            "commit",
            "--quiet",
            "--allow-empty",
            "-m",
            BASE_COMMIT_MESSAGE,
            cwd=target,
        )
        return git("rev-parse", "HEAD", cwd=target).strip()

    async def capture_patch(
        self, lease: WorkspaceLease, *, exclude: Sequence[str] = ()
    ) -> CapturedPatch:
        return await asyncio.to_thread(self._capture, Path(lease.path), lease.base_commit, exclude)

    @staticmethod
    def _capture(path: Path, base_commit: str, exclude: Sequence[str]) -> CapturedPatch:
        excludes = _excludes(exclude)
        untracked = tuple(
            line
            for line in git(
                "ls-files", "--others", "--exclude-standard", "--", ".", *excludes, cwd=path
            ).splitlines()
            if line
        )
        git("add", "--all", "--", ".", *excludes, cwd=path)
        diff = git_bytes(
            "diff", "--cached", "--binary", base_commit, "--", ".", *excludes, cwd=path
        )
        return CapturedPatch(diff=diff, untracked=untracked)

    async def outputs(self, lease: WorkspaceLease) -> list[tuple[str, bytes]]:
        return await asyncio.to_thread(self._outputs, Path(lease.path))

    @staticmethod
    def _outputs(path: Path) -> list[tuple[str, bytes]]:
        root = path / "outputs"
        if not root.is_dir():
            return []
        files: list[tuple[str, bytes]] = []
        for item in sorted(root.rglob("*")):
            if item.is_file() and not item.is_symlink():
                files.append((item.relative_to(path).as_posix(), item.read_bytes()))
        return files

    async def release(
        self,
        lease: WorkspaceLease,
        *,
        patch_artifact_ref: str | None,
        snapshot_ref: str | None = None,
    ) -> WorkspaceLease:
        """Release the lease (fenced) and remove its worktree, only after its patch is stored
        (the caller passes its ref, and the frozen lane snapshot a fork of the run restores)."""

        if lease.released:
            return lease
        if lease.slot is None:
            await asyncio.to_thread(self._remove_initialized, Path(lease.path))
            return await self._store.release(
                lease.request_scope,
                lease.lease_id,
                patch_artifact_ref=patch_artifact_ref,
                released_at=self._clock(),
                snapshot_ref=snapshot_ref,
            )
        released = await self._store.release_fenced(
            lease.request_scope,
            lease.lease_id,
            fence=lease.fence,
            patch_artifact_ref=patch_artifact_ref,
            snapshot_ref=snapshot_ref,
            released_at=self._clock(),
            cleanup_status="pending",
        )
        await self._backend.remove(released)
        return await self._store.mark_cleanup(
            released.request_scope, released.lease_id, status="completed"
        )

    def _remove_initialized(self, path: Path) -> None:
        target = contained(self._root, path)
        if target.exists():
            shutil.rmtree(target, ignore_errors=True)


__all__ = [
    "ALWAYS_EXCLUDED",
    "CapturedPatch",
    "GitWorkspaceError",
    "GitWorktreeLeaser",
    "git",
]
