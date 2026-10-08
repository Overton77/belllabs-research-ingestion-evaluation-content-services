"""Git worktree workspace leases for `cursor_local` (SPEC-07 section 5.1; FT-G3).

`prepare` leases a Workspace: a detached `git worktree` of the target repository at the
binding's base ref (Mission 3), or an initialized repository with one empty base commit
(research missions). The lease row records path, base commit, fence and expiry. At the end of
the session the patch (`git diff --binary` against the base commit, untracked files included,
Mission Control's own projections and packet excluded) is captured and stored before the lease
is released. Git runs in worker threads (`asyncio.to_thread`) so every event loop works.
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
from uuid import UUID

from mission_control.application.execution.harness.leases import (
    WorkspaceLease,
    WorkspaceLeaseStore,
    lease_identity,
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


def _is_remote(repository: str) -> bool:
    return "://" in repository or repository.startswith("git@")


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
        store: WorkspaceLeaseStore,
        *,
        lease_root: Path,
        clock: Callable[[], datetime] = _utc_now,
        lease_ttl: timedelta = timedelta(hours=4),
    ) -> None:
        self._store = store
        self._root = lease_root
        self._clock = clock
        self._ttl = lease_ttl

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
        lease_id, key = lease_identity(lane_profile, harness_execution_id, generation)
        existing = await self._store.get(request_scope, lease_id)
        if existing is not None and not existing.released:
            if await asyncio.to_thread(Path(existing.path).is_dir):
                return existing
        path = self.lease_path(run_id, attempt_no, generation)
        base_commit = await asyncio.to_thread(self._materialize, path, repository, base_ref)
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
            repository=repository,
            base_ref=base_ref,
            base_commit=base_commit,
            fence=generation,
            expires_at=self._clock() + self._ttl,
        )
        return await self._store.record(lease)

    def _mirror(self, repository: str) -> Path:
        if not _is_remote(repository):
            return Path(repository)
        return self._root / "_repos" / hashlib.sha256(repository.encode()).hexdigest()[:16]

    def _source(self, repository: str) -> Path:
        mirror = self._mirror(repository)
        if not _is_remote(repository):
            return mirror
        if not (mirror / ".git").exists() and not (mirror / "HEAD").exists():
            mirror.parent.mkdir(parents=True, exist_ok=True)
            git("clone", "--quiet", "--no-checkout", repository, str(mirror), cwd=mirror.parent)
        else:
            git("fetch", "--quiet", "origin", cwd=mirror)
        return mirror

    def _materialize(self, path: Path, repository: str | None, base_ref: str) -> str:
        if (path / ".git").exists():
            # A re-lease after a lost worker: the worktree is still there; reuse it.
            return git("rev-parse", "HEAD", cwd=path).strip()
        path.parent.mkdir(parents=True, exist_ok=True)
        if repository is not None:
            source = self._source(repository)
            git("worktree", "add", "--detach", "--force", str(path), base_ref, cwd=source)
        else:
            path.mkdir(parents=True, exist_ok=True)
            git("init", "--quiet", cwd=path)
            git(
                *_IDENTITY,
                "commit",
                "--quiet",
                "--allow-empty",
                "-m",
                BASE_COMMIT_MESSAGE,
                cwd=path,
            )
        return git("rev-parse", "HEAD", cwd=path).strip()

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
        self, lease: WorkspaceLease, *, patch_artifact_ref: str | None
    ) -> WorkspaceLease:
        """Remove the worktree only after its patch is stored (the caller passes its ref)."""

        if lease.released:
            return lease
        await asyncio.to_thread(self._remove, Path(lease.path), lease.repository)
        return await self._store.release(
            lease.request_scope,
            lease.lease_id,
            patch_artifact_ref=patch_artifact_ref,
            released_at=self._clock(),
        )

    def _remove(self, path: Path, repository: str | None) -> None:
        if repository is not None and path.exists():
            source = self._mirror(repository)
            git("worktree", "remove", "--force", str(path), cwd=source, check=False)
            git("worktree", "prune", cwd=source, check=False)
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)


__all__ = [
    "ALWAYS_EXCLUDED",
    "CapturedPatch",
    "GitWorkspaceError",
    "GitWorktreeLeaser",
    "git",
]
