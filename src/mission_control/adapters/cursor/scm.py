"""Branch control and diffs through the SCM for `cursor_cloud` (SPEC-07 section 6; FT-G5).

Cloud agents read `.cursor/rules`, `.cursor/agents`, `.cursor/hooks.json` and project MCP from
the repository, and v1 has no branch-name field and no diff or commit SHA. So `prepare`
creates branch `mc/<run_id>` from the binding's base ref, commits the Host Projection and the
Context Packet files on it and pushes it; the agent then works on that branch
(`starting_ref` plus `workOnCurrentBranch`). `end_session` fetches the branch back and takes the
diff against the base commit as the patch artifact, excluding Mission Control's own files.

Git runs in worker threads through a local mirror per repository. Credentials are the worker's
git configuration (credential helper, SSH agent); none is passed through Mission Control.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import subprocess
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from mission_control.adapters.cursor.workspace import ALWAYS_EXCLUDED, GitWorkspaceError

_IDENTITY = ("-c", "user.name=Mission Control", "-c", "user.email=mission-control@localhost")


def _environment() -> dict[str, str]:
    # Keep the worker's git credential configuration; never forward Cursor credentials.
    env = {name: value for name, value in os.environ.items() if not name.startswith("CURSOR_")}
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env


def _git(*args: str, cwd: Path) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, env=_environment(), check=False
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()[:400]
        raise GitWorkspaceError(f"git {args[0]} failed: {detail}")
    return completed.stdout.decode("utf-8", "replace")


def _git_bytes(*args: str, cwd: Path) -> bytes:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, env=_environment(), check=False
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()[:400]
        raise GitWorkspaceError(f"git {args[0]} failed: {detail}")
    return completed.stdout


@dataclass(frozen=True)
class PublishedBranch:
    branch: str
    head: str
    base_commit: str


class GitBranchPublisher:
    def __init__(self, root: Path) -> None:
        self._root = root

    def _mirror(self, repository: str) -> Path:
        mirror = self._root / hashlib.sha256(repository.encode("utf-8")).hexdigest()[:16]
        if not (mirror / "HEAD").exists():
            mirror.parent.mkdir(parents=True, exist_ok=True)
            _git("clone", "--quiet", "--bare", repository, str(mirror), cwd=mirror.parent)
        else:
            _git("fetch", "--quiet", "--prune", "origin", "+refs/heads/*:refs/heads/*", cwd=mirror)
        return mirror

    async def publish(
        self,
        *,
        repository: str,
        base_ref: str,
        branch: str,
        files: Sequence[tuple[str, bytes]],
    ) -> PublishedBranch:
        return await asyncio.to_thread(self._publish, repository, base_ref, branch, files)

    def _publish(
        self, repository: str, base_ref: str, branch: str, files: Sequence[tuple[str, bytes]]
    ) -> PublishedBranch:
        mirror = self._mirror(repository)
        base_commit = _git("rev-parse", f"{base_ref}^{{commit}}", cwd=mirror).strip()
        existing = _git("ls-remote", "--heads", "origin", branch, cwd=mirror).split()
        if existing:
            # Idempotent prepare: the branch was already published for this run.
            return PublishedBranch(branch=branch, head=existing[0], base_commit=base_commit)
        with tempfile.TemporaryDirectory(prefix="mc-branch-") as scratch:
            work = Path(scratch) / "work"
            _git("worktree", "add", "--quiet", "-b", branch, str(work), base_commit, cwd=mirror)
            try:
                for path, content in files:
                    relative = PurePosixPath(path.replace("\\", "/").lstrip("/"))
                    if ".." in relative.parts or not relative.parts:
                        raise GitWorkspaceError(f"unsafe branch path: {path}")
                    target = work.joinpath(*relative.parts)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(content)
                _git("add", "--all", cwd=work)
                _git(
                    *_IDENTITY,
                    "commit",
                    "--quiet",
                    "--allow-empty",
                    "-m",
                    f"mission control: projections and context for {branch}",
                    cwd=work,
                )
                head = _git("rev-parse", "HEAD", cwd=work).strip()
                _git("push", "--quiet", "origin", f"{branch}:refs/heads/{branch}", cwd=mirror)
            finally:
                _git("worktree", "remove", "--force", str(work), cwd=mirror)
        return PublishedBranch(branch=branch, head=head, base_commit=base_commit)

    async def diff(
        self,
        *,
        repository: str,
        base: str,
        branch: str,
        exclude: Sequence[str] = (),
    ) -> tuple[bytes, str]:
        """The branch's diff against `base` (projections and packet excluded) and its head."""

        return await asyncio.to_thread(self._diff, repository, base, branch, exclude)

    def _diff(
        self, repository: str, base: str, branch: str, exclude: Sequence[str]
    ) -> tuple[bytes, str]:
        mirror = self._mirror(repository)
        head = _git("rev-parse", f"refs/heads/{branch}", cwd=mirror).strip()
        excludes = [
            f":(exclude){path}" for path in dict.fromkeys((*ALWAYS_EXCLUDED, *exclude)) if path
        ]
        diff = _git_bytes("diff", "--binary", base, head, "--", ".", *excludes, cwd=mirror)
        return diff, head


__all__ = ["GitBranchPublisher", "PublishedBranch"]
