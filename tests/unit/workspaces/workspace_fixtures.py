"""Real-git fixtures for MP-04 workspace allocation tests (temporary repositories, the
in-memory lease ledger and an in-memory artifact store; no provider is contacted)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from mission_control.adapters.cursor.git_workspaces import GitWorkspaceBackend, run_git
from mission_control.application.execution.harness.leases import InMemoryWorkspaceLeaseStore
from mission_control.application.workspaces.service import AllocationRequest, WorkspaceAllocator
from mission_control.domain.execution.bindings import WorkspacePolicyPin

SCOPE = "scope/00000000-0000-0000-0000-000000000001/app/00000000-0000-0000-0000-000000000002"
IDENTITY = ("-c", "user.name=Fixture", "-c", "user.email=fixture@localhost")


def git(*args: str, cwd: Path) -> str:
    return run_git(*args, cwd=cwd).decode("utf-8", "replace")


def commit(cwd: Path, message: str) -> str:
    git(*IDENTITY, "commit", "--quiet", "-m", message, cwd=cwd)
    return git("rev-parse", "HEAD", cwd=cwd).strip()


def make_primary(root: Path) -> Path:
    """The developer's checkout: a few text files, one binary, one ignored pattern."""

    root.mkdir(parents=True)
    git("init", "--quiet", "--initial-branch=main", cwd=root)
    files = {
        "a.txt": b"a0\n",
        "b.txt": b"b0\n",
        "c.txt": b"c0\n",
        "d.txt": b"d0\n",
        "e.txt": b"e0\n",
        "f.txt": b"f0\n",
        "g.txt": b"g0\n",
        "blob.bin": bytes(range(256)),
        "docs/guide.md": b"# guide\n",
        ".gitignore": b"*.log\n",
    }
    for path, content in files.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    git("add", "--all", cwd=root)
    commit(root, "base")
    return root


class MemoryArtifacts:
    def __init__(self) -> None:
        self.staged: dict[str, bytes] = {}

    async def stage(self, *, request_scope: str, name: str, content: bytes, media_type: str) -> str:
        ref = f"payload://{len(self.staged)}/{name}"
        self.staged[ref] = content
        return ref

    async def retrieve(self, durable_ref: str) -> bytes:
        try:
            return self.staged[durable_ref]
        except KeyError as error:
            raise LookupError(durable_ref) from error


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 10, 8, 12, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, delta: timedelta) -> None:
        self.now += delta


@dataclass
class Stack:
    allocator: WorkspaceAllocator
    ledger: InMemoryWorkspaceLeaseStore
    backend: GitWorkspaceBackend
    artifacts: MemoryArtifacts
    clock: Clock
    root: Path
    primary: Path


def stack(tmp_path: Path, *, primary: Path | None = None, owner: str = "worker-a") -> Stack:
    ledger = InMemoryWorkspaceLeaseStore()
    root = tmp_path / "leases"
    backend = GitWorkspaceBackend(root)
    clock = Clock()
    allocator = WorkspaceAllocator(
        ledger=ledger, backend=backend, root=root, allocator_ref=owner, clock=clock
    )
    return Stack(
        allocator=allocator,
        ledger=ledger,
        backend=backend,
        artifacts=MemoryArtifacts(),
        clock=clock,
        root=root,
        primary=primary or make_primary(tmp_path / "primary"),
    )


def policy(**changes: Any) -> WorkspacePolicyPin:
    fields: dict[str, Any] = {
        "mode": "managed_worktree",
        "reuse": "within_run",
        "dirty_input": "reject",
        "cleanup": "retain_until_artifacts_registered",
        **changes,
    }
    return WorkspacePolicyPin.model_validate(fields)


def request(
    repository: Path | str | None,
    *,
    run_id: str = "run-1",
    harness_execution_id: UUID | None = None,
    generation: int = 1,
    base_ref: str = "main",
    lane_profile: str = "claude_agent_sdk",
    **changes: Any,
) -> AllocationRequest:
    fields: dict[str, Any] = {
        "request_scope": SCOPE,
        "lane_profile": lane_profile,
        "harness_execution_id": harness_execution_id or uuid4(),
        "generation": generation,
        "run_id": run_id,
        "attempt_no": 1,
        "policy": policy(),
        "repository": str(repository) if repository is not None else None,
        "base_ref": base_ref,
        **changes,
    }
    return AllocationRequest.model_validate(fields)


def link(target: Path, at: Path) -> None:
    """A directory symlink, or a junction where this Windows account cannot create symlinks."""

    try:
        at.symlink_to(target, target_is_directory=True)
    except OSError:
        if os.name != "nt":
            raise
        import _winapi

        _winapi.CreateJunction(str(target), str(at))


def porcelain(root: Path) -> list[str]:
    raw = run_git("status", "--porcelain=v1", "-z", "--untracked-files=all", cwd=root)
    return sorted(item.decode() for item in raw.split(b"\0") if item)


def present(path: str | Path) -> bool:
    return Path(path).exists()


def within(path: str | Path, root: Path) -> bool:
    return Path(path).resolve().is_relative_to(root.resolve())


__all__ = [
    "IDENTITY",
    "SCOPE",
    "Clock",
    "MemoryArtifacts",
    "Stack",
    "commit",
    "git",
    "link",
    "make_primary",
    "policy",
    "porcelain",
    "present",
    "request",
    "stack",
    "within",
]
