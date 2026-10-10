"""The leased workspace and the local session state root of a Claude session (MP-04/MP-07).

The workspace is allocated through the MP-04 allocator (`application/workspaces/service.py`,
a managed git worktree under a fenced lease). Under it, `.mission/state/claude/` is the
*state root* every local fact of the session lives in, so a worker that re-opens the lease
reads what the dead process left behind:

- `dispatch.json`: the lane's own create/send journal (`DispatchLedger`), written `intended`
  before the native write and `acknowledged` after it, which is what makes
  `reconcile_dispatch` authoritative about a key it never journaled;
- `sessions/<project>/<session_id>.jsonl`: the transcript mirror the pinned SDK writes
  through `ClaudeAgentOptions.session_store` (`claude_agent_sdk.types.SessionStore`:
  `append` is called after the CLI's local write, entries are opaque JSONL lines; `load`
  is used for store-backed resume). Its presence is the lane's "history present" check;
- `config/`: the `CLAUDE_CONFIG_DIR` of the subprocess on credential-in-environment auth
  routes, so the CLI's own transcripts and settings live under the lease too. On the
  owner's CLI-login route the config dir is the owner's (its credentials live there) and the
  mirror is the only copy under the lease (`harness.py` decides per admitted route).
"""

from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal, Protocol
from uuid import UUID

from claude_agent_sdk._internal.session_summary import fold_session_summary
from claude_agent_sdk.types import (
    SessionKey,
    SessionListSubkeysKey,
    SessionStoreEntry,
    SessionStoreListEntry,
    SessionSummaryEntry,
)

from mission_control.application.execution.harness.leases import WorkspaceLease, lease_identity
from mission_control.application.workspaces.service import AllocationRequest, WorkspaceAllocator
from mission_control.application.workspaces.snapshots import CapturedSnapshot, SnapshotArtifacts

STATE_ROOT: Final = ".mission/state/claude"
LEDGER_FILE: Final = "dispatch.json"
SESSIONS_DIR: Final = "sessions"
CONFIG_DIR: Final = "config"
LedgerPhase = Literal["intended", "acknowledged"]
_SAFE: Final = re.compile(r"[^A-Za-z0-9._-]+")


class ClaudeWorkspace(Protocol):
    """What the lane needs from the workspace allocator: lease, find, custody, release."""

    async def acquire(self, request: AllocationRequest) -> WorkspaceLease: ...

    async def find(
        self, request_scope: str, harness_execution_id: UUID, generation: int
    ) -> WorkspaceLease | None: ...

    async def capture(self, lease: WorkspaceLease, *, name: str) -> CapturedSnapshot | None: ...

    async def release(self, lease: WorkspaceLease, custody: CapturedSnapshot | None) -> None: ...


class LeaseReader(Protocol):
    async def get(self, request_scope: str, lease_id: UUID) -> WorkspaceLease | None: ...


class AllocatedWorkspace:
    """`ClaudeWorkspace` over the MP-04 `WorkspaceAllocator` and its lease ledger."""

    def __init__(
        self,
        allocator: WorkspaceAllocator,
        leases: LeaseReader,
        artifacts: SnapshotArtifacts,
        *,
        exclude: tuple[str, ...] = (STATE_ROOT,),
    ) -> None:
        self._allocator = allocator
        self._leases = leases
        self._artifacts = artifacts
        self._exclude = exclude

    async def acquire(self, request: AllocationRequest) -> WorkspaceLease:
        return await self._allocator.allocate(request, artifacts=self._artifacts)

    async def find(
        self, request_scope: str, harness_execution_id: UUID, generation: int
    ) -> WorkspaceLease | None:
        lease_id, _key = lease_identity("claude_agent_sdk", harness_execution_id, generation)
        return await self._leases.get(request_scope, lease_id)

    async def capture(self, lease: WorkspaceLease, *, name: str) -> CapturedSnapshot | None:
        del name
        return await self._allocator.snapshot(
            lease, artifacts=self._artifacts, exclude=self._exclude
        )

    async def release(self, lease: WorkspaceLease, custody: CapturedSnapshot | None) -> None:
        await self._allocator.release(lease, custody=custody)


def state_root(lease_path: str | Path) -> Path:
    return Path(lease_path) / STATE_ROOT


def _safe(value: str) -> str:
    return _SAFE.sub("_", value)[:200] or "_"


@dataclass(frozen=True)
class LedgerEntry:
    phase: LedgerPhase
    native_ref: str | None


class DispatchLedger:
    """`dispatch.json` under the state root: `{"<kind>:<key>": {"phase", "native_ref"}}`."""

    def __init__(self, root: Path) -> None:
        self._path = root / LEDGER_FILE
        self._entries: dict[str, LedgerEntry] | None = None
        self._lock = asyncio.Lock()

    @property
    def path(self) -> Path:
        return self._path

    def _read(self) -> dict[str, LedgerEntry]:
        if not self._path.is_file():
            return {}
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        entries: dict[str, LedgerEntry] = {}
        if isinstance(raw, dict):
            for key, value in raw.items():
                if isinstance(value, dict) and value.get("phase") in {"intended", "acknowledged"}:
                    ref = value.get("native_ref")
                    entries[str(key)] = LedgerEntry(
                        phase=value["phase"], native_ref=ref if isinstance(ref, str) else None
                    )
        return entries

    def _write(self, entries: Mapping[str, LedgerEntry]) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            key: {"phase": entry.phase, "native_ref": entry.native_ref}
            for key, entry in sorted(entries.items())
        }
        tmp = self._path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp.replace(self._path)

    async def _entries_loaded(self) -> dict[str, LedgerEntry]:
        if self._entries is None:
            self._entries = await asyncio.to_thread(self._read)
        return self._entries

    async def intend(self, kind: str, idempotency_key: str) -> None:
        async with self._lock:
            entries = await self._entries_loaded()
            current = entries.get(f"{kind}:{idempotency_key}")
            if current is not None and current.phase == "acknowledged":
                return
            entries[f"{kind}:{idempotency_key}"] = LedgerEntry("intended", None)
            await asyncio.to_thread(self._write, entries)

    async def acknowledge(self, kind: str, idempotency_key: str, native_ref: str) -> None:
        async with self._lock:
            entries = await self._entries_loaded()
            entries[f"{kind}:{idempotency_key}"] = LedgerEntry("acknowledged", native_ref)
            await asyncio.to_thread(self._write, entries)

    async def lookup(self, kind: str, idempotency_key: str) -> LedgerEntry | None:
        async with self._lock:
            # Always re-read: another process (a dead owner) may have written the file.
            self._entries = await asyncio.to_thread(self._read)
            return self._entries.get(f"{kind}:{idempotency_key}")


class StateRootSessionStore:
    """`claude_agent_sdk.types.SessionStore` (append + load) over JSONL files in the state
    root. Entries are passed through verbatim; a repeated `uuid` is appended once."""

    def __init__(self, root: Path) -> None:
        self._root = root / SESSIONS_DIR
        self._seen: dict[str, set[str]] = {}

    @property
    def root(self) -> Path:
        return self._root

    def path_for(self, key: SessionKey) -> Path:
        name = _safe(key["session_id"])
        subpath = key.get("subpath")
        if subpath:
            name = f"{name}__{_safe(subpath)}"
        return self._root / _safe(key["project_key"]) / f"{name}.jsonl"

    def transcript_paths(self, session_id: str) -> tuple[Path, ...]:
        if not self._root.is_dir():
            return ()
        name = f"{_safe(session_id)}.jsonl"
        return tuple(sorted(path for path in self._root.glob(f"*/{name}") if path.is_file()))

    def transcript_present(self, session_id: str) -> bool:
        return any(path.stat().st_size > 0 for path in self.transcript_paths(session_id))

    async def append(self, key: SessionKey, entries: list[SessionStoreEntry]) -> None:
        path = self.path_for(key)
        seen = self._seen.setdefault(str(path), set())
        lines: list[str] = []
        for entry in entries:
            uuid = entry.get("uuid")
            if isinstance(uuid, str) and uuid:
                if uuid in seen:
                    continue
                seen.add(uuid)
            lines.append(json.dumps(entry, sort_keys=True, default=str))
        if not lines:
            return

        def write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as handle:
                handle.write("\n".join(lines) + "\n")

        await asyncio.to_thread(write)

    async def load(self, key: SessionKey) -> list[SessionStoreEntry] | None:
        path = self.path_for(key)
        if not path.is_file():
            return None

        def read() -> list[SessionStoreEntry]:
            items: list[SessionStoreEntry] = []
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    items.append(json.loads(line))
            return items

        return await asyncio.to_thread(read)

    # The optional `SessionStore` methods (the SDK probes for their presence): listing by file
    # modification time, the summary folded from the stored entries, subpath discovery, and
    # a cascading delete of one session's files.

    def _project_dir(self, project_key: str) -> Path:
        return self._root / _safe(project_key)

    @staticmethod
    def _mtime_ms(path: Path) -> int:
        return int(path.stat().st_mtime * 1000)

    async def list_sessions(self, project_key: str) -> list[SessionStoreListEntry]:
        directory = self._project_dir(project_key)

        def scan() -> list[SessionStoreListEntry]:
            if not directory.is_dir():
                return []
            return [
                {"session_id": path.stem, "mtime": self._mtime_ms(path)}
                for path in sorted(directory.glob("*.jsonl"))
                if "__" not in path.stem
            ]

        return await asyncio.to_thread(scan)

    async def list_session_summaries(self, project_key: str) -> list[SessionSummaryEntry]:
        summaries: list[SessionSummaryEntry] = []
        for listed in await self.list_sessions(project_key):
            key: SessionKey = {"project_key": project_key, "session_id": listed["session_id"]}
            entries = await self.load(key) or []
            folded = fold_session_summary(None, key, entries)
            summaries.append({**folded, "mtime": listed["mtime"]})
        return summaries

    async def list_subkeys(self, key: SessionListSubkeysKey) -> list[str]:
        directory = self._project_dir(key["project_key"])
        prefix = f"{_safe(key['session_id'])}__"

        def scan() -> list[str]:
            if not directory.is_dir():
                return []
            return sorted(
                path.stem[len(prefix) :]
                for path in directory.glob(f"{prefix}*.jsonl")
                if path.stem.startswith(prefix)
            )

        return await asyncio.to_thread(scan)

    async def delete(self, key: SessionKey) -> None:
        targets = [self.path_for(key)]
        if not key.get("subpath"):
            directory = self._project_dir(key["project_key"])
            targets.extend(directory.glob(f"{_safe(key['session_id'])}__*.jsonl"))

        def remove() -> None:
            for path in targets:
                if path.is_file():
                    path.unlink()

        await asyncio.to_thread(remove)


def config_dir(root: Path) -> Path:
    return root / CONFIG_DIR


def ensure_state_root(root: Path) -> None:
    (root / SESSIONS_DIR).mkdir(parents=True, exist_ok=True)
    (root / CONFIG_DIR).mkdir(parents=True, exist_ok=True)


def lease_payload(lease: WorkspaceLease) -> dict[str, Any]:
    return {"lease_id": str(lease.lease_id), "path": lease.path, "fence": lease.fence}


__all__ = [
    "CONFIG_DIR",
    "LEDGER_FILE",
    "SESSIONS_DIR",
    "STATE_ROOT",
    "AllocatedWorkspace",
    "ClaudeWorkspace",
    "DispatchLedger",
    "LedgerEntry",
    "StateRootSessionStore",
    "config_dir",
    "ensure_state_root",
    "lease_payload",
    "state_root",
]
