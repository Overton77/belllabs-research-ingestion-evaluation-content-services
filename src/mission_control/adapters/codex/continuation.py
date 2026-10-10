"""`codex` continuation: workspace snapshots and the fresh-thread hydrator (MP-12).

SPEC-07 section 7 `request_continuation` is *emulated* on this lane, as on Cursor and Claude:

1. `CodexWorkspaceSnapshots` is the `WorkspaceSnapshotPort` for a live session. It freezes the
   lease's `inputs/`, `outputs/` and `.mission/` content-addressed (the shared
   `adapters/cursor/snapshot.packet_tree` excludes `.mission/state/**`, so the lane's state
   root and the session's `CODEX_HOME` never leave the lease) and takes custody of the lease's
   git patch (`CodexLocalHarness.workspace_patch`: projection files and the state root
   excluded) as `/.mission/continuation/workspace.patch`. The manifest is stored so `load`
   finds it on any worker.
2. `ContinuationService.seal` builds and validates the Continuation Checkpoint and its
   `purpose = continuation` packet from that snapshot.
3. `CodexSessionHydrator.hydrate` restores the snapshot files into the live lease (the patch
   as evidence: the working tree is the same lease, so nothing is re-applied), writes the
   continuation packet's `.mission/context.md` / `.mission/inputs.json`, reads back what it
   wrote (the receipt's digests) and asks the harness for a **fresh** thread on a **fresh**
   app-server (`CodexLocalHarness.hydrate_session`: `thread/start`, no `thread/resume` of the
   source, no `thread/fork`). The checkpoint packet is the only carrier of the source
   conversation; no provider conversation fork is claimed. The continuation turn is sent to
   the target by the next `lane.turn` (activated target) or by the in-segment handover
   (`LaneTurnService._hand_over`), which adopts the target and terminates the source.

`codex_continuation_registration` is the lane's `LaneContinuationRegistration`
(`qualified=False`: only a recorded live drill flips it).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Final, Protocol

from mission_control.adapters.codex.harness import PROFILE, CodexLocalHarness
from mission_control.adapters.cursor.projection import safe_relative
from mission_control.adapters.cursor.snapshot import CHECKPOINT_INVALID, bytes_digest, packet_tree
from mission_control.application.context.continuation import (
    ContinuationRejected,
    HydrationReceipt,
    HydrationRequest,
    WorkspaceSnapshot,
)
from mission_control.application.context.hydrators import LaneContinuationRegistration

WORKSPACE_SNAPSHOT_PREFIX: Final = "codex-workspace:"
PATCH_PATH: Final = ".mission/continuation/workspace.patch"
_LABEL: Final = "codex-local"


class SnapshotStore(Protocol):
    """Content-addressed custody (`adapters/cursor/snapshot.SnapshotArtifacts` shape)."""

    async def stage(
        self, *, request_scope: str, name: str, content: bytes, media_type: str
    ) -> str: ...

    async def retrieve(self, durable_ref: str) -> bytes: ...


def _selected(path: str, roots: Sequence[str]) -> bool:
    prefixes = tuple(item.strip("/").rstrip("*").rstrip("/") for item in roots)
    return not prefixes or any(
        path == prefix or path.startswith(prefix + "/") for prefix in prefixes
    )


def _write(root: Path, path: str, content: bytes) -> None:
    relative = safe_relative(path)
    target = root.joinpath(*relative.parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.chmod(0o644)  # packet files are materialized read-only
    target.write_bytes(content)


def _read_digests(root: Path, paths: Sequence[str]) -> dict[str, str]:
    observed: dict[str, str] = {}
    for path in paths:
        relative = safe_relative(path)
        target = root.joinpath(*relative.parts)
        if target.is_file():
            observed[f"/{relative.as_posix()}"] = bytes_digest(target.read_bytes())
    return observed


class CodexWorkspaceSnapshots:
    """`WorkspaceSnapshotPort` over the lease of a live `codex` session."""

    def __init__(self, harness: CodexLocalHarness, store: SnapshotStore) -> None:
        self._harness = harness
        self._store = store

    async def snapshot(
        self, *, request_scope: str, run_key: str, session_ref: str, roots: Sequence[str]
    ) -> WorkspaceSnapshot:
        heid = self._harness.live_session(session_ref)
        if heid is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID, f"no live codex lease holds thread {session_ref}"
            )
        root = Path(self._harness.lease_path(heid))
        tree = await asyncio.to_thread(packet_tree, root)
        files = [(path, content) for path, content in tree if path != PATCH_PATH]
        files.append((PATCH_PATH, await self._harness.workspace_patch(heid)))
        manifest: dict[str, str] = {}
        durable: dict[str, str] = {}
        total = 0
        for path, content in files:
            if path != PATCH_PATH and not _selected(path, roots):
                continue
            manifest[f"/{path}"] = bytes_digest(content)
            durable[f"/{path}"] = await self._store.stage(
                request_scope=request_scope,
                name=f"{_LABEL}/continuation/{run_key}/{path}",
                content=content,
                media_type="application/octet-stream",
            )
            total += len(content)
        body = {"manifest": manifest, "durable_refs": durable, "total_bytes": total}
        manifest_ref = await self._store.stage(
            request_scope=request_scope,
            name=f"{_LABEL}/continuation/{run_key}/workspace-snapshot.json",
            content=json.dumps(body, sort_keys=True).encode("utf-8"),
            media_type="application/json",
        )
        return WorkspaceSnapshot(
            snapshot_ref=f"{WORKSPACE_SNAPSHOT_PREFIX}{manifest_ref}",
            manifest=manifest,
            durable_refs=durable,
            total_bytes=total,
        )

    async def load(self, *, request_scope: str, snapshot_ref: str) -> WorkspaceSnapshot | None:
        del request_scope
        if not snapshot_ref.startswith(WORKSPACE_SNAPSHOT_PREFIX):
            return None
        try:
            raw = await self._store.retrieve(snapshot_ref.removeprefix(WORKSPACE_SNAPSHOT_PREFIX))
        except LookupError:
            return None
        body: dict[str, Any] = json.loads(raw)
        return WorkspaceSnapshot(
            snapshot_ref=snapshot_ref,
            manifest=body.get("manifest", {}),
            durable_refs=body.get("durable_refs", {}),
            total_bytes=int(body.get("total_bytes", 0)),
        )


class CodexSessionHydrator:
    """`SessionHydrator` for `codex`: a fresh thread on a fresh app-server in the source's
    lease; the source conversation is carried only by the sealed continuation packet."""

    def __init__(self, harness: CodexLocalHarness, store: SnapshotStore) -> None:
        self._harness = harness
        self._store = store
        self.hydrated: list[str] = []

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        heid = self._harness.live_session(request.source_session_ref)
        if heid is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID,
                f"the source thread {request.source_session_ref} has no live lease on this worker",
            )
        root = Path(self._harness.lease_path(heid))
        restored_paths: list[str] = []
        for path, durable_ref in sorted(request.snapshot.durable_refs.items()):
            try:
                content = await self._store.retrieve(durable_ref)
            except LookupError as error:
                raise ContinuationRejected(
                    CHECKPOINT_INVALID, f"snapshot bytes for {path} are missing"
                ) from error
            # A digest mismatch is reported by the continuity check from what is read back.
            await asyncio.to_thread(_write, root, path, content)
            restored_paths.append(path)
        for name, text in sorted(request.mission_files.items()):
            await asyncio.to_thread(_write, root, name, text.encode("utf-8"))
            restored_paths.append(name)
        handover = await self._harness.hydrate_session(
            heid,
            transfer_id=request.transfer_id,
            prompt_text=request.prompt_text,
            source_session_ref=request.source_session_ref,
        )
        target = handover.session.native_session_ref
        assert target is not None
        self.hydrated.append(target)
        restored = await asyncio.to_thread(_read_digests, root, restored_paths)
        return HydrationReceipt(
            target_session_ref=target,
            restored=restored,
            native_identity={
                "thread_id": target,
                "source_thread_id": request.source_session_ref,
                "checkpoint_id": request.checkpoint.checkpoint_id,
                "app_server_epoch": handover.session.native_details.get("app_server_epoch", ""),
                "conversation": "fresh",
            },
        )


def codex_continuation_registration(
    harness: CodexLocalHarness, store: SnapshotStore
) -> LaneContinuationRegistration:
    """The lane's MP-12 registration (unqualified until a recorded live drill)."""

    return LaneContinuationRegistration(
        lane_profile=PROFILE,
        hydrator=lambda _scope: CodexSessionHydrator(harness, store),
        snapshots=lambda _scope: CodexWorkspaceSnapshots(harness, store),
        qualified=False,
    )


__all__ = [
    "PATCH_PATH",
    "WORKSPACE_SNAPSHOT_PREFIX",
    "CodexSessionHydrator",
    "CodexWorkspaceSnapshots",
    "SnapshotStore",
    "codex_continuation_registration",
]
