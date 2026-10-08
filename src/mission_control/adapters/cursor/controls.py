"""Cursor continuation: workspace snapshots and the fresh-agent hydrators (FT-G4, FT-G6).

SPEC-07 section 7 `request_continuation` is emulated on Cursor (`Agent.resume` continues the
same Agent Session within one attempt only; a continuation always creates a new agent):

1. `CursorWorkspaceSnapshots` is the B4 `WorkspaceSnapshotPort` for a live `cursor_local`
   session: it freezes the lease's `inputs/`, `outputs/` and `.mission/` (lease-private files
   excluded) content-addressed, and stores the manifest so `load` finds it on any worker.
2. `ContinuationService.seal` (B4) builds and validates the Continuation Checkpoint and its
   `purpose = continuation` packet from that snapshot.
3. `CursorSessionHydrator.hydrate` (B4 `SessionHydrator`) restores the snapshot files into the
   live lease (digest-verified), writes the continuation packet's `.mission/context.md` and
   `.mission/inputs.json`, creates a **new agent** in the same lease with the same pinned
   options, stages the hydration prompt as the next turn's text and offers the handover:
   the next `lane.turn` sends that turn to the new agent, records the supersession of the
   native session, and writes the new session's `session_init` frame (which confirms the
   transfer and releases held commands). `session.transferred` is written by B4.

The source agent's patch stays in the lease (the same worktree), so code changes carry over.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Final, Protocol

from mission_control.adapters.cursor.cloud import CursorCloudHarness
from mission_control.adapters.cursor.local import CursorLocalHarness
from mission_control.adapters.cursor.projection import safe_relative
from mission_control.adapters.cursor.snapshot import (
    CHECKPOINT_INVALID,
    SNAPSHOT_ROOTS,
    SnapshotArtifacts,
    bytes_digest,
    packet_tree,
)
from mission_control.application.context.continuation import (
    ContinuationRejected,
    HydrationReceipt,
    HydrationRequest,
    WorkspaceSnapshot,
)
from mission_control.application.execution.harness.controls import SessionHandover

WORKSPACE_SNAPSHOT_PREFIX: Final = "cursor-workspace:"
CONTINUATION_TURN_PREFIX: Final = "continuation:"


class BytesSource(Protocol):
    async def retrieve(self, durable_ref: str) -> bytes: ...


def _write(root: Path, path: str, content: bytes) -> None:
    relative = safe_relative(path)
    target = root.joinpath(*relative.parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.chmod(0o644)
    target.write_bytes(content)


def _read_digests(root: Path, paths: Sequence[str]) -> dict[str, str]:
    observed: dict[str, str] = {}
    for path in paths:
        target = root.joinpath(*safe_relative(path).parts)
        if target.is_file():
            observed[f"/{safe_relative(path).as_posix()}"] = bytes_digest(target.read_bytes())
    return observed


def _selected(path: str, roots: Sequence[str]) -> bool:
    prefixes = tuple(item.strip("/").rstrip("*").rstrip("/") for item in roots)
    return not prefixes or any(
        path == prefix or path.startswith(prefix + "/") for prefix in prefixes
    )


class _StagedSnapshots:
    """Stage a frozen tree content-addressed and store its manifest (shared by both lanes)."""

    def __init__(self, store: SnapshotArtifacts, *, label: str) -> None:
        self._store = store
        self._label = label

    async def _freeze(
        self,
        tree: Sequence[tuple[str, bytes]],
        *,
        request_scope: str,
        run_key: str,
        roots: Sequence[str],
    ) -> WorkspaceSnapshot:
        manifest: dict[str, str] = {}
        durable: dict[str, str] = {}
        total = 0
        for path, content in tree:
            if not _selected(path, roots):
                continue
            manifest[f"/{path}"] = bytes_digest(content)
            durable[f"/{path}"] = await self._store.stage(
                request_scope=request_scope,
                name=f"{self._label}/continuation/{run_key}/{path}",
                content=content,
                media_type="application/octet-stream",
            )
            total += len(content)
        body = {"manifest": manifest, "durable_refs": durable, "total_bytes": total}
        manifest_ref = await self._store.stage(
            request_scope=request_scope,
            name=f"{self._label}/continuation/{run_key}/workspace-snapshot.json",
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


class CursorWorkspaceSnapshots(_StagedSnapshots):
    """B4 `WorkspaceSnapshotPort` over a live `cursor_local` lease."""

    def __init__(self, harness: CursorLocalHarness, store: SnapshotArtifacts) -> None:
        super().__init__(store, label="cursor-local")
        self._harness = harness

    async def snapshot(
        self, *, request_scope: str, run_key: str, session_ref: str, roots: Sequence[str]
    ) -> WorkspaceSnapshot:
        found = self._harness.live_session(session_ref)
        if found is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID, f"no live cursor_local lease holds session {session_ref}"
            )
        root = Path(self._harness.lease_path(found))
        tree = await asyncio.to_thread(packet_tree, root)
        return await self._freeze(tree, request_scope=request_scope, run_key=run_key, roots=roots)


class CursorCloudWorkspaceSnapshots(_StagedSnapshots):
    """B4 `WorkspaceSnapshotPort` over a live `cursor_cloud` session: the run branch's
    `inputs/`, `outputs/` and `.mission/` at its current head."""

    def __init__(self, harness: CursorCloudHarness, store: SnapshotArtifacts) -> None:
        super().__init__(store, label="cursor-cloud")
        self._harness = harness

    async def snapshot(
        self, *, request_scope: str, run_key: str, session_ref: str, roots: Sequence[str]
    ) -> WorkspaceSnapshot:
        found = self._harness.live_session(session_ref)
        if found is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID, f"no live cursor_cloud session for agent {session_ref}"
            )
        tree = await self._harness.read_branch(found, SNAPSHOT_ROOTS)
        return await self._freeze(tree, request_scope=request_scope, run_key=run_key, roots=roots)


class CursorCloudSessionHydrator:
    """B4 `SessionHydrator` for `cursor_cloud`: the continuation's files committed to the run
    branch and a fresh agent created on it (its first run is the continuation turn)."""

    def __init__(self, harness: CursorCloudHarness, bytes_source: BytesSource) -> None:
        self._harness = harness
        self._bytes = bytes_source
        self.hydrated: list[str] = []

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        found = self._harness.live_session(request.source_session_ref)
        if found is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID,
                f"the source agent {request.source_session_ref} has no live cloud session",
            )
        files: list[tuple[str, bytes]] = []
        for path, durable_ref in sorted(request.snapshot.durable_refs.items()):
            try:
                files.append((path.lstrip("/"), await self._bytes.retrieve(durable_ref)))
            except LookupError as error:
                raise ContinuationRejected(
                    CHECKPOINT_INVALID, f"snapshot bytes for {path} are missing"
                ) from error
        files.extend(
            (name.lstrip("/"), text.encode("utf-8"))
            for name, text in sorted(request.mission_files.items())
        )
        agent_id, restored = await self._harness.hydrate_agent(
            found,
            transfer_id=request.transfer_id,
            files=files,
            prompt=request.prompt_text,
            source_session_ref=request.source_session_ref,
        )
        self.hydrated.append(agent_id)
        return HydrationReceipt(
            target_session_ref=agent_id,
            restored=restored,
            native_identity={
                "agent_id": agent_id,
                "source_agent_id": request.source_session_ref,
                "checkpoint_id": request.checkpoint.checkpoint_id,
            },
        )


class CursorSessionHydrator:
    """B4 `SessionHydrator` for `cursor_local`: a new agent in the source session's lease."""

    def __init__(self, harness: CursorLocalHarness, bytes_source: BytesSource | None = None):
        self._harness = harness
        source = bytes_source or harness.snapshot_store
        if source is None:
            raise ValueError("the Cursor hydrator needs a snapshot byte source")
        self._bytes: BytesSource = source
        self.hydrated: list[str] = []

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        found = self._harness.live_session(request.source_session_ref)
        if found is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID,
                f"the source session {request.source_session_ref} has no live lease on this worker",
            )
        root = Path(self._harness.lease_path(found))
        restored_paths: list[str] = []
        for path, durable_ref in sorted(request.snapshot.durable_refs.items()):
            try:
                content = await self._bytes.retrieve(durable_ref)
            except LookupError as error:
                raise ContinuationRejected(
                    CHECKPOINT_INVALID, f"snapshot bytes for {path} are missing"
                ) from error
            # A digest mismatch is reported by B4's continuity check from what is read back.
            await asyncio.to_thread(_write, root, path, content)
            restored_paths.append(path)
        for name, text in sorted(request.mission_files.items()):
            await asyncio.to_thread(_write, root, name, text.encode("utf-8"))
            restored_paths.append(name)
        bridge = await self._harness.open_bridge(found)
        agent_id = await bridge.create_agent(self._harness.agent_spec(found))
        instruction_ref = f"{CONTINUATION_TURN_PREFIX}{request.transfer_id}"
        self._harness.stage_turn(found, instruction_ref, request.prompt_text)
        handle = self._harness.session_handle(found, agent_id)
        self._harness.offer_handover(
            found,
            SessionHandover(
                transfer_id=request.transfer_id,
                session=handle,
                instruction_ref=instruction_ref,
                source_session_ref=request.source_session_ref,
            ),
        )
        self.hydrated.append(agent_id)
        restored = await asyncio.to_thread(_read_digests, root, restored_paths)
        return HydrationReceipt(
            target_session_ref=agent_id,
            restored=restored,
            native_identity={
                "agent_id": agent_id,
                "source_agent_id": request.source_session_ref,
                "checkpoint_id": request.checkpoint.checkpoint_id,
            },
        )


__all__ = [
    "CONTINUATION_TURN_PREFIX",
    "WORKSPACE_SNAPSHOT_PREFIX",
    "CursorCloudSessionHydrator",
    "CursorCloudWorkspaceSnapshots",
    "CursorSessionHydrator",
    "CursorWorkspaceSnapshots",
]
