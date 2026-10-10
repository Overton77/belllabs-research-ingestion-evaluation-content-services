"""`claude_agent_sdk` continuation: workspace snapshots and the fresh-session hydrator (MP-12).

SPEC-07 section 7 `request_continuation` is *emulated* on this lane, exactly as on Cursor:

1. `ClaudeWorkspaceSnapshots` is the `WorkspaceSnapshotPort` for a live session: it freezes
   the lease's `inputs/`, `outputs/` and `.mission/` content-addressed (the shared
   `adapters/cursor/snapshot.packet_tree` excludes `.mission/state/**`, so this lane's state
   root - transcript mirror, dispatch ledger, relocated config dir - never leaves the lease)
   and stores the manifest so `load` finds it on any worker.
2. `ContinuationService.seal` builds and validates the Continuation Checkpoint and its
   `purpose = continuation` packet from that snapshot.
3. `ClaudeSessionHydrator.hydrate` restores the snapshot files into the live lease, writes the
   continuation packet's `.mission/context.md` / `.mission/inputs.json`, reads back what it
   wrote (the receipt's digests), and asks the harness for a **fresh** `ClaudeSDKClient`
   session in the same lease (`ClaudeAgentSdkHarness.hydrate_session`: no `resume`, no
   `fork_session`). The checkpoint packet is the only carrier of the source conversation;
   no provider conversation fork is claimed. The next `lane.turn` sends the continuation turn
   to the target and records the supersession of the native session
   (`LaneTurnService._hand_over`), whose `session_init` frame confirms the transfer.

The target is a new native session (new connection, new `session_id`) under the same harness
execution and lease; the source's working-tree changes stay in the lease. The Continuation
Checkpoint's target generation is the continuation service's record, not a provider claim.
`claude_continuation_registration` is the lane's `LaneContinuationRegistration`
(`qualified=False`: only a recorded live drill flips it).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Final, Protocol

from mission_control.adapters.claude.describe import PROFILE
from mission_control.adapters.claude.harness import ClaudeAgentSdkHarness
from mission_control.adapters.cursor.projection import safe_relative
from mission_control.adapters.cursor.snapshot import CHECKPOINT_INVALID, bytes_digest, packet_tree
from mission_control.application.context.continuation import (
    ContinuationRejected,
    HydrationReceipt,
    HydrationRequest,
    WorkspaceSnapshot,
)
from mission_control.application.context.hydrators import LaneContinuationRegistration

WORKSPACE_SNAPSHOT_PREFIX: Final = "claude-workspace:"
_LABEL: Final = "claude-agent-sdk"


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
        target.chmod(0o644)
    target.write_bytes(content)


def _read_digests(root: Path, paths: Sequence[str]) -> dict[str, str]:
    observed: dict[str, str] = {}
    for path in paths:
        relative = safe_relative(path)
        target = root.joinpath(*relative.parts)
        if target.is_file():
            observed[f"/{relative.as_posix()}"] = bytes_digest(target.read_bytes())
    return observed


class ClaudeWorkspaceSnapshots:
    """`WorkspaceSnapshotPort` over the lease of a live `claude_agent_sdk` session."""

    def __init__(self, harness: ClaudeAgentSdkHarness, store: SnapshotStore) -> None:
        self._harness = harness
        self._store = store

    async def snapshot(
        self, *, request_scope: str, run_key: str, session_ref: str, roots: Sequence[str]
    ) -> WorkspaceSnapshot:
        heid = self._harness.live_execution(session_ref)
        if heid is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID, f"no live claude_agent_sdk lease holds session {session_ref}"
            )
        tree = await asyncio.to_thread(packet_tree, self._harness.lease_path(heid))
        manifest: dict[str, str] = {}
        durable: dict[str, str] = {}
        total = 0
        for path, content in tree:
            if not _selected(path, roots):
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


class ClaudeSessionHydrator:
    """`SessionHydrator` for `claude_agent_sdk`: a fresh SDK session in the source's lease."""

    def __init__(self, harness: ClaudeAgentSdkHarness, store: SnapshotStore) -> None:
        self._harness = harness
        self._store = store
        self.hydrated: list[str] = []

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        heid = self._harness.live_execution(request.source_session_ref)
        if heid is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID,
                f"the source session {request.source_session_ref} has no live lease on this worker",
            )
        root = self._harness.lease_path(heid)
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
                "session_id": target,
                "source_session_id": request.source_session_ref,
                "checkpoint_id": request.checkpoint.checkpoint_id,
                "conversation": "fresh",
            },
        )


def claude_continuation_registration(
    harness: ClaudeAgentSdkHarness, store: SnapshotStore
) -> LaneContinuationRegistration:
    """The lane's MP-12 registration (unqualified until a recorded live drill)."""

    return LaneContinuationRegistration(
        lane_profile=PROFILE,
        hydrator=lambda _scope: ClaudeSessionHydrator(harness, store),
        snapshots=lambda _scope: ClaudeWorkspaceSnapshots(harness, store),
        qualified=False,
    )


__all__ = [
    "WORKSPACE_SNAPSHOT_PREFIX",
    "ClaudeSessionHydrator",
    "ClaudeWorkspaceSnapshots",
    "SnapshotStore",
    "claude_continuation_registration",
]
