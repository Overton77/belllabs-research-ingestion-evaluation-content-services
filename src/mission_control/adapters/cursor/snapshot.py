"""Emulated `snapshot` and its restore for `cursor_local` (SPEC-07 sections 5.3 and 7; FT-G4).

Cursor has no native snapshot. The lane freezes a leased workspace as `mc.cursor_snapshot.v1`:

- the agent's **patch**: `git diff --binary` against the lease's base commit with every
  untracked file included (Mission Control's own projections and packet excluded), stored as
  an artifact and bound by its digest;
- the **untracked files** the patch adds (path and content digest, as observed on disk);
- the **packet files**: `.mission/` (without the hook token, the hook context, the kernel
  hook script and the bridge state), `inputs/` and `outputs/`, each stored and bound by
  digest (the B4 continuation roots);
- the **native refs** (agent id, run id) and the lease's base commit.

The manifest itself is stored and named by `cursor-snapshot:<manifest artifact ref>`, the
`snapshot_ref` a fork's or a continuation's packet `workspace` item carries. `restore`
re-applies it into a fresh lease (the patch with `git apply`, then the files), verifying every
digest; a missing or altered byte is `CHECKPOINT_INVALID` and nothing runs.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Final, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from mission_control.adapters.cursor.projection import (
    HOOK_CONTEXT_PATH,
    KERNEL_HOOK_PATH,
    STATE_ROOT,
    TOKEN_PATH,
    LaneProjectionError,
    safe_relative,
)
from mission_control.adapters.cursor.workspace import CapturedPatch, git

CURSOR_SNAPSHOT_SCHEMA: Final = "mc.cursor_snapshot.v1"
SNAPSHOT_REF_PREFIX: Final = "cursor-snapshot:"
CHECKPOINT_INVALID: Final = "CHECKPOINT_INVALID"
SNAPSHOT_ROOTS: Final = (".mission", "inputs", "outputs")
# Lease-private files: never frozen, never restored (secrets, per-lease wiring, bridge store).
_PRIVATE: Final = (TOKEN_PATH, HOOK_CONTEXT_PATH, KERNEL_HOOK_PATH)
# `.mission/hooks/` holds the kernel and catalog hook scripts: per-lease projections.
_PRIVATE_DIRS: Final = (STATE_ROOT, ".mission/bin", ".mission/hooks")


def bytes_digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SnapshotFile(_Strict):
    path: str = Field(min_length=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    bytes: int = Field(ge=0)
    ref: str | None = Field(default=None, min_length=1)


class SnapshotPatch(_Strict):
    ref: str = Field(min_length=1)
    digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    bytes: int = Field(ge=0)


class CursorSnapshotManifest(_Strict):
    """`mc.cursor_snapshot.v1`: what one emulated snapshot froze."""

    schema_version: Literal["mc.cursor_snapshot.v1"] = CURSOR_SNAPSHOT_SCHEMA
    lane_profile: Literal["cursor_local"] = "cursor_local"
    harness_execution_id: str = Field(min_length=1)
    generation: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=512)
    base_commit: str = Field(min_length=1)
    base_ref: str = Field(min_length=1)
    repository: str | None = None
    native: dict[str, str] = Field(default_factory=dict)
    patch: SnapshotPatch
    untracked: tuple[SnapshotFile, ...] = ()
    files: tuple[SnapshotFile, ...] = ()

    @property
    def mission_digests(self) -> dict[str, str]:
        return {item.path: item.digest for item in self.files if item.path.startswith(".mission/")}

    def workspace_manifest(self) -> dict[str, str]:
        """`/<path> -> digest` of the restorable packet files (B4 snapshot manifest shape)."""

        return {f"/{item.path}": item.digest for item in self.files}


class SnapshotArtifacts(Protocol):
    """Content-addressed artifact custody: stage bytes, retrieve them by the returned ref."""

    async def stage(
        self, *, request_scope: str, name: str, content: bytes, media_type: str
    ) -> str: ...

    async def retrieve(self, durable_ref: str) -> bytes: ...


@dataclass(frozen=True)
class FrozenSnapshot:
    snapshot_ref: str
    manifest: CursorSnapshotManifest
    manifest_ref: str


def snapshot_ref_of(manifest_ref: str) -> str:
    return f"{SNAPSHOT_REF_PREFIX}{manifest_ref}"


def manifest_ref_of(snapshot_ref: str) -> str | None:
    if not snapshot_ref.startswith(SNAPSHOT_REF_PREFIX):
        return None
    return snapshot_ref.removeprefix(SNAPSHOT_REF_PREFIX) or None


def _private(relative: str) -> bool:
    if relative in _PRIVATE:
        return True
    return any(relative == root or relative.startswith(f"{root}/") for root in _PRIVATE_DIRS)


def packet_tree(root: Path) -> list[tuple[str, bytes]]:
    """The snapshot roots' files (lease-private files excluded), sorted by path."""

    files: list[tuple[str, bytes]] = []
    for name in SNAPSHOT_ROOTS:
        base = root / name
        if not base.is_dir():
            continue
        for item in sorted(base.rglob("*")):
            if not item.is_file() or item.is_symlink():
                continue
            relative = item.relative_to(root).as_posix()
            if _private(relative):
                continue
            files.append((relative, item.read_bytes()))
    return files


def _untracked(root: Path, paths: Sequence[str]) -> tuple[SnapshotFile, ...]:
    entries: list[SnapshotFile] = []
    for path in paths:
        target = root.joinpath(*PurePosixPath(path).parts)
        if target.is_file():
            content = target.read_bytes()
            entries.append(
                SnapshotFile(path=path, digest=bytes_digest(content), bytes=len(content))
            )
    return tuple(entries)


async def freeze(
    *,
    root: Path,
    patch: CapturedPatch,
    artifacts: SnapshotArtifacts,
    request_scope: str,
    name: str,
    harness_execution_id: str,
    generation: int,
    reason: str,
    base_commit: str,
    base_ref: str,
    repository: str | None,
    native: Mapping[str, str],
) -> FrozenSnapshot:
    """Store the patch and the packet files, then the manifest that binds them."""

    patch_ref = await artifacts.stage(
        request_scope=request_scope,
        name=f"{name}/patch.diff",
        content=patch.diff,
        media_type="text/x-diff",
    )
    tree = await asyncio.to_thread(packet_tree, root)
    files: list[SnapshotFile] = []
    for path, content in tree:
        ref = await artifacts.stage(
            request_scope=request_scope,
            name=f"{name}/files/{path}",
            content=content,
            media_type="application/octet-stream",
        )
        files.append(
            SnapshotFile(path=path, digest=bytes_digest(content), bytes=len(content), ref=ref)
        )
    manifest = CursorSnapshotManifest(
        harness_execution_id=harness_execution_id,
        generation=generation,
        reason=reason[:512],
        base_commit=base_commit,
        base_ref=base_ref,
        repository=repository,
        native={key: value for key, value in native.items() if value},
        patch=SnapshotPatch(ref=patch_ref, digest=bytes_digest(patch.diff), bytes=len(patch.diff)),
        untracked=await asyncio.to_thread(_untracked, root, patch.untracked),
        files=tuple(files),
    )
    manifest_ref = await artifacts.stage(
        request_scope=request_scope,
        name=f"{name}/snapshot-manifest.json",
        content=manifest.model_dump_json().encode("utf-8"),
        media_type="application/json",
    )
    return FrozenSnapshot(snapshot_ref_of(manifest_ref), manifest, manifest_ref)


async def load(snapshot_ref: str, artifacts: SnapshotArtifacts) -> CursorSnapshotManifest:
    manifest_ref = manifest_ref_of(snapshot_ref)
    if manifest_ref is None:
        raise LaneProjectionError(
            CHECKPOINT_INVALID, f"{snapshot_ref} is not a cursor_local workspace snapshot"
        )
    try:
        raw = await artifacts.retrieve(manifest_ref)
    except LookupError as error:
        raise LaneProjectionError(CHECKPOINT_INVALID, "snapshot manifest is missing") from error
    return CursorSnapshotManifest.model_validate_json(raw)


async def _verified(artifacts: SnapshotArtifacts, ref: str, digest: str, what: str) -> bytes:
    try:
        content = await artifacts.retrieve(ref)
    except LookupError as error:
        raise LaneProjectionError(CHECKPOINT_INVALID, f"{what} bytes are missing") from error
    if bytes_digest(content) != digest:
        raise LaneProjectionError(CHECKPOINT_INVALID, f"{what} digest differs from the snapshot")
    return content


def _apply_patch(root: Path, diff: bytes) -> None:
    if not diff:
        return
    patch_file = root / STATE_ROOT / "restore.patch"
    patch_file.parent.mkdir(parents=True, exist_ok=True)
    patch_file.write_bytes(diff)
    try:
        git("apply", "--binary", "--whitespace=nowarn", str(patch_file), cwd=root)
    finally:
        patch_file.unlink(missing_ok=True)


def _write(root: Path, path: str, content: bytes) -> None:
    relative = safe_relative(path)
    target = root.joinpath(*relative.parts)
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        target.chmod(0o644)
    target.write_bytes(content)


async def restore(
    manifest: CursorSnapshotManifest,
    root: Path,
    artifacts: SnapshotArtifacts,
    *,
    restore_paths: Sequence[str] = ("/",),
    include_patch: bool = True,
) -> dict[str, str]:
    """Re-apply a frozen snapshot into `root`; return `/<path> -> digest` read back from disk.

    `restore_paths` narrows the packet files (B4 continuation restores `/inputs/**`,
    `/outputs/**` and `/.mission/**`); the patch is applied when `include_patch` holds.
    """

    prefixes = tuple(path.rstrip("*").rstrip("/") for path in restore_paths)

    def selected(path: str) -> bool:
        absolute = f"/{path}"
        return any(
            prefix in {"", "/"} or absolute == prefix or absolute.startswith(prefix + "/")
            for prefix in prefixes
        )

    if include_patch:
        diff = await _verified(artifacts, manifest.patch.ref, manifest.patch.digest, "patch")
        await asyncio.to_thread(_apply_patch, root, diff)
    for item in manifest.files:
        if not selected(item.path) or item.ref is None:
            continue
        content = await _verified(artifacts, item.ref, item.digest, item.path)
        await asyncio.to_thread(_write, root, item.path, content)
    return await asyncio.to_thread(observed_digests, root, manifest)


def observed_digests(root: Path, manifest: CursorSnapshotManifest) -> dict[str, str]:
    """What is on disk now for every path the snapshot names (`/<path> -> digest`)."""

    observed: dict[str, str] = {}
    for item in (*manifest.files, *manifest.untracked):
        target = root.joinpath(*safe_relative(item.path).parts)
        if target.is_file():
            observed[f"/{item.path}"] = bytes_digest(target.read_bytes())
    return observed


def manifest_refs(snapshot: FrozenSnapshot) -> tuple[str, ...]:
    """The `SnapshotManifest.refs` of a frozen snapshot: the snapshot ref, the patch ref and
    digest, every untracked file, every `.mission/` digest and the native refs."""

    manifest = snapshot.manifest
    refs: list[str] = [
        snapshot.snapshot_ref,
        f"patch:{manifest.patch.ref}",
        f"patch_digest:{manifest.patch.digest}",
        f"base_commit:{manifest.base_commit}",
    ]
    refs.extend(f"untracked:{item.path}={item.digest}" for item in manifest.untracked)
    refs.extend(
        f"mission:{path}={digest}" for path, digest in sorted(manifest.mission_digests.items())
    )
    refs.extend(f"native:{key}={value}" for key, value in sorted(manifest.native.items()))
    return tuple(refs)


__all__ = [
    "CHECKPOINT_INVALID",
    "CURSOR_SNAPSHOT_SCHEMA",
    "SNAPSHOT_REF_PREFIX",
    "CursorSnapshotManifest",
    "FrozenSnapshot",
    "SnapshotArtifacts",
    "SnapshotFile",
    "SnapshotPatch",
    "freeze",
    "load",
    "manifest_ref_of",
    "manifest_refs",
    "observed_digests",
    "packet_tree",
    "restore",
    "snapshot_ref_of",
]
