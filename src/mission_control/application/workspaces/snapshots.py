"""Portable workspace snapshots behind `mc.workspace_snapshot.v1` (SPEC-02 "Worktrees and
snapshots"; MP-04).

A plain `git diff` cannot carry the index/worktree split, untracked files or binary content, so
a local snapshot is a set of content-addressed artifacts bound by one manifest
(`mc.workspace_artifact_manifest.v1`):

- `commits`: a git bundle of `base..W` where `I` is a commit of the index tree on top of HEAD
  (staged state, staged deletions) and `W` a commit of the tracked worktree on top of `I`
  (unstaged edits, unstaged deletions, file modes). The agent's own commits ride along;
- `patch`: the reviewable `git diff --binary --full-index base..W+untracked` (the frozen
  contract's `patch_artifact_ref`);
- `untracked`: a deterministic tar of the untracked *included* files and in-root symlinks
  (`untracked_artifact_ref`), with every member's path, kind, mode, size and digest listed;
- the exclusion rules that applied, tracked deletions, file modes and submodule commits.

`WorkspaceSnapshot.manifest_digest` is the digest of the stored manifest bytes; restore checks
that digest, every artifact digest, the git object ids and finally what is on disk. A provider
workspace snapshot (Cursor cloud) records the actual branch head commit it read back.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Final, Literal, Protocol
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.application.workspaces.errors import CHECKPOINT_INVALID, WorkspaceError
from mission_control.domain.execution.bindings import WorkspaceSnapshot
from mission_control.domain.execution.lanes import LaneProfileName

ARTIFACT_MANIFEST_SCHEMA: Final = "mc.workspace_artifact_manifest.v1"
WORKSPACE_SNAPSHOT_REF_PREFIX: Final = "workspace-snapshot:"
BRANCH_SNAPSHOT_REF_PREFIX: Final = "branch:"
DIGEST: Final = r"^sha256:[0-9a-f]{64}$"
OBJECT_ID: Final = r"^[0-9a-f]{40,64}$"
GITIGNORED_RULE: Final = "gitignore:--exclude-standard"
LFS_RULE: Final = "lfs:pointer-content-only"


def bytes_digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StoredArtifact(_Strict):
    ref: str = Field(min_length=1, max_length=2_048)
    digest: str = Field(pattern=DIGEST)
    bytes: int = Field(ge=0)


class UntrackedEntry(_Strict):
    path: str = Field(min_length=1, max_length=4_096)
    kind: Literal["file", "symlink"]
    mode: Literal["100644", "100755", "120000"]
    digest: str = Field(pattern=DIGEST)
    bytes: int = Field(ge=0)
    link_target: str | None = Field(default=None, min_length=1, max_length=4_096)

    @model_validator(mode="after")
    def shape(self) -> UntrackedEntry:
        if (self.kind == "symlink") != (self.link_target is not None):
            raise ValueError("a symlink entry (and only a symlink) carries its link_target")
        if (self.kind == "symlink") != (self.mode == "120000"):
            raise ValueError("mode 120000 is the symlink mode")
        return self


class WorkspaceArtifactManifest(_Strict):
    """`mc.workspace_artifact_manifest.v1`: the artifacts one local snapshot froze."""

    schema_version: Literal["mc.workspace_artifact_manifest.v1"] = ARTIFACT_MANIFEST_SCHEMA
    lane_profile: LaneProfileName
    producer_lease_id: UUID
    producer_fence: int = Field(ge=0)
    producer_generation: int = Field(ge=1)
    base_commit: str = Field(pattern=OBJECT_ID)
    head_commit: str = Field(pattern=OBJECT_ID)
    branch: str | None = Field(default=None, min_length=1, max_length=256)
    index_tree: str = Field(pattern=OBJECT_ID)
    index_commit: str = Field(pattern=OBJECT_ID)
    worktree_tree: str = Field(pattern=OBJECT_ID)
    worktree_commit: str = Field(pattern=OBJECT_ID)
    commits: StoredArtifact
    patch: StoredArtifact
    untracked_archive: StoredArtifact | None = None
    untracked: tuple[UntrackedEntry, ...] = ()
    tracked_deletions: tuple[str, ...] = ()
    file_modes: dict[str, str] = Field(default_factory=dict)
    submodule_commits: dict[str, str] = Field(default_factory=dict)
    exclusions: tuple[str, ...] = ()
    captured_at: AwareDatetime

    @model_validator(mode="after")
    def untracked_bound(self) -> WorkspaceArtifactManifest:
        if bool(self.untracked) != (self.untracked_archive is not None):
            raise ValueError("untracked entries and their archive come together")
        paths = [entry.path for entry in self.untracked]
        if len(set(paths)) != len(paths) or paths != sorted(paths):
            raise ValueError("untracked entries are unique and sorted by path")
        return self

    def encoded(self) -> bytes:
        return self.model_dump_json().encode("utf-8")


class SnapshotArtifacts(Protocol):
    """Content-addressed artifact custody: stage bytes, retrieve them by the returned ref."""

    async def stage(
        self, *, request_scope: str, name: str, content: bytes, media_type: str
    ) -> str: ...

    async def retrieve(self, durable_ref: str) -> bytes: ...


class CapturedSnapshot(_Strict):
    """A frozen snapshot: the contract record, its manifest and where the manifest is stored."""

    snapshot: WorkspaceSnapshot
    manifest: WorkspaceArtifactManifest | None = None
    manifest_ref: str | None = None
    snapshot_ref: str = Field(min_length=1)


class RestoreReceipt(_Strict):
    """What restore verified on disk after re-applying a snapshot."""

    snapshot_ref: str
    head_commit: str
    index_tree: str
    worktree_tree: str
    untracked: dict[str, str]


def workspace_snapshot_ref(manifest_ref: str) -> str:
    return f"{WORKSPACE_SNAPSHOT_REF_PREFIX}{manifest_ref}"


def manifest_ref_of(snapshot_ref: str) -> str | None:
    if not snapshot_ref.startswith(WORKSPACE_SNAPSHOT_REF_PREFIX):
        return None
    return snapshot_ref.removeprefix(WORKSPACE_SNAPSHOT_REF_PREFIX) or None


def branch_snapshot_ref(branch: str, head_commit: str) -> str:
    """The `branch:<branch>@<sha>` ref a provider-workspace fork restores from."""

    return f"{BRANCH_SNAPSHOT_REF_PREFIX}{branch}@{head_commit}"


def contract_snapshot(
    manifest: WorkspaceArtifactManifest, manifest_digest: str
) -> WorkspaceSnapshot:
    """The frozen `mc.workspace_snapshot.v1` record of a stored manifest."""

    return WorkspaceSnapshot(
        lane_profile=manifest.lane_profile,
        base_commit=manifest.base_commit,
        branch=manifest.branch,
        head_commit=manifest.head_commit,
        patch_artifact_ref=manifest.patch.ref,
        untracked_artifact_ref=(
            manifest.untracked_archive.ref if manifest.untracked_archive is not None else None
        ),
        tracked_deletions=manifest.tracked_deletions,
        file_modes=manifest.file_modes,
        submodule_commits=manifest.submodule_commits,
        exclusions=manifest.exclusions,
        manifest_digest=manifest_digest,
        producer_lease_id=str(manifest.producer_lease_id),
        producer_generation=manifest.producer_generation,
        captured_at=manifest.captured_at,
        emulated=True,
    )


def branch_snapshot(
    *,
    lane_profile: LaneProfileName,
    branch: str,
    head_commit: str,
    base_commit: str,
    producer_lease_id: UUID,
    producer_generation: int,
    captured_at: datetime,
) -> CapturedSnapshot:
    """A provider workspace snapshot: the branch head commit actually read back from the
    provider's repository. Uncommitted provider-VM state is not part of it (recorded as an
    exclusion), and Mission Control produced it, so it stays `emulated`."""

    exclusions = ("provider:uncommitted-state-not-captured",)
    digest_input = "\n".join(
        (lane_profile, branch, head_commit, base_commit, str(producer_lease_id), *exclusions)
    )
    snapshot = WorkspaceSnapshot(
        lane_profile=lane_profile,
        base_commit=base_commit,
        branch=branch,
        head_commit=head_commit,
        exclusions=exclusions,
        manifest_digest=bytes_digest(digest_input.encode("utf-8")),
        producer_lease_id=str(producer_lease_id),
        producer_generation=producer_generation,
        captured_at=captured_at,
        emulated=True,
    )
    return CapturedSnapshot(
        snapshot=snapshot, snapshot_ref=branch_snapshot_ref(branch, head_commit)
    )


async def load_manifest(
    snapshot: WorkspaceSnapshot, manifest_ref: str, artifacts: SnapshotArtifacts
) -> WorkspaceArtifactManifest:
    """Retrieve a manifest and prove it is the one the contract record names."""

    try:
        raw = await artifacts.retrieve(manifest_ref)
    except LookupError as error:
        raise WorkspaceError(CHECKPOINT_INVALID, "the snapshot manifest is missing") from error
    if bytes_digest(raw) != snapshot.manifest_digest:
        raise WorkspaceError(CHECKPOINT_INVALID, "the snapshot manifest digest differs")
    manifest = WorkspaceArtifactManifest.model_validate_json(raw)
    expected = contract_snapshot(manifest, snapshot.manifest_digest)
    if expected != snapshot:
        raise WorkspaceError(CHECKPOINT_INVALID, "the snapshot record and its manifest disagree")
    return manifest


async def verified_bytes(
    artifacts: SnapshotArtifacts, artifact: StoredArtifact, what: str
) -> bytes:
    try:
        content = await artifacts.retrieve(artifact.ref)
    except LookupError as error:
        raise WorkspaceError(CHECKPOINT_INVALID, f"the {what} artifact is missing") from error
    if len(content) != artifact.bytes or bytes_digest(content) != artifact.digest:
        raise WorkspaceError(CHECKPOINT_INVALID, f"the {what} artifact digest differs")
    return content


WORKSPACE_CONTRACTS: Final[dict[str, type[BaseModel]]] = {
    "workspace_artifact_manifest": WorkspaceArtifactManifest,
}


def workspace_contract_schemas() -> dict[str, dict[str, Any]]:
    """Registered application contracts of this module (same shape as the frozen domain
    `binding_contract_schemas`): `mc.workspace_artifact_manifest.v1`, whose stored bytes are
    what `WorkspaceSnapshot.manifest_digest` seals."""

    return {name: model.model_json_schema() for name, model in WORKSPACE_CONTRACTS.items()}


__all__ = [
    "ARTIFACT_MANIFEST_SCHEMA",
    "BRANCH_SNAPSHOT_REF_PREFIX",
    "GITIGNORED_RULE",
    "LFS_RULE",
    "WORKSPACE_CONTRACTS",
    "WORKSPACE_SNAPSHOT_REF_PREFIX",
    "CapturedSnapshot",
    "RestoreReceipt",
    "SnapshotArtifacts",
    "StoredArtifact",
    "UntrackedEntry",
    "WorkspaceArtifactManifest",
    "branch_snapshot",
    "branch_snapshot_ref",
    "bytes_digest",
    "contract_snapshot",
    "load_manifest",
    "manifest_ref_of",
    "verified_bytes",
    "workspace_contract_schemas",
    "workspace_snapshot_ref",
]
