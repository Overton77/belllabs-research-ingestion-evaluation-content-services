"""MP-04 portable snapshots on real git repositories: a fork restores staged, unstaged,
deleted and untracked included content exactly, and every digest is checked on restore."""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from mission_control.application.workspaces.errors import CHECKPOINT_INVALID, WorkspaceError
from mission_control.application.workspaces.snapshots import (
    GITIGNORED_RULE,
    UntrackedEntry,
    WorkspaceArtifactManifest,
    bytes_digest,
    contract_snapshot,
    workspace_snapshot_ref,
)
from tests.unit.workspaces.workspace_fixtures import (
    Stack,
    commit,
    git,
    link,
    porcelain,
    request,
    stack,
)


def _dirty_the_workspace(root: Path) -> None:
    """Every kind of change a snapshot must carry."""

    (root / "a.txt").write_bytes(b"a1 committed by the agent\n")
    git("add", "a.txt", cwd=root)
    commit(root, "agent work")
    (root / "b.txt").write_bytes(b"b1 staged\n")
    git("add", "b.txt", cwd=root)
    (root / "c.txt").write_bytes(b"c1 staged\n")
    git("add", "c.txt", cwd=root)
    (root / "c.txt").write_bytes(b"c2 unstaged over staged\n")
    (root / "d.txt").write_bytes(b"d1 unstaged\n")
    git("rm", "--quiet", "e.txt", cwd=root)
    (root / "f.txt").unlink()
    git("rm", "--quiet", "--cached", "g.txt", cwd=root)
    (root / "new_staged.txt").write_bytes(b"new and staged\n")
    git("add", "new_staged.txt", cwd=root)
    (root / "blob.bin").write_bytes(bytes(reversed(range(256))) * 3)
    git("add", "blob.bin", cwd=root)
    (root / "notes").mkdir()
    (root / "notes" / "untracked.txt").write_bytes(b"untracked text\r\nwith crlf\r\n")
    (root / "untracked.bin").write_bytes(b"\x00\xff\x00binary\x01" * 64)
    (root / "debug.log").write_bytes(b"ignored, never captured\n")


def _state(root: Path) -> dict[str, object]:
    files = {
        path: (root / path).read_bytes()
        for path in (
            "a.txt",
            "b.txt",
            "c.txt",
            "d.txt",
            "g.txt",
            "new_staged.txt",
            "blob.bin",
            "notes/untracked.txt",
            "untracked.bin",
        )
    }
    return {
        "head": git("rev-parse", "HEAD", cwd=root).strip(),
        "status": porcelain(root),
        "staged": git("diff", "--cached", "--binary", "--full-index", cwd=root),
        "unstaged": git("diff", "--binary", "--full-index", cwd=root),
        "index": git("ls-files", "--stage", cwd=root),
        "files": files,
        "missing": sorted(p for p in ("e.txt", "f.txt") if (root / p).exists()),
    }


async def _fork(s: Stack, base_commit: str, run_id: str = "run-fork"):
    return await s.allocator.allocate(request(s.primary, run_id=run_id, base_ref=base_commit))


async def test_fork_restore_preserves_staged_unstaged_deleted_and_untracked(
    tmp_path: Path,
) -> None:
    s = stack(tmp_path)
    source = await s.allocator.allocate(request(s.primary))
    root = Path(source.path)
    _dirty_the_workspace(root)
    before = _state(root)
    captured = await s.allocator.snapshot(source, artifacts=s.artifacts)
    snapshot = captured.snapshot
    assert captured.manifest is not None
    # The capture never touched the leased worktree's index or files.
    assert _state(root) == before
    assert snapshot.schema_version == "mc.workspace_snapshot.v1"
    assert snapshot.base_commit == source.base_commit
    assert snapshot.branch == source.branch and snapshot.branch is not None
    assert snapshot.head_commit == before["head"] != source.base_commit
    assert set(snapshot.tracked_deletions) == {"e.txt", "f.txt", "g.txt"}
    assert snapshot.patch_artifact_ref and snapshot.untracked_artifact_ref
    assert snapshot.producer_lease_id == str(source.lease_id)
    assert GITIGNORED_RULE in snapshot.exclusions
    untracked = {entry.path for entry in captured.manifest.untracked}
    assert untracked == {"g.txt", "notes/untracked.txt", "untracked.bin"}
    assert "debug.log" not in untracked
    recorded = await s.ledger.get(source.request_scope, source.lease_id)
    assert recorded is not None and recorded.workspace_snapshot == snapshot
    assert recorded.snapshot_ref == captured.snapshot_ref

    fork = await _fork(s, snapshot.base_commit)
    assert fork.path != source.path and fork.branch != source.branch
    receipt = await s.allocator.restore(
        snapshot, captured.snapshot_ref, fork, artifacts=s.artifacts
    )
    assert receipt.head_commit == before["head"]
    after = _state(Path(fork.path))
    assert after == before
    assert not (Path(fork.path) / "debug.log").exists()
    # The reviewable patch carries the tracked and untracked change set from the base.
    patch = s.artifacts.staged[snapshot.patch_artifact_ref]
    assert b"untracked.bin" in patch and b"GIT binary patch" in patch
    assert b"deleted file mode" in patch


async def test_restore_checks_every_digest_before_applying_anything(tmp_path: Path) -> None:
    s = stack(tmp_path)
    source = await s.allocator.allocate(request(s.primary))
    _dirty_the_workspace(Path(source.path))
    captured = await s.allocator.snapshot(source, artifacts=s.artifacts)
    manifest = captured.manifest
    assert manifest is not None and manifest.untracked_archive is not None

    # A flipped byte in the untracked archive: refused, the fork stays at its clean base.
    archive_ref = manifest.untracked_archive.ref
    original = s.artifacts.staged[archive_ref]
    s.artifacts.staged[archive_ref] = original[:-1] + bytes([original[-1] ^ 1])
    fork = await _fork(s, captured.snapshot.base_commit, "run-fork-1")
    with pytest.raises(WorkspaceError) as tampered:
        await s.allocator.restore(
            captured.snapshot, captured.snapshot_ref, fork, artifacts=s.artifacts
        )
    assert tampered.value.code == CHECKPOINT_INVALID
    assert porcelain(Path(fork.path)) == []
    s.artifacts.staged[archive_ref] = original

    # A rewritten manifest no longer matches the contract record's manifest digest.
    assert captured.manifest_ref is not None
    s.artifacts.staged[captured.manifest_ref] = manifest.model_copy(
        update={"head_commit": manifest.base_commit}
    ).encoded()
    with pytest.raises(WorkspaceError) as rewritten:
        await s.allocator.restore(
            captured.snapshot, captured.snapshot_ref, fork, artifacts=s.artifacts
        )
    assert rewritten.value.code == CHECKPOINT_INVALID

    # A missing bundle is refused too.
    s.artifacts.staged[captured.manifest_ref] = manifest.encoded()
    del s.artifacts.staged[manifest.commits.ref]
    with pytest.raises(WorkspaceError) as missing:
        await s.allocator.restore(
            captured.snapshot, captured.snapshot_ref, fork, artifacts=s.artifacts
        )
    assert missing.value.code == CHECKPOINT_INVALID
    assert porcelain(Path(fork.path)) == []


async def test_restore_needs_a_clean_target_at_the_snapshot_base(tmp_path: Path) -> None:
    s = stack(tmp_path)
    source = await s.allocator.allocate(request(s.primary))
    _dirty_the_workspace(Path(source.path))
    captured = await s.allocator.snapshot(source, artifacts=s.artifacts)
    (s.primary / "z.txt").write_bytes(b"z\n")
    git("add", "z.txt", cwd=s.primary)
    commit(s.primary, "moved on")
    elsewhere = await s.allocator.allocate(request(s.primary, run_id="run-moved"))
    with pytest.raises(WorkspaceError) as moved:
        await s.allocator.restore(
            captured.snapshot, captured.snapshot_ref, elsewhere, artifacts=s.artifacts
        )
    assert moved.value.code == CHECKPOINT_INVALID
    dirty = await _fork(s, captured.snapshot.base_commit)
    (Path(dirty.path) / "stray.txt").write_bytes(b"stray\n")
    with pytest.raises(WorkspaceError) as unclean:
        await s.allocator.restore(
            captured.snapshot, captured.snapshot_ref, dirty, artifacts=s.artifacts
        )
    assert unclean.value.code == CHECKPOINT_INVALID


async def _forged(
    s: Stack,
    manifest: WorkspaceArtifactManifest,
    members: list[tuple[tarfile.TarInfo, bytes | None]],
    entries: tuple[UntrackedEntry, ...],
) -> tuple[object, str]:
    """A self-consistent manifest whose archive was crafted by someone with artifact write
    access: every digest matches, so only the path checks can refuse it."""

    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for info, content in members:
            archive.addfile(info, io.BytesIO(content) if content is not None else None)
    blob = buffer.getvalue()
    ref = await s.artifacts.stage(request_scope="x", name="forged.tar", content=blob, media_type="")
    forged = manifest.model_copy(
        update={
            "untracked_archive": manifest.commits.model_copy(
                update={"ref": ref, "digest": bytes_digest(blob), "bytes": len(blob)}
            ),
            "untracked": entries,
        }
    )
    encoded = forged.encoded()
    manifest_ref = await s.artifacts.stage(
        request_scope="x", name="forged.json", content=encoded, media_type=""
    )
    return contract_snapshot(forged, bytes_digest(encoded)), workspace_snapshot_ref(manifest_ref)


async def test_restore_refuses_archive_traversal_and_escaping_links(tmp_path: Path) -> None:
    s = stack(tmp_path)
    source = await s.allocator.allocate(request(s.primary))
    (Path(source.path) / "u.txt").write_bytes(b"u\n")
    captured = await s.allocator.snapshot(source, artifacts=s.artifacts)
    assert captured.manifest is not None
    content = b"escaped\n"
    traversal = tarfile.TarInfo("../outside.txt")
    traversal.size = len(content)
    entry = UntrackedEntry(
        path="../outside.txt", kind="file", mode="100644", digest=bytes_digest(content), bytes=8
    )
    snapshot, ref = await _forged(s, captured.manifest, [(traversal, content)], (entry,))
    fork = await _fork(s, captured.snapshot.base_commit, "run-traversal")
    with pytest.raises(WorkspaceError) as traversed:
        await s.allocator.restore(snapshot, ref, fork, artifacts=s.artifacts)  # type: ignore[arg-type]
    assert traversed.value.code == CHECKPOINT_INVALID
    assert not (Path(fork.path).parent / "outside.txt").exists()

    escape = tarfile.TarInfo("escape")
    escape.type = tarfile.SYMTYPE
    escape.linkname = "../../../../outside"
    link_entry = UntrackedEntry(
        path="escape",
        kind="symlink",
        mode="120000",
        digest=bytes_digest(escape.linkname.encode()),
        bytes=len(escape.linkname),
        link_target=escape.linkname,
    )
    snapshot, ref = await _forged(s, captured.manifest, [(escape, None)], (link_entry,))
    other = await _fork(s, captured.snapshot.base_commit, "run-escape")
    with pytest.raises(WorkspaceError) as escaped:
        await s.allocator.restore(snapshot, ref, other, artifacts=s.artifacts)  # type: ignore[arg-type]
    assert escaped.value.code == CHECKPOINT_INVALID
    assert not (Path(other.path) / "escape").exists()


async def test_an_untracked_link_out_of_the_workspace_is_excluded_and_recorded(
    tmp_path: Path,
) -> None:
    s = stack(tmp_path)
    outside = tmp_path / "secrets"
    outside.mkdir()
    (outside / "token.txt").write_bytes(b"never captured\n")
    source = await s.allocator.allocate(request(s.primary))
    link(outside, Path(source.path) / "linked")
    (Path(source.path) / "kept.txt").write_bytes(b"kept\n")
    captured = await s.allocator.snapshot(source, artifacts=s.artifacts)
    assert captured.manifest is not None
    assert [entry.path for entry in captured.manifest.untracked] == ["kept.txt"]
    assert any(rule.endswith("linked") for rule in captured.snapshot.exclusions)
    assert all(b"never captured" not in blob for blob in s.artifacts.staged.values())


def test_workspace_artifact_manifest_is_a_registered_contract() -> None:
    from mission_control.application.workspaces.snapshots import (
        ARTIFACT_MANIFEST_SCHEMA,
        workspace_contract_schemas,
    )

    schemas = workspace_contract_schemas()
    assert set(schemas) == {"workspace_artifact_manifest"}
    manifest = schemas["workspace_artifact_manifest"]
    assert manifest["properties"]["schema_version"]["const"] == ARTIFACT_MANIFEST_SCHEMA
    assert manifest["additionalProperties"] is False
    assert {"commits", "patch", "captured_at", "base_commit", "head_commit"} <= set(
        manifest["required"]
    )
