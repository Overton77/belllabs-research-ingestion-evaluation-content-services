"""Git implementation of the provider-neutral `WorkspaceBackend` (SPEC-02 "Worktrees and
snapshots"; MP-04, relocated from `adapters/cursor/` by MP-09). It started as the Cursor local
leaser's git code and is shared by every local lane; nothing here is Cursor-specific.

- **Dedicated clone.** Each repository (remote URL or local path) is fetched into
  `<root>/_clones/<digest>` (a bare repository whose fetch refspec maps the source's branches to
  `refs/remotes/origin/*`), so a lease never adds a worktree to, writes the index of, or reads
  uncommitted content from the developer's checkout. A local checkout is inspected read-only
  (`GIT_OPTIONAL_LOCKS=0`) to report whether it is dirty.
- **Unique branch.** A worktree is added on the slot's branch (`mc/ws/<digest>`) at the pinned
  base commit, never with `--force`/`-B`; an existing branch or path is a refusal.
- **Non-mutating capture.** A snapshot runs git against a scratch repository whose object store
  borrows the source's objects (`objects/info/alternates`) and whose index is a copy, so it
  works on a leased worktree and on a primary checkout alike without touching either. It
  commits the index tree (I) and the tracked worktree tree (W) and bundles `base..W`.
- **Verified restore.** Bundle, patch and archive digests are checked before anything is
  applied; afterwards HEAD, the index tree, the worktree tree and every untracked file's digest
  are read back and compared with the manifest.

Git runs in worker threads (`asyncio.to_thread`); every subprocess gets an environment without
the caller's `GIT_*` overrides and with prompts disabled.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import os
import shutil
import stat
import subprocess
import tarfile
import tempfile
import threading
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Final, cast

from mission_control.application.execution.harness.leases import WorkspaceLease
from mission_control.application.workspaces.errors import (
    CHECKPOINT_INVALID,
    WORKSPACE_BRANCH_COLLISION,
    WORKSPACE_PATH_ESCAPE,
    WORKSPACE_PATH_OCCUPIED,
    WORKSPACE_SNAPSHOT_FAILED,
    WorkspaceError,
)
from mission_control.application.workspaces.policy import contained, resolved_root
from mission_control.application.workspaces.ports import SourceState
from mission_control.application.workspaces.snapshots import (
    GITIGNORED_RULE,
    LFS_RULE,
    CapturedSnapshot,
    RestoreReceipt,
    SnapshotArtifacts,
    StoredArtifact,
    UntrackedEntry,
    WorkspaceArtifactManifest,
    bytes_digest,
    contract_snapshot,
    verified_bytes,
    workspace_snapshot_ref,
)
from mission_control.domain.execution.lanes import LaneProfileName

CLONES_DIR: Final = "_clones"
SNAPSHOT_REF: Final = "refs/mc/snapshot"
_IDENTITY: Final = {
    "GIT_AUTHOR_NAME": "Mission Control",
    "GIT_AUTHOR_EMAIL": "mission-control@localhost",
    "GIT_COMMITTER_NAME": "Mission Control",
    "GIT_COMMITTER_EMAIL": "mission-control@localhost",
}
# The checkout settings that decide how worktree bytes map to blobs; a snapshot of a checkout
# uses that checkout's effective values.
_CONTENT_CONFIG: Final = (
    "core.autocrlf",
    "core.eol",
    "core.filemode",
    "core.symlinks",
    "core.ignorecase",
    "core.safecrlf",
    "core.excludesfile",
    "core.attributesfile",
)
_PATHSPEC_PREFIX: Final = "pathspec:"
_SYMLINK_ESCAPE: Final = "symlink-escape:"
_NESTED_REPOSITORY: Final = "nested-repository:"


class GitCommandError(RuntimeError):
    """A git command failed (stderr excerpt only, never secrets)."""

    def __init__(self, args: Sequence[str], returncode: int, stderr: str) -> None:
        super().__init__(f"git {args[0] if args else ''} failed ({returncode}): {stderr}")
        self.stderr = stderr


def _environment(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith("GIT_") and not name.startswith("CURSOR_")
    }
    env["GIT_TERMINAL_PROMPT"] = "0"
    env["GIT_OPTIONAL_LOCKS"] = "0"
    if extra:
        env.update(extra)
    return env


def run_git(
    *args: str,
    cwd: Path,
    env: Mapping[str, str] | None = None,
    check: bool = True,
) -> bytes:
    completed = subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, env=_environment(env), check=False
    )
    if check and completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", "replace").strip()[:400]
        raise GitCommandError(args, completed.returncode, detail)
    return completed.stdout


def _text(*args: str, cwd: Path, env: Mapping[str, str] | None = None) -> str:
    return run_git(*args, cwd=cwd, env=env).decode("utf-8", "replace").strip()


def is_remote(repository: str) -> bool:
    return "://" in repository or repository.startswith("git@")


def _nul_split(raw: bytes) -> list[str]:
    return [item.decode("utf-8", "surrogateescape") for item in raw.split(b"\0") if item]


def _excludes(paths: Sequence[str]) -> list[str]:
    return [f":(exclude){path}" for path in dict.fromkeys(paths) if path]


def _safe_relative(path: str) -> PurePosixPath:
    relative = PurePosixPath(path)
    if (
        not relative.parts
        or relative.is_absolute()
        or ".." in relative.parts
        or relative.parts[0] == ".git"
        or ":" in relative.parts[0]
    ):
        raise WorkspaceError(CHECKPOINT_INVALID, f"unsafe snapshot path: {path}")
    return relative


@dataclass(frozen=True)
class _Untracked:
    entry: UntrackedEntry
    content: bytes


@dataclass(frozen=True)
class _Observed:
    head: str
    index_tree: str
    worktree_tree: str
    all_tree: str
    untracked: tuple[_Untracked, ...]
    exclusions: tuple[str, ...]


@dataclass
class _Scratch:
    """A scratch repository borrowing a checkout's objects, with copies of its index."""

    checkout: Path
    directory: Path
    git_dir: Path
    config: tuple[str, ...]
    index: Path
    extra: dict[str, str] = field(default_factory=dict)

    def git(
        self,
        *args: str,
        index: Path | None = None,
        env: Mapping[str, str] | None = None,
        check: bool = True,
    ) -> bytes:
        merged = {"GIT_INDEX_FILE": str(index or self.index), **self.extra, **dict(env or {})}
        return run_git(
            "--git-dir",
            str(self.git_dir),
            "--work-tree",
            str(self.checkout),
            *self.config,
            *args,
            cwd=self.checkout,
            env=merged,
            check=check,
        )

    def text(self, *args: str, index: Path | None = None) -> str:
        return self.git(*args, index=index).decode("utf-8", "replace").strip()

    def copy_index(self, name: str, source: Path | None = None) -> Path:
        target = self.directory / name
        origin = source or self.index
        if origin.exists():
            shutil.copyfile(origin, target)
        return target


def _scratch(checkout: Path, directory: Path) -> _Scratch:
    git_dir = Path(_text("rev-parse", "--absolute-git-dir", cwd=checkout))
    common = Path(_text("rev-parse", "--git-common-dir", cwd=checkout))
    if not common.is_absolute():
        common = (checkout / common).resolve()
    config: list[str] = []
    for key in _CONTENT_CONFIG:
        value = run_git("config", "--get", key, cwd=checkout, check=False).decode().strip()
        if value:
            config += ["-c", f"{key}={value}"]
    scratch_dir = directory / "scratch"
    run_git("init", "--quiet", str(scratch_dir), cwd=directory)
    scratch_git = scratch_dir / ".git"
    alternates = scratch_git / "objects" / "info" / "alternates"
    alternates.write_bytes((common / "objects").resolve().as_posix().encode("utf-8") + b"\n")
    for name in ("exclude", "attributes"):
        origin = common / "info" / name
        if origin.is_file():
            target = scratch_git / "info" / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(origin, target)
    index = directory / "index"
    if (git_dir / "index").is_file():
        shutil.copyfile(git_dir / "index", index)
    return _Scratch(
        checkout=checkout,
        directory=directory,
        git_dir=scratch_git,
        config=tuple(config),
        index=index,
    )


def _filemode(scratch: _Scratch) -> bool:
    for position, item in enumerate(scratch.config):
        if item.startswith("core.filemode=") and position > 0:
            return item.split("=", 1)[1].lower() in {"true", "1", "yes", "on"}
    return os.name != "nt"


def _observe(checkout: Path, directory: Path, exclude: Sequence[str]) -> tuple[_Scratch, _Observed]:
    """Read a checkout's HEAD, index tree, tracked worktree tree and untracked included files
    through a scratch repository (the checkout's index and refs are never written)."""

    scratch = _scratch(checkout, directory)
    try:
        head = _text("rev-parse", "--verify", "HEAD^{commit}", cwd=checkout)
    except GitCommandError as error:
        raise WorkspaceError(
            WORKSPACE_SNAPSHOT_FAILED, "the checkout has no HEAD commit"
        ) from error
    pathspec = [".", *_excludes(exclude)]
    if scratch.git("ls-files", "--unmerged").strip():
        raise WorkspaceError(
            WORKSPACE_SNAPSHOT_FAILED, "the index has unmerged entries; resolve them first"
        )
    try:
        index_tree = scratch.text("write-tree")
    except GitCommandError as error:
        raise WorkspaceError(
            WORKSPACE_SNAPSHOT_FAILED, f"the index tree cannot be written: {error.stderr}"
        ) from error
    worktree_index = scratch.copy_index("index-worktree")
    scratch.git("add", "--update", "--", *pathspec, index=worktree_index)
    worktree_tree = scratch.text("write-tree", index=worktree_index)
    listed = _nul_split(
        scratch.git("ls-files", "-z", "--others", "--exclude-standard", "--", *pathspec)
    )
    root = resolved_root(checkout)
    filemode = _filemode(scratch)
    exclusions: list[str] = [GITIGNORED_RULE, *(f"{_PATHSPEC_PREFIX}{p}" for p in exclude)]
    escaped: list[str] = []
    untracked: list[_Untracked] = []
    for path in sorted(listed):
        relative = path.rstrip("/")
        target = checkout.joinpath(*PurePosixPath(relative).parts)
        if path.endswith("/") or (target.is_dir() and not target.is_symlink()):
            exclusions.append(f"{_NESTED_REPOSITORY}{relative}")
            escaped.append(relative)
            continue
        try:
            contained(root, target)
        except WorkspaceError:
            # The entry, or a linked directory above it, resolves out of the workspace.
            exclusions.append(f"{_SYMLINK_ESCAPE}{relative}")
            escaped.append(relative)
            continue
        if target.is_symlink() or target.is_junction():
            link = str(target.readlink())
            encoded = Path(link).as_posix().encode("utf-8")
            untracked.append(
                _Untracked(
                    UntrackedEntry(
                        path=relative,
                        kind="symlink",
                        mode="120000",
                        digest=bytes_digest(encoded),
                        bytes=len(encoded),
                        link_target=Path(link).as_posix(),
                    ),
                    encoded,
                )
            )
            continue
        content = target.read_bytes()
        executable = filemode and bool(target.stat().st_mode & stat.S_IXUSR)
        untracked.append(
            _Untracked(
                UntrackedEntry(
                    path=relative,
                    kind="file",
                    mode="100755" if executable else "100644",
                    digest=bytes_digest(content),
                    bytes=len(content),
                ),
                content,
            )
        )
    all_index = scratch.copy_index("index-all", worktree_index)
    scratch.git("add", "--all", "--", *pathspec, *_excludes(escaped), index=all_index)
    all_tree = scratch.text("write-tree", index=all_index)
    attributes = checkout / ".gitattributes"
    if attributes.is_file() and b"filter=lfs" in attributes.read_bytes():
        exclusions.append(LFS_RULE)
    observed = _Observed(
        head=head,
        index_tree=index_tree,
        worktree_tree=worktree_tree,
        all_tree=all_tree,
        untracked=tuple(untracked),
        exclusions=tuple(dict.fromkeys(exclusions)),
    )
    return scratch, observed


def _diff_raw(scratch: _Scratch, base: str, tree: str) -> list[tuple[str, str, str]]:
    """`(status, new mode, path)` of every change from `base` to `tree`."""

    raw = scratch.git("diff-tree", "-r", "-z", "--no-renames", base, tree)
    fields = raw.split(b"\0")
    changes: list[tuple[str, str, str]] = []
    position = 0
    while position < len(fields) - 1:
        header = fields[position].decode()
        if not header.startswith(":"):
            position += 1
            continue
        parts = header[1:].split()
        path = fields[position + 1].decode("utf-8", "surrogateescape")
        changes.append((parts[4], parts[1], path))
        position += 2
    return changes


def _tar(untracked: Sequence[_Untracked]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for item in untracked:
            info = tarfile.TarInfo(item.entry.path)
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            if item.entry.kind == "symlink":
                info.type = tarfile.SYMTYPE
                info.linkname = item.entry.link_target or ""
                info.mode = 0o777
                archive.addfile(info)
            else:
                info.mode = 0o755 if item.entry.mode == "100755" else 0o644
                info.size = len(item.content)
                archive.addfile(info, io.BytesIO(item.content))
    return buffer.getvalue()


def _commit_tree(scratch: _Scratch, tree: str, parent: str, message: str, when: datetime) -> str:
    stamp = f"{int(when.timestamp())} +0000"
    env = {**_IDENTITY, "GIT_AUTHOR_DATE": stamp, "GIT_COMMITTER_DATE": stamp}
    return scratch.git("commit-tree", tree, "-p", parent, "-m", message, env=env).decode().strip()


@dataclass(frozen=True)
class _Frozen:
    head: str
    index_tree: str
    index_commit: str
    worktree_tree: str
    worktree_commit: str
    bundle: bytes
    patch: bytes
    archive: bytes | None
    untracked: tuple[UntrackedEntry, ...]
    deletions: tuple[str, ...]
    modes: dict[str, str]
    submodules: dict[str, str]
    exclusions: tuple[str, ...]


def _freeze(checkout: Path, base_commit: str, exclude: Sequence[str], when: datetime) -> _Frozen:
    with tempfile.TemporaryDirectory(prefix="mc-snapshot-") as temporary:
        scratch, observed = _observe(checkout, Path(temporary), exclude)
        index_commit = _commit_tree(
            scratch, observed.index_tree, observed.head, "mission control snapshot: index", when
        )
        worktree_commit = _commit_tree(
            scratch,
            observed.worktree_tree,
            index_commit,
            "mission control snapshot: worktree",
            when,
        )
        scratch.git("update-ref", SNAPSHOT_REF, worktree_commit)
        bundle_path = Path(temporary) / "snapshot.bundle"
        try:
            scratch.git("bundle", "create", str(bundle_path), SNAPSHOT_REF, f"^{base_commit}")
        except GitCommandError as error:
            raise WorkspaceError(
                WORKSPACE_SNAPSHOT_FAILED, f"the commits could not be bundled: {error.stderr}"
            ) from error
        bundle = bundle_path.read_bytes()
        patch = scratch.git("diff", "--binary", "--full-index", base_commit, observed.all_tree)
        changes = _diff_raw(scratch, base_commit, observed.all_tree)
        deletions = tuple(
            path
            for status, _mode, path in _diff_raw(scratch, base_commit, observed.worktree_tree)
            if status == "D"
        )
        modes = {path: mode for status, mode, path in changes if status != "D"}
        submodules: dict[str, str] = {}
        for line in _nul_split(scratch.git("ls-tree", "-r", "-z", observed.worktree_tree)):
            meta, _tab, path = line.partition("\t")
            mode, kind, object_id = meta.split()
            if mode == "160000" and kind == "commit":
                submodules[path] = object_id
    return _Frozen(
        head=observed.head,
        index_tree=observed.index_tree,
        index_commit=index_commit,
        worktree_tree=observed.worktree_tree,
        worktree_commit=worktree_commit,
        bundle=bundle,
        patch=patch,
        archive=_tar(observed.untracked) if observed.untracked else None,
        untracked=tuple(item.entry for item in observed.untracked),
        deletions=deletions,
        modes=modes,
        submodules=submodules,
        exclusions=observed.exclusions,
    )


def _pathspec_excludes(manifest: WorkspaceArtifactManifest) -> tuple[str, ...]:
    return tuple(
        rule.removeprefix(_PATHSPEC_PREFIX)
        for rule in manifest.exclusions
        if rule.startswith(_PATHSPEC_PREFIX)
    )


def _matches(checkout: Path, manifest: WorkspaceArtifactManifest) -> tuple[bool, _Observed]:
    with tempfile.TemporaryDirectory(prefix="mc-verify-") as temporary:
        _scratch_repo, observed = _observe(checkout, Path(temporary), _pathspec_excludes(manifest))
    ok = (
        observed.head == manifest.head_commit
        and observed.index_tree == manifest.index_tree
        and observed.worktree_tree == manifest.worktree_tree
        and tuple(item.entry for item in observed.untracked) == manifest.untracked
    )
    return ok, observed


class GitWorkspaceBackend:
    """`WorkspaceBackend` over the git CLI, rooted at the admitted workspace root."""

    def __init__(self, root: Path) -> None:
        self._root = root
        self._locks: dict[str, threading.Lock] = {}
        self._guard = threading.Lock()

    @property
    def root(self) -> Path:
        return self._root

    def clone_path(self, repository: str) -> Path:
        digest = hashlib.sha256(repository.encode("utf-8")).hexdigest()[:24]
        return self._root / CLONES_DIR / digest

    def _lock(self, key: str) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    # --- source -----------------------------------------------------------------------------

    async def prepare_source(self, repository: str | None, base_ref: str) -> SourceState:
        if repository is None:
            raise WorkspaceError(
                WORKSPACE_SNAPSHOT_FAILED, "a managed worktree needs a source repository"
            )
        return await asyncio.to_thread(self._prepare_source, repository, base_ref)

    def ensure_clone(self, repository: str) -> Path:
        clone = self.clone_path(repository)
        with self._lock(str(clone)):
            if not (clone / "HEAD").is_file():
                clone.parent.mkdir(parents=True, exist_ok=True)
                staging = Path(tempfile.mkdtemp(prefix="clone-", dir=clone.parent))
                run_git("init", "--quiet", "--bare", str(staging), cwd=clone.parent)
                run_git("remote", "add", "origin", repository, cwd=staging)
                run_git(
                    "config",
                    "--replace-all",
                    "remote.origin.fetch",
                    "+refs/heads/*:refs/remotes/origin/*",
                    cwd=staging,
                )
                run_git("config", "core.autocrlf", "false", cwd=staging)
                try:
                    staging.replace(clone)
                except OSError:
                    shutil.rmtree(staging, ignore_errors=True)
            run_git("fetch", "--quiet", "--tags", "origin", cwd=clone)
        return clone

    def resolve(self, clone: Path, base_ref: str, *, local_head: str | None) -> str:
        candidates: list[str] = []
        if base_ref == "HEAD" and local_head is not None:
            candidates.append(local_head)
        candidates += [f"refs/remotes/origin/{base_ref}", f"refs/tags/{base_ref}", base_ref]
        for candidate in candidates:
            completed = run_git(
                "rev-parse",
                "--verify",
                "--quiet",
                f"{candidate}^{{commit}}",
                cwd=clone,
                check=False,
            )
            commit = completed.decode().strip()
            if commit:
                return commit
        raise WorkspaceError(
            WORKSPACE_SNAPSHOT_FAILED, f"base ref {base_ref} does not resolve in the source"
        )

    def _prepare_source(self, repository: str, base_ref: str) -> SourceState:
        checkout: Path | None = None
        head: str | None = None
        dirty = 0
        if not is_remote(repository):
            local = Path(repository)
            inside = run_git("rev-parse", "--is-inside-work-tree", cwd=local, check=False)
            if inside.decode().strip() == "true":
                checkout = Path(_text("rev-parse", "--show-toplevel", cwd=local))
                head = (
                    run_git(
                        "rev-parse", "--verify", "--quiet", "HEAD^{commit}", cwd=local, check=False
                    )
                    .decode()
                    .strip()
                    or None
                )
                status = run_git(
                    "status", "--porcelain=v1", "-z", "--untracked-files=all", cwd=checkout
                )
                dirty = len(_nul_split(status))
        clone = self.ensure_clone(repository)
        if head is not None and not self._has(clone, head):
            run_git("fetch", "--quiet", "origin", head, cwd=clone, check=False)
        base_commit = self.resolve(clone, base_ref, local_head=head)
        return SourceState(
            repository=repository,
            base_ref=base_ref,
            base_commit=base_commit,
            clone=str(clone),
            checkout=str(checkout) if checkout is not None else None,
            checkout_head=head,
            dirty_entries=dirty,
        )

    @staticmethod
    def _has(clone: Path, commit: str) -> bool:
        completed = subprocess.run(
            ["git", "cat-file", "-e", f"{commit}^{{commit}}"],
            cwd=clone,
            capture_output=True,
            env=_environment(),
            check=False,
        )
        return completed.returncode == 0

    # --- worktrees --------------------------------------------------------------------------

    async def materialize(self, lease: WorkspaceLease, source: SourceState) -> None:
        await asyncio.to_thread(self._materialize, lease, source)

    def _materialize(self, lease: WorkspaceLease, source: SourceState) -> None:
        if source.clone is None:
            raise WorkspaceError(WORKSPACE_SNAPSHOT_FAILED, "the source has no dedicated clone")
        clone = Path(source.clone)
        contained(self._root, clone)
        path = contained(self._root, Path(lease.path))
        if path.exists() and any(path.iterdir()):
            raise WorkspaceError(
                WORKSPACE_PATH_OCCUPIED, f"{path} already exists; a lease never adopts it"
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        contained(self._root, path.parent)
        with self._lock(str(clone)):
            run_git("worktree", "prune", cwd=clone)
            if lease.branch is None:
                run_git(
                    "worktree",
                    "add",
                    "--quiet",
                    "--detach",
                    str(path),
                    lease.base_commit,
                    cwd=clone,
                )
            else:
                self._check_branch_name(clone, lease.branch)
                if self._branch_exists(clone, lease.branch):
                    raise WorkspaceError(
                        WORKSPACE_BRANCH_COLLISION,
                        f"branch {lease.branch} already exists in the dedicated clone",
                    )
                try:
                    run_git(
                        "worktree",
                        "add",
                        "--quiet",
                        "-b",
                        lease.branch,
                        str(path),
                        lease.base_commit,
                        cwd=clone,
                    )
                except GitCommandError as error:
                    if "already exists" in error.stderr:
                        raise WorkspaceError(
                            WORKSPACE_BRANCH_COLLISION, f"branch {lease.branch}: {error.stderr}"
                        ) from error
                    raise
        self._verify(lease)

    @staticmethod
    def _check_branch_name(clone: Path, branch: str) -> None:
        completed = run_git("check-ref-format", "--branch", branch, cwd=clone, check=False)
        if not completed.strip():
            raise WorkspaceError(WORKSPACE_BRANCH_COLLISION, f"{branch} is not a branch name")

    @staticmethod
    def _branch_exists(clone: Path, branch: str) -> bool:
        completed = subprocess.run(
            ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
            cwd=clone,
            capture_output=True,
            env=_environment(),
            check=False,
        )
        return completed.returncode == 0

    async def verify(self, lease: WorkspaceLease) -> None:
        await asyncio.to_thread(self._verify, lease)

    def _verify(self, lease: WorkspaceLease) -> None:
        path = contained(self._root, Path(lease.path))
        top = Path(_text("rev-parse", "--show-toplevel", cwd=path)).resolve()
        if top != path:
            raise WorkspaceError(
                WORKSPACE_PATH_ESCAPE, f"{path} is not its own worktree (top level {top})"
            )
        common = Path(_text("rev-parse", "--git-common-dir", cwd=path))
        if not common.is_absolute():
            common = path / common
        contained(self._root, common)
        if lease.branch is not None:
            current = run_git("symbolic-ref", "--quiet", "HEAD", cwd=path, check=False)
            if current.decode().strip() != f"refs/heads/{lease.branch}":
                raise WorkspaceError(
                    WORKSPACE_BRANCH_COLLISION,
                    f"{path} is not on the lease branch {lease.branch}",
                )

    async def remove(self, lease: WorkspaceLease) -> None:
        await asyncio.to_thread(self._remove, lease)

    def _remove(self, lease: WorkspaceLease) -> None:
        path = contained(self._root, Path(lease.path))
        clone: Path | None = None
        if path.is_dir():
            completed = run_git("rev-parse", "--git-common-dir", cwd=path, check=False).decode()
            if completed.strip():
                candidate = Path(completed.strip())
                clone = (candidate if candidate.is_absolute() else path / candidate).resolve()
                contained(self._root, clone)
        if clone is None and lease.repository is not None:
            clone = self.clone_path(lease.repository)
        if clone is not None and (clone / "HEAD").is_file():
            with self._lock(str(clone)):
                run_git("worktree", "remove", "--force", str(path), cwd=clone, check=False)
                run_git("worktree", "prune", cwd=clone, check=False)
                if lease.branch is not None:
                    run_git("branch", "-D", lease.branch, cwd=clone, check=False)
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)

    # --- snapshots --------------------------------------------------------------------------

    async def capture(
        self,
        root: Path,
        *,
        lease: WorkspaceLease,
        artifacts: SnapshotArtifacts,
        name: str,
        exclude: Sequence[str] = (),
    ) -> CapturedSnapshot:
        when = datetime.now(UTC).replace(microsecond=0)
        base = lease.base_commit
        frozen = await asyncio.to_thread(_freeze, root, base, tuple(exclude), when)
        scope = lease.request_scope

        async def stored(suffix: str, content: bytes, media_type: str) -> StoredArtifact:
            ref = await artifacts.stage(
                request_scope=scope, name=f"{name}/{suffix}", content=content, media_type=media_type
            )
            return StoredArtifact(ref=ref, digest=bytes_digest(content), bytes=len(content))

        commits = await stored("commits.bundle", frozen.bundle, "application/x-git-bundle")
        patch = await stored("patch.diff", frozen.patch, "text/x-diff")
        archive = (
            await stored("untracked.tar", frozen.archive, "application/x-tar")
            if frozen.archive is not None
            else None
        )
        manifest = WorkspaceArtifactManifest(
            lane_profile=cast(LaneProfileName, lease.lane_profile),
            producer_lease_id=lease.lease_id,
            producer_fence=lease.fence,
            producer_generation=lease.generation,
            base_commit=base,
            head_commit=frozen.head,
            branch=lease.branch,
            index_tree=frozen.index_tree,
            index_commit=frozen.index_commit,
            worktree_tree=frozen.worktree_tree,
            worktree_commit=frozen.worktree_commit,
            commits=commits,
            patch=patch,
            untracked_archive=archive,
            untracked=frozen.untracked,
            tracked_deletions=frozen.deletions,
            file_modes=frozen.modes,
            submodule_commits=frozen.submodules,
            exclusions=frozen.exclusions,
            captured_at=when,
        )
        encoded = manifest.encoded()
        manifest_ref = await artifacts.stage(
            request_scope=scope,
            name=f"{name}/workspace-manifest.json",
            content=encoded,
            media_type="application/json",
        )
        return CapturedSnapshot(
            snapshot=contract_snapshot(manifest, bytes_digest(encoded)),
            manifest=manifest,
            manifest_ref=manifest_ref,
            snapshot_ref=workspace_snapshot_ref(manifest_ref),
        )

    async def restore(
        self,
        manifest: WorkspaceArtifactManifest,
        lease: WorkspaceLease,
        artifacts: SnapshotArtifacts,
        *,
        snapshot_ref: str,
    ) -> RestoreReceipt:
        bundle = await verified_bytes(artifacts, manifest.commits, "commits bundle")
        await verified_bytes(artifacts, manifest.patch, "patch")
        archive = (
            await verified_bytes(artifacts, manifest.untracked_archive, "untracked archive")
            if manifest.untracked_archive is not None
            else None
        )
        observed = await asyncio.to_thread(self._restore, manifest, lease, bundle, archive)
        return RestoreReceipt(
            snapshot_ref=snapshot_ref,
            head_commit=observed.head,
            index_tree=observed.index_tree,
            worktree_tree=observed.worktree_tree,
            untracked={item.entry.path: item.entry.digest for item in observed.untracked},
        )

    def _restore(
        self,
        manifest: WorkspaceArtifactManifest,
        lease: WorkspaceLease,
        bundle: bytes,
        archive: bytes | None,
    ) -> _Observed:
        root = contained(self._root, Path(lease.path))
        head = _text("rev-parse", "--verify", "HEAD^{commit}", cwd=root)
        status = run_git("status", "--porcelain=v1", "-z", "--untracked-files=all", cwd=root)
        if head != manifest.base_commit or _nul_split(status):
            raise WorkspaceError(
                CHECKPOINT_INVALID, "a snapshot is restored only into a clean lease at its base"
            )
        with tempfile.TemporaryDirectory(prefix="mc-restore-") as temporary:
            bundle_path = Path(temporary) / "snapshot.bundle"
            bundle_path.write_bytes(bundle)
            try:
                run_git("bundle", "verify", "--quiet", str(bundle_path), cwd=root)
                run_git("bundle", "unbundle", str(bundle_path), cwd=root)
            except GitCommandError as error:
                raise WorkspaceError(
                    CHECKPOINT_INVALID, f"the commits bundle does not apply: {error.stderr}"
                ) from error
        expected = {
            f"{manifest.worktree_commit}^{{tree}}": manifest.worktree_tree,
            f"{manifest.index_commit}^{{tree}}": manifest.index_tree,
            f"{manifest.worktree_commit}^": manifest.index_commit,
            f"{manifest.index_commit}^": manifest.head_commit,
        }
        for revision, object_id in expected.items():
            if _text("rev-parse", "--verify", revision, cwd=root) != object_id:
                raise WorkspaceError(CHECKPOINT_INVALID, f"{revision} is not {object_id}")
        run_git("reset", "--quiet", "--hard", manifest.head_commit, cwd=root)
        run_git("read-tree", "-u", "--reset", manifest.worktree_commit, cwd=root)
        run_git("read-tree", manifest.index_commit, cwd=root)
        run_git("update-index", "-q", "--refresh", cwd=root, check=False)
        if archive is not None:
            self._extract(root, manifest, archive)
        ok, observed = _matches(root, manifest)
        if not ok:
            raise WorkspaceError(
                CHECKPOINT_INVALID, "the restored workspace differs from its snapshot"
            )
        return observed

    @staticmethod
    def _extract(root: Path, manifest: WorkspaceArtifactManifest, archive: bytes) -> None:
        entries = {entry.path: entry for entry in manifest.untracked}
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as tar:
            members = tar.getmembers()
            if sorted(member.name for member in members) != sorted(entries):
                raise WorkspaceError(
                    CHECKPOINT_INVALID, "the untracked archive and its manifest disagree"
                )
            for member in members:
                entry = entries[member.name]
                relative = _safe_relative(member.name)
                target = root.joinpath(*relative.parts)
                if os.path.lexists(target):
                    raise WorkspaceError(
                        CHECKPOINT_INVALID, f"untracked {member.name} would overwrite a path"
                    )
                target.parent.mkdir(parents=True, exist_ok=True)
                contained(root, target.parent)
                if entry.kind == "symlink":
                    if not member.issym() or member.linkname != entry.link_target:
                        raise WorkspaceError(CHECKPOINT_INVALID, f"{member.name} is not the link")
                    link = entry.link_target or ""
                    try:
                        contained(root, target.parent / link)
                    except WorkspaceError as error:
                        raise WorkspaceError(
                            CHECKPOINT_INVALID, f"{member.name} links outside the workspace"
                        ) from error
                    if bytes_digest(link.encode("utf-8")) != entry.digest:
                        raise WorkspaceError(CHECKPOINT_INVALID, f"{member.name} digest differs")
                    target.symlink_to(link)
                    continue
                extracted = tar.extractfile(member) if member.isfile() else None
                if extracted is None:
                    raise WorkspaceError(CHECKPOINT_INVALID, f"{member.name} is not a file")
                content = extracted.read()
                if len(content) != entry.bytes or bytes_digest(content) != entry.digest:
                    raise WorkspaceError(CHECKPOINT_INVALID, f"{member.name} digest differs")
                target.write_bytes(content)
                if entry.mode == "100755":
                    target.chmod(target.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    async def matches(self, lease: WorkspaceLease, manifest: WorkspaceArtifactManifest) -> bool:
        root = contained(self._root, Path(lease.path))
        ok, _observed = await asyncio.to_thread(_matches, root, manifest)
        return ok


__all__ = [
    "CLONES_DIR",
    "GitCommandError",
    "GitWorkspaceBackend",
    "is_remote",
    "run_git",
]
