"""Complete immutable directory bundles; no archive extraction or install hooks.

The local store is an offline/disposable deployment adapter for the capability-bundles
object namespace. Its root must be service-owned (not writable by sandbox agents).
Every load verifies bytes; filesystem permissions alone are not the integrity boundary.
"""

from __future__ import annotations

import json
import os
import re
import stat
import tempfile
import unicodedata
from hashlib import sha256
from pathlib import Path
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from mission_control.domain.authoring.canonical import sha256_digest, stable_json_dump

DIGEST = r"^sha256:[0-9a-f]{64}$"
MAX_FILES = 1024
MAX_FILE_BYTES = 16 * 1024 * 1024
MAX_BUNDLE_BYTES = 64 * 1024 * 1024


class BundleError(ValueError):
    """An incomplete, unsafe, conflicting or corrupted capability bundle."""


def bytes_digest(content: bytes) -> str:
    return "sha256:" + sha256(content).hexdigest()


def safe_relative_path(raw: str) -> str:
    """Reject ambiguous spellings, including Windows ADS/device names on POSIX."""
    if not raw or raw != unicodedata.normalize("NFC", raw):
        raise BundleError("bundle path must be nonempty NFC text")
    parts = raw.split("/")
    if any(c in raw for c in "\\:\x00") or any(ord(c) < 32 for c in raw):
        raise BundleError("unsafe bundle path")
    for part in parts:
        if part in ("", ".", "..") or part.endswith((" ", ".")):
            raise BundleError("unsafe bundle path")
        if re.fullmatch(r"(?i)(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])", part.split(".")[0]):
            raise BundleError("reserved bundle path")
    return raw


def safe_storage_namespace(value: str) -> str:
    # storage3 parses object paths as URLs. Never admit encoded separators, query
    # fragments or dot segments to the configured custody namespace.
    if not re.fullmatch(r"[A-Za-z0-9_-]+(?:/[A-Za-z0-9_-]+)*", value):
        raise BundleError("storage namespace must contain only safe ASCII path segments")
    return value


def reject_links(path: Path) -> None:
    for item in (path, *path.parents):
        if item.is_symlink() or item.is_junction():
            raise BundleError("bundle path contains a symlink or junction")


def directory_files(directory: Path) -> tuple[tuple[str, bytes], ...]:
    reject_links(directory)
    if not directory.is_dir():
        raise BundleError("bundle directory is unavailable")
    files: list[tuple[str, bytes]] = []
    total = 0
    for parent, dirs, names in os.walk(directory, followlinks=False):
        for name in [*dirs, *names]:
            path = Path(parent) / name
            reject_links(path)
            relative = safe_relative_path(path.relative_to(directory).as_posix())
            info = path.stat(follow_symlinks=False)
            if stat.S_ISDIR(info.st_mode):
                continue
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise BundleError("bundle contains a special file or hard link")
            if info.st_size > MAX_FILE_BYTES:
                raise BundleError("bundle file exceeds quota")
            with path.open("rb") as handle:
                content = handle.read(MAX_FILE_BYTES + 1)
                opened = os.fstat(handle.fileno())
            reject_links(path)
            if (opened.st_dev, opened.st_ino) != (info.st_dev, info.st_ino):
                raise BundleError("bundle changed while reading")
            total += len(content)
            if len(content) > MAX_FILE_BYTES or total > MAX_BUNDLE_BYTES:
                raise BundleError("bundle bytes exceed quota")
            files.append((relative, content))
            if len(files) > MAX_FILES:
                raise BundleError("bundle file count exceeds quota")
    result = tuple(sorted(files))
    file_manifest(result)
    return result


class BundleFile(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    path: str
    digest: str = Field(pattern=DIGEST)
    size_bytes: int = Field(ge=0, le=MAX_FILE_BYTES)
    mode: Literal["read_only"] = "read_only"


def file_manifest(files: tuple[tuple[str, bytes], ...]) -> tuple[BundleFile, ...]:
    seen: set[str] = set()
    entries: list[BundleFile] = []
    if len(files) > MAX_FILES or sum(len(content) for _, content in files) > MAX_BUNDLE_BYTES:
        raise BundleError("bundle exceeds quota")
    for name, content in sorted(files):
        path = safe_relative_path(name)
        folded = path.casefold()
        if folded in seen:
            raise BundleError("duplicate bundle path")
        seen.add(folded)
        entries.append(BundleFile(path=path, digest=bytes_digest(content), size_bytes=len(content)))
    for path in seen:
        if any("/".join(path.split("/")[:i]) in seen for i in range(1, len(path.split("/")))):
            raise BundleError("bundle file/directory collision")
    if not any(entry.path == "SKILL.md" for entry in entries):
        raise BundleError("bundle must contain SKILL.md")
    return tuple(entries)


def bundle_digest(files: tuple[tuple[str, bytes], ...]) -> str:
    return sha256_digest([stable_json_dump(entry) for entry in file_manifest(files)])


class BundleManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["mission-control.skill-bundle.v1"] = "mission-control.skill-bundle.v1"
    asset_id: str = Field(min_length=1, max_length=256)
    version: int = Field(ge=1)
    files: tuple[BundleFile, ...] = Field(min_length=1, max_length=MAX_FILES)
    bundle_digest: str = Field(pattern=DIGEST)

    @property
    def digest(self) -> str:
        return sha256_digest(stable_json_dump(self))


class BundleReader(Protocol):
    def load(self, digest: str) -> tuple[BundleManifest, tuple[tuple[str, bytes], ...]]: ...


class DirectoryBundleStore:
    """Immutable version binding and content-addressed objects, with atomic publication.

    Failed writes leave unreferenced objects, never a partially published version.
    There is deliberately no overwrite, purge, download URL or postinstall operation.
    """

    def __init__(self, root: Path) -> None:
        reject_links(root)
        self.root = root

    def _put(self, path: Path, content: bytes) -> None:
        reject_links(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            try:
                os.link(temporary, path)
            except FileExistsError:
                if path.read_bytes() != content:
                    raise BundleError("immutable bundle version/object conflicts") from None
        finally:
            temporary.unlink()

    def publish(self, directory: Path, *, asset_id: str, version: int) -> BundleManifest:
        files = directory_files(directory)
        manifest = BundleManifest(
            asset_id=asset_id,
            version=version,
            files=file_manifest(files),
            bundle_digest=bundle_digest(files),
        )
        for _, content in files:
            self._put(self.root / "objects" / bytes_digest(content)[7:], content)
        self._put(
            self.root / "manifests" / manifest.digest[7:],
            json.dumps(stable_json_dump(manifest), sort_keys=True, separators=(",", ":")).encode(),
        )
        version_key = sha256_digest({"asset_id": asset_id, "version": version})[7:]
        self._put(self.root / "versions" / version_key, manifest.digest.encode())
        return manifest

    def load(self, digest: str) -> tuple[BundleManifest, tuple[tuple[str, bytes], ...]]:
        if not re.fullmatch(DIGEST, digest):
            raise BundleError("invalid manifest digest")
        manifest_path = self.root / "manifests" / digest[7:]
        reject_links(manifest_path)
        with manifest_path.open("rb") as stream:
            raw = stream.read(MAX_FILE_BYTES + 1)
        if len(raw) > MAX_FILE_BYTES:
            raise BundleError("bundle manifest exceeds quota")
        manifest = BundleManifest.model_validate_json(raw)
        if sum(entry.size_bytes for entry in manifest.files) > MAX_BUNDLE_BYTES:
            raise BundleError("bundle exceeds total byte quota")
        if manifest.digest != digest:
            raise BundleError("bundle manifest digest mismatch")
        files = []
        for entry in manifest.files:
            safe_relative_path(entry.path)
            path = self.root / "objects" / entry.digest[7:]
            reject_links(path)
            with path.open("rb") as stream:
                content = stream.read(MAX_FILE_BYTES + 1)
            if len(content) != entry.size_bytes or bytes_digest(content) != entry.digest:
                raise BundleError("bundle object digest/size mismatch")
            files.append((entry.path, content))
        result = tuple(files)
        if (
            file_manifest(result) != manifest.files
            or bundle_digest(result) != manifest.bundle_digest
        ):
            raise BundleError("bundle file manifest mismatch")
        version_key = sha256_digest({"asset_id": manifest.asset_id, "version": manifest.version})[
            7:
        ]
        version_path = self.root / "versions" / version_key
        reject_links(version_path)
        if version_path.read_text() != digest:
            raise BundleError("bundle version is not published with this manifest")
        return manifest, result
