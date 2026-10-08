"""Capability bundle manifests and digest-addressed custody paths (ADR-0024, SPEC-01).

A Skill Bundle, hook script directory or long subagent prompt is stored once in the private
``capability-bundles`` bucket under
``<application>/<kind>/<capability_id>/<version>/<manifest_sha256>/<relative path>``. The
manifest digest is the sha256 of the canonical manifest JSON (sorted keys, no whitespace);
the manifest pins every file's sha256 and size, so a path collision implies identical bytes.
``computed_hash`` is the Vercel ``skills`` CLI directory hash, so pins interoperate with
``skills-lock.json``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from mission_control.domain.authoring.canonical import stable_json_dump

BUNDLE_MANIFEST_SCHEMA: Final = "mc.capability_bundle_manifest.v1"
BUNDLE_BUCKET = "capability-bundles"
SIGNED_DOWNLOAD_TTL_SECONDS = 300
RESUMABLE_THRESHOLD_BYTES = 6 * 1024 * 1024
RESUMABLE_CHUNK_BYTES = 6 * 1024 * 1024
MAX_BUNDLE_FILE_BYTES = 50 * 1024 * 1024
_SKIPPED_PARTS = frozenset({".git", "node_modules"})
_SHA256 = r"^sha256:[0-9a-f]{64}$"

BundleKind = Literal["skill_bundle", "hook_script", "subagent_profile"]


class CapabilityDrift(ValueError):
    """Bytes differ from the pinned manifest (error code ``CAPABILITY_DRIFT``)."""

    code = "CAPABILITY_DRIFT"


def sha256_hex(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def normalize_bundle_path(raw: str) -> str:
    """A safe, normalized POSIX relative path; rejects traversal and ambiguous spellings."""
    path = raw.replace("\\", "/")
    parts = path.split("/")
    if (
        not path
        or path.startswith("/")
        or re.match(r"^[A-Za-z]:", path)
        or any(part in {"", ".", ".."} for part in parts)
        or any(ord(char) < 32 for char in path)
        or ":" in path
    ):
        raise CapabilityDrift(f"unsafe bundle path: {raw!r}")
    return path


def skipped(path: str) -> bool:
    return any(part in _SKIPPED_PARTS for part in path.split("/"))


def computed_hash(files: Iterable[tuple[str, bytes]]) -> str:
    """The Vercel ``skills`` CLI directory hash (hex).

    Files are sorted by relative POSIX path (case-insensitive, then exact), skipping ``.git``
    and ``node_modules``; the digest is sha256 over each path followed by its bytes.
    """
    selected = [
        (path.replace("\\", "/"), content)
        for path, content in files
        if not skipped(path.replace("\\", "/"))
    ]
    digest = hashlib.sha256()
    for path, content in sorted(selected, key=lambda item: (item[0].lower(), item[0])):
        digest.update(path.encode("utf-8"))
        digest.update(content)
    return digest.hexdigest()


class BundleFileEntry(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str = Field(min_length=1)
    sha256: str = Field(pattern=_SHA256)
    size_bytes: int = Field(ge=0, le=MAX_BUNDLE_FILE_BYTES)
    executable: bool = False

    @field_validator("path")
    @classmethod
    def safe_path(cls, value: str) -> str:
        return normalize_bundle_path(value)


class CapabilityBundleManifest(BaseModel):
    """``mc.capability_bundle_manifest.v1``: what a pin's bytes are, file by file."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["mc.capability_bundle_manifest.v1"] = BUNDLE_MANIFEST_SCHEMA
    application_id: str = Field(pattern=r"^[a-z][a-z0-9-]{0,62}$")
    kind: BundleKind
    capability_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]*$")
    version: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")
    files: tuple[BundleFileEntry, ...] = Field(min_length=1, max_length=1024)
    computed_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def files_are_sorted_unique_and_complete(self) -> CapabilityBundleManifest:
        paths = [entry.path for entry in self.files]
        if paths != sorted(paths):
            raise ValueError("bundle manifest files must be sorted by path")
        folded = [path.casefold() for path in paths]
        if len(folded) != len(set(folded)):
            raise ValueError("bundle manifest paths must be unique (case-insensitively)")
        if self.kind == "skill_bundle" and "SKILL.md" not in paths:
            raise ValueError("a skill bundle manifest must contain SKILL.md")
        return self

    def canonical_json(self) -> bytes:
        return json.dumps(
            stable_json_dump(self), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")

    @property
    def digest(self) -> str:
        return "sha256:" + sha256_hex(self.canonical_json())

    @property
    def object_prefix(self) -> str:
        return (
            f"{self.application_id}/{self.kind}/{self.capability_id}/{self.version}/"
            f"{self.digest.removeprefix('sha256:')}"
        )

    def object_path(self, relative: str) -> str:
        return f"{self.object_prefix}/{normalize_bundle_path(relative)}"

    @property
    def total_bytes(self) -> int:
        return sum(entry.size_bytes for entry in self.files)

    def entry(self, relative: str) -> BundleFileEntry:
        for item in self.files:
            if item.path == relative:
                return item
        raise KeyError(relative)


def build_bundle_manifest(
    files: Iterable[tuple[str, bytes]],
    *,
    application_id: str,
    kind: BundleKind,
    capability_id: str,
    version: str,
    executable: frozenset[str] = frozenset(),
) -> CapabilityBundleManifest:
    selected = [
        (normalize_bundle_path(path), content)
        for path, content in files
        if not skipped(path.replace("\\", "/"))
    ]
    return CapabilityBundleManifest(
        application_id=application_id,
        kind=kind,
        capability_id=capability_id,
        version=version,
        files=tuple(
            BundleFileEntry(
                path=path,
                sha256="sha256:" + sha256_hex(content),
                size_bytes=len(content),
                executable=path in executable,
            )
            for path, content in sorted(selected)
        ),
        computed_hash=computed_hash(selected),
    )


def verify_bundle_files(
    manifest: CapabilityBundleManifest, files: Iterable[tuple[str, bytes]]
) -> tuple[tuple[str, bytes], ...]:
    """Every manifest file exactly once with its size and sha256, nothing else."""
    received: dict[str, bytes] = {}
    for raw, content in files:
        path = normalize_bundle_path(raw)
        if path.casefold() in {item.casefold() for item in received}:
            raise CapabilityDrift(f"duplicate bundle path: {path}")
        received[path] = content
    expected = {entry.path for entry in manifest.files}
    if set(received) != expected:
        missing = sorted(expected - set(received))
        extra = sorted(set(received) - expected)
        raise CapabilityDrift(
            f"bundle files differ from the manifest: missing={missing} extra={extra}"
        )
    for entry in manifest.files:
        content = received[entry.path]
        if len(content) != entry.size_bytes or "sha256:" + sha256_hex(content) != entry.sha256:
            raise CapabilityDrift(f"bundle file drifted from its pin: {entry.path}")
    return tuple((entry.path, received[entry.path]) for entry in manifest.files)


def parse_skill_frontmatter(skill_md: bytes) -> dict[str, str]:
    """Top-level ``key: value`` pairs of a SKILL.md YAML frontmatter block (strings only)."""
    text = skill_md.decode("utf-8").replace("\r\n", "\n")
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---", 4)
    if end == -1:
        return {}
    values: dict[str, str] = {}
    for line in text[4:end].splitlines():
        if not line or line.startswith((" ", "\t", "#")) or ":" not in line:
            continue
        key, _, raw = line.partition(":")
        value = raw.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        if value and not value.startswith(("|", ">")):
            values[key.strip()] = value
    return values
