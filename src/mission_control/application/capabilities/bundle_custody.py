"""Capability bundle custody: upload once, register as proposed, download through signed URLs.

ADR-0024 / SPEC-01 "Custody in Supabase Storage". Three resumable steps:

1. custody: each file goes to its digest path, never overwritten (no ``upsert``); an object
   that already exists counts only after its bytes are re-read and compared;
2. registration: the catalog row is written ``proposed`` with the object prefix and the
   manifest digest; promotion to ``admitted`` stays an operator decision;
3. materialization: a worker asks for short-lived signed URLs, downloads, verifies every
   file against the manifest and mounts read-only; any mismatch is ``CAPABILITY_DRIFT``.

A crash between steps leaves unreferenced objects (listed by an audited cleanup, never
deleted automatically); re-running a step skips work already done and verified.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from mission_control.domain.authoring.contracts import (
    Definition,
    HookScriptDefinition,
    SkillDefinition,
    SubagentProfileDefinition,
)
from mission_control.domain.capabilities.bundles import (
    BUNDLE_BUCKET,
    SIGNED_DOWNLOAD_TTL_SECONDS,
    BundleKind,
    CapabilityBundleManifest,
    CapabilityDrift,
    build_bundle_manifest,
    normalize_bundle_path,
    parse_skill_frontmatter,
    sha256_hex,
    verify_bundle_files,
)
from mission_control.domain.capabilities.host_support import all_profiles

PutOutcome = Literal["created", "exists"]


class SignedUpload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: str
    url: str
    token: str | None = None


class BundleObjectStore(Protocol):
    """Port over the private ``capability-bundles`` bucket (Supabase Storage in production)."""

    async def exists(self, path: str) -> bool: ...

    async def put(self, path: str, content: bytes, *, content_type: str) -> PutOutcome: ...

    async def get(self, path: str) -> bytes: ...

    async def signed_download_urls(
        self, paths: Sequence[str], ttl_seconds: int
    ) -> Mapping[str, str]: ...

    async def signed_upload_url(self, path: str) -> SignedUpload: ...


class BundleRegistry(Protocol):
    """Port: the catalog row for a bundle (PostgreSQL ``asset_version``)."""

    async def find(self, manifest: CapabilityBundleManifest) -> str | None: ...

    async def register_proposed(
        self, manifest: CapabilityBundleManifest, definition: Definition, actor_ref: str
    ) -> str: ...


class BundleFetcher(Protocol):
    async def fetch(self, url: str) -> bytes: ...


class BundleCustodyConflict(RuntimeError):
    """An existing object under a digest path holds different bytes (never overwritten)."""


class UploadReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest_digest: str
    object_prefix: str
    uploaded: tuple[str, ...] = ()
    verified_existing: tuple[str, ...] = ()


class PublishPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest: CapabilityBundleManifest
    manifest_digest: str
    bucket: str = BUNDLE_BUCKET
    object_prefix: str
    uploads: tuple[SignedUpload, ...] = ()
    already_present: tuple[str, ...] = ()


class PublishResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    pin: str
    status: Literal["proposed", "existing"]
    manifest_digest: str
    object_prefix: str


@dataclass
class BundleCustodyService:
    store: BundleObjectStore
    registry: BundleRegistry | None = None
    actor_ref: str = "service:mission-control-catalog"
    signed_ttl_seconds: int = SIGNED_DOWNLOAD_TTL_SECONDS
    _content_type: str = field(default="application/octet-stream", repr=False)

    async def upload(
        self, manifest: CapabilityBundleManifest, files: Sequence[tuple[str, bytes]]
    ) -> UploadReport:
        """Custody step: write missing objects; verify the bytes of objects already there."""
        verified = verify_bundle_files(manifest, files)
        uploaded: list[str] = []
        existing: list[str] = []
        for path, content in verified:
            object_path = manifest.object_path(path)
            if await self.store.exists(object_path):
                await self._verify_object(manifest, path, object_path)
                existing.append(path)
                continue
            outcome = await self.store.put(object_path, content, content_type=self._content_type)
            if outcome == "exists":
                # Raced with another publisher: success only once the bytes compare equal.
                await self._verify_object(manifest, path, object_path)
                existing.append(path)
            else:
                uploaded.append(path)
        return UploadReport(
            manifest_digest=manifest.digest,
            object_prefix=manifest.object_prefix,
            uploaded=tuple(uploaded),
            verified_existing=tuple(existing),
        )

    async def prepare(self, manifest: CapabilityBundleManifest) -> PublishPlan:
        """Signed-URL handshake: one signed upload URL per object not yet stored."""
        uploads: list[SignedUpload] = []
        present: list[str] = []
        for entry in manifest.files:
            object_path = manifest.object_path(entry.path)
            if await self.store.exists(object_path):
                present.append(entry.path)
            else:
                uploads.append(await self.store.signed_upload_url(object_path))
        return PublishPlan(
            manifest=manifest,
            manifest_digest=manifest.digest,
            object_prefix=manifest.object_prefix,
            uploads=tuple(uploads),
            already_present=tuple(present),
        )

    async def complete(
        self, manifest: CapabilityBundleManifest, definition_fields: Mapping[str, Any]
    ) -> PublishResult:
        """Registration step: every object verified, then a ``proposed`` catalog row."""
        if self.registry is None:
            raise RuntimeError("bundle registration is not configured")
        existing = await self.registry.find(manifest)
        if existing is not None:
            return PublishResult(
                pin=existing,
                status="existing",
                manifest_digest=manifest.digest,
                object_prefix=manifest.object_prefix,
            )
        stored: list[tuple[str, bytes]] = []
        for entry in manifest.files:
            object_path = manifest.object_path(entry.path)
            if not await self.store.exists(object_path):
                raise CapabilityDrift(f"bundle object is missing: {entry.path}")
            stored.append((entry.path, await self.store.get(object_path)))
        files = verify_bundle_files(manifest, stored)
        definition = bundle_definition(manifest, files, definition_fields)
        pin = await self.registry.register_proposed(manifest, definition, self.actor_ref)
        return PublishResult(
            pin=pin,
            status="proposed",
            manifest_digest=manifest.digest,
            object_prefix=manifest.object_prefix,
        )

    async def publish(
        self,
        manifest: CapabilityBundleManifest,
        files: Sequence[tuple[str, bytes]],
        definition_fields: Mapping[str, Any],
    ) -> PublishResult:
        await self.upload(manifest, files)
        return await self.complete(manifest, definition_fields)

    async def signed_urls(
        self, manifest: CapabilityBundleManifest, ttl_seconds: int | None = None
    ) -> dict[str, str]:
        """One short-lived download URL per manifest file, keyed by relative path."""
        ttl = ttl_seconds or self.signed_ttl_seconds
        paths = [manifest.object_path(entry.path) for entry in manifest.files]
        signed = await self.store.signed_download_urls(paths, ttl)
        missing = [path for path in paths if path not in signed]
        if missing:
            raise CapabilityDrift(f"no signed URL for {len(missing)} bundle object(s)")
        return {entry.path: signed[manifest.object_path(entry.path)] for entry in manifest.files}

    async def _verify_object(
        self, manifest: CapabilityBundleManifest, relative: str, object_path: str
    ) -> None:
        entry = manifest.entry(relative)
        content = await self.store.get(object_path)
        if len(content) != entry.size_bytes or "sha256:" + sha256_hex(content) != entry.sha256:
            raise BundleCustodyConflict(
                f"existing object {object_path} differs from the pinned bytes; never overwritten"
            )


async def download_bundle(
    manifest: CapabilityBundleManifest,
    urls: Mapping[str, str],
    fetcher: BundleFetcher,
) -> tuple[tuple[str, bytes], ...]:
    """Materialization step: fetch every file through its signed URL and verify it."""
    files: list[tuple[str, bytes]] = []
    for entry in manifest.files:
        url = urls.get(entry.path)
        if url is None:
            raise CapabilityDrift(f"no signed URL for {entry.path}")
        files.append((entry.path, await fetcher.fetch(url)))
    return verify_bundle_files(manifest, files)


_BUNDLE_KINDS: dict[str, BundleKind] = {
    "skill_bundle": "skill_bundle",
    "hook_script": "hook_script",
    "subagent_profile": "subagent_profile",
}


def definition_object_prefix(definition: SkillDefinition | HookScriptDefinition) -> str:
    """The digest path prefix a published bundle definition names (``bundle_ref.uri``)."""

    bundle_ref = definition.bundle_ref
    if bundle_ref is None or not bundle_ref.uri.startswith(f"{BUNDLE_BUCKET}://"):
        raise CapabilityDrift(f"{definition.logical_id} has no capability-bundles reference")
    return bundle_ref.uri.removeprefix(f"{BUNDLE_BUCKET}://")


async def fetch_definition_bundle(
    definition: SkillDefinition | HookScriptDefinition,
    store: BundleObjectStore,
    fetcher: BundleFetcher,
    *,
    ttl_seconds: int = SIGNED_DOWNLOAD_TTL_SECONDS,
) -> tuple[CapabilityBundleManifest, tuple[tuple[str, bytes], ...]]:
    """Materialization of a catalog definition's bytes (FT-A7 consumer of FT-A2 custody).

    The object prefix (``<application>/<kind>/<id>/<version>/<manifest sha256>``) and the
    definition's file manifest drive one short-lived signed URL per file; every file is
    verified by size and sha256, the manifest is rebuilt from the bytes and must reproduce
    the definition's ``manifest_digest`` and prefix. Any difference is ``CAPABILITY_DRIFT``.
    """

    prefix = definition_object_prefix(definition)
    parts = prefix.split("/")
    if len(parts) != 5 or parts[1] not in _BUNDLE_KINDS:
        raise CapabilityDrift(f"malformed capability bundle prefix: {prefix}")
    application_id, kind, capability_id, version, digest_hex = parts
    entries = {normalize_bundle_path(entry.path): entry for entry in definition.file_manifest}
    paths = [f"{prefix}/{path}" for path in sorted(entries)]
    urls = await store.signed_download_urls(paths, ttl_seconds)
    files: list[tuple[str, bytes]] = []
    for path in sorted(entries):
        url = urls.get(f"{prefix}/{path}")
        if url is None:
            raise CapabilityDrift(f"no signed URL for {path}")
        content = await fetcher.fetch(url)
        entry = entries[path]
        if len(content) != entry.size_bytes or "sha256:" + sha256_hex(content) != entry.digest:
            raise CapabilityDrift(f"bundle file drifted from its pin: {path}")
        files.append((path, content))
    manifest = build_bundle_manifest(
        files,
        application_id=application_id,
        kind=_BUNDLE_KINDS[kind],
        capability_id=capability_id,
        version=version,
        executable=frozenset(path for path, entry in entries.items() if entry.executable),
    )
    if manifest.digest != definition.manifest_digest or manifest.object_prefix != prefix:
        raise CapabilityDrift(
            f"{definition.logical_id} bytes do not reproduce manifest {digest_hex[:12]}"
        )
    return manifest, verify_bundle_files(manifest, files)


def bundle_definition(
    manifest: CapabilityBundleManifest,
    files: Sequence[tuple[str, bytes]],
    fields: Mapping[str, Any],
) -> Definition:
    """The catalog Definition a published bundle registers as (kind-specific fields merged)."""
    file_manifest = [
        {"path": entry.path, "digest": entry.sha256, "size_bytes": entry.size_bytes}
        | ({"executable": True} if entry.executable else {})
        for entry in manifest.files
    ]
    bundle_ref = {
        "uri": f"{BUNDLE_BUCKET}://{manifest.object_prefix}",
        "digest": manifest.digest,
        "media_type": "application/vnd.mc.capability-bundle+json",
        "size_bytes": manifest.total_bytes,
    }
    provenance = {
        "source": "local",
        "locator": f"{BUNDLE_BUCKET}://{manifest.object_prefix}",
        "upstream_identity": manifest.capability_id,
        "upstream_version": manifest.version,
    }
    content = dict(files)
    extra = dict(fields)
    if manifest.kind == "skill_bundle":
        frontmatter = parse_skill_frontmatter(content["SKILL.md"])
        name = frontmatter.get("name") or manifest.capability_id.rsplit(".", 1)[-1]
        description = frontmatter.get("description") or extra.pop("description", None)
        if not description:
            raise ValueError("SKILL.md frontmatter needs a description")
        values: dict[str, Any] = {
            "logical_id": manifest.capability_id,
            "title": extra.pop("title", name),
            "description": description[:1024],
            "skill_name": name,
            "frontmatter": frontmatter,
            "body_summary": description[:1024],
            "bundle_ref": bundle_ref,
            "manifest_digest": manifest.digest,
            "file_manifest": file_manifest,
            "source_provenance": provenance,
            "host_support": extra.pop("host_support", all_profiles().model_dump(mode="json")),
        }
        values.update(extra)
        return SkillDefinition.model_validate(values)
    if manifest.kind == "hook_script":
        values = {
            "logical_id": manifest.capability_id,
            "title": extra.pop("title", manifest.capability_id),
            "description": extra.pop("description", f"Hook script {manifest.capability_id}"),
            "file_manifest": file_manifest,
            "manifest_digest": manifest.digest,
            "bundle_ref": bundle_ref,
            "source_provenance": provenance,
        }
        values.update(extra)
        return HookScriptDefinition.model_validate(values)
    prompt_path = str(extra.pop("prompt_path", "prompt.md"))
    if prompt_path not in content:
        raise ValueError(f"subagent prompt file {prompt_path} is not in the bundle")
    profile = dict(extra.pop("profile", {}))
    profile.setdefault(
        "prompt_bundle_ref", f"{BUNDLE_BUCKET}://{manifest.object_path(prompt_path)}"
    )
    values = {
        "logical_id": manifest.capability_id,
        "title": extra.pop("title", manifest.capability_id),
        "description": extra.pop("description", profile.get("description", manifest.capability_id)),
        "profile": profile,
    }
    values.update(extra)
    return SubagentProfileDefinition.model_validate(values)


class PublishPrepareRequest(BaseModel):
    """Body of ``POST /catalog/publish:prepare`` (and ``publish:complete``)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest: CapabilityBundleManifest
    definition: dict[str, Any] = Field(default_factory=dict)
