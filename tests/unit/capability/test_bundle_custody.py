"""FT-A2: bundle custody — digest paths, no overwrite, resumable steps, signed download."""

from __future__ import annotations

import json
from argparse import Namespace
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from storage3 import SyncStorageClient

from mission_control.adapters.capabilities.capability_bundles import BundleError
from mission_control.adapters.supabase_storage.bundles import (
    SupabaseBundleObjectStore,
    SupabaseCapabilityBundleStore,
    SupabaseTusUploader,
)
from mission_control.application.authoring.control_plane_repository import (
    InMemoryDefinitionRepository,
)
from mission_control.application.capabilities.bundle_custody import (
    BundleCustodyConflict,
    BundleCustodyService,
    SignedUpload,
    bundle_definition,
    download_bundle,
)
from mission_control.domain.authoring.contracts import (
    Definition,
    HookScriptDefinition,
    SkillDefinition,
)
from mission_control.domain.capabilities.bundles import (
    RESUMABLE_CHUNK_BYTES,
    CapabilityBundleManifest,
    CapabilityDrift,
    build_bundle_manifest,
    computed_hash,
)
from mission_control.domain.capabilities.catalog_entry import capability_pin
from mission_control.interfaces.cli.main import MissionClient, publish_bundle
from tests.fixtures.catalog_http import catalog_harness

ROOT = Path(__file__).resolve().parents[3]
SKILL_FILES = (
    ("SKILL.md", b"---\nname: agent-browser\ndescription: Drive a real browser.\n---\n\nBody.\n"),
    ("scripts/open.sh", b'#!/bin/sh\nagent-browser open "$1"\n'),
)


def _manifest(files: Sequence[tuple[str, bytes]] = SKILL_FILES, **values: Any) -> Any:
    defaults: dict[str, Any] = {
        "application_id": "biotech",
        "kind": "skill_bundle",
        "capability_id": "skill.agent-browser",
        "version": "0.38.2",
    }
    defaults.update(values)
    return build_bundle_manifest(files, **defaults)


@dataclass
class FakeStore:
    """In-memory bucket honouring no-overwrite semantics; counts every write."""

    objects: dict[str, bytes] = field(default_factory=dict)
    puts: list[str] = field(default_factory=list)
    signed: list[tuple[tuple[str, ...], int]] = field(default_factory=list)

    async def exists(self, path: str) -> bool:
        return path in self.objects

    async def put(self, path: str, content: bytes, *, content_type: str) -> Any:
        self.puts.append(path)
        if path in self.objects:
            return "exists"
        self.objects[path] = content
        return "created"

    async def get(self, path: str) -> bytes:
        return self.objects[path]

    async def signed_download_urls(
        self, paths: Sequence[str], ttl_seconds: int
    ) -> Mapping[str, str]:
        self.signed.append((tuple(paths), ttl_seconds))
        return {path: f"https://storage.test/sign/{path}?token=t" for path in paths}

    async def signed_upload_url(self, path: str) -> SignedUpload:
        return SignedUpload(
            path=path, url=f"https://storage.test/upload/sign/{path}?token=u", token="u"
        )


@dataclass
class FakeRegistry:
    definitions: InMemoryDefinitionRepository = field(default_factory=InMemoryDefinitionRepository)
    rows: dict[str, str] = field(default_factory=dict)

    async def find(self, manifest: CapabilityBundleManifest) -> str | None:
        return self.rows.get(manifest.digest)

    async def register_proposed(
        self, manifest: CapabilityBundleManifest, definition: Definition, actor_ref: str
    ) -> str:
        from datetime import UTC, datetime

        published = await self.definitions.publish(definition, actor_ref, datetime.now(UTC), 0)
        pin = capability_pin(published).render()
        self.rows[manifest.digest] = pin
        return pin


@dataclass
class StoreFetcher:
    store: FakeStore

    async def fetch(self, url: str) -> bytes:
        path = urlsplit(url).path.removeprefix("/sign/")
        return self.store.objects[path]


# --- manifest and hash ------------------------------------------------------------------------


def test_digest_paths_and_canonical_manifest_digest() -> None:
    manifest = _manifest()
    digest = manifest.digest.removeprefix("sha256:")
    assert manifest.object_prefix == f"biotech/skill_bundle/skill.agent-browser/0.38.2/{digest}"
    assert manifest.object_path("scripts/open.sh").endswith(f"{digest}/scripts/open.sh")
    reordered = build_bundle_manifest(
        tuple(reversed(SKILL_FILES)),
        application_id="biotech",
        kind="skill_bundle",
        capability_id="skill.agent-browser",
        version="0.38.2",
    )
    assert reordered.digest == manifest.digest
    assert json.loads(manifest.canonical_json()) == manifest.model_dump(mode="json")


def test_manifest_rejects_unsafe_and_incomplete_bundles() -> None:
    with pytest.raises(CapabilityDrift):
        _manifest((("../escape.md", b"x"), ("SKILL.md", b"x")))
    with pytest.raises(ValueError, match="SKILL.md"):
        _manifest((("README.md", b"x"),))
    with pytest.raises(ValueError, match="unique"):
        _manifest((("SKILL.md", b"x"), ("skill.md", b"y")))
    hook = _manifest((("policy.py", b"print()"),), kind="hook_script", capability_id="hook.p")
    assert hook.kind == "hook_script"


@pytest.mark.parametrize("skill", ["ask-matt", "deep-agents-core", "diagnosing-bugs", "swarm"])
def test_computed_hash_matches_skills_lock(skill: str) -> None:
    lock = json.loads((ROOT / "skills-lock.json").read_text(encoding="utf-8"))
    directory = ROOT / ".agents" / "skills" / skill
    files = [
        (path.relative_to(directory).as_posix(), path.read_bytes())
        for path in directory.rglob("*")
        if path.is_file()
    ]
    assert computed_hash(files) == lock["skills"][skill]["computedHash"]


# --- custody service ------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publish_is_idempotent_and_never_overwrites() -> None:
    store, registry = FakeStore(), FakeRegistry()
    service = BundleCustodyService(store=store, registry=registry)
    manifest = _manifest()
    first = await service.publish(manifest, SKILL_FILES, {})
    assert first.status == "proposed"
    assert first.pin.startswith("skill.agent-browser@0.38.2#sha256:")
    assert set(store.objects) == {manifest.object_path(path) for path, _ in SKILL_FILES}
    puts = len(store.puts)
    second = await service.publish(manifest, SKILL_FILES, {})
    assert second.status == "existing" and second.pin == first.pin
    assert len(store.puts) == puts  # nothing re-uploaded
    store.objects[manifest.object_path("SKILL.md")] = b"tampered"
    with pytest.raises(BundleCustodyConflict):
        await service.upload(manifest, SKILL_FILES)


@pytest.mark.asyncio
async def test_crash_between_upload_and_registration_resumes_without_reupload() -> None:
    store, registry = FakeStore(), FakeRegistry()
    manifest = _manifest()
    report = await BundleCustodyService(store=store).upload(manifest, SKILL_FILES)
    assert set(report.uploaded) == {"SKILL.md", "scripts/open.sh"}
    puts = len(store.puts)
    # "process restarts": a new service finishes the publication
    result = await BundleCustodyService(store=store, registry=registry).publish(
        manifest, SKILL_FILES, {}
    )
    assert result.status == "proposed"
    assert len(store.puts) == puts
    assert list(registry.rows) == [manifest.digest]


@pytest.mark.asyncio
async def test_signed_download_verifies_and_refuses_tampering() -> None:
    store, registry = FakeStore(), FakeRegistry()
    service = BundleCustodyService(store=store, registry=registry)
    manifest = _manifest()
    await service.publish(manifest, SKILL_FILES, {})
    urls = await service.signed_urls(manifest)
    assert store.signed[-1][1] == 300
    files = await download_bundle(manifest, urls, StoreFetcher(store))
    assert dict(files) == dict(SKILL_FILES)
    store.objects[manifest.object_path("scripts/open.sh")] = b"#!/bin/sh\nrm -rf /\n"
    with pytest.raises(CapabilityDrift) as raised:
        await download_bundle(manifest, urls, StoreFetcher(store))
    assert raised.value.code == "CAPABILITY_DRIFT"
    with pytest.raises(CapabilityDrift):
        await service.complete(manifest.model_copy(update={"version": "9"}), {})


def test_bundle_definitions_for_skill_and_hook() -> None:
    manifest = _manifest()
    skill = bundle_definition(manifest, SKILL_FILES, {})
    assert isinstance(skill, SkillDefinition)
    assert skill.skill_name == "agent-browser" and skill.manifest_digest == manifest.digest
    assert skill.bundle_ref.uri == f"capability-bundles://{manifest.object_prefix}"
    files = (("policy.py", b"import json\n"),)
    hook_manifest = _manifest(files, kind="hook_script", capability_id="hook.mc-policy-template")
    hook = bundle_definition(
        hook_manifest,
        files,
        {"events": ["before_shell"], "entrypoint": "policy.py", "interpreter": "python"},
    )
    assert isinstance(hook, HookScriptDefinition) and hook.entrypoint == "policy.py"


# --- HTTP handshake and CLI ---------------------------------------------------------------


def _publishing_harness() -> tuple[Any, FakeStore, FakeRegistry]:
    store, registry = FakeStore(), FakeRegistry()
    harness = catalog_harness(
        InMemoryDefinitionRepository(),
        None,
        permissions=frozenset({"catalog:read", "catalog:publish"}),
    )
    harness.service = harness.service.__class__(
        **{
            **harness.service.__dict__,
            "custody": BundleCustodyService(store=store, registry=registry),
        }
    )
    services = harness.app.state.mission_control_catalog_services
    harness.app.state.mission_control_catalog_services = dict.fromkeys(services, harness.service)
    return harness, store, registry


def test_http_prepare_and_complete_handshake() -> None:
    harness, store, _registry = _publishing_harness()
    manifest = _manifest()
    body = {"manifest": manifest.model_dump(mode="json"), "definition": {}}
    plan = harness.client.post(harness.url("publish:prepare"), json=body)
    assert plan.status_code == 200, plan.text
    uploads = plan.json()["uploads"]
    assert {item["path"] for item in uploads} == {
        manifest.object_path(path) for path, _ in SKILL_FILES
    }
    incomplete = harness.client.post(harness.url("publish:complete"), json=body)
    assert incomplete.status_code == 409
    assert incomplete.json()["detail"]["code"] == "CAPABILITY_DRIFT"
    for path, content in SKILL_FILES:
        store.objects[manifest.object_path(path)] = content
    done = harness.client.post(harness.url("publish:complete"), json=body)
    assert done.status_code == 200, done.text
    assert done.json()["status"] == "proposed"
    assert done.json()["pin"].startswith("skill.agent-browser@0.38.2#")
    other_app = manifest.model_copy(update={"application_id": "ai-engineer"})
    denied = harness.client.post(
        harness.url("publish:prepare"),
        json={"manifest": other_app.model_dump(mode="json"), "definition": {}},
    )
    assert denied.status_code == 403


def test_publish_requires_permission() -> None:
    harness = catalog_harness(InMemoryDefinitionRepository(), None)
    body = {"manifest": _manifest().model_dump(mode="json"), "definition": {}}
    assert harness.client.post(harness.url("publish:prepare"), json=body).status_code == 403


def test_cli_publish_uploads_missing_objects_and_prints_the_pin(tmp_path: Path) -> None:
    harness, store, _registry = _publishing_harness()
    directory = tmp_path / "agent-browser"
    for path, content in SKILL_FILES:
        target = directory / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    (directory / ".git").mkdir()
    (directory / ".git" / "HEAD").write_bytes(b"ref: x")

    def storage_handler(request: httpx.Request) -> httpx.Response:
        assert "authorization" not in request.headers  # signed token only, never a bearer
        assert parse_qs(request.url.query.decode())["token"] == ["u"]
        assert request.headers["x-upsert"] == "false"
        object_path = request.url.path.removeprefix("/upload/sign/")
        payload = request.read()
        content = payload.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n--", 1)[0]
        store.objects[object_path] = content
        return httpx.Response(200, json={"Key": object_path})

    storage = httpx.Client(transport=httpx.MockTransport(storage_handler))
    args = Namespace(
        dir=str(directory),
        kind="skill_bundle",
        capability_id="skill.agent-browser",
        version="0.38.2",
    )
    result, status = publish_bundle(
        MissionClient(harness.client, "biotech"), storage, "biotech", args
    )
    assert status == 0, (result, sorted(store.objects))
    assert result["status"] == "proposed"
    # The CLI marks files with a shebang executable; that is part of the manifest.
    manifest = _manifest(executable=frozenset({"scripts/open.sh"}))
    assert manifest.entry("scripts/open.sh").executable
    assert store.objects[manifest.object_path("SKILL.md")] == SKILL_FILES[0][1]
    assert not any(".git" in path for path in store.objects)
    again, status = publish_bundle(
        MissionClient(harness.client, "biotech"), storage, "biotech", args
    )
    assert status == 0 and again["status"] == "existing" and again["pin"] == result["pin"]


# --- Supabase adapter (recorded storage API, no live bucket) --------------------------------


@dataclass
class StorageApi:
    """Records Storage API calls and answers like Supabase (no overwrite)."""

    objects: dict[str, bytes] = field(default_factory=dict)
    calls: list[tuple[str, str]] = field(default_factory=list)
    tus: dict[str, bytearray] = field(default_factory=dict)

    def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.calls.append((request.method, path))
        prefix = "/storage/v1/object/capability-bundles/"
        if request.method == "HEAD" and path.startswith(prefix):
            return httpx.Response(200 if path.removeprefix(prefix) in self.objects else 400)
        if request.method == "POST" and path.startswith(prefix):
            key = path.removeprefix(prefix)
            assert request.headers.get("x-upsert") == "false"
            if key in self.objects:
                return httpx.Response(
                    400,
                    json={
                        "statusCode": "409",
                        "error": "Duplicate",
                        "message": "The resource already exists",
                    },
                )
            payload = request.read()
            self.objects[key] = payload.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n--", 1)[0]
            return httpx.Response(200, json={"Key": key})
        if request.method == "GET" and "/object/" in path:
            key = path.rsplit("/capability-bundles/", 1)[1]
            return httpx.Response(200, content=self.objects[key])
        if request.method == "POST" and path == "/storage/v1/object/sign/capability-bundles":
            body = json.loads(request.read())
            assert body["expiresIn"] == "300"
            return httpx.Response(
                200,
                json=[
                    {
                        "path": item,
                        "signedURL": f"/object/sign/capability-bundles/{item}?token=x",
                        "error": None,
                    }
                    for item in body["paths"]
                ],
            )
        if path.endswith("/upload/resumable") and request.method == "POST":
            assert request.headers["x-upsert"] == "false"
            assert request.headers["tus-resumable"] == "1.0.0"
            location = "https://ref.storage.supabase.co/storage/v1/upload/resumable/abc"
            self.tus[location] = bytearray()
            return httpx.Response(201, headers={"location": location})
        if request.method == "PATCH":
            chunk = request.read()
            assert len(chunk) <= RESUMABLE_CHUNK_BYTES
            buffer = self.tus[str(request.url)]
            assert int(request.headers["upload-offset"]) == len(buffer)
            buffer.extend(chunk)
            return httpx.Response(204, headers={"upload-offset": str(len(buffer))})
        return httpx.Response(404)


def _supabase(api: StorageApi, credential: str = "publisher") -> SupabaseBundleObjectStore:
    http = httpx.Client(transport=httpx.MockTransport(api))
    client = SyncStorageClient(
        "https://ref.supabase.co/storage/v1/",
        {"Authorization": "Bearer publisher-jwt"},
        http_client=http,
    )
    tus = SupabaseTusUploader(
        http,
        endpoint="https://ref.storage.supabase.co/storage/v1/upload/resumable",
        headers={"Authorization": "Bearer publisher-jwt"},
    )
    return SupabaseBundleObjectStore(client, credential=credential, tus=tus)  # type: ignore[arg-type]


def test_supabase_upload_bundle_digest_paths_no_upsert_and_signed_urls() -> None:
    api = StorageApi()
    custody = _supabase(api)
    http = httpx.Client(transport=httpx.MockTransport(api))
    store = SupabaseCapabilityBundleStore(
        SyncStorageClient("https://ref.supabase.co/storage/v1/", {}, http_client=http),
        namespace="biotech",
        custody=custody,
    )
    manifest = _manifest()
    report = store.upload_bundle(manifest, SKILL_FILES)
    assert set(report.uploaded) == {"SKILL.md", "scripts/open.sh"}
    assert set(api.objects) == {manifest.object_path(path) for path, _ in SKILL_FILES}
    again = store.upload_bundle(manifest, SKILL_FILES)
    assert again.uploaded == () and set(again.verified_existing) == {"SKILL.md", "scripts/open.sh"}
    urls = store.signed_urls(manifest)
    assert set(urls) == {"SKILL.md", "scripts/open.sh"} and all(
        "token=" in url for url in urls.values()
    )
    api.objects[manifest.object_path("SKILL.md")] = b"different"
    with pytest.raises(BundleError, match="never overwritten"):
        store.upload_bundle(manifest, SKILL_FILES)


def test_supabase_large_objects_use_tus_chunks() -> None:
    api = StorageApi()
    custody = _supabase(api)
    big = bytes(range(256)) * ((RESUMABLE_CHUNK_BYTES * 2 + 1024) // 256)
    files = (("SKILL.md", b"---\nname: big\ndescription: d\n---\n"), ("data.bin", big))
    manifest = _manifest(files, capability_id="skill.big")
    outcome = custody.put_sync(
        manifest.object_path("data.bin"), big, content_type="application/octet-stream"
    )
    assert outcome == "created"
    assert bytes(next(iter(api.tus.values()))) == big
    assert sum(1 for method, _ in api.calls if method == "PATCH") == 3


def test_runtime_never_uses_the_service_key_and_readers_cannot_write() -> None:
    api = StorageApi()
    with pytest.raises(BundleError, match="service key"):
        _supabase(api, credential="service")
    reader = _supabase(api, credential="reader")
    with pytest.raises(BundleError, match="publisher"):
        reader.put_sync("biotech/x", b"x", content_type="text/plain")


def test_materialize_read_only_writes_verified_bytes(tmp_path: Path) -> None:
    import stat

    from mission_control.adapters.capabilities.capability_bundles import materialize_read_only

    manifest = _manifest(executable=frozenset({"scripts/open.sh"}))
    written = materialize_read_only(manifest, SKILL_FILES, tmp_path / "mount")
    assert {path.relative_to(tmp_path / "mount").as_posix() for path in written} == {
        "SKILL.md",
        "scripts/open.sh",
    }
    skill = tmp_path / "mount" / "SKILL.md"
    assert skill.read_bytes() == SKILL_FILES[0][1]
    assert not (skill.stat().st_mode & stat.S_IWUSR)
    with pytest.raises(CapabilityDrift):
        materialize_read_only(manifest, (("SKILL.md", b"x"), SKILL_FILES[1]), tmp_path / "other")
    for path in written:
        path.chmod(0o644)  # let pytest clean up the temporary directory
