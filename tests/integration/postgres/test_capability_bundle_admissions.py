"""Real disposable SQL plus the actual storage3 SDK over an offline HTTP transport."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import asyncpg
import httpx
import pytest
from storage3 import SyncStorageClient

from mission_control.adapters.capabilities.capability_bundles import BundleError, bytes_digest
from mission_control.adapters.capabilities.capability_pins import CapabilityPins, PinnedSkill
from mission_control.adapters.postgres.capability_bundles import PostgresCapabilityBundleAdmissions
from mission_control.adapters.supabase_storage.bundles import SupabaseCapabilityBundleStore
from tests.integration.postgres.test_immutable_runtime_documents import (
    document_pool as document_pool,
)
from tests.unit.integrations.test_capability_directory_bundles import source


@pytest.fixture
def staged_bundle(tmp_path):
    objects = {}
    requests = []
    namespace = "fixture-" + uuid4().hex

    def handle(request):
        requests.append(request)
        path = request.url.path
        assert "/capability-bundles/" + namespace + "/" in path
        if request.method == "POST":
            assert request.headers["x-upsert"] == "false"
            if path in objects:
                return httpx.Response(
                    409, json={"statusCode": "409", "error": "Duplicate", "message": "duplicate"}
                )
            objects[path] = request.read().split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n--", 1)[0]
            return httpx.Response(200, json={"Key": path})
        assert request.method == "GET"
        return httpx.Response(200, content=objects[path])

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        sdk = SyncStorageClient("https://offline.invalid/storage/v1/", {}, http_client=http)
        reader = SupabaseCapabilityBundleStore(sdk, namespace=namespace)
        directory = source(tmp_path)
        manifest = reader.stage(directory, asset_id="skill.fixture", version=1)
        pin = PinnedSkill.model_validate(
            {
                "ref": {
                    "kind": "skill",
                    "logical_id": "skill.fixture",
                    "revision": 1,
                    "digest": "sha256:" + "a" * 64,
                },
                "skill_name": "fixture",
                "source_locator": "capability-bundles://" + manifest.digest,
                "bundle_digest": manifest.bundle_digest,
                "digest_format": "bytes_v1",
                "skill_md_digest": bytes_digest((directory / "SKILL.md").read_bytes()),
                "mount_root": "/skills/fixture",
            }
        )
        yield reader, pin, directory, objects, requests


@pytest.mark.asyncio
async def test_concurrent_immutable_registration_and_worker_resolution(
    document_pool, staged_bundle
):
    reader, pin, _, _, _ = staged_bundle
    scope = str(uuid4())
    admissions = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=scope, storage_namespace=reader.storage_namespace
    )
    results = await asyncio.gather(*(admissions.register(pin, reader=reader) for _ in range(6)))
    assert all(result == results[0] for result in results)
    admitted = await admissions.resolve_pins(CapabilityPins(skills=(pin,)), reader=reader)
    bundle = pin.bundle(store=admitted)
    assert dict(bundle.files)["assets/image.bin"] == b"\x00\xff\x01"
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    from mission_control.adapters.temporal.deployment_composition import (
        build_deployment_capability_registry,
    )
    from mission_control.bootstrap.settings import get_settings

    settings = get_settings().model_copy(update={"capability_bundle_backend": "supabase"})
    registry = build_deployment_capability_registry(
        settings,
        CapabilityPins(skills=(pin,)),
        saver=InMemorySaver(),
        store=InMemoryStore(),
        bundle_reader=admitted,
    )
    assert registry.registry.skill_bundles[pin.bundle_digest] == bundle
    conflict = pin.model_copy(
        update={"ref": pin.ref.model_copy(update={"digest": "sha256:" + "b" * 64})}
    )
    with pytest.raises(BundleError, match="conflicts"):
        await admissions.register(conflict, reader=reader)
    assert (await admissions.resolve(pin, reader=reader))[0] == results[0]


@pytest.mark.asyncio
async def test_unregistered_and_cross_scope_do_not_fetch_cloud(document_pool, staged_bundle):
    reader, pin, _, _, requests = staged_bundle
    admissions = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=str(uuid4()), storage_namespace=reader.storage_namespace
    )
    before = len(requests)
    with pytest.raises(BundleError, match="not admitted"):
        await admissions.resolve(pin, reader=reader)
    assert len(requests) == before
    await admissions.register(pin, reader=reader)
    other = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=str(uuid4()), storage_namespace=reader.storage_namespace
    )
    before = len(requests)
    with pytest.raises(BundleError, match="not admitted"):
        await other.resolve(pin, reader=reader)
    assert len(requests) == before


@pytest.mark.asyncio
async def test_changed_bytes_leave_orphans_not_rebound_version(document_pool, staged_bundle):
    reader, pin, directory, objects, _ = staged_bundle
    admissions = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=str(uuid4()), storage_namespace=reader.storage_namespace
    )
    first = await admissions.register(pin, reader=reader)
    (directory / "assets" / "image.bin").write_bytes(b"new bytes")
    staged = reader.stage(directory, asset_id=pin.ref.logical_id, version=pin.ref.revision)
    changed = pin.model_copy(
        update={
            "bundle_digest": staged.bundle_digest,
            "source_locator": "capability-bundles://" + staged.digest,
        }
    )
    with pytest.raises(BundleError, match="conflicts"):
        await admissions.register(changed, reader=reader)
    assert (await admissions.resolve(pin, reader=reader))[0] == first
    assert any(staged.digest[7:] in path for path in objects)  # Unreferenced, retained bytes.


@pytest.mark.asyncio
async def test_concurrent_different_manifests_have_exactly_one_winner(document_pool, staged_bundle):
    reader, first, directory, _, _ = staged_bundle
    admissions = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=str(uuid4()), storage_namespace=reader.storage_namespace
    )
    (directory / "assets" / "image.bin").write_bytes(b"different captured bytes")
    staged = reader.stage(directory, asset_id=first.ref.logical_id, version=first.ref.revision)
    second = first.model_copy(
        update={
            "source_locator": "capability-bundles://" + staged.digest,
            "bundle_digest": staged.bundle_digest,
        }
    )
    outcomes = await asyncio.gather(
        admissions.register(first, reader=reader),
        admissions.register(second, reader=reader),
        return_exceptions=True,
    )
    assert sum(isinstance(outcome, BundleError) for outcome in outcomes) == 1
    winner = first if not isinstance(outcomes[0], BaseException) else second
    assert (await admissions.resolve(winner, reader=reader))[0] == next(
        outcome for outcome in outcomes if not isinstance(outcome, BaseException)
    )


@pytest.mark.asyncio
async def test_corrupt_remote_bytes_never_register(document_pool, staged_bundle):
    reader, pin, _, objects, _ = staged_bundle
    scope = str(uuid4())
    admissions = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=scope, storage_namespace=reader.storage_namespace
    )
    key = next(key for key in objects if "/objects/" in key)
    objects[key] = b"tampered"
    with pytest.raises(BundleError, match="digest/size"):
        await admissions.register(pin, reader=reader)
    async with document_pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('belllabs.catalog_scope',$1,true)", scope)
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM belllabs_control.capability_bundle_admissions"
            )
            == 0
        )


@pytest.mark.asyncio
async def test_scoped_rls_and_append_only_grants(document_pool, staged_bundle):
    reader, pin, _, _, _ = staged_bundle
    scope = str(uuid4())
    admissions = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=scope, storage_namespace=reader.storage_namespace
    )
    await admissions.register(pin, reader=reader)
    async with document_pool.acquire() as connection, connection.transaction():
        await connection.execute(
            "SELECT set_config('belllabs.catalog_scope',$1,true)", str(uuid4())
        )
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM belllabs_control.capability_bundle_admissions"
            )
            == 0
        )
    for verb in ("DELETE FROM", "UPDATE"):
        async with document_pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('belllabs.catalog_scope',$1,true)", scope)
            sql = verb + " belllabs_control.capability_bundle_admissions"
            if verb == "UPDATE":
                sql += " SET version=2"
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await connection.execute(sql)


@pytest.mark.asyncio
async def test_mismatched_namespace_and_definition_kind_fail_closed(document_pool, staged_bundle):
    reader, pin, _, _, requests = staged_bundle
    admissions = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=str(uuid4()), storage_namespace="other-installation"
    )
    before = len(requests)
    with pytest.raises(BundleError, match="namespace"):
        await admissions.register(pin, reader=reader)
    assert len(requests) == before
    admissions = PostgresCapabilityBundleAdmissions(
        document_pool, catalog_scope=str(uuid4()), storage_namespace=reader.storage_namespace
    )
    from mission_control.domain.authoring.contracts import DefinitionKind

    wrong = pin.model_copy(update={"ref": pin.ref.model_copy(update={"kind": DefinitionKind.TOOL})})
    with pytest.raises(BundleError, match="exact bytes_v1 skill pin"):
        await admissions.register(wrong, reader=reader)
    assert len(requests) == before
