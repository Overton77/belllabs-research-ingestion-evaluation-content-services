"""Offline tests using the installed Supabase SDK against an HTTP transport fixture."""

from pathlib import Path

import httpx
import pytest
from storage3 import SyncStorageClient
from storage3.exceptions import StorageApiError

from mission_control.adapters.capabilities.capability_bundles import BundleError
from mission_control.adapters.supabase_storage.bundles import SupabaseCapabilityBundleStore
from tests.unit.integrations.test_capability_directory_bundles import source


def test_sdk_stages_without_upsert_and_verifies_every_download(tmp_path: Path) -> None:
    objects: dict[str, bytes] = {}
    requests: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        assert path.startswith("/storage/v1/object/capability-bundles/app-fixture/")
        if request.method == "POST":
            assert request.headers["x-upsert"] == "false"
            if path in objects:
                return httpx.Response(
                    409,
                    json={"statusCode": "409", "error": "Duplicate", "message": "already exists"},
                )
            # SDK uses multipart; retain exactly the file field, excluding boundaries.
            body = request.read()
            content = body.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n--", 1)[0]
            objects[path] = content
            return httpx.Response(200, json={"Key": path})
        assert request.method == "GET"
        return httpx.Response(200, content=objects[path])

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        client = SyncStorageClient("https://storage.example/storage/v1/", {}, http_client=http)
        store = SupabaseCapabilityBundleStore(client, namespace="app-fixture")
        directory = source(tmp_path)
        manifest = store.stage(directory, asset_id="fixture", version=1)
        assert store.stage(directory, asset_id="fixture", version=1) == manifest
        assert store.load(manifest.digest)[0] == manifest
        key = next(path for path in objects if "/objects/" in path)
        objects[key] = b"corrupted"
        with pytest.raises(BundleError, match="digest/size"):
            store.load(manifest.digest)
        assert {request.method for request in requests} == {"GET", "POST"}


def test_permission_failure_does_not_retry_as_overwrite(tmp_path: Path) -> None:
    calls = 0

    def handle(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            403, json={"statusCode": "403", "error": "Unauthorized", "message": "denied"}
        )

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        client = SyncStorageClient("https://storage.example/storage/v1/", {}, http_client=http)
        store = SupabaseCapabilityBundleStore(client, namespace="app-fixture")
        with pytest.raises(StorageApiError):
            store.stage(source(tmp_path), asset_id="fixture", version=1)
        assert calls == 1


@pytest.mark.parametrize(
    "namespace", ["../other", "%2e%2e/other", "x?query", "x#fragment", "x%2fy"]
)
def test_storage_namespace_cannot_change_sdk_url_interpretation(namespace: str) -> None:
    with httpx.Client(
        transport=httpx.MockTransport(lambda request: pytest.fail("unexpected I/O"))
    ) as http:
        client = SyncStorageClient("https://storage.example/storage/v1/", {}, http_client=http)
        with pytest.raises(BundleError, match="namespace"):
            SupabaseCapabilityBundleStore(client, namespace=namespace)


def test_remote_registry_requires_admitted_reader() -> None:
    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    from mission_control.adapters.capabilities.capability_pins import CapabilityPins
    from mission_control.adapters.temporal.deployment_composition import (
        build_deployment_capability_registry,
    )
    from mission_control.bootstrap.settings import get_settings

    settings = get_settings().model_copy(update={"capability_bundle_backend": "supabase"})
    with pytest.raises(ValueError, match="PostgreSQL-admitted"):
        build_deployment_capability_registry(
            settings, CapabilityPins(), saver=InMemorySaver(), store=InMemoryStore()
        )


def test_configured_reader_uses_deployment_auth_and_refuses_redirects() -> None:
    from pydantic import SecretStr

    from mission_control.adapters.supabase_storage.bundles import configured_supabase_bundle_reader
    from mission_control.bootstrap.settings import get_settings

    settings = get_settings().model_copy(
        update={
            "supabase_url": "https://storage.example",
            "supabase_secret_key": SecretStr("fixture-token"),
            "capability_bundle_namespace": "app-fixture",
        }
    )

    def handle(request):
        assert request.headers["Authorization"] == "Bearer fixture-token"
        assert request.headers["apikey"] == "fixture-token"
        return httpx.Response(
            403, json={"statusCode": "403", "error": "Unauthorized", "message": "fixture denial"}
        )

    with httpx.Client(transport=httpx.MockTransport(handle), follow_redirects=False) as http:
        reader = configured_supabase_bundle_reader(settings, http_client=http)
        with pytest.raises(StorageApiError):
            reader.load("sha256:" + "a" * 64)
    with httpx.Client(follow_redirects=True) as http:
        with pytest.raises(BundleError, match="redirects"):
            configured_supabase_bundle_reader(settings, http_client=http)
