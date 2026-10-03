"""Supabase Storage byte custody, separate from catalog/version admission.

Uses the installed storage3 upload/download API. Bucket and credentials are provisioned
by deployment; this adapter never creates buckets, applies policies, upserts or deletes.
`stage` does NOT publish a catalog asset: PostgreSQL admission must bind asset/version
uniquely to the returned manifest digest. Unreferenced staged objects are harmless.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from storage3 import SyncStorageClient
from storage3.exceptions import StorageApiError

from mission_control.adapters.capabilities.capability_bundles import (
    DIGEST,
    MAX_BUNDLE_BYTES,
    MAX_FILE_BYTES,
    BundleError,
    BundleManifest,
    bundle_digest,
    bytes_digest,
    directory_files,
    file_manifest,
    safe_relative_path,
    safe_storage_namespace,
)
from mission_control.bootstrap.settings import IntegrationConfigurationError, Settings
from mission_control.domain.authoring.canonical import stable_json_dump


class SupabaseCapabilityBundleStore:
    """App-scoped client and namespace are server configuration, never agent inputs."""

    def __init__(self, client: SyncStorageClient, *, namespace: str) -> None:
        safe_storage_namespace(namespace)
        self._namespace = namespace
        self._bucket = client.from_("capability-bundles")

    @property
    def storage_namespace(self) -> str:
        return self._namespace

    def _put(self, path: str, content: bytes, *, media_type: str) -> None:
        try:
            self._bucket.upload(path, content, {"content-type": media_type, "upsert": "false"})
        except StorageApiError as error:
            # A duplicate can be either modern 409 or the Storage API's older 400.
            # All other failures (including authorization) propagate unchanged.
            if (
                error.code not in {"Duplicate", "ResourceAlreadyExists"}
                and str(error.status) != "409"
            ):
                raise
        # Read-after-write also validates preexisting objects after an idempotent retry.
        if self._bucket.download(path) != content:
            raise BundleError("Supabase capability object differs from immutable bytes")

    def stage(self, directory: Path, *, asset_id: str, version: int) -> BundleManifest:
        files = directory_files(directory)
        manifest = BundleManifest(
            asset_id=asset_id,
            version=version,
            files=file_manifest(files),
            bundle_digest=bundle_digest(files),
        )
        for _, content in files:
            self._put(
                f"{self._namespace}/objects/{bytes_digest(content)[7:]}",
                content,
                media_type="application/octet-stream",
            )
        self._put(
            f"{self._namespace}/manifests/{manifest.digest[7:]}",
            json.dumps(stable_json_dump(manifest), sort_keys=True, separators=(",", ":")).encode(),
            media_type="application/json",
        )
        return manifest

    def load(self, digest: str) -> tuple[BundleManifest, tuple[tuple[str, bytes], ...]]:
        """Fetch exactly an admitted digest, never an alias or 'latest' version."""
        if not re.fullmatch(DIGEST, digest):
            raise BundleError("invalid manifest digest")
        raw = self._bucket.download(f"{self._namespace}/manifests/{digest[7:]}")
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
            content = self._bucket.download(f"{self._namespace}/objects/{entry.digest[7:]}")
            if len(content) != entry.size_bytes or bytes_digest(content) != entry.digest:
                raise BundleError("bundle object digest/size mismatch")
            files.append((entry.path, content))
        result = tuple(files)
        if (
            file_manifest(result) != manifest.files
            or bundle_digest(result) != manifest.bundle_digest
        ):
            raise BundleError("bundle file manifest mismatch")
        return manifest, result


def configured_supabase_bundle_reader(
    settings: Settings, *, http_client: httpx.Client
) -> SupabaseCapabilityBundleStore:
    """Deployment-only construction; no destination or credential from a mission."""
    if (
        not settings.supabase_url
        or not settings.supabase_url.strip()
        or settings.supabase_secret_key is None
        or not settings.supabase_secret_key.get_secret_value().strip()
    ):
        raise IntegrationConfigurationError(
            "SUPABASE_URL and SUPABASE_SECRET_KEY are required for Supabase capability bundles"
        )
    endpoint = urlsplit(settings.supabase_url)
    local = endpoint.scheme == "http" and endpoint.hostname in {"127.0.0.1", "localhost", "::1"}
    if (
        not endpoint.hostname
        or (endpoint.scheme != "https" and not local)
        or endpoint.username
        or endpoint.password
        or endpoint.query
        or endpoint.fragment
        or endpoint.path not in {"", "/"}
    ):
        raise BundleError(
            "Supabase bundle origin must be an HTTPS project origin or local test origin"
        )
    if http_client.follow_redirects:
        raise BundleError("Supabase bundle transport must disable redirects")
    if not settings.capability_bundle_namespace:
        raise BundleError("trusted capability storage namespace is required")
    secret = settings.supabase_secret_key.get_secret_value()
    client = SyncStorageClient(
        settings.supabase_url.rstrip("/") + "/storage/v1/",
        {"apikey": secret, "Authorization": "Bearer " + secret},
        http_client=http_client,
    )
    return SupabaseCapabilityBundleStore(client, namespace=settings.capability_bundle_namespace)
