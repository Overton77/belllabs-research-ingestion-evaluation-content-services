"""Supabase Storage byte custody, separate from catalog/version admission.

Uses the installed storage3 upload/download API. Bucket and credentials are provisioned
by deployment; this adapter never creates buckets, applies policies, upserts or deletes.
`stage` does NOT publish a catalog asset: PostgreSQL admission must bind asset/version
uniquely to the returned manifest digest. Unreferenced staged objects are harmless.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
from collections.abc import Sequence
from pathlib import Path
from typing import Literal
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
from mission_control.application.capabilities.bundle_custody import (
    PutOutcome,
    SignedUpload,
    UploadReport,
)
from mission_control.bootstrap.settings import IntegrationConfigurationError, Settings
from mission_control.domain.authoring.canonical import stable_json_dump
from mission_control.domain.capabilities.bundles import (
    BUNDLE_BUCKET,
    RESUMABLE_CHUNK_BYTES,
    RESUMABLE_THRESHOLD_BYTES,
    SIGNED_DOWNLOAD_TTL_SECONDS,
    CapabilityBundleManifest,
    verify_bundle_files,
)


class SupabaseCapabilityBundleStore:
    """App-scoped client and namespace are server configuration, never agent inputs."""

    def __init__(
        self,
        client: SyncStorageClient,
        *,
        namespace: str,
        custody: SupabaseBundleObjectStore | None = None,
    ) -> None:
        safe_storage_namespace(namespace)
        self._namespace = namespace
        self._bucket = client.from_("capability-bundles")
        self._custody = custody

    def _digest_custody(self) -> SupabaseBundleObjectStore:
        if self._custody is None:
            raise BundleError("digest-path custody needs a publisher or reader credential store")
        return self._custody

    def upload_bundle(
        self, manifest: CapabilityBundleManifest, files: Sequence[tuple[str, bytes]]
    ) -> UploadReport:
        """FT-A2: write each file once under its digest path (resumable above 6 MiB)."""
        return _upload_bundle(self._digest_custody(), manifest, files)

    def signed_urls(
        self, manifest: CapabilityBundleManifest, ttl: int = SIGNED_DOWNLOAD_TTL_SECONDS
    ) -> dict[str, str]:
        """FT-A2: one short-lived signed download URL per manifest file (by relative path)."""
        paths = {manifest.object_path(entry.path): entry.path for entry in manifest.files}
        signed = self._digest_custody().signed_download_urls_sync(list(paths), ttl)
        return {paths[path]: url for path, url in signed.items() if path in paths}

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


# ------------------------------------------------------------------------------------------
# FT-A2: digest-path custody (ADR-0024) — upload once, signed URLs, resumable uploads
# ------------------------------------------------------------------------------------------

CredentialRole = Literal["publisher", "reader", "service"]
_DUPLICATE_CODES = {"Duplicate", "ResourceAlreadyExists", "AssetAlreadyExists"}


def _is_duplicate(error: StorageApiError) -> bool:
    """Supabase answers an existing path with 409, or the older 400 Asset Already Exists."""
    message = str(getattr(error, "message", "")).lower()
    return (
        error.code in _DUPLICATE_CODES
        or str(error.status) == "409"
        or (str(error.status) == "400" and "already exists" in message)
    )


class SupabaseTusUploader:
    """Resumable (TUS 1.0.0) uploads in 6 MiB chunks to the direct storage hostname.

    ``endpoint`` is ``https://<project-ref>.storage.supabase.co/storage/v1/upload/resumable``.
    ``x-upsert`` is always ``false``: an existing object answers 409 and is reported as
    ``exists`` for the caller to verify, never overwritten.
    """

    def __init__(
        self,
        http_client: httpx.Client,
        *,
        endpoint: str,
        headers: dict[str, str],
        bucket: str = BUNDLE_BUCKET,
        chunk_bytes: int = RESUMABLE_CHUNK_BYTES,
    ) -> None:
        if chunk_bytes < 1:
            raise ValueError("TUS chunk size must be positive")
        self._http = http_client
        self._endpoint = endpoint
        self._headers = dict(headers)
        self._bucket = bucket
        self._chunk = chunk_bytes

    def upload(self, object_path: str, content: bytes, *, content_type: str) -> PutOutcome:
        metadata = ",".join(
            f"{key} {base64.b64encode(value.encode()).decode()}"
            for key, value in (
                ("bucketName", self._bucket),
                ("objectName", object_path),
                ("contentType", content_type),
                ("cacheControl", "3600"),
            )
        )
        created = self._http.post(
            self._endpoint,
            headers={
                **self._headers,
                "Tus-Resumable": "1.0.0",
                "Upload-Length": str(len(content)),
                "Upload-Metadata": metadata,
                "x-upsert": "false",
            },
        )
        if created.status_code == 409:
            return "exists"
        if created.status_code != 201 or "location" not in created.headers:
            raise BundleError(f"resumable upload could not start (HTTP {created.status_code})")
        location = created.headers["location"]
        offset = 0
        while offset < len(content):
            chunk = content[offset : offset + self._chunk]
            response = self._http.patch(
                location,
                content=chunk,
                headers={
                    **self._headers,
                    "Tus-Resumable": "1.0.0",
                    "Upload-Offset": str(offset),
                    "Content-Type": "application/offset+octet-stream",
                },
            )
            if response.status_code == 409:
                # Offset mismatch after an interrupted PATCH: resume from the server offset.
                head = self._http.head(
                    location, headers={**self._headers, "Tus-Resumable": "1.0.0"}
                )
                offset = int(head.headers.get("upload-offset", offset))
                continue
            if response.status_code not in {200, 204}:
                raise BundleError(f"resumable upload chunk failed (HTTP {response.status_code})")
            offset = int(response.headers.get("upload-offset", offset + len(chunk)))
        return "created"


class SupabaseBundleObjectStore:
    """``BundleObjectStore`` over Supabase Storage with a publisher or reader credential.

    The runtime never holds the service key for custody: publishing uses a publisher
    credential (INSERT + SELECT policies on the application prefix), workers use a reader
    credential (SELECT) to mint signed download URLs. Neither can UPDATE or DELETE, so a
    digest path can never be overwritten.
    """

    def __init__(
        self,
        client: SyncStorageClient,
        *,
        credential: CredentialRole,
        tus: SupabaseTusUploader | None = None,
        bucket: str = BUNDLE_BUCKET,
    ) -> None:
        if credential == "service":
            raise BundleError("capability bundle custody never uses the service key at runtime")
        self._credential = credential
        self._bucket = client.from_(bucket)
        self._tus = tus

    @property
    def credential(self) -> CredentialRole:
        return self._credential

    def _require_publisher(self) -> None:
        if self._credential != "publisher":
            raise BundleError("only the publisher credential may write capability bundles")

    def exists_sync(self, path: str) -> bool:
        return bool(self._bucket.exists(path))

    def put_sync(self, path: str, content: bytes, *, content_type: str) -> PutOutcome:
        self._require_publisher()
        if len(content) > RESUMABLE_THRESHOLD_BYTES:
            if self._tus is None:
                raise BundleError("objects above 6 MiB need the resumable (TUS) uploader")
            return self._tus.upload(path, content, content_type=content_type)
        try:
            self._bucket.upload(path, content, {"content-type": content_type, "upsert": "false"})
        except StorageApiError as error:
            if _is_duplicate(error):
                return "exists"
            raise
        return "created"

    def get_sync(self, path: str) -> bytes:
        return bytes(self._bucket.download(path))

    def signed_download_urls_sync(self, paths: Sequence[str], ttl_seconds: int) -> dict[str, str]:
        if not 1 <= ttl_seconds <= 3600:
            raise BundleError("signed download URLs must be short-lived")
        signed = self._bucket.create_signed_urls(list(paths), ttl_seconds)
        result: dict[str, str] = {}
        for item in signed:
            if item.get("error") or not item.get("signedURL") or item.get("path") is None:
                raise BundleError(f"could not sign {item.get('path')}")
            result[str(item["path"])] = str(item["signedURL"])
        return result

    def signed_upload_url_sync(self, path: str) -> SignedUpload:
        self._require_publisher()
        signed = self._bucket.create_signed_upload_url(path)
        return SignedUpload(path=path, url=str(signed["signed_url"]), token=str(signed["token"]))

    async def exists(self, path: str) -> bool:
        return await asyncio.to_thread(self.exists_sync, path)

    async def put(self, path: str, content: bytes, *, content_type: str) -> PutOutcome:
        return await asyncio.to_thread(self.put_sync, path, content, content_type=content_type)

    async def get(self, path: str) -> bytes:
        return await asyncio.to_thread(self.get_sync, path)

    async def signed_download_urls(self, paths: Sequence[str], ttl_seconds: int) -> dict[str, str]:
        return await asyncio.to_thread(self.signed_download_urls_sync, paths, ttl_seconds)

    async def signed_upload_url(self, path: str) -> SignedUpload:
        return await asyncio.to_thread(self.signed_upload_url_sync, path)


def _upload_bundle(
    store: SupabaseBundleObjectStore,
    manifest: CapabilityBundleManifest,
    files: Sequence[tuple[str, bytes]],
) -> UploadReport:
    uploaded: list[str] = []
    existing: list[str] = []
    for path, content in verify_bundle_files(manifest, files):
        object_path = manifest.object_path(path)
        outcome = (
            "exists"
            if store.exists_sync(object_path)
            else store.put_sync(object_path, content, content_type="application/octet-stream")
        )
        if outcome == "exists":
            stored = store.get_sync(object_path)
            if stored != content:
                raise BundleError(
                    f"existing object {object_path} differs from the pinned bytes; "
                    "never overwritten"
                )
            existing.append(path)
        else:
            uploaded.append(path)
    return UploadReport(
        manifest_digest=manifest.digest,
        object_prefix=manifest.object_prefix,
        uploaded=tuple(uploaded),
        verified_existing=tuple(existing),
    )


def configured_supabase_bundle_custody(
    settings: Settings,
    *,
    http_client: httpx.Client,
    credential: Literal["publisher", "reader"],
) -> SupabaseBundleObjectStore:
    """Deployment construction from a publisher or reader credential, never the service key."""
    token = (
        settings.capability_bundle_publisher_token
        if credential == "publisher"
        else settings.capability_bundle_reader_token
    )
    if not settings.supabase_url or token is None or not token.get_secret_value().strip():
        raise IntegrationConfigurationError(
            f"SUPABASE_URL and the capability bundle {credential} token are required"
        )
    if http_client.follow_redirects:
        raise BundleError("Supabase bundle transport must disable redirects")
    origin = urlsplit(settings.supabase_url)
    if not origin.hostname:
        raise BundleError("Supabase bundle origin must name a host")
    anon = settings.supabase_publishable_key
    headers = {"Authorization": "Bearer " + token.get_secret_value()}
    if anon is not None and anon.get_secret_value().strip():
        headers["apikey"] = anon.get_secret_value()
    client = SyncStorageClient(
        settings.supabase_url.rstrip("/") + "/storage/v1/", headers, http_client=http_client
    )
    tus = None
    if credential == "publisher":
        host = origin.hostname
        direct = (
            host.replace(".supabase.co", ".storage.supabase.co")
            if host.endswith(".supabase.co") and ".storage." not in host
            else host
        )
        port = f":{origin.port}" if origin.port else ""
        tus = SupabaseTusUploader(
            http_client,
            endpoint=f"{origin.scheme}://{direct}{port}/storage/v1/upload/resumable",
            headers=headers,
        )
    return SupabaseBundleObjectStore(client, credential=credential, tus=tus)
