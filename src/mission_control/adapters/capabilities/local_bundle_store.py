"""A filesystem ``BundleObjectStore`` with the custody semantics of the private bucket.

FT-A7: the storage used by ``scripts/seeds_publish_bundles.py --storage local:DIR`` and by
tests in place of Supabase Storage. Objects live at ``<root>/<bucket>/<object path>``; a
put never overwrites (``exists`` is returned instead, exactly like Storage without
``upsert``), signed URLs are ``file://`` URLs with an expiry query, and the fetcher refuses
any URL outside the store root or past its expiry. Nothing here is a production path: the
deployment store is ``adapters/supabase_storage/bundles.py`` (publisher or reader
credential, never the service key).
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from mission_control.adapters.capabilities.capability_bundles import (
    BundleError,
    reject_links,
    safe_relative_path,
)
from mission_control.application.capabilities.bundle_custody import PutOutcome, SignedUpload
from mission_control.domain.capabilities.bundles import BUNDLE_BUCKET


class FilesystemBundleObjectStore:
    def __init__(self, root: Path, *, bucket: str = BUNDLE_BUCKET) -> None:
        self._root = root.resolve() / bucket
        self._root.mkdir(parents=True, exist_ok=True)

    @property
    def root(self) -> Path:
        return self._root

    def path_for(self, object_path: str) -> Path:
        target = self._root / safe_relative_path(object_path)
        reject_links(target.parent if target.parent.exists() else self._root)
        if not target.resolve().is_relative_to(self._root):
            raise BundleError("object path escapes the bundle store")
        return target

    async def exists(self, path: str) -> bool:
        return self.path_for(path).is_file()

    async def put(self, path: str, content: bytes, *, content_type: str) -> PutOutcome:
        target = self.path_for(path)
        if target.exists():
            return "exists"
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with target.open("xb") as handle:  # exclusive create: never an overwrite
                handle.write(content)
        except FileExistsError:
            return "exists"
        return "created"

    async def get(self, path: str) -> bytes:
        target = self.path_for(path)
        if not target.is_file():
            raise FileNotFoundError(path)
        return target.read_bytes()

    async def signed_download_urls(
        self, paths: Sequence[str], ttl_seconds: int
    ) -> Mapping[str, str]:
        if not 1 <= ttl_seconds <= 3600:
            raise BundleError("signed download URLs must be short-lived")
        expires = int(time.time()) + ttl_seconds
        return {
            path: f"{self.path_for(path).as_uri()}?expires={expires}"
            for path in paths
            if self.path_for(path).is_file()
        }

    async def signed_upload_url(self, path: str) -> SignedUpload:
        return SignedUpload(path=path, url=self.path_for(path).as_uri(), token=None)


class FilesystemBundleFetcher:
    """Fetches ``file://`` signed URLs of one :class:`FilesystemBundleObjectStore`."""

    def __init__(self, store: FilesystemBundleObjectStore) -> None:
        self._store = store

    async def fetch(self, url: str) -> bytes:
        return self._read(url)

    def _read(self, url: str) -> bytes:
        parsed = urlsplit(url)
        if parsed.scheme != "file":
            raise BundleError("the local bundle fetcher reads file:// URLs only")
        expires = parse_qs(parsed.query).get("expires", ["0"])[0]
        if not expires.isdigit() or int(expires) < int(time.time()):
            raise BundleError("signed URL expired")
        raw = unquote(parsed.path)
        if len(raw) > 2 and raw[0] == "/" and raw[2] == ":":
            raw = raw[1:]  # file:///C:/... on Windows
        target = Path(raw).resolve()
        if not target.is_relative_to(self._store.root):
            raise BundleError("signed URL is outside the bundle store")
        return target.read_bytes()
