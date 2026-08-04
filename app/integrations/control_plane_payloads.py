from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol

from app.config import Settings
from app.domain.control_plane.errors import PayloadIntegrityError
from app.integrations.s3 import s3_client


@dataclass(frozen=True, slots=True)
class ContentAddress:
    uri: str
    digest: str
    size: int
    version_id: str | None = None


def _bytes_digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


class ContentAddressedPayloadStore(Protocol):
    async def put(self, payload: bytes) -> ContentAddress: ...

    async def retrieve(self, address: ContentAddress) -> bytes: ...


class InMemoryPayloadStore:
    def __init__(self) -> None:
        self._payloads: dict[str, bytes] = {}

    async def put(self, payload: bytes) -> ContentAddress:
        digest = _bytes_digest(payload)
        uri = f"memory://control-plane/{digest.removeprefix('sha256:')}"
        self._payloads[uri] = payload
        return ContentAddress(uri=uri, digest=digest, size=len(payload))

    async def retrieve(self, address: ContentAddress) -> bytes:
        try:
            payload = self._payloads[address.uri]
        except KeyError as exc:
            raise PayloadIntegrityError(f"payload not found: {address.uri}") from exc
        _verify_payload(payload, address)
        return payload


class UnavailablePayloadStore:
    """Fail explicitly when durable externalization has not been configured."""

    async def put(self, payload: bytes) -> ContentAddress:
        del payload
        raise PayloadIntegrityError(
            "durable object storage is required to externalize this configuration"
        )

    async def retrieve(self, address: ContentAddress) -> bytes:
        raise PayloadIntegrityError(f"durable object storage is unavailable for {address.uri}")


class S3PayloadStore:
    def __init__(
        self,
        settings: Settings,
        bucket: str,
        prefix: str = "control-plane/erc",
        *,
        media_type: str = "application/json",
        suffix: str = ".json",
    ) -> None:
        if not media_type.strip():
            raise ValueError("S3 payload media type must be non-empty")
        if (
            not suffix.startswith(".")
            or len(suffix) == 1
            or "/" in suffix
            or "\\" in suffix
        ):
            raise ValueError("S3 payload suffix must be a simple dot-prefixed extension")
        self._settings = settings
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")
        self._media_type = media_type
        self._suffix = suffix

    async def put(self, payload: bytes) -> ContentAddress:
        digest = _bytes_digest(payload)
        key = f"{self._prefix}/{digest.removeprefix('sha256:')}{self._suffix}"
        async with s3_client(self._settings) as client:
            response = await client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=payload,
                ContentType=self._media_type,
                Metadata={"sha256": digest.removeprefix("sha256:")},
            )
        return ContentAddress(
            uri=f"s3://{self._bucket}/{key}",
            digest=digest,
            size=len(payload),
            version_id=response.get("VersionId"),
        )

    async def retrieve(self, address: ContentAddress) -> bytes:
        prefix = f"s3://{self._bucket}/"
        if not address.uri.startswith(prefix):
            raise PayloadIntegrityError("payload address belongs to a different S3 bucket")
        key = address.uri.removeprefix(prefix)
        async with s3_client(self._settings) as client:
            arguments = {"Bucket": self._bucket, "Key": key}
            if address.version_id is not None:
                arguments["VersionId"] = address.version_id
            response = await client.get_object(**arguments)
            async with response["Body"] as stream:
                payload = await stream.read()
        _verify_payload(payload, address)
        return payload


def _verify_payload(payload: bytes, address: ContentAddress) -> None:
    actual = _bytes_digest(payload)
    if actual != address.digest or len(payload) != address.size:
        raise PayloadIntegrityError(
            f"content-address mismatch for {address.uri}: expected {address.digest}, got {actual}"
        )
