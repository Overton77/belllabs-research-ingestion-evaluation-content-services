from __future__ import annotations

from hashlib import sha256

from app.config import Settings
from app.domain.operation_execution.contracts import SnapshotPayloadAddress
from app.domain.operation_execution.errors import SnapshotPayloadMismatch
from app.integrations.s3 import s3_client

MAX_SNAPSHOT_PAYLOAD_BYTES = 268_435_456


class S3SnapshotPayloadStore:
    """Content-addressed snapshot archives; MongoDB stores their immutable addresses."""

    def __init__(
        self,
        settings: Settings,
        bucket: str,
        *,
        prefix: str = "sandbox-snapshots/sha256",
    ) -> None:
        self._settings = settings
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")

    async def stage(
        self,
        *,
        snapshot_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> SnapshotPayloadAddress:
        if len(content) > MAX_SNAPSHOT_PAYLOAD_BYTES:
            raise SnapshotPayloadMismatch("snapshot payload exceeds the configured size limit")
        _verify(content, content_digest, len(content))
        digest_value = content_digest.removeprefix("sha256:")
        key = f"{self._prefix}/{digest_value}"
        async with s3_client(self._settings) as client:
            await client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=content,
                ContentType=media_type,
                Metadata={
                    "sha256": digest_value,
                    "snapshot-id": snapshot_id,
                },
            )
        return SnapshotPayloadAddress(
            object_ref=f"s3://{self._bucket}/{key}",
            content_digest=content_digest,
            size_bytes=len(content),
            media_type=media_type,
        )

    async def retrieve(self, address: SnapshotPayloadAddress) -> bytes:
        prefix = f"s3://{self._bucket}/"
        if not address.object_ref.startswith(prefix):
            raise SnapshotPayloadMismatch("snapshot belongs to a different object store")
        key = address.object_ref.removeprefix(prefix)
        expected_key = (
            f"{self._prefix}/{address.content_digest.removeprefix('sha256:')}"
        )
        if key != expected_key:
            raise SnapshotPayloadMismatch("snapshot object reference is not content-addressed")
        if address.size_bytes > MAX_SNAPSHOT_PAYLOAD_BYTES:
            raise SnapshotPayloadMismatch("snapshot payload exceeds the configured size limit")
        async with s3_client(self._settings) as client:
            response = await client.get_object(Bucket=self._bucket, Key=key)
            if response.get("ContentLength") != address.size_bytes:
                raise SnapshotPayloadMismatch("snapshot object size does not match metadata")
            async with response["Body"] as stream:
                content = await stream.read()
        _verify(content, address.content_digest, address.size_bytes)
        return content


def _verify(content: bytes, content_digest: str, size_bytes: int) -> None:
    actual = f"sha256:{sha256(content).hexdigest()}"
    if actual != content_digest or len(content) != size_bytes:
        raise SnapshotPayloadMismatch(
            f"snapshot payload mismatch: expected {content_digest}, got {actual}"
        )
