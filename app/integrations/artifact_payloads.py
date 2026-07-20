from __future__ import annotations

from hashlib import sha256

from app.application.artifact_promotion import ArtifactPayloadAddress
from app.config import Settings
from app.domain.operation_execution.errors import WorkspaceDigestMismatch
from app.integrations.s3 import s3_client


class InMemoryArtifactPayloadStore:
    def __init__(self) -> None:
        self.payloads: dict[str, bytes] = {}

    async def stage(
        self,
        *,
        artifact_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> ArtifactPayloadAddress:
        del artifact_id, media_type
        _verify(content, content_digest, len(content))
        object_ref = f"memory://artifacts/{content_digest.removeprefix('sha256:')}"
        prior = self.payloads.get(object_ref)
        if prior is not None and prior != content:
            raise WorkspaceDigestMismatch("content address contains conflicting bytes")
        self.payloads[object_ref] = content
        return ArtifactPayloadAddress(
            object_ref=object_ref,
            content_digest=content_digest,
            size_bytes=len(content),
        )

    async def retrieve(self, address: ArtifactPayloadAddress) -> bytes:
        try:
            content = self.payloads[address.object_ref]
        except KeyError as error:
            raise WorkspaceDigestMismatch("artifact payload is unavailable") from error
        _verify(content, address.content_digest, address.size_bytes)
        return content


class S3ArtifactPayloadStore:
    def __init__(
        self,
        settings: Settings,
        bucket: str,
        *,
        prefix: str = "artifacts/sha256",
    ) -> None:
        self._settings = settings
        self._bucket = bucket
        self._prefix = prefix.rstrip("/")

    async def stage(
        self,
        *,
        artifact_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> ArtifactPayloadAddress:
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
                    "artifact-id": artifact_id,
                },
            )
        return ArtifactPayloadAddress(
            object_ref=f"s3://{self._bucket}/{key}",
            content_digest=content_digest,
            size_bytes=len(content),
        )

    async def retrieve(self, address: ArtifactPayloadAddress) -> bytes:
        prefix = f"s3://{self._bucket}/"
        if not address.object_ref.startswith(prefix):
            raise WorkspaceDigestMismatch("artifact belongs to a different object store")
        key = address.object_ref.removeprefix(prefix)
        async with s3_client(self._settings) as client:
            response = await client.get_object(Bucket=self._bucket, Key=key)
            async with response["Body"] as stream:
                content = await stream.read()
        _verify(content, address.content_digest, address.size_bytes)
        return content


def _verify(content: bytes, content_digest: str, size_bytes: int) -> None:
    actual = f"sha256:{sha256(content).hexdigest()}"
    if actual != content_digest or len(content) != size_bytes:
        raise WorkspaceDigestMismatch(
            f"artifact payload mismatch: expected {content_digest}, got {actual}"
        )
