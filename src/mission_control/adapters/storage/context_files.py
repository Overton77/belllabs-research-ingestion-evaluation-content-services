"""Content-addressed custody for Context Packet files (FT-B2, SPEC-02).

Rendered ``.mission/`` files are staged in the artifact payload store like any other durable
input, so the workspace materializer fetches and digest-verifies them through the existing
``DurableInputManifestEntry`` path, and lanes read the same verified bytes.
"""

from __future__ import annotations

import hashlib

from mission_control.application.artifacts.artifact_promotion import (
    ArtifactPayloadAddress,
    ArtifactPayloadPort,
)
from mission_control.domain.context.refs import (
    durable_input_locator,
    parse_durable_input_locator,
)


class PayloadContextFiles:
    """``MissionFileStagingPort`` and ``DurableInputReader`` over the artifact payload store."""

    def __init__(self, payloads: ArtifactPayloadPort) -> None:
        self._payloads = payloads

    async def stage(self, *, request_scope: str, name: str, content: bytes, media_type: str) -> str:
        digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
        scope_digest = hashlib.sha256(request_scope.encode("utf-8")).hexdigest()
        address = await self._payloads.stage(
            artifact_id=f"context-packet:{scope_digest}:{name}",
            content=content,
            content_digest=digest,
            media_type=media_type,
        )
        if address.content_digest != digest or address.size_bytes != len(content):
            raise ValueError("staged context file address does not match its bytes")
        return durable_input_locator(address.object_ref, digest, len(content))

    async def retrieve(self, durable_ref: str) -> bytes:
        object_ref, digest, size = parse_durable_input_locator(durable_ref)
        return await self._payloads.retrieve(
            ArtifactPayloadAddress(object_ref=object_ref, content_digest=digest, size_bytes=size)
        )
