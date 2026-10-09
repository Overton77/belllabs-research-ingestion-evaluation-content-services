"""Full transcript bodies through the artifact read path (SPEC-03 `--full`).

`GrantedArtifactBodyReader` resolves a transcript entry's `artifact_ref` (a durable
`artifact://<request_scope>/<run_id>/<artifact_id>` reference, or `artifact:<id>`) to the
admitted artifact's bytes under the caller's grant: the actor needs the artifact read grant
on top of `workflow_run.read`, the artifact must belong to the service's tenant scope (and to
the reference's run when it names one), and only admitted, non-prohibited revisions are read.
The payload store verifies the content digest and size on retrieval.
"""

from __future__ import annotations

from typing import Final, Protocol

from mission_control.application.artifacts.artifact_promotion import ArtifactPayloadAddress
from mission_control.domain.execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
)
from mission_control.domain.policies.contracts import ActorContext

ARTIFACT_READ_PERMISSION: Final = "workflow.result.read"
MAX_FULL_BODY_BYTES: Final = 4_000_000
_DURABLE_PREFIX: Final = "artifact://"
_SHORT_PREFIX: Final = "artifact:"


class ArtifactBodyDenied(PermissionError):
    code = "unauthorized"


class ArtifactBodyMissing(LookupError):
    code = "artifact_unavailable"


class ArtifactMetadataReader(Protocol):
    async def get_by_artifact(self, artifact_id: str) -> ArtifactMetadataRevision | None: ...


class ArtifactPayloadReader(Protocol):
    async def retrieve(self, address: ArtifactPayloadAddress) -> bytes: ...


def parse_artifact_ref(artifact_ref: str) -> tuple[str | None, str | None, str]:
    """(request scope, run id, artifact id) of a durable or short artifact reference."""

    if artifact_ref.startswith(_DURABLE_PREFIX):
        rest = artifact_ref.removeprefix(_DURABLE_PREFIX)
        scope, _sep, tail = rest.rpartition("/")
        scope, _sep, run_id = scope.rpartition("/")
        if not scope or not run_id or not tail:
            raise ArtifactBodyMissing("malformed artifact reference")
        return scope, run_id, tail
    if artifact_ref.startswith(_SHORT_PREFIX) and len(artifact_ref) > len(_SHORT_PREFIX):
        return None, None, artifact_ref.removeprefix(_SHORT_PREFIX)
    raise ArtifactBodyMissing("not an artifact reference")


class GrantedArtifactBodyReader:
    """`ArtifactBodyReader` over artifact metadata and the artifact payload store."""

    def __init__(
        self,
        metadata: ArtifactMetadataReader,
        payloads: ArtifactPayloadReader,
        *,
        request_scope: str,
        max_bytes: int = MAX_FULL_BODY_BYTES,
    ) -> None:
        self._metadata = metadata
        self._payloads = payloads
        self.request_scope = request_scope
        self._max_bytes = max_bytes

    async def read(self, request_scope: str, actor: ActorContext, artifact_ref: str) -> str:
        if ARTIFACT_READ_PERMISSION not in actor.permissions:
            raise ArtifactBodyDenied(f"actor lacks {ARTIFACT_READ_PERMISSION}")
        if request_scope != self.request_scope:
            raise ArtifactBodyDenied("artifact reader serves another tenant scope")
        ref_scope, ref_run, artifact_id = parse_artifact_ref(artifact_ref)
        if ref_scope is not None and ref_scope != request_scope:
            raise ArtifactBodyDenied("artifact reference names another tenant scope")
        revision = await self._metadata.get_by_artifact(artifact_id)
        if revision is None or revision.request_scope != request_scope:
            raise ArtifactBodyMissing("artifact not found")
        if ref_run is not None and revision.run_id != ref_run:
            raise ArtifactBodyMissing("artifact not found")
        if revision.permission_outcome == "prohibited":
            raise ArtifactBodyDenied("artifact read is prohibited by its permission outcome")
        if revision.state != ArtifactPromotionState.ADMITTED or revision.object_ref is None:
            raise ArtifactBodyMissing("artifact is not admitted")
        if revision.size_bytes > self._max_bytes:
            raise ArtifactBodyMissing("artifact body exceeds the transcript full-body cap")
        content = await self._payloads.retrieve(
            ArtifactPayloadAddress(
                object_ref=revision.object_ref,
                content_digest=revision.content_digest,
                size_bytes=revision.size_bytes,
            )
        )
        return content.decode("utf-8", errors="replace")


__all__ = [
    "ARTIFACT_READ_PERMISSION",
    "MAX_FULL_BODY_BYTES",
    "ArtifactBodyDenied",
    "ArtifactBodyMissing",
    "GrantedArtifactBodyReader",
    "parse_artifact_ref",
]
