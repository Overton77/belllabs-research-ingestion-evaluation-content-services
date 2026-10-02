"""Durable workspace candidate contents: object-store bytes, MongoDB descriptors (RRM-009).

A Deep Agent's writable-slot outputs are captured as workspace candidates after cognition.
Their bytes go to the content-addressed artifact payload store (S3 or the shared directory)
and their descriptors to MongoDB, so artifact promotion (`artifact.promote`) can run on any
worker and a worker-local file is never the only copy. Candidates are insert-once by
identity; a second capture of identical content is idempotent and different content
conflicts.
"""

from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

from pymongo.errors import DuplicateKeyError

from app.application.workspaces.artifact_promotion import (
    ArtifactPayloadAddress,
    ArtifactPayloadPort,
)
from app.domain.operation_execution.contracts import CapturedWorkspaceCandidate
from app.domain.operation_execution.errors import (
    UndeclaredWorkspacePath,
    WorkspaceDigestMismatch,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.models.workspace_materialization import WorkspaceCandidateDocument


class ObjectStoreWorkspaceCandidateContents:
    def __init__(self, payloads: ArtifactPayloadPort) -> None:
        self._payloads = payloads

    async def put(self, candidate: CapturedWorkspaceCandidate, content: bytes) -> None:
        if (
            candidate.content_digest != f"sha256:{sha256(content).hexdigest()}"
            or candidate.size_bytes != len(content)
        ):
            raise WorkspaceDigestMismatch("workspace candidate bytes do not match their descriptor")
        address = await self._payloads.stage(
            artifact_id=f"workspace-candidate:{candidate.candidate_id}",
            content=content,
            content_digest=candidate.content_digest,
            media_type=candidate.media_type,
        )
        document = WorkspaceCandidateDocument(
            candidate_id=candidate.candidate_id,
            namespace_id=candidate.namespace_id,
            workspace_id=candidate.workspace_id,
            logical_path=candidate.logical_path,
            content_digest=candidate.content_digest,
            descriptor=candidate.model_dump(mode="json"),
            object_ref=address.object_ref,
            size_bytes=address.size_bytes,
            recorded_at=datetime.now(UTC),
        )
        try:
            await document.insert()
        except DuplicateKeyError:
            prior = await WorkspaceCandidateDocument.find_one(
                WorkspaceCandidateDocument.candidate_id == candidate.candidate_id
            )
            if prior is None or prior.descriptor != document.descriptor:
                raise IdempotencyConflict("workspace candidate content conflict") from None

    async def get(self, candidate_id: str) -> bytes:
        document = await self._document(candidate_id)
        return await self._payloads.retrieve(
            ArtifactPayloadAddress(
                object_ref=document.object_ref,
                content_digest=document.content_digest,
                size_bytes=document.size_bytes,
            )
        )

    async def describe(self, candidate_id: str) -> CapturedWorkspaceCandidate:
        document = await self._document(candidate_id)
        return CapturedWorkspaceCandidate.model_validate(document.descriptor)

    async def find(
        self, namespace_id: str, workspace_id: str, logical_path: str
    ) -> CapturedWorkspaceCandidate | None:
        documents = (
            await WorkspaceCandidateDocument.find(
                WorkspaceCandidateDocument.namespace_id == namespace_id,
                WorkspaceCandidateDocument.workspace_id == workspace_id,
                WorkspaceCandidateDocument.logical_path == logical_path,
            )
            .sort("-recorded_at")
            .limit(1)
            .to_list()
        )
        if not documents:
            return None
        return CapturedWorkspaceCandidate.model_validate(documents[0].descriptor)

    async def _document(self, candidate_id: str) -> WorkspaceCandidateDocument:
        document = await WorkspaceCandidateDocument.find_one(
            WorkspaceCandidateDocument.candidate_id == candidate_id
        )
        if document is None:
            raise UndeclaredWorkspacePath("workspace candidate descriptor is unavailable")
        return document
