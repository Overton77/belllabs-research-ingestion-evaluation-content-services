from __future__ import annotations

from pymongo.errors import DuplicateKeyError

from app.domain.operation_execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionState,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.models.artifact_promotion import ArtifactMetadataRevisionDocument


class MongoArtifactMetadataRepository:
    async def get_by_intent(self, intent_key: str) -> ArtifactMetadataRevision | None:
        document = (
            await ArtifactMetadataRevisionDocument.find(
                ArtifactMetadataRevisionDocument.intent_key == intent_key
            )
            .sort("-revision")
            .first_or_none()
        )
        return (
            ArtifactMetadataRevision.model_validate(document.payload)
            if document is not None
            else None
        )

    async def get_by_artifact(self, artifact_id: str) -> ArtifactMetadataRevision | None:
        document = (
            await ArtifactMetadataRevisionDocument.find(
                ArtifactMetadataRevisionDocument.artifact_id == artifact_id
            )
            .sort("-revision")
            .first_or_none()
        )
        return (
            ArtifactMetadataRevision.model_validate(document.payload)
            if document is not None
            else None
        )

    async def append(self, revision: ArtifactMetadataRevision) -> ArtifactMetadataRevision:
        current = await self.get_by_intent(revision.intent_key)
        if current is not None:
            if current.artifact_id != revision.artifact_id:
                raise IdempotencyConflict("artifact intent belongs to another artifact")
            if revision.revision <= current.revision:
                matching = await ArtifactMetadataRevisionDocument.find_one(
                    ArtifactMetadataRevisionDocument.artifact_id == revision.artifact_id,
                    ArtifactMetadataRevisionDocument.revision == revision.revision,
                )
                if matching is not None:
                    prior = ArtifactMetadataRevision.model_validate(matching.payload)
                    if prior == revision:
                        return prior
                raise IdempotencyConflict("artifact metadata revision conflict")
            if revision.revision != current.revision + 1:
                raise IdempotencyConflict("artifact metadata revision gap")
        elif revision.revision != 1:
            raise IdempotencyConflict("first artifact metadata revision must be one")
        document = ArtifactMetadataRevisionDocument(
            promotion_id=revision.promotion_id,
            artifact_id=revision.artifact_id,
            intent_key=revision.intent_key,
            promotion_identity=revision.promotion_identity,
            revision=revision.revision,
            state=revision.state.value,
            run_id=revision.run_id,
            workspace_id=revision.workspace_id,
            content_digest=revision.content_digest,
            payload=revision.model_dump(mode="json"),
            recorded_at=revision.recorded_at,
        )
        try:
            await document.insert()
            return revision
        except DuplicateKeyError:
            matching = await ArtifactMetadataRevisionDocument.find_one(
                ArtifactMetadataRevisionDocument.artifact_id == revision.artifact_id,
                ArtifactMetadataRevisionDocument.revision == revision.revision,
            )
            if matching is None:
                raise IdempotencyConflict("artifact metadata identity collision") from None
            prior = ArtifactMetadataRevision.model_validate(matching.payload)
            if prior != revision:
                raise IdempotencyConflict("artifact metadata identity conflict") from None
            return prior

    async def reconciliation_required(
        self,
    ) -> tuple[ArtifactMetadataRevision, ...]:
        documents = await ArtifactMetadataRevisionDocument.find(
            ArtifactMetadataRevisionDocument.state
            == ArtifactPromotionState.RECONCILIATION_REQUIRED.value
        ).to_list()
        current: list[ArtifactMetadataRevision] = []
        for document in documents:
            latest = await self.get_by_artifact(document.artifact_id)
            if (
                latest is not None
                and latest.state == ArtifactPromotionState.RECONCILIATION_REQUIRED
                and latest not in current
            ):
                current.append(latest)
        return tuple(current)

    async def rejected(self) -> tuple[ArtifactMetadataRevision, ...]:
        documents = await ArtifactMetadataRevisionDocument.find(
            ArtifactMetadataRevisionDocument.state == ArtifactPromotionState.REJECTED.value
        ).to_list()
        current: list[ArtifactMetadataRevision] = []
        for document in documents:
            latest = await self.get_by_artifact(document.artifact_id)
            if (
                latest is not None
                and latest.state == ArtifactPromotionState.REJECTED
                and latest not in current
            ):
                current.append(latest)
        return tuple(current)
