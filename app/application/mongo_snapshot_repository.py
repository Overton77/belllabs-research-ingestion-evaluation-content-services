from __future__ import annotations

from datetime import datetime

from pymongo.errors import DuplicateKeyError

from app.domain.operation_execution.contracts import (
    SandboxSnapshot,
    SnapshotCloneRecord,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.models.sandbox_snapshot import (
    SandboxSnapshotClaimDocument,
    SandboxSnapshotCloneDocument,
    SandboxSnapshotDocument,
)


class MongoSandboxSnapshotRepository:
    """Immutable snapshot metadata and clone lineage in MongoDB/Beanie."""

    async def claim_creation(
        self, snapshot_id: str, creation_identity: str, claimed_at: datetime
    ) -> bool:
        return await self._claim(
            claim_kind="creation",
            identity=snapshot_id,
            fingerprint=creation_identity,
            claimed_at=claimed_at,
        )

    async def claim_clone(
        self, request_fingerprint: str, clone_id: str, claimed_at: datetime
    ) -> bool:
        return await self._claim(
            claim_kind="clone",
            identity=clone_id,
            fingerprint=request_fingerprint,
            claimed_at=claimed_at,
        )

    async def get_snapshot(self, snapshot_id: str) -> SandboxSnapshot | None:
        document = await SandboxSnapshotDocument.find_one(
            SandboxSnapshotDocument.snapshot_id == snapshot_id
        )
        return (
            SandboxSnapshot.model_validate(document.payload)
            if document is not None
            else None
        )

    async def create_snapshot(self, snapshot: SandboxSnapshot) -> SandboxSnapshot:
        document = SandboxSnapshotDocument(
            snapshot_id=snapshot.snapshot_id,
            creation_identity=snapshot.creation_identity,
            request_scope=snapshot.request_scope,
            source_namespace_id=snapshot.source_namespace_id,
            source_workspace_id=snapshot.source_workspace_id,
            parent_snapshot_id=snapshot.parent_snapshot_id,
            provider=snapshot.provider,
            provider_snapshot_id=snapshot.provider_snapshot_id,
            payload_digest=snapshot.payload.content_digest,
            object_ref=snapshot.payload.object_ref,
            producer_binding_id=snapshot.producer_binding_id,
            created_at=snapshot.created_at,
            retain_until=snapshot.retention.retain_until,
            payload=snapshot.model_dump(mode="json"),
        )
        try:
            await document.insert()
            return snapshot
        except DuplicateKeyError:
            prior = await self.get_snapshot(snapshot.snapshot_id)
            if prior is None or prior != snapshot:
                raise IdempotencyConflict("snapshot immutable metadata conflict") from None
            return prior

    async def get_clone(self, clone_id: str) -> SnapshotCloneRecord | None:
        document = await SandboxSnapshotCloneDocument.find_one(
            SandboxSnapshotCloneDocument.clone_id == clone_id
        )
        return (
            SnapshotCloneRecord.model_validate(document.payload)
            if document is not None
            else None
        )

    async def create_clone(self, clone: SnapshotCloneRecord) -> SnapshotCloneRecord:
        document = SandboxSnapshotCloneDocument(
            clone_id=clone.clone_id,
            snapshot_id=clone.snapshot_id,
            parent_workspace_id=clone.parent_workspace_id,
            target_namespace_id=clone.target_namespace_id,
            target_workspace_id=clone.target_workspace_id,
            binding_id=clone.binding_id,
            created_at=clone.created_at,
            payload=clone.model_dump(mode="json"),
        )
        try:
            await document.insert()
            return clone
        except DuplicateKeyError:
            prior = await self.get_clone(clone.clone_id)
            if prior is None or prior != clone:
                raise IdempotencyConflict("snapshot clone lineage conflict") from None
            return prior

    @staticmethod
    async def _claim(
        *,
        claim_kind: str,
        identity: str,
        fingerprint: str,
        claimed_at: datetime,
    ) -> bool:
        claim_key = f"{claim_kind}:{identity}"
        document = SandboxSnapshotClaimDocument(
            claim_key=claim_key,
            claim_kind=claim_kind,
            identity=identity,
            fingerprint=fingerprint,
            claimed_at=claimed_at,
        )
        try:
            await document.insert()
            return True
        except DuplicateKeyError:
            prior = await SandboxSnapshotClaimDocument.find_one(
                SandboxSnapshotClaimDocument.claim_key == claim_key
            )
            if prior is None or prior.fingerprint != fingerprint:
                raise IdempotencyConflict(
                    f"snapshot {claim_kind} claim has conflicting intent"
                ) from None
            return False
