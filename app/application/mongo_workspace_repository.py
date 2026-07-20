from __future__ import annotations

from pymongo.errors import DuplicateKeyError

from app.domain.operation_execution.contracts import (
    WorkspaceMaterializationManifest,
    WorkspaceMaterializationRequest,
)
from app.domain.operation_execution.errors import WorkspaceSlotConflict
from app.domain.run_control.errors import IdempotencyConflict
from app.models.workspace_materialization import (
    WorkspaceMaterializationManifestDocument,
    WorkspaceSlotReservationDocument,
)


class MongoWorkspaceManifestRepository:
    """Immutable manifests plus unique writable-slot reservations in MongoDB."""

    async def reserve_writable_slots(self, request: WorkspaceMaterializationRequest) -> None:
        for slot in request.slots:
            if slot.access != "exclusive_write":
                continue
            reservation = WorkspaceSlotReservationDocument(
                namespace_id=request.namespace_id,
                workspace_id=request.workspace_id,
                logical_path=slot.logical_path,
                owner_id=slot.owner.owner_id,
                reserved_at=request.created_at,
            )
            try:
                await reservation.insert()
            except DuplicateKeyError:
                prior = await WorkspaceSlotReservationDocument.find_one(
                    WorkspaceSlotReservationDocument.namespace_id == request.namespace_id,
                    WorkspaceSlotReservationDocument.logical_path == slot.logical_path,
                )
                if prior is None or (
                    prior.workspace_id,
                    prior.owner_id,
                ) != (request.workspace_id, slot.owner.owner_id):
                    raise WorkspaceSlotConflict(
                        f"writable slot {slot.logical_path} is owned by another workspace"
                    ) from None

    async def get_current(
        self, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest | None:
        document = (
            await WorkspaceMaterializationManifestDocument.find(
                WorkspaceMaterializationManifestDocument.namespace_id == namespace_id,
                WorkspaceMaterializationManifestDocument.workspace_id == workspace_id,
            )
            .sort("-revision")
            .first_or_none()
        )
        return (
            WorkspaceMaterializationManifest.model_validate(document.payload)
            if document is not None
            else None
        )

    async def append(
        self, manifest: WorkspaceMaterializationManifest
    ) -> WorkspaceMaterializationManifest:
        current = await self.get_current(manifest.namespace_id, manifest.workspace_id)
        if current is not None and (
            manifest.revision != current.revision + 1
            or manifest.prior_manifest_digest != current.manifest_digest
        ):
            matching = await WorkspaceMaterializationManifestDocument.find_one(
                WorkspaceMaterializationManifestDocument.manifest_id == manifest.manifest_id
            )
            if matching is not None:
                prior = WorkspaceMaterializationManifest.model_validate(matching.payload)
                if prior == manifest:
                    return prior
            raise IdempotencyConflict("workspace manifest lineage conflict")
        if current is None and (
            manifest.revision != 1 or manifest.prior_manifest_digest is not None
        ):
            raise IdempotencyConflict("first workspace manifest must be revision one")
        document = WorkspaceMaterializationManifestDocument(
            manifest_id=manifest.manifest_id,
            namespace_id=manifest.namespace_id,
            workspace_id=manifest.workspace_id,
            revision=manifest.revision,
            manifest_digest=manifest.manifest_digest,
            prior_manifest_digest=manifest.prior_manifest_digest,
            payload=manifest.model_dump(mode="json"),
            created_at=manifest.created_at,
        )
        try:
            await document.insert()
            return manifest
        except DuplicateKeyError:
            matching = await WorkspaceMaterializationManifestDocument.find_one(
                WorkspaceMaterializationManifestDocument.manifest_id == manifest.manifest_id
            )
            if matching is None:
                raise IdempotencyConflict("workspace manifest identity collision") from None
            prior = WorkspaceMaterializationManifest.model_validate(matching.payload)
            if prior != manifest:
                raise IdempotencyConflict("workspace manifest identity conflict") from None
            return prior
