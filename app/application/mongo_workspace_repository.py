from __future__ import annotations

from pymongo.errors import DuplicateKeyError

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    WorkspaceMaterializationManifest,
    WorkspaceMaterializationRequest,
)
from app.domain.operation_execution.errors import WorkspaceSlotConflict
from app.domain.operation_execution.materialization import (
    verify_workspace_manifest,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.models.workspace_materialization import (
    WorkspaceMaterializationManifestDocument,
    WorkspaceSlotReservationDocument,
)


class MongoWorkspaceManifestRepository:
    """Immutable manifests plus unique writable-slot reservations in MongoDB."""

    async def reserve_writable_slots(self, request: WorkspaceMaterializationRequest) -> None:
        inserted: list[WorkspaceSlotReservationDocument] = []
        reservation_token = sha256_digest(request.model_dump(mode="json"))
        try:
            for slot in request.slots:
                if slot.access != "exclusive_write":
                    continue
                reservation_path = _ownership_boundary(slot.logical_path)
                reservation = WorkspaceSlotReservationDocument(
                    namespace_id=request.namespace_id,
                    workspace_id=request.workspace_id,
                    logical_path=reservation_path,
                    owner_id=slot.owner.owner_id,
                    reservation_token=reservation_token,
                    reserved_at=request.created_at,
                )
                try:
                    await reservation.insert()
                    inserted.append(reservation)
                except DuplicateKeyError:
                    prior = await WorkspaceSlotReservationDocument.find_one(
                        WorkspaceSlotReservationDocument.namespace_id == request.namespace_id,
                        WorkspaceSlotReservationDocument.logical_path == reservation_path,
                    )
                    if prior is None or (
                        prior.workspace_id,
                        prior.owner_id,
                        prior.reservation_token,
                    ) != (
                        request.workspace_id,
                        slot.owner.owner_id,
                        reservation_token,
                    ):
                        raise WorkspaceSlotConflict(
                            f"writable slot {slot.logical_path} is owned by another workspace"
                        ) from None
        except Exception:
            for reservation in inserted:
                await WorkspaceSlotReservationDocument.find(
                    WorkspaceSlotReservationDocument.id == reservation.id,
                    WorkspaceSlotReservationDocument.reservation_token == reservation_token,
                ).delete()
            raise

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
        if document is None:
            return None
        manifest = WorkspaceMaterializationManifest.model_validate(document.payload)
        verify_workspace_manifest(manifest)
        if manifest.revision > 1:
            prior = await WorkspaceMaterializationManifestDocument.find_one(
                WorkspaceMaterializationManifestDocument.namespace_id == namespace_id,
                WorkspaceMaterializationManifestDocument.workspace_id == workspace_id,
                WorkspaceMaterializationManifestDocument.revision == manifest.revision - 1,
            )
            if prior is None or prior.manifest_digest != manifest.prior_manifest_digest:
                raise IdempotencyConflict("workspace manifest lineage is incomplete")
        return manifest

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


def _ownership_boundary(logical_path: str) -> str:
    parts = [part for part in logical_path.split("/") if part]
    boundary = parts[:2]
    return "/" + "/".join(boundary)
