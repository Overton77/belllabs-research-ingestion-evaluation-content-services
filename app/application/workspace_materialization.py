from __future__ import annotations

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    DurableInputManifestEntry,
    LocalCandidateManifestEntry,
    MaterializedWorkspace,
    OperationExecutionBinding,
    PromotedArtifactManifestEntry,
    WorkspaceMaterializationManifest,
    WorkspaceMaterializationRequest,
    WorkspaceOwner,
)
from app.domain.operation_execution.errors import (
    UndeclaredWorkspacePath,
    WorkspaceDigestMismatch,
    WorkspaceSlotConflict,
)
from app.domain.operation_execution.materialization import (
    verify_workspace_manifest,
)
from app.domain.run_control.errors import IdempotencyConflict


class WorkspaceManifestRepository(Protocol):
    async def reserve_writable_slots(self, request: WorkspaceMaterializationRequest) -> None: ...

    async def get_current(
        self, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest | None: ...

    async def append(
        self, manifest: WorkspaceMaterializationManifest
    ) -> WorkspaceMaterializationManifest: ...


class WorkspaceProvisionerPort(Protocol):
    async def provision(
        self,
        request: WorkspaceMaterializationRequest,
        manifest: WorkspaceMaterializationManifest,
        durable_inputs: Mapping[str, bytes],
    ) -> MaterializedWorkspace: ...


class DurableWorkspaceInputPort(Protocol):
    async def retrieve(self, durable_ref: str) -> bytes: ...


class WorkspaceMaterializationService:
    def __init__(
        self,
        *,
        manifests: WorkspaceManifestRepository,
        provisioner: WorkspaceProvisionerPort,
        durable_inputs: DurableWorkspaceInputPort,
    ) -> None:
        self._manifests = manifests
        self._provisioner = provisioner
        self._durable_inputs = durable_inputs

    async def materialize(self, request: WorkspaceMaterializationRequest) -> MaterializedWorkspace:
        prior = await self._manifests.get_current(request.namespace_id, request.workspace_id)
        if prior is not None:
            verify_workspace_manifest(prior)
            if (
                prior.namespace_id != request.namespace_id
                or prior.workspace_id != request.workspace_id
                or prior.template_ref != request.template_ref
                or prior.workflow_contract_digest != request.workflow_contract_digest
                or prior.slots != request.slots
            ):
                raise IdempotencyConflict(
                    "workspace identity was reused with different materialization"
                )
            inputs = await self._load_and_verify_inputs(prior)
            workspace = await self._provisioner.provision(request, prior, inputs)
            return workspace.model_copy(update={"materialization_manifest": prior})

        await self._manifests.reserve_writable_slots(request)
        manifest = await self._manifests.append(self._initial_manifest(request))
        inputs = await self._load_and_verify_inputs(manifest)
        workspace = await self._provisioner.provision(request, manifest, inputs)
        return workspace.model_copy(update={"materialization_manifest": manifest})

    async def register_candidate(
        self,
        *,
        namespace_id: str,
        workspace_id: str,
        slot_name: str,
        logical_path: str,
        owner: WorkspaceOwner,
        candidate_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
        recorded_at: datetime | None = None,
    ) -> WorkspaceMaterializationManifest:
        current = await self._require_current(namespace_id, workspace_id)
        slot = next((item for item in current.slots if item.slot_name == slot_name), None)
        if (
            slot is None
            or slot.access != "exclusive_write"
            or not _path_within_slot(logical_path, slot.logical_path)
            or slot.owner != owner
        ):
            raise UndeclaredWorkspacePath(
                "candidate path, slot, or owner is outside the materialized contract"
            )
        if _digest_bytes(content) != content_digest:
            raise WorkspaceDigestMismatch("candidate bytes do not match the declared digest")
        prior_candidate = next(
            (
                entry
                for entry in current.entries
                if entry.kind == "local_candidate" and entry.candidate_id == candidate_id
            ),
            None,
        )
        if prior_candidate is not None:
            if (
                prior_candidate.content_digest != content_digest
                or prior_candidate.logical_path != logical_path
            ):
                raise IdempotencyConflict("candidate identity was reused with conflicting content")
            return current
        entry = LocalCandidateManifestEntry(
            entry_id=_stable_id("workspace-candidate", workspace_id, candidate_id, content_digest),
            slot_name=slot_name,
            logical_path=logical_path,
            owner=owner,
            candidate_id=candidate_id,
            content_digest=content_digest,
            media_type=media_type,
            size_bytes=len(content),
        )
        return await self._append_revision(
            current,
            entries=tuple(item for item in current.entries if item.logical_path != logical_path)
            + (entry,),
            created_at=recorded_at or datetime.now(UTC),
        )

    async def current_manifest(
        self, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest:
        return await self._require_current(namespace_id, workspace_id)

    async def link_promoted_artifact(
        self,
        *,
        namespace_id: str,
        workspace_id: str,
        candidate_id: str,
        artifact_id: str,
        artifact_metadata_revision: int,
        content_digest: str,
        recorded_at: datetime | None = None,
    ) -> WorkspaceMaterializationManifest:
        current = await self._require_current(namespace_id, workspace_id)
        existing = next(
            (
                entry
                for entry in current.entries
                if entry.kind == "promoted_artifact" and entry.artifact_id == artifact_id
            ),
            None,
        )
        if existing is not None:
            if existing.candidate_id != candidate_id or existing.content_digest != content_digest:
                raise IdempotencyConflict(
                    "artifact identity conflicts with current manifest linkage"
                )
            if existing.artifact_metadata_revision == artifact_metadata_revision:
                return current
            relinked = existing.model_copy(
                update={
                    "entry_id": _stable_id(
                        "workspace-promoted",
                        workspace_id,
                        artifact_id,
                        content_digest,
                        str(artifact_metadata_revision),
                    ),
                    "artifact_metadata_revision": artifact_metadata_revision,
                }
            )
            return await self._append_revision(
                current,
                entries=tuple(
                    entry for entry in current.entries if entry.entry_id != existing.entry_id
                )
                + (relinked,),
                created_at=recorded_at or datetime.now(UTC),
            )
        candidate = next(
            (
                entry
                for entry in current.entries
                if entry.kind == "local_candidate" and entry.candidate_id == candidate_id
            ),
            None,
        )
        if candidate is None or candidate.content_digest != content_digest:
            raise UndeclaredWorkspacePath(
                "promotion requires the current digest-matched local candidate"
            )
        promoted = PromotedArtifactManifestEntry(
            entry_id=_stable_id(
                "workspace-promoted",
                workspace_id,
                artifact_id,
                content_digest,
                str(artifact_metadata_revision),
            ),
            slot_name=candidate.slot_name,
            logical_path=candidate.logical_path,
            owner=candidate.owner,
            candidate_id=candidate.candidate_id,
            artifact_id=artifact_id,
            artifact_metadata_revision=artifact_metadata_revision,
            content_digest=content_digest,
        )
        return await self._append_revision(
            current,
            entries=tuple(
                entry for entry in current.entries if entry.entry_id != candidate.entry_id
            )
            + (promoted,),
            created_at=recorded_at or datetime.now(UTC),
        )

    async def _require_current(
        self, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest:
        current = await self._manifests.get_current(namespace_id, workspace_id)
        if current is None:
            raise UndeclaredWorkspacePath("workspace has not been materialized")
        verify_workspace_manifest(current)
        return current

    async def _load_and_verify_inputs(
        self, manifest: WorkspaceMaterializationManifest
    ) -> dict[str, bytes]:
        values: dict[str, bytes] = {}
        for entry in manifest.entries:
            if entry.kind != "durable_input":
                continue
            content = await self._durable_inputs.retrieve(entry.durable_ref)
            if _digest_bytes(content) != entry.content_digest:
                raise WorkspaceDigestMismatch(
                    f"durable input digest mismatch for {entry.logical_path}"
                )
            values[entry.logical_path] = content
        return values

    def _initial_manifest(
        self, request: WorkspaceMaterializationRequest
    ) -> WorkspaceMaterializationManifest:
        entries = tuple(
            DurableInputManifestEntry(
                entry_id=_stable_id(
                    "workspace-input",
                    request.workspace_id,
                    slot.slot_name,
                    slot.content_digest or "",
                ),
                slot_name=slot.slot_name,
                logical_path=slot.logical_path,
                owner=slot.owner,
                durable_ref=slot.durable_ref or "",
                content_digest=slot.content_digest or "",
            )
            for slot in request.slots
            if slot.access == "read_only"
        )
        payload = {
            "namespace_id": request.namespace_id,
            "workspace_id": request.workspace_id,
            "revision": 1,
            "template_ref": request.template_ref.model_dump(mode="json"),
            "workflow_contract_digest": request.workflow_contract_digest,
            "slots": [slot.model_dump(mode="json") for slot in request.slots],
            "entries": [entry.model_dump(mode="json") for entry in entries],
            "prior_manifest_digest": None,
        }
        digest = sha256_digest(payload)
        return WorkspaceMaterializationManifest(
            manifest_id=_stable_id("workspace-manifest", request.workspace_id, "1", digest),
            namespace_id=request.namespace_id,
            workspace_id=request.workspace_id,
            revision=1,
            template_ref=request.template_ref,
            workflow_contract_digest=request.workflow_contract_digest,
            slots=request.slots,
            entries=entries,
            manifest_digest=digest,
            created_at=request.created_at,
        )

    async def _append_revision(
        self,
        current: WorkspaceMaterializationManifest,
        *,
        entries: tuple,
        created_at: datetime,
    ) -> WorkspaceMaterializationManifest:
        revision = current.revision + 1
        payload = {
            "namespace_id": current.namespace_id,
            "workspace_id": current.workspace_id,
            "revision": revision,
            "template_ref": current.template_ref.model_dump(mode="json"),
            "workflow_contract_digest": current.workflow_contract_digest,
            "slots": [slot.model_dump(mode="json") for slot in current.slots],
            "entries": [entry.model_dump(mode="json") for entry in entries],
            "prior_manifest_digest": current.manifest_digest,
        }
        digest = sha256_digest(payload)
        return await self._manifests.append(
            WorkspaceMaterializationManifest(
                manifest_id=_stable_id(
                    "workspace-manifest", current.workspace_id, str(revision), digest
                ),
                namespace_id=current.namespace_id,
                workspace_id=current.workspace_id,
                revision=revision,
                template_ref=current.template_ref,
                workflow_contract_digest=current.workflow_contract_digest,
                slots=current.slots,
                entries=entries,
                prior_manifest_digest=current.manifest_digest,
                manifest_digest=digest,
                created_at=created_at,
            )
        )


class BindingWorkspaceMaterializer:
    """Adapts exact operation bindings to the runtime SandboxPort."""

    def __init__(self, service: WorkspaceMaterializationService) -> None:
        self._service = service

    async def materialize(self, binding: OperationExecutionBinding) -> MaterializedWorkspace:
        workspace = binding.workspace
        if not workspace.slot_bindings or workspace.workflow_contract_digest is None:
            raise ValueError("operation binding lacks compiled workspace slots and contract digest")
        return await self._service.materialize(
            WorkspaceMaterializationRequest(
                namespace_id=workspace.namespace_id,
                workspace_id=workspace.workspace_id,
                provider=workspace.provider,
                template_ref=workspace.template_ref,
                workflow_contract_digest=workspace.workflow_contract_digest,
                slots=workspace.slot_bindings,
                runtime_digest=workspace.runtime_digest,
                image_digest=workspace.image_digest,
                created_at=binding.bound_at,
            )
        )


class InMemoryWorkspaceManifestRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._manifests: dict[tuple[str, str], list[WorkspaceMaterializationManifest]] = {}
        self._reservations: dict[tuple[str, str], tuple[str, str, str]] = {}

    async def reserve_writable_slots(self, request: WorkspaceMaterializationRequest) -> None:
        async with self._lock:
            reservation_token = sha256_digest(request.model_dump(mode="json"))
            requested = [
                (
                    request.namespace_id,
                    _ownership_boundary(slot.logical_path),
                    slot.owner.owner_id,
                )
                for slot in request.slots
                if slot.access == "exclusive_write"
            ]
            for namespace_id, logical_path, owner_id in requested:
                prior = self._reservations.get((namespace_id, logical_path))
                if prior is not None and prior != (
                    request.workspace_id,
                    owner_id,
                    reservation_token,
                ):
                    raise WorkspaceSlotConflict(
                        f"writable slot {logical_path} is owned by another workspace"
                    )
            for namespace_id, logical_path, owner_id in requested:
                self._reservations[(namespace_id, logical_path)] = (
                    request.workspace_id,
                    owner_id,
                    reservation_token,
                )

    async def get_current(
        self, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest | None:
        values = self._manifests.get((namespace_id, workspace_id), [])
        return deepcopy(values[-1]) if values else None

    async def append(
        self, manifest: WorkspaceMaterializationManifest
    ) -> WorkspaceMaterializationManifest:
        async with self._lock:
            key = (manifest.namespace_id, manifest.workspace_id)
            values = self._manifests.setdefault(key, [])
            if values:
                prior = values[-1]
                if manifest.revision <= prior.revision:
                    matching = next(
                        (item for item in values if item.revision == manifest.revision), None
                    )
                    if matching == manifest:
                        return deepcopy(matching)
                    raise IdempotencyConflict("workspace manifest revision conflict")
                if (
                    manifest.revision != prior.revision + 1
                    or manifest.prior_manifest_digest != prior.manifest_digest
                ):
                    raise IdempotencyConflict("workspace manifest lineage conflict")
            elif manifest.revision != 1 or manifest.prior_manifest_digest is not None:
                raise IdempotencyConflict("first workspace manifest must be revision one")
            values.append(deepcopy(manifest))
            return deepcopy(manifest)


class InMemoryDurableWorkspaceInputs:
    def __init__(self, values: Mapping[str, bytes] | None = None) -> None:
        self._values = dict(values or {})

    async def retrieve(self, durable_ref: str) -> bytes:
        try:
            return self._values[durable_ref]
        except KeyError as error:
            raise UndeclaredWorkspacePath(f"durable input is unavailable: {durable_ref}") from error


def _digest_bytes(value: bytes) -> str:
    return f"sha256:{sha256(value).hexdigest()}"


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))


def _path_within_slot(logical_path: str, slot_path: str) -> bool:
    normalized_slot = slot_path.rstrip("/")
    return logical_path == normalized_slot or logical_path.startswith(normalized_slot + "/")


def _ownership_boundary(logical_path: str) -> str:
    parts = [part for part in logical_path.split("/") if part]
    return "/" + "/".join(parts[:2])
