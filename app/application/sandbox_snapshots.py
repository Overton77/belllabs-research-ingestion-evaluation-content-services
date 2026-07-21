from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from typing import Protocol

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    OperationExecutionBinding,
    ReacquiredRuntimeResources,
    SandboxSnapshot,
    SandboxSnapshotCapture,
    SandboxSnapshotCreateRequest,
    SnapshotCapabilityShape,
    SnapshotCloneRecord,
    SnapshotCloneRequest,
    SnapshotCloneResult,
    SnapshotPayloadAddress,
    WorkspaceMaterializationManifest,
)
from app.domain.operation_execution.errors import (
    SnapshotAuthorityError,
    SnapshotCloneInProgress,
    SnapshotCompatibilityError,
    SnapshotCreationInProgress,
    SnapshotMigrationRequired,
    SnapshotPayloadMismatch,
)
from app.domain.run_control.errors import IdempotencyConflict


class SandboxSnapshotRepository(Protocol):
    async def claim_creation(
        self, snapshot_id: str, creation_identity: str, claimed_at: datetime
    ) -> bool: ...

    async def claim_clone(
        self, request_fingerprint: str, clone_id: str, claimed_at: datetime
    ) -> bool: ...

    async def get_snapshot(self, snapshot_id: str) -> SandboxSnapshot | None: ...

    async def create_snapshot(self, snapshot: SandboxSnapshot) -> SandboxSnapshot: ...

    async def get_clone(self, clone_id: str) -> SnapshotCloneRecord | None: ...

    async def create_clone(self, clone: SnapshotCloneRecord) -> SnapshotCloneRecord: ...


class SnapshotPayloadStore(Protocol):
    async def stage(
        self,
        *,
        snapshot_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> SnapshotPayloadAddress: ...

    async def retrieve(self, address: SnapshotPayloadAddress) -> bytes: ...


class SnapshotSandboxPort(Protocol):
    async def capture(self, request: SandboxSnapshotCreateRequest) -> SandboxSnapshotCapture: ...

    async def clone(
        self,
        *,
        snapshot: SandboxSnapshot,
        payload: bytes,
        request: SnapshotCloneRequest,
    ) -> MaterializedWorkspace: ...

    async def discard_clone(self, request: SnapshotCloneRequest) -> None: ...


class SnapshotAuthorityPort(Protocol):
    async def verify_creation(self, request: SandboxSnapshotCreateRequest) -> None: ...

    async def verify_restore(
        self, request: SnapshotCloneRequest, snapshot: SandboxSnapshot
    ) -> None: ...


class SnapshotBindingRepository(Protocol):
    async def get_binding_by_id(self, binding_id: str) -> OperationExecutionBinding | None: ...


class CurrentSnapshotBindingAuthority(Protocol):
    async def verify_binding(self, binding: OperationExecutionBinding) -> None: ...


class SnapshotWorkspaceManifestRepository(Protocol):
    async def get_current(
        self, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest | None: ...


class SnapshotResourceReacquisitionPort(Protocol):
    async def reacquire(
        self, request: SnapshotCloneRequest, snapshot: SandboxSnapshot
    ) -> ReacquiredRuntimeResources: ...

    async def release(
        self,
        request: SnapshotCloneRequest,
        snapshot: SandboxSnapshot,
        resources: ReacquiredRuntimeResources,
    ) -> None: ...


class BindingSnapshotAuthority:
    """Checks immutable binding lineage and delegates present authz to its authority port."""

    def __init__(
        self,
        *,
        bindings: SnapshotBindingRepository,
        manifests: SnapshotWorkspaceManifestRepository,
        current: CurrentSnapshotBindingAuthority,
    ) -> None:
        self._bindings = bindings
        self._manifests = manifests
        self._current = current

    async def verify_creation(self, request: SandboxSnapshotCreateRequest) -> None:
        binding = await self._bindings.get_binding_by_id(request.producer_binding_id)
        if binding is None:
            raise SnapshotAuthorityError("snapshot producer binding is unavailable")
        await self._current.verify_binding(binding)
        workspace = binding.workspace
        manifest = await self._manifests.get_current(
            request.source_namespace_id, request.source_workspace_id
        )
        if manifest is None or manifest.manifest_digest != request.mount_manifest_digest:
            raise SnapshotAuthorityError("snapshot source manifest is not current and exact")
        if (
            binding.request_scope != request.request_scope
            or workspace.namespace_id != request.source_namespace_id
            or workspace.workspace_id != request.source_workspace_id
            or workspace.provider != request.provider
            or workspace.runtime_digest != request.runtime_digest
            or workspace.image_digest != request.image_digest
            or workspace.package_digest != request.package_digest
            or workspace.environment_digest != request.environment_digest
            or workspace.workflow_contract_digest != request.workspace_contract_digest
            or workspace.restore_snapshot_id != request.parent_snapshot_id
            or binding.snapshot_policy_ref != request.snapshot_policy_ref
            or _capability_shape(binding) != request.capability_shape
        ):
            raise SnapshotAuthorityError(
                "snapshot request does not match its immutable producer binding"
            )

    async def verify_restore(
        self, request: SnapshotCloneRequest, snapshot: SandboxSnapshot
    ) -> None:
        binding = await self._bindings.get_binding_by_id(request.binding_id)
        if binding is None:
            raise SnapshotAuthorityError("restore binding is unavailable")
        await self._current.verify_binding(binding)
        workspace = binding.workspace
        manifest = await self._manifests.get_current(
            request.target_namespace_id, request.target_workspace_id
        )
        if (
            manifest is None
            or manifest.manifest_digest != request.target_mount_manifest_digest
        ):
            raise SnapshotAuthorityError("restore target manifest is not current and exact")
        if (
            binding.request_scope != request.request_scope
            or workspace.restore_snapshot_id != snapshot.snapshot_id
            or workspace.namespace_id != request.target_namespace_id
            or workspace.workspace_id != request.target_workspace_id
            or workspace.provider != snapshot.provider
            or workspace.runtime_digest != request.runtime_digest
            or workspace.image_digest != request.image_digest
            or workspace.package_digest != request.package_digest
            or workspace.environment_digest != request.environment_digest
            or workspace.workflow_contract_digest != request.workspace_contract_digest
            or _capability_shape(binding) != request.capability_shape
        ):
            raise SnapshotAuthorityError(
                "restore request does not match its current accepted operation binding"
            )


class SandboxSnapshotService:
    """Owns immutable metadata, content addressing, and clone-only restoration."""

    def __init__(
        self,
        *,
        snapshots: SandboxSnapshotRepository,
        payloads: SnapshotPayloadStore,
        sandbox: SnapshotSandboxPort,
        authority: SnapshotAuthorityPort,
        resources: SnapshotResourceReacquisitionPort,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._snapshots = snapshots
        self._payloads = payloads
        self._sandbox = sandbox
        self._authority = authority
        self._resources = resources
        self._clock = clock or (lambda: datetime.now(UTC))

    async def create(self, request: SandboxSnapshotCreateRequest) -> SandboxSnapshot:
        creation_identity = sha256_digest(
            request.model_dump(mode="json", exclude={"created_at"})
        )
        prior = await self._snapshots.get_snapshot(request.snapshot_id)
        if prior is not None:
            if prior.creation_identity != creation_identity:
                raise IdempotencyConflict("snapshot identity was reused with conflicting intent")
            return prior

        await self._authority.verify_creation(request)
        if request.parent_snapshot_id is not None:
            parent = await self._snapshots.get_snapshot(request.parent_snapshot_id)
            if (
                parent is None
                or parent.request_scope != request.request_scope
                or parent.provider != request.provider
            ):
                raise SnapshotCompatibilityError("parent snapshot lineage is unavailable")
        claimed = await self._snapshots.claim_creation(
            request.snapshot_id, creation_identity, self._clock()
        )
        if not claimed:
            prior = await self._snapshots.get_snapshot(request.snapshot_id)
            if prior is not None and prior.creation_identity == creation_identity:
                return prior
            raise SnapshotCreationInProgress(
                "snapshot creation has a durable claim without visible immutable metadata"
            )

        capture = await self._sandbox.capture(request)
        payload_digest = _digest_bytes(capture.payload)
        address = await self._payloads.stage(
            snapshot_id=request.snapshot_id,
            content=capture.payload,
            content_digest=payload_digest,
            media_type=capture.media_type,
        )
        _verify_payload(capture.payload, address)
        snapshot = SandboxSnapshot(
            snapshot_id=request.snapshot_id,
            creation_identity=creation_identity,
            request_scope=request.request_scope,
            source_namespace_id=request.source_namespace_id,
            source_workspace_id=request.source_workspace_id,
            parent_snapshot_id=request.parent_snapshot_id,
            provider=request.provider,
            provider_snapshot_id=capture.provider_snapshot_id,
            filesystem_digest=capture.filesystem_digest,
            content_manifest_digest=capture.content_manifest_digest,
            payload=address,
            runtime_digest=request.runtime_digest,
            image_digest=request.image_digest,
            package_digest=request.package_digest,
            environment_digest=request.environment_digest,
            workspace_contract_digest=request.workspace_contract_digest,
            mount_manifest_digest=request.mount_manifest_digest,
            reason=request.reason,
            producer_binding_id=request.producer_binding_id,
            snapshot_policy_ref=request.snapshot_policy_ref,
            capability_shape=request.capability_shape,
            retention=request.retention,
            created_at=request.created_at,
        )
        return await self._snapshots.create_snapshot(snapshot)

    async def clone_restore(self, request: SnapshotCloneRequest) -> SnapshotCloneResult:
        snapshot = await self._snapshots.get_snapshot(request.snapshot_id)
        if snapshot is None:
            raise SnapshotCompatibilityError("snapshot is unavailable")
        self._verify_clone_contract(request, snapshot)
        await self._authority.verify_restore(request, snapshot)

        request_fingerprint = sha256_digest(
            request.model_dump(mode="json", exclude={"requested_at"})
        )
        prior_clone = await self._snapshots.get_clone(request.clone_id)
        if prior_clone is not None:
            if (
                prior_clone.snapshot_id != snapshot.snapshot_id
                or prior_clone.parent_workspace_id != snapshot.source_workspace_id
                or prior_clone.target_namespace_id != request.target_namespace_id
                or prior_clone.target_workspace_id != request.target_workspace_id
                or prior_clone.binding_id != request.binding_id
            ):
                raise IdempotencyConflict(
                    "snapshot clone identity was reused with conflicting intent"
                )
            return SnapshotCloneResult(
                clone_id=prior_clone.clone_id,
                workspace=MaterializedWorkspace(
                    workspace_id=request.target_workspace_id,
                    namespace_id=request.target_namespace_id,
                    provider=snapshot.provider,
                    runtime_digest=request.runtime_digest,
                    image_digest=request.image_digest,
                    mount_manifest_digest=request.target_mount_manifest_digest,
                ),
                parent_snapshot_id=snapshot.snapshot_id,
                parent_workspace_id=snapshot.source_workspace_id,
                resources=prior_clone.resources,
            )
        claimed = await self._snapshots.claim_clone(
            request_fingerprint, request.clone_id, self._clock()
        )
        if not claimed and prior_clone is None:
            raise SnapshotCloneInProgress(
                "snapshot clone has a durable claim without visible clone lineage"
            )

        payload = await self._payloads.retrieve(snapshot.payload)
        _verify_payload(payload, snapshot.payload)
        resources = await self._resources.reacquire(request, snapshot)
        clone_materialized = False
        try:
            workspace = await self._sandbox.clone(
                snapshot=snapshot,
                payload=payload,
                request=request,
            )
            clone_materialized = True
            if (
                workspace.workspace_id != request.target_workspace_id
                or workspace.namespace_id != request.target_namespace_id
                or workspace.provider != snapshot.provider
                or workspace.runtime_digest != request.runtime_digest
                or workspace.image_digest != request.image_digest
                or workspace.mount_manifest_digest != request.target_mount_manifest_digest
            ):
                raise SnapshotCompatibilityError(
                    "sandbox provider returned a workspace outside the admitted clone contract"
                )

            clone = SnapshotCloneRecord(
                clone_id=request.clone_id,
                snapshot_id=snapshot.snapshot_id,
                parent_workspace_id=snapshot.source_workspace_id,
                target_namespace_id=request.target_namespace_id,
                target_workspace_id=request.target_workspace_id,
                binding_id=request.binding_id,
                resources=resources,
                created_at=self._clock(),
            )
            clone = await self._snapshots.create_clone(clone)
        except BaseException:
            try:
                if clone_materialized:
                    await self._sandbox.discard_clone(request)
            finally:
                await self._resources.release(request, snapshot, resources)
            raise
        return SnapshotCloneResult(
            clone_id=clone.clone_id,
            workspace=workspace,
            parent_snapshot_id=snapshot.snapshot_id,
            parent_workspace_id=snapshot.source_workspace_id,
            resources=resources,
        )

    def _verify_clone_contract(
        self, request: SnapshotCloneRequest, snapshot: SandboxSnapshot
    ) -> None:
        if request.target_workspace_id == snapshot.source_workspace_id:
            raise SnapshotCompatibilityError(
                "snapshot restore must create a new workspace identity"
            )
        if request.request_scope != snapshot.request_scope:
            raise SnapshotCompatibilityError("snapshot restore scope does not match its owner")
        if (
            snapshot.retention.retain_until is not None
            and self._clock() >= snapshot.retention.retain_until
        ):
            raise SnapshotCompatibilityError("snapshot retention has expired")
        compatibility = (
            (request.runtime_digest, snapshot.runtime_digest, "runtime"),
            (request.image_digest, snapshot.image_digest, "image"),
            (request.package_digest, snapshot.package_digest, "package"),
            (request.environment_digest, snapshot.environment_digest, "environment"),
            (
                request.workspace_contract_digest,
                snapshot.workspace_contract_digest,
                "workspace contract",
            ),
        )
        mismatches = [name for current, saved, name in compatibility if current != saved]
        if request.capability_shape != snapshot.capability_shape:
            mismatches.append("capability shape")
        if mismatches:
            raise SnapshotMigrationRequired(
                "incompatible snapshot restore requires an authored migration as a new "
                "semantic operation: " + ", ".join(mismatches)
            )


class InMemorySandboxSnapshotRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._snapshots: dict[str, SandboxSnapshot] = {}
        self._clones: dict[str, SnapshotCloneRecord] = {}
        self._workspace_clones: dict[tuple[str, str], str] = {}
        self._claims: dict[str, str] = {}

    async def claim_creation(
        self, snapshot_id: str, creation_identity: str, claimed_at: datetime
    ) -> bool:
        del claimed_at
        return await self._claim(f"creation:{snapshot_id}", creation_identity)

    async def claim_clone(
        self, request_fingerprint: str, clone_id: str, claimed_at: datetime
    ) -> bool:
        del claimed_at
        return await self._claim(f"clone:{clone_id}", request_fingerprint)

    async def get_snapshot(self, snapshot_id: str) -> SandboxSnapshot | None:
        return deepcopy(self._snapshots.get(snapshot_id))

    async def create_snapshot(self, snapshot: SandboxSnapshot) -> SandboxSnapshot:
        async with self._lock:
            prior = self._snapshots.get(snapshot.snapshot_id)
            if prior is not None:
                if prior != snapshot:
                    raise IdempotencyConflict("snapshot identity conflict")
                return deepcopy(prior)
            self._snapshots[snapshot.snapshot_id] = deepcopy(snapshot)
            return deepcopy(snapshot)

    async def get_clone(self, clone_id: str) -> SnapshotCloneRecord | None:
        return deepcopy(self._clones.get(clone_id))

    async def create_clone(self, clone: SnapshotCloneRecord) -> SnapshotCloneRecord:
        async with self._lock:
            prior = self._clones.get(clone.clone_id)
            if prior is not None:
                if prior != clone:
                    raise IdempotencyConflict("snapshot clone identity conflict")
                return deepcopy(prior)
            workspace_key = (clone.target_namespace_id, clone.target_workspace_id)
            prior_clone_id = self._workspace_clones.get(workspace_key)
            if prior_clone_id is not None and prior_clone_id != clone.clone_id:
                raise IdempotencyConflict("target workspace already has snapshot lineage")
            self._clones[clone.clone_id] = deepcopy(clone)
            self._workspace_clones[workspace_key] = clone.clone_id
            return deepcopy(clone)

    async def _claim(self, key: str, fingerprint: str) -> bool:
        async with self._lock:
            prior = self._claims.get(key)
            if prior is not None:
                if prior != fingerprint:
                    raise IdempotencyConflict("snapshot side-effect claim conflicts")
                return False
            self._claims[key] = fingerprint
            return True


class InMemorySnapshotPayloadStore:
    def __init__(self, payloads: Mapping[str, bytes] | None = None) -> None:
        self.payloads = dict(payloads or {})

    async def stage(
        self,
        *,
        snapshot_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> SnapshotPayloadAddress:
        del snapshot_id
        if _digest_bytes(content) != content_digest:
            raise SnapshotPayloadMismatch("snapshot payload digest is invalid")
        object_ref = f"memory://snapshots/sha256/{content_digest.removeprefix('sha256:')}"
        prior = self.payloads.get(object_ref)
        if prior is not None and prior != content:
            raise SnapshotPayloadMismatch("content address contains conflicting snapshot bytes")
        self.payloads[object_ref] = content
        return SnapshotPayloadAddress(
            object_ref=object_ref,
            content_digest=content_digest,
            size_bytes=len(content),
            media_type=media_type,
        )

    async def retrieve(self, address: SnapshotPayloadAddress) -> bytes:
        try:
            content = self.payloads[address.object_ref]
        except KeyError as error:
            raise SnapshotPayloadMismatch("snapshot payload is unavailable") from error
        _verify_payload(content, address)
        return content


def _verify_payload(content: bytes, address: SnapshotPayloadAddress) -> None:
    if _digest_bytes(content) != address.content_digest or len(content) != address.size_bytes:
        raise SnapshotPayloadMismatch("snapshot payload does not match immutable metadata")


def _digest_bytes(content: bytes) -> str:
    return f"sha256:{sha256(content).hexdigest()}"


def _capability_shape(binding: OperationExecutionBinding) -> SnapshotCapabilityShape:
    grant = binding.capability_grant
    return SnapshotCapabilityShape(
        capabilities=grant.capabilities,
        tool_ids=grant.tool_ids,
        mcp_server_ids=grant.mcp_server_ids,
        data_scope_refs=grant.data_scope_refs,
        network_hosts=grant.network_hosts,
        network_policy=binding.workspace.network_policy,
    )
