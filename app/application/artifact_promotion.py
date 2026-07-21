from __future__ import annotations

import asyncio
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
from hashlib import sha256
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, ConfigDict, Field

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    ArtifactMetadataRevision,
    ArtifactPromotionRequest,
    ArtifactPromotionState,
    LocalCandidateManifestEntry,
    OperationExecutionBinding,
    PromotedArtifact,
    PromotedArtifactManifestEntry,
    WorkspaceMaterializationManifest,
)
from app.domain.operation_execution.errors import (
    UndeclaredWorkspacePath,
    WorkspaceDigestMismatch,
)
from app.domain.run_control.errors import IdempotencyConflict

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"


class ArtifactPayloadAddress(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    object_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    size_bytes: int = Field(ge=0)


class ArtifactBindingRepository(Protocol):
    async def get_binding_by_id(self, binding_id: str) -> OperationExecutionBinding | None: ...


class ArtifactMetadataRepository(Protocol):
    async def get_by_intent(self, intent_key: str) -> ArtifactMetadataRevision | None: ...

    async def get_by_artifact(self, artifact_id: str) -> ArtifactMetadataRevision | None: ...

    async def append(self, revision: ArtifactMetadataRevision) -> ArtifactMetadataRevision: ...

    async def reconciliation_required(
        self,
    ) -> tuple[ArtifactMetadataRevision, ...]: ...

    async def rejected(self) -> tuple[ArtifactMetadataRevision, ...]: ...


class ArtifactPayloadPort(Protocol):
    async def stage(
        self,
        *,
        artifact_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> ArtifactPayloadAddress: ...

    async def retrieve(self, address: ArtifactPayloadAddress) -> bytes: ...


class ArtifactDurableReferencePort(Protocol):
    async def admit(
        self,
        *,
        request_scope: str,
        run_id: str,
        artifact: ArtifactMetadataRevision,
    ) -> str: ...

    async def get(self, request_scope: str, artifact_id: str) -> str | None: ...


class ArtifactWorkspacePort(Protocol):
    async def current_manifest(
        self, namespace_id: str, workspace_id: str
    ) -> WorkspaceMaterializationManifest: ...

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
    ) -> WorkspaceMaterializationManifest: ...


class ArtifactValidationAuthorityPort(Protocol):
    async def verify(
        self,
        request: ArtifactPromotionRequest,
        binding: OperationExecutionBinding,
    ) -> None: ...


class ArtifactPromotionService:
    """Retry-safe application boundary for the only workspace-to-artifact path."""

    def __init__(
        self,
        *,
        bindings: ArtifactBindingRepository,
        metadata: ArtifactMetadataRepository,
        payloads: ArtifactPayloadPort,
        workspaces: ArtifactWorkspacePort,
        durable_references: ArtifactDurableReferencePort,
        validation_authority: ArtifactValidationAuthorityPort,
    ) -> None:
        self._bindings = bindings
        self._metadata = metadata
        self._payloads = payloads
        self._workspaces = workspaces
        self._durable_references = durable_references
        self._validation_authority = validation_authority

    async def promote(self, request: ArtifactPromotionRequest, content: bytes) -> PromotedArtifact:
        binding = await self._validate_authority(request, content)
        intent_key = _stable_id(
            "artifact-intent",
            binding.run_id,
            binding.semantic_attempt_key,
            request.output_slot,
            request.candidate_id,
        )
        identity = sha256_digest(
            {
                "run_id": binding.run_id,
                "semantic_attempt_key": binding.semantic_attempt_key,
                "output_slot": request.output_slot,
                "candidate_id": request.candidate_id,
                "content_digest": request.content_digest,
            }
        )
        artifact_id = _stable_id("artifact", identity)
        promotion_id = _stable_id("artifact-promotion", intent_key)
        current = await self._metadata.get_by_intent(intent_key)
        if current is not None and current.promotion_identity != identity:
            raise IdempotencyConflict(
                "artifact candidate identity was reused with conflicting content"
            )
        await self._validate_candidate(request)
        if current is not None and current.state == ArtifactPromotionState.ADMITTED:
            visible = await self.get_visible(current.artifact_id)
            if visible is None:
                durable_reference = await self._durable_references.admit(
                    request_scope=request.request_scope,
                    run_id=binding.run_id,
                    artifact=current,
                )
                if durable_reference != current.durable_reference:
                    raise IdempotencyConflict("durable artifact reference conflict")
                visible = await self.get_visible(current.artifact_id)
            if visible is None:
                raise RuntimeError("admitted artifact failed its visibility predicate")
            return visible
        if current is not None and current.state == ArtifactPromotionState.REJECTED:
            raise ValueError(f"artifact promotion was rejected: {current.reason}")
        if current is None:
            current = await self._metadata.append(
                self._revision(
                    request=request,
                    binding=binding,
                    promotion_id=promotion_id,
                    artifact_id=artifact_id,
                    intent_key=intent_key,
                    identity=identity,
                    revision=1,
                    state=ArtifactPromotionState.CANDIDATE,
                )
            )
        failed_checks = [
            check.check_id
            for check in request.checks
            if check.required and check.outcome != "passed"
        ]
        if request.permission_outcome not in {"allowed", "allowed_with_conditions"}:
            failed_checks.append("permission")
        if failed_checks:
            current = await self._append_state(
                current,
                request,
                binding,
                ArtifactPromotionState.REJECTED,
                reason="required_validation_failed:" + ",".join(sorted(failed_checks)),
            )
            raise ValueError(f"artifact promotion was rejected: {current.reason}")
        address: ArtifactPayloadAddress | None = None
        try:
            address = await self._payloads.stage(
                artifact_id=artifact_id,
                content=content,
                content_digest=request.content_digest,
                media_type=request.media_type,
            )
            current = await self._append_state(
                current,
                request,
                binding,
                ArtifactPromotionState.PAYLOAD_STAGED,
                object_ref=address.object_ref,
            )
            verified = await self._payloads.retrieve(address)
            if _digest_bytes(verified) != request.content_digest:
                raise WorkspaceDigestMismatch("staged artifact payload failed verification")
            manifest = await self._workspaces.link_promoted_artifact(
                namespace_id=request.namespace_id,
                workspace_id=request.workspace_id,
                candidate_id=request.candidate_id,
                artifact_id=artifact_id,
                artifact_metadata_revision=current.revision + 2,
                content_digest=request.content_digest,
                recorded_at=request.requested_at,
            )
            current = await self._append_state(
                current,
                request,
                binding,
                ArtifactPromotionState.METADATA_COMMITTED,
                object_ref=address.object_ref,
                manifest_revision=manifest.revision,
            )
            durable_reference = artifact_durable_reference(
                request.request_scope, binding.run_id, artifact_id
            )
            current = await self._append_state(
                current,
                request,
                binding,
                ArtifactPromotionState.ADMITTED,
                object_ref=address.object_ref,
                manifest_revision=manifest.revision,
                durable_reference=durable_reference,
            )
            admitted_reference = await self._durable_references.admit(
                request_scope=request.request_scope,
                run_id=binding.run_id,
                artifact=current,
            )
            if admitted_reference != durable_reference:
                raise IdempotencyConflict("durable artifact reference conflict")
        except Exception as error:
            if current.state not in {
                ArtifactPromotionState.ADMITTED,
                ArtifactPromotionState.REJECTED,
                ArtifactPromotionState.RECONCILIATION_REQUIRED,
            }:
                await self._append_state(
                    current,
                    request,
                    binding,
                    ArtifactPromotionState.RECONCILIATION_REQUIRED,
                    object_ref=address.object_ref if address is not None else current.object_ref,
                    manifest_revision=current.manifest_revision,
                    reason=type(error).__name__,
                )
            raise
        visible = await self.get_visible(artifact_id)
        if visible is None:
            raise RuntimeError("promotion completed without satisfying visibility predicate")
        return visible

    async def get_visible(self, artifact_id: str) -> PromotedArtifact | None:
        metadata = await self._metadata.get_by_artifact(artifact_id)
        if (
            metadata is None
            or metadata.state != ArtifactPromotionState.ADMITTED
            or metadata.object_ref is None
            or metadata.manifest_revision is None
            or metadata.durable_reference is None
        ):
            return None
        durable_reference = await self._durable_references.get(metadata.request_scope, artifact_id)
        if durable_reference != metadata.durable_reference:
            return None
        address = ArtifactPayloadAddress(
            object_ref=metadata.object_ref,
            content_digest=metadata.content_digest,
            size_bytes=metadata.size_bytes,
        )
        try:
            content = await self._payloads.retrieve(address)
            manifest = await self._workspaces.current_manifest(
                metadata.namespace_id, metadata.workspace_id
            )
        except Exception:
            return None
        if _digest_bytes(content) != metadata.content_digest:
            return None
        linked = next(
            (
                entry
                for entry in manifest.entries
                if entry.kind == "promoted_artifact"
                and entry.artifact_id == artifact_id
                and entry.candidate_id == metadata.candidate_id
                and entry.content_digest == metadata.content_digest
            ),
            None,
        )
        if linked is None or linked.artifact_metadata_revision != metadata.revision:
            return None
        return PromotedArtifact(
            artifact_id=artifact_id,
            content_digest=metadata.content_digest,
            object_ref=metadata.object_ref,
            metadata_revision=metadata.revision,
            manifest_revision=metadata.manifest_revision,
            durable_reference=metadata.durable_reference,
            status="admitted",
        )

    async def _validate_authority(
        self, request: ArtifactPromotionRequest, content: bytes
    ) -> OperationExecutionBinding:
        binding = await self._bindings.get_binding_by_id(request.binding_id)
        if binding is None:
            raise ValueError("artifact producer binding does not exist")
        if (
            binding.request_scope != request.request_scope
            or binding.workspace.namespace_id != request.namespace_id
            or binding.workspace.workspace_id != request.workspace_id
        ):
            raise ValueError("artifact promotion exceeds producer binding scope")
        if "artifact.promote" not in binding.capability_grant.capabilities:
            raise ValueError("producer binding lacks artifact promotion authority")
        if request.output_contract_ref != binding.operation_contract_ref:
            raise ValueError("artifact output contract does not match producer binding")
        await self._validation_authority.verify(request, binding)
        if len(content) != request.size_bytes or _digest_bytes(content) != request.content_digest:
            raise WorkspaceDigestMismatch("candidate payload does not match declared metadata")
        return binding

    async def _validate_candidate(self, request: ArtifactPromotionRequest) -> None:
        manifest = await self._workspaces.current_manifest(
            request.namespace_id, request.workspace_id
        )
        candidate: LocalCandidateManifestEntry | PromotedArtifactManifestEntry | None = None
        for entry in manifest.entries:
            if not isinstance(entry, (LocalCandidateManifestEntry, PromotedArtifactManifestEntry)):
                continue
            if (
                entry.candidate_id == request.candidate_id
                and entry.slot_name == request.output_slot
            ):
                candidate = entry
                break
        if (
            candidate is None
            or candidate.logical_path != request.logical_path
            or candidate.owner != request.owner
            or candidate.content_digest != request.content_digest
            or (
                candidate.kind == "local_candidate"
                and (
                    candidate.media_type != request.media_type
                    or candidate.size_bytes != request.size_bytes
                )
            )
        ):
            raise UndeclaredWorkspacePath(
                "promotion candidate is not the current owned manifest entry"
            )

    async def _append_state(
        self,
        current: ArtifactMetadataRevision,
        request: ArtifactPromotionRequest,
        binding: OperationExecutionBinding,
        state: ArtifactPromotionState,
        *,
        object_ref: str | None = None,
        manifest_revision: int | None = None,
        durable_reference: str | None = None,
        reason: str | None = None,
    ) -> ArtifactMetadataRevision:
        if current.state == state:
            return current
        return await self._metadata.append(
            self._revision(
                request=request,
                binding=binding,
                promotion_id=current.promotion_id,
                artifact_id=current.artifact_id,
                intent_key=current.intent_key,
                identity=current.promotion_identity,
                revision=current.revision + 1,
                state=state,
                object_ref=object_ref or current.object_ref,
                manifest_revision=manifest_revision or current.manifest_revision,
                durable_reference=durable_reference or current.durable_reference,
                reason=reason,
            )
        )

    @staticmethod
    def _revision(
        *,
        request: ArtifactPromotionRequest,
        binding: OperationExecutionBinding,
        promotion_id: str,
        artifact_id: str,
        intent_key: str,
        identity: str,
        revision: int,
        state: ArtifactPromotionState,
        object_ref: str | None = None,
        manifest_revision: int | None = None,
        durable_reference: str | None = None,
        reason: str | None = None,
    ) -> ArtifactMetadataRevision:
        return ArtifactMetadataRevision(
            promotion_id=promotion_id,
            artifact_id=artifact_id,
            intent_key=intent_key,
            promotion_identity=identity,
            revision=revision,
            state=state,
            request_scope=request.request_scope,
            run_id=binding.run_id,
            semantic_attempt_key=binding.semantic_attempt_key,
            producer_binding_id=binding.binding_id,
            namespace_id=request.namespace_id,
            workspace_id=request.workspace_id,
            output_slot=request.output_slot,
            logical_path=request.logical_path,
            owner=request.owner,
            candidate_id=request.candidate_id,
            content_digest=request.content_digest,
            media_type=request.media_type,
            size_bytes=request.size_bytes,
            permission_ref=request.permission_ref,
            permission_outcome=request.permission_outcome,
            output_contract_ref=request.output_contract_ref,
            checks=request.checks,
            object_ref=object_ref,
            manifest_revision=manifest_revision,
            durable_reference=durable_reference,
            reason=reason,
            recorded_at=request.requested_at,
        )


class InMemoryArtifactMetadataRepository:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._revisions: dict[str, list[ArtifactMetadataRevision]] = {}
        self._intent_artifacts: dict[str, str] = {}

    async def get_by_intent(self, intent_key: str) -> ArtifactMetadataRevision | None:
        artifact_id = self._intent_artifacts.get(intent_key)
        return await self.get_by_artifact(artifact_id) if artifact_id is not None else None

    async def get_by_artifact(self, artifact_id: str) -> ArtifactMetadataRevision | None:
        values = self._revisions.get(artifact_id, [])
        return deepcopy(values[-1]) if values else None

    async def append(self, revision: ArtifactMetadataRevision) -> ArtifactMetadataRevision:
        async with self._lock:
            prior_artifact = self._intent_artifacts.get(revision.intent_key)
            if prior_artifact is not None and prior_artifact != revision.artifact_id:
                raise IdempotencyConflict("artifact intent belongs to another artifact")
            values = self._revisions.setdefault(revision.artifact_id, [])
            if values:
                prior = values[-1]
                if revision.revision <= prior.revision:
                    matching = next(
                        (item for item in values if item.revision == revision.revision),
                        None,
                    )
                    if matching == revision:
                        return deepcopy(matching)
                    raise IdempotencyConflict("artifact metadata revision conflict")
                if revision.revision != prior.revision + 1:
                    raise IdempotencyConflict("artifact metadata revision gap")
            elif revision.revision != 1:
                raise IdempotencyConflict("first artifact metadata revision must be one")
            self._intent_artifacts[revision.intent_key] = revision.artifact_id
            values.append(deepcopy(revision))
            return deepcopy(revision)

    async def list_revisions(self, artifact_id: str) -> tuple[ArtifactMetadataRevision, ...]:
        return tuple(deepcopy(self._revisions.get(artifact_id, [])))

    async def reconciliation_required(
        self,
    ) -> tuple[ArtifactMetadataRevision, ...]:
        return tuple(
            deepcopy(values[-1])
            for values in self._revisions.values()
            if values[-1].state == ArtifactPromotionState.RECONCILIATION_REQUIRED
        )

    async def rejected(self) -> tuple[ArtifactMetadataRevision, ...]:
        return tuple(
            deepcopy(values[-1])
            for values in self._revisions.values()
            if values[-1].state == ArtifactPromotionState.REJECTED
        )


class InMemoryArtifactDurableReferences:
    def __init__(self) -> None:
        self.references: dict[str, str] = {}

    async def admit(
        self,
        *,
        request_scope: str,
        run_id: str,
        artifact: ArtifactMetadataRevision,
    ) -> str:
        reference = artifact_durable_reference(request_scope, run_id, artifact.artifact_id)
        prior = self.references.get(artifact.artifact_id)
        if prior is not None and prior != reference:
            raise IdempotencyConflict("durable artifact reference conflict")
        self.references[artifact.artifact_id] = reference
        return reference

    async def get(self, request_scope: str, artifact_id: str) -> str | None:
        del request_scope
        return self.references.get(artifact_id)


class StaticArtifactValidationAuthority:
    """Immutable fixture authority until governed validation catalogs land."""

    def __init__(
        self,
        *,
        permission_outcomes: Mapping[tuple[str, str], str],
        check_outcomes: Mapping[tuple[str, str, str], str],
        required_check_ids: Mapping[str, frozenset[str]],
    ) -> None:
        self._permission_outcomes = dict(permission_outcomes)
        self._check_outcomes = dict(check_outcomes)
        self._required_check_ids = dict(required_check_ids)

    async def verify(
        self,
        request: ArtifactPromotionRequest,
        binding: OperationExecutionBinding,
    ) -> None:
        contract_ref = binding.operation_contract_ref
        if (
            self._permission_outcomes.get((contract_ref, request.permission_ref))
            != request.permission_outcome
        ):
            raise ValueError("artifact permission evidence is not authoritative")
        observed_ids = {check.check_id for check in request.checks}
        missing = self._required_check_ids.get(contract_ref, frozenset()) - observed_ids
        if missing:
            raise ValueError("required artifact checks are missing: " + ", ".join(sorted(missing)))
        for check in request.checks:
            if (
                check.check_id in self._required_check_ids.get(contract_ref, frozenset())
                and not check.required
            ):
                raise ValueError(f"required artifact check was marked optional: {check.check_id}")
            expected = self._check_outcomes.get((contract_ref, check.check_id, check.evidence_ref))
            if expected != check.outcome:
                raise ValueError(f"artifact check outcome is not authoritative: {check.check_id}")


def _digest_bytes(value: bytes) -> str:
    return f"sha256:{sha256(value).hexdigest()}"


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))


def artifact_durable_reference(request_scope: str, run_id: str, artifact_id: str) -> str:
    return f"artifact://{request_scope}/{run_id}/{artifact_id}"
