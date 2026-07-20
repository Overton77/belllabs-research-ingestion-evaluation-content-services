from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest

from app.application.artifact_promotion import (
    ArtifactPromotionService,
    InMemoryArtifactDurableReferences,
    InMemoryArtifactMetadataRepository,
    StaticArtifactValidationAuthority,
)
from app.application.workspace_materialization import (
    InMemoryDurableWorkspaceInputs,
    InMemoryWorkspaceManifestRepository,
    WorkspaceMaterializationService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    ArtifactCheckEvidence,
    ArtifactPromotionRequest,
    ArtifactPromotionState,
    WorkspaceMaterializationRequest,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.artifact_payloads import InMemoryArtifactPayloadStore
from tests.test_operation_execution import operation_request, service_fixture
from tests.test_workspace_materialization import RecordingProvisioner

CONTENT = b"# Generic research\n\nInitial finding.\n\nPatched conclusion.\n"
CONTENT_DIGEST = f"sha256:{sha256(CONTENT).hexdigest()}"
OWNER = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:generic-research")
NOW = datetime(2026, 7, 20, 19, 0, tzinfo=UTC)


class FailOnceRetrievePayloadStore(InMemoryArtifactPayloadStore):
    def __init__(self) -> None:
        super().__init__()
        self.failures = 1

    async def retrieve(self, address):
        if self.failures:
            self.failures -= 1
            raise RuntimeError("injected object verification failure")
        return await super().retrieve(address)


class LoseFirstAdmissionResponse(InMemoryArtifactDurableReferences):
    def __init__(self) -> None:
        super().__init__()
        self.failures = 1

    async def admit(self, *, request_scope, run_id, artifact):
        reference = await super().admit(
            request_scope=request_scope,
            run_id=run_id,
            artifact=artifact,
        )
        if self.failures:
            self.failures -= 1
            raise RuntimeError("injected lost PostgreSQL response")
        return reference


class FailOnceMetadataCommit(InMemoryArtifactMetadataRepository):
    def __init__(self) -> None:
        super().__init__()
        self.failures = 1

    async def append(self, revision):
        if revision.state == ArtifactPromotionState.METADATA_COMMITTED and self.failures:
            self.failures -= 1
            raise RuntimeError("injected metadata commit failure")
        return await super().append(revision)


async def promotion_fixture(payloads=None, durable=None, metadata=None):
    operation_service, bindings, *_ = service_fixture()
    base = operation_request()
    contract_digest = sha256_digest("generic-workspace-contract@1")
    slot = WorkspaceSlotBinding(
        slot_name="report",
        logical_path="/workspace/output",
        access="exclusive_write",
        owner=OWNER,
    )
    request = base.model_copy(
        update={
            "capability_grant": base.capability_grant.model_copy(
                update={
                    "capabilities": base.capability_grant.capabilities | {"artifact.promote"},
                    "data_scope_refs": frozenset(
                        {
                            "permission:generic-research@1",
                            "digest:" + CONTENT_DIGEST,
                            "finding:failure",
                        }
                    ),
                }
            ),
            "workspace": base.workspace.model_copy(
                update={
                    "workflow_contract_digest": contract_digest,
                    "slot_bindings": (slot,),
                    "exclusive_write_paths": (slot.logical_path,),
                }
            ),
        }
    )
    result = await operation_service.execute(request)
    assert result.status == "completed"

    workspace_repository = InMemoryWorkspaceManifestRepository()
    workspaces = WorkspaceMaterializationService(
        manifests=workspace_repository,
        provisioner=RecordingProvisioner(),
        durable_inputs=InMemoryDurableWorkspaceInputs(),
    )
    await workspaces.materialize(
        WorkspaceMaterializationRequest(
            namespace_id=request.workspace.namespace_id,
            workspace_id=request.workspace.workspace_id,
            provider=request.workspace.provider,
            template_ref=request.workspace.template_ref,
            workflow_contract_digest=contract_digest,
            slots=(slot,),
            runtime_digest=request.workspace.runtime_digest,
            image_digest=request.workspace.image_digest,
            created_at=NOW,
        )
    )
    await workspaces.register_candidate(
        namespace_id=request.workspace.namespace_id,
        workspace_id=request.workspace.workspace_id,
        slot_name=slot.slot_name,
        logical_path="/workspace/output/report.md",
        owner=OWNER,
        candidate_id="candidate:report",
        content=CONTENT,
        content_digest=CONTENT_DIGEST,
        media_type="text/markdown",
        recorded_at=NOW,
    )
    metadata = metadata or InMemoryArtifactMetadataRepository()
    durable = durable or InMemoryArtifactDurableReferences()
    service = ArtifactPromotionService(
        bindings=bindings,
        metadata=metadata,
        payloads=payloads or InMemoryArtifactPayloadStore(),
        workspaces=workspaces,
        durable_references=durable,
        validation_authority=StaticArtifactValidationAuthority(
            permission_outcomes={
                (
                    "operation:sandbox-agent@1",
                    "permission:generic-research@1",
                ): "allowed"
            },
            check_outcomes={
                (
                    "operation:sandbox-agent@1",
                    "content-integrity",
                    "digest:" + CONTENT_DIGEST,
                ): "passed",
                (
                    "operation:sandbox-agent@1",
                    "declared-policy-check",
                    "finding:failure",
                ): "failed",
            },
            required_check_ids={"operation:sandbox-agent@1": frozenset({"content-integrity"})},
        ),
    )
    promotion = ArtifactPromotionRequest(
        request_scope=request.request_scope,
        binding_id=result.binding_id,
        namespace_id=request.workspace.namespace_id,
        workspace_id=request.workspace.workspace_id,
        output_slot=slot.slot_name,
        logical_path="/workspace/output/report.md",
        owner=OWNER,
        candidate_id="candidate:report",
        content_digest=CONTENT_DIGEST,
        media_type="text/markdown",
        size_bytes=len(CONTENT),
        permission_ref="permission:generic-research@1",
        permission_outcome="allowed",
        output_contract_ref=request.operation_contract_ref,
        checks=(
            ArtifactCheckEvidence(
                check_id="content-integrity",
                outcome="passed",
                evidence_ref="digest:" + CONTENT_DIGEST,
            ),
        ),
        requested_at=NOW,
    )
    return service, metadata, durable, workspaces, promotion


async def test_promotion_is_visible_only_after_all_authorities_agree() -> None:
    service, metadata, durable, workspaces, request = await promotion_fixture()

    promoted = await service.promote(request, CONTENT)
    replayed = await service.promote(request, CONTENT)
    revisions = await metadata.list_revisions(promoted.artifact_id)
    manifest = await workspaces.current_manifest(request.namespace_id, request.workspace_id)

    assert promoted == replayed
    assert promoted.status == "admitted"
    assert tuple(item.state for item in revisions) == (
        ArtifactPromotionState.CANDIDATE,
        ArtifactPromotionState.PAYLOAD_STAGED,
        ArtifactPromotionState.METADATA_COMMITTED,
        ArtifactPromotionState.ADMITTED,
    )
    assert manifest.entries[-1].kind == "promoted_artifact"
    assert manifest.entries[-1].artifact_metadata_revision == promoted.metadata_revision
    assert (
        await durable.get(request.request_scope, promoted.artifact_id) == promoted.durable_reference
    )
    assert await service.get_visible(promoted.artifact_id) == promoted
    notes = b"unrelated workspace note"
    await workspaces.register_candidate(
        namespace_id=request.namespace_id,
        workspace_id=request.workspace_id,
        slot_name=request.output_slot,
        logical_path="/workspace/output/notes.md",
        owner=request.owner,
        candidate_id="candidate:notes",
        content=notes,
        content_digest=f"sha256:{sha256(notes).hexdigest()}",
        media_type="text/markdown",
    )
    assert await service.get_visible(promoted.artifact_id) == promoted


async def test_conflicting_digest_under_same_candidate_identity_is_rejected() -> None:
    service, _metadata, _durable, _workspaces, request = await promotion_fixture()
    await service.promote(request, CONTENT)
    changed = CONTENT + b"changed"

    with pytest.raises(IdempotencyConflict):
        await service.promote(
            request.model_copy(
                update={
                    "content_digest": f"sha256:{sha256(changed).hexdigest()}",
                    "size_bytes": len(changed),
                }
            ),
            changed,
        )


async def test_partial_payload_failure_is_invisible_and_retry_completes() -> None:
    payloads = FailOnceRetrievePayloadStore()
    service, metadata, _durable, _workspaces, request = await promotion_fixture(payloads)

    with pytest.raises(RuntimeError, match="injected"):
        await service.promote(request, CONTENT)
    current = (await metadata.reconciliation_required())[0]
    assert current.state == ArtifactPromotionState.RECONCILIATION_REQUIRED
    assert await service.get_visible(current.artifact_id) is None

    promoted = await service.promote(request, CONTENT)
    assert promoted.status == "admitted"
    states = tuple(item.state for item in await metadata.list_revisions(promoted.artifact_id))
    assert ArtifactPromotionState.RECONCILIATION_REQUIRED in states
    assert states[-1] == ArtifactPromotionState.ADMITTED


async def test_failed_required_check_is_recorded_as_rejected() -> None:
    service, metadata, _durable, _workspaces, request = await promotion_fixture()
    rejected = request.model_copy(
        update={
            "checks": (
                request.checks[0],
                ArtifactCheckEvidence(
                    check_id="declared-policy-check",
                    outcome="failed",
                    evidence_ref="finding:failure",
                ),
            )
        }
    )

    with pytest.raises(ValueError, match="rejected"):
        await service.promote(rejected, CONTENT)
    current = (await metadata.rejected())[0]
    assert current is not None
    assert current.state == ArtifactPromotionState.REJECTED


async def test_required_check_cannot_be_omitted_or_marked_optional() -> None:
    service, _metadata, _durable, _workspaces, request = await promotion_fixture()

    with pytest.raises(ValueError, match="missing"):
        await service.promote(request.model_copy(update={"checks": ()}), CONTENT)
    optional = request.model_copy(
        update={
            "checks": (
                request.checks[0].model_copy(update={"required": False, "outcome": "failed"}),
            )
        }
    )
    with pytest.raises(ValueError, match="marked optional"):
        await service.promote(optional, CONTENT)


async def test_lost_postgres_response_retries_exact_admitted_revision() -> None:
    durable = LoseFirstAdmissionResponse()
    service, metadata, _durable, _workspaces, request = await promotion_fixture(durable=durable)

    with pytest.raises(RuntimeError, match="lost PostgreSQL response"):
        await service.promote(request, CONTENT)
    admitted = next(
        revision
        for revision in await metadata.list_revisions(next(iter(durable.references)))
        if revision.state == ArtifactPromotionState.ADMITTED
    )
    assert await service.get_visible(admitted.artifact_id) is not None

    replayed = await service.promote(request, CONTENT)
    revisions = await metadata.list_revisions(replayed.artifact_id)
    assert sum(item.state == ArtifactPromotionState.ADMITTED for item in revisions) == 1


async def test_retry_relinks_manifest_to_eventual_admitted_revision() -> None:
    metadata = FailOnceMetadataCommit()
    service, _metadata, _durable, workspaces, request = await promotion_fixture(metadata=metadata)

    with pytest.raises(RuntimeError, match="metadata commit"):
        await service.promote(request, CONTENT)
    promoted = await service.promote(request, CONTENT)
    manifest = await workspaces.current_manifest(request.namespace_id, request.workspace_id)

    linked = manifest.entries[-1]
    assert linked.kind == "promoted_artifact"
    assert linked.artifact_metadata_revision == promoted.metadata_revision
