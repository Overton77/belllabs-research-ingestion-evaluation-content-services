from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha256

import pytest

from app.application.workspace_materialization import (
    InMemoryDurableWorkspaceInputs,
    InMemoryWorkspaceManifestRepository,
    WorkspaceMaterializationService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    WorkspaceMaterializationRequest,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from app.domain.operation_execution.errors import (
    UndeclaredWorkspacePath,
    WorkspaceDigestMismatch,
    WorkspaceSlotConflict,
)
from app.integrations.filesystem_workspace import (
    FilesystemWorkspaceProvisioner,
    is_read_only,
)

INPUT = b"immutable governed input"
INPUT_DIGEST = f"sha256:{sha256(INPUT).hexdigest()}"
RUNTIME_DIGEST = sha256_digest("workspace-runtime")
IMAGE_DIGEST = sha256_digest("workspace-image")
CONTRACT_DIGEST = sha256_digest("workflow-workspace-contract")


class RecordingProvisioner:
    def __init__(self) -> None:
        self.inputs: dict[str, bytes] = {}

    async def provision(self, request, manifest, durable_inputs):
        self.inputs = dict(durable_inputs)
        return MaterializedWorkspace(
            workspace_id=request.workspace_id,
            namespace_id=request.namespace_id,
            provider=request.provider,
            runtime_digest=request.runtime_digest,
            image_digest=request.image_digest,
            mount_manifest_digest=manifest.manifest_digest,
            manifest_revision=manifest.revision,
        )


def owner(
    owner_id: str = "stage:research",
    kind: WorkspaceOwnerKind = WorkspaceOwnerKind.STAGE,
    parent_owner_id: str | None = None,
) -> WorkspaceOwner:
    return WorkspaceOwner(
        kind=kind,
        owner_id=owner_id,
        parent_owner_id=parent_owner_id,
    )


def request(
    *,
    workspace_id: str = "workspace-1",
    write_owner: WorkspaceOwner | None = None,
) -> WorkspaceMaterializationRequest:
    return WorkspaceMaterializationRequest(
        namespace_id="run-workspace:run-1",
        workspace_id=workspace_id,
        provider="conformance-filesystem",
        template_ref=ExactDefinitionRef(
            kind=DefinitionKind.WORKSPACE_TEMPLATE,
            logical_id="generic-workspace",
            revision=1,
            digest=sha256_digest("generic-workspace@1"),
        ),
        workflow_contract_digest=CONTRACT_DIGEST,
        slots=(
            WorkspaceSlotBinding(
                slot_name="input",
                logical_path="/workspace/input/source.md",
                access="read_only",
                owner=owner("run:run-1", WorkspaceOwnerKind.RUN),
                durable_ref="artifact:input-1",
                content_digest=INPUT_DIGEST,
            ),
            WorkspaceSlotBinding(
                slot_name="output",
                logical_path="/workspace/output/report.md",
                access="exclusive_write",
                owner=write_owner or owner(),
            ),
        ),
        runtime_digest=RUNTIME_DIGEST,
        image_digest=IMAGE_DIGEST,
        created_at=datetime(2026, 7, 20, 18, 0, tzinfo=UTC),
    )


def service(
    repository: InMemoryWorkspaceManifestRepository | None = None,
    *,
    values: dict[str, bytes] | None = None,
) -> tuple[WorkspaceMaterializationService, RecordingProvisioner]:
    provisioner = RecordingProvisioner()
    return (
        WorkspaceMaterializationService(
            manifests=repository or InMemoryWorkspaceManifestRepository(),
            provisioner=provisioner,
            durable_inputs=InMemoryDurableWorkspaceInputs(
                values if values is not None else {"artifact:input-1": INPUT}
            ),
        ),
        provisioner,
    )


async def test_materializes_exact_slots_and_records_immutable_lineage() -> None:
    materializer, provisioner = service()

    workspace = await materializer.materialize(request())
    replayed = await materializer.materialize(request())
    manifest = await materializer.current_manifest("run-workspace:run-1", "workspace-1")

    assert workspace == replayed
    assert workspace.manifest_revision == 1
    assert provisioner.inputs == {"/workspace/input/source.md": INPUT}
    assert tuple(entry.kind for entry in manifest.entries) == ("durable_input",)
    assert manifest.slots[1].owner.kind == WorkspaceOwnerKind.STAGE


async def test_parallel_workspaces_cannot_claim_the_same_writable_slot() -> None:
    repository = InMemoryWorkspaceManifestRepository()
    first, _ = service(repository)
    second, _ = service(repository)
    await first.materialize(request())

    with pytest.raises(WorkspaceSlotConflict):
        await second.materialize(
            request(workspace_id="workspace-2", write_owner=owner("agent:other"))
        )


async def test_delegate_requires_parent_and_receives_private_owner() -> None:
    with pytest.raises(ValueError, match="explicit parent"):
        owner("delegate:one", WorkspaceOwnerKind.DELEGATE)

    delegate = owner(
        "delegate:one",
        WorkspaceOwnerKind.DELEGATE,
        parent_owner_id="agent:parent",
    )
    materializer, _ = service()
    await materializer.materialize(request(write_owner=delegate))
    manifest = await materializer.current_manifest("run-workspace:run-1", "workspace-1")
    assert manifest.slots[1].owner == delegate


async def test_digest_mismatch_and_undeclared_candidate_fail_closed() -> None:
    materializer, _ = service(values={"artifact:input-1": b"tampered"})
    with pytest.raises(WorkspaceDigestMismatch):
        await materializer.materialize(request())

    valid, _ = service()
    await valid.materialize(request())
    with pytest.raises(UndeclaredWorkspacePath):
        await valid.register_candidate(
            namespace_id="run-workspace:run-1",
            workspace_id="workspace-1",
            slot_name="other",
            logical_path="/workspace/output/other.md",
            owner=owner(),
            candidate_id="candidate-1",
            content=b"report",
            content_digest=f"sha256:{sha256(b'report').hexdigest()}",
            media_type="text/markdown",
        )


async def test_candidate_registration_is_digest_verified_and_retry_safe() -> None:
    materializer, _ = service()
    await materializer.materialize(request())
    content = b"# Research report\n"
    digest = f"sha256:{sha256(content).hexdigest()}"

    manifest = await materializer.register_candidate(
        namespace_id="run-workspace:run-1",
        workspace_id="workspace-1",
        slot_name="output",
        logical_path="/workspace/output/report.md",
        owner=owner(),
        candidate_id="candidate-1",
        content=content,
        content_digest=digest,
        media_type="text/markdown",
    )
    replayed = await materializer.register_candidate(
        namespace_id="run-workspace:run-1",
        workspace_id="workspace-1",
        slot_name="output",
        logical_path="/workspace/output/report.md",
        owner=owner(),
        candidate_id="candidate-1",
        content=content,
        content_digest=digest,
        media_type="text/markdown",
    )

    assert manifest == replayed
    assert manifest.revision == 2
    assert manifest.entries[-1].kind == "local_candidate"


async def test_real_filesystem_mounts_inputs_read_only_and_hides_host_identity(
    tmp_path,
) -> None:
    provisioner = FilesystemWorkspaceProvisioner(tmp_path)
    materializer = WorkspaceMaterializationService(
        manifests=InMemoryWorkspaceManifestRepository(),
        provisioner=provisioner,
        durable_inputs=InMemoryDurableWorkspaceInputs({"artifact:input-1": INPUT}),
    )
    workspace = await materializer.materialize(request())
    manifest = await materializer.current_manifest("run-workspace:run-1", "workspace-1")
    mounted = provisioner.governed_host_path(
        manifest, "/workspace/input/source.md"
    )
    output = provisioner.write_candidate(
        manifest, "/workspace/output/report.md", b"first draft"
    )

    assert mounted.read_bytes() == INPUT
    assert is_read_only(mounted)
    assert output.read_bytes() == b"first draft"
    assert str(tmp_path) not in workspace.model_dump_json()
    assert str(tmp_path) not in manifest.model_dump_json()
    with pytest.raises(UndeclaredWorkspacePath):
        provisioner.governed_host_path(manifest, "/workspace/unmapped.txt")
