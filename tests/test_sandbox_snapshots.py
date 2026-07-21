from __future__ import annotations

import io
import tarfile
from datetime import UTC, datetime, timedelta

import pytest

from app.application.operation_execution import _binding_for
from app.application.sandbox_snapshots import (
    InMemorySandboxSnapshotRepository,
    InMemorySnapshotPayloadStore,
    SandboxSnapshotService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    ReacquiredRuntimeResources,
    SandboxSnapshotCapture,
    SandboxSnapshotCreateRequest,
    SnapshotCapabilityShape,
    SnapshotCloneRequest,
    SnapshotCreationReason,
    SnapshotRetention,
)
from app.domain.operation_execution.errors import (
    SnapshotAuthorityError,
    SnapshotCompatibilityError,
    SnapshotMigrationRequired,
    SnapshotPayloadMismatch,
)
from app.integrations.openai_sandbox_snapshots import OpenAIAgentsSnapshotBridge
from tests.test_operation_execution import operation_request

NOW = datetime(2026, 7, 21, 18, 0, tzinfo=UTC)
RUNTIME = sha256_digest("runtime")
IMAGE = sha256_digest("image")
PACKAGE = sha256_digest("packages")
ENVIRONMENT = sha256_digest("environment-without-secret-values")
MOUNTS = sha256_digest("mount-manifest")
WORKSPACE_CONTRACT = sha256_digest("workspace-contract")
ARCHIVE = b"immutable sandbox tar payload"
CAPABILITIES = SnapshotCapabilityShape(
    capabilities=frozenset({"filesystem.read", "filesystem.write"}),
    tool_ids=frozenset({"sandbox.filesystem"}),
    data_scope_refs=frozenset({"source:one"}),
)


class Authority:
    def __init__(self) -> None:
        self.revoked = False
        self.creation_checks = 0
        self.restore_checks = 0

    async def verify_creation(self, request: SandboxSnapshotCreateRequest) -> None:
        self.creation_checks += 1
        if request.snapshot_policy_ref != "snapshot:on-failure@1":
            raise SnapshotAuthorityError("snapshot policy does not admit creation")

    async def verify_restore(self, request, snapshot) -> None:  # type: ignore[no-untyped-def]
        self.restore_checks += 1
        if self.revoked or request.binding_id == snapshot.producer_binding_id:
            raise SnapshotAuthorityError("current binding authority is unavailable")


class Sandbox:
    def __init__(self) -> None:
        self.captures = 0
        self.clones: list[str] = []

    async def capture(self, request: SandboxSnapshotCreateRequest) -> SandboxSnapshotCapture:
        self.captures += 1
        return SandboxSnapshotCapture(
            provider_snapshot_id=f"provider:{request.snapshot_id}",
            filesystem_digest=sha256_digest("filesystem-tree"),
            content_manifest_digest=sha256_digest("filesystem-manifest"),
            payload=ARCHIVE,
        )

    async def clone(self, *, snapshot, payload, request):  # type: ignore[no-untyped-def]
        assert payload == ARCHIVE
        self.clones.append(request.target_workspace_id)
        return MaterializedWorkspace(
            workspace_id=request.target_workspace_id,
            namespace_id=request.target_namespace_id,
            provider=snapshot.provider,
            runtime_digest=request.runtime_digest,
            image_digest=request.image_digest,
            mount_manifest_digest=request.target_mount_manifest_digest,
        )

    async def discard_clone(self, request) -> None:  # type: ignore[no-untyped-def]
        self.clones.remove(request.target_workspace_id)


class Resources:
    async def reacquire(self, request, snapshot):  # type: ignore[no-untyped-def]
        del request, snapshot
        return ReacquiredRuntimeResources(
            secret_names=("OPENAI_API_KEY",),
            credential_names=("aws-role",),
            lease_names=("sandbox-network",),
            mcp_connection_names=("search",),
            socket_names=("runtime-events",),
        )

    async def release(self, request, snapshot, resources) -> None:  # type: ignore[no-untyped-def]
        del request, snapshot, resources


def create_request() -> SandboxSnapshotCreateRequest:
    return SandboxSnapshotCreateRequest(
        snapshot_id="snapshot-1",
        idempotency_key="snapshot:create:one",
        request_scope="tenant:one",
        source_namespace_id="run-workspace:run-1",
        source_workspace_id="workspace-source",
        provider="openai-agents-docker",
        reason=SnapshotCreationReason.FAILURE,
        producer_binding_id="binding-source",
        snapshot_policy_ref="snapshot:on-failure@1",
        runtime_digest=RUNTIME,
        image_digest=IMAGE,
        package_digest=PACKAGE,
        environment_digest=ENVIRONMENT,
        workspace_contract_digest=WORKSPACE_CONTRACT,
        mount_manifest_digest=MOUNTS,
        capability_shape=CAPABILITIES,
        retention=SnapshotRetention(
            policy_ref="retention:debug-30d@1",
            retain_until=NOW + timedelta(days=30),
        ),
        created_at=NOW,
    )


def clone_request(workspace_id: str, clone_id: str) -> SnapshotCloneRequest:
    return SnapshotCloneRequest(
        snapshot_id="snapshot-1",
        clone_id=clone_id,
        request_scope="tenant:one",
        target_namespace_id="run-workspace:run-2",
        target_workspace_id=workspace_id,
        binding_id=f"binding:{clone_id}",
        runtime_digest=RUNTIME,
        image_digest=IMAGE,
        package_digest=PACKAGE,
        environment_digest=ENVIRONMENT,
        workspace_contract_digest=WORKSPACE_CONTRACT,
        target_mount_manifest_digest=MOUNTS,
        capability_shape=CAPABILITIES,
        requested_at=NOW + timedelta(hours=1),
    )


def service():
    repository = InMemorySandboxSnapshotRepository()
    payloads = InMemorySnapshotPayloadStore()
    sandbox = Sandbox()
    authority = Authority()
    return (
        SandboxSnapshotService(
            snapshots=repository,
            payloads=payloads,
            sandbox=sandbox,
            authority=authority,
            resources=Resources(),
        ),
        repository,
        payloads,
        sandbox,
        authority,
    )


async def test_snapshot_is_content_addressed_immutable_and_idempotent() -> None:
    snapshots, repository, payloads, sandbox, authority = service()

    first = await snapshots.create(create_request())
    replayed = await snapshots.create(create_request())

    assert first == replayed
    assert sandbox.captures == 1
    assert authority.creation_checks == 1
    assert first.payload.object_ref.endswith(first.payload.content_digest.removeprefix("sha256:"))
    assert await repository.get_snapshot("snapshot-1") == first
    assert "OPENAI_API_KEY" not in first.model_dump_json()
    assert payloads.payloads[first.payload.object_ref] == ARCHIVE


async def test_restore_twice_creates_distinct_clone_lineage_without_live_resources() -> None:
    snapshots, repository, _payloads, sandbox, _authority = service()
    source = await snapshots.create(create_request())

    first = await snapshots.clone_restore(clone_request("workspace-clone-1", "clone-1"))
    second = await snapshots.clone_restore(clone_request("workspace-clone-2", "clone-2"))
    replayed = await snapshots.clone_restore(
        clone_request("workspace-clone-1", "clone-1")
    )

    assert replayed == first
    assert first.workspace.workspace_id != second.workspace.workspace_id
    assert first.parent_snapshot_id == second.parent_snapshot_id == source.snapshot_id
    assert first.parent_workspace_id == second.parent_workspace_id == "workspace-source"
    assert first.live_resources_restored == second.live_resources_restored == ()
    assert first.artifact_promotion_required is True
    assert first.resources.secret_names == ("OPENAI_API_KEY",)
    assert sandbox.clones == ["workspace-clone-1", "workspace-clone-2"]
    assert await repository.get_snapshot(source.snapshot_id) == source
    assert (await repository.get_clone("clone-1")).target_workspace_id == "workspace-clone-1"  # type: ignore[union-attr]


async def test_restore_rejects_tampered_payload_before_provider_clone() -> None:
    snapshots, _repository, payloads, sandbox, _authority = service()
    snapshot = await snapshots.create(create_request())
    payloads.payloads[snapshot.payload.object_ref] = b"tampered"

    with pytest.raises(SnapshotPayloadMismatch):
        await snapshots.clone_restore(clone_request("workspace-clone-1", "clone-1"))

    assert sandbox.clones == []


async def test_restore_rejects_incompatible_runtime_as_new_migration_operation() -> None:
    snapshots, _repository, _payloads, sandbox, _authority = service()
    await snapshots.create(create_request())
    request = clone_request("workspace-clone-1", "clone-1").model_copy(
        update={"runtime_digest": sha256_digest("different-runtime")}
    )

    with pytest.raises(SnapshotMigrationRequired, match="new semantic operation"):
        await snapshots.clone_restore(request)

    assert sandbox.clones == []


async def test_restore_revalidates_present_authority_before_payload_or_clone() -> None:
    snapshots, _repository, _payloads, sandbox, authority = service()
    await snapshots.create(create_request())
    authority.revoked = True

    with pytest.raises(SnapshotAuthorityError):
        await snapshots.clone_restore(clone_request("workspace-clone-1", "clone-1"))

    assert authority.restore_checks == 1
    assert sandbox.clones == []


async def test_restore_uses_trusted_time_and_rejects_cross_scope() -> None:
    snapshots, _repository, _payloads, sandbox, _authority = service()
    await snapshots.create(create_request())
    expired = clone_request("workspace-clone-1", "clone-1").model_copy(
        update={"requested_at": NOW - timedelta(days=365)}
    )
    snapshots._clock = lambda: NOW + timedelta(days=31)

    with pytest.raises(SnapshotCompatibilityError, match="retention has expired"):
        await snapshots.clone_restore(expired)

    cross_scope = clone_request("workspace-clone-2", "clone-2").model_copy(
        update={"request_scope": "tenant:other"}
    )
    snapshots._clock = lambda: NOW + timedelta(hours=1)
    with pytest.raises(SnapshotCompatibilityError, match="scope"):
        await snapshots.clone_restore(cross_scope)
    assert sandbox.clones == []


async def test_openai_sdk_archive_bridge_exposes_provider_capture() -> None:
    class Client:
        pass

    bridge = OpenAIAgentsSnapshotBridge(  # type: ignore[arg-type]
        Client(),
        captured_policy_refs=frozenset({"snapshot:on-failure@1"}),
    )
    operation = operation_request()
    binding = _binding_for(
        operation,
        sha256_digest(operation.model_dump(mode="json", exclude={"requested_at"})),
    )
    workspace = MaterializedWorkspace(
        workspace_id="workspace-source",
        namespace_id="run-workspace:run-1",
        provider="openai-agents-docker",
        runtime_digest=RUNTIME,
        image_digest=IMAGE,
        mount_manifest_digest=MOUNTS,
    )
    archive = bridge.begin_capture(binding, workspace)
    assert archive is not None
    sdk_archive = io.BytesIO()
    with tarfile.open(fileobj=sdk_archive, mode="w") as tar:
        content = b"restorable report"
        entry = tarfile.TarInfo("workspace/output/report.md")
        entry.size = len(content)
        tar.addfile(entry, io.BytesIO(content))
    await archive.persist(io.BytesIO(sdk_archive.getvalue()))
    bridge.complete_capture(binding, workspace, archive)

    capture = await bridge.capture(create_request())

    with tarfile.open(fileobj=io.BytesIO(capture.payload), mode="r:") as restored:
        assert restored.getnames() == ["workspace/output/report.md"]
    assert capture.provider_snapshot_id == archive.id
    assert capture.filesystem_digest.startswith("sha256:")


async def test_openai_sdk_archive_bridge_rejects_secret_content_at_innocuous_path() -> None:
    class Client:
        pass

    bridge = OpenAIAgentsSnapshotBridge(  # type: ignore[arg-type]
        Client(),
        captured_policy_refs=frozenset({"snapshot:on-failure@1"}),
    )
    operation = operation_request()
    binding = _binding_for(
        operation,
        sha256_digest(operation.model_dump(mode="json", exclude={"requested_at"})),
    )
    workspace = MaterializedWorkspace(
        workspace_id="workspace-source",
        namespace_id="run-workspace:run-1",
        provider="openai-agents-docker",
        runtime_digest=RUNTIME,
        image_digest=IMAGE,
        mount_manifest_digest=MOUNTS,
    )
    archive = bridge.begin_capture(binding, workspace)
    assert archive is not None
    sdk_archive = io.BytesIO()
    with tarfile.open(fileobj=sdk_archive, mode="w") as tar:
        content = b"stale-token"
        entry = tarfile.TarInfo("workspace/output/report.txt")
        entry.size = len(content)
        tar.addfile(entry, io.BytesIO(content))
    await archive.persist(io.BytesIO(sdk_archive.getvalue()))

    with pytest.raises(SnapshotCompatibilityError, match="resolved secret value"):
        bridge.complete_capture(
            binding,
            workspace,
            archive,
            sensitive_values=(b"stale-token",),
        )
