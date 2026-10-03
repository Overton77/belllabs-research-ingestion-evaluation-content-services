from __future__ import annotations

import asyncio

import pytest

from mission_control.application.artifacts.artifact_promotion import ScopedArtifactPromotionService
from mission_control.application.artifacts.workspace_candidates import (
    InMemoryWorkspaceCandidateContents,
    WorkspaceCandidateCaptureService,
)
from mission_control.application.artifacts.workspace_materialization import (
    BindingWorkspaceMaterializer,
    InMemoryDurableWorkspaceInputs,
    InMemoryWorkspaceManifestRepository,
    WorkspaceMaterializationService,
)
from mission_control.domain.execution.contracts import CapturedWorkspaceCandidate
from mission_control.domain.execution.errors import UndeclaredWorkspacePath, WorkspaceDigestMismatch
from tests.unit.workspaces.test_artifact_promotion import CONTENT, promotion_fixture
from tests.unit.workspaces.test_workspace_materialization import RecordingProvisioner


async def test_binding_scope_routes_materialization_and_candidate_access_without_ambient_state():
    promotion, _, _, _, request = await promotion_fixture()
    binding = await promotion._bindings.get_binding_by_id(
        request.binding_id, request_scope=request.request_scope
    )
    assert binding is not None
    services = {
        scope: WorkspaceMaterializationService(
            manifests=InMemoryWorkspaceManifestRepository(),
            provisioner=RecordingProvisioner(),
            durable_inputs=InMemoryDurableWorkspaceInputs(),
        )
        for scope in ("first", "second")
    }
    sandbox = BindingWorkspaceMaterializer(service_for_scope=services.__getitem__)
    candidates = WorkspaceCandidateCaptureService(
        materializer_for_scope=services.__getitem__, contents=InMemoryWorkspaceCandidateContents()
    )
    first = binding.model_copy(update={"request_scope": "first", "binding_id": "binding-first"})
    second = binding.model_copy(update={"request_scope": "second", "binding_id": "binding-second"})
    await asyncio.gather(sandbox.materialize(first), sandbox.materialize(second))
    await candidates.capture(first, request.logical_path, b"first content")
    with pytest.raises(UndeclaredWorkspacePath):
        await candidates.get_for_path(
            request.namespace_id, request.workspace_id, request.logical_path, request_scope="second"
        )
    await candidates.capture(second, request.logical_path, b"second content")
    results = await asyncio.gather(
        *(
            candidates.get_for_path(
                request.namespace_id,
                request.workspace_id,
                request.logical_path,
                request_scope=scope,
            )
            for scope in ("first", "second")
        )
    )
    assert [content for _, content in results] == [b"first content", b"second content"]
    with pytest.raises(ValueError, match="explicit request scope"):
        await candidates.get_for_path(
            request.namespace_id, request.workspace_id, request.logical_path
        )


async def test_scoped_promotion_routes_request_and_requires_lookup_scope():
    service, _, _, workspaces, request = await promotion_fixture()
    observed = []

    def for_scope(scope):
        observed.append(scope)
        if scope != request.request_scope:
            raise ValueError("unregistered scope")
        return service

    scoped = ScopedArtifactPromotionService(for_scope)
    contents = InMemoryWorkspaceCandidateContents()
    await contents.put(
        CapturedWorkspaceCandidate.model_validate(
            request.model_dump(
                include={
                    "namespace_id",
                    "workspace_id",
                    "output_slot",
                    "logical_path",
                    "owner",
                    "candidate_id",
                    "content_digest",
                    "media_type",
                    "size_bytes",
                }
            )
        ),
        CONTENT,
    )
    candidates = WorkspaceCandidateCaptureService(materializer=workspaces, contents=contents)
    artifact = await scoped.promote(request, CONTENT)
    # Activity retries resolve the original captured bytes after the manifest now links
    # a promoted artifact, allowing the service's admitted idempotency path to run.
    candidate, recovered = await candidates.get_for_path(
        request.namespace_id, request.workspace_id, request.logical_path
    )
    assert recovered == CONTENT and candidate.candidate_id == request.candidate_id
    contents._candidates[candidate.candidate_id] = candidate.model_copy(
        update={"content_digest": "sha256:" + "0" * 64}
    )
    with pytest.raises(WorkspaceDigestMismatch, match="not current"):
        await candidates.get_for_path(
            request.namespace_id, request.workspace_id, request.logical_path
        )
    assert (
        await scoped.get_visible(artifact.artifact_id, request_scope=request.request_scope)
        == artifact
    )
    assert observed == [request.request_scope, request.request_scope]
    with pytest.raises(ValueError, match="request scope"):
        await scoped.get_visible(artifact.artifact_id, request_scope="")
    with pytest.raises(ValueError, match="unregistered scope"):
        await scoped.get_visible(artifact.artifact_id, request_scope="other")
