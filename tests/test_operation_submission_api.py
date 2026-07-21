from __future__ import annotations

from fastapi.testclient import TestClient

from app.api.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
)
from app.api.run_control import (
    get_generic_artifact_submitter,
    get_run_control_service,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    ArtifactPromotionPlan,
    GenericArtifactWorkflowRequest,
    GenericArtifactWorkflowResult,
    OperationExecutionResult,
    PromotedArtifact,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from app.domain.run_control.contracts import ReserveBudgetAction, StartAction
from app.server import api
from tests.test_operation_execution import operation_request
from tests.test_run_control import command
from tests.test_run_control import request as run_request
from tests.test_run_control import service as run_control_service


class RecordingSubmitter:
    def __init__(self) -> None:
        self.requests: list[GenericArtifactWorkflowRequest] = []

    async def submit(
        self, request: GenericArtifactWorkflowRequest
    ) -> GenericArtifactWorkflowResult:
        self.requests.append(request)
        return GenericArtifactWorkflowResult(
            workflow_id="generic-artifact-workflow:test",
            operation=OperationExecutionResult(
                binding_id="binding:test",
                semantic_attempt_key=request.operation.identity.semantic_key,
                status="completed",
            ),
            artifact=PromotedArtifact(
                artifact_id="artifact:test",
                content_digest="sha256:" + "a" * 64,
                object_ref="s3://test/artifact",
                metadata_revision=4,
                manifest_revision=3,
                durable_reference="artifact://tenant-1/run/artifact:test",
                status="admitted",
            ),
        )


async def test_run_control_api_submits_only_active_reserved_operation(
    monkeypatch,
) -> None:
    async def noop(_application: object) -> None:
        return None

    monkeypatch.setattr("app.server.initialize_run_control_resources", noop)
    service, _ = run_control_service()
    admitted = await service.admit(run_request())
    assert admitted.run_id is not None
    run_id = admitted.run_id
    await service.execute(command(run_id, 1, "start", StartAction()))
    await service.execute(
        command(
            run_id,
            2,
            "reserve-operation",
            ReserveBudgetAction(
                reservation_id="operation-1",
                amounts={"tokens.total": 20},
            ),
        )
    )
    run = await service.get_run("tenant-1", run_id)
    owner = WorkspaceOwner(
        kind=WorkspaceOwnerKind.STAGE,
        owner_id="stage:generic-research",
    )
    slot = WorkspaceSlotBinding(
        slot_name="report",
        logical_path="/workspace/output/report.md",
        access="exclusive_write",
        owner=owner,
    )
    base = operation_request()
    operation = base.model_copy(
        update={
            "identity": base.identity.model_copy(update={"run_id": run_id}),
            "effective_configuration_digest": run.effective_configuration_digest,
            "run_control_revision": run.version,
            "budget_reservation_id": "operation-1",
            "budget_limits": {"tokens.total": 20},
            "workspace": base.workspace.model_copy(
                update={
                    "namespace_id": f"workspace-namespace:{run_id}",
                    "workspace_id": f"workspace:{run_id}",
                    "workflow_contract_digest": sha256_digest("generic-workspace-contract@1"),
                    "slot_bindings": (slot,),
                    "exclusive_write_paths": (slot.logical_path,),
                }
            ),
        }
    )
    submission = GenericArtifactWorkflowRequest(
        request_scope="tenant-1",
        run_id=run_id,
        operation=operation,
        promotion=ArtifactPromotionPlan(
            namespace_id=operation.workspace.namespace_id,
            workspace_id=operation.workspace.workspace_id,
            output_slot=slot.slot_name,
            logical_path=slot.logical_path,
            owner=owner,
            permission_ref="permission:generic@1",
            permission_outcome="allowed",
            output_contract_ref=operation.operation_contract_ref,
        ),
    )
    submitter = RecordingSubmitter()
    api.dependency_overrides[get_run_control_service] = lambda: service
    api.dependency_overrides[get_generic_artifact_submitter] = lambda: submitter
    api.dependency_overrides[get_control_plane_principal] = lambda: ControlPlanePrincipal(
        actor_id="operator",
        roles=frozenset({"operator"}),
        tenant_scopes=frozenset({"tenant-1"}),
    )
    try:
        with TestClient(api) as client:
            response = client.post(
                f"/run-control/v1/runs/{run_id}/operations",
                json=submission.model_dump(mode="json"),
            )
    finally:
        api.dependency_overrides.pop(get_run_control_service, None)
        api.dependency_overrides.pop(get_generic_artifact_submitter, None)
        api.dependency_overrides.pop(get_control_plane_principal, None)

    assert response.status_code == 201
    assert response.json()["artifact"]["status"] == "admitted"
    assert submitter.requests == [submission]
