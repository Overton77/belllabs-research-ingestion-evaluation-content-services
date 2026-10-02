"""Writable-slot ownership of GoalDirected role roots on the governed materializer (RRM-009).

RRM-016 binds a GoalDirected unit's compiled slots under `/goal/{iteration}/{role}` in the
run's one namespace (`run/{run_id}`). The reservation key used to be the first two path
components, so the iteration's executor and its independently bound verifier collided on
`/goal/{iteration}` (`WorkspaceSlotConflict`) on the production materializer. The role root
is now the ownership boundary; every other overlap still conflicts.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.workspaces.workspace_materialization import (
    InMemoryDurableWorkspaceInputs,
    InMemoryWorkspaceManifestRepository,
    WorkspaceMaterializationService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, ExactDefinitionRef
from app.domain.operation_execution.contracts import (
    WorkspaceMaterializationRequest,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from app.domain.operation_execution.errors import WorkspaceSlotConflict
from app.domain.operation_execution.materialization import slot_ownership_boundary
from app.domain.orchestration.runtime_units import goal_unit_workspace_root
from app.domain.run_control.errors import IdempotencyConflict
from tests.fixtures.checkpoint_lineage import goal_unit
from tests.unit.workspaces.test_workspace_materialization import RecordingProvisioner

NAMESPACE = "run/run-1"


def _service() -> WorkspaceMaterializationService:
    return WorkspaceMaterializationService(
        manifests=InMemoryWorkspaceManifestRepository(),
        provisioner=RecordingProvisioner(),
        durable_inputs=InMemoryDurableWorkspaceInputs({}),
    )


def _goal_request(workspace_id: str, iteration: int, role: str) -> WorkspaceMaterializationRequest:
    """The request the GoalDirected preparer derives: the compiled `/work` slot rebased
    under the unit's role root, owned by the iteration's executor or verifier."""

    unit = goal_unit(
        request_scope="tenant-1",
        run_id="run-1",
        operation_id=f"goal-iteration/{iteration}/{role}",
        goal_iteration=iteration,
        role=role,  # type: ignore[arg-type]
    )
    root = goal_unit_workspace_root(unit)
    assert root == f"/goal/{iteration}/{role}"
    return _request(
        workspace_id,
        f"{root}/work",
        WorkspaceOwner(
            kind=(
                WorkspaceOwnerKind.ITERATION if role == "executor" else WorkspaceOwnerKind.EVALUATOR
            ),
            owner_id=f"goal-iteration/{iteration}/{role}",
        ),
    )


def _request(
    workspace_id: str, path: str, owner: WorkspaceOwner
) -> WorkspaceMaterializationRequest:
    return WorkspaceMaterializationRequest(
        namespace_id=NAMESPACE,
        workspace_id=workspace_id,
        provider="conformance-filesystem",
        template_ref=ExactDefinitionRef(
            kind=DefinitionKind.WORKSPACE_TEMPLATE,
            logical_id="goal-workspace",
            revision=1,
            digest=sha256_digest("goal-workspace@1"),
        ),
        workflow_contract_digest=sha256_digest("goal-workspace-contract"),
        slots=(
            WorkspaceSlotBinding(
                slot_name="work", logical_path=path, access="exclusive_write", owner=owner
            ),
        ),
        runtime_digest=sha256_digest("runtime"),
        image_digest=sha256_digest("image"),
        created_at=datetime(2026, 10, 2, tzinfo=UTC),
    )


def test_role_root_is_the_ownership_boundary_and_other_paths_keep_two_components() -> None:
    assert slot_ownership_boundary("/goal/1/executor/work") == "/goal/1/executor"
    assert slot_ownership_boundary("/goal/12/verifier/work/notes") == "/goal/12/verifier"
    assert slot_ownership_boundary("/goal/1/reviewer/work") == "/goal/1"
    assert slot_ownership_boundary("/goal/x/executor/work") == "/goal/x"
    assert slot_ownership_boundary("/workspace/output") == "/workspace/output"
    assert slot_ownership_boundary("/workspace/output/report.md") == "/workspace/output"


@pytest.mark.asyncio
async def test_executor_and_verifier_of_one_iteration_own_disjoint_roots() -> None:
    service = _service()
    executor = await service.materialize(_goal_request("workspace/1", 1, "executor"))
    verifier = await service.materialize(_goal_request("workspace/1:verifier", 1, "verifier"))
    assert (executor.workspace_id, verifier.workspace_id) == ("workspace/1", "workspace/1:verifier")
    # A second workspace still cannot take a role root another workspace owns.
    with pytest.raises(WorkspaceSlotConflict):
        await service.materialize(_goal_request("workspace/other", 1, "executor"))
    # Nested or sibling slots under one two-component root still conflict.
    owner = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:a")
    await service.materialize(_request("stage-a", "/workspace/output", owner))
    with pytest.raises(WorkspaceSlotConflict):
        await service.materialize(_request("stage-b", "/workspace/output/sub", owner))


@pytest.mark.asyncio
@pytest.mark.xfail(
    strict=True,
    raises=IdempotencyConflict,
    reason=(
        "RRM-020: a `shared` GoalDirected workspace keeps one workspace identity across "
        "iterations while RRM-016 rebases its slots under the iteration's role root, so the "
        "second iteration's executor re-materializes the same workspace with other slots"
    ),
)
async def test_shared_goal_workspace_is_materialized_again_at_the_next_iteration() -> None:
    service = _service()
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    await service.materialize(_goal_request(workspace_id, 1, "executor"))
    await service.materialize(_goal_request(workspace_id, 2, "executor"))
