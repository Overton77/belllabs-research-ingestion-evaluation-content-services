"""Writable-slot ownership of GoalDirected role roots on the governed materializer (RRM-009).

RRM-016 binds a GoalDirected unit's compiled slots under `/goal/{iteration}/{role}` in the
run's one namespace (`run/{run_id}`). The reservation key used to be the first two path
components, so the iteration's executor and its independently bound verifier collided on
`/goal/{iteration}` (`WorkspaceSlotConflict`) on the production materializer. The role root
is now the ownership boundary; every other overlap still conflicts.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path

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
from app.domain.operation_execution.errors import UndeclaredWorkspacePath, WorkspaceSlotConflict
from app.domain.operation_execution.materialization import (
    slot_ownership_boundary,
    verify_workspace_manifest,
)
from app.domain.orchestration.runtime_units import goal_unit_workspace_root
from app.domain.run_control.errors import IdempotencyConflict
from app.integrations.filesystem_workspace import FilesystemWorkspaceProvisioner
from tests.fixtures.checkpoint_lineage import goal_unit
from tests.unit.workspaces.test_workspace_materialization import RecordingProvisioner

NAMESPACE = "run/run-1"


def _service(
    manifests: InMemoryWorkspaceManifestRepository | None = None,
) -> WorkspaceMaterializationService:
    return WorkspaceMaterializationService(
        manifests=manifests or InMemoryWorkspaceManifestRepository(),
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
async def test_shared_goal_workspace_is_materialized_again_at_the_next_iteration() -> None:
    """RRM-020 (option 1): one workspace identity; the next iteration's role-rooted slots join
    it as exactly one new manifest revision, and earlier roots keep their owners."""

    manifests = InMemoryWorkspaceManifestRepository()
    service = _service(manifests)
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    first = await service.materialize(_goal_request(workspace_id, 1, "executor"))
    second = await service.materialize(_goal_request(workspace_id, 2, "executor"))

    assert first.workspace_id == second.workspace_id == workspace_id
    assert (first.manifest_revision, second.manifest_revision) == (1, 2)
    manifest = second.materialization_manifest
    assert manifest is not None
    verify_workspace_manifest(manifest)
    assert manifest.prior_manifest_digest == first.mount_manifest_digest
    assert [(slot.logical_path, slot.owner.owner_id) for slot in manifest.slots] == [
        ("/goal/1/executor/work", "goal-iteration/1/executor"),
        ("/goal/2/executor/work", "goal-iteration/2/executor"),
    ]
    # Each role root is reserved once, for this workspace and its own iteration's owner.
    assert {
        path: (workspace, owner_id)
        for (_, path), (workspace, owner_id, _) in manifests._reservations.items()
    } == {
        "/goal/1/executor": (workspace_id, "goal-iteration/1/executor"),
        "/goal/2/executor": (workspace_id, "goal-iteration/2/executor"),
    }

    # Exactly once: a retry of either iteration appends no revision.
    retried = await service.materialize(_goal_request(workspace_id, 2, "executor"))
    earlier = await service.materialize(_goal_request(workspace_id, 1, "executor"))
    assert retried.materialization_manifest == earlier.materialization_manifest == manifest
    assert len(manifests._manifests[(NAMESPACE, workspace_id)]) == 2


@pytest.mark.asyncio
async def test_shared_executor_and_verifier_keep_disjoint_owned_roots_across_iterations() -> None:
    """REQ-BP-GD-004 / REQ-CP-DA-013 on shared workspaces: the executor's and the verifier's
    workspaces each grow by their own role root, and neither takes the other's."""

    service = _service()
    executor_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    verifier_id = f"{executor_id}:verifier"
    for iteration in (1, 2):
        await service.materialize(_goal_request(executor_id, iteration, "executor"))
        await service.materialize(_goal_request(verifier_id, iteration, "verifier"))
    executor = await service.current_manifest(NAMESPACE, executor_id)
    verifier = await service.current_manifest(NAMESPACE, verifier_id)
    executor_roots = {slot_ownership_boundary(slot.logical_path) for slot in executor.slots}
    verifier_roots = {slot_ownership_boundary(slot.logical_path) for slot in verifier.slots}
    assert executor_roots == {"/goal/1/executor", "/goal/2/executor"}
    assert verifier_roots == {"/goal/1/verifier", "/goal/2/verifier"}
    assert {slot.owner.kind for slot in executor.slots} == {WorkspaceOwnerKind.ITERATION}
    assert {slot.owner.kind for slot in verifier.slots} == {WorkspaceOwnerKind.EVALUATOR}
    # The verifier's next root never joins the executor's workspace, and the reverse.
    with pytest.raises(IdempotencyConflict):
        await service.materialize(_goal_request(executor_id, 3, "verifier"))
    with pytest.raises(IdempotencyConflict):
        await service.materialize(_goal_request(verifier_id, 3, "executor"))
    # Another workspace cannot take a root the shared workspace now owns.
    with pytest.raises(WorkspaceSlotConflict):
        await service.materialize(_goal_request("workspace/other", 2, "executor"))


def _iteration_owner(iteration: int, kind: WorkspaceOwnerKind = WorkspaceOwnerKind.ITERATION):
    return WorkspaceOwner(kind=kind, owner_id=f"goal-iteration/{iteration}/executor")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "path", "owner"),
    [
        # A recorded iteration requested with other slots.
        ("recorded iteration, other path", "/goal/2/executor/other", _iteration_owner(2)),
        # An iteration earlier than the workspace's latest that it never held.
        ("earlier iteration", "/goal/0/executor/work", _iteration_owner(0)),
        # Not the compiled slot set rebased under the next root.
        ("other compiled path", "/goal/3/executor/scratch", _iteration_owner(3)),
        ("other owner kind", "/goal/3/executor/work", _iteration_owner(3, WorkspaceOwnerKind.RUN)),
        ("an owner the workspace has", "/goal/3/executor/work", _iteration_owner(2)),
        # Not role-rooted at all.
        ("unrooted slot", "/workspace/output", _iteration_owner(3)),
        # RRM-020 review: the next root is exactly the latest plus one, in canonical form.
        ("skipped iteration", "/goal/4/executor/work", _iteration_owner(4)),
        ("zero-padded iteration", "/goal/03/executor/work", _iteration_owner(3)),
    ],
)
async def test_slot_sets_outside_the_declared_rule_still_conflict(
    case: str, path: str, owner: WorkspaceOwner
) -> None:
    manifests = InMemoryWorkspaceManifestRepository()
    service = _service(manifests)
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    await service.materialize(_goal_request(workspace_id, 1, "executor"))
    await service.materialize(_goal_request(workspace_id, 2, "executor"))
    with pytest.raises(IdempotencyConflict, match="different materialization"):
        await service.materialize(_request(workspace_id, path, owner))
    # Nothing was appended or reserved for the refused request.
    assert len(manifests._manifests[(NAMESPACE, workspace_id)]) == 2, case
    assert {path for _, path in manifests._reservations} == {
        "/goal/1/executor",
        "/goal/2/executor",
    }, case


@pytest.mark.asyncio
async def test_two_roots_in_one_request_and_unrooted_workspaces_keep_exact_identity() -> None:
    service = _service()
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    await service.materialize(_goal_request(workspace_id, 1, "executor"))
    third = _request(workspace_id, "/goal/3/executor/work", _iteration_owner(3))
    fourth = _request(workspace_id, "/goal/4/executor/work", _iteration_owner(4)).slots[0]
    two_roots = third.model_copy(
        update={"slots": (*third.slots, fourth.model_copy(update={"slot_name": "work-b"}))}
    )
    with pytest.raises(IdempotencyConflict):
        await service.materialize(two_roots)
    # A workspace that is not role-rooted keeps the exact-identity rule (StageGraph).
    stage = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="stage:a")
    await service.materialize(_request("stage-a", "/workspace/output", stage))
    with pytest.raises(IdempotencyConflict):
        await service.materialize(_request("stage-a", "/workspace/report", stage))
    assert (await service.current_manifest(NAMESPACE, workspace_id)).revision == 1


@pytest.mark.asyncio
async def test_each_iteration_registers_candidates_in_its_own_root(tmp_path: Path) -> None:
    """The real filesystem provisioner holds both roots of the shared workspace; a candidate is
    governed by the slot whose path holds it, so an iteration registers only in its own root."""

    provisioner = FilesystemWorkspaceProvisioner(tmp_path)
    service = WorkspaceMaterializationService(
        manifests=InMemoryWorkspaceManifestRepository(),
        provisioner=provisioner,
        durable_inputs=InMemoryDurableWorkspaceInputs({}),
    )
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    for iteration in (1, 2):
        await service.materialize(_goal_request(workspace_id, iteration, "executor"))
        content = f"iteration {iteration}".encode()
        await service.register_candidate(
            namespace_id=NAMESPACE,
            workspace_id=workspace_id,
            slot_name="work",
            logical_path=f"/goal/{iteration}/executor/work/report.md",
            owner=_iteration_owner(iteration),
            candidate_id=f"candidate-{iteration}",
            content=content,
            content_digest=f"sha256:{sha256(content).hexdigest()}",
            media_type="text/markdown",
        )
    manifest = await service.current_manifest(NAMESPACE, workspace_id)
    # rev 1 (iteration 1), rev 2 (its candidate), rev 3 (iteration 2), rev 4 (its candidate).
    assert manifest.revision == 4
    assert sorted(
        (entry.logical_path, entry.owner.owner_id)
        for entry in manifest.entries
        if entry.kind == "local_candidate"
    ) == [
        ("/goal/1/executor/work/report.md", "goal-iteration/1/executor"),
        ("/goal/2/executor/work/report.md", "goal-iteration/2/executor"),
    ]
    for iteration in (1, 2):
        assert provisioner.governed_host_path(manifest, f"/goal/{iteration}/executor/work").is_dir()
    # Iteration 2 cannot register into iteration 1's root.
    content = b"overwrite"
    with pytest.raises(UndeclaredWorkspacePath):
        await service.register_candidate(
            namespace_id=NAMESPACE,
            workspace_id=workspace_id,
            slot_name="work",
            logical_path="/goal/1/executor/work/report.md",
            owner=_iteration_owner(2),
            candidate_id="candidate-cross",
            content=content,
            content_digest=f"sha256:{sha256(content).hexdigest()}",
            media_type="text/markdown",
        )


@pytest.mark.asyncio
async def test_a_read_only_input_joins_each_iteration_under_its_own_root() -> None:
    durable = b"governed input"
    digest = f"sha256:{sha256(durable).hexdigest()}"
    recorder = RecordingProvisioner()
    service = WorkspaceMaterializationService(
        manifests=InMemoryWorkspaceManifestRepository(),
        provisioner=recorder,
        durable_inputs=InMemoryDurableWorkspaceInputs({"artifact:input": durable}),
    )
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"

    def with_input(iteration: int) -> WorkspaceMaterializationRequest:
        request = _goal_request(workspace_id, iteration, "executor")
        mounted = WorkspaceSlotBinding(
            slot_name="input",
            logical_path=f"/goal/{iteration}/executor/input",
            access="read_only",
            owner=request.slots[0].owner,
            durable_ref="artifact:input",
            content_digest=digest,
        )
        return request.model_copy(update={"slots": (*request.slots, mounted)})

    await service.materialize(with_input(1))
    second = await service.materialize(with_input(2))
    manifest = second.materialization_manifest
    assert manifest is not None and manifest.revision == 2
    inputs = [entry for entry in manifest.entries if entry.kind == "durable_input"]
    assert [entry.logical_path for entry in inputs] == [
        "/goal/1/executor/input",
        "/goal/2/executor/input",
    ]
    assert len({entry.entry_id for entry in inputs}) == 2
    assert set(recorder.inputs) == {"/goal/1/executor/input", "/goal/2/executor/input"}
    # The slot set must be the compiled one: an iteration without the input is outside the rule.
    with pytest.raises(IdempotencyConflict):
        await service.materialize(_goal_request(workspace_id, 3, "executor"))


def test_only_canonical_ascii_iterations_are_role_roots() -> None:
    """RRM-020 review: `"\u00b2".isdigit()` is true but `int` refuses it, and `"03"` would name
    iteration 3 under a second root. Neither is a role root, and neither raises."""

    assert slot_ownership_boundary("/goal/3/executor/work") == "/goal/3/executor"
    assert slot_ownership_boundary("/goal/\u00b2/executor/work") == "/goal/\u00b2"
    assert slot_ownership_boundary("/goal/03/executor/work") == "/goal/03"
    assert slot_ownership_boundary("/goal/0/verifier/work") == "/goal/0/verifier"


@pytest.mark.asyncio
async def test_a_non_ascii_iteration_is_the_typed_conflict_not_a_value_error() -> None:
    service = _service()
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    await service.materialize(_goal_request(workspace_id, 1, "executor"))
    next_request = _goal_request(workspace_id, 2, "executor")
    # The slot contract's path pattern refuses the character on validation, so the request is
    # built without it: the rule itself must refuse it with the typed conflict.
    superscript = next_request.model_copy(
        update={
            "slots": (
                next_request.slots[0].model_copy(
                    update={"logical_path": "/goal/\u00b2/executor/work"}
                ),
            )
        }
    )
    with pytest.raises(IdempotencyConflict, match="different materialization"):
        await service.materialize(superscript)
    assert (await service.current_manifest(NAMESPACE, workspace_id)).revision == 1


def _rebound(
    request: WorkspaceMaterializationRequest, minutes: int = 5
) -> WorkspaceMaterializationRequest:
    """The same materialization under a later operation attempt's binding (`bound_at`)."""

    return request.model_copy(
        update={"created_at": request.created_at + timedelta(minutes=minutes)}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("iteration", [1, 2])
async def test_a_crash_between_reservation_and_manifest_does_not_conflict_on_rebinding(
    iteration: int,
) -> None:
    """RRM-020 review: attempt A reserves its role root and crashes before its manifest;
    attempt B re-binds the same unit (another `bound_at`) and materializes the same workspace.
    Its own reservation is not a conflict, and exactly one revision is written."""

    manifests = InMemoryWorkspaceManifestRepository()
    service = _service(manifests)
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    if iteration == 2:
        await service.materialize(_goal_request(workspace_id, 1, "executor"))
    attempt_a = _goal_request(workspace_id, iteration, "executor")
    await manifests.reserve_writable_slots(attempt_a)  # then the worker dies

    materialized = await service.materialize(_rebound(attempt_a))
    assert materialized.manifest_revision == iteration
    assert len(manifests._manifests[(NAMESPACE, workspace_id)]) == iteration
    # Attempt A's own retry after B converges on the same revision.
    again = await service.materialize(attempt_a)
    assert again.materialization_manifest == materialized.materialization_manifest
    # A different workspace still cannot take the root.
    with pytest.raises(WorkspaceSlotConflict):
        await service.materialize(_rebound(_goal_request("workspace/other", iteration, "executor")))


@pytest.mark.asyncio
async def test_an_append_differing_only_in_created_at_is_the_same_revision() -> None:
    manifests = InMemoryWorkspaceManifestRepository()
    service = _service(manifests)
    workspace_id = "run/run-1/execution-epoch/1/goal/workspace/1"
    await service.materialize(_goal_request(workspace_id, 1, "executor"))
    stored = await service.current_manifest(NAMESPACE, workspace_id)
    raced = stored.model_copy(update={"created_at": stored.created_at + timedelta(minutes=5)})
    assert await manifests.append(raced) == stored
    with pytest.raises(IdempotencyConflict):
        await manifests.append(stored.model_copy(update={"manifest_digest": "sha256:" + "0" * 64}))
