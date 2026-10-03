"""Scoped facade drives the actual snapshot validator and independent fork admission."""

from uuid import uuid4

import pytest

from mission_control.application.missions.runtime import MissionControlRuntimeService
from mission_control.application.recovery.run_forks import (
    ForkPatchPolicyRegistry,
    ForkSnapshotNotFound,
)
from mission_control.contracts.contracts import MissionControlRejected
from mission_control.contracts.runtime_contracts import (
    MissionForkRequest,
    MissionReconciliationRequest,
    MissionSnapshotRequest,
)
from mission_control.domain.policies.contracts import ReconcileUnitAction
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.policies.forks import ForkPatchChange
from tests.fixtures.checkpoint_recovery import recovery_harness, stage_recovery_unit
from tests.fixtures.run_forks import (
    FakeForkSourceReader,
    compose_in_memory_forks,
    inspection_reads,
    stage_policy,
    stagegraph_head,
)
from tests.unit.operations.test_checkpoint_recovery_classification import (
    _namespace,
    _write_foreign_root_checkpoint,
)
from tests.unit.run_control.test_run_control import WORKFLOW_DIGEST, actor


@pytest.mark.asyncio
async def test_snapshot_fork_scoping_sponsorship_and_changed_replay() -> None:
    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id, "draft")
    result = await harness.run(await harness.request(unit))
    assert result.status == "completed"
    sources = FakeForkSourceReader(
        harness.run_control,
        heads={harness.run_id: (stagegraph_head(stages={"draft": "completed"}),)},
    )
    policies = ForkPatchPolicyRegistry()
    policies.register(WORKFLOW_DIGEST, stage_policy())
    forks = compose_in_memory_forks(
        harness.run_control,
        harness.repository,
        inspection_reads(harness.repository, harness.lineage, harness.journal),
        sources,
        policies=policies,
    )
    facade = MissionControlRuntimeService(forks.snapshots, forks.forks, request_scope="tenant-1")
    caller = actor().model_copy(
        update={"permissions": actor().permissions | {"workflow_run.snapshot", "workflow_run.fork"}}
    )
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    snapshot = await facade.snapshot(
        harness.run_id, MissionSnapshotRequest(expected_version=run.version), caller
    )
    assert snapshot.boundary_kind == "stage_settled"
    assert await facade.get_snapshot(harness.run_id, snapshot.snapshot_id, caller) == snapshot
    with pytest.raises(ForkSnapshotNotFound):
        await facade.get_snapshot("other-run", snapshot.snapshot_id, caller)
    other_tenant = MissionControlRuntimeService(
        forks.snapshots, forks.forks, request_scope="other-tenant"
    )
    with pytest.raises(ForkSnapshotNotFound):
        await other_tenant.get_snapshot(harness.run_id, snapshot.snapshot_id, caller)
    request = MissionForkRequest(
        request_id=uuid4(),
        snapshot_id=snapshot.snapshot_id,
        snapshot_digest=snapshot.snapshot_digest,
        changes=(ForkPatchChange(path="stage_objectives.review", value="Review strictly."),),
        invalidation_frontier=("review",),
        baseline_reservations={"tokens.total": 20},
        sponsorship_ref="sponsorship:test",
        approval_refs=("approval:test",),
        reason="independently admitted fork",
    )
    grants = {
        "sponsorship_refs": frozenset({"sponsorship:test"}),
        "approval_refs": frozenset({"approval:test"}),
    }
    with pytest.raises(MissionControlRejected, match="sponsorship"):
        await facade.fork(
            harness.run_id,
            request,
            caller,
            sponsorship_refs=frozenset(),
            approval_refs=grants["approval_refs"],
        )
    receipt = await facade.fork(harness.run_id, request, caller, **grants)
    assert receipt.request_id == request.request_id
    assert receipt.receipt.target_execution_epoch == 1
    assert receipt.receipt.source_run_id == harness.run_id
    assert await facade.fork(harness.run_id, request, caller, **grants) == receipt
    with pytest.raises(IdempotencyConflict):
        await facade.fork(
            harness.run_id,
            request.model_copy(update={"reason": "changed reason"}),
            caller,
            **grants,
        )


@pytest.mark.asyncio
async def test_privileged_reconciliation_resolves_real_incident_without_model_rerun() -> None:
    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    operation = await harness.request(unit)
    await _write_foreign_root_checkpoint(harness, _namespace(operation))
    assert (await harness.run(operation)).status == "in_doubt"
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None
    forks = compose_in_memory_forks(
        harness.run_control,
        harness.repository,
        inspection_reads(harness.repository, harness.lineage, harness.journal),
        FakeForkSourceReader(harness.run_control, heads={}),
    )
    facade = MissionControlRuntimeService(
        forks.snapshots,
        forks.forks,
        request_scope="tenant-1",
        reconciliation_service=harness.reconciliation,
    )
    current = await harness.run_control.get_run("tenant-1", harness.run_id)
    request = MissionReconciliationRequest(
        request_id=uuid4(),
        expected_version=current.version,
        action=ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=incident.incident_id,
            decision="abandon_unit",
        ),
        reason="abandon ambiguous execution without repeating effects",
    )
    with pytest.raises(MissionControlRejected, match="permission"):
        await facade.reconcile(harness.run_id, request, actor())
    reconciler = actor().model_copy(
        update={"permissions": actor().permissions | {"workflow_run.reconcile_unit"}}
    )
    receipt = await facade.reconcile(harness.run_id, request, reconciler)
    assert receipt.status == "accepted"
    assert await facade.reconcile(harness.run_id, request, reconciler) == receipt
    settled = await harness.run(operation)
    assert settled.status == "failed"
    assert settled.failure_code == "in_doubt_abandoned"
    assert harness.model.calls == []
