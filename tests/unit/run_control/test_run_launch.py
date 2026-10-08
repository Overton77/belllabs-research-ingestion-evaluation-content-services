"""RRM-009: the governed launch binds the caller's family input to the admitted authority."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any

import pytest

from mission_control.application.execution.run_launch import (
    RunLaunchRejected,
    RunLaunchRequest,
    RunLaunchService,
    fork_semantic_input_binding_ref,
)
from mission_control.application.recovery.run_forks import ForkOfRun
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.coordinator.launch import BlueprintFamily, WorkflowSubmission
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.programs.contracts import StageGraphRunInput
from tests.fixtures.rrm009_production_stack import stage_blueprint
from tests.unit.run_control.test_run_control import (
    DIGEST,
    WORKFLOW_DIGEST,
    actor,
    request,
    service,
)

SCOPE = "tenant-1"


class RecordingSubmitter:
    def __init__(self) -> None:
        self.submissions: list[tuple[str, BlueprintFamily, str | None]] = []

    async def submit(
        self,
        workflow_input: object,
        *,
        workflow_id: str,
        blueprint_family: BlueprintFamily,
        parent_run_id: str | None = None,
    ) -> WorkflowSubmission:
        self.submissions.append((workflow_id, blueprint_family, parent_run_id))
        return WorkflowSubmission(workflow_id=workflow_id, temporal_run_id="temporal-run-1")


class UnmaterializedFork:
    async def fork_of_run(self, request_scope: str, derived_run_id: str) -> ForkOfRun | None:
        del request_scope, derived_run_id
        return ForkOfRun(fork_request_id="fork-1", materialized=False)


class NoFork:
    async def fork_of_run(self, request_scope: str, derived_run_id: str) -> ForkOfRun | None:
        del request_scope, derived_run_id
        return None


class NoForkReceipts:
    async def get_request(self, request_scope: str, request_id: str) -> Any:
        del request_scope, request_id
        return None

    async def get(self, request_scope: str, request_id: str) -> Any:
        del request_scope, request_id
        return None


def _input(run_id: str, *, version: int = 1, **overrides: Any) -> dict[str, Any]:
    graph = stage_blueprint()
    values = asdict(
        StageGraphRunInput(
            run_id=run_id,
            request_scope=SCOPE,
            effective_configuration_digest=DIGEST,
            workflow_type_digest=WORKFLOW_DIGEST,
            blueprint_digest=sha256_digest(graph),
            blueprint=graph.model_dump(mode="json"),
            initial_run_version=version,
            semantic_input_binding_ref="semantic-input:launch",
            # `request()` admits this baseline reservation.
            baseline_reservation={"tokens.total": 20},
        )
    )
    values.update(overrides)
    return values


def _launch(run_id: str, payload: dict[str, Any], **extra: Any) -> RunLaunchRequest:
    return RunLaunchRequest(
        request_scope=SCOPE, run_id=run_id, family="StageGraph", stagegraph=payload, **extra
    )


@pytest.mark.asyncio
async def test_launch_starts_only_an_input_bound_to_the_admitted_run() -> None:
    run_control, _ = service()
    admitted = await run_control.admit(request(request_id="launch-1"))
    run_id = admitted.run_id
    assert run_id is not None
    submitter = RecordingSubmitter()
    launcher = RunLaunchService(run_control=run_control, submitter=submitter)

    with pytest.raises(RunLaunchRejected, match="workflow_run.start"):
        await launcher.launch(
            _launch(run_id, _input(run_id)),
            ActorContext(actor_id="operator", permissions=frozenset()),
        )
    for payload, code in (
        (_input(run_id, version=2), "stale_run_version"),
        (
            _input(run_id, effective_configuration_digest="sha256:" + "f" * 64),
            "configuration_mismatch",
        ),
        (_input(run_id, workflow_type_digest="sha256:" + "f" * 64), "configuration_mismatch"),
        (_input(run_id, blueprint_digest="sha256:" + "f" * 64), "configuration_mismatch"),
        (_input("another-run"), "identity_mismatch"),
        (_input(run_id, semantic_input_binding_ref=""), "invalid_family_input"),
        (_input(run_id, execution_epoch=2), "invalid_family_input"),
        (_input(run_id, baseline_reservation={}), "budget_mismatch"),
        (_input(run_id, baseline_reservation={"tokens.total": 21}), "budget_mismatch"),
    ):
        with pytest.raises(RunLaunchRejected) as rejected:
            await launcher.launch(_launch(run_id, payload), actor())
        assert rejected.value.code == code, (code, rejected.value.message)
    assert submitter.submissions == []

    receipt = await launcher.launch(_launch(run_id, _input(run_id)), actor())
    assert receipt.workflow_id == f"belllabs-run/{run_id}"
    assert receipt.parent_run_id is None and receipt.fork_request_id is None
    assert receipt.accepted_run_version == 1
    assert submitter.submissions == [(f"belllabs-run/{run_id}", BlueprintFamily.STAGE_GRAPH, None)]

    with pytest.raises(RunLaunchRejected, match="exactly one family input"):
        await launcher.launch(
            RunLaunchRequest(request_scope=SCOPE, run_id=run_id, family="GoalDirected"), actor()
        )


@pytest.mark.asyncio
async def test_fork_derived_run_waits_for_materialization_and_binds_the_fork_reference() -> None:
    run_control, _ = service()
    admitted = await run_control.admit(request(request_id="launch-fork"))
    run_id = admitted.run_id
    assert run_id is not None
    submitter = RecordingSubmitter()
    pending = RunLaunchService(
        run_control=run_control,
        submitter=submitter,
        forks=NoForkReceipts(),
        materializations=UnmaterializedFork(),
    )
    with pytest.raises(RunLaunchRejected) as rejected:
        await pending.launch(_launch(run_id, _input(run_id)), actor())
    assert rejected.value.code == "fork_not_materialized" and rejected.value.retryable
    assert submitter.submissions == []

    plain = RunLaunchService(
        run_control=run_control,
        submitter=submitter,
        forks=NoForkReceipts(),
        materializations=NoFork(),
    )
    receipt = await plain.launch(_launch(run_id, _input(run_id)), actor())
    assert receipt.parent_run_id is None
    assert fork_semantic_input_binding_ref("fork-1") == "semantic-input:fork:fork-1"


class MissionAwareSubmitter(RecordingSubmitter):
    def __init__(self) -> None:
        super().__init__()
        self.mission_ids: list[str | None] = []

    async def submit(
        self,
        workflow_input: object,
        *,
        workflow_id: str,
        blueprint_family: BlueprintFamily,
        parent_run_id: str | None = None,
        mission_id: str | None = None,
    ) -> WorkflowSubmission:
        self.mission_ids.append(mission_id)
        return await super().submit(
            workflow_input,
            workflow_id=workflow_id,
            blueprint_family=blueprint_family,
            parent_run_id=parent_run_id,
        )


@pytest.mark.asyncio
async def test_launch_passes_the_ledger_mission_to_the_root_search_attributes() -> None:
    """FT-C4: the root starts with `mc_mission_id` from the ledger when it is resolvable."""

    run_control, _ = service()
    admitted = await run_control.admit(request(request_id="launch-mission"))
    run_id = admitted.run_id
    assert run_id is not None
    submitter = MissionAwareSubmitter()

    async def mission_ids(request_scope: str, run: str) -> str | None:
        assert (request_scope, run) == (SCOPE, run_id)
        return "mission-ft-c4"

    launcher = RunLaunchService(
        run_control=run_control, submitter=submitter, mission_ids=mission_ids
    )
    await launcher.launch(_launch(run_id, _input(run_id)), actor())
    assert submitter.mission_ids == ["mission-ft-c4"]
