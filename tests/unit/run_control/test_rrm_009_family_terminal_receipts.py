"""RRM-009 (composing RRM-008 F1): a family-admitted terminalization closes the ledger.

StageGraph terminalizes through its atomic family admission (`execute_family_admission`
with a `terminalize` action), not through a plain lifecycle command. The cancel it was
cancelled by must still be `applied` in the terminalizing commit, and every other pending
command must reach its terminal receipt, exactly as on the plain path. The production
cancellation drill found the gap (`tests/acceptance/control_plane/
test_rrm_009_production_cancellation.py`: the run was terminal `cancelled`, its cancel stayed
`delivered`).
"""

from __future__ import annotations

from typing import Any

import pytest

from mission_control.application.execution.run_control_repository import (
    InMemoryRunControlRepository,
)
from mission_control.application.execution.service import FamilyAdmissionRegistry, RunControlService
from mission_control.domain.policies.contracts import (
    CancelAction,
    CommandStatus,
    RecordUsageAction,
    RunOutcome,
    RunPhase,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
)
from tests.unit.run_control.test_atomic_family_admission import (
    TestFamilyMutation,
    admission_policies,
)
from tests.unit.run_control.test_boundary_commands import TARGET, pause, states
from tests.unit.run_control.test_run_control import (
    EMPTY_EVIDENCE_DIGEST,
    INITIAL_EVIDENCE_FRONTIER,
    NOW,
    WORKFLOW_DIGEST,
    ConfigurationVerifier,
    command,
    request,
)


def terminal_family_registry() -> FamilyAdmissionRegistry:
    families = FamilyAdmissionRegistry()
    families.register(
        TestFamilyMutation,
        family_kind="test_family",
        mutation_kind="completion_proposed",
        required_permission="workflow_run.terminalize",
        allowed_action_kinds=frozenset({"terminalize"}),
    )
    return families


def terminal_family_service(repository: Any) -> RunControlService:
    return RunControlService(
        repository, ConfigurationVerifier(), admission_policies(), terminal_family_registry()
    )


async def cancelled_run(run_service: RunControlService) -> str:
    """A started run with a pending pause; the cancel is accepted (never delivered: no
    transport) and the baseline released, so the run may terminalize `cancelled`."""

    admitted = await run_service.admit(request(request_id="family-terminal"))
    run_id = admitted.run_id
    assert run_id is not None
    for version, command_id, action in (
        (1, "start", StartAction(execution_target=TARGET)),
        (2, "pause", pause()),
        (2, "cancel", CancelAction()),
        (
            3,
            "release-baseline",
            RecordUsageAction(
                usage_id="usage:release",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        ),
    ):
        result = await run_service.execute(command(run_id, version, command_id, action))
        assert result.status == CommandStatus.ACCEPTED, result
    run = await run_service.get_run("tenant-1", run_id)
    assert run.phase == RunPhase.CANCELLING
    return run_id


async def terminalize_through_the_family(run_service: RunControlService, run_id: str) -> Any:
    """StageGraph's shape: the completion proposal is admitted atomically with its family
    mutation (`execute_family_admission` with `terminalize`)."""

    run = await run_service.get_run("tenant-1", run_id)
    return await run_service.execute_family_admission(
        command(
            run_id,
            run.version,
            "family-terminalize",
            TerminalizeAction(
                proposal=TerminalizationProposal(
                    proposal_id="family-terminal",
                    expected_run_version=run.version,
                    workflow_type_digest=WORKFLOW_DIGEST,
                    obligation_revision="obligations:1",
                    evidence_frontier_digest=INITIAL_EVIDENCE_FRONTIER,
                    accepted_obligation_evidence_digest=EMPTY_EVIDENCE_DIGEST,
                    proposing_execution_binding_ref="execution:test",
                    required_obligations_accepted=True,
                    cancellation_settled=True,
                    budget_settled=True,
                    effects_settled=True,
                    proposed_at=NOW,
                )
            ),
        ),
        TestFamilyMutation(
            family_kind="test_family",
            mutation_kind="completion_proposed",
            mutation_id="family-completion-1",
            request_scope="tenant-1",
            run_id=run_id,
            expected_family_version=0,
            exact_operation_request_ref="operation-request:sha256-test",
            decided_at=NOW,
            candidate_ref="output:none",
        ),
    )


async def assert_ledger_closed(run_service: RunControlService, run_id: str, receipt: Any) -> None:
    assert receipt.command_result.status == CommandStatus.ACCEPTED, receipt
    assert receipt.command_result.terminal_outcome == RunOutcome.CANCELLED
    statuses = {
        status.command.command_id: status
        for status in await run_service.list_boundary_commands("tenant-1", run_id)
    }
    # The cancel is applied by the cancelled outcome (delivered recorded by run control,
    # since no transport ever delivered it); the pause is closed `terminal_run`.
    assert states(statuses["cancel"]) == ["accepted", "delivered", "applied"]
    assert [item.recorded_by for item in statuses["cancel"].receipts[1:]] == [
        "run_control",
        "run_control",
    ]
    assert states(statuses["pause"]) == ["accepted", "rejected"]
    assert statuses["pause"].receipts[-1].rejection_reason == "terminal_run"
    assert await run_service.runs_with_pending_boundary_commands("tenant-1") == ()


@pytest.mark.asyncio
async def test_family_admitted_cancelled_terminalization_applies_the_cancel() -> None:
    run_service = terminal_family_service(InMemoryRunControlRepository())
    run_id = await cancelled_run(run_service)
    receipt = await terminalize_through_the_family(run_service, run_id)
    await assert_ledger_closed(run_service, run_id, receipt)
    run = await run_service.get_run("tenant-1", run_id)
    assert (run.phase, run.terminal_outcome) == (RunPhase.TERMINAL, RunOutcome.CANCELLED)
