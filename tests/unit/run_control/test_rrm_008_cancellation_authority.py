"""RRM-008: run control under cancellation (REQ-CP-EXEC-008 steps 1, 2 and 7).

Journal first: the cancel is accepted and recorded before anything reaches Temporal. Its
delivery goes root-first through the boundary-command ledger in the `cancel` sequence space
(`accepted -> delivered`), and `applied` is recorded only by the `cancelled` terminal outcome.
A family that starts after the cancel was accepted binds its execution target and keeps the
run `cancelling`; a cancelling run terminalizes over its declared waits but never over an
`operator_reconciliation` wait (REQ-CP-RUN-007).
"""

from __future__ import annotations

import pytest

from app.application.run_control.boundary_interventions import (
    BoundaryCommandDeliveryService,
    BoundaryInterventionService,
)
from app.application.run_control.run_control_repository import pending_delivery
from app.domain.orchestration.contracts import CancelAck, CancelDelivery
from app.domain.run_control.contracts import (
    CancelAction,
    CommandStatus,
    RecordUsageAction,
    RunOutcome,
    RunPhase,
    SetWaitAction,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
)
from app.integrations.temporal_boundary_commands import (
    CANCEL_DELIVERY_UPDATE,
    TemporalBoundaryCommandTransport,
)
from tests.unit.run_control.test_boundary_commands import (
    FAMILY_WORKFLOW_ID,
    ROOT_WORKFLOW_ID,
    TARGET,
    declared_wait,
    started,
    states,
)
from tests.unit.run_control.test_boundary_interventions import (
    ScriptedClient,
    ScriptedHandle,
    ScriptedTransport,
    _status,
    pause,
)
from tests.unit.run_control.test_run_control import (
    EMPTY_EVIDENCE_DIGEST,
    INITIAL_EVIDENCE_FRONTIER,
    NOW,
    WORKFLOW_DIGEST,
    command,
    operator_wait,
    reconcile,
    reconciler_command,
    request,
    service,
)


def _terminal(version: int) -> TerminalizeAction:
    return TerminalizeAction(
        proposal=TerminalizationProposal(
            proposal_id=f"terminal:{version}",
            expected_run_version=version,
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
    )


def _release_baseline() -> RecordUsageAction:
    return RecordUsageAction(
        usage_id="usage:release-baseline",
        reservation_id="baseline",
        actual_amounts={},
        release_amounts={"tokens.total": 20},
    )


@pytest.mark.asyncio
async def test_start_on_a_cancelling_run_binds_the_target_and_keeps_cancelling() -> None:
    """A cancel accepted before the family's `start` fact (run control was the boundary):
    the family still binds its execution target and runs the saga under `cancelling`."""

    run_service, _ = service()
    admitted = await run_service.admit(request(request_id="cancel-before-start"))
    run_id = admitted.run_id
    assert run_id is not None
    cancelled = await run_service.execute(command(run_id, 1, "cancel", CancelAction()))
    assert cancelled.status == CommandStatus.ACCEPTED and cancelled.phase == RunPhase.CANCELLING
    assert states(await _status(run_service, run_id, "cancel")) == ["accepted", "delivered"]

    begun = await run_service.execute(
        command(run_id, 2, "start", StartAction(execution_target=TARGET))
    )
    assert begun.status == CommandStatus.ACCEPTED
    assert begun.phase == RunPhase.CANCELLING
    run = await run_service.get_run("tenant-1", run_id)
    assert run.execution_target == TARGET
    assert run.phase == RunPhase.CANCELLING

    await run_service.execute(command(run_id, 3, "release", _release_baseline()))
    terminal = await run_service.execute(command(run_id, 4, "terminal", _terminal(4)))
    assert terminal.terminal_outcome == RunOutcome.CANCELLED
    # The cancel accepted while run control was the boundary is applied by the outcome.
    assert states(await _status(run_service, run_id, "cancel")) == [
        "accepted",
        "delivered",
        "applied",
    ]


@pytest.mark.asyncio
async def test_cancelling_terminalization_cancels_declared_waits_but_not_operator_waits() -> None:
    run_service, _ = service()
    run_id = await started(run_service, "cancel-waits", TARGET)
    held = await run_service.execute(
        command(
            run_id,
            2,
            "wait",
            SetWaitAction(condition=declared_wait(), runnable_work_remains=False),
        )
    )
    assert held.phase == RunPhase.WAITING
    cancelled = await run_service.execute(command(run_id, 3, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    await run_service.execute(command(run_id, 4, "release", _release_baseline()))
    # The declared wait is still active: a cancelled run cancels it with the outcome.
    waiting = await run_service.get_run("tenant-1", run_id)
    assert [item.condition_id for item in waiting.active_waits] == ["stagegraph-wait:release"]
    terminal = await run_service.execute(command(run_id, 5, "terminal", _terminal(5)))
    assert terminal.status == CommandStatus.ACCEPTED, terminal.reason
    assert terminal.terminal_outcome == RunOutcome.CANCELLED

    # An `operator_reconciliation` wait (an in_doubt unit) keeps the run cancelling.
    run_service, _ = service()
    run_id = await started(run_service, "cancel-operator-wait", TARGET)
    await run_service.execute(command(run_id, 2, "cancel", CancelAction()))
    parked = await run_service.execute(
        command(
            run_id,
            3,
            "operator-wait",
            SetWaitAction(condition=operator_wait(), runnable_work_remains=False),
        )
    )
    assert parked.status == CommandStatus.ACCEPTED and parked.phase == RunPhase.CANCELLING
    await run_service.execute(command(run_id, 4, "release", _release_baseline()))
    refused = await run_service.execute(command(run_id, 5, "terminal", _terminal(5)))
    assert refused.status == CommandStatus.REJECTED
    assert refused.reason_code == "unresolved_terminal_dependencies"
    assert (await run_service.get_run("tenant-1", run_id)).phase == RunPhase.CANCELLING
    # Review note: a cancelling run re-executes nothing, so `start_new_generation` is
    # rejected; `abandon_unit` settles the parked unit and the saga can finish.
    new_generation = await run_service.execute(
        reconciler_command(run_id, 5, "new-generation", reconcile("start_new_generation"))
    )
    assert new_generation.status == CommandStatus.REJECTED
    assert new_generation.reason_code == "cancelling_run_rejects_new_generation"
    abandoned = await run_service.execute(
        reconciler_command(run_id, 5, "abandon", reconcile("abandon_unit"))
    )
    assert abandoned.status == CommandStatus.ACCEPTED and abandoned.phase == RunPhase.CANCELLING
    terminal = await run_service.execute(command(run_id, 6, "terminal-2", _terminal(6)))
    assert terminal.status == CommandStatus.ACCEPTED, terminal.reason
    assert terminal.terminal_outcome == RunOutcome.CANCELLED


@pytest.mark.asyncio
async def test_cancel_is_delivered_root_first_in_its_own_space_before_other_commands() -> None:
    """Steps 1 and 2 of the saga: the `accepted` receipt precedes any delivery; the cancel is
    pending delivery in the `cancel` space, is delivered ahead of the `execution` space and
    is not blocked by a failing `execution` delivery; the family acknowledgement records
    `delivered`; the `cancelled` outcome records `applied`."""

    run_service, _ = service()
    run_id = await started(run_service, "cancel-delivery", TARGET)
    transport = ScriptedTransport({"pause": RuntimeError("Temporal unavailable")})
    facade = BoundaryInterventionService(
        run_service, BoundaryCommandDeliveryService(run_service, transport)
    )
    await facade.execute(command(run_id, 2, "pause", pause()))
    assert transport.deliveries == []
    cancelled = await facade.execute(command(run_id, 2, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    status = await _status(run_service, run_id, "cancel")
    assert (status.command.target.kind, status.command.target.sequence_space) == ("root", "cancel")
    assert status.command.target_sequence == 1
    assert transport.deliveries == ["cancel"], "delivered once, ahead of the blocked pause"
    assert states(status) == ["accepted", "delivered"]
    assert status.receipts[-1].recorded_by == "boundary-delivery"
    assert not pending_delivery(status)
    # Redelivery never delivers the cancel twice.
    transport.script.clear()
    await facade.redeliver("tenant-1", run_id)
    assert transport.deliveries == ["cancel", "pause"]

    await run_service.execute(command(run_id, 3, "release", _release_baseline()))
    terminal = await run_service.execute(command(run_id, 4, "terminal", _terminal(4)))
    assert terminal.terminal_outcome == RunOutcome.CANCELLED
    assert states(await _status(run_service, run_id, "cancel")) == [
        "accepted",
        "delivered",
        "applied",
    ]
    assert states(await _status(run_service, run_id, "pause")) == [
        "accepted",
        "delivered",
        "rejected",
    ]


@pytest.mark.asyncio
async def test_temporal_transport_delivers_a_cancel_through_dedicated_updates() -> None:
    """The cancel never consumes an `execution` sequence: it is delivered to the root and to
    the family through `deliver_cancel`, and only the family acknowledgement is `delivered`."""

    run_service, _ = service()
    run_id = await started(run_service, "transport-cancel", TARGET)
    await run_service.execute(command(run_id, 2, "cancel", CancelAction()))
    status = await _status(run_service, run_id, "cancel")
    root = ScriptedHandle("RUNNING", [CancelAck("cancel", "delivered", 1)])
    family = ScriptedHandle("RUNNING", [CancelAck("cancel", "delivered", 1, "cancelling")])
    transport = TemporalBoundaryCommandTransport(
        ScriptedClient({ROOT_WORKFLOW_ID: root, FAMILY_WORKFLOW_ID: family})  # type: ignore[arg-type]
    )
    ack = await transport.deliver(status)
    assert (ack.status, ack.transport_ref) == ("delivered", f"{FAMILY_WORKFLOW_ID}@segment:1")
    assert [name for name, _ in root.calls] == [CANCEL_DELIVERY_UPDATE]
    assert [name for name, _ in family.calls] == [CANCEL_DELIVERY_UPDATE]
    delivery = root.calls[0][1]
    assert isinstance(delivery, CancelDelivery)
    assert (delivery.command_id, delivery.target_sequence, delivery.execution_generation) == (
        "cancel",
        1,
        1,
    )
    assert delivery.payload_digest == status.command.payload_digest

    # A root that moved past the command's generation is a terminal rejection.
    stale_root = ScriptedHandle("RUNNING", [CancelAck("cancel", "stale_generation", 2)])
    transport = TemporalBoundaryCommandTransport(
        ScriptedClient({ROOT_WORKFLOW_ID: stale_root, FAMILY_WORKFLOW_ID: family})  # type: ignore[arg-type]
    )
    assert (await transport.deliver(status)).status == "stale_generation"
    # A closed family is `stale_target` before any Update.
    closed = ScriptedHandle("COMPLETED", [])
    transport = TemporalBoundaryCommandTransport(
        ScriptedClient({ROOT_WORKFLOW_ID: root, FAMILY_WORKFLOW_ID: closed})  # type: ignore[arg-type]
    )
    assert (await transport.deliver(status)).status == "stale_target"
