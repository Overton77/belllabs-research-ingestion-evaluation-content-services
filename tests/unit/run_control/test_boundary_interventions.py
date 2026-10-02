"""RRM-007: governed delivery of accepted boundary commands (REQ-CP-EXEC-006/007).

The delivery service delivers accepted commands to their exact target in target-sequence
order through a transport whose acknowledgement is evidence of `delivered` only. A
transport failure stops the pass so no later command overtakes an earlier one; redelivery
is safe because receipts never transition twice. Stale targets are terminal rejections.
"""

from __future__ import annotations

import pytest

from app.application.run_control.boundary_interventions import (
    BoundaryCommandApplicationService,
    BoundaryCommandDeliveryService,
    BoundaryDeliveryResult,
    BoundaryInterventionService,
)
from app.domain.run_control.contracts import (
    ActorContext,
    BoundaryCommandStatus,
    CommandStatus,
    RunPhase,
    SatisfyWaitAction,
    SetWaitAction,
)
from tests.unit.run_control.test_boundary_commands import (
    BOUNDARY_PERMISSIONS,
    FAMILY_WORKFLOW_ID,
    TARGET,
    declared_wait,
    pause,
    started,
    states,
)
from tests.unit.run_control.test_run_control import command, service


class ScriptedTransport:
    """A transport that records deliveries and follows a per-command script."""

    def __init__(self, script: dict[str, str | Exception] | None = None) -> None:
        self.script = script or {}
        self.deliveries: list[str] = []

    async def deliver(self, status: BoundaryCommandStatus) -> BoundaryDeliveryResult:
        command_id = status.command.command_id
        outcome = self.script.get(command_id, "delivered")
        if isinstance(outcome, Exception):
            raise outcome
        self.deliveries.append(command_id)
        return BoundaryDeliveryResult(outcome, f"{FAMILY_WORKFLOW_ID}@segment:1", "scripted")


def boundary_actor() -> ActorContext:
    return ActorContext(
        actor_id="orchestration-authority",
        authority_refs=frozenset({"orchestration-authority"}),
        permissions=BOUNDARY_PERMISSIONS,
    )


@pytest.mark.asyncio
async def test_delivery_follows_target_sequence_and_stops_at_the_first_failure() -> None:
    run_service, _ = service()
    run_id = await started(run_service, "delivery-order", TARGET)
    await run_service.execute(
        command(
            run_id,
            2,
            "declare",
            SetWaitAction(condition=declared_wait(), runnable_work_remains=True),
        )
    )
    transport = ScriptedTransport({"second": RuntimeError("Temporal unavailable")})
    facade = BoundaryInterventionService(
        run_service, BoundaryCommandDeliveryService(run_service, transport)
    )

    first = await facade.execute(command(run_id, 3, "first", pause("p1")))
    assert first.status == CommandStatus.ACCEPTED
    assert transport.deliveries == ["first"]
    assert states(await _status(run_service, run_id, "first")) == ["accepted", "delivered"]

    second = await facade.execute(
        command(
            run_id,
            3,
            "second",
            SatisfyWaitAction(
                condition_id=declared_wait().condition_id,
                verification_evidence_ref="evidence:operator",
            ),
        )
    )
    third = await facade.execute(command(run_id, 3, "third", pause("p3")))
    assert second.status == third.status == CommandStatus.ACCEPTED
    assert transport.deliveries == ["first"], "the third command never overtakes the second"
    assert states(await _status(run_service, run_id, "second")) == ["accepted"]
    assert states(await _status(run_service, run_id, "third")) == ["accepted"]
    assert [
        (item.command.command_id, item.command.target_sequence)
        for item in await facade.list_commands("tenant-1", run_id)
    ] == [("first", 1), ("second", 2), ("third", 3)]

    # Redelivery after the transport recovers delivers the rest in order, once each.
    transport.script.clear()
    redelivered = await facade.redeliver("tenant-1", run_id)
    assert [item.command.command_id for item in redelivered] == ["second", "third"]
    assert transport.deliveries == ["first", "second", "third"]
    assert await facade.redeliver("tenant-1", run_id) == ()
    assert transport.deliveries == ["first", "second", "third"]
    for command_id in ("first", "second", "third"):
        assert states(await _status(run_service, run_id, command_id)) == ["accepted", "delivered"]
    # Delivery is not application: the projection still shows no pause and the wait held.
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.phase == RunPhase.ACTIVE
    assert projection.active_pauses == ()
    assert [item.condition_id for item in projection.active_waits] == [declared_wait().condition_id]


@pytest.mark.asyncio
async def test_stale_target_acknowledgements_are_terminal_rejections() -> None:
    run_service, _ = service()
    run_id = await started(run_service, "delivery-stale", TARGET)
    transport = ScriptedTransport({"old": "stale_generation", "dup": "duplicate"})
    delivery = BoundaryCommandDeliveryService(run_service, transport)
    facade = BoundaryInterventionService(run_service, delivery)

    await facade.execute(command(run_id, 2, "old", pause("p-old")))
    await facade.execute(command(run_id, 2, "dup", pause("p-dup")))
    old = await _status(run_service, run_id, "old")
    assert states(old) == ["accepted", "rejected"]
    assert old.receipts[-1].rejection_reason == "stale_generation"
    assert old.receipts[-1].transport_ref == f"{FAMILY_WORKFLOW_ID}@segment:1"
    dup = await _status(run_service, run_id, "dup")
    assert states(dup) == ["accepted", "delivered"], "a duplicate ack is still a delivery"
    assert await facade.redeliver("tenant-1", run_id) == ()


@pytest.mark.asyncio
async def test_application_service_binds_the_current_version_and_replays_idempotently() -> None:
    """A family never fails on version drift: the fact binds the version at execution, and
    an exact replay of a fact returns its stored result."""

    run_service, _ = service()
    run_id = await started(run_service, "application-drift", TARGET)
    transport = ScriptedTransport()
    facade = BoundaryInterventionService(
        run_service, BoundaryCommandDeliveryService(run_service, transport)
    )
    await facade.execute(command(run_id, 2, "pause", pause()))
    # Drift: another family fact moved the version after the pause was accepted.
    await run_service.execute(
        command(
            run_id,
            2,
            "declare",
            SetWaitAction(condition=declared_wait(), runnable_work_remains=True),
        )
    )
    assert (await run_service.get_run("tenant-1", run_id)).version == 3

    application = BoundaryCommandApplicationService(run_service, boundary_actor())
    action = {
        "kind": "apply_boundary_command",
        "command_id": "pause",
        "action": pause().model_dump(mode="json"),
        "boundary_ref": FAMILY_WORKFLOW_ID,
        "runnable_work_remains": False,
        "boundary_state": {"family": "GoalDirected", "next_goal_iteration": 2},
    }
    applied = await application.execute(
        request_scope="tenant-1",
        run_id=run_id,
        command_id="boundary-apply:pause",
        idempotency_issuer="goal-directed-worker",
        correlation_id="goal:run",
        action=action,
        reason="applied at the iteration boundary",
    )
    assert (applied.status, applied.phase, applied.resulting_run_version) == (
        CommandStatus.ACCEPTED,
        RunPhase.PAUSED,
        4,
    )
    status = await _status(run_service, run_id, "pause")
    assert states(status) == ["accepted", "delivered", "applied"]
    assert status.receipts[-1].boundary_state == {
        "family": "GoalDirected",
        "next_goal_iteration": 2,
    }
    replayed = await application.execute(
        request_scope="tenant-1",
        run_id=run_id,
        command_id="boundary-apply:pause",
        idempotency_issuer="goal-directed-worker",
        correlation_id="goal:run",
        action=action,
        reason="applied at the iteration boundary",
    )
    assert replayed == applied
    assert (await run_service.get_run("tenant-1", run_id)).version == 4
    assert await application.boundary_receipt_state("tenant-1", run_id, "pause") == "applied"


async def _status(run_service, run_id: str, command_id: str) -> BoundaryCommandStatus:  # type: ignore[no-untyped-def]
    status = await run_service.get_boundary_command("tenant-1", run_id, command_id)
    assert status is not None
    return status
