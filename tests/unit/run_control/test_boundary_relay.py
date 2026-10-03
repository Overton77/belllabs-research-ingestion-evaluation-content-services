"""RRM-009 (RRM-007 F6): the relay re-drives accepted commands whose inline delivery failed."""

from __future__ import annotations

import pytest

from app.application.run_control.boundary_interventions import (
    BoundaryCommandDeliveryService,
    BoundaryInterventionService,
)
from app.application.run_control.boundary_relay import BoundaryCommandRelay
from app.domain.run_control.contracts import (
    CANCEL_SEQUENCE_SPACE,
    CancelAction,
    CommandStatus,
    RunPhase,
)
from tests.unit.run_control.test_boundary_commands import TARGET, pause, started, states
from tests.unit.run_control.test_boundary_interventions import ScriptedTransport, _status
from tests.unit.run_control.test_run_control import command, service


@pytest.mark.asyncio
async def test_relay_delivers_what_inline_delivery_could_not_in_order_and_once() -> None:
    run_service, _ = service()
    run_id = await started(run_service, "relay-run", TARGET)
    quiet = await started(run_service, "relay-quiet", TARGET)
    transport = ScriptedTransport({"first": RuntimeError("Temporal unavailable")})
    facade = BoundaryInterventionService(
        run_service, BoundaryCommandDeliveryService(run_service, transport)
    )
    accepted = await facade.execute(command(run_id, 2, "first", pause("p1")))
    assert accepted.status == CommandStatus.ACCEPTED
    assert transport.deliveries == []
    assert states(await _status(run_service, run_id, "first")) == ["accepted"]
    # Only the run with an accepted, undelivered family command is pending, in its scope.
    assert await run_service.runs_with_pending_boundary_commands("tenant-1") == (run_id,)
    assert await run_service.runs_with_pending_boundary_commands("tenant-2") == ()
    assert quiet not in await run_service.runs_with_pending_boundary_commands("tenant-1")

    relay = BoundaryCommandRelay(
        run_control=run_service,
        interventions=facade,
        request_scopes=("tenant-1", "tenant-1", "tenant-2"),
        interval_seconds=0.5,
    )
    assert relay.request_scopes == ("tenant-1", "tenant-2")
    still_down = await relay.run_once()
    assert still_down.runs == (("tenant-1", run_id),) and still_down.delivered == 0
    assert transport.deliveries == []

    transport.script.clear()
    recovered = await relay.run_once()
    assert recovered.runs == (("tenant-1", run_id),) and recovered.delivered == 1
    assert transport.deliveries == ["first"]
    assert states(await _status(run_service, run_id, "first")) == ["accepted", "delivered"]
    # Delivered commands leave the pending set; a further pass delivers nothing twice.
    assert await run_service.runs_with_pending_boundary_commands("tenant-1") == ()
    idle = await relay.run_once()
    assert idle.runs == () and transport.deliveries == ["first"]


@pytest.mark.asyncio
async def test_relay_delivers_a_cancel_in_its_own_space_before_the_blocked_pause() -> None:
    """RRM-008 composed by RRM-009: a cancel whose inline delivery failed is pending in its
    own `cancel` space; the relay delivers it first and the earlier pause after it."""

    run_service, _ = service()
    run_id = await started(run_service, "relay-cancel", TARGET)
    transport = ScriptedTransport(
        {"pause": RuntimeError("Temporal unavailable"), "cancel": RuntimeError("down")}
    )
    facade = BoundaryInterventionService(
        run_service, BoundaryCommandDeliveryService(run_service, transport)
    )
    await facade.execute(command(run_id, 2, "pause", pause("p1")))
    cancelled = await facade.execute(command(run_id, 2, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    assert transport.deliveries == []
    cancel_status = await _status(run_service, run_id, "cancel")
    assert cancel_status.command.target.sequence_space == CANCEL_SEQUENCE_SPACE
    assert cancel_status.command.target_sequence == 1
    assert states(cancel_status) == ["accepted"]
    assert await run_service.runs_with_pending_boundary_commands("tenant-1") == (run_id,)

    relay = BoundaryCommandRelay(
        run_control=run_service,
        interventions=facade,
        request_scopes=("tenant-1",),
        interval_seconds=0.5,
    )
    transport.script.clear()
    recovered = await relay.run_once()
    assert recovered.runs == (("tenant-1", run_id),)
    # The cancel space is delivered first; the pause keeps its own order behind it.
    assert transport.deliveries == ["cancel", "pause"]
    assert states(await _status(run_service, run_id, "cancel")) == ["accepted", "delivered"]
    assert await run_service.runs_with_pending_boundary_commands("tenant-1") == ()
    assert (await relay.run_once()).runs == ()
    assert transport.deliveries == ["cancel", "pause"]


def test_relay_requires_a_positive_interval() -> None:
    run_service, _ = service()
    with pytest.raises(ValueError, match="positive"):
        BoundaryCommandRelay(
            run_control=run_service,
            interventions=BoundaryInterventionService(run_service),
            request_scopes=("tenant-1",),
            interval_seconds=0,
        )
