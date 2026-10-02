"""RRM-009 (RRM-007 F6): the relay re-drives accepted commands whose inline delivery failed."""

from __future__ import annotations

import pytest

from app.application.run_control.boundary_interventions import (
    BoundaryCommandDeliveryService,
    BoundaryInterventionService,
)
from app.application.run_control.boundary_relay import BoundaryCommandRelay
from app.domain.run_control.contracts import CommandStatus
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


def test_relay_requires_a_positive_interval() -> None:
    run_service, _ = service()
    with pytest.raises(ValueError, match="positive"):
        BoundaryCommandRelay(
            run_control=run_service,
            interventions=BoundaryInterventionService(run_service),
            request_scopes=("tenant-1",),
            interval_seconds=0,
        )
