"""RRM-007: the governed intervention facade and the `reconcile_unit` route (API seam).

Every public command enters `/run-control/v1/runs/{run_id}/commands` and reaches Temporal only
as a recorded delivery; the receipt ledger is readable; `reconcile_unit` has its own
privileged route that the plain `operator` role cannot use.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from mission_control.application.execution.boundary_interventions import (
    BoundaryCommandDeliveryService,
    BoundaryDeliveryResult,
    BoundaryInterventionService,
)
from mission_control.bootstrap.technical_api import api
from mission_control.domain.policies.contracts import (
    BoundaryCommandStatus,
    CommandResult,
    CommandStatus,
    LifecycleCommand,
    RunPhase,
    StartAction,
)
from mission_control.interfaces.http.control_plane import (
    ControlPlanePrincipal,
    get_control_plane_principal,
)
from mission_control.interfaces.http.run_control import (
    ROLE_PERMISSIONS,
    get_boundary_intervention_service,
    get_run_control_service,
    get_unit_reconciliation_service,
)
from tests.unit.run_control.test_boundary_commands import TARGET, pause
from tests.unit.run_control.test_run_control import (
    ALL_PERMISSIONS,
    actor,
    command,
    operator_wait,
    reconcile,
    request,
    service,
)
from tests.unit.run_control.test_run_control import (
    SetWaitAction as _SetWaitAction,
)

PARAMS = {"request_scope": "tenant-1"}


class RecordingTransport:
    def __init__(self) -> None:
        self.deliveries: list[str] = []

    async def deliver(self, status: BoundaryCommandStatus) -> BoundaryDeliveryResult:
        self.deliveries.append(status.command.command_id)
        return BoundaryDeliveryResult("delivered", "family/x@segment:1")


class RecordingReconciliation:
    def __init__(self, run_control: Any) -> None:
        self.run_control = run_control
        self.commands: list[LifecycleCommand] = []

    async def reconcile_unit(self, lifecycle: LifecycleCommand) -> CommandResult:
        self.commands.append(lifecycle)
        return await self.run_control.execute(lifecycle)


def _principal(roles: set[str]) -> ControlPlanePrincipal:
    return ControlPlanePrincipal(
        actor_id="operator",
        roles=frozenset(roles),
        tenant_scopes=frozenset({"tenant-1"}),
        authority_refs=frozenset({"authority:lifecycle"}),
    )


def _body(lifecycle: LifecycleCommand, permissions: frozenset[str] | None = None) -> dict:
    body = lifecycle.model_copy(
        update={"actor": actor().model_copy(update={"permissions": permissions or frozenset()})}
    ).model_dump(mode="json")
    return body


@pytest.mark.asyncio
async def test_commands_route_through_the_facade_and_receipts_are_readable() -> None:
    run_service, _ = service()
    admitted = await run_service.admit(request(request_id="api-boundary"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    started = await run_service.execute(
        command(run_id, 1, "start", StartAction(execution_target=TARGET))
    )
    assert started.status == CommandStatus.ACCEPTED
    transport = RecordingTransport()
    facade = BoundaryInterventionService(
        run_service, BoundaryCommandDeliveryService(run_service, transport)
    )
    api.dependency_overrides[get_run_control_service] = lambda: run_service
    api.dependency_overrides[get_boundary_intervention_service] = lambda: facade
    api.dependency_overrides[get_control_plane_principal] = lambda: _principal(
        {"operator", "relay"}
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://run-control"
        ) as client:
            response = await client.post(
                f"/run-control/v1/runs/{run_id}/commands",
                json=_body(command(run_id, 2, "pause", pause())),
            )
            assert response.status_code == 200, response.text
            result = response.json()
            assert result["status"] == "accepted"
            assert result["reason_code"] == "accepted_pending_application"
            assert result["phase"] == "active"
            assert transport.deliveries == ["pause"], "accepted commands are delivered"

            ledger = await client.get(
                f"/run-control/v1/runs/{run_id}/boundary-commands", params=PARAMS
            )
            assert ledger.status_code == 200
            [status] = ledger.json()
            assert status["command"]["kind"] == "pause"
            assert status["command"]["target"]["kind"] == "family"
            assert status["command"]["target_sequence"] == 1
            assert [item["state"] for item in status["receipts"]] == ["accepted", "delivered"]

            redelivered = await client.post(
                f"/run-control/v1/runs/{run_id}/boundary-commands/redeliver", params=PARAMS
            )
            assert redelivered.status_code == 200 and redelivered.json() == []
            assert transport.deliveries == ["pause"]

            refused = await client.post(
                f"/run-control/v1/runs/{run_id}/commands",
                json=_body(command(run_id, 2, "reconcile-here", reconcile())),
            )
            assert refused.status_code == 422
            assert "reconcile-unit" in refused.json()["detail"]
            schemas = (await client.get("/run-control/v1/schemas")).json()
            assert "boundary_command_status" in schemas
    finally:
        api.dependency_overrides.clear()
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.phase == RunPhase.ACTIVE and projection.active_pauses == ()


@pytest.mark.asyncio
async def test_reconcile_unit_route_requires_the_reconciliation_operator_role() -> None:
    assert "workflow_run.reconcile_unit" not in ROLE_PERMISSIONS["operator"]
    assert "workflow_run.reconcile_unit" in ROLE_PERMISSIONS["reconciliation_operator"]
    run_service, _ = service()
    admitted = await run_service.admit(request(request_id="api-reconcile"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    await run_service.execute(command(run_id, 1, "start", StartAction(execution_target=TARGET)))
    parked = await run_service.execute(
        command(
            run_id,
            2,
            "park",
            _SetWaitAction(condition=operator_wait(), runnable_work_remains=False),
        )
    )
    assert parked.status == CommandStatus.ACCEPTED
    reconciliation = RecordingReconciliation(run_service)
    facade = BoundaryInterventionService(run_service)
    api.dependency_overrides[get_run_control_service] = lambda: run_service
    api.dependency_overrides[get_boundary_intervention_service] = lambda: facade
    api.dependency_overrides[get_unit_reconciliation_service] = lambda: reconciliation
    body = _body(command(run_id, 3, "reconcile-abandon", reconcile()))
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=api), base_url="http://run-control"
        ) as client:
            api.dependency_overrides[get_control_plane_principal] = lambda: _principal({"operator"})
            forbidden = await client.post(
                f"/run-control/v1/runs/{run_id}/reconcile-unit", json=body
            )
            assert forbidden.status_code == 403
            assert reconciliation.commands == []

            api.dependency_overrides[get_control_plane_principal] = lambda: _principal(
                {"reconciliation_operator"}
            )
            decided = await client.post(f"/run-control/v1/runs/{run_id}/reconcile-unit", json=body)
            assert decided.status_code == 200, decided.text
            assert decided.json()["status"] == "accepted"
            [delivered] = reconciliation.commands
            assert "workflow_run.reconcile_unit" in delivered.actor.permissions
            assert delivered.actor.permissions.isdisjoint(ALL_PERMISSIONS - {"workflow_run.read"})

            ledger = await client.get(
                f"/run-control/v1/runs/{run_id}/boundary-commands", params=PARAMS
            )
            [status] = ledger.json()
            assert status["command"]["kind"] == "reconcile_unit"
            assert status["command"]["target"]["kind"] == "unit"
            assert [item["state"] for item in status["receipts"]] == ["accepted"]
    finally:
        api.dependency_overrides.clear()
    projection = await run_service.get_run("tenant-1", run_id)
    assert [item.decision for item in projection.unit_reconciliations] == ["abandon_unit"]
