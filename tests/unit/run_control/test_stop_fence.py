"""FT-F3: the Stop Fence of an immediate cancel, its gate and its admission (no provider)."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.stop_fence import (
    InMemoryStopFenceRepository,
    KernelHookFenceGate,
)
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import (
    CancelPayload,
    CommandTarget,
    MissionCommandRequest,
    MissionControlRejected,
)
from mission_control.domain.authoring.canonical import contract_fingerprint, sha256_digest
from mission_control.domain.policies.contracts import ActorContext, CancelAction, RunPhase
from mission_control.domain.policies.stop_fence import (
    STOP_FENCED,
    STOP_NOW_NOTE,
    EffectAdmission,
    FenceVerdict,
    ImmediateCancelReport,
    StopFence,
    fence_verdict,
    immediate_cancel_permissions,
)
from tests.unit.run_control.test_boundary_commands import TARGET, started
from tests.unit.run_control.test_mission_control_facade import actor
from tests.unit.run_control.test_run_control import service

NOW = datetime(2026, 10, 8, 9, 0, tzinfo=UTC)


def _fence(generation: int = 1, command_id: str = "cancel-1") -> StopFence:
    return StopFence(
        request_scope="tenant-1",
        run_id="run-1",
        generation=generation,
        command_id=command_id,
        reason="wrong repo",
        requested_at=NOW,
    )


def _admission(effect_ref: str = "tool_use:1", generation: int = 1) -> EffectAdmission:
    return EffectAdmission(
        request_scope="tenant-1",
        run_id="run-1",
        generation=generation,
        effect_ref=effect_ref,
        effect_kind="shell",
    )


def test_fence_denies_its_generation_and_earlier_ones_only() -> None:
    assert fence_verdict(None, _admission()).allowed
    denied = fence_verdict(_fence(generation=2), _admission(generation=2))
    assert denied.decision == "deny" and denied.reason_code == STOP_FENCED
    assert denied.fence_command_id == "cancel-1"
    assert not fence_verdict(_fence(generation=2), _admission(generation=1)).allowed
    assert fence_verdict(_fence(generation=1), _admission(generation=2)).allowed
    with pytest.raises(ValueError, match="different runs"):
        fence_verdict(_fence(), _admission().model_copy(update={"run_id": "run-2"}))
    with pytest.raises(ValidationError):
        FenceVerdict(decision="deny", effect_ref="x")
    with pytest.raises(ValidationError):
        FenceVerdict(decision="allow", reason_code=STOP_FENCED, effect_ref="x")


def test_immediate_cancel_needs_admin_while_work_may_be_active() -> None:
    for phase in ("active", "waiting", "cancelling"):
        assert "workflow_run.admin" in immediate_cancel_permissions(phase)
    for phase in ("pending", "paused"):
        assert immediate_cancel_permissions(phase) == frozenset({"workflow_run.cancel"})


def test_report_keeps_four_timestamps_in_order_and_says_stop_now() -> None:
    report = ImmediateCancelReport(
        command_id="c",
        run_id="r",
        generation=1,
        requested_at=NOW,
        fence_persisted_at=NOW + timedelta(milliseconds=3),
    )
    assert report.state == "fence_persisted" and report.note == STOP_NOW_NOTE
    acknowledged = report.model_copy(
        update={"provider_acknowledged_at": NOW + timedelta(seconds=1)}
    )
    assert acknowledged.state == "provider_acknowledged"
    with pytest.raises(ValidationError, match="fence"):
        ImmediateCancelReport(
            command_id="c",
            run_id="r",
            generation=1,
            requested_at=NOW,
            fence_persisted_at=NOW - timedelta(seconds=1),
        )


def test_normal_cancel_keeps_its_recorded_digest() -> None:
    # Commands recorded before FT-F3 were fingerprinted over `{"kind": "cancel"}`.
    assert CancelAction().model_dump() == {"kind": "cancel"}
    assert contract_fingerprint(CancelAction()) == sha256_digest({"kind": "cancel"})
    immediate = CancelAction(urgency="immediate")
    assert immediate.model_dump() == {"kind": "cancel", "urgency": "immediate"}
    assert contract_fingerprint(immediate) != contract_fingerprint(CancelAction())


async def test_kernel_hook_gate_denies_after_the_fence_and_records_a_fenced_frame() -> None:
    fences = InMemoryStopFenceRepository()
    gate = KernelHookFenceGate(fences)
    before = await gate.before_effect(_admission("tool_use:before"))
    assert before.allowed and before.frame is None
    stored = await fences.persist(_fence())
    assert stored.fenced_at is not None
    assert await fences.persist(_fence(command_id="cancel-2")) == stored  # first fence wins
    after = await gate.before_effect(_admission("tool_use:after"))
    assert not after.allowed
    assert after.frame is not None and after.frame["reason"] == "fenced"
    assert after.frame["reason_code"] == STOP_FENCED and after.frame["kind"] == "hook"
    # The same effect id keeps its first decision (the pre-fence admission stays admitted).
    assert (await gate.before_effect(_admission("tool_use:before"))).allowed
    assert not (await gate.before_effect(_admission("tool_use:after"))).allowed


async def test_in_memory_fence_and_admissions_serialize() -> None:
    fences = InMemoryStopFenceRepository()

    async def admit(index: int) -> FenceVerdict:
        await asyncio.sleep(0)
        return await fences.admit_effect(_admission(f"tool_use:{index}"))

    results = await asyncio.gather(
        *(admit(index) for index in range(20)), fences.persist(_fence()), return_exceptions=False
    )
    verdicts = [item for item in results if isinstance(item, FenceVerdict)]
    allowed = [item for item in verdicts if item.allowed]
    denied = [item for item in verdicts if not item.allowed]
    assert len(allowed) + len(denied) == 20
    # Every admission after the fence is denied; nothing is admitted after a denial.
    for index in range(20):
        verdict = await fences.admit_effect(_admission(f"tool_use:{index}"))
        assert verdict == verdicts[index]


def _cancel(run_id: str, version: int, urgency: str) -> MissionCommandRequest:
    return MissionCommandRequest(
        request_id=uuid4(),
        expected_version=version,
        expected_generation=1,
        target=CommandTarget(id=run_id),
        kind="cancel",
        payload=CancelPayload(urgency=urgency),  # type: ignore[arg-type]
        reason="wrong repo",
    )


class FenceObservingInterventions(BoundaryInterventionService):
    """Records whether the fence was already persisted when the cancel was admitted."""

    def __init__(self, authority: Any, fences: InMemoryStopFenceRepository) -> None:
        super().__init__(authority)
        self.fences = fences
        self.fenced_at_admission: list[bool] = []

    async def execute(self, command: Any) -> Any:
        fence = await self.fences.get(command.request_scope, command.run_id)
        self.fenced_at_admission.append(fence is not None)
        return await super().execute(command)


def admin() -> ActorContext:
    source = actor()
    return source.model_copy(update={"permissions": source.permissions | {"workflow_run.admin"}})


async def test_immediate_cancel_persists_the_fence_before_admission() -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-immediate", TARGET)
    fences = InMemoryStopFenceRepository()
    interventions = FenceObservingInterventions(authority, fences)
    facade = MissionControlService(
        authority, interventions, request_scope="tenant-1", stop_fences=fences
    )
    version = (await facade.inspect(run_id, admin())).version
    receipt = await facade.command(run_id, _cancel(run_id, version, "immediate"), admin())
    assert receipt.admission.status == "accepted"
    assert interventions.fenced_at_admission == [True]
    fence = await fences.get("tenant-1", run_id)
    assert fence is not None and fence.command_id == str(receipt.request_id)
    assert (await facade.inspect(run_id, admin())).projection.phase == RunPhase.CANCELLING
    report = await facade.stop_fence_report(run_id, admin())
    assert report is not None and report.state == "fence_persisted"
    assert report.requested_at <= report.fence_persisted_at
    gate = KernelHookFenceGate(fences)
    denied = await gate.before_effect(
        EffectAdmission(
            request_scope="tenant-1", run_id=run_id, generation=1, effect_ref="tool_use:late"
        )
    )
    assert not denied.allowed


async def test_immediate_cancel_of_active_work_requires_admin_and_writes_nothing() -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-immediate-denied", TARGET)
    fences = InMemoryStopFenceRepository()
    facade = MissionControlService(
        authority,
        BoundaryInterventionService(authority),
        request_scope="tenant-1",
        stop_fences=fences,
    )
    version = (await facade.inspect(run_id, actor())).version
    with pytest.raises(MissionControlRejected) as rejected:
        await facade.command(run_id, _cancel(run_id, version, "immediate"), actor())
    assert rejected.value.code == "unauthorized"
    assert await fences.get("tenant-1", run_id) is None
    assert await facade.commands(run_id, actor()) == ()


async def test_normal_cancel_writes_no_fence() -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-normal-cancel", TARGET)
    fences = InMemoryStopFenceRepository()
    facade = MissionControlService(
        authority,
        BoundaryInterventionService(authority),
        request_scope="tenant-1",
        stop_fences=fences,
    )
    version = (await facade.inspect(run_id, actor())).version
    receipt = await facade.command(run_id, _cancel(run_id, version, "normal"), actor())
    assert receipt.admission.status == "accepted"
    assert await fences.get("tenant-1", run_id) is None
    assert await facade.stop_fence_report(run_id, actor()) is None


async def test_milestones_complete_the_report() -> None:
    fences = InMemoryStopFenceRepository()
    await fences.record_milestone("tenant-1", "run-1", None, "settled")  # no fence: ignored
    await fences.persist(_fence())
    await fences.record_milestone(
        "tenant-1",
        "run-1",
        None,
        "provider_acknowledged",
        unit_key="u1",
        recorded_at=NOW + timedelta(seconds=2),
    )
    await fences.record_milestone(
        "tenant-1", "run-1", 1, "settled", unit_key="u1", recorded_at=NOW + timedelta(seconds=3)
    )
    report = await fences.report("tenant-1", "run-1")
    assert report is not None and report.state == "settled"
    assert report.provider_acknowledged_at == NOW + timedelta(seconds=2)


async def test_http_immediate_cancel_returns_202_and_the_report_route_shows_it() -> None:
    import httpx
    from fastapi import FastAPI

    from mission_control.interfaces.http import mission_control as mission_http
    from mission_control.interfaces.http.stop_fence import router as stop_fence_router

    authority, _ = service()
    run_id = await started(authority, "mission-immediate-http", TARGET)
    fences = InMemoryStopFenceRepository()
    facade = MissionControlService(
        authority,
        BoundaryInterventionService(authority),
        request_scope="tenant-1",
        stop_fences=fences,
    )
    principal = mission_http.MissionPrincipal(
        installation_id=uuid4(),
        application_id="biotech",
        tenant_id=uuid4(),
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=admin(),
    )
    app = FastAPI()
    app.include_router(mission_http.router)
    app.include_router(stop_fence_router)
    app.dependency_overrides[mission_http.get_mission_principal] = lambda: principal
    app.dependency_overrides[mission_http.get_mission_service] = lambda: facade
    version = (await facade.inspect(run_id, admin())).version
    body = _cancel(run_id, version, "immediate").model_dump(mode="json")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://local-test"
    ) as client:
        base = f"/v1/applications/biotech/runs/{run_id}"
        assert (await client.get(f"{base}/stop-fence")).status_code == 404
        sent = await client.post(f"{base}/commands", json=body)
        assert sent.status_code == 202, sent.text
        report = await client.get(f"{base}/stop-fence")
        assert report.status_code == 200
        assert report.json()["state"] == "fence_persisted"
        assert report.json()["command_id"] == body["request_id"]
