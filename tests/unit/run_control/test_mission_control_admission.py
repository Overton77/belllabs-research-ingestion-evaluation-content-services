from uuid import uuid4

import pytest
from pydantic import ValidationError

from mission_control.application.execution.run_launch import RunLaunchRejected, RunLaunchService
from mission_control.application.missions.admission import MissionAdmissionService
from mission_control.contracts.admission_contracts import (
    MissionAdmissionRequest,
    MissionLaunchRequest,
)
from mission_control.contracts.contracts import MissionControlRejected
from tests.unit.run_control.test_run_control import actor, request, service
from tests.unit.run_control.test_run_launch import RecordingSubmitter, _input


def admission_request() -> MissionAdmissionRequest:
    fields = request().model_dump(
        mode="python",
        exclude={
            "schema_version",
            "request_scope",
            "idempotency_issuer",
            "request_id",
            "actor",
            "requested_at",
            "correlation_id",
            "causation_id",
        },
    )
    return MissionAdmissionRequest(request_id=uuid4(), **fields)


@pytest.mark.asyncio
async def test_admission_pins_trusted_scope_actor_and_checks_current_grants_on_replay() -> None:
    authority, _ = service()
    facade = MissionAdmissionService(authority, request_scope="tenant-1")
    body = admission_request()
    grants = {
        "sponsorship_refs": frozenset({body.sponsorship_ref}),
        "approval_refs": frozenset(body.approval_refs),
    }
    receipt = await facade.admit(body, actor(), **grants)
    assert receipt.status == "accepted"
    assert await facade.admit(body, actor(), **grants) == receipt
    assert receipt.run_id is not None
    projection = await authority.get_run("tenant-1", receipt.run_id)
    assert projection.request_scope == "tenant-1"
    with pytest.raises(MissionControlRejected, match="sponsorship"):
        await facade.admit(body, actor(), sponsorship_refs=frozenset(), approval_refs=frozenset())


@pytest.mark.asyncio
async def test_launch_uses_governed_binding_validation_and_explicit_runtime_availability() -> None:
    authority, _ = service()
    submitter = RecordingSubmitter()
    facade = MissionAdmissionService(
        authority,
        request_scope="tenant-1",
        launch_service=RunLaunchService(run_control=authority, submitter=submitter),
    )
    body = admission_request()
    admission = await facade.admit(
        body,
        actor(),
        sponsorship_refs=frozenset({body.sponsorship_ref}),
        approval_refs=frozenset(body.approval_refs),
    )
    assert admission.run_id is not None
    launch = MissionLaunchRequest.model_validate(
        {"family": "StageGraph", "stagegraph": _input(admission.run_id)}
    )
    receipt = await facade.launch(admission.run_id, launch, actor())
    assert receipt.run_id == admission.run_id
    assert len(submitter.submissions) == 1
    wrong = MissionLaunchRequest.model_validate(
        {"family": "StageGraph", "stagegraph": _input(admission.run_id, request_scope="other")}
    )
    with pytest.raises(RunLaunchRejected):
        await facade.launch(admission.run_id, wrong, actor())
    assert len(submitter.submissions) == 1
    disconnected = MissionAdmissionService(authority, request_scope="tenant-1")
    with pytest.raises(MissionControlRejected, match="not configured"):
        await disconnected.launch(admission.run_id, launch, actor())


def test_admission_body_cannot_supply_actor_or_tenant() -> None:
    body = admission_request().model_dump(mode="json")
    body["request_scope"] = "attacker-selected"
    with pytest.raises(ValidationError):
        MissionAdmissionRequest.model_validate(body)
