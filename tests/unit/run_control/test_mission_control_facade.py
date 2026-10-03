"""Public adaptation exercises the real reducer and receipt ledger, without a provider."""

from uuid import uuid4

import pytest
from pydantic import ValidationError

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import (
    CancelPayload,
    CommandTarget,
    InstructionPayload,
    MissionCommandRequest,
    MissionControlRejected,
    PausePayload,
    ResumePayload,
    WaitPayload,
)
from mission_control.domain.policies.contracts import ActorContext, RunPhase, SetWaitAction
from mission_control.domain.policies.errors import IdempotencyConflict, RunControlNotFound
from tests.unit.run_control.test_boundary_commands import (
    TARGET,
    declared_wait,
    pause,
    resume,
    started,
)
from tests.unit.run_control.test_run_control import actor as control_actor
from tests.unit.run_control.test_run_control import command, service


def actor() -> ActorContext:
    source = control_actor()
    return source.model_copy(update={"permissions": source.permissions | {"workflow_run.read"}})


def pause_request(run_id: str, version: int = 2) -> MissionCommandRequest:
    action = pause()
    return MissionCommandRequest(
        request_id=uuid4(),
        expected_version=version,
        expected_generation=1,
        target=CommandTarget(id=run_id),
        kind="pause",
        payload=PausePayload(
            decision=action.decision, runnable_work_remains=action.runnable_work_remains
        ),
        reason="operator hold",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["StageGraph", "GoalDirected"])
async def test_family_pause_is_admission_not_application_and_replays(family: str) -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-pause", TARGET.model_copy(update={"family": family}))
    facade = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope="tenant-1"
    )
    request = pause_request(run_id)
    receipt = await facade.command(run_id, request, actor())
    assert receipt.admission.status == "accepted"
    assert receipt.delivery is not None
    assert [r.state.value for r in receipt.delivery.receipts] == ["accepted"]
    assert (await facade.inspect(run_id, actor())).projection.phase == RunPhase.ACTIVE
    assert (await facade.command(run_id, request, actor())).admission == receipt.admission
    assert len(await facade.commands(run_id, actor())) == 1


@pytest.mark.asyncio
async def test_wait_release_and_cancel_use_distinct_existing_sequence_spaces() -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-wait-cancel", TARGET)
    wait = declared_wait()
    await authority.execute(
        command(run_id, 2, "declare", SetWaitAction(condition=wait, runnable_work_remains=True))
    )
    facade = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope="tenant-1"
    )
    version = (await facade.inspect(run_id, actor())).version
    released = await facade.command(
        run_id,
        MissionCommandRequest(
            request_id=uuid4(),
            expected_version=version,
            expected_generation=1,
            target=CommandTarget(id=run_id),
            kind="satisfy_wait",
            payload=WaitPayload(
                condition_id=wait.condition_id, verification_evidence_ref="evidence:1"
            ),
            reason="approved declared wait",
        ),
        actor(),
    )
    cancelled = await facade.command(
        run_id,
        MissionCommandRequest(
            request_id=uuid4(),
            expected_version=version,
            expected_generation=1,
            target=CommandTarget(id=run_id),
            kind="cancel",
            payload=CancelPayload(),
            reason="operator cancelled",
        ),
        actor(),
    )
    assert released.admission.status == cancelled.admission.status == "accepted"
    assert released.delivery is not None and cancelled.delivery is not None
    assert released.delivery.command.target.sequence_space == "execution"
    assert cancelled.delivery.command.target.sequence_space == "cancel"
    assert cancelled.delivery.command.target.kind == "root"
    assert (await facade.inspect(run_id, actor())).projection.phase == RunPhase.CANCELLING


@pytest.mark.asyncio
async def test_pause_resume_without_execution_uses_same_durable_reducer() -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-resume", None)
    facade = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope="tenant-1"
    )
    await facade.command(run_id, pause_request(run_id), actor())
    inspected = await facade.inspect(run_id, actor())
    assert inspected.lifecycle == "paused"
    resume_action = resume()
    await facade.command(
        run_id,
        MissionCommandRequest(
            request_id=uuid4(),
            expected_version=inspected.version,
            expected_generation=1,
            target=CommandTarget(id=run_id),
            kind="resume",
            payload=ResumePayload(decision=resume_action.decision),
            reason="continue",
        ),
        actor(),
    )
    assert (await facade.inspect(run_id, actor())).lifecycle == "running"


@pytest.mark.asyncio
async def test_generation_and_changed_replay_rejected_without_new_command() -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-fence", TARGET)
    facade = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope="tenant-1"
    )
    request = pause_request(run_id)
    with pytest.raises(MissionControlRejected, match="generation"):
        await facade.command(run_id, request.model_copy(update={"expected_generation": 2}), actor())
    assert await facade.commands(run_id, actor()) == ()
    await facade.command(run_id, request, actor())
    with pytest.raises(IdempotencyConflict):
        await facade.command(run_id, request.model_copy(update={"expected_generation": 2}), actor())
    assert len(await facade.commands(run_id, actor())) == 1


@pytest.mark.asyncio
async def test_scope_and_read_authority_are_not_inferred_from_run_id() -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-scope", TARGET)
    facade = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope="tenant-1"
    )
    with pytest.raises(MissionControlRejected) as denied:
        await facade.inspect(run_id, ActorContext(actor_id="unprivileged"))
    assert denied.value.code == "unauthorized"
    with pytest.raises(MissionControlRejected) as denied_command:
        await facade.command(run_id, pause_request(run_id), ActorContext(actor_id="unprivileged"))
    assert denied_command.value.code == "unauthorized"
    other = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope="other-tenant"
    )
    with pytest.raises(RunControlNotFound):
        await other.inspect(run_id, actor())


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["queue_instruction", "interrupt_and_inject", "cancel"])
async def test_unqualified_controls_never_become_durable_admissions(kind: str) -> None:
    authority, _ = service()
    run_id = await started(authority, "mission-unqualified", TARGET)
    facade = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope="tenant-1"
    )
    raw = pause_request(run_id).model_dump(mode="json")
    raw.update(
        kind=kind,
        payload=(
            CancelPayload(urgency="immediate").model_dump()
            if kind == "cancel"
            else InstructionPayload(
                content_ref="artifact:instruction",
                content_digest="sha256:" + "a" * 64,
                boundary="next_turn",
            ).model_dump()
        ),
    )
    with pytest.raises(MissionControlRejected) as unsupported:
        await facade.command(run_id, MissionCommandRequest.model_validate(raw), actor())
    assert unsupported.value.code == "unsupported_control"
    assert await facade.commands(run_id, actor()) == ()


def test_payload_kind_mismatch_and_unknown_authority_fields_rejected() -> None:
    raw = pause_request("run").model_dump(mode="json")
    raw["kind"] = "cancel"
    with pytest.raises(ValidationError):
        MissionCommandRequest.model_validate(raw)
    raw["kind"] = "pause"
    raw["grant_extra_tools"] = True
    with pytest.raises(ValidationError):
        MissionCommandRequest.model_validate(raw)
