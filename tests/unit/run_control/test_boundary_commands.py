"""RRM-007: boundary commands and their durable receipts at the run-control seam.

`CON-CP-WORKFLOW-MESSAGE-V1` (AMD-RRM-001): acceptance is not delivery, delivery is not
application. With a family execution target bound, a pause, resume or wait release is
accepted as a pending command whose phase effect is recorded only by the boundary's
`applied` fact (REQ-CP-RUN-004); without one, run control is the boundary and records
`accepted`, `delivered` and `applied` together. Every receipt is a distinct durable record
and a command is never applied twice.
"""

from __future__ import annotations

import pytest

from app.domain.run_control.boundary_commands import (
    boundary_command_record,
    run_control_boundary_receipts,
)
from app.domain.run_control.contracts import (
    ApplyBoundaryCommandAction,
    BoundaryCommandReceipt,
    BoundaryCommandStatus,
    BoundaryTarget,
    CancelAction,
    CommandStatus,
    ExecutionTarget,
    LifecycleCommand,
    ObserveQuiescenceAction,
    PauseAction,
    PauseDecision,
    ReceiptState,
    RecordUsageAction,
    ReserveBudgetAction,
    ResumeAction,
    ResumeDecision,
    RunOutcome,
    RunPhase,
    SatisfyWaitAction,
    SetWaitAction,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
    WaitCondition,
)
from app.domain.run_control.errors import ReceiptTransitionRejected
from tests.unit.run_control.test_run_control import (
    ALL_PERMISSIONS,
    EMPTY_EVIDENCE_DIGEST,
    INITIAL_EVIDENCE_FRONTIER,
    NOW,
    WORKFLOW_DIGEST,
    actor,
    command,
    operator_wait,
    reconcile,
    reconciler_command,
    request,
    service,
)

FAMILY_WORKFLOW_ID = "family/run-boundary/1"
ROOT_WORKFLOW_ID = "belllabs-run/run-boundary"
TARGET = ExecutionTarget(
    family="GoalDirected",
    family_workflow_id=FAMILY_WORKFLOW_ID,
    root_workflow_id=ROOT_WORKFLOW_ID,
    execution_epoch=1,
)
BOUNDARY_PERMISSIONS = ALL_PERMISSIONS | {"workflow_run.apply_boundary_command"}


def pause(decision_id: str = "pause-1", *, runnable: bool = False) -> PauseAction:
    return PauseAction(
        decision=PauseDecision(
            decision_id=decision_id,
            scope=frozenset({"run"}),
            reason="operator hold",
            authority_ref="authority:lifecycle",
        ),
        runnable_work_remains=runnable,
    )


def resume(pause_decision_id: str = "pause-1", decision_id: str = "resume-1") -> ResumeAction:
    return ResumeAction(
        decision=ResumeDecision(
            decision_id=decision_id,
            pause_decision_id=pause_decision_id,
            reason="operator release",
            authority_ref="authority:lifecycle",
        )
    )


def declared_wait(condition_id: str = "stagegraph-wait:release") -> WaitCondition:
    return WaitCondition(
        condition_id=condition_id,
        kind="approval",
        scope=frozenset({"workflow:graph"}),
        verification_ref=f"stagegraph-wait:{condition_id}",
        timeout_policy_ref="timeout:none",
    )


def boundary_command(
    run_id: str, version: int, command_id: str, action: object, **extra: object
) -> LifecycleCommand:
    """The family boundary's command: the orchestration identity, not the operator's."""

    return command(run_id, version, command_id, action, **extra).model_copy(
        update={
            "actor": actor().model_copy(
                update={
                    "actor_id": "orchestration-authority",
                    "authority_refs": frozenset({"orchestration-authority"}),
                    "permissions": BOUNDARY_PERMISSIONS,
                }
            ),
            "idempotency_issuer": "goal-directed-worker",
        }
    )


def apply(
    command_id: str,
    action: PauseAction | ResumeAction | SatisfyWaitAction,
    *,
    runnable: bool = False,
    boundary_ref: str = FAMILY_WORKFLOW_ID,
    reservation: dict[str, int] | None = None,
    state: dict[str, object] | None = None,
) -> ApplyBoundaryCommandAction:
    return ApplyBoundaryCommandAction(
        command_id=command_id,
        command_issuer="operator",
        action=action,
        boundary_ref=boundary_ref,
        runnable_work_remains=runnable,
        resume_reservation=reservation or {},
        boundary_state=state or {},
    )


def states(status: BoundaryCommandStatus) -> list[str]:
    return [item.state.value for item in status.receipts]


def delivered(status: BoundaryCommandStatus) -> BoundaryCommandReceipt:
    return BoundaryCommandReceipt(
        command_id=status.command.command_id,
        idempotency_issuer=status.command.idempotency_issuer,
        run_id=status.command.run_id,
        request_scope=status.command.request_scope,
        ordinal=1,
        state=ReceiptState.DELIVERED,
        recorded_by="delivery-service",
        transport_ref=f"{FAMILY_WORKFLOW_ID}@segment:1",
        recorded_at=NOW,
    )


async def started(run_service, request_id: str, target: ExecutionTarget | None):  # type: ignore[no-untyped-def]
    admitted = await run_service.admit(request(request_id=request_id))
    assert admitted.run_id is not None
    started = await run_service.execute(
        command(admitted.run_id, 1, "start", StartAction(execution_target=target))
    )
    assert started.status == CommandStatus.ACCEPTED
    return admitted.run_id


@pytest.mark.asyncio
async def test_run_control_is_the_boundary_when_no_execution_target_is_declared() -> None:
    """With no root execution, acceptance, delivery and application are one commit."""

    run_service, _ = service()
    run_id = await started(run_service, "boundary-direct", None)
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.execution_target is None

    paused = await run_service.execute(command(run_id, 2, "pause", pause()))
    assert (paused.status, paused.phase, paused.resulting_run_version) == (
        CommandStatus.ACCEPTED,
        RunPhase.PAUSED,
        3,
    )
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    assert status is not None
    assert states(status) == ["accepted", "delivered", "applied"]
    assert status.command.target.kind == "run_control"
    assert status.command.target_sequence == 0
    assert status.receipts[-1].applied_run_version == 3
    assert {item.recorded_by for item in status.receipts} == {"run_control"}

    # A cancel is applied only when the reducer records the terminal outcome.
    cancelled = await run_service.execute(command(run_id, 3, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    cancel_status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "cancel")
    assert cancel_status is not None and states(cancel_status) == ["accepted", "delivered"]
    await run_service.execute(
        command(
            run_id,
            4,
            "release-baseline",
            RecordUsageAction(
                usage_id="usage:release",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    terminal = await run_service.execute(
        command(
            run_id,
            5,
            "terminalize",
            TerminalizeAction(
                proposal=TerminalizationProposal(
                    proposal_id="terminal",
                    expected_run_version=5,
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
        )
    )
    assert terminal.terminal_outcome == RunOutcome.CANCELLED
    cancel_status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "cancel")
    assert cancel_status is not None
    assert states(cancel_status) == ["accepted", "delivered", "applied"]
    assert cancel_status.receipts[-1].applied_run_version == 6
    listed = await run_service.list_boundary_commands("tenant-1", run_id)
    assert [item.command.command_id for item in listed] == ["pause", "cancel"]


@pytest.mark.asyncio
async def test_family_target_makes_interventions_pending_until_the_boundary_applies() -> None:
    """REQ-CP-RUN-004: accepted but undelivered leaves the phase; delivered shows pending;
    application moves the phase. The same command never applies twice."""

    run_service, _ = service()
    run_id = await started(run_service, "boundary-pending", TARGET)
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.execution_target == TARGET

    accepted = await run_service.execute(command(run_id, 2, "pause", pause()))
    assert accepted.status == CommandStatus.ACCEPTED
    assert accepted.reason_code == "accepted_pending_application"
    assert (accepted.phase, accepted.resulting_run_version) == (RunPhase.ACTIVE, 2)
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.version == 2 and projection.active_pauses == ()
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    assert status is not None
    assert states(status) == ["accepted"]
    assert (status.command.target.kind, status.command.target_sequence) == ("family", 1)
    assert status.command.target.root_workflow_id == ROOT_WORKFLOW_ID
    assert status.command.accepted_run_version == 2

    # An exact replay returns the stored result without a second receipt.
    replayed = await run_service.execute(command(run_id, 2, "pause", pause()))
    assert replayed == accepted
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    assert status is not None and states(status) == ["accepted"]

    # The boundary cannot apply what was not delivered... unless it saw the delivery
    # itself: then it records `delivered` and `applied` together.
    wait = await run_service.execute(
        command(
            run_id,
            2,
            "declare-wait",
            SetWaitAction(condition=declared_wait(), runnable_work_remains=True),
        )
    )
    assert wait.status == CommandStatus.ACCEPTED and wait.resulting_run_version == 3
    release = await run_service.execute(
        command(
            run_id,
            3,
            "release",
            SatisfyWaitAction(
                condition_id=declared_wait().condition_id,
                verification_evidence_ref="evidence:operator",
            ),
        )
    )
    assert release.status == CommandStatus.ACCEPTED and release.resulting_run_version == 3
    release_status = await run_service.get_boundary_command(
        "tenant-1", run_id, "operator", "release"
    )
    assert release_status is not None and release_status.command.target_sequence == 2

    delivered_status = await run_service.record_boundary_receipt("tenant-1", delivered(status))
    assert states(delivered_status) == ["accepted", "delivered"]
    again = await run_service.record_boundary_receipt("tenant-1", delivered(status))
    assert again == delivered_status, "a repeated delivery receipt is a no-op"
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.phase == RunPhase.ACTIVE, "delivered is not applied"

    applied = await run_service.execute(
        boundary_command(run_id, 3, "apply:pause", apply("pause", pause()))
    )
    assert (applied.status, applied.phase, applied.resulting_run_version) == (
        CommandStatus.ACCEPTED,
        RunPhase.PAUSED,
        4,
    )
    projection = await run_service.get_run("tenant-1", run_id)
    assert [item.decision_id for item in projection.active_pauses] == ["pause-1"]
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    assert status is not None
    assert states(status) == ["accepted", "delivered", "applied"]
    assert status.receipts[-1].applied_run_version == 4
    assert status.receipts[-1].recorded_by == FAMILY_WORKFLOW_ID

    duplicate = await run_service.execute(
        boundary_command(run_id, 4, "apply:pause:again", apply("pause", pause()))
    )
    assert duplicate.status == CommandStatus.REJECTED
    assert duplicate.reason_code == "boundary_command_already_applied"
    assert (await run_service.get_run("tenant-1", run_id)).version == 4
    mismatch = await run_service.execute(
        boundary_command(run_id, 4, "apply:release:wrong", apply("release", pause("other")))
    )
    assert mismatch.reason_code == "boundary_command_mismatch"

    # The wait release applied without a delivery receipt records both receipts.
    released = await run_service.execute(
        boundary_command(
            run_id,
            4,
            "apply:release",
            apply(
                "release",
                SatisfyWaitAction(
                    condition_id=declared_wait().condition_id,
                    verification_evidence_ref="evidence:operator",
                ),
            ),
        )
    )
    assert released.status == CommandStatus.ACCEPTED and released.phase == RunPhase.PAUSED
    release_status = await run_service.get_boundary_command(
        "tenant-1", run_id, "operator", "release"
    )
    assert release_status is not None
    assert states(release_status) == ["accepted", "delivered", "applied"]
    assert release_status.receipts[1].detail == "delivery acknowledged by the applying boundary"
    assert (await run_service.get_run("tenant-1", run_id)).active_waits == ()


@pytest.mark.asyncio
async def test_rejections_are_typed_terminal_receipts_and_never_consume_a_sequence() -> None:
    run_service, _ = service()
    run_id = await started(run_service, "boundary-rejections", TARGET)

    stale = await run_service.execute(command(run_id, 1, "stale-pause", pause()))
    assert stale.status == CommandStatus.STALE
    stale_status = await run_service.get_boundary_command(
        "tenant-1", run_id, "operator", "stale-pause"
    )
    assert stale_status is not None
    assert states(stale_status) == ["rejected"]
    assert stale_status.receipts[0].rejection_reason == "stale_version"
    assert stale_status.command.target_sequence == 0

    orphan = await run_service.execute(command(run_id, 2, "orphan-resume", resume("missing")))
    assert orphan.status == CommandStatus.REJECTED and orphan.reason_code == "pause_not_found"
    orphan_status = await run_service.get_boundary_command(
        "tenant-1", run_id, "operator", "orphan-resume"
    )
    assert orphan_status is not None
    assert orphan_status.receipts[0].rejection_reason == "not_applicable"

    unauthorized = command(run_id, 2, "unauthorized-pause", pause()).model_copy(
        update={"actor": actor().model_copy(update={"authority_refs": frozenset()})}
    )
    rejected = await run_service.execute(unauthorized)
    assert rejected.reason_code == "invalid_pause_authority"
    unauthorized_status = await run_service.get_boundary_command(
        "tenant-1", run_id, "operator", "unauthorized-pause"
    )
    assert unauthorized_status is not None
    assert unauthorized_status.receipts[0].rejection_reason == "unauthorized"

    accepted = await run_service.execute(command(run_id, 2, "pause", pause()))
    assert accepted.status == CommandStatus.ACCEPTED
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    assert status is not None and status.command.target_sequence == 1, "no sequence gap"

    # A boundary that is not the declared target cannot apply the command.
    foreign = await run_service.execute(
        boundary_command(
            run_id, 2, "apply:foreign", apply("pause", pause(), boundary_ref="family/other/1")
        )
    )
    assert foreign.status == CommandStatus.REJECTED and foreign.reason_code == "stale_target"
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    assert status is not None
    assert states(status) == ["accepted", "rejected"]
    assert status.receipts[-1].rejection_reason == "stale_target"
    with pytest.raises(ReceiptTransitionRejected):
        await run_service.record_boundary_receipt("tenant-1", delivered(status))
    assert (await run_service.get_run("tenant-1", run_id)).phase == RunPhase.ACTIVE


@pytest.mark.asyncio
async def test_resume_must_re_reserve_the_next_unit_of_work() -> None:
    """REQ-BP-GD-011: a resume that cannot re-reserve is rejected `insufficient_budget` and
    the run stays paused; a resume that fits continues."""

    run_service, _ = service()
    run_id = await started(run_service, "boundary-reserve", TARGET)
    await run_service.execute(command(run_id, 2, "pause", pause()))
    applied = await run_service.execute(
        boundary_command(run_id, 2, "apply:pause", apply("pause", pause()))
    )
    assert applied.phase == RunPhase.PAUSED and applied.resulting_run_version == 3
    # The baseline holds 20 of the 100 hard cap; reserve 70 more so only 10 remain.
    reserved = await run_service.execute(
        command(
            run_id,
            3,
            "reserve-most",
            ReserveBudgetAction(reservation_id="held", amounts={"tokens.total": 70}),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED

    accepted = await run_service.execute(command(run_id, 4, "resume", resume()))
    assert accepted.status == CommandStatus.ACCEPTED and accepted.phase == RunPhase.PAUSED
    rejected = await run_service.execute(
        boundary_command(
            run_id, 4, "apply:resume", apply("resume", resume(), reservation={"tokens.total": 11})
        )
    )
    assert rejected.status == CommandStatus.REJECTED
    assert rejected.reason_code == "insufficient_budget"
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "resume")
    assert status is not None
    assert states(status) == ["accepted", "delivered", "rejected"]
    assert status.receipts[-1].rejection_reason == "insufficient_budget"
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.phase == RunPhase.PAUSED and projection.version == 4
    assert [item.decision_id for item in projection.active_pauses] == ["pause-1"]

    fitting = await run_service.execute(command(run_id, 4, "resume-2", resume(decision_id="r2")))
    assert fitting.status == CommandStatus.ACCEPTED
    resumed = await run_service.execute(
        boundary_command(
            run_id,
            4,
            "apply:resume-2",
            apply(
                "resume-2",
                resume(decision_id="r2"),
                runnable=True,
                reservation={"tokens.total": 10},
            ),
        )
    )
    assert resumed.status == CommandStatus.ACCEPTED and resumed.phase == RunPhase.ACTIVE
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.active_pauses == ()
    assert [item.decision_id for item in projection.resume_decisions] == ["r2"]
    # The probe reserved nothing: the boundary reserves at its next admission.
    budget = await run_service.get_budget("tenant-1", run_id)
    assert budget.reserved == {"tokens.total": 90}


@pytest.mark.asyncio
async def test_quiescence_keeps_the_aggregate_phase_accurate_under_scoped_pauses() -> None:
    """A scoped pause with unrelated admissible work keeps the run active; when the boundary
    runs out of admissible work the run is paused, and active again when work returns."""

    run_service, _ = service()
    run_id = await started(run_service, "boundary-quiescence", TARGET)
    scoped = PauseAction(
        decision=PauseDecision(
            decision_id="pause-slow",
            scope=frozenset({"stage:slow"}),
            reason="hold the slow stage",
            authority_ref="authority:lifecycle",
        ),
        runnable_work_remains=False,
    )
    await run_service.execute(command(run_id, 2, "pause-slow", scoped))
    applied = await run_service.execute(
        boundary_command(run_id, 2, "apply:pause-slow", apply("pause-slow", scoped, runnable=True))
    )
    assert applied.phase == RunPhase.ACTIVE, "unrelated admissible work keeps the run active"
    quiet = await run_service.execute(
        boundary_command(
            run_id,
            3,
            "quiescence:1",
            ObserveQuiescenceAction(boundary_ref=FAMILY_WORKFLOW_ID, runnable_work_remains=False),
        )
    )
    assert quiet.phase == RunPhase.PAUSED
    busy = await run_service.execute(
        boundary_command(
            run_id,
            4,
            "quiescence:2",
            ObserveQuiescenceAction(boundary_ref=FAMILY_WORKFLOW_ID, runnable_work_remains=True),
        )
    )
    assert busy.phase == RunPhase.ACTIVE
    foreign = await run_service.execute(
        boundary_command(
            run_id,
            5,
            "quiescence:foreign",
            ObserveQuiescenceAction(boundary_ref="family/other/1", runnable_work_remains=False),
        )
    )
    assert foreign.status == CommandStatus.REJECTED and foreign.reason_code == "stale_target"


@pytest.mark.asyncio
async def test_reconcile_unit_is_sequenced_per_unit_and_applied_by_the_operation_boundary() -> None:
    run_service, _ = service()
    run_id = await started(run_service, "boundary-reconcile", TARGET)
    parked = await run_service.execute(
        command(
            run_id, 2, "park", SetWaitAction(condition=operator_wait(), runnable_work_remains=False)
        )
    )
    assert parked.status == CommandStatus.ACCEPTED
    decided = await run_service.execute(
        reconciler_command(run_id, 3, "reconcile-abandon", reconcile())
    )
    assert decided.status == CommandStatus.ACCEPTED and decided.resulting_run_version == 4
    status = await run_service.get_boundary_command(
        "tenant-1", run_id, "operator", "reconcile-abandon"
    )
    assert status is not None
    assert states(status) == ["accepted"]
    assert status.command.kind == "reconcile_unit"
    assert status.command.target.kind == "unit"
    assert status.command.target.sequence_space.startswith("unit:bl-unit-v1:")
    assert status.command.target_sequence == 1
    hinted = await run_service.record_boundary_receipt(
        "tenant-1",
        delivered(status).model_copy(
            update={"recorded_by": "unit-reconciliation", "transport_ref": "operation/x"}
        ),
    )
    assert states(hinted) == ["accepted", "delivered"]
    applied = await run_service.record_boundary_receipt(
        "tenant-1",
        delivered(status).model_copy(
            update={
                "state": ReceiptState.APPLIED,
                "recorded_by": "operation-boundary",
                "applied_run_version": 4,
            }
        ),
    )
    assert states(applied) == ["accepted", "delivered", "applied"]
    with pytest.raises(ReceiptTransitionRejected):
        await run_service.record_boundary_receipt(
            "tenant-1",
            delivered(status).model_copy(
                update={"state": ReceiptState.REJECTED, "rejection_reason": "superseded"}
            ),
        )


def test_run_control_receipts_follow_the_special_cases() -> None:
    """Run control as the boundary records accepted, delivered and applied together, except
    for a cancel, which is applied by the terminal outcome."""

    pause_command = command("run-x", 2, "pause", pause())
    receipts = run_control_boundary_receipts(
        pause_command, target=BoundaryTarget(kind="run_control"), resulting_run_version=3
    )
    assert [item.state.value for item in receipts] == ["accepted", "delivered", "applied"]
    assert receipts[-1].applied_run_version == 3
    cancel_command = command("run-x", 2, "cancel", CancelAction())
    assert [
        item.state.value
        for item in run_control_boundary_receipts(
            cancel_command, target=BoundaryTarget(kind="run_control"), resulting_run_version=3
        )
    ] == ["accepted", "delivered"]
    family = BoundaryTarget(kind="family", target_ref=FAMILY_WORKFLOW_ID)
    assert [
        item.state.value
        for item in run_control_boundary_receipts(
            pause_command, target=family, resulting_run_version=3
        )
    ] == ["accepted"]
    with pytest.raises(ValueError, match="closed receipt state machine"):
        BoundaryCommandStatus(
            command=boundary_command_record(
                pause_command,
                target=BoundaryTarget(kind="run_control"),
                target_sequence=0,
                accepted_run_version=2,
            ),
            receipts=(receipts[0], receipts[2]),
        )


@pytest.mark.asyncio
async def test_terminal_outcome_closes_every_open_command_in_the_same_commit() -> None:
    """F1: a run that ends leaves no command at `accepted` or `delivered`; the terminalizing
    commit rejects them `terminal_run` (a cancel overtaken by another outcome is `superseded`)."""

    run_service, _ = service()
    run_id = await started(run_service, "boundary-terminal", TARGET)
    await run_service.execute(command(run_id, 2, "pause", pause()))
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    assert status is not None
    await run_service.record_boundary_receipt("tenant-1", delivered(status))
    await run_service.execute(command(run_id, 2, "pause-2", pause("p2")))
    cancelled = await run_service.execute(command(run_id, 2, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    await run_service.execute(
        command(
            run_id,
            3,
            "release-baseline",
            RecordUsageAction(
                usage_id="usage:release",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    terminal = await run_service.execute(
        command(
            run_id,
            4,
            "terminalize",
            TerminalizeAction(
                proposal=TerminalizationProposal(
                    proposal_id="terminal",
                    expected_run_version=4,
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
        )
    )
    assert terminal.terminal_outcome == RunOutcome.CANCELLED
    ledger = {
        item.command.command_id: item
        for item in await run_service.list_boundary_commands("tenant-1", run_id)
    }
    assert states(ledger["pause"]) == ["accepted", "delivered", "rejected"]
    assert states(ledger["pause-2"]) == ["accepted", "rejected"]
    assert {ledger[name].receipts[-1].rejection_reason for name in ("pause", "pause-2")} == {
        "terminal_run"
    }
    assert states(ledger["cancel"]) == ["accepted", "delivered", "applied"]
    # A command accepted against a terminal run is rejected at acceptance.
    late = await run_service.execute(command(run_id, 5, "late-pause", pause("late")))
    assert late.status == CommandStatus.REJECTED and late.reason_code == "run_is_terminal"
    late_status = await run_service.get_boundary_command(
        "tenant-1", run_id, "operator", "late-pause"
    )
    assert late_status is not None
    assert late_status.receipts[0].rejection_reason == "terminal_run"


@pytest.mark.asyncio
async def test_two_principals_may_reuse_a_command_id() -> None:
    """F5: the command identity is (issuer, command_id), as for every lifecycle command."""

    run_service, _ = service()
    run_id = await started(run_service, "boundary-issuers", TARGET)
    first = await run_service.execute(command(run_id, 2, "pause", pause("p-operator")))
    other = command(run_id, 2, "pause", pause("p-other")).model_copy(
        update={"idempotency_issuer": "other-operator"}
    )
    second = await run_service.execute(other)
    assert first.status == second.status == CommandStatus.ACCEPTED
    ledger = await run_service.list_boundary_commands("tenant-1", run_id)
    assert [
        (item.command.idempotency_issuer, item.command.command_id, item.command.target_sequence)
        for item in ledger
    ] == [("operator", "pause", 1), ("other-operator", "pause", 2)]
    applied = await run_service.execute(
        boundary_command(
            run_id,
            2,
            "apply:other",
            apply("pause", pause("p-other")).model_copy(
                update={"command_issuer": "other-operator"}
            ),
        )
    )
    assert applied.status == CommandStatus.ACCEPTED
    mine = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    theirs = await run_service.get_boundary_command("tenant-1", run_id, "other-operator", "pause")
    assert mine is not None and theirs is not None
    assert states(mine) == ["accepted"]
    assert states(theirs) == ["accepted", "delivered", "applied"]


@pytest.mark.asyncio
async def test_boundary_facts_never_persist_a_stale_result() -> None:
    """F2: a boundary fact that lost a version race is answered STALE without a stored result,
    so the retry at the new version is not an idempotency conflict."""

    run_service, _ = service()
    run_id = await started(run_service, "boundary-stale-fact", TARGET)
    await run_service.execute(command(run_id, 2, "pause", pause()))
    stale = await run_service.execute(
        boundary_command(run_id, 1, "apply:pause", apply("pause", pause()))
    )
    assert stale.status == CommandStatus.STALE
    assert (
        await run_service.get_command_result(
            "tenant-1", run_id, "goal-directed-worker", "apply:pause"
        )
        is None
    ), "no STALE result is stored for a boundary fact"
    retried = await run_service.execute(
        boundary_command(run_id, 2, "apply:pause", apply("pause", pause()))
    )
    assert retried.status == CommandStatus.ACCEPTED and retried.phase == RunPhase.PAUSED
