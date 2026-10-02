"""RRM-007: governed delivery of accepted boundary commands (REQ-CP-EXEC-006/007).

The delivery service delivers accepted commands to their exact target in target-sequence
order through a transport whose acknowledgement is evidence of `delivered` only. A
transport failure stops the pass so no later command overtakes an earlier one; redelivery
is safe because receipts never transition twice. Stale targets are terminal rejections.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from app.application.run_control.boundary_interventions import (
    BoundaryCommandApplicationService,
    BoundaryCommandDeliveryService,
    BoundaryDeliveryResult,
    BoundaryInterventionService,
)
from app.domain.orchestration.contracts import BoundaryCommandAck, WorkflowMessageReceipt
from app.domain.run_control.contracts import (
    ActorContext,
    BoundaryCommandStatus,
    CancelAction,
    CommandStatus,
    RecordUsageAction,
    RunOutcome,
    RunPhase,
    SatisfyWaitAction,
    SetWaitAction,
    TerminalizationProposal,
    TerminalizeAction,
)
from app.integrations.temporal_boundary_commands import (
    BoundaryDeliveryGap,
    TemporalBoundaryCommandTransport,
)
from tests.unit.run_control.test_boundary_commands import (
    BOUNDARY_PERMISSIONS,
    FAMILY_WORKFLOW_ID,
    ROOT_WORKFLOW_ID,
    TARGET,
    declared_wait,
    pause,
    started,
    states,
)
from tests.unit.run_control.test_run_control import (
    EMPTY_EVIDENCE_DIGEST,
    INITIAL_EVIDENCE_FRONTIER,
    NOW,
    WORKFLOW_DIGEST,
    command,
    service,
)


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
        "command_issuer": "operator",
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
        # F8: the reservations actually held, recorded by budget authority at application.
        "held_reservation_ids": ["baseline"],
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
    assert await application.boundary_receipt_state(
        "tenant-1", run_id, "operator", "pause"
    ) == ("applied", 1)


async def _status(run_service, run_id: str, command_id: str) -> BoundaryCommandStatus:  # type: ignore[no-untyped-def]
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", command_id)
    assert status is not None
    return status


class RacingRunControl:
    """Run control whose version moves between the boundary's read and its execute (F2)."""

    def __init__(self, inner: Any, run_id: str) -> None:
        self._inner = inner
        self._run_id = run_id
        self.raced = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def execute(self, lifecycle: Any) -> Any:
        if not self.raced and lifecycle.action.kind == "apply_boundary_command":
            self.raced = True
            run = await self._inner.get_run("tenant-1", self._run_id)
            bumped = await self._inner.execute(
                command(
                    self._run_id,
                    run.version,
                    "concurrent-fact",
                    SetWaitAction(condition=declared_wait("w2"), runnable_work_remains=True),
                )
            )
            assert bumped.status == CommandStatus.ACCEPTED
        return await self._inner.execute(lifecycle)


@pytest.mark.asyncio
async def test_version_race_never_strands_a_command() -> None:
    """F2 regression: a fact that races with another version bump is retried at the new
    version and the command is applied exactly once."""

    run_service, _ = service()
    run_id = await started(run_service, "application-race", TARGET)
    await run_service.execute(command(run_id, 2, "pause", pause()))
    racing = RacingRunControl(run_service, run_id)
    application = BoundaryCommandApplicationService(racing, boundary_actor())  # type: ignore[arg-type]
    applied = await application.execute(
        request_scope="tenant-1",
        run_id=run_id,
        command_id="boundary-apply:pause",
        idempotency_issuer="goal-directed-worker",
        correlation_id="goal:run",
        action={
            "kind": "apply_boundary_command",
            "command_id": "pause",
            "command_issuer": "operator",
            "action": pause().model_dump(mode="json"),
            "boundary_ref": FAMILY_WORKFLOW_ID,
            "runnable_work_remains": False,
        },
        reason="applied after a race",
    )
    assert racing.raced
    assert (applied.status, applied.phase) == (CommandStatus.ACCEPTED, RunPhase.PAUSED)
    assert states(await _status(run_service, run_id, "pause")) == [
        "accepted",
        "delivered",
        "applied",
    ]
    assert (await run_service.get_run("tenant-1", run_id)).version == 4


@pytest.mark.asyncio
async def test_cancel_is_not_delivered_by_the_family_transport_and_terminal_runs_reject() -> None:
    """F3: `cancel` delivery is RRM-008's; it never enters the family delivery pass. F1: a
    terminal run's pending commands are closed `terminal_run` by the delivery service."""

    run_service, _ = service()
    run_id = await started(run_service, "delivery-cancel", TARGET)
    transport = ScriptedTransport({"pause": RuntimeError("Temporal unavailable")})
    facade = BoundaryInterventionService(
        run_service, BoundaryCommandDeliveryService(run_service, transport)
    )
    await facade.execute(command(run_id, 2, "pause", pause()))
    cancelled = await facade.execute(command(run_id, 2, "cancel", CancelAction()))
    assert cancelled.phase == RunPhase.CANCELLING
    assert transport.deliveries == []
    assert states(await _status(run_service, run_id, "cancel")) == ["accepted"]
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
    transport.script.clear()
    assert await facade.redeliver("tenant-1", run_id) == ()
    assert transport.deliveries == [], "a terminal run receives no delivery"
    assert states(await _status(run_service, run_id, "pause")) == ["accepted", "rejected"]
    assert states(await _status(run_service, run_id, "cancel")) == [
        "accepted",
        "delivered",
        "applied",
    ]


class ScriptedHandle:
    def __init__(self, status: str, updates: list[Any]) -> None:
        self._status = status
        self._updates = list(updates)
        self.calls: list[tuple[str, Any]] = []

    async def describe(self) -> Any:
        return SimpleNamespace(status=SimpleNamespace(name=self._status))

    async def execute_update(self, name: str, argument: Any, *, result_type: Any = None) -> Any:
        self.calls.append((name, argument))
        return self._updates.pop(0)


class ScriptedClient:
    def __init__(self, handles: dict[str, ScriptedHandle]) -> None:
        self.handles = handles

    def get_workflow_handle(self, workflow_id: str, **_: Any) -> ScriptedHandle:
        return self.handles[workflow_id]


@pytest.mark.asyncio
async def test_transport_never_proceeds_past_a_cached_root_gap_or_stale_result() -> None:
    """F7: a root `duplicate` answer carries the cached status; only a cached `accepted` is a
    delivery. A closed family is `stale_target` before any Update."""

    run_service, _ = service()
    run_id = await started(run_service, "transport-root", TARGET)
    await run_service.execute(command(run_id, 2, "pause", pause()))
    status = await _status(run_service, run_id, "pause")

    cached_gap = WorkflowMessageReceipt("pause", 1, "duplicate", 1, cached_status="gap")
    root = ScriptedHandle("RUNNING", [cached_gap])
    family = ScriptedHandle("RUNNING", [])
    transport = TemporalBoundaryCommandTransport(
        ScriptedClient({ROOT_WORKFLOW_ID: root, FAMILY_WORKFLOW_ID: family})  # type: ignore[arg-type]
    )
    with pytest.raises(BoundaryDeliveryGap):
        await transport.deliver(status)
    assert family.calls == [], "no family Update after a cached root gap"

    cached_stale = WorkflowMessageReceipt(
        "pause", 1, "duplicate", 1, cached_status="stale_generation"
    )
    root = ScriptedHandle("RUNNING", [cached_stale])
    family = ScriptedHandle("RUNNING", [])
    transport = TemporalBoundaryCommandTransport(
        ScriptedClient({ROOT_WORKFLOW_ID: root, FAMILY_WORKFLOW_ID: family})  # type: ignore[arg-type]
    )
    result = await transport.deliver(status)
    assert result.status == "stale_generation" and family.calls == []

    accepted = WorkflowMessageReceipt("pause", 1, "duplicate", 1, cached_status="accepted")
    root = ScriptedHandle("RUNNING", [accepted])
    family = ScriptedHandle("RUNNING", [BoundaryCommandAck("pause", "delivered", 1)])
    transport = TemporalBoundaryCommandTransport(
        ScriptedClient({ROOT_WORKFLOW_ID: root, FAMILY_WORKFLOW_ID: family})  # type: ignore[arg-type]
    )
    result = await transport.deliver(status)
    assert result.status == "delivered"
    [(name, delivery)] = family.calls
    assert name == "deliver_boundary_command"
    assert (delivery.command_id, delivery.idempotency_issuer, delivery.target_sequence) == (
        "pause",
        "operator",
        1,
    )

    closed_family = ScriptedHandle("COMPLETED", [])
    root = ScriptedHandle("RUNNING", [])
    transport = TemporalBoundaryCommandTransport(
        ScriptedClient({ROOT_WORKFLOW_ID: root, FAMILY_WORKFLOW_ID: closed_family})  # type: ignore[arg-type]
    )
    result = await transport.deliver(status)
    assert result.status == "stale_target" and root.calls == [] and closed_family.calls == []


def test_root_receipt_cache_keeps_the_status_and_never_caches_a_gap() -> None:
    """F7 at the root: a gap is decided again once the missing message arrives; a duplicate
    of a stale message reports the cached stale status."""

    from app.domain.orchestration.contracts import WorkflowMessage
    from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow

    root = BellLabsRunWorkflow()
    gap = root._accept_message(WorkflowMessage("m2", 2, "control", "ref:2"))  # noqa: SLF001
    assert gap.status == "gap"
    first = root._accept_message(WorkflowMessage("m1", 1, "control", "ref:1"))  # noqa: SLF001
    assert first.status == "accepted"
    retried = root._accept_message(WorkflowMessage("m2", 2, "control", "ref:2"))  # noqa: SLF001
    assert retried.status == "accepted", "the gap was not cached"
    duplicate = root._accept_message(WorkflowMessage("m2", 2, "control", "ref:2"))  # noqa: SLF001
    assert (duplicate.status, duplicate.cached_status) == ("duplicate", "accepted")
    stale = root._accept_message(  # noqa: SLF001
        WorkflowMessage("m3", 3, "control", "ref:3", execution_generation=2)
    )
    assert stale.status == "stale_generation"
    again = root._accept_message(  # noqa: SLF001
        WorkflowMessage("m3", 3, "control", "ref:3", execution_generation=2)
    )
    assert (again.status, again.cached_status) == ("duplicate", "stale_generation")
