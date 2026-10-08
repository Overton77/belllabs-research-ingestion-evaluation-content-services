"""FT-F1: queue_instruction and add_context through the Run's command mailbox.

Deterministic: the real reducer, receipt ledger and in-memory run-control repository (the
same atomic boundaries as PostgreSQL), the mailbox delivery service, the HTTP router, the
CLI and the coordinator MCP tool. No provider is called.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from fastmcp import Client, Context

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.execution.service import RunControlService
from mission_control.application.installations.registry import (
    ApplicationBinding,
    ApplicationRegistry,
    InstallationObservation,
)
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import (
    CancelPayload,
    CommandTarget,
    MissionCommandRequest,
    MissionControlRejected,
)
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.policies.contracts import (
    RECEIPT_TRANSITIONS,
    ActorContext,
    CancelAction,
    CommandStatus,
    ExecutionTarget,
    LifecycleCommand,
    ReceiptState,
    RecordUsageAction,
    RunOutcome,
    RunProjection,
    StartAction,
    TerminalizationProposal,
    TerminalizeAction,
    next_receipt_state,
)
from mission_control.domain.policies.mailbox import (
    MAX_INLINE_BYTES,
    MailboxBoundaryPoint,
    MailboxEntry,
    MailboxState,
    claim_decision,
    content_digest,
    inline_content_ref,
)
from mission_control.interfaces.cli.main import main
from mission_control.interfaces.http.mission_control import (
    MissionPrincipal,
    get_mission_principal,
    router,
)
from mission_control.interfaces.mcp.coordinator_server import (
    CoordinatorPrincipal,
    create_coordinator_server,
)
from mission_control.interfaces.mcp.run_control_tools import COMMAND_SEND_TOOL, ScopedRunControl
from tests.unit.coordinator.test_coordinator_mcp_read_surface import FakeFacade
from tests.unit.run_control.test_run_control import (
    EMPTY_EVIDENCE_DIGEST,
    INITIAL_EVIDENCE_FRONTIER,
    WORKFLOW_DIGEST,
    service,
)
from tests.unit.run_control.test_run_control import actor as control_actor
from tests.unit.run_control.test_run_control import request as run_request

INSTALLATION = "00000000-0000-7000-8000-0000000000aa"
TENANT = "00000000-0000-7000-8000-0000000000bb"
SCOPE = f"mc/{INSTALLATION}/biotech/{TENANT}"
NOW = datetime(2026, 10, 8, 12, tzinfo=UTC)
TARGET = ExecutionTarget(
    family="GoalDirected",
    family_workflow_id="family/run/1",
    root_workflow_id="root/run",
    execution_epoch=1,
)


def operator() -> ActorContext:
    source = control_actor()
    return source.model_copy(
        update={"permissions": source.permissions | {"workflow_run.read", "workflow_run.control"}}
    )


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        self.now += timedelta(seconds=1)
        return self.now


class World:
    def __init__(self) -> None:
        self.authority, self.repository = service()
        self.clock = Clock()
        self.mailbox = MailboxDeliveryService(
            self.repository.mailbox, self.authority, clock=self.clock
        )
        self.facade = MissionControlService(
            self.authority,
            BoundaryInterventionService(self.authority),
            request_scope=SCOPE,
            mailbox=self.mailbox,
        )
        self.run_id = ""

    async def start(self, request_id: str = "f1-run", target: ExecutionTarget | None = TARGET):
        admitted = await self.authority.admit(
            run_request(request_scope=SCOPE, request_id=request_id)
        )
        assert admitted.run_id is not None
        self.run_id = admitted.run_id
        started = await self.authority.execute(
            self.lifecycle("start", 1, StartAction(execution_target=target))
        )
        assert started.status == CommandStatus.ACCEPTED
        return self

    def lifecycle(self, command_id: str, version: int, action: object) -> LifecycleCommand:
        return LifecycleCommand(
            command_id=command_id,
            idempotency_issuer="operator",
            request_scope=SCOPE,
            run_id=self.run_id,
            expected_run_version=version,
            actor=control_actor(),
            action=action,  # type: ignore[arg-type]
            reason=f"test {command_id}",
            occurred_at=NOW + timedelta(minutes=version),
            correlation_id="correlation-1",
        )

    async def projection(self) -> RunProjection:
        return await self.authority.get_run(SCOPE, self.run_id)

    async def entries(self) -> tuple[MailboxEntry, ...]:
        return await self.repository.mailbox.list_entries(SCOPE, self.run_id)

    async def queue(
        self,
        text: str = "Also update the README with the new flag",
        *,
        kind: str = "queue_instruction",
        boundary: str = "next_turn",
        version: int | None = None,
        **payload: Any,
    ) -> Any:
        projection = await self.projection()
        return await self.facade.command(
            self.run_id,
            self.request(
                kind,
                {"boundary": boundary, "content": {"text": text}, **payload},
                version=version or projection.version,
            ),
            operator(),
        )

    def request(
        self, kind: str, payload: dict[str, Any], *, version: int, generation: int = 1
    ) -> MissionCommandRequest:
        return MissionCommandRequest.model_validate(
            {
                "request_id": str(uuid4()),
                "expected_version": version,
                "expected_generation": generation,
                "target": {"kind": "run", "id": self.run_id},
                "kind": kind,
                "payload": payload,
                "reason": "operator steering",
            }
        )


def states(status: Any) -> list[str]:
    return [item.state.value for item in status.receipts]


def outbox_types(world: World) -> list[str]:
    return [record.envelope.event_type for record in world.repository._outbox.values()]


# --- Domain rules ----------------------------------------------------------------------------


def test_receipt_state_machine_adds_queued_observed_and_terminal_outcomes() -> None:
    assert next_receipt_state(ReceiptState.ACCEPTED, ReceiptState.QUEUED)
    assert next_receipt_state(ReceiptState.QUEUED, ReceiptState.DELIVERED)
    assert next_receipt_state(ReceiptState.QUEUED, ReceiptState.EXPIRED)
    assert next_receipt_state(ReceiptState.DELIVERED, ReceiptState.OBSERVED)
    assert next_receipt_state(ReceiptState.OBSERVED, ReceiptState.APPLIED)
    assert next_receipt_state(ReceiptState.OBSERVED, ReceiptState.FAILED)
    assert not next_receipt_state(ReceiptState.QUEUED, ReceiptState.APPLIED)
    assert not next_receipt_state(ReceiptState.ACCEPTED, ReceiptState.OBSERVED)
    for terminal in (
        ReceiptState.APPLIED,
        ReceiptState.REJECTED,
        ReceiptState.EXPIRED,
        ReceiptState.FAILED,
    ):
        assert RECEIPT_TRANSITIONS[terminal] == frozenset()
    # The RRM-007 chain is unchanged.
    assert next_receipt_state(ReceiptState.ACCEPTED, ReceiptState.DELIVERED)
    assert next_receipt_state(ReceiptState.DELIVERED, ReceiptState.APPLIED)


def _entry(**overrides: Any) -> MailboxEntry:
    text = "Prefer peer-reviewed sources"
    digest = content_digest(text)
    values: dict[str, Any] = {
        "entry_id": "entry-1",
        "request_scope": SCOPE,
        "run_id": "run-1",
        "command_id": "command-1",
        "command_issuer": "issuer",
        "kind": "queue_instruction",
        "generation": 1,
        "boundary": "next_turn",
        "content_ref": inline_content_ref(digest),
        "content_digest": digest,
        "media_type": "text/markdown",
        "content_bytes": len(text),
        "content_inline": text,
        "admission_sequence": 1,
        "accepted_at": NOW,
    }
    values.update(overrides)
    return MailboxEntry.model_validate(values)


@pytest.mark.parametrize(
    ("entry", "point", "decision"),
    [
        (
            {},
            {"family": "GoalDirected", "node_key": "goal/executor", "generation": 1},
            "claim",
        ),
        # The verifier stays independent of operator steering unless an entry names it.
        ({}, {"family": "GoalDirected", "node_key": "goal/verifier", "generation": 1}, "wait"),
        (
            {"node_key": "goal/verifier"},
            {"family": "GoalDirected", "node_key": "goal/verifier", "generation": 1},
            "claim",
        ),
        # next_iteration waits for an iteration start (a retry attempt is not one).
        (
            {"boundary": "next_iteration"},
            {
                "family": "GoalDirected",
                "node_key": "goal/executor",
                "generation": 1,
                "iteration_start": False,
            },
            "wait",
        ),
        ({}, {"family": "StageGraph", "node_key": "synthesize", "generation": 1}, "claim"),
        (
            {"node_key": "review"},
            {"family": "StageGraph", "node_key": "synthesize", "generation": 1},
            "wait",
        ),
        # A Generation that moved on expires the entry; it is never redirected.
        (
            {},
            {"family": "StageGraph", "node_key": "synthesize", "generation": 2},
            "stale_generation",
        ),
        (
            {"deadline": NOW - timedelta(minutes=1)},
            {"family": "StageGraph", "node_key": "synthesize", "generation": 1},
            "deadline_passed",
        ),
    ],
)
def test_claim_decision(entry: dict[str, Any], point: dict[str, Any], decision: str) -> None:
    assert (
        claim_decision(_entry(**entry), MailboxBoundaryPoint.model_validate(point), now=NOW)
        == decision
    )


def test_inline_entry_must_match_its_digest_and_reference_view_drops_the_body() -> None:
    entry = _entry()
    assert entry.reference_view().content_inline is None
    with pytest.raises(ValueError):
        _entry(content_inline="tampered text")


# --- Admission -------------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_instruction_is_accepted_and_queued_without_a_transition() -> None:
    world = await World().start()
    before = await world.projection()
    receipt = await world.queue()
    assert receipt.admission.status == CommandStatus.ACCEPTED
    assert receipt.admission.reason_code == "accepted_queued"
    assert receipt.delivery is not None
    assert states(receipt.delivery) == ["accepted", "queued"]
    assert receipt.delivery.command.target.sequence_space == "mailbox:1"
    assert receipt.delivery.command.target_sequence == 1
    # A queued instruction never moves the run version or phase.
    after = await world.projection()
    assert (after.version, after.phase) == (before.version, before.phase)
    (entry,) = await world.entries()
    assert entry.state == MailboxState.QUEUED
    assert entry.content_inline == "Also update the README with the new flag"
    assert entry.content_digest == content_digest(entry.content_inline)
    assert entry.content_ref == inline_content_ref(entry.content_digest)
    assert (entry.boundary, entry.generation, entry.admission_sequence) == ("next_turn", 1, 1)
    # The command record binds the content by digest; the body lives only in the mailbox.
    assert "README" not in receipt.delivery.command.model_dump_json()
    assert "command.queued" in outbox_types(world)


@pytest.mark.asyncio
async def test_add_context_reference_and_sequencing_in_the_mailbox_space() -> None:
    world = await World().start()
    await world.queue("first")
    digest = "sha256:" + "c" * 64
    receipt = await world.queue(
        kind="add_context",
        boundary="next_iteration",
        expand="materialize",
        content={"artifact_ref": "artifact://evidence/1", "content_digest": digest},
        node_key="goal/executor",
    )
    assert receipt.delivery is not None
    assert receipt.delivery.command.kind == "add_context"
    assert receipt.delivery.command.target_sequence == 2
    first, second = await world.entries()
    assert first.admission_sequence == 1
    assert second.content_ref == "artifact://evidence/1"
    assert second.content_inline is None
    assert (second.expand, second.boundary, second.node_key) == (
        "materialize",
        "next_iteration",
        "goal/executor",
    )


@pytest.mark.asyncio
async def test_inline_text_above_the_cap_is_a_typed_rejection_and_digest_is_verified() -> None:
    world = await World().start()
    with pytest.raises(MissionControlRejected) as too_large:
        await world.queue("x" * (MAX_INLINE_BYTES + 1))
    assert too_large.value.code == "content_too_large"
    with pytest.raises(MissionControlRejected) as mismatch:
        await world.queue(content={"text": "hi", "content_digest": "sha256:" + "0" * 64})
    assert mismatch.value.code == "content_digest_mismatch"
    assert await world.entries() == ()
    assert await world.facade.commands(world.run_id, operator()) == ()


@pytest.mark.asyncio
async def test_identical_retry_returns_the_same_receipt_and_one_entry() -> None:
    world = await World().start()
    version = (await world.projection()).version
    request = world.request("queue_instruction", {"content": {"text": "retry me"}}, version=version)
    first = await world.facade.command(world.run_id, request, operator())
    second = await world.facade.command(world.run_id, request, operator())
    assert second.replay is True
    assert second.admission == first.admission
    assert second.delivery == first.delivery
    assert len(await world.entries()) == 1


@pytest.mark.asyncio
async def test_stale_version_and_generation_are_rejected_with_the_current_frontier() -> None:
    world = await World().start()
    version = (await world.projection()).version
    with pytest.raises(MissionControlRejected) as stale:
        await world.facade.command(
            world.run_id,
            world.request("queue_instruction", {"content": {"text": "x"}}, version=version - 1),
            operator(),
        )
    assert stale.value.code == "stale_version"
    assert stale.value.frontier == {
        "version": version,
        "execution_generation": 1,
        "phase": "active",
    }
    with pytest.raises(MissionControlRejected) as generation:
        await world.facade.command(
            world.run_id,
            world.request(
                "queue_instruction", {"content": {"text": "x"}}, version=version, generation=2
            ),
            operator(),
        )
    assert generation.value.code == "stale_generation"
    assert await world.entries() == ()


@pytest.mark.asyncio
async def test_control_grant_is_required_and_cancelling_runs_take_no_instruction() -> None:
    world = await World().start()
    reader = control_actor().model_copy(
        update={"permissions": control_actor().permissions | {"workflow_run.read"}}
    )
    version = (await world.projection()).version
    with pytest.raises(MissionControlRejected) as denied:
        await world.facade.command(
            world.run_id,
            world.request("queue_instruction", {"content": {"text": "x"}}, version=version),
            reader,
        )
    assert denied.value.code == "unauthorized"
    cancel = MissionCommandRequest(
        request_id=uuid4(),
        expected_version=version,
        expected_generation=1,
        target=CommandTarget(id=world.run_id),
        kind="cancel",
        payload=CancelPayload(),
        reason="stop",
    )
    await world.facade.command(world.run_id, cancel, operator())
    rejected = await world.queue("too late")
    assert rejected.admission.status == CommandStatus.REJECTED
    assert rejected.admission.reason_code == "run_is_cancelling"
    assert rejected.delivery is not None and states(rejected.delivery) == ["rejected"]
    assert await world.entries() == ()


@pytest.mark.asyncio
async def test_without_a_mailbox_composition_the_kinds_stay_unsupported() -> None:
    world = await World().start()
    bare = MissionControlService(
        world.authority, BoundaryInterventionService(world.authority), request_scope=SCOPE
    )
    version = (await world.projection()).version
    with pytest.raises(MissionControlRejected) as unsupported:
        await bare.command(
            world.run_id,
            world.request("queue_instruction", {"content": {"text": "x"}}, version=version),
            operator(),
        )
    assert unsupported.value.code == "unsupported_control"


# --- Boundary delivery -----------------------------------------------------------------------


async def _deliver(world: World, key: str, **point: Any) -> tuple[MailboxEntry, ...]:
    return await world.mailbox.deliver(
        SCOPE,
        world.run_id,
        delivery_key=key,
        family=point.get("family", "GoalDirected"),
        node_key=point.get("node_key", "goal/executor"),
        iteration_start=point.get("iteration_start", True),
        lane_profile=point.get("lane_profile", "deep_agents"),
    )


async def _status(world: World, entry: MailboxEntry) -> Any:
    return await world.authority.get_boundary_command(
        SCOPE, world.run_id, entry.command_issuer, entry.command_id
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("lane", "semantics"),
    [("deep_agents", "turn_boundary_guaranteed"), ("cursor_local", "wait_then_send")],
)
async def test_boundary_delivery_observation_and_completion_carry_the_delivery_report(
    lane: str, semantics: str
) -> None:
    world = await World().start()
    await world.queue()
    (delivered,) = await _deliver(world, "goal:op-1:generation:1", lane_profile=lane)
    assert delivered.state == MailboxState.DELIVERED
    status = await _status(world, delivered)
    assert states(status) == ["accepted", "queued", "delivered"]
    report = status.receipts[-1].delivery_report
    assert report is not None
    assert (report.requested_semantics, report.delivered_semantics) == (semantics, semantics)
    assert report.native_refs.lane_profile == lane

    (consumed,) = await world.mailbox.turn_started(
        SCOPE,
        world.run_id,
        delivery_key="goal:op-1:generation:1",
        lane_profile=lane,
        session_ref="thread-1",
    )
    assert consumed.state == MailboxState.CONSUMED
    await world.mailbox.turn_settled(
        SCOPE,
        world.run_id,
        delivery_key="goal:op-1:generation:1",
        lane_profile=lane,
        succeeded=True,
        turn_ref="turn-1",
    )
    status = await _status(world, delivered)
    assert states(status) == ["accepted", "queued", "delivered", "observed", "applied"]
    assert status.receipts[3].delivery_report.native_refs.session_ref == "thread-1"
    assert status.receipts[4].delivery_report.observed_outcome == "applied"
    events = {
        record.envelope.event_type: record.envelope.payload
        for record in world.repository._outbox.values()
    }
    assert events["command.delivered"]["delivery_report"]["delivered_semantics"] == semantics
    assert events["command.completed"]["outcome"] == "applied"
    assert events["command.completed"]["delivery_report"]["observed_outcome"] == "applied"
    assert "README" not in json.dumps(events)  # reference-only events
    # A replayed settlement records nothing twice.
    await world.mailbox.turn_settled(
        SCOPE,
        world.run_id,
        delivery_key="goal:op-1:generation:1",
        lane_profile=lane,
        succeeded=True,
    )
    assert len((await _status(world, delivered)).receipts) == 5


@pytest.mark.asyncio
async def test_a_failed_turn_completes_failed_and_a_turn_that_never_started_requeues() -> None:
    world = await World().start()
    await world.queue("one")
    await _deliver(world, "key-1")
    released = await world.mailbox.turn_not_started(SCOPE, world.run_id, delivery_key="key-1")
    assert [entry.state for entry in released] == [MailboxState.QUEUED]
    (again,) = await _deliver(world, "key-2")
    await world.mailbox.turn_started(
        SCOPE, world.run_id, delivery_key="key-2", lane_profile="deep_agents"
    )
    await world.mailbox.turn_settled(
        SCOPE, world.run_id, delivery_key="key-2", lane_profile="deep_agents", succeeded=False
    )
    assert states(await _status(world, again))[-1] == "failed"


@pytest.mark.asyncio
async def test_claim_is_idempotent_per_delivery_key_even_when_it_took_nothing() -> None:
    world = await World().start()
    assert await _deliver(world, "key-empty") == ()
    await world.queue("arrived after the first claim")
    # A retried preparation of the same operation re-reads its own claim: still nothing.
    assert await _deliver(world, "key-empty") == ()
    (first,) = await _deliver(world, "key-next")
    # ...and a retry of the next one gets exactly what it took.
    await world.queue("a later instruction")
    assert [entry.entry_id for entry in await _deliver(world, "key-next")] == [first.entry_id]
    # Delivered once: the receipt ledger holds one `delivered` per command.
    assert states(await _status(world, first)).count("delivered") == 1


@pytest.mark.asyncio
async def test_a_consumed_entry_is_never_delivered_again() -> None:
    world = await World().start()
    await world.queue("exactly once")
    (entry,) = await _deliver(world, "key-1")
    for _attempt in range(2):  # a worker restart repeats the turn start
        await world.mailbox.turn_started(
            SCOPE, world.run_id, delivery_key="key-1", lane_profile="deep_agents"
        )
    assert await _deliver(world, "key-2") == ()
    assert states(await _status(world, entry)).count("observed") == 1


@pytest.mark.asyncio
async def test_cancel_before_delivery_supersedes_pending_entries() -> None:
    world = await World().start()
    await world.queue("queued")
    await world.queue("delivered, not started")
    await _deliver(world, "key-1", node_key="goal/executor")
    await world.queue("queued after delivery")
    version = (await world.projection()).version
    cancel = await world.facade.command(
        world.run_id,
        MissionCommandRequest(
            request_id=uuid4(),
            expected_version=version,
            expected_generation=1,
            target=CommandTarget(id=world.run_id),
            kind="cancel",
            payload=CancelPayload(),
            reason="wrong repo",
        ),
        operator(),
    )
    assert cancel.admission.status == CommandStatus.ACCEPTED
    entries = await world.entries()
    assert {entry.state for entry in entries} == {MailboxState.SUPERSEDED}
    assert {entry.superseded_by for entry in entries} == {str(cancel.request_id)}
    for entry in entries:
        status = await _status(world, entry)
        assert status.state == ReceiptState.EXPIRED
        assert status.receipts[-1].boundary_state["superseded_by"] == str(cancel.request_id)
    assert await _deliver(world, "key-2") == ()


@pytest.mark.asyncio
async def test_a_generation_that_moved_on_expires_the_entry_with_stale_generation() -> None:
    world = await World().start()
    await world.queue("for generation 1")

    class NextGeneration:
        def __init__(self, inner: RunControlService) -> None:
            self.inner = inner

        async def get_run(self, request_scope: str, run_id: str) -> RunProjection:
            projection = await self.inner.get_run(request_scope, run_id)
            assert projection.execution_target is not None
            return projection.model_copy(
                update={
                    "execution_target": projection.execution_target.model_copy(
                        update={"execution_generation": 2}
                    )
                }
            )

        def __getattr__(self, name: str) -> Any:
            return getattr(self.inner, name)

    mailbox = MailboxDeliveryService(
        world.repository.mailbox,
        NextGeneration(world.authority),  # type: ignore[arg-type]
        clock=world.clock,
    )
    delivered = await mailbox.deliver(
        SCOPE,
        world.run_id,
        delivery_key="key-g2",
        family="GoalDirected",
        node_key="goal/executor",
        iteration_start=True,
        lane_profile="deep_agents",
    )
    assert delivered == ()
    (entry,) = await world.entries()
    assert (entry.state, entry.expired_reason) == (MailboxState.EXPIRED, "stale_generation")
    status = await _status(world, entry)
    assert status.state == ReceiptState.EXPIRED
    assert status.receipts[-1].boundary_state["expired_reason"] == "stale_generation"


@pytest.mark.asyncio
async def test_terminalization_expires_queued_mailbox_commands() -> None:
    world = await World().start()
    receipt = await world.queue("never read")
    assert receipt.delivery is not None
    # A cancel admitted outside the facade (no supersession): the terminal commit closes it.
    await world.authority.execute(world.lifecycle("cancel", 2, CancelAction()))
    await world.authority.execute(
        world.lifecycle(
            "release-baseline",
            3,
            RecordUsageAction(
                usage_id="usage:release",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    terminal = await world.authority.execute(
        world.lifecycle(
            "terminalize",
            4,
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
    status = await world.authority.get_boundary_command(
        SCOPE, world.run_id, receipt.delivery.command.idempotency_issuer, str(receipt.request_id)
    )
    assert status is not None and status.state == ReceiptState.EXPIRED
    assert status.receipts[-1].boundary_state == {"expired_reason": "terminal_run"}


# --- Interfaces ------------------------------------------------------------------------------


def _app(world: World, actor: ActorContext | None = None) -> tuple[FastAPI, Any]:
    installation, tenant = parse_request_scope(SCOPE).installation_id, uuid4()
    del tenant
    scope = parse_request_scope(SCOPE)
    registry = ApplicationRegistry(
        (
            ApplicationBinding.seal(
                application_id="biotech",
                installation_id=installation,
                binding_version="1",
                supabase_project_ref="project",
                database_secret_ref="TEST_DATABASE_URL",
                accepted_issuers={"https://issuer.invalid"},
                accepted_audiences={"authenticated"},
                required_component_version="1",
            ),
        )
    )
    registry.observe(
        "biotech", InstallationObservation(installation, "biotech", "project", frozenset({"1"}))
    )
    app = FastAPI()
    app.include_router(router)
    app.state.mission_control_registry = registry
    app.state.mission_control_services = {(installation, "biotech", scope.tenant_id): world.facade}
    app.dependency_overrides[get_mission_principal] = lambda: MissionPrincipal(
        installation_id=installation,
        application_id="biotech",
        tenant_id=scope.tenant_id,
        issuer="https://issuer.invalid",
        audiences={"authenticated"},
        actor=actor or operator(),
    )
    return app, scope


@pytest.mark.asyncio
async def test_http_accepts_the_kinds_with_202_queued_413_and_409_frontier() -> None:
    world = await World().start()
    app, _scope = _app(world)
    client = TestClient(app)
    version = (await world.projection()).version
    base = f"/v1/applications/biotech/runs/{world.run_id}/commands"
    body = world.request(
        "add_context", {"content": {"text": "a short note"}}, version=version
    ).model_dump(mode="json")
    accepted = client.post(base, json=body)
    assert accepted.status_code == 202
    assert [item["state"] for item in accepted.json()["delivery"]["receipts"]] == [
        "accepted",
        "queued",
    ]
    assert client.post(base, json=body).status_code == 200  # identical retry, same receipt
    too_large = world.request(
        "queue_instruction", {"content": {"text": "x" * (MAX_INLINE_BYTES + 1)}}, version=version
    ).model_dump(mode="json")
    response = client.post(base, json=too_large)
    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "content_too_large"
    stale = world.request(
        "queue_instruction", {"content": {"text": "late"}}, version=version - 1
    ).model_dump(mode="json")
    response = client.post(base, json=stale)
    assert response.status_code == 409
    assert response.json()["detail"]["frontier"]["version"] == version
    listing = client.get(base).json()["commands"]
    assert [item["receipts"][-1]["state"] for item in listing] == ["queued"]


def test_openapi_exports_the_mailbox_payload_unions() -> None:
    app = FastAPI()
    app.include_router(router)
    schemas = app.openapi()["components"]["schemas"]
    for name in (
        "QueueInstructionPayload",
        "AddContextPayload",
        "InterruptAndInjectPayload",
        "ContentRef",
        "InlineText",
    ):
        assert name in schemas
    kinds = schemas["MissionCommandRequest"]["properties"]["kind"]["enum"]
    assert {"queue_instruction", "add_context", "interrupt_and_inject"} <= set(kinds)


@pytest.mark.asyncio
async def test_cli_command_queue_binds_version_and_generation(tmp_path, monkeypatch) -> None:
    world = await World().start()
    app, _scope = _app(world)
    http = TestClient(app)
    instruction = tmp_path / "add-readme.json"
    instruction.write_text(
        json.dumps(
            {
                "boundary": "next_iteration",
                "text": "Also update the README with the new flag",
                "reason": "operator steering",
            }
        )
    )
    sent: list[dict[str, Any]] = []

    def receive(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            sent.append(json.loads(request.content))
        forwarded = http.request(
            request.method,
            request.url.path,
            content=request.content,
            headers={key: value for key, value in request.headers.items() if key != "host"},
        )
        return httpx.Response(forwarded.status_code, content=forwarded.content)

    original = httpx.Client
    monkeypatch.setattr(
        httpx, "Client", lambda **kwargs: original(**kwargs, transport=httpx.MockTransport(receive))
    )
    monkeypatch.setenv("MISSION_CONTROL_TOKEN", "token")
    code = main(
        [
            "command",
            "queue",
            world.run_id,
            "--file",
            str(instruction),
            "--application",
            "biotech",
            "--url",
            "http://127.0.0.1:8000",
        ]
    )
    assert code == 0
    (body,) = sent
    projection = await world.projection()
    assert body["expected_version"] == projection.version
    assert body["expected_generation"] == 1
    assert body["kind"] == "queue_instruction"
    assert body["payload"] == {
        "boundary": "next_iteration",
        "content": {"text": "Also update the README with the new flag"},
    }
    (entry,) = await world.entries()
    assert entry.boundary == "next_iteration"
    # A non-JSON file is the instruction text itself; --add-context sends add_context.
    note = tmp_path / "note.md"
    note.write_text("Prefer the 2024 meta-analysis.")
    assert (
        main(
            [
                "command",
                "queue",
                world.run_id,
                "--file",
                str(note),
                "--add-context",
                "--application",
                "biotech",
                "--url",
                "http://127.0.0.1:8000",
            ]
        )
        == 0
    )
    assert sent[-1]["kind"] == "add_context"
    assert sent[-1]["payload"]["expand"] == "auto"


class Resolver:
    def __init__(self, permissions: frozenset[str]) -> None:
        self.principal = CoordinatorPrincipal(
            actor_id="operator",
            tenant_scope="tenant",
            roles=frozenset({"operator"}),
            permissions=permissions,
            request_scope=SCOPE,
        )

    async def resolve(self, _context: Context) -> CoordinatorPrincipal:
        return self.principal


@pytest.mark.asyncio
async def test_mcp_mission_command_send_answers_like_http() -> None:
    world = await World().start()
    # The MCP principal carries grants only (no authority refs); the HTTP principal matches it,
    # so the second surface replays the first one's command exactly.
    app, _scope = _app(world, ActorContext(actor_id="operator", permissions=operator().permissions))
    server = create_coordinator_server(
        FakeFacade(),
        Resolver(operator().permissions),
        run_control=ScopedRunControl({SCOPE: world.facade}),
    )
    version = (await world.projection()).version
    request = world.request(
        "queue_instruction", {"content": {"text": "same request"}}, version=version
    ).model_dump(mode="json")
    async with Client(server) as mcp:
        result = await mcp.call_tool(
            COMMAND_SEND_TOOL, {"run_id": world.run_id, "request": request}
        )
    envelope = result.structured_content
    assert envelope is not None and envelope["ok"] is True
    receipt = envelope["data"]
    http = TestClient(app).post(
        f"/v1/applications/biotech/runs/{world.run_id}/commands", json=request
    )
    assert http.status_code == 200  # the same request is a replay on HTTP
    http_receipt = {key: value for key, value in http.json().items() if key != "application_id"}
    assert {**http_receipt, "replay": False} == receipt
    async with Client(server) as mcp:
        stale = await mcp.call_tool(
            COMMAND_SEND_TOOL,
            {
                "run_id": world.run_id,
                "request": {**request, "request_id": str(uuid4()), "expected_version": 1},
            },
        )
    assert stale.structured_content["error"]["code"] == "CONFLICT"
    assert stale.structured_content["error"]["details"]["code"] == "stale_version"
