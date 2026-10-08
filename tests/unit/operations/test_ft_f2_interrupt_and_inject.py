"""FT-F2: interrupt_and_inject per lane with uncertain-effect settlement.

The operation boundary runs a real Deep Agent graph (the RRM-004 harness: journaled
coordinator, checkpoint lineage, in-memory run control and the command mailbox). An
`interrupt_and_inject` Command admitted through the mission service while the turn runs is
taken by the lane boundary: the Deep Agents lane declares `cancel_and_replace`, so the running
turn is cancelled, uncertain effects settle (or the unit parks `in_doubt`), and the
replacement turn continues the same session with the injected item. Fake lanes cover
`cooperative_inject`, `unsupported` and the AgentHarness-protocol path FT-G4 reuses.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Mapping
from typing import Any
from uuid import uuid4

import pytest
from langchain_core.messages import BaseMessage

from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.harness.describe import DEEP_AGENTS_DESCRIBE
from mission_control.application.execution.harness.inject import (
    InjectionParked,
    InjectionSettings,
    InterruptAndInjectService,
    TurnRecord,
    cancel_and_replace_turn,
)
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.execution.contracts import (
    PromptSegment,
    RuntimeInvocation,
    RuntimeResult,
)
from mission_control.domain.execution.lanes import (
    CancelReceipt,
    CancelTurnRequest,
    HarnessScope,
    LaneFrame,
    ObserveRequest,
    SendTurnRequest,
    SessionHandle,
    TurnHandle,
)
from mission_control.domain.policies.contracts import (
    ActorContext,
    ClaimEffectAction,
    CommandStatus,
    ReceiptState,
)
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.checkpoint_recovery import (
    RecoveryHarness,
    ScriptedRecoveryModel,
    recovery_harness,
    stage_recovery_unit,
)
from tests.unit.run_control.test_run_control import actor, command

FAST = InjectionSettings(poll_seconds=0.01, settle_grace_seconds=0.3, settle_poll_seconds=0.01)
INJECTED = "Redirect: use release/2.3, not main."


class RecordingModel(ScriptedRecoveryModel):
    """The RRM-004 script, plus the text of every human message each call saw."""

    def _observe(self, messages: list[BaseMessage]) -> tuple[int, int]:
        self.__dict__.setdefault("humans", []).append(
            [str(item.content) for item in messages if item.type == "human"]
        )
        return super()._observe(messages)

    @property
    def humans(self) -> list[list[str]]:
        return self.__dict__.get("humans", [])


def operator() -> ActorContext:
    source = actor()
    return source.model_copy(
        update={"permissions": source.permissions | {"workflow_run.read", "workflow_run.control"}}
    )


async def harness_with_injections() -> RecoveryHarness:
    return await recovery_harness(model=RecordingModel(), injection_settings=FAST)


def facade(harness: RecoveryHarness) -> MissionControlService:
    return MissionControlService(
        harness.run_control,
        BoundaryInterventionService(harness.run_control),
        request_scope="tenant-1",
        mailbox=harness.mailbox,
    )


async def inject(harness: RecoveryHarness, request_id: str | None = None) -> Any:
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    return await facade(harness).command(
        harness.run_id,
        MissionCommandRequest.model_validate(
            {
                "request_id": request_id or str(uuid4()),
                "expected_version": run.version,
                "expected_generation": 1,
                "target": {"kind": "run", "id": harness.run_id},
                "kind": "interrupt_and_inject",
                "payload": {"content": {"text": INJECTED}},
                "reason": "wrong branch",
            }
        ),
        operator(),
    )


async def status_of(harness: RecoveryHarness, receipt: Any) -> Any:
    return await harness.run_control.get_boundary_command(
        "tenant-1",
        harness.run_id,
        receipt.delivery.command.idempotency_issuer,
        str(receipt.request_id),
    )


def states(status: Any) -> list[str]:
    return [item.state.value for item in status.receipts]


@pytest.mark.asyncio
async def test_inject_with_no_running_tool_cancels_and_replaces_at_once() -> None:
    harness = await harness_with_injections()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    entered, _gate = harness.model.gate_on(1)  # the first model call never returns
    running = asyncio.create_task(harness.run(request))
    await asyncio.wait_for(entered.wait(), timeout=30)

    receipt = await inject(harness)
    assert receipt.admission.status == CommandStatus.ACCEPTED
    assert states(receipt.delivery) == ["accepted", "queued"]
    result = await asyncio.wait_for(running, timeout=60)

    assert result.status == "completed"
    # The replacement turn saw the injected item after the original objective, once.
    replacement = harness.model.humans[1]
    assert len(replacement) == 2 and INJECTED in replacement[-1]
    assert sum(INJECTED in text for call in harness.model.humans for text in call) == 2
    status = await status_of(harness, receipt)
    assert states(status) == ["accepted", "queued", "delivered", "observed", "applied"]
    delivered = status.receipts[2].delivery_report
    observed = status.receipts[3].delivery_report
    assert delivered.requested_semantics == "cancel_and_replace"
    assert delivered.native_refs.cancelled_turn_ref == "turn:1"
    assert observed.delivered_semantics == "cancel_and_replace"
    assert observed.native_refs.replacement_turn_ref == "turn:2"
    assert observed.settled_effect_ids == () and observed.pending_effect_ids == ()
    (entry,) = await harness.mailbox.list_entries("tenant-1", harness.run_id)
    assert entry.state == MailboxState.CONSUMED


class SettlesDuringCancel:
    """The journal port, except that the claimed tool effect settles while the turn is being
    cancelled (the tool completed): `unsettled_effect_ids` empties after the first read."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.reads = 0

    async def unsettled_effect_ids(self, binding: Any, claim: Any) -> tuple[str, ...]:
        self.reads += 1
        found = await self._inner.unsettled_effect_ids(binding, claim)
        return found if self.reads <= 2 else ()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def _claim_tool_effect(harness: RecoveryHarness, request: Any) -> None:
    binding = bind_operation_execution_request(request)
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    claimed = await harness.run_control.execute(
        command(
            harness.run_id,
            run.version,
            "claim-tool-effect",
            ClaimEffectAction(
                effect_id="tool-effect:push-branch",
                effect_kind="tool.consequential",
                operation_ref=binding.binding_id,
                provider_idempotency_key="tool-effect:push-branch",
                reservation_id=request.budget_reservation_id,
            ),
        )
    )
    assert claimed.status == CommandStatus.ACCEPTED


@pytest.mark.asyncio
async def test_inject_with_a_running_tool_that_completes_during_cancel_settles_first() -> None:
    harness = await harness_with_injections()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    harness.service._journal = SettlesDuringCancel(harness.service._journal)  # type: ignore[assignment]

    async def claim() -> None:
        await _claim_tool_effect(harness, request)

    harness.model.before_call(2, claim)
    entered, _gate = harness.model.gate_on(2)
    running = asyncio.create_task(harness.run(request))
    await asyncio.wait_for(entered.wait(), timeout=30)
    receipt = await inject(harness)
    result = await asyncio.wait_for(running, timeout=60)

    assert result.status == "completed"
    status = await status_of(harness, receipt)
    assert states(status) == ["accepted", "queued", "delivered", "observed", "applied"]
    observed = status.receipts[3].delivery_report
    assert observed.settled_effect_ids == ("tool-effect:push-branch",)
    # The replacement continued after the tool step: one tool result and the injected item.
    assert INJECTED in harness.model.humans[-1][-1]


@pytest.mark.asyncio
async def test_inject_with_a_tool_that_never_closes_parks_the_unit_in_doubt() -> None:
    harness = await harness_with_injections()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)

    async def claim() -> None:
        await _claim_tool_effect(harness, request)

    harness.model.before_call(2, claim)
    entered, _gate = harness.model.gate_on(2)
    running = asyncio.create_task(harness.run(request))
    await asyncio.wait_for(entered.wait(), timeout=30)
    receipt = await inject(harness)
    result = await asyncio.wait_for(running, timeout=60)

    assert (result.status, result.failure_code) == ("in_doubt", "unsettled_effect_claims")
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None and incident.unsettled_effect_ids == ("tool-effect:push-branch",)
    # No replacement ran: two model calls, the second interrupted.
    assert len(harness.model.calls) == 2
    status = await status_of(harness, receipt)
    assert status.state == ReceiptState.DELIVERED  # waits for the next turn after reconcile
    (entry,) = await harness.mailbox.list_entries("tenant-1", harness.run_id)
    assert entry.state == MailboxState.QUEUED
    events = [
        record.envelope
        for record in harness.repository._outbox.values()
        if record.envelope.event_type == "command.in_doubt"
    ]
    (parked,) = events
    report = parked.payload["delivery_report"]
    assert report["pending_effect_ids"] == ["tool-effect:push-branch"]
    assert report["native_refs"]["cancelled_turn_ref"] == "turn:1"


@pytest.mark.asyncio
async def test_a_duplicate_command_id_interrupts_once() -> None:
    harness = await harness_with_injections()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    entered, _gate = harness.model.gate_on(1)
    running = asyncio.create_task(harness.run(request))
    await asyncio.wait_for(entered.wait(), timeout=30)
    command_id = str(uuid4())
    first = await inject(harness, command_id)
    second = await inject(harness, command_id)
    assert second.replay and second.delivery == first.delivery
    result = await asyncio.wait_for(running, timeout=60)
    assert result.status == "completed"
    assert len(await harness.mailbox.list_entries("tenant-1", harness.run_id)) == 1
    assert len(harness.model.calls) == 3  # interrupted call, then one replacement turn


# --- Lane semantics: unsupported and cooperative_inject (fake lanes) ------------------------


class SlowLane:
    """A bounded execute lane whose turn ends when released; it declares `semantics`."""

    def __init__(self, semantics: str) -> None:
        self.release = asyncio.Event()
        self.started = asyncio.Event()
        self.cancelled = 0
        self.injected: list[str] = []
        self._describe = DEEP_AGENTS_DESCRIBE.model_copy(
            update={
                "delivery_semantics": {
                    **DEEP_AGENTS_DESCRIBE.delivery_semantics,
                    "interrupt_and_inject": semantics,
                }
            }
        )

    def describe(self) -> Any:
        return self._describe

    def requires_checkpoint_lineage(self, request: Any) -> bool:
        return False

    async def execute(
        self, invocation: RuntimeInvocation, resolved_secrets: Mapping[str, str]
    ) -> RuntimeResult:
        self.started.set()
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        return RuntimeResult(output_text="done")

    async def inject(self, invocation: RuntimeInvocation, content: PromptSegment) -> None:
        self.injected.append(content.content)
        self.release.set()


async def _fake_turn(semantics: str) -> tuple[RecoveryHarness, SlowLane, Any, Any, TurnRecord]:
    harness = await harness_with_injections()
    request = await harness.request(stage_recovery_unit(harness.run_id))
    lane = SlowLane(semantics)
    service = InterruptAndInjectService(harness.mailbox, settings=FAST)
    record = TurnRecord()

    async def nothing() -> tuple[str, ...]:
        return ()

    invocation = RuntimeInvocation.model_construct(prompt_segments=request.prompt_segments)
    turn = asyncio.create_task(
        service.run_turn(
            request=request,
            lane=lane,  # type: ignore[arg-type]
            invocation=invocation,
            secrets={},
            unsettled=nothing,
            record=record,
        )
    )
    await asyncio.wait_for(lane.started.wait(), timeout=10)
    receipt = await inject(harness)
    return harness, lane, turn, receipt, record


@pytest.mark.asyncio
async def test_a_lane_reporting_unsupported_rejects_with_a_typed_delivery_report() -> None:
    harness, lane, turn, receipt, record = await _fake_turn("unsupported")
    for _ in range(200):
        status = await status_of(harness, receipt)
        if status.state == ReceiptState.REJECTED:
            break
        await asyncio.sleep(0.01)
    lane.release.set()
    result = await asyncio.wait_for(turn, timeout=10)
    assert result.output_text == "done" and lane.cancelled == 0
    status = await status_of(harness, receipt)
    assert states(status) == ["accepted", "queued", "rejected"]
    assert status.receipts[-1].rejection_reason == "not_applicable"
    assert status.receipts[-1].delivery_report.delivered_semantics == "unsupported"
    (entry,) = await harness.mailbox.list_entries("tenant-1", harness.run_id)
    assert (entry.state, entry.expired_reason) == (MailboxState.EXPIRED, "unsupported_by_lane")
    assert record.delivery_keys == []


@pytest.mark.asyncio
async def test_a_cooperative_lane_steers_the_running_turn_without_cancelling_it() -> None:
    harness, lane, turn, receipt, record = await _fake_turn("cooperative_inject")
    result = await asyncio.wait_for(turn, timeout=10)
    assert result.output_text == "done" and lane.cancelled == 0
    assert lane.injected and INJECTED in lane.injected[0]
    status = await status_of(harness, receipt)
    assert states(status) == ["accepted", "queued", "delivered", "observed"]
    assert status.receipts[-1].delivery_report.delivered_semantics == "cooperative_inject"
    assert len(record.delivery_keys) == 1


# --- The AgentHarness protocol path (FT-G4 reuses it for Cursor) -----------------------------


class ProtocolLane:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def describe(self) -> Any:
        return DEEP_AGENTS_DESCRIBE

    async def cancel_turn(self, request: CancelTurnRequest) -> CancelReceipt:
        self.calls.append("cancel_turn")
        return CancelReceipt(acknowledged=True, native_status="cancelled")

    async def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]:
        self.calls.append("observe")
        yield LaneFrame(
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            provider_key="run:cancelled",
            cursor="terminal",
            kind="terminal",
            terminal=True,
            digest="sha256:" + "0" * 64,
        )

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        self.calls.append(f"send_turn:{request.turn_no}:{request.instruction_ref}")
        return TurnHandle(session=request.session, turn_no=request.turn_no, native_turn_ref="run-2")


def _protocol_requests() -> tuple[CancelTurnRequest, SendTurnRequest]:
    scope = HarnessScope(
        installation_id="installation", application_id="biotech", tenant_id="tenant", actor_id="op"
    )
    common: dict[str, Any] = {
        "scope": scope,
        "lane_profile": "cursor_local",
        "harness_execution_id": "harness-1",
        "binding_digest": "sha256:" + "1" * 64,
        "idempotency_key": "turn-1",
        "generation": 1,
    }
    session = SessionHandle(
        lane_profile="cursor_local",
        harness_execution_id="harness-1",
        generation=1,
        native_session_ref="agent-1",
    )
    turn = TurnHandle(session=session, turn_no=1, native_turn_ref="run-1")
    return (
        CancelTurnRequest(**common, turn=turn, reason="interrupt_and_inject"),
        SendTurnRequest(
            **{**common, "idempotency_key": "turn-2"},
            session=session,
            turn_no=2,
            instruction_ref="mailbox://command-1",
        ),
    )


@pytest.mark.asyncio
async def test_protocol_cancel_and_replace_settles_then_sends_the_replacement_turn() -> None:
    lane = ProtocolLane()
    cancel, replacement = _protocol_requests()
    reads = iter([("effect-1",), ("effect-1",), ()])

    async def unsettled() -> tuple[str, ...]:
        return next(reads, ())

    async def no_sleep(_seconds: float) -> None:
        return None

    replaced = await cancel_and_replace_turn(
        lane,  # type: ignore[arg-type]
        cancel=cancel,
        replacement=replacement,
        unsettled=unsettled,
        settings=FAST,
        sleep=no_sleep,
    )
    assert lane.calls == ["cancel_turn", "observe", "send_turn:2:mailbox://command-1"]
    assert replaced.cancelled_turn_ref == "run-1"
    assert replaced.handle.native_turn_ref == "run-2"
    assert replaced.settled_effect_ids == ("effect-1",)

    stuck = ProtocolLane()

    async def never() -> tuple[str, ...]:
        return ("effect-2",)

    with pytest.raises(InjectionParked) as parked:
        await cancel_and_replace_turn(
            stuck,  # type: ignore[arg-type]
            cancel=cancel,
            replacement=replacement,
            unsettled=never,
            settings=FAST,
            sleep=no_sleep,
        )
    assert parked.value.pending_effect_ids == ("effect-2",)
    assert "send_turn:2:mailbox://command-1" not in stuck.calls


@pytest.mark.asyncio
async def test_http_accepts_interrupt_and_inject_with_202_and_a_command_record() -> None:
    from fastapi.testclient import TestClient

    from tests.unit.run_control.test_ft_f1_command_mailbox import World, _app

    world = await World().start()
    app, _scope = _app(world)
    version = (await world.projection()).version
    body = world.request(
        "interrupt_and_inject",
        {"content": {"text": INJECTED}, "settle_uncertain_effects": True},
        version=version,
    ).model_dump(mode="json")
    response = TestClient(app).post(
        f"/v1/applications/biotech/runs/{world.run_id}/commands", json=body
    )
    assert response.status_code == 202, response.text
    delivery = response.json()["delivery"]
    assert delivery["command"]["kind"] == "interrupt_and_inject"
    assert [item["state"] for item in delivery["receipts"]] == ["accepted", "queued"]
    (entry,) = await world.entries()
    assert entry.kind == "interrupt_and_inject" and entry.content_inline == INJECTED


@pytest.mark.asyncio
async def test_cli_command_inject_sends_interrupt_and_inject_without_a_boundary(
    tmp_path: Any, monkeypatch: Any
) -> None:
    import json

    import httpx
    from fastapi.testclient import TestClient

    from mission_control.interfaces.cli.main import main
    from tests.unit.run_control.test_ft_f1_command_mailbox import World, _app

    world = await World().start()
    app, _scope = _app(world)
    http = TestClient(app)
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
    redirect = tmp_path / "redirect.md"
    redirect.write_text(INJECTED)
    common = ["--application", "biotech", "--url", "http://127.0.0.1:8000"]
    assert main(["command", "inject", world.run_id, "--file", str(redirect), *common]) == 0
    (body,) = sent
    assert body["kind"] == "interrupt_and_inject"
    assert body["expected_version"] == (await world.projection()).version
    assert body["expected_generation"] == 1
    # An injected item rides the replacement turn: no boundary and no deadline.
    assert body["payload"] == {"content": {"text": INJECTED}}
    (entry,) = await world.entries()
    assert (entry.kind, entry.boundary) == ("interrupt_and_inject", "next_turn")
    # A JSON file may target a node; it may not ask `inject` for another kind.
    targeted = tmp_path / "targeted.json"
    targeted.write_text(json.dumps({"text": INJECTED, "node_key": "goal/executor"}))
    assert main(["command", "inject", world.run_id, "--file", str(targeted), *common]) == 0
    assert sent[-1]["payload"]["node_key"] == "goal/executor"
    wrong = tmp_path / "wrong.json"
    wrong.write_text(json.dumps({"kind": "queue_instruction", "text": INJECTED}))
    assert main(["command", "inject", world.run_id, "--file", str(wrong), *common]) != 0
    assert len(sent) == 2


@pytest.mark.asyncio
async def test_mcp_mission_command_send_admits_interrupt_and_inject_like_http() -> None:
    from fastapi.testclient import TestClient
    from fastmcp import Client

    from mission_control.interfaces.mcp.coordinator_server import create_coordinator_server
    from mission_control.interfaces.mcp.run_control_tools import (
        COMMAND_SEND_TOOL,
        ScopedRunControl,
    )
    from tests.unit.coordinator.test_coordinator_mcp_read_surface import FakeFacade
    from tests.unit.run_control.test_ft_f1_command_mailbox import SCOPE, Resolver, World, _app
    from tests.unit.run_control.test_ft_f1_command_mailbox import operator as f1_operator

    world = await World().start()
    grants = f1_operator().permissions
    app, _scope = _app(world, ActorContext(actor_id="operator", permissions=grants))
    server = create_coordinator_server(
        FakeFacade(), Resolver(grants), run_control=ScopedRunControl({SCOPE: world.facade})
    )
    version = (await world.projection()).version
    request = world.request(
        "interrupt_and_inject", {"content": {"text": INJECTED}}, version=version
    ).model_dump(mode="json")
    async with Client(server) as mcp:
        result = await mcp.call_tool(
            COMMAND_SEND_TOOL, {"run_id": world.run_id, "request": request}
        )
    envelope = result.structured_content
    assert envelope is not None and envelope["ok"] is True
    receipt = envelope["data"]
    assert receipt["delivery"]["command"]["kind"] == "interrupt_and_inject"
    http = TestClient(app).post(
        f"/v1/applications/biotech/runs/{world.run_id}/commands", json=request
    )
    assert http.status_code == 200  # the same request replays on HTTP
    http_receipt = {key: value for key, value in http.json().items() if key != "application_id"}
    assert {**http_receipt, "replay": False} == receipt
    (entry,) = await world.entries()
    assert entry.kind == "interrupt_and_inject"
