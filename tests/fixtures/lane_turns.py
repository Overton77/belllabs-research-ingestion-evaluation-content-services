"""FT-G2 fixtures: a scripted Session Lane, a Cursor-runtime operation and a lane turn stack.

`ScriptedSessionLane` is a protocol-complete fake provider: it replays scripted frames with a
monotonic offset cursor, can hold the stream open (a long tool call), report `busy`, lose a
turn, and records every native call so tests can prove a resume never sends twice and a
cancel reaches the provider only when it was requested.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from tests.unit.operations.test_operation_execution import MCP_DIGEST, SKILL_DIGEST

from mission_control.adapters.operations.conformance import (
    ConformanceAssetVerifier,
    ConformanceAuthority,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceRuntime,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from mission_control.application.execution.harness.deep_agents_harness import DeepAgentsHarness
from mission_control.application.execution.harness.describe import CURSOR_LOCAL_DESCRIBE
from mission_control.application.execution.harness.lane_turns import LaneTurnService
from mission_control.application.execution.harness.protocol import NativeTurnLost
from mission_control.application.execution.harness.registry import LaneRegistry
from mission_control.application.execution.harness.state import InMemoryLaneExecutionStateStore
from mission_control.application.execution.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    OperationExecutionService,
)
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import OperationExecutionRequest
from mission_control.domain.execution.lane_turns import ClosingFacts
from mission_control.domain.execution.lanes import (
    CancelReceipt,
    CancelTurnRequest,
    CleanupReceipt,
    CursorExecutionBinding,
    EndSessionRequest,
    LaneDescribe,
    LaneFrame,
    ObserveRequest,
    PreparedSession,
    PrepareRequest,
    ProviderStatus,
    ReattachRequest,
    SendTurnRequest,
    SessionHandle,
    SnapshotManifest,
    SnapshotRequest,
    StartRequest,
    StatusRequest,
    TurnHandle,
    UsageReport,
    UsageRequest,
)

INSTALLATION = UUID("11111111-1111-4111-8111-111111111111")
TENANT = UUID("22222222-2222-4222-8222-222222222222")
SCOPE = f"mc/{INSTALLATION}/biotech/{TENANT}"
LANE_QUEUE = "lane-turn-conformance"
DIGEST = "sha256:" + "c" * 64
RUN_UUID = UUID("33333333-3333-4333-8333-333333333333")
ACTIVATION_UUID = UUID("44444444-4444-4444-8444-444444444444")


def cursor_binding(profile: str = "cursor_local", **changes: Any) -> CursorExecutionBinding:
    fields: dict[str, Any] = {
        "lane_profile": profile,
        "pins": {
            "cursor_sdk": "1.0.37",
            "bridge": "1.0.37",
            "protocol": "sdk.v1",
            "cloud_api": "v1",
        },
        "model_id": "composer-2",
        "workspace": {"base_ref": "main"},
        "projections": {"rules_digest": DIGEST, "agents_digest": DIGEST, "hooks_digest": DIGEST},
        "budgets": {"max_turns": 4, "max_segments": 8, "wall_clock_s": 3600},
        "task_queue": LANE_QUEUE,
    }
    if profile == "cursor_local":
        fields["hook_callback"] = {"listen": "127.0.0.1:47555", "token_ttl_s": 3600}
    else:
        fields["workspace"] = {"base_ref": "main", "repo_url": "https://github.com/acme/repo"}
        fields["cloud"] = {"auto_create_pr": False}
    fields.update(changes)
    return CursorExecutionBinding.sealed(**fields)


def cursor_operation(
    profile: str = "cursor_local", *, binding: CursorExecutionBinding | None = None
) -> OperationExecutionRequest:
    from tests.unit.operations.test_operation_execution import operation_request

    payload = operation_request().model_dump(mode="python")
    payload.update(
        request_scope=SCOPE,
        execution_runtime="cursor",
        native_placement=None,
        lane_profile=profile,
        cursor_binding=binding or cursor_binding(profile),
        budget_limits={"model.turns": 2, "tokens.total": 100_000},
    )
    return OperationExecutionRequest.model_validate(payload)


@dataclass
class ScriptedSessionLane:
    """A protocol-complete fake `cursor_local` lane over scripted frames."""

    frames: list[tuple[str, dict[str, Any]]] = field(default_factory=list)
    describe_matrix: LaneDescribe = CURSOR_LOCAL_DESCRIBE
    busy_sends: int = 0
    lose_turn: bool = False
    hold_after: int | None = None
    terminal_status: str = "finished"
    cost_micros: int | None = None
    never_terminal: bool = False
    cancel_delay_s: float = 0.0
    fail_at: int | None = None
    calls: list[str] = field(default_factory=list)
    staged: dict[str, OperationExecutionRequest] = field(default_factory=dict)
    sends: list[str] = field(default_factory=list)
    cancels: list[str] = field(default_factory=list)
    released: asyncio.Event = field(default_factory=asyncio.Event)
    held: asyncio.Event = field(default_factory=asyncio.Event)
    cancelled: bool = False

    def describe(self) -> LaneDescribe:
        return self.describe_matrix

    def stage(self, harness_execution_id: str, operation: OperationExecutionRequest) -> None:
        self.staged[harness_execution_id] = operation

    async def prepare(self, request: PrepareRequest) -> PreparedSession:
        self.calls.append("prepare")
        return PreparedSession(
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            workspace_ref="workspace:fake",
        )

    async def start(self, request: StartRequest) -> SessionHandle:
        self.calls.append("start")
        return SessionHandle(
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            native_session_ref="agent-fake-1",
        )

    async def reattach(self, request: ReattachRequest) -> SessionHandle:
        self.calls.append("reattach")
        if self.lose_turn:
            raise NativeTurnLost(request.native_turn_ref or "turn")
        return SessionHandle(
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            native_session_ref=request.native_session_ref,
        )

    async def send_turn(self, request: SendTurnRequest) -> TurnHandle:
        self.calls.append("send_turn")
        if self.busy_sends > 0:
            self.busy_sends -= 1
            return TurnHandle(session=request.session, turn_no=request.turn_no, status="busy")
        self.sends.append(request.idempotency_key)
        return TurnHandle(
            session=request.session, turn_no=request.turn_no, native_turn_ref="run-fake-1"
        )

    async def cancel_turn(self, request: CancelTurnRequest) -> CancelReceipt:
        self.calls.append("cancel_turn")
        self.cancels.append(request.reason)
        if self.cancel_delay_s:
            await asyncio.sleep(self.cancel_delay_s)
        already = self.cancelled
        self.cancelled = True
        self.released.set()
        return CancelReceipt(acknowledged=True, already_terminal=already, native_status="cancelled")

    async def observe(self, request: ObserveRequest) -> AsyncIterator[LaneFrame]:
        self.calls.append(f"observe:{request.after}")
        after = int(request.after) if request.after is not None else -1
        for offset, (raw_kind, body) in enumerate(self.frames):
            if offset <= after:
                continue
            if self.fail_at is not None and offset == self.fail_at:
                self.fail_at = None
                raise ConnectionError("bridge stream dropped")
            if self.hold_after is not None and offset > self.hold_after:
                self.held.set()
                await self.released.wait()
                if self.cancelled:
                    return
            yield LaneFrame(
                harness_execution_id=request.harness_execution_id,
                generation=request.generation,
                provider_key=f"fake:run-fake-1:{offset}",
                cursor=str(offset),
                kind=raw_kind,
                raw_kind=raw_kind,
                body=body,
                terminal=raw_kind == "result",
                digest=sha256_digest(body),
                native_turn_ref="run-fake-1",
                tool_call_ref=body.get("call_id"),
            )

    async def snapshot(self, request: SnapshotRequest) -> SnapshotManifest:
        refs = ("patch:fake",)
        return SnapshotManifest(
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            kind="git_patch",
            refs=refs,
            emulated=True,
            digest=sha256_digest(list(refs)),
        )

    async def usage(self, request: UsageRequest) -> UsageReport:
        self.calls.append("usage")
        return UsageReport(
            disposition="settled",
            input_tokens=10,
            output_tokens=5,
            total_tokens=15,
            cost_micros_usd=self.cost_micros,
        )

    async def end_session(self, request: EndSessionRequest) -> CleanupReceipt:
        self.calls.append("end_session")
        return CleanupReceipt(released=True, artifact_refs=("artifact://fake/patch.diff",))

    def closing_facts(self, turn: TurnHandle, frame: LaneFrame) -> ClosingFacts:
        body = frame.body if isinstance(frame.body, dict) else {}
        return ClosingFacts(
            native_status=body.get("status", self.terminal_status),
            result_excerpt=str(body.get("result", "")),
            usage=UsageReport(disposition="estimated", total_tokens=12),
            cost_disposition="estimated",
        )

    async def status(self, request: StatusRequest) -> ProviderStatus:
        self.calls.append("status")
        if self.lose_turn:
            raise NativeTurnLost("gone")
        terminal = self.cancelled and not self.never_terminal
        return ProviderStatus(
            status="cancelled" if terminal else "running", terminal=terminal, idle=terminal
        )

    def resume_cursor(self, provider_key: str) -> str | None:
        return provider_key.rsplit(":", 1)[-1] if provider_key.startswith("fake:") else None


def scripted_frames(
    tool_calls: int = 3, *, status: str = "finished"
) -> list[tuple[str, dict[str, Any]]]:
    frames: list[tuple[str, dict[str, Any]]] = [("system", {"subtype": "init"})]
    for index in range(tool_calls):
        frames.append(("tool_call", {"call_id": f"call-{index}", "status": "running"}))
        frames.append(("tool_call", {"call_id": f"call-{index}", "status": "completed"}))
    frames.append(("TurnEndedUpdate", {"usage": {"input_tokens": 10, "output_tokens": 5}}))
    frames.append(("result", {"status": status, "result": "done"}))
    return frames


@dataclass
class RecordingSignals:
    """`TurnSignals` that records each heartbeat with the frames stored at that moment."""

    store: InMemoryFrameStore
    harness_execution_id: UUID | None = None
    fail_on: int | None = None
    cancel: bool = False
    prior: str | None = None
    beats: list[tuple[str | None, int, int]] = field(default_factory=list)

    def heartbeat(self, cursor: str | None, frames_persisted: int) -> None:
        stored = 0
        if self.harness_execution_id is not None:
            stored = len(self.store._executions[self.harness_execution_id].frames)
        self.beats.append((cursor, frames_persisted, stored))
        if self.fail_on is not None and len(self.beats) == self.fail_on:
            raise SimulatedWorkerLoss

    def prior_cursor(self) -> str | None:
        return self.prior

    def cancel_requested(self) -> bool:
        return self.cancel


class SimulatedWorkerLoss(BaseException):
    """A worker that dies between a persisted frame and its heartbeat."""


@dataclass
class LaneStack:
    service: LaneTurnService
    boundary: OperationExecutionService
    bindings: InMemoryOperationBindingRepository
    frames: InMemoryFrameStore
    states: InMemoryLaneExecutionStateStore
    lane: Any
    budget: ConformanceBudgetAuthority


def lane_stack(
    lane: Any = None,
    *,
    frames: InMemoryFrameStore | None = None,
    operation: OperationExecutionRequest | None = None,
) -> LaneStack:
    """The lane turn service over a governed in-memory boundary and `lane` (a Session Lane)."""

    lane = lane or ScriptedSessionLane(frames=scripted_frames())
    request = operation or cursor_operation()
    runtime = ConformanceRuntime()
    registry = LaneRegistry([DeepAgentsHarness(runtime), lane], allow_unqualified=True)
    bindings = InMemoryOperationBindingRepository()
    budget = ConformanceBudgetAuthority()
    verifier = ConformanceAssetVerifier(
        mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
        asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
    )
    boundary = OperationExecutionService(
        authority=ConformanceAuthority(
            accepted_run_id=request.identity.run_id,
            configuration_digest=request.effective_configuration_digest,
            control_revision=request.run_control_revision,
            reservation_id=request.budget_reservation_id,
        ),
        bindings=bindings,
        runtime=runtime,
        sandbox=ConformanceSandbox(),
        assets=verifier,
        mcp=verifier,
        secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": "sk-fixture-secret"}),
        events=ConformanceEventSink(),
        budget=budget,
        lanes=registry,
    )
    frames = frames or InMemoryFrameStore()
    frames.register_run(
        SCOPE,
        request.identity.run_id,
        RUN_UUID,
        {request.identity.operation_id: ACTIVATION_UUID},
    )
    states = InMemoryLaneExecutionStateStore()
    service = LaneTurnService(
        lanes=registry,
        boundary=boundary,
        frames=frames,
        states=states,
        secrets=ConformanceSecretResolver({"environment:OPENAI_API_KEY": "sk-fixture-secret"}),
    )
    return LaneStack(service, boundary, bindings, frames, states, lane, budget)
