"""`lane.turn`, `lane.status` and `lane.cancel` for Session Lanes (SPEC-07 section 4; FT-G2).

One lifecycle synthesis for every protocol lane (ADR-0031). A segment of `lane.turn`:

1. admits the attempt at the operation boundary (binding, authority and the run-control
   effect claim exist before any provider work; a settled attempt returns its settlement);
2. on `start` prepares, starts and sends the turn, recording the native session and turn on
   the harness execution *before* it observes; on `resume` (or a retried `start` whose turn
   is already recorded) it reattaches and never sends again: a lost native turn makes the
   unit `in_doubt`;
3. persists every observed Provider Frame through the FrameSink, deduplicated by provider
   key, and only then heartbeats the cursor (the persisted frames are the resume truth, the
   throttled heartbeat a hint);
4. stops at the segment bound (`done=False` with the cursor) or at a terminal frame, whose
   closing facts end the session (patch, outputs, lease) and settle the operation once.

The Deep Agents lane is not a Session Lane: its governed `operation.execute` body already
synthesizes a bounded turn with checkpoint lineage, so the activity adapter runs it through
`lane.turn` unchanged (`governed`).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import aclosing, suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import UUID

from mission_control.application.execution.harness.protocol import (
    NativeTurnLost,
    SessionLane,
)
from mission_control.application.execution.harness.registry import LaneRegistry
from mission_control.application.execution.harness.state import (
    LaneExecutionStateStore,
    LaneExecutionUpdate,
    segment_update,
)
from mission_control.application.frames.kinds import classify
from mission_control.application.frames.reducer import FrameFactProjector, FrameFactTarget
from mission_control.application.frames.sink import FrameStore, harness_execution_id
from mission_control.application.frames.writer import FrameWriter
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.contracts import SecretRef
from mission_control.domain.execution.checkpoint_lineage import OperationActivityAttempt
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    OperationExecutionResult,
)
from mission_control.domain.execution.lane_turns import (
    ClosingFacts,
    LaneCancelRequest,
    LaneCancelResult,
    LaneStatusRequest,
    LaneStatusResult,
    LaneTurnRequest,
    LaneTurnResult,
    NativeRefs,
)
from mission_control.domain.execution.lanes import (
    CancelReceipt,
    CancelTurnRequest,
    EndSessionRequest,
    HarnessScope,
    LaneFrame,
    ObserveRequest,
    PrepareRequest,
    ReattachRequest,
    SendTurnRequest,
    SessionHandle,
    StartRequest,
    StatusRequest,
    TurnHandle,
    UsageRequest,
)
from mission_control.domain.frames.contracts import (
    FrameObservation,
    HarnessExecutionHandle,
    HarnessExecutionStart,
    LaneProfile,
)

_LOGGER = logging.getLogger(__name__)
LANE_ACTOR = "mission-control-lane-turn"
_RECORDED_DETAILS = frozenset(
    {"cursor_sdk_version", "bridge_state_root", "cloud_branch", "cloud_agent_url"}
)


class LaneSessionAdmissionView(Protocol):
    binding: Any
    settled: OperationExecutionResult | None


class LaneBoundary(Protocol):
    """What `lane.turn` needs from the governed operation boundary."""

    async def admit_lane_session(
        self, request: OperationExecutionRequest
    ) -> LaneSessionAdmissionView: ...

    async def settle_lane_session(
        self,
        request: OperationExecutionRequest,
        facts: ClosingFacts,
        *,
        attempt: OperationActivityAttempt | None = None,
        native_turn_ref: str | None = None,
        cancelled_by_command: bool = False,
    ) -> OperationExecutionResult: ...

    async def lane_settlement(
        self, request: OperationExecutionRequest
    ) -> OperationExecutionResult | None: ...

    async def lane_in_doubt(
        self, request: OperationExecutionRequest, *, reason: str
    ) -> OperationExecutionResult: ...


class SecretValues(Protocol):
    async def resolve(self, refs: tuple[SecretRef, ...]) -> Mapping[str, str]: ...


class TurnSignals(Protocol):
    """The activity's view of Temporal: heartbeat, prior heartbeat and requested cancel."""

    def heartbeat(self, cursor: str | None, frames_persisted: int) -> None: ...

    def prior_cursor(self) -> str | None: ...

    def cancel_requested(self) -> bool: ...


class LaneNotSessionDriven(TypeError):
    """The lane is governed by the operation boundary's bounded execute path (Deep Agents)."""


def _utc_now() -> datetime:
    return datetime.now(UTC)


def harness_scope(request_scope: str, actor_id: str = LANE_ACTOR) -> HarnessScope:
    try:
        parsed = parse_request_scope(request_scope)
    except ValueError:
        # Non-canonical scopes (local proof) name all three parts by the same value.
        return HarnessScope(
            installation_id=request_scope,
            application_id=request_scope,
            tenant_id=request_scope,
            actor_id=actor_id,
        )
    return HarnessScope(
        installation_id=str(parsed.installation_id),
        application_id=parsed.application_id,
        tenant_id=str(parsed.tenant_id),
        actor_id=actor_id,
    )


@dataclass(frozen=True)
class LaneExecutionIdentity:
    """Where one lane attempt's frames and state live (SPEC-03 harness execution)."""

    request_scope: str
    run_key: str
    activation_key: str
    attempt_no: int
    lane_profile: str
    harness_execution_id: UUID

    @classmethod
    def of(cls, operation: OperationExecutionRequest, lane_profile: str, generation: int) -> Any:
        unit = operation.runtime_unit
        request_scope = unit.request_scope if unit is not None else operation.request_scope
        run_key = unit.belllabs_run_id if unit is not None else operation.identity.run_id
        activation_key = unit.unit_key if unit is not None else operation.identity.operation_id
        return cls(
            request_scope=request_scope,
            run_key=run_key,
            activation_key=activation_key,
            attempt_no=generation,
            lane_profile=lane_profile,
            harness_execution_id=harness_execution_id(
                request_scope=request_scope,
                run_key=run_key,
                activation_key=activation_key,
                attempt_no=generation,
                lane=lane_profile,
            ),
        )


def _binding_digest(operation: OperationExecutionRequest) -> str:
    if operation.cursor_binding is not None:
        return operation.cursor_binding.binding_digest
    return operation.effective_configuration_digest


def execution_start(
    operation: OperationExecutionRequest,
    identity: LaneExecutionIdentity,
    generation: int,
    session_ref: str,
) -> HarnessExecutionStart:
    """The harness execution a Session Lane's frames (provider and hook) are written under."""

    placement = "cloud" if identity.lane_profile == "cursor_cloud" else "worker_hosted"
    return HarnessExecutionStart(
        harness_execution_id=identity.harness_execution_id,
        request_scope=identity.request_scope,
        run_key=identity.run_key,
        activation_key=identity.activation_key,
        attempt_no=identity.attempt_no,
        generation=generation,
        lane_profile=LaneProfile(identity.lane_profile),
        native_session_ref=session_ref,
        runtime_kind=operation.execution_runtime,
        provider_kind=identity.lane_profile,
        placement_kind=placement,
        intended_binding_digest=_binding_digest(operation),
        launch_key=f"{identity.harness_execution_id}:{generation}",
        native_identity={"operation_id": operation.identity.operation_id},
    )


class LaneTurnService:
    """Drives a Session Lane through `lane.turn` segments, `lane.status` and `lane.cancel`."""

    def __init__(
        self,
        *,
        lanes: LaneRegistry,
        boundary: LaneBoundary,
        frames: FrameStore,
        states: LaneExecutionStateStore,
        secrets: SecretValues | None = None,
        frame_facts: FrameFactProjector | None = None,
        clock: Callable[[], datetime] = _utc_now,
        excerpt_cap_bytes: int = 8_192,
    ) -> None:
        self._lanes = lanes
        self._boundary = boundary
        self._frames = frames
        self._states = states
        self._secrets = secrets
        self._frame_facts = frame_facts
        self._clock = clock
        self._cap = excerpt_cap_bytes

    # --- dispatch ---------------------------------------------------------------------------

    def governed(self, lane_profile: str) -> bool:
        """True when the lane runs the boundary's governed execute path (Deep Agents)."""

        return not isinstance(self._lanes.for_profile(lane_profile), SessionLane)

    def _session_lane(
        self, lane_profile: str, operation: OperationExecutionRequest, generation: int
    ) -> SessionLane:
        harness = self._lanes.admit(lane_profile)
        if not isinstance(harness, SessionLane):
            raise LaneNotSessionDriven(f"lane profile {lane_profile} is governed, not driven")
        identity = LaneExecutionIdentity.of(operation, lane_profile, generation)
        harness.stage(str(identity.harness_execution_id), operation)
        return harness

    # --- shared request fields --------------------------------------------------------------

    def _fields(
        self,
        operation: OperationExecutionRequest,
        identity: LaneExecutionIdentity,
        generation: int,
        key: str,
    ) -> dict[str, Any]:
        return {
            "scope": harness_scope(identity.request_scope),
            "lane_profile": identity.lane_profile,
            "harness_execution_id": str(identity.harness_execution_id),
            "binding_digest": _binding_digest(operation),
            "idempotency_key": f"{identity.harness_execution_id}:{generation}:{key}",
            "generation": generation,
        }

    async def _secret_values(self, operation: OperationExecutionRequest) -> tuple[str, ...]:
        if self._secrets is None or not operation.secret_refs:
            return ()
        resolved = await self._secrets.resolve(operation.secret_refs)
        return tuple(value for value in resolved.values() if value)

    async def _open(
        self,
        operation: OperationExecutionRequest,
        identity: LaneExecutionIdentity,
        generation: int,
        session_ref: str,
    ) -> HarnessExecutionHandle:
        return await self._frames.open_execution(
            execution_start(operation, identity, generation, session_ref)
        )

    # --- lane.turn --------------------------------------------------------------------------

    async def turn(
        self,
        request: LaneTurnRequest,
        signals: TurnSignals,
        *,
        attempt: OperationActivityAttempt | None = None,
    ) -> LaneTurnResult:
        operation = request.operation
        harness = self._session_lane(request.lane_profile, operation, request.generation)
        admission = await self._boundary.admit_lane_session(operation)
        if admission.settled is not None:
            return LaneTurnResult(
                done=True,
                segment_no=request.segment_no,
                operation_result=admission.settled.model_dump(mode="json"),
            )
        identity = LaneExecutionIdentity.of(operation, request.lane_profile, request.generation)
        scope, heid = identity.request_scope, identity.harness_execution_id
        state = await self._states.load(scope, heid)
        fields = self._fields(operation, identity, request.generation, f"turn:{request.turn_no}")
        sent = state is not None and state.native_turn_ref is not None
        if request.capacity_exhausted:
            return await self._settle(
                request,
                harness,
                identity,
                None,
                ClosingFacts(native_status="error", error_code="capacity"),
                attempt=attempt,
                native=NativeRefs(
                    session_ref=state.native_session_ref if state else None,
                    turn_ref=state.native_turn_ref if state else None,
                ),
            )
        if request.phase == "start" and not sent:
            started = await self._start_and_send(request, harness, identity, fields)
            if isinstance(started, LaneTurnResult):
                return started
            session, turn = started
            cursor = None
        else:
            if state is None or state.native_session_ref is None:
                return await self._lost(request, identity, reason="native_session_unrecorded")
            try:
                session = await harness.reattach(
                    ReattachRequest(
                        **fields,
                        native_session_ref=state.native_session_ref,
                        native_turn_ref=state.native_turn_ref,
                    )
                )
            except NativeTurnLost:
                return await self._lost(request, identity, reason="native_turn_lost")
            turn = TurnHandle(
                session=session, turn_no=request.turn_no, native_turn_ref=state.native_turn_ref
            )
            cursor = await self._resume_cursor(
                harness, identity, request, signals.prior_cursor() or state.provider_cursor
            )
        handle = await self._open(
            operation, identity, request.generation, session.native_session_ref or str(heid)
        )
        writer = FrameWriter(
            self._frames,
            handle,
            excerpt_cap_bytes=self._cap,
            secret_values=await self._secret_values(operation),
        )
        return await self._observe_segment(
            request, harness, identity, fields, turn, cursor, writer, signals, attempt=attempt
        )

    async def _start_and_send(
        self,
        request: LaneTurnRequest,
        harness: SessionLane,
        identity: LaneExecutionIdentity,
        fields: dict[str, Any],
    ) -> tuple[SessionHandle, TurnHandle] | LaneTurnResult:
        operation = request.operation
        scope, heid = identity.request_scope, identity.harness_execution_id
        prepared = await harness.prepare(
            PrepareRequest(
                **fields,
                run_id=operation.identity.run_id,
                operation_id=operation.identity.operation_id,
                attempt_no=identity.attempt_no,
            )
        )
        state = await self._states.load(scope, heid)
        if state is not None and state.native_session_ref is not None:
            session = await harness.reattach(
                ReattachRequest(**fields, native_session_ref=state.native_session_ref)
            )
        else:
            session = await harness.start(StartRequest(**fields, prepared=prepared))
            if session.native_session_ref is None:
                raise ValueError("a Session Lane start returns the native session identity")
            # Native identity first, then observation (SPEC-07 section 5.2).
            await self._open(operation, identity, request.generation, session.native_session_ref)
            details = {
                key: value
                for key, value in session.native_details.items()
                if key in _RECORDED_DETAILS and value
            }
            await self._states.record(
                scope,
                heid,
                LaneExecutionUpdate(native_session_ref=session.native_session_ref, **details),
            )
        turn = await harness.send_turn(
            SendTurnRequest(
                **fields,
                session=session,
                turn_no=request.turn_no,
                instruction_ref=request.instruction_ref
                or f"operation:{operation.identity.semantic_key}:turn:{request.turn_no}",
            )
        )
        if turn.status == "busy":
            # `wait_then_send`: nothing was sent; the workflow polls `lane.status` until idle.
            return LaneTurnResult(
                done=False,
                busy=True,
                segment_no=request.segment_no,
                native=NativeRefs(session_ref=session.native_session_ref),
            )
        if turn.native_turn_ref is None:
            raise ValueError("a Session Lane send returns the native turn identity")
        await self._states.record(
            scope, heid, LaneExecutionUpdate(native_turn_ref=turn.native_turn_ref)
        )
        return session, turn

    async def _resume_cursor(
        self,
        harness: SessionLane,
        identity: LaneExecutionIdentity,
        request: LaneTurnRequest,
        hint: str | None,
    ) -> str | None:
        """The persisted frames are the truth; the heartbeat, workflow and recorded segment
        cursors are hints (each is written only after the frames it names were persisted)."""

        last = await self._frames.last_cursor(
            identity.harness_execution_id, request.generation, request_scope=identity.request_scope
        )
        if last is not None:
            persisted = harness.resume_cursor(last.provider_key)
            if persisted is not None:
                return persisted
        return hint or request.cursor

    async def _observe_segment(
        self,
        request: LaneTurnRequest,
        harness: SessionLane,
        identity: LaneExecutionIdentity,
        fields: dict[str, Any],
        turn: TurnHandle,
        cursor: str | None,
        writer: FrameWriter,
        signals: TurnSignals,
        *,
        attempt: OperationActivityAttempt | None,
    ) -> LaneTurnResult:
        bounds = request.segment
        persisted = duplicates = observed = 0
        terminal: LaneFrame | None = None
        native = NativeRefs(
            session_ref=turn.session.native_session_ref, turn_ref=turn.native_turn_ref
        )
        last_cursor = cursor
        ticker = asyncio.create_task(
            _heartbeat_ticker(signals, lambda: (last_cursor, persisted), bounds.heartbeat_timeout_s)
        )
        stream: AsyncIterator[LaneFrame] = harness.observe(
            ObserveRequest(**fields, turn=turn, after=cursor, max_frames=bounds.max_frames)
        )
        try:
            async with aclosing(stream) as frames:  # type: ignore[type-var]
                with suppress(TimeoutError):
                    async with asyncio.timeout(bounds.max_duration_s):
                        async for frame in frames:
                            receipt = await writer.write([self._observation(frame, identity)])
                            persisted += receipt.new
                            duplicates += receipt.duplicate
                            observed += 1
                            last_cursor = frame.cursor
                            # Persisted first, then the heartbeat (the resume hint).
                            signals.heartbeat(last_cursor, persisted)
                            if frame.terminal:
                                terminal = frame
                                break
                            if observed >= bounds.max_frames:
                                break
        except asyncio.CancelledError:
            if signals.cancel_requested():
                # A requested cancel, not a worker shutdown, pause or reset.
                with suppress(Exception):
                    await harness.cancel_turn(
                        CancelTurnRequest(**fields, turn=turn, reason="command")
                    )
            raise
        except NativeTurnLost:
            return await self._lost(request, identity, reason="native_turn_lost")
        finally:
            ticker.cancel()
            with suppress(asyncio.CancelledError):
                await ticker
        now = self._clock()
        await self._states.record(
            identity.request_scope,
            identity.harness_execution_id,
            segment_update(last_cursor, now),
        )
        if terminal is None:
            await self._project(writer.handle, identity)
            return LaneTurnResult(
                done=False,
                cursor=last_cursor,
                segment_no=request.segment_no,
                frames_persisted=persisted,
                frames_duplicate=duplicates,
                native=native,
            )
        facts = harness.closing_facts(turn, terminal)
        return await self._settle(
            request,
            harness,
            identity,
            turn,
            facts,
            attempt=attempt,
            native=native,
            cursor=last_cursor,
            persisted=persisted,
            duplicates=duplicates,
            handle=writer.handle,
        )

    def _observation(self, frame: LaneFrame, identity: LaneExecutionIdentity) -> FrameObservation:
        lane = LaneProfile(identity.lane_profile)
        raw_kind = frame.raw_kind or frame.kind
        body = frame.body if frame.body is not None else {"excerpt": frame.excerpt}
        return FrameObservation(
            provider_key=frame.provider_key,
            raw_kind=raw_kind,
            kind=classify(lane, raw_kind, body).kind,
            body=body,
            native_turn_ref=frame.native_turn_ref,
            tool_call_ref=frame.tool_call_ref,
            provider_timestamp=frame.provider_timestamp,
        )

    async def _settle(
        self,
        request: LaneTurnRequest,
        harness: SessionLane,
        identity: LaneExecutionIdentity,
        turn: TurnHandle | None,
        facts: ClosingFacts,
        *,
        attempt: OperationActivityAttempt | None,
        native: NativeRefs,
        cursor: str | None = None,
        persisted: int = 0,
        duplicates: int = 0,
        handle: HarnessExecutionHandle | None = None,
        cancelled_by_command: bool = False,
    ) -> LaneTurnResult:
        operation = request.operation
        fields = self._fields(operation, identity, request.generation, "settle")
        if turn is not None:
            facts = await self._settled_usage(harness, fields, turn, facts)
            facts = await self._end_session(harness, fields, turn.session, facts)
        result = await self._boundary.settle_lane_session(
            operation,
            facts,
            attempt=attempt,
            native_turn_ref=native.turn_ref,
            cancelled_by_command=cancelled_by_command,
        )
        await self._states.record(
            identity.request_scope,
            identity.harness_execution_id,
            LaneExecutionUpdate(usage_disposition=facts.cost_disposition),
        )
        if handle is not None:
            await self._project(handle, identity, cancelled_by_command=cancelled_by_command)
        return LaneTurnResult(
            done=True,
            cursor=cursor,
            segment_no=request.segment_no,
            frames_persisted=persisted,
            frames_duplicate=duplicates,
            native=native,
            closing_facts=facts,
            usage_estimate=facts.usage,
            operation_result=result.model_dump(mode="json"),
        )

    async def _settled_usage(
        self,
        harness: SessionLane,
        fields: dict[str, Any],
        turn: TurnHandle,
        facts: ClosingFacts,
    ) -> ClosingFacts:
        """Tokens settle per turn; cost upgrades from `estimated` only when the provider
        reports it (`feature_unavailable` leaves it estimated, never zero)."""

        try:
            report = await harness.usage(UsageRequest(**fields, session=turn.session, turn=turn))
        except Exception:
            _LOGGER.warning("lane usage was not read; closing usage stays as observed")
            return facts
        usage = facts.usage if report.disposition == "unknown" else report
        cost = facts.cost_disposition
        if report.cost_micros_usd is not None:
            cost = "settled"
        elif cost == "unknown" and usage.disposition != "unknown":
            cost = "estimated"
        return facts.model_copy(update={"usage": usage, "cost_disposition": cost})

    async def _end_session(
        self,
        harness: SessionLane,
        fields: dict[str, Any],
        session: SessionHandle,
        facts: ClosingFacts,
    ) -> ClosingFacts:
        receipt = await harness.end_session(
            EndSessionRequest(**fields, session=session, reason=facts.native_status)
        )
        refs = tuple(
            ref
            for ref in dict.fromkeys((*facts.output_refs, *receipt.artifact_refs))
            if ref != receipt.patch_ref
        )
        patch = receipt.patch_ref or facts.patch_ref
        return facts.model_copy(update={"output_refs": refs, "patch_ref": patch})

    async def _lost(
        self, request: LaneTurnRequest, identity: LaneExecutionIdentity, *, reason: str
    ) -> LaneTurnResult:
        result = await self._boundary.lane_in_doubt(request.operation, reason=reason)
        return LaneTurnResult(
            done=True,
            segment_no=request.segment_no,
            closing_facts=ClosingFacts(native_status="in_doubt", error_code=reason[:128]),
            operation_result=result.model_dump(mode="json"),
        )

    async def _project(
        self,
        handle: HarnessExecutionHandle,
        identity: LaneExecutionIdentity,
        *,
        cancelled_by_command: bool = False,
    ) -> None:
        """Closing-frame facts become mission events; best effort (frames are durable and the
        next projection re-derives every fact). The operation settlement decides `finished`."""

        if self._frame_facts is None:
            return
        try:
            await self._frame_facts.project(
                FrameFactTarget.from_handle(handle, run_key=identity.run_key),
                completion_policy="deferred",
                cancel_command_admitted=cancelled_by_command,
            )
        except Exception:
            _LOGGER.warning("lane frame facts were not applied; the next projection re-derives")

    # --- lane.status ------------------------------------------------------------------------

    async def status(self, request: LaneStatusRequest) -> LaneStatusResult:
        settled = await self._boundary.lane_settlement(request.operation)
        if settled is not None:
            return LaneStatusResult(
                status=settled.status,
                terminal=True,
                settled=True,
                idle=True,
                operation_result=settled.model_dump(mode="json"),
            )
        harness = self._session_lane(request.lane_profile, request.operation, request.generation)
        identity = LaneExecutionIdentity.of(
            request.operation, request.lane_profile, request.generation
        )
        state = await self._states.load(identity.request_scope, identity.harness_execution_id)
        session_ref = request.native.session_ref or (state.native_session_ref if state else None)
        turn_ref = request.native.turn_ref or (state.native_turn_ref if state else None)
        if session_ref is None:
            return LaneStatusResult(status="not_started", terminal=False, idle=True)
        fields = self._fields(request.operation, identity, request.generation, "status")
        session = SessionHandle(
            lane_profile=request.lane_profile,
            harness_execution_id=str(identity.harness_execution_id),
            generation=request.generation,
            native_session_ref=session_ref,
        )
        turn = (
            None
            if turn_ref is None
            else TurnHandle(session=session, turn_no=1, native_turn_ref=turn_ref)
        )
        try:
            observed = await harness.status(StatusRequest(**fields, session=session, turn=turn))
        except NativeTurnLost:
            return LaneStatusResult(status="lost", terminal=False)
        return LaneStatusResult(
            status=observed.status,
            terminal=observed.terminal,
            idle=observed.idle,
            usage=observed.usage,
        )

    # --- lane.cancel ------------------------------------------------------------------------

    async def cancel(
        self,
        request: LaneCancelRequest,
        *,
        attempt: OperationActivityAttempt | None = None,
    ) -> LaneCancelResult:
        """Idempotent provider cancel; settles `cancelled` once the provider is terminal."""

        settled = await self._boundary.lane_settlement(request.operation)
        if settled is not None:
            return LaneCancelResult(
                receipt=CancelReceipt(
                    acknowledged=True, already_terminal=True, native_status=settled.status
                ),
                settled=True,
                operation_result=settled.model_dump(mode="json"),
            )
        harness = self._session_lane(request.lane_profile, request.operation, request.generation)
        identity = LaneExecutionIdentity.of(
            request.operation, request.lane_profile, request.generation
        )
        if request.in_doubt:
            result = await self._boundary.lane_in_doubt(
                request.operation, reason="lane_status_unresolved"
            )
            return LaneCancelResult(
                receipt=CancelReceipt(acknowledged=False, native_status="unknown"),
                operation_result=result.model_dump(mode="json"),
            )
        state = await self._states.load(identity.request_scope, identity.harness_execution_id)
        session_ref = request.native.session_ref or (state.native_session_ref if state else None)
        turn_ref = request.native.turn_ref or (state.native_turn_ref if state else None)
        fields = self._fields(request.operation, identity, request.generation, "cancel")
        if session_ref is None or turn_ref is None:
            # Never sent: nothing to cancel at the provider; the unit settles cancelled.
            receipt = CancelReceipt(acknowledged=True, already_terminal=True, native_status="idle")
            turn: TurnHandle | None = None
        else:
            session = SessionHandle(
                lane_profile=request.lane_profile,
                harness_execution_id=str(identity.harness_execution_id),
                generation=request.generation,
                native_session_ref=session_ref,
            )
            turn = TurnHandle(session=session, turn_no=1, native_turn_ref=turn_ref)
            receipt = await harness.cancel_turn(
                CancelTurnRequest(
                    **fields, turn=turn, reason=request.reason, urgency=request.urgency
                )
            )
        if not request.settle or not (receipt.already_terminal or receipt.acknowledged):
            return LaneCancelResult(receipt=receipt)
        if turn is not None:
            observed = await harness.status(
                StatusRequest(**fields, session=turn.session, turn=turn)
            )
            if not observed.terminal:
                return LaneCancelResult(receipt=receipt)
        turn_request = LaneTurnRequest(
            operation=request.operation,
            lane_profile=request.lane_profile,
            generation=request.generation,
            phase="resume",
        )
        cancelled = await self._settle(
            turn_request,
            harness,
            identity,
            turn,
            ClosingFacts(native_status="cancelled", error_code="cancelled_by_command"),
            attempt=attempt,
            native=NativeRefs(session_ref=session_ref, turn_ref=turn_ref),
            cancelled_by_command=True,
        )
        return LaneCancelResult(
            receipt=receipt, settled=True, operation_result=cancelled.operation_result
        )


async def _heartbeat_ticker(
    signals: TurnSignals,
    progress: Callable[[], tuple[str | None, int]],
    heartbeat_timeout_s: int,
) -> None:
    """Keep heartbeating the last persisted cursor while the provider is silent, so a long
    tool call is not mistaken for a lost worker (the SDK throttles sends anyway)."""

    interval = max(heartbeat_timeout_s / 3, 1.0)
    while True:
        await asyncio.sleep(interval)
        cursor, persisted = progress()
        signals.heartbeat(cursor, persisted)


__all__ = [
    "LaneBoundary",
    "LaneExecutionIdentity",
    "LaneNotSessionDriven",
    "LaneTurnService",
    "TurnSignals",
    "execution_start",
    "harness_scope",
]
