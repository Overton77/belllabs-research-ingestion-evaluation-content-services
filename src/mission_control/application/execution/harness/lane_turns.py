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

from mission_control.application.execution.harness.controls import (
    SessionHandover,
    SessionHandoverLane,
    TurnTextStaging,
    open_tool_calls,
    usage_from_frames,
)
from mission_control.application.execution.harness.inject import (
    INJECT_KIND,
    InjectionParked,
    InterruptAndInjectService,
    cancel_and_replace_turn,
    interrupt_semantics,
    unit_boundary,
)
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
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.frames.kinds import classify
from mission_control.application.frames.reducer import FrameFactProjector, FrameFactTarget
from mission_control.application.frames.sink import FrameReader, FrameStore, harness_execution_id
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
    UsageReport,
    UsageRequest,
)
from mission_control.domain.frames.contracts import (
    FrameKind,
    FrameObservation,
    HarnessExecutionHandle,
    HarnessExecutionStart,
    LaneProfile,
    ProviderFrame,
)
from mission_control.domain.policies.mailbox import MailboxEntry, MailboxState

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
        mailbox: MailboxDeliveryService | None = None,
        injections: InterruptAndInjectService | None = None,
        frame_reader: FrameReader | None = None,
    ) -> None:
        # FT-G4: the mailbox consumes queued content at the send that carries it
        # (`wait_then_send`) and settles it with the turn; `injections` watches a running
        # turn for `interrupt_and_inject` (`cancel_and_replace`); the frame reader names the
        # turn's Uncertain Effects and its last reported usage.
        self._mailbox = mailbox
        self._injections = injections
        reader = frame_reader
        if reader is None and hasattr(frames, "frames_for_execution"):
            reader = frames  # type: ignore[assignment]
        self._reader: FrameReader | None = reader
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
            if not sent:
                await self._turn_not_started(operation)
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
        await self._turn_started(operation, harness, session, turn)
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
        progress = _SegmentProgress(cursor=cursor, max_frames=bounds.max_frames)
        terminal: LaneFrame | None = None
        ticker = asyncio.create_task(
            _heartbeat_ticker(
                signals, lambda: (progress.cursor, progress.persisted), bounds.heartbeat_timeout_s
            )
        )
        try:
            with suppress(TimeoutError):
                async with asyncio.timeout(bounds.max_duration_s):
                    while True:
                        pumped = await self._pump(
                            request, harness, identity, fields, turn, writer, signals, progress
                        )
                        if pumped.injection is not None:
                            replaced = await self._interrupt(
                                request,
                                harness,
                                identity,
                                fields,
                                turn,
                                pumped.injection,
                                writer,
                                signals,
                                progress,
                                cancel_first=True,
                            )
                            if isinstance(replaced, LaneTurnResult):
                                return replaced
                            turn = replaced
                            continue
                        if pumped.terminal is None:
                            break
                        following = await self._after_terminal(
                            request,
                            harness,
                            identity,
                            fields,
                            turn,
                            pumped.terminal,
                            writer,
                            signals,
                            progress,
                        )
                        if isinstance(following, LaneTurnResult):
                            return following
                        if following is None:
                            terminal = pumped.terminal
                            break
                        turn = following
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
        native = NativeRefs(
            session_ref=turn.session.native_session_ref, turn_ref=turn.native_turn_ref
        )
        now = self._clock()
        await self._states.record(
            identity.request_scope,
            identity.harness_execution_id,
            segment_update(progress.cursor, now),
        )
        if terminal is None:
            await self._project(writer.handle, identity)
            return LaneTurnResult(
                done=False,
                cursor=progress.cursor,
                segment_no=request.segment_no,
                frames_persisted=progress.persisted,
                frames_duplicate=progress.duplicates,
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
            cursor=progress.cursor,
            persisted=progress.persisted,
            duplicates=progress.duplicates,
            handle=writer.handle,
        )

    async def _pump(
        self,
        request: LaneTurnRequest,
        harness: SessionLane,
        identity: LaneExecutionIdentity,
        fields: dict[str, Any],
        turn: TurnHandle,
        writer: FrameWriter,
        signals: TurnSignals,
        progress: _SegmentProgress,
    ) -> _Pumped:
        """Observe the turn from the progress cursor, persisting before each heartbeat, until
        a terminal frame, the frame bound, the end of the stream, or an injected command."""

        bounds = request.segment
        stream: AsyncIterator[LaneFrame] = harness.observe(
            ObserveRequest(**fields, turn=turn, after=progress.cursor, max_frames=bounds.max_frames)
        )
        watch = self._watch(request)
        try:
            async with aclosing(stream) as frames:  # type: ignore[type-var]
                iterator = aiter(frames)
                if watch is None:
                    while True:
                        try:
                            frame = await anext(iterator)
                        except StopAsyncIteration:
                            return _Pumped()
                        ended = await self._take(frame, identity, writer, signals, turn, progress)
                        if ended is not None:
                            return ended
                pending: asyncio.Future[LaneFrame] | None = None
                try:
                    while True:
                        pending = asyncio.ensure_future(anext(iterator))
                        await asyncio.wait({pending, watch}, return_when=asyncio.FIRST_COMPLETED)
                        if not pending.done():
                            return _Pumped(injection=watch.result())
                        done, pending = pending, None
                        try:
                            frame = done.result()
                        except StopAsyncIteration:
                            return _Pumped()
                        ended = await self._take(frame, identity, writer, signals, turn, progress)
                        if ended is not None:
                            return ended
                finally:
                    # The stream is closed only once no read of it is in flight.
                    if pending is not None and not pending.done():
                        pending.cancel()
                        with suppress(asyncio.CancelledError, StopAsyncIteration, Exception):
                            await pending
        finally:
            if watch is not None:
                watch.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await watch

    async def _take(
        self,
        frame: LaneFrame,
        identity: LaneExecutionIdentity,
        writer: FrameWriter,
        signals: TurnSignals,
        turn: TurnHandle,
        progress: _SegmentProgress,
    ) -> _Pumped | None:
        await self._persist(frame, identity, writer, turn, progress)
        # Persisted first, then the heartbeat (the resume hint).
        signals.heartbeat(progress.cursor, progress.persisted)
        if frame.terminal:
            return _Pumped(terminal=frame)
        if progress.max_frames and progress.observed >= progress.max_frames:
            return _Pumped()
        return None

    async def _persist(
        self,
        frame: LaneFrame,
        identity: LaneExecutionIdentity,
        writer: FrameWriter,
        turn: TurnHandle,
        progress: _SegmentProgress,
    ) -> None:
        receipt = await writer.write(
            [self._observation(frame, identity, session_ref=turn.session.native_session_ref)]
        )
        progress.persisted += receipt.new
        progress.duplicates += receipt.duplicate
        progress.observed += 1
        progress.cursor = frame.cursor

    # --- FT-G4 interrupt_and_inject: cancel_and_replace on a Session Lane ---------------------

    def _watch(self, request: LaneTurnRequest) -> asyncio.Task[MailboxEntry] | None:
        """Watch the mailbox for an `interrupt_and_inject` this turn would take."""

        if self._injections is None or self._mailbox is None:
            return None
        node = unit_boundary(request.operation)
        if node is None:
            return None
        injections = self._injections

        async def poll() -> MailboxEntry:
            while True:
                entry = await injections.pending(request.operation, node)
                if entry is not None:
                    return entry
                await asyncio.sleep(injections.settings.poll_seconds)

        return asyncio.create_task(poll())

    def _inject_prefix(self, operation: OperationExecutionRequest) -> str:
        return f"{operation.idempotency_key}:inject:"

    async def _claimed_injection(self, operation: OperationExecutionRequest) -> MailboxEntry | None:
        """An injected command this turn claimed but never replaced (a retried replacement)."""

        if self._mailbox is None:
            return None
        prefix = self._inject_prefix(operation)
        for entry in await self._mailbox.list_entries(
            operation.request_scope, operation.identity.run_id
        ):
            if (
                entry.kind == INJECT_KIND
                and entry.state == MailboxState.DELIVERED
                and (entry.delivery_key or "").startswith(prefix)
            ):
                return entry
        return None

    async def _interrupt(
        self,
        request: LaneTurnRequest,
        harness: SessionLane,
        identity: LaneExecutionIdentity,
        fields: dict[str, Any],
        turn: TurnHandle,
        entry: MailboxEntry,
        writer: FrameWriter,
        signals: TurnSignals,
        progress: _SegmentProgress,
        *,
        cancel_first: bool,
    ) -> TurnHandle | LaneTurnResult:
        """SPEC-07 section 7 `interrupt_and_inject` on a lane without a native steer: cancel
        the running turn, settle its Uncertain Effects (a running tool call without a
        completion parks the unit `in_doubt`), then send a replacement turn carrying the
        injected item; the Delivery Report says `cancel_and_replace`."""

        assert self._mailbox is not None and self._injections is not None
        operation = request.operation
        describe = harness.describe()
        lane_profile = describe.lane_profile
        node = unit_boundary(operation)
        semantics = interrupt_semantics(describe)
        scope, run_id = operation.request_scope, operation.identity.run_id
        if node is None or semantics != "cancel_and_replace":
            await self._mailbox.inject_unsupported(scope, entry, lane_profile=lane_profile)
            return turn  # the running turn is never interrupted
        old_ref = turn.native_turn_ref
        key = f"{self._inject_prefix(operation)}{entry.command_id}"
        delivered = await self._mailbox.deliver(
            scope,
            run_id,
            delivery_key=key,
            family=node[0],
            node_key=node[1],
            iteration_start=False,
            lane_profile=lane_profile,
            kinds=(INJECT_KIND,),
            cancelled_turn_ref=old_ref,
        )
        if not delivered:
            return turn  # another boundary took it first; keep observing this turn
        follow_up = await self._injections.follow_up_segment(operation, delivered, key)
        if isinstance(harness, TurnTextStaging):
            harness.stage_turn(fields["harness_execution_id"], key, follow_up.content)
        replacement = SendTurnRequest(
            **{
                **fields,
                "idempotency_key": (
                    f"{identity.harness_execution_id}:{request.generation}:replace:{old_ref}"
                ),
            },
            session=turn.session,
            turn_no=turn.turn_no + 1,
            instruction_ref=key,
        )

        async def unsettled() -> tuple[str, ...]:
            return await self._open_effects(identity, request.generation, old_ref)

        async def persist(frame: LaneFrame) -> None:
            await self._persist(frame, identity, writer, turn, progress)
            signals.heartbeat(progress.cursor, progress.persisted)

        # The interrupted turn's Uncertain Effects, before the cancel (reported as settled).
        uncertain = await unsettled()
        try:
            replaced = await cancel_and_replace_turn(
                harness,
                cancel=CancelTurnRequest(**fields, turn=turn, reason="interrupt_and_inject"),
                replacement=replacement,
                unsettled=unsettled,
                settings=self._injections.settings,
                after=progress.cursor,
                on_frame=persist,
                cancel_first=cancel_first,
            )
        except InjectionParked as parked:
            await self._mailbox.inject_parked(
                scope,
                run_id,
                delivery_key=key,
                lane_profile=lane_profile,
                cancelled_turn_ref=old_ref,
                pending_effect_ids=parked.pending_effect_ids,
            )
            result = await self._boundary.lane_in_doubt(operation, reason="unsettled_effect_claims")
            return LaneTurnResult(
                done=True,
                cursor=progress.cursor,
                segment_no=request.segment_no,
                frames_persisted=progress.persisted,
                frames_duplicate=progress.duplicates,
                native=NativeRefs(session_ref=turn.session.native_session_ref, turn_ref=old_ref),
                closing_facts=ClosingFacts(
                    native_status="in_doubt", error_code="unsettled_effect_claims"
                ),
                operation_result=result.model_dump(mode="json"),
            )
        new_turn = replaced.handle
        if new_turn.status == "busy" or new_turn.native_turn_ref is None:
            new_turn = await self._send_when_idle(harness, fields, replacement)
        await self._states.record(
            identity.request_scope,
            identity.harness_execution_id,
            LaneExecutionUpdate(
                native_turn_ref=new_turn.native_turn_ref, supersedes_turn_ref=old_ref
            ),
        )
        await self._mailbox.inject_replaced(
            scope,
            run_id,
            delivery_key=key,
            lane_profile=lane_profile,
            delivered_semantics="cancel_and_replace",
            cancelled_turn_ref=old_ref,
            replacement_turn_ref=new_turn.native_turn_ref or f"turn:{new_turn.turn_no}",
            settled_effect_ids=tuple(dict.fromkeys((*uncertain, *replaced.settled_effect_ids))),
            session_ref=turn.session.native_session_ref,
        )
        # The replacement turn is observed from its start (its own provider keys).
        progress.cursor = None
        signals.heartbeat(None, progress.persisted)
        return new_turn

    async def _send_when_idle(
        self, harness: SessionLane, fields: dict[str, Any], request: SendTurnRequest
    ) -> TurnHandle:
        """A cancelled run can take a moment to release the agent (`409 agent_busy`)."""

        session = request.session
        for _attempt in range(30):
            observed = await harness.status(StatusRequest(**fields, session=session))
            if observed.idle or observed.terminal:
                sent = await harness.send_turn(request)
                if sent.status != "busy" and sent.native_turn_ref is not None:
                    return sent
            await asyncio.sleep(0.5)
        raise NativeTurnLost("the agent never became idle for the replacement turn")

    async def _frames_of(
        self, identity: LaneExecutionIdentity, generation: int
    ) -> tuple[ProviderFrame, ...]:
        if self._reader is None:
            return ()
        return await self._reader.frames_for_execution(
            identity.request_scope, identity.harness_execution_id, generation, limit=10_000
        )

    async def _open_effects(
        self, identity: LaneExecutionIdentity, generation: int, turn_ref: str | None
    ) -> tuple[str, ...]:
        return open_tool_calls(await self._frames_of(identity, generation), turn_ref)

    async def _turn_usage(
        self, identity: LaneExecutionIdentity, generation: int, turn_ref: str | None
    ) -> UsageReport:
        """The cancelled turn's usage from its last `TurnEndedUpdate` (`estimated`)."""

        return usage_from_frames(await self._frames_of(identity, generation), turn_ref)

    # --- terminal frame: retried replacement, continuation handover ---------------------------

    async def _after_terminal(
        self,
        request: LaneTurnRequest,
        harness: SessionLane,
        identity: LaneExecutionIdentity,
        fields: dict[str, Any],
        turn: TurnHandle,
        frame: LaneFrame,
        writer: FrameWriter,
        signals: TurnSignals,
        progress: _SegmentProgress,
    ) -> TurnHandle | LaneTurnResult | None:
        """The turn after a terminal frame, if any: the replacement of an injection claimed
        before a lost worker (the cancelled turn is already terminal), or the continuation
        turn on a freshly hydrated agent. None settles the session."""

        facts = harness.closing_facts(turn, frame)
        if facts.native_status == "cancelled":
            claimed = await self._claimed_injection(request.operation)
            if claimed is not None and self._injections is not None:
                return await self._interrupt(
                    request,
                    harness,
                    identity,
                    fields,
                    turn,
                    claimed,
                    writer,
                    signals,
                    progress,
                    cancel_first=False,
                )
        if facts.native_status != "finished" or not isinstance(harness, SessionHandoverLane):
            return None
        handover = await harness.pending_handover(fields["harness_execution_id"])
        if handover is None:
            return None
        return await self._hand_over(
            request, harness, identity, fields, turn, handover, writer, progress
        )

    async def _hand_over(
        self,
        request: LaneTurnRequest,
        harness: SessionLane,
        identity: LaneExecutionIdentity,
        fields: dict[str, Any],
        turn: TurnHandle,
        handover: SessionHandover,
        writer: FrameWriter,
        progress: _SegmentProgress,
    ) -> TurnHandle:
        """SPEC-07 section 7 `request_continuation` (emulated): the next turn runs the fresh
        agent a sealed Continuation Checkpoint hydrated; the native session moves by an
        explicit supersession and the new session's `session_init` frame confirms it."""

        assert isinstance(harness, SessionHandoverLane)
        new_session = handover.session
        await writer.write(
            [
                FrameObservation(
                    provider_key=(
                        f"continuation:{handover.transfer_id}:{new_session.native_session_ref}"
                    )[:512],
                    raw_kind="agent.created",
                    kind=FrameKind.SESSION_INIT,
                    body={
                        "agentId": new_session.native_session_ref,
                        "transfer_id": handover.transfer_id,
                        "source_session_ref": handover.source_session_ref,
                    },
                    native_session_ref=new_session.native_session_ref,
                )
            ]
        )
        sent = await harness.send_turn(
            SendTurnRequest(
                **{
                    **fields,
                    "idempotency_key": (
                        f"{identity.harness_execution_id}:{request.generation}:"
                        f"continuation:{handover.transfer_id}"
                    ),
                },
                session=new_session,
                turn_no=turn.turn_no + 1,
                instruction_ref=handover.instruction_ref,
            )
        )
        if sent.native_turn_ref is None:
            raise NativeTurnLost("the continuation turn was not accepted")
        await self._states.record(
            identity.request_scope,
            identity.harness_execution_id,
            LaneExecutionUpdate(
                native_session_ref=new_session.native_session_ref,
                supersedes_session_ref=turn.session.native_session_ref,
                native_turn_ref=sent.native_turn_ref,
                supersedes_turn_ref=turn.native_turn_ref,
            ),
        )
        await harness.complete_handover(fields["harness_execution_id"], handover.transfer_id)
        progress.cursor = None
        return sent

    # --- FT-G4 wait_then_send: the mailbox at the send that carries it -------------------------

    async def _turn_started(
        self,
        operation: OperationExecutionRequest,
        harness: SessionLane,
        session: SessionHandle,
        turn: TurnHandle,
    ) -> None:
        """The send carrying the delivered entries was accepted: consume them once. Content
        queued while a turn runs waits for the following boundary (`wait_then_send`)."""

        if self._mailbox is None:
            return
        await self._mailbox.turn_started(
            operation.request_scope,
            operation.identity.run_id,
            delivery_key=operation.idempotency_key,
            lane_profile=harness.describe().lane_profile,
            session_ref=session.native_session_ref,
            turn_ref=turn.native_turn_ref,
        )

    async def _turn_not_started(self, operation: OperationExecutionRequest) -> None:
        if self._mailbox is None:
            return
        await self._mailbox.turn_not_started(
            operation.request_scope,
            operation.identity.run_id,
            delivery_key=operation.idempotency_key,
        )

    async def _turns_settled(
        self,
        operation: OperationExecutionRequest,
        lane_profile: str,
        result: OperationExecutionResult,
        turn_ref: str | None,
    ) -> None:
        if self._mailbox is None:
            return
        succeeded = result.status == "completed"
        prefix = self._inject_prefix(operation)
        keys = {operation.idempotency_key}
        try:
            for entry in await self._mailbox.list_entries(
                operation.request_scope, operation.identity.run_id
            ):
                key = entry.delivery_key or ""
                if entry.state == MailboxState.CONSUMED and key.startswith(prefix):
                    keys.add(key)
            for key in sorted(keys):
                await self._mailbox.turn_settled(
                    operation.request_scope,
                    operation.identity.run_id,
                    delivery_key=key,
                    lane_profile=lane_profile,
                    succeeded=succeeded,
                    turn_ref=turn_ref,
                )
        except Exception:
            # Receipts are evidence of an already settled unit (FT-F1 rule).
            _LOGGER.exception("mailbox settlement receipts were not recorded")

    def _observation(
        self,
        frame: LaneFrame,
        identity: LaneExecutionIdentity,
        *,
        session_ref: str | None = None,
    ) -> FrameObservation:
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
            native_session_ref=session_ref,
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
        if turn is not None or cancelled_by_command:
            await self._turns_settled(
                operation, harness.describe().lane_profile, result, native.turn_ref
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
        usage = await self._turn_usage(identity, request.generation, turn_ref)
        cancelled = await self._settle(
            turn_request,
            harness,
            identity,
            turn,
            ClosingFacts(
                native_status="cancelled",
                error_code="cancelled_by_command",
                usage=usage,
                cost_disposition="estimated" if usage.disposition != "unknown" else "unknown",
            ),
            attempt=attempt,
            native=NativeRefs(session_ref=session_ref, turn_ref=turn_ref),
            cancelled_by_command=True,
        )
        return LaneCancelResult(
            receipt=receipt, settled=True, operation_result=cancelled.operation_result
        )


@dataclass
class _SegmentProgress:
    cursor: str | None = None
    max_frames: int = 0
    persisted: int = 0
    duplicates: int = 0
    observed: int = 0


@dataclass(frozen=True)
class _Pumped:
    terminal: LaneFrame | None = None
    injection: MailboxEntry | None = None


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
