from __future__ import annotations

import asyncio
from datetime import timedelta
from functools import partial
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from mission_control.adapters.temporal.search_attributes import (
        ensure_workflow_search_attributes,
        operation_visibility,
        operation_workflow_search_attributes,
        upsert_mission_visibility,
    )
    from mission_control.contracts.identities import mission_operation_id
    from mission_control.domain.execution.contracts import (
        MAX_ACTIVE_ASYNC_CHILDREN,
        MAX_ASYNC_CHILD_ID_LENGTH,
        OperationWorkflowRequest,
        OperationWorkflowResult,
    )
    from mission_control.domain.execution.lane_turns import (
        LANE_COMMAND_SEMANTICS,
        LANE_PAUSE_SEMANTICS,
        LANE_RESUME_SEMANTICS,
        UNSUPPORTED_CONTROL,
        ControlDecision,
        LaneCancelRequest,
        LaneCommandReceipt,
        LaneStatusRequest,
        LaneStatusResult,
        LaneTurnRequest,
        LaneTurnResult,
        NativeRefs,
        default_lane_profile,
        pause_decision_for,
    )
    from mission_control.domain.execution.lanes import LaneResumePoint, LaneSegmentBounds
    from mission_control.domain.execution.usage_admission import (
        LimitWaitLedger,
        ProviderLimitSignal,
        plan_limit_response,
        wait_for_limit_reset,
    )
    from mission_control.domain.programs.search_attributes import (
        MissionPhase,
        phase_for_disposition,
    )

PARK_IN_DOUBT_PATCH = "rrm-004-park-in-doubt-units"
# RRM-008 (REQ-CP-EXEC-008): the cognitive Activity declares a heartbeat timeout, a cancel
# reaches it through the heartbeat (TRY_CANCEL), and every reconciliation after a cancel runs
# `operation.cancel`, which never dispatches or resumes cognition. Histories recorded before
# the patch replay the exact earlier command sequence.
CANCELLATION_SAGA_PATCH = "rrm-008-operation-cancellation-saga"
NUDGE_SNAPSHOT_PATCH = "rrm-008-nudge-snapshot-before-activity"
# FT-G2 (SPEC-07 section 4.2): a segment-driven unit (any `cursor` operation, or one whose
# family passed `segments`) runs as a loop of `lane.turn` segments with the lane cancel path.
# Requests without the new fields never reach the marker, so every earlier history replays.
SEGMENT_LOOP_PATCH = "ft-g2-segment-loop"
# FT-G4 (SPEC-07 section 7): a lane without a mid-run pause applies an accepted pause at the
# run boundary only; the marker is written only when a pause actually holds the next segment,
# so every history recorded without a pause replays unchanged.
LANE_PAUSE_PATCH = "ft-g4-lane-boundary-pause"
# MP-05/MP-06: a `lane.turn` whose create/send a provider limit refused (nothing accepted,
# journaled `declined`) fails `provider_capacity_limit`; the loop waits out the reset on a
# Temporal timer (`plan_limit_response` / `wait_for_limit_reset`), woken by a cancel, then
# runs the segment again, or settles `failed(capacity)` when the reset is past the deadline
# or the waits are spent. The marker is written only when such a failure occurs; before
# it the failure failed the workflow, which every earlier history replays unchanged.
CAPACITY_WAIT_PATCH = "mp05-capacity-wait"
PROVIDER_CAPACITY_LIMIT = "provider_capacity_limit"
TERMINAL_DISPOSITIONS = frozenset({"completed", "cancelled", "failed", "in_doubt"})
# `lane.turn` retries infrastructure failures (a lost worker, a heartbeat timeout) only:
# rejections are non-retryable application errors raised by the activity.
LANE_TURN_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
    maximum_attempts=5,
)
LANE_CONTROL_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=15),
    backoff_coefficient=2.0,
    maximum_attempts=10,
)


def _parks(result: dict[str, object]) -> bool:
    return (
        result.get("status") == "in_doubt" and result.get("failure_code") != "generation_superseded"
    )


def _capacity_signal(error: ActivityError) -> ProviderLimitSignal | None:
    """The provider limit a failed `lane.turn` reported, if that is why it failed."""

    cause = error.cause
    if not isinstance(cause, ApplicationError) or cause.type != PROVIDER_CAPACITY_LIMIT:
        return None
    if not cause.details:
        return None
    try:
        return ProviderLimitSignal.model_validate(cause.details[0])
    except ValueError:
        return None


def superseded_generation(result: OperationWorkflowResult) -> bool:
    """The unit generation was fenced by an accepted `start_new_generation` (no settlement)."""

    return (
        result.disposition == "in_doubt"
        and result.result.get("failure_code") == "generation_superseded"
    )


def _cancel_activity_options(request: OperationWorkflowRequest) -> dict[str, Any]:
    # `operation.cancel` is retried until a live holder's lease is released or expires; it is
    # bounded by its schedule-to-close (REQ-CP-EXEC-008 step 3 timeout meanings).
    return {
        "task_queue": request.activity_task_queue,
        "start_to_close_timeout": timedelta(seconds=request.timeout_seconds),
        "schedule_to_close_timeout": timedelta(
            seconds=2 * request.timeout_seconds + request.heartbeat_timeout_seconds
        ),
        "heartbeat_timeout": timedelta(seconds=request.heartbeat_timeout_seconds),
        "retry_policy": RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_interval=timedelta(seconds=15),
            backoff_coefficient=2.0,
            maximum_attempts=0,
        ),
    }


async def settle_superseded_generation(
    request: OperationWorkflowRequest, result: OperationWorkflowResult
) -> OperationWorkflowResult:
    """RRM-008 re-review (REQ-CP-EXEC-005/008): one `operation.cancel` for a superseded unit.

    A `start_new_generation` accepted before the cancel ends the unit's OperationWorkflow
    with `in_doubt` / `generation_superseded` and an unsettled claim. A cancelling run
    admits no new generation, so the family's cancellation saga runs `operation.cancel` for
    that unit exactly once (from the family's own history; deterministic): the operation
    boundary settles the superseded generation `cancelled` (claim, reservation and effect),
    or reports it still unsettled when a consequential effect awaits the operator. The
    caller gates this behind its own `workflow.patched` marker.
    """

    settled: dict[str, object] = await workflow.execute_activity(
        "operation.cancel",
        request.operation.model_dump(mode="json"),
        result_type=dict,
        **_cancel_activity_options(request),
    )
    status = settled.get("status", "completed")
    return result.model_validate(
        {
            **result.model_dump(mode="python"),
            "disposition": status if status in TERMINAL_DISPOSITIONS else "failed",
            "result": settled,
        }
    )


class _SegmentLoop:
    """Where the segment loop of one workflow run stands (deterministic, in-memory)."""

    def __init__(self, request: OperationWorkflowRequest) -> None:
        self.bounds = request.segments or LaneSegmentBounds()
        self.lane_profile: str = default_lane_profile(request.operation)
        point = request.lane_resume or LaneResumePoint()
        self.phase = point.phase
        self.cursor = point.cursor
        self.segment_no = point.segment_no
        self.turn_no = point.turn_no
        self.native = NativeRefs()
        self.capacity_exhausted = False
        if request.operation.execution_runtime == "cursor":
            self.start_to_close = timedelta(seconds=self.bounds.start_to_close_s)
            self.heartbeat_timeout = timedelta(seconds=self.bounds.heartbeat_timeout_s)
        else:
            # The governed Deep Agents body keeps its own claim-lease deadline and heartbeat.
            self.start_to_close = timedelta(seconds=request.timeout_seconds)
            self.heartbeat_timeout = timedelta(seconds=request.heartbeat_timeout_seconds)

    @classmethod
    def start(cls, request: OperationWorkflowRequest) -> _SegmentLoop:
        return cls(request)


class _CancellationRequested(Exception):
    """The unit's cancellation was requested while its attempt was in flight."""


def _uncancel() -> None:
    # The workflow's own task was cancelled once (a Temporal cancel). Clearing the counter
    # lets the saga keep awaiting its reconciliation activities (as the SDK does itself).
    task = asyncio.current_task()
    if task is not None:
        task.uncancel()


@workflow.defn(name="belllabs.operation.v2")
class OperationWorkflow:
    """Durable technical wrapper for one stable semantic operation attempt.

    Cancellation (RRM-008, REQ-CP-EXEC-008): a cancel reaches the workflow as a Temporal
    cancellation from its family or as the `request_cancel` signal. An in-flight
    `operation.execute` Activity is cancelled (the cancel reaches cognition through its
    heartbeat and the holder settles the unit `cancelled` with its latest durable
    checkpoint); then `operation.cancel` reconciles the unit from durable facts, standing
    down behind a live holder and taking over a lost one, so cancellation survives worker
    loss. A unit parked `in_doubt` is reached by the cancel too: it keeps its incident and
    is settled only by the operator's `reconcile_unit` decision, never speculatively. The
    workflow then completes normally with the unit's disposition; it never fails in place
    of reconciliation.
    """

    @workflow.init
    def __init__(self, request: OperationWorkflowRequest) -> None:
        self._cancel_requested = False
        self._execution_generation = 1
        self._active_async_child_ids: tuple[str, ...] = ()
        self._reconciliation_nudges = 0
        self._nudges_seen = 0
        self._stop_fence_command_id: str | None = None
        # FT-G2 (SPEC-07 section 4.4): command ids applied by this workflow id, carried
        # across continue-as-new; validators reject a repeat within and across runs.
        self._seen_cmds: set[str] = set(request.seen_cmds)
        self._command_receipts: list[dict[str, Any]] = []
        self._cancel_urgency = "normal"
        self._cancel_command_id: str | None = None
        self._terminal = False
        # FT-G4: the lane profile, whether a provider turn is in flight, and a boundary pause.
        self._lane_profile = default_lane_profile(request.operation)
        self._turn_in_flight = False
        self._paused = False
        # MP-05: provider-limit waits spent by this run (not yet carried across
        # continue-as-new; the request field is a proposed contract delta).
        self._limit_ledger = LimitWaitLedger()

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True

    @workflow.signal
    def request_immediate_cancel(self, command_id: str) -> None:
        """FT-F3 immediate branch: the run's Stop Fence for `command_id` is already persisted
        (Kernel Hooks deny new effects from that moment); the in-flight attempt is cancelled
        at once and `operation.cancel` settles the unit. No command differs from a normal
        cancel, so histories with or without this signal replay identically."""

        if not command_id:
            raise ApplicationError(
                "immediate cancel names its fencing command",
                type="invalid_immediate_cancel",
                non_retryable=True,
            )
        if self._stop_fence_command_id is None:
            self._stop_fence_command_id = command_id
        self._cancel_requested = True

    @workflow.update(name="cancel_command")
    async def cancel_command(
        self, command_id: str, urgency: str = "normal", reason: str = "command"
    ) -> dict[str, Any]:
        """FT-G2 cancel Update (SPEC-07 section 4.3): the handler records the command and
        sets the flag; the segment loop does the work (activity cancel, `lane.cancel`,
        `lane.status`, settlement). Its receipt names the lane's delivery semantics."""

        del reason
        self._seen_cmds.add(command_id)
        self._cancel_urgency = urgency
        self._cancel_command_id = command_id
        self._cancel_requested = True
        receipt = LaneCommandReceipt(
            command_id=command_id,
            kind="cancel",
            delivery_semantics=LANE_COMMAND_SEMANTICS["cancel"][self._lane_profile],
        ).model_dump(mode="json")
        self._command_receipts.append(receipt)
        return receipt

    @cancel_command.validator
    def _validate_cancel_command(
        self, command_id: str, urgency: str = "normal", reason: str = "command"
    ) -> None:
        del reason
        if not command_id or len(command_id) > 512:
            raise ValueError("a cancel command names its command id")
        if command_id in self._seen_cmds:
            raise ValueError(f"command {command_id} was already applied to this unit")
        if urgency not in {"normal", "immediate"}:
            raise ValueError("cancel urgency is normal or immediate")
        if self._terminal:
            raise ValueError("the unit is terminal; a cancel has nothing to stop")

    @workflow.update(name="pause_command")
    async def pause_command(self, command_id: str, reason: str = "command") -> dict[str, Any]:
        """FT-G4 (SPEC-07 section 7): a lane that cannot pause mid-run (Cursor) refuses a pause
        while its turn runs (the validator's typed `unsupported_control` rejection) and holds
        an accepted pause at the run boundary: no new segment starts until `resume_command`."""

        del reason
        decision = self._pause_decision()
        self._seen_cmds.add(command_id)
        self._paused = True
        receipt = LaneCommandReceipt(
            command_id=command_id,
            kind="pause",
            delivery_semantics=decision.delivery_semantics,
            detail=decision.detail,
        ).model_dump(mode="json")
        self._command_receipts.append(receipt)
        return receipt

    @pause_command.validator
    def _validate_pause_command(self, command_id: str, reason: str = "command") -> None:
        del reason
        self._validate_command_id(command_id)
        decision = self._pause_decision()
        if not decision.accepted:
            raise ApplicationError(
                decision.detail,
                {"reason_code": decision.reason_code, "delivery_semantics": "unsupported"},
                type=UNSUPPORTED_CONTROL,
                non_retryable=True,
            )

    @workflow.update(name="resume_command")
    async def resume_command(self, command_id: str) -> dict[str, Any]:
        """Release a boundary pause: the next segment may start."""

        self._seen_cmds.add(command_id)
        self._paused = False
        receipt = LaneCommandReceipt(
            command_id=command_id,
            kind="resume",
            delivery_semantics=LANE_RESUME_SEMANTICS[self._lane_profile],
        ).model_dump(mode="json")
        self._command_receipts.append(receipt)
        return receipt

    @resume_command.validator
    def _validate_resume_command(self, command_id: str) -> None:
        self._validate_command_id(command_id)
        if not self._paused:
            raise ValueError("the unit is not paused")

    def _pause_decision(self) -> ControlDecision:
        return pause_decision_for(
            self._lane_profile,
            LANE_PAUSE_SEMANTICS[self._lane_profile],
            turn_in_flight=self._turn_in_flight,
        )

    def _validate_command_id(self, command_id: str) -> None:
        if not command_id or len(command_id) > 512:
            raise ValueError("a command names its command id")
        if command_id in self._seen_cmds:
            raise ValueError(f"command {command_id} was already applied to this unit")
        if self._terminal:
            raise ValueError("the unit is terminal")

    @workflow.query
    def paused(self) -> bool:
        return self._paused

    @workflow.query
    def seen_commands(self) -> list[str]:
        return sorted(self._seen_cmds)

    @workflow.query
    def command_receipts(self) -> list[dict[str, Any]]:
        """The Delivery Report seeds of the commands this unit accepted."""

        return list(self._command_receipts)

    @workflow.query
    def stop_fence_command(self) -> str | None:
        """The immediate cancel that fenced this unit, if any (diagnostic)."""

        return self._stop_fence_command_id

    @workflow.signal
    def unit_reconciliation_recorded(self, decision_ref: str) -> None:
        """A compact wake-up hint after run control accepted `reconcile_unit`.

        It releases nothing by itself (REQ-CP-EXEC-007): the next `operation.execute`
        attempt reads the accepted decision from PostgreSQL authority, and a hint without
        an accepted decision simply parks the unit again.
        """

        del decision_ref
        self._reconciliation_nudges += 1

    @workflow.query
    def execution_generation(self) -> int:
        return self._execution_generation

    @workflow.query
    def cancellation_requested(self) -> bool:
        """Diagnostic only (REQ-CP-EXEC-007); the settlement is the authority."""

        return self._cancel_requested

    @workflow.signal
    def record_async_child(self, child_execution_id: str) -> None:
        if not child_execution_id or len(child_execution_id) > MAX_ASYNC_CHILD_ID_LENGTH:
            raise ApplicationError(
                "async child identity is outside the exact operation workflow bound",
                type="invalid_async_child_identity",
                non_retryable=True,
            )
        if child_execution_id in self._active_async_child_ids:
            return
        if len(self._active_async_child_ids) >= MAX_ACTIVE_ASYNC_CHILDREN:
            raise ApplicationError(
                "operation workflow active async child ceiling exceeded",
                type="active_async_child_ceiling_exceeded",
                non_retryable=True,
            )
        self._active_async_child_ids = (*self._active_async_child_ids, child_execution_id)

    @workflow.query
    def active_async_children(self) -> tuple[str, ...]:
        return self._active_async_child_ids

    @workflow.run
    async def run(self, request: OperationWorkflowRequest) -> OperationWorkflowResult:
        self._execution_generation = request.execution_generation
        # REQ-CP-EXEC-015: a `required` operation carries its run, unit and generation
        # attributes (the family normally starts it with them; nothing is upserted then).
        ensure_workflow_search_attributes(
            request.search_attribute_policy,
            operation_workflow_search_attributes(request),
        )
        pre_start_signal_ids = self._active_async_child_ids
        merged_ids = list(request.active_async_child_ids)
        seen_ids = set(merged_ids)
        for child_execution_id in pre_start_signal_ids:
            if child_execution_id in seen_ids:
                continue
            if len(merged_ids) >= MAX_ACTIVE_ASYNC_CHILDREN:
                raise ApplicationError(
                    "operation workflow active async child ceiling exceeded during initialization",
                    type="active_async_child_ceiling_exceeded",
                    non_retryable=True,
                )
            merged_ids.append(child_execution_id)
            seen_ids.add(child_execution_id)
        self._active_async_child_ids = tuple(merged_ids)
        if workflow.patched(CANCELLATION_SAGA_PATCH):
            # FT-G7: `mc_run_id`, `mc_lane` and `mc_phase` at the first segment boundary.
            self._visibility(request, "executing")
            if request.segment_driven and workflow.patched(SEGMENT_LOOP_PATCH):
                result = await self._run_segments(request)
            else:
                result = await self._run_governed(request)
        else:
            legacy = await self._run_legacy(request)
            if legacy is None:
                return self._result(request, "cancelled", None)
            result = legacy
        status = result.get("status", "completed")
        disposition = status if status in TERMINAL_DISPOSITIONS else "failed"
        self._terminal = True
        self._visibility(request, phase_for_disposition(str(disposition)))
        if request.segment_driven:
            # Handlers mutate state only; none may be cut off by completion (SPEC-07 4.4).
            await workflow.wait_condition(workflow.all_handlers_finished)
        return self._result(request, str(disposition), result)

    @staticmethod
    def _visibility(request: OperationWorkflowRequest, phase: MissionPhase) -> None:
        """Segment-boundary listing attributes; no command under `disabled` or on replay of
        a history recorded before the FT-G7 marker (never per frame)."""

        upsert_mission_visibility(
            request.search_attribute_policy, operation_visibility(request, phase)
        )

    def _result(
        self, request: OperationWorkflowRequest, disposition: str, result: dict[str, object] | None
    ) -> OperationWorkflowResult:
        return OperationWorkflowResult(
            semantic_attempt_id=request.semantic_attempt_id,
            execution_generation=request.execution_generation,
            disposition=disposition,
            result=result or {},
            message_cursor=request.message_cursor,
            effect_frontier=request.effect_frontier,
            active_async_child_ids=self._active_async_child_ids,
        )

    # --- FT-G2 segment loop (SPEC-07 sections 4.2 to 4.4) -------------------------------------

    async def _run_segments(self, request: OperationWorkflowRequest) -> dict[str, object]:
        """`lane.turn` segments until the turn settles; the cancel path once a cancel lands.

        An `in_doubt` settlement parks exactly like the governed path: the unit waits for an
        operator `reconcile_unit` hint (then the next segment classifies again) or a cancel
        (then the lane cancel path runs again); nothing is re-sent speculatively.
        """

        loop = _SegmentLoop.start(request)
        while True:
            if self._cancel_requested:
                outcome: dict[str, object] | None = await self._lane_cancel_and_settle(
                    request, loop
                )
            else:
                outcome = await self._next_segment(request, loop)
                if outcome is None:
                    continue
            assert outcome is not None
            if not _parks(outcome):
                return outcome
            self._visibility(request, "in_doubt")
            self._turn_in_flight = False
            cancelling = self._cancel_requested
            seen = self._reconciliation_nudges

            def woken(cancelling: bool = cancelling, seen: int = seen) -> bool:
                return self._reconciliation_nudges > seen or (
                    self._cancel_requested and not cancelling
                )

            try:
                await workflow.wait_condition(woken)
            except asyncio.CancelledError:
                self._cancel_requested = True
                _uncancel()
            self._nudges_seen = self._reconciliation_nudges
            loop.phase = "start"

    async def _next_segment(
        self, request: OperationWorkflowRequest, loop: _SegmentLoop
    ) -> dict[str, object] | None:
        """One `lane.turn` segment: its settled result, or None to continue the loop."""

        bounds = loop.bounds
        if loop.segment_no > bounds.max_segments:
            # A turn that never closes within its segment budget is not guessed at.
            return await self._lane_in_doubt(request, loop)
        if self._paused and loop.phase == "start" and workflow.patched(LANE_PAUSE_PATCH):
            # FT-G4: the run boundary; nothing is sent while the unit is paused.
            await workflow.wait_condition(lambda: not self._paused or self._cancel_requested)
            if self._cancel_requested:
                return None
        turn = LaneTurnRequest(
            operation=request.operation,
            lane_profile=loop.lane_profile,
            generation=request.execution_generation,
            phase=loop.phase,
            cursor=loop.cursor,
            turn_no=loop.turn_no,
            segment_no=loop.segment_no,
            segment=bounds,
            capacity_exhausted=loop.capacity_exhausted,
        )
        handle = workflow.start_activity(
            "lane.turn",
            turn.model_dump(mode="json"),
            result_type=dict,
            task_queue=request.activity_task_queue,
            start_to_close_timeout=loop.start_to_close,
            heartbeat_timeout=loop.heartbeat_timeout,
            retry_policy=LANE_TURN_RETRY,
            cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )
        self._turn_in_flight = True
        try:
            await workflow.wait_condition(partial(self._turn_ended_or_cancelled, handle))
        except asyncio.CancelledError:
            self._cancel_requested = True
            _uncancel()
        if self._cancel_requested and not handle.done():
            # The activity's cleanup (the provider cancel, when this is a requested cancel)
            # completes before the saga continues (WAIT_CANCELLATION_COMPLETED).
            handle.cancel()
            try:
                await handle
            except (Exception, asyncio.CancelledError):
                _uncancel()
            return None
        try:
            raw = handle.result()
        except ActivityError as error:
            signal = _capacity_signal(error)
            if signal is None or not workflow.patched(CAPACITY_WAIT_PATCH):
                raise
            self._turn_in_flight = False
            await self._wait_out_capacity(loop, signal)
            return None
        result = LaneTurnResult.model_validate(raw)
        if result.native.session_ref is not None:
            loop.native = result.native
        # Between segments a sent turn is still running at the provider.
        self._turn_in_flight = not (result.done or result.busy)
        if result.done:
            assert result.operation_result is not None
            return dict(result.operation_result)
        if result.busy:
            # `wait_then_send`: nothing was sent; wait for the agent to be idle.
            loop.capacity_exhausted = not await self._wait_until_idle(request, loop)
            return None
        loop.cursor, loop.phase = result.cursor, "resume"
        loop.segment_no += 1
        self._visibility(request, "executing")
        await self._deliver_mailbox_at_boundary(request)
        if workflow.info().is_continue_as_new_suggested():
            await workflow.wait_condition(workflow.all_handlers_finished)
            workflow.continue_as_new(
                request.model_copy(
                    update={
                        "seen_cmds": tuple(sorted(self._seen_cmds)),
                        "lane_resume": LaneResumePoint(
                            phase=loop.phase,
                            cursor=loop.cursor,
                            segment_no=loop.segment_no,
                            turn_no=loop.turn_no,
                        ),
                        "active_async_child_ids": self._active_async_child_ids,
                    }
                )
            )
        return None

    def _turn_ended_or_cancelled(self, handle: workflow.ActivityHandle[Any]) -> bool:
        return handle.done() or self._cancel_requested

    async def _wait_out_capacity(self, loop: _SegmentLoop, signal: ProviderLimitSignal) -> None:
        """Wait for a provider limit reset on a Temporal timer, bounded by the unit's
        segment budget; a rejection or a spent wait settles `failed(capacity)` next."""

        bounds = loop.bounds
        deadline = workflow.info().workflow_start_time + timedelta(
            seconds=bounds.max_segments * bounds.start_to_close_s
        )
        decision = plan_limit_response(
            signal, now=workflow.now(), deadline=deadline, ledger=self._limit_ledger
        )
        if decision.disposition == "reject":
            loop.capacity_exhausted = True
            return
        if decision.disposition != "wait":
            return

        async def timer(predicate: Any, timeout_s: float) -> bool:
            try:
                await workflow.wait_condition(predicate, timeout=timedelta(seconds=timeout_s))
            except TimeoutError:
                return False
            return True

        outcome = await wait_for_limit_reset(
            decision,
            now=workflow.now,
            deadline=deadline,
            wait=timer,
            cancelled=lambda: self._cancel_requested,
            ledger=self._limit_ledger,
        )
        self._limit_ledger = outcome.ledger
        if outcome.outcome == "deadline_reached":
            loop.capacity_exhausted = True

    async def _deliver_mailbox_at_boundary(self, request: OperationWorkflowRequest) -> None:
        """Segment boundary hook for `next_turn` mailbox items (FT-F1 fills it). The mailbox
        stays in PostgreSQL; a continue-as-new never loses an instruction."""

        del request

    @staticmethod
    def _lane_payload(request: OperationWorkflowRequest, loop: _SegmentLoop) -> dict[str, object]:
        return {
            "operation": request.operation,
            "lane_profile": loop.lane_profile,
            "generation": request.execution_generation,
            "native": loop.native,
        }

    async def _lane_status(
        self, request: OperationWorkflowRequest, loop: _SegmentLoop
    ) -> LaneStatusResult:
        status: dict[str, object] = await workflow.execute_activity(
            "lane.status",
            LaneStatusRequest.model_validate(self._lane_payload(request, loop)).model_dump(
                mode="json"
            ),
            result_type=dict,
            task_queue=request.activity_task_queue,
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=LANE_CONTROL_RETRY,
        )
        return LaneStatusResult.model_validate(status)

    async def _lane_cancel(
        self,
        request: OperationWorkflowRequest,
        loop: _SegmentLoop,
        *,
        in_doubt: bool = False,
    ) -> dict[str, object]:
        cancel = LaneCancelRequest.model_validate(
            {
                **self._lane_payload(request, loop),
                "reason": "command",
                "urgency": self._cancel_urgency,
                "command_id": self._cancel_command_id or self._stop_fence_command_id,
                "in_doubt": in_doubt,
            }
        )
        outcome: dict[str, object] = await workflow.execute_activity(
            "lane.cancel",
            cancel.model_dump(mode="json"),
            result_type=dict,
            task_queue=request.activity_task_queue,
            start_to_close_timeout=timedelta(seconds=120),
            retry_policy=LANE_CONTROL_RETRY,
        )
        return outcome

    async def _lane_cancel_and_settle(
        self, request: OperationWorkflowRequest, loop: _SegmentLoop
    ) -> dict[str, object]:
        """`lane.cancel` (idempotent), then `lane.status` until the provider is terminal and
        the unit settles `cancelled`; after the poll bound the unit is `in_doubt`."""

        self._visibility(request, "cancelling")
        bounds = loop.bounds
        polls = 0
        while True:
            outcome = await self._lane_cancel(request, loop)
            settled = outcome.get("operation_result")
            if isinstance(settled, dict):
                return dict(settled)
            while True:
                status = await self._lane_status(request, loop)
                if status.operation_result is not None:
                    return dict(status.operation_result)
                if status.terminal:
                    break
                polls += 1
                if polls >= bounds.status_poll_limit:
                    return await self._lane_in_doubt(request, loop)
                await workflow.sleep(
                    timedelta(seconds=bounds.status_poll_interval_s * min(2 ** (polls - 1), 8))
                )

    async def _lane_in_doubt(
        self, request: OperationWorkflowRequest, loop: _SegmentLoop
    ) -> dict[str, object]:
        self._visibility(request, "in_doubt")
        outcome = await self._lane_cancel(request, loop, in_doubt=True)
        result = outcome.get("operation_result")
        if not isinstance(result, dict):
            raise ApplicationError(
                "lane.cancel did not report the in_doubt disposition",
                type="lane_in_doubt_unrecorded",
                non_retryable=True,
            )
        return dict(result)

    async def _wait_until_idle(self, request: OperationWorkflowRequest, loop: _SegmentLoop) -> bool:
        """Poll `lane.status` until the agent is idle within `busy_wait_s` (False after)."""

        bounds = loop.bounds
        deadline = workflow.now() + timedelta(seconds=bounds.busy_wait_s)
        while workflow.now() < deadline and not self._cancel_requested:
            status = await self._lane_status(request, loop)
            if status.idle:
                return True
            await workflow.sleep(timedelta(seconds=bounds.status_poll_interval_s))
        return self._cancel_requested

    # --- RRM-008 governed path ----------------------------------------------------------------

    async def _run_governed(self, request: OperationWorkflowRequest) -> dict[str, object]:
        cancel_mode = self._cancel_requested
        while True:
            # Review F2: snapshot the hint counter before the Activity, as the legacy path
            # does, so a `reconcile_unit` hint that lands while it runs is not lost.
            snapshot_first = workflow.patched(NUDGE_SNAPSHOT_PATCH)
            if snapshot_first:
                self._nudges_seen = self._reconciliation_nudges
            try:
                if cancel_mode:
                    result = await self._cancel_operation(request)
                else:
                    result = await self._execute_cancellable(request)
            except _CancellationRequested:
                cancel_mode = True
                self._visibility(request, "cancelling")
                continue
            if not _parks(result):
                return result
            self._visibility(request, "in_doubt")
            # REQ-CP-RUN-007 / REQ-CP-DA-018: an `in_doubt` unit keeps its claim unsettled
            # and waits durably for operator reconciliation. A cancel reaches it here; it
            # is never re-executed speculatively, and after a cancel every wake-up runs the
            # cancellation settlement, which applies only an accepted decision.
            if not snapshot_first:
                self._nudges_seen = self._reconciliation_nudges

            def woken(cancelling: bool = cancel_mode) -> bool:
                return self._reconciliation_nudges > self._nudges_seen or (
                    self._cancel_requested and not cancelling
                )

            try:
                await workflow.wait_condition(woken)
            except asyncio.CancelledError:
                self._cancel_requested = True
                _uncancel()
            if self._cancel_requested:
                cancel_mode = True

    async def _execute_cancellable(self, request: OperationWorkflowRequest) -> dict[str, object]:
        """One `operation.execute` attempt that a cancel can interrupt in flight."""

        handle = workflow.start_activity(
            "operation.execute",
            request.operation.model_dump(mode="json"),
            result_type=dict,
            task_queue=request.activity_task_queue,
            # Timeout meanings: start-to-close is the holder's maximum (and its claim lease
            # deadline); heartbeat detects a lost worker and carries the cancel; retries
            # recover the unit (REQ-CP-DA-018) as long as no cancel was requested.
            start_to_close_timeout=timedelta(seconds=request.timeout_seconds),
            heartbeat_timeout=timedelta(seconds=request.heartbeat_timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=3),
            cancellation_type=workflow.ActivityCancellationType.TRY_CANCEL,
        )
        try:
            await workflow.wait_condition(lambda: handle.done() or self._cancel_requested)
        except asyncio.CancelledError:
            self._cancel_requested = True
            _uncancel()
        if self._cancel_requested and not handle.done():
            # The cancel reaches the Activity through its heartbeat; the holder settles the
            # unit `cancelled` itself when it can. Whatever the attempt ends with,
            # `operation.cancel` reconciles the unit from durable facts.
            handle.cancel()
            try:
                await handle
            except (Exception, asyncio.CancelledError):
                _uncancel()
            raise _CancellationRequested
        return handle.result()

    async def _cancel_operation(self, request: OperationWorkflowRequest) -> dict[str, object]:
        """`operation.cancel`: retried until a live holder's lease is released or expires."""

        handle = workflow.start_activity(
            "operation.cancel",
            request.operation.model_dump(mode="json"),
            result_type=dict,
            **_cancel_activity_options(request),
        )
        while True:
            try:
                return await asyncio.shield(handle)
            except asyncio.CancelledError:
                # A repeated cancel of this workflow must not cancel the reconciliation.
                self._cancel_requested = True
                _uncancel()
                if handle.done():
                    return handle.result()

    # --- pre-RRM-008 path (replay of recorded histories only) ---------------------------------

    async def _run_legacy(self, request: OperationWorkflowRequest) -> dict[str, object] | None:
        if self._cancel_requested:
            return None
        self._nudges_seen = self._reconciliation_nudges
        result = await self._execute_operation(request)
        if _parks(result) and workflow.patched(PARK_IN_DOUBT_PATCH):
            while _parks(result):
                await workflow.wait_condition(
                    lambda: self._reconciliation_nudges > self._nudges_seen
                )
                self._nudges_seen = self._reconciliation_nudges
                result = await self._execute_operation(request)
        return result

    async def _execute_operation(self, request: OperationWorkflowRequest) -> dict[str, object]:
        result: dict[str, object] = await workflow.execute_activity(
            "operation.execute",
            request.operation.model_dump(mode="json"),
            result_type=dict,
            task_queue=request.activity_task_queue,
            start_to_close_timeout=timedelta(seconds=request.timeout_seconds),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )
        return result


__all__: tuple[str, ...] = (
    "CANCELLATION_SAGA_PATCH",
    "CAPACITY_WAIT_PATCH",
    "LANE_PAUSE_PATCH",
    "SEGMENT_LOOP_PATCH",
    "OperationWorkflow",
    "settle_superseded_generation",
    "superseded_generation",
)


@workflow.defn(name="mc.operation.v1")
class MissionOperationWorkflow(OperationWorkflow):
    @workflow.run
    async def run(self, request: OperationWorkflowRequest) -> OperationWorkflowResult:
        try:
            expected = mission_operation_id(
                request.operation.request_scope,
                request.operation.identity.run_id,
                request.semantic_attempt_id,
            )
            if workflow.info().workflow_id != expected:
                raise ValueError("operation workflow identity differs from admitted scope")
        except ValueError as error:
            raise ApplicationError(
                str(error), type="InvalidMissionBinding", non_retryable=True
            ) from error
        return await super().run(request)
