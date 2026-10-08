"""Fact derivation from closing frames, and the projector that applies them (SPEC-03, C2).

`derive(frames, context)` is pure and deterministic: it reads only closing frames of the
execution's current generation, in arrival order, and returns typed `FrameFact`s
following the SPEC-03 lifecycle synthesis table:

Closing frame -> fact -> mission event:

- first closing frame of a native session -> session started -> `session.started`
  (plus `activation.phase_changed{executing}` on Cursor lanes);
- first closing frame of a turn -> turn n opened -> `session.turn_started`;
- `tool_call_completed` / `tool_call_failed` -> tool effect settled or failed, deduplicated
  by `tool_call_ref` -> `tool_call.completed{status}`;
- `hook_result{decision: deny}` naming a tool call -> effect denied before claim ->
  `tool_call.completed{status: denied}`;
- `session_state{requires_action | awaiting_human ...}` -> approval requested ->
  `activation.phase_changed{awaiting_human}`; `approval_resolved` -> `{executing}`;
- `after_compaction` -> compaction observed -> `session.compaction_observed`;
- `usage` within a turn, then `turn_ended` -> turn n closed with usage dispositions ->
  `session.turn_completed`; a `usage` after the turn closed (Cursor `charged_cents`) ->
  `session.usage_settled`;
- `run_result` -> execution outcome per workflow-types/05 section 3.5 ->
  `attempt.completed` (or `activation.phase_changed{follow_up_turn}`), and `session.ended`;
- `error{unknown_state: true}` -> unit in doubt (existing reconciliation path) ->
  `activation.lifecycle_changed{waiting, blocker: in_doubt}`.

Turn ordinals come from closing frames alone, so lanes without native turn ids (Cursor)
and lanes with them (Deep Agents invocations) derive identically. Deltas, starts, status
and heartbeats never reach this module. `FrameFactProjector` reads a harness execution's
closing frames, derives, and applies the facts not yet applied through the run-control
reducer's `apply_frame_facts` action (idempotent by the run's frame-fact cursor).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from uuid import UUID

from mission_control.application.frames.sink import FrameReader
from mission_control.domain.frames.body import frame_body_object
from mission_control.domain.frames.contracts import (
    FrameKind,
    HarnessExecutionHandle,
    LaneProfile,
    ProviderFrame,
)
from mission_control.domain.frames.facts import (
    ApprovalRequestedFact,
    ApprovalResolvedFact,
    AttemptOutcome,
    CompactionObservedFact,
    ExecutionOutcomeFact,
    FailureClass,
    FrameFact,
    MissingOutputPolicy,
    SessionEndedFact,
    SessionStartedFact,
    ToolEffectFact,
    TurnCompletedFact,
    TurnStartedFact,
    UnitInDoubtFact,
    UsageSettledFact,
)
from mission_control.domain.frames.usage import usage_report
from mission_control.domain.policies.contracts import (
    MAX_FRAME_FACTS_PER_ACTION,
    ActorContext,
    ApplyFrameFactsAction,
    CommandResult,
    CommandStatus,
    LifecycleCommand,
    RunProjection,
)

logger = logging.getLogger(__name__)

DIGEST_PREFIX = "sha256:"
AWAITING_STATES = frozenset(
    {
        "requires_action",
        "awaiting_human",
        "awaiting_approval",
        "waiting_on_approval",
        "waiting_on_user_input",
    }
)
CAPACITY_CODES = frozenset(
    {"agent_busy", "409", "429", "rate_limited", "session_token_limit_reached"}
)
_FINISHED = frozenset({"finished", "completed", "succeeded", "success", "end_turn"})
_FAILED = frozenset({"error", "failed", "failure"})
_EXPIRED = frozenset({"expired", "timeout", "timed_out"})
_CANCELLED = frozenset({"cancelled", "canceled", "interrupted", "aborted"})
_TURN_NEUTRAL = frozenset({FrameKind.RUN_RESULT, FrameKind.ERROR})


@dataclass(frozen=True)
class CompletionEvidence:
    """The Completion Candidate the attempt registered (workflow-types/05 section 3.4)."""

    declared_outputs: frozenset[str] = frozenset()
    registered_outputs: frozenset[str] = frozenset()

    @property
    def covers_declared_outputs(self) -> bool:
        return self.declared_outputs <= self.registered_outputs


CompletionPolicy = Literal["candidate_required", "deferred"]


@dataclass(frozen=True)
class DeriveContext:
    """What derivation needs beyond the frames.

    `completion_policy = "deferred"` leaves a provider `finished` without an attempt outcome
    because another authority (the Deep Agents operation settlement) decides it; errors,
    expiry and cancellation still map per section 3.5.
    """

    lane: LaneProfile
    current_generation: int
    completion: CompletionEvidence | None = None
    completion_policy: CompletionPolicy = "candidate_required"
    missing_output_policy: MissingOutputPolicy = "follow_up_turn"
    cancel_command_admitted: bool = False


def _digest(value: Any) -> str | None:
    return (
        value
        if isinstance(value, str) and value.startswith(DIGEST_PREFIX) and len(value) == 71
        else None
    )


def _status(body: Mapping[str, Any]) -> str:
    for key in ("status", "outcome", "result"):
        value = body.get(key)
        if isinstance(value, str) and value:
            return value.lower()
    return "unknown"


def _error_code(body: Mapping[str, Any]) -> str:
    for key in ("error_code", "code", "error_type", "terminal_reason"):
        value = body.get(key)
        if isinstance(value, str | int) and str(value):
            return str(value).lower()
    return ""


def map_execution_outcome(
    provider_status: str,
    *,
    error_code: str,
    context: DeriveContext,
) -> tuple[AttemptOutcome | None, FailureClass | None, bool, MissingOutputPolicy | None]:
    """workflow-types/05 section 3.5: (outcome, failure class, missing outputs, policy).

    Returns outcome None for a `finished` whose outputs are not covered under
    `follow_up_turn` (or whose completion is deferred) and for an unknown status.
    """

    status = provider_status.lower()
    if status in _FINISHED:
        if context.completion_policy == "deferred":
            return None, None, False, None
        covered = context.completion is not None and context.completion.covers_declared_outputs
        if covered:
            return AttemptOutcome.SUCCEEDED, None, False, None
        if context.missing_output_policy == "not_accepted":
            return AttemptOutcome.FAILED, FailureClass.OUTPUTS_MISSING, True, "not_accepted"
        return None, None, True, "follow_up_turn"
    if status in _EXPIRED:
        return AttemptOutcome.FAILED, FailureClass.TIMEOUT, False, None
    if status in _CANCELLED:
        if context.cancel_command_admitted:
            return AttemptOutcome.CANCELLED, FailureClass.CANCELLED_BY_COMMAND, False, None
        return AttemptOutcome.FAILED, FailureClass.PROVIDER_ERROR, False, None
    if status in _FAILED or status in CAPACITY_CODES:
        if error_code in CAPACITY_CODES or status in CAPACITY_CODES:
            return AttemptOutcome.FAILED, FailureClass.CAPACITY, False, None
        return AttemptOutcome.FAILED, FailureClass.PROVIDER_ERROR, False, None
    return None, None, False, None


@dataclass
class _ExecutionState:
    sessions: set[str] = field(default_factory=set)
    open_turn: int | None = None
    closed_turns: int = 0
    turn_usage: list[tuple[ProviderFrame, dict[str, Any]]] = field(default_factory=list)
    settled_tools: set[str] = field(default_factory=set)
    run_result_seen: bool = False


def _evidence(frame: ProviderFrame) -> dict[str, Any]:
    return {
        "frame_id": str(frame.frame_id),
        "arrival_ordinal": frame.arrival_ordinal,
        "harness_execution_id": str(frame.harness_execution_id),
        "generation": frame.generation,
        "native_session_ref": frame.native_session_ref,
        "native_turn_ref": frame.native_turn_ref,
        "subordinate_ref": frame.subordinate_ref,
        "observed_at": frame.observed_at,
    }


def _tool_ref(frame: ProviderFrame, body: Mapping[str, Any]) -> str | None:
    if frame.tool_call_ref:
        return frame.tool_call_ref
    for key in ("tool_call_id", "callId", "call_id", "tool_use_id"):
        value = body.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _exit_code(body: Mapping[str, Any]) -> int | None:
    for key in ("exit_code", "exitCode"):
        value = body.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def derive(frames: Sequence[ProviderFrame], context: DeriveContext) -> tuple[FrameFact, ...]:
    """Typed facts from closing frames only; deterministic for the same input."""

    closing = sorted(
        (
            frame
            for frame in frames
            if frame.closing and frame.generation == context.current_generation
        ),
        key=lambda frame: (str(frame.harness_execution_id), frame.arrival_ordinal),
    )
    states: dict[UUID, _ExecutionState] = {}
    facts: list[FrameFact] = []
    for frame in closing:
        state = states.setdefault(frame.harness_execution_id, _ExecutionState())
        body = frame_body_object(frame.body_excerpt, frame.body_bytes)
        evidence = _evidence(frame)
        if frame.native_session_ref not in state.sessions:
            state.sessions.add(frame.native_session_ref)
            facts.append(SessionStartedFact(**evidence, lane_profile=context.lane))
        if frame.kind == FrameKind.USAGE and state.open_turn is None and state.closed_turns:
            facts.append(
                UsageSettledFact(
                    **evidence,
                    turn_ordinal=state.closed_turns,
                    usage=usage_report(context.lane, [(str(frame.frame_id), body)]),
                )
            )
            continue
        if frame.kind not in _TURN_NEUTRAL and state.open_turn is None:
            state.open_turn = state.closed_turns + 1
            facts.append(TurnStartedFact(**evidence, turn_ordinal=state.open_turn))
        turn = state.open_turn or max(state.closed_turns, 1)
        if frame.kind == FrameKind.USAGE:
            state.turn_usage.append((frame, body))
        elif frame.kind in (FrameKind.TOOL_CALL_COMPLETED, FrameKind.TOOL_CALL_FAILED):
            ref = _tool_ref(frame, body)
            if ref is None or ref in state.settled_tools:
                continue
            state.settled_tools.add(ref)
            name = body.get("name")
            facts.append(
                ToolEffectFact(
                    **evidence,
                    turn_ordinal=turn,
                    tool_call_ref=ref,
                    name=name if isinstance(name, str) else None,
                    status=(
                        "completed" if frame.kind == FrameKind.TOOL_CALL_COMPLETED else "failed"
                    ),
                    args_digest=_digest(body.get("args_digest")),
                    result_digest=_digest(body.get("result_digest")) or frame.body_digest,
                    exit_code=_exit_code(body),
                )
            )
        elif frame.kind == FrameKind.HOOK_RESULT:
            decision = str(body.get("decision") or body.get("permission") or "").lower()
            ref = _tool_ref(frame, body)
            if decision == "deny" and ref is not None and ref not in state.settled_tools:
                state.settled_tools.add(ref)
                name = body.get("tool_name") or body.get("name")
                facts.append(
                    ToolEffectFact(
                        **evidence,
                        turn_ordinal=turn,
                        tool_call_ref=ref,
                        name=name if isinstance(name, str) else None,
                        status="denied",
                        args_digest=_digest(body.get("input_digest")),
                        result_digest=frame.body_digest,
                    )
                )
        elif frame.kind == FrameKind.SESSION_STATE:
            state_name = str(body.get("state") or body.get("status") or "").lower()
            if state_name in AWAITING_STATES:
                request = body.get("request_id") or body.get("request_ref")
                facts.append(
                    ApprovalRequestedFact(**evidence, request_ref=str(request) if request else None)
                )
        elif frame.kind == FrameKind.APPROVAL_RESOLVED:
            request = body.get("request_id") or body.get("request_ref")
            resolution = body.get("decision")
            facts.append(
                ApprovalResolvedFact(
                    **evidence,
                    request_ref=str(request) if request else None,
                    decision=str(resolution) if resolution else None,
                )
            )
        elif frame.kind == FrameKind.AFTER_COMPACTION:
            facts.append(
                CompactionObservedFact(
                    **evidence,
                    summary_digest=_digest(body.get("summary_digest")) or frame.body_digest,
                )
            )
        elif frame.kind == FrameKind.TURN_ENDED:
            bodies = state.turn_usage or [(frame, body)]
            report = usage_report(
                context.lane, [(str(item.frame_id), data) for item, data in bodies]
            )
            stop = body.get("stop_reason")
            summary = body.get("result_summary_ref")
            facts.append(
                TurnCompletedFact(
                    **evidence,
                    turn_ordinal=turn,
                    usage=report,
                    usage_frame_ids=tuple(str(item.frame_id) for item, _data in state.turn_usage),
                    stop_reason=str(stop) if stop else None,
                    result_summary_ref=str(summary) if summary else None,
                )
            )
            state.closed_turns = turn
            state.open_turn = None
            state.turn_usage = []
        elif frame.kind == FrameKind.RUN_RESULT:
            if state.run_result_seen:
                continue
            state.run_result_seen = True
            provider_status = _status(body)
            outcome, failure, missing, policy = map_execution_outcome(
                provider_status, error_code=_error_code(body), context=context
            )
            if outcome is not None or missing:
                facts.append(
                    ExecutionOutcomeFact(
                        **evidence,
                        provider_status=provider_status,
                        outcome=outcome,
                        failure_class=failure,
                        missing_outputs=missing,
                        missing_output_policy=policy,
                    )
                )
            elif provider_status not in _FINISHED:
                # A status the 05 section 3.5 table does not name: never guessed, in doubt.
                facts.append(UnitInDoubtFact(**evidence, reason=f"run_result:{provider_status}"))
            if context.lane == LaneProfile.DEEP_AGENTS or body.get("session_ended") is True:
                facts.append(SessionEndedFact(**evidence))
        elif frame.kind == FrameKind.ERROR:
            if body.get("unknown_state") is True:
                reason = body.get("reason") or body.get("error_type")
                facts.append(UnitInDoubtFact(**evidence, reason=str(reason) if reason else None))
    return tuple(facts)


# --- Projector ------------------------------------------------------------------------------


class FrameFactRunControl(Protocol):
    async def execute(self, command: LifecycleCommand) -> CommandResult: ...

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...


@dataclass(frozen=True)
class FrameFactTarget:
    """The harness execution whose closing frames are projected into run state."""

    request_scope: str
    run_key: str
    harness_execution_id: UUID
    activation_id: UUID
    attempt_no: int
    lane: LaneProfile

    @classmethod
    def from_handle(cls, handle: HarnessExecutionHandle, *, run_key: str) -> FrameFactTarget:
        return cls(
            request_scope=handle.request_scope,
            run_key=run_key,
            harness_execution_id=handle.harness_execution_id,
            activation_id=handle.activation_id,
            attempt_no=handle.attempt_no,
            lane=handle.lane_profile,
        )


@dataclass(frozen=True)
class FrameFactProjection:
    generation: int | None
    derived: int
    applied: int
    through_ordinal: int
    rejected_reason: str | None = None


class FrameFactsRejected(RuntimeError):
    def __init__(self, reason_code: str, reason: str) -> None:
        super().__init__(reason)
        self.reason_code = reason_code


def _batches(facts: Sequence[FrameFact], size: int) -> list[list[FrameFact]]:
    """Batches that never split the facts of one frame (the cursor is per ordinal)."""

    batches: list[list[FrameFact]] = []
    current: list[FrameFact] = []
    index = 0
    while index < len(facts):
        ordinal = facts[index].arrival_ordinal
        group = []
        while index < len(facts) and facts[index].arrival_ordinal == ordinal:
            group.append(facts[index])
            index += 1
        if current and len(current) + len(group) > size:
            batches.append(current)
            current = []
        current.extend(group)
    if current:
        batches.append(current)
    return batches


def _utc_now() -> datetime:
    return datetime.now(UTC)


class FrameFactProjector:
    """Derive facts from an execution's closing frames and apply the unapplied ones."""

    def __init__(
        self,
        frames: FrameReader,
        run_control: FrameFactRunControl,
        *,
        actor: ActorContext,
        idempotency_issuer: str = "mission-control-frame-facts",
        clock: Callable[[], datetime] = _utc_now,
        batch_size: int = 64,
        max_frames: int = 10_000,
    ) -> None:
        if not 1 <= batch_size <= MAX_FRAME_FACTS_PER_ACTION:
            raise ValueError("frame fact batch size is out of bounds")
        self._frames = frames
        self._run_control = run_control
        self._actor = actor
        self._issuer = idempotency_issuer
        self._clock = clock
        self._batch_size = batch_size
        self._max_frames = max_frames

    async def project(
        self,
        target: FrameFactTarget,
        *,
        completion: CompletionEvidence | None = None,
        completion_policy: CompletionPolicy = "candidate_required",
        missing_output_policy: MissingOutputPolicy = "follow_up_turn",
        cancel_command_admitted: bool = False,
    ) -> FrameFactProjection:
        generation = await self._frames.current_generation(
            target.request_scope, target.harness_execution_id
        )
        if generation is None:
            return FrameFactProjection(generation=None, derived=0, applied=0, through_ordinal=0)
        closing = await self._frames.frames_for_execution(
            target.request_scope,
            target.harness_execution_id,
            generation,
            closing_only=True,
            limit=self._max_frames,
        )
        context = DeriveContext(
            lane=target.lane,
            current_generation=generation,
            completion=completion,
            completion_policy=completion_policy,
            missing_output_policy=missing_output_policy,
            cancel_command_admitted=cancel_command_admitted,
        )
        facts = derive(closing, context)
        applied = 0
        through = 0
        for _attempt in range(16):
            run = await self._run_control.get_run(target.request_scope, target.run_key)
            cursor = next(
                (
                    item.through_ordinal
                    for item in run.frame_fact_cursors
                    if item.harness_execution_id == str(target.harness_execution_id)
                    and item.generation == generation
                ),
                0,
            )
            through = max(through, cursor)
            pending = [fact for fact in facts if fact.arrival_ordinal > cursor]
            if not pending:
                return FrameFactProjection(generation, len(facts), applied, through)
            batch = _batches(pending, self._batch_size)[0]
            last = batch[-1].arrival_ordinal
            # The cursor covers every closing frame up to the last fact's frame; trailing
            # closing frames that produced no fact are covered when the batch is the last.
            covered = max(last, closing[-1].arrival_ordinal) if len(batch) == len(pending) else last
            command = LifecycleCommand(
                command_id=(
                    f"frame-facts:{target.harness_execution_id}:{generation}:{covered}"
                    f":v{run.version}"
                ),
                idempotency_issuer=self._issuer,
                request_scope=target.request_scope,
                run_id=target.run_key,
                expected_run_version=run.version,
                actor=self._actor,
                action=ApplyFrameFactsAction(
                    harness_execution_id=str(target.harness_execution_id),
                    generation=generation,
                    lane_profile=target.lane.value,
                    activation_id=str(target.activation_id),
                    attempt_no=target.attempt_no,
                    through_ordinal=covered,
                    facts=tuple(batch),
                ),
                reason="derive mission state from closing provider frames",
                evidence_refs=tuple(dict.fromkeys(fact.native_event_ref for fact in batch))[:64],
                occurred_at=self._clock(),
                correlation_id=f"frames:{target.harness_execution_id}:{generation}",
            )
            result = await self._run_control.execute(command)
            if result.status == CommandStatus.ACCEPTED:
                applied += len(batch)
                through = covered
                continue
            if result.status == CommandStatus.STALE or result.reason_code in {
                "stale_run_version",
                "frame_facts_already_applied",
            }:
                continue
            return FrameFactProjection(
                generation, len(facts), applied, through, rejected_reason=result.reason_code
            )
        raise FrameFactsRejected(
            "frame_facts_contended", "frame facts stayed stale after repeated retries"
        )
