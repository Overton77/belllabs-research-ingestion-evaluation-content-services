"""Bind, wait (bounded), translate, expire and recover native approval requests (MP-11).

The lane adapters (Claude ``can_use_tool`` / user questions, Codex approval and user-input
RPCs, Cursor ``ask`` where qualified, forwarded MCP elicitations) call one ``ApprovalBroker``:

1. ``bind`` persists the ``mc.approval_binding.v1`` (the approval task) and a fresh live
   correlation for this native delivery **before** anyone waits. A task that is already
   closed answers at once: a denial/cancel always replays; an approval replays into a fresh
   native request only under ``reissue_native_request`` and after revalidation.
2. ``wait`` is bounded. It re-reads the persisted task on every wake and on a bounded poll
   (a lost wake delays, never loses, a resolution). When the bound elapses the correlation
   expires with a system ``deny`` + interrupt; the durable task stays open. Never keep an
   Activity or a native hook process sleeping indefinitely (SPEC-03).
3. Before a decision reaches a native request it is revalidated: the correlation must still be
   ``live`` (compare-and-set), the execution generation, policy digest and grants must match,
   and an approval of an effect is admitted through the Stop Fence (the same effect-admission
   repository MP-06 uses for native dispatches).
4. ``recover`` (after a restart) closes every live correlation another connection held as
   ``lost`` and returns the replay plan per the recorded strategy; the lane reconciles
   effects (MP-06 dispatch journal) and then either obtains a fresh native request (which
   binds a new correlation to the same task) or parks.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Final, Literal

from pydantic import Field

from mission_control.application.execution.approvals import (
    ApprovalContextProbe,
    ApprovalContract,
    ApprovalCorrelation,
    ApprovalCorrelationRepository,
    ApprovalTaskRepository,
    ApprovalTaskView,
    ApprovalTimeoutPolicy,
    ElicitationPrompt,
    NativeOrigin,
    NativeReply,
    QuestionPrompt,
    ReplyReason,
    approval_timed_out,
    native_reply,
    open_approval_packet,
    revalidation_failure,
    system_reply,
)
from mission_control.application.execution.stop_fence import StopFenceRepository
from mission_control.contracts.identities import uuid7
from mission_control.domain.execution.approvals import NativeApprovalCorrelation, ReplayStrategy
from mission_control.domain.execution.lanes import DIGEST_PATTERN, LaneProfileName
from mission_control.domain.policies.stop_fence import EffectAdmission, EffectKind

BROKER_ACTOR: Final = "mission-control-runtime/approval-broker"
DEFAULT_POLL_SECONDS: Final = 0.5
MAX_WAIT_SECONDS: Final = 600.0

OutcomeStatus = Literal[
    "approved",
    "denied",
    "answered",
    "cancelled",
    "expired",
    "superseded",
    "stale",
    "fenced",
    "task_expired",
    "task_cancelled",
    "not_replayable",
]
RecoveryAction = Literal[
    "await_fresh_request",
    "restart_at_safe_boundary",
    "park_for_reconciliation",
    "deny_and_park",
]

_STATUS_OF_REASON: Final[dict[str, OutcomeStatus]] = {
    "wait_expired": "expired",
    "correlation_superseded": "superseded",
    "correlation_lost": "superseded",
    "stale_generation": "stale",
    "policy_changed": "stale",
    "grant_revoked": "stale",
    "stop_fenced": "fenced",
    "task_expired": "task_expired",
    "task_cancelled": "task_cancelled",
    "not_replayable": "not_replayable",
}
_RECOVERY_OF: Final[dict[str, RecoveryAction]] = {
    "reissue_native_request": "await_fresh_request",
    "restart_at_safe_boundary": "restart_at_safe_boundary",
    "park_for_reconciliation": "park_for_reconciliation",
    "deny_and_park": "deny_and_park",
}


def _utcnow() -> datetime:
    return datetime.now(UTC)


class NativeApprovalRequest(ApprovalContract):
    """What a lane adapter hands the broker for one native request delivery."""

    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    lane_profile: LaneProfileName
    origin: NativeOrigin
    harness_execution_id: str = Field(min_length=1, max_length=512)
    generation: int = Field(ge=1)
    native: NativeApprovalCorrelation
    tool_name: str | None = Field(default=None, min_length=1, max_length=256)
    arguments: dict[str, Any] = Field(default_factory=dict)
    effect_kind: EffectKind = "other"
    policy_digest: str = Field(pattern=DIGEST_PATTERN)
    reviewers: tuple[str, ...] = Field(min_length=1, max_length=64)
    prompt: str = Field(min_length=1, max_length=8_192)
    timeout_seconds: int | None = Field(default=None, ge=1)
    on_timeout: ApprovalTimeoutPolicy = "keep_waiting"
    replay_strategy: ReplayStrategy
    question: QuestionPrompt | None = None
    elicitation: ElicitationPrompt | None = None


class ApprovalOutcome(ApprovalContract):
    """What the native callback returns: the reply plus durable evidence refs."""

    status: OutcomeStatus
    reply: NativeReply
    human_task_id: str
    correlation_id: str
    resolution_ref: str | None = None
    replayed: bool = False


class BoundApproval(ApprovalContract):
    task: ApprovalTaskView
    correlation: ApprovalCorrelation
    immediate: ApprovalOutcome | None = None


class RecoveryItem(ApprovalContract):
    """A correlation lost with its process and what the lane does next."""

    correlation: ApprovalCorrelation
    task: ApprovalTaskView | None
    action: RecoveryAction
    decision_ref: str | None = None


class ApprovalWakeHub:
    """In-process waiters keyed by task id; also the HumanTaskService approval wake.

    A hint only: waiters always re-read the persisted task, and the bounded poll covers a
    resolution committed by another process (the API) whose nudge cannot reach this one.
    """

    def __init__(self) -> None:
        self._events: dict[str, set[asyncio.Event]] = defaultdict(set)

    async def resolution_committed(self, task: ApprovalTaskView) -> None:
        self.notify(task.human_task_id)

    def notify(self, human_task_id: str) -> None:
        for event in tuple(self._events.get(human_task_id, ())):
            event.set()

    async def wait(self, human_task_id: str, seconds: float) -> None:
        event = asyncio.Event()
        self._events[human_task_id].add(event)
        try:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(event.wait(), timeout=max(0.0, seconds))
        finally:
            waiters = self._events.get(human_task_id)
            if waiters is not None:
                waiters.discard(event)
                if not waiters:
                    self._events.pop(human_task_id, None)


class ApprovalBroker:
    """One worker process's native approval correlations (``connection_ref`` = this owner)."""

    def __init__(
        self,
        tasks: ApprovalTaskRepository,
        correlations: ApprovalCorrelationRepository,
        *,
        probe: ApprovalContextProbe,
        connection_ref: str,
        fences: StopFenceRepository | None = None,
        hub: ApprovalWakeHub | None = None,
        clock: Callable[[], datetime] = _utcnow,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        actor_ref: str = BROKER_ACTOR,
    ) -> None:
        if not connection_ref:
            raise ValueError("an approval broker names the connection that holds native handles")
        self._tasks = tasks
        self._correlations = correlations
        self._probe = probe
        self._fences = fences
        self._hub = hub or ApprovalWakeHub()
        self._clock = clock
        self._poll = poll_seconds
        self._actor = actor_ref
        self.connection_ref = connection_ref

    @property
    def hub(self) -> ApprovalWakeHub:
        return self._hub

    async def bind(self, request: NativeApprovalRequest) -> BoundApproval:
        """Persist the binding and a live correlation before any wait."""

        now = self._clock()
        packet = open_approval_packet(
            request_scope=request.request_scope,
            run_id=request.run_id,
            origin=request.origin,
            lane_profile=request.lane_profile,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            native=request.native,
            tool_name=request.tool_name,
            arguments=request.arguments,
            policy_digest=request.policy_digest,
            reviewers=request.reviewers,
            prompt=request.prompt,
            opened_at=now,
            replay_strategy=request.replay_strategy,
            timeout_seconds=request.timeout_seconds,
            on_timeout=request.on_timeout,
            effect_kind=request.effect_kind,
            connection_ref=self.connection_ref,
            question=request.question,
            elicitation=request.elicitation,
        )
        task = await self._tasks.open_task(packet, actor_ref=self._actor)
        correlation = await self._correlations.open_correlation(
            ApprovalCorrelation(
                correlation_id=str(uuid7()),
                request_scope=request.request_scope,
                human_task_id=task.human_task_id,
                run_id=request.run_id,
                harness_execution_id=request.harness_execution_id,
                generation=request.generation,
                connection_ref=self.connection_ref,
                native=request.native,
                input_digest=task.packet.binding.input_digest,
                opened_at=now,
            )
        )
        immediate = None
        if task.lifecycle != "open":
            immediate = await self._conclude(task, correlation, replay=True)
        return BoundApproval(task=task, correlation=correlation, immediate=immediate)

    async def wait(self, bound: BoundApproval, *, wait_seconds: float) -> ApprovalOutcome:
        """Wait at most `wait_seconds`; on expiry the correlation (not the task) expires."""

        if bound.immediate is not None:
            return bound.immediate
        scope = bound.correlation.request_scope
        task_id = bound.task.human_task_id
        loop = asyncio.get_running_loop()
        give_up = loop.time() + min(max(wait_seconds, 0.0), MAX_WAIT_SECONDS)
        while True:
            task = await self._tasks.get_task(scope, task_id)
            if task is None:
                raise LookupError(f"approval task {task_id} is not persisted")
            if approval_timed_out(task, self._clock()):
                task = await self._tasks.apply_timeout(scope, task_id, now=self._clock()) or task
            if task.lifecycle != "open":
                return await self._conclude(task, bound.correlation, replay=False)
            current = await self._correlations.get_correlation(
                scope, bound.correlation.correlation_id
            )
            if current is None or current.state != "live":
                return self._outcome(
                    task, bound.correlation, system_reply(task.packet, "correlation_superseded")
                )
            remaining = give_up - loop.time()
            if remaining <= 0:
                break
            await self._hub.wait(task_id, min(self._poll, remaining))
        reply = system_reply(task.packet, "wait_expired")
        closed = await self._correlations.close_correlation(
            scope,
            bound.correlation.correlation_id,
            state="expired",
            at=self._clock(),
            reason="wait_expired",
            reply=reply,
        )
        if closed is None:
            # Something closed it concurrently (superseded); the task decides the outcome.
            latest = await self._tasks.get_task(scope, task_id)
            if latest is not None and latest.lifecycle != "open":
                return await self._conclude(latest, bound.correlation, replay=False)
            return self._outcome(
                task, bound.correlation, system_reply(task.packet, "correlation_superseded")
            )
        return self._outcome(task, bound.correlation, reply)

    async def recover(
        self, request_scope: str, harness_execution_id: str
    ) -> tuple[RecoveryItem, ...]:
        """After a restart: every correlation another connection held is lost, never reused."""

        lost = await self._correlations.mark_lost(
            request_scope,
            harness_execution_id=harness_execution_id,
            except_connection_ref=self.connection_ref,
            at=self._clock(),
        )
        items: list[RecoveryItem] = []
        for correlation in lost:
            task = await self._tasks.get_task(request_scope, correlation.human_task_id)
            strategy = (
                task.packet.binding.replay_strategy
                if task is not None
                else "park_for_reconciliation"
            )
            items.append(
                RecoveryItem(
                    correlation=correlation,
                    task=task,
                    action=_RECOVERY_OF[strategy],
                    decision_ref=(
                        task.resolution.resolution_ref
                        if task is not None and task.resolution is not None
                        else None
                    ),
                )
            )
        return tuple(items)

    async def _conclude(
        self, task: ApprovalTaskView, correlation: ApprovalCorrelation, *, replay: bool
    ) -> ApprovalOutcome:
        """Turn a closed task into the reply for this correlation (revalidated, CAS-closed)."""

        packet = task.packet
        reply: NativeReply
        if task.lifecycle == "expired":
            reply = system_reply(packet, "task_expired")
        elif task.lifecycle == "cancelled":
            reply = system_reply(packet, "task_cancelled")
        else:
            resolution = task.resolution
            if resolution is None:
                raise LookupError(f"resolved approval task {task.human_task_id} has no answer")
            failure: ReplyReason | None = None
            if resolution.authorizes_effect or resolution.resolution_action == "answered":
                if replay and packet.binding.replay_strategy != "reissue_native_request":
                    failure = "not_replayable"
                if failure is None:
                    state = await self._probe.current(
                        correlation.request_scope,
                        run_id=packet.run_id,
                        harness_execution_id=packet.binding.harness_execution_id,
                    )
                    failure = revalidation_failure(packet.binding, state)
                if failure is None and resolution.authorizes_effect and self._fences is not None:
                    verdict = await self._fences.admit_effect(
                        EffectAdmission(
                            request_scope=correlation.request_scope,
                            run_id=packet.run_id,
                            generation=packet.binding.generation,
                            effect_ref=_effect_ref(packet.binding.native, correlation),
                            effect_kind=packet.effect_kind,
                            lane_profile=packet.binding.lane_profile,
                        )
                    )
                    if not verdict.allowed:
                        failure = "stop_fenced"
            reply = (
                system_reply(packet, failure)
                if failure is not None
                else native_reply(packet, resolution)
            )
        closed = await self._correlations.close_correlation(
            correlation.request_scope,
            correlation.correlation_id,
            state="answered",
            at=self._clock(),
            reason=reply.reason,
            reply=reply,
            replayed_from=(
                task.resolution.resolution_ref
                if replay and task.resolution is not None and reply.reason == "human_decision"
                else None
            ),
        )
        if closed is None:
            return self._outcome(task, correlation, system_reply(packet, "correlation_superseded"))
        return self._outcome(task, correlation, reply, replayed=replay)

    @staticmethod
    def _outcome(
        task: ApprovalTaskView,
        correlation: ApprovalCorrelation,
        reply: NativeReply,
        *,
        replayed: bool = False,
    ) -> ApprovalOutcome:
        if reply.reason == "human_decision":
            status: OutcomeStatus = {
                "allow": "approved",
                "answer": "answered",
                "deny": "denied",
                "cancel": "cancelled",
            }[reply.action]  # type: ignore[assignment]
        else:
            status = _STATUS_OF_REASON[reply.reason]
        return ApprovalOutcome(
            status=status,
            reply=reply,
            human_task_id=task.human_task_id,
            correlation_id=correlation.correlation_id,
            resolution_ref=task.resolution.resolution_ref if task.resolution is not None else None,
            replayed=replayed and reply.reason == "human_decision",
        )


def _effect_ref(native: NativeApprovalCorrelation, correlation: ApprovalCorrelation) -> str:
    """The Stop Fence effect identity: the stable tool call, else this correlation."""

    if native.tool_call_ref is not None:
        return f"approval:call:{native.tool_call_ref}"[:512]
    return f"approval:correlation:{correlation.correlation_id}"


__all__ = [
    "BROKER_ACTOR",
    "ApprovalBroker",
    "ApprovalOutcome",
    "ApprovalWakeHub",
    "BoundApproval",
    "NativeApprovalRequest",
    "RecoveryItem",
]
