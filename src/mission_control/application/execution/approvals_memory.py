"""In-memory approval tasks, correlations and governed intents (FIXTURE semantics).

Same rules as ``adapters/postgres/approvals`` for unit tests and in-process compositions;
persistence, row locks and RLS are proven only against a real PostgreSQL.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from mission_control.application.execution.approvals import (
    APPROVAL_TIMEOUT_ACTOR,
    ApprovalContextState,
    ApprovalCorrelation,
    ApprovalResolution,
    ApprovalResolutionCheck,
    ApprovalTaskPacket,
    ApprovalTaskView,
    CorrelationState,
    NativeReply,
    approval_timed_out,
)
from mission_control.application.execution.approvals_governed import (
    GovernedIntent,
    GovernedIntentState,
    GovernedReceipt,
)
from mission_control.domain.policies.errors import IdempotencyConflict


@dataclass(frozen=True, slots=True)
class RecordedApprovalEvent:
    event_type: str
    human_task_id: str
    version: int
    actor_ref: str


class InMemoryApprovalStore:
    """Tasks + correlations, one lock (the PostgreSQL row locks' serialization)."""

    def __init__(self) -> None:
        self._tasks: dict[tuple[str, str], ApprovalTaskView] = {}
        self._correlations: dict[tuple[str, str], ApprovalCorrelation] = {}
        self._lock = asyncio.Lock()
        self.events: list[RecordedApprovalEvent] = []

    # -- tasks ------------------------------------------------------------------------------

    async def open_task(self, packet: ApprovalTaskPacket, *, actor_ref: str) -> ApprovalTaskView:
        key = (packet.request_scope, str(packet.human_task_id))
        async with self._lock:
            prior = self._tasks.get(key)
            if prior is not None:
                if prior.packet.intent() != packet.intent():
                    raise IdempotencyConflict("approval task identity has conflicting intent")
                return prior
            opened = packet.binding.opened_at
            view = ApprovalTaskView(
                human_task_id=str(packet.human_task_id),
                task_key=packet.task_key,
                kind=packet.kind,
                target_ref=packet.target_ref,
                lifecycle="open",
                version=1,
                deadline_at=packet.binding.deadline,
                on_timeout=packet.on_timeout,
                packet=packet,
                created_at=opened,
                updated_at=opened,
            )
            self._tasks[key] = view
            self.events.append(
                RecordedApprovalEvent("human_task.created", view.human_task_id, 1, actor_ref)
            )
            return view

    async def get_task(
        self, request_scope: str, human_task_id: UUID | str
    ) -> ApprovalTaskView | None:
        return self._tasks.get((request_scope, str(human_task_id)))

    async def list_tasks(
        self,
        request_scope: str,
        *,
        run_id: str | None = None,
        lifecycle: str | None = None,
        limit: int = 100,
    ) -> tuple[ApprovalTaskView, ...]:
        items = [
            task
            for (scope, _), task in self._tasks.items()
            if scope == request_scope
            and (run_id is None or task.packet.run_id == run_id)
            and (lifecycle is None or task.lifecycle == lifecycle)
        ]
        return tuple(sorted(items, key=lambda item: (item.created_at, item.task_key))[:limit])

    async def resolve_task(
        self,
        request_scope: str,
        human_task_id: UUID | str,
        decide: Callable[[ApprovalTaskView], ApprovalResolutionCheck],
    ) -> tuple[ApprovalResolutionCheck, ApprovalTaskView] | None:
        async with self._lock:
            task = self._tasks.get((request_scope, str(human_task_id)))
            if task is None:
                return None
            check = decide(task)
            if check.status != "accept" or check.resolution is None:
                return check, task
            return check, self._resolved(task, check.resolution)

    async def apply_timeout(
        self, request_scope: str, human_task_id: UUID | str, *, now: datetime
    ) -> ApprovalTaskView | None:
        async with self._lock:
            task = self._tasks.get((request_scope, str(human_task_id)))
            if task is None or not approval_timed_out(task, now):
                return task
            return self._move(task, "expired", "human_task.expired", now, APPROVAL_TIMEOUT_ACTOR)

    async def cancel_task(
        self, request_scope: str, human_task_id: UUID | str, *, now: datetime, actor_ref: str
    ) -> ApprovalTaskView | None:
        async with self._lock:
            task = self._tasks.get((request_scope, str(human_task_id)))
            if task is None or task.lifecycle != "open":
                return task
            return self._move(task, "cancelled", "human_task.cancelled", now, actor_ref)

    def _resolved(self, task: ApprovalTaskView, resolution: ApprovalResolution) -> ApprovalTaskView:
        task = task.model_copy(update={"resolution": resolution})
        return self._move(
            task, "resolved", "human_task.resolved", resolution.decided_at, resolution.actor_ref
        )

    def _move(
        self, task: ApprovalTaskView, lifecycle: str, event: str, at: datetime, actor_ref: str
    ) -> ApprovalTaskView:
        version = task.version + 1
        moved = task.model_copy(
            update={"lifecycle": lifecycle, "version": version, "updated_at": at}
        )
        self._tasks[(task.packet.request_scope, task.human_task_id)] = moved
        self.events.append(RecordedApprovalEvent(event, task.human_task_id, version, actor_ref))
        return moved

    # -- correlations -----------------------------------------------------------------------

    async def open_correlation(self, correlation: ApprovalCorrelation) -> ApprovalCorrelation:
        async with self._lock:
            for key, prior in list(self._correlations.items()):
                if (
                    prior.request_scope == correlation.request_scope
                    and prior.human_task_id == correlation.human_task_id
                    and prior.state == "live"
                ):
                    self._correlations[key] = prior.model_copy(
                        update={
                            "state": "superseded",
                            "closed_at": correlation.opened_at,
                            "close_reason": "correlation_superseded",
                            "version": prior.version + 1,
                        }
                    )
            self._correlations[(correlation.request_scope, correlation.correlation_id)] = (
                correlation
            )
            return correlation

    async def get_correlation(
        self, request_scope: str, correlation_id: str
    ) -> ApprovalCorrelation | None:
        return self._correlations.get((request_scope, correlation_id))

    async def close_correlation(
        self,
        request_scope: str,
        correlation_id: str,
        *,
        state: CorrelationState,
        at: datetime,
        reason: str,
        reply: NativeReply | None = None,
        replayed_from: str | None = None,
    ) -> ApprovalCorrelation | None:
        async with self._lock:
            key = (request_scope, correlation_id)
            prior = self._correlations.get(key)
            if prior is None or prior.state != "live":
                return None
            closed = prior.model_copy(
                update={
                    "state": state,
                    "closed_at": at,
                    "close_reason": reason,
                    "reply": reply,
                    "replayed_from": replayed_from,
                    "version": prior.version + 1,
                }
            )
            self._correlations[key] = ApprovalCorrelation.model_validate(closed.model_dump())
            return self._correlations[key]

    async def mark_lost(
        self,
        request_scope: str,
        *,
        harness_execution_id: str,
        except_connection_ref: str,
        at: datetime,
    ) -> tuple[ApprovalCorrelation, ...]:
        lost: list[ApprovalCorrelation] = []
        async with self._lock:
            for key, prior in list(self._correlations.items()):
                if (
                    prior.request_scope == request_scope
                    and prior.harness_execution_id == harness_execution_id
                    and prior.connection_ref != except_connection_ref
                    and prior.state == "live"
                ):
                    closed = prior.model_copy(
                        update={
                            "state": "lost",
                            "closed_at": at,
                            "close_reason": "correlation_lost",
                            "version": prior.version + 1,
                        }
                    )
                    self._correlations[key] = closed
                    lost.append(closed)
        return tuple(sorted(lost, key=lambda item: item.opened_at))

    def correlations(self) -> tuple[ApprovalCorrelation, ...]:
        return tuple(self._correlations.values())


class InMemoryGovernedIntents:
    def __init__(self) -> None:
        self._intents: dict[tuple[str, str], GovernedIntent] = {}
        self._lock = asyncio.Lock()

    async def prepare(self, intent: GovernedIntent) -> GovernedIntent:
        async with self._lock:
            key = (intent.request_scope, intent.intent_id)
            prior = self._intents.get(key)
            if prior is not None:
                if prior.intent_key != intent.intent_key or prior.arguments != intent.arguments:
                    raise IdempotencyConflict("governed intent identity has conflicting intent")
                return prior
            self._intents[key] = intent
            return intent

    async def get(self, request_scope: str, intent_id: str) -> GovernedIntent | None:
        return self._intents.get((request_scope, intent_id))

    async def transition(
        self,
        request_scope: str,
        intent_id: str,
        *,
        from_states: frozenset[str],
        to_state: GovernedIntentState,
        at: datetime,
        reason: str | None = None,
        claimant_ref: str | None = None,
    ) -> GovernedIntent | None:
        async with self._lock:
            key = (request_scope, intent_id)
            prior = self._intents.get(key)
            if prior is None or prior.state not in from_states:
                return None
            update: dict[str, object] = {
                "state": to_state,
                "updated_at": at,
                "version": prior.version + 1,
                "reason": reason,
            }
            if to_state == "executing":
                update.update(claimant_ref=claimant_ref, claimed_at=at)
            moved = GovernedIntent.model_validate({**prior.model_dump(), **update})
            self._intents[key] = moved
            return moved

    async def record_receipt(
        self, request_scope: str, intent_id: str, receipt: GovernedReceipt, *, at: datetime
    ) -> GovernedIntent | None:
        async with self._lock:
            key = (request_scope, intent_id)
            prior = self._intents.get(key)
            if prior is None or prior.state != "executing":
                return None
            moved = prior.model_copy(
                update={
                    "state": "executed",
                    "receipt": receipt,
                    "updated_at": at,
                    "version": prior.version + 1,
                }
            )
            self._intents[key] = moved
            return moved


class StaticApprovalContext:
    """FIXTURE probe: a mutable current generation/policy per execution."""

    def __init__(self, generation: int, policy_digest: str, *, granted: bool = True) -> None:
        self.state = ApprovalContextState(
            generation=generation, policy_digest=policy_digest, granted=granted
        )

    async def current(
        self, request_scope: str, *, run_id: str, harness_execution_id: str
    ) -> ApprovalContextState:
        del request_scope, run_id, harness_execution_id
        return self.state


__all__ = [
    "InMemoryApprovalStore",
    "InMemoryGovernedIntents",
    "RecordedApprovalEvent",
    "StaticApprovalContext",
]
