"""In-memory Human Gate task store with the PostgreSQL repository's semantics.

For unit tests and in-process compositions only; persistence claims are proven against
``adapters/postgres/human_tasks`` on a real database.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.programs.human_gate import (
    HUMAN_TASK_CANCELLED,
    HUMAN_TASK_CREATED,
    HUMAN_TASK_ESCALATED,
    HUMAN_TASK_EXPIRED,
    HUMAN_TASK_RESOLVED,
    TIMEOUT_POLICY_ACTOR,
    HumanGateActivation,
    HumanResolution,
    HumanTaskView,
    ResolutionCheck,
    default_resolution,
    timeout_disposition,
)


@dataclass(frozen=True, slots=True)
class RecordedEvent:
    event_type: str
    human_task_id: str
    version: int
    actor_ref: str


class InMemoryHumanTaskRepository:
    def __init__(self) -> None:
        self._tasks: dict[tuple[str, str], HumanTaskView] = {}
        self._lock = asyncio.Lock()
        self.events: list[RecordedEvent] = []

    async def open(self, activation: HumanGateActivation, *, actor_ref: str) -> HumanTaskView:
        key = (activation.request_scope, str(activation.human_task_id))
        async with self._lock:
            prior = self._tasks.get(key)
            if prior is not None:
                if prior.activation != activation:
                    raise IdempotencyConflict("human gate task identity has conflicting intent")
                return prior
            view = HumanTaskView(
                human_task_id=str(activation.human_task_id),
                task_key=activation.task_key,
                kind=activation.kind,
                target_ref=activation.target_ref,
                lifecycle="open",
                version=1,
                deadline_at=activation.deadline_at,
                on_timeout=activation.spec.on_timeout,
                activation=activation,
                created_at=activation.opened_at,
                updated_at=activation.opened_at,
            )
            self._tasks[key] = view
            self.events.append(RecordedEvent(HUMAN_TASK_CREATED, view.human_task_id, 1, actor_ref))
            return view

    async def get(self, request_scope: str, human_task_id: UUID | str) -> HumanTaskView | None:
        return self._tasks.get((request_scope, str(human_task_id)))

    async def list(
        self,
        request_scope: str,
        *,
        run_id: str | None = None,
        lifecycle: str | None = None,
        limit: int = 100,
    ) -> tuple[HumanTaskView, ...]:
        items = [
            task
            for (scope, _), task in self._tasks.items()
            if scope == request_scope
            and (run_id is None or task.activation.run_id == run_id)
            and (lifecycle is None or task.lifecycle == lifecycle)
        ]
        return tuple(sorted(items, key=lambda item: (item.created_at, item.task_key))[:limit])

    async def resolve(
        self,
        request_scope: str,
        human_task_id: UUID | str,
        decide: Callable[[HumanTaskView], ResolutionCheck],
    ) -> tuple[ResolutionCheck, HumanTaskView] | None:
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
    ) -> HumanTaskView | None:
        async with self._lock:
            task = self._tasks.get((request_scope, str(human_task_id)))
            if task is None:
                return None
            disposition = timeout_disposition(task, now)
            if disposition in {"none", "keep_waiting"}:
                return task
            if disposition == "default_answer":
                return self._resolved(task, default_resolution(task, now))
            if disposition == "escalate":
                return self._move(task, "open", HUMAN_TASK_ESCALATED, now, TIMEOUT_POLICY_ACTOR)
            return self._move(task, "expired", HUMAN_TASK_EXPIRED, now, TIMEOUT_POLICY_ACTOR)

    async def cancel(
        self,
        request_scope: str,
        human_task_id: UUID | str,
        *,
        now: datetime,
        actor_ref: str,
    ) -> HumanTaskView | None:
        async with self._lock:
            task = self._tasks.get((request_scope, str(human_task_id)))
            if task is None or task.lifecycle != "open":
                return task
            return self._move(task, "cancelled", HUMAN_TASK_CANCELLED, now, actor_ref)

    def _resolved(self, task: HumanTaskView, resolution: HumanResolution) -> HumanTaskView:
        task = task.model_copy(update={"resolution": resolution})
        return self._move(
            task, "resolved", HUMAN_TASK_RESOLVED, resolution.decided_at, resolution.actor_ref
        )

    def _move(
        self, task: HumanTaskView, lifecycle: str, event: str, at: datetime, actor_ref: str
    ) -> HumanTaskView:
        version = task.version + 1
        moved = task.model_copy(
            update={
                "lifecycle": lifecycle,
                "version": version,
                "updated_at": at,
                "escalated": lifecycle == "open",
            }
        )
        self._tasks[(task.activation.request_scope, task.human_task_id)] = moved
        self.events.append(RecordedEvent(event, task.human_task_id, version, actor_ref))
        return moved


__all__ = ["InMemoryHumanTaskRepository", "RecordedEvent"]
