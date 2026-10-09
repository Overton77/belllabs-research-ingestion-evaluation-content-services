"""The one Human Task service for Human Gate tasks (SPEC-03 "Transport and authorization").

HTTP, MCP and any realtime surface call this service; reviewer authorization lives here, not
in a transport (socket room membership or a tool name is never authority). A resolution is
committed with its event and outbox row first; only then is the waiting control activation
nudged. The nudge is a hint: the activation re-reads the persisted task on every wake and on
a bounded poll, so a lost nudge (API crash after commit) delays, never loses, a resolution.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import UUID

from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.programs.human_gate import (
    HumanGateActivation,
    HumanResolutionRequest,
    HumanTaskView,
    ResolutionCheck,
    decide_resolution,
)

LOGGER = logging.getLogger(__name__)


class HumanTaskRepository(Protocol):
    async def open(self, activation: HumanGateActivation, *, actor_ref: str) -> HumanTaskView: ...

    async def get(self, request_scope: str, human_task_id: UUID | str) -> HumanTaskView | None: ...

    async def list(
        self,
        request_scope: str,
        *,
        run_id: str | None = None,
        lifecycle: str | None = None,
        limit: int = 100,
    ) -> tuple[HumanTaskView, ...]: ...

    async def resolve(
        self,
        request_scope: str,
        human_task_id: UUID | str,
        decide: Callable[[HumanTaskView], ResolutionCheck],
    ) -> tuple[ResolutionCheck, HumanTaskView] | None: ...

    async def apply_timeout(
        self, request_scope: str, human_task_id: UUID | str, *, now: datetime
    ) -> HumanTaskView | None: ...

    async def cancel(
        self,
        request_scope: str,
        human_task_id: UUID | str,
        *,
        now: datetime,
        actor_ref: str,
    ) -> HumanTaskView | None: ...


class HumanGateWake(Protocol):
    """Nudges the control activation waiting on a task (Temporal signal in production)."""

    async def resolution_committed(self, task: HumanTaskView) -> None: ...


class HumanTaskRejected(Exception):
    """A typed refusal; `code` maps to the HTTP status and the MCP error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class ResolutionReceipt:
    status: Literal["accepted", "duplicate"]
    task: HumanTaskView

    def public(self) -> dict[str, object]:
        return {"status": self.status, "task": self.task.public()}


_MESSAGES: dict[str, str] = {
    "already_resolved": "the human task already has a different resolution",
    "task_expired": "the human task expired under its timeout policy",
    "task_cancelled": "the human task was cancelled",
    "not_reviewer": "the principal is not a reviewer of this gate",
    "stale_version": "expected_task_version is stale; re-read the task",
    "packet_digest_mismatch": "the reviewed packet digest is not the task's review packet",
    "decision_not_admitted": "the decision is not admitted for this gate and round",
    "feedback_required": "request_changes carries feedback artifact refs or a comment",
    "deadline_passed": "the task deadline passed under a policy that does not keep waiting",
}


def _utcnow() -> datetime:
    return datetime.now(UTC)


class HumanTaskService:
    """Tenant-scoped Human Gate task reads and the one resolution mutation."""

    def __init__(
        self,
        repository: HumanTaskRepository,
        *,
        request_scope: str,
        wake: HumanGateWake | None = None,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._repository = repository
        self.request_scope = request_scope
        self._wake = wake
        self._clock = clock

    async def get(self, human_task_id: str, actor: ActorContext) -> HumanTaskView:
        del actor  # tenant-scoped read; the transport authorized the application principal
        task = await self._repository.get(self.request_scope, _task_id(human_task_id))
        if task is None:
            raise HumanTaskRejected("not_found", "human task not found")
        return task

    async def list(
        self,
        actor: ActorContext,
        *,
        run_id: str | None = None,
        lifecycle: str | None = None,
        limit: int = 100,
    ) -> tuple[HumanTaskView, ...]:
        del actor
        if lifecycle is not None and lifecycle not in {"open", "resolved", "expired", "cancelled"}:
            raise HumanTaskRejected("invalid_filter", "unknown lifecycle filter")
        return await self._repository.list(
            self.request_scope, run_id=run_id, lifecycle=lifecycle, limit=max(1, min(limit, 500))
        )

    async def resolve(
        self,
        human_task_id: str,
        request: HumanResolutionRequest,
        actor: ActorContext,
    ) -> ResolutionReceipt:
        """Exactly one attributed resolution; a retry of the same request is idempotent."""

        now = self._clock()

        def decide(task: HumanTaskView) -> ResolutionCheck:
            return decide_resolution(
                task,
                request,
                actor_id=actor.actor_id,
                permissions=frozenset(actor.permissions),
                now=now,
            )

        result = await self._repository.resolve(self.request_scope, _task_id(human_task_id), decide)
        if result is None:
            raise HumanTaskRejected("not_found", "human task not found")
        check, task = result
        if check.status == "reject":
            code = check.code or "rejected"
            raise HumanTaskRejected(code, _MESSAGES.get(code, code))
        await self._nudge(task)
        return ResolutionReceipt("accepted" if check.status == "accept" else "duplicate", task)

    async def _nudge(self, task: HumanTaskView) -> None:
        if self._wake is None:
            return
        try:
            await self._wake.resolution_committed(task)
        except Exception:  # the activation's poll reads the committed resolution anyway
            LOGGER.warning(
                "human gate wake failed for task %s; the activation poll will observe it",
                task.human_task_id,
                exc_info=True,
            )


def _task_id(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise HumanTaskRejected("not_found", "human task not found") from None


__all__ = [
    "HumanGateWake",
    "HumanTaskRejected",
    "HumanTaskRepository",
    "HumanTaskService",
    "ResolutionReceipt",
]
