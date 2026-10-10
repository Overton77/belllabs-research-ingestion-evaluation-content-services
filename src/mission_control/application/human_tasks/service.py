"""The one Human Task service (SPEC-03 "Transport and authorization").

HTTP, MCP and any realtime surface call this service; reviewer authorization lives here, not
in a transport (socket room membership or a tool name is never authority). A resolution is
committed with its event and outbox row first; only then is the waiting control activation
nudged. The nudge is a hint: the activation re-reads the persisted task on every wake and on
a bounded poll, so a lost nudge (API crash after commit) delays, never loses, a resolution.

MP-11 (additive): the same service also serves approval-origin tasks (kind ``approval:*``:
provider permission, provider question, MCP elicitation, governed effect) when an
``ApprovalTaskRepository`` is composed. They share the reads, the one resolution mutation and
its idempotency rules; the frozen Human Gate body resolves them with ``approve``/``deny`` (+
feedback), and ``ApprovalResolutionRequest`` adds ``approve_edited``, question answers,
elicitation content and ``cancel`` (distinct from ``deny``).
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import UUID

from mission_control.application.execution.approvals import (
    NATIVE_ORIGINS,
    ApprovalResolutionCheck,
    ApprovalResolutionRequest,
    ApprovalTaskRepository,
    ApprovalTaskView,
    ApprovalTaskWake,
    decide_approval_resolution,
)
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


AnyHumanTask = HumanTaskView | ApprovalTaskView


@dataclass(frozen=True, slots=True)
class ResolutionReceipt:
    status: Literal["accepted", "duplicate"]
    task: AnyHumanTask

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
    "edited_arguments_required": "approve_edited carries the edited arguments with a new digest",
    "answer_required": "answering a question carries an answer or an offered selection",
    "elicitation_content_invalid": "elicitation content does not match the requested schema",
    "invalid_request": "the resolution body does not fit this task's origin",
}


def _utcnow() -> datetime:
    return datetime.now(UTC)


class HumanTaskService:
    """Tenant-scoped Human Task reads and the one resolution mutation (gate + approval)."""

    def __init__(
        self,
        repository: HumanTaskRepository,
        *,
        request_scope: str,
        wake: HumanGateWake | None = None,
        clock: Callable[[], datetime] = _utcnow,
        approvals: ApprovalTaskRepository | None = None,
        approval_wake: ApprovalTaskWake | None = None,
    ) -> None:
        self._repository = repository
        self.request_scope = request_scope
        self._wake = wake
        self._clock = clock
        self._approvals = approvals
        self._approval_wake = approval_wake

    @property
    def approvals_composed(self) -> bool:
        return self._approvals is not None

    async def get(self, human_task_id: str, actor: ActorContext) -> AnyHumanTask:
        del actor  # tenant-scoped read; the transport authorized the application principal
        task_id = _task_id(human_task_id)
        task: AnyHumanTask | None = await self._repository.get(self.request_scope, task_id)
        if task is None and self._approvals is not None:
            task = await self._approvals.get_task(self.request_scope, task_id)
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
        origin: str | None = None,
    ) -> tuple[AnyHumanTask, ...]:
        del actor
        if lifecycle is not None and lifecycle not in {"open", "resolved", "expired", "cancelled"}:
            raise HumanTaskRejected("invalid_filter", "unknown lifecycle filter")
        if origin is not None and origin != "workflow_gate" and origin not in NATIVE_ORIGINS:
            raise HumanTaskRejected("invalid_filter", "unknown origin filter")
        bounded = max(1, min(limit, 500))
        tasks: list[AnyHumanTask] = []
        if origin in {None, "workflow_gate"}:
            tasks.extend(
                await self._repository.list(
                    self.request_scope, run_id=run_id, lifecycle=lifecycle, limit=bounded
                )
            )
        if self._approvals is None:
            return tuple(tasks)
        if origin != "workflow_gate":
            approvals = await self._approvals.list_tasks(
                self.request_scope, run_id=run_id, lifecycle=lifecycle, limit=bounded
            )
            tasks.extend(task for task in approvals if origin is None or task.origin == origin)
        return tuple(sorted(tasks, key=lambda task: (task.created_at, task.task_key))[:bounded])

    async def resolve(
        self,
        human_task_id: str,
        request: HumanResolutionRequest | ApprovalResolutionRequest,
        actor: ActorContext,
    ) -> ResolutionReceipt:
        """Exactly one attributed resolution; a retry of the same request is idempotent."""

        if isinstance(request, ApprovalResolutionRequest):
            return await self._resolve_approval(human_task_id, request, actor)
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
            if self._approvals is not None:
                return await self._resolve_approval(human_task_id, request, actor)
            raise HumanTaskRejected("not_found", "human task not found")
        check, task = result
        if check.status == "reject":
            code = check.code or "rejected"
            raise HumanTaskRejected(code, _MESSAGES.get(code, code))
        await self._nudge(task)
        return ResolutionReceipt("accepted" if check.status == "accept" else "duplicate", task)

    async def resolve_approval(
        self, human_task_id: str, request: ApprovalResolutionRequest, actor: ActorContext
    ) -> ResolutionReceipt:
        """The extended approval body (approve_edited, answers, elicitation, cancel)."""

        return await self._resolve_approval(human_task_id, request, actor)

    async def cancel_run_approvals(
        self, run_id: str, *, actor_ref: str
    ) -> tuple[ApprovalTaskView, ...]:
        """Stop/cancel path: close every open approval task of the run (no resolution)."""

        if self._approvals is None:
            return ()
        now = self._clock()
        cancelled: list[ApprovalTaskView] = []
        for task in await self._approvals.list_tasks(
            self.request_scope, run_id=run_id, lifecycle="open", limit=500
        ):
            moved = await self._approvals.cancel_task(
                self.request_scope, task.human_task_id, now=now, actor_ref=actor_ref
            )
            if moved is not None and moved.lifecycle == "cancelled":
                cancelled.append(moved)
                await self._nudge_approval(moved)
        return tuple(cancelled)

    async def _resolve_approval(
        self,
        human_task_id: str,
        request: HumanResolutionRequest | ApprovalResolutionRequest,
        actor: ActorContext,
    ) -> ResolutionReceipt:
        if self._approvals is None:
            raise HumanTaskRejected("not_found", "human task not found")
        now = self._clock()
        refused: list[str] = []

        def decide(task: ApprovalTaskView) -> ApprovalResolutionCheck:
            body = request
            if not isinstance(body, ApprovalResolutionRequest):
                try:
                    body = ApprovalResolutionRequest.from_gate_body(body, origin=task.origin)
                except ValueError:
                    refused.append("invalid_request")
                    return ApprovalResolutionCheck("reject", code="decision_not_admitted")
            return decide_approval_resolution(
                task,
                body,
                actor_id=actor.actor_id,
                permissions=frozenset(actor.permissions),
                now=now,
            )

        result = await self._approvals.resolve_task(
            self.request_scope, _task_id(human_task_id), decide
        )
        if result is None:
            raise HumanTaskRejected("not_found", "human task not found")
        check, task = result
        if check.status == "reject":
            code = refused[0] if refused else (check.code or "rejected")
            raise HumanTaskRejected(code, check.message or _MESSAGES.get(code, code))
        await self._nudge_approval(task)
        return ResolutionReceipt("accepted" if check.status == "accept" else "duplicate", task)

    async def _nudge_approval(self, task: ApprovalTaskView) -> None:
        if self._approval_wake is None:
            return
        try:
            await self._approval_wake.resolution_committed(task)
        except Exception:  # the broker's bounded poll reads the committed resolution anyway
            LOGGER.warning(
                "approval wake failed for task %s; the broker poll will observe it",
                task.human_task_id,
                exc_info=True,
            )

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
    "AnyHumanTask",
    "HumanGateWake",
    "HumanTaskRejected",
    "HumanTaskRepository",
    "HumanTaskService",
    "ResolutionReceipt",
]
