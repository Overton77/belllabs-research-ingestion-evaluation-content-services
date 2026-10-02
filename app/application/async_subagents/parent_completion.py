"""Parent-boundary completion of async children (RRM-009; REQ-CP-DA-011, REQ-CP-RUN-009).

A parent operation's async children are BellLabs effects of that operation. Before the parent
settles, the operation boundary brings each child it spawned to a governed end:

1. reconcile the child with its provider until it is terminal or the bounded wait elapses;
2. for a blocking dependency class, record the result decision the contract's declared
   admission policy yields (a policy this deployment does not register decides nothing);
3. settle the terminal child against the parent run's budget exactly once.

A child that is still active, in doubt or undecided is left exactly as it is: its effect
stays unsettled, so the run cannot terminalize until an operator or a later boundary
resolves it. Nothing is assumed and no second child is ever started. Every step is
idempotent (a decided link is not decided again; a settled link returns as it is), so a
retried or reconstructed operation completes the same children the same way.

Cancellation of active children is RRM-008's saga (`cancel_children`); this module only
completes children whose parent finished its cognition.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol

from app.application.async_subagents.inspection import AsyncChildLineageView
from app.application.async_subagents.service import AsyncSubagentService
from app.domain.operation_execution.contracts import (
    AsyncSubagentDependencyClass,
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    OperationExecutionBinding,
)

TERMINAL_LIFECYCLES = frozenset(
    {
        AsyncSubagentLifecycle.COMPLETED,
        AsyncSubagentLifecycle.FAILED,
        AsyncSubagentLifecycle.CANCELLED,
        AsyncSubagentLifecycle.ORPHANED,
    }
)
BLOCKING_CLASSES = frozenset(
    {
        AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
        AsyncSubagentDependencyClass.DEGRADABLE_BLOCKING,
    }
)
ResultDecision = Literal["admit", "conditionally_admit", "reject", "defer"]
AdmissionRule = Callable[[AsyncSubagentExecution], ResultDecision]


def admit_typed_manifest(execution: AsyncSubagentExecution) -> ResultDecision:
    """Admit a completed child whose typed manifest was captured (identity re-verified at
    completion, REQ-CP-DA-019); reject a child that ended without one."""

    if (
        execution.lifecycle == AsyncSubagentLifecycle.COMPLETED
        and execution.result_manifest is not None
    ):
        return "admit"
    return "reject"


class ChildLineage(Protocol):
    async def list_children(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[AsyncChildLineageView, ...]: ...


async def children_of(lineage: ChildLineage, binding: OperationExecutionBinding) -> tuple[str, ...]:
    """The children the parent binding spawned, from authority rows, in creation order."""

    return tuple(
        view.child_execution_id
        for view in await lineage.list_children(binding.request_scope, binding.run_id)
        if view.parent_binding_id == binding.binding_id
    )


@dataclass(frozen=True)
class AsyncChildCompletionRecord:
    child_execution_id: str
    lifecycle: str
    dependency_class: str
    result_decision: str | None
    usage_disposition: str
    reason: str | None = None

    def as_payload(self) -> dict[str, object]:
        return {
            "child_execution_id": self.child_execution_id,
            "lifecycle": self.lifecycle,
            "dependency_class": self.dependency_class,
            "result_decision": self.result_decision,
            "usage_disposition": self.usage_disposition,
            "reason": self.reason,
        }


class AsyncChildCompletion:
    def __init__(
        self,
        service: AsyncSubagentService,
        lineage: ChildLineage,
        *,
        policies: Mapping[str, AdmissionRule],
        wait_seconds: float = 120.0,
        poll_seconds: float = 2.0,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if wait_seconds < 0 or poll_seconds <= 0:
            raise ValueError("completion wait must be non-negative and the poll positive")
        self._service = service
        self._lineage = lineage
        self._policies = dict(policies)
        self._wait = wait_seconds
        self._poll = poll_seconds
        self._clock = clock or (lambda: datetime.now(UTC))

    async def complete(
        self, binding: OperationExecutionBinding, *, execution_generation: int
    ) -> tuple[AsyncChildCompletionRecord, ...]:
        scope = binding.request_scope
        records: list[AsyncChildCompletionRecord] = []
        deadline = time.monotonic() + self._wait
        for child_id in await children_of(self._lineage, binding):
            execution = await self._service.reconcile(scope, child_id)
            while execution.lifecycle not in TERMINAL_LIFECYCLES and time.monotonic() < deadline:
                await asyncio.sleep(self._poll)
                execution = await self._service.reconcile(scope, child_id)
            link = await self._service.link(scope, child_id)
            if execution.lifecycle not in TERMINAL_LIFECYCLES:
                records.append(
                    self._record(
                        execution,
                        link.dependency_class,
                        None,
                        "unsettled",
                        reason=f"child_{execution.lifecycle.value}",
                    )
                )
                continue
            if link.result_decision is None and link.dependency_class in BLOCKING_CLASSES:
                rule = self._policies.get(link.result_admission_policy_ref)
                if rule is None:
                    records.append(
                        self._record(
                            execution,
                            link.dependency_class,
                            None,
                            "unsettled",
                            reason="admission_policy_not_registered",
                        )
                    )
                    continue
                link = await self._service.decide_result(
                    scope,
                    child_id,
                    rule(execution),
                    parent_open=True,
                    current_generation=execution_generation,
                    decided_at=self._clock(),
                )
            settled = await self._service.settle(
                scope, child_id, f"settlement:{child_id}:parent-boundary", self._clock()
            )
            records.append(
                self._record(
                    execution,
                    settled.dependency_class,
                    settled.result_decision,
                    "settled" if settled.settled else "pending_usage",
                )
            )
        return tuple(records)

    @staticmethod
    def _record(
        execution: AsyncSubagentExecution,
        dependency_class: AsyncSubagentDependencyClass,
        decision: str | None,
        disposition: str,
        *,
        reason: str | None = None,
    ) -> AsyncChildCompletionRecord:
        return AsyncChildCompletionRecord(
            child_execution_id=execution.child_execution_id,
            lifecycle=execution.lifecycle.value,
            dependency_class=dependency_class.value,
            result_decision=decision,
            usage_disposition=disposition,
            reason=reason,
        )


__all__ = [
    "AdmissionRule",
    "AsyncChildCompletion",
    "AsyncChildCompletionRecord",
    "admit_typed_manifest",
    "children_of",
]
