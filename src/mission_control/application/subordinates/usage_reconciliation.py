"""Privileged reconciliation of an async child's pending usage (RRM-008 composed by RRM-009).

A cancelled child whose provider run did not attribute its usage leaves that usage pending
on the child's effect; the parent run stays `cancelling` until an operator with
`workflow_run.reconcile_async_child` records what the provider attributes (zero is a recorded
decision, never a default; REQ-CP-RUN-009). This is the production caller of
`AsyncSubagentService.reconcile_usage`: it is served by the API, never reaches the provider
(the attributed amounts are the operator's privileged statement, for example read from the
provider's durable thread state), and afterwards hints the cancelling family
(`liability_reconciled`) so it re-proposes terminalization at once instead of on its timer.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime

from mission_control.application.execution.liability_hints import FamilyLiabilityHints
from mission_control.application.subordinates.service import (
    AsyncSubagentError,
    AsyncSubagentService,
    ProviderAsyncObservation,
)
from mission_control.domain.execution.async_subagent_reconciliation import (
    AsyncProviderRunRecord,
    AsyncServedGraphIdentity,
    AsyncSpawnKeyObservation,
)
from mission_control.domain.execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentExecution,
    AsyncSubagentMessage,
    AsyncSubagentUsage,
    ParentAsyncSubagentLink,
)
from mission_control.domain.policies.contracts import ActorContext


class ProviderNotComposed:
    """The API's `AsyncSubagentProviderPort`: usage reconciliation never calls the provider,
    so the API holds no Agent Server credential and refuses every provider call."""

    def _refuse(self) -> AsyncSubagentError:
        return AsyncSubagentError("the provider is not composed for usage reconciliation")

    async def verify_served_graph(
        self, contract: AsyncSubagentContract
    ) -> AsyncServedGraphIdentity:
        raise self._refuse()

    async def start(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution, objective: str
    ) -> ProviderAsyncObservation:
        raise self._refuse()

    async def observe_spawn_key(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> AsyncSpawnKeyObservation:
        raise self._refuse()

    async def check(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation:
        raise self._refuse()

    async def update(
        self,
        contract: AsyncSubagentContract,
        execution: AsyncSubagentExecution,
        message: AsyncSubagentMessage,
    ) -> ProviderAsyncObservation:
        raise self._refuse()

    async def cancel(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation:
        raise self._refuse()

    async def cancel_run(
        self, contract: AsyncSubagentContract, execution: AsyncSubagentExecution, run_id: str
    ) -> AsyncProviderRunRecord:
        raise self._refuse()

    async def list(
        self, executions: tuple[tuple[AsyncSubagentContract, AsyncSubagentExecution], ...]
    ) -> tuple[ProviderAsyncObservation, ...]:
        raise self._refuse()


class AsyncChildUsageReconciliation:
    def __init__(
        self, service: AsyncSubagentService, hints: FamilyLiabilityHints | None = None
    ) -> None:
        self._service = service
        self._hints = hints

    async def reconcile_usage(
        self,
        request_scope: str,
        run_id: str,
        child_execution_id: str,
        *,
        actor: ActorContext,
        run_usage: Mapping[str, AsyncSubagentUsage],
        settlement_ref: str,
        reconciled_at: datetime,
    ) -> tuple[ParentAsyncSubagentLink, bool]:
        """Reconcile, then hint the family; returns the link and whether the hint was sent."""

        execution = await self._service.execution(request_scope, child_execution_id)
        if execution.parent_run_id != run_id:
            raise AsyncSubagentError("the async child belongs to another run")
        link = await self._service.reconcile_usage(
            request_scope,
            child_execution_id,
            actor=actor,
            run_usage=run_usage,
            settlement_ref=settlement_ref,
            reconciled_at=reconciled_at,
        )
        hinted = False
        if self._hints is not None and link.settled:
            hinted = await self._hints.notify(
                request_scope, run_id, f"async-child-usage:{child_execution_id}"
            )
        return link, hinted


__all__ = ["AsyncChildUsageReconciliation", "ProviderNotComposed"]
