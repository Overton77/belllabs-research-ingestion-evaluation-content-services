"""`lane.turn`, `lane.status` and `lane.cancel` Temporal activities (SPEC-07 section 4.1; FT-G2).

Registered on the `-agent-cognitive` queue beside `operation.execute` / `operation.cancel`
(kept as the Deep Agents fallback until FT-G6). A Session Lane (Cursor) is driven through
`LaneTurnService`; the Deep Agents lane runs the governed `operation.execute` body through
`lane.turn` unchanged (`OperationExecutionActivities.run_governed`), so its claim lease,
checkpoint lineage and cancel semantics stay exactly those the acceptance suites prove.

The heartbeat details carry `{cursor, frames_persisted}`; a retried attempt reads them back
as a hint, while the persisted frames remain the resume truth. A requested cancel
(`cancellation_details().cancel_requested`) reaches the provider; a worker shutdown, pause or
reset never does.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any, Final, TypeVar

from temporalio import activity
from temporalio.exceptions import ApplicationError

from mission_control.adapters.temporal.operation_activities import OperationExecutionActivities
from mission_control.application.context.lane_support import ContinuationInFlight
from mission_control.application.execution.harness.dispatch import (
    ProviderCapacityLimited,
    SessionOwnedElsewhere,
    StaleSessionOwner,
)
from mission_control.application.execution.harness.lane_turns import (
    LaneNotSessionDriven,
    LaneTurnService,
)
from mission_control.application.execution.harness.protocol import HarnessUnsupported
from mission_control.application.execution.harness.registry import (
    LaneNotQualified,
    UnknownLaneProfile,
)
from mission_control.application.execution.operations.operation_execution import (
    OperationExecutionInProgress,
)
from mission_control.domain.execution.checkpoint_lineage import OperationActivityAttempt
from mission_control.domain.execution.contracts import OperationExecutionResult
from mission_control.domain.execution.lane_turns import (
    ClosingFacts,
    LaneCancelRequest,
    LaneCancelResult,
    LaneStatusRequest,
    LaneStatusResult,
    LaneTurnRequest,
    LaneTurnResult,
    NativeStatus,
)
from mission_control.domain.execution.lanes import CancelReceipt, UsageReport
from mission_control.domain.policies.errors import IdempotencyConflict

_ResultT = TypeVar("_ResultT")
# MP-06 failure types the operation workflow reads (`provider_capacity_limit` carries the
# MP-05 `ProviderLimitSignal` as its first detail).
SESSION_OWNED_ELSEWHERE: Final = "session_owned_elsewhere"
STALE_SESSION_OWNER: Final = "stale_session_owner"
PROVIDER_CAPACITY_LIMIT: Final = "provider_capacity_limit"
_DA_NATIVE_STATUS: dict[str, NativeStatus] = {
    "completed": "finished",
    "failed": "error",
    "cancelled": "cancelled",
    "timed_out": "expired",
    "in_doubt": "in_doubt",
}


class TemporalTurnSignals:
    """`TurnSignals` over the running activity's context."""

    def heartbeat(self, cursor: str | None, frames_persisted: int) -> None:
        activity.heartbeat({"cursor": cursor, "frames_persisted": frames_persisted})

    def prior_cursor(self) -> str | None:
        details = activity.info().heartbeat_details
        if not details:
            return None
        last = details[-1]
        if isinstance(last, dict):
            cursor = last.get("cursor")
            return cursor if isinstance(cursor, str) and cursor else None
        return None

    def cancel_requested(self) -> bool:
        details = activity.cancellation_details()
        return details is not None and details.cancel_requested


def governed_closing_facts(result: OperationExecutionResult) -> ClosingFacts:
    """Closing facts of a Deep Agents unit read from its settled operation result."""

    amounts = result.usage.amounts
    usage = (
        UsageReport(
            disposition="settled",
            input_tokens=amounts.get("tokens.input", 0),
            output_tokens=amounts.get("tokens.output", 0),
            total_tokens=amounts.get("tokens.total", 0),
        )
        if amounts
        else UsageReport(disposition="unknown")
    )
    return ClosingFacts(
        native_status=_DA_NATIVE_STATUS[result.status],
        result_excerpt=result.output_text[:4_096],
        output_refs=result.output_refs,
        usage=usage,
        cost_disposition="estimated" if amounts else "unknown",
        error_code=(result.failure_code or None) and result.failure_code[:128],
    )


class LaneTurnActivities:
    def __init__(
        self,
        service: LaneTurnService,
        operations: OperationExecutionActivities,
    ) -> None:
        self._service = service
        self._operations = operations

    def _attempt(self) -> OperationActivityAttempt:
        info = activity.info()
        return OperationActivityAttempt(
            workflow_id=info.workflow_id,
            workflow_run_id=info.workflow_run_id,
            activity_id=info.activity_id,
            attempt=info.attempt,
            worker_identity=self._operations.worker_identity,
            lease_expires_at=(
                info.started_time + info.start_to_close_timeout
                if info.start_to_close_timeout is not None
                else None
            ),
        )

    async def _classified(self, body: Callable[[], Awaitable[_ResultT]]) -> _ResultT:
        try:
            return await body()
        except OperationExecutionInProgress as error:
            raise ApplicationError(str(error), type="operation_execution_in_progress") from error
        except SessionOwnedElsewhere as error:
            # MP-06: another worker's session manager holds a live lease; retried once it
            # can have expired (Temporal schedules the retry, nothing sleeps here).
            delay = error.lease_expires_at - datetime.now(UTC)
            raise ApplicationError(
                str(error),
                type=SESSION_OWNED_ELSEWHERE,
                next_retry_delay=max(delay, timedelta(seconds=1)),
            ) from error
        except StaleSessionOwner as error:
            # A takeover fenced this attempt out; its result must never be applied.
            raise ApplicationError(
                str(error), type=STALE_SESSION_OWNER, non_retryable=True
            ) from error
        except ContinuationInFlight as error:
            # MP-12: the session is frozen for a continuation; nothing was sent. Retried after
            # a short delay, by when the operation workflow has advanced or ended the transfer.
            raise ApplicationError(
                str(error), type=error.code, next_retry_delay=timedelta(seconds=5)
            ) from error
        except ProviderCapacityLimited as error:
            raise ApplicationError(
                str(error),
                error.signal.model_dump(mode="json"),
                type=PROVIDER_CAPACITY_LIMIT,
                non_retryable=True,
            ) from error
        except (
            LaneNotQualified,
            UnknownLaneProfile,
            LaneNotSessionDriven,
            HarnessUnsupported,
            IdempotencyConflict,
            ValueError,
        ) as error:
            raise ApplicationError(
                str(error), type="lane_turn_rejected", non_retryable=True
            ) from error

    @activity.defn(name="lane.turn")
    async def turn(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            request = LaneTurnRequest.model_validate(payload)
        except ValueError as error:
            raise ApplicationError(
                str(error), type="lane_turn_rejected", non_retryable=True
            ) from error
        if self._service.governed(request.lane_profile):
            settled = await self._operations.run_governed(request.operation.model_dump(mode="json"))
            result = OperationExecutionResult.model_validate(settled)
            return LaneTurnResult(
                done=True,
                segment_no=request.segment_no,
                closing_facts=governed_closing_facts(result),
                operation_result=settled,
            ).model_dump(mode="json")
        turned = await self._classified(
            lambda: self._service.turn(request, TemporalTurnSignals(), attempt=self._attempt())
        )
        return turned.model_dump(mode="json")

    @activity.defn(name="lane.status")
    async def status(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = LaneStatusRequest.model_validate(payload)
        if self._service.governed(request.lane_profile):
            settled = await self._operations.service.lane_settlement(request.operation)
            if settled is None:
                return LaneStatusResult(status="running", terminal=False).model_dump(mode="json")
            return LaneStatusResult(
                status=settled.status,
                terminal=True,
                settled=True,
                idle=True,
                operation_result=settled.model_dump(mode="json"),
            ).model_dump(mode="json")
        observed = await self._classified(lambda: self._service.status(request))
        return observed.model_dump(mode="json")

    @activity.defn(name="lane.cancel")
    async def cancel(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = LaneCancelRequest.model_validate(payload)
        if self._service.governed(request.lane_profile):
            # The RRM-008 settlement of a cancelled Deep Agents unit (`operation.cancel`).
            settled = await self._operations.run_governed(
                request.operation.model_dump(mode="json"), cancel=True
            )
            status = str(settled.get("status", "cancelled"))
            return LaneCancelResult(
                receipt=CancelReceipt(
                    acknowledged=True,
                    already_terminal=status != "cancelled",
                    native_status=status,
                ),
                settled=status != "in_doubt",
                operation_result=settled,
            ).model_dump(mode="json")
        cancelled = await self._classified(
            lambda: self._service.cancel(request, attempt=self._attempt())
        )
        return cancelled.model_dump(mode="json")


__all__ = [
    "PROVIDER_CAPACITY_LIMIT",
    "SESSION_OWNED_ELSEWHERE",
    "STALE_SESSION_OWNER",
    "LaneTurnActivities",
    "TemporalTurnSignals",
    "governed_closing_facts",
]
