"""The `liability_reconciled` hint to a cancelling family (RRM-008 composed by RRM-009).

While a run is `cancelling`, its family's saga proposes terminalization and the reducer
refuses it as long as a liability remains: pending child usage, an unsettled effect, an
unresolved child, an operator wait (REQ-CP-EXEC-008 step 5, REQ-CP-RUN-009/010). The family
retries on a doubling timer (30 s up to one hour) or when it receives the compact
`liability_reconciled` signal. This service sends that signal after an operator decision
that may have resolved a liability: the privileged usage reconciliation of an async child,
an accepted `reconcile_unit`, or an accepted operator settlement command.

The hint carries a reference only and releases nothing: authority stays in run control and
the family re-proposes terminalization, which the reducer decides (REQ-CP-EXEC-007). A hint
that cannot be sent (the family is not running, Temporal is down) is not an error of the
decision; the family's timer remains the fallback.
"""

from __future__ import annotations

import logging
from typing import Protocol

from mission_control.application.execution.service import RunControlService
from mission_control.domain.policies.contracts import RunPhase

logger = logging.getLogger(__name__)

LIABILITY_RECONCILED_SIGNAL = "liability_reconciled"
# Accepted operator commands (`/run-control/v1/runs/{run_id}/commands`) that can resolve a
# liability a cancelling family waits on.
LIABILITY_DECISION_KINDS = frozenset(
    {
        "settle_effect",
        "settle_pending_usage",
        "record_usage",
        "record_async_child_fact",
        "decide_async_child_fact",
    }
)


class FamilyLiabilityHintTransport(Protocol):
    async def liability_reconciled(self, family_workflow_id: str, reference: str) -> None: ...


class FamilyLiabilityHints:
    def __init__(self, run_control: RunControlService, transport: FamilyLiabilityHintTransport):
        self._run_control = run_control
        self._transport = transport

    async def notify(self, request_scope: str, run_id: str, reference: str) -> bool:
        """Signal the run's family if the run is cancelling; True when the hint was sent."""

        run = await self._run_control.get_run(request_scope, run_id)
        if run.phase != RunPhase.CANCELLING or run.execution_target is None:
            return False
        family = run.execution_target.family_workflow_id
        try:
            await self._transport.liability_reconciled(family, reference)
        except Exception:  # noqa: BLE001 - a hint; the family's timer is the fallback
            logger.warning(
                "liability hint not delivered",
                extra={"run_id": run_id, "family_workflow_id": family},
                exc_info=True,
            )
            return False
        return True


__all__ = [
    "LIABILITY_DECISION_KINDS",
    "LIABILITY_RECONCILED_SIGNAL",
    "FamilyLiabilityHintTransport",
    "FamilyLiabilityHints",
]
