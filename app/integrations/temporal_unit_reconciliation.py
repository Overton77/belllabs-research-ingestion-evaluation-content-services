"""Temporal delivery of the `reconcile_unit` wake-up hint to a parked `OperationWorkflow`.

The signal carries only the accepted decision's identity. The workflow treats it as a hint
to re-run classification; authority stays in PostgreSQL (REQ-CP-EXEC-007). The governed
receipt ledger for command delivery is RRM-007's; this adapter is the minimal transport.
"""

from __future__ import annotations

from temporalio.client import Client

UNIT_RECONCILIATION_SIGNAL = "unit_reconciliation_recorded"


class TemporalUnitReconciliationNudge:
    def __init__(self, client: Client) -> None:
        self._client = client

    async def nudge(self, *, operation_workflow_id: str, decision_id: str) -> None:
        await self._client.get_workflow_handle(operation_workflow_id).signal(
            UNIT_RECONCILIATION_SIGNAL, decision_id
        )
