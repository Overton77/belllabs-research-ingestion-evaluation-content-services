"""Temporal wake-up hint for a waiting Human Gate control activation (MP-10)."""

from __future__ import annotations

from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

from mission_control.domain.programs.human_gate import HumanTaskView, human_gate_workflow_id

RESOLUTION_COMMITTED_SIGNAL = "resolution_committed"


class TemporalHumanGateWake:
    """Signals `resolution_committed` after the resolution commit; never authority."""

    def __init__(self, client: Client) -> None:
        self._client = client

    async def resolution_committed(self, task: HumanTaskView) -> None:
        handle = self._client.get_workflow_handle(human_gate_workflow_id(task.human_task_id))
        try:
            await handle.signal(RESOLUTION_COMMITTED_SIGNAL, task.human_task_id)
        except RPCError as error:
            # The activation already completed (or never started): nothing waits on it.
            if error.status != RPCStatusCode.NOT_FOUND:
                raise


__all__ = ["RESOLUTION_COMMITTED_SIGNAL", "TemporalHumanGateWake"]
