"""Temporal Visibility reader for inspection (REQ-CP-EXEC-015).

It lists a run's executions through the Visibility API by the BellLabs Search
Attributes, filtered by run ID and scope hash. It never queries Temporal's persistence
database and never sends a workflow Query: Visibility rows are qualified runtime
evidence, not authority. A failure or timeout surfaces as `RuntimeSourceUnavailable`, so
inspection degrades the section instead of failing the persisted read.
"""

from __future__ import annotations

from datetime import UTC, timedelta
from typing import Any

from temporalio.api.workflowservice.v1 import ListWorkflowExecutionsRequest
from temporalio.client import Client
from temporalio.converter import decode_typed_search_attributes

from mission_control.adapters.temporal.search_attributes import visible_values
from mission_control.application.execution.inspection import RuntimeSourceUnavailable
from mission_control.application.frames.search import RunListUnavailable, VisibilityExecution
from mission_control.domain.policies.inspection import TemporalExecution
from mission_control.domain.programs.search_attributes import (
    EXECUTION_GENERATION,
    FORKED_FROM_RUN_ID,
    MC_LANE,
    MC_MISSION_ID,
    MC_PHASE,
    MC_RUN_ID,
    RUN_ID,
    UNIT_KEY,
    WORKFLOW_KIND,
    visibility_run_query,
)

DEFAULT_RPC_TIMEOUT = timedelta(seconds=5)
MAX_RUN_EXECUTIONS = 1_000


class TemporalVisibilityInspectionReader:
    def __init__(
        self,
        client: Client,
        *,
        rpc_timeout: timedelta = DEFAULT_RPC_TIMEOUT,
        limit: int = MAX_RUN_EXECUTIONS,
    ) -> None:
        self._client = client
        self._rpc_timeout = rpc_timeout
        self._limit = limit

    async def list_run_executions(
        self, request_scope: str, run_id: str
    ) -> tuple[TemporalExecution, ...]:
        executions: list[TemporalExecution] = []
        try:
            async for item in self._client.list_workflows(
                visibility_run_query(run_id, request_scope),
                limit=self._limit,
                rpc_timeout=self._rpc_timeout,
            ):
                values = visible_values(item.typed_search_attributes)
                generation = values.get(EXECUTION_GENERATION)
                unit_key = values.get(UNIT_KEY)
                kind = values.get(WORKFLOW_KIND)
                executions.append(
                    TemporalExecution(
                        workflow_id=item.id,
                        temporal_run_id=item.run_id,
                        workflow_type=item.workflow_type,
                        status=item.status.name if item.status is not None else "UNKNOWN",
                        workflow_kind=str(kind) if kind is not None else None,
                        unit_key=str(unit_key) if unit_key is not None else None,
                        execution_generation=(
                            int(generation) if isinstance(generation, int) else None
                        ),
                        started_at=item.start_time,
                        closed_at=item.close_time,
                    )
                )
        except Exception as error:
            raise RuntimeSourceUnavailable("Temporal Visibility is unavailable") from error
        return tuple(sorted(executions, key=lambda item: (item.workflow_id, item.temporal_run_id)))


# FT-C4: run list over the fast-track typed Search Attributes. The raw Visibility API is
# used (not `Client.list_workflows`) so an execution whose status the Python SDK enum does
# not know (`PAUSED` on newer servers) is decoded defensively instead of failing the page.
_STATUS_NAMES = {
    0: "UNSPECIFIED",
    1: "RUNNING",
    2: "COMPLETED",
    3: "FAILED",
    4: "CANCELED",
    5: "TERMINATED",
    6: "CONTINUED_AS_NEW",
    7: "TIMED_OUT",
    8: "PAUSED",
}
LIST_PAGE_SIZE = 500


class TemporalRunVisibility:
    """Implements the ``RunVisibility`` port over the namespace's Visibility store."""

    def __init__(self, client: Client, *, rpc_timeout: timedelta = DEFAULT_RPC_TIMEOUT) -> None:
        self._client = client
        self._rpc_timeout = rpc_timeout

    async def list(self, query: str, *, limit: int) -> tuple[VisibilityExecution, ...]:
        rows: list[VisibilityExecution] = []
        token = b""
        try:
            while len(rows) < limit:
                response = await self._client.workflow_service.list_workflow_executions(
                    ListWorkflowExecutionsRequest(
                        namespace=self._client.namespace,
                        page_size=min(LIST_PAGE_SIZE, limit - len(rows)),
                        next_page_token=token,
                        query=query,
                    ),
                    timeout=self._rpc_timeout,
                )
                for info in response.executions:
                    row = _execution(info)
                    if row is not None:
                        rows.append(row)
                token = response.next_page_token
                if not token:
                    break
        except Exception as error:
            raise RunListUnavailable("Temporal Visibility is unavailable") from error
        return tuple(rows[:limit])


def _execution(info: Any) -> VisibilityExecution | None:
    attributes = decode_typed_search_attributes(info.search_attributes)
    values = visible_values(attributes)
    run_key = values.get(RUN_ID)
    if not isinstance(run_key, str):
        listed = values.get(MC_RUN_ID)
        run_key = listed[0] if isinstance(listed, list) and listed else None
    if not isinstance(run_key, str):
        return None
    kind = values.get(WORKFLOW_KIND)
    forked = values.get(FORKED_FROM_RUN_ID)
    return VisibilityExecution(
        workflow_id=info.execution.workflow_id,
        run_key=run_key,
        workflow_kind=str(kind) if kind is not None else None,
        status=_STATUS_NAMES.get(int(info.status), f"STATUS_{int(info.status)}"),
        mission_id=_text(values.get(MC_MISSION_ID)),
        lane=_text(values.get(MC_LANE)),
        phase=_text(values.get(MC_PHASE)),
        forked_from=tuple(forked) if isinstance(forked, list) else (),
        started_at=info.start_time.ToDatetime(tzinfo=UTC) if info.HasField("start_time") else None,
        closed_at=info.close_time.ToDatetime(tzinfo=UTC) if info.HasField("close_time") else None,
    )


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None
