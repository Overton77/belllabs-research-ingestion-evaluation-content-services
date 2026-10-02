"""Temporal Visibility reader for inspection (REQ-CP-EXEC-015).

It lists a run's executions through the Visibility API by the BellLabs Search
Attributes, filtered by run ID and scope hash. It never queries Temporal's persistence
database and never sends a workflow Query: Visibility rows are qualified runtime
evidence, not authority. A failure or timeout surfaces as `RuntimeSourceUnavailable`, so
inspection degrades the section instead of failing the persisted read.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.client import Client

from app.application.run_control.inspection import RuntimeSourceUnavailable
from app.domain.orchestration.search_attributes import (
    EXECUTION_GENERATION,
    UNIT_KEY,
    WORKFLOW_KIND,
    visibility_run_query,
)
from app.domain.run_control.inspection import TemporalExecution
from app.temporal.search_attributes import visible_values

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
        except Exception as error:  # noqa: BLE001 - Visibility outages degrade the section
            raise RuntimeSourceUnavailable("Temporal Visibility is unavailable") from error
        return tuple(sorted(executions, key=lambda item: (item.workflow_id, item.temporal_run_id)))
