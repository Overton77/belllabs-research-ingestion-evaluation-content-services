from __future__ import annotations

from pathlib import Path
from typing import Literal, Protocol

from app.application.operation_execution import OperationExecutionService
from app.application.schema_context_selection import AgentRunOutput
from app.domain.operation_execution.contracts import (
    OperationExecutionRequest,
    OperationExecutionResult,
)
from app.domain.schema_context.contracts import (
    SchemaContextSelection,
    SchemaSelectionReview,
)
from app.domain.schema_grounding.contracts import BoundedQueryPlan

SCHEMA_SELECTOR_CONTRACT = "operation-contract:schema-context-selector:v1"
SCHEMA_REVIEWER_CONTRACT = "operation-contract:schema-context-reviewer:v1"
GRAPH_RECONCILIATION_PLANNER_CONTRACT = (
    "operation-contract:supporting-graph-reconciliation-planner:v1"
)

SchemaAgentRole = Literal["selector", "reviewer", "query_planner"]


class SchemaAgentOperationRequestFactory(Protocol):
    async def create(
        self,
        *,
        role: SchemaAgentRole,
        run_root: Path,
        revision_feedback: str | None = None,
        retry_reason: str | None = None,
    ) -> OperationExecutionRequest: ...


class GovernedSchemaAgentRuntime:
    """Adapter that routes semantic schema work through immutable F4 bindings and budgets."""

    def __init__(
        self,
        service: OperationExecutionService,
        requests: SchemaAgentOperationRequestFactory,
    ) -> None:
        self._service = service
        self._requests = requests

    async def select(
        self,
        run_root: Path,
        *,
        revision_feedback: str | None = None,
    ) -> AgentRunOutput:
        result = await self._execute(
            role="selector",
            run_root=run_root,
            expected_contract=SCHEMA_SELECTOR_CONTRACT,
            revision_feedback=revision_feedback,
        )
        return AgentRunOutput(
            output=SchemaContextSelection.model_validate(result.structured_output),
            usage=dict(result.usage.amounts),
        )

    async def review(
        self,
        run_root: Path,
        *,
        retry_reason: str | None = None,
    ) -> AgentRunOutput:
        result = await self._execute(
            role="reviewer",
            run_root=run_root,
            expected_contract=SCHEMA_REVIEWER_CONTRACT,
            retry_reason=retry_reason,
        )
        return AgentRunOutput(
            output=SchemaSelectionReview.model_validate(result.structured_output),
            usage=dict(result.usage.amounts),
        )

    async def plan_queries(
        self,
        run_root: Path,
        *,
        retry_reason: str | None = None,
    ) -> AgentRunOutput:
        result = await self._execute(
            role="query_planner",
            run_root=run_root,
            expected_contract=GRAPH_RECONCILIATION_PLANNER_CONTRACT,
            retry_reason=retry_reason,
        )
        return AgentRunOutput(
            output=BoundedQueryPlan.model_validate(result.structured_output),
            usage=dict(result.usage.amounts),
        )

    async def _execute(
        self,
        *,
        role: SchemaAgentRole,
        run_root: Path,
        expected_contract: str,
        revision_feedback: str | None = None,
        retry_reason: str | None = None,
    ) -> OperationExecutionResult:
        request = await self._requests.create(
            role=role,
            run_root=run_root,
            revision_feedback=revision_feedback,
            retry_reason=retry_reason,
        )
        if request.operation_contract_ref != expected_contract:
            raise ValueError(
                f"{role} operation is not bound to the canonical contract {expected_contract}"
            )
        result = await self._service.execute(request)
        if result.status != "completed" or result.structured_output is None:
            raise RuntimeError(
                f"governed {role} operation did not produce admitted structured output"
            )
        return result
