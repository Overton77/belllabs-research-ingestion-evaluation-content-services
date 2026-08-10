from __future__ import annotations

from typing import Protocol

from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingRepository,
)
from app.domain.coordinator.launch import (
    LaunchAuthorizationError,
    LaunchRequestContext,
    TerminalWorkflowCompletion,
    WorkflowResultRecord,
    WorkflowResultView,
)
from app.domain.run_control.contracts import RunProjection


class WorkflowResultRepository(Protocol):
    async def save(self, result: WorkflowResultRecord) -> WorkflowResultRecord: ...

    async def get(
        self,
        tenant_scope: str,
        request_scope: str,
        run_id: str,
    ) -> WorkflowResultRecord | None: ...


class RunProjectionPort(Protocol):
    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...


class TerminalWorkflowCompletionPort(Protocol):
    async def complete(
        self,
        completion: TerminalWorkflowCompletion,
    ) -> WorkflowResultRecord: ...


class InMemoryWorkflowResultRepository:
    def __init__(self) -> None:
        self._records: dict[tuple[str, str, str], WorkflowResultRecord] = {}

    async def save(self, result: WorkflowResultRecord) -> WorkflowResultRecord:
        key = (result.tenant_scope, result.request_scope, result.run_id)
        prior = self._records.get(key)
        if prior is not None and prior != result:
            raise ValueError("typed Workflow Result is immutable")
        self._records[key] = result
        return result

    async def get(
        self,
        tenant_scope: str,
        request_scope: str,
        run_id: str,
    ) -> WorkflowResultRecord | None:
        return self._records.get((tenant_scope, request_scope, run_id))


class CoordinatorResultService:
    def __init__(
        self,
        *,
        runs: RunProjectionPort,
        results: WorkflowResultRepository,
    ) -> None:
        self._runs = runs
        self._results = results

    async def get_workflow_result(
        self,
        run_id: str,
        context: LaunchRequestContext,
    ) -> WorkflowResultView:
        run = await self._runs.get_run(context.request_scope, run_id)
        if run.request_scope != context.request_scope:
            raise LaunchAuthorizationError("Workflow Run belongs to another request scope")
        result = await self._results.get(
            context.tenant_scope,
            context.request_scope,
            run_id,
        )
        if run.phase.value == "terminal" and result is None:
            raise RuntimeError("terminal Workflow Run has no durable typed result")
        if result is not None and result.request_scope != run.request_scope:
            raise LaunchAuthorizationError("typed Workflow Result belongs to another scope")
        return WorkflowResultView(
            run_id=run_id,
            request_scope=context.request_scope,
            phase=run.phase.value,
            result=result,
        )


class TerminalWorkflowCompletionService:
    """Converge terminal run state and one immutable typed result."""

    def __init__(
        self,
        *,
        runs: RunProjectionPort,
        results: WorkflowResultRepository,
        bindings: RunSemanticInputBindingRepository | None = None,
    ) -> None:
        self._runs = runs
        self._results = results
        self._bindings = bindings

    async def complete(
        self,
        completion: TerminalWorkflowCompletion,
    ) -> WorkflowResultRecord:
        run = await self._runs.get_run(completion.request_scope, completion.run_id)
        if run.request_scope != completion.request_scope:
            raise LaunchAuthorizationError("Workflow Run belongs to another request scope")
        if run.phase.value != "terminal" or run.terminal_outcome is None:
            raise ValueError("typed Workflow Result requires authoritative run terminality")
        if run.terminal_outcome != completion.terminal_outcome:
            raise ValueError("typed Workflow Result conflicts with the terminal run outcome")
        operation_binding_refs = completion.operation_binding_refs
        if self._bindings is not None:
            binding = await self._bindings.get_for_run(
                request_scope=completion.request_scope,
                run_id=completion.run_id,
            )
            if binding is None:
                raise ValueError("typed Workflow Result requires its semantic input binding")
            if (
                operation_binding_refs
                and operation_binding_refs != binding.operation_execution_binding_refs
            ):
                raise ValueError(
                    "typed Workflow Result operation bindings conflict with launch authority"
                )
            operation_binding_refs = binding.operation_execution_binding_refs
        result = WorkflowResultRecord(
            run_id=completion.run_id,
            tenant_scope=completion.tenant_scope,
            request_scope=completion.request_scope,
            blueprint_family=completion.blueprint_family,
            terminal_outcome=completion.terminal_outcome,
            output_contract_results=completion.output_contract_results,
            artifact_refs=completion.artifact_refs,
            evidence_refs=completion.evidence_refs,
            warnings=completion.warnings,
            degradations=completion.degradations,
            operation_binding_refs=operation_binding_refs,
            usage_summary=completion.usage_summary,
            family_result=completion.family_result,
            completed_at=completion.completed_at,
        )
        persisted = await self._results.save(result)
        reconciled = await self._runs.get_run(
            completion.request_scope,
            completion.run_id,
        )
        if (
            reconciled.phase.value != "terminal"
            or reconciled.terminal_outcome != completion.terminal_outcome
        ):
            raise RuntimeError("Workflow Run terminality changed during result materialization")
        return persisted
