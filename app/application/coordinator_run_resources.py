from __future__ import annotations

from app.application.coordinator_results import RunProjectionPort
from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingRepository,
    SemanticInputBindingNotFound,
)


class CoordinatorRunResourceService:
    """Authorized compact launch and exact semantic-binding run resources."""

    def __init__(
        self,
        *,
        runs: RunProjectionPort,
        bindings: RunSemanticInputBindingRepository,
    ) -> None:
        self._runs = runs
        self._bindings = bindings

    async def launch(self, request_scope: str, run_id: str) -> object:
        run = await self._runs.get_run(request_scope, run_id)
        if run.request_scope != request_scope:
            raise LookupError("Workflow Run was not found")
        payload = run.model_dump(mode="json")
        return {
            "run": payload,
            "resource_uris": {
                "result": f"belllabs://runs/{run_id}/result",
                "bindings": f"belllabs://runs/{run_id}/bindings",
            },
        }

    async def bindings(self, request_scope: str, run_id: str) -> object:
        run = await self._runs.get_run(request_scope, run_id)
        if run.request_scope != request_scope:
            raise LookupError("Workflow Run was not found")
        binding = await self._bindings.get_for_run(
            request_scope=request_scope,
            run_id=run_id,
        )
        if binding is None:
            raise SemanticInputBindingNotFound("Workflow Run semantic input binding was not found")
        return {
            "semantic_input_binding": binding.model_dump(mode="json"),
            "operation_execution_binding_refs": list(binding.operation_execution_binding_refs),
        }
