from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, Protocol

from pydantic import ValidationError

from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingRepository,
    SemanticInputBindingNotFound,
)
from app.domain.operation_execution.contracts import OperationExecutionBinding
from app.domain.orchestration.bindings import (
    RunSemanticInputBinding,
    SemanticHandlerBinding,
)
from app.domain.orchestration.contracts import (
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationRequest,
    WorkflowEvaluationResult,
)


class SemanticRoutingError(ValueError):
    """A durable activity could not resolve an exact admitted semantic route."""


class OperationExecutionBindingReader(Protocol):
    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None: ...


class StageSemanticHandler(Protocol):
    async def execute(
        self,
        request: StageOperationRequest,
        binding: SemanticHandlerBinding,
    ) -> StageOperationResult: ...


class WorkflowEvaluationSemanticHandler(Protocol):
    async def evaluate(
        self,
        request: WorkflowEvaluationRequest,
        binding: SemanticHandlerBinding,
    ) -> WorkflowEvaluationResult: ...


class SemanticHandlerRegistry:
    """Process-local implementations addressed only by exact cataloged revisions."""

    def __init__(self) -> None:
        self._stage: dict[str, StageSemanticHandler] = {}
        self._workflow_evaluator: dict[str, WorkflowEvaluationSemanticHandler] = {}

    def register_stage(
        self,
        handler_id: str,
        revision: int,
        handler: StageSemanticHandler,
    ) -> None:
        self._register(self._stage, handler_id, revision, handler)

    def register_workflow_evaluator(
        self,
        handler_id: str,
        revision: int,
        handler: WorkflowEvaluationSemanticHandler,
    ) -> None:
        self._register(self._workflow_evaluator, handler_id, revision, handler)

    def stage(self, binding: SemanticHandlerBinding) -> StageSemanticHandler:
        return self._resolve(self._stage, binding)

    def workflow_evaluator(
        self,
        binding: SemanticHandlerBinding,
    ) -> WorkflowEvaluationSemanticHandler:
        return self._resolve(self._workflow_evaluator, binding)

    @staticmethod
    def _register(
        handlers: dict[str, Any],
        handler_id: str,
        revision: int,
        handler: object,
    ) -> None:
        if not handler_id or revision < 1:
            raise ValueError("semantic handler registration requires exact identity")
        key = f"{handler_id}@{revision}"
        if key in handlers:
            raise ValueError(f"semantic handler is already registered: {key}")
        handlers[key] = handler

    @staticmethod
    def _resolve(
        handlers: dict[str, Any],
        binding: SemanticHandlerBinding,
    ) -> Any:
        handler = handlers.get(binding.exact_handler_ref)
        if handler is None:
            raise SemanticRoutingError(f"unknown semantic handler: {binding.exact_handler_ref}")
        return handler


class BoundStageOperationExecutor:
    def __init__(
        self,
        repository: RunSemanticInputBindingRepository,
        registry: SemanticHandlerRegistry,
        operation_bindings: OperationExecutionBindingReader | None = None,
    ) -> None:
        self._repository = repository
        self._registry = registry
        self._operation_bindings = operation_bindings

    async def execute(self, request: StageOperationRequest) -> StageOperationResult:
        binding = await _load(
            self._repository,
            binding_ref=request.semantic_input_binding_ref,
            request_scope=request.request_scope,
            run_id=request.identity.run_id,
            family="StageGraph",
            effective_configuration_digest=request.effective_configuration_digest,
            blueprint_digest=request.blueprint_digest,
        )
        route = next(
            (
                item.handler
                for item in binding.stage_handlers
                if item.stage_id == request.identity.stage_id
            ),
            None,
        )
        if route is None:
            raise SemanticRoutingError(
                f"no semantic handler is bound for stage: {request.identity.stage_id}"
            )
        await _verify_operation_authority(
            self._operation_bindings,
            route,
            request_scope=request.request_scope,
            run_id=request.identity.run_id,
            effective_configuration_digest=request.effective_configuration_digest,
            operation_id=request.identity.stage_id,
        )
        result = await _validated_handler_call(
            self._registry.stage(route).execute(request, route),
            route,
        )
        if result.identity != request.identity:
            raise SemanticRoutingError(
                "stage semantic handler returned a mismatched execution identity"
            )
        if result.output_contract_ref != route.output_contract_ref:
            raise SemanticRoutingError(
                "stage semantic handler returned a different output contract"
            )
        return result


class BoundWorkflowEvaluator:
    def __init__(
        self,
        repository: RunSemanticInputBindingRepository,
        registry: SemanticHandlerRegistry,
        operation_bindings: OperationExecutionBindingReader | None = None,
    ) -> None:
        self._repository = repository
        self._registry = registry
        self._operation_bindings = operation_bindings

    async def evaluate(
        self,
        request: WorkflowEvaluationRequest,
    ) -> WorkflowEvaluationResult:
        binding = await _load(
            self._repository,
            binding_ref=request.semantic_input_binding_ref,
            request_scope=request.request_scope,
            run_id=request.run_id,
            family="StageGraph",
            effective_configuration_digest=request.effective_configuration_digest,
            blueprint_digest=request.blueprint_digest,
        )
        route = binding.workflow_evaluator
        if route is None:
            raise SemanticRoutingError("StageGraph requires an explicitly bound workflow evaluator")
        await _verify_operation_authority(
            self._operation_bindings,
            route,
            request_scope=request.request_scope,
            run_id=request.run_id,
            effective_configuration_digest=request.effective_configuration_digest,
            operation_id="workflow_evaluator",
        )
        result = await _validated_handler_call(
            self._registry.workflow_evaluator(route).evaluate(request, route),
            route,
        )
        if result.evaluation_contract_ref != request.evaluation_contract_ref:
            raise SemanticRoutingError(
                "workflow evaluator returned a result for a different frozen contract"
            )
        if result.output_contract_ref != route.output_contract_ref:
            raise SemanticRoutingError("workflow evaluator returned a different output contract")
        return result


async def _verify_operation_authority(
    repository: OperationExecutionBindingReader | None,
    route: SemanticHandlerBinding,
    *,
    request_scope: str,
    run_id: str,
    effective_configuration_digest: str,
    operation_id: str,
) -> None:
    binding_ref = route.operation_execution_binding_ref
    if binding_ref is None:
        return
    if repository is None:
        raise SemanticRoutingError(
            "semantic handler requires an Operation Execution Binding repository"
        )
    binding = await repository.get_binding_by_id(
        binding_ref,
        request_scope=request_scope,
    )
    if binding is None:
        raise SemanticRoutingError("semantic handler Operation Execution Binding is unavailable")
    if (
        binding.request_scope != request_scope
        or binding.run_id != run_id
        or binding.effective_configuration_digest != effective_configuration_digest
        or binding.operation_id != operation_id
    ):
        raise SemanticRoutingError(
            "semantic handler Operation Execution Binding differs from run authority"
        )


async def _load(
    repository: RunSemanticInputBindingRepository,
    *,
    binding_ref: str,
    request_scope: str,
    run_id: str,
    family: str,
    effective_configuration_digest: str,
    blueprint_digest: str,
) -> RunSemanticInputBinding:
    if not all(
        (
            binding_ref,
            request_scope,
            run_id,
            effective_configuration_digest,
            blueprint_digest,
        )
    ):
        raise SemanticRoutingError("semantic activity request lacks its run-scoped input binding")
    binding = await repository.get(
        binding_ref,
        request_scope=request_scope,
        run_id=run_id,
    )
    if binding is None:
        raise SemanticInputBindingNotFound(f"semantic input binding was not found: {binding_ref}")
    if binding.blueprint_family != family:
        raise SemanticRoutingError(f"semantic input binding family mismatch: expected {family}")
    if (
        binding.effective_configuration_digest != effective_configuration_digest
        or binding.blueprint_digest != blueprint_digest
    ):
        raise SemanticRoutingError(
            "semantic input binding does not match the admitted configuration and blueprint"
        )
    return binding


async def _validated_handler_call[HandlerResultT](
    operation: Awaitable[HandlerResultT],
    binding: SemanticHandlerBinding,
) -> HandlerResultT:
    try:
        return await operation
    except ValidationError as error:
        raise SemanticRoutingError(
            f"semantic input does not satisfy handler schema: {binding.exact_handler_ref}"
        ) from error
