from __future__ import annotations

from datetime import datetime
from typing import Protocol

from pydantic import BaseModel, ConfigDict, model_validator

from app.application.operations.operation_execution import (
    bind_operation_execution_request,
)
from app.domain.coordinator.launch import PreparedLaunchTicket
from app.domain.operation_execution.contracts import (
    OperationAttemptIdentity,
    OperationExecutionBinding,
    OperationExecutionRequest,
)


class SemanticOperationBindingTemplates(BaseModel):
    """Exact pre-admission OER templates keyed by semantic operation identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    operations: dict[str, OperationExecutionRequest]

    @model_validator(mode="after")
    def keys_match_operation_identity(self) -> SemanticOperationBindingTemplates:
        if not self.operations:
            raise ValueError("semantic operation binding templates cannot be empty")
        if any(
            operation_id != request.identity.operation_id
            for operation_id, request in self.operations.items()
        ):
            raise ValueError("semantic operation template keys must match their operation ids")
        return self


class SemanticOperationExecutionBindingService:
    """Persist real OEB documents after admission and before Temporal dispatch."""

    def __init__(self, repository: SemanticOperationBindingRepository) -> None:
        self._repository = repository

    async def freeze(
        self,
        templates: SemanticOperationBindingTemplates,
        ticket: PreparedLaunchTicket,
        *,
        run_id: str,
        bound_at: datetime,
    ) -> dict[str, str]:
        refs: dict[str, str] = {}
        for operation_id, template in sorted(templates.operations.items()):
            request = _bind_request(
                template,
                ticket,
                run_id=run_id,
                bound_at=bound_at,
            )
            expected = bind_operation_execution_request(request)
            persisted = await self._repository.create_binding(
                expected,
                request_scope=ticket.request_scope,
            )
            if persisted != expected:
                raise ValueError("persisted Operation Execution Binding differs from frozen intent")
            refs[operation_id] = persisted.binding_id
        return refs


class SemanticOperationBindingRepository(Protocol):
    async def get_binding_by_id(
        self,
        binding_id: str,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding | None: ...

    async def create_binding(
        self,
        binding: OperationExecutionBinding,
        *,
        request_scope: str,
    ) -> OperationExecutionBinding: ...


def _bind_request(
    template: OperationExecutionRequest,
    ticket: PreparedLaunchTicket,
    *,
    run_id: str,
    bound_at: datetime,
) -> OperationExecutionRequest:
    workspace = template.workspace.model_copy(
        update={
            "namespace_id": template.workspace.namespace_id.replace("{run_id}", run_id),
            "workspace_id": template.workspace.workspace_id.replace("{run_id}", run_id),
        }
    )
    return template.model_copy(
        update={
            "identity": OperationAttemptIdentity(
                run_id=run_id,
                operation_id=template.identity.operation_id,
                operation_attempt=template.identity.operation_attempt,
            ),
            "request_scope": ticket.request_scope,
            "effective_configuration_digest": ticket.effective_configuration_digest,
            "workspace": workspace,
            "budget_reservation_id": template.budget_reservation_id.replace("{run_id}", run_id),
            "prior_binding_id": None,
            "requested_at": bound_at,
            "idempotency_key": template.idempotency_key.replace("{run_id}", run_id),
        }
    )


__all__ = [
    "SemanticOperationBindingTemplates",
    "SemanticOperationExecutionBindingService",
]
