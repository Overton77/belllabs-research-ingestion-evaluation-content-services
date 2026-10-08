"""Run control on the coordinator MCP server (SPEC-06 Interfaces).

- `mission_command_send(run_id, request)`: the `mc.command.v1` request the HTTP surface takes
  on `POST /runs/{id}/commands` (FT-F1: `queue_instruction`, `add_context`, plus the existing
  kinds), answered with the same `mc.command_receipt.v1` receipt.

Every tool calls the same application services as HTTP and CLI for the principal's verified
tenant scope, so the three surfaces answer one request identically. A principal from
another application is refused; nothing here widens a grant.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

from fastmcp import Context, FastMCP

from mission_control.application.missions.runtime import MissionControlRuntimeService
from mission_control.application.missions.service import MissionControlService
from mission_control.contracts.contracts import MissionCommandRequest, MissionControlRejected
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.errors import (
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)

COMMAND_SEND_TOOL = "mission_command_send"
RUN_CONTROL_TOOL_NAMES = (COMMAND_SEND_TOOL,)


class RunControlPrincipal(Protocol):
    @property
    def actor_id(self) -> str: ...

    @property
    def permissions(self) -> frozenset[str]: ...

    @property
    def request_scope(self) -> str: ...


_REJECTION_CODES: dict[str, CoordinatorErrorCode] = {
    "unauthorized": CoordinatorErrorCode.FORBIDDEN,
    "stale_version": CoordinatorErrorCode.CONFLICT,
    "stale_generation": CoordinatorErrorCode.CONFLICT,
    "unavailable": CoordinatorErrorCode.DEPENDENCY_UNAVAILABLE,
}


def domain_error(error: Exception) -> CoordinatorDomainError:
    """The MCP error envelope of a run-control rejection (the HTTP status's equivalent)."""

    if isinstance(error, MissionControlRejected):
        details = {"code": error.code}
        if error.frontier is not None:
            details.update({key: str(value) for key, value in error.frontier.items()})
        return CoordinatorDomainError(
            code=_REJECTION_CODES.get(error.code, CoordinatorErrorCode.INVALID_ARGUMENT),
            message=str(error),
            details=details,
        )
    if isinstance(error, RunControlNotFound):
        return CoordinatorDomainError(code=CoordinatorErrorCode.NOT_FOUND, message="run not found")
    if isinstance(error, IdempotencyConflict | RunVersionConflict):
        return CoordinatorDomainError(
            code=CoordinatorErrorCode.IDEMPOTENCY_CONFLICT, message="request conflict"
        )
    raise error


class ScopedRunControl:
    """Selects the run-control services of the principal's verified tenant scope."""

    def __init__(
        self,
        lifecycle: Mapping[str, MissionControlService],
        runtime: Mapping[str, MissionControlRuntimeService] | None = None,
    ) -> None:
        self._lifecycle = dict(lifecycle)
        self._runtime = dict(runtime or {})

    @staticmethod
    def _scope(principal: RunControlPrincipal) -> str:
        scope = principal.request_scope
        try:
            parse_request_scope(scope)
        except ValueError:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN,
                message="run control needs a canonical tenant request scope",
            ) from None
        return scope

    def lifecycle(self, principal: RunControlPrincipal) -> MissionControlService:
        service = self._lifecycle.get(self._scope(principal))
        if service is None:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="no run control for scope"
            )
        return service

    def runtime(self, principal: RunControlPrincipal) -> MissionControlRuntimeService:
        service = self._runtime.get(self._scope(principal))
        if service is None:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="no runtime control for scope"
            )
        return service

    @staticmethod
    def actor(principal: RunControlPrincipal) -> ActorContext:
        return ActorContext(
            actor_id=principal.actor_id,
            authority_refs=frozenset(),
            permissions=frozenset(principal.permissions),
        )

    async def send(
        self, principal: RunControlPrincipal, *, run_id: str, request: Mapping[str, Any]
    ) -> dict[str, object]:
        service = self.lifecycle(principal)
        try:
            receipt = await service.command(
                run_id, MissionCommandRequest.model_validate(dict(request)), self.actor(principal)
            )
        except (MissionControlRejected, RunControlNotFound, IdempotencyConflict) as error:
            raise domain_error(error) from None
        return receipt.model_dump(mode="json")


def register_run_control_tools(
    server: FastMCP,
    run_control: ScopedRunControl,
    principals: Any,
    *,
    call: Callable[..., Awaitable[dict[str, object]]],
) -> None:
    """Register the run-control tools; `call` wraps results in the MCP envelope."""

    @server.tool(name=COMMAND_SEND_TOOL)
    async def mission_command_send(
        run_id: str, request: dict[str, Any], context: Context
    ) -> dict[str, object]:
        async def invoke(principal: Any) -> object:
            return await run_control.send(principal, run_id=run_id, request=request)

        return await call(context, principals, invoke)


__all__ = [
    "COMMAND_SEND_TOOL",
    "RUN_CONTROL_TOOL_NAMES",
    "ScopedRunControl",
    "domain_error",
    "register_run_control_tools",
]
