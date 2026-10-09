"""Human Tasks on the coordinator MCP server (SPEC-03 parity with HTTP, MP-10).

- `mission_human_task_list(run_id?, lifecycle?)` and `mission_human_task_get(human_task_id)`:
  the bodies `GET /human-tasks` and `GET /human-tasks/{id}` return, read-only.
- `mission_human_task_resolve(human_task_id, request)`: the `POST
  /human-tasks/{id}/resolutions` body, answered with the same receipt.

Every tool calls the one `HumanTaskService` of the principal's verified tenant scope;
reviewer authorization is the service's. This is not MCP elicitation: the client answers a
durable Human Task through an ordinary tool call under its own authenticated principal.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Protocol

from fastmcp import Context, FastMCP

from mission_control.application.human_tasks.service import HumanTaskRejected, HumanTaskService
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.programs.human_gate import HumanResolutionRequest

HUMAN_TASK_LIST_TOOL = "mission_human_task_list"
HUMAN_TASK_GET_TOOL = "mission_human_task_get"
HUMAN_TASK_RESOLVE_TOOL = "mission_human_task_resolve"
HUMAN_TASK_TOOL_NAMES = (HUMAN_TASK_GET_TOOL, HUMAN_TASK_LIST_TOOL, HUMAN_TASK_RESOLVE_TOOL)

_CODES: dict[str, CoordinatorErrorCode] = {
    "not_found": CoordinatorErrorCode.NOT_FOUND,
    "not_reviewer": CoordinatorErrorCode.FORBIDDEN,
    "stale_version": CoordinatorErrorCode.CONFLICT,
    "packet_digest_mismatch": CoordinatorErrorCode.CONFLICT,
    "already_resolved": CoordinatorErrorCode.CONFLICT,
    "task_expired": CoordinatorErrorCode.CONFLICT,
    "task_cancelled": CoordinatorErrorCode.CONFLICT,
    "deadline_passed": CoordinatorErrorCode.CONFLICT,
}


class HumanTaskPrincipal(Protocol):
    @property
    def actor_id(self) -> str: ...

    @property
    def permissions(self) -> frozenset[str]: ...

    @property
    def request_scope(self) -> str: ...


def domain_error(error: HumanTaskRejected) -> CoordinatorDomainError:
    return CoordinatorDomainError(
        code=_CODES.get(error.code, CoordinatorErrorCode.INVALID_ARGUMENT),
        message=str(error),
        details={"code": error.code},
    )


class ScopedHumanTasks:
    """Selects the Human Task service of the principal's verified tenant scope."""

    def __init__(self, services: Mapping[str, HumanTaskService]) -> None:
        self._services = dict(services)

    def service(self, principal: HumanTaskPrincipal) -> HumanTaskService:
        scope = principal.request_scope
        try:
            parse_request_scope(scope)
        except ValueError:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN,
                message="human tasks need a canonical tenant request scope",
            ) from None
        service = self._services.get(scope)
        if service is None:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="no human task service for scope"
            )
        return service

    @staticmethod
    def actor(principal: HumanTaskPrincipal) -> ActorContext:
        return ActorContext(
            actor_id=principal.actor_id, permissions=frozenset(principal.permissions)
        )

    async def list(
        self,
        principal: HumanTaskPrincipal,
        *,
        run_id: str | None = None,
        lifecycle: str | None = None,
    ) -> dict[str, object]:
        try:
            tasks = await self.service(principal).list(
                self.actor(principal), run_id=run_id, lifecycle=lifecycle
            )
        except HumanTaskRejected as error:
            raise domain_error(error) from None
        return {"human_tasks": [task.public() for task in tasks]}

    async def get(self, principal: HumanTaskPrincipal, *, human_task_id: str) -> dict[str, object]:
        try:
            task = await self.service(principal).get(human_task_id, self.actor(principal))
        except HumanTaskRejected as error:
            raise domain_error(error) from None
        return task.public()

    async def resolve(
        self,
        principal: HumanTaskPrincipal,
        *,
        human_task_id: str,
        request: Mapping[str, Any],
    ) -> dict[str, object]:
        body = HumanResolutionRequest.model_validate(dict(request))
        try:
            receipt = await self.service(principal).resolve(
                human_task_id, body, self.actor(principal)
            )
        except HumanTaskRejected as error:
            raise domain_error(error) from None
        return receipt.public()


def register_human_task_tools(
    server: FastMCP,
    human_tasks: ScopedHumanTasks,
    principals: Any,
    *,
    call: Callable[..., Awaitable[dict[str, object]]],
) -> None:
    """Register the Human Task tools; `call` wraps results in the MCP envelope."""

    @server.tool(name=HUMAN_TASK_LIST_TOOL, annotations={"readOnlyHint": True})
    async def mission_human_task_list(
        context: Context, run_id: str | None = None, lifecycle: str | None = None
    ) -> dict[str, object]:
        async def invoke(principal: Any) -> object:
            return await human_tasks.list(principal, run_id=run_id, lifecycle=lifecycle)

        return await call(context, principals, invoke)

    @server.tool(name=HUMAN_TASK_GET_TOOL, annotations={"readOnlyHint": True})
    async def mission_human_task_get(human_task_id: str, context: Context) -> dict[str, object]:
        async def invoke(principal: Any) -> object:
            return await human_tasks.get(principal, human_task_id=human_task_id)

        return await call(context, principals, invoke)

    @server.tool(name=HUMAN_TASK_RESOLVE_TOOL)
    async def mission_human_task_resolve(
        human_task_id: str, request: dict[str, Any], context: Context
    ) -> dict[str, object]:
        async def invoke(principal: Any) -> object:
            return await human_tasks.resolve(
                principal, human_task_id=human_task_id, request=request
            )

        return await call(context, principals, invoke)


__all__ = [
    "HUMAN_TASK_GET_TOOL",
    "HUMAN_TASK_LIST_TOOL",
    "HUMAN_TASK_RESOLVE_TOOL",
    "HUMAN_TASK_TOOL_NAMES",
    "ScopedHumanTasks",
    "domain_error",
    "register_human_task_tools",
]
