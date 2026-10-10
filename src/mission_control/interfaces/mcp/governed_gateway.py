"""Governed MCP gateway for Mission-Control-owned effect tools (SPEC-03, ADR-0038, MP-11).

Tools (an application protocol on the coordinator MCP server, not a standard MCP feature):

- ``mission_governed_prepare(tool_name, arguments, run_id, harness_execution_id, generation,
  lane_profile)`` persists the intent and, where policy requires review, returns
  ``pending_approval`` + the intent id + the Human Task reference **without executing**. A
  tool that asks for structured input negotiates MCP elicitation first: a client without the
  capability gets a typed ``elicitation_unsupported`` refusal unless the tool admits the
  durable ``review_arguments`` fallback (the gateway terminates the protocol and a human
  reviews the supplied arguments).
- ``mission_governed_execute(intent_id, arguments?)`` consumes the approval-bound intent once
  and returns its stable receipt; repeats return the pending state or the same receipt.
- ``mission_governed_status(intent_id)`` reads the intent.
- ``mission_approval_resolve(human_task_id, request)`` is the extended approval resolution body
  (``approve_edited``, answers, elicitation content, ``cancel``) through the one
  ``HumanTaskService`` (MCP parity with HTTP and the socket; reviewer authority is the
  service's, never this tool's).

Every tool resolves the service of the principal's verified tenant scope and needs
``workflow_run.claim_effect`` for prepare/execute/status.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final, Protocol

from fastmcp import Context, FastMCP

from mission_control.application.execution.approvals import ApprovalResolutionRequest
from mission_control.application.execution.approvals_governed import (
    ElicitedInput,
    GovernedEffectService,
    GovernedPrepareRequest,
    GovernedRejected,
)
from mission_control.application.human_tasks.service import HumanTaskRejected
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from mission_control.domain.policies.contracts import ActorContext
from mission_control.interfaces.mcp.governed_elicitation import elicit_from_client
from mission_control.interfaces.mcp.human_task_tools import ScopedHumanTasks, domain_error

GOVERNED_PREPARE_TOOL: Final = "mission_governed_prepare"
GOVERNED_EXECUTE_TOOL: Final = "mission_governed_execute"
GOVERNED_STATUS_TOOL: Final = "mission_governed_status"
APPROVAL_RESOLVE_TOOL: Final = "mission_approval_resolve"
GOVERNED_TOOL_NAMES: Final = (
    APPROVAL_RESOLVE_TOOL,
    GOVERNED_EXECUTE_TOOL,
    GOVERNED_PREPARE_TOOL,
    GOVERNED_STATUS_TOOL,
)

_CODES: Final[dict[str, CoordinatorErrorCode]] = {
    "unknown_tool": CoordinatorErrorCode.CAPABILITY_NOT_FOUND,
    "not_permitted": CoordinatorErrorCode.FORBIDDEN,
    "not_found": CoordinatorErrorCode.NOT_FOUND,
    "stop_fenced": CoordinatorErrorCode.CONFLICT,
    "arguments_changed": CoordinatorErrorCode.CONFLICT,
    "invalid_arguments": CoordinatorErrorCode.INVALID_ARGUMENT,
    "elicitation_unsupported": CoordinatorErrorCode.CAPABILITY_INCOMPATIBLE,
    "elicitation_mode_unsupported": CoordinatorErrorCode.CAPABILITY_INCOMPATIBLE,
    "gate_unenforceable": CoordinatorErrorCode.ADMISSION_REJECTED,
}
_NEGOTIATION_FAILURES: Final = frozenset(
    {"elicitation_unsupported", "elicitation_mode_unsupported"}
)


class GovernedPrincipal(Protocol):
    @property
    def actor_id(self) -> str: ...

    @property
    def permissions(self) -> frozenset[str]: ...

    @property
    def request_scope(self) -> str: ...


def governed_error(error: GovernedRejected) -> CoordinatorDomainError:
    return CoordinatorDomainError(
        code=_CODES.get(error.code, CoordinatorErrorCode.INVALID_ARGUMENT),
        message=str(error),
        details={"code": error.code},
    )


class ScopedGovernedEffects:
    """Selects the governed effect service of the principal's verified tenant scope."""

    def __init__(self, services: Mapping[str, GovernedEffectService]) -> None:
        self._services = dict(services)

    def service(self, principal: GovernedPrincipal) -> GovernedEffectService:
        scope = principal.request_scope
        try:
            parse_request_scope(scope)
        except ValueError:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN,
                message="governed effects need a canonical tenant request scope",
            ) from None
        service = self._services.get(scope)
        if service is None:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="no governed gateway for scope"
            )
        return service

    @staticmethod
    def actor(principal: GovernedPrincipal) -> ActorContext:
        return ActorContext(
            actor_id=principal.actor_id, permissions=frozenset(principal.permissions)
        )

    async def prepare(
        self,
        principal: GovernedPrincipal,
        request: GovernedPrepareRequest,
        *,
        context: Context | None = None,
    ) -> dict[str, object]:
        service = self.service(principal)
        try:
            tool = service.registry.get(request.tool_name)
            elicited: ElicitedInput | None = None
            prompt = tool.policy.elicitation
            if prompt is not None:
                try:
                    if context is None:
                        raise GovernedRejected(
                            "elicitation_unsupported", "no MCP session to elicit through"
                        )
                    elicited = await elicit_from_client(
                        context,
                        prompt,
                        elicitation_id=f"{request.run_id}:{request.tool_name}",
                    )
                except GovernedRejected as rejected:
                    if rejected.code not in _NEGOTIATION_FAILURES:
                        raise
                    elicited = ElicitedInput("unsupported")
            state = await service.prepare(request, self.actor(principal), elicited=elicited)
        except GovernedRejected as error:
            raise governed_error(error) from None
        return state.public()

    async def execute(
        self,
        principal: GovernedPrincipal,
        *,
        intent_id: str,
        arguments: Mapping[str, Any] | None = None,
    ) -> dict[str, object]:
        try:
            state = await self.service(principal).execute(
                intent_id, self.actor(principal), arguments=arguments
            )
        except GovernedRejected as error:
            raise governed_error(error) from None
        return state.public()

    async def status(self, principal: GovernedPrincipal, *, intent_id: str) -> dict[str, object]:
        try:
            state = await self.service(principal).status(intent_id, self.actor(principal))
        except GovernedRejected as error:
            raise governed_error(error) from None
        return state.public()


async def resolve_approval(
    human_tasks: ScopedHumanTasks,
    principal: GovernedPrincipal,
    *,
    human_task_id: str,
    request: Mapping[str, Any],
) -> dict[str, object]:
    """The extended approval body through the same HumanTaskService (MCP parity)."""

    body = ApprovalResolutionRequest.model_validate(dict(request))
    try:
        receipt = await human_tasks.service(principal).resolve_approval(
            human_task_id, body, human_tasks.actor(principal)
        )
    except HumanTaskRejected as error:
        raise domain_error(error) from None
    return receipt.public()


def register_governed_tools(
    server: FastMCP,
    governed: ScopedGovernedEffects,
    principals: Any,
    *,
    call: Callable[..., Awaitable[dict[str, object]]],
    human_tasks: ScopedHumanTasks | None = None,
) -> None:
    """Register the governed gateway tools; `call` wraps results in the MCP envelope."""

    @server.tool(
        name=GOVERNED_PREPARE_TOOL,
        annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
    )
    async def mission_governed_prepare(
        tool_name: str,
        arguments: dict[str, Any],
        run_id: str,
        harness_execution_id: str,
        generation: int,
        lane_profile: str,
        context: Context,
    ) -> dict[str, object]:
        async def invoke(principal: Any) -> object:
            request = GovernedPrepareRequest.model_validate(
                {
                    "tool_name": tool_name,
                    "arguments": arguments,
                    "run_id": run_id,
                    "harness_execution_id": harness_execution_id,
                    "generation": generation,
                    "lane_profile": lane_profile,
                }
            )
            return await governed.prepare(principal, request, context=context)

        return await call(context, principals, invoke)

    @server.tool(
        name=GOVERNED_EXECUTE_TOOL,
        annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True},
        tags={"consequential"},
    )
    async def mission_governed_execute(
        intent_id: str, context: Context, arguments: dict[str, Any] | None = None
    ) -> dict[str, object]:
        async def invoke(principal: Any) -> object:
            return await governed.execute(principal, intent_id=intent_id, arguments=arguments)

        return await call(context, principals, invoke)

    @server.tool(name=GOVERNED_STATUS_TOOL, annotations={"readOnlyHint": True})
    async def mission_governed_status(intent_id: str, context: Context) -> dict[str, object]:
        async def invoke(principal: Any) -> object:
            return await governed.status(principal, intent_id=intent_id)

        return await call(context, principals, invoke)

    if human_tasks is not None:
        scoped_tasks = human_tasks

        @server.tool(name=APPROVAL_RESOLVE_TOOL)
        async def mission_approval_resolve(
            human_task_id: str, request: dict[str, Any], context: Context
        ) -> dict[str, object]:
            async def invoke(principal: Any) -> object:
                return await resolve_approval(
                    scoped_tasks, principal, human_task_id=human_task_id, request=request
                )

            return await call(context, principals, invoke)


__all__ = [
    "APPROVAL_RESOLVE_TOOL",
    "GOVERNED_EXECUTE_TOOL",
    "GOVERNED_PREPARE_TOOL",
    "GOVERNED_STATUS_TOOL",
    "GOVERNED_TOOL_NAMES",
    "ScopedGovernedEffects",
    "governed_error",
    "register_governed_tools",
    "resolve_approval",
]
