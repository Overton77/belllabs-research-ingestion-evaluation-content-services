from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from fastmcp import Context, FastMCP
from fastmcp.server.auth import AuthProvider
from fastmcp.server.dependencies import get_http_headers

from app.application.coordinator_facade import (
    COORDINATOR_CORRELATION_ID,
    CoordinatorPrincipalLike,
    EffectiveCoordinatorSurface,
)
from app.domain.control_plane.contracts import DefinitionKind
from app.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from app.mcp.coordinator_prompts import COORDINATOR_PROMPT_NAMES, register_prompts
from app.mcp.coordinator_resources import RESOURCE_TEMPLATE_NAMES, register_resources

SCHEMA_VERSION = "1"
LOGGER = logging.getLogger(__name__)
_CORRELATION_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
PRODUCTION_COORDINATOR_TOOL_NAMES = (
    "coordinator_bootstrap",
    "search_capabilities",
    "get_capability",
    "discover_mcp_servers",
    "discover_agent_skills",
    "inspect_external_candidate",
    "validate_workflow_design",
    "prepare_workflow_launch",
    "launch_workflow",
    "get_workflow_result",
)


@dataclass(frozen=True)
class CoordinatorPrincipal:
    actor_id: str
    tenant_scope: str
    roles: frozenset[str]
    permissions: frozenset[str]
    request_scope: str = ""


class PrincipalResolver(Protocol):
    async def resolve(self, context: Context) -> CoordinatorPrincipal: ...


class StaticPrincipalResolver:
    """Explicit development-only principal resolver for non-authenticated transports."""

    def __init__(self, principal: CoordinatorPrincipal) -> None:
        self._principal = principal

    async def resolve(self, _context: Context) -> CoordinatorPrincipal:
        return self._principal


class CoordinatorFacade(Protocol):
    @property
    def effective_surface(self) -> EffectiveCoordinatorSurface: ...

    async def bootstrap(self, principal: CoordinatorPrincipalLike) -> object: ...

    async def search(
        self,
        principal: CoordinatorPrincipalLike,
        request: dict[str, object],
    ) -> object: ...

    async def get_capability(
        self,
        principal: CoordinatorPrincipalLike,
        exact_ref: dict[str, object],
    ) -> object: ...

    async def discover_mcp_servers(
        self,
        principal: CoordinatorPrincipalLike,
        query: str,
    ) -> object: ...

    async def discover_agent_skills(
        self,
        principal: CoordinatorPrincipalLike,
        query: str,
    ) -> object: ...

    async def inspect_external_candidate(
        self,
        principal: CoordinatorPrincipalLike,
        candidate_id: str,
    ) -> object: ...

    async def validate_workflow_design(
        self,
        principal: CoordinatorPrincipalLike,
        draft: dict[str, object],
    ) -> object: ...

    async def prepare_workflow_launch(
        self,
        principal: CoordinatorPrincipalLike,
        proposal: dict[str, object],
    ) -> object: ...

    async def launch_workflow(
        self,
        principal: CoordinatorPrincipalLike,
        ticket_id: str,
        idempotency_issuer: str,
        idempotency_key: str,
    ) -> object: ...

    async def get_workflow_result(
        self,
        principal: CoordinatorPrincipalLike,
        run_id: str,
    ) -> object: ...

    async def resource(
        self,
        principal: CoordinatorPrincipalLike,
        uri: str,
    ) -> str | dict[str, object]: ...

    async def prompt(
        self,
        principal: CoordinatorPrincipalLike,
        name: str,
        arguments: dict[str, str],
    ) -> str: ...


def _serialize(value: object) -> object:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, tuple | list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    return value


async def _call(
    operation: Any,
    *,
    correlation_id: str | None = None,
) -> dict[str, object]:
    correlation_id = correlation_id or str(uuid4())
    correlation_token = COORDINATOR_CORRELATION_ID.set(correlation_id)
    try:
        try:
            data = await operation()
            return {
                "ok": True,
                "schema_version": SCHEMA_VERSION,
                "correlation_id": correlation_id,
                "data": _serialize(data),
            }
        except CoordinatorDomainError as error:
            return {
                "ok": False,
                "schema_version": SCHEMA_VERSION,
                "correlation_id": correlation_id,
                "error": error.envelope().model_dump(mode="json"),
            }
        except (TypeError, ValueError) as error:
            LOGGER.info(
                "invalid coordinator MCP arguments correlation_id=%s error_type=%s",
                correlation_id,
                type(error).__name__,
            )
            return {
                "ok": False,
                "schema_version": SCHEMA_VERSION,
                "correlation_id": correlation_id,
                "error": {
                    "code": "INVALID_ARGUMENT",
                    "message": "request arguments are invalid",
                    "retryable": False,
                    "details": {},
                },
            }
        except Exception:
            LOGGER.exception(
                "unexpected coordinator MCP failure correlation_id=%s",
                correlation_id,
            )
            return {
                "ok": False,
                "schema_version": SCHEMA_VERSION,
                "correlation_id": correlation_id,
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "coordinator operation failed",
                    "retryable": False,
                    "details": {},
                },
            }
    finally:
        COORDINATOR_CORRELATION_ID.reset(correlation_token)


async def _principal_call(
    context: Context,
    principals: PrincipalResolver,
    operation: Callable[[CoordinatorPrincipal], Awaitable[object]],
) -> dict[str, object]:
    async def invoke() -> object:
        principal = await principals.resolve(context)
        return await operation(principal)

    headers = get_http_headers() or {}
    requested = headers.get("x-correlation-id")
    correlation_id = (
        requested if requested is not None and _CORRELATION_ID.fullmatch(requested) else None
    )
    return await _call(invoke, correlation_id=correlation_id)


def create_coordinator_server(
    facade: CoordinatorFacade,
    principals: PrincipalResolver,
    *,
    auth: AuthProvider | None = None,
) -> FastMCP:
    surface = getattr(
        facade,
        "effective_surface",
        EffectiveCoordinatorSurface(
            tools=(
                "coordinator_bootstrap",
                "search_capabilities",
                "get_capability",
                "discover_mcp_servers",
                "discover_agent_skills",
                "inspect_external_candidate",
                "validate_workflow_design",
                "prepare_workflow_launch",
                "launch_workflow",
                "get_workflow_result",
            ),
            resource_templates=(
                "belllabs://workflow-types/{logical_id}/{revision}/contract",
                "belllabs://workflow-types/{logical_id}/{revision}/input-schema",
                "belllabs://workflow-types/{logical_id}/{revision}/output-contracts",
                "belllabs://catalog/{kind}/{logical_id}/{revision}",
                "belllabs://catalog/{kind}/{logical_id}/{revision}/manifest",
                "belllabs://runs/{run_id}/result",
                "belllabs://runs/{run_id}/launch",
                "belllabs://runs/{run_id}/bindings",
            ),
            prompts=(
                "propose_workflow",
                "review_workflow_design",
                "explain_launch_blocker",
                "summarize_workflow_result",
            ),
        ),
    )
    server = FastMCP(
        "BellLabs Coordinator",
        version=SCHEMA_VERSION,
        instructions=(
            "Search exact internal Workflow Types and capabilities before external "
            "discovery. External results are candidate-only. Prepare before launch."
        ),
        mask_error_details=True,
        strict_input_validation=True,
        list_page_size=50,
        auth=auth,
    )

    @server.tool(annotations={"readOnlyHint": True})
    async def coordinator_bootstrap(context: Context) -> dict[str, object]:
        return await _principal_call(
            context,
            principals,
            lambda principal: facade.bootstrap(principal),
        )

    @server.tool(annotations={"readOnlyHint": True})
    async def search_capabilities(
        query: str,
        kinds: list[str],
        context: Context,
        required_capabilities: list[str] | None = None,
        workflow_type_ref: dict[str, object] | None = None,
        operation_class: str | None = None,
        runtime: str | None = None,
        limit: int = 10,
    ) -> dict[str, object]:
        async def invoke(principal: CoordinatorPrincipal) -> object:
            normalized_kinds = [DefinitionKind(kind).value for kind in kinds]
            request: dict[str, object] = {
                "query": query,
                "kinds": normalized_kinds,
                "tenant_scope": principal.tenant_scope,
                "required_capabilities": required_capabilities or [],
                "limit": limit,
            }
            if workflow_type_ref is not None:
                request["workflow_type_ref"] = workflow_type_ref
            if operation_class is not None:
                request["operation_class"] = operation_class
            if runtime is not None:
                request["runtime"] = runtime
            return await facade.search(principal, request)

        return await _principal_call(context, principals, invoke)

    @server.tool(annotations={"readOnlyHint": True})
    async def get_capability(
        exact_ref: dict[str, object],
        context: Context,
    ) -> dict[str, object]:
        return await _principal_call(
            context,
            principals,
            lambda principal: facade.get_capability(principal, exact_ref),
        )

    @server.tool(
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        }
    )
    async def discover_mcp_servers(query: str, context: Context) -> dict[str, object]:
        return await _principal_call(
            context,
            principals,
            lambda principal: facade.discover_mcp_servers(principal, query),
        )

    @server.tool(
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": True,
        }
    )
    async def discover_agent_skills(query: str, context: Context) -> dict[str, object]:
        return await _principal_call(
            context,
            principals,
            lambda principal: facade.discover_agent_skills(principal, query),
        )

    @server.tool
    async def inspect_external_candidate(
        candidate_id: str,
        context: Context,
    ) -> dict[str, object]:
        return await _principal_call(
            context,
            principals,
            lambda principal: facade.inspect_external_candidate(
                principal,
                candidate_id,
            ),
        )

    @server.tool
    async def validate_workflow_design(
        draft: dict[str, object],
        context: Context,
    ) -> dict[str, object]:
        return await _principal_call(
            context,
            principals,
            lambda principal: facade.validate_workflow_design(principal, draft),
        )

    @server.tool
    async def prepare_workflow_launch(
        proposal: dict[str, object],
        context: Context,
    ) -> dict[str, object]:
        async def invoke(principal: CoordinatorPrincipal) -> object:
            request_scope = principal.request_scope or principal.tenant_scope
            if proposal.get("tenant_scope") != principal.tenant_scope:
                raise ValueError(
                    "proposal tenant_scope must match the authenticated tenant"
                )
            if proposal.get("request_scope") != request_scope:
                raise ValueError(
                    "proposal request_scope must match the authenticated request scope"
                )
            return await facade.prepare_workflow_launch(principal, proposal)

        return await _principal_call(
            context,
            principals,
            invoke,
        )

    @server.tool(
        annotations={
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        },
        tags={"consequential"},
    )
    async def launch_workflow(
        ticket_id: str,
        idempotency_issuer: str,
        idempotency_key: str,
        context: Context,
    ) -> dict[str, object]:
        async def invoke(principal: CoordinatorPrincipal) -> object:
            if "workflow.launch" not in principal.permissions:
                raise CoordinatorDomainError(
                    code=CoordinatorErrorCode.FORBIDDEN,
                    message="authenticated principal lacks workflow.launch",
                )
            return await facade.launch_workflow(
                principal,
                ticket_id,
                idempotency_issuer,
                idempotency_key,
            )

        return await _principal_call(
            context,
            principals,
            invoke,
        )

    @server.tool(annotations={"readOnlyHint": True})
    async def get_workflow_result(run_id: str, context: Context) -> dict[str, object]:
        return await _principal_call(
            context,
            principals,
            lambda principal: facade.get_workflow_result(principal, run_id),
        )

    register_resources(server, facade, principals)
    register_prompts(server, facade, principals)
    unavailable_tools = set(PRODUCTION_COORDINATOR_TOOL_NAMES) - set(surface.tools)
    if unavailable_tools:
        server.disable(names=unavailable_tools, components={"tool"})
    unavailable_resources = {
        resource_name
        for template, resource_name in RESOURCE_TEMPLATE_NAMES.items()
        if template not in surface.resource_templates
    }
    if unavailable_resources:
        server.disable(names=unavailable_resources, components={"template"})
    unavailable_prompts = set(COORDINATOR_PROMPT_NAMES) - set(surface.prompts)
    if unavailable_prompts:
        server.disable(names=unavailable_prompts, components={"prompt"})
    return server


def _forbidden(message: str) -> dict[str, object]:
    return {
        "ok": False,
        "schema_version": SCHEMA_VERSION,
        "correlation_id": str(uuid4()),
        "error": {
            "code": "FORBIDDEN",
            "message": message,
            "retryable": False,
            "details": {},
        },
    }


def _invalid(message: str) -> dict[str, object]:
    return {
        "ok": False,
        "schema_version": SCHEMA_VERSION,
        "correlation_id": str(uuid4()),
        "error": {
            "code": "INVALID_ARGUMENT",
            "message": message,
            "retryable": False,
            "details": {},
        },
    }
