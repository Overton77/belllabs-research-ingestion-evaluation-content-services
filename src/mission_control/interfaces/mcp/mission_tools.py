"""Mission authoring and chain tools on the coordinator MCP server (SPEC-04, SPEC-05).

- tool `mission_chain_inspect(chain_id)` (read-only) and resource
  `mc://applications/{application_id}/chains/{chain_id}`: the `mc.chain.v1` projection with
  each member's run, the same document `GET /chains/{id}` returns for the same principal.
- tool `mission_manifest_compile(manifest_yaml)` (read-only; mission read + catalog read): the
  Validation Report with the `mc.manifest_resolution.v1` document, as `POST /missions:compile`.

The principal's canonical request scope selects the tenant's service; a principal from
another application is refused.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Protocol
from uuid import UUID

from fastmcp import Context, FastMCP

from mission_control.application.authoring.manifest_service import (
    ManifestPermissionDenied,
    MissionManifestService,
)
from mission_control.application.chains.service import ChainInspectionService, ChainNotFound
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from mission_control.domain.policies.contracts import ActorContext

CHAIN_RESOURCE = "mc://applications/{application_id}/chains/{chain_id}"
CHAIN_TOOL = "mission_chain_inspect"
READ_GRANTS = frozenset({"workflow_run.read", "mission.read", "workflow.result.read"})


class MissionToolPrincipal(Protocol):
    @property
    def actor_id(self) -> str: ...

    @property
    def permissions(self) -> frozenset[str]: ...

    @property
    def request_scope(self) -> str: ...


class PrincipalResolver(Protocol):
    async def resolve(self, context: Context) -> Any: ...


def scope_of(principal: MissionToolPrincipal, application_id: str | None) -> str:
    scope = principal.request_scope
    try:
        parsed = parse_request_scope(scope)
    except ValueError:
        raise CoordinatorDomainError(
            code=CoordinatorErrorCode.FORBIDDEN,
            message="mission tools need a canonical tenant request scope",
        ) from None
    if application_id is not None and parsed.application_id != application_id:
        raise CoordinatorDomainError(
            code=CoordinatorErrorCode.FORBIDDEN, message="application scope denied"
        )
    return scope


def read_actor(principal: MissionToolPrincipal) -> ActorContext:
    permissions = frozenset(principal.permissions)
    if not permissions & READ_GRANTS:
        raise CoordinatorDomainError(
            code=CoordinatorErrorCode.FORBIDDEN, message="principal lacks mission read"
        )
    return ActorContext(
        actor_id=principal.actor_id,
        authority_refs=frozenset(),
        permissions=permissions | {"workflow_run.read"},
    )


class ScopedChains:
    """Selects the chain inspection service of the principal's verified tenant scope."""

    def __init__(self, services: Mapping[str, ChainInspectionService]) -> None:
        self._services = dict(services)

    async def inspect(
        self,
        principal: MissionToolPrincipal,
        chain_id: str,
        *,
        application_id: str | None = None,
    ) -> dict[str, object]:
        service = self._services.get(scope_of(principal, application_id))
        if service is None:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="no chain service for scope"
            )
        try:
            inspection = await service.inspect(UUID(chain_id), read_actor(principal))
        except ChainNotFound:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.NOT_FOUND, message="chain not found"
            ) from None
        return inspection.model_dump(mode="json")


def register_chain_tools(
    server: FastMCP,
    chains: ScopedChains,
    principals: PrincipalResolver,
    *,
    call: Any,
) -> None:
    """The read-only chain tool and resource; ``call`` wraps results in the MCP envelope."""

    @server.tool(name=CHAIN_TOOL, annotations={"readOnlyHint": True})
    async def mission_chain_inspect(chain_id: str, context: Context) -> dict[str, object]:
        """A Mission Chain: every member's lifecycle and run, every link's state."""

        async def invoke(principal: Any) -> object:
            return await chains.inspect(principal, chain_id)

        return await call(context, principals, invoke)

    @server.resource(CHAIN_RESOURCE, mime_type="application/json")
    async def chain_resource(application_id: str, chain_id: str, context: Context) -> str:
        principal = await principals.resolve(context)
        document = await chains.inspect(principal, chain_id, application_id=application_id)
        return json.dumps(document, sort_keys=True)


# ------------------------------------------------------------------------------------------
# Mission Manifest tools (SPEC-05): compile (read-only); submit and start come with FT-E3.
# ------------------------------------------------------------------------------------------

COMPILE_TOOL = "mission_manifest_compile"


class ScopedManifests:
    """Selects the manifest service of the principal's verified tenant scope."""

    def __init__(self, services: Mapping[str, MissionManifestService]) -> None:
        self._services = dict(services)

    def service(self, principal: MissionToolPrincipal) -> MissionManifestService:
        service = self._services.get(scope_of(principal, None))
        if service is None:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message="no manifest service for scope"
            )
        return service

    async def compile(self, principal: MissionToolPrincipal, manifest_yaml: str) -> object:
        try:
            compilation = await self.service(principal).compile(
                manifest_yaml,
                actor_id=principal.actor_id,
                permissions=frozenset(principal.permissions),
            )
        except ManifestPermissionDenied as denied:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message=str(denied)
            ) from None
        return compilation.report.model_dump(mode="json", by_alias=True)


def register_manifest_tools(
    server: FastMCP,
    manifests: ScopedManifests,
    principals: PrincipalResolver,
    *,
    call: Any,
) -> None:
    @server.tool(name=COMPILE_TOOL, annotations={"readOnlyHint": True})
    async def mission_manifest_compile(manifest_yaml: str, context: Context) -> dict[str, object]:
        """Compile a Mission Manifest: Validation Report and resolution; persists nothing."""

        async def invoke(principal: Any) -> object:
            return await manifests.compile(principal, manifest_yaml)

        return await call(context, principals, invoke)
