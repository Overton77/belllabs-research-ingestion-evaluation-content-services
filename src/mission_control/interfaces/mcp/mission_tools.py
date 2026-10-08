"""Mission authoring and chain tools on the coordinator MCP server (SPEC-04, SPEC-05).

- tool `mission_chain_inspect(chain_id)` (read-only) and resource
  `mc://applications/{application_id}/chains/{chain_id}`: the `mc.chain.v1` projection with
  each member's run, the same document `GET /chains/{id}` returns for the same principal.
- tool `mission_manifest_compile(manifest_yaml)` (read-only; mission read + catalog read): the
  Validation Report with the `mc.manifest_resolution.v1` document, as `POST /missions:compile`.
- tools `mission_manifest_submit(manifest_yaml, request_id)` (consequential; `mission.author`)
  and `mission_run_start(run_id)` (consequential; `mission.start`), as `POST /missions:submit`
  and `POST /missions:start`.

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
from mission_control.application.authoring.manifest_submit import (
    ManifestBlocked,
    ManifestIdempotencyConflict,
    ManifestStartUnavailable,
    SubmitRequest,
)
from mission_control.application.chains.service import ChainInspectionService, ChainNotFound
from mission_control.application.execution.run_launch import RunLaunchRejected
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
SUBMIT_TOOL = "mission_manifest_submit"
START_TOOL = "mission_run_start"


class ScopedManifests:
    """Selects the manifest service of the principal's verified tenant scope.

    ``sponsorship_refs`` are the run sponsorships this coordinator deployment may bind on a
    submit (an MCP principal carries permissions, not sponsorships).
    """

    def __init__(
        self,
        services: Mapping[str, MissionManifestService],
        *,
        sponsorship_refs: frozenset[str] = frozenset(),
        approval_refs: frozenset[str] = frozenset(),
    ) -> None:
        self._services = dict(services)
        self._sponsorships = sponsorship_refs
        self._approvals = approval_refs

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

    def _lifecycle(self, principal: MissionToolPrincipal) -> Any:
        lifecycle = self.service(principal).lifecycle
        if lifecycle is None:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.DEPENDENCY_UNAVAILABLE,
                message="manifest submit and start are not composed",
            )
        return lifecycle

    @staticmethod
    def _actor(principal: MissionToolPrincipal) -> ActorContext:
        return ActorContext(
            actor_id=principal.actor_id,
            authority_refs=frozenset(),
            permissions=frozenset(principal.permissions),
        )

    async def submit(
        self, principal: MissionToolPrincipal, manifest_yaml: str, request_id: str
    ) -> object:
        try:
            receipt, replayed = await self._lifecycle(principal).submit(
                SubmitRequest(
                    manifest_yaml=manifest_yaml,
                    request_id=UUID(request_id),
                    actor=self._actor(principal),
                    sponsorship_refs=self._sponsorships,
                    approval_refs=self._approvals,
                )
            )
        except ManifestPermissionDenied as denied:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message=str(denied)
            ) from None
        except ManifestIdempotencyConflict as conflict:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.IDEMPOTENCY_CONFLICT, message=str(conflict)
            ) from None
        except ManifestBlocked as blocked:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.ADMISSION_REJECTED,
                message=str(blocked) or "the manifest has blockers",
            ) from None
        return {"replayed": replayed, **receipt.model_dump(mode="json")}

    async def start(self, principal: MissionToolPrincipal, run_id: str) -> object:
        try:
            receipt = await self._lifecycle(principal).start(run_id, self._actor(principal))
        except ManifestPermissionDenied as denied:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.FORBIDDEN, message=str(denied)
            ) from None
        except (ManifestStartUnavailable, RunLaunchRejected) as error:
            raise CoordinatorDomainError(
                code=CoordinatorErrorCode.CONFLICT, message=str(error)
            ) from None
        return receipt.model_dump(mode="json")


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

    @server.tool(
        name=SUBMIT_TOOL,
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
        tags={"consequential"},
    )
    async def mission_manifest_submit(
        manifest_yaml: str, request_id: str, context: Context
    ) -> dict[str, object]:
        """Commit the manifest as a revision and admit its run (or chain); never starts."""

        async def invoke(principal: Any) -> object:
            return await manifests.submit(principal, manifest_yaml, request_id)

        return await call(context, principals, invoke)

    @server.tool(
        name=START_TOOL,
        annotations={
            "readOnlyHint": False,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        },
        tags={"consequential"},
    )
    async def mission_run_start(run_id: str, context: Context) -> dict[str, object]:
        """Launch an admitted manifest run (the only verb that starts agents)."""

        async def invoke(principal: Any) -> object:
            return await manifests.start(principal, run_id)

        return await call(context, principals, invoke)
