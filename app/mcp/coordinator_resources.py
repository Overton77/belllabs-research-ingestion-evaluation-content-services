from __future__ import annotations

from typing import Any, Protocol

from fastmcp import Context, FastMCP

RESOURCE_TEMPLATE_NAMES = {
    "belllabs://workflow-types/{logical_id}/{revision}/contract": (
        "workflow_type_contract"
    ),
    "belllabs://workflow-types/{logical_id}/{revision}/input-schema": (
        "workflow_input_schema"
    ),
    "belllabs://workflow-types/{logical_id}/{revision}/output-contracts": (
        "workflow_output_contracts"
    ),
    "belllabs://catalog/{kind}/{logical_id}/{revision}": "catalog_asset",
    "belllabs://catalog/{kind}/{logical_id}/{revision}/manifest": "catalog_manifest",
    "belllabs://runs/{run_id}/result": "run_result",
    "belllabs://runs/{run_id}/launch": "run_launch",
    "belllabs://runs/{run_id}/bindings": "run_bindings",
}


class ResourceFacade(Protocol):
    async def resource(
        self,
        principal: Any,
        uri: str,
    ) -> str | dict[str, object]: ...


class ResourcePrincipalResolver(Protocol):
    async def resolve(self, context: Context) -> Any: ...


def register_resources(
    server: FastMCP,
    facade: ResourceFacade,
    principals: ResourcePrincipalResolver,
) -> None:
    async def read(context: Context, uri: str) -> str | dict[str, object]:
        return await facade.resource(await principals.resolve(context), uri)

    @server.resource("belllabs://workflow-types/{logical_id}/{revision}/contract")
    async def workflow_type_contract(
        logical_id: str,
        revision: int,
        context: Context,
    ) -> str | dict[str, object]:
        return await read(
            context,
            f"belllabs://workflow-types/{logical_id}/{revision}/contract",
        )

    @server.resource("belllabs://workflow-types/{logical_id}/{revision}/input-schema")
    async def workflow_input_schema(
        logical_id: str,
        revision: int,
        context: Context,
    ) -> str | dict[str, object]:
        return await read(
            context,
            f"belllabs://workflow-types/{logical_id}/{revision}/input-schema",
        )

    @server.resource("belllabs://workflow-types/{logical_id}/{revision}/output-contracts")
    async def workflow_output_contracts(
        logical_id: str,
        revision: int,
        context: Context,
    ) -> str | dict[str, object]:
        return await read(
            context,
            f"belllabs://workflow-types/{logical_id}/{revision}/output-contracts",
        )

    @server.resource("belllabs://catalog/{kind}/{logical_id}/{revision}")
    async def catalog_asset(
        kind: str,
        logical_id: str,
        revision: int,
        context: Context,
    ) -> str | dict[str, object]:
        return await read(
            context,
            f"belllabs://catalog/{kind}/{logical_id}/{revision}",
        )

    @server.resource("belllabs://catalog/{kind}/{logical_id}/{revision}/manifest")
    async def catalog_manifest(
        kind: str,
        logical_id: str,
        revision: int,
        context: Context,
    ) -> str | dict[str, object]:
        return await read(
            context,
            f"belllabs://catalog/{kind}/{logical_id}/{revision}/manifest",
        )

    @server.resource("belllabs://runs/{run_id}/result")
    async def run_result(run_id: str, context: Context) -> str | dict[str, object]:
        return await read(context, f"belllabs://runs/{run_id}/result")

    @server.resource("belllabs://runs/{run_id}/launch")
    async def run_launch(run_id: str, context: Context) -> str | dict[str, object]:
        return await read(context, f"belllabs://runs/{run_id}/launch")

    @server.resource("belllabs://runs/{run_id}/bindings")
    async def run_bindings(run_id: str, context: Context) -> str | dict[str, object]:
        return await read(context, f"belllabs://runs/{run_id}/bindings")
