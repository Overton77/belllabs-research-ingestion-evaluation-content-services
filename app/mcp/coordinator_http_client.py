from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI
from mcp.types import (
    LATEST_PROTOCOL_VERSION,
    CallToolResult,
    ListToolsResult,
    ReadResourceResult,
)

from app.config import Settings
from app.mcp.coordinator_bootstrap import (
    create_coordinator_http_deployment,
    mount_coordinator_http,
)
from app.mcp.coordinator_server import (
    CoordinatorFacade,
    CoordinatorPrincipal,
    StaticPrincipalResolver,
)


@dataclass(frozen=True)
class CoordinatorHttpToolResult:
    data: object


class CoordinatorStreamableHttpClient:
    """Small MCP client used by the acceptance harness over the mounted ASGI stack."""

    def __init__(self, client: httpx.AsyncClient, path: str) -> None:
        self._client = client
        self._path = f"{path.rstrip('/')}/"
        self._request_id = 0

    async def initialize(self) -> None:
        await self._rpc(
            "initialize",
            {
                "protocolVersion": LATEST_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "belllabs-coordinator-tracer", "version": "1"},
            },
        )
        response = await self._client.post(
            self._path,
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
        )
        response.raise_for_status()

    async def list_tools(self):
        result = await self._rpc("tools/list", {})
        return ListToolsResult.model_validate(result).tools

    async def call_tool(
        self,
        name: str,
        arguments: dict[str, object],
        *,
        timeout: float | None = None,  # noqa: ASYNC109 - FastMCP client compatibility
    ) -> CoordinatorHttpToolResult:
        del timeout
        result = CallToolResult.model_validate(
            await self._rpc(
                "tools/call",
                {"name": name, "arguments": arguments},
            )
        )
        return CoordinatorHttpToolResult(data=result.structuredContent)

    async def read_resource(self, uri: str):
        result = ReadResourceResult.model_validate(await self._rpc("resources/read", {"uri": uri}))
        return result.contents

    async def _rpc(self, method: str, params: dict[str, object]) -> object:
        self._request_id += 1
        response = await self._client.post(
            self._path,
            json={
                "jsonrpc": "2.0",
                "id": self._request_id,
                "method": method,
                "params": params,
            },
        )
        response.raise_for_status()
        envelope = response.json()
        if "error" in envelope:
            raise RuntimeError(f"MCP {method} failed: {envelope['error']}")
        return envelope["result"]


@asynccontextmanager
async def mounted_coordinator_client(
    *,
    settings: Settings,
    facade: CoordinatorFacade,
    principal: CoordinatorPrincipal,
) -> AsyncIterator[CoordinatorStreamableHttpClient]:
    """Exercise the same Streamable HTTP adapter mounted in FastAPI."""

    deployment = create_coordinator_http_deployment(
        settings=settings.model_copy(update={"coordinator_mcp_enabled": True}),
        facade=facade,
        auth=None,
        principals=StaticPrincipalResolver(principal),
    )

    @asynccontextmanager
    async def lifespan(_application: FastAPI):
        async with deployment.app.router.lifespan_context(deployment.app):
            yield

    application = FastAPI(lifespan=lifespan)
    mount_coordinator_http(
        application,
        deployment,
        lifespan_is_combined=True,
    )
    transport = httpx.ASGITransport(app=application)
    async with (
        application.router.lifespan_context(application),
        httpx.AsyncClient(
            transport=transport,
            base_url="http://coordinator.internal",
            headers={
                "accept": "application/json, text/event-stream",
                "content-type": "application/json",
            },
            timeout=120,
        ) as http,
    ):
        client = CoordinatorStreamableHttpClient(http, deployment.mount_path)
        await client.initialize()
        yield client
