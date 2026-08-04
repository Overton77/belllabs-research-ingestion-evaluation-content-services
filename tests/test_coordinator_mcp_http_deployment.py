from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import FastAPI
from mcp.types import LATEST_PROTOCOL_VERSION

from app.config import get_settings
from app.mcp.coordinator_bootstrap import (
    create_coordinator_http_deployment,
    mount_coordinator_http,
)
from app.mcp.coordinator_http_client import mounted_coordinator_client
from app.mcp.coordinator_server import (
    CoordinatorPrincipal,
    StaticPrincipalResolver,
)
from tests.test_coordinator_mcp_read_surface import FakeFacade


@pytest.mark.asyncio
async def test_streamable_http_mount_bootstraps_through_fastapi() -> None:
    settings = get_settings().model_copy(
        update={"coordinator_mcp_enabled": True}
    )
    principal = CoordinatorPrincipal(
        actor_id="http-operator",
        tenant_scope="tenant-a",
        request_scope="request-a",
        roles=frozenset({"operator"}),
        permissions=frozenset({"catalog.read"}),
    )
    deployment = create_coordinator_http_deployment(
        settings=settings,
        facade=FakeFacade(),
        auth=None,
        principals=StaticPrincipalResolver(principal),
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        async with deployment.app.router.lifespan_context(deployment.app):
            yield

    application = FastAPI(lifespan=lifespan)
    mount_coordinator_http(
        application,
        deployment,
        lifespan_is_combined=True,
    )
    transport = httpx.ASGITransport(app=application)
    headers = {
        "accept": "application/json, text/event-stream",
        "content-type": "application/json",
        "x-correlation-id": "phase-1-http-test",
    }
    async with (
        application.router.lifespan_context(application),
        httpx.AsyncClient(
            transport=transport,
            base_url="http://testserver",
            headers=headers,
        ) as client,
    ):
        initialize = await client.post(
            "/mcp/coordinator/",
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": LATEST_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "phase-1-test", "version": "1"},
                },
            },
        )
        assert initialize.status_code == 200
        initialized = initialize.json()
        assert initialized["result"]["serverInfo"]["name"] == "BellLabs Coordinator"

        bootstrap = await client.post(
            "/mcp/coordinator/",
            json={
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "coordinator_bootstrap",
                    "arguments": {},
                },
            },
        )
        assert bootstrap.status_code == 200
        payload = bootstrap.json()["result"]["structuredContent"]
        assert payload["ok"] is True
        assert payload["correlation_id"] == "phase-1-http-test"
        assert payload["data"]["actor_id"] == "http-operator"


@pytest.mark.asyncio
async def test_acceptance_client_uses_mounted_streamable_http_transport() -> None:
    principal = CoordinatorPrincipal(
        actor_id="tracer",
        tenant_scope="tenant-a",
        request_scope="request-a",
        roles=frozenset({"operator"}),
        permissions=frozenset({"catalog.read"}),
    )
    async with mounted_coordinator_client(
        settings=get_settings(),
        facade=FakeFacade(),
        principal=principal,
    ) as client:
        tools = await client.list_tools()
        bootstrap = await client.call_tool("coordinator_bootstrap", {})
    assert any(tool.name == "coordinator_bootstrap" for tool in tools)
    assert bootstrap.data["ok"] is True
