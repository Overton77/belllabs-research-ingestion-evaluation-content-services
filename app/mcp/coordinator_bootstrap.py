from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastmcp.server.auth import AuthProvider
from starlette.applications import Starlette

from app.config import Settings
from app.mcp.coordinator_auth import VerifiedAccessTokenPrincipalResolver
from app.mcp.coordinator_server import (
    CoordinatorFacade,
    PrincipalResolver,
    create_coordinator_server,
)


@dataclass(frozen=True)
class CoordinatorHttpDeployment:
    """HTTP adapter plus the lifespan the owning FastAPI app must enter."""

    app: Starlette
    mount_path: str


def create_coordinator_http_deployment(
    *,
    settings: Settings,
    facade: CoordinatorFacade,
    auth: AuthProvider | None,
    mount_path: str = "/mcp/coordinator",
    principals: PrincipalResolver | None = None,
) -> CoordinatorHttpDeployment:
    """Build authenticated Streamable HTTP without creating a second control plane."""
    if not settings.coordinator_mcp_enabled:
        raise RuntimeError("COORDINATOR_MCP_ENABLED is false")
    if not mount_path.startswith("/") or mount_path == "/":
        raise ValueError("coordinator MCP mount path must be a non-root absolute path")
    if auth is None and principals is None:
        raise ValueError(
            "coordinator HTTP requires authentication or an explicit principal resolver"
        )
    origins = settings.cors_origins
    allowed_hosts = sorted(
        {
            parsed.hostname
            for origin in origins
            if (parsed := urlsplit(origin)).hostname is not None
        }
        | {"127.0.0.1", "localhost"}
    )
    server = create_coordinator_server(
        facade,
        principals or VerifiedAccessTokenPrincipalResolver(),
        auth=auth,
    )
    app = server.http_app(
        path="/",
        transport="streamable-http",
        stateless_http=True,
        json_response=True,
        allowed_hosts=allowed_hosts,
        allowed_origins=origins,
    )
    return CoordinatorHttpDeployment(app=app, mount_path=mount_path.rstrip("/"))


def mount_coordinator_http(
    application: FastAPI,
    deployment: CoordinatorHttpDeployment,
    *,
    lifespan_is_combined: bool,
) -> None:
    """Mount only after the owner explicitly combines the FastMCP lifespan."""
    if not lifespan_is_combined:
        raise RuntimeError(
            "combine deployment.app.lifespan with the FastAPI lifespan before mounting"
        )
    application.mount(deployment.mount_path, deployment.app)
