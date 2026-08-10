from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from app.agent_server.auth import authentication_is_configured
from app.agent_server.graphs import GRAPH_REGISTRY

DependencyProbe = Callable[[], Awaitable[bool]]


@dataclass(frozen=True)
class ReadinessReport:
    status: str
    dependencies: dict[str, str]
    capabilities: dict[str, str]

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "dependencies": self.dependencies,
            "capabilities": self.capabilities,
        }


async def readiness_report(
    probes: dict[str, DependencyProbe] | None = None,
) -> ReadinessReport:
    dependencies: dict[str, str] = {}
    for name, probe in (probes or {}).items():
        try:
            dependencies[name] = "ready" if await probe() else "degraded"
        except Exception:
            dependencies[name] = "degraded"
    tracing_enabled = os.environ.get("LANGSMITH_TRACING", "").lower() in {"1", "true"}
    trace_io_hidden = all(
        os.environ.get(name, "").lower() in {"1", "true"}
        for name in ("LANGSMITH_HIDE_INPUTS", "LANGSMITH_HIDE_OUTPUTS")
    )
    tracing_configured = all(
        os.environ.get(name, "").strip()
        for name in ("LANGSMITH_API_KEY", "AGENT_SERVER_LANGSMITH_PROJECT")
    )
    capabilities = {
        "graph_registry": (
            "ready"
            if set(GRAPH_REGISTRY) == {"belllabs_stagegraph", "belllabs_goal_directed"}
            else "degraded"
        ),
        "authentication": ("ready" if authentication_is_configured() else "not_configured"),
        "tracing": (
            "ready"
            if tracing_enabled and trace_io_hidden and tracing_configured
            else "disabled"
            if not tracing_enabled
            else "not_configured"
            if not tracing_configured
            else "degraded"
        ),
    }
    degraded = any(value == "degraded" for value in dependencies.values()) or any(
        value in {"degraded", "not_configured"} for value in capabilities.values()
    )
    return ReadinessReport(
        status="degraded" if degraded else "ready",
        dependencies=dependencies,
        capabilities=capabilities,
    )
