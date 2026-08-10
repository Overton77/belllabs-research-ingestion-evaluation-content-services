from __future__ import annotations

from typing import Any, Protocol

from temporalio import activity


class IdempotentControlPlanePort(Protocol):
    async def apply(self, payload: dict[str, Any]) -> dict[str, Any]: ...


class ControlPlaneActivities:
    """Temporal adapter only; BellLabs authority remains in the application service."""

    def __init__(self, service: IdempotentControlPlanePort) -> None:
        self._service = service

    @activity.defn(name="belllabs.control-plane.apply.v1")
    async def apply(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._service.apply(payload)
