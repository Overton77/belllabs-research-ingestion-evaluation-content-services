"""MCP subscriptions: the ``mission_subscribe`` tool and ``notifications/mission/event``.

``mission_subscribe`` registers an ``mcp_session`` Subscription for the calling session.
The subscription relay delivers through :class:`McpSessionHub`, which sends the
``mc.event.v1`` envelope as a ``notifications/mission/event`` notification; a session that
is gone makes ``notify`` return False and the relay pauses the Subscription, which resumes
from its cursor when the session subscribes again.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final, Literal, Protocol, cast
from uuid import UUID

from fastmcp import Context, FastMCP
from pydantic import BaseModel, ConfigDict

from mission_control.application.subscriptions.service import (
    SubscriptionRejected,
    SubscriptionService,
)
from mission_control.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.contracts import (
    McpSessionChannel,
    MissionEventEnvelope,
    SubscriptionRequest,
)

LOGGER = logging.getLogger(__name__)
NOTIFICATION_METHOD: Final = "notifications/mission/event"


class MissionEventNotification(BaseModel):
    """JSON-RPC notification body: ``{"method": ..., "params": <mc.event.v1 + subscription>}``."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    method: Literal["notifications/mission/event"] = NOTIFICATION_METHOD
    params: dict[str, Any]


def notification_for(
    envelope: MissionEventEnvelope, subscription_id: str | None = None
) -> MissionEventNotification:
    params = envelope.model_dump(mode="json")
    if subscription_id is not None:
        params["subscription_id"] = subscription_id
    return MissionEventNotification(params=params)


class _Session(Protocol):
    async def send_notification(
        self, notification: Any, related_request_id: Any = None
    ) -> None: ...


class McpSessionHub:
    """Live MCP sessions by ``session_ref``; implements the relay's McpSessionNotifier."""

    def __init__(self) -> None:
        self._sessions: dict[str, _Session] = {}

    def register(self, session_ref: str, session: object) -> None:
        self._sessions[session_ref] = cast(_Session, session)

    def unregister(self, session_ref: str) -> None:
        self._sessions.pop(session_ref, None)

    def session(self, session_ref: str) -> object | None:
        return self._sessions.get(session_ref)

    def connected(self, session_ref: str) -> bool:
        return session_ref in self._sessions

    async def notify(self, session_ref: str, envelope: MissionEventEnvelope) -> bool:
        session = self._sessions.get(session_ref)
        if session is None:
            return False
        try:
            await session.send_notification(notification_for(envelope))
        except Exception:  # A closed stream means the session is gone.
            LOGGER.info("mcp session %s is gone; pausing its subscriptions", session_ref)
            self.unregister(session_ref)
            return False
        return True


class SubscriptionPrincipal(Protocol):
    @property
    def actor_id(self) -> str: ...

    @property
    def permissions(self) -> frozenset[str]: ...

    @property
    def request_scope(self) -> str: ...


class McpSubscriptionBridge:
    """Resolves the principal's scoped SubscriptionService and binds the session channel."""

    def __init__(self, services: Mapping[str, SubscriptionService], hub: McpSessionHub) -> None:
        self._services = services
        self.hub = hub

    async def subscribe(
        self,
        principal: SubscriptionPrincipal,
        *,
        target: Literal["mission", "run"],
        target_id: str,
        events: list[str],
        node_keys: list[str],
        session_ref: str,
        session: object,
    ) -> dict[str, object]:
        service = self._services.get(principal.request_scope)
        if service is None:
            raise SubscriptionRejected("unavailable", "subscriptions are not composed here")
        request = SubscriptionRequest(
            target=target,
            target_id=UUID(target_id),
            events=tuple(events),
            node_keys=tuple(node_keys),
            channel=McpSessionChannel(session_ref=session_ref),
        )
        actor = ActorContext(actor_id=principal.actor_id, permissions=principal.permissions)
        subscription = await service.subscribe(request, actor)
        self.hub.register(session_ref, session)
        return {
            "subscription_id": str(subscription.subscription_id),
            "state": subscription.state.value,
            "cursor_seq": subscription.cursor_seq,
            "notification_method": NOTIFICATION_METHOD,
            "session_ref": session_ref,
        }


def register_subscription_tools(
    server: FastMCP,
    bridge: McpSubscriptionBridge,
    resolve: Callable[[Context], Awaitable[SubscriptionPrincipal]],
    call: Callable[[Callable[[], Awaitable[object]]], Awaitable[dict[str, object]]],
) -> None:
    @server.tool(name="mission_subscribe", annotations={"readOnlyHint": False})
    async def mission_subscribe(
        target: Literal["mission", "run"],
        target_id: str,
        events: list[str],
        context: Context,
        node_keys: list[str] | None = None,
    ) -> dict[str, object]:
        """Receive mission events in this MCP session as notifications/mission/event."""

        async def invoke() -> object:
            principal = await resolve(context)
            session_ref = context.session_id or f"mcp-session:{id(context.session)}"
            try:
                return await bridge.subscribe(
                    principal,
                    target=target,
                    target_id=target_id,
                    events=events,
                    node_keys=node_keys or [],
                    session_ref=session_ref,
                    session=context.session,
                )
            except SubscriptionRejected as exc:
                code = {
                    "unauthorized": CoordinatorErrorCode.FORBIDDEN,
                    "not_found": CoordinatorErrorCode.NOT_FOUND,
                    "unavailable": CoordinatorErrorCode.DEPENDENCY_UNAVAILABLE,
                }.get(exc.code, CoordinatorErrorCode.INVALID_ARGUMENT)
                raise CoordinatorDomainError(code, str(exc)) from None

        return await call(invoke)
