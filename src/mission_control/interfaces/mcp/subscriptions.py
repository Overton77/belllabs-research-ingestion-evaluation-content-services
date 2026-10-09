"""MCP subscriptions: the ``mission_subscribe`` tool and ``notifications/mission/event``.

``mission_subscribe`` registers an ``mcp_session`` Subscription for the calling session.
The subscription relay delivers through :class:`McpSessionHub`, which sends the
``mc.event.v1`` envelope as a ``notifications/mission/event`` notification; a session that
is gone makes ``notify`` return False and the relay pauses the Subscription, which resumes
from its cursor when the session subscribes again.

MP-15 coordinator inbox (registered only when the bridge carries inbox services):

- `coordinator_inbox_subscribe` registers a durable coordinator inbox with the default
  coordinator filter (`mc.coordinator_profile.v1`); `delivery` is `poll` (default),
  `mcp_session` or `webhook` (signed, egress-validated);
- `coordinator_inbox_poll` returns pending notifications after the acknowledged cursor
  (each pending notification once by id; the polling fallback is always available);
- `coordinator_inbox_ack` acknowledges by id and/or through an inbox sequence;
- `coordinator_inbox_command` admits a command because of a notification, under the
  recursion and per-run rate bounds (the same `MissionControlService.command` path);
- resource `mc://coordinator-inboxes/{subscription_id}` reads the pending page.

`notifications/coordinator/inbox` is only a wake-up for a connected session (qualified while
the session is registered and connected; a stateless HTTP session cannot receive it): it
never acknowledges and is never assumed to become model input. The inbox stays durable.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Final, Literal, Protocol, cast
from uuid import UUID

from fastmcp import Context, FastMCP
from pydantic import BaseModel, ConfigDict

from mission_control.application.subscriptions.coordinator import CoordinatorProfile
from mission_control.application.subscriptions.coordinator_ports import InboxHint, PollDelivery
from mission_control.application.subscriptions.coordinator_service import (
    CoordinatorInboxService,
    CoordinatorRejected,
    CoordinatorSubscribeRequest,
)
from mission_control.application.subscriptions.service import (
    SubscriptionRejected,
    SubscriptionService,
)
from mission_control.contracts.contracts import MissionCommandRequest, MissionControlRejected
from mission_control.domain.coordinator.errors import CoordinatorDomainError, CoordinatorErrorCode
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.contracts import (
    McpSessionChannel,
    MissionEventEnvelope,
    SubscriptionRequest,
    WebhookChannel,
)

LOGGER = logging.getLogger(__name__)
NOTIFICATION_METHOD: Final = "notifications/mission/event"
INBOX_NOTIFICATION_METHOD: Final = "notifications/coordinator/inbox"
INBOX_RESOURCE: Final = "mc://coordinator-inboxes/{subscription_id}"
INBOX_TOOL_NAMES: Final = (
    "coordinator_inbox_subscribe",
    "coordinator_inbox_poll",
    "coordinator_inbox_ack",
    "coordinator_inbox_command",
)


class MissionEventNotification(BaseModel):
    """JSON-RPC notification body: ``{"method": ..., "params": <mc.event.v1 + subscription>}``."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    method: Literal["notifications/mission/event"] = NOTIFICATION_METHOD
    params: dict[str, Any]


class InboxNotification(BaseModel):
    """`notifications/coordinator/inbox`: a wake-up carrying cursors only, never content."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    method: Literal["notifications/coordinator/inbox"] = INBOX_NOTIFICATION_METHOD
    params: dict[str, Any]


def inbox_notification_for(hint: InboxHint) -> InboxNotification:
    return InboxNotification(
        params={
            "subscription_id": str(hint.subscription_id),
            "high_inbox_seq": hint.high_inbox_seq,
            "acked_inbox_seq": hint.acked_inbox_seq,
            "pending": hint.pending,
            "poll_tool": "coordinator_inbox_poll",
        }
    )


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

    async def notify_inbox(self, session_ref: str, hint: InboxHint) -> bool:
        """Wake a connected coordinator; False (and nothing lost) when the session is gone."""

        session = self._sessions.get(session_ref)
        if session is None:
            return False
        try:
            await session.send_notification(inbox_notification_for(hint))
        except Exception:  # A closed stream means the session is gone; the inbox stays.
            LOGGER.info("mcp session %s is gone; its coordinator inbox keeps polling", session_ref)
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

    def __init__(
        self,
        services: Mapping[str, SubscriptionService],
        hub: McpSessionHub,
        *,
        inboxes: Mapping[str, CoordinatorInboxService] | None = None,
    ) -> None:
        self._services = services
        self.hub = hub
        self.inboxes: Mapping[str, CoordinatorInboxService] = inboxes or {}

    def inbox_service(self, principal: SubscriptionPrincipal) -> CoordinatorInboxService:
        service = self.inboxes.get(principal.request_scope)
        if service is None:
            raise CoordinatorRejected("unavailable", "coordinator inboxes are not composed here")
        return service

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

    if bridge.inboxes:
        _register_inbox_tools(server, bridge, resolve, call)


_INBOX_ERRORS: Final = {
    "unauthorized": CoordinatorErrorCode.FORBIDDEN,
    "not_found": CoordinatorErrorCode.NOT_FOUND,
    "unavailable": CoordinatorErrorCode.DEPENDENCY_UNAVAILABLE,
    "closed": CoordinatorErrorCode.CONFLICT,
    "rate_limited": CoordinatorErrorCode.RATE_LIMITED,
    "recursion_bound": CoordinatorErrorCode.CONFLICT,
}


def _domain_error(error: CoordinatorRejected | MissionControlRejected) -> CoordinatorDomainError:
    code = _INBOX_ERRORS.get(error.code, CoordinatorErrorCode.INVALID_ARGUMENT)
    return CoordinatorDomainError(
        code,
        str(error),
        retryable=error.code == "rate_limited",
        details={"reason": error.code},
    )


def _actor(principal: SubscriptionPrincipal) -> ActorContext:
    return ActorContext(actor_id=principal.actor_id, permissions=principal.permissions)


def _register_inbox_tools(
    server: FastMCP,
    bridge: McpSubscriptionBridge,
    resolve: Callable[[Context], Awaitable[SubscriptionPrincipal]],
    call: Callable[[Callable[[], Awaitable[object]]], Awaitable[dict[str, object]]],
) -> None:
    async def guarded(operation: Callable[[], Awaitable[object]]) -> dict[str, object]:
        async def invoke() -> object:
            try:
                return await operation()
            except (CoordinatorRejected, MissionControlRejected) as error:
                raise _domain_error(error) from None

        return await call(invoke)

    @server.tool(name="coordinator_inbox_subscribe", annotations={"readOnlyHint": False})
    async def coordinator_inbox_subscribe(
        target: Literal["mission", "run"],
        target_id: str,
        context: Context,
        delivery: Literal["poll", "mcp_session", "webhook"] = "poll",
        webhook_url: str | None = None,
        secret_ref: str | None = None,
        profile: dict[str, Any] | None = None,
        coordinator_run_ref: str | None = None,
        after_seq: int | None = None,
    ) -> dict[str, object]:
        """Register a durable coordinator inbox (default significance filter, deltas off)."""

        async def operation() -> object:
            principal = await resolve(context)
            service = bridge.inbox_service(principal)
            session_ref = context.session_id or f"mcp-session:{id(context.session)}"
            channel: PollDelivery | McpSessionChannel | WebhookChannel
            if delivery == "mcp_session":
                channel = McpSessionChannel(session_ref=session_ref)
            elif delivery == "webhook":
                if webhook_url is None or secret_ref is None:
                    raise CoordinatorRejected("invalid", "webhook delivery needs a url and secret")
                channel = WebhookChannel.model_validate(
                    {"url": webhook_url, "secret_ref": secret_ref}
                )
            else:
                channel = PollDelivery()
            request = CoordinatorSubscribeRequest(
                target=target,
                target_id=UUID(target_id),
                profile=CoordinatorProfile.model_validate(profile or {}),
                delivery=channel,
                coordinator_run_ref=coordinator_run_ref,
                after_seq=after_seq,
            )
            inbox = await service.subscribe(request, _actor(principal))
            if delivery == "mcp_session":
                bridge.hub.register(session_ref, context.session)
            return {
                "subscription_id": str(inbox.subscription_id),
                "state": inbox.state.value,
                "cursor_seq": inbox.cursor_seq,
                "acked_inbox_seq": inbox.acked_inbox_seq,
                "profile": inbox.profile.model_dump(mode="json"),
                "delivery": delivery,
                "notification_method": INBOX_NOTIFICATION_METHOD
                if delivery == "mcp_session"
                else None,
                "notification_qualified": delivery == "mcp_session"
                and bridge.hub.connected(session_ref),
                "poll_tool": "coordinator_inbox_poll",
            }

        return await guarded(operation)

    @server.tool(name="coordinator_inbox_poll", annotations={"readOnlyHint": True})
    async def coordinator_inbox_poll(
        subscription_id: str,
        context: Context,
        after_inbox_seq: int | None = None,
        limit: int = 50,
    ) -> dict[str, object]:
        """Pending notifications after the acknowledged cursor (acknowledge to move on)."""

        async def operation() -> object:
            principal = await resolve(context)
            page = await bridge.inbox_service(principal).poll(
                UUID(subscription_id),
                _actor(principal),
                after_inbox_seq=after_inbox_seq,
                limit=limit,
            )
            return page.model_dump(mode="json")

        return await guarded(operation)

    @server.tool(name="coordinator_inbox_ack", annotations={"readOnlyHint": False})
    async def coordinator_inbox_ack(
        subscription_id: str,
        context: Context,
        notification_ids: list[str] | None = None,
        through_inbox_seq: int | None = None,
    ) -> dict[str, object]:
        """Acknowledge notifications (idempotent; the cursor only moves forward)."""

        async def operation() -> object:
            principal = await resolve(context)
            outcome = await bridge.inbox_service(principal).ack(
                UUID(subscription_id),
                _actor(principal),
                notification_ids=tuple(UUID(item) for item in notification_ids or ()),
                through_inbox_seq=through_inbox_seq,
            )
            return {
                "acknowledged": [str(item) for item in outcome.acknowledged],
                "already": [str(item) for item in outcome.already],
                "unknown": [str(item) for item in outcome.unknown],
                "acked_inbox_seq": outcome.acked_inbox_seq,
            }

        return await guarded(operation)

    @server.tool(name="coordinator_inbox_command", annotations={"readOnlyHint": False})
    async def coordinator_inbox_command(
        subscription_id: str,
        notification_id: str,
        command: dict[str, Any],
        context: Context,
    ) -> dict[str, object]:
        """Admit an `mc.command.v1` because of a notification (recursion and rate bounded)."""

        async def operation() -> object:
            principal = await resolve(context)
            request = MissionCommandRequest.model_validate(command)
            receipt = await bridge.inbox_service(principal).command_from_notification(
                UUID(subscription_id),
                UUID(notification_id),
                request.target.id,
                request,
                _actor(principal),
            )
            return receipt.model_dump(mode="json")

        return await guarded(operation)

    @server.resource(INBOX_RESOURCE, mime_type="application/json")
    async def coordinator_inbox_resource(subscription_id: str, context: Context) -> str:
        principal = await resolve(context)
        try:
            page = await bridge.inbox_service(principal).poll(
                UUID(subscription_id), _actor(principal)
            )
        except CoordinatorRejected as error:
            raise _domain_error(error) from None
        return page.model_dump_json()
