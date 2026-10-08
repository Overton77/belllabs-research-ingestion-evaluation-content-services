"""FT-F5: MCP ``mission_subscribe`` and the ``notifications/mission/event`` shape."""

from __future__ import annotations

from typing import Any

import pytest
from fastmcp import Client, Context
from mcp.types import JSONRPCNotification

from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.domain.subscriptions.contracts import McpSessionChannel, SubscriptionState
from mission_control.interfaces.mcp.coordinator_server import (
    CoordinatorPrincipal,
    create_coordinator_server,
)
from mission_control.interfaces.mcp.subscriptions import (
    NOTIFICATION_METHOD,
    McpSessionHub,
    McpSubscriptionBridge,
    notification_for,
)
from tests.unit.coordinator.test_coordinator_mcp_read_surface import FakeFacade
from tests.unit.subscriptions.fakes import (
    MISSION,
    SCOPE,
    FakeWebhook,
    InMemoryStore,
    StaticSecrets,
    event,
)


class Resolver:
    def __init__(self, permissions: frozenset[str]) -> None:
        self.principal = CoordinatorPrincipal(
            actor_id="coordinator-1",
            tenant_scope="tenant-a",
            roles=frozenset({"operator"}),
            permissions=permissions,
            request_scope=SCOPE,
        )

    async def resolve(self, _context: Context) -> CoordinatorPrincipal:
        return self.principal


class RecordingSession:
    def __init__(self, inner: Any) -> None:
        self.inner = inner
        self.sent: list[JSONRPCNotification] = []

    async def send_notification(self, notification: Any, related_request_id: Any = None) -> None:
        self.sent.append(
            JSONRPCNotification(
                jsonrpc="2.0",
                **notification.model_dump(by_alias=True, mode="json", exclude_none=True),
            )
        )
        await self.inner.send_notification(notification, related_request_id)


def test_notification_shape():
    notification = notification_for(event(4, "human_task.opened"), "sub-1")
    dumped = notification.model_dump(mode="json")
    assert dumped["method"] == "notifications/mission/event" == NOTIFICATION_METHOD
    params = dumped["params"]
    assert params["schema_version"] == "mc.event.v1"
    assert params["seq"] == 4 and params["event_type"] == "human_task.opened"
    assert params["subscription_id"] == "sub-1"
    assert "payload" not in params


@pytest.mark.asyncio
async def test_mission_subscribe_registers_a_session_subscription_and_notifies() -> None:
    store = InMemoryStore(events=[event(1, "run.admitted")])
    service = SubscriptionService(store)
    hub = McpSessionHub()
    server = create_coordinator_server(
        FakeFacade(),
        Resolver(frozenset({"workflow_run.read"})),
        subscriptions=McpSubscriptionBridge({SCOPE: service}, hub),
    )
    async with Client(server) as client:
        tools = {tool.name: tool for tool in await client.list_tools()}
        assert "mission_subscribe" in tools
        result = await client.call_tool(
            "mission_subscribe",
            {"target": "mission", "target_id": str(MISSION), "events": ["run.*", "human_task.*"]},
        )
        assert result.data["ok"] is True, result.data
        data = result.data["data"]
        assert data["notification_method"] == NOTIFICATION_METHOD
        assert data["cursor_seq"] == 1
        subscription = next(iter(store.subscriptions.values()))
        assert isinstance(subscription.channel, McpSessionChannel)
        assert subscription.channel.session_ref == data["session_ref"]
        assert hub.connected(data["session_ref"])

        # Record what goes on the wire while still forwarding to the live session. (The
        # Python MCP client drops notification methods it does not know, so the shape is
        # asserted at the session boundary as the JSON-RPC message the transport sends.)
        session = hub.session(data["session_ref"])
        hub.register(data["session_ref"], RecordingSession(session))

        store.events.extend([event(2, "human_task.opened"), event(3, "run.completed")])
        relay = SubscriptionRelay(
            store, transport=FakeWebhook(), secrets=StaticSecrets({}), notifier=hub
        )
        report = await relay.run_once()
        assert [seq for _, seq in report.delivered] == [2, 3]
        recorded = hub.session(data["session_ref"])
        assert isinstance(recorded, RecordingSession)
        assert [message.method for message in recorded.sent] == [NOTIFICATION_METHOD] * 2
        assert [message.params["seq"] for message in recorded.sent] == [2, 3]
        assert all(message.params["schema_version"] == "mc.event.v1" for message in recorded.sent)
        await client.ping()  # the session stays healthy after the notifications

    # After the client disconnected, the hub reports the session gone and the relay pauses.
    store.events.append(event(4, "run.completed"))
    hub.unregister(data["session_ref"])
    await SubscriptionRelay(
        store, transport=FakeWebhook(), secrets=StaticSecrets({}), notifier=hub
    ).run_once()
    assert next(iter(store.subscriptions.values())).state is SubscriptionState.PAUSED


@pytest.mark.asyncio
async def test_mission_subscribe_requires_workflow_run_read() -> None:
    store = InMemoryStore(events=[event(1)])
    server = create_coordinator_server(
        FakeFacade(),
        Resolver(frozenset()),
        subscriptions=McpSubscriptionBridge({SCOPE: SubscriptionService(store)}, McpSessionHub()),
    )
    async with Client(server) as client:
        result = await client.call_tool(
            "mission_subscribe",
            {"target": "mission", "target_id": str(MISSION), "events": ["*"]},
        )
    assert result.data["ok"] is False
    assert result.data["error"]["code"] == "FORBIDDEN"
    assert store.subscriptions == {}


@pytest.mark.asyncio
async def test_tool_is_absent_without_a_subscription_bridge() -> None:
    server = create_coordinator_server(FakeFacade(), Resolver(frozenset()))
    async with Client(server) as client:
        assert "mission_subscribe" not in {tool.name for tool in await client.list_tools()}
