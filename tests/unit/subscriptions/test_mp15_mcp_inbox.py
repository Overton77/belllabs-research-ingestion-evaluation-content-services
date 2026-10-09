"""MP-15: connected-MCP coordinator consumers (subscribe/poll/ack/command tools, inbox resource)."""

from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from fastmcp import Client, Context
from mcp.types import JSONRPCNotification

from mission_control.application.subscriptions.coordinator_service import CoordinatorInboxService
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.interfaces.mcp.coordinator_server import (
    CoordinatorPrincipal,
    create_coordinator_server,
)
from mission_control.interfaces.mcp.subscriptions import (
    INBOX_NOTIFICATION_METHOD,
    INBOX_TOOL_NAMES,
    McpSessionHub,
    McpSubscriptionBridge,
)
from tests.unit.coordinator.test_coordinator_mcp_read_surface import FakeFacade
from tests.unit.subscriptions.coordinator_fakes import (
    RUN,
    FakeGateway,
    InMemoryInboxStore,
    journal_event,
)
from tests.unit.subscriptions.fakes import SCOPE, InMemoryStore


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


def compose(
    permissions: frozenset[str] = frozenset({"workflow_run.read", "workflow_run.control"}),
) -> tuple[Any, InMemoryInboxStore, McpSessionHub, FakeGateway]:
    subscriptions = InMemoryStore()
    inbox = InMemoryInboxStore(subscriptions)
    hub = McpSessionHub()
    gateway = FakeGateway()
    inboxes = CoordinatorInboxService(subscriptions, inbox, commands=gateway, notifier=hub)
    server = create_coordinator_server(
        FakeFacade(),
        Resolver(permissions),
        subscriptions=McpSubscriptionBridge(
            {SCOPE: SubscriptionService(subscriptions)}, hub, inboxes={SCOPE: inboxes}
        ),
    )
    return server, inbox, hub, gateway


async def test_inbox_tools_poll_ack_and_resource_without_any_push():
    server, inbox, _hub, _gateway = compose()
    inbox.add(journal_event(1, "human_task.created"), human_task_id="t-1")
    inbox.add(journal_event(2, "tool_call.completed"))
    async with Client(server) as client:
        names = {tool.name for tool in await client.list_tools()}
        assert set(INBOX_TOOL_NAMES) <= names
        created = await client.call_tool(
            "coordinator_inbox_subscribe",
            {"target": "run", "target_id": str(RUN), "after_seq": 0},
        )
        assert created.data["ok"] is True, created.data
        data = created.data["data"]
        assert data["delivery"] == "poll" and data["notification_qualified"] is False
        assert data["profile"]["include_deltas"] is False
        sid = data["subscription_id"]
        polled = await client.call_tool("coordinator_inbox_poll", {"subscription_id": sid})
        page = polled.data["data"]
        assert [item["kind"] for item in page["notifications"]] == ["review_required"]
        (review,) = page["notifications"]
        assert review["actionable"] is True and review["facts"]["human_task_id"] == "t-1"
        resource = await client.read_resource(f"mc://coordinator-inboxes/{sid}")
        assert (
            json.loads(resource[0].text)["notifications"][0]["notification_id"]
            == (  # type: ignore[union-attr]
                review["notification_id"]
            )
        )
        acked = await client.call_tool(
            "coordinator_inbox_ack",
            {"subscription_id": sid, "notification_ids": [review["notification_id"]]},
        )
        assert acked.data["data"]["acked_inbox_seq"] == 1
        again = await client.call_tool("coordinator_inbox_poll", {"subscription_id": sid})
        assert again.data["data"]["notifications"] == []


async def test_connected_session_gets_a_cursor_only_wake_up_and_still_polls():
    server, inbox, hub, _gateway = compose()
    async with Client(server) as client:
        created = await client.call_tool(
            "coordinator_inbox_subscribe",
            {
                "target": "run",
                "target_id": str(RUN),
                "after_seq": 0,
                "delivery": "mcp_session",
                "profile": {"batch_window_seconds": 0},
            },
        )
        data = created.data["data"]
        assert data["notification_method"] == INBOX_NOTIFICATION_METHOD
        assert data["notification_qualified"] is True
        session_ref = next(iter(hub._sessions))
        recording = RecordingSession(hub.session(session_ref))
        hub.register(session_ref, recording)
        inbox.add(journal_event(1, "human_task.created"), human_task_id="t-secret-free")
        polled = await client.call_tool(
            "coordinator_inbox_poll", {"subscription_id": data["subscription_id"]}
        )
        assert len(polled.data["data"]["notifications"]) == 1
        (hint,) = recording.sent
        assert hint.method == INBOX_NOTIFICATION_METHOD
        assert hint.params == {
            "subscription_id": data["subscription_id"],
            "high_inbox_seq": 1,
            "acked_inbox_seq": 0,
            "pending": 1,
            "poll_tool": "coordinator_inbox_poll",
        }
    # Disconnected: no hint can be sent, nothing is lost, polling still returns the item.
    hub.unregister(session_ref)
    inbox.add(journal_event(2, "workflow_run.terminalize"))
    async with Client(server) as client:
        polled = await client.call_tool(
            "coordinator_inbox_poll", {"subscription_id": data["subscription_id"]}
        )
        assert [item["kind"] for item in polled.data["data"]["notifications"]] == [
            "review_required",
            "terminal_result",
        ]


async def test_command_from_notification_maps_the_recursion_bound_to_a_typed_error():
    server, inbox, _hub, gateway = compose()
    inbox.add(journal_event(1, "human_task.created"))
    async with Client(server) as client:
        created = await client.call_tool(
            "coordinator_inbox_subscribe",
            {
                "target": "run",
                "target_id": str(RUN),
                "after_seq": 0,
                "profile": {"max_recursion_depth": 0},
            },
        )
        sid = created.data["data"]["subscription_id"]
        page = (await client.call_tool("coordinator_inbox_poll", {"subscription_id": sid})).data
        notification = page["data"]["notifications"][0]["notification_id"]
        result = await client.call_tool(
            "coordinator_inbox_command",
            {
                "subscription_id": sid,
                "notification_id": notification,
                "command": {
                    "request_id": str(uuid4()),
                    "expected_version": 3,
                    "expected_generation": 1,
                    "target": {"kind": "run", "id": "run-key-1"},
                    "kind": "queue_instruction",
                    "payload": {"content": {"text": "react"}},
                    "reason": "react to the review",
                },
            },
        )
    assert result.data["ok"] is False
    assert result.data["error"]["code"] == "CONFLICT"
    assert result.data["error"]["details"] == {"reason": "recursion_bound"}
    assert gateway.calls == []


async def test_inbox_tools_require_read_and_are_absent_without_inbox_services():
    server, _inbox, _hub, _gateway = compose(frozenset())
    async with Client(server) as client:
        refused = await client.call_tool(
            "coordinator_inbox_subscribe", {"target": "run", "target_id": str(RUN)}
        )
    assert refused.data["ok"] is False and refused.data["error"]["code"] == "FORBIDDEN"
    subscriptions = InMemoryStore()
    bare = create_coordinator_server(
        FakeFacade(),
        Resolver(frozenset({"workflow_run.read"})),
        subscriptions=McpSubscriptionBridge(
            {SCOPE: SubscriptionService(subscriptions)}, McpSessionHub()
        ),
    )
    async with Client(bare) as client:
        names = {tool.name for tool in await client.list_tools()}
    assert "mission_subscribe" in names and not set(INBOX_TOOL_NAMES) & names
