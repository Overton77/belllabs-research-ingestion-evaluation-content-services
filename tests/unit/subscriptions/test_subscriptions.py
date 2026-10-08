"""FT-F5: subscription rules, relay delivery (at least once, HMAC, backoff, dead letter),
SSE stream framing and the HTTP surface, all against in-memory doubles (no network)."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from uuid import UUID

import httpx
import pytest
from fastapi import FastAPI

from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import (
    SubscriptionRejected,
    SubscriptionService,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.contracts import (
    DEAD_LETTER_AFTER,
    McpSessionChannel,
    MissionEventEnvelope,
    StreamTicketChannel,
    SubscriptionFilters,
    SubscriptionRequest,
    SubscriptionState,
    WebhookChannel,
    after_failure,
    event_type_matches,
    retry_delay,
    sign,
    sse_frame,
    verify_signature,
)
from mission_control.interfaces.http import mission_control as http_mc
from mission_control.interfaces.http import subscriptions as http_subscriptions
from mission_control.interfaces.http.subscriptions import router
from tests.unit.subscriptions.fakes import (
    MISSION,
    RUN,
    SCOPE,
    T0,
    Crash,
    FakeWebhook,
    InMemoryStore,
    StaticSecrets,
    event,
)

SECRET = "environment:MC_TEST_WEBHOOK_SECRET"
SECRET_VALUE = "local-test-signing-key"
READER = ActorContext(actor_id="actor:reader", permissions=frozenset({"workflow_run.read"}))
OTHER = ActorContext(actor_id="actor:other", permissions=frozenset({"workflow_run.read"}))
ADMIN = ActorContext(actor_id="actor:admin", permissions=frozenset({"workflow_run.admin"}))
NOBODY = ActorContext(actor_id="actor:nobody")


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self):  # type: ignore[no-untyped-def]
        return self.now


def webhook_request(**overrides: Any) -> SubscriptionRequest:
    body: dict[str, Any] = {
        "target": "mission",
        "target_id": str(MISSION),
        "events": ["*"],
        "channel": {"kind": "webhook", "url": "http://127.0.0.1:9/hook", "secret_ref": SECRET},
        "after_seq": 0,
        **overrides,
    }
    return SubscriptionRequest.model_validate(body)


def relay(
    store: InMemoryStore, hook: FakeWebhook, clock: Clock, **kwargs: Any
) -> SubscriptionRelay:
    return SubscriptionRelay(
        store,
        transport=hook,
        secrets=StaticSecrets({SECRET: SECRET_VALUE}),
        owner="relay-test",
        clock=clock,
        jitter=lambda: 0.5,
        **kwargs,
    )


# --- pure rules ---------------------------------------------------------------------------


def test_event_type_patterns_and_node_filters():
    assert event_type_matches("*", "anything.at_all")
    assert event_type_matches("human_task.*", "human_task.opened")
    assert not event_type_matches("human_task.*", "human_tasks.opened")
    assert event_type_matches("run.completed", "run.completed")
    assert not event_type_matches("run.completed", "run.completed_late")
    filters = SubscriptionFilters(event_types=("run.*",), node_keys=("collect",))
    assert filters.matches(event(1, "run.progressed", node_key="collect"))
    assert not filters.matches(event(1, "run.progressed", node_key="review"))
    assert not filters.matches(event(1, "human_task.opened", node_key="collect"))
    with pytest.raises(ValueError):
        SubscriptionFilters(event_types=("Run Completed",))


def test_hmac_signature_is_over_the_raw_body():
    body = event(3).body()
    header = sign(SECRET_VALUE.encode(), body)
    assert header.startswith("sha256=") and len(header) == 7 + 64
    assert verify_signature(SECRET_VALUE.encode(), body, header)
    assert not verify_signature(SECRET_VALUE.encode(), body + b" ", header)
    assert not verify_signature(b"another-key", body, header)


def test_event_envelope_carries_references_never_bodies():
    document = json.loads(event(1).body())
    assert document["schema_version"] == "mc.event.v1"
    assert "payload" not in document
    assert document["payload_ref"].endswith("/events/1")
    assert document["payload_digest"].startswith("sha256:")


def test_backoff_is_exponential_with_full_jitter_and_dead_letters_at_twelve():
    assert retry_delay(1, 0.999) <= timedelta(seconds=2)
    assert retry_delay(5, 0.999) <= timedelta(seconds=32)
    assert retry_delay(30, 0.999) <= timedelta(minutes=15)
    assert retry_delay(3, 0.0) == timedelta(seconds=0.5)
    with pytest.raises(ValueError):
        retry_delay(0, 0.5)
    with pytest.raises(ValueError):
        retry_delay(1, 1.0)
    assert not after_failure(DEAD_LETTER_AFTER - 2, T0, 0.1).dead_letter
    assert after_failure(DEAD_LETTER_AFTER - 1, T0, 0.1).dead_letter


def test_channels_hold_references_only():
    channel = WebhookChannel.model_validate(
        {"kind": "webhook", "url": "https://hooks.example/x", "secret_ref": SECRET}
    )
    assert channel.secret_ref.provider == "environment"
    assert channel.secret_ref.key == "MC_TEST_WEBHOOK_SECRET"
    for url in ("http://hooks.example/x", "https://user:pw@hooks.example/x", "ftp://x"):
        with pytest.raises(ValueError):
            WebhookChannel.model_validate({"kind": "webhook", "url": url, "secret_ref": SECRET})
    with pytest.raises(ValueError):
        WebhookChannel.model_validate(
            {
                "kind": "webhook",
                "url": "https://h/x",
                "secret_ref": {"provider": "environment", "key": "K", "value": "leak"},
            }
        )
    with pytest.raises(ValueError):
        WebhookChannel.model_validate(
            {"kind": "webhook", "url": "https://h/x", "secret_ref": "not-a-ref"}
        )


def test_sse_frames():
    assert sse_frame("mission_event", '{"a":1}', event_id="7") == (
        'event: mission_event\nid: 7\ndata: {"a":1}\n\n'
    )
    assert sse_frame("heartbeat", "{}").startswith("event: heartbeat\n")


# --- service and grants ----------------------------------------------------------------------


async def test_subscribe_requires_read_and_close_requires_owner_or_admin():
    store = InMemoryStore(events=[event(1), event(2)])
    service = SubscriptionService(store, clock=Clock())
    with pytest.raises(SubscriptionRejected) as denied:
        await service.subscribe(webhook_request(), NOBODY)
    assert denied.value.code == "unauthorized"
    created = await service.subscribe(webhook_request(after_seq=None), READER)
    assert created.cursor_seq == 2  # starts from now unless a replay cursor is given
    with pytest.raises(SubscriptionRejected) as other:
        await service.close(created.subscription_id, OTHER)
    assert other.value.code == "unauthorized"
    closed = await service.close(created.subscription_id, ADMIN)
    assert closed.state is SubscriptionState.CLOSED
    own = await service.subscribe(webhook_request(), READER)
    assert (await service.close(own.subscription_id, READER)).state is SubscriptionState.CLOSED
    with pytest.raises(SubscriptionRejected) as missing:
        await service.subscribe(webhook_request(target_id=str(UUID(int=99))), READER)
    assert missing.value.code == "not_found"


# --- relay ----------------------------------------------------------------------------------


async def test_relay_delivers_matching_events_in_order_with_signed_bodies():
    store = InMemoryStore(
        events=[event(1, "run.admitted"), event(2, "human_task.opened"), event(3, "run.completed")]
    )
    clock = Clock()
    subscription = await SubscriptionService(store, clock=clock).subscribe(
        webhook_request(events=["human_task.*", "run.completed"]), READER
    )
    hook = FakeWebhook()
    report = await relay(store, hook, clock).run_once()
    assert [item.headers["X-MC-Event-Seq"] for item in hook.received] == ["2", "3"]
    for item in hook.received:
        assert verify_signature(SECRET_VALUE.encode(), item.body, item.headers["X-MC-Signature"])
        assert json.loads(item.body)["event_id"] == item.headers["X-MC-Event-Id"]
    assert report.delivered == [
        (str(subscription.subscription_id), 2),
        (str(subscription.subscription_id), 3),
    ]
    current = store.subscriptions[subscription.subscription_id]
    assert current.cursor_seq == 3 and current.failure_count == 0
    assert [(item.seq, item.status.value, item.response_code) for item in store.receipts] == [
        (2, "delivered", 204),
        (3, "delivered", 204),
    ]
    assert store.leases == {}
    # Nothing new: a second pass sends nothing.
    await relay(store, hook, clock).run_once()
    assert len(hook.received) == 2


async def test_at_least_once_after_a_relay_crash_mid_batch():
    store = InMemoryStore(events=[event(seq) for seq in range(1, 6)], crash_on_success=3)
    clock = Clock()
    await SubscriptionService(store, clock=clock).subscribe(webhook_request(), READER)
    hook = FakeWebhook()
    with pytest.raises(Crash):
        await relay(store, hook, clock).run_once()
    store.leases.clear()  # a dead process's lease expires
    await relay(store, hook, clock).run_once()
    seqs = [int(item.headers["X-MC-Event-Seq"]) for item in hook.received]
    ids = [item.headers["X-MC-Event-Id"] for item in hook.received]
    assert seqs == [1, 2, 3, 3, 4, 5]  # event 3 sent twice, nothing skipped
    assert ids[2] == ids[3]
    assert sorted(set(seqs)) == list(range(1, 6))
    assert [item.seq for item in store.receipts] == [1, 2, 3, 4, 5]


async def test_failures_back_off_then_dead_letter_after_twelve():
    store = InMemoryStore(events=[event(1), event(2)])
    clock = Clock()
    subscription = await SubscriptionService(store, clock=clock).subscribe(
        webhook_request(), READER
    )
    hook = FakeWebhook(default_status=500)
    for attempt in range(1, DEAD_LETTER_AFTER + 1):
        report = await relay(store, hook, clock).run_once()
        current = store.subscriptions[subscription.subscription_id]
        assert current.failure_count == attempt
        if attempt < DEAD_LETTER_AFTER:
            assert current.state is SubscriptionState.ACTIVE
            assert current.next_attempt_at > clock.now
            # Not due yet: an immediate pass leases nothing.
            assert (await relay(store, hook, clock).run_once()).leased == 0
            clock.now = current.next_attempt_at
        else:
            assert report.dead_lettered == [str(subscription.subscription_id)]
    current = store.subscriptions[subscription.subscription_id]
    assert current.state is SubscriptionState.DEAD_LETTERED
    assert current.cursor_seq == 0  # never advanced past the undelivered event
    assert store.dead_letter_events == [subscription.subscription_id]
    assert [item.attempt for item in store.receipts] == list(range(1, DEAD_LETTER_AFTER + 1))
    assert store.receipts[-1].status.value == "dead_lettered"
    assert {item.headers["X-MC-Event-Seq"] for item in hook.received} == {"1"}
    # A dead-lettered subscription is not leased again until resumed.
    clock.now += timedelta(days=1)
    assert (await relay(store, hook, clock).run_once()).leased == 0
    resumed = await SubscriptionService(store, clock=clock).resume(
        subscription.subscription_id, READER
    )
    assert resumed.state is SubscriptionState.ACTIVE and resumed.failure_count == 0


async def test_transport_errors_and_missing_secrets_count_as_failures():
    store = InMemoryStore(events=[event(1)])
    clock = Clock()
    await SubscriptionService(store, clock=clock).subscribe(webhook_request(), READER)
    await relay(store, FakeWebhook(fail_transport=True), clock).run_once()
    assert store.receipts[-1].error_class == "transport_error"
    clock.now += timedelta(hours=1)
    unsigned = SubscriptionRelay(
        store,
        transport=FakeWebhook(),
        secrets=StaticSecrets({}),
        owner="relay-test",
        clock=clock,
        jitter=lambda: 0.5,
    )
    await unsigned.run_once()
    assert store.receipts[-1].error_class == "secret_unavailable"
    assert {item.status.value for item in store.receipts} == {"failed"}


async def test_mcp_session_channel_pauses_when_the_session_is_gone():
    class Notifier:
        def __init__(self, connected: bool) -> None:
            self.connected = connected
            self.sent: list[MissionEventEnvelope] = []

        async def notify(self, session_ref: str, envelope: MissionEventEnvelope) -> bool:
            if self.connected:
                self.sent.append(envelope)
            return self.connected

    store = InMemoryStore(events=[event(1), event(2)])
    clock = Clock()
    request = webhook_request(channel={"kind": "mcp_session", "session_ref": "session-1"})
    subscription = await SubscriptionService(store, clock=clock).subscribe(request, READER)
    live = Notifier(True)
    await relay(store, FakeWebhook(), clock, notifier=live).run_once()
    assert [item.seq for item in live.sent] == [1, 2]
    store.events.append(event(3))
    await relay(store, FakeWebhook(), clock, notifier=Notifier(False)).run_once()
    current = store.subscriptions[subscription.subscription_id]
    assert current.state is SubscriptionState.PAUSED and current.cursor_seq == 2
    assert isinstance(current.channel, McpSessionChannel)


async def test_stream_ticket_subscriptions_are_not_pushed():
    store = InMemoryStore(events=[event(1)])
    clock = Clock()
    request = webhook_request(channel={"kind": "stream_ticket", "ticket_id": "t-1"})
    subscription = await SubscriptionService(store, clock=clock).subscribe(request, READER)
    assert isinstance(subscription.channel, StreamTicketChannel)
    assert (await relay(store, FakeWebhook(), clock).run_once()).leased == 0


async def test_run_target_receives_only_its_run():
    other = event(2).model_copy(update={"run_id": UUID(int=5)})
    store = InMemoryStore(events=[event(1), other, event(3)])
    clock = Clock()
    await SubscriptionService(store, clock=clock).subscribe(
        webhook_request(target="run", target_id=str(RUN)), READER
    )
    hook = FakeWebhook()
    await relay(store, hook, clock).run_once()
    assert [item.headers["X-MC-Event-Seq"] for item in hook.received] == ["1", "3"]


# --- SSE ------------------------------------------------------------------------------------


async def collect(frames: Any) -> list[str]:
    return [frame async for frame in frames]


async def test_stream_replays_then_heartbeats_while_idle():
    store = InMemoryStore(events=[event(1, "run.admitted"), event(2, "human_task.opened")])
    slept: list[float] = []

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    service = SubscriptionService(store, clock=Clock(), sleep=sleep)
    frames = await collect(
        await service.stream(
            MISSION, READER, after_seq=0, heartbeat_seconds=2, poll_seconds=1, idle_polls=3
        )
    )
    assert frames[0].startswith("event: mission_event\nid: 1\n")
    assert frames[1].startswith("event: mission_event\nid: 2\n")
    assert [frame.split("\n")[0] for frame in frames[2:]] == ["event: heartbeat"]
    filtered = await collect(
        await service.stream(
            MISSION, READER, after_seq=0, event_types=("human_task.*",), idle_polls=0
        )
    )
    assert len(filtered) == 1 and "id: 2" in filtered[0]


async def test_stream_signals_resync_for_an_expired_cursor():
    store = InMemoryStore(events=[event(40), event(41)], oldest=40)
    service = SubscriptionService(store, clock=Clock())
    frames = await collect(await service.stream(MISSION, READER, after_seq=10, idle_polls=0))
    assert frames == [
        'event: resync_required\ndata: {"code": "CURSOR_EXPIRED", "oldest_seq": 40}\n\n'
    ]
    with pytest.raises(SubscriptionRejected):
        await service.stream(MISSION, NOBODY)


# --- HTTP -----------------------------------------------------------------------------------


def http_app(service: SubscriptionService, actor: ActorContext) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    installation, _, tenant = SCOPE.split("/")[1:]
    principal = http_mc.MissionPrincipal(
        installation_id=UUID(installation),
        application_id="biotech",
        tenant_id=UUID(tenant),
        issuer="https://issuer.invalid",
        audiences=frozenset({"authenticated"}),
        actor=actor,
    )
    app.dependency_overrides[http_mc.get_mission_principal] = lambda: principal
    app.state.mission_control_subscription_services = {
        (UUID(installation), "biotech", UUID(tenant)): service
    }
    return app


async def test_http_subscribe_list_close_and_sse(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(http_subscriptions, "authorize_application", lambda *_args: None)
    store = InMemoryStore(events=[event(1, "run.admitted"), event(2, "run.completed")])
    service = SubscriptionService(store, clock=Clock(), live_idle_polls=0)
    app = http_app(service, READER)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        prefix = "/v1/applications/biotech"
        created = await client.post(
            f"{prefix}/subscriptions",
            json={
                "target": "mission",
                "target_id": str(MISSION),
                "events": ["run.*"],
                "channel": {
                    "kind": "webhook",
                    "url": "https://hooks.example/h",
                    "secret_ref": SECRET,
                },
            },
        )
        assert created.status_code == 201, created.text
        body = created.json()
        assert body["channel"]["secret_ref"] == SECRET
        assert "request_scope" not in body
        listed = await client.get(f"{prefix}/subscriptions")
        assert [item["subscription_id"] for item in listed.json()["subscriptions"]] == [
            body["subscription_id"]
        ]
        events = await client.get(f"{prefix}/missions/{MISSION}/events", params={"after_seq": 1})
        assert events.headers["content-type"].startswith("text/event-stream")
        assert "id: 2" in events.text and "id: 1\n" not in events.text
        resumed = await client.get(
            f"{prefix}/missions/{MISSION}/events", headers={"Last-Event-ID": "0"}
        )
        assert resumed.text.count("event: mission_event") == 2
        closed = await client.delete(f"{prefix}/subscriptions/{body['subscription_id']}")
        assert closed.json()["state"] == "closed"
        missing = await client.get(f"{prefix}/missions/{UUID(int=9)}/events")
        assert missing.status_code == 404
    denied = http_app(service, NOBODY)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=denied), base_url="http://test"
    ) as client:
        response = await client.get(f"/v1/applications/biotech/missions/{MISSION}/events")
        assert response.status_code == 403
