"""MP-15: signed webhook deliveries, egress validation, inbox callbacks (FIXTURE transports)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from mission_control.adapters.subscriptions.webhook import (
    EgressGuardedWebhookTransport,
    EgressPolicy,
)
from mission_control.application.subscriptions.coordinator import delivery_id
from mission_control.application.subscriptions.coordinator_service import (
    CoordinatorInboxService,
    CoordinatorRejected,
    CoordinatorSubscribeRequest,
)
from mission_control.application.subscriptions.ports import WebhookEgressRejected
from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.application.subscriptions.webhook_signing import (
    DELIVERY_ATTEMPT_HEADER,
    DELIVERY_ID_HEADER,
    SIGNATURE_V1_HEADER,
    TIMESTAMP_HEADER,
    event_delivery_id,
    retry_schedule,
    signed_headers,
    verify_delivery,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.contracts import (
    DEAD_LETTER_AFTER,
    SubscriptionRequest,
    SubscriptionState,
    verify_signature,
)
from tests.unit.subscriptions.coordinator_fakes import RUN, InMemoryInboxStore, journal_event
from tests.unit.subscriptions.fakes import (
    MISSION,
    FakeWebhook,
    InMemoryStore,
    StaticSecrets,
    event,
)

SECRET = "environment:MC_HOOK"
SECRET_VALUE = "disposable-unit-key"
NOW = datetime(2026, 10, 9, 12, tzinfo=UTC)
READER = ActorContext(
    actor_id="coordinator-1", permissions=frozenset({"workflow_run.read", "workflow_run.control"})
)


def sent_at(item: Any) -> datetime:
    """The receiver's clock at arrival: the request's own signing timestamp."""

    return datetime.fromtimestamp(int(item.headers[TIMESTAMP_HEADER]), UTC)


def static_resolver(mapping: dict[str, Sequence[str]]):
    async def resolve(host: str, port: int) -> Sequence[str]:
        if host not in mapping:
            raise OSError("no such host")
        return mapping[host]

    return resolve


def test_signed_headers_verify_and_reject_tampering_staleness_and_replay():
    body = b'{"seq":1}'
    delivery = event_delivery_id(MISSION, RUN)
    headers = signed_headers(SECRET_VALUE.encode(), body, delivery=delivery, attempt=2, now=NOW)
    assert headers[DELIVERY_ID_HEADER] == str(delivery)
    assert headers[DELIVERY_ATTEMPT_HEADER] == "2"
    assert headers[TIMESTAMP_HEADER] == str(int(NOW.timestamp()))
    assert verify_signature(SECRET_VALUE.encode(), body, headers["X-MC-Signature"])
    key = SECRET_VALUE.encode()
    lowered = {name.lower(): value for name, value in headers.items()}
    assert verify_delivery(key, body, lowered, now=NOW + timedelta(seconds=30)).ok
    assert verify_delivery(key, body + b" ", headers, now=NOW).failure == "bad_signature"
    assert verify_delivery(b"other", body, headers, now=NOW).failure == "bad_signature"
    assert (
        verify_delivery(key, body, headers, now=NOW + timedelta(minutes=6)).failure
        == "stale_timestamp"
    )
    # The same body under another delivery id or a fresh timestamp does not verify.
    swapped = {**headers, DELIVERY_ID_HEADER: str(event_delivery_id(RUN, MISSION))}
    assert verify_delivery(key, body, swapped, now=NOW).failure == "bad_signature"
    later = int(NOW.timestamp()) + 60
    restamped = {
        **headers,
        TIMESTAMP_HEADER: str(later),
        SIGNATURE_V1_HEADER: headers[SIGNATURE_V1_HEADER].replace(
            f"t={int(NOW.timestamp())}", f"t={later}"
        ),
    }
    assert verify_delivery(key, body, restamped, now=NOW).failure == "bad_signature"
    missing = {k: v for k, v in headers.items() if k != TIMESTAMP_HEADER}
    assert verify_delivery(key, body, missing, now=NOW).failure == "missing_header"


def test_retry_schedule_is_bounded_exponential():
    schedule = retry_schedule()
    assert len(schedule) == DEAD_LETTER_AFTER - 1
    assert schedule[0] <= timedelta(seconds=2) and schedule[-1] <= timedelta(minutes=15)
    assert list(schedule) == sorted(schedule)


async def test_relay_signs_with_timestamp_and_a_delivery_id_stable_across_retries():
    store = InMemoryStore(events=[event(1, "run.admitted")])
    service = SubscriptionService(store, clock=lambda: NOW)
    await service.subscribe(
        SubscriptionRequest.model_validate(
            {
                "target": "mission",
                "target_id": str(MISSION),
                "events": ["*"],
                "channel": {
                    "kind": "webhook",
                    "url": "https://hooks.example/x",
                    "secret_ref": SECRET,
                },
                "after_seq": 0,
            }
        ),
        READER,
    )
    hook = FakeWebhook(statuses=[503, 204])
    clock = [NOW]
    relay = SubscriptionRelay(
        store,
        transport=hook,
        secrets=StaticSecrets({SECRET: SECRET_VALUE}),
        clock=lambda: clock[0],
        jitter=lambda: 0.5,
    )
    await relay.run_once()
    clock[0] += timedelta(hours=1)
    await relay.run_once()
    first, second = hook.received
    assert first.headers[DELIVERY_ID_HEADER] == second.headers[DELIVERY_ID_HEADER]
    assert [first.headers[DELIVERY_ATTEMPT_HEADER], second.headers[DELIVERY_ATTEMPT_HEADER]] == [
        "1",
        "2",
    ]
    for item in hook.received:
        assert verify_delivery(SECRET_VALUE.encode(), item.body, item.headers, now=sent_at(item)).ok


class RejectingTransport:
    def __init__(self) -> None:
        self.calls = 0

    async def post(self, url: str, body: bytes, headers: Any) -> Any:
        self.calls += 1
        raise WebhookEgressRejected("resolves into 10.0.0.5")


async def test_egress_rejection_dead_letters_on_the_first_attempt():
    store = InMemoryStore(events=[event(1, "run.admitted")])
    subscription = await SubscriptionService(store).subscribe(
        SubscriptionRequest.model_validate(
            {
                "target": "mission",
                "target_id": str(MISSION),
                "events": ["*"],
                "channel": {
                    "kind": "webhook",
                    "url": "https://internal.example",
                    "secret_ref": SECRET,
                },
                "after_seq": 0,
            }
        ),
        READER,
    )
    transport = RejectingTransport()
    report = await SubscriptionRelay(
        store, transport=transport, secrets=StaticSecrets({SECRET: SECRET_VALUE})
    ).run_once()
    assert report.dead_lettered == [str(subscription.subscription_id)]
    assert transport.calls == 1
    assert (
        store.subscriptions[subscription.subscription_id].state is SubscriptionState.DEAD_LETTERED
    )
    assert [item.error_class for item in store.receipts] == ["egress_rejected"]


@pytest.mark.parametrize(
    ("url", "answers", "loopback"),
    [
        ("https://hooks.internal/x", ["10.1.2.3"], False),
        ("https://metadata.internal/x", ["169.254.169.254"], False),
        ("https://mixed.example/x", ["93.184.216.34", "192.168.1.5"], False),
        ("https://v6.internal/x", ["fd00:ec2::254"], False),
        ("https://mapped.internal/x", ["::ffff:127.0.0.1"], False),
        ("https://loop.example/x", ["127.0.0.1"], False),
        ("http://public.example/x", ["93.184.216.34"], False),
        ("http://public.example/x", ["93.184.216.34"], True),
        ("https://[::1]/x", [], False),
        ("https://10.0.0.1/x", [], False),
        ("https://user:pw@public.example/x", ["93.184.216.34"], False),
        ("https://nowhere.example/x", None, False),
    ],
)
async def test_egress_policy_rejects_internal_and_ambiguous_destinations(
    url: str, answers: list[str] | None, loopback: bool
):
    host = url.split("//", 1)[1].split("/", 1)[0].rsplit("@", maxsplit=1)[-1]
    mapping = {} if answers is None else {host: answers}
    policy = EgressPolicy(allow_loopback=loopback, resolver=static_resolver(mapping))
    with pytest.raises(WebhookEgressRejected):
        await policy.validate(url)


async def test_egress_policy_allows_public_https_and_explicit_loopback_and_pins_the_address():
    policy = EgressPolicy(resolver=static_resolver({"hooks.example": ["93.184.216.34"]}))
    pinned = await policy.resolve("https://hooks.example:8443/cb?x=1")
    assert pinned.pinned_url == "https://93.184.216.34:8443/cb?x=1"
    assert pinned.host_header == "hooks.example:8443"
    local = EgressPolicy(
        allow_loopback=True, resolver=static_resolver({"localhost": ["127.0.0.1"]})
    )
    assert (
        await local.resolve("http://localhost:9000/hook")
    ).pinned_url == "http://127.0.0.1:9000/hook"
    ports = EgressPolicy(
        allowed_ports=frozenset({443}),
        resolver=static_resolver({"hooks.example": ["93.184.216.34"]}),
    )
    with pytest.raises(WebhookEgressRejected):
        await ports.validate("https://hooks.example:8443/cb")
    allowed = EgressPolicy(
        allowed_networks=("10.20.0.0/16",),
        resolver=static_resolver({"partner.internal": ["10.20.1.1"]}),
    )
    await allowed.validate("https://partner.internal/cb")


async def test_guarded_transport_connects_to_the_validated_address_with_host_and_sni():
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(204)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    transport = EgressGuardedWebhookTransport(
        EgressPolicy(resolver=static_resolver({"hooks.example": ["93.184.216.34"]})), client
    )
    response = await transport.post("https://hooks.example/cb", b"{}", {"X-A": "1"})
    assert response.status_code == 204
    (request,) = seen
    assert request.url.host == "93.184.216.34"
    assert request.headers["host"] == "hooks.example"
    assert request.extensions["sni_hostname"] == "hooks.example"
    with pytest.raises(WebhookEgressRejected):
        await EgressGuardedWebhookTransport(
            EgressPolicy(resolver=static_resolver({"hooks.example": ["10.0.0.9"]})), client
        ).post("https://hooks.example/cb", b"{}", {})
    assert len(seen) == 1  # nothing was sent to the internal address
    await client.aclose()


async def test_inbox_webhook_callbacks_retry_without_touching_run_work_and_dead_letter():
    subscriptions = InMemoryStore()
    inbox = InMemoryInboxStore(subscriptions)
    inbox.add(journal_event(1, "human_task.created"), human_task_id="t-1")
    inbox.add(journal_event(2, "workflow_run.terminalize"), terminal_outcome="completed")
    hook = FakeWebhook(statuses=[503, 500, 204, 204])
    clock = [NOW]

    class NoCommands:
        calls = 0

        async def inspect(self, *_: Any) -> Any:
            self.calls += 1
            raise AssertionError("callbacks never inspect or command a run")

        async def command(self, *_: Any) -> Any:
            self.calls += 1
            raise AssertionError("callbacks never inspect or command a run")

    commands = NoCommands()
    service = CoordinatorInboxService(
        subscriptions,
        inbox,
        commands=commands,
        transport=hook,
        secrets=StaticSecrets({SECRET: SECRET_VALUE}),
        clock=lambda: clock[0],
        jitter=lambda: 0.5,
    )
    created = await service.subscribe(
        CoordinatorSubscribeRequest.model_validate(
            {
                "target": "run",
                "target_id": str(RUN),
                "after_seq": 0,
                "profile": {"batch_window_seconds": 0},
                "delivery": {
                    "kind": "webhook",
                    "url": "https://hooks.example/cb",
                    "secret_ref": SECRET,
                },
            }
        ),
        READER,
    )
    sid = created.subscription_id
    for _ in range(4):
        await service.deliver_callbacks()
        clock[0] += timedelta(hours=1)
    bodies = [json.loads(item.body) for item in hook.received]
    assert [body["inbox_seq"] for body in bodies] == [1, 1, 1, 2]
    delivery_ids = [item.headers[DELIVERY_ID_HEADER] for item in hook.received]
    assert delivery_ids[:3] == [str(delivery_id(sid, bodies[0]["notification_id"]))] * 3
    assert len(set(delivery_ids)) == 2
    for item in hook.received:
        assert verify_delivery(SECRET_VALUE.encode(), item.body, item.headers, now=sent_at(item)).ok
        assert "payload" not in json.loads(item.body)
    assert (await inbox.get(sid)).acked_inbox_seq == 2
    assert commands.calls == 0
    statuses = [(receipt.status.value, receipt.attempt) for receipt in subscriptions.receipts]
    assert statuses == [("failed", 1), ("failed", 2), ("delivered", 3), ("delivered", 1)]
    # A dead destination dead-letters after twelve attempts; the poll fallback still works.
    inbox.add(journal_event(3, "attempt.completed"), outcome="failed")
    hook.statuses = []
    hook.default_status = 503
    for _ in range(DEAD_LETTER_AFTER):
        await service.deliver_callbacks()
        clock[0] += timedelta(hours=1)
    assert subscriptions.subscriptions[sid].state is SubscriptionState.DEAD_LETTERED
    page = await service.poll(sid, READER)
    assert [item.kind.value for item in page.notifications] == ["failed"]
    assert commands.calls == 0


async def test_inbox_webhook_subscription_rejects_internal_destinations():
    subscriptions = InMemoryStore()
    service = CoordinatorInboxService(
        subscriptions,
        InMemoryInboxStore(subscriptions),
        destinations=EgressPolicy(resolver=static_resolver({"hooks.internal": ["10.0.0.7"]})),
    )
    with pytest.raises(CoordinatorRejected) as refused:
        await service.subscribe(
            CoordinatorSubscribeRequest.model_validate(
                {
                    "target": "run",
                    "target_id": str(RUN),
                    "delivery": {
                        "kind": "webhook",
                        "url": "https://hooks.internal/cb",
                        "secret_ref": SECRET,
                    },
                }
            ),
            READER,
        )
    assert refused.value.code == "egress_rejected"
    assert subscriptions.subscriptions == {}
