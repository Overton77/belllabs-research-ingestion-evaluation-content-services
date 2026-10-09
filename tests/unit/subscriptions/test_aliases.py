"""Public event aliases (MP-13): `run.completed`, `activation.completed`, `human_task.opened`
are derived notifications of canonical events, delivered once per canonical event."""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from mission_control.application.subscriptions.aliases import (
    PUBLIC_ALIASES,
    derive_notification,
    derived_event_id,
    select,
)
from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.contracts import (
    SubscriptionFilters,
    SubscriptionRequest,
    verify_signature,
)
from tests.unit.subscriptions.fakes import (
    MISSION,
    T0,
    FakeWebhook,
    InMemoryStore,
    StaticSecrets,
    event,
)

SECRET = "environment:MC_TEST_WEBHOOK_SECRET"
SECRET_VALUE = "local-test-signing-key"
READER = ActorContext(actor_id="actor:reader", permissions=frozenset({"workflow_run.read"}))

# A run's canonical journal: lifecycle transitions that must NOT read as completions, the
# canonical transitions that must, and frame-derived events around them.
JOURNAL = (
    (1, "workflow_run.admitted"),
    (2, "activation.phase_changed"),  # {executing}
    (3, "workflow_run.set_wait"),  # a wait is not a human task
    (4, "activation.phase_changed"),  # {follow_up_turn}: activation still open
    (5, "activation.lifecycle_changed"),  # {waiting, in_doubt}: still open
    (6, "human_task.created"),
    (7, "attempt.completed"),
    (8, "session.ended"),
    (9, "workflow_run.terminalize"),
    (10, "human_task.opened"),  # a writer appending the public name itself
)


def journal() -> list[Any]:
    return [event(seq, name) for seq, name in JOURNAL]


def filters(*names: str) -> SubscriptionFilters:
    return SubscriptionFilters(event_types=names)


class Clock:
    def __init__(self) -> None:
        self.now = T0

    def __call__(self):  # type: ignore[no-untyped-def]
        return self.now


def test_each_public_name_comes_from_exactly_one_canonical_type():
    assert PUBLIC_ALIASES == {
        "workflow_run.terminalize": "run.completed",
        "attempt.completed": "activation.completed",
        "human_task.created": "human_task.opened",
    }
    for name in ("activation.phase_changed", "activation.lifecycle_changed"):
        assert derive_notification(event(1, name)) is None
    assert derive_notification(event(1, "workflow_run.set_wait")) is None


def test_derived_notification_links_its_canonical_event_and_keeps_the_payload():
    canonical = event(9, "workflow_run.terminalize")
    derived = derive_notification(canonical)
    assert derived is not None
    assert derived.event_type == "run.completed"
    assert derived.event_id == derived_event_id("run.completed", canonical.event_id)
    assert derived.event_id != canonical.event_id
    assert derived.causation_ref == f"mission_event:{canonical.event_id}"
    assert (derived.seq, derived.payload_ref, derived.payload_digest) == (
        canonical.seq,
        canonical.payload_ref,
        canonical.payload_digest,
    )
    # Stable: the same canonical event always yields the same derived id.
    again = derive_notification(canonical)
    assert again is not None and again.event_id == derived.event_id


def test_filters_receive_only_the_correct_derived_transitions():
    events = journal()
    for public, expected_seq in (
        ("run.completed", [9]),
        ("activation.completed", [7]),
        ("human_task.opened", [6, 10]),
    ):
        selected = [item for item in (select(filters(public), e) for e in events) if item]
        assert [item.envelope.seq for item in selected] == expected_seq, public
        assert {item.envelope.event_type for item in selected} == {public}
    run_completed = [select(filters("run.completed"), e) for e in events]
    chosen = [item for item in run_completed if item is not None]
    assert chosen[0].derived and chosen[0].canonical_event_id == events[8].event_id
    # The pass-through public name is the canonical event itself, not a derivation.
    opened = [select(filters("human_task.opened"), e) for e in events]
    assert [item.derived for item in opened if item is not None] == [True, False]


def test_canonical_filters_are_unchanged_and_one_envelope_per_event():
    events = journal()
    everything = [select(filters("*"), e) for e in events]
    assert all(item is not None and not item.derived for item in everything)
    both = filters("workflow_run.terminalize", "run.completed")
    chosen = [item for item in (select(both, e) for e in events) if item is not None]
    assert [(item.envelope.seq, item.envelope.event_type) for item in chosen] == [
        (9, "workflow_run.terminalize")
    ]
    family = filters("activation.*")
    chosen = [item for item in (select(family, e) for e in events) if item is not None]
    assert [(item.envelope.seq, item.envelope.event_type) for item in chosen] == [
        (2, "activation.phase_changed"),
        (4, "activation.phase_changed"),
        (5, "activation.lifecycle_changed"),
        (7, "activation.completed"),
    ]


async def test_relay_delivers_derived_notifications_signed_with_canonical_receipts():
    store = InMemoryStore(events=journal())
    clock = Clock()
    request = SubscriptionRequest.model_validate(
        {
            "target": "mission",
            "target_id": str(MISSION),
            "events": ["run.completed", "activation.completed", "human_task.opened"],
            "channel": {"kind": "webhook", "url": "http://127.0.0.1:9/hook", "secret_ref": SECRET},
            "after_seq": 0,
        }
    )
    subscription = await SubscriptionService(store, clock=clock).subscribe(request, READER)
    hook = FakeWebhook()
    relay = SubscriptionRelay(
        store,
        transport=hook,
        secrets=StaticSecrets({SECRET: SECRET_VALUE}),
        owner="relay-test",
        clock=clock,
        jitter=lambda: 0.5,
    )
    await relay.run_once()
    bodies = [json.loads(item.body) for item in hook.received]
    assert [(body["seq"], body["event_type"]) for body in bodies] == [
        (6, "human_task.opened"),
        (7, "activation.completed"),
        (9, "run.completed"),
        (10, "human_task.opened"),
    ]
    by_seq = {item.seq: item for item in journal()}
    for item, body in zip(hook.received, bodies, strict=True):
        assert verify_signature(SECRET_VALUE.encode(), item.body, item.headers["X-MC-Signature"])
        assert item.headers["X-MC-Event-Id"] == body["event_id"]
        canonical = by_seq[body["seq"]]
        if body["seq"] != 10:
            assert body["causation_ref"] == f"mission_event:{canonical.event_id}"
            assert body["event_id"] == str(derived_event_id(body["event_type"], canonical.event_id))
    # Receipts reference the canonical events (the delivery table's foreign key).
    assert [(r.seq, r.event_id) for r in store.receipts] == [
        (seq, by_seq[seq].event_id) for seq in (6, 7, 9, 10)
    ]
    assert store.subscriptions[subscription.subscription_id].cursor_seq == 10
    # Replay after the cursor sends nothing again.
    await relay.run_once()
    assert len(hook.received) == 4


async def test_redelivery_after_failure_keeps_the_same_derived_event_id():
    store = InMemoryStore(events=[event(9, "workflow_run.terminalize")])
    clock = Clock()
    request = SubscriptionRequest.model_validate(
        {
            "target": "mission",
            "target_id": str(MISSION),
            "events": ["run.completed"],
            "channel": {"kind": "webhook", "url": "http://127.0.0.1:9/hook", "secret_ref": SECRET},
            "after_seq": 0,
        }
    )
    await SubscriptionService(store, clock=clock).subscribe(request, READER)
    hook = FakeWebhook(statuses=[500, 204])
    relay = SubscriptionRelay(
        store,
        transport=hook,
        secrets=StaticSecrets({SECRET: SECRET_VALUE}),
        owner="relay-test",
        clock=clock,
        jitter=lambda: 0.0,
    )
    await relay.run_once()
    clock.now += timedelta(hours=1)  # past the retry backoff
    await relay.run_once()
    ids = [item.headers["X-MC-Event-Id"] for item in hook.received]
    assert len(ids) == 2 and ids[0] == ids[1]
    assert [(r.attempt, r.status.value) for r in store.receipts] == [
        (1, "failed"),
        (2, "delivered"),
    ]


async def test_sse_stream_serves_the_public_vocabulary():
    store = InMemoryStore(events=journal())
    service = SubscriptionService(store, clock=Clock())
    frames = [
        frame
        async for frame in await service.stream(
            MISSION, READER, after_seq=0, event_types=("run.completed",), idle_polls=0
        )
    ]
    assert len(frames) == 1
    data = json.loads(frames[0].split("data: ", 1)[1])
    assert frames[0].startswith("event: mission_event\nid: 9\n")
    assert data["event_type"] == "run.completed"
