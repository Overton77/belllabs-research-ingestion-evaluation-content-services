"""MP-15 signed webhook callbacks on a disposable PostgreSQL 17 and a local receiver.

The receiver is a local asyncio HTTP endpoint on 127.0.0.1 (no external HTTP); the egress
policy allows loopback explicitly for it, and rejects a destination that resolves into a
private network before any byte is sent. Coordinator tables come from released migration 0033
(component 1.2.0) in the scratch database (see `test_mp15_coordinator_inbox_postgres`).

Proves:
- callback retries re-send the stored notification with one delivery id, verify by signature,
  timestamp and delivery id, and never rerun agent work (no run-control transition, request
  receipt, mailbox entry, effect or harness execution is written by delivery or retry);
- the dead-letter disposition lives in the subscription tables (state, receipts and the
  `subscription.dead_lettered` mission event), separate from task execution, while the poll
  fallback still serves the inbox;
- the FT-F5 relay signs with timestamp and delivery id and dead-letters an egress-rejected
  destination on the first attempt.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import pytest
import pytest_asyncio

from mission_control.adapters.operations.runtime_ports import EnvironmentSecretResolver
from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.subscriptions.coordinator_inbox import (
    PostgresCoordinatorInboxStore,
)
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
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
from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.application.subscriptions.webhook_signing import (
    DELIVERY_ATTEMPT_HEADER,
    DELIVERY_ID_HEADER,
    TIMESTAMP_HEADER,
    event_delivery_id,
    verify_delivery,
)
from mission_control.domain.subscriptions.contracts import (
    DEAD_LETTER_AFTER,
    SubscriptionRequest,
    SubscriptionState,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.integration.postgres.test_mp15_coordinator_inbox_postgres import (
    COORDINATOR,
    append_fixture_events,
    owner_fetch,
    started_run,
)
from tests.integration.postgres.test_subscriptions import Receiver
from tests.unit.human_tasks.fixtures import activation

pytestmark = pytest.mark.common_db

SECRET_ENV = "MCFT_MP15_CALLBACK_SECRET"
SECRET_VALUE = "disposable-local-mp15-key"
SECRET_REF = f"environment:{SECRET_ENV}"


@pytest_asyncio.fixture
async def inbox_db(common_db: CommonDatabase) -> AsyncIterator[CommonDatabase]:  # noqa: F811
    """The scratch database; the coordinator tables come from released migration 0033."""

    yield common_db


class Clock:
    def __init__(self) -> None:
        self.now = datetime.now(UTC)

    def __call__(self) -> datetime:
        return self.now


class ScriptedReceiver(Receiver):
    """The FT-F5 local receiver answering a status script, then `after` for every request."""

    def __init__(self, script: Sequence[int], after: int = 204) -> None:
        super().__init__(status=after)
        self.script = list(script)
        self.after = after

    async def _handle(self, reader: Any, writer: Any) -> None:
        self.status = self.script.pop(0) if self.script else self.after
        await super()._handle(reader, writer)


def sent_at(headers: dict[str, str]) -> datetime:
    return datetime.fromtimestamp(int(headers[TIMESTAMP_HEADER.lower()]), UTC)


WORK_TABLES = (
    "run_lifecycle_transition",
    "request_receipt",
    "command_mailbox",
    "effect_ledger_entry",
    "harness_execution",
    "delivery_report",
)


async def work_snapshot(db: CommonDatabase, run_key: str) -> dict[str, int]:
    """Everything that would change if delivery reran agent work (counts, run version)."""

    counts: dict[str, int] = {}
    for table in WORK_TABLES:
        rows = await owner_fetch(db, f"SELECT count(*) AS n FROM mission_control.{table}")
        counts[table] = rows[0]["n"]
    run = await owner_fetch(
        db, "SELECT version FROM mission_control.mission_run WHERE run_key = $1", run_key
    )
    counts["run_version"] = run[0]["version"]
    events = await owner_fetch(
        db,
        "SELECT count(*) AS n FROM mission_control.mission_event "
        "WHERE event_type <> 'subscription.dead_lettered'",
    )
    counts["mission_events"] = events[0]["n"]
    return counts


async def test_callback_retries_never_rerun_agent_work_and_dead_letter_separately(
    inbox_db: CommonDatabase,
) -> None:
    pool = await inbox_db.pool(max_size=6)
    receiver = ScriptedReceiver([503, 503])
    policy = EgressPolicy(allow_loopback=True)
    transport = EgressGuardedWebhookTransport(policy)
    try:
        scope = inbox_db.scope()
        run_key, _mission, run_uuid = await started_run(pool, inbox_db)
        url = await receiver.start()
        clock = Clock()
        inboxes = CoordinatorInboxService(
            PostgresSubscriptionStore(pool, scope),
            PostgresCoordinatorInboxStore(pool, scope),
            transport=transport,
            secrets=EnvironmentSecretResolver({SECRET_ENV: SECRET_VALUE}),
            destinations=policy,
            clock=clock,
            jitter=lambda: 0.5,
            owner="mp15-callbacks",
        )
        created = await inboxes.subscribe(
            CoordinatorSubscribeRequest.model_validate(
                {
                    "target": "run",
                    "target_id": str(run_uuid),
                    "profile": {"batch_window_seconds": 0},
                    "delivery": {"kind": "webhook", "url": url, "secret_ref": SECRET_REF},
                }
            ),
            COORDINATOR,
        )
        sid = created.subscription_id
        stored = await owner_fetch(
            inbox_db,
            "SELECT delivery::text AS delivery FROM mission_control.coordinator_inbox "
            "WHERE subscription_id = $1",
            sid,
        )
        assert SECRET_VALUE not in stored[0]["delivery"]
        assert json.loads(stored[0]["delivery"])["secret_ref"] == SECRET_REF
        await PostgresHumanTaskRepository(pool).open(
            activation(scope=scope, run_id=run_key), actor_ref="runtime"
        )
        before = await work_snapshot(inbox_db, run_key)
        for _ in range(3):
            report = await inboxes.deliver_callbacks()
            clock.now += timedelta(hours=1)
        assert report.delivered == [(str(sid), 1)]
        assert len(receiver.received) == 3
        bodies = [json.loads(body) for _headers, body in receiver.received]
        assert {body["notification_id"] for body in bodies} == {bodies[0]["notification_id"]}
        assert bodies[0]["kind"] == "review_required" and "payload" not in bodies[0]
        expected_id = str(delivery_id(sid, UUID(bodies[0]["notification_id"])))
        for attempt, (headers, body) in enumerate(receiver.received, start=1):
            assert headers[DELIVERY_ID_HEADER.lower()] == expected_id
            assert headers[DELIVERY_ATTEMPT_HEADER.lower()] == str(attempt)
            check = verify_delivery(SECRET_VALUE.encode(), body, headers, now=sent_at(headers))
            assert check.ok, check
            stale = verify_delivery(
                SECRET_VALUE.encode(), body, headers, now=sent_at(headers) + timedelta(hours=1)
            )
            assert stale.failure == "stale_timestamp"
        receipts = await owner_fetch(
            inbox_db,
            "SELECT status, attempt, response_code FROM mission_control.subscription_delivery "
            "WHERE subscription_id = $1 ORDER BY attempt",
            sid,
        )
        assert [(row["status"], row["attempt"], row["response_code"]) for row in receipts] == [
            ("failed", 1, 503),
            ("failed", 2, 503),
            ("delivered", 3, 204),
        ]
        assert (await inboxes.inbox(sid, COORDINATOR)).acked_inbox_seq == 1
        # Retries touched nothing but the subscription tables.
        assert await work_snapshot(inbox_db, run_key) == before

        # A destination that stays down: the next notification dead-letters after twelve.
        receiver.after = 503
        await append_fixture_events(
            pool, scope, run_key, [("attempt.completed", {"outcome": "failed"})]
        )
        before = await work_snapshot(inbox_db, run_key)
        for _ in range(DEAD_LETTER_AFTER):
            await inboxes.deliver_callbacks()
            clock.now += timedelta(hours=1)
        subscription = await PostgresSubscriptionStore(pool, scope).get(sid)
        assert subscription.state is SubscriptionState.DEAD_LETTERED
        assert subscription.failure_count == DEAD_LETTER_AFTER
        dead = await owner_fetch(
            inbox_db,
            "SELECT payload->'payload'->>'subscription_id' AS sid "
            "FROM mission_control.mission_event WHERE event_type = 'subscription.dead_lettered'",
        )
        assert [row["sid"] for row in dead] == [str(sid)]
        assert await work_snapshot(inbox_db, run_key) == before
        # No more pushes once dead-lettered; the coordinator still polls the durable inbox.
        sent = len(receiver.received)
        await inboxes.deliver_callbacks()
        assert len(receiver.received) == sent
        page = await inboxes.poll(sid, COORDINATOR)
        assert [item.kind.value for item in page.notifications] == ["failed", "blocked"]
        assert page.state is SubscriptionState.DEAD_LETTERED
        # Resuming the subscription restarts delivery from the acknowledged cursor.
        resumed = await SubscriptionService(PostgresSubscriptionStore(pool, scope)).resume(
            sid, COORDINATOR
        )
        assert resumed.state is SubscriptionState.ACTIVE
        receiver.after = 204
        clock.now += timedelta(hours=1)
        report = await inboxes.deliver_callbacks()
        assert report.delivered == [(str(sid), 2), (str(sid), 3)]
        assert (await inboxes.inbox(sid, COORDINATOR)).acked_inbox_seq == 3
    finally:
        await transport.aclose()
        await receiver.stop()
        await pool.close()


async def test_relay_signatures_and_egress_rejection_on_postgres(inbox_db: CommonDatabase) -> None:
    pool = await inbox_db.pool(max_size=4)
    receiver = Receiver()

    async def resolve(host: str, port: int) -> Sequence[str]:
        return {"127.0.0.1": ["127.0.0.1"], "hooks.internal.test": ["10.20.30.40"]}[host]

    policy = EgressPolicy(allow_loopback=True, resolver=resolve)
    transport = EgressGuardedWebhookTransport(policy)
    try:
        scope = inbox_db.scope()
        _run_key, mission_id, run_uuid = await started_run(pool, inbox_db)
        url = await receiver.start()
        store = PostgresSubscriptionStore(pool, scope)
        clock = Clock()
        service = SubscriptionService(store, clock=clock)

        def webhook(target_url: str) -> SubscriptionRequest:
            return SubscriptionRequest.model_validate(
                {
                    "target": "mission",
                    "target_id": str(mission_id),
                    "events": ["*"],
                    "channel": {"kind": "webhook", "url": target_url, "secret_ref": SECRET_REF},
                    "after_seq": 0,
                }
            )

        good = await service.subscribe(webhook(url), COORDINATOR)
        internal = await service.subscribe(webhook("https://hooks.internal.test/hook"), COORDINATOR)
        relay = SubscriptionRelay(
            store,
            transport=transport,
            secrets=EnvironmentSecretResolver({SECRET_ENV: SECRET_VALUE}),
            owner="mp15-relay",
            clock=clock,
            jitter=lambda: 0.5,
        )
        report = await relay.run_once()
        assert report.dead_lettered == [str(internal.subscription_id)]
        events = await owner_fetch(
            inbox_db,
            "SELECT event_id, seq FROM mission_control.mission_event WHERE mission_id = $1 "
            "AND event_type <> 'subscription.dead_lettered' ORDER BY seq",
            mission_id,
        )
        assert [json.loads(body)["seq"] for _h, body in receiver.received] == [
            row["seq"] for row in events
        ]
        for (headers, body), row in zip(receiver.received, events, strict=True):
            assert headers[DELIVERY_ID_HEADER.lower()] == str(
                event_delivery_id(good.subscription_id, row["event_id"])
            )
            assert verify_delivery(SECRET_VALUE.encode(), body, headers, now=sent_at(headers)).ok
            tampered = verify_delivery(
                SECRET_VALUE.encode(),
                body.replace(b'"seq"', b'"Seq"'),
                headers,
                now=sent_at(headers),
            )
            assert tampered.failure == "bad_signature"
        # The internal destination never received a byte and dead-lettered on attempt one.
        rejected = await owner_fetch(
            inbox_db,
            "SELECT status, attempt, error_class FROM mission_control.subscription_delivery "
            "WHERE subscription_id = $1",
            internal.subscription_id,
        )
        assert [(row["status"], row["attempt"], row["error_class"]) for row in rejected] == [
            ("dead_lettered", 1, "egress_rejected")
        ]
        assert (await store.get(internal.subscription_id)).state is SubscriptionState.DEAD_LETTERED
        # The coordinator inbox refuses such a destination at subscribe time (nothing written).
        inboxes = CoordinatorInboxService(
            store, PostgresCoordinatorInboxStore(pool, scope), destinations=policy
        )
        with pytest.raises(CoordinatorRejected) as refused:
            await inboxes.subscribe(
                CoordinatorSubscribeRequest.model_validate(
                    {
                        "target": "run",
                        "target_id": str(run_uuid),
                        "delivery": {
                            "kind": "webhook",
                            "url": "https://hooks.internal.test/cb",
                            "secret_ref": SECRET_REF,
                        },
                    }
                ),
                COORDINATOR,
            )
        assert refused.value.code == "egress_rejected"
        inbox_rows = await owner_fetch(
            inbox_db, "SELECT count(*) AS n FROM mission_control.coordinator_inbox"
        )
        assert inbox_rows[0]["n"] == 0
    finally:
        await transport.aclose()
        await receiver.stop()
        await pool.close()
