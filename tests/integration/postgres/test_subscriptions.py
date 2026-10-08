"""FT-F5 on the real local stack: PostgreSQL 17 subscription store, committed mission events
from a real run admission, and a local fake webhook receiver on 127.0.0.1 (no external HTTP).
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from mission_control.adapters.operations.runtime_ports import EnvironmentSecretResolver
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.subscriptions.webhook import HttpxWebhookTransport
from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.domain.policies.contracts import ActorContext, StartAction
from mission_control.domain.subscriptions.contracts import (
    DEAD_LETTER_AFTER,
    SubscriptionRequest,
    SubscriptionState,
    verify_signature,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.unit.run_control.test_boundary_commands import TARGET
from tests.unit.run_control.test_run_control import command, request, service

pytestmark = pytest.mark.common_db

SECRET_ENV = "MCFT_SUBSCRIPTION_TEST_SECRET"
SECRET_VALUE = "disposable-local-signing-key"
READER = ActorContext(actor_id="actor:subscriber", permissions=frozenset({"workflow_run.read"}))


@dataclass
class Receiver:
    """A local HTTP endpoint that records raw bodies and answers with scripted statuses."""

    status: int = 204
    received: list[tuple[dict[str, str], bytes]] = field(default_factory=list)
    server: asyncio.AbstractServer | None = None

    async def start(self) -> str:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{port}/hook"

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            await reader.readline()
            headers: dict[str, str] = {}
            while (line := await reader.readline()) not in (b"\r\n", b""):
                name, _, value = line.decode("latin-1").partition(":")
                headers[name.strip().lower()] = value.strip()
            body = await reader.readexactly(int(headers.get("content-length", "0")))
            self.received.append((headers, body))
            status_line = f"HTTP/1.1 {self.status} X\r\n"
            writer.write((status_line + "content-length: 0\r\nconnection: close\r\n\r\n").encode())
            await writer.drain()
        finally:
            writer.close()


class Clock:
    def __init__(self) -> None:
        self.now = datetime.now(UTC)

    def __call__(self) -> datetime:
        return self.now


async def admitted_mission(pool: asyncpg.Pool, db: CommonDatabase) -> tuple[UUID, UUID]:
    """A real admitted and started run; its mission stream holds committed events."""

    scope = db.scope()
    authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
    assert admission.run_id is not None
    await authority.execute(
        command(admission.run_id, 1, "start", StartAction(execution_target=TARGET)).model_copy(
            update={"request_scope": scope}
        )
    )
    owner = await asyncpg.connect(db.owner_dsn)
    try:
        row = await owner.fetchrow(
            "SELECT run_id, mission_id FROM mission_control.mission_run WHERE run_key = $1",
            admission.run_id,
        )
    finally:
        await owner.close()
    assert row is not None
    return row["mission_id"], row["run_id"]


async def owner_fetch(db: CommonDatabase, query: str, *args: Any) -> list[asyncpg.Record]:
    owner = await asyncpg.connect(db.owner_dsn)
    try:
        return list(await owner.fetch(query, *args))
    finally:
        await owner.close()


def subscription_request(mission_id: UUID, url: str, **overrides: Any) -> SubscriptionRequest:
    return SubscriptionRequest.model_validate(
        {
            "target": "mission",
            "target_id": str(mission_id),
            "events": ["*"],
            "channel": {"kind": "webhook", "url": url, "secret_ref": f"environment:{SECRET_ENV}"},
            "after_seq": 0,
            **overrides,
        }
    )


class CrashingStore(PostgresSubscriptionStore):
    """Kills the relay after the second webhook send and before its receipt is written."""

    def __init__(self, *args: Any, crash_at: int) -> None:
        super().__init__(*args)
        self.crash_at = crash_at
        self.calls = 0

    async def record_success(self, *args: Any, **kwargs: Any) -> Any:
        self.calls += 1
        if self.calls == self.crash_at:
            raise SystemExit("relay killed mid-batch")
        return await super().record_success(*args, **kwargs)


def relay(
    store: PostgresSubscriptionStore, clock: Clock, transport: HttpxWebhookTransport
) -> SubscriptionRelay:
    return SubscriptionRelay(
        store,
        transport=transport,
        secrets=EnvironmentSecretResolver({SECRET_ENV: SECRET_VALUE}),
        owner="relay-it",
        clock=clock,
        jitter=lambda: 0.5,
    )


async def test_webhook_delivery_is_signed_at_least_once_and_gap_free(common_db: CommonDatabase):
    pool = await common_db.pool()
    receiver = Receiver()
    transport = HttpxWebhookTransport()
    try:
        mission_id, _run_id = await admitted_mission(pool, common_db)
        url = await receiver.start()
        clock = Clock()
        store = CrashingStore(pool, common_db.scope(), crash_at=2)
        subscription = await SubscriptionService(store, clock=clock).subscribe(
            subscription_request(mission_id, url), READER
        )
        events = await owner_fetch(
            common_db,
            "SELECT event_id, seq FROM mission_control.mission_event "
            "WHERE mission_id = $1 ORDER BY seq",
            mission_id,
        )
        assert len(events) >= 3
        # Row carries references only.
        rows = await owner_fetch(
            common_db,
            "SELECT channel::text AS channel FROM mission_control.mission_subscription "
            "WHERE subscription_id = $1",
            subscription.subscription_id,
        )
        channel = json.loads(rows[0]["channel"])
        assert channel["secret_ref"] == f"environment:{SECRET_ENV}"
        assert SECRET_VALUE not in rows[0]["channel"]

        with pytest.raises(SystemExit):
            await relay(store, clock, transport).run_once()
        clock.now += timedelta(minutes=5)  # the dead relay's lease expires
        report = await relay(store, clock, transport).run_once()
        assert report.failed == [] and report.dead_lettered == []

        seqs = [json.loads(body)["seq"] for _, body in receiver.received]
        ids = [json.loads(body)["event_id"] for _, body in receiver.received]
        assert seqs[:3] == [1, 2, 2]  # the second event was redelivered after the crash
        assert ids[1] == ids[2]
        assert sorted(set(seqs)) == [row["seq"] for row in events]  # nothing skipped
        for headers, body in receiver.received:
            assert verify_signature(SECRET_VALUE.encode(), body, headers["x-mc-signature"])
            assert "payload" not in json.loads(body)
        receipts = await owner_fetch(
            common_db,
            "SELECT seq, status FROM mission_control.subscription_delivery "
            "WHERE subscription_id = $1 ORDER BY seq",
            subscription.subscription_id,
        )
        assert [row["seq"] for row in receipts] == [row["seq"] for row in events]
        current = await store.get(subscription.subscription_id)
        assert current.cursor_seq == events[-1]["seq"]
    finally:
        await transport.aclose()
        await receiver.stop()
        await pool.close()


async def test_dead_letter_after_twelve_failures_emits_a_mission_event(common_db: CommonDatabase):
    pool = await common_db.pool()
    receiver = Receiver(status=503)
    transport = HttpxWebhookTransport()
    try:
        mission_id, _run_id = await admitted_mission(pool, common_db)
        url = await receiver.start()
        clock = Clock()
        store = PostgresSubscriptionStore(pool, common_db.scope())
        service = SubscriptionService(store, clock=clock)
        subscription = await service.subscribe(subscription_request(mission_id, url), READER)
        for _ in range(DEAD_LETTER_AFTER):
            await relay(store, clock, transport).run_once()
            clock.now += timedelta(hours=1)
        current = await store.get(subscription.subscription_id)
        assert current.state is SubscriptionState.DEAD_LETTERED
        assert current.failure_count == DEAD_LETTER_AFTER
        assert current.cursor_seq == 0
        assert len(receiver.received) == DEAD_LETTER_AFTER
        events = await owner_fetch(
            common_db,
            "SELECT event_type, payload->'payload'->>'subscription_id' AS subscription_id "
            "FROM mission_control.mission_event WHERE mission_id = $1 ORDER BY seq DESC LIMIT 1",
            mission_id,
        )
        assert events[0]["event_type"] == "subscription.dead_lettered"
        assert events[0]["subscription_id"] == str(subscription.subscription_id)
        listed = await service.list(READER, mission_id=mission_id)
        assert [item.state for item in listed] == [SubscriptionState.DEAD_LETTERED]
        statuses = await owner_fetch(
            common_db,
            "SELECT status, response_code FROM mission_control.subscription_delivery "
            "WHERE subscription_id = $1 ORDER BY attempt",
            subscription.subscription_id,
        )
        assert [row["status"] for row in statuses] == ["failed"] * 11 + ["dead_lettered"]
        assert {row["response_code"] for row in statuses} == {503}
    finally:
        await transport.aclose()
        await receiver.stop()
        await pool.close()


async def test_sse_replay_and_scope_isolation(common_db: CommonDatabase):
    pool = await common_db.pool()
    try:
        mission_id, run_id = await admitted_mission(pool, common_db)
        service = SubscriptionService(PostgresSubscriptionStore(pool, common_db.scope()))
        frames = [
            frame
            async for frame in await service.stream(mission_id, READER, after_seq=1, idle_polls=0)
        ]
        seqs = [
            int(line.removeprefix("id: "))
            for frame in frames
            for line in frame.splitlines()
            if line.startswith("id: ")
        ]
        assert seqs and seqs[0] == 2 and seqs == list(range(2, 2 + len(seqs)))
        run_subscription = await service.subscribe(
            SubscriptionRequest.model_validate(
                {
                    "target": "run",
                    "target_id": str(run_id),
                    "events": ["run.*"],
                    "channel": {"kind": "stream_ticket", "ticket_id": "ticket-1"},
                }
            ),
            READER,
        )
        assert run_subscription.target.mission_id == mission_id
        other_tenant = SubscriptionService(
            PostgresSubscriptionStore(pool, common_db.scope("tenant-2"))
        )
        assert await other_tenant.list(READER) == ()
        closed = await service.close(run_subscription.subscription_id, READER)
        assert closed.state is SubscriptionState.CLOSED and closed.closed_at is not None
    finally:
        await pool.close()
