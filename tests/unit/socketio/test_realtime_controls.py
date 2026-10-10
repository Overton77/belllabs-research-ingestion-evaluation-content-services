"""Reauthorization of idle connections, the `lineage` event and durable receipt following.

Fixture resolver and in-memory stream source as in `test_mission_socket.py`; the receipt
ledger is the real in-memory run-control repository (`tests/unit/run_control`). The
PostgreSQL and real-verifier proofs are in
`tests/integration/postgres/test_realtime_acceptance_postgres.py`.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from uuid import uuid4

import pytest

from mission_control.contracts.contracts import MissionCommandReceipt
from mission_control.contracts.realtime import (
    CLIENT_EVENTS,
    SERVER_EVENTS,
    CommandReceiptProgress,
    LineageNotice,
)
from mission_control.domain.policies.contracts import CommandStatus, ReceiptState
from mission_control.interfaces.socketio import commands as socket_commands
from mission_control.interfaces.socketio.commands import (
    SocketCommand,
    follow_command_receipts,
    receipt_stage,
)
from mission_control.interfaces.socketio.server import MissionNamespace
from tests.fixtures.provider_frames import FIXTURE_HARNESS
from tests.unit.frames.test_lineage_stream import claude_frames
from tests.unit.run_control.test_boundary_commands import (
    TARGET,
    delivered,
    pause,
    started,
)
from tests.unit.run_control.test_run_control import command, service
from tests.unit.socketio.harness import SERVER_EVENTS as HARNESS_EVENTS
from tests.unit.socketio.harness import MissionClient, serve
from tests.unit.socketio.test_mission_socket import (
    FixtureResolver,
    Lifecycle,
    auth,
    build,
)
from tests.unit.streams.fakes import FakeStreamSource
from tests.unit.streams.test_stream_lineage import rekeyed


class LineageClient(MissionClient):
    def __init__(self) -> None:
        super().__init__()
        self.sio.on("lineage", self._recorder("lineage"), namespace="/missions")


def test_the_versioned_event_vocabulary_covers_every_namespace_handler() -> None:
    handlers = {name.removeprefix("on_") for name in dir(MissionNamespace)}
    assert set(CLIENT_EVENTS) <= handlers
    assert set(HARNESS_EVENTS) <= set(SERVER_EVENTS)
    assert {"lineage", "human_task_receipt"} <= set(SERVER_EVENTS)


async def test_an_idle_connection_is_disconnected_when_its_grant_is_revoked() -> None:
    lifecycle, resolver, source = Lifecycle(), FixtureResolver(), FakeStreamSource()
    _app, asgi = build(lifecycle, resolver, source, reauthorize_seconds=0.2)
    async with serve(asgi) as (url, _server):
        idle, kept = MissionClient(), MissionClient()
        revoked_token = resolver.grant("viewer")
        await idle.connect(url, auth(revoked_token))
        await kept.connect(url, auth(resolver.grant("operator")))
        del resolver.tokens[revoked_token]  # the grant or binding is withdrawn
        # No client event is ever sent: the watchdog alone notices.
        await idle.wait_for(lambda: not idle.sio.connected, within=5)
        assert [e["code"] for e in idle.events["stream_error"]] == ["UNAUTHORIZED"]
        await asyncio.sleep(0.5)
        assert kept.sio.connected and kept.events["stream_error"] == []
        await kept.close()


async def test_an_idle_connection_without_subscriptions_ends_at_its_expiry() -> None:
    lifecycle, resolver, source = Lifecycle(), FixtureResolver(), FakeStreamSource()
    _app, asgi = build(lifecycle, resolver, source, reauthorize_seconds=30.0)
    async with serve(asgi) as (url, _server):
        client = MissionClient()
        await client.connect(url, auth(resolver.grant("viewer", exp=time.time() + 1.5)))
        began = time.monotonic()
        await client.wait_for(lambda: not client.sio.connected, within=6)
        assert time.monotonic() - began < 5  # at `exp`, not at the 30 s reverification
        assert [e["code"] for e in client.events["stream_error"]] == ["UNAUTHORIZED"]


async def test_the_lineage_event_lists_then_updates_subordinates_by_reference() -> None:
    frames = rekeyed(await claude_frames())
    first_child = next(f.arrival_ordinal for f in frames if f.subordinate_ref)
    source = FakeStreamSource()
    # Committed so far: the parent's start and the child's task_started lifecycle frame.
    source.stored_frames[1] = frames[:first_child]
    lifecycle, resolver = Lifecycle(), FixtureResolver()
    _app, asgi = build(lifecycle, resolver, source)
    async with serve(asgi) as (url, _server):
        client = LineageClient()
        await client.connect(url, auth(resolver.grant("viewer")))
        reply = await client.call(
            "subscribe",
            {
                "request_id": "req-lineage",
                "application_id": "biotech",
                "target": {"kind": "execution", "id": str(FIXTURE_HARNESS)},
                "streams": ["provider_frames"],
                "cursors": [
                    {
                        "stream": "provider_frames",
                        "position": f"{FIXTURE_HARNESS}:{first_child}",
                        "generation": 1,
                    }
                ],
                "include_descendants": True,
            },
        )
        assert reply["ok"] is True, reply
        assert reply["snapshot_versions"]["provider_frames.lineage"] == "complete"
        await client.wait_for(lambda: bool(client.events["lineage"]))
        listing = LineageNotice.model_validate(client.events["lineage"][0])
        assert listing.full is True and len(listing.subordinates) == 1
        (child,) = listing.subordinates
        assert (child.ref.visibility, child.lifecycle) == ("lifecycle_only", "started")
        source.stored_frames[1] = frames  # the rest of the session commits
        await client.wait_for(
            lambda: any(
                node["lifecycle"] == "ended"
                for notice in client.events["lineage"]
                for node in notice["subordinates"]
            )
        )
        updates = [LineageNotice.model_validate(n) for n in client.events["lineage"][1:]]
        assert all(not update.full for update in updates)
        final = updates[-1].subordinates[-1]
        assert (final.ref.visibility, final.lifecycle) == ("full", "ended")
        # References only: no child message or tool content in the lineage event.
        assert "body_excerpt" not in str(client.events["lineage"])
        await client.close()


class LedgerService:
    """The application read handler `MissionControlService.commands` over the in-memory
    run-control ledger (the same ledger `GET /runs/{run_id}/commands` reads)."""

    def __init__(self, run_service: Any) -> None:
        self.run_service = run_service
        self.reads = 0

    async def commands(self, run_id: str, actor: Any) -> Any:
        self.reads += 1
        return await self.run_service.list_boundary_commands("tenant-1", run_id)


async def accepted_pause() -> tuple[Any, str, MissionCommandReceipt]:
    run_service, _repository = service()
    run_id = await started(run_service, f"req-{uuid4()}", TARGET)
    result = await run_service.execute(command(run_id, 2, "pause", pause()))
    assert result.status == CommandStatus.ACCEPTED
    status = await run_service.get_boundary_command("tenant-1", run_id, "operator", "pause")
    receipt = MissionCommandReceipt(request_id=uuid4(), admission=result, delivery=status)
    return run_service, run_id, receipt


def socket_command(run_id: str) -> SocketCommand:
    return SocketCommand.model_validate(
        {
            "request_id": "req-socket",
            "application_id": "biotech",
            "run_id": run_id,
            "command": {
                "request_id": str(uuid4()),
                "expected_version": 2,
                "expected_generation": 1,
                "target": {"id": run_id},
                "kind": "cancel",
                "payload": {},
                "reason": "receipt following",
            },
        }
    )


async def test_later_receipt_states_are_followed_once_in_ledger_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_service, run_id, receipt = await accepted_pause()
    assert receipt_stage(receipt) == ("accepted", False)
    ledger = LedgerService(run_service)
    monkeypatch.setattr(socket_commands, "mission_service", lambda _app, _principal: ledger)
    emitted: list[dict[str, Any]] = []

    async def emit(body: dict[str, Any]) -> None:
        emitted.append(body)

    principal: Any = type("P", (), {"application_id": "biotech", "actor": None})()
    follower = asyncio.create_task(
        follow_command_receipts(
            None,  # type: ignore[arg-type]
            principal,
            socket_command(run_id),
            receipt,
            emit,
            poll_interval=0.02,
            window=5.0,
        )
    )
    await asyncio.sleep(0.1)
    assert receipt.delivery is not None
    status = receipt.delivery
    await run_service.record_boundary_receipt("tenant-1", delivered(status))
    await asyncio.sleep(0.1)
    await run_service.record_boundary_receipt(
        "tenant-1",
        delivered(status).model_copy(
            update={
                "ordinal": 2,
                "state": ReceiptState.REJECTED,
                "rejection_reason": "superseded",
                "recorded_by": "family-boundary",
            }
        ),
    )
    assert await asyncio.wait_for(follower, timeout=5) == "rejected"
    progress = [CommandReceiptProgress.model_validate(body) for body in emitted]
    assert [(item.stage, item.final) for item in progress] == [
        ("delivered", False),
        ("rejected", True),
    ]
    assert all(item.command_id == "pause" for item in progress)
    assert progress[-1].receipt is not None
    assert progress[-1].receipt["rejection_reason"] == "superseded"


async def test_receipt_following_is_bounded_and_ends_as_unfollowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_service, run_id, receipt = await accepted_pause()
    monkeypatch.setattr(
        socket_commands, "mission_service", lambda _app, _principal: LedgerService(run_service)
    )
    emitted: list[dict[str, Any]] = []

    async def emit(body: dict[str, Any]) -> None:
        emitted.append(body)

    principal: Any = type("P", (), {"application_id": "biotech", "actor": None})()
    stage = await follow_command_receipts(
        None,  # type: ignore[arg-type]
        principal,
        socket_command(run_id),
        receipt,
        emit,
        poll_interval=0.02,
        window=0.2,
    )
    assert stage == "unfollowed"
    (last,) = emitted
    assert last["stage"] == "unfollowed" and last["final"] is True
