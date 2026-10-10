"""MP-08: JSON-RPC correlation over an in-process channel (FIXTURE peer, not Codex).

Responses correlate by id whatever their order; notifications and server requests are one
ordered event stream apart from responses; a lost response is a typed `ResponseLost` that
re-sends nothing; a disconnect fails every pending request and ends the event stream after
it drains; a cursor behind the bounded buffer is `CursorExpired`; the stdio framing parses
newline-delimited JSON and refuses invalid lines.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from mission_control.adapters.codex.transport import (
    AppServerConnection,
    AppServerError,
    CursorExpired,
    InboundEvent,
    JsonLinesChannel,
    ProtocolViolation,
    ResponseLost,
    TransportClosed,
)
from tests.unit.codex.fixture_app_server import MemoryChannel, channel_pair


async def _peer_echo(channel: MemoryChannel, *, reverse: bool = True) -> None:
    """FIXTURE peer: answers a batch of requests in reverse order, interleaving a
    notification before and a server request after."""

    batch: list[dict[str, Any]] = []
    while len(batch) < 3:
        message = await channel.receive()
        assert message is not None
        batch.append(message)
    await channel.send({"jsonrpc": "2.0", "method": "turn/started", "params": {"threadId": "t"}})
    order = reversed(batch) if reverse else batch
    for message in order:
        if message["method"] == "fail/me":
            await channel.send(
                {
                    "jsonrpc": "2.0",
                    "id": message["id"],
                    "error": {
                        "code": -32000,
                        "message": "nope",
                        "data": {"codexErrorInfo": "other"},
                    },
                }
            )
        else:
            await channel.send(
                {"jsonrpc": "2.0", "id": message["id"], "result": {"echo": message["method"]}}
            )
    await channel.send(
        {
            "jsonrpc": "2.0",
            "id": "srv-1",
            "method": "item/commandExecution/requestApproval",
            "params": {"threadId": "t", "turnId": "u", "itemId": "i"},
        }
    )
    answer = await channel.receive()
    assert (
        answer is not None
        and answer["id"] == "srv-1"
        and answer["result"] == {"decision": "decline"}
    )
    await channel.send({"jsonrpc": "2.0", "id": 999, "result": {"unmatched": True}})
    await channel.send({"jsonrpc": "2.0", "method": "turn/completed", "params": {"threadId": "t"}})


async def test_responses_correlate_out_of_order_and_streams_stay_separate() -> None:
    client, remote = channel_pair()
    peer = asyncio.create_task(_peer_echo(remote))
    connection = AppServerConnection(client, request_timeout_s=5)
    await connection.open()
    first, second, third = await asyncio.gather(
        connection.request("thread/read", {"threadId": "t"}),
        connection.request("turn/start", {"threadId": "t"}),
        connection.request("fail/me", None),
        return_exceptions=True,
    )
    assert first == {"echo": "thread/read"} and second == {"echo": "turn/start"}
    assert isinstance(third, AppServerError) and third.code == -32000
    assert third.data == {"codexErrorInfo": "other"}
    assert not connection.pending_methods()

    events: list[InboundEvent] = []
    async for event in connection.events(0):
        events.append(event)
        if event.kind == "server_request":
            assert event.request_id == "srv-1"
            await connection.respond(event.request_id, {"decision": "decline"})
        if event.method == "turn/completed":
            break
    await peer
    assert [(e.seq, e.kind, e.method) for e in events] == [
        (1, "notification", "turn/started"),
        (2, "server_request", "item/commandExecution/requestApproval"),
        (3, "notification", "turn/completed"),
    ]
    # Replay from a cursor is the same stream; responses never appear in it.
    replayed = [event.seq for event in connection.buffered(1)]
    assert replayed == [2, 3]
    assert connection.stats.unmatched_responses == 1, "a stray response is counted, not fatal"
    assert connection.stats.responses_matched == 3 and connection.stats.responses_sent == 1
    await connection.close()


async def test_a_lost_response_is_typed_and_nothing_is_resent() -> None:
    client, remote = channel_pair()
    seen: list[dict[str, Any]] = []

    async def silent_peer() -> None:
        while True:
            message = await remote.receive()
            if message is None:
                return
            seen.append(message)
            if message["method"] == "turn/start":
                continue  # the provider accepted it; the response is lost
            await remote.send({"jsonrpc": "2.0", "id": message["id"], "result": {}})

    peer = asyncio.create_task(silent_peer())
    connection = AppServerConnection(client, request_timeout_s=0.2)
    await connection.open()
    with pytest.raises(ResponseLost) as lost:
        await connection.request("turn/start", {"threadId": "t"})
    assert lost.value.method == "turn/start"
    assert not connection.pending_methods(), "the lost request is dropped from pending"
    assert await connection.request("thread/read", {"threadId": "t"}) == {}
    assert [m["method"] for m in seen] == ["turn/start", "thread/read"], "sent once each"
    assert connection.stats.lost_responses == 1
    await connection.close()
    await peer


async def test_a_disconnect_fails_pending_requests_and_ends_the_event_stream() -> None:
    client, remote = channel_pair()
    connection = AppServerConnection(client, request_timeout_s=5)
    await connection.open()
    pending = asyncio.create_task(connection.request("turn/start", {"threadId": "t"}))
    await remote.receive()
    await remote.send({"jsonrpc": "2.0", "method": "item/started", "params": {"threadId": "t"}})
    await remote.close()  # the process died
    with pytest.raises(TransportClosed):
        await pending
    assert connection.closed and "end of stream" in connection.close_reason
    drained = []
    with pytest.raises(TransportClosed):
        async for event in connection.events(0):
            drained.append(event.method)
    assert drained == ["item/started"], "buffered events are still delivered before the close"
    with pytest.raises(TransportClosed):
        await connection.request("thread/read", {"threadId": "t"})


async def test_a_cursor_behind_the_bounded_buffer_is_expired() -> None:
    client, remote = channel_pair()
    connection = AppServerConnection(client, buffer_limit=2)
    await connection.open()
    for index in range(4):
        await remote.send({"jsonrpc": "2.0", "method": f"n/{index}", "params": {}})
    await asyncio.sleep(0.05)
    assert [event.seq for event in connection.buffered(0)] == [3, 4]
    with pytest.raises(CursorExpired):
        async for _event in connection.events(0):
            pass
    tail = []
    async for event in connection.events(2):
        tail.append(event.seq)
        if event.seq == 4:
            break
    assert tail == [3, 4] and connection.stats.dropped_events == 2
    await connection.close()


class _Writer:
    """A minimal StreamWriter stand-in capturing bytes."""

    def __init__(self) -> None:
        self.buffer = bytearray()
        self.closed = False

    def write(self, data: bytes) -> None:
        self.buffer.extend(data)

    async def drain(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


async def test_json_lines_framing_parses_and_refuses_invalid_lines() -> None:
    reader = asyncio.StreamReader()
    writer = _Writer()
    channel = JsonLinesChannel(reader, writer)  # type: ignore[arg-type]
    await channel.send({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"a": "é"}})
    line = bytes(writer.buffer)
    assert line.endswith(b"\n") and line.count(b"\n") == 1
    assert json.loads(line) == {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {"a": "é"},
    }
    reader.feed_data(b'\n{"jsonrpc":"2.0","method":"turn/started","params":{}}\n')
    assert await channel.receive() == {"jsonrpc": "2.0", "method": "turn/started", "params": {}}
    reader.feed_data(b"not json\n")
    with pytest.raises(ProtocolViolation):
        await channel.receive()
    reader.feed_data(b'{"jsonrpc":"2.0","id":2,"result":{}}')
    reader.feed_eof()
    with pytest.raises(ProtocolViolation):
        await channel.receive()  # a line without its newline at EOF is a truncated message
    await channel.close()
    assert writer.closed
