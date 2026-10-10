"""JSON-RPC connection to a Codex app-server with separated correlation (MP-08, SPEC-01).

Three inbound streams, three different owners:

- **responses** to our requests are correlated by request id to the awaiting caller; a
  response nobody waits for is counted (`unmatched_responses`), never crashes the reader;
- **notifications** (`method` without `id`) and **server-originated requests** (`method` with
  `id`: approvals, user input, elicitations) are appended in arrival order to one bounded
  event buffer with a monotonic `seq`, so an observer resumes from a cursor and the
  approval port answers requests whether or not anyone observes;
- a request whose response never arrives within its timeout is `ResponseLost`: the pending
  entry is dropped and nothing is re-sent (the caller's dispatch journal decides);
- the channel ending (EOF, broken pipe, invalid framing) is `TransportClosed`: every pending
  request fails with it and event consumers see it once they drain the buffer.

The connection is framing-agnostic: `JsonLinesChannel` carries newline-delimited JSON over
the CLI's stdio (the app-server's `stdio://` transport); tests supply an in-process channel.
Correlation is JSON-RPC 2.0 as the pinned `JSONRPCMessage.json` declares it: a request has
`id` and `method`, a response has `id` and `result` or `error`, a notification only `method`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
from collections.abc import AsyncIterator, Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

_LOGGER = logging.getLogger(__name__)
DEFAULT_REQUEST_TIMEOUT_S = 60.0
DEFAULT_BUFFER_LIMIT = 100_000
MAX_LINE_BYTES = 64 * 1024 * 1024

RequestId = int | str
EventKind = Literal["notification", "server_request"]


class TransportClosed(ConnectionError):
    """The channel ended (process exit, broken pipe, framing violation)."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class ResponseLost(TimeoutError):
    """No response arrived for a request within its timeout; it is not re-sent here."""

    def __init__(self, method: str, request_id: RequestId, timeout_s: float) -> None:
        super().__init__(f"no response to {method} (id {request_id}) within {timeout_s}s")
        self.method = method
        self.request_id = request_id


class ProtocolViolation(ValueError):
    """The peer sent something that is not JSON-RPC 2.0 as pinned."""


class CursorExpired(LookupError):
    """The requested event position left the bounded buffer; resync from provider history."""


class AppServerError(RuntimeError):
    """A JSON-RPC error response (`JSONRPCErrorError{code, message, data}`)."""

    def __init__(self, method: str, code: int, message: str, data: Any = None) -> None:
        super().__init__(f"{method} failed ({code}): {message}")
        self.method = method
        self.code = code
        self.message = message
        self.data = data


class MessageChannel(Protocol):
    async def send(self, message: Mapping[str, Any]) -> None: ...

    async def receive(self) -> dict[str, Any] | None:
        """The next message, or None at end of stream."""
        ...

    async def close(self) -> None: ...


class JsonLinesChannel:
    """Newline-delimited JSON over asyncio streams (the app-server's stdio transport)."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        max_line_bytes: int = MAX_LINE_BYTES,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._max = max_line_bytes

    async def send(self, message: Mapping[str, Any]) -> None:
        line = json.dumps(dict(message), separators=(",", ":"), ensure_ascii=False) + "\n"
        self._writer.write(line.encode("utf-8"))
        await self._writer.drain()

    async def receive(self) -> dict[str, Any] | None:
        while True:
            try:
                line = await self._reader.readuntil(b"\n")
            except asyncio.IncompleteReadError as error:
                if not error.partial.strip():
                    return None
                raise ProtocolViolation("stream ended inside a JSON-RPC line") from error
            except asyncio.LimitOverrunError as error:
                raise ProtocolViolation("JSON-RPC line exceeds the reader limit") from error
            if len(line) > self._max:
                raise ProtocolViolation(f"JSON-RPC line exceeds {self._max} bytes")
            text = line.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            try:
                message = json.loads(text)
            except ValueError as error:
                raise ProtocolViolation(f"invalid JSON-RPC line: {text[:80]!r}") from error
            if not isinstance(message, dict):
                raise ProtocolViolation("a JSON-RPC message is an object")
            return message

    async def close(self) -> None:
        with suppress(Exception):
            self._writer.close()
            await self._writer.wait_closed()


@dataclass(frozen=True)
class InboundEvent:
    """One notification or server request in arrival order (`seq` starts at 1)."""

    seq: int
    kind: EventKind
    method: str
    params: dict[str, Any]
    request_id: RequestId | None = None


@dataclass
class _Pending:
    method: str
    future: asyncio.Future[Any]


@dataclass
class ConnectionStats:
    requests_sent: int = 0
    responses_matched: int = 0
    unmatched_responses: int = 0
    notifications: int = 0
    server_requests: int = 0
    responses_sent: int = 0
    dropped_events: int = 0
    lost_responses: int = 0
    events: list[InboundEvent] = field(default_factory=list)


class AppServerConnection:
    """One JSON-RPC connection: requests out, responses/notifications/server requests in."""

    def __init__(
        self,
        channel: MessageChannel,
        *,
        request_timeout_s: float = DEFAULT_REQUEST_TIMEOUT_S,
        buffer_limit: int = DEFAULT_BUFFER_LIMIT,
        epoch: str | None = None,
    ) -> None:
        self._channel = channel
        self._timeout = request_timeout_s
        self._limit = max(1, buffer_limit)
        # Names this process's view of the event sequence: a cursor from another epoch
        # (another app-server launch) cannot index this buffer and triggers a history resync.
        self.epoch = epoch or secrets.token_hex(4)
        self._next_id = 1
        self._pending: dict[RequestId, _Pending] = {}
        self._events: list[InboundEvent] = []
        self._first_seq = 1
        self._seq = 0
        self._arrived = asyncio.Event()
        self._closed = False
        self._close_reason = ""
        self._reader: asyncio.Task[None] | None = None
        self._send_lock = asyncio.Lock()
        self.stats = ConnectionStats()

    # --- lifecycle ----------------------------------------------------------------------------

    async def open(self) -> None:
        if self._reader is None:
            self._reader = asyncio.create_task(self._read_loop(), name="codex-app-server-reader")

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def close_reason(self) -> str:
        return self._close_reason

    @property
    def last_seq(self) -> int:
        return self._seq

    @property
    def first_seq(self) -> int:
        """The oldest sequence still retained in the bounded buffer."""

        return self._first_seq

    def pending_methods(self) -> tuple[str, ...]:
        return tuple(item.method for item in self._pending.values())

    async def close(self, reason: str = "closed by mission control") -> None:
        await self._channel.close()
        self._finish(reason)
        if self._reader is not None and self._reader is not asyncio.current_task():
            self._reader.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._reader

    # --- outbound -----------------------------------------------------------------------------

    async def request(
        self,
        method: str,
        params: Mapping[str, Any] | None = None,
        *,
        timeout_s: float | None = None,
    ) -> Any:
        if self._closed:
            raise TransportClosed(self._close_reason or "connection is closed")
        request_id = self._next_id
        self._next_id += 1
        loop = asyncio.get_running_loop()
        pending = _Pending(method, loop.create_future())
        self._pending[request_id] = pending
        message: dict[str, Any] = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            message["params"] = dict(params)
        try:
            async with self._send_lock:
                await self._channel.send(message)
        except Exception as error:
            self._pending.pop(request_id, None)
            self._finish(f"send failed: {error}")
            raise TransportClosed(self._close_reason) from error
        self.stats.requests_sent += 1
        wait = self._timeout if timeout_s is None else timeout_s
        try:
            return await asyncio.wait_for(asyncio.shield(pending.future), wait)
        except TimeoutError:
            self._pending.pop(request_id, None)
            self.stats.lost_responses += 1
            raise ResponseLost(method, request_id, wait) from None
        except asyncio.CancelledError:
            # The caller was cancelled; a late response is counted as unmatched.
            self._pending.pop(request_id, None)
            raise

    async def notify(self, method: str, params: Mapping[str, Any] | None = None) -> None:
        if self._closed:
            raise TransportClosed(self._close_reason or "connection is closed")
        message: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if params is not None:
            message["params"] = dict(params)
        async with self._send_lock:
            await self._channel.send(message)

    async def respond(self, request_id: RequestId, result: Mapping[str, Any] | None) -> None:
        await self._send_response(
            {"jsonrpc": "2.0", "id": request_id, "result": dict(result) if result else {}}
        )

    async def respond_error(
        self, request_id: RequestId, code: int, message: str, data: Any = None
    ) -> None:
        error: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            error["data"] = data
        await self._send_response({"jsonrpc": "2.0", "id": request_id, "error": error})

    async def _send_response(self, message: dict[str, Any]) -> None:
        if self._closed:
            raise TransportClosed(self._close_reason or "connection is closed")
        async with self._send_lock:
            await self._channel.send(message)
        self.stats.responses_sent += 1

    # --- inbound ------------------------------------------------------------------------------

    async def events(self, after_seq: int = 0) -> AsyncIterator[InboundEvent]:
        """Every notification and server request after `after_seq`, buffered then live.

        Raises `CursorExpired` when `after_seq` precedes the retained window and
        `TransportClosed` once the buffer is drained on a closed connection.
        """

        if after_seq + 1 < self._first_seq:
            raise CursorExpired(f"event {after_seq + 1} left the buffer (first {self._first_seq})")
        position = after_seq
        while True:
            while position < self._seq:
                index = position - self._first_seq + 1
                if index < 0:
                    raise CursorExpired(f"event {position + 1} left the buffer")
                event = self._events[index]
                position = event.seq
                yield event
            if self._closed:
                raise TransportClosed(self._close_reason or "connection closed")
            self._arrived.clear()
            await self._arrived.wait()

    def buffered(self, after_seq: int = 0) -> tuple[InboundEvent, ...]:
        """The retained events after `after_seq` (no waiting)."""

        return tuple(event for event in self._events if event.seq > after_seq)

    async def _read_loop(self) -> None:
        reason = "end of stream"
        try:
            while True:
                try:
                    message = await self._channel.receive()
                except ProtocolViolation as error:
                    reason = f"protocol violation: {error}"
                    break
                except (ConnectionError, OSError, asyncio.IncompleteReadError) as error:
                    reason = f"channel error: {error}"
                    break
                if message is None:
                    break
                self._dispatch(message)
        except asyncio.CancelledError:
            reason = "reader cancelled"
            raise
        finally:
            self._finish(reason)

    def _dispatch(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        has_id = "id" in message
        if isinstance(method, str):
            params = message.get("params")
            body = dict(params) if isinstance(params, Mapping) else {}
            if has_id:
                self._append("server_request", method, body, message["id"])
                self.stats.server_requests += 1
            else:
                self._append("notification", method, body, None)
                self.stats.notifications += 1
            return
        if has_id and ("result" in message or "error" in message):
            pending = self._pending.pop(message["id"], None)
            if pending is None:
                self.stats.unmatched_responses += 1
                _LOGGER.warning("unmatched app-server response id=%r", message["id"])
                return
            self.stats.responses_matched += 1
            if pending.future.done():
                return
            error = message.get("error")
            if isinstance(error, Mapping):
                pending.future.set_exception(
                    AppServerError(
                        pending.method,
                        int(error.get("code", -32000)),
                        str(error.get("message", "")),
                        error.get("data"),
                    )
                )
            else:
                pending.future.set_result(message.get("result"))
            return
        _LOGGER.warning("ignored non-JSON-RPC message from the app-server: %s", sorted(message))

    def _append(
        self, kind: EventKind, method: str, params: dict[str, Any], request_id: RequestId | None
    ) -> None:
        self._seq += 1
        event = InboundEvent(self._seq, kind, method, params, request_id)
        self._events.append(event)
        self.stats.events.append(event)
        if len(self._events) > self._limit:
            dropped = len(self._events) - self._limit
            del self._events[:dropped]
            self._first_seq += dropped
            self.stats.dropped_events += dropped
        self._arrived.set()

    def _finish(self, reason: str) -> None:
        if self._closed:
            return
        self._closed = True
        self._close_reason = reason
        for request_id, pending in list(self._pending.items()):
            if not pending.future.done():
                pending.future.set_exception(
                    TransportClosed(f"{pending.method} (id {request_id}) lost: {reason}")
                )
        self._pending.clear()
        self._arrived.set()


__all__ = [
    "DEFAULT_BUFFER_LIMIT",
    "DEFAULT_REQUEST_TIMEOUT_S",
    "AppServerConnection",
    "AppServerError",
    "ConnectionStats",
    "CursorExpired",
    "EventKind",
    "InboundEvent",
    "JsonLinesChannel",
    "MessageChannel",
    "ProtocolViolation",
    "RequestId",
    "ResponseLost",
    "TransportClosed",
]
