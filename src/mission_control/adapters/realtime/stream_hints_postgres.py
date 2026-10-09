"""PostgreSQL `LISTEN mc_stream_hint` stream hints (migration 0032; ADR-0036, MP-14).

The 0032 triggers notify the channel after every `mission_event` / `provider_frame` insert
with the request scope and the mission or harness execution id only. One dedicated
connection per application database listens and wakes the socket pumps; it is never the
replay ledger (pumps compare high-watermarks against the tables), so a dropped connection or
a missed notification delays delivery until the next poll and nothing is lost. The payload
shape is the one `stream_hints.decode_hint` reads, so Redis and PostgreSQL hints are
interchangeable sources.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg

from mission_control.adapters.realtime.stream_hints import decode_hint
from mission_control.application.streams.ports import StreamHint, StreamHintSource

logger = logging.getLogger(__name__)

STREAM_HINT_CHANNEL = "mc_stream_hint"
Connect = Callable[[str], Awaitable[asyncpg.Connection]]


async def _connect(dsn: str) -> asyncpg.Connection:
    return await asyncpg.connect(dsn, timeout=10)


class PostgresStreamHints:
    """`StreamHintSource` over one listening connection; reconnects while not stopped."""

    def __init__(
        self,
        dsn: str,
        *,
        channel: str = STREAM_HINT_CHANNEL,
        reconnect_seconds: float = 1.0,
        connect: Connect = _connect,
    ) -> None:
        self._dsn = dsn
        self._channel = channel
        self._reconnect = reconnect_seconds
        self._connect = connect
        self.connections = 0

    async def run(self, deliver: Callable[[StreamHint], None], stop: asyncio.Event) -> None:
        def on_notify(_conn: Any, _pid: int, _channel: str, payload: str) -> None:
            hint = decode_hint(payload)
            if hint is not None:
                deliver(hint)

        while not stop.is_set():
            try:
                connection = await self._connect(self._dsn)
            except (TimeoutError, OSError, asyncpg.PostgresError):
                logger.warning("stream hint listener cannot connect; polling continues")
                await _sleep_unless_stopped(stop, self._reconnect)
                continue
            self.connections += 1
            try:
                await connection.add_listener(self._channel, on_notify)
                while not stop.is_set() and not connection.is_closed():
                    await _sleep_unless_stopped(stop, 1.0)
            except (OSError, asyncpg.PostgresError):
                logger.warning("stream hint listener lost; polling continues", exc_info=True)
            finally:
                with contextlib.suppress(Exception):
                    await connection.remove_listener(self._channel, on_notify)
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(connection.close(), timeout=5)
            if not stop.is_set():
                await _sleep_unless_stopped(stop, self._reconnect)


class CompositeStreamHints:
    """Several hint sources (PostgreSQL per application, Redis) feeding one hub."""

    def __init__(self, *sources: StreamHintSource) -> None:
        self._sources = sources

    async def run(self, deliver: Callable[[StreamHint], None], stop: asyncio.Event) -> None:
        if not self._sources:
            await stop.wait()
            return
        results = await asyncio.gather(
            *(source.run(deliver, stop) for source in self._sources), return_exceptions=True
        )
        for result in results:
            if isinstance(result, Exception):
                logger.warning("a stream hint source stopped early", exc_info=result)


async def _sleep_unless_stopped(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


__all__ = ["STREAM_HINT_CHANNEL", "CompositeStreamHints", "PostgresStreamHints"]
