"""One subscription's delivery loop: replay then live, from the store, inside a window.

The pump reads pages after its position, emits envelopes while they fit the subscription's
unacknowledged-bytes window, and waits for a wake-up (hint, ack) or the poll interval. A
full window stops emission; when it blocks emission without progress for the grace period
the pump reports
`SLOW_CONSUMER` with the acknowledged cursors to resume from and detaches. A worker is never
blocked: emission only enqueues on the client's connection, and the window bounds that
queue. Reauthorization is checked before every read.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Final, Protocol

from mission_control.application.streams.flow import InflightWindow
from mission_control.application.streams.service import (
    MissionStreamService,
    OpenedStream,
    ResyncNotice,
    StreamFailure,
)
from mission_control.domain.subscriptions.streams import (
    StreamCursor,
    StreamEnvelope,
    StreamError,
    StreamName,
)

logger = logging.getLogger(__name__)

MAX_SOURCE_FAILURES: Final = 3


class StreamDelivery(Protocol):
    async def envelope(self, envelope: StreamEnvelope) -> None: ...

    async def resync_required(self, notice: dict[str, Any]) -> None: ...

    async def error(self, error: StreamError) -> None: ...


@dataclass(frozen=True)
class PumpSettings:
    page_size: int = 200
    poll_interval: float = 1.0
    slow_consumer_grace: float = 10.0

    def __post_init__(self) -> None:
        if not 1 <= self.page_size <= 1_000:
            raise ValueError("replay page size must be between 1 and 1000")
        if self.poll_interval <= 0 or self.slow_consumer_grace <= 0:
            raise ValueError("poll interval and slow consumer grace must be positive")


def envelope_size(envelope: StreamEnvelope) -> int:
    return len(json.dumps(envelope.model_dump(mode="json"), separators=(",", ":")))


class SubscriptionPump:
    def __init__(
        self,
        service: MissionStreamService,
        opened: OpenedStream,
        delivery: StreamDelivery,
        window: InflightWindow,
        settings: PumpSettings,
        *,
        authorized: Callable[[], bool],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._service = service
        self.opened = opened
        self._delivery = delivery
        self._window = window
        self._settings = settings
        self._authorized = authorized
        self._clock = clock
        self.wake = asyncio.Event()
        coalesce = opened.subscription.delta_coalesce_ms / 1000
        self._delta_interval = 0.0 if opened.frame_filters.exclude_deltas else coalesce
        self._frames_at = 0.0
        self.emitted = 0

    @property
    def subscription_id(self) -> str:
        return self.opened.subscription.subscription_id

    def acknowledge(self, cursors: tuple[StreamCursor, ...]) -> dict[StreamName, int]:
        acked = self._service.acknowledge(self.opened, cursors)
        for stream, upto in acked.items():
            self._window.release(stream, upto)
        self.wake.set()
        return acked

    async def run(self) -> str:
        """Deliver until detached; returns the detach reason (an error code)."""

        blocked_since: float | None = None
        failures = 0
        try:
            while True:
                if not self._authorized():
                    await self._delivery.error(
                        StreamError(
                            code="UNAUTHORIZED",
                            retryable=True,
                            subscription_id=self.subscription_id,
                            detail="credential expired or revoked; reauthenticate",
                        )
                    )
                    return "UNAUTHORIZED"
                self.wake.clear()
                try:
                    moved, blocked = await self._pass()
                    failures = 0
                except StreamFailure as failure:
                    await self._delivery.error(failure.error(subscription_id=self.subscription_id))
                    return failure.code
                except Exception:
                    failures += 1
                    logger.exception("mission stream read failed", extra={"failures": failures})
                    if failures >= MAX_SOURCE_FAILURES:
                        await self._resync("UNAVAILABLE", "stream source unavailable")
                        return "UNAVAILABLE"
                    moved = blocked = False
                now = self._clock()
                if blocked:
                    if moved or blocked_since is None:
                        blocked_since = now
                    if now - blocked_since >= self._settings.slow_consumer_grace:
                        await self._resync("SLOW_CONSUMER", "acknowledgements stopped")
                        return "SLOW_CONSUMER"
                    timeout = min(
                        self._settings.poll_interval,
                        self._settings.slow_consumer_grace - (now - blocked_since),
                    )
                else:
                    blocked_since = None
                    if moved:
                        continue
                    timeout = self._settings.poll_interval
                    if self._frames_at > now:
                        timeout = min(timeout, self._frames_at - now)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(self.wake.wait(), timeout=max(timeout, 0.001))
        finally:
            self._window.close()

    async def _pass(self) -> tuple[bool, bool]:
        """One read of each durable stream: (positions moved, window blocked emission)."""

        moved = blocked = False
        for stream in self.opened.durable_streams:
            if stream == "provider_frames" and self._clock() < self._frames_at:
                continue
            stream_moved, stream_blocked = await self._pump(stream)
            moved = moved or stream_moved
            blocked = blocked or stream_blocked
        return moved, blocked

    async def _pump(self, stream: StreamName) -> tuple[bool, bool]:
        page = await self._service.next_page(self.opened, stream, limit=self._settings.page_size)
        if page.resync is not None:
            await self._resync_stream(page.resync)
            return True, False
        position = self.opened.positions[stream]
        start = position.after
        for item in page.items:
            size = envelope_size(item.envelope)
            if not self._window.fits(size):
                return position.after > start, True
            await self._delivery.envelope(item.envelope)
            self._window.add(stream, item.position, size)
            position.after = item.position
            position.sent = max(position.sent, item.position)
            self.emitted += 1
        position.after = max(position.after, page.scanned_to)
        if stream == "provider_frames" and page.items and self._delta_interval:
            self._frames_at = self._clock() + self._delta_interval
        return position.after > start, False

    async def _resync_stream(self, notice: ResyncNotice) -> None:
        self._service.apply_resync(self.opened, notice)
        self._window.reset_stream(notice.stream)
        await self._delivery.resync_required(
            {
                "subscription_id": self.subscription_id,
                "code": notice.code,
                "detached": False,
                "streams": [notice.stream],
                "replay_from": [notice.replay_from.model_dump(mode="json")],
            }
        )

    async def _resync(self, code: str, detail: str) -> None:
        await self._delivery.resync_required(
            {
                "subscription_id": self.subscription_id,
                "code": code,
                "detached": True,
                "detail": detail,
                "streams": list(self.opened.durable_streams),
                "replay_from": [
                    cursor.model_dump(mode="json")
                    for cursor in self._service.acked_cursors(self.opened)
                ],
            }
        )


__all__ = ["PumpSettings", "StreamDelivery", "SubscriptionPump", "envelope_size"]
