"""Bounded delivery: unacknowledged bytes per subscription and connection, and rate limits.

Socket.IO queues emitted packets per connection without a bound, so the bound lives here:
a subscription only emits while its unacknowledged envelopes fit both its own window
(`StreamSubscription.max_queue_bytes`) and the connection's shared budget. Reading stops
when the window is full; durable rows stay in PostgreSQL until the client acknowledges, so
memory is bounded by the windows, not by how far behind a client is.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from mission_control.domain.subscriptions.streams import StreamName


@dataclass
class ConnectionBudget:
    max_bytes: int
    used: int = 0

    def fits(self, size: int) -> bool:
        return self.used + size <= self.max_bytes


@dataclass
class InflightWindow:
    """Unacknowledged envelopes of one subscription, in emission order per stream."""

    max_bytes: int
    budget: ConnectionBudget
    used: int = 0
    _entries: dict[StreamName, deque[tuple[int, int]]] = field(default_factory=dict)

    def fits(self, size: int) -> bool:
        if self.used == 0 and self.budget.used == 0:
            return True  # one envelope always fits an idle connection
        return self.used + size <= self.max_bytes and self.budget.fits(size)

    def add(self, stream: StreamName, position: int, size: int) -> None:
        self._entries.setdefault(stream, deque()).append((position, size))
        self.used += size
        self.budget.used += size

    def release(self, stream: StreamName, upto: int) -> int:
        """Forget envelopes at or below `upto`; returns the bytes freed."""

        freed = 0
        entries = self._entries.get(stream)
        while entries and entries[0][0] <= upto:
            freed += entries.popleft()[1]
        self.used -= freed
        self.budget.used -= freed
        return freed

    def reset_stream(self, stream: StreamName) -> None:
        entries = self._entries.pop(stream, deque())
        freed = sum(size for _position, size in entries)
        self.used -= freed
        self.budget.used -= freed

    def close(self) -> None:
        self.budget.used -= self.used
        self.used = 0
        self._entries.clear()


@dataclass
class TokenBucket:
    """Client message rate limit for one connection."""

    rate: float
    burst: int
    clock: Callable[[], float] = time.monotonic
    _tokens: float = -1.0
    _at: float = 0.0

    def take(self) -> bool:
        now = self.clock()
        if self._tokens < 0:
            self._tokens, self._at = float(self.burst), now
        self._tokens = min(float(self.burst), self._tokens + (now - self._at) * self.rate)
        self._at = now
        if self._tokens < 1:
            return False
        self._tokens -= 1
        return True


__all__ = ["ConnectionBudget", "InflightWindow", "TokenBucket"]
