"""Process-local wake-up routing from stream hints to the pumps reading that target.

Keys include the request scope, so a hint for one tenant never wakes another tenant's pump;
a woken pump still reads only through its own scope-bound source. Hints are advisory: the
pumps also compare high-watermarks on every poll interval.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

from mission_control.application.streams.ports import ResolvedTarget, StreamHint

HintKey = tuple[str, str, UUID]


def target_keys(request_scope: str, target: ResolvedTarget) -> tuple[HintKey, ...]:
    keys: list[HintKey] = [(request_scope, "mission", target.mission_id)]
    if target.harness_execution_id is not None:
        keys.append((request_scope, "execution", target.harness_execution_id))
    return tuple(keys)


def hint_keys(hint: StreamHint) -> tuple[HintKey, ...]:
    keys: list[HintKey] = []
    if hint.mission_id is not None:
        keys.append((hint.request_scope, "mission", hint.mission_id))
    if hint.harness_execution_id is not None:
        keys.append((hint.request_scope, "execution", hint.harness_execution_id))
    return tuple(keys)


class StreamWakeups:
    def __init__(self) -> None:
        self._waiters: dict[HintKey, set[asyncio.Event]] = {}

    def register(self, keys: tuple[HintKey, ...], event: asyncio.Event) -> None:
        for key in keys:
            self._waiters.setdefault(key, set()).add(event)

    def unregister(self, keys: tuple[HintKey, ...], event: asyncio.Event) -> None:
        for key in keys:
            waiters = self._waiters.get(key)
            if waiters is None:
                continue
            waiters.discard(event)
            if not waiters:
                del self._waiters[key]

    def deliver(self, hint: StreamHint) -> None:
        for key in hint_keys(hint):
            for event in self._waiters.get(key, ()):
                event.set()

    def __len__(self) -> int:
        return len(self._waiters)


__all__ = ["HintKey", "StreamWakeups", "hint_keys", "target_keys"]
