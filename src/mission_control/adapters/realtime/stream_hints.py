"""Redis pub/sub stream hints: cross-process wake-ups, never the replay ledger (ADR-0036).

A hint names only a request scope and a mission or harness execution id. A subscriber that
misses a hint (Redis restart, disconnect, slow pub/sub reader) still delivers the event on
its next high-watermark comparison against PostgreSQL. Malformed messages are dropped.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from typing import Any
from uuid import UUID

from redis.asyncio import Redis
from redis.exceptions import RedisError

from mission_control.application.streams.ports import StreamHint

logger = logging.getLogger(__name__)

DEFAULT_HINT_CHANNEL = "mission-control:stream-hints:v1"


def encode_hint(hint: StreamHint) -> str:
    return json.dumps(
        {
            "scope": hint.request_scope,
            "mission_id": None if hint.mission_id is None else str(hint.mission_id),
            "execution_id": (
                None if hint.harness_execution_id is None else str(hint.harness_execution_id)
            ),
        },
        separators=(",", ":"),
    )


def decode_hint(raw: Any) -> StreamHint | None:
    try:
        data = json.loads(raw.decode() if isinstance(raw, bytes) else raw)
        scope = data["scope"]
        if not isinstance(scope, str) or not scope or len(scope) > 512:
            return None
        mission = data.get("mission_id")
        execution = data.get("execution_id")
        hint = StreamHint(
            request_scope=scope,
            mission_id=None if mission is None else UUID(mission),
            harness_execution_id=None if execution is None else UUID(execution),
        )
    except (ValueError, TypeError, KeyError, AttributeError):
        return None
    if hint.mission_id is None and hint.harness_execution_id is None:
        return None
    return hint


class RedisStreamHints:
    """`StreamHintSource` and `StreamHintPublisher` over one Redis channel."""

    def __init__(self, redis: Redis, *, channel: str = DEFAULT_HINT_CHANNEL) -> None:
        self._redis = redis
        self._channel = channel

    async def publish(self, hint: StreamHint) -> None:
        await self._redis.publish(self._channel, encode_hint(hint))

    async def run(self, deliver: Callable[[StreamHint], None], stop: asyncio.Event) -> None:
        while not stop.is_set():
            pubsub = self._redis.pubsub()
            try:
                await pubsub.subscribe(self._channel)
                while not stop.is_set():
                    message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.5)
                    if message is None or message.get("type") != "message":
                        continue
                    hint = decode_hint(message.get("data"))
                    if hint is not None:
                        deliver(hint)
            except (OSError, RedisError):
                logger.warning("stream hint subscription lost; polling continues", exc_info=True)
                await asyncio.sleep(1.0)
            finally:
                await pubsub.aclose()


__all__ = ["DEFAULT_HINT_CHANNEL", "RedisStreamHints", "decode_hint", "encode_hint"]
