"""Composition for subscriptions: scoped services and the opt-in relay loop (FT-F5).

The relay runs inside the API process when ``MISSION_CONTROL_SUBSCRIPTION_RELAY=1``; it
uses the runtime pool already bound to each tenant scope, resolves webhook secrets from the
process environment (``environment:<KEY>`` references only) and never introduces a second
scheduler: it only consumes committed mission events.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import Sequence

import asyncpg

from mission_control.adapters.operations.runtime_ports import EnvironmentSecretResolver
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.subscriptions.webhook import HttpxWebhookTransport
from mission_control.application.subscriptions.ports import McpSessionNotifier
from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import SubscriptionService

LOGGER = logging.getLogger(__name__)
RELAY_ENV = "MISSION_CONTROL_SUBSCRIPTION_RELAY"


def compose_subscription_service(pool: asyncpg.Pool, request_scope: str) -> SubscriptionService:
    return SubscriptionService(PostgresSubscriptionStore(pool, request_scope))


def relay_enabled() -> bool:
    return os.environ.get(RELAY_ENV, "") == "1"


async def run_subscription_relays(
    stores: Sequence[PostgresSubscriptionStore],
    stop: asyncio.Event,
    *,
    notifier: McpSessionNotifier | None = None,
    interval_seconds: float = 2.0,
) -> None:
    transport = HttpxWebhookTransport()
    relays = [
        SubscriptionRelay(
            store,
            transport=transport,
            secrets=EnvironmentSecretResolver(),
            notifier=notifier,
            owner=f"api-relay:{os.getpid()}",
        )
        for store in stores
    ]
    try:
        while not stop.is_set():
            for relay in relays:
                try:
                    await relay.run_once()
                except Exception:  # One tenant's failure must not stop the others.
                    LOGGER.exception("subscription relay pass failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
    finally:
        await transport.aclose()
