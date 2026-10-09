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
from collections.abc import Callable, Sequence

import asyncpg

from mission_control.adapters.operations.runtime_ports import EnvironmentSecretResolver
from mission_control.adapters.postgres.subscriptions.coordinator_inbox import (
    PostgresCoordinatorInboxStore,
)
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.adapters.subscriptions.webhook import (
    EgressGuardedWebhookTransport,
    EgressPolicy,
)
from mission_control.application.subscriptions.coordinator_ports import (
    CoordinatorCommandGateway,
    InboxNotifier,
)
from mission_control.application.subscriptions.coordinator_service import (
    CoordinatorInboxService,
)
from mission_control.application.subscriptions.ports import McpSessionNotifier, WebhookTransport
from mission_control.application.subscriptions.relay import SubscriptionRelay
from mission_control.application.subscriptions.service import SubscriptionService
from mission_control.bootstrap.settings import Settings, get_settings

LOGGER = logging.getLogger(__name__)
RELAY_ENV = "MISSION_CONTROL_SUBSCRIPTION_RELAY"


def compose_subscription_service(pool: asyncpg.Pool, request_scope: str) -> SubscriptionService:
    return SubscriptionService(PostgresSubscriptionStore(pool, request_scope))


def webhook_egress_policy(settings: Settings | None = None) -> EgressPolicy:
    """MP-15: https to global addresses; loopback/private networks only when configured."""

    settings = settings or get_settings()
    return EgressPolicy(
        allow_loopback=settings.webhook_allow_loopback,
        allowed_networks=settings.webhook_allowed_networks,
    )


MailboxSemantics = Callable[[str, str], str]
InboxComposition = tuple[
    asyncpg.Pool, str, CoordinatorCommandGateway | None, MailboxSemantics | None
]


def compose_coordinator_inbox_service(
    pool: asyncpg.Pool,
    request_scope: str,
    *,
    commands: CoordinatorCommandGateway | None = None,
    notifier: InboxNotifier | None = None,
    transport: WebhookTransport | None = None,
    mailbox_semantics: MailboxSemantics | None = None,
    settings: Settings | None = None,
) -> CoordinatorInboxService:
    """MP-15: the durable coordinator inbox of one tenant scope (needs migration 0033)."""

    return CoordinatorInboxService(
        PostgresSubscriptionStore(pool, request_scope),
        PostgresCoordinatorInboxStore(pool, request_scope),
        commands=commands,
        notifier=notifier,
        transport=transport,
        secrets=EnvironmentSecretResolver(),
        destinations=webhook_egress_policy(settings),
        mailbox_semantics=mailbox_semantics,
        owner=f"api-coordinator-inbox:{os.getpid()}",
    )


def relay_enabled() -> bool:
    return os.environ.get(RELAY_ENV, "") == "1"


async def run_subscription_relays(
    stores: Sequence[PostgresSubscriptionStore],
    stop: asyncio.Event,
    *,
    notifier: McpSessionNotifier | None = None,
    interval_seconds: float = 2.0,
    inboxes: Sequence[InboxComposition] = (),
    inbox_notifier: InboxNotifier | None = None,
    settings: Settings | None = None,
) -> None:
    # MP-15: every outbound callback is egress-validated and pinned to the validated address.
    transport = EgressGuardedWebhookTransport(webhook_egress_policy(settings))
    inbox_services = [
        compose_coordinator_inbox_service(
            pool,
            scope,
            commands=commands,
            notifier=inbox_notifier,
            transport=transport,
            mailbox_semantics=semantics,
            settings=settings,
        )
        for pool, scope, commands, semantics in inboxes
    ]
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
            for inbox in inbox_services:
                try:
                    await inbox.run_once()  # webhook callbacks, MCP hints, mailbox prompts
                except Exception:
                    LOGGER.exception("coordinator inbox pass failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
    finally:
        await transport.aclose()
