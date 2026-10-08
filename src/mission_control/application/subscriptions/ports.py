"""Ports for subscriptions: the durable store, the webhook transport and MCP sessions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol
from uuid import UUID

from mission_control.domain.subscriptions.contracts import (
    McpSessionChannel,
    MissionEventEnvelope,
    StreamTicketChannel,
    Subscription,
    SubscriptionDelivery,
    SubscriptionFilters,
    SubscriptionState,
    SubscriptionTarget,
    WebhookChannel,
)


class SubscriptionNotFound(LookupError):
    """No subscription (or no target) with that id is visible in this scope."""


@dataclass(frozen=True)
class NewSubscription:
    target: SubscriptionTarget
    filters: SubscriptionFilters
    channel: WebhookChannel | StreamTicketChannel | McpSessionChannel
    cursor_seq: int
    actor_ref: str
    now: datetime


class SubscriptionStore(Protocol):
    """Scope-bound persistence (one store per installation, application and tenant)."""

    @property
    def request_scope(self) -> str: ...

    async def resolve_target(self, kind: str, target_id: UUID) -> SubscriptionTarget: ...

    async def last_seq(self, mission_id: UUID) -> int: ...

    async def oldest_seq(self, mission_id: UUID) -> int | None: ...

    async def create(self, subscription: NewSubscription) -> Subscription: ...

    async def get(self, subscription_id: UUID) -> Subscription: ...

    async def list(
        self, *, mission_id: UUID | None = None, run_id: UUID | None = None
    ) -> tuple[Subscription, ...]: ...

    async def set_state(
        self,
        subscription_id: UUID,
        state: SubscriptionState,
        *,
        now: datetime,
        actor_ref: str | None = None,
    ) -> Subscription: ...

    async def lease_due(
        self, *, now: datetime, owner: str, lease_seconds: int, limit: int
    ) -> tuple[Subscription, ...]: ...

    async def events_after(
        self, target: SubscriptionTarget, after_seq: int, limit: int
    ) -> tuple[MissionEventEnvelope, ...]: ...

    async def attempts(self, subscription_id: UUID, event_id: UUID) -> int: ...

    async def record_success(
        self, subscription: Subscription, delivery: SubscriptionDelivery, *, owner: str
    ) -> Subscription: ...

    async def advance_cursor(
        self, subscription: Subscription, cursor_seq: int, *, now: datetime, owner: str
    ) -> Subscription: ...

    async def record_failure(
        self,
        subscription: Subscription,
        delivery: SubscriptionDelivery,
        *,
        failure_count: int,
        next_attempt_at: datetime,
        dead_letter: bool,
        owner: str,
    ) -> Subscription: ...

    async def release(self, subscription: Subscription, *, owner: str) -> None: ...


@dataclass(frozen=True)
class WebhookResponse:
    status_code: int
    latency_ms: int


class WebhookTransportError(Exception):
    """The request did not produce an HTTP response (connect, timeout, protocol)."""


class WebhookTransport(Protocol):
    async def post(self, url: str, body: bytes, headers: Mapping[str, str]) -> WebhookResponse: ...


class McpSessionNotifier(Protocol):
    async def notify(self, session_ref: str, envelope: MissionEventEnvelope) -> bool:
        """Send ``notifications/mission/event``; False when the session is gone."""
        ...
