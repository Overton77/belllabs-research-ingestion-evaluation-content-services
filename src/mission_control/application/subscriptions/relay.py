"""The subscription relay: an outbox consumer that delivers committed mission events.

One pass leases due subscriptions (webhook and MCP session channels; SSE stream tickets are
pulled by the client), reads committed events after each cursor, applies the filters, and
delivers in ``seq`` order. The cursor advances only after a delivery receipt is written, so
a relay that dies between sending and recording sends the same event again (same
``event_id``): delivery is at least once and never skips a ``seq``. Failures back off
exponentially with full jitter; the twelfth consecutive failure dead-letters the
subscription and the store emits ``subscription.dead_lettered`` into the mission stream.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

from mission_control.application.execution.operations.operation_execution import (
    SecretResolutionPort,
)
from mission_control.application.subscriptions.ports import (
    McpSessionNotifier,
    SubscriptionStore,
    WebhookResponse,
    WebhookTransport,
    WebhookTransportError,
)
from mission_control.domain.subscriptions.contracts import (
    DeliveryStatus,
    McpSessionChannel,
    MissionEventEnvelope,
    StreamTicketChannel,
    Subscription,
    SubscriptionDelivery,
    SubscriptionState,
    WebhookChannel,
    after_failure,
    secret_ref_name,
    sign,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass
class RelayPassReport:
    leased: int = 0
    delivered: list[tuple[str, int]] = field(default_factory=list)
    failed: list[tuple[str, int]] = field(default_factory=list)
    dead_lettered: list[str] = field(default_factory=list)
    paused: list[str] = field(default_factory=list)


class SubscriptionRelay:
    def __init__(
        self,
        store: SubscriptionStore,
        *,
        transport: WebhookTransport,
        secrets: SecretResolutionPort,
        notifier: McpSessionNotifier | None = None,
        owner: str = "subscription-relay",
        clock: Callable[[], datetime] = _utc_now,
        jitter: Callable[[], float] = random.random,
        lease_seconds: int = 60,
        batch: int = 50,
    ) -> None:
        self._store = store
        self._transport = transport
        self._secrets = secrets
        self._notifier = notifier
        self._owner = owner
        self._clock = clock
        self._jitter = jitter
        self._lease_seconds = lease_seconds
        self._batch = batch

    async def run_once(self, *, limit: int = 20) -> RelayPassReport:
        report = RelayPassReport()
        leased = await self._store.lease_due(
            now=self._clock(), owner=self._owner, lease_seconds=self._lease_seconds, limit=limit
        )
        report.leased = len(leased)
        for subscription in leased:
            try:
                await self._drain(subscription, report)
            finally:
                await self._store.release(subscription, owner=self._owner)
        return report

    async def _drain(self, subscription: Subscription, report: RelayPassReport) -> None:
        events = await self._store.events_after(
            subscription.target, subscription.cursor_seq, self._batch
        )
        current = subscription
        skipped_to = current.cursor_seq
        for event in events:
            if not current.filters.matches(event):
                skipped_to = event.seq
                continue
            if skipped_to > current.cursor_seq:
                current = await self._store.advance_cursor(
                    current, skipped_to, now=self._clock(), owner=self._owner
                )
            outcome = await self._deliver(current, event, report)
            if outcome is None:
                return
            current = outcome
            skipped_to = current.cursor_seq
        if skipped_to > current.cursor_seq:
            await self._store.advance_cursor(
                current, skipped_to, now=self._clock(), owner=self._owner
            )

    async def _deliver(
        self, subscription: Subscription, event: MissionEventEnvelope, report: RelayPassReport
    ) -> Subscription | None:
        """Deliver one event; the updated subscription to continue with, or None to stop."""

        channel = subscription.channel
        attempt = await self._store.attempts(subscription.subscription_id, event.event_id) + 1
        name = str(subscription.subscription_id)
        if isinstance(channel, StreamTicketChannel):
            return None
        if isinstance(channel, McpSessionChannel):
            delivered = self._notifier is not None and await self._notifier.notify(
                channel.session_ref, event
            )
            if not delivered:
                # A disconnected session pauses; it resumes from its cursor on reconnect.
                await self._store.set_state(
                    subscription.subscription_id, SubscriptionState.PAUSED, now=self._clock()
                )
                report.paused.append(name)
                return None
            report.delivered.append((name, event.seq))
            return await self._store.record_success(
                subscription,
                self._receipt(subscription, event, attempt, DeliveryStatus.DELIVERED),
                owner=self._owner,
            )
        assert isinstance(channel, WebhookChannel)
        response, error_class = await self._post(channel, subscription, event)
        if response is not None and 200 <= response.status_code < 300:
            report.delivered.append((name, event.seq))
            return await self._store.record_success(
                subscription,
                self._receipt(
                    subscription,
                    event,
                    attempt,
                    DeliveryStatus.DELIVERED,
                    response=response,
                ),
                owner=self._owner,
            )
        decision = after_failure(subscription.failure_count, self._clock(), self._jitter())
        status = DeliveryStatus.DEAD_LETTERED if decision.dead_letter else DeliveryStatus.FAILED
        await self._store.record_failure(
            subscription,
            self._receipt(
                subscription,
                event,
                attempt,
                status,
                response=response,
                error_class=error_class
                or (f"http_{response.status_code}" if response is not None else None),
            ),
            failure_count=decision.failure_count,
            next_attempt_at=decision.next_attempt_at,
            dead_letter=decision.dead_letter,
            owner=self._owner,
        )
        if decision.dead_letter:
            report.dead_lettered.append(name)
        else:
            report.failed.append((name, event.seq))
        return None

    async def _post(
        self, channel: WebhookChannel, subscription: Subscription, event: MissionEventEnvelope
    ) -> tuple[WebhookResponse | None, str | None]:
        try:
            resolved = await self._secrets.resolve((channel.secret_ref,))
            secret = resolved[secret_ref_name(channel.secret_ref)].encode("utf-8")
        except (LookupError, KeyError):
            return None, "secret_unavailable"
        body = event.body()
        headers = {
            "Content-Type": "application/json",
            channel.signature_header: sign(secret, body),
            "X-MC-Event-Id": str(event.event_id),
            "X-MC-Event-Seq": str(event.seq),
            "X-MC-Subscription-Id": str(subscription.subscription_id),
        }
        try:
            response = await self._transport.post(channel.url, body, headers)
        except WebhookTransportError:
            return None, "transport_error"
        return response, None

    def _receipt(
        self,
        subscription: Subscription,
        event: MissionEventEnvelope,
        attempt: int,
        status: DeliveryStatus,
        *,
        response: WebhookResponse | None = None,
        error_class: str | None = None,
    ) -> SubscriptionDelivery:
        return SubscriptionDelivery(
            subscription_id=subscription.subscription_id,
            event_id=event.event_id,
            seq=event.seq,
            attempt=attempt,
            status=status,
            response_code=response.status_code if response is not None else None,
            latency_ms=response.latency_ms if response is not None else None,
            error_class=error_class,
            recorded_at=self._clock(),
        )
