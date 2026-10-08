"""In-memory subscription store and webhook transport for unit tests (no I/O)."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4, uuid5

from mission_control.application.subscriptions.ports import (
    NewSubscription,
    SubscriptionNotFound,
    WebhookResponse,
    WebhookTransportError,
)
from mission_control.domain.subscriptions.contracts import (
    MissionEventEnvelope,
    Subscription,
    SubscriptionDelivery,
    SubscriptionState,
    SubscriptionTarget,
    payload_digest,
)

SCOPE = "mc/00000000-0000-7000-8000-0000000000aa/biotech/00000000-0000-7000-8000-0000000000bb"
MISSION = UUID("00000000-0000-7000-8000-000000000001")
RUN = UUID("00000000-0000-7000-8000-000000000002")
T0 = datetime(2026, 10, 7, 12, tzinfo=UTC)


def event(
    seq: int, event_type: str = "run.progressed", node_key: str | None = None
) -> MissionEventEnvelope:
    return MissionEventEnvelope(
        event_id=uuid5(MISSION, f"event:{seq}"),
        application_id="biotech",
        mission_id=MISSION,
        run_id=RUN,
        seq=seq,
        event_type=event_type,
        event_version=1,
        actor_ref="actor:system",
        happened_at=T0 + timedelta(seconds=seq),
        recorded_at=T0 + timedelta(seconds=seq),
        node_key=node_key,
        payload_ref=f"mc://applications/biotech/missions/{MISSION}/events/{seq}",
        payload_digest=payload_digest({"seq": seq}),
    )


class Crash(Exception):
    """Simulates the relay process dying after a send and before its receipt."""


@dataclass
class InMemoryStore:
    events: list[MissionEventEnvelope] = field(default_factory=list)
    subscriptions: dict[UUID, Subscription] = field(default_factory=dict)
    receipts: list[SubscriptionDelivery] = field(default_factory=list)
    leases: dict[UUID, str] = field(default_factory=dict)
    dead_letter_events: list[UUID] = field(default_factory=list)
    crash_on_success: int | None = None
    successes: int = 0
    oldest: int | None = None

    @property
    def request_scope(self) -> str:
        return SCOPE

    async def resolve_target(self, kind: str, target_id: UUID) -> SubscriptionTarget:
        if kind == "mission" and target_id == MISSION:
            return SubscriptionTarget(kind="mission", mission_id=MISSION)
        if kind == "run" and target_id == RUN:
            return SubscriptionTarget(kind="run", mission_id=MISSION, run_id=RUN)
        raise SubscriptionNotFound(str(target_id))

    async def last_seq(self, mission_id: UUID) -> int:
        return max((item.seq for item in self.events), default=0)

    async def oldest_seq(self, mission_id: UUID) -> int | None:
        if self.oldest is not None:
            return self.oldest
        return min((item.seq for item in self.events), default=None)

    async def create(self, subscription: NewSubscription) -> Subscription:
        created = Subscription(
            subscription_id=uuid4(),
            request_scope=SCOPE,
            target=subscription.target,
            filters=subscription.filters,
            channel=subscription.channel,
            cursor_seq=subscription.cursor_seq,
            state=SubscriptionState.ACTIVE,
            next_attempt_at=subscription.now,
            actor_ref=subscription.actor_ref,
            created_at=subscription.now,
            updated_at=subscription.now,
        )
        self.subscriptions[created.subscription_id] = created
        return created

    async def get(self, subscription_id: UUID) -> Subscription:
        if subscription_id not in self.subscriptions:
            raise SubscriptionNotFound(str(subscription_id))
        return self.subscriptions[subscription_id]

    async def list(
        self, *, mission_id: UUID | None = None, run_id: UUID | None = None
    ) -> tuple[Subscription, ...]:
        return tuple(self.subscriptions.values())

    def _put(self, subscription: Subscription, **changes: object) -> Subscription:
        updated = subscription.model_copy(update={**changes, "version": subscription.version + 1})
        self.subscriptions[subscription.subscription_id] = updated
        return updated

    async def set_state(
        self,
        subscription_id: UUID,
        state: SubscriptionState,
        *,
        now: datetime,
        actor_ref: str | None = None,
    ) -> Subscription:
        current = self.subscriptions[subscription_id]
        self.leases.pop(subscription_id, None)
        return self._put(
            current,
            state=state,
            closed_at=now if state is SubscriptionState.CLOSED else None,
            failure_count=0 if state is SubscriptionState.ACTIVE else current.failure_count,
            next_attempt_at=now if state is SubscriptionState.ACTIVE else current.next_attempt_at,
        )

    async def lease_due(
        self, *, now: datetime, owner: str, lease_seconds: int, limit: int
    ) -> tuple[Subscription, ...]:
        due = [
            item
            for item in self.subscriptions.values()
            if item.state is SubscriptionState.ACTIVE
            and item.channel.kind != "stream_ticket"
            and item.next_attempt_at <= now
            and item.subscription_id not in self.leases
        ][:limit]
        for item in due:
            self.leases[item.subscription_id] = owner
        return tuple(due)

    async def events_after(
        self, target: SubscriptionTarget, after_seq: int, limit: int
    ) -> tuple[MissionEventEnvelope, ...]:
        found = [
            item
            for item in self.events
            if item.seq > after_seq and (target.run_id is None or item.run_id == target.run_id)
        ]
        return tuple(sorted(found, key=lambda item: item.seq)[:limit])

    async def attempts(self, subscription_id: UUID, event_id: UUID) -> int:
        return max(
            (
                item.attempt
                for item in self.receipts
                if item.subscription_id == subscription_id and item.event_id == event_id
            ),
            default=0,
        )

    def _fence(self, subscription: Subscription, owner: str) -> Subscription:
        if self.leases.get(subscription.subscription_id) != owner:
            raise RuntimeError("lease lost")
        return self.subscriptions[subscription.subscription_id]

    async def record_success(
        self, subscription: Subscription, delivery: SubscriptionDelivery, *, owner: str
    ) -> Subscription:
        current = self._fence(subscription, owner)
        self.successes += 1
        if self.crash_on_success is not None and self.successes == self.crash_on_success:
            raise Crash("relay died before writing its receipt")
        self.receipts.append(delivery)
        return self._put(
            current,
            cursor_seq=max(current.cursor_seq, delivery.seq),
            failure_count=0,
            next_attempt_at=delivery.recorded_at,
        )

    async def advance_cursor(
        self, subscription: Subscription, cursor_seq: int, *, now: datetime, owner: str
    ) -> Subscription:
        current = self._fence(subscription, owner)
        return self._put(current, cursor_seq=max(current.cursor_seq, cursor_seq))

    async def record_failure(
        self,
        subscription: Subscription,
        delivery: SubscriptionDelivery,
        *,
        failure_count: int,
        next_attempt_at: datetime,
        dead_letter: bool,
        owner: str,
    ) -> Subscription:
        current = self._fence(subscription, owner)
        self.receipts.append(delivery)
        if dead_letter:
            self.dead_letter_events.append(current.subscription_id)
            return self._put(
                current,
                failure_count=failure_count,
                state=SubscriptionState.DEAD_LETTERED,
                dead_lettered_at=delivery.recorded_at,
            )
        return self._put(current, failure_count=failure_count, next_attempt_at=next_attempt_at)

    async def release(self, subscription: Subscription, *, owner: str) -> None:
        if self.leases.get(subscription.subscription_id) == owner:
            del self.leases[subscription.subscription_id]


@dataclass
class Received:
    url: str
    body: bytes
    headers: dict[str, str]


@dataclass
class FakeWebhook:
    """Records requests like a local receiver; status codes are scripted."""

    statuses: list[int] = field(default_factory=list)
    default_status: int = 204
    fail_transport: bool = False
    received: list[Received] = field(default_factory=list)

    async def post(self, url: str, body: bytes, headers: Mapping[str, str]) -> WebhookResponse:
        if self.fail_transport:
            raise WebhookTransportError("ConnectError")
        self.received.append(Received(url, body, dict(headers)))
        status = self.statuses.pop(0) if self.statuses else self.default_status
        return WebhookResponse(status_code=status, latency_ms=3)


class StaticSecrets:
    def __init__(self, values: Mapping[str, str]) -> None:
        self._values = dict(values)

    async def resolve(self, refs: tuple[object, ...]) -> Mapping[str, str]:
        resolved: dict[str, str] = {}
        for ref in refs:
            name = f"{ref.provider}:{ref.key}"  # type: ignore[attr-defined]
            if name not in self._values:
                raise LookupError(name)
            resolved[name] = self._values[name]
        return resolved
