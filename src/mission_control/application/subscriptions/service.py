"""Subscribe, list, close, and the SSE event stream (replay from a cursor, then live)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

from mission_control.application.subscriptions.aliases import select
from mission_control.application.subscriptions.ports import (
    NewSubscription,
    SubscriptionNotFound,
    SubscriptionStore,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.contracts import (
    HEARTBEAT_SECONDS,
    SUBSCRIBE_PERMISSION,
    Subscription,
    SubscriptionFilters,
    SubscriptionRequest,
    SubscriptionState,
    SubscriptionTarget,
    can_close,
    sse_frame,
)

Clock = Callable[[], datetime]
Sleep = Callable[[float], Awaitable[None]]


class SubscriptionRejected(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _require_read(actor: ActorContext) -> None:
    if SUBSCRIBE_PERMISSION not in actor.permissions:
        raise SubscriptionRejected("unauthorized", f"actor lacks {SUBSCRIBE_PERMISSION}")


class SubscriptionService:
    def __init__(
        self,
        store: SubscriptionStore,
        *,
        clock: Clock = _utc_now,
        sleep: Sleep = asyncio.sleep,
        live_idle_polls: int | None = None,
    ) -> None:
        self._store = store
        self._clock = clock
        self._sleep = sleep
        self._live_idle_polls = live_idle_polls

    @property
    def request_scope(self) -> str:
        return self._store.request_scope

    async def subscribe(self, request: SubscriptionRequest, actor: ActorContext) -> Subscription:
        _require_read(actor)
        try:
            target = await self._store.resolve_target(request.target, request.target_id)
        except SubscriptionNotFound:
            raise SubscriptionRejected("not_found", "subscription target not found") from None
        cursor = (
            request.after_seq
            if request.after_seq is not None
            else await self._store.last_seq(target.mission_id)
        )
        return await self._store.create(
            NewSubscription(
                target=target,
                filters=SubscriptionFilters(
                    event_types=request.events, node_keys=request.node_keys
                ),
                channel=request.channel,
                cursor_seq=cursor,
                actor_ref=actor.actor_id,
                now=self._clock(),
            )
        )

    async def list(
        self, actor: ActorContext, *, mission_id: UUID | None = None, run_id: UUID | None = None
    ) -> tuple[Subscription, ...]:
        _require_read(actor)
        return await self._store.list(mission_id=mission_id, run_id=run_id)

    async def close(self, subscription_id: UUID, actor: ActorContext) -> Subscription:
        try:
            subscription = await self._store.get(subscription_id)
        except SubscriptionNotFound:
            raise SubscriptionRejected("not_found", "subscription not found") from None
        if not can_close(subscription, actor.actor_id, actor.permissions):
            raise SubscriptionRejected(
                "unauthorized", "closing another actor's subscription needs workflow_run.admin"
            )
        if subscription.state is SubscriptionState.CLOSED:
            return subscription
        return await self._store.set_state(
            subscription_id, SubscriptionState.CLOSED, now=self._clock(), actor_ref=actor.actor_id
        )

    async def resume(self, subscription_id: UUID, actor: ActorContext) -> Subscription:
        """Reactivate a paused or dead-lettered subscription; it resumes from its cursor."""

        subscription = await self._store.get(subscription_id)
        if not can_close(subscription, actor.actor_id, actor.permissions):
            raise SubscriptionRejected("unauthorized", "resuming needs ownership or admin")
        if subscription.state is SubscriptionState.CLOSED:
            raise SubscriptionRejected("closed", "a closed subscription cannot resume")
        return await self._store.set_state(
            subscription_id, SubscriptionState.ACTIVE, now=self._clock()
        )

    async def stream(
        self,
        mission_id: UUID,
        actor: ActorContext,
        *,
        after_seq: int = 0,
        event_types: tuple[str, ...] = ("*",),
        node_keys: tuple[str, ...] = (),
        heartbeat_seconds: float = HEARTBEAT_SECONDS,
        poll_seconds: float = 1.0,
        idle_polls: int | None = None,
        batch: int = 100,
    ) -> AsyncIterator[str]:
        """SSE frames: replay committed events after ``after_seq``, then follow live.

        A cursor older than the retained history yields ``resync_required`` with
        ``CURSOR_EXPIRED`` and ends the stream; the client resyncs from inspection.
        ``idle_polls`` bounds the live phase (``None`` streams until the client leaves).
        """

        _require_read(actor)
        try:
            target = await self._store.resolve_target("mission", mission_id)
        except SubscriptionNotFound:
            raise SubscriptionRejected("not_found", "mission not found") from None
        filters = SubscriptionFilters(event_types=event_types, node_keys=node_keys)
        return self._frames(
            target=target,
            filters=filters,
            after_seq=after_seq,
            heartbeat_seconds=heartbeat_seconds,
            poll_seconds=poll_seconds,
            idle_polls=idle_polls if idle_polls is not None else self._live_idle_polls,
            batch=batch,
        )

    async def _frames(
        self,
        *,
        target: SubscriptionTarget,
        filters: SubscriptionFilters,
        after_seq: int,
        heartbeat_seconds: float,
        poll_seconds: float,
        idle_polls: int | None,
        batch: int,
    ) -> AsyncIterator[str]:
        oldest = await self._store.oldest_seq(target.mission_id)
        if after_seq > 0 and oldest is not None and after_seq < oldest - 1:
            yield sse_frame(
                "resync_required",
                json.dumps({"code": "CURSOR_EXPIRED", "oldest_seq": oldest}),
            )
            return
        cursor = after_seq
        idle = 0
        since_frame = 0.0
        while True:
            events = await self._store.events_after(target, cursor, batch)
            for event in events:
                cursor = event.seq
                selected = select(filters, event)
                if selected is not None:
                    yield sse_frame(
                        "mission_event",
                        selected.envelope.body().decode("utf-8"),
                        event_id=str(event.seq),
                    )
                    since_frame = 0.0
            if events:
                idle = 0
                continue
            idle += 1
            if idle_polls is not None and idle > idle_polls:
                return
            await self._sleep(poll_seconds)
            since_frame += poll_seconds
            if since_frame >= heartbeat_seconds:
                yield sse_frame("heartbeat", json.dumps({"recorded_at": self._clock().isoformat()}))
                since_frame = 0.0
