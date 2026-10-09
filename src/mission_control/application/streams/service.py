"""Scoped mission stream subscriptions: authorize, plan replay from cursors, read pages.

SPEC-04 "Replay algorithm" over the frozen `mc.stream_subscription.v1` and MP-13's
per-stream cursors (`application/frames/streams.py`):

1. The caller is authorized (`workflow_run.read`; `tool_detail="full"` also needs the
   artifact read grant) and the target is resolved under the service's own scope; a target
   of another tenant is indistinguishable from an absent one (`TARGET_NOT_FOUND`).
2. Each durable stream reads its committed high-watermark H first: `mission.last_event_seq`
   is written in the same transaction as the events it covers, and provider frame appends
   lock their harness execution row, so every position <= H is committed and visible.
3. A client cursor is checked against H (`CURSOR_AHEAD`, `SCOPE_MISMATCH`, ...) and replay
   covers `(cursor, H]`. Without a cursor, or with one older than retention, the stream
   starts at H behind a snapshot notice and the ack reports `gap`.
4. Live delivery is the same read loop continuing past H: the store is the only source, so
   a lost notification delays an event until the next high-watermark comparison and a
   duplicate notification reads nothing new. Positions only move forward, so a subscription
   never emits an event twice; across reconnects the client dedupes by `event_id`.

Mission events go through the public alias projection (`aliases.select`), so filters use
the documented vocabulary (`run.completed`, ...) and receive at most one envelope per
canonical event. Socket.IO arrival is at most once; recovery is the client's acknowledged
cursor, never an in-memory history.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Literal
from uuid import UUID

from pydantic import ValidationError

from mission_control.application.frames.artifact_bodies import ARTIFACT_READ_PERMISSION
from mission_control.application.frames.streams import (
    DELTA_KINDS,
    StreamCursorError,
    frame_envelope,
    frame_passes,
    mission_cursor,
    parse_frame_cursor,
    parse_mission_cursor,
)
from mission_control.application.frames.transcript import READ_PERMISSION
from mission_control.application.streams.ports import (
    ResolvedTarget,
    StreamSource,
    StreamTargetNotFound,
)
from mission_control.application.subscriptions.aliases import select
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.frames.contracts import FrameKind, ProviderFrame
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.subscriptions.contracts import (
    MissionEventEnvelope,
    SubscriptionFilters,
)
from mission_control.domain.subscriptions.streams import (
    StreamCursor,
    StreamEnvelope,
    StreamError,
    StreamErrorCode,
    StreamFilters,
    StreamName,
    StreamScope,
    StreamSubscription,
    SubscribeAck,
)

FULL_DETAIL_PERMISSION: Final = ARTIFACT_READ_PERMISSION
DURABLE_STREAMS: Final[tuple[StreamName, ...]] = ("mission_events", "provider_frames")
MAX_COALESCED_EXCERPT: Final = 8_192
_FRAME_KINDS: Final = frozenset(kind.value for kind in FrameKind)
_REF_LIMIT: Final = 512


class StreamFailure(Exception):
    """A typed stream error; `detail` never carries exception text from lower layers."""

    def __init__(self, code: StreamErrorCode, detail: str, *, retryable: bool = False) -> None:
        super().__init__(detail)
        self.code: StreamErrorCode = code
        self.detail = detail
        self.retryable = retryable

    def error(
        self, *, request_id: str | None = None, subscription_id: str | None = None
    ) -> StreamError:
        return StreamError(
            code=self.code,
            retryable=self.retryable,
            request_id=request_id,
            subscription_id=subscription_id,
            detail=self.detail[:512],
        )


@dataclass
class StreamPosition:
    """`after`: last position read past; `sent`: the bound for client acks."""

    stream: StreamName
    after: int
    sent: int
    acked: int
    generation: int | None = None


SnapshotReason = Literal["no_cursor", "cursor_expired", "stale_generation"]
"""`cursor_expired` applies to the mission journal only: frame retention deletes expired
non-closing raw frames in place, so a frame replay returns what is retained (closing facts
always are) rather than treating missing raw detail as an expired cursor."""


@dataclass(frozen=True)
class SnapshotNotice:
    stream: StreamName
    covered: StreamCursor
    reason: SnapshotReason


@dataclass(frozen=True)
class ResyncNotice:
    stream: StreamName
    code: StreamErrorCode
    replay_from: StreamCursor


@dataclass(frozen=True)
class PageItem:
    position: int
    envelope: StreamEnvelope


@dataclass(frozen=True)
class Page:
    stream: StreamName
    items: tuple[PageItem, ...] = ()
    scanned_to: int = 0
    resync: ResyncNotice | None = None


@dataclass
class OpenedStream:
    subscription: StreamSubscription
    target: ResolvedTarget
    ack: SubscribeAck
    event_filters: SubscriptionFilters
    frame_filters: StreamFilters
    positions: dict[StreamName, StreamPosition] = field(default_factory=dict)
    snapshots: tuple[SnapshotNotice, ...] = ()

    @property
    def durable_streams(self) -> tuple[StreamName, ...]:
        return tuple(s for s in self.subscription.streams if s in DURABLE_STREAMS)


def split_filters(filters: StreamFilters) -> tuple[SubscriptionFilters, StreamFilters]:
    """Route each filter kind to its stream: frame kinds filter frames, everything else is a
    mission event type pattern (exact, `*`, `family.*`, or a public alias)."""

    frame_kinds = tuple(kind for kind in filters.kinds if kind in _FRAME_KINDS)
    event_kinds = tuple(kind for kind in filters.kinds if kind not in _FRAME_KINDS)
    try:
        events = SubscriptionFilters(event_types=event_kinds or ("*",))
    except ValidationError:
        raise StreamFailure("UNSUPPORTED_FILTER", "unsupported event type filter") from None
    return events, filters.model_copy(update={"kinds": frame_kinds})


def _bounded(value: str | None) -> str | None:
    return value if value is None or len(value) <= _REF_LIMIT else None


class MissionStreamService:
    """One tenant scope's mission and provider-frame streams over a read-only source."""

    def __init__(self, source: StreamSource, *, request_scope: str) -> None:
        if source.request_scope != request_scope:
            raise ValueError("stream source scope differs from the service scope")
        parsed = parse_request_scope(request_scope)
        self._source = source
        self.request_scope = request_scope
        self.scope = StreamScope(
            installation_id=str(parsed.installation_id),
            application_id=parsed.application_id,
            tenant_id=str(parsed.tenant_id),
        )

    # --- subscribe -------------------------------------------------------------------

    def authorize(self, actor: ActorContext, filters: StreamFilters) -> None:
        if READ_PERMISSION not in actor.permissions:
            raise StreamFailure("UNAUTHORIZED", f"actor lacks {READ_PERMISSION}")
        if filters.tool_detail == "full" and FULL_DETAIL_PERMISSION not in actor.permissions:
            raise StreamFailure("UNAUTHORIZED", f"full tool detail needs {FULL_DETAIL_PERMISSION}")

    async def open(self, subscription: StreamSubscription, actor: ActorContext) -> OpenedStream:
        self.authorize(actor, subscription.filters)
        if subscription.scope != self.scope:
            raise StreamFailure("SCOPE_MISMATCH", "subscription scope is not the caller's scope")
        if subscription.target.kind == "chain":
            raise StreamFailure("UNSUPPORTED_FILTER", "chain targets are not streamed yet")
        if "provider_frames" in subscription.streams and subscription.target.kind != "execution":
            raise StreamFailure(
                "UNSUPPORTED_FILTER",
                "provider_frames cursors are per execution: subscribe each execution",
            )
        event_filters, frame_filters = split_filters(subscription.filters)
        try:
            target = await self._source.resolve(subscription.target)
        except StreamTargetNotFound:
            raise StreamFailure("TARGET_NOT_FOUND", "target not found") from None
        cursors = {cursor.stream: cursor for cursor in subscription.cursors}
        opened = OpenedStream(
            subscription=subscription,
            target=target,
            ack=SubscribeAck(subscription_id=subscription.subscription_id),
            event_filters=event_filters,
            frame_filters=frame_filters,
        )
        snapshots: list[SnapshotNotice] = []
        high: list[StreamCursor] = []
        replay: list[StreamCursor] = []
        versions: dict[str, str] = {}
        if "mission_events" in subscription.streams:
            notice = await self._plan_mission(opened, cursors.get("mission_events"))
            position = opened.positions["mission_events"]
            high.append(mission_cursor(position.sent))
            replay.append(mission_cursor(position.after))
            versions["mission_events"] = str(position.sent)
            if subscription.include_descendants:
                versions["mission_events.descendants"] = "unavailable"
            if notice is not None:
                snapshots.append(notice)
        if "provider_frames" in subscription.streams:
            notice = await self._plan_frames(opened, cursors.get("provider_frames"))
            position = opened.positions["provider_frames"]
            high.append(self._frame_cursor(target, position, position.sent))
            replay.append(self._frame_cursor(target, position, position.after))
            versions["provider_frames"] = f"{position.sent}@{position.generation}"
            if notice is not None:
                snapshots.append(notice)
        if "presence" in subscription.streams:
            opened.positions["presence"] = StreamPosition("presence", 0, 0, 0)
        opened.snapshots = tuple(snapshots)
        opened.ack = SubscribeAck(
            subscription_id=subscription.subscription_id,
            snapshot_ref=self.snapshot_ref(target) if snapshots else None,
            snapshot_versions=versions,
            high_watermarks=tuple(high),
            replay_from=tuple(replay),
            gap=any(notice.reason != "no_cursor" for notice in snapshots),
        )
        return opened

    def snapshot_ref(self, target: ResolvedTarget) -> str:
        base = f"mc://applications/{self.scope.application_id}"
        if target.target.kind == "mission":
            return f"{base}/missions/{target.mission_id}"
        suffix = "inspection" if target.target.kind == "run" else "transcript"
        return f"{base}/runs/{target.run_key}/{suffix}"

    async def _plan_mission(
        self, opened: OpenedStream, cursor: StreamCursor | None
    ) -> SnapshotNotice | None:
        mission_id = opened.target.mission_id
        high = await self._source.mission_high_watermark(mission_id)
        notice: SnapshotNotice | None = None
        if cursor is None:
            start = high
            notice = SnapshotNotice("mission_events", mission_cursor(high), "no_cursor")
        else:
            start = self._parse_mission(cursor, high)
            oldest = await self._source.mission_low_watermark(mission_id)
            if oldest is not None and start < oldest - 1:
                start = high
                notice = SnapshotNotice("mission_events", mission_cursor(high), "cursor_expired")
        opened.positions["mission_events"] = StreamPosition("mission_events", start, high, start)
        return notice

    async def _plan_frames(
        self, opened: OpenedStream, cursor: StreamCursor | None
    ) -> SnapshotNotice | None:
        execution = self._execution(opened.target)
        generation = await self._source.frame_generation(execution)
        if generation is None:
            raise StreamFailure("TARGET_NOT_FOUND", "target not found")
        newest = await self._source.frame_high_watermark(execution, generation)
        notice: SnapshotNotice | None = None
        position = StreamPosition("provider_frames", 0, newest, 0, generation)
        if cursor is None:
            position.after = position.acked = newest
            reason: SnapshotReason | None = "no_cursor"
        else:
            reason = None
            try:
                parsed = parse_frame_cursor(
                    cursor,
                    harness_execution_id=execution,
                    current_generation=generation,
                    high_watermark=newest,
                )
            except StreamCursorError as error:
                if error.code != "STALE_GENERATION" or (
                    cursor.generation is not None and cursor.generation > generation
                ):
                    raise StreamFailure(error.code, error.detail) from None
                # The execution was relaunched: replay the current generation from its start.
                reason = "stale_generation"
            else:
                position.after = position.acked = parsed.arrival_ordinal
        if reason is not None:
            notice = SnapshotNotice(
                "provider_frames",
                self._frame_cursor(opened.target, position, position.after),
                reason,
            )
        opened.positions["provider_frames"] = position
        return notice

    # --- read ------------------------------------------------------------------------

    async def next_page(self, opened: OpenedStream, stream: StreamName, *, limit: int) -> Page:
        if stream == "mission_events":
            return await self._mission_page(opened, limit)
        if stream == "provider_frames":
            return await self._frame_page(opened, limit)
        raise StreamFailure("UNSUPPORTED_FILTER", "presence is not a durable stream")

    async def _mission_page(self, opened: OpenedStream, limit: int) -> Page:
        position = opened.positions["mission_events"]
        high = await self._source.mission_high_watermark(opened.target.mission_id)
        if high <= position.after:
            return Page("mission_events", scanned_to=position.after)
        events = await self._source.mission_events(
            opened.target, after_seq=position.after, upto_seq=high, limit=limit
        )
        scanned = high if len(events) < limit else events[-1].seq
        items = []
        for event in events:
            selected = select(opened.event_filters, event)
            if selected is not None:
                items.append(PageItem(event.seq, self._mission_envelope(selected.envelope)))
        return Page("mission_events", tuple(items), scanned)

    async def _frame_page(self, opened: OpenedStream, limit: int) -> Page:
        position = opened.positions["provider_frames"]
        execution = self._execution(opened.target)
        generation = await self._source.frame_generation(execution)
        if generation is None:
            raise StreamFailure("TARGET_NOT_FOUND", "target not found")
        assert position.generation is not None
        if generation > position.generation:
            restart = StreamPosition("provider_frames", 0, 0, 0, generation)
            return Page(
                "provider_frames",
                resync=ResyncNotice(
                    "provider_frames",
                    "STALE_GENERATION",
                    self._frame_cursor(opened.target, restart, 0),
                ),
            )
        newest = await self._source.frame_high_watermark(execution, generation)
        if newest <= position.after:
            return Page("provider_frames", scanned_to=position.after)
        frames = await self._source.frames(
            execution,
            generation,
            after_ordinal=position.after,
            upto_ordinal=newest,
            limit=limit,
        )
        scanned = newest if len(frames) < limit else frames[-1].arrival_ordinal
        include_children = opened.subscription.include_descendants
        selected = [
            frame
            for frame in frames
            if frame_passes(frame, opened.frame_filters)
            and (include_children or frame.subordinate_ref is None)
        ]
        items = tuple(self._frame_items(opened, selected))
        return Page("provider_frames", items, scanned)

    def apply_resync(self, opened: OpenedStream, notice: ResyncNotice) -> None:
        """A relaunched execution: continue with its current generation from the start."""

        position = opened.positions[notice.stream]
        position.generation = notice.replay_from.generation
        position.after = position.sent = position.acked = 0

    def _frame_items(
        self, opened: OpenedStream, frames: Sequence[ProviderFrame]
    ) -> Iterable[PageItem]:
        detail = opened.subscription.filters.tool_detail
        mission_ref = f"mission:{opened.target.mission_id}"
        index = 0
        while index < len(frames):
            frame = frames[index]
            run = [frame]
            if frame.kind in DELTA_KINDS:
                while index + len(run) < len(frames) and _same_delta(
                    run[-1], frames[index + len(run)]
                ):
                    run.append(frames[index + len(run)])
            index += len(run)
            envelope = frame_envelope(
                run[-1], scope=self.scope, mission_ref=mission_ref, tool_detail=detail
            )
            if len(run) > 1:
                envelope = _coalesce(envelope, run)
            yield PageItem(run[-1].arrival_ordinal, envelope)

    def _mission_envelope(self, event: MissionEventEnvelope) -> StreamEnvelope:
        return StreamEnvelope(
            stream="mission_events",
            event_id=str(event.event_id),
            scope=self.scope,
            mission_ref=f"mission:{event.mission_id}",
            run_ref=f"run:{event.run_id}" if event.run_id is not None else None,
            cursor=mission_cursor(event.seq),
            occurred_at=event.happened_at.isoformat(),
            recorded_at=event.recorded_at.isoformat(),
            kind=event.event_type,
            payload=event.model_dump(mode="json"),
            causation_id=_bounded(event.causation_ref),
        )

    # --- acknowledge -----------------------------------------------------------------

    def acknowledge(
        self, opened: OpenedStream, cursors: Sequence[StreamCursor]
    ) -> dict[StreamName, int]:
        """Monotone acks bounded by what the server sent; returns the acked positions."""

        acked: dict[StreamName, int] = {}
        for cursor in cursors:
            position = opened.positions.get(cursor.stream)
            if position is None or cursor.stream not in DURABLE_STREAMS:
                raise StreamFailure("SCOPE_MISMATCH", "cursor names a stream not subscribed")
            try:
                if cursor.stream == "mission_events":
                    value = parse_mission_cursor(cursor, high_watermark=position.sent)
                else:
                    assert position.generation is not None
                    value = parse_frame_cursor(
                        cursor,
                        harness_execution_id=self._execution(opened.target),
                        current_generation=position.generation,
                        high_watermark=position.sent,
                    ).arrival_ordinal
            except StreamCursorError as error:
                raise StreamFailure(error.code, error.detail) from None
            position.acked = max(position.acked, value)
            acked[cursor.stream] = position.acked
        return acked

    def acked_cursors(self, opened: OpenedStream) -> tuple[StreamCursor, ...]:
        """Where a resubscribe resumes without losing an unacknowledged event."""

        result = []
        for stream in opened.durable_streams:
            position = opened.positions[stream]
            if stream == "mission_events":
                result.append(mission_cursor(position.acked))
            else:
                result.append(self._frame_cursor(opened.target, position, position.acked))
        return tuple(result)

    # --- helpers ---------------------------------------------------------------------

    @staticmethod
    def _execution(target: ResolvedTarget) -> UUID:
        if target.harness_execution_id is None:
            raise StreamFailure("UNSUPPORTED_FILTER", "provider frames need an execution target")
        return target.harness_execution_id

    def _frame_cursor(
        self, target: ResolvedTarget, position: StreamPosition, ordinal: int
    ) -> StreamCursor:
        return StreamCursor(
            stream="provider_frames",
            position=f"{self._execution(target)}:{ordinal}",
            generation=position.generation,
        )

    def _parse_mission(self, cursor: StreamCursor, high: int) -> int:
        try:
            return parse_mission_cursor(cursor, high_watermark=high)
        except StreamCursorError as error:
            raise StreamFailure(error.code, error.detail) from None


def _same_delta(left: ProviderFrame, right: ProviderFrame) -> bool:
    return (
        right.kind == left.kind
        and right.native_turn_ref == left.native_turn_ref
        and right.tool_call_ref == left.tool_call_ref
        and right.subordinate_ref == left.subordinate_ref
    )


def _coalesce(envelope: StreamEnvelope, run: Sequence[ProviderFrame]) -> StreamEnvelope:
    """Display coalescing of consecutive durable deltas: the last frame's cursor, the count,
    and the joined (bounded) excerpts; every frame stays readable from the store."""

    payload: dict[str, Any] = dict(envelope.payload)
    if "body_excerpt" in payload:
        joined = "".join(frame.body_excerpt or "" for frame in run)
        payload["body_excerpt"] = joined[:MAX_COALESCED_EXCERPT]
        payload["body_excerpt_truncated"] = len(joined) > MAX_COALESCED_EXCERPT
    payload["coalesced_from"] = str(run[0].frame_id)
    return envelope.model_copy(update={"payload": payload, "coalesced": len(run)})


__all__ = [
    "DURABLE_STREAMS",
    "FULL_DETAIL_PERMISSION",
    "MissionStreamService",
    "OpenedStream",
    "Page",
    "PageItem",
    "ResyncNotice",
    "SnapshotNotice",
    "StreamFailure",
    "StreamPosition",
    "split_filters",
]
