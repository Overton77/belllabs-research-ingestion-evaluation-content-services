"""Scoped mission stream subscriptions: authorize, plan replay from cursors, read pages.

SPEC-04 "Replay algorithm" over `mc.stream_subscription.v1` and MP-13's per-stream cursors
(`application/frames/streams.py`):

1. The caller is authorized (`workflow_run.read`; `tool_detail="full"` also needs the
   artifact read grant) and the target is resolved under the service's own scope; a target
   of another tenant or application is indistinguishable from an absent one
   (`TARGET_NOT_FOUND`).
2. Each cursor domain reads its committed high-watermark H first: `mission.last_event_seq`
   is written in the same transaction as the events it covers, and provider frame appends
   lock their harness execution row, so every position <= H is committed and visible.
3. A client cursor is checked against H (`CURSOR_AHEAD`, `SCOPE_MISMATCH`, ...) and replay
   covers `(cursor, H]`. Without a cursor the domain starts at H behind a `no_cursor`
   snapshot. A cursor older than retention (`cursor_expired`: a journal low-watermark past
   it, or provider frames that retention deleted after it) also starts at H behind a
   snapshot, and the ack reports `gap`. A live subscription that falls behind retention
   gets `resync_required{CURSOR_EXPIRED}` and continues at H.
4. Live delivery is the same read loop continuing past H: the store is the only source, so
   a lost notification delays an event until the next high-watermark comparison and a
   duplicate notification reads nothing new. Positions only move forward, so a subscription
   never emits an event twice; across reconnects the client dedupes by `event_id`.

Cursor domains ("channels"). Each stream keeps its own cursor domain, and within it one
domain per execution (provider frames) or per member mission (a `chain` target's journals):

- `mission` / `run` / `execution` targets read one journal: channel `mission_events`, plain
  `<seq>` cursors, exactly as before;
- an `execution` target reads one frame domain: channel `provider_frames`, as before;
- `run` and `mission` targets read provider frames of every execution of the run (or of
  every run of the mission), one channel `provider_frames:<execution>` each, discovered as
  executions start (bounded by `MAX_FRAME_EXECUTIONS`); an execution the client has no
  cursor for is replayed from its start when the client resumes with other frame cursors;
- `chain` targets read every member mission's journal, one channel
  `mission_events:<mission>` each, with keyed `<mission>:<seq>` cursors, plus the chain's
  lifecycle and links (reported through `lineage`). Provider frames of a chain are
  subscribed per member mission or run.

Mission events go through the public alias projection (`aliases.select`), so filters use
the documented vocabulary (`run.completed`, ...) and receive at most one envelope per
canonical event. Socket.IO arrival is at most once; recovery is the client's acknowledged
cursor, never an in-memory history.

Provider frame envelopes carry the common lifecycle projection (`frames/projections.py`):
normalized facts beside the native kind, and usage attribution so recursive views never
double count. With `include_descendants`, a `LineageTracker` folds every scanned frame
(delivered or filtered) in arrival order, seeded from each execution's first frame, so each
child frame's `subordinate_ref` reports the visibility actually persisted so far and lineage
changes are reported as they happen. Mission events with `include_descendants` list the
linked missions this scope can see (`lifecycle_only`: their journals are separate cursor
domains and are never merged into this one).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Final, Literal
from uuid import UUID

from pydantic import ValidationError

from mission_control.application.frames.artifact_bodies import ARTIFACT_READ_PERMISSION
from mission_control.application.frames.lineage import (
    SubordinateNode,
    UsageRule,
    linked_mission,
    usage_rule,
)
from mission_control.application.frames.lineage_stream import LineageTracker, Transition
from mission_control.application.frames.projections import projection
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
    ChainState,
    LinkedMission,
    ResolvedTarget,
    StreamSource,
    StreamTargetNotFound,
)
from mission_control.application.subscriptions.aliases import select
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.frames.contracts import FrameKind, LaneProfile, ProviderFrame
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
    StreamTarget,
    SubscribeAck,
)

FULL_DETAIL_PERMISSION: Final = ARTIFACT_READ_PERMISSION
DURABLE_STREAMS: Final[tuple[StreamName, ...]] = ("mission_events", "provider_frames")
MAX_COALESCED_EXCERPT: Final = 8_192
# Lineage seeding reads an execution's frames before the replay start once per subscription;
# past this bound the tracker is reported `partial` and its visibility is a lower bound.
MAX_LINEAGE_SEED_FRAMES: Final = 50_000
LINEAGE_SEED_PAGE: Final = 500
# A run or mission frame subscription follows at most this many executions.
MAX_FRAME_EXECUTIONS: Final = 64
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
    """One cursor domain. `after`: last position read past; `sent`: the bound for client
    acks. `key` is the execution (frames) or mission (events) the domain reads; `keyed`
    mission cursors name it (`<mission>:<seq>`)."""

    stream: StreamName
    after: int
    sent: int
    acked: int
    generation: int | None = None
    key: UUID | None = None
    channel: str = ""
    keyed: bool = False

    def __post_init__(self) -> None:
        if not self.channel:
            self.channel = self.stream


SnapshotReason = Literal["no_cursor", "cursor_expired", "stale_generation"]
LineageCoverage = Literal["complete", "partial"]


@dataclass(frozen=True)
class SnapshotNotice:
    stream: StreamName
    covered: StreamCursor
    reason: SnapshotReason


@dataclass(frozen=True)
class ResyncNotice:
    """Continue one domain elsewhere: a relaunched execution's new generation from its
    start (`STALE_GENERATION`) or the high-watermark past retention (`CURSOR_EXPIRED`)."""

    stream: StreamName
    code: StreamErrorCode
    replay_from: StreamCursor
    channel: str = ""
    restart_at: int = 0


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
    lineage: tuple[SubordinateNode, ...] = ()
    """Subordinate nodes this page changed (started, widened, resolved, ended, released)."""
    channel: str = ""
    chain: dict[str, Any] | None = None
    """The chain's lifecycle when this page observed a change of it."""


@dataclass
class LineageState:
    """Per-subscription lineage: provider subagents of the executions, linked missions."""

    tracker: LineageTracker | None = None
    observed_through: dict[str, int] = field(default_factory=dict)
    pending: dict[tuple[str, int], tuple[Transition, ...]] = field(default_factory=dict)
    linked: dict[str, SubordinateNode] = field(default_factory=dict)
    linked_available: bool = False

    @property
    def coverage(self) -> LineageCoverage:
        return "complete" if self.tracker is None or self.tracker.complete else "partial"

    def nodes(self) -> tuple[SubordinateNode, ...]:
        frames = self.tracker.nodes() if self.tracker is not None else ()
        return (*frames, *self.linked.values())


@dataclass
class OpenedStream:
    subscription: StreamSubscription
    target: ResolvedTarget
    ack: SubscribeAck
    event_filters: SubscriptionFilters
    frame_filters: StreamFilters
    positions: dict[str, StreamPosition] = field(default_factory=dict)
    snapshots: tuple[SnapshotNotice, ...] = ()
    lineage: LineageState = field(default_factory=LineageState)
    discovers_frames: bool = False
    frames_truncated: bool = False
    chain: ChainState | None = None

    @property
    def durable_streams(self) -> tuple[StreamName, ...]:
        return tuple(s for s in self.subscription.streams if s in DURABLE_STREAMS)

    def channels(self) -> tuple[str, ...]:
        """Durable cursor domains in subscription order (journals first, then frames)."""

        return tuple(
            channel
            for stream in self.durable_streams
            for channel, position in self.positions.items()
            if position.stream == stream
        )

    def frame_executions(self) -> tuple[UUID, ...]:
        return tuple(
            position.key
            for position in self.positions.values()
            if position.stream == "provider_frames" and position.key is not None
        )


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


def _cursor_error(error: StreamCursorError) -> StreamFailure:
    return StreamFailure(error.code, error.detail)


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
        if subscription.target.kind == "chain" and "provider_frames" in subscription.streams:
            raise StreamFailure(
                "UNSUPPORTED_FILTER", "provider frames are subscribed per mission, run or execution"
            )
        event_filters, frame_filters = split_filters(subscription.filters)
        try:
            target = await self._source.resolve(subscription.target)
        except StreamTargetNotFound:
            raise StreamFailure("TARGET_NOT_FOUND", "target not found") from None
        opened = OpenedStream(
            subscription=subscription,
            target=target,
            ack=SubscribeAck(subscription_id=subscription.subscription_id),
            event_filters=event_filters,
            frame_filters=frame_filters,
        )
        snapshots: list[SnapshotNotice] = []
        versions: dict[str, str] = {}
        if target.chain_id is not None:
            try:
                opened.chain = await self._source.chain(target.chain_id)
            except StreamTargetNotFound:
                raise StreamFailure("TARGET_NOT_FOUND", "target not found") from None
            self._update_links(opened, opened.chain.links)
            versions["chain.lifecycle"] = opened.chain.lifecycle
            versions["chain.version"] = str(opened.chain.version)
        if "mission_events" in subscription.streams:
            snapshots.extend(await self._open_journals(opened, versions))
        if "provider_frames" in subscription.streams:
            snapshots.extend(await self._open_frames(opened, versions))
        if "presence" in subscription.streams:
            opened.positions["presence"] = StreamPosition("presence", 0, 0, 0)
        opened.snapshots = tuple(snapshots)
        channels = opened.channels()
        opened.ack = SubscribeAck(
            subscription_id=subscription.subscription_id,
            snapshot_ref=self.snapshot_ref(target) if snapshots else None,
            snapshot_versions=versions,
            high_watermarks=tuple(
                self._cursor(opened, c, opened.positions[c].sent) for c in channels
            ),
            replay_from=tuple(self._cursor(opened, c, opened.positions[c].after) for c in channels),
            gap=any(notice.reason != "no_cursor" for notice in snapshots),
        )
        return opened

    async def _open_journals(
        self, opened: OpenedStream, versions: dict[str, str]
    ) -> list[SnapshotNotice]:
        subscription, target = opened.subscription, opened.target
        cursors = [c for c in subscription.cursors if c.stream == "mission_events"]
        notices: list[SnapshotNotice] = []
        if target.chain_id is not None:
            by_mission: dict[UUID, StreamCursor] = {}
            for cursor in cursors:
                mission = _uuid(cursor.key)
                if mission is None or mission not in target.members:
                    raise StreamFailure("SCOPE_MISMATCH", "chain cursors name a member mission")
                by_mission[mission] = cursor
            for mission in target.members:
                position = StreamPosition(
                    "mission_events",
                    0,
                    0,
                    0,
                    key=mission,
                    channel=f"mission_events:{mission}",
                    keyed=True,
                )
                notice = await self._plan_mission(opened, position, by_mission.get(mission))
                versions[position.channel] = str(position.sent)
                if notice is not None:
                    notices.append(notice)
            versions["mission_events.members"] = str(len(target.members))
        else:
            if len(cursors) > 1:
                raise StreamFailure("SCOPE_MISMATCH", "one mission journal takes one cursor")
            position = StreamPosition("mission_events", 0, 0, 0, key=target.mission_id)
            notice = await self._plan_mission(opened, position, cursors[0] if cursors else None)
            versions["mission_events"] = str(position.sent)
            if notice is not None:
                notices.append(notice)
        if subscription.include_descendants and target.chain_id is None:
            await self._refresh_linked(opened)
            # Linked missions are listed by lifecycle; their journals stay separate cursor
            # domains (subscribe each linked mission, or the chain, to read its events).
            versions["mission_events.descendants"] = (
                "lifecycle_only" if opened.lineage.linked_available else "unavailable"
            )
        elif target.chain_id is not None:
            versions["mission_events.descendants"] = "lifecycle_only"
        return notices

    async def _open_frames(
        self, opened: OpenedStream, versions: dict[str, str]
    ) -> list[SnapshotNotice]:
        subscription, target = opened.subscription, opened.target
        cursors = [c for c in subscription.cursors if c.stream == "provider_frames"]
        notices: list[SnapshotNotice] = []
        if subscription.include_descendants:
            opened.lineage.tracker = LineageTracker()
        if target.harness_execution_id is not None:
            # One execution: the domain is named `provider_frames`, exactly as before.
            execution = target.harness_execution_id
            if any(cursor.key != str(execution) for cursor in cursors[1:]):
                raise StreamFailure("SCOPE_MISMATCH", "cursor names another execution")
            position = StreamPosition("provider_frames", 0, 0, 0, key=execution)
            notice = await self._plan_frames(opened, position, cursors[0] if cursors else None)
            versions["provider_frames"] = f"{position.sent}@{position.generation}"
            if notice is not None:
                notices.append(notice)
        else:
            opened.discovers_frames = True
            refs = await self._source.executions(target, limit=MAX_FRAME_EXECUTIONS + 1)
            opened.frames_truncated = len(refs) > MAX_FRAME_EXECUTIONS
            refs = refs[:MAX_FRAME_EXECUTIONS]
            known = {str(ref.harness_execution_id) for ref in refs}
            by_execution = {cursor.key: cursor for cursor in cursors}
            if set(by_execution) - known:
                # A cursor of an execution outside this run or mission (or scope).
                raise StreamFailure("SCOPE_MISMATCH", "cursor names another execution")
            resumed = bool(by_execution)
            for ref in refs:
                execution = ref.harness_execution_id
                position = StreamPosition(
                    "provider_frames",
                    0,
                    0,
                    0,
                    key=execution,
                    channel=f"provider_frames:{execution}",
                )
                cursor = by_execution.get(str(execution))
                notice = await self._plan_frames(
                    opened, position, cursor, from_start=resumed and cursor is None
                )
                versions[position.channel] = f"{position.sent}@{position.generation}"
                if notice is not None:
                    notices.append(notice)
            versions["provider_frames.executions"] = str(len(refs))
            if opened.frames_truncated:
                versions["provider_frames.truncated"] = "true"
        if subscription.include_descendants:
            for channel in [c for c in opened.channels() if c.startswith("provider_frames")]:
                await self._seed_lineage(opened, opened.positions[channel])
            versions["provider_frames.lineage"] = opened.lineage.coverage
        return notices

    async def _seed_lineage(self, opened: OpenedStream, position: StreamPosition) -> None:
        """Fold an execution's frames up to the replay start, from its first frame."""

        tracker = opened.lineage.tracker
        assert tracker is not None and position.generation is not None
        assert position.key is not None
        after = 0
        while after < position.after:
            if after >= MAX_LINEAGE_SEED_FRAMES:
                tracker.complete = False
                break
            frames = await self._source.frames(
                position.key,
                position.generation,
                after_ordinal=after,
                upto_ordinal=position.after,
                limit=LINEAGE_SEED_PAGE,
            )
            if not frames:
                break
            for frame in frames:
                tracker.observe(frame)
            after = frames[-1].arrival_ordinal
        opened.lineage.observed_through[position.channel] = position.after

    async def _refresh_linked(self, opened: OpenedStream) -> tuple[SubordinateNode, ...]:
        """Linked missions released from the target's mission; returns the changed ones."""

        try:
            links = await self._source.linked_missions(opened.target.mission_id)
        except NotImplementedError:
            return ()
        opened.lineage.linked_available = True
        return self._update_links(opened, links)

    @staticmethod
    def _update_links(
        opened: OpenedStream, links: Iterable[LinkedMission]
    ) -> tuple[SubordinateNode, ...]:
        changed: list[SubordinateNode] = []
        for link in links:
            node = _linked_node(link)
            key = str(link.link_id)
            if opened.lineage.linked.get(key) != node:
                opened.lineage.linked[key] = node
                changed.append(node)
        return tuple(changed)

    def snapshot_ref(self, target: ResolvedTarget) -> str:
        base = f"mc://applications/{self.scope.application_id}"
        if target.chain_id is not None:
            return f"{base}/chains/{target.chain_id}"
        if target.target.kind == "mission":
            return f"{base}/missions/{target.mission_id}"
        suffix = "inspection" if target.target.kind == "run" else "transcript"
        return f"{base}/runs/{target.run_key}/{suffix}"

    def chain_body(self, opened: OpenedStream) -> dict[str, Any] | None:
        chain = opened.chain
        if chain is None:
            return None
        return {
            "chain_id": str(chain.chain_id),
            "chain_key": chain.chain_key,
            "lifecycle": chain.lifecycle,
            "phase": chain.phase,
            "terminal_outcome": chain.terminal_outcome,
            "version": chain.version,
            "members": [f"mission:{member}" for member in opened.target.members],
            "links": [
                {
                    "link_key": link.link_key,
                    "from_mission_ref": f"mission:{link.from_mission_id}",
                    "to_mission_ref": f"mission:{link.to_mission_id}",
                    "state": link.state,
                    "released_run_ref": (
                        None if link.released_run_key is None else f"run:{link.released_run_key}"
                    ),
                }
                for link in chain.links
            ],
        }

    async def _plan_mission(
        self, opened: OpenedStream, position: StreamPosition, cursor: StreamCursor | None
    ) -> SnapshotNotice | None:
        assert position.key is not None
        mission_id = position.key
        high = await self._source.mission_high_watermark(mission_id)
        notice: SnapshotNotice | None = None
        if cursor is None:
            start = high
            notice = SnapshotNotice("mission_events", self._cursor_at(position, high), "no_cursor")
        else:
            try:
                start = parse_mission_cursor(cursor, high_watermark=high, mission_id=mission_id)
            except StreamCursorError as error:
                raise _cursor_error(error) from None
            oldest = await self._source.mission_low_watermark(mission_id)
            if oldest is not None and start < oldest - 1:
                start = high
                notice = SnapshotNotice(
                    "mission_events", self._cursor_at(position, high), "cursor_expired"
                )
        position.after = position.acked = start
        position.sent = high
        opened.positions[position.channel] = position
        return notice

    async def _plan_frames(
        self,
        opened: OpenedStream,
        position: StreamPosition,
        cursor: StreamCursor | None,
        *,
        from_start: bool = False,
    ) -> SnapshotNotice | None:
        assert position.key is not None
        execution = position.key
        generation = await self._source.frame_generation(execution)
        if generation is None:
            raise StreamFailure("TARGET_NOT_FOUND", "target not found")
        newest = await self._source.frame_high_watermark(execution, generation)
        position.generation, position.sent = generation, newest
        reason: SnapshotReason | None = None
        if from_start:
            # New to a resuming client: everything this execution recorded is after it.
            position.after = position.acked = 0
        elif cursor is None:
            position.after = position.acked = newest
            reason = "no_cursor"
        else:
            try:
                parsed = parse_frame_cursor(
                    cursor,
                    harness_execution_id=execution,
                    current_generation=generation,
                    high_watermark=newest,
                )
            except StreamCursorError as error:
                if error.code == "STALE_GENERATION" and not (
                    cursor.generation is not None and cursor.generation > generation
                ):
                    # The execution was relaunched: replay the current generation from start.
                    reason = "stale_generation"
                elif (
                    error.code == "CURSOR_AHEAD"
                    and cursor.generation == generation
                    and await self._past_retention(execution, generation, _ordinal(cursor))
                ):
                    # Retention deleted the frames up to and past the cursor.
                    position.after = position.acked = newest
                    reason = "cursor_expired"
                else:
                    raise _cursor_error(error) from None
            else:
                ordinal = parsed.arrival_ordinal
                if await self._frames_expired(execution, generation, ordinal):
                    position.after = position.acked = newest
                    reason = "cursor_expired"
                else:
                    position.after = position.acked = ordinal
        opened.positions[position.channel] = position
        if reason is None:
            return None
        return SnapshotNotice("provider_frames", self._cursor_at(position, position.after), reason)

    async def _frames_expired(self, execution: UUID, generation: int, ordinal: int) -> bool:
        """Did retention delete frames this cursor still needs? Conservative: an ordinal gap
        after an old execution's cursor is treated as expired (a resync, never a loss)."""

        probe = await self._source.frame_retention(execution, generation, after_ordinal=ordinal)
        if ordinal > 0 and not probe.cursor_present:
            return True
        return (
            probe.horizon_passed
            and probe.next_ordinal is not None
            and probe.next_ordinal > ordinal + 1
        )

    async def _past_retention(self, execution: UUID, generation: int, ordinal: int) -> bool:
        probe = await self._source.frame_retention(execution, generation, after_ordinal=ordinal)
        return probe.horizon_passed and not probe.cursor_present

    async def refresh_channels(self, opened: OpenedStream) -> tuple[UUID, ...]:
        """Executions a run or mission frame subscription has not seen yet: each starts at
        its first frame (all of it is after the subscription began)."""

        if not opened.discovers_frames:
            return ()
        known = set(opened.frame_executions())
        if len(known) >= MAX_FRAME_EXECUTIONS:
            return ()
        refs = await self._source.executions(opened.target, limit=MAX_FRAME_EXECUTIONS + 1)
        added: list[UUID] = []
        for ref in refs:
            if ref.harness_execution_id in known:
                continue
            if len(known) + len(added) >= MAX_FRAME_EXECUTIONS:
                opened.frames_truncated = True
                break
            execution = ref.harness_execution_id
            position = StreamPosition(
                "provider_frames",
                0,
                0,
                0,
                generation=ref.generation,
                key=execution,
                channel=f"provider_frames:{execution}",
            )
            opened.positions[position.channel] = position
            if opened.lineage.tracker is not None:
                opened.lineage.observed_through[position.channel] = 0
            added.append(execution)
        return tuple(added)

    # --- read ------------------------------------------------------------------------

    async def next_page(self, opened: OpenedStream, channel: str, *, limit: int) -> Page:
        position = opened.positions.get(channel)
        if position is None or position.stream not in DURABLE_STREAMS:
            raise StreamFailure("UNSUPPORTED_FILTER", "presence is not a durable stream")
        if position.stream == "mission_events":
            return await self._mission_page(opened, position, limit)
        return await self._frame_page(opened, position, limit)

    async def _mission_page(
        self, opened: OpenedStream, position: StreamPosition, limit: int
    ) -> Page:
        assert position.key is not None
        channel = position.channel
        high = await self._source.mission_high_watermark(position.key)
        if high <= position.after:
            return Page("mission_events", scanned_to=position.after, channel=channel)
        reader = (
            opened.target
            if opened.target.chain_id is None
            else ResolvedTarget(StreamTarget(kind="mission", id=str(position.key)), position.key)
        )
        events = await self._source.mission_events(
            reader, after_seq=position.after, upto_seq=high, limit=limit
        )
        scanned = high if len(events) < limit else events[-1].seq
        items = []
        for event in events:
            selected = select(opened.event_filters, event)
            if selected is not None:
                envelope = self._mission_envelope(selected.envelope, position)
                items.append(PageItem(event.seq, envelope))
        changed: tuple[SubordinateNode, ...] = ()
        chain: dict[str, Any] | None = None
        if events and opened.chain is not None:
            changed, chain = await self._refresh_chain(opened)
        elif opened.subscription.include_descendants and events:
            changed = await self._refresh_linked(opened)
        return Page(
            "mission_events", tuple(items), scanned, lineage=changed, channel=channel, chain=chain
        )

    async def _refresh_chain(
        self, opened: OpenedStream
    ) -> tuple[tuple[SubordinateNode, ...], dict[str, Any] | None]:
        assert opened.chain is not None
        try:
            fresh = await self._source.chain(opened.chain.chain_id)
        except StreamTargetNotFound:
            return (), None
        if fresh.version == opened.chain.version and fresh.links == opened.chain.links:
            return (), None
        opened.chain = fresh
        return self._update_links(opened, fresh.links), self.chain_body(opened)

    async def _frame_page(self, opened: OpenedStream, position: StreamPosition, limit: int) -> Page:
        assert position.key is not None and position.generation is not None
        channel, execution = position.channel, position.key
        generation = await self._source.frame_generation(execution)
        if generation is None:
            raise StreamFailure("TARGET_NOT_FOUND", "target not found")
        if generation > position.generation:
            replay_from = self._frame_cursor(execution, 0, generation)
            return Page(
                "provider_frames",
                resync=ResyncNotice(
                    "provider_frames", "STALE_GENERATION", replay_from, channel, restart_at=0
                ),
                channel=channel,
            )
        newest = await self._source.frame_high_watermark(execution, generation)
        if newest <= position.after:
            return Page("provider_frames", scanned_to=position.after, channel=channel)
        frames = await self._source.frames(
            execution,
            generation,
            after_ordinal=position.after,
            upto_ordinal=newest,
            limit=limit,
        )
        if frames and frames[0].arrival_ordinal > position.after + 1:
            if await self._frames_expired(execution, generation, position.after):
                # A lagging subscription fell behind retention: continue at H, say so.
                replay_from = self._frame_cursor(execution, newest, generation)
                return Page(
                    "provider_frames",
                    resync=ResyncNotice(
                        "provider_frames", "CURSOR_EXPIRED", replay_from, channel, newest
                    ),
                    channel=channel,
                )
        scanned = newest if len(frames) < limit else frames[-1].arrival_ordinal
        include_children = opened.subscription.include_descendants
        changed = self._observe_lineage(opened, position, frames, scanned)
        selected = [
            frame
            for frame in frames
            if frame_passes(frame, opened.frame_filters)
            and (include_children or frame.subordinate_ref is None)
        ]
        items = tuple(self._frame_items(opened, channel, selected))
        return Page("provider_frames", items, scanned, lineage=changed, channel=channel)

    def _observe_lineage(
        self,
        opened: OpenedStream,
        position: StreamPosition,
        frames: Sequence[ProviderFrame],
        scanned: int,
    ) -> tuple[SubordinateNode, ...]:
        """Fold each frame once, even when a blocked window makes the pump reread a page;
        transitions are kept until the frame's envelope has been emitted."""

        state = opened.lineage
        channel = position.channel
        for key in [key for key in state.pending if key[0] == channel and key[1] <= position.after]:
            del state.pending[key]
        if state.tracker is None:
            return ()
        changed: dict[tuple[str, str, str], SubordinateNode] = {}
        observed = state.observed_through.get(channel, 0)
        for frame in frames:
            if frame.arrival_ordinal <= observed:
                continue
            change = state.tracker.observe(frame)
            observed = frame.arrival_ordinal
            if change.transitions:
                state.pending[(channel, frame.arrival_ordinal)] = change.transitions
            for node in change.nodes:
                changed[node.identity] = node
        state.observed_through[channel] = max(observed, scanned)
        return tuple(changed.values())

    def apply_resync(self, opened: OpenedStream, notice: ResyncNotice) -> None:
        """Continue a domain where the notice says: a relaunched execution's current
        generation from its start, or the high-watermark past retention."""

        channel = notice.channel or notice.stream
        position = opened.positions[channel]
        position.generation = notice.replay_from.generation
        position.after = position.sent = position.acked = notice.restart_at
        state = opened.lineage
        if position.stream == "provider_frames" and state.tracker is not None:
            for key in [key for key in state.pending if key[0] == channel]:
                del state.pending[key]
            state.observed_through[channel] = notice.restart_at
            if notice.restart_at:
                # Frames were skipped: lineage visibility is now a lower bound.
                state.tracker.complete = False

    def lineage_nodes(self, opened: OpenedStream) -> tuple[SubordinateNode, ...]:
        return opened.lineage.nodes()

    def _frame_items(
        self, opened: OpenedStream, channel: str, frames: Sequence[ProviderFrame]
    ) -> Iterable[PageItem]:
        detail = opened.subscription.filters.tool_detail
        mission_ref = f"mission:{opened.target.mission_id}"
        tracker = opened.lineage.tracker
        subordinates = tracker.index() if tracker is not None else None
        pending = opened.lineage.pending
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
            transitions = tuple(
                transition
                for item in run
                for transition in pending.get((channel, item.arrival_ordinal), ())
            )
            envelope = frame_envelope(
                run[-1],
                scope=self.scope,
                mission_ref=mission_ref,
                tool_detail=detail,
                subordinates=subordinates,
                annotations=projection(run[-1], transitions),
            )
            if len(run) > 1:
                envelope = _coalesce(envelope, run)
            yield PageItem(run[-1].arrival_ordinal, envelope)

    def _mission_envelope(
        self, event: MissionEventEnvelope, position: StreamPosition
    ) -> StreamEnvelope:
        return StreamEnvelope(
            stream="mission_events",
            event_id=str(event.event_id),
            scope=self.scope,
            mission_ref=f"mission:{event.mission_id}",
            run_ref=f"run:{event.run_id}" if event.run_id is not None else None,
            cursor=self._cursor_at(position, event.seq),
            occurred_at=event.happened_at.isoformat(),
            recorded_at=event.recorded_at.isoformat(),
            kind=event.event_type,
            payload=event.model_dump(mode="json"),
            causation_id=_bounded(event.causation_ref),
        )

    # --- acknowledge -----------------------------------------------------------------

    def _position_for(self, opened: OpenedStream, cursor: StreamCursor) -> StreamPosition:
        if cursor.stream not in DURABLE_STREAMS:
            raise StreamFailure("SCOPE_MISMATCH", "cursor names a stream not subscribed")
        candidates = [p for p in opened.positions.values() if p.stream == cursor.stream]
        key = cursor.key
        for position in candidates:
            if key and str(position.key) == key:
                return position
        if not key and cursor.stream == "mission_events":
            plain = [p for p in candidates if not p.keyed]
            if len(plain) == 1:
                return plain[0]
        if cursor.stream == "provider_frames" and len(candidates) == 1:
            return candidates[0]  # parsing reports the foreign execution
        raise StreamFailure("SCOPE_MISMATCH", "cursor names a stream not subscribed")

    def acknowledge(self, opened: OpenedStream, cursors: Sequence[StreamCursor]) -> dict[str, int]:
        """Monotone acks bounded by what the server sent; returns the acked positions by
        cursor domain (`mission_events`, `provider_frames`, or `<stream>:<key>`)."""

        acked: dict[str, int] = {}
        for cursor in cursors:
            position = self._position_for(opened, cursor)
            try:
                if position.stream == "mission_events":
                    value = parse_mission_cursor(
                        cursor, high_watermark=position.sent, mission_id=position.key
                    )
                else:
                    assert position.generation is not None and position.key is not None
                    value = parse_frame_cursor(
                        cursor,
                        harness_execution_id=position.key,
                        current_generation=position.generation,
                        high_watermark=position.sent,
                    ).arrival_ordinal
            except StreamCursorError as error:
                raise _cursor_error(error) from None
            position.acked = max(position.acked, value)
            acked[position.channel] = position.acked
        return acked

    def acked_cursors(self, opened: OpenedStream) -> tuple[StreamCursor, ...]:
        """Where a resubscribe resumes without losing an unacknowledged event."""

        return tuple(
            self._cursor(opened, channel, opened.positions[channel].acked)
            for channel in opened.channels()
        )

    # --- helpers ---------------------------------------------------------------------

    def _cursor(self, opened: OpenedStream, channel: str, value: int) -> StreamCursor:
        return self._cursor_at(opened.positions[channel], value)

    def _cursor_at(self, position: StreamPosition, value: int) -> StreamCursor:
        if position.stream == "mission_events":
            return mission_cursor(value, position.key if position.keyed else None)
        assert position.key is not None
        return self._frame_cursor(position.key, value, position.generation)

    @staticmethod
    def _frame_cursor(execution: UUID, ordinal: int, generation: int | None) -> StreamCursor:
        return StreamCursor(
            stream="provider_frames", position=f"{execution}:{ordinal}", generation=generation
        )


def _uuid(value: str) -> UUID | None:
    try:
        return UUID(value) if value else None
    except ValueError:
        return None


def _ordinal(cursor: StreamCursor) -> int:
    try:
        return int(cursor.position.rpartition(":")[2])
    except ValueError:
        return -1


def _linked_node(link: LinkedMission) -> SubordinateNode:
    return linked_mission(
        link_id=str(link.link_id),
        from_mission_id=str(link.from_mission_id),
        to_mission_id=str(link.to_mission_id),
        released_run_id=link.released_run_key,
    )


def lineage_body(node: SubordinateNode) -> dict[str, Any]:
    """One subordinate as the `lineage` server event shows it (refs, never child content)."""

    rule: UsageRule | None = None
    if node.ref.kind != "provider_subagent":
        # The lane does not change the rule of Agent Server children or linked missions.
        rule = usage_rule(LaneProfile.DEEP_AGENTS, node.ref.kind)
    elif node.lane is not None:
        rule = usage_rule(node.lane, "provider_subagent")
    return {
        "ref": node.ref.model_dump(mode="json"),
        "resolved": node.resolved,
        "lifecycle": node.lifecycle,
        "frame_count": node.frame_count,
        "lane": None if node.lane is None else node.lane.value,
        "usage": None if rule is None else {"tokens": rule.tokens, "cost": rule.cost},
    }


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
    if isinstance(payload.get("normalized"), list):
        # A coalesced run carries every normalized fact of its frames (deltas have none of
        # their own; a child's first frame may still be `subordinate.started`).
        payload["normalized"] = list(dict.fromkeys(payload["normalized"]))
    return envelope.model_copy(update={"payload": payload, "coalesced": len(run)})


__all__ = [
    "DURABLE_STREAMS",
    "FULL_DETAIL_PERMISSION",
    "MAX_FRAME_EXECUTIONS",
    "LineageState",
    "MissionStreamService",
    "OpenedStream",
    "Page",
    "PageItem",
    "ResyncNotice",
    "SnapshotNotice",
    "StreamFailure",
    "StreamPosition",
    "lineage_body",
    "split_filters",
]
