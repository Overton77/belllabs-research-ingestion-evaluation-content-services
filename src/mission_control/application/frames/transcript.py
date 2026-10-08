"""Transcript materialization: mission events + provider frames + artifacts (SPEC-03, C3).

`materialize` is a streaming-shaped merge of three ordered sources for one run:

1. mission events by `seq` (canonical; `artifact.*` events are the artifact source,
   enriched with digest and media type from their payload; `human_task.*` render as
   `human`, commands and lifecycle results as `mission_control`);
2. provider frames by arrival order, each placed under its governing event (the latest
   mission event recorded at or before the frame was observed), never canonical;
3. `frame_expired` placeholders for events whose cited frame has left the Native Event
   Store under retention, carrying the digest the event recorded.

Entries carry an opaque cursor that increases strictly within the run; `since` resumes
after a cursor, and a cursor older than retention simply yields canonical entries plus
placeholders. The Transcript is a view, never persisted (the search projection is C4).
"""

from __future__ import annotations

import bisect
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from mission_control.application.frames.sink import FrameReader
from mission_control.domain.frames.body import frame_body_object, redact_text
from mission_control.domain.frames.contracts import (
    NATIVE_EVENT_REF_PREFIX,
    FrameKind,
    ProviderFrame,
    frame_id_from_native_event_ref,
)
from mission_control.domain.frames.transcript import (
    EVENT_TAG,
    CursorExpired,
    TranscriptCursor,
    TranscriptEntry,
    TranscriptPage,
    TranscriptQuery,
    TranscriptRefs,
    TranscriptRole,
    TranscriptSource,
)
from mission_control.domain.frames.usage import UsageReport, usage_report
from mission_control.domain.policies.contracts import ActorContext

READ_PERMISSION = "workflow_run.read"
EXCERPT_CHARS = 2_048
MAX_EVENTS = 20_000
MAX_FRAMES = 50_000

_AGENT_FRAMES = frozenset({FrameKind.MESSAGE, FrameKind.MESSAGE_DELTA, FrameKind.THINKING_DELTA})
_TOOL_FRAMES = frozenset(
    {
        FrameKind.TOOL_CALL_STARTED,
        FrameKind.TOOL_CALL_DELTA,
        FrameKind.TOOL_CALL_COMPLETED,
        FrameKind.TOOL_CALL_FAILED,
    }
)
_HUMAN_FRAMES = frozenset({FrameKind.APPROVAL_REQUESTED, FrameKind.APPROVAL_RESOLVED})


class TranscriptDenied(PermissionError):
    code = "unauthorized"


class TranscriptRunNotFound(LookupError):
    code = "resource_not_found"


class FullBodyUnavailable(RuntimeError):
    """`--full` needs an artifact read path that checks the caller's grant; none is wired."""

    code = "full_body_unavailable"


class ArtifactBodyReader(Protocol):
    """Reads a referenced body artifact under the caller's artifact read grant."""

    async def read(self, request_scope: str, actor: ActorContext, artifact_ref: str) -> str: ...


@dataclass(frozen=True)
class MissionEventRecord:
    """One persisted mission event of the run (the ledger's `mission_event` row)."""

    seq: int
    event_id: str
    event_type: str
    recorded_at: datetime
    actor_ref: str
    payload: Mapping[str, Any]


class MissionEventReader(Protocol):
    async def run_identity(self, request_scope: str, run_key: str) -> UUID | None: ...

    async def events_for_run(
        self, request_scope: str, run_key: str, *, limit: int = MAX_EVENTS
    ) -> tuple[MissionEventRecord, ...]: ...


@dataclass(frozen=True)
class SessionSummary:
    """Per harness execution summary for inspection (F6 consumes `summarize_sessions`)."""

    harness_execution_id: str
    lane_profile: str
    generation: int
    native_session_refs: tuple[str, ...]
    turns_closed: int
    tool_calls: int
    last_frame_kind: str | None
    last_observed_at: datetime | None
    run_result_status: str | None
    usage: UsageReport | None


@dataclass
class _Placed:
    cursor: TranscriptCursor
    entry: TranscriptEntry


@dataclass(frozen=True)
class _EventView:
    record: MissionEventRecord
    payload: Mapping[str, Any]
    execution: Mapping[str, Any] = field(default_factory=dict)


def _excerpt(value: Any, secret_values: Sequence[str]) -> str | None:
    if value is None or value == {} or value == "":
        return None
    text = (
        value
        if isinstance(value, str)
        else json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
        )
    )
    return redact_text(text[:EXCERPT_CHARS], secret_values)[0]


def _str(value: Any) -> str | None:
    return str(value) if isinstance(value, str | int) and str(value) else None


def _event_source_role(event_type: str) -> tuple[TranscriptSource, TranscriptRole]:
    family = event_type.split(".", 1)[0]
    if family == "artifact":
        return "artifact", "mission_control"
    if family == "human_task":
        return "human", "human"
    if family in {"command", "workflow_run"}:
        return "command", "mission_control"
    if family == "tool_call":
        return "mission_event", "tool"
    if family == "session":
        return "mission_event", "agent"
    return "mission_event", "mission_control"


def _event_title(event_type: str, payload: Mapping[str, Any]) -> str:
    parts = [event_type]
    if event_type == "tool_call.completed":
        parts.append(f"{payload.get('name') or payload.get('tool_call_ref') or 'tool'}")
        parts.append(str(payload.get("status") or ""))
    elif event_type in {"session.turn_started", "session.turn_completed"}:
        parts.append(f"turn {payload.get('turn_ordinal')}")
    elif event_type == "attempt.completed":
        parts.append(str(payload.get("outcome")))
        if payload.get("failure_class"):
            parts.append(f"({payload['failure_class']})")
    elif event_type.startswith("activation."):
        parts.append(str(payload.get("phase") or payload.get("lifecycle") or ""))
    elif event_type.startswith("workflow_run.") and (
        payload.get("prior_phase") or payload.get("resulting_phase")
    ):
        parts.append(
            f"{payload.get('prior_phase') or ''} -> {payload.get('resulting_phase') or ''}"
        )
    title = " ".join(part for part in parts if part and part != "None")
    return title[:512]


def _frame_role(kind: FrameKind) -> TranscriptRole:
    if kind in _AGENT_FRAMES:
        return "agent"
    if kind in _TOOL_FRAMES:
        return "tool"
    if kind in _HUMAN_FRAMES:
        return "human"
    return "system"


def _frame_title(frame: ProviderFrame, body: Mapping[str, Any]) -> str:
    parts = [frame.kind.value]
    name = body.get("name") or body.get("tool_name")
    if isinstance(name, str) and name:
        parts.append(name)
    if frame.tool_call_ref:
        parts.append(f"[{frame.tool_call_ref}]")
    status = body.get("status")
    if isinstance(status, str) and status and frame.kind != FrameKind.STATUS:
        parts.append(status)
    if frame.kind == FrameKind.STATUS and isinstance(status, str):
        parts.append(status)
    if frame.kind == FrameKind.UNKNOWN:
        parts.append(f"({frame.raw_kind})")
    return " ".join(parts)[:512]


def _usage_from_payload(payload: Mapping[str, Any]) -> UsageReport | None:
    usage = payload.get("usage")
    if isinstance(usage, Mapping) and "dimensions" in usage:
        try:
            return UsageReport.model_validate(usage)
        except ValueError:
            return None
    return None


def _cited_digest(payload: Mapping[str, Any]) -> str | None:
    for key in ("result_digest", "summary_digest", "args_digest"):
        value = payload.get(key)
        if isinstance(value, str) and value.startswith("sha256:"):
            return value
    return None


class TranscriptService:
    """Materializes one tenant scope's run transcripts; authority: `workflow_run.read`."""

    def __init__(
        self,
        events: MissionEventReader,
        frames: FrameReader,
        *,
        request_scope: str,
        secret_values: Sequence[str] = (),
        artifacts: ArtifactBodyReader | None = None,
    ) -> None:
        self._events = events
        self._frames = frames
        self.request_scope = request_scope
        self._secrets = tuple(secret_values)
        self._artifacts = artifacts

    def _authorize(self, actor: ActorContext) -> None:
        if READ_PERMISSION not in actor.permissions:
            raise TranscriptDenied("actor lacks workflow_run.read")

    async def _sources(
        self, run_key: str
    ) -> tuple[UUID, tuple[MissionEventRecord, ...], tuple[ProviderFrame, ...]]:
        run_uuid = await self._events.run_identity(self.request_scope, run_key)
        if run_uuid is None:
            raise TranscriptRunNotFound(f"run not found: {run_key}")
        events = await self._events.events_for_run(self.request_scope, run_key)
        frames = await self._frames.frames_for_run(self.request_scope, run_uuid, limit=MAX_FRAMES)
        return run_uuid, events, frames

    async def materialize(
        self,
        run_key: str,
        *,
        actor: ActorContext,
        query: TranscriptQuery | None = None,
        full: bool = False,
    ) -> TranscriptPage:
        self._authorize(actor)
        if full and self._artifacts is None:
            raise FullBodyUnavailable("full bodies need the artifact read API under the grant")
        query = query or TranscriptQuery()
        since = TranscriptCursor.decode(query.since) if query.since else None
        _run_uuid, events, frames = await self._sources(run_key)
        placed = merge(run_key, events, frames, secret_values=self._secrets)
        selected: list[TranscriptEntry] = []
        has_more = False
        for item in placed:
            if since is not None and item.cursor <= since:
                continue
            if not _matches(item.entry, query):
                continue
            if len(selected) >= query.limit:
                has_more = True
                break
            selected.append(item.entry)
        if full and self._artifacts is not None:
            selected = [await self._with_full_body(entry, actor) for entry in selected]
        next_cursor = selected[-1].cursor if selected else query.since
        return TranscriptPage(
            run_id=run_key, entries=tuple(selected), next_cursor=next_cursor, has_more=has_more
        )

    async def _with_full_body(self, entry: TranscriptEntry, actor: ActorContext) -> TranscriptEntry:
        ref = entry.refs.artifact_ref
        if ref is None or self._artifacts is None:
            return entry
        body = await self._artifacts.read(self.request_scope, actor, ref)
        return entry.model_copy(update={"body_excerpt": redact_text(body, self._secrets)[0]})

    async def projection_entries(self, run_key: str) -> tuple[TranscriptEntry, ...]:
        """Every entry of the run, redacted, in transcript order (the C4 search projection
        source). No actor: the projection job indexes; searches authorize on read."""

        _run_uuid, events, frames = await self._sources(run_key)
        return tuple(
            item.entry for item in merge(run_key, events, frames, secret_values=self._secrets)
        )

    async def tail_frames(
        self,
        run_key: str,
        *,
        actor: ActorContext,
        after: str | None = None,
        limit: int = 500,
    ) -> TranscriptPage:
        """Non-canonical live tail over the Native Event Store (dashboards, `--follow`)."""

        page = await self.materialize(
            run_key,
            actor=actor,
            query=TranscriptQuery(since=after, limit=limit),
        )
        frames_only = tuple(entry for entry in page.entries if not entry.canonical)
        return page.model_copy(update={"entries": frames_only})

    async def summarize_sessions(
        self, run_key: str, *, actor: ActorContext
    ) -> tuple[SessionSummary, ...]:
        self._authorize(actor)
        _run_uuid, _events, frames = await self._sources(run_key)
        return summarize_sessions(frames)

    async def inspection_summary(self, run_key: str, *, actor: ActorContext) -> RunFrameSummary:
        """FT-F6: sessions and the transcript cursor from persisted frames and events only."""

        self._authorize(actor)
        _run_uuid, events, frames = await self._sources(run_key)
        placed = merge(run_key, events, frames, secret_values=self._secrets)
        return RunFrameSummary(
            sessions=summarize_sessions(frames),
            frame_count=len(frames),
            last_arrival_ordinal=max((frame.arrival_ordinal for frame in frames), default=None),
            transcript_cursor=placed[-1].entry.cursor if placed else None,
        )


def _matches(entry: TranscriptEntry, query: TranscriptQuery) -> bool:
    if query.canonical_only and not entry.canonical:
        return False
    if query.activation and entry.activation_id != query.activation:
        return False
    if query.node and entry.node_key != query.node:
        return False
    if query.kinds and entry.kind not in query.kinds:
        return False
    return not (query.subordinate and entry.subordinate_ref != query.subordinate)


def _event_view(record: MissionEventRecord) -> _EventView:
    envelope = record.payload
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), Mapping) else envelope
    assert isinstance(payload, Mapping)
    execution = payload.get("execution")
    return _EventView(
        record=record,
        payload=payload,
        execution=execution if isinstance(execution, Mapping) else {},
    )


def merge(
    run_key: str,
    events: Iterable[MissionEventRecord],
    frames: Iterable[ProviderFrame],
    *,
    secret_values: Sequence[str] = (),
) -> list[_Placed]:
    """The ordered transcript of one run (pure; deterministic for the same inputs)."""

    views = sorted((_event_view(record) for record in events), key=lambda item: item.record.seq)
    frame_list = sorted(
        frames,
        key=lambda frame: (
            str(frame.harness_execution_id),
            frame.generation,
            frame.arrival_ordinal,
        ),
    )
    stored = {str(frame.frame_id) for frame in frame_list}
    node_by_activation: dict[str, str] = {}
    for view in views:
        activation = _str(view.execution.get("activation_id") or view.payload.get("activation_id"))
        node = _str(view.payload.get("node_key") or view.payload.get("stage_id"))
        if activation and node:
            node_by_activation.setdefault(activation, node)
    placed: list[_Placed] = []
    recorded = [view.record.recorded_at for view in views]
    for view in views:
        placed.extend(_event_entries(run_key, view, stored, node_by_activation, secret_values))
    # Effective observation time is monotonic within one execution generation, so a
    # writer clock step backwards cannot reorder frames against their arrival order.
    effective: dict[tuple[str, int], datetime] = {}
    for frame in frame_list:
        key = (str(frame.harness_execution_id), frame.generation)
        at = max(frame.observed_at, effective.get(key, frame.observed_at))
        effective[key] = at
        index = bisect.bisect_right(recorded, at) - 1
        seq = views[index].record.seq if index >= 0 else 0
        cursor = TranscriptCursor(
            seq=seq,
            observed_us=int(at.timestamp() * 1_000_000),
            ordinal=frame.arrival_ordinal,
            tag=frame.harness_execution_id.hex[:8],
        )
        placed.append(
            _Placed(cursor, _frame_entry(run_key, frame, cursor, node_by_activation, secret_values))
        )
    placed.sort(key=lambda item: item.cursor)
    return placed


def _event_entries(
    run_key: str,
    view: _EventView,
    stored: set[str],
    node_by_activation: Mapping[str, str],
    secret_values: Sequence[str],
) -> list[_Placed]:
    record, payload, execution = view.record, view.payload, view.execution
    source, role = _event_source_role(record.event_type)
    cursor = TranscriptCursor(seq=record.seq)
    source_ref = payload.get("source")
    native_ref = (
        _str(source_ref.get("native_event_ref")) if isinstance(source_ref, Mapping) else None
    )
    body = {
        key: value for key, value in payload.items() if key not in {"execution", "source", "usage"}
    }
    artifact_ref = (
        _str(payload.get("artifact_ref") or payload.get("durable_reference") or payload.get("ref"))
        if source == "artifact"
        else None
    )
    activation = _str(execution.get("activation_id") or payload.get("activation_id"))
    entry = TranscriptEntry(
        cursor=cursor.encode(),
        run_id=run_key,
        activation_id=activation,
        attempt_no=execution.get("attempt_no")
        if isinstance(execution.get("attempt_no"), int)
        else None,
        node_key=_str(payload.get("node_key") or payload.get("stage_id"))
        or (node_by_activation.get(activation) if activation else None),
        iteration_id=_str(payload.get("iteration_id") or payload.get("goal_iteration")),
        subordinate_ref=_str(payload.get("subordinate_ref")),
        at=record.recorded_at,
        source=source,
        kind=record.event_type,
        role=role,
        title=redact_text(_event_title(record.event_type, payload), secret_values)[0],
        body_excerpt=_excerpt(body, secret_values),
        refs=TranscriptRefs(
            event_id=record.event_id,
            native_event_ref=native_ref,
            artifact_ref=artifact_ref,
            tool_call_ref=_str(payload.get("tool_call_ref")),
            command_id=_str(payload.get("command_id")),
            human_task_id=_str(payload.get("human_task_id")),
            body_digest=(
                _str(payload.get("content_digest") or payload.get("digest"))
                if source == "artifact"
                else _cited_digest(payload)
            ),
        ),
        usage=_usage_from_payload(payload),
        canonical=True,
    )
    placed = [_Placed(cursor, entry)]
    if native_ref and native_ref.startswith(NATIVE_EVENT_REF_PREFIX):
        frame_id = str(frame_id_from_native_event_ref(native_ref))
        if frame_id not in stored:
            digest = _cited_digest(payload)
            marker = TranscriptCursor(seq=record.seq, tag="e" + frame_id.replace("-", "")[:7])
            placed.append(
                _Placed(
                    marker,
                    TranscriptEntry(
                        cursor=marker.encode(),
                        run_id=run_key,
                        activation_id=entry.activation_id,
                        attempt_no=entry.attempt_no,
                        node_key=entry.node_key,
                        subordinate_ref=entry.subordinate_ref,
                        at=record.recorded_at,
                        source="provider_frame",
                        kind="frame_expired",
                        role="system",
                        title=(
                            f"(frame expired, digest {digest})" if digest else "(frame expired)"
                        ),
                        refs=TranscriptRefs(
                            frame_id=frame_id, native_event_ref=native_ref, body_digest=digest
                        ),
                        canonical=False,
                    ),
                )
            )
    return placed


def _frame_entry(
    run_key: str,
    frame: ProviderFrame,
    cursor: TranscriptCursor,
    node_by_activation: Mapping[str, str],
    secret_values: Sequence[str],
) -> TranscriptEntry:
    body = frame_body_object(frame.body_excerpt, frame.body_bytes)
    usage = (
        usage_report(frame.lane_profile, [(str(frame.frame_id), body)])
        if frame.kind in (FrameKind.TURN_ENDED, FrameKind.USAGE) and body
        else None
    )
    if usage is not None and all(item.value is None for item in usage.dimensions.values()):
        usage = None
    activation = str(frame.activation_id)
    return TranscriptEntry(
        cursor=cursor.encode(),
        run_id=run_key,
        activation_id=activation,
        attempt_no=frame.attempt_no,
        node_key=node_by_activation.get(activation),
        subordinate_ref=frame.subordinate_ref,
        at=frame.observed_at,
        source="provider_frame",
        kind=frame.kind.value,
        role=_frame_role(frame.kind),
        title=redact_text(_frame_title(frame, body), secret_values)[0],
        body_excerpt=redact_text(frame.body_excerpt[:EXCERPT_CHARS], secret_values)[0] or None,
        refs=TranscriptRefs(
            frame_id=str(frame.frame_id),
            native_event_ref=frame.native_event_ref,
            artifact_ref=frame.body_artifact_ref,
            tool_call_ref=frame.tool_call_ref,
            body_digest=frame.body_digest,
        ),
        usage=usage,
        canonical=False,
    )


@dataclass(frozen=True)
class RunFrameSummary:
    """What inspection reads from the Native Event Store (never a provider call)."""

    sessions: tuple[SessionSummary, ...]
    frame_count: int
    last_arrival_ordinal: int | None
    transcript_cursor: str | None


def summarize_sessions(frames: Sequence[ProviderFrame]) -> tuple[SessionSummary, ...]:
    """Per harness execution: lane, generation, sessions, turns, tool calls, usage."""

    grouped: dict[tuple[str, int], list[ProviderFrame]] = {}
    for frame in frames:
        grouped.setdefault((str(frame.harness_execution_id), frame.generation), []).append(frame)
    summaries: list[SessionSummary] = []
    for (harness, generation), items in sorted(grouped.items()):
        items.sort(key=lambda frame: frame.arrival_ordinal)
        usage_bodies = [
            (str(frame.frame_id), frame_body_object(frame.body_excerpt, frame.body_bytes))
            for frame in items
            if frame.kind == FrameKind.USAGE
        ]
        result = next(
            (frame for frame in reversed(items) if frame.kind == FrameKind.RUN_RESULT), None
        )
        status = (
            frame_body_object(result.body_excerpt, result.body_bytes).get("status")
            if result is not None
            else None
        )
        summaries.append(
            SessionSummary(
                harness_execution_id=harness,
                lane_profile=items[0].lane_profile.value,
                generation=generation,
                native_session_refs=tuple(
                    dict.fromkeys(frame.native_session_ref for frame in items)
                ),
                turns_closed=sum(1 for frame in items if frame.kind == FrameKind.TURN_ENDED),
                tool_calls=len(
                    {
                        frame.tool_call_ref
                        for frame in items
                        if frame.tool_call_ref
                        and frame.kind
                        in (FrameKind.TOOL_CALL_COMPLETED, FrameKind.TOOL_CALL_FAILED)
                    }
                ),
                last_frame_kind=items[-1].kind.value,
                last_observed_at=items[-1].observed_at,
                run_result_status=str(status) if status else None,
                usage=usage_report(items[0].lane_profile, usage_bodies) if usage_bodies else None,
            )
        )
    return tuple(summaries)


__all__ = [
    "EVENT_TAG",
    "CursorExpired",
    "MissionEventReader",
    "MissionEventRecord",
    "RunFrameSummary",
    "SessionSummary",
    "TranscriptDenied",
    "TranscriptRunNotFound",
    "TranscriptService",
    "merge",
    "summarize_sessions",
]
