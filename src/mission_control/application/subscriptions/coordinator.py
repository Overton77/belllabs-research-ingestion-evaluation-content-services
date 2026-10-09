"""Bounded coordinator notifications (multi-provider SPEC-04 "Coordinator callbacks", MP-15).

A coordinator callback is an ADR-0032 Subscription read through a durable inbox (ADR-0040):
committed mission events are classified over the public vocabulary, significant ones become
one notification each, related progress is batched into bounded summaries, token and tool
deltas stay off unless the profile asks for them, and every notification carries the
canonical event ids it covers, its causation refs and a recursion depth.

This module is pure: `plan` turns one page of journal events plus the inbox's durable state
into the notifications to write. The store runs it inside one transaction (inbox row lock),
so a crash before commit replays the same page to the same deterministic ids.

Rules, in the order they apply to each event:

1. **Dedupe.** An event at or below the materialization cursor, or one whose canonical id
   is already covered, is skipped. Notification ids are uuid5 of the subscription, the kind
   and the first canonical event id they cover, so a replanned page yields the same ids.
2. **Classification** (`classify`), on the public name where MP-13 derives one
   (`run.completed`, `activation.completed`, `human_task.opened`); a `workflow_run.set_wait`
   is progress, never a review.
3. **Recursion.** An event caused by a command this inbox admitted (prompt or triggered
   command, recorded with its depth) inherits that depth. Above `max_recursion_depth` it is
   recorded `suppressed` (`recursion_bound`) and never delivered, so a notification cannot
   re-trigger itself indefinitely through causation.
4. **Batching.** Progress joins the run's open batch; the batch seals after
   `batch_window_seconds` (measured from its first event) or at `batch_max_events`, and
   before any significant notification of the same run so the inbox keeps journal order.
5. **Rate cap.** Per run, at most `rate_limit_per_run` notifications seal within
   `rate_window_seconds`; anything over folds into one `rate_limited` summary (actionable
   when anything folded was) that seals once the window has room. Commands whose causation
   is lost downstream (a coordinator acting outside the inbox) are bounded by this cap.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Final, Literal
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.application.subscriptions.aliases import derive_notification
from mission_control.domain.subscriptions.contracts import MissionEventEnvelope, event_type_matches

PROFILE_SCHEMA_VERSION: Final = "mc.coordinator_profile.v1"
NOTIFICATION_SCHEMA_VERSION: Final = "mc.coordinator_notification.v1"
INBOX_TICKET_PREFIX: Final = "coordinator-inbox:"
_NAMESPACE: Final = UUID("2b0f5f4e-3c4d-5e6f-8a9b-0c1d2e3f4a5b")
MAX_CAUSATION_REFS: Final = 16
MAX_FACT_CHARS: Final = 256


class NotificationKind(StrEnum):
    REVIEW_REQUIRED = "review_required"
    BLOCKED = "blocked"
    FAILED = "failed"
    TERMINAL_RESULT = "terminal_result"
    ACCEPTED_OUTPUT = "accepted_output"
    CHILD_LIFECYCLE = "child_lifecycle"
    REVIEW_CLOSED = "review_closed"
    PROGRESS = "progress"
    RATE_LIMITED = "rate_limited"


ACTIONABLE_KINDS: Final = frozenset(
    {
        NotificationKind.REVIEW_REQUIRED,
        NotificationKind.BLOCKED,
        NotificationKind.FAILED,
        NotificationKind.TERMINAL_RESULT,
    }
)
DEFAULT_KINDS: Final = (
    NotificationKind.REVIEW_REQUIRED,
    NotificationKind.BLOCKED,
    NotificationKind.FAILED,
    NotificationKind.TERMINAL_RESULT,
    NotificationKind.ACCEPTED_OUTPUT,
    NotificationKind.CHILD_LIFECYCLE,
    NotificationKind.REVIEW_CLOSED,
    NotificationKind.PROGRESS,
)

# Token and tool deltas, per-turn bookkeeping and usage: off by default. Raw provider deltas
# (`message_delta`, `thinking_delta`, `tool_call_delta`) live in the Native Event Store and
# never reach the mission journal at all; these are their journal-level counterparts.
DELTA_EVENT_PATTERNS: Final = (
    "tool_call.*",
    "session.turn_started",
    "session.turn_completed",
    "session.usage_settled",
    "workflow_run.record_usage",
    "workflow_run.settle_pending_usage",
    "workflow_run.reserve_budget",
    "workflow_run.claim_effect",
    "workflow_run.observe_effect",
    "workflow_run.settle_effect",
    "workflow_run.apply_frame_facts",
)
REVIEW_REQUIRED_EVENTS: Final = frozenset({"human_task.opened", "human_task.escalated"})
REVIEW_CLOSED_EVENTS: Final = frozenset({"human_task.resolved", "human_task.cancelled"})
BLOCKED_EVENTS: Final = frozenset(
    {"human_task.expired", "command.in_doubt", "subscription.dead_lettered"}
)
FAILED_EVENTS: Final = frozenset({"session.continuation_failed"})
TERMINAL_EVENTS: Final = frozenset({"run.completed", "chain.completed"})
ACCEPTED_OUTPUT_EVENTS: Final = frozenset(
    {
        "workflow_run.record_output_evidence",
        "workflow_run.accept_finalization_plan",
        "workflow_run.record_finalization_result",
        "artifact.admitted",
    }
)
DEFAULT_CHILD_LIFECYCLE: Final = (
    "workflow_run.register_async_child",
    "workflow_run.decide_async_child_fact",
    "workflow_run.admit_linked_result",
    "activation.completed",
)
_FAILED_OUTCOMES: Final = frozenset({"failed", "cancelled", "execution_failed", "not_accepted"})
# Small scalar classification facts copied from the canonical payload (never bodies).
FACT_KEYS: Final = (
    "human_task_id",
    "kind",
    "lifecycle",
    "gate_key",
    "review_round",
    "outcome",
    "terminal_outcome",
    "failure_class",
    "blocker",
    "phase",
    "reason",
    "decision",
    "subscription_id",
)


class CoordinatorContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CoordinatorProfile(CoordinatorContract):
    """`mc.coordinator_profile.v1`: the default coordinator filter and its bounds."""

    schema_version: Literal["mc.coordinator_profile.v1"] = PROFILE_SCHEMA_VERSION
    kinds: tuple[NotificationKind, ...] = Field(default=DEFAULT_KINDS, min_length=1)
    child_lifecycle_events: tuple[str, ...] = Field(default=DEFAULT_CHILD_LIFECYCLE, max_length=32)
    include_deltas: bool = False
    batch_window_seconds: int = Field(default=60, ge=0, le=3600)
    batch_max_events: int = Field(default=50, ge=1, le=500)
    rate_window_seconds: int = Field(default=300, ge=1, le=86_400)
    rate_limit_per_run: int = Field(default=12, ge=1, le=1000)
    max_recursion_depth: int = Field(default=2, ge=0, le=8)
    prompt_mode: Literal["off", "queue_instruction", "add_context"] = "off"
    prompt_limit_per_window: int = Field(default=6, ge=1, le=100)

    @model_validator(mode="after")
    def _no_rate_limited_kind(self) -> CoordinatorProfile:
        if NotificationKind.RATE_LIMITED in self.kinds:
            raise ValueError("rate_limited is produced by the rate cap, not selected")
        return self

    @property
    def batch_window(self) -> timedelta:
        return timedelta(seconds=self.batch_window_seconds)

    @property
    def rate_window(self) -> timedelta:
        return timedelta(seconds=self.rate_window_seconds)


NotificationState = Literal["open", "pending", "acknowledged", "suppressed"]
SuppressedReason = Literal["recursion_bound", "rate_folded"]
FactValue = str | int | bool | None


class NotificationEventRef(CoordinatorContract):
    """One canonical event a notification covers, with its public name when derived."""

    event_id: UUID
    seq: int = Field(ge=1)
    event_type: str
    public_event_id: UUID | None = None


class CoordinatorNotification(CoordinatorContract):
    """`mc.coordinator_notification.v1`: reference-only; read the events for detail."""

    schema_version: Literal["mc.coordinator_notification.v1"] = NOTIFICATION_SCHEMA_VERSION
    notification_id: UUID
    subscription_id: UUID
    inbox_seq: int | None = Field(default=None, ge=1)
    state: NotificationState
    kind: NotificationKind
    actionable: bool
    mission_id: UUID
    run_id: UUID | None = None
    seq_from: int = Field(ge=1)
    seq_to: int = Field(ge=1)
    event_count: int = Field(ge=1)
    events: tuple[NotificationEventRef, ...] = Field(min_length=1)
    counts: dict[str, int] = Field(default_factory=dict)
    truncated: bool = False
    anchor_event_id: UUID
    causation_refs: tuple[str, ...] = ()
    depth: int = Field(default=0, ge=0)
    facts: dict[str, FactValue] = Field(default_factory=dict)
    suppressed_reason: SuppressedReason | None = None
    opened_at: AwareDatetime
    sealed_at: AwareDatetime | None = None
    acknowledged_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def _consistent(self) -> CoordinatorNotification:
        if self.seq_from > self.seq_to:
            raise ValueError("seq_from is after seq_to")
        if (self.state == "pending" or self.state == "acknowledged") != (
            self.inbox_seq is not None
        ):
            raise ValueError("only delivered notifications (pending/acknowledged) hold inbox_seq")
        if (self.state == "suppressed") != (self.suppressed_reason is not None):
            raise ValueError("a suppressed notification names its reason, and only then")
        if self.state != "open" and self.sealed_at is None:
            raise ValueError("a sealed notification records sealed_at")
        return self

    def body(self) -> bytes:
        """The exact bytes a webhook callback signs and sends (sorted keys, compact)."""

        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")


@dataclass(frozen=True)
class JournalEvent:
    """A committed canonical mission event plus its whitelisted classification facts."""

    envelope: MissionEventEnvelope
    facts: Mapping[str, FactValue] = field(default_factory=dict)


@dataclass(frozen=True)
class Classification:
    kind: NotificationKind
    actionable: bool
    public_event_type: str
    public_event_id: UUID | None


def notification_id(subscription_id: UUID, kind: NotificationKind, first_event_id: UUID) -> UUID:
    return uuid5(_NAMESPACE, f"v1:{subscription_id}:{kind.value}:{first_event_id}")


def prompt_request_id(subscription_id: UUID, notification: UUID) -> UUID:
    """The coordinator prompt's command request id: one prompt per notification, replayable."""

    return uuid5(_NAMESPACE, f"v1:prompt:{subscription_id}:{notification}")


def delivery_id(subscription_id: UUID, notification: UUID) -> UUID:
    """Stable webhook delivery id of one notification (the receiver dedupes on it)."""

    return uuid5(_NAMESPACE, f"v1:delivery:{subscription_id}:{notification}")


def is_delta(event_type: str) -> bool:
    return any(event_type_matches(pattern, event_type) for pattern in DELTA_EVENT_PATTERNS)


def extract_facts(payload: object) -> dict[str, FactValue]:
    """Whitelisted scalar facts of a canonical payload (`DomainEventEnvelope` dump or body)."""

    if not isinstance(payload, Mapping):
        return {}
    inner = payload.get("payload") if isinstance(payload.get("payload"), Mapping) else payload
    assert isinstance(inner, Mapping)
    facts: dict[str, FactValue] = {}
    for key in FACT_KEYS:
        value = inner.get(key)
        if isinstance(value, bool | int) or value is None:
            if value is not None:
                facts[key] = value
        elif isinstance(value, str) and value:
            facts[key] = value[:MAX_FACT_CHARS]
    resolution = inner.get("resolution")
    if "decision" not in facts and isinstance(resolution, Mapping):
        decision = resolution.get("decision")
        if isinstance(decision, str) and decision:
            facts["decision"] = decision[:MAX_FACT_CHARS]
    return facts


def classify(event: JournalEvent, profile: CoordinatorProfile) -> Classification | None:
    """The notification kind of one canonical event under a profile, or None (not notified)."""

    canonical = event.envelope
    derived = derive_notification(canonical)
    public = derived.event_type if derived is not None else canonical.event_type
    public_id = derived.event_id if derived is not None else None
    facts = event.facts
    if is_delta(canonical.event_type):
        if not profile.include_deltas:
            return None
        kind = NotificationKind.PROGRESS
    elif public in REVIEW_REQUIRED_EVENTS:
        kind = NotificationKind.REVIEW_REQUIRED
    elif public in REVIEW_CLOSED_EVENTS:
        kind = NotificationKind.REVIEW_CLOSED
    elif public in BLOCKED_EVENTS or (
        canonical.event_type == "activation.lifecycle_changed" and facts.get("blocker")
    ):
        kind = NotificationKind.BLOCKED
    elif public in FAILED_EVENTS or (
        public == "activation.completed" and str(facts.get("outcome")) in _FAILED_OUTCOMES
    ):
        kind = NotificationKind.FAILED
    elif public in TERMINAL_EVENTS:
        kind = NotificationKind.TERMINAL_RESULT
    elif public in ACCEPTED_OUTPUT_EVENTS:
        kind = NotificationKind.ACCEPTED_OUTPUT
    elif public in profile.child_lifecycle_events or (
        canonical.event_type in profile.child_lifecycle_events
    ):
        kind = NotificationKind.CHILD_LIFECYCLE
    else:
        kind = NotificationKind.PROGRESS
    if kind not in profile.kinds:
        return None
    return Classification(kind, kind in ACTIONABLE_KINDS, public, public_id)


@dataclass(frozen=True)
class PlanInput:
    subscription_id: UUID
    mission_id: UUID
    profile: CoordinatorProfile
    cursor_seq: int
    next_inbox_seq: int
    open: tuple[CoordinatorNotification, ...]
    recent_sealed: Mapping[UUID | None, tuple[datetime, ...]]
    depths: Mapping[str, int]
    events: tuple[JournalEvent, ...]
    now: datetime


@dataclass(frozen=True)
class Plan:
    """What one materialization step writes: upserts (by id) and the new positions."""

    upserts: tuple[CoordinatorNotification, ...]
    cursor_seq: int
    next_inbox_seq: int

    @property
    def sealed(self) -> tuple[CoordinatorNotification, ...]:
        return tuple(item for item in self.upserts if item.state == "pending")


def plan(data: PlanInput) -> Plan:
    return _Planner(data).run()


class _Planner:
    def __init__(self, data: PlanInput) -> None:
        self.data = data
        self.profile = data.profile
        self.now = data.now
        self.next_inbox_seq = data.next_inbox_seq
        self.batches: dict[UUID | None, CoordinatorNotification] = {}
        self.overflow: dict[UUID | None, CoordinatorNotification] = {}
        for item in data.open:
            target = self.overflow if item.kind is NotificationKind.RATE_LIMITED else self.batches
            target[item.run_id] = item
        self.recent: dict[UUID | None, list[datetime]] = {
            run: [moment for moment in moments if moment > data.now - self.profile.rate_window]
            for run, moments in data.recent_sealed.items()
        }
        self.covered: set[UUID] = {ref.event_id for item in data.open for ref in item.events}
        self.suppressed: dict[UUID | None, CoordinatorNotification] = {}
        self.out: dict[UUID, CoordinatorNotification] = {}
        self.cursor = data.cursor_seq

    def run(self) -> Plan:
        for event in sorted(self.data.events, key=lambda item: item.envelope.seq):
            envelope = event.envelope
            if envelope.seq <= self.cursor:
                continue
            self.cursor = envelope.seq
            if envelope.event_id in self.covered:
                continue
            self.covered.add(envelope.event_id)
            classification = classify(event, self.profile)
            if classification is None:
                continue
            depth = self._depth(envelope)
            if depth > self.profile.max_recursion_depth:
                self._suppress(event, classification, depth)
            elif classification.kind is NotificationKind.PROGRESS:
                self._batch(event, classification, depth)
            else:
                self._significant(event, classification, depth)
        for run in list(self.batches):
            batch = self.batches[run]
            if self.now - batch.opened_at >= self.profile.batch_window:
                self._seal_batch(run)
        for run in list(self.overflow):
            if self._room(run):
                self._seal_overflow(run)
        return Plan(
            upserts=tuple(self.out.values()),
            cursor_seq=self.cursor,
            next_inbox_seq=self.next_inbox_seq,
        )

    # -- rules ----------------------------------------------------------------------------

    def _depth(self, envelope: MissionEventEnvelope) -> int:
        ref = envelope.causation_ref
        return self.data.depths.get(ref, 0) if ref else 0

    def _room(self, run: UUID | None) -> bool:
        window_start = self.now - self.profile.rate_window
        recent = [moment for moment in self.recent.get(run, []) if moment > window_start]
        self.recent[run] = recent
        return len(recent) < self.profile.rate_limit_per_run

    def _new(
        self,
        event: JournalEvent,
        classification: Classification,
        depth: int,
        *,
        kind: NotificationKind | None = None,
        state: NotificationState = "open",
    ) -> CoordinatorNotification:
        envelope = event.envelope
        chosen = kind or classification.kind
        return CoordinatorNotification(
            notification_id=notification_id(self.data.subscription_id, chosen, envelope.event_id),
            subscription_id=self.data.subscription_id,
            state=state,
            kind=chosen,
            actionable=classification.actionable,
            mission_id=envelope.mission_id,
            run_id=envelope.run_id,
            seq_from=envelope.seq,
            seq_to=envelope.seq,
            event_count=1,
            events=(_ref(envelope, classification),),
            counts={classification.public_event_type: 1},
            anchor_event_id=envelope.event_id,
            causation_refs=(envelope.causation_ref,) if envelope.causation_ref else (),
            depth=depth,
            facts=dict(event.facts) if chosen is not NotificationKind.PROGRESS else {},
            opened_at=min(envelope.recorded_at, self.now),
        )

    def _extend(
        self,
        item: CoordinatorNotification,
        event: JournalEvent,
        classification: Classification,
        depth: int,
        *,
        actionable: bool = False,
    ) -> CoordinatorNotification:
        envelope = event.envelope
        counts = dict(item.counts)
        counts[classification.public_event_type] = (
            counts.get(classification.public_event_type, 0) + 1
        )
        keep = len(item.events) < self.profile.batch_max_events
        causes = list(item.causation_refs)
        if envelope.causation_ref and envelope.causation_ref not in causes:
            causes.append(envelope.causation_ref)
        return item.model_copy(
            update={
                "seq_to": max(item.seq_to, envelope.seq),
                "event_count": item.event_count + 1,
                "events": (*item.events, _ref(envelope, classification)) if keep else item.events,
                "truncated": item.truncated or not keep,
                "counts": counts,
                "anchor_event_id": envelope.event_id,
                "causation_refs": tuple(causes[:MAX_CAUSATION_REFS]),
                "depth": max(item.depth, depth),
                "actionable": item.actionable or actionable,
            }
        )

    def _suppress(self, event: JournalEvent, classification: Classification, depth: int) -> None:
        run = event.envelope.run_id
        if classification.kind is NotificationKind.PROGRESS:
            prior = self.suppressed.get(run)
            if prior is not None:
                self.suppressed[run] = self._emit(self._extend(prior, event, classification, depth))
                return
        created = self._new(event, classification, depth).model_copy(
            update={
                "state": "suppressed",
                "suppressed_reason": "recursion_bound",
                "sealed_at": self.now,
            }
        )
        if classification.kind is NotificationKind.PROGRESS:
            self.suppressed[run] = created
        self._emit(created)

    def _batch(self, event: JournalEvent, classification: Classification, depth: int) -> None:
        run = event.envelope.run_id
        current = self.batches.get(run)
        if current is None:
            current = self._new(event, classification, depth)
        else:
            current = self._extend(current, event, classification, depth)
        self.batches[run] = self._emit(current)
        if current.event_count >= self.profile.batch_max_events:
            self._seal_batch(run)

    def _significant(self, event: JournalEvent, classification: Classification, depth: int) -> None:
        run = event.envelope.run_id
        if run in self.batches:
            self._seal_batch(run)  # keep journal order: earlier progress first
        created = self._new(event, classification, depth)
        if self._room(run) and run not in self.overflow:
            self._seal(created)
        else:
            self._fold(created, event, classification, depth)

    def _fold(
        self,
        item: CoordinatorNotification,
        event: JournalEvent | None,
        classification: Classification | None,
        depth: int,
    ) -> None:
        """Fold a notification over the run's rate cap into its `rate_limited` summary."""

        run = item.run_id
        current = self.overflow.get(run)
        if current is None:
            current = item.model_copy(
                update={
                    "notification_id": notification_id(
                        self.data.subscription_id,
                        NotificationKind.RATE_LIMITED,
                        item.events[0].event_id,
                    ),
                    "kind": NotificationKind.RATE_LIMITED,
                    "facts": {},
                    "opened_at": self.now,
                }
            )
        elif event is not None and classification is not None:
            current = self._extend(
                current, event, classification, depth, actionable=item.actionable
            )
        else:
            current = _merge(current, item, self.profile.batch_max_events)
        self.overflow[run] = self._emit(current)

    def _seal_batch(self, run: UUID | None) -> None:
        batch = self.batches.pop(run)
        if self._room(run) and run not in self.overflow:
            self._seal(batch)
            return
        # The persisted open batch is folded; its row stays, marked as folded.
        self._emit(
            batch.model_copy(
                update={
                    "state": "suppressed",
                    "suppressed_reason": "rate_folded",
                    "sealed_at": self.now,
                }
            )
        )
        self._fold(batch, None, None, batch.depth)

    def _seal_overflow(self, run: UUID | None) -> None:
        self._seal(self.overflow.pop(run))

    def _seal(self, item: CoordinatorNotification) -> None:
        sealed = item.model_copy(
            update={"state": "pending", "inbox_seq": self.next_inbox_seq, "sealed_at": self.now}
        )
        self.next_inbox_seq += 1
        self.recent.setdefault(item.run_id, []).append(self.now)
        self._emit(sealed)

    def _emit(self, item: CoordinatorNotification) -> CoordinatorNotification:
        self.out[item.notification_id] = item
        return item


def _ref(envelope: MissionEventEnvelope, classification: Classification) -> NotificationEventRef:
    return NotificationEventRef(
        event_id=envelope.event_id,
        seq=envelope.seq,
        event_type=classification.public_event_type,
        public_event_id=classification.public_event_id,
    )


def _merge(
    into: CoordinatorNotification, item: CoordinatorNotification, cap: int
) -> CoordinatorNotification:
    counts = dict(into.counts)
    for name, count in item.counts.items():
        counts[name] = counts.get(name, 0) + count
    room = max(0, cap - len(into.events))
    causes = list(into.causation_refs)
    causes.extend(ref for ref in item.causation_refs if ref not in causes)
    return into.model_copy(
        update={
            "seq_from": min(into.seq_from, item.seq_from),
            "seq_to": max(into.seq_to, item.seq_to),
            "event_count": into.event_count + item.event_count,
            "events": (*into.events, *item.events[:room]),
            "truncated": into.truncated or item.truncated or len(item.events) > room,
            "counts": counts,
            "anchor_event_id": item.anchor_event_id
            if item.seq_to >= into.seq_to
            else into.anchor_event_id,
            "causation_refs": tuple(causes[:MAX_CAUSATION_REFS]),
            "depth": max(into.depth, item.depth),
            "actionable": into.actionable or item.actionable,
        }
    )


def render_prompt(notification: CoordinatorNotification) -> str:
    """The reference-only text a coordinator prompt carries (no payload bodies)."""

    lines = [
        f"Mission Control notification {notification.notification_id} "
        f"({notification.kind.value}{', actionable' if notification.actionable else ''}).",
        f"mission {notification.mission_id}"
        + (f", run {notification.run_id}" if notification.run_id else "")
        + f", events seq {notification.seq_from}..{notification.seq_to}"
        + f" ({notification.event_count}).",
    ]
    if notification.counts:
        lines.append(
            "event types: "
            + ", ".join(f"{name} x{count}" for name, count in sorted(notification.counts.items()))
        )
    if notification.facts:
        lines.append(
            "facts: "
            + ", ".join(f"{key}={value}" for key, value in sorted(notification.facts.items()))
        )
    lines.append(
        "Inspect the run on demand and acknowledge this notification at a safe turn boundary."
    )
    return "\n".join(lines)


def causation_tag(notification: UUID) -> str:
    return f"coordinator-notification:{notification}"


__all__ = [
    "ACTIONABLE_KINDS",
    "DEFAULT_CHILD_LIFECYCLE",
    "DEFAULT_KINDS",
    "DELTA_EVENT_PATTERNS",
    "INBOX_TICKET_PREFIX",
    "NOTIFICATION_SCHEMA_VERSION",
    "PROFILE_SCHEMA_VERSION",
    "Classification",
    "CoordinatorNotification",
    "CoordinatorProfile",
    "JournalEvent",
    "NotificationEventRef",
    "NotificationKind",
    "Plan",
    "PlanInput",
    "causation_tag",
    "classify",
    "delivery_id",
    "extract_facts",
    "is_delta",
    "notification_id",
    "plan",
    "prompt_request_id",
    "render_prompt",
]
