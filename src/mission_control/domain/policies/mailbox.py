"""The Run's command mailbox (FT-F1; SPEC-06 "Mailbox and boundary delivery", ADR-0032).

`queue_instruction` and `add_context` are admitted by the Reducer as pending Commands and
each writes one durable mailbox entry for the Run's current Generation, sequenced in the
`mailbox:<generation>` space. The family boundary (the StageGraph admission of the next
operation, the GoalDirected iteration boundary) claims undelivered entries in admission order
and hands them to the Context Packer as mandatory `queued_instruction` items; the lane marks
them consumed when the turn that carries them starts. An entry is never deleted and never
redirected: a cancel admitted before delivery supersedes it, a Generation that moved on
expires it with `stale_generation`, and a passed deadline expires it with `deadline_passed`.

Everything here is pure: entry construction, the claim decision at a boundary, and the
Delivery Report a lane's declared semantics produce. Storage and receipts live in
`application/execution/mailbox.py` and its adapters.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from enum import StrEnum
from typing import Final, Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import AwareDatetime, Field, model_validator

from mission_control.domain.policies.contracts import (
    DIGEST_PATTERN,
    MAILBOX_INLINE_REF_PREFIX,
    AddContextAction,
    BoundaryCommandRecord,
    Contract,
    DeliveryNativeRefs,
    DeliveryObservedOutcome,
    DeliveryReport,
    MailboxBoundary,
    MailboxExpand,
    QueueInstructionAction,
)

# SPEC-06: inline text is bounded (default 8 KiB); larger content travels as an artifact ref.
MAX_INLINE_BYTES: Final = 8 * 1024
MAILBOX_KIND = Literal["queue_instruction", "add_context"]
# The two mission events whose payload carries the Delivery Report (SPEC-06 acceptance).
COMMAND_DELIVERED_EVENT: Final = "command.delivered"
COMMAND_COMPLETED_EVENT: Final = "command.completed"
COMMAND_QUEUED_EVENT: Final = "command.queued"
COMMAND_OBSERVED_EVENT: Final = "command.observed"
# Lanes that report nothing for a command kind deliver it at the turn boundary.
DEFAULT_MAILBOX_SEMANTICS: Final = "turn_boundary_guaranteed"


class MailboxState(StrEnum):
    QUEUED = "queued"
    DELIVERED = "delivered"
    CONSUMED = "consumed"
    SUPERSEDED = "superseded"
    EXPIRED = "expired"


ExpiredReason = Literal["superseded", "stale_generation", "deadline_passed", "terminal_run"]
MailboxFamily = Literal["StageGraph", "GoalDirected"]
GOAL_EXECUTOR_NODE: Final = "goal/executor"
GOAL_VERIFIER_NODE: Final = "goal/verifier"


class MailboxContentTooLarge(ValueError):
    """Inline text above the configured cap (SPEC-06: a 413-style typed rejection)."""

    code = "content_too_large"

    def __init__(self, size: int, cap: int) -> None:
        super().__init__(f"inline content is {size} bytes; the mailbox cap is {cap} bytes")
        self.size = size
        self.cap = cap


def content_digest(text: str) -> str:
    """`sha256:<hex>` over the UTF-8 bytes of an inline text."""

    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"


def inline_content_ref(digest: str) -> str:
    return f"{MAILBOX_INLINE_REF_PREFIX}{digest}"


def check_inline_text(text: str, *, cap: int = MAX_INLINE_BYTES) -> int:
    """The byte size of an inline text, or `MailboxContentTooLarge` above the cap."""

    size = len(text.encode("utf-8"))
    if size > cap:
        raise MailboxContentTooLarge(size, cap)
    return size


class MailboxEntry(Contract):
    """One durable mailbox entry (`command_mailbox` row). Reference-only reads drop the text."""

    schema_version: Literal["mc.mailbox_entry.v1"] = "mc.mailbox_entry.v1"
    entry_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    command_id: str = Field(min_length=1)
    command_issuer: str = Field(min_length=1)
    kind: MAILBOX_KIND
    generation: int = Field(ge=1)
    boundary: MailboxBoundary
    node_key: str | None = None
    content_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=DIGEST_PATTERN)
    media_type: str = Field(min_length=1)
    content_bytes: int = Field(ge=0)
    content_inline: str | None = Field(default=None, max_length=MAX_INLINE_BYTES)
    expand: MailboxExpand | None = None
    admission_sequence: int = Field(ge=1)
    deadline: AwareDatetime | None = None
    state: MailboxState = MailboxState.QUEUED
    delivery_key: str | None = None
    superseded_by: str | None = None
    expired_reason: ExpiredReason | None = None
    accepted_at: AwareDatetime
    delivered_at: AwareDatetime | None = None
    consumed_at: AwareDatetime | None = None
    expired_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def state_shape(self) -> MailboxEntry:
        if self.content_inline is not None and (
            self.content_ref != inline_content_ref(self.content_digest)
            or content_digest(self.content_inline) != self.content_digest
        ):
            raise ValueError("inline mailbox content must match its digest and ref")
        if (self.state == MailboxState.SUPERSEDED) != (self.superseded_by is not None):
            raise ValueError("exactly a superseded entry names the command that superseded it")
        if (self.state in {MailboxState.SUPERSEDED, MailboxState.EXPIRED}) != (
            self.expired_reason is not None
        ):
            raise ValueError("exactly a superseded or expired entry carries an expired reason")
        if self.state in {MailboxState.DELIVERED, MailboxState.CONSUMED} and (
            self.delivery_key is None or self.delivered_at is None
        ):
            raise ValueError("a delivered entry names the delivery that took it")
        if self.state == MailboxState.CONSUMED and self.consumed_at is None:
            raise ValueError("a consumed entry records when its turn started")
        return self

    @property
    def pending(self) -> bool:
        """Not yet consumed by a turn, superseded or expired."""

        return self.state in {MailboxState.QUEUED, MailboxState.DELIVERED}

    def reference_view(self) -> MailboxEntry:
        """The entry without its inline body (inspection is reference-only)."""

        return self.model_copy(update={"content_inline": None})


def mailbox_entry_id(request_scope: str, run_id: str, issuer: str, command_id: str) -> str:
    return str(
        uuid5(NAMESPACE_URL, f"mc.mailbox_entry.v1|{request_scope}|{run_id}|{issuer}|{command_id}")
    )


def entry_for(
    record: BoundaryCommandRecord,
    *,
    content_inline: str | None,
    accepted_at: datetime,
) -> MailboxEntry:
    """The mailbox entry an accepted, sequenced mailbox command writes."""

    action = record.action
    if not isinstance(action, QueueInstructionAction | AddContextAction):
        raise ValueError("only queue_instruction and add_context write mailbox entries")
    if action.inline != (content_inline is not None):
        raise ValueError("inline mailbox commands carry their text; references carry none")
    if record.target_sequence < 1:
        raise ValueError("a mailbox entry needs its admission sequence")
    return MailboxEntry(
        entry_id=mailbox_entry_id(
            record.request_scope, record.run_id, record.idempotency_issuer, record.command_id
        ),
        request_scope=record.request_scope,
        run_id=record.run_id,
        command_id=record.command_id,
        command_issuer=record.idempotency_issuer,
        kind=action.kind,
        generation=action.generation,
        boundary=action.boundary,
        node_key=action.node_key,
        content_ref=action.content_ref,
        content_digest=action.content_digest,
        media_type=action.media_type,
        content_bytes=action.content_bytes,
        content_inline=content_inline,
        expand=action.expand if isinstance(action, AddContextAction) else None,
        admission_sequence=record.target_sequence,
        deadline=action.deadline,
        accepted_at=accepted_at,
    )


class MailboxBoundaryPoint(Contract):
    """Where a family boundary stands when it takes queued content.

    `node_key` is the StageGraph stage id or `goal/executor` / `goal/verifier`.
    `iteration_start` is true at a StageGraph admission and at the first attempt of a
    GoalDirected executor iteration: only there does a `next_iteration` entry apply.
    """

    family: MailboxFamily
    node_key: str = Field(min_length=1)
    generation: int = Field(ge=1)
    iteration_start: bool = True


ClaimDecision = Literal["claim", "wait", "stale_generation", "deadline_passed"]


def claim_decision(
    entry: MailboxEntry, point: MailboxBoundaryPoint, *, now: datetime
) -> ClaimDecision:
    """What the boundary does with one queued entry (SPEC-06; pure).

    - A Generation that moved on expires the entry (`stale_generation`), never redirects it.
    - A passed deadline expires it (`deadline_passed`).
    - A `node_key` names the one node that takes it; without one, StageGraph's next admitted
      operation or GoalDirected's next executor turn takes it (the verifier stays independent
      of operator steering unless an entry names `goal/verifier`).
    - `next_iteration` waits for an iteration start; `next_turn` applies at any turn.
    """

    if entry.state != MailboxState.QUEUED:
        return "wait"
    if entry.generation < point.generation:
        return "stale_generation"
    if entry.generation > point.generation:
        return "wait"
    if entry.deadline is not None and entry.deadline < now:
        return "deadline_passed"
    if entry.node_key is not None:
        if entry.node_key != point.node_key:
            return "wait"
    elif point.family == "GoalDirected" and point.node_key != GOAL_EXECUTOR_NODE:
        return "wait"
    if entry.boundary == "next_iteration" and not point.iteration_start:
        return "wait"
    return "claim"


def requested_semantics(delivery_semantics: dict[str, str] | None, kind: str) -> str:
    """The semantics a lane's `describe` names for a mailbox kind.

    `add_context` shares the queue path; lanes describe it as `queue_instruction`.
    """

    semantics = delivery_semantics or {}
    return semantics.get(kind) or semantics.get("queue_instruction") or DEFAULT_MAILBOX_SEMANTICS


def delivery_report(
    entry: MailboxEntry,
    *,
    lane_profile: str,
    requested: str,
    delivered: str,
    observed_outcome: DeliveryObservedOutcome,
    recorded_at: datetime,
    session_ref: str | None = None,
    turn_ref: str | None = None,
) -> DeliveryReport:
    return DeliveryReport(
        command_id=entry.command_id,
        requested_semantics=requested,
        delivered_semantics=delivered,
        native_refs=DeliveryNativeRefs(
            lane_profile=lane_profile, session_ref=session_ref, turn_ref=turn_ref
        ),
        observed_outcome=observed_outcome,
        recorded_at=recorded_at,
    )


def mailbox_event_payload(
    entry: MailboxEntry, report: DeliveryReport | None, **extra: object
) -> dict[str, object]:
    """Reference-only mission event payload (never the instruction text).

    Only the entry's immutable fields appear, so a retried transition repeats the event
    exactly; transition facts (delivery key, outcome) come from the persisted receipt.
    """

    payload: dict[str, object] = {
        "command_id": entry.command_id,
        "command_kind": entry.kind,
        "entry_id": entry.entry_id,
        "generation": entry.generation,
        "boundary": entry.boundary,
        "admission_sequence": entry.admission_sequence,
        "content_digest": entry.content_digest,
    }
    if report is not None:
        payload["delivery_report"] = report.model_dump(mode="json")
    payload.update(extra)
    return payload


__all__ = [
    "COMMAND_COMPLETED_EVENT",
    "COMMAND_DELIVERED_EVENT",
    "COMMAND_OBSERVED_EVENT",
    "COMMAND_QUEUED_EVENT",
    "DEFAULT_MAILBOX_SEMANTICS",
    "GOAL_EXECUTOR_NODE",
    "GOAL_VERIFIER_NODE",
    "MAX_INLINE_BYTES",
    "ClaimDecision",
    "ExpiredReason",
    "MailboxBoundaryPoint",
    "MailboxContentTooLarge",
    "MailboxEntry",
    "MailboxState",
    "check_inline_text",
    "claim_decision",
    "content_digest",
    "delivery_report",
    "entry_for",
    "inline_content_ref",
    "mailbox_entry_id",
    "mailbox_event_payload",
    "requested_semantics",
]
