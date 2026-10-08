"""Command mailbox port, in-memory store and boundary delivery (FT-F1; SPEC-06, ADR-0032).

- `CommandMailboxRepository` is the durable mailbox: run control writes an entry in the same
  commit that admits a `queue_instruction` / `add_context` Command (see the run-control
  repositories); the family boundary claims, the operation boundary consumes and settles,
  a cancel supersedes. Every transition is idempotent and keeps the row (never deleted).
- `MailboxDeliveryService` turns those transitions into the Command's receipts
  (`queued -> delivered -> observed -> applied | failed`, or `expired`), each carrying the
  Delivery Report the lane's `describe` names, and appends the `command.delivered` and
  `command.completed` mission events (reference-only payloads).

A claim is idempotent per delivery key (the bound operation's idempotency key): a retried
preparation receives exactly the entries it took the first time, even when none, so the
sealed packet, and the persisted operation binding, never differ between attempts.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from mission_control.contracts.contracts import ContentRef, InlineText
from mission_control.domain.execution.lanes import LaneDescribe
from mission_control.domain.policies.contracts import (
    COMPLETED_RECEIPT_STATES,
    ActorContext,
    AddContextAction,
    BoundaryCommandReceipt,
    BoundaryCommandStatus,
    DeliveryObservedOutcome,
    DeliveryReport,
    DomainEventEnvelope,
    MailboxBoundary,
    MailboxExpand,
    QueueInstructionAction,
    ReceiptState,
    RunProjection,
)
from mission_control.domain.policies.errors import RunControlNotFound
from mission_control.domain.policies.mailbox import (
    COMMAND_COMPLETED_EVENT,
    COMMAND_DELIVERED_EVENT,
    COMMAND_QUEUED_EVENT,
    MAX_INLINE_BYTES,
    ExpiredReason,
    MailboxBoundaryPoint,
    MailboxContentTooLarge,
    MailboxEntry,
    MailboxFamily,
    MailboxState,
    check_inline_text,
    claim_decision,
    content_digest,
    delivery_report,
    inline_content_ref,
    mailbox_event_payload,
    requested_semantics,
)

Clock = Callable[[], datetime]
MAILBOX_RECORDER = "mailbox-boundary"
MAILBOX_ACTOR = ActorContext(actor_id=MAILBOX_RECORDER)


class MailboxContentRejected(ValueError):
    """Queued content refused before admission (`content_too_large`, digest mismatch)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def mailbox_command_action(
    kind: str,
    content: ContentRef | InlineText,
    *,
    boundary: MailboxBoundary,
    generation: int,
    expand: MailboxExpand = "auto",
    node_key: str | None = None,
    deadline: datetime | None = None,
    inline_cap: int = MAX_INLINE_BYTES,
) -> tuple[QueueInstructionAction | AddContextAction, str | None]:
    """The Reducer action of a mailbox command and its inline body (FT-F1, FT-F4).

    Inline text is capped (`content_too_large`) and bound by digest; an artifact reference is
    bound by the digest the caller states. The body travels beside the action, never in it.
    """

    text: str | None = None
    if isinstance(content, InlineText):
        try:
            size = check_inline_text(content.text, cap=inline_cap)
        except MailboxContentTooLarge as error:
            raise MailboxContentRejected(error.code, str(error)) from None
        digest = content_digest(content.text)
        if content.content_digest is not None and content.content_digest != digest:
            raise MailboxContentRejected(
                "content_digest_mismatch", "inline content_digest differs from the text"
            )
        text = content.text
        fields: dict[str, object] = {
            "content_ref": inline_content_ref(digest),
            "content_digest": digest,
            "media_type": content.media_type,
            "content_bytes": size,
            "inline": True,
        }
    else:
        fields = {
            "content_ref": content.artifact_ref,
            "content_digest": content.content_digest,
            "media_type": content.media_type,
            "content_bytes": content.size_bytes,
        }
    common: dict[str, object] = {
        **fields,
        "boundary": boundary,
        "generation": generation,
        "node_key": node_key,
        "deadline": deadline,
    }
    if kind == "add_context":
        return AddContextAction.model_validate({**common, "expand": expand}), text
    if kind != "queue_instruction":
        raise ValueError(f"{kind} is not a mailbox command")
    return QueueInstructionAction.model_validate(common), text


@dataclass(frozen=True, slots=True)
class MailboxClaim:
    """What one boundary took: the entries it delivers and those it found expired."""

    delivery_key: str
    delivered: tuple[MailboxEntry, ...]
    expired: tuple[MailboxEntry, ...] = ()
    replay: bool = False


class CommandMailboxRepository(Protocol):
    async def list_entries(self, request_scope: str, run_id: str) -> tuple[MailboxEntry, ...]: ...

    async def claim(
        self,
        request_scope: str,
        run_id: str,
        *,
        delivery_key: str,
        point: MailboxBoundaryPoint,
        now: datetime,
    ) -> MailboxClaim: ...

    async def consume(
        self, request_scope: str, run_id: str, *, delivery_key: str, now: datetime
    ) -> tuple[MailboxEntry, ...]: ...

    async def release(
        self, request_scope: str, run_id: str, *, delivery_key: str
    ) -> tuple[MailboxEntry, ...]: ...

    async def supersede(
        self,
        request_scope: str,
        run_id: str,
        *,
        superseded_by: str,
        now: datetime,
    ) -> tuple[MailboxEntry, ...]: ...

    async def append_events(
        self, request_scope: str, run_id: str, events: Sequence[DomainEventEnvelope]
    ) -> None: ...


class BoundaryReceiptLedger(Protocol):
    """The run-control receipt ledger and run reads (`RunControlService`)."""

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...

    async def get_boundary_command(
        self, request_scope: str, run_id: str, idempotency_issuer: str, command_id: str
    ) -> BoundaryCommandStatus | None: ...

    async def record_boundary_receipt(
        self, request_scope: str, receipt: BoundaryCommandReceipt
    ) -> BoundaryCommandStatus: ...


class ReusedUnitOracle(Protocol):
    """FT-F4: whether a fork-derived Run settles a unit by reference (no turn runs)."""

    async def reused(self, request_scope: str, run_id: str, unit_key: str) -> bool: ...


class InMemoryCommandMailbox:
    """Behavioral mailbox with the PostgreSQL adapter's transitions (tests, local proof)."""

    def __init__(
        self, *, sink: Callable[[Sequence[DomainEventEnvelope]], None] | None = None
    ) -> None:
        self._lock = asyncio.Lock()
        self._entries: dict[str, MailboxEntry] = {}
        self._claims: dict[tuple[str, str, str], tuple[str, ...]] = {}
        self.events: dict[str, DomainEventEnvelope] = {}
        # The in-memory run-control outbox, so mailbox events join the Run's mission events.
        self._sink = sink

    def __eq__(self, other: object) -> bool:
        """State equality (tests snapshot repositories and compare them)."""

        if not isinstance(other, InMemoryCommandMailbox):
            return NotImplemented
        return (self._entries, self._claims, self.events) == (
            other._entries,
            other._claims,
            other.events,
        )

    __hash__ = None  # type: ignore[assignment]

    def insert_unlocked(self, entry: MailboxEntry) -> MailboxEntry:
        """Called by the in-memory run-control repository inside its admission commit."""

        prior = self._entries.get(entry.entry_id)
        if prior is not None:
            return deepcopy(prior)
        if any(
            item.request_scope == entry.request_scope
            and item.run_id == entry.run_id
            and item.generation == entry.generation
            and item.admission_sequence == entry.admission_sequence
            for item in self._entries.values()
        ):
            raise ValueError("mailbox admission sequence already taken for this generation")
        self._entries[entry.entry_id] = deepcopy(entry)
        return deepcopy(entry)

    def _run_entries(self, request_scope: str, run_id: str) -> list[MailboxEntry]:
        return sorted(
            (
                item
                for item in self._entries.values()
                if item.request_scope == request_scope and item.run_id == run_id
            ),
            key=lambda item: (item.generation, item.admission_sequence),
        )

    async def list_entries(self, request_scope: str, run_id: str) -> tuple[MailboxEntry, ...]:
        return tuple(deepcopy(self._run_entries(request_scope, run_id)))

    async def claim(
        self,
        request_scope: str,
        run_id: str,
        *,
        delivery_key: str,
        point: MailboxBoundaryPoint,
        now: datetime,
    ) -> MailboxClaim:
        async with self._lock:
            key = (request_scope, run_id, delivery_key)
            recorded = self._claims.get(key)
            if recorded is not None:
                return MailboxClaim(
                    delivery_key=delivery_key,
                    delivered=tuple(deepcopy(self._entries[item]) for item in recorded),
                    replay=True,
                )
            delivered: list[MailboxEntry] = []
            expired: list[MailboxEntry] = []
            for entry in self._run_entries(request_scope, run_id):
                decision = claim_decision(entry, point, now=now)
                if decision == "claim":
                    updated = entry.model_copy(
                        update={
                            "state": MailboxState.DELIVERED,
                            "delivery_key": delivery_key,
                            "delivered_at": now,
                        }
                    )
                    delivered.append(updated)
                elif decision in {"stale_generation", "deadline_passed"}:
                    updated = entry.model_copy(
                        update={
                            "state": MailboxState.EXPIRED,
                            "expired_reason": decision,
                            "expired_at": now,
                        }
                    )
                    expired.append(updated)
                else:
                    continue
                self._entries[updated.entry_id] = updated
            self._claims[key] = tuple(item.entry_id for item in delivered)
            return MailboxClaim(
                delivery_key=delivery_key,
                delivered=tuple(deepcopy(delivered)),
                expired=tuple(deepcopy(expired)),
            )

    async def consume(
        self, request_scope: str, run_id: str, *, delivery_key: str, now: datetime
    ) -> tuple[MailboxEntry, ...]:
        async with self._lock:
            consumed: list[MailboxEntry] = []
            for entry in self._run_entries(request_scope, run_id):
                if entry.delivery_key != delivery_key:
                    continue
                if entry.state == MailboxState.DELIVERED:
                    entry = entry.model_copy(
                        update={"state": MailboxState.CONSUMED, "consumed_at": now}
                    )
                    self._entries[entry.entry_id] = entry
                if entry.state == MailboxState.CONSUMED:
                    consumed.append(entry)
            return tuple(deepcopy(consumed))

    async def release(
        self, request_scope: str, run_id: str, *, delivery_key: str
    ) -> tuple[MailboxEntry, ...]:
        async with self._lock:
            released: list[MailboxEntry] = []
            for entry in self._run_entries(request_scope, run_id):
                if entry.delivery_key == delivery_key and entry.state == MailboxState.DELIVERED:
                    entry = entry.model_copy(
                        update={
                            "state": MailboxState.QUEUED,
                            "delivery_key": None,
                            "delivered_at": None,
                        }
                    )
                    self._entries[entry.entry_id] = entry
                    released.append(entry)
            return tuple(deepcopy(released))

    async def supersede(
        self,
        request_scope: str,
        run_id: str,
        *,
        superseded_by: str,
        now: datetime,
    ) -> tuple[MailboxEntry, ...]:
        async with self._lock:
            superseded: list[MailboxEntry] = []
            for entry in self._run_entries(request_scope, run_id):
                if entry.state == MailboxState.SUPERSEDED and entry.superseded_by == superseded_by:
                    superseded.append(entry)
                    continue
                if not entry.pending:
                    continue
                entry = entry.model_copy(
                    update={
                        "state": MailboxState.SUPERSEDED,
                        "superseded_by": superseded_by,
                        "expired_reason": "superseded",
                        "expired_at": now,
                    }
                )
                self._entries[entry.entry_id] = entry
                superseded.append(entry)
            return tuple(deepcopy(superseded))

    async def append_events(
        self, request_scope: str, run_id: str, events: Sequence[DomainEventEnvelope]
    ) -> None:
        del request_scope, run_id
        for event in events:
            prior = self.events.get(event.event_id)
            if prior is not None and prior != event:
                raise ValueError("mailbox event identity reused with different content")
            self.events[event.event_id] = event
        if self._sink is not None:
            self._sink(tuple(events))


# The receipt states a target state requires first (catch-up after a crash between steps).
_RECEIPT_PATH: dict[ReceiptState, tuple[ReceiptState, ...]] = {
    ReceiptState.DELIVERED: (ReceiptState.DELIVERED,),
    ReceiptState.OBSERVED: (ReceiptState.DELIVERED, ReceiptState.OBSERVED),
    ReceiptState.APPLIED: (ReceiptState.DELIVERED, ReceiptState.OBSERVED, ReceiptState.APPLIED),
    ReceiptState.FAILED: (ReceiptState.DELIVERED, ReceiptState.OBSERVED, ReceiptState.FAILED),
    ReceiptState.EXPIRED: (ReceiptState.EXPIRED,),
}


class MailboxDeliveryService:
    """Boundary delivery of queued content and the receipts and events it produces."""

    def __init__(
        self,
        mailbox: CommandMailboxRepository,
        receipts: BoundaryReceiptLedger,
        *,
        describe: Callable[[str], LaneDescribe | None] | None = None,
        clock: Clock = lambda: datetime.now(UTC),
        reuse: ReusedUnitOracle | None = None,
    ) -> None:
        self._mailbox = mailbox
        self._receipts = receipts
        self._describe = describe or _declared_describe
        self._clock = clock
        # A unit a fork reuses runs no turn: it must not take queued content.
        self._reuse = reuse

    @property
    def mailbox(self) -> CommandMailboxRepository:
        return self._mailbox

    def semantics(self, lane_profile: str, kind: str) -> str:
        describe = self._describe(lane_profile)
        return requested_semantics(
            dict(describe.delivery_semantics) if describe is not None else None, kind
        )

    async def list_entries(self, request_scope: str, run_id: str) -> tuple[MailboxEntry, ...]:
        return await self._mailbox.list_entries(request_scope, run_id)

    # -- the family boundary --------------------------------------------------------------

    async def deliver(
        self,
        request_scope: str,
        run_id: str,
        *,
        delivery_key: str,
        family: MailboxFamily,
        node_key: str,
        iteration_start: bool,
        lane_profile: str,
        unit_key: str | None = None,
    ) -> tuple[MailboxEntry, ...]:
        """Claim what this boundary takes; record `delivered` (or `expired`) receipts.

        The boundary stands at the Run's current Generation (the execution target's): an
        entry admitted for an earlier Generation expires here with `stale_generation`. A unit
        a fork settles by reference (`unit_key` reused) takes nothing: no turn would read it.
        """

        if (
            self._reuse is not None
            and unit_key is not None
            and await self._reuse.reused(request_scope, run_id, unit_key)
        ):
            return ()
        projection = await self._receipts.get_run(request_scope, run_id)
        point = MailboxBoundaryPoint(
            family=family,
            node_key=node_key,
            generation=(
                projection.execution_target.execution_generation
                if projection.execution_target is not None
                else 1
            ),
            iteration_start=iteration_start,
        )
        claim = await self._mailbox.claim(
            request_scope,
            run_id,
            delivery_key=delivery_key,
            point=point,
            now=self._clock(),
        )
        for entry in claim.expired:
            await self._complete(
                request_scope,
                entry,
                ReceiptState.EXPIRED,
                report=None,
                detail=f"expired: {entry.expired_reason}",
                boundary_state={"expired_reason": entry.expired_reason or "expired"},
            )
        for entry in claim.delivered:
            semantics = self.semantics(lane_profile, entry.kind)
            report = delivery_report(
                entry,
                lane_profile=lane_profile,
                requested=semantics,
                delivered=semantics,
                observed_outcome="delivered",
                recorded_at=entry.delivered_at or self._clock(),
            )
            status = await self._advance(
                request_scope,
                entry,
                ReceiptState.DELIVERED,
                report=report,
                detail=f"taken into the packet of {delivery_key}",
                transport_ref=delivery_key,
            )
            await self._event(request_scope, entry, COMMAND_DELIVERED_EVENT, status, report)
        return claim.delivered

    # -- the operation boundary (lane turn) ----------------------------------------------

    async def turn_started(
        self,
        request_scope: str,
        run_id: str,
        *,
        delivery_key: str,
        lane_profile: str,
        session_ref: str | None = None,
        turn_ref: str | None = None,
    ) -> tuple[MailboxEntry, ...]:
        """The turn that carries the delivered entries starts: consume them once."""

        consumed = await self._mailbox.consume(
            request_scope, run_id, delivery_key=delivery_key, now=self._clock()
        )
        for entry in consumed:
            semantics = self.semantics(lane_profile, entry.kind)
            await self._advance(
                request_scope,
                entry,
                ReceiptState.OBSERVED,
                report=delivery_report(
                    entry,
                    lane_profile=lane_profile,
                    requested=semantics,
                    delivered=semantics,
                    observed_outcome="delivered",
                    recorded_at=entry.consumed_at or self._clock(),
                    session_ref=session_ref,
                    turn_ref=turn_ref,
                ),
                detail="the turn carrying the entry started",
                transport_ref=delivery_key,
            )
        return consumed

    async def turn_settled(
        self,
        request_scope: str,
        run_id: str,
        *,
        delivery_key: str,
        lane_profile: str,
        succeeded: bool,
        turn_ref: str | None = None,
    ) -> tuple[MailboxEntry, ...]:
        """The carrying turn settled: the Commands complete `applied` or `failed`."""

        entries = tuple(
            entry
            for entry in await self._mailbox.list_entries(request_scope, run_id)
            if entry.delivery_key == delivery_key and entry.state == MailboxState.CONSUMED
        )
        outcome: DeliveryObservedOutcome = "applied" if succeeded else "delivered"
        for entry in entries:
            semantics = self.semantics(lane_profile, entry.kind)
            report = delivery_report(
                entry,
                lane_profile=lane_profile,
                requested=semantics,
                delivered=semantics,
                observed_outcome=outcome,
                recorded_at=self._clock(),
                turn_ref=turn_ref,
            )
            await self._complete(
                request_scope,
                entry,
                ReceiptState.APPLIED if succeeded else ReceiptState.FAILED,
                report=report,
                detail="the carrying turn settled" + ("" if succeeded else " failed"),
                transport_ref=delivery_key,
            )
        return entries

    async def turn_not_started(
        self, request_scope: str, run_id: str, *, delivery_key: str
    ) -> tuple[MailboxEntry, ...]:
        """SPEC-06: a turn that fails before it starts returns its entries to `queued`."""

        return await self._mailbox.release(request_scope, run_id, delivery_key=delivery_key)

    # -- supersession ---------------------------------------------------------------------

    async def supersede(
        self, request_scope: str, run_id: str, *, superseded_by: str
    ) -> tuple[MailboxEntry, ...]:
        """A cancel admitted before delivery expires every pending entry of the Run."""

        superseded = await self._mailbox.supersede(
            request_scope, run_id, superseded_by=superseded_by, now=self._clock()
        )
        for entry in superseded:
            await self._complete(
                request_scope,
                entry,
                ReceiptState.EXPIRED,
                report=None,
                detail=f"superseded by {superseded_by}",
                boundary_state={"expired_reason": "superseded", "superseded_by": superseded_by},
            )
        return superseded

    # -- receipts and events --------------------------------------------------------------

    async def _complete(
        self,
        request_scope: str,
        entry: MailboxEntry,
        state: ReceiptState,
        *,
        report: DeliveryReport | None,
        detail: str,
        transport_ref: str | None = None,
        boundary_state: dict[str, object] | None = None,
    ) -> None:
        status = await self._advance(
            request_scope,
            entry,
            state,
            report=report,
            detail=detail,
            transport_ref=transport_ref,
            boundary_state=boundary_state,
        )
        extra: dict[str, object] = {}
        if status is not None:
            extra["outcome"] = status.state.value
        if entry.expired_reason is not None:
            extra["expired_reason"] = entry.expired_reason
        if entry.superseded_by is not None:
            extra["superseded_by"] = entry.superseded_by
        await self._event(request_scope, entry, COMMAND_COMPLETED_EVENT, status, report, **extra)

    async def _advance(
        self,
        request_scope: str,
        entry: MailboxEntry,
        state: ReceiptState,
        *,
        report: DeliveryReport | None,
        detail: str,
        transport_ref: str | None = None,
        boundary_state: dict[str, object] | None = None,
    ) -> BoundaryCommandStatus | None:
        status = await self._receipts.get_boundary_command(
            request_scope, entry.run_id, entry.command_issuer, entry.command_id
        )
        if status is None:
            raise RunControlNotFound(f"mailbox command not found: {entry.command_id}")
        for step in _RECEIPT_PATH[state]:
            if any(item.state == step for item in status.receipts):
                continue
            if status.state in COMPLETED_RECEIPT_STATES:
                # Completed elsewhere (for example terminal_run when the Run ended).
                return status
            status = await self._receipts.record_boundary_receipt(
                request_scope,
                BoundaryCommandReceipt(
                    command_id=entry.command_id,
                    idempotency_issuer=entry.command_issuer,
                    run_id=entry.run_id,
                    request_scope=request_scope,
                    ordinal=len(status.receipts) + 1,
                    state=step,
                    recorded_by=MAILBOX_RECORDER,
                    detail=detail[:1024],
                    transport_ref=transport_ref,
                    boundary_state=dict(boundary_state or {}) if step == state else {},
                    # A catch-up step (a crash between two transitions) carries no report.
                    delivery_report=report if step == state else None,
                    recorded_at=report.recorded_at if report is not None else self._clock(),
                ),
            )
        return status

    async def _event(
        self,
        request_scope: str,
        entry: MailboxEntry,
        event_type: str,
        status: BoundaryCommandStatus | None,
        report: DeliveryReport | None,
        **extra: object,
    ) -> None:
        # The event repeats exactly on a retry: its instant, report and delivery key are the
        # persisted receipt's (a redelivery after a release keeps the first `delivered`).
        found = _receipt_for_event(status, event_type)
        anchor = found.recorded_at if found is not None else entry.accepted_at
        stored_report = found.delivery_report if found is not None else report
        if found is not None and found.transport_ref is not None:
            extra = {**extra, "delivery_key": found.transport_ref}
        await self._mailbox.append_events(
            request_scope,
            entry.run_id,
            (
                DomainEventEnvelope(
                    event_id=f"{event_type}:{entry.command_id}",
                    event_type=event_type,
                    aggregate_id=f"command:{entry.command_id}",
                    aggregate_version=2
                    if event_type == COMMAND_DELIVERED_EVENT
                    else 3,  # queued is 1
                    sequence=1,
                    occurred_at=anchor,
                    recorded_at=anchor,
                    actor=MAILBOX_ACTOR,
                    correlation_id=entry.command_id,
                    causation_id=entry.command_id,
                    payload=mailbox_event_payload(entry, stored_report, **extra),
                ),
            ),
        )


def queued_event(entry: MailboxEntry) -> DomainEventEnvelope:
    """`command.queued`, appended by run control with the mailbox entry it admits."""

    return DomainEventEnvelope(
        event_id=f"{COMMAND_QUEUED_EVENT}:{entry.command_id}",
        event_type=COMMAND_QUEUED_EVENT,
        aggregate_id=f"command:{entry.command_id}",
        aggregate_version=1,
        sequence=1,
        occurred_at=entry.accepted_at,
        recorded_at=entry.accepted_at,
        actor=MAILBOX_ACTOR,
        correlation_id=entry.command_id,
        causation_id=entry.command_id,
        payload=mailbox_event_payload(entry, None),
    )


_EVENT_STATES = {
    COMMAND_DELIVERED_EVENT: frozenset({ReceiptState.DELIVERED}),
    COMMAND_COMPLETED_EVENT: COMPLETED_RECEIPT_STATES,
}


def _receipt_for_event(
    status: BoundaryCommandStatus | None, event_type: str
) -> BoundaryCommandReceipt | None:
    if status is None:
        return None
    states = _EVENT_STATES.get(event_type, frozenset())
    return next((item for item in status.receipts if item.state in states), None)


def _declared_describe(lane_profile: str) -> LaneDescribe | None:
    from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES

    return DECLARED_LANE_MATRICES.get(lane_profile)  # type: ignore[call-overload]


__all__ = [
    "MAILBOX_RECORDER",
    "BoundaryReceiptLedger",
    "CommandMailboxRepository",
    "ExpiredReason",
    "InMemoryCommandMailbox",
    "MailboxClaim",
    "MailboxDeliveryService",
    "queued_event",
]
