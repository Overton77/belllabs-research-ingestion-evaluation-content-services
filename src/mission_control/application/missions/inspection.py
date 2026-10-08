"""Inspection enrichment (FT-F6; SPEC-06 "Inspection", SPEC-03 sessions and cursor).

`mc.inspection.v1` gains optional, reference-only sections assembled from authority Mission
Control already holds, never from a provider call:

- `lane` and `sessions` from the persisted provider frames (FT-C1/C2) of the Run;
- `frames_cursor`: the last frame ordinal and the Transcript cursor for `run transcript
  --since`;
- `mailbox`: the Run's command mailbox entries (kind, boundary, state, sequence, digest);
- `delivery_reports`: requested and delivered semantics, outcome and native refs of every
  Command in the receipt ledger;
- `chain` and `subscriptions` from the chain and subscription tables;
- `stop_fence`: the immediate cancel's four-timestamp report (FT-F3).

Each section is present only where its source is composed; an absent section is left out of
the body, so existing clients keep parsing the same document.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.frames.transcript import TranscriptService
from mission_control.contracts.contracts import (
    ChainMembership,
    CommandDeliveryView,
    FramesCursor,
    LaneView,
    MailboxEntryView,
    MissionInspection,
    SessionView,
    SubscriptionsView,
)
from mission_control.domain.execution.lanes import LaneDescribe
from mission_control.domain.frames.usage import UsageDisposition, UsageReport
from mission_control.domain.policies.contracts import (
    ActorContext,
    BoundaryCommandStatus,
    ReceiptState,
)
from mission_control.domain.policies.mailbox import MailboxEntry

# The lane `describe` key a Command kind is delivered under.
_DESCRIBE_KEY = {
    "pause": "pause",
    "resume": "resume",
    "cancel": "cancel",
    "queue_instruction": "queue_instruction",
    "add_context": "queue_instruction",
    "interrupt_and_inject": "interrupt_and_inject",
}
_OUTCOMES: dict[ReceiptState, Literal["applied", "failed", "rejected", "expired"]] = {
    ReceiptState.APPLIED: "applied",
    ReceiptState.FAILED: "failed",
    ReceiptState.REJECTED: "rejected",
    ReceiptState.EXPIRED: "expired",
}


class InspectionSectionReader(Protocol):
    """Chain membership and Subscription counts of a Run (scope-bound reads)."""

    async def chain_membership(self, request_scope: str, run_id: str) -> ChainMembership | None: ...

    async def active_subscriptions(self, request_scope: str, run_id: str) -> int: ...


@dataclass(frozen=True)
class InspectionSources:
    """What the composition makes available to inspection (each optional)."""

    transcripts: TranscriptService | None = None
    sections: InspectionSectionReader | None = None
    describe: Callable[[str], LaneDescribe | None] | None = None


def usage_disposition(usage: UsageReport | None) -> str | None:
    """The weakest disposition across a session's usage dimensions (unknown < estimated)."""

    if usage is None or not usage.dimensions:
        return None
    found = {item.disposition for item in usage.dimensions.values()}
    for disposition in (UsageDisposition.UNKNOWN, UsageDisposition.ESTIMATED):
        if disposition in found:
            return disposition.value
    return UsageDisposition.SETTLED.value


def mailbox_views(entries: Sequence[MailboxEntry]) -> tuple[MailboxEntryView, ...]:
    return tuple(
        MailboxEntryView(
            entry_id=entry.entry_id,
            command_id=entry.command_id,
            kind=entry.kind,
            boundary=entry.boundary,
            state=entry.state.value,
            generation=entry.generation,
            admission_sequence=entry.admission_sequence,
            content_digest=entry.content_digest,
        )
        for entry in entries
    )


def delivery_views(
    commands: Sequence[BoundaryCommandStatus], lane: LaneDescribe | None
) -> tuple[CommandDeliveryView, ...]:
    views: list[CommandDeliveryView] = []
    for status in commands:
        report = next(
            (item.delivery_report for item in reversed(status.receipts) if item.delivery_report),
            None,
        )
        key = _DESCRIBE_KEY.get(status.command.kind)
        declared = lane.delivery_semantics.get(key) if lane is not None and key else None
        refs = [item.transport_ref for item in status.receipts if item.transport_ref]
        if report is not None:
            native = report.native_refs
            refs.extend(
                ref
                for ref in (
                    native.session_ref,
                    native.turn_ref,
                    native.cancelled_turn_ref,
                    native.replacement_turn_ref,
                )
                if ref
            )
        views.append(
            CommandDeliveryView(
                command_id=status.command.command_id,
                kind=status.command.kind,
                lifecycle=status.state.value,
                outcome=_OUTCOMES.get(status.state),
                requested_semantics=(
                    report.requested_semantics if report is not None else declared
                ),
                delivered_semantics=report.delivered_semantics if report is not None else None,
                observed_outcome=report.observed_outcome if report is not None else None,
                emulation_note=report.emulation_note if report is not None else None,
                native_refs=tuple(dict.fromkeys(refs)),
            )
        )
    return tuple(views)


async def enrich_inspection(
    inspection: MissionInspection,
    *,
    request_scope: str,
    actor: ActorContext,
    sources: InspectionSources,
    commands: Sequence[BoundaryCommandStatus],
    mailbox: MailboxDeliveryService | None,
) -> MissionInspection:
    """The inspection with every section its composed sources can answer."""

    run_id = inspection.run_id
    describe = sources.describe or _declared_describe
    update: dict[str, object] = {}
    lane_describe: LaneDescribe | None = None
    if sources.transcripts is not None:
        summary = await sources.transcripts.inspection_summary(run_id, actor=actor)
        sessions = tuple(
            SessionView(
                harness_execution_id=item.harness_execution_id,
                lane_profile=item.lane_profile,
                generation=item.generation,
                native_session_refs=item.native_session_refs,
                turn_count=item.turns_closed,
                tool_calls=item.tool_calls,
                last_turn_status=item.run_result_status or item.last_frame_kind,
                usage_disposition=usage_disposition(item.usage),
                last_observed_at=item.last_observed_at,
            )
            for item in summary.sessions
        )
        update["sessions"] = sessions
        update["frames_cursor"] = FramesCursor(
            frame_count=summary.frame_count,
            last_arrival_ordinal=summary.last_arrival_ordinal,
            transcript_cursor=summary.transcript_cursor,
        )
        current = max(
            summary.sessions,
            key=lambda item: (item.last_observed_at is not None, item.last_observed_at),
            default=None,
        )
        if current is not None:
            lane_describe = describe(current.lane_profile)
            update["lane"] = LaneView(
                lane_profile=current.lane_profile,
                describe_digest=lane_describe.digest if lane_describe is not None else None,
                qualified=lane_describe.qualified if lane_describe is not None else None,
                harness_execution_id=current.harness_execution_id,
                generation=current.generation,
                native_session_refs=current.native_session_refs,
            )
    update["delivery_reports"] = delivery_views(commands, lane_describe or describe("deep_agents"))
    if mailbox is not None:
        update["mailbox"] = mailbox_views(await mailbox.list_entries(request_scope, run_id))
    if sources.sections is not None:
        update["chain"] = await sources.sections.chain_membership(request_scope, run_id)
        update["subscriptions"] = SubscriptionsView(
            active=await sources.sections.active_subscriptions(request_scope, run_id)
        )
    return inspection.model_copy(update=update)


def _declared_describe(lane_profile: str) -> LaneDescribe | None:
    from mission_control.application.execution.harness.describe import DECLARED_LANE_MATRICES

    return DECLARED_LANE_MATRICES.get(lane_profile)  # type: ignore[call-overload]


__all__ = [
    "InspectionSectionReader",
    "InspectionSources",
    "delivery_views",
    "enrich_inspection",
    "mailbox_views",
]
