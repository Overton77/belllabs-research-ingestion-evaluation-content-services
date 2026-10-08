"""Versioned public contracts for the inherited, qualified lifecycle controls.

StageGraph and GoalDirected remain the execution authorities. These contracts do not
claim that the inherited GoalDirected executor implements every GENERAL Goal Loop node.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.policies.contracts import (
    BoundaryCommandStatus,
    CommandResult,
    PauseDecision,
    ResumeDecision,
    RunProjection,
)
from mission_control.domain.policies.stop_fence import ImmediateCancelReport


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ResourceRef(Contract):
    installation_id: UUID
    application_id: str = Field(min_length=1)
    tenant_id: UUID
    resource_id: UUID


class CommandTarget(Contract):
    kind: Literal["run"] = "run"
    id: str = Field(min_length=1)


class PausePayload(Contract):
    decision: PauseDecision
    runnable_work_remains: bool


class ResumePayload(Contract):
    decision: ResumeDecision
    runnable_work_remains: bool = True


class CancelPayload(Contract):
    urgency: Literal["normal", "immediate"] = "normal"


class WaitPayload(Contract):
    condition_id: str = Field(min_length=1)
    verification_evidence_ref: str = Field(min_length=1)
    runnable_work_remains: bool = True


class InstructionPayload(Contract):
    """The flat pre-SPEC-06 instruction form (an artifact ref); still accepted."""

    content_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    boundary: Literal["next_turn", "next_iteration"]


class ContinuationRequestPayload(Contract):
    """``request_continuation`` (SPEC-02/SPEC-06, FT-B4): seal and transfer at the next turn
    boundary. ``activation_id`` names the operation whose session continues; omitted, the
    run's most recently active session is chosen."""

    activation_id: str | None = Field(default=None, min_length=1, max_length=256)
    boundary: Literal["next_turn"] = "next_turn"


class ContentRef(Contract):
    """SPEC-06: content held as an artifact, bound by digest."""

    artifact_ref: str = Field(min_length=1, max_length=2048)
    content_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    media_type: str = Field(default="text/markdown", min_length=1, max_length=128)
    size_bytes: int = Field(default=0, ge=0)


class InlineText(Contract):
    """SPEC-06: a short text carried inline. The mailbox cap (default 8 KiB of UTF-8) is
    enforced by the service with a typed `content_too_large` rejection (HTTP 413)."""

    text: str = Field(min_length=1, max_length=1_048_576)
    # Optional; when given it must equal sha256 over the UTF-8 text.
    content_digest: str | None = Field(default=None, pattern=r"^sha256:[0-9a-f]{64}$")
    media_type: str = Field(default="text/markdown", min_length=1, max_length=128)


MailboxContent = ContentRef | InlineText


class QueueInstructionPayload(Contract):
    """`queue_instruction`: read at the next turn or iteration boundary, exactly once."""

    boundary: Literal["next_turn", "next_iteration"] = "next_turn"
    content: MailboxContent
    deadline: AwareDatetime | None = None
    node_key: str | None = Field(default=None, min_length=1, max_length=256)


class AddContextPayload(Contract):
    """`add_context`: an artifact or short note added to the next Context Packet."""

    boundary: Literal["next_turn", "next_iteration"] = "next_turn"
    content: MailboxContent
    expand: Literal["inline", "reference", "materialize", "auto"] = "auto"
    deadline: AwareDatetime | None = None
    node_key: str | None = Field(default=None, min_length=1, max_length=256)


class InterruptAndInjectPayload(Contract):
    """`interrupt_and_inject`: the lane's declared semantics deliver the content now."""

    content: MailboxContent
    settle_uncertain_effects: Literal[True] = True
    node_key: str | None = Field(default=None, min_length=1, max_length=256)


CommandPayload = (
    PausePayload
    | ResumePayload
    | CancelPayload
    | WaitPayload
    | QueueInstructionPayload
    | AddContextPayload
    | InterruptAndInjectPayload
    | InstructionPayload
    | ContinuationRequestPayload
)
_PAYLOADS_BY_KIND: dict[str, tuple[type[Contract], ...]] = {
    "pause": (PausePayload,),
    "resume": (ResumePayload,),
    "cancel": (CancelPayload,),
    "satisfy_wait": (WaitPayload,),
    "queue_instruction": (QueueInstructionPayload, InstructionPayload),
    "add_context": (AddContextPayload,),
    "interrupt_and_inject": (InterruptAndInjectPayload, InstructionPayload),
    "request_continuation": (ContinuationRequestPayload,),
}


class MissionCommandRequest(Contract):
    schema_version: Literal["mc.command.v1"] = "mc.command.v1"
    request_id: UUID
    expected_version: int = Field(ge=1)
    expected_generation: int = Field(ge=1)
    target: CommandTarget
    kind: Literal[
        "pause",
        "resume",
        "cancel",
        "satisfy_wait",
        "queue_instruction",
        "add_context",
        "interrupt_and_inject",
        "request_continuation",
    ]
    payload: CommandPayload
    reason: str = Field(min_length=1, max_length=4096)

    @model_validator(mode="before")
    @classmethod
    def payload_for_kind(cls, data: Any) -> Any:
        """Validate the payload as its kind's contract: the payload shapes are not mutually
        exclusive (`queue_instruction` and `add_context` share fields), so the kind decides."""

        if not isinstance(data, dict):
            return data
        payload = data.get("payload")
        options = _PAYLOADS_BY_KIND.get(str(data.get("kind")))
        if not isinstance(payload, dict) or options is None:
            return data
        first = options[0]
        if len(options) > 1 and "content" not in payload:
            # The flat legacy InstructionPayload (content_ref, content_digest, boundary).
            first = options[1]
        return {**data, "payload": first.model_validate(payload)}

    @model_validator(mode="after")
    def payload_matches_kind(self) -> MissionCommandRequest:
        expected = _PAYLOADS_BY_KIND[self.kind]
        if not isinstance(self.payload, expected):
            raise ValueError(f"{self.kind} requires {expected[0].__name__}")
        return self


class MissionCommandReceipt(Contract):
    schema_version: Literal["mc.command_receipt.v1"] = "mc.command_receipt.v1"
    request_id: UUID
    replay: bool = False
    admission: CommandResult
    delivery: BoundaryCommandStatus | None = None


class ForkLineageRef(Contract):
    """One fork edge: the source Run, the Snapshot it was taken at and the derived Run."""

    fork_request_id: str = Field(min_length=1)
    source_run_id: str = Field(min_length=1)
    target_run_id: str = Field(min_length=1)
    snapshot_id: str = Field(min_length=1)
    snapshot_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class RunLineage(Contract):
    """FT-F4: fork lineage of a Run, read from `mission_relationship` and the fork records."""

    forked_from: ForkLineageRef | None = None
    forks: tuple[ForkLineageRef, ...] = ()


def _absent(value: object) -> bool:
    return value is None


class LaneView(Contract):
    """FT-F6: the lane of the current harness execution (from persisted provider frames)."""

    lane_profile: str = Field(min_length=1)
    describe_digest: str | None = None
    qualified: bool | None = None
    harness_execution_id: str | None = None
    generation: int | None = Field(default=None, ge=1)
    native_session_refs: tuple[str, ...] = ()


class SessionView(Contract):
    """One harness execution's agent session(s): turns, tools, last status, usage disposition."""

    harness_execution_id: str = Field(min_length=1)
    lane_profile: str = Field(min_length=1)
    generation: int = Field(ge=1)
    native_session_refs: tuple[str, ...] = ()
    turn_count: int = Field(ge=0)
    tool_calls: int = Field(ge=0)
    last_turn_status: str | None = None
    usage_disposition: str | None = None
    last_observed_at: AwareDatetime | None = None


class MailboxEntryView(Contract):
    """A mailbox entry, reference-only: never its content."""

    entry_id: str = Field(min_length=1)
    command_id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    boundary: str = Field(min_length=1)
    state: str = Field(min_length=1)
    generation: int = Field(ge=1)
    admission_sequence: int = Field(ge=1)
    content_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class CommandDeliveryView(Contract):
    """Requested and delivered semantics, outcome and native refs of one Command."""

    command_id: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    lifecycle: str = Field(min_length=1)
    outcome: Literal["applied", "failed", "rejected", "expired"] | None = None
    requested_semantics: str | None = None
    delivered_semantics: str | None = None
    observed_outcome: str | None = None
    emulation_note: str | None = None
    native_refs: tuple[str, ...] = ()


class FramesCursor(Contract):
    """Where the Run's Transcript stands: pass `transcript_cursor` to `run transcript --since`."""

    frame_count: int = Field(ge=0)
    last_arrival_ordinal: int | None = Field(default=None, ge=1)
    transcript_cursor: str | None = None


class ChainLinkView(Contract):
    link_key: str = Field(min_length=1)
    from_mission_key: str = Field(min_length=1)
    to_mission_key: str = Field(min_length=1)
    kind: str = Field(min_length=1)
    state: str = Field(min_length=1)
    released_run_id: str | None = None


class ChainMembership(Contract):
    """The Mission Chain this Run's mission belongs to (SPEC-04), with its links."""

    chain_id: str = Field(min_length=1)
    chain_key: str = Field(min_length=1)
    lifecycle: str = Field(min_length=1)
    links: tuple[ChainLinkView, ...] = ()


class SubscriptionsView(Contract):
    """Active Subscriptions targeting this Run or its mission (SPEC-06)."""

    active: int = Field(ge=0)


class MissionInspection(Contract):
    """`mc.inspection.v1`. Sections added by SPEC-06 are optional and left out while absent,
    so existing clients keep parsing the same body."""

    schema_version: Literal["mc.inspection.v1"] = "mc.inspection.v1"
    run_id: str
    version: int = Field(ge=1)
    execution_generation: int = Field(ge=1)
    lifecycle: Literal["pending", "running", "paused", "completed"]
    phase: str
    # This is execution outcome, not generalized mission acceptance.
    execution_outcome: str | None
    projection: RunProjection
    lineage: RunLineage | None = Field(default=None, exclude_if=_absent)
    # FT-F6: reference-only sections, each present only where its source is composed.
    lane: LaneView | None = Field(default=None, exclude_if=_absent)
    sessions: tuple[SessionView, ...] | None = Field(default=None, exclude_if=_absent)
    mailbox: tuple[MailboxEntryView, ...] | None = Field(default=None, exclude_if=_absent)
    delivery_reports: tuple[CommandDeliveryView, ...] | None = Field(
        default=None, exclude_if=_absent
    )
    frames_cursor: FramesCursor | None = Field(default=None, exclude_if=_absent)
    chain: ChainMembership | None = Field(default=None, exclude_if=_absent)
    subscriptions: SubscriptionsView | None = Field(default=None, exclude_if=_absent)
    stop_fence: ImmediateCancelReport | None = Field(default=None, exclude_if=_absent)


class MissionControlRejected(ValueError):
    def __init__(
        self, code: str, message: str, *, frontier: dict[str, object] | None = None
    ) -> None:
        super().__init__(message)
        self.code = code
        # SPEC-06: a stale `expected_version` / `expected_generation` is answered with the
        # current frontier so the caller can re-read and retry deliberately.
        self.frontier = dict(frontier) if frontier is not None else None
