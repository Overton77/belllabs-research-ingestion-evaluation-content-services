"""Frame facts: what closing provider frames establish, and the mission events they become.

`FrameFact`s are plain typed records derived only from closing frames (SPEC-03 lifecycle
synthesis table, rules 1-5). Each carries the frame it was derived from, so the mission
event the reducer writes points back by `source.native_event_ref`. Facts and events carry
references and digests, never bodies (workflow-types/09 section 5).

Pure: no database, Temporal, FastAPI or provider SDK imports.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import AwareDatetime, Field

from mission_control.domain.frames.contracts import (
    DIGEST_PATTERN,
    FrameContract,
    LaneProfile,
    native_event_ref,
)
from mission_control.domain.frames.usage import UsageReport


class AttemptOutcome(StrEnum):
    """Attempt outcomes of workflow-types/05 section 3.5 that frames can establish."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class FailureClass(StrEnum):
    PROVIDER_ERROR = "provider_error"
    TIMEOUT = "timeout"
    CAPACITY = "capacity"
    CANCELLED_BY_COMMAND = "cancelled_by_command"
    INFRASTRUCTURE = "infrastructure"
    OUTPUTS_MISSING = "outputs_missing"


MissingOutputPolicy = Literal["follow_up_turn", "not_accepted"]


UUID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"


class _Fact(FrameContract):
    """The frame evidence every fact carries (identities as canonical UUID strings, so a
    fact is canonical JSON inside a lifecycle command)."""

    frame_id: str = Field(pattern=UUID_PATTERN)
    arrival_ordinal: int = Field(ge=1)
    harness_execution_id: str = Field(pattern=UUID_PATTERN)
    generation: int = Field(ge=1)
    native_session_ref: str = Field(min_length=1)
    native_turn_ref: str | None = None
    subordinate_ref: str | None = None
    observed_at: AwareDatetime

    @property
    def native_event_ref(self) -> str:
        return native_event_ref(self.frame_id)


class SessionStartedFact(_Fact):
    fact: Literal["session_started"] = "session_started"
    lane_profile: LaneProfile


class TurnStartedFact(_Fact):
    fact: Literal["turn_started"] = "turn_started"
    turn_ordinal: int = Field(ge=1)


class TurnCompletedFact(_Fact):
    fact: Literal["turn_completed"] = "turn_completed"
    turn_ordinal: int = Field(ge=1)
    usage: UsageReport
    usage_frame_ids: tuple[str, ...] = ()
    stop_reason: str | None = None
    result_summary_ref: str | None = None


class ToolEffectFact(_Fact):
    fact: Literal["tool_effect"] = "tool_effect"
    turn_ordinal: int = Field(ge=1)
    tool_call_ref: str = Field(min_length=1)
    name: str | None = None
    status: Literal["completed", "failed", "denied"]
    args_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    result_digest: str = Field(pattern=DIGEST_PATTERN)
    exit_code: int | None = None


class ApprovalRequestedFact(_Fact):
    fact: Literal["approval_requested"] = "approval_requested"
    request_ref: str | None = None


class ApprovalResolvedFact(_Fact):
    fact: Literal["approval_resolved"] = "approval_resolved"
    request_ref: str | None = None
    decision: str | None = None


class CompactionObservedFact(_Fact):
    fact: Literal["compaction_observed"] = "compaction_observed"
    summary_digest: str = Field(pattern=DIGEST_PATTERN)


class ExecutionOutcomeFact(_Fact):
    """05 section 3.5: a provider run result mapped to an attempt outcome.

    `outcome` is None when the provider finished but the declared outputs are not covered by
    a Completion Candidate and the policy is `follow_up_turn`: a native `FINISHED` is never,
    by itself, `succeeded`.
    """

    fact: Literal["execution_outcome"] = "execution_outcome"
    provider_status: str = Field(min_length=1)
    outcome: AttemptOutcome | None
    failure_class: FailureClass | None = None
    missing_outputs: bool = False
    missing_output_policy: MissingOutputPolicy | None = None


class UsageSettledFact(_Fact):
    """A usage frame after its turn closed (e.g. Cursor `get_usage` settling cost)."""

    fact: Literal["usage_settled"] = "usage_settled"
    turn_ordinal: int = Field(ge=1)
    usage: UsageReport


class UnitInDoubtFact(_Fact):
    fact: Literal["unit_in_doubt"] = "unit_in_doubt"
    reason: str | None = None


class SessionEndedFact(_Fact):
    fact: Literal["session_ended"] = "session_ended"


FrameFact = Annotated[
    SessionStartedFact
    | TurnStartedFact
    | TurnCompletedFact
    | ToolEffectFact
    | ApprovalRequestedFact
    | ApprovalResolvedFact
    | CompactionObservedFact
    | ExecutionOutcomeFact
    | UsageSettledFact
    | UnitInDoubtFact
    | SessionEndedFact,
    Field(discriminator="fact"),
]

CURSOR_LANES = frozenset({LaneProfile.CURSOR_LOCAL, LaneProfile.CURSOR_CLOUD})


def _execution(fact: _Fact, *, activation_id: str, attempt_no: int) -> dict[str, Any]:
    return {
        "activation_id": activation_id,
        "attempt_no": attempt_no,
        "harness_execution_id": fact.harness_execution_id,
        "generation": fact.generation,
        "native_session_ref": fact.native_session_ref,
        "native_turn_ref": fact.native_turn_ref,
        "arrival_ordinal": fact.arrival_ordinal,
    }


def _usage(report: UsageReport) -> dict[str, Any]:
    return report.model_dump(mode="json")


def fact_events(
    fact: FrameFact,
    *,
    lane_profile: LaneProfile,
    activation_id: str,
    attempt_no: int,
) -> list[tuple[str, dict[str, Any]]]:
    """The mission events (workflow-types/09 section 3) one fact produces, in order.

    Every payload carries `execution` (activation, attempt, harness execution, generation,
    native refs) and `source` (`adapter`, `native_event_ref` of the frame). Payloads are
    deterministic functions of the fact: replaying the same frames yields identical events.
    """

    common: dict[str, Any] = {
        "execution": _execution(fact, activation_id=activation_id, attempt_no=attempt_no),
        "source": {"kind": "adapter", "native_event_ref": fact.native_event_ref},
    }
    if fact.subordinate_ref:
        common["subordinate_ref"] = fact.subordinate_ref
    events: list[tuple[str, dict[str, Any]]] = []
    if isinstance(fact, SessionStartedFact):
        events.append(("session.started", {**common, "lane_profile": fact.lane_profile.value}))
        if lane_profile in CURSOR_LANES:
            events.append(("activation.phase_changed", {**common, "phase": "executing"}))
    elif isinstance(fact, TurnStartedFact):
        events.append(("session.turn_started", {**common, "turn_ordinal": fact.turn_ordinal}))
    elif isinstance(fact, TurnCompletedFact):
        events.append(
            (
                "session.turn_completed",
                {
                    **common,
                    "turn_ordinal": fact.turn_ordinal,
                    "usage": _usage(fact.usage),
                    "usage_refs": [native_event_ref(item) for item in fact.usage_frame_ids],
                    "stop_reason": fact.stop_reason,
                    "result_summary_ref": fact.result_summary_ref,
                },
            )
        )
    elif isinstance(fact, ToolEffectFact):
        events.append(
            (
                "tool_call.completed",
                {
                    **common,
                    "turn_ordinal": fact.turn_ordinal,
                    "tool_call_ref": fact.tool_call_ref,
                    "name": fact.name,
                    "status": fact.status,
                    "args_digest": fact.args_digest,
                    "result_digest": fact.result_digest,
                    "exit_code": fact.exit_code,
                },
            )
        )
    elif isinstance(fact, ApprovalRequestedFact):
        events.append(
            (
                "activation.phase_changed",
                {**common, "phase": "awaiting_human", "request_ref": fact.request_ref},
            )
        )
    elif isinstance(fact, ApprovalResolvedFact):
        events.append(
            (
                "activation.phase_changed",
                {
                    **common,
                    "phase": "executing",
                    "reason": "approval_resolved",
                    "request_ref": fact.request_ref,
                    "decision": fact.decision,
                },
            )
        )
    elif isinstance(fact, CompactionObservedFact):
        events.append(
            ("session.compaction_observed", {**common, "summary_digest": fact.summary_digest})
        )
    elif isinstance(fact, ExecutionOutcomeFact):
        if fact.outcome is not None:
            events.append(
                (
                    "attempt.completed",
                    {
                        **common,
                        "outcome": fact.outcome.value,
                        "failure_class": fact.failure_class.value if fact.failure_class else None,
                        "provider_status": fact.provider_status,
                        "missing_outputs": fact.missing_outputs,
                        "missing_output_policy": fact.missing_output_policy,
                    },
                )
            )
        else:
            events.append(
                (
                    "activation.phase_changed",
                    {
                        **common,
                        "phase": "follow_up_turn",
                        "reason": "outputs_missing",
                        "provider_status": fact.provider_status,
                        "missing_output_policy": fact.missing_output_policy,
                    },
                )
            )
    elif isinstance(fact, UsageSettledFact):
        events.append(
            (
                "session.usage_settled",
                {**common, "turn_ordinal": fact.turn_ordinal, "usage": _usage(fact.usage)},
            )
        )
    elif isinstance(fact, UnitInDoubtFact):
        events.append(
            (
                "activation.lifecycle_changed",
                {**common, "lifecycle": "waiting", "blocker": "in_doubt", "reason": fact.reason},
            )
        )
    elif isinstance(fact, SessionEndedFact):
        events.append(("session.ended", common))
    return events


FRAME_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "session.started",
        "session.turn_started",
        "session.turn_completed",
        "session.compaction_observed",
        "session.usage_settled",
        "session.ended",
        "tool_call.completed",
        "attempt.completed",
        "activation.phase_changed",
        "activation.lifecycle_changed",
    }
)
