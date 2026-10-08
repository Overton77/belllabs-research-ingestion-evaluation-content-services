"""Lane activity payloads: `lane.turn`, `lane.status` and `lane.cancel` (SPEC-07 section 4.1).

One lifecycle synthesis for every lane (ADR-0031): the operation workflow schedules
`lane.turn` segments; each segment starts or reattaches the provider session, persists
Provider Frames through the FrameSink before heartbeating the provider cursor, and stops at
a segment bound or a terminal frame. Only `ClosingFacts` cross the activity boundary: text
bodies stay in frames and artifacts. `lane.status` reconciles, `lane.cancel` is idempotent.

Pure contracts: no Temporal, database or provider SDK imports (FT-G2).
"""

from __future__ import annotations

from typing import Any, Final, Literal

from pydantic import Field, model_validator

from mission_control.domain.execution.contracts import Contract, OperationExecutionRequest
from mission_control.domain.execution.lanes import (
    LANE_OF_PROFILE,
    CancelReceipt,
    LaneProfileName,
    LaneResumePoint,
    LaneSegmentBounds,
    UsageDisposition,
    UsageReport,
)

LANE_TURN_REQUEST_SCHEMA: Final = "mc.lane_turn_request.v1"
LANE_TURN_RESULT_SCHEMA: Final = "mc.lane_turn_result.v1"
CLOSING_FACTS_SCHEMA: Final = "mc.closing_facts.v1"
MAX_EXCERPT_CHARS: Final = 4_096

NativeStatus = Literal["finished", "error", "cancelled", "expired", "in_doubt"]
TurnPhase = Literal["start", "resume"]
TERMINAL_NATIVE_STATUSES: Final = frozenset({"finished", "error", "cancelled", "expired"})


def default_lane_profile(operation: OperationExecutionRequest) -> LaneProfileName:
    """The lane profile an operation runs on (absent: its runtime's default lane)."""

    if operation.lane_profile is not None:
        return operation.lane_profile
    if operation.execution_runtime == "cursor":
        assert operation.cursor_binding is not None
        return operation.cursor_binding.lane_profile
    return "deep_agents"


class NativeRefs(Contract):
    """Native identity of the session and turn (agent id, run id); never a credential."""

    session_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    turn_ref: str | None = Field(default=None, min_length=1, max_length=1_024)


class LaneGitBranch(Contract):
    repo_url: str = Field(min_length=1, max_length=2_048)
    branch: str | None = Field(default=None, min_length=1, max_length=512)
    pr_url: str | None = Field(default=None, min_length=1, max_length=2_048)


class ClosingFacts(Contract):
    """`mc.closing_facts.v1`: the only lane output the reducer and settlement read."""

    schema_version: Literal["mc.closing_facts.v1"] = CLOSING_FACTS_SCHEMA
    native_status: NativeStatus
    result_excerpt: str = Field(default="", max_length=MAX_EXCERPT_CHARS)
    result_text_ref: str | None = Field(default=None, min_length=1, max_length=2_048)
    output_refs: tuple[str, ...] = ()
    usage: UsageReport = Field(default_factory=lambda: UsageReport(disposition="unknown"))
    cost_disposition: UsageDisposition = "unknown"
    git_branches: tuple[LaneGitBranch, ...] = ()
    patch_ref: str | None = Field(default=None, min_length=1, max_length=2_048)
    error_code: str | None = Field(default=None, min_length=1, max_length=128)
    error_message: str | None = Field(default=None, max_length=1_024)
    duration_ms: int | None = Field(default=None, ge=0)
    model: str | None = Field(default=None, min_length=1, max_length=256)
    # FT-G4: declared outputs (`/outputs/...`) the finished session did not register. A
    # native `finished` with missing outputs is never acceptance: it settles
    # `not_accepted(outputs_missing)`. Left out of dumps and digests while empty.
    missing_outputs: tuple[str, ...] = Field(default=(), exclude_if=lambda value: not value)

    @property
    def terminal(self) -> bool:
        return self.native_status in TERMINAL_NATIVE_STATUSES


class _LaneOperationPayload(Contract):
    operation: OperationExecutionRequest
    lane_profile: LaneProfileName
    generation: int = Field(ge=1)

    @model_validator(mode="after")
    def lane_belongs_to_operation(self) -> _LaneOperationPayload:
        expected = default_lane_profile(self.operation)
        if self.lane_profile != expected:
            raise ValueError(
                f"lane profile {self.lane_profile} differs from the operation's {expected}"
            )
        lane = LANE_OF_PROFILE[self.lane_profile]
        runtime = self.operation.execution_runtime
        if (runtime == "cursor") != (lane == "cursor"):
            raise ValueError(f"a {runtime} operation cannot run on lane {lane}")
        return self


class LaneTurnRequest(_LaneOperationPayload):
    """`lane.turn` input: one segment of one Session Turn."""

    schema_version: Literal["mc.lane_turn_request.v1"] = LANE_TURN_REQUEST_SCHEMA
    phase: TurnPhase = "start"
    cursor: str | None = Field(default=None, min_length=1, max_length=1_024)
    turn_no: int = Field(default=1, ge=1)
    segment_no: int = Field(default=1, ge=1)
    instruction_ref: str | None = Field(default=None, min_length=1, max_length=2_048)
    segment: LaneSegmentBounds = Field(default_factory=LaneSegmentBounds)
    # `wait_then_send` exceeded its bound: settle `failed(capacity)` without sending.
    capacity_exhausted: bool = False


class LaneTurnResult(Contract):
    """`lane.turn` output: the cursor to resume from, or the closing facts and settlement."""

    schema_version: Literal["mc.lane_turn_result.v1"] = LANE_TURN_RESULT_SCHEMA
    done: bool
    busy: bool = False
    cursor: str | None = Field(default=None, min_length=1, max_length=1_024)
    segment_no: int = Field(ge=1)
    frames_persisted: int = Field(default=0, ge=0)
    frames_duplicate: int = Field(default=0, ge=0)
    native: NativeRefs = Field(default_factory=NativeRefs)
    closing_facts: ClosingFacts | None = None
    usage_estimate: UsageReport = Field(default_factory=lambda: UsageReport(disposition="unknown"))
    # The operation boundary's public result (`OperationExecutionResult`) once settled.
    operation_result: dict[str, Any] | None = None

    @model_validator(mode="after")
    def done_carries_the_settlement(self) -> LaneTurnResult:
        if self.done and self.operation_result is None:
            raise ValueError("a finished lane turn carries the operation's settled result")
        if self.busy and self.done:
            raise ValueError("a busy lane turn sent nothing and cannot be done")
        return self


class LaneStatusRequest(_LaneOperationPayload):
    native: NativeRefs = Field(default_factory=NativeRefs)


class LaneStatusResult(Contract):
    status: str = Field(min_length=1, max_length=64)
    terminal: bool
    settled: bool = False
    idle: bool = False
    usage: UsageReport | None = None
    operation_result: dict[str, Any] | None = None


class LaneCancelRequest(_LaneOperationPayload):
    native: NativeRefs = Field(default_factory=NativeRefs)
    reason: str = Field(default="command", min_length=1, max_length=512)
    urgency: Literal["normal", "immediate"] = "normal"
    command_id: str | None = Field(default=None, min_length=1, max_length=512)
    # Settle the unit `cancelled` once the provider is terminal (the saga's last step).
    settle: bool = True
    # The status poll bound passed with the provider never terminal: record `in_doubt`.
    in_doubt: bool = False


class LaneCancelResult(Contract):
    receipt: CancelReceipt
    settled: bool = False
    operation_result: dict[str, Any] | None = None


# What a command does on every lane profile today (00-ARCHITECTURE section 6; the describe
# honesty test holds each declared matrix to it). Workflows read it without the registry.
LANE_COMMAND_SEMANTICS: Final[dict[str, str]] = {
    "cancel": "turn_boundary_guaranteed",
    "interrupt_and_inject": "cancel_and_replace",
}


# FT-G4: what `pause` and `resume` do on every lane profile (00-ARCHITECTURE section 6;
# the describe-honesty tests hold each declared matrix to it). `unsupported` pause means the
# lane cannot pause mid-run: a pause is refused while a turn runs and applies at the run
# boundary only (no new segment starts until resume).
LANE_PAUSE_SEMANTICS: Final[dict[str, str]] = {
    "deep_agents": "pause_at_tool_gate",
    "cursor_local": "unsupported",
    "cursor_cloud": "unsupported",
}
LANE_RESUME_SEMANTICS: Final[dict[str, str]] = {
    "deep_agents": "turn_boundary_guaranteed",
    "cursor_local": "wait_then_send",
    "cursor_cloud": "wait_then_send",
}
UNSUPPORTED_CONTROL: Final = "unsupported_control"


class ControlDecision(Contract):
    """What a lane does with a control right now (the Delivery Report's semantics)."""

    accepted: bool
    delivery_semantics: str = Field(min_length=1, max_length=64)
    reason_code: str | None = Field(default=None, min_length=1, max_length=64)
    detail: str = Field(default="", max_length=512)


def pause_decision_for(
    lane_profile: str, pause_semantics: str, *, turn_in_flight: bool
) -> ControlDecision:
    """SPEC-07 section 7 `pause`: as declared where the lane pauses mid-run; otherwise a typed
    `unsupported_control` rejection while a turn runs and a boundary-only pause."""

    if pause_semantics != "unsupported":
        return ControlDecision(accepted=True, delivery_semantics=pause_semantics)
    if turn_in_flight:
        return ControlDecision(
            accepted=False,
            delivery_semantics="unsupported",
            reason_code=UNSUPPORTED_CONTROL,
            detail=(
                f"pause is unsupported mid-run on lane profile {lane_profile}; "
                "it applies at the run boundary only"
            ),
        )
    return ControlDecision(
        accepted=True,
        delivery_semantics="turn_boundary_guaranteed",
        detail="applied at the run boundary",
    )


class LaneCommandReceipt(Contract):
    """What a command Update on the operation workflow returns (its Delivery Report seed)."""

    command_id: str = Field(min_length=1, max_length=512)
    kind: Literal["cancel", "interrupt_and_inject", "pause", "resume"]
    accepted: bool = True
    delivery_semantics: str = Field(min_length=1, max_length=64)
    # FT-G4: what the lane did with a boundary-only control (left out while empty).
    detail: str = Field(default="", max_length=512, exclude_if=lambda value: not value)


__all__ = [
    "CLOSING_FACTS_SCHEMA",
    "LANE_COMMAND_SEMANTICS",
    "LANE_PAUSE_SEMANTICS",
    "LANE_RESUME_SEMANTICS",
    "LANE_TURN_REQUEST_SCHEMA",
    "LANE_TURN_RESULT_SCHEMA",
    "TERMINAL_NATIVE_STATUSES",
    "UNSUPPORTED_CONTROL",
    "ClosingFacts",
    "ControlDecision",
    "LaneCancelRequest",
    "LaneCancelResult",
    "LaneCommandReceipt",
    "LaneGitBranch",
    "LaneResumePoint",
    "LaneSegmentBounds",
    "LaneStatusRequest",
    "LaneStatusResult",
    "LaneTurnRequest",
    "LaneTurnResult",
    "NativeRefs",
    "NativeStatus",
    "TurnPhase",
    "default_lane_profile",
    "pause_decision_for",
]
