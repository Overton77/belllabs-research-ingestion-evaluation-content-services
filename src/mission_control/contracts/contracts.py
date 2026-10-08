"""Versioned public contracts for the inherited, qualified lifecycle controls.

StageGraph and GoalDirected remain the execution authorities. These contracts do not
claim that the inherited GoalDirected executor implements every GENERAL Goal Loop node.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.policies.contracts import (
    BoundaryCommandStatus,
    CommandResult,
    PauseDecision,
    ResumeDecision,
    RunProjection,
)


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
    content_ref: str = Field(min_length=1)
    content_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    boundary: Literal["next_turn", "next_iteration"]


class ContinuationRequestPayload(Contract):
    """``request_continuation`` (SPEC-02/SPEC-06, FT-B4): seal and transfer at the next turn
    boundary. ``activation_id`` names the operation whose session continues; omitted, the
    run's most recently active session is chosen."""

    activation_id: str | None = Field(default=None, min_length=1, max_length=256)
    boundary: Literal["next_turn"] = "next_turn"


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
        "interrupt_and_inject",
        "request_continuation",
    ]
    payload: (
        PausePayload
        | ResumePayload
        | CancelPayload
        | WaitPayload
        | InstructionPayload
        | ContinuationRequestPayload
    )
    reason: str = Field(min_length=1, max_length=4096)

    @model_validator(mode="before")
    @classmethod
    def payload_by_kind(cls, value: Any) -> Any:
        # All-default payloads (`cancel`, `request_continuation`) are ambiguous in a union;
        # the command kind selects the payload model.
        if isinstance(value, dict) and value.get("kind") == "request_continuation":
            payload = value.get("payload")
            if isinstance(payload, dict):
                return {**value, "payload": ContinuationRequestPayload.model_validate(payload)}
        return value

    @model_validator(mode="after")
    def payload_matches_kind(self) -> MissionCommandRequest:
        expected = {
            "pause": PausePayload,
            "resume": ResumePayload,
            "cancel": CancelPayload,
            "satisfy_wait": WaitPayload,
            "queue_instruction": InstructionPayload,
            "interrupt_and_inject": InstructionPayload,
            "request_continuation": ContinuationRequestPayload,
        }[self.kind]
        if not isinstance(self.payload, expected):
            raise ValueError(f"{self.kind} requires {expected.__name__}")
        return self


class MissionCommandReceipt(Contract):
    schema_version: Literal["mc.command_receipt.v1"] = "mc.command_receipt.v1"
    request_id: UUID
    replay: bool = False
    admission: CommandResult
    delivery: BoundaryCommandStatus | None = None


class MissionInspection(Contract):
    schema_version: Literal["mc.inspection.v1"] = "mc.inspection.v1"
    run_id: str
    version: int = Field(ge=1)
    execution_generation: int = Field(ge=1)
    lifecycle: Literal["pending", "running", "paused", "completed"]
    phase: str
    # This is execution outcome, not generalized mission acceptance.
    execution_outcome: str | None
    projection: RunProjection


class MissionControlRejected(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
