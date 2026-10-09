"""Provider frames: the lane-neutral record of what a provider said (SPEC-03, ADR-0028).

A Provider Frame is one raw event a lane observed from its provider or from a hook,
persisted verbatim (redacted, digested and excerpted under a cap) in the Native Event
Store before any derivation. Frames are keyed by ``(harness_execution_id, generation,
provider_key)`` and ordered by a writer-assigned ``arrival_ordinal``. Only Closing Frames
are read by the reducer; deltas and starts are evidence for the transcript only.

Pure contracts: no database, Temporal, FastAPI or provider SDK imports.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.contracts.identities import RequestScope, parse_request_scope

# The one Lane Profile vocabulary (MP-01): frames use the canonical enum rather than a copy,
# so a profile added for the runtime is a profile that may write frames, and vice versa.
from mission_control.domain.capabilities.host_support import LaneProfile as LaneProfile

DIGEST_PATTERN = r"^sha256:[0-9a-f]{64}$"
PROVIDER_FRAME_SCHEMA = "mc.provider_frame.v1"
DEFAULT_EXCERPT_CAP_BYTES = 8_192
MAX_PROVIDER_KEY_LENGTH = 1_024
NATIVE_EVENT_REF_PREFIX = "provider_frame:"


class FrameContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FrameKind(StrEnum):
    """Provider-neutral classification of one frame."""

    SESSION_INIT = "session_init"
    SESSION_STATE = "session_state"
    TURN_STARTED = "turn_started"
    MESSAGE_DELTA = "message_delta"
    MESSAGE = "message"
    THINKING_DELTA = "thinking_delta"
    TOOL_CALL_STARTED = "tool_call_started"
    TOOL_CALL_DELTA = "tool_call_delta"
    TOOL_CALL_COMPLETED = "tool_call_completed"
    TOOL_CALL_FAILED = "tool_call_failed"
    APPROVAL_REQUESTED = "approval_requested"
    APPROVAL_RESOLVED = "approval_resolved"
    HOOK_INVOKED = "hook_invoked"
    HOOK_RESULT = "hook_result"
    BEFORE_COMPACTION = "before_compaction"
    AFTER_COMPACTION = "after_compaction"
    USAGE = "usage"
    TURN_ENDED = "turn_ended"
    RUN_RESULT = "run_result"
    STATUS = "status"
    ERROR = "error"
    HEARTBEAT = "heartbeat"
    UNKNOWN = "unknown"


# SPEC-03 closing rule: the reducer may read exactly these kinds; nothing else moves state.
CLOSING_KINDS: frozenset[FrameKind] = frozenset(
    {
        FrameKind.TOOL_CALL_COMPLETED,
        FrameKind.TOOL_CALL_FAILED,
        FrameKind.APPROVAL_RESOLVED,
        FrameKind.HOOK_RESULT,
        FrameKind.AFTER_COMPACTION,
        FrameKind.USAGE,
        FrameKind.TURN_ENDED,
        FrameKind.RUN_RESULT,
        FrameKind.SESSION_STATE,
        FrameKind.ERROR,
    }
)

# Frames whose arrival creates or advances a native identity record in the same
# transaction (harness_execution, agent_session, session_turn).
IDENTITY_KINDS: frozenset[FrameKind] = frozenset(
    {FrameKind.SESSION_INIT, FrameKind.TURN_STARTED, FrameKind.TURN_ENDED, FrameKind.RUN_RESULT}
)

# Kinds whose oversized bodies may be promoted to a `provider_frame_body` artifact.
FULL_BODY_KINDS: frozenset[FrameKind] = frozenset(
    {FrameKind.TOOL_CALL_COMPLETED, FrameKind.MESSAGE, FrameKind.RUN_RESULT}
)


def is_closing(kind: FrameKind) -> bool:
    return kind in CLOSING_KINDS


class FrameScope(FrameContract):
    installation_id: UUID
    application_id: str = Field(min_length=1, max_length=63)
    tenant_id: UUID

    @classmethod
    def from_request_scope(cls, request_scope: str) -> FrameScope:
        parsed = parse_request_scope(request_scope)
        return cls(
            installation_id=parsed.installation_id,
            application_id=parsed.application_id,
            tenant_id=parsed.tenant_id,
        )

    @property
    def request_scope(self) -> str:
        return str(RequestScope(self.installation_id, self.application_id, self.tenant_id))


class FrameRedaction(FrameContract):
    """One redacted location: a JSON path and the pattern class that matched (never the value)."""

    path: str = Field(min_length=1, max_length=512)
    pattern: str = Field(min_length=1, max_length=64)


class ProviderFrame(FrameContract):
    """`mc.provider_frame.v1`: one persisted frame."""

    schema_version: Literal["mc.provider_frame.v1"] = "mc.provider_frame.v1"
    frame_id: UUID
    scope: FrameScope
    run_id: UUID
    activation_id: UUID
    attempt_no: int = Field(ge=1)
    harness_execution_id: UUID
    generation: int = Field(ge=1)
    lane_profile: LaneProfile
    native_session_ref: str = Field(min_length=1, max_length=1_024)
    native_turn_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    provider_key: str = Field(min_length=1, max_length=MAX_PROVIDER_KEY_LENGTH)
    arrival_ordinal: int = Field(ge=1)
    observed_at: AwareDatetime
    provider_timestamp: AwareDatetime | None = None
    kind: FrameKind
    closing: bool
    subordinate_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    tool_call_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    body_digest: str = Field(pattern=DIGEST_PATTERN)
    body_bytes: int = Field(ge=0)
    body_media_type: str = Field(min_length=1, max_length=128)
    body_excerpt: str
    body_artifact_ref: str | None = Field(default=None, min_length=1, max_length=2_048)
    redactions: tuple[FrameRedaction, ...] = ()
    raw_kind: str = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def closing_rule(self) -> ProviderFrame:
        if self.closing != is_closing(self.kind):
            raise ValueError(
                f"closing must be {is_closing(self.kind)} for frame kind {self.kind.value}"
            )
        return self

    @property
    def native_event_ref(self) -> str:
        return native_event_ref(self.frame_id)


def native_event_ref(frame_id: UUID | str) -> str:
    """The `source.native_event_ref` a mission event carries for a stored frame."""

    return f"{NATIVE_EVENT_REF_PREFIX}{UUID(str(frame_id))}"


def frame_id_from_native_event_ref(value: str) -> UUID:
    if not value.startswith(NATIVE_EVENT_REF_PREFIX):
        raise ValueError("not a provider frame reference")
    return UUID(value.removeprefix(NATIVE_EVENT_REF_PREFIX))


class ProviderCursor(FrameContract):
    """The last persisted frame of one (harness execution, generation)."""

    harness_execution_id: UUID
    generation: int = Field(ge=1)
    arrival_ordinal: int = Field(ge=1)
    provider_key: str = Field(min_length=1)


class AppendReceipt(FrameContract):
    """`FrameSink.append` outcome: newly stored, idempotent duplicates and fenced stale."""

    new: int = Field(ge=0)
    duplicate: int = Field(ge=0)
    stale: int = Field(ge=0)
    new_frame_ids: tuple[UUID, ...] = ()

    def __add__(self, other: AppendReceipt) -> AppendReceipt:
        return AppendReceipt(
            new=self.new + other.new,
            duplicate=self.duplicate + other.duplicate,
            stale=self.stale + other.stale,
            new_frame_ids=(*self.new_frame_ids, *other.new_frame_ids),
        )


class HarnessExecutionStart(FrameContract):
    """What a lane knows when it opens (or re-opens) one harness execution.

    The run and activation are named by their stable keys; the store resolves the scoped
    row identities. `harness_execution_id` is deterministic per attempt so a resumed
    activity re-opens the same record.
    """

    harness_execution_id: UUID
    request_scope: str = Field(min_length=1)
    run_key: str = Field(min_length=1, max_length=512)
    activation_key: str = Field(min_length=1, max_length=2_048)
    attempt_no: int = Field(ge=1)
    generation: int = Field(ge=1)
    lane_profile: LaneProfile
    native_session_ref: str = Field(min_length=1, max_length=1_024)
    runtime_kind: str = Field(min_length=1, max_length=128)
    provider_kind: str = Field(min_length=1, max_length=128)
    placement_kind: str = Field(min_length=1, max_length=128)
    intended_binding_digest: str = Field(pattern=DIGEST_PATTERN)
    launch_key: str | None = Field(default=None, min_length=1, max_length=1_024)
    native_identity: dict[str, Any] = Field(default_factory=dict)


class HarnessExecutionHandle(FrameContract):
    """The resolved identity a frame writer stamps on every frame of one execution."""

    harness_execution_id: UUID
    scope: FrameScope
    run_id: UUID
    activation_id: UUID
    attempt_no: int = Field(ge=1)
    generation: int = Field(ge=1)
    lane_profile: LaneProfile
    native_session_ref: str = Field(min_length=1)
    last_cursor: ProviderCursor | None = None

    @property
    def request_scope(self) -> str:
        return self.scope.request_scope


class FrameObservation(FrameContract):
    """One provider event as a lane writer saw it, before identity, ordinal and body handling.

    `body` is the provider payload already converted to JSON-compatible values; the frame
    writer canonicalizes, redacts, digests and excerpts it.
    """

    provider_key: str = Field(min_length=1, max_length=MAX_PROVIDER_KEY_LENGTH)
    raw_kind: str = Field(min_length=1, max_length=256)
    kind: FrameKind
    body: Any = None
    body_media_type: str = Field(default="application/json", min_length=1, max_length=128)
    native_turn_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    native_session_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    subordinate_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    tool_call_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    provider_timestamp: AwareDatetime | None = None
    observed_at: AwareDatetime | None = None


class StaleGeneration(ValueError):
    """`STALE_GENERATION`: a frame of an older generation than the execution's current one."""

    code = "STALE_GENERATION"


def frame_json_schemas() -> dict[str, dict[str, Any]]:
    """JSON Schema export of the frame contracts (beside the other public schemas)."""

    return {
        "provider_frame": ProviderFrame.model_json_schema(),
        "provider_cursor": ProviderCursor.model_json_schema(),
        "append_receipt": AppendReceipt.model_json_schema(),
        "harness_execution_start": HarnessExecutionStart.model_json_schema(),
        "frame_observation": FrameObservation.model_json_schema(),
    }


def ordered(frames: list[ProviderFrame] | tuple[ProviderFrame, ...]) -> list[ProviderFrame]:
    """Arrival order within one execution generation; stable across executions."""

    return sorted(
        frames,
        key=lambda frame: (
            str(frame.harness_execution_id),
            frame.generation,
            frame.arrival_ordinal,
        ),
    )
