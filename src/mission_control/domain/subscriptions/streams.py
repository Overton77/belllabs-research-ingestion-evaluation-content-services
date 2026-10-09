"""`mc.stream_subscription.v1`: a scoped realtime subscription with per-stream cursors
(multi-provider SPEC-04 "Streams and envelopes", "Socket contract"; MP-01).

Three logical streams, each with its own cursor domain: mission events (reducer journal,
`mission_seq` per mission), provider frames (Native Event Store, `frame_seq` per
execution/generation) and best-effort presence. A browser subscription is distinct from a
durable callback registration (`mc.subscription.v1`); this contract is the former. Scope is
the authenticated scope, never a client-selectable namespace or room name; cursors are
monotone and bounded by server-sent high-watermarks.

The error vocabulary is UPPER_SNAKE like the existing public codes (`STALE_GENERATION`).
"""

from __future__ import annotations

from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

STREAM_SUBSCRIPTION_SCHEMA: Final = "mc.stream_subscription.v1"
STREAM_ENVELOPE_SCHEMA: Final = "mc.stream_envelope.v1"

StreamName = Literal["mission_events", "provider_frames", "presence"]
STREAMS: Final[tuple[StreamName, ...]] = ("mission_events", "provider_frames", "presence")
TargetKind = Literal["mission", "run", "execution", "chain"]
Visibility = Literal["full", "lifecycle_only", "unavailable"]
SubordinateKind = Literal["provider_subagent", "agent_server_child", "linked_mission"]

STREAM_ERROR_CODES: Final[tuple[str, ...]] = (
    "UNAUTHORIZED",
    "SCOPE_MISMATCH",
    "TARGET_NOT_FOUND",
    "CURSOR_EXPIRED",
    "CURSOR_AHEAD",
    "STALE_GENERATION",
    "UNSUPPORTED_FILTER",
    "RATE_LIMITED",
    "SLOW_CONSUMER",
    "COMMAND_CONFLICT",
    # MP-14 integration: a failing store or handler (retryable) and an operation this
    # deployment does not expose over the socket (not retryable).
    "UNAVAILABLE",
    "UNSUPPORTED_OPERATION",
)
StreamErrorCode = Literal[
    "UNAUTHORIZED",
    "SCOPE_MISMATCH",
    "TARGET_NOT_FOUND",
    "CURSOR_EXPIRED",
    "CURSOR_AHEAD",
    "STALE_GENERATION",
    "UNSUPPORTED_FILTER",
    "RATE_LIMITED",
    "SLOW_CONSUMER",
    "COMMAND_CONFLICT",
    "UNAVAILABLE",
    "UNSUPPORTED_OPERATION",
]


class StreamContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StreamScope(StreamContract):
    installation_id: str = Field(min_length=1, max_length=128)
    application_id: str = Field(min_length=1, max_length=63)
    tenant_id: str = Field(min_length=1, max_length=128)


class StreamTarget(StreamContract):
    kind: TargetKind
    id: str = Field(min_length=1, max_length=512)


class StreamCursor(StreamContract):
    """Position in one stream's own cursor domain; never compared across streams."""

    stream: StreamName
    position: str = Field(min_length=1, max_length=512)
    generation: int | None = Field(default=None, ge=1)


class StreamFilters(StreamContract):
    kinds: tuple[str, ...] = ()
    exclude_deltas: bool = True
    tool_detail: Literal["none", "summary", "full"] = "summary"

    @model_validator(mode="after")
    def unique_kinds(self) -> StreamFilters:
        if len(set(self.kinds)) != len(self.kinds):
            raise ValueError("filter kinds must be unique")
        return self


class StreamSubscription(StreamContract):
    """`mc.stream_subscription.v1`: what one connected client is subscribed to."""

    schema_version: Literal["mc.stream_subscription.v1"] = STREAM_SUBSCRIPTION_SCHEMA
    subscription_id: str = Field(min_length=1, max_length=512)
    request_id: str = Field(min_length=1, max_length=512)
    scope: StreamScope
    target: StreamTarget
    streams: tuple[StreamName, ...] = Field(min_length=1)
    filters: StreamFilters = Field(default_factory=StreamFilters)
    cursors: tuple[StreamCursor, ...] = ()
    include_descendants: bool = False
    visibility: Visibility = "full"
    max_queue_bytes: int = Field(default=1_048_576, ge=65_536, le=67_108_864)
    delta_coalesce_ms: int = Field(default=250, ge=0, le=10_000)

    @model_validator(mode="after")
    def coherent(self) -> StreamSubscription:
        if len(set(self.streams)) != len(self.streams):
            raise ValueError("streams must be unique")
        cursor_streams = [cursor.stream for cursor in self.cursors]
        if len(set(cursor_streams)) != len(cursor_streams):
            raise ValueError("at most one cursor per stream")
        unknown = sorted(set(cursor_streams) - set(self.streams))
        if unknown:
            raise ValueError(f"cursors name streams not subscribed: {unknown}")
        if "provider_frames" in self.streams and self.target.kind == "chain":
            raise ValueError("provider frames are subscribed per mission, run or execution")
        return self


class SubscribeAck(StreamContract):
    subscription_id: str = Field(min_length=1, max_length=512)
    snapshot_ref: str | None = Field(default=None, min_length=1, max_length=2_048)
    snapshot_versions: dict[str, str] = Field(default_factory=dict)
    high_watermarks: tuple[StreamCursor, ...] = ()
    replay_from: tuple[StreamCursor, ...] = ()
    gap: bool = False


class SubordinateRef(StreamContract):
    kind: SubordinateKind
    parent_execution_ref: str = Field(min_length=1, max_length=512)
    native_parent_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    native_child_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    spawn_correlation: str | None = Field(default=None, min_length=1, max_length=512)
    generation: int | None = Field(default=None, ge=1)
    visibility: Visibility = "lifecycle_only"


class StreamEnvelope(StreamContract):
    """`mc.stream_envelope.v1`: one delivered item of one stream."""

    schema_version: Literal["mc.stream_envelope.v1"] = STREAM_ENVELOPE_SCHEMA
    stream: StreamName
    event_id: str = Field(min_length=1, max_length=512)
    scope: StreamScope
    mission_ref: str | None = Field(default=None, min_length=1, max_length=512)
    run_ref: str | None = Field(default=None, min_length=1, max_length=512)
    execution_ref: str | None = Field(default=None, min_length=1, max_length=512)
    generation: int | None = Field(default=None, ge=1)
    cursor: StreamCursor
    occurred_at: str = Field(min_length=1, max_length=64)
    recorded_at: str = Field(min_length=1, max_length=64)
    kind: str = Field(min_length=1, max_length=128)
    payload: Any = None
    payload_ref: str | None = Field(default=None, min_length=1, max_length=2_048)
    correlation_id: str | None = Field(default=None, min_length=1, max_length=512)
    causation_id: str | None = Field(default=None, min_length=1, max_length=512)
    subordinate_ref: SubordinateRef | None = None
    coalesced: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def stream_fields(self) -> StreamEnvelope:
        if self.cursor.stream != self.stream:
            raise ValueError("an envelope's cursor belongs to its own stream")
        if self.stream == "provider_frames" and (
            self.execution_ref is None or self.generation is None
        ):
            raise ValueError("a provider frame envelope names execution and generation")
        if self.stream == "mission_events" and self.mission_ref is None:
            raise ValueError("a mission event envelope names its mission")
        if self.stream == "presence" and (self.payload_ref is not None or self.coalesced > 1):
            raise ValueError("presence is best-effort telemetry: inline, uncoalesced")
        if self.payload is not None and self.payload_ref is not None:
            raise ValueError("an envelope carries payload or payload_ref, not both")
        return self


class StreamError(StreamContract):
    code: StreamErrorCode
    retryable: bool
    request_id: str | None = Field(default=None, min_length=1, max_length=512)
    subscription_id: str | None = Field(default=None, min_length=1, max_length=512)
    detail: str = Field(default="", max_length=512)


def stream_contract_schemas() -> dict[str, dict[str, Any]]:
    return {
        "stream_subscription": StreamSubscription.model_json_schema(),
        "subscribe_ack": SubscribeAck.model_json_schema(),
        "stream_envelope": StreamEnvelope.model_json_schema(),
        "stream_error": StreamError.model_json_schema(),
    }


__all__ = [
    "STREAMS",
    "STREAM_ENVELOPE_SCHEMA",
    "STREAM_ERROR_CODES",
    "STREAM_SUBSCRIPTION_SCHEMA",
    "StreamCursor",
    "StreamEnvelope",
    "StreamError",
    "StreamErrorCode",
    "StreamFilters",
    "StreamName",
    "StreamScope",
    "StreamSubscription",
    "StreamTarget",
    "SubordinateRef",
    "SubscribeAck",
    "stream_contract_schemas",
]
