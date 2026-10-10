"""`mc.realtime.v1`: the `/missions` Socket.IO event vocabulary and its additive payloads.

The subscription, envelope, acknowledgement and error bodies are the frozen
`mc.stream_subscription.v1` / `mc.stream_envelope.v1` contracts in
`domain/subscriptions/streams.py`. This module names the socket events (SPEC-04 "Socket
contract") and versions the two server payloads added on top of them:

- `lineage`: subordinate lineage of a subscription (provider subagents of the execution,
  linked missions of the mission), by reference only: refs, resolution, lifecycle, observed
  frame counts, visibility (`full | lifecycle_only | unavailable`) and the usage inclusion rule.
  Never child content.
- `command_receipt` progress: after the admission receipt, each later state of the same
  command's durable receipt ledger (`accepted -> queued -> delivered -> observed -> applied`,
  or `rejected | expired | failed`), read through the same application handler as
  `GET /runs/{run_id}/commands`. A network acknowledgement is never a receipt.

Additive only: existing events keep their bodies; new optional fields are appended.
"""

from __future__ import annotations

from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from mission_control.domain.subscriptions.streams import SubordinateRef

REALTIME_SCHEMA: Final = "mc.realtime.v1"
NAMESPACE: Final = "/missions"

CLIENT_EVENTS: Final[tuple[str, ...]] = (
    "subscribe",
    "ack",
    "unsubscribe",
    "command",
    "resolve_human_task",
    "reauthenticate",
)
SERVER_EVENTS: Final[tuple[str, ...]] = (
    "subscribed",
    "snapshot",
    "mission_event",
    "provider_frame",
    "presence",
    "lineage",
    "command_receipt",
    "human_task_receipt",
    "resync_required",
    "stream_error",
)

ReceiptStage = Literal[
    "accepted",
    "queued",
    "delivered",
    "observed",
    "applied",
    "rejected",
    "expired",
    "failed",
    "stale",
    "unfollowed",
]
MAX_LINEAGE_NODES: Final = 1_024
FINAL_STAGES: Final[frozenset[str]] = frozenset(
    {"applied", "rejected", "expired", "failed", "stale", "unfollowed"}
)


class RealtimeContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class UsageInclusionView(RealtimeContract):
    tokens: str = Field(min_length=1, max_length=64)
    cost: str = Field(min_length=1, max_length=64)


class SubordinateView(RealtimeContract):
    ref: SubordinateRef
    resolved: bool
    lifecycle: Literal["started", "ended", "unknown"]
    frame_count: int = Field(ge=0)
    lane: str | None = Field(default=None, max_length=64)
    usage: UsageInclusionView | None = None


class LineageNotice(RealtimeContract):
    """`lineage`: the full listing on subscribe (`full=true`), then changed nodes."""

    schema_version: Literal["mc.realtime.v1"] = REALTIME_SCHEMA
    subscription_id: str = Field(min_length=1, max_length=512)
    stream: Literal["mission_events", "provider_frames"] | None = None
    full: bool
    coverage: Literal["complete", "partial"]
    subordinates: tuple[SubordinateView, ...] = Field(default=(), max_length=MAX_LINEAGE_NODES)
    truncated: bool = False
    # A `chain` target: the chain's lifecycle, phase, members and links (on the full listing
    # and whenever its recorded version changes). Refs only.
    chain: dict[str, Any] | None = None


class CommandReceiptProgress(RealtimeContract):
    """`command_receipt` after admission: one later state of the command's receipt ledger."""

    schema_version: Literal["mc.realtime.v1"] = REALTIME_SCHEMA
    request_id: str = Field(min_length=1, max_length=512)
    application_id: str = Field(min_length=1, max_length=63)
    run_id: str = Field(min_length=1, max_length=512)
    command_id: str = Field(min_length=1, max_length=512)
    stage: ReceiptStage
    final: bool
    receipt: dict[str, Any] | None = None
    detail: str = Field(default="", max_length=512)


__all__ = [
    "CLIENT_EVENTS",
    "FINAL_STAGES",
    "MAX_LINEAGE_NODES",
    "NAMESPACE",
    "REALTIME_SCHEMA",
    "SERVER_EVENTS",
    "CommandReceiptProgress",
    "LineageNotice",
    "ReceiptStage",
    "SubordinateView",
    "UsageInclusionView",
]
