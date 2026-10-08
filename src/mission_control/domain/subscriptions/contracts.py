"""Subscriptions (``mc.subscription.v1``) and the ``mc.event.v1`` delivery envelope.

Pure rules for SPEC-06 "Subscriptions": filters, channels (references only, never secret
values), the reference-only event envelope, HMAC-SHA256 webhook signatures over the raw
body, exponential backoff with full jitter, dead-lettering after twelve failures, SSE frame
rendering and the grants that guard subscribe and close.
"""

from __future__ import annotations

import fnmatch
import hashlib
import hmac
import json
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any, Final, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from mission_control.domain.authoring.contracts import SecretRef

SUBSCRIPTION_SCHEMA_VERSION: Final = "mc.subscription.v1"
EVENT_SCHEMA_VERSION: Final = "mc.event.v1"
SIGNATURE_HEADER: Final = "X-MC-Signature"
DEAD_LETTER_AFTER: Final = 12
HEARTBEAT_SECONDS: Final = 30
SUBSCRIBE_PERMISSION: Final = "workflow_run.read"
ADMIN_PERMISSION: Final = "workflow_run.admin"
DEAD_LETTERED_EVENT: Final = "subscription.dead_lettered"
EVENT_TYPE_PATTERN: Final = r"^[a-z][a-z0-9_]*(?:\.(?:[a-z][a-z0-9_]*|\*))*$|^\*$"
_LOOPBACK = frozenset({"127.0.0.1", "localhost", "::1"})


class SubscriptionContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SubscriptionState(StrEnum):
    ACTIVE = "active"
    PAUSED = "paused"
    DEAD_LETTERED = "dead_lettered"
    CLOSED = "closed"


class DeliveryStatus(StrEnum):
    DELIVERED = "delivered"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


def parse_secret_ref(value: str) -> SecretRef:
    """``<provider>:<key>`` (for example ``environment:MC_WEBHOOK_SECRET``) to a SecretRef."""

    provider, separator, key = value.partition(":")
    if not separator or not key:
        raise ValueError("secret_ref is <provider>:<key>, e.g. environment:MC_WEBHOOK_SECRET")
    return SecretRef.model_validate({"provider": provider, "key": key})


def secret_ref_name(ref: SecretRef) -> str:
    return f"{ref.provider}:{ref.key}"


class WebhookChannel(SubscriptionContract):
    kind: Literal["webhook"] = "webhook"
    url: str = Field(min_length=1, max_length=2048)
    secret_ref: SecretRef
    signature_header: Literal["X-MC-Signature"] = SIGNATURE_HEADER

    @field_validator("secret_ref", mode="before")
    @classmethod
    def _secret_ref_text(cls, value: object) -> object:
        return parse_secret_ref(value) if isinstance(value, str) else value

    @field_validator("url")
    @classmethod
    def _https_or_loopback(cls, value: str) -> str:
        parts = urlsplit(value)
        if parts.username or parts.password or parts.fragment:
            raise ValueError("webhook URL must not carry credentials or a fragment")
        if parts.scheme == "https" and parts.hostname:
            return value
        if parts.scheme == "http" and parts.hostname in _LOOPBACK:
            return value
        raise ValueError("webhook URL must be https (http is allowed only for loopback)")


class StreamTicketChannel(SubscriptionContract):
    kind: Literal["stream_ticket"] = "stream_ticket"
    ticket_id: str = Field(min_length=1, max_length=256)


class McpSessionChannel(SubscriptionContract):
    kind: Literal["mcp_session"] = "mcp_session"
    session_ref: str = Field(min_length=1, max_length=256)


Channel = Annotated[
    WebhookChannel | StreamTicketChannel | McpSessionChannel, Field(discriminator="kind")
]


class SubscriptionTarget(SubscriptionContract):
    kind: Literal["mission", "run"]
    mission_id: UUID
    run_id: UUID | None = None

    @model_validator(mode="after")
    def _run_target(self) -> SubscriptionTarget:
        if (self.kind == "run") != (self.run_id is not None):
            raise ValueError("a run target names run_id; a mission target does not")
        return self


EventType = Annotated[str, Field(pattern=EVENT_TYPE_PATTERN)]


class SubscriptionFilters(SubscriptionContract):
    event_types: tuple[EventType, ...] = Field(min_length=1)
    node_keys: tuple[str, ...] = ()

    def matches(self, event: MissionEventEnvelope) -> bool:
        if not any(event_type_matches(pattern, event.event_type) for pattern in self.event_types):
            return False
        return not self.node_keys or event.node_key in self.node_keys


def event_type_matches(pattern: str, event_type: str) -> bool:
    """Exact names, ``*`` for everything, or a trailing ``.*`` family (``human_task.*``)."""

    if pattern == "*":
        return True
    if pattern.endswith(".*"):
        return event_type.startswith(pattern[:-1])
    return fnmatch.fnmatchcase(event_type, pattern) if "*" in pattern else pattern == event_type


class Subscription(SubscriptionContract):
    """``mc.subscription.v1``."""

    schema_version: Literal["mc.subscription.v1"] = SUBSCRIPTION_SCHEMA_VERSION
    subscription_id: UUID
    request_scope: str = Field(min_length=1)
    target: SubscriptionTarget
    filters: SubscriptionFilters
    channel: Channel
    cursor_seq: int = Field(ge=0)
    state: SubscriptionState
    failure_count: int = Field(default=0, ge=0)
    next_attempt_at: AwareDatetime
    actor_ref: str = Field(min_length=1)
    created_at: AwareDatetime
    updated_at: AwareDatetime
    dead_lettered_at: AwareDatetime | None = None
    closed_at: AwareDatetime | None = None
    version: int = Field(default=1, ge=1)


class SubscriptionDelivery(SubscriptionContract):
    subscription_id: UUID
    event_id: UUID
    seq: int = Field(ge=1)
    attempt: int = Field(ge=1)
    status: DeliveryStatus
    response_code: int | None = Field(default=None, ge=100, le=599)
    latency_ms: int | None = Field(default=None, ge=0)
    error_class: str | None = None
    recorded_at: AwareDatetime


class SubscriptionRequest(SubscriptionContract):
    """Public create request; the actor and scope come from authentication."""

    target: Literal["mission", "run"]
    target_id: UUID
    events: tuple[EventType, ...] = Field(min_length=1, max_length=64)
    node_keys: tuple[str, ...] = Field(default=(), max_length=64)
    channel: Channel
    after_seq: int | None = Field(default=None, ge=0)


class MissionEventEnvelope(SubscriptionContract):
    """``mc.event.v1``: a committed mission event by reference (no payload bodies)."""

    schema_version: Literal["mc.event.v1"] = EVENT_SCHEMA_VERSION
    event_id: UUID
    application_id: str
    mission_id: UUID
    run_id: UUID | None = None
    activation_id: UUID | None = None
    seq: int = Field(ge=1)
    event_type: str = Field(min_length=1)
    event_version: int = Field(ge=1)
    actor_ref: str
    happened_at: AwareDatetime
    recorded_at: AwareDatetime
    causation_ref: str | None = None
    node_key: str | None = None
    payload_ref: str
    payload_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")

    def body(self) -> bytes:
        """The exact bytes signed and sent (sorted keys, compact)."""

        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")


def payload_digest(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "sha256:" + hashlib.sha256(data.encode("utf-8")).hexdigest()


def node_key_of(payload: Any) -> str | None:
    """The program node an event concerns, when its payload names one."""

    if not isinstance(payload, dict):
        return None
    inner = payload.get("payload") if isinstance(payload.get("payload"), dict) else payload
    for key in ("node_key", "stage_id", "node_id"):
        value = inner.get(key) if isinstance(inner, dict) else None
        if isinstance(value, str) and value:
            return value
    return None


def sign(secret: bytes, body: bytes) -> str:
    """``X-MC-Signature`` value: ``sha256=<hex hmac>`` over the raw request body."""

    return "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()


def verify_signature(secret: bytes, body: bytes, header: str) -> bool:
    return hmac.compare_digest(sign(secret, body), header)


def retry_delay(
    failures: int,
    jitter: float,
    *,
    base: timedelta = timedelta(seconds=2),
    cap: timedelta = timedelta(minutes=15),
) -> timedelta:
    """Exponential backoff with full jitter: uniform in [0, min(cap, base * 2**(n-1))]."""

    if failures < 1:
        raise ValueError("retry_delay is for the n-th failure, n >= 1")
    if not 0 <= jitter < 1:
        raise ValueError("jitter is a uniform sample in [0, 1)")
    ceiling = min(cap, base * (2 ** min(failures - 1, 30)))
    return timedelta(seconds=max(0.5, ceiling.total_seconds() * jitter))


class FailureDecision(SubscriptionContract):
    failure_count: int
    dead_letter: bool
    next_attempt_at: AwareDatetime


def after_failure(failure_count: int, now: datetime, jitter: float) -> FailureDecision:
    count = failure_count + 1
    return FailureDecision(
        failure_count=count,
        dead_letter=count >= DEAD_LETTER_AFTER,
        next_attempt_at=now + retry_delay(count, jitter),
    )


def sse_frame(event: str, data: str, *, event_id: str | None = None) -> str:
    lines = [f"event: {event}"]
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.extend(f"data: {line}" for line in data.splitlines() or [""])
    return "\n".join(lines) + "\n\n"


def can_close(subscription: Subscription, actor_id: str, permissions: frozenset[str]) -> bool:
    """Creators close with ``workflow_run.read``; anyone else needs ``workflow_run.admin``."""

    if ADMIN_PERMISSION in permissions:
        return True
    return subscription.actor_ref == actor_id and SUBSCRIBE_PERMISSION in permissions
