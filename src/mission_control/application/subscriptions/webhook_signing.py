"""Signed webhook deliveries: signature, timestamp and delivery id (SPEC-04, ADR-0040, MP-15).

Every webhook request (durable Subscription events and coordinator inbox callbacks) carries:

- `X-MC-Signature`: `sha256=<hex>` HMAC-SHA256 over the raw body (FT-F5, unchanged, so
  existing receivers keep verifying);
- `X-MC-Timestamp`: Unix seconds at signing;
- `X-MC-Delivery-Id`: stable for one (subscription, delivered item) across every retry, so a
  receiver dedupes at-least-once delivery on it;
- `X-MC-Delivery-Attempt`: 1-based attempt number;
- `X-MC-Signature-256`: `t=<timestamp>,v1=<hex>` HMAC-SHA256 over
  `v1:<timestamp>:<delivery id>:` followed by the raw body. Binding the timestamp and the
  delivery id stops a captured body from being replayed under a fresh timestamp or id.

`verify_delivery` is what a receiver runs: all headers present, the timestamp within the
tolerance (default 300 s either way), the v1 signature equal in constant time, and (when
given) the body-only signature too. Retries follow `domain.subscriptions.contracts`
(`retry_delay`: exponential from 2 s, capped at 15 min, full jitter; the twelfth consecutive
failure dead-letters). A retry re-sends the same body; it never reruns agent work.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final, Literal
from uuid import UUID, uuid5

from mission_control.domain.subscriptions.contracts import (
    DEAD_LETTER_AFTER,
    SIGNATURE_HEADER,
    retry_delay,
    sign,
)

TIMESTAMP_HEADER: Final = "X-MC-Timestamp"
DELIVERY_ID_HEADER: Final = "X-MC-Delivery-Id"
DELIVERY_ATTEMPT_HEADER: Final = "X-MC-Delivery-Attempt"
SIGNATURE_V1_HEADER: Final = "X-MC-Signature-256"
DEFAULT_TOLERANCE: Final = timedelta(minutes=5)
_EVENT_DELIVERY_NAMESPACE: Final = UUID("8d1c6f0a-7b2e-4f3d-9a51-6e4b2c7d8f90")
# Failures no retry may repeat: a destination that resolves into a blocked network is
# dead-lettered at once rather than probed twelve times.
NON_RETRYABLE_ERRORS: Final = frozenset({"egress_rejected"})


def event_delivery_id(subscription_id: UUID, canonical_event_id: UUID) -> UUID:
    """Stable delivery id of one canonical event to one Subscription."""

    return uuid5(_EVENT_DELIVERY_NAMESPACE, f"v1:{subscription_id}:{canonical_event_id}")


def signing_input(timestamp: int, delivery: UUID | str, body: bytes) -> bytes:
    return f"v1:{timestamp}:{delivery}:".encode() + body


def sign_v1(secret: bytes, timestamp: int, delivery: UUID | str, body: bytes) -> str:
    digest = hmac.new(secret, signing_input(timestamp, delivery, body), hashlib.sha256)
    return f"t={timestamp},v1={digest.hexdigest()}"


def signed_headers(
    secret: bytes,
    body: bytes,
    *,
    delivery: UUID,
    attempt: int,
    now: datetime,
    extra: Mapping[str, str] | None = None,
) -> dict[str, str]:
    timestamp = int(now.timestamp())
    headers = {
        "Content-Type": "application/json",
        SIGNATURE_HEADER: sign(secret, body),
        TIMESTAMP_HEADER: str(timestamp),
        DELIVERY_ID_HEADER: str(delivery),
        DELIVERY_ATTEMPT_HEADER: str(attempt),
        SIGNATURE_V1_HEADER: sign_v1(secret, timestamp, delivery, body),
    }
    headers.update(extra or {})
    return headers


VerificationFailure = Literal[
    "missing_header", "malformed_signature", "stale_timestamp", "bad_signature"
]


@dataclass(frozen=True)
class Verification:
    ok: bool
    failure: VerificationFailure | None = None
    delivery_id: str | None = None
    timestamp: int | None = None


def verify_delivery(
    secret: bytes,
    body: bytes,
    headers: Mapping[str, str],
    *,
    now: datetime,
    tolerance: timedelta = DEFAULT_TOLERANCE,
) -> Verification:
    """Receiver-side verification (header names are matched case-insensitively)."""

    lowered = {name.lower(): value for name, value in headers.items()}
    signature = lowered.get(SIGNATURE_V1_HEADER.lower())
    stamp = lowered.get(TIMESTAMP_HEADER.lower())
    delivery = lowered.get(DELIVERY_ID_HEADER.lower())
    if signature is None or stamp is None or delivery is None:
        return Verification(False, "missing_header")
    parts = dict(part.split("=", 1) for part in signature.split(",") if "=" in part)
    if "t" not in parts or "v1" not in parts or parts["t"] != stamp or not stamp.isdigit():
        return Verification(False, "malformed_signature", delivery)
    timestamp = int(stamp)
    if abs(now.timestamp() - timestamp) > tolerance.total_seconds():
        return Verification(False, "stale_timestamp", delivery, timestamp)
    if not hmac.compare_digest(sign_v1(secret, timestamp, delivery, body), signature):
        return Verification(False, "bad_signature", delivery, timestamp)
    legacy = lowered.get(SIGNATURE_HEADER.lower())
    if legacy is not None and not hmac.compare_digest(sign(secret, body), legacy):
        return Verification(False, "bad_signature", delivery, timestamp)
    return Verification(True, None, delivery, timestamp)


def retry_schedule(jitter: float = 1.0) -> tuple[timedelta, ...]:
    """The backoff ceilings (jitter 1.0 approaches the cap) before dead-lettering."""

    return tuple(
        retry_delay(failure, min(jitter, 0.999999)) for failure in range(1, DEAD_LETTER_AFTER)
    )


__all__ = [
    "DEFAULT_TOLERANCE",
    "DELIVERY_ATTEMPT_HEADER",
    "DELIVERY_ID_HEADER",
    "NON_RETRYABLE_ERRORS",
    "SIGNATURE_V1_HEADER",
    "TIMESTAMP_HEADER",
    "Verification",
    "event_delivery_id",
    "retry_schedule",
    "sign_v1",
    "signed_headers",
    "signing_input",
    "verify_delivery",
]
