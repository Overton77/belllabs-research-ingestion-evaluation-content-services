from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.domain.graph_runtime.identities import DIGEST_PATTERN


@dataclass(frozen=True)
class ReducerMergeConflict(ValueError):
    """Fail-closed evidence that two writers claimed one key differently."""

    key: str
    left_digest: str
    right_digest: str

    def __str__(self) -> str:
        return (
            f"canonical digest conflict for key {self.key!r}: "
            f"{self.left_digest!r} != {self.right_digest!r}"
        )


class CanonicalReducerEntry(BaseModel):
    """Small durable value used in conflict-detecting keyed state channels."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str = Field(min_length=1, max_length=1_024)
    canonical_digest: str = Field(pattern=DIGEST_PATTERN)
    value_ref: str | None = Field(default=None, min_length=1, max_length=1_024)


class ReducerConflictIncident(BaseModel):
    """Compact reconciliation input emitted when two canonical claims disagree."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    key: str = Field(min_length=1, max_length=1_024)
    left_digest: str = Field(pattern=DIGEST_PATTERN)
    right_digest: str = Field(pattern=DIGEST_PATTERN)
    incident_kind: str = "canonical_digest_conflict"


def merge_single_assignment[T](left: T | None, right: T | None) -> T | None:
    """Merge a write-once state channel without last-writer-wins behavior."""

    if left is None:
        return right
    if right is None or left == right:
        return left
    raise ValueError("immutable state channel received conflicting assignments")


def merge_monotonic_integer(left: int | None, right: int | None) -> int | None:
    """Merge version/cursor channels as a commutative idempotent maximum."""

    for value in (left, right):
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int) or value < 0
        ):
            raise ValueError("monotonic reducer accepts non-negative integers only")
    if left is None and right is None:
        return None
    return max(value for value in (left, right) if value is not None)


def merge_keyed_canonical_digest[T: Mapping[str, Any] | BaseModel](
    left: Mapping[str, T] | Sequence[T] | None,
    right: Mapping[str, T] | Sequence[T] | None,
) -> dict[str, T]:
    """Merge records by identity, accepting only replayed identical canonical digests.

    Records must expose ``key`` and ``canonical_digest`` fields (as mapping keys or
    Pydantic/model attributes).  The sorted result makes merge order deterministic.
    """

    merged: dict[str, T] = {}
    for record in _records(left) + _records(right):
        key, digest = _canonical_entry(record)
        existing = merged.get(key)
        if existing is None:
            merged[key] = record
            continue
        _, existing_digest = _canonical_entry(existing)
        if existing_digest != digest:
            raise ReducerMergeConflict(key, existing_digest, digest)
    return {key: merged[key] for key in sorted(merged)}


def conflict_incident(error: ReducerMergeConflict) -> ReducerConflictIncident:
    """Turn a reducer conflict into a compact reconciliation incident."""

    return ReducerConflictIncident(
        key=error.key,
        left_digest=error.left_digest,
        right_digest=error.right_digest,
    )


def _canonical_entry(record: Mapping[str, Any] | BaseModel) -> tuple[str, str]:
    if isinstance(record, BaseModel):
        data = record.model_dump(mode="python")
        key = getattr(record, "key", getattr(record, "canonical_key", None))
    else:
        data = dict(record)
        key = data.get("key", data.get("canonical_key"))
    digest = data.get("canonical_digest")
    if not isinstance(key, str) or not key:
        raise ValueError("canonical merge entries require a non-empty key")
    if not isinstance(digest, str) or not digest.startswith("sha256:"):
        raise ValueError("canonical merge entries require a canonical_digest")
    return key, digest


def _records[T](value: Mapping[str, T] | Sequence[T] | None) -> tuple[T, ...]:
    if value is None:
        return ()
    if isinstance(value, Mapping):
        return tuple(value.values())
    return tuple(value)


def merge_unique_events(
    left: Sequence[str] | None,
    right: Sequence[str] | None,
) -> tuple[str, ...]:
    """Deterministically merge compact event references, never event payloads."""

    combined = tuple(left or ()) + tuple(right or ())
    return tuple(dict.fromkeys(combined))
