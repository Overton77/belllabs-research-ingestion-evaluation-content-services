"""Compact graph checkpoint state; durable bodies remain behind references."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.domain.graph_runtime.identities import DIGEST_PATTERN
from app.domain.graph_runtime.kernel import DecisionRequest

MAX_COMPACT_VALUE_BYTES = 4_096
_SENSITIVE_FRAGMENTS = ("secret", "token", "password", "credential", "api_key", "apikey", "phi")
_LARGE_PAYLOAD_FRAGMENTS = ("payload", "transcript", "content", "body", "raw", "messages")


class CommonStateMetadata(BaseModel):
    """Only immutable identity, references, digests, and small projections are checkpointed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    runtime_binding_ref: str = Field(min_length=1, max_length=1_024)
    definition_digest: str = Field(pattern=DIGEST_PATTERN)
    assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    lifecycle_projection_ref: str = Field(min_length=1, max_length=1_024)
    lifecycle_projection_version: int = Field(ge=1)
    pending_decisions: tuple[DecisionRequest, ...] = ()
    outbox_position: int = Field(default=0, ge=0)
    redacted_diagnostic_refs: tuple[str, ...] = ()
    final_result_ref: str | None = Field(default=None, min_length=1, max_length=1_024)

    @field_validator("redacted_diagnostic_refs")
    @classmethod
    def diagnostics_are_references_only(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        _reject_noncompact_payload(value)
        return value

    @field_validator("pending_decisions")
    @classmethod
    def decisions_have_distinct_ids(
        cls, value: tuple[DecisionRequest, ...]
    ) -> tuple[DecisionRequest, ...]:
        if len({item.decision_id for item in value}) != len(value):
            raise ValueError("pending decisions must have distinct decision IDs")
        return value


def _reject_noncompact_payload(value: Any) -> None:
    """Reject secret-bearing or body-like values before they enter graph state."""

    encoded = json.dumps(value, default=str, separators=(",", ":"))
    if len(encoded.encode()) > MAX_COMPACT_VALUE_BYTES:
        raise ValueError("common state may contain compact references and digests only")

    def inspect(item: Any) -> None:
        if isinstance(item, dict):
            for key, nested in item.items():
                normalized = str(key).lower().replace("-", "_")
                if any(fragment in normalized for fragment in _SENSITIVE_FRAGMENTS):
                    raise ValueError("common state cannot contain sensitive payloads")
                if any(fragment in normalized for fragment in _LARGE_PAYLOAD_FRAGMENTS) and not (
                    normalized.endswith("_ref") or normalized.endswith("_digest")
                ):
                    raise ValueError("common state cannot contain payload bodies")
                inspect(nested)
        elif isinstance(item, list | tuple):
            for nested in item:
                inspect(nested)
        elif isinstance(item, str) and len(item.encode()) > MAX_COMPACT_VALUE_BYTES:
            raise ValueError("common state cannot contain large strings")

    inspect(value)
