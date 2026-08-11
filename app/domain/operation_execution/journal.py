from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal, cast

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import DIGEST_PATTERN


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EffectClaimStatus(StrEnum):
    CLAIMED = "claimed"
    EXECUTING = "executing"
    SETTLED = "settled"
    RECONCILIATION_REQUIRED = "reconciliation_required"
    CANCELLED = "cancelled"


class TechnicalAttemptDisposition(StrEnum):
    CREATED = "created"
    RUNNING = "running"
    AMBIGUOUS = "ambiguous"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class OperationEffectClaim(Contract):
    effect_claim_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    belllabs_run_id: str = Field(min_length=1)
    operation_contract_digest: str = Field(pattern=DIGEST_PATTERN)
    idempotency_key: str = Field(min_length=1)
    request_digest: str = Field(pattern=DIGEST_PATTERN)
    semantic_binding_id: str = Field(min_length=1)
    semantic_binding_digest: str = Field(pattern=DIGEST_PATTERN)
    semantic_attempt_key: str = Field(min_length=1)
    claim_mode: Literal["active", "shadow"] = "active"
    status: EffectClaimStatus = EffectClaimStatus.CLAIMED
    claimed_by: str = Field(min_length=1)
    claimed_at: AwareDatetime
    heartbeat_at: AwareDatetime | None = None
    lease_expires_at: AwareDatetime | None = None


class OperationTechnicalAttempt(Contract):
    operation_attempt_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    effect_claim_id: str = Field(min_length=1)
    technical_attempt: int = Field(ge=1)
    provider: str = Field(min_length=1)
    provider_attempt_id: str | None = None
    disposition: TechnicalAttemptDisposition
    idempotency_supported: bool
    retry_class: Literal["safe", "claim_then_reconcile", "non_retryable"]
    usage: dict[str, int] = Field(default_factory=dict)
    started_at: AwareDatetime
    finished_at: AwareDatetime | None = None
    failure_code: str | None = None

    @model_validator(mode="after")
    def retry_policy_matches_provider(self) -> OperationTechnicalAttempt:
        if not self.idempotency_supported and self.retry_class == "safe":
            raise ValueError("non-idempotent provider attempts cannot be blindly retried")
        if any(not dimension or amount < 0 for dimension, amount in self.usage.items()):
            raise ValueError("attempt usage must be non-negative and dimensioned")
        return self


class OperationJournalSettlement(Contract):
    settlement_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    effect_claim_id: str = Field(min_length=1)
    settlement_revision: int = Field(ge=1)
    digest_version: Literal["legacy-v1", "complete-v2"]
    settlement_digest: str = Field(pattern=DIGEST_PATTERN)
    status: Literal[
        "completed",
        "failed",
        "cancelled",
        "timed_out",
        "reconciliation_required",
    ]
    usage: dict[str, int] = Field(default_factory=dict)
    released_usage: dict[str, int] = Field(default_factory=dict)
    pending_external_usage: dict[str, int] = Field(default_factory=dict)
    result_manifest_ref: str | None = None
    result_manifest_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    result_manifest_size_bytes: int | None = Field(default=None, ge=1)
    failure_code: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)
    settled_at: AwareDatetime

    @model_validator(mode="before")
    @classmethod
    def detect_legacy_digest_shape(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        payload = dict(value)
        if "digest_version" not in payload:
            if "released_usage" in payload:
                raise ValueError(
                    "digest_version is required when released_usage is present"
                )
            payload["digest_version"] = "legacy-v1"
            payload["released_usage"] = {}
        return payload

    @field_validator("detail")
    @classmethod
    def detail_has_no_secret_values(cls, value: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "schema_version",
            "reason_code",
            "reconciliation_ref",
            "provider_status_code",
        }
        if not set(value) <= allowed:
            raise ValueError("settlement detail accepts governed operational metadata only")
        if any(not isinstance(item, str | int | bool | None) for item in value.values()):
            raise ValueError("settlement detail values must be scalar operational metadata")
        return value

    @model_validator(mode="after")
    def terminal_shape_is_consistent(self) -> OperationJournalSettlement:
        manifest_fields = (
            self.result_manifest_ref,
            self.result_manifest_digest,
            self.result_manifest_size_bytes,
        )
        if any(value is not None for value in manifest_fields) and not all(
            value is not None for value in manifest_fields
        ):
            raise ValueError("result manifest ref, digest, and size form one exact address")
        if self.status == "completed" and self.failure_code is not None:
            raise ValueError("completed settlements cannot carry a failure code")
        if any(
            not dimension or amount < 0
            for usage in (
                self.usage,
                self.released_usage,
                self.pending_external_usage,
            )
            for dimension, amount in usage.items()
        ):
            raise ValueError("settlement usage must be non-negative and dimensioned")
        if self.digest_version == "legacy-v1":
            if self.settlement_revision != 1 or self.released_usage:
                raise ValueError("legacy settlement digest shape is not canonical")
            content = self.model_dump(
                mode="json",
                exclude={
                    "settlement_digest",
                    "digest_version",
                    "released_usage",
                },
            )
        else:
            content = self.model_dump(mode="json", exclude={"settlement_digest"})
        if sha256_digest(content) != self.settlement_digest:
            raise ValueError("operation journal settlement digest mismatch")
        return self

    @classmethod
    def create(cls, **values: object) -> OperationJournalSettlement:
        values = dict(values)
        values.pop("digest_version", None)
        values.pop("settlement_digest", None)
        draft = cast(Any, cls).model_construct(
            **values,
            digest_version="complete-v2",
            settlement_digest="sha256:" + "0" * 64,
        )
        digest = sha256_digest(draft.model_dump(mode="json", exclude={"settlement_digest"}))
        return cls(
            **values,
            digest_version="complete-v2",
            settlement_digest=digest,
        )


class OperationClaimResult(Contract):
    status: Literal["acquired", "existing", "conflict", "shadow_denied"]
    claim: OperationEffectClaim | None = None
    reason: str = Field(min_length=1)
