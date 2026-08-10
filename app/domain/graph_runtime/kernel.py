"""Frozen, provider-neutral primitives shared by durable graph runtimes."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from app.domain.graph_runtime.identities import DIGEST_PATTERN, IDENTIFIER_PATTERN


class KernelContract(BaseModel):
    """A compact contract suitable for a checkpoint or durable journal."""

    model_config = ConfigDict(extra="forbid", frozen=True)


class OperationFailureClass(StrEnum):
    AUTHORITY_DENIED = "authority_denied"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"
    CAPABILITY_DRIFT = "capability_drift"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    BUDGET_EXHAUSTED = "budget_exhausted"
    APPROVAL_REQUIRED = "approval_required"
    TRANSIENT_PROVIDER_FAILURE = "transient_provider_failure"
    AMBIGUOUS_EXTERNAL_EFFECT = "ambiguous_external_effect"
    INVALID_RESULT_CONTRACT = "invalid_result_contract"
    INCOMPATIBLE_RESUME = "incompatible_resume"
    CANCELLED = "cancelled"
    INTERNAL_INVARIANT_VIOLATION = "internal_invariant_violation"


class OperationFailureClassV2(StrEnum):
    AUTHORITY_DENIED = "authority_denied"
    CAPABILITY_UNAVAILABLE = "capability_unavailable"
    CAPABILITY_DRIFT = "capability_drift"
    RESOURCE_EXHAUSTED = "resource_exhausted"
    BUDGET_EXHAUSTED = "budget_exhausted"
    APPROVAL_REQUIRED = "approval_required"
    TRANSIENT_PROVIDER_FAILURE = "transient_provider_failure"
    AMBIGUOUS_EXTERNAL_EFFECT = "ambiguous_external_effect"
    INVALID_RESULT_CONTRACT = "invalid_result_contract"
    INCOMPATIBLE_RESUME = "incompatible_resume"
    STALE_EXECUTION_GENERATION = "stale_execution_generation"
    CANCELLED = "cancelled"
    INTERNAL_INVARIANT_VIOLATION = "internal_invariant_violation"


class LineageKind(StrEnum):
    BELL_LABS_RUN = "belllabs_run"
    EXECUTION_EPOCH = "execution_epoch"
    SEMANTIC_OPERATION_ATTEMPT = "semantic_operation_attempt"
    RUNTIME_ATTEMPT = "runtime_attempt"
    AGENT_INVOCATION = "agent_invocation"
    AGENT_THREAD = "agent_thread"
    AGENT_RUN = "agent_run"
    ASYNC_TASK = "async_task"
    EFFECT_CLAIM = "effect_claim"
    USAGE_SETTLEMENT = "usage_settlement"
    ARTIFACT = "artifact"
    RESULT_MANIFEST = "result_manifest"
    TRACE = "trace"


class ProviderQualifiedLineageRecord(KernelContract):
    """One identity in a lineage; identities from different providers never collide."""

    kind: LineageKind
    provider: str = Field(pattern=IDENTIFIER_PATTERN)
    provider_identity: str = Field(pattern=IDENTIFIER_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    canonical_digest: str = Field(pattern=DIGEST_PATTERN)
    manifest_ref: str | None = Field(default=None, min_length=1, max_length=1_024)

    @property
    def canonical_key(self) -> str:
        return (
            f"{self.kind.value}|{len(self.provider)}:{self.provider}"
            f"|{len(self.provider_identity)}:{self.provider_identity}"
        )


class LineageParentEdge(KernelContract):
    child: ProviderQualifiedLineageRecord
    parent: ProviderQualifiedLineageRecord
    relationship: Literal[
        "contains",
        "attempt_of",
        "invokes",
        "spawns",
        "produces",
        "traces",
        "claims",
    ]

    @model_validator(mode="after")
    def edge_cannot_be_self_referential(self) -> LineageParentEdge:
        if self.child.canonical_key == self.parent.canonical_key:
            raise ValueError("lineage edges require distinct parent and child identities")
        if self.child.request_scope != self.parent.request_scope:
            raise ValueError("lineage edges cannot cross request scopes")
        return self


class DecisionRequest(KernelContract):
    decision_id: str = Field(pattern=IDENTIFIER_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    binding_id: str = Field(pattern=IDENTIFIER_PATTERN)
    decision_type: str = Field(pattern=IDENTIFIER_PATTERN)
    schema_ref: str = Field(min_length=1, max_length=1_024)
    choices_ref: str | None = Field(default=None, min_length=1, max_length=1_024)
    evidence_refs: tuple[str, ...] = ()
    expected_lifecycle_version: int = Field(ge=1)
    policy_ref: str = Field(min_length=1, max_length=1_024)
    request_digest: str = Field(pattern=DIGEST_PATTERN)
    requested_at: AwareDatetime
    expires_at: AwareDatetime | None = None


class DecisionResponse(KernelContract):
    decision_id: str = Field(pattern=IDENTIFIER_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    response_id: str = Field(pattern=IDENTIFIER_PATTERN)
    response_schema_ref: str = Field(min_length=1, max_length=1_024)
    response_payload_ref: str = Field(min_length=1, max_length=1_024)
    response_digest: str = Field(pattern=DIGEST_PATTERN)
    expected_lifecycle_version: int = Field(ge=1)
    actor_ref: str = Field(min_length=1, max_length=1_024)
    decided_at: AwareDatetime


class ResourceKind(StrEnum):
    TENANT = "tenant"
    WORKFLOW_RUN = "workflow_run"
    STAGE = "stage"
    OPERATION_WORKER = "operation_worker"
    RESUMPTION = "resumption"
    MODEL_CALL = "model_call"
    TOOL_CALL = "tool_call"
    MCP_CALL = "mcp_call"
    SYNC_SUBAGENT = "sync_subagent"
    ASYNC_CHILD = "async_child"
    LINKED_RUN = "linked_run"
    PROVIDER_QUOTA = "provider_quota"
    BUDGET_RESERVATION = "budget_reservation"


RESOURCE_ACQUISITION_ORDER: tuple[ResourceKind, ...] = tuple(ResourceKind)


class ResourceLeaseRequest(KernelContract):
    lease_id: str = Field(pattern=IDENTIFIER_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    semantic_identity: str = Field(min_length=1, max_length=1_024)
    envelope_digest: str = Field(pattern=DIGEST_PATTERN)
    resources: tuple[ResourceKind, ...] = Field(min_length=1)
    requested_at: AwareDatetime
    deadline: AwareDatetime
    ttl_seconds: int = Field(ge=1, le=86_400)

    @field_validator("resources")
    @classmethod
    def resources_are_canonical_and_unique(
        cls, value: tuple[ResourceKind, ...]
    ) -> tuple[ResourceKind, ...]:
        if len(value) != len(set(value)):
            raise ValueError("resource lease requests cannot acquire a resource twice")
        ordered = tuple(sorted(value, key=RESOURCE_ACQUISITION_ORDER.index))
        if value != ordered:
            raise ValueError("resource lease requests must use canonical acquisition order")
        return value

    @model_validator(mode="after")
    def deadline_is_after_request(self) -> ResourceLeaseRequest:
        if self.deadline <= self.requested_at:
            raise ValueError("resource lease deadline must be after its request")
        return self


class ResourceLeaseStatus(StrEnum):
    REQUESTED = "requested"
    ACQUIRED = "acquired"
    RETAINED = "retained"
    RELEASED = "released"
    EXPIRED = "expired"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class ResourceLeaseRecord(KernelContract):
    request: ResourceLeaseRequest
    status: ResourceLeaseStatus
    acquired_at: AwareDatetime | None = None
    expires_at: AwareDatetime | None = None
    released_at: AwareDatetime | None = None
    canonical_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def status_has_consistent_timestamps(self) -> ResourceLeaseRecord:
        if self.status in {ResourceLeaseStatus.ACQUIRED, ResourceLeaseStatus.RETAINED} and (
            self.acquired_at is None or self.expires_at is None
        ):
            raise ValueError("acquired leases require acquisition and expiry times")
        if self.status == ResourceLeaseStatus.RELEASED and self.released_at is None:
            raise ValueError("released leases require a release time")
        return self


class ResourceKindV2(StrEnum):
    TENANT = "tenant"
    ENVIRONMENT = "environment"
    WORKFLOW_RUN = "workflow_run"
    FAMILY_SCHEDULER = "family_scheduler"
    STAGE = "stage"
    OPERATION_WORKFLOW = "operation_workflow"
    OPERATION_WORKER = "operation_worker"
    RESUMPTION = "resumption"
    MODEL_CALL = "model_call"
    TOOL_CALL = "tool_call"
    MCP_CALL = "mcp_call"
    SYNC_SUBAGENT = "sync_subagent"
    ASYNC_CHILD = "async_child"
    LINKED_RUN = "linked_run"
    PROVIDER_QUOTA = "provider_quota"
    BUDGET_RESERVATION = "budget_reservation"


RESOURCE_ACQUISITION_ORDER_V2: tuple[ResourceKindV2, ...] = tuple(ResourceKindV2)


class ResourceLeaseRequestV2(KernelContract):
    schema_version: Literal["belllabs.resource-lease-request.v2"] = (
        "belllabs.resource-lease-request.v2"
    )
    lease_id: str = Field(pattern=IDENTIFIER_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    semantic_identity: str = Field(min_length=1, max_length=1_024)
    envelope_digest: str = Field(pattern=DIGEST_PATTERN)
    resources: tuple[ResourceKindV2, ...] = Field(min_length=1)
    requested_at: AwareDatetime
    deadline: AwareDatetime
    ttl_seconds: int = Field(ge=1, le=86_400)

    @field_validator("resources")
    @classmethod
    def resources_are_canonical_and_unique(
        cls, value: tuple[ResourceKindV2, ...]
    ) -> tuple[ResourceKindV2, ...]:
        if len(value) != len(set(value)):
            raise ValueError("resource lease requests cannot acquire a resource twice")
        ordered = tuple(sorted(value, key=RESOURCE_ACQUISITION_ORDER_V2.index))
        if value != ordered:
            raise ValueError("resource lease requests must use canonical acquisition order")
        return value

    @model_validator(mode="after")
    def deadline_is_after_request(self) -> ResourceLeaseRequestV2:
        if self.deadline <= self.requested_at:
            raise ValueError("resource lease deadline must be after its request")
        return self


class ResourceLeaseRecordV2(KernelContract):
    schema_version: Literal["belllabs.resource-lease-record.v2"] = (
        "belllabs.resource-lease-record.v2"
    )
    request: ResourceLeaseRequestV2
    status: ResourceLeaseStatus
    acquired_at: AwareDatetime | None = None
    expires_at: AwareDatetime | None = None
    released_at: AwareDatetime | None = None
    canonical_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def status_has_consistent_timestamps(self) -> ResourceLeaseRecordV2:
        if self.status in {ResourceLeaseStatus.ACQUIRED, ResourceLeaseStatus.RETAINED} and (
            self.acquired_at is None or self.expires_at is None
        ):
            raise ValueError("acquired leases require acquisition and expiry times")
        if self.status == ResourceLeaseStatus.RELEASED and self.released_at is None:
            raise ValueError("released leases require a release time")
        return self


class WaitLeaseProjection(KernelContract):
    wait_binding_ref: str = Field(min_length=1, max_length=1_024)
    retained_reservations: tuple[str, ...] = ()
    released_reservations: tuple[str, ...] = ()

    @model_validator(mode="after")
    def retained_and_released_are_disjoint(self) -> WaitLeaseProjection:
        if set(self.retained_reservations) & set(self.released_reservations):
            raise ValueError("a wait reservation cannot be both retained and released")
        return self


class CancellationContext(KernelContract):
    cancellation_id: str = Field(pattern=IDENTIFIER_PATTERN)
    requested: bool = False
    requested_at: AwareDatetime | None = None
    deadline: AwareDatetime | None = None
    cascade_policy_ref: str = Field(min_length=1, max_length=1_024)

    @model_validator(mode="after")
    def requested_cancellation_has_timestamp(self) -> CancellationContext:
        if self.requested and self.requested_at is None:
            raise ValueError("requested cancellation requires requested_at")
        return self

    def is_cancelled_or_expired(self, now: datetime) -> bool:
        return self.requested or (self.deadline is not None and now >= self.deadline)
