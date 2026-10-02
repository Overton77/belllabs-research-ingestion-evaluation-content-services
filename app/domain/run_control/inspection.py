"""`CON-CP-INSPECTION-READ-V1`: scoped, non-mutating inspection reads (REQ-CP-RUN-011/012).

The envelope (`InspectionRead`) carries `data` plus a `sections` map. Every section states
its source, observation time, freshness, reconciliation state, and the redaction applied,
so a reader can tell persisted authority from qualified runtime evidence and from data
that is stale or unavailable. Nothing here is a lifecycle authority: these contracts are
read models built from PostgreSQL authority, immutable detail documents, Temporal
Visibility (Search Attributes only), and the registered checkpointer.

The redaction policy is allowlist-based. Checkpoint bodies, message and tool-argument
content, transcripts, and secret values are never part of a read model; a redacted
checkpoint state summary exposes only the allowlisted facts below and requires a
separate permission.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Final, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from app.domain.control_plane.canonical import sha256_digest, stable_json_dump
from app.domain.control_plane.contracts import ExactDefinitionRef
from app.domain.graph_runtime.identities import (
    DIGEST_PATTERN,
    UNIT_KEY_PATTERN,
    QualifiedCheckpointKey,
    RuntimeUnitIdentity,
)
from app.domain.operation_execution.checkpoint_lineage import (
    ActivityAttemptObservation,
    CheckpointTransitionObservation,
    LineageWriteRejection,
    UnitReconciliationIncident,
    UnitResultObservation,
)
from app.domain.run_control.contracts import (
    BudgetDimensionLimit,
    EffectDisposition,
    EffectSettlementOutcome,
    RunOutcome,
    RunPhase,
    RunProjection,
    UnitReconciliationDecision,
    WaitCondition,
)
from app.domain.run_control.errors import RunControlError

INSPECTION_READ_SCHEMA_VERSION: Final = "belllabs.inspection-read.v1"
INSPECTION_REDACTION_POLICY_REF: Final = "belllabs.inspection-redaction.v1"
CHECKPOINT_SUMMARY_SCHEMA_VERSION: Final = "belllabs.redacted-checkpoint-summary.v1"

# Permissions (application authorization; RLS is defense in depth).
INSPECTION_READ_PERMISSION: Final = "workflow_run.read"
CHECKPOINT_SUMMARY_PERMISSION: Final = "workflow_run.read_checkpoint_summary"

MAX_PAGE_SIZE: Final = 100
DEFAULT_PAGE_SIZE: Final = 25
MAX_ARTIFACT_INDEX_KEYS: Final = 64

SectionSource = Literal[
    "postgres_authority",
    "temporal_visibility",
    "checkpointer",
    "mongo_detail",
    "diagnostic_query",
]
Freshness = Literal["current", "stale", "unavailable"]
ReconciliationState = Literal["none", "pending", "in_doubt", "operator_required"]
UnitStatus = Literal["pending", "active", "settled", "in_doubt", "superseded"]
LeaseState = Literal["held", "expired", "released"]
CheckpointRole = Literal[
    "expected_source",
    "result",
    "namespace_head",
    "incident_candidate",
    "accepted_descendant",
]

_RECONCILIATION_ORDER: Final = ("none", "pending", "in_doubt", "operator_required")


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --- Errors (typed; never coerced) -------------------------------------------------------


class InvalidInspectionCursor(RunControlError):
    code = "invalid_cursor"
    status_code = 400


class ExpiredInspectionCursor(RunControlError):
    code = "cursor_expired"
    status_code = 410


class InspectionNotFound(RunControlError):
    """Unknown, or outside the caller's scope: the two are indistinguishable by design."""

    code = "inspection_not_found"
    status_code = 404


class CheckpointNotInUnitLineage(RunControlError):
    code = "checkpoint_not_in_unit_lineage"
    status_code = 404


class IncompatibleCheckpoint(RunControlError):
    """Stamped binding or state-schema digest differs from the unit's binding (RUN-011)."""

    code = "incompatible_checkpoint"
    status_code = 409


class InspectionSourceUnavailable(RunControlError):
    code = "inspection_source_unavailable"
    status_code = 503


# --- Envelope ----------------------------------------------------------------------------


class RedactionMarker(Contract):
    policy_ref: str = INSPECTION_REDACTION_POLICY_REF
    withheld_field_count: int = Field(default=0, ge=0)


class InspectionSection(Contract):
    source: SectionSource
    observed_at: AwareDatetime | None = None
    projection_version: int | None = Field(default=None, ge=1)
    freshness: Freshness
    reconciliation_state: ReconciliationState = "none"
    redaction: RedactionMarker = Field(default_factory=RedactionMarker)
    # A typed reason for `stale` or `unavailable` (never a provider message or secret).
    reason: str | None = Field(default=None, max_length=128)


class InspectionRead[T](Contract):
    schema_version: Literal["belllabs.inspection-read.v1"] = INSPECTION_READ_SCHEMA_VERSION
    data: T
    sections: dict[str, InspectionSection]


def worst_reconciliation_state(states: Sequence[ReconciliationState]) -> ReconciliationState:
    worst: ReconciliationState = "none"
    for state in states:
        if _RECONCILIATION_ORDER.index(state) > _RECONCILIATION_ORDER.index(worst):
            worst = state
    return worst


# --- Read models -------------------------------------------------------------------------


class RunListFilter(Contract):
    phases: frozenset[RunPhase] = Field(default_factory=frozenset)


class RunListItem(Contract):
    run_id: str
    request_scope: str
    version: int = Field(ge=1)
    phase: RunPhase
    terminal_outcome: RunOutcome | None = None
    workflow_type_ref: ExactDefinitionRef
    effective_configuration_digest: str = Field(pattern=DIGEST_PATTERN)
    reconciliation_state: ReconciliationState
    updated_at: AwareDatetime


class RunListPage(Contract):
    items: tuple[RunListItem, ...]
    next_cursor: str | None = None


class BudgetSummary(Contract):
    account_id: str
    limits: tuple[BudgetDimensionLimit, ...]
    reserved: dict[str, int] = Field(default_factory=dict)
    consumed: dict[str, int] = Field(default_factory=dict)
    pending_settlement: dict[str, int] = Field(default_factory=dict)
    reservation_ids: tuple[str, ...] = ()
    outstanding_usage_count: int = Field(default=0, ge=0)


class EffectStatus(Contract):
    effect_id: str
    effect_kind: str
    operation_ref: str
    disposition: EffectDisposition
    observed_dispositions: tuple[EffectDisposition, ...] = ()
    settlement_outcome: EffectSettlementOutcome | None = None
    ambiguous: bool = False


class TemporalExecution(Contract):
    """One execution as Temporal Visibility reports it (Search Attributes only)."""

    workflow_id: str
    temporal_run_id: str
    workflow_type: str
    status: str
    workflow_kind: str | None = None
    unit_key: str | None = None
    execution_generation: int | None = None
    started_at: AwareDatetime | None = None
    closed_at: AwareDatetime | None = None


class AsyncChildInspection(Contract):
    """Parent-owned async child: BellLabs authority plus the optional immutable detail.

    Lifecycle values are opaque strings so that newly specified states (for example
    `in_doubt`) are shown as recorded rather than rejected.
    """

    child_execution_id: str
    parent_operation_id: str
    link_id: str
    contract_id: str
    binding_digest: str
    execution_generation: int = Field(ge=1)
    dependency_class: str
    lifecycle: str | None = None
    result_decision: str | None = None
    result_manifest_digest: str | None = None
    settlement_ref: str | None = None
    cancellation_requested: bool = False
    # From the immutable detail document: the exact parent binding that spawned the child.
    parent_binding_id: str | None = None
    provider_thread_id: str | None = None
    provider_run_id: str | None = None
    detail_lifecycle: str | None = None


class UnitSummary(Contract):
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    unit_kind: str
    family: str
    semantic_operation_id: str
    semantic_attempt: int = Field(ge=1)
    latest_generation: int | None = Field(default=None, ge=1)
    status: UnitStatus
    reconciliation_state: ReconciliationState


class RunInspection(Contract):
    projection: RunProjection
    reconciliation_state: ReconciliationState
    operator_reconciliation_waits: tuple[WaitCondition, ...] = ()
    output_refs: tuple[str, ...] = ()
    budget: BudgetSummary | None = None
    effects: tuple[EffectStatus, ...] = ()
    units: tuple[UnitSummary, ...] = ()
    async_children: tuple[AsyncChildInspection, ...] = ()
    temporal_executions: tuple[TemporalExecution, ...] = ()


class TechnicalAttempt(Contract):
    technical_attempt: int = Field(ge=1)
    provider: str
    disposition: str
    retry_class: str
    started_at: AwareDatetime
    finished_at: AwareDatetime | None = None
    failure_code: str | None = None


class JournalSettlementSummary(Contract):
    settlement_id: str
    settlement_revision: int = Field(ge=1)
    status: str
    result_manifest_ref: str | None = None
    result_manifest_digest: str | None = None
    failure_code: str | None = None
    usage: dict[str, int] = Field(default_factory=dict)
    pending_external_usage: dict[str, int] = Field(default_factory=dict)
    settled_at: AwareDatetime


class JournalClaimInspection(Contract):
    effect_claim_id: str
    semantic_binding_id: str
    semantic_binding_digest: str
    status: str
    claimed_at: AwareDatetime
    technical_attempts: tuple[TechnicalAttempt, ...] = ()
    settlements: tuple[JournalSettlementSummary, ...] = ()


class GenerationInspection(Contract):
    execution_generation: int = Field(ge=1)
    claim_fence: int = Field(ge=1)
    binding_id: str
    binding_digest: str = Field(pattern=DIGEST_PATTERN)
    cognitive_namespace: str | None = None
    state_schema_digest: str | None = None
    lease_holder: str | None = None
    lease_expires_at: AwareDatetime | None = None
    lease_state: LeaseState
    superseded: bool = False
    attempts: tuple[ActivityAttemptObservation, ...] = ()
    transition: CheckpointTransitionObservation | None = None
    result: UnitResultObservation | None = None
    incidents: tuple[UnitReconciliationIncident, ...] = ()
    rejections: tuple[LineageWriteRejection, ...] = ()
    namespace_head: QualifiedCheckpointKey | None = None
    namespace_in_flight: bool = False
    status: UnitStatus
    reconciliation_state: ReconciliationState


class UnitInspection(Contract):
    unit: RuntimeUnitIdentity
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    run_phase: RunPhase
    status: UnitStatus
    reconciliation_state: ReconciliationState
    generations: tuple[GenerationInspection, ...]
    journal: tuple[JournalClaimInspection, ...] = ()
    effects: tuple[EffectStatus, ...] = ()
    # Accepted `reconcile_unit` commands targeting this unit, and their pending waits.
    reconciliation_decisions: tuple[UnitReconciliationDecision, ...] = ()
    operator_reconciliation_waits: tuple[WaitCondition, ...] = ()
    async_children: tuple[AsyncChildInspection, ...] = ()
    temporal_executions: tuple[TemporalExecution, ...] = ()


class CheckpointObservation(Contract):
    """Metadata of one root checkpoint as the registered saver reports it; never a body."""

    key: QualifiedCheckpointKey
    step: int | None = None
    source: str | None = None
    created_at: str | None = Field(default=None, max_length=64)
    stamps: dict[str, str | int] = Field(default_factory=dict)
    pending_task_names: tuple[str, ...] = ()
    withheld_metadata_fields: int = Field(default=0, ge=0)


class CheckpointHistoryEntry(Contract):
    key: QualifiedCheckpointKey
    step: int | None = None
    source: str | None = None
    created_at: str | None = None
    stamped: bool
    binding_compatible: bool
    state_schema_compatible: bool
    roles: tuple[CheckpointRole, ...] = ()
    pending_task_names: tuple[str, ...] = ()


class CheckpointHistoryPage(Contract):
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    namespace: str
    checkpointer_ref_digest: str | None = None
    recorded_keys: tuple[QualifiedCheckpointKey, ...] = ()
    entries: tuple[CheckpointHistoryEntry, ...] = ()
    next_cursor: str | None = None


class RedactedStateFacts(Contract):
    """The allowlisted facts of one checkpoint's channel values (RUN-012 minimum)."""

    channel_names: tuple[str, ...]
    message_count: int = Field(ge=0)
    todo_count: int | None = Field(default=None, ge=0)
    artifact_index_keys: tuple[str, ...] = ()
    artifact_index_count: int = Field(default=0, ge=0)
    has_structured_response: bool = False
    withheld_value_count: int = Field(ge=0)


class RedactedCheckpointStateSummary(Contract):
    schema_version: Literal["belllabs.redacted-checkpoint-summary.v1"] = (
        CHECKPOINT_SUMMARY_SCHEMA_VERSION
    )
    key: QualifiedCheckpointKey
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    step: int | None = None
    facts: RedactedStateFacts
    pending_task_names: tuple[str, ...] = ()
    stamped_digests: dict[str, str | int] = Field(default_factory=dict)
    summary_digest: str = Field(pattern=DIGEST_PATTERN)
    observed_at: AwareDatetime


def summarize_channel_values(values: Mapping[str, object]) -> RedactedStateFacts:
    """Reduce checkpoint channel values to allowlisted counts and names only.

    No message, tool argument, file, todo text, structured response, or other channel value
    leaves this function; every channel value counts as withheld. Callers pass state
    channels only (LangGraph control channels are not state).
    """

    messages = values.get("messages")
    todos = values.get("todos")
    artifact_index = values.get("artifact_index")
    keys: tuple[str, ...] = ()
    count = 0
    if isinstance(artifact_index, Mapping):
        count = len(artifact_index)
        keys = tuple(sorted(str(key) for key in artifact_index))[:MAX_ARTIFACT_INDEX_KEYS]
    return RedactedStateFacts(
        channel_names=tuple(sorted(str(name) for name in values)),
        message_count=len(messages) if isinstance(messages, Sequence) else 0,
        todo_count=len(todos) if isinstance(todos, Sequence) else None,
        artifact_index_keys=keys,
        artifact_index_count=count,
        has_structured_response=values.get("structured_response") is not None,
        withheld_value_count=len(values),
    )


def checkpoint_summary_digest(
    key: QualifiedCheckpointKey,
    facts: RedactedStateFacts,
    pending_task_names: Sequence[str],
    stamped_digests: Mapping[str, str | int],
) -> str:
    return sha256_digest(
        {
            "key": stable_json_dump(key),
            "facts": stable_json_dump(facts),
            "pending_task_names": sorted(pending_task_names),
            "stamped_digests": dict(sorted(stamped_digests.items())),
        }
    )


def lease_state(
    lease_holder: str | None, lease_expires_at: datetime | None, now: datetime
) -> LeaseState:
    if lease_holder is None or lease_expires_at is None:
        return "released"
    return "held" if lease_expires_at > now else "expired"
