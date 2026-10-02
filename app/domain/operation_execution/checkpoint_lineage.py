"""`CON-CP-CHECKPOINT-LINEAGE-V1`: cognitive namespaces, stamps, and transition observations.

These contracts are framework-neutral. The Deep Agents adapter maps them onto LangGraph
config; application repositories persist them. A checkpoint is subordinate evidence until
a transition observation is accepted by compare-and-set on its namespace head.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Final, Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import (
    DIGEST_PATTERN,
    UNIT_KEY_PATTERN,
    GoalDirectedUnitLocation,
    QualifiedCheckpointKey,
    RuntimeUnitIdentity,
)

CHECKPOINT_INVOCATION_PLAN_SCHEMA_VERSION: Final = "belllabs.checkpoint-invocation-plan.v1"
CHECKPOINT_TRANSITION_SCHEMA_VERSION: Final = "belllabs.checkpoint-transition.v1"
ACTIVITY_ATTEMPT_OBSERVATION_SCHEMA_VERSION: Final = "belllabs.activity-attempt-observation.v1"
ROOT_CHECKPOINT_NS: Final = ""

# Scalar invocation metadata LangGraph copies onto every checkpoint (REQ-CP-DA-016).
STAMP_UNIT_KEY = "belllabs_unit_key"
STAMP_EXECUTION_GENERATION = "belllabs_execution_generation"
STAMP_INVOCATION_ID = "belllabs_invocation_id"
STAMP_BINDING_DIGEST = "belllabs_binding_digest"
STAMP_STATE_SCHEMA_DIGEST = "belllabs_state_schema_digest"


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class CheckpointClassification(StrEnum):
    """Pre-dispatch classification of one unit generation (REQ-CP-DA-018)."""

    SETTLED = "settled"
    OBSERVED_UNSETTLED = "observed_unsettled"
    NOT_SUBMITTED = "not_submitted"
    INTERRUPTED = "interrupted"
    TERMINAL_UNOBSERVED = "terminal_unobserved"
    IN_DOUBT = "in_doubt"


NamespaceOwnerKind = Literal["stage_unit_generation", "goal_session_role", "goal_unit_generation"]


class CheckpointLineageError(RuntimeError):
    """A checkpoint lineage invariant failed closed; no provider work may follow."""


class CheckpointLineageConflict(CheckpointLineageError):
    """A conflicting, out-of-order, or different-content observation was rejected."""


class CheckpointLineageInDoubt(CheckpointLineageError):
    """The unit generation cannot be classified as a unique safe case (REQ-CP-DA-018)."""


class IncompatibleCheckpointSchema(CheckpointLineageInDoubt):
    """A stamped state-schema digest differs from the reading binding (REQ-CP-CS-007)."""


class CheckpointNamespaceBusy(CheckpointLineageError):
    """Another unit generation holds the namespace's single in-flight invocation."""


class CheckpointNamespaceOwnershipError(CheckpointLineageConflict):
    """A unit addressed a cognitive namespace it does not own."""


class StaleClaimFence(CheckpointLineageConflict):
    """A write presented a superseded claim fence or execution generation (REQ-CP-EXEC-014)."""


def cognitive_session_namespace(unit: RuntimeUnitIdentity, execution_generation: int) -> str:
    """Deterministic namespace (and LangGraph `thread_id`) for one unit generation."""

    if execution_generation < 1:
        raise ValueError("execution generation must be positive")
    if unit.family == "stage_graph":
        return f"belllabs/stage/{unit.unit_key}/gen/{execution_generation}"
    location = unit.location
    assert isinstance(location, GoalDirectedUnitLocation)
    base = f"belllabs/goal/{unit.belllabs_run_id}/epoch/{unit.execution_epoch}"
    if execution_generation == 1:
        return f"{base}/session/{location.session_generation}/role/{location.operation_role}"
    return f"{base}/unit/{unit.unit_key}/gen/{execution_generation}"


def namespace_owner(
    unit: RuntimeUnitIdentity, execution_generation: int
) -> tuple[NamespaceOwnerKind, str]:
    """The owner of a namespace; every unit that addresses it must present the same owner."""

    if unit.family == "goal_directed" and execution_generation == 1:
        location = unit.location
        assert isinstance(location, GoalDirectedUnitLocation)
        return (
            "goal_session_role",
            sha256_digest(
                {
                    "request_scope": unit.request_scope,
                    "belllabs_run_id": unit.belllabs_run_id,
                    "execution_epoch": unit.execution_epoch,
                    "session_generation": location.session_generation,
                    "operation_role": location.operation_role,
                }
            ),
        )
    kind: NamespaceOwnerKind = (
        "stage_unit_generation" if unit.family == "stage_graph" else "goal_unit_generation"
    )
    return kind, sha256_digest({"unit_key": unit.unit_key, "generation": execution_generation})


def submission_invocation_id(unit_key: str, execution_generation: int) -> str:
    """`belllabs_invocation_id`: stable across Activity attempts of one unit generation."""

    return sha256_digest([unit_key, execution_generation, "submit"])


def checkpoint_transition_id(request_scope: str, unit_key: str, execution_generation: int) -> str:
    identity = f"checkpoint-transition:{request_scope}:{unit_key}:{execution_generation}"
    return f"checkpoint-transition:{uuid5(NAMESPACE_URL, identity)}"


class OperationActivityAttempt(Contract):
    """Technical Temporal delivery of `operation.execute`; never part of unit identity."""

    workflow_id: str = Field(min_length=1, max_length=1024)
    workflow_run_id: str = Field(min_length=1, max_length=256)
    activity_id: str = Field(min_length=1, max_length=256)
    attempt: int = Field(ge=1)
    worker_identity: str = Field(min_length=1, max_length=512)


class ActivityAttemptObservation(Contract):
    """Idempotent record of one Activity attempt before any provider dispatch (EXEC-014)."""

    schema_version: Literal["belllabs.activity-attempt-observation.v1"] = (
        ACTIVITY_ATTEMPT_OBSERVATION_SCHEMA_VERSION
    )
    request_scope: str = Field(min_length=1)
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    claim_fence: int = Field(ge=1)
    attempt: OperationActivityAttempt
    binding_id: str = Field(min_length=1)
    namespace: str | None = Field(default=None, min_length=1, max_length=1024)
    expected_source: QualifiedCheckpointKey | None = None
    dispatching: bool
    observed_at: AwareDatetime

    @property
    def observation_id(self) -> str:
        attempt = self.attempt
        identity = (
            f"activity-attempt:{self.request_scope}:{self.unit_key}:"
            f"{self.execution_generation}:{attempt.workflow_id}:{attempt.workflow_run_id}:"
            f"{attempt.activity_id}:{attempt.attempt}"
        )
        return f"activity-attempt:{uuid5(NAMESPACE_URL, identity)}"


class NamespaceClaim(Contract):
    """The cognitive namespace a unit generation addresses, with its owner."""

    namespace: str = Field(min_length=1, max_length=1024)
    owner_kind: NamespaceOwnerKind
    owner_digest: str = Field(pattern=DIGEST_PATTERN)
    binding_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)


class AttemptAdmission(Contract):
    """Repository answer to an attempt observation: the stored observation and prior transition."""

    observation: ActivityAttemptObservation
    existing_transition: CheckpointTransitionObservation | None = None


class CheckpointInvocationPlan(Contract):
    """What the adapter must address, pin, and stamp for one submission (REQ-CP-DA-016/017)."""

    schema_version: Literal["belllabs.checkpoint-invocation-plan.v1"] = (
        CHECKPOINT_INVOCATION_PLAN_SCHEMA_VERSION
    )
    request_scope: str = Field(min_length=1)
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    claim_fence: int = Field(ge=1)
    namespace: str = Field(min_length=1, max_length=1024)
    checkpoint_ns: Literal[""] = ROOT_CHECKPOINT_NS
    mode: Literal["submit"] = "submit"
    invocation_id: str = Field(pattern=DIGEST_PATTERN)
    expected_source: QualifiedCheckpointKey | None = None
    checkpointer_ref_digest: str = Field(pattern=DIGEST_PATTERN)
    binding_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def source_is_a_root_checkpoint_of_this_namespace(self) -> CheckpointInvocationPlan:
        if self.invocation_id != submission_invocation_id(self.unit_key, self.execution_generation):
            raise ValueError("invocation id is not the unit generation's submission id")
        source = self.expected_source
        if source is not None and (
            source.thread_id != self.namespace
            or not source.is_root
            or source.checkpointer_ref_digest != self.checkpointer_ref_digest
        ):
            raise ValueError("expected source is not a root checkpoint of this namespace")
        return self

    def metadata_stamps(self) -> dict[str, str | int]:
        """Scalar stamps only: no secret, prompt, or scope value is stamped."""

        return {
            STAMP_UNIT_KEY: self.unit_key,
            STAMP_EXECUTION_GENERATION: self.execution_generation,
            STAMP_INVOCATION_ID: self.invocation_id,
            STAMP_BINDING_DIGEST: self.binding_digest,
            STAMP_STATE_SCHEMA_DIGEST: self.state_schema_digest,
        }


class CheckpointCapture(Contract):
    """Adapter evidence of one invocation's source and captured result checkpoint."""

    namespace: str = Field(min_length=1, max_length=1024)
    invocation_id: str = Field(pattern=DIGEST_PATTERN)
    source_key: QualifiedCheckpointKey | None = None
    result_key: QualifiedCheckpointKey
    ancestry_verified: bool
    stamped_checkpoint_count: int = Field(ge=1)
    redacted_summary_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def result_descends_in_namespace(self) -> CheckpointCapture:
        result = self.result_key
        if result.thread_id != self.namespace or not result.is_root:
            raise ValueError("result checkpoint is not a root checkpoint of the namespace")
        if result.parent_checkpoint_id is None:
            raise ValueError("result checkpoint must carry its parent")
        if self.source_key is not None and (
            self.source_key.thread_id != self.namespace
            or self.source_key.checkpointer_ref_digest != result.checkpointer_ref_digest
        ):
            raise ValueError("source and result checkpoints are in different lineages")
        return self


class CheckpointTransitionObservation(Contract):
    """Links one unit generation's source, result, and immutable result manifest (DA-017)."""

    schema_version: Literal["belllabs.checkpoint-transition.v1"] = (
        CHECKPOINT_TRANSITION_SCHEMA_VERSION
    )
    transition_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    claim_fence: int = Field(ge=1)
    namespace: str = Field(min_length=1, max_length=1024)
    source_key: QualifiedCheckpointKey | None = None
    result_key: QualifiedCheckpointKey
    ancestry_verified: bool
    binding_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    classification: CheckpointClassification
    invocation_id: str = Field(pattern=DIGEST_PATTERN)
    result_manifest_ref: str = Field(min_length=1)
    result_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    redacted_summary_digest: str = Field(pattern=DIGEST_PATTERN)
    observed_at: AwareDatetime

    @model_validator(mode="after")
    def exact_lineage_shape(self) -> CheckpointTransitionObservation:
        if self.transition_id != checkpoint_transition_id(
            self.request_scope, self.unit_key, self.execution_generation
        ):
            raise ValueError("transition id is not derived from its unit generation")
        if self.invocation_id != submission_invocation_id(self.unit_key, self.execution_generation):
            raise ValueError("transition invocation id is not the unit generation's")
        CheckpointCapture(
            namespace=self.namespace,
            invocation_id=self.invocation_id,
            source_key=self.source_key,
            result_key=self.result_key,
            ancestry_verified=self.ancestry_verified,
            stamped_checkpoint_count=1,
            redacted_summary_digest=self.redacted_summary_digest,
        )
        if not self.ancestry_verified:
            raise ValueError("a transition is accepted only with verified ancestry")
        if self.classification not in {
            CheckpointClassification.NOT_SUBMITTED,
            CheckpointClassification.INTERRUPTED,
            CheckpointClassification.TERMINAL_UNOBSERVED,
        }:
            raise ValueError("only invoking or reconstructing classifications record transitions")
        return self

    @property
    def content_digest(self) -> str:
        """Digest for exact-duplicate detection; the observation time is not content."""

        return sha256_digest(self.model_dump(mode="json", exclude={"observed_at"}))


class LineageWriteRejection(Contract):
    """Recorded, never applied: a write that presented a superseded fence or generation."""

    request_scope: str = Field(min_length=1)
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    presented_fence: int = Field(ge=1)
    current_fence: int = Field(ge=1)
    current_generation: int = Field(ge=1)
    reason: Literal["stale_claim_fence", "stale_execution_generation"]
    payload_digest: str = Field(pattern=DIGEST_PATTERN)
    rejected_at: AwareDatetime


AttemptAdmission.model_rebuild()
