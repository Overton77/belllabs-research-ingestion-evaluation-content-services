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
UNIT_RESULT_OBSERVATION_SCHEMA_VERSION: Final = "belllabs.unit-result-observation.v1"
UNIT_RECONCILIATION_INCIDENT_SCHEMA_VERSION: Final = "belllabs.unit-reconciliation-incident.v1"
ROOT_CHECKPOINT_NS: Final = ""

# Scalar invocation metadata LangGraph copies onto every checkpoint (REQ-CP-DA-016).
STAMP_UNIT_KEY = "belllabs_unit_key"
STAMP_EXECUTION_GENERATION = "belllabs_execution_generation"
STAMP_INVOCATION_ID = "belllabs_invocation_id"
STAMP_BINDING_DIGEST = "belllabs_binding_digest"
STAMP_STATE_SCHEMA_DIGEST = "belllabs_state_schema_digest"
# RRM-004: the Activity attempt (lease holder) that wrote a checkpoint. It is not part of the
# unit generation's attribution stamps above; it lets an attempt capture its own result tip
# and never another, superseded attempt's checkpoint.
STAMP_ATTEMPT_REF = "belllabs_attempt_ref"


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


InDoubtReason = Literal[
    "unclassifiable",
    "missing_checkpoint",
    "ancestry_mismatch",
    "schema_mismatch",
    "foreign_descendant",
    "multiple_stamped_leaves",
    "pending_interrupt",
    "terminal_result_after_failure",
    "unsettled_effect_claims",
    "ambiguous_native_effect",
    "unrecoverable_result_manifest",
    "accepted_descendant_invalid",
]


class CheckpointLineageInDoubt(CheckpointLineageError):
    """The unit generation cannot be classified as a unique safe case (REQ-CP-DA-018).

    It carries the typed reason and the candidate checkpoints an operator may accept, so the
    incident is written from structured evidence rather than from a message.
    """

    def __init__(
        self,
        message: str,
        *,
        reason: InDoubtReason = "unclassifiable",
        candidates: tuple[QualifiedCheckpointKey, ...] = (),
    ) -> None:
        super().__init__(message)
        self.reason: InDoubtReason = reason
        self.candidates = candidates


class IncompatibleCheckpointSchema(CheckpointLineageInDoubt):
    """A stamped state-schema digest differs from the reading binding (REQ-CP-CS-007)."""

    def __init__(self, message: str) -> None:
        super().__init__(message, reason="schema_mismatch")


class CheckpointNamespaceBusy(CheckpointLineageError):
    """Another unit generation holds the namespace's single in-flight invocation."""


class CheckpointNamespaceOwnershipError(CheckpointLineageConflict):
    """A unit addressed a cognitive namespace it does not own."""


class StaleClaimFence(CheckpointLineageConflict):
    """A write presented a superseded claim fence or execution generation (REQ-CP-EXEC-014)."""


INVOKING_CLASSIFICATIONS: Final = frozenset(
    {
        CheckpointClassification.NOT_SUBMITTED,
        CheckpointClassification.INTERRUPTED,
        CheckpointClassification.TERMINAL_UNOBSERVED,
    }
)


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
    # REQ-CP-EXEC-014 (RRM-004): the attempt's scheduler deadline (Temporal start time plus
    # start-to-close timeout). It bounds this attempt's claim lease; absent means the
    # composition default applies.
    lease_expires_at: AwareDatetime | None = None


def activity_attempt_observation_id(
    request_scope: str,
    unit_key: str,
    execution_generation: int,
    attempt: OperationActivityAttempt,
) -> str:
    """Identity of one Activity attempt of a unit generation; also its claim-lease holder."""

    identity = (
        f"activity-attempt:{request_scope}:{unit_key}:"
        f"{execution_generation}:{attempt.workflow_id}:{attempt.workflow_run_id}:"
        f"{attempt.activity_id}:{attempt.attempt}"
    )
    return f"activity-attempt:{uuid5(NAMESPACE_URL, identity)}"


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
        return activity_attempt_observation_id(
            self.request_scope, self.unit_key, self.execution_generation, self.attempt
        )


class NamespaceClaim(Contract):
    """The cognitive namespace a unit generation addresses, with its owner."""

    namespace: str = Field(min_length=1, max_length=1024)
    owner_kind: NamespaceOwnerKind
    owner_digest: str = Field(pattern=DIGEST_PATTERN)
    binding_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)


class AttemptAdmission(Contract):
    """Repository answer to an attempt observation.

    It carries the stored observation, the claim lease outcome (REQ-CP-EXEC-014), and every
    durable fact the attempt must classify before provider work (REQ-CP-DA-018): a prior
    transition or result observation, an open incident, and whether an earlier holder
    already dispatched this unit generation.
    """

    observation: ActivityAttemptObservation
    existing_transition: CheckpointTransitionObservation | None = None
    lease_granted: bool = True
    took_over: bool = False
    prior_dispatch: bool = False
    existing_result: UnitResultObservation | None = None
    incident: UnitReconciliationIncident | None = None


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
    # The invocation identity is always the unit generation's submission: a resumed or
    # reconstructed invocation keeps the same stamps, so its checkpoints stay attributable.
    mode: Literal["submit"] = "submit"
    invocation_id: str = Field(pattern=DIGEST_PATTERN)
    expected_source: QualifiedCheckpointKey | None = None
    checkpointer_ref_digest: str = Field(pattern=DIGEST_PATTERN)
    binding_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    # `reconcile_unit` `accept_descendant`: classify as if this stamped key were the leaf.
    accepted_leaf: QualifiedCheckpointKey | None = None
    # The lease holder (Activity attempt observation) this invocation runs for.
    attempt_ref: str | None = Field(default=None, min_length=1, max_length=256)

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
        leaf = self.accepted_leaf
        if leaf is not None and (
            leaf.thread_id != self.namespace
            or not leaf.is_root
            or leaf.checkpointer_ref_digest != self.checkpointer_ref_digest
        ):
            raise ValueError("accepted descendant is not a root checkpoint of this namespace")
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

    def invocation_metadata(self) -> dict[str, str | int]:
        """The stamps plus this attempt's ownership marker (scalar, no secrets)."""

        metadata = self.metadata_stamps()
        if self.attempt_ref is not None:
            metadata[STAMP_ATTEMPT_REF] = self.attempt_ref
        return metadata


class CheckpointCapture(Contract):
    """Adapter evidence of one invocation's source and captured result checkpoint."""

    namespace: str = Field(min_length=1, max_length=1024)
    invocation_id: str = Field(pattern=DIGEST_PATTERN)
    source_key: QualifiedCheckpointKey | None = None
    result_key: QualifiedCheckpointKey
    ancestry_verified: bool
    stamped_checkpoint_count: int = Field(ge=1)
    redacted_summary_digest: str = Field(pattern=DIGEST_PATTERN)
    # REQ-CP-DA-018: the classification the adapter acted on (submit, resume, reconstruct).
    classification: CheckpointClassification = CheckpointClassification.NOT_SUBMITTED

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
        if self.classification not in INVOKING_CLASSIFICATIONS:
            raise ValueError("a capture records only a submitting, resuming or reconstructing act")
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
    # RRM-008 (REQ-BP-GD-012): whether a later unit of a shared session namespace may be
    # pinned to this transition's result checkpoint. A `failed` or `timed_out` settlement
    # advances the head for bookkeeping only: its cognition was never verified, so the
    # head is sealed and the session continues only in a new session generation.
    seedable: bool = True
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
        if self.classification not in INVOKING_CLASSIFICATIONS:
            raise ValueError("only invoking or reconstructing classifications record transitions")
        return self

    @property
    def content_digest(self) -> str:
        """Digest for exact-duplicate detection; the observation time is not content."""

        return sha256_digest(self.model_dump(mode="json", exclude={"observed_at"}))


def unit_result_observation_id(request_scope: str, unit_key: str, execution_generation: int) -> str:
    identity = f"unit-result:{request_scope}:{unit_key}:{execution_generation}"
    return f"unit-result:{uuid5(NAMESPACE_URL, identity)}"


def unit_incident_id(
    request_scope: str, unit_key: str, execution_generation: int, revision: int = 1
) -> str:
    """Identity of one incident revision of a unit generation (revision 1 keeps the base ID).

    A unit generation re-enters `in_doubt` only when an accepted decision could not be
    applied (for example an `accept_descendant` key that no longer classifies). That opens
    a new revision with its own operator wait, so the unit is never stranded.
    """

    identity = f"unit-in-doubt:{request_scope}:{unit_key}:{execution_generation}"
    if revision > 1:
        identity = f"{identity}:revision:{revision}"
    return f"unit-in-doubt:{uuid5(NAMESPACE_URL, identity)}"


class UnitResultObservation(Contract):
    """The fenced write that fixes a unit generation's result manifest (REQ-CP-EXEC-014).

    Recorded by compare-and-set on the claim fence before authority settlement, for every
    unit (native or cognitive) and every status. Once it exists only this exact manifest
    can settle the unit generation: a later holder settles it (`observed_unsettled`)
    without provider work, and a superseded holder's competing write is rejected.
    """

    schema_version: Literal["belllabs.unit-result-observation.v1"] = (
        UNIT_RESULT_OBSERVATION_SCHEMA_VERSION
    )
    observation_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    claim_fence: int = Field(ge=1)
    binding_id: str = Field(min_length=1)
    settlement_id: str = Field(min_length=1)
    status: Literal["completed", "failed", "cancelled", "timed_out"]
    result_manifest_ref: str = Field(min_length=1)
    result_manifest_digest: str = Field(pattern=DIGEST_PATTERN)
    result_manifest_size_bytes: int = Field(ge=1)
    checkpoint_transition_id: str | None = None
    observed_at: AwareDatetime

    @model_validator(mode="after")
    def identity_is_derived(self) -> UnitResultObservation:
        if self.observation_id != unit_result_observation_id(
            self.request_scope, self.unit_key, self.execution_generation
        ):
            raise ValueError("result observation id is not derived from its unit generation")
        return self

    @property
    def content_digest(self) -> str:
        """The recorded result; the writer's fence and time are not result content."""

        return sha256_digest(self.model_dump(mode="python", exclude={"observed_at", "claim_fence"}))


IncidentDecision = Literal["accept_descendant", "abandon_unit", "start_new_generation"]


class UnitReconciliationIncident(Contract):
    """Typed `in_doubt` incident of one unit generation (REQ-CP-DA-018, REQ-CP-RUN-007).

    It records why no unique safe classification exists and the candidate checkpoints an
    operator may accept. It holds keys and digests only, never checkpoint bodies or prompts.
    """

    schema_version: Literal["belllabs.unit-reconciliation-incident.v1"] = (
        UNIT_RECONCILIATION_INCIDENT_SCHEMA_VERSION
    )
    incident_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    belllabs_run_id: str = Field(min_length=1)
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    binding_id: str = Field(min_length=1)
    operation_workflow_id: str = Field(min_length=1, max_length=1024)
    classification: Literal["in_doubt"] = "in_doubt"
    reason: InDoubtReason
    namespace: str | None = Field(default=None, min_length=1, max_length=1024)
    expected_source: QualifiedCheckpointKey | None = None
    candidates: tuple[QualifiedCheckpointKey, ...] = Field(default=(), max_length=64)
    unsettled_effect_ids: tuple[str, ...] = Field(default=(), max_length=64)
    status: Literal["operator_required", "resolved"] = "operator_required"
    revision: int = Field(default=1, ge=1)
    decision: IncidentDecision | None = None
    decision_id: str | None = None
    accepted_checkpoint: QualifiedCheckpointKey | None = None
    recorded_at: AwareDatetime

    @model_validator(mode="after")
    def incident_shape(self) -> UnitReconciliationIncident:
        if self.incident_id != unit_incident_id(
            self.request_scope, self.unit_key, self.execution_generation, self.revision
        ):
            raise ValueError("incident id is not derived from its unit generation revision")
        if (self.status == "resolved") != (self.decision is not None):
            raise ValueError("exactly a resolved incident carries its decision")
        if (self.decision is None) != (self.decision_id is None):
            raise ValueError("a decision carries its accepted command identity")
        if (self.decision == "accept_descendant") != (self.accepted_checkpoint is not None):
            raise ValueError("only accept_descendant names an accepted checkpoint")
        return self

    @property
    def identity_digest(self) -> str:
        identity: dict[str, object] = {
            "request_scope": self.request_scope,
            "unit_key": self.unit_key,
            "execution_generation": self.execution_generation,
        }
        if self.revision > 1:
            identity["revision"] = self.revision
        return sha256_digest(identity)


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
