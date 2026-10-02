"""Semantic forks from safe macro snapshots (`CON-CP-CONTINUATION-V1`).

REQ-CP-EXEC-016: a `RunSnapshotManifest` is built only at a declared safe boundary from
PostgreSQL authority (never from workflow Queries). REQ-CP-EXEC-012: a fork validates a typed
`RunForkPatch` against protected fields and the declared patchable fields, computes the reuse
frontier, and is admitted as a new BellLabs run at epoch 1. Nothing implicit is copied:
pending commands, message ledgers, effect claims, reservations, provider tasks, active
operations, async children, and linked runs stay with the source run.

Everything here is pure and provider-, family-instance-, and company-neutral. The reuse rule
is the spec's: a settled, accepted, unquarantined source result outside the invalidation
frontier is reused by the derived-run unit whose `CON-CP-RUNTIME-UNIT-V1` identity is equal
after substituting `belllabs_run_id` and `execution_epoch`.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Annotated, Any, Final, Literal, Self

from pydantic import AwareDatetime, Field, JsonValue, model_validator

from app.domain.control_plane.canonical import sha256_digest, stable_json_digest, stable_json_dump
from app.domain.control_plane.contracts import ExactDefinitionRef, RunInputManifestRef
from app.domain.graph_runtime.identities import (
    DIGEST_PATTERN,
    UNIT_KEY_PATTERN,
    GoalDirectedUnitLocation,
    QualifiedCheckpointKey,
    RuntimeUnitIdentity,
    StageGraphUnitLocation,
)
from app.domain.run_control.contracts import BudgetDimensionLimit, Contract, RunRequest

RUN_SNAPSHOT_SCHEMA_VERSION: Final = "belllabs.run-snapshot.v1"
RUN_FORK_PATCH_SCHEMA_VERSION: Final = "belllabs.run-fork-patch.v1"
RUN_FORK_REQUEST_SCHEMA_VERSION: Final = "belllabs.run-fork-request.v2"
RUN_FORK_RECEIPT_SCHEMA_VERSION: Final = "belllabs.run-fork-receipt.v2"
FORK_REUSE_DECISION_SCHEMA_VERSION: Final = "belllabs.fork-reuse-decision.v1"
FORK_LINEAGE_SCHEMA_VERSION: Final = "belllabs.fork-lineage-manifest.v1"
FORK_PATCH_POLICY_SCHEMA_VERSION: Final = "belllabs.fork-patch-policy.v1"

#: The derived run of a fork always starts at execution epoch 1 (REQ-CP-EXEC-012).
DERIVED_EXECUTION_EPOCH: Final = 1
#: Invalidation-frontier member that invalidates every unit of the source run.
INVALIDATE_ALL: Final = "*"
SNAPSHOT_ID_PREFIX: Final = "run-snapshot:"
SAFE_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$"
PATCH_PATH_PATTERN = r"^[a-z][a-z0-9_]*(\.[A-Za-z0-9_-]+)*$"
FRONTIER_MEMBER_PATTERN = r"^(\*|[a-z][a-z0-9_-]*)$"

ForkFamily = Literal["stage_graph", "goal_directed"]
SnapshotBoundaryKind = Literal["stage_settled", "goal_verifier_settled"]
ForkRejectionCode = Literal[
    "snapshot_not_quiescent",
    "unsupported_boundary",
    "stale_expected_version",
    "snapshot_source_moving",
    "stale_snapshot",
    "protected_field",
    "field_not_patchable",
    "invalid_patch",
    "cognitive_seed_not_supported",
    "incompatible_restore",
    "cross_scope_fork",
    "fork_admission_rejected",
    "fork_materialization_ambiguous",
    "fork_not_materialized",
    "unauthorized",
]

#: REQ-CP-EXEC-012: identity, scope, authority and capability grants, budget ceilings,
#: accepted evidence and results, effects, settlements, terminality, and frozen binding
#: digests are never patchable. A path equal to, or below, any of these is rejected.
PROTECTED_PATCH_PATHS: Final = frozenset(
    {
        "identity",
        "run_id",
        "request_scope",
        "scope",
        "execution_epoch",
        "unit_keys",
        "authority",
        "authority_refs",
        "capability_grants",
        "permissions",
        "budget",
        "budget_ceilings",
        "accepted_evidence",
        "accepted_results",
        "obligation_evidence",
        "output_evidence",
        "effects",
        "settlements",
        "terminality",
        "terminal_outcome",
        "binding_digests",
        "workflow_type",
        "workflow_type_ref",
    }
)


class ForkRejected(ValueError):
    """A typed, fail-closed fork or snapshot rejection; nothing was admitted or copied."""

    def __init__(
        self, code: ForkRejectionCode, message: str, *, reasons: Iterable[str] = ()
    ) -> None:
        super().__init__(message)
        self.code: ForkRejectionCode = code
        self.message = message
        self.reasons: tuple[str, ...] = tuple(reasons)


Digest = Annotated[str, Field(pattern=DIGEST_PATTERN)]


# --- RunSnapshotManifest (belllabs.run-snapshot.v1) ---------------------------------------


class FamilyPosition(Contract):
    """The family's authoritative position: its last accepted family-admission head."""

    family: ForkFamily
    family_kind: str = Field(min_length=1, max_length=128)
    family_version: int = Field(ge=1)
    head_mutation_id: str = Field(min_length=1, max_length=512)
    head_mutation_kind: str = Field(min_length=1, max_length=128)
    head_mutation_fingerprint: Digest
    # StageGraph: the accepted projection the head committed.
    accepted_projection_digest: Digest | None = None
    workflow_cycle_ordinal: int | None = Field(default=None, ge=0)
    accepted_stage_ids: tuple[str, ...] = ()
    # GoalDirected: accepted revision, iteration, and handoff refs.
    goal_revision_id: str | None = Field(default=None, min_length=1, max_length=512)
    goal_revision_digest: Digest | None = None
    goal_iteration: int | None = Field(default=None, ge=1)
    head_operation_role: Literal["executor", "verifier"] | None = None
    handoff_ref: str | None = Field(default=None, min_length=1, max_length=1024)


class ReuseCandidate(Contract):
    """A settled, accepted, unquarantined source unit result (immutable refs only)."""

    unit: RuntimeUnitIdentity
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    execution_generation: int = Field(ge=1)
    binding_id: str = Field(min_length=1, max_length=1024)
    binding_digest: Digest
    state_schema_digest: Digest | None = None
    settlement_id: str = Field(min_length=1, max_length=512)
    result_manifest_ref: str = Field(min_length=1, max_length=1024)
    result_manifest_digest: Digest
    result_manifest_size_bytes: int = Field(ge=1)
    result_checkpoint: QualifiedCheckpointKey | None = None

    @model_validator(mode="after")
    def unit_key_matches(self) -> Self:
        if self.unit.unit_key != self.unit_key:
            raise ValueError("reuse candidate unit key does not match its identity")
        return self


class ExcludedUnit(Contract):
    """A source unit that is not a reuse candidate, with the reason (audit only)."""

    unit: RuntimeUnitIdentity
    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    reason: Literal["not_completed", "not_accepted", "quarantined", "superseded"]


class AcceptedEvidenceRef(Contract):
    kind: Literal["obligation", "output", "operation_settlement"]
    ref: str = Field(min_length=1, max_length=1024)
    evidence_digest: Digest


class BudgetFrontier(Contract):
    """Informational budget state at the boundary; never copied into the derived run."""

    account_id: str = Field(min_length=1)
    limits: tuple[BudgetDimensionLimit, ...] = Field(min_length=1)
    reserved: dict[str, int] = Field(default_factory=dict)
    consumed: dict[str, int] = Field(default_factory=dict)
    pending_settlement: dict[str, int] = Field(default_factory=dict)
    reservation_ids: tuple[str, ...] = ()


class EffectFrontierEntry(Contract):
    """Informational effect state at the boundary; effects are never copied."""

    effect_id: str = Field(min_length=1, max_length=512)
    effect_kind: str = Field(min_length=1, max_length=256)
    disposition: str = Field(min_length=1, max_length=64)
    settled: bool


class AsyncChildDisposition(Contract):
    """A parent-owned async child at the boundary; it stays parent-owned (never copied)."""

    child_execution_id: str = Field(min_length=1, max_length=1024)
    lifecycle: str | None = Field(default=None, max_length=64)
    disposition: Literal["terminal", "active"]
    result_decision: str | None = Field(default=None, max_length=64)


class LinkedRunDisposition(Contract):
    """A linked child run at the boundary; it stays linked to the source (never copied)."""

    link_id: str = Field(min_length=1, max_length=512)
    child_run_id: str = Field(min_length=1, max_length=512)
    terminal_status: str | None = Field(default=None, max_length=64)
    disposition: Literal["terminal", "active"]


class RunSnapshotManifest(Contract):
    """Immutable, content-addressed macro snapshot of one run at a safe boundary.

    `snapshot_digest` covers every field except the digest itself and `taken_at`, so the
    same authority at the same projection version always yields the same digest.
    """

    schema_version: Literal["belllabs.run-snapshot.v1"] = RUN_SNAPSHOT_SCHEMA_VERSION
    snapshot_id: str = Field(pattern=SAFE_ID_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    source_run_id: str = Field(min_length=1, max_length=512)
    execution_epoch: int = Field(ge=1)
    family: ForkFamily
    projection_version: int = Field(ge=1)
    run_phase: str = Field(min_length=1, max_length=32)
    effective_configuration_digest: Digest
    workflow_type_ref: ExactDefinitionRef
    input_manifest: RunInputManifestRef
    obligation_revision: str = Field(min_length=1)
    evidence_frontier_digest: Digest
    blueprint_digest: Digest | None = None
    semantic_input_binding_digest: Digest | None = None
    boundary_kind: SnapshotBoundaryKind
    boundary_ref: str = Field(min_length=1, max_length=512)
    quiescence_ref: str | None = Field(default=None, min_length=1, max_length=512)
    family_position: FamilyPosition
    accepted_evidence: tuple[AcceptedEvidenceRef, ...] = ()
    reuse_candidates: tuple[ReuseCandidate, ...] = ()
    excluded_units: tuple[ExcludedUnit, ...] = ()
    sandbox_snapshot_refs: tuple[str, ...] = ()
    budget_frontier: BudgetFrontier
    effect_frontier: tuple[EffectFrontierEntry, ...] = ()
    async_children: tuple[AsyncChildDisposition, ...] = ()
    linked_runs: tuple[LinkedRunDisposition, ...] = ()
    # Listed for audit only; never copied (REQ-CP-EXEC-012).
    pending_commands: tuple[str, ...] = ()
    taken_at: AwareDatetime
    snapshot_digest: Digest

    @model_validator(mode="after")
    def snapshot_is_safe_and_content_addressed(self) -> Self:
        if self.snapshot_id != snapshot_id_for(
            self.request_scope, self.source_run_id, self.projection_version, self.boundary_ref
        ):
            raise ValueError("snapshot identity is not derived from its run, version, boundary")
        if any(child.disposition == "active" for child in self.async_children):
            raise ValueError("a snapshot cannot hold an active async child (EXEC-016)")
        if any(link.disposition == "active" for link in self.linked_runs):
            raise ValueError("a snapshot cannot hold an active linked run (EXEC-016)")
        if any(not entry.settled for entry in self.effect_frontier):
            raise ValueError("a snapshot cannot hold an unsettled effect claim (EXEC-016)")
        keys = [item.unit_key for item in self.reuse_candidates]
        if len(keys) != len(set(keys)):
            raise ValueError("a snapshot lists each reuse candidate unit once")
        if any(
            item.unit.request_scope != self.request_scope
            or item.unit.belllabs_run_id != self.source_run_id
            or item.unit.execution_epoch != self.execution_epoch
            for item in self.reuse_candidates
        ):
            raise ValueError("reuse candidates must be units of the snapshot's run and epoch")
        if self.snapshot_digest != run_snapshot_digest(self):
            raise ValueError("run snapshot digest does not match its content")
        return self

    @classmethod
    def create(cls, **values: Any) -> RunSnapshotManifest:
        values.pop("snapshot_digest", None)
        draft = cls.model_construct(**values, snapshot_digest="sha256:" + "0" * 64)
        return cls.model_validate({**values, "snapshot_digest": run_snapshot_digest(draft)})

    @property
    def snapshot_ref(self) -> str:
        return f"{self.snapshot_id}@{self.snapshot_digest}"


def snapshot_id_for(
    request_scope: str, source_run_id: str, projection_version: int, boundary_ref: str
) -> str:
    """Deterministic snapshot identity: one snapshot per run version and boundary."""

    digest = sha256_digest(
        {
            "schema_version": RUN_SNAPSHOT_SCHEMA_VERSION,
            "request_scope": request_scope,
            "source_run_id": source_run_id,
            "projection_version": projection_version,
            "boundary_ref": boundary_ref,
        }
    )
    return SNAPSHOT_ID_PREFIX + digest.removeprefix("sha256:")


def run_snapshot_digest(snapshot: RunSnapshotManifest) -> str:
    return stable_json_digest(snapshot, exclude={"snapshot_digest", "taken_at"})


# --- RunForkPatch (belllabs.run-fork-patch.v1) and its policy -----------------------------


class ForkPatchChange(Contract):
    path: str = Field(pattern=PATCH_PATH_PATTERN, max_length=256)
    value: JsonValue


class CognitiveSeed(Contract):
    """An explicit seed: the terminal result checkpoint of a settled source unit."""

    unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    checkpoint: QualifiedCheckpointKey


class RunForkPatch(Contract):
    schema_version: Literal["belllabs.run-fork-patch.v1"] = RUN_FORK_PATCH_SCHEMA_VERSION
    source_snapshot_id: str = Field(pattern=SAFE_ID_PATTERN)
    source_snapshot_digest: Digest
    target_admission_request_ref: str = Field(min_length=1, max_length=1024)
    changes: tuple[ForkPatchChange, ...] = Field(default=(), max_length=64)
    invalidation_frontier: tuple[str, ...] = Field(default=(), max_length=256)
    cognitive_seed: CognitiveSeed | None = None
    patch_digest: Digest

    @model_validator(mode="after")
    def patch_is_canonical(self) -> Self:
        paths = [change.path for change in self.changes]
        if len(paths) != len(set(paths)):
            raise ValueError("a fork patch changes each path at most once")
        if list(self.invalidation_frontier) != sorted(set(self.invalidation_frontier)):
            raise ValueError("the invalidation frontier is a sorted set of members")
        if any(
            re.fullmatch(FRONTIER_MEMBER_PATTERN, member) is None
            for member in self.invalidation_frontier
        ):
            raise ValueError("invalidation frontier members are stage ids or '*'")
        if self.patch_digest != stable_json_digest(self, exclude={"patch_digest"}):
            raise ValueError("fork patch digest does not match its content")
        return self

    @classmethod
    def create(cls, **values: Any) -> RunForkPatch:
        values.pop("patch_digest", None)
        changes = tuple(
            item if isinstance(item, ForkPatchChange) else ForkPatchChange.model_validate(item)
            for item in values.get("changes", ())
        )
        values["changes"] = tuple(sorted(changes, key=lambda item: item.path))
        values["invalidation_frontier"] = tuple(
            sorted(set(values.get("invalidation_frontier", ())))
        )
        draft = cls.model_construct(**values, patch_digest="sha256:" + "0" * 64)
        return cls.model_validate(
            {**values, "patch_digest": stable_json_digest(draft, exclude={"patch_digest"})}
        )

    def change(self, path: str) -> ForkPatchChange | None:
        return next((item for item in self.changes if item.path == path), None)


class PatchablePath(Contract):
    """One field a Workflow Type or blueprint declares patchable, with what it invalidates.

    `invalidates` is the closed set of stage ids whose units the change affects (including
    their dependants), or `*` for every unit of the run.
    """

    path: str = Field(pattern=PATCH_PATH_PATTERN, max_length=256)
    invalidates: tuple[str, ...] = Field(min_length=1)
    value_kind: Literal["text", "digest", "input_manifest"] = "text"


class ForkPatchPolicy(Contract):
    """The patchable-field declaration for one Workflow Type (or family default)."""

    schema_version: Literal["belllabs.fork-patch-policy.v1"] = FORK_PATCH_POLICY_SCHEMA_VERSION
    policy_id: str = Field(pattern=SAFE_ID_PATTERN)
    family: ForkFamily
    patchable: tuple[PatchablePath, ...] = ()

    def declared(self, path: str) -> PatchablePath | None:
        return next((item for item in self.patchable if item.path == path), None)


def protected_path(path: str) -> bool:
    head = path.split(".", 1)[0]
    return head in PROTECTED_PATCH_PATHS or path in PROTECTED_PATCH_PATHS


def required_invalidation_frontier(patch: RunForkPatch, policy: ForkPatchPolicy) -> frozenset[str]:
    """Validate every change against protected and declared fields; return the frontier.

    Protected fields are rejected before anything else (`protected_field`); undeclared fields
    are `field_not_patchable`; a declared frontier that does not cover the changes'
    invalidation sets is `invalid_patch`. A cognitive seed is not yet qualified
    (`cognitive_seed_not_supported`; RRM-001 §8 #5).
    """

    protected = sorted(change.path for change in patch.changes if protected_path(change.path))
    if protected:
        raise ForkRejected(
            "protected_field",
            "a fork patch cannot change protected fields",
            reasons=protected,
        )
    required: set[str] = set()
    undeclared: list[str] = []
    for change in patch.changes:
        declared = policy.declared(change.path)
        if declared is None:
            undeclared.append(change.path)
            continue
        _validate_value(change, declared)
        required.update(declared.invalidates)
    if undeclared:
        raise ForkRejected(
            "field_not_patchable",
            "the Workflow Type does not declare these fields patchable",
            reasons=sorted(undeclared),
        )
    frontier = set(patch.invalidation_frontier)
    if INVALIDATE_ALL not in frontier and not required <= frontier:
        raise ForkRejected(
            "invalid_patch",
            "the invalidation frontier does not cover the patched fields",
            reasons=sorted(required - frontier),
        )
    if patch.cognitive_seed is not None:
        raise ForkRejected(
            "cognitive_seed_not_supported",
            "an explicit cognitive seed is not yet qualified; cognition starts fresh",
            reasons=(patch.cognitive_seed.unit_key,),
        )
    return frozenset(frontier | required)


def _validate_value(change: ForkPatchChange, declared: PatchablePath) -> None:
    value = change.value
    if declared.value_kind == "text":
        if not isinstance(value, str) or not value.strip() or len(value) > 8_192:
            raise ForkRejected("invalid_patch", "a text patch value is a non-empty string")
    elif declared.value_kind == "digest":
        if not isinstance(value, str) or re.fullmatch(DIGEST_PATTERN, value) is None:
            raise ForkRejected("invalid_patch", "a digest patch value is a sha256 digest")
    else:
        try:
            RunInputManifestRef.model_validate(value)
        except ValueError as error:
            raise ForkRejected(
                "invalid_patch", "an input manifest patch value is a manifest ref"
            ) from error


# --- Reuse frontier ------------------------------------------------------------------------


def derived_unit_identity(unit: RuntimeUnitIdentity, derived_run_id: str) -> RuntimeUnitIdentity:
    """The derived-run identity a source unit matches: substitute run and epoch only."""

    return unit.model_copy(
        update={"belllabs_run_id": derived_run_id, "execution_epoch": DERIVED_EXECUTION_EPOCH}
    )


def unit_stage_id(unit: RuntimeUnitIdentity) -> str | None:
    location = unit.location
    return location.stage_id if isinstance(location, StageGraphUnitLocation) else None


ReuseDecisionKind = Literal["reuse", "invalidated", "not_reusable", "excluded"]


class ForkReuseDecision(Contract):
    """One recorded reuse decision of a fork (REQ-CP-EXEC-012 reuse frontier)."""

    schema_version: Literal["belllabs.fork-reuse-decision.v1"] = FORK_REUSE_DECISION_SCHEMA_VERSION
    fork_request_id: str = Field(pattern=SAFE_ID_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    source_run_id: str = Field(min_length=1, max_length=512)
    derived_run_id: str = Field(min_length=1, max_length=512)
    source_unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    derived_unit: RuntimeUnitIdentity
    derived_unit_key: str = Field(pattern=UNIT_KEY_PATTERN)
    decision: ReuseDecisionKind
    reason: str = Field(min_length=1, max_length=128)
    candidate: ReuseCandidate | None = None

    @model_validator(mode="after")
    def decision_is_exact(self) -> Self:
        if self.derived_unit.unit_key != self.derived_unit_key:
            raise ValueError("derived unit key does not match the derived identity")
        if (
            self.derived_unit.belllabs_run_id != self.derived_run_id
            or self.derived_unit.execution_epoch != DERIVED_EXECUTION_EPOCH
            or self.derived_unit.request_scope != self.request_scope
        ):
            raise ValueError("derived unit must belong to the derived run at epoch 1")
        if (self.decision == "reuse") != (self.candidate is not None):
            raise ValueError("exactly the reuse decisions carry the reused candidate")
        if self.candidate is not None and (
            self.candidate.unit_key != self.source_unit_key
            or derived_unit_identity(self.candidate.unit, self.derived_run_id) != self.derived_unit
        ):
            raise ValueError("a reused candidate must match after substituting run and epoch")
        return self


def compute_reuse_decisions(
    snapshot: RunSnapshotManifest,
    *,
    fork_request_id: str,
    derived_run_id: str,
    frontier: frozenset[str],
) -> tuple[ForkReuseDecision, ...]:
    """Decide every source unit: reuse only settled compatible results outside the frontier.

    GoalDirected units are never reusable: their revision identity is run-bound (the
    initial revision id is derived from the run), so no derived unit can equal a source unit
    after substituting run and epoch. GoalDirected cognition starts fresh (RRM-001 §8 #5).
    """

    if derived_run_id == snapshot.source_run_id:
        raise ForkRejected("invalid_patch", "a fork must create a new BellLabs run")
    decisions: list[ForkReuseDecision] = []

    def record(
        unit: RuntimeUnitIdentity,
        decision: ReuseDecisionKind,
        reason: str,
        candidate: ReuseCandidate | None = None,
    ) -> None:
        derived = derived_unit_identity(unit, derived_run_id)
        decisions.append(
            ForkReuseDecision(
                fork_request_id=fork_request_id,
                request_scope=snapshot.request_scope,
                source_run_id=snapshot.source_run_id,
                derived_run_id=derived_run_id,
                source_unit_key=unit.unit_key,
                derived_unit=derived,
                derived_unit_key=derived.unit_key,
                decision=decision,
                reason=reason,
                candidate=candidate,
            )
        )

    for candidate in snapshot.reuse_candidates:
        unit = candidate.unit
        stage = unit_stage_id(unit)
        if isinstance(unit.location, GoalDirectedUnitLocation):
            record(unit, "not_reusable", "goal_revision_identity_is_run_bound")
        elif INVALIDATE_ALL in frontier or (stage is not None and stage in frontier):
            record(unit, "invalidated", "inside_invalidation_frontier")
        else:
            record(unit, "reuse", "settled_compatible_outside_frontier", candidate)
    for excluded in snapshot.excluded_units:
        record(excluded.unit, "excluded", excluded.reason)
    return tuple(sorted(decisions, key=lambda item: item.derived_unit_key))


def reuse_manifest_digest(decisions: Iterable[ForkReuseDecision]) -> str:
    return sha256_digest(
        [stable_json_digest(item) for item in sorted(decisions, key=lambda d: d.derived_unit_key)]
    )


# --- RunForkRequest / RunForkReceipt (versioned saga contracts) ----------------------------


def admission_request_ref(request: RunRequest) -> str:
    return f"run-request:{request.request_scope}:{request.idempotency_issuer}:{request.request_id}"


class RunForkRequest(Contract):
    """The immutable fork intent the saga reserves (version 2 of `ForkRequest`).

    It binds the validated patch, the recorded reuse decisions, and the compiled admission of
    the derived run. A fork never requires the source's run-plan or ERC digest to be unchanged.
    """

    schema_version: Literal["belllabs.run-fork-request.v2"] = RUN_FORK_REQUEST_SCHEMA_VERSION
    request_id: str = Field(pattern=SAFE_ID_PATTERN)
    idempotency_key: str = Field(min_length=1, max_length=512)
    request_scope: str = Field(min_length=1, max_length=256)
    source_run_id: str = Field(min_length=1, max_length=512)
    source_execution_epoch: int = Field(ge=1)
    snapshot_id: str = Field(pattern=SAFE_ID_PATTERN)
    snapshot_digest: Digest
    patch: RunForkPatch
    target: RunRequest
    derived_run_id: str = Field(min_length=1, max_length=512)
    reuse_decisions: tuple[ForkReuseDecision, ...] = ()
    actor_id: str = Field(min_length=1, max_length=256)
    reason: str = Field(min_length=1, max_length=2_000)
    requested_at: AwareDatetime

    @model_validator(mode="after")
    def fork_is_exact(self) -> Self:
        if self.target.request_scope != self.request_scope:
            raise ValueError("a fork cannot cross request scopes")
        if self.derived_run_id == self.source_run_id:
            raise ValueError("a fork must create a new BellLabs run")
        if self.target.parent_run_id is not None:
            raise ValueError("a fork is admitted independently, never as a linked child")
        if (
            self.patch.source_snapshot_id != self.snapshot_id
            or self.patch.source_snapshot_digest != self.snapshot_digest
        ):
            raise ValueError("the fork patch is bound to another snapshot")
        if self.patch.target_admission_request_ref != admission_request_ref(self.target):
            raise ValueError("the fork patch is bound to another admission request")
        if any(
            item.fork_request_id != self.request_id
            or item.derived_run_id != self.derived_run_id
            or item.source_run_id != self.source_run_id
            or item.request_scope != self.request_scope
            for item in self.reuse_decisions
        ):
            raise ValueError("reuse decisions belong to another fork")
        return self


def fork_request_fingerprint(request: RunForkRequest) -> str:
    """Idempotency identity of a fork intent: every field except the request times.

    The API stamps `requested_at` (and the compiled admission's `requested_at`) per call, so a
    replay of the same intent at a later time is the same fork; any other difference is a
    conflicting intent.
    """

    content = stable_json_dump(request, exclude={"requested_at"})
    content["target"].pop("requested_at", None)
    return sha256_digest(content)


class ForkLineageManifest(Contract):
    """Fork lineage: source run, snapshot, patch, seed, and the derived run's identities."""

    schema_version: Literal["belllabs.fork-lineage-manifest.v1"] = FORK_LINEAGE_SCHEMA_VERSION
    fork_request_id: str = Field(pattern=SAFE_ID_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    source_run_id: str = Field(min_length=1, max_length=512)
    source_execution_epoch: int = Field(ge=1)
    snapshot_id: str = Field(pattern=SAFE_ID_PATTERN)
    snapshot_digest: Digest
    patch_digest: Digest
    seed_checkpoint: QualifiedCheckpointKey | None = None
    derived_run_id: str = Field(min_length=1, max_length=512)
    derived_execution_epoch: Literal[1] = 1
    admission_ref: str = Field(min_length=1, max_length=1024)
    reuse_manifest_digest: Digest
    reused_unit_keys: tuple[str, ...] = ()
    invalidated_unit_keys: tuple[str, ...] = ()
    lineage_digest: Digest

    @classmethod
    def create(cls, **values: Any) -> ForkLineageManifest:
        values.pop("lineage_digest", None)
        draft = cls.model_construct(**values, lineage_digest="sha256:" + "0" * 64)
        return cls.model_validate(
            {**values, "lineage_digest": stable_json_digest(draft, exclude={"lineage_digest"})}
        )

    @model_validator(mode="after")
    def lineage_is_content_addressed(self) -> Self:
        if self.lineage_digest != stable_json_digest(self, exclude={"lineage_digest"}):
            raise ValueError("fork lineage digest does not match its content")
        return self


class RunForkReceipt(Contract):
    """The one durable receipt of a fork (version 2 of `ForkReceipt`)."""

    schema_version: Literal["belllabs.run-fork-receipt.v2"] = RUN_FORK_RECEIPT_SCHEMA_VERSION
    request_id: str = Field(pattern=SAFE_ID_PATTERN)
    request_scope: str = Field(min_length=1, max_length=256)
    source_run_id: str = Field(min_length=1, max_length=512)
    snapshot_id: str = Field(pattern=SAFE_ID_PATTERN)
    snapshot_digest: Digest
    patch_digest: Digest
    target_run_id: str = Field(min_length=1, max_length=512)
    target_execution_epoch: Literal[1] = 1
    admission_ref: str = Field(min_length=1, max_length=1024)
    admitted_effective_configuration_digest: Digest
    lineage: ForkLineageManifest
    status: Literal["accepted"] = "accepted"
    recorded_at: AwareDatetime

    @model_validator(mode="after")
    def receipt_matches_lineage(self) -> Self:
        lineage = self.lineage
        if (
            lineage.fork_request_id != self.request_id
            or lineage.derived_run_id != self.target_run_id
            or lineage.source_run_id != self.source_run_id
            or lineage.snapshot_digest != self.snapshot_digest
            or lineage.patch_digest != self.patch_digest
        ):
            raise ValueError("fork receipt does not match its lineage manifest")
        return self


def lineage_for(request: RunForkRequest, *, admission_ref: str) -> ForkLineageManifest:
    return ForkLineageManifest.create(
        fork_request_id=request.request_id,
        request_scope=request.request_scope,
        source_run_id=request.source_run_id,
        source_execution_epoch=request.source_execution_epoch,
        snapshot_id=request.snapshot_id,
        snapshot_digest=request.snapshot_digest,
        patch_digest=request.patch.patch_digest,
        seed_checkpoint=(
            request.patch.cognitive_seed.checkpoint
            if request.patch.cognitive_seed is not None
            else None
        ),
        derived_run_id=request.derived_run_id,
        admission_ref=admission_ref,
        reuse_manifest_digest=reuse_manifest_digest(request.reuse_decisions),
        reused_unit_keys=tuple(
            sorted(
                item.derived_unit_key
                for item in request.reuse_decisions
                if item.decision == "reuse"
            )
        ),
        invalidated_unit_keys=tuple(
            sorted(
                item.derived_unit_key
                for item in request.reuse_decisions
                if item.decision != "reuse"
            )
        ),
    )


def default_patch_policy(family: ForkFamily) -> ForkPatchPolicy:
    """Conservative family default until Workflow Types declare patchable fields.

    Only whole-run fields are declared, so every change invalidates every unit: the run's
    input manifest and, for GoalDirected, the initial goal objective.
    """

    patchable = [
        PatchablePath(
            path="input_manifest", invalidates=(INVALIDATE_ALL,), value_kind="input_manifest"
        ),
        PatchablePath(
            path="effective_configuration_digest",
            invalidates=(INVALIDATE_ALL,),
            value_kind="digest",
        ),
    ]
    if family == "goal_directed":
        patchable.append(PatchablePath(path="goal.objective", invalidates=(INVALIDATE_ALL,)))
    return ForkPatchPolicy(
        policy_id=f"fork-patch-policy:{family}:default", family=family, patchable=tuple(patchable)
    )


def stage_objective_path(stage_id: str) -> str:
    return f"stage_objectives.{stage_id}"


def changed_values(patch: RunForkPatch) -> Mapping[str, JsonValue]:
    return {change.path: change.value for change in patch.changes}
