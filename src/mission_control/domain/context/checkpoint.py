"""Continuation Checkpoint (``mc.continuation_checkpoint.v1``), its validator and governors.

ADR-0027, SPEC-02 "Continuation checkpoint and compaction", workflow-types/08 sections 5 to
14. A checkpoint is a sealed Context Packet (``purpose = continuation``) plus typed state:
everything a fresh Agent Session needs to continue one logical execution, and nothing it
must not see (no secrets, no raw transcript, no unbounded tool payloads).

Everything here is pure. The application layer captures the ledger facts
(:class:`ContinuationFacts`), runs the optional admitted compactor, packs the continuation
packet and persists; this module decides:

- :func:`reduce_checkpoint` - the deterministic compactor: every checkpoint field from ledger
  facts, with an admitted compactor's synthesis merged only into
  ``decisions[].rationale_summary`` and ``recommended_next_actions`` (a corrupted summary
  can never drop queued commands, budgets or open Human Tasks);
- :func:`validate_checkpoint` - schema, digests, references, identity, authority and
  capability bounds, budgets, unresolved gates and workspace consistency, each failure a
  typed :class:`CheckpointInvalidReason`;
- :func:`seal_checkpoint` - the immutable, digested checkpoint with its validator verdict;
- :func:`next_failure_step` - the 08 section 8 failed-compaction policy;
- :func:`evaluate_governors` - the 08 section 13 governors (transfers, cumulative tokens,
  cost and wall-clock, failed compactions, no-progress transfers);
- :func:`verify_restore` - the hydration continuity check (``CHECKPOINT_INVALID``);
- :func:`continuation_delivery` - how a ``request_continuation`` command is delivered per
  lane profile (``turn_boundary_guaranteed`` on Deep Agents, ``wait_then_send`` on Cursor).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from enum import StrEnum
from typing import Any, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, ValidationError, model_validator

from mission_control.contracts.canonical import canonical_digest
from mission_control.domain.context.packet import (
    DIGEST_PATTERN,
    PACKER_VERSION,
    ContextPacket,
    ContextPurpose,
    ExpansionTier,
    PacketScope,
)

CHECKPOINT_SCHEMA_VERSION: Literal["mc.continuation_checkpoint.v1"] = (
    "mc.continuation_checkpoint.v1"
)
DETERMINISTIC_COMPACTOR_REF = "mc.continuation_reducer/1"
CHECKPOINT_VALIDATOR_REF = "mc.continuation_validator/1"
CHECKPOINT_INVALID = "CHECKPOINT_INVALID"
CONTINUATION_GOVERNOR_EXHAUSTED = "continuation_governor_exhausted"

# The workspace tier of a continuation packet restores exactly these roots (SPEC-02 seal).
CONTINUATION_RESTORE_ROOTS: tuple[str, ...] = ("/inputs/**", "/outputs/**", "/.mission/**")
MAX_RATIONALE_CHARS = 2_000
MAX_NEXT_ACTION_CHARS = 1_000
MAX_LIST_ITEMS = 512

_REF = Field(min_length=1, max_length=1_024)


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --------------------------------------------------------------------------------------
# Triggers and delivery (08 section 5; SPEC-02 seal step 1 and request_continuation)
# --------------------------------------------------------------------------------------


class ContinuationTriggerKind(StrEnum):
    CONTEXT_HEALTH_SOFT = "context_health_soft"
    CONTEXT_HEALTH_HARD = "context_health_hard"
    PROVIDER_COMPACTION = "provider_compaction"
    TURN_COUNT = "turn_count"
    WORKFLOW_BOUNDARY = "workflow_boundary"
    REQUEST_CONTINUATION = "request_continuation"


class ContinuationTrigger(_Contract):
    """Why a seal was scheduled. A soft trigger schedules; a hard one blocks agent work."""

    kind: ContinuationTriggerKind
    ref: str = _REF
    """The frame, command or policy reference that raised the trigger."""
    observed_at: AwareDatetime

    @property
    def blocks_agent_work(self) -> bool:
        return self.kind == ContinuationTriggerKind.CONTEXT_HEALTH_HARD


ContinuationDelivery = Literal["turn_boundary_guaranteed", "wait_then_send"]

_DELIVERY_BY_LANE: dict[str, ContinuationDelivery] = {
    "deep_agents": "turn_boundary_guaranteed",
    "cursor_local": "wait_then_send",
    "cursor_cloud": "wait_then_send",
    # MP-07: a sealed-checkpoint transfer into a fresh Claude SDK session at a turn boundary.
    "claude_agent_sdk": "turn_boundary_guaranteed",
    # MP-08: the continuation turn is a `turn/start` on the fresh thread, sent only when idle.
    "codex": "wait_then_send",
}


class ContinuationUnsupported(ValueError):
    """The lane profile has no qualified continuation delivery."""


def continuation_delivery(lane_profile: str) -> ContinuationDelivery:
    """SPEC-02 seal step 7: delivery semantics of ``request_continuation`` per lane."""

    try:
        return _DELIVERY_BY_LANE[lane_profile]
    except KeyError as error:
        raise ContinuationUnsupported(
            f"lane profile {lane_profile} has no qualified continuation delivery"
        ) from error


# --------------------------------------------------------------------------------------
# Contract: mc.continuation_checkpoint.v1 (fields follow workflow-types/08 section 6)
# --------------------------------------------------------------------------------------


class CheckpointIdentities(_Contract):
    mission_id: str = _REF
    run_id: str = _REF
    revision_id: str = _REF
    node_key: str = _REF
    activation_id: str = _REF
    logical_execution_id: str = _REF
    source_agent_session_ref: str = _REF


class GoalsAndCriteria(_Contract):
    goal_refs: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    objective_refs: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    criterion_refs: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    acceptance_state: str = Field(min_length=1, max_length=128)


class CheckpointDecision(_Contract):
    decision_ref: str = _REF
    rationale_summary: str = Field(default="", max_length=MAX_RATIONALE_CHARS)


class ArtifactRefs(_Contract):
    inputs: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    outputs: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)


class WorkState(_Contract):
    completed: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    active: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    pending: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    blocked: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)


class VerificationDisposition(_Contract):
    subject_ref: str = _REF
    disposition: Literal["passed", "failed", "pending", "waived", "not_run"]
    evidence_ref: str | None = None


class Unresolved(_Contract):
    questions: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    human_task_refs: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)


class BudgetsRemaining(_Contract):
    """Remaining budget per dimension. ``None`` means unbounded, never zero."""

    tokens: int | None = Field(default=None, ge=0)
    cost_micros: int | None = Field(default=None, ge=0)
    wall_clock_seconds: int | None = Field(default=None, ge=0)


class GovernorsRemaining(_Contract):
    transfers: int = Field(ge=0)
    failed_compactions: int = Field(ge=0)
    no_progress_transfers: int = Field(ge=0)


class CheckpointVersions(_Contract):
    capability_pins: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    model_profile_ref: str = _REF
    lane_profile: str = Field(min_length=1, max_length=64)
    packer_version: str = Field(default=PACKER_VERSION, min_length=1)


class TypedStateRef(_Contract):
    """Loop State or stage state, by reference (the Journal is never embedded)."""

    schema_ref: str = _REF
    state_version: int = Field(ge=0)
    state_digest: str = Field(pattern=DIGEST_PATTERN)
    state_ref: str = _REF


class CompactorKind(StrEnum):
    DETERMINISTIC = "deterministic"
    ADMITTED_AGENT = "admitted_agent"


class CompactorRef(_Contract):
    kind: CompactorKind
    ref: str = _REF


class CheckpointInvalidReason(StrEnum):
    SCHEMA_INVALID = "schema_invalid"
    MISSING_FIELD = "missing_field"
    DIGEST_MISMATCH = "digest_mismatch"
    DANGLING_REFERENCE = "dangling_reference"
    IDENTITY_MISMATCH = "identity_mismatch"
    AUTHORITY_WIDENED = "authority_widened"
    CAPABILITY_WIDENED = "capability_widened"
    BUDGET_WIDENED = "budget_widened"
    MANDATORY_STATE_DROPPED = "mandatory_state_dropped"
    UNRESOLVED_GATE = "unresolved_gate"
    WORKSPACE_INCONSISTENT = "workspace_inconsistent"


class ValidationFinding(_Contract):
    code: CheckpointInvalidReason
    path: str = Field(min_length=1, max_length=256)
    detail: str = Field(default="", max_length=1_024)


class CheckpointValidatorVerdict(_Contract):
    kind: Literal["deterministic"] = "deterministic"
    ref: str = CHECKPOINT_VALIDATOR_REF
    result: Literal["valid", "invalid"]
    reasons: tuple[ValidationFinding, ...] = ()

    @model_validator(mode="after")
    def reasons_match_result(self) -> CheckpointValidatorVerdict:
        if (self.result == "valid") != (not self.reasons):
            raise ValueError("a valid verdict has no reasons; an invalid one has at least one")
        return self


class CheckpointBody(_Contract):
    """Every checkpoint field except the seal (validator verdict and digest)."""

    schema_version: Literal["mc.continuation_checkpoint.v1"] = CHECKPOINT_SCHEMA_VERSION
    checkpoint_id: str = _REF
    scope: PacketScope
    identities: CheckpointIdentities
    goals_and_criteria: GoalsAndCriteria
    decisions: tuple[CheckpointDecision, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    artifact_refs: ArtifactRefs
    workspace_snapshot_ref: str = _REF
    sandbox_snapshot_ref: str | None = None
    work: WorkState
    verification_dispositions: tuple[VerificationDisposition, ...] = Field(
        default=(), max_length=MAX_LIST_ITEMS
    )
    unresolved: Unresolved
    queued_commands: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    """Command ids held in the mailbox at seal time, never their delivery."""
    event_cursor: int = Field(ge=0)
    """Mission event seq at seal."""
    budgets_remaining: BudgetsRemaining
    governors_remaining: GovernorsRemaining
    versions: CheckpointVersions
    invariants: tuple[str, ...] = Field(default=(), max_length=MAX_LIST_ITEMS)
    recommended_next_actions: tuple[str, ...] = Field(default=(), max_length=64)
    typed_state: TypedStateRef
    context_packet_ref: str = _REF
    """``context_packet:<packet_id>#<packet_digest>``: the packet a fresh session hydrates from."""
    compactor: CompactorRef
    author: str = _REF
    authored_at: AwareDatetime
    supersedes: str | None = None
    """Prior checkpoint id when a human correction created this one (08 section 14)."""

    @model_validator(mode="after")
    def bounded_text(self) -> CheckpointBody:
        if any(len(action) > MAX_NEXT_ACTION_CHARS for action in self.recommended_next_actions):
            raise ValueError("recommended next actions are bounded")
        return self


_DIGEST_EXCLUDED = frozenset({"checkpoint_id", "checkpoint_digest", "validator"})


class ContinuationCheckpoint(CheckpointBody):
    """A sealed checkpoint (ContinuationCheckpoint@1). Immutable once sealed."""

    validator: CheckpointValidatorVerdict
    checkpoint_digest: str = Field(pattern=DIGEST_PATTERN)

    @model_validator(mode="after")
    def digest_matches(self) -> ContinuationCheckpoint:
        if checkpoint_digest(self) != self.checkpoint_digest:
            raise ValueError("checkpoint_digest does not match the checkpoint content")
        return self

    @property
    def valid(self) -> bool:
        return self.validator.result == "valid"

    def body(self) -> CheckpointBody:
        return CheckpointBody.model_validate(
            self.model_dump(mode="python", exclude={"validator", "checkpoint_digest"})
        )


def checkpoint_digest(checkpoint: CheckpointBody | Mapping[str, Any]) -> str:
    """sha256 over canonical JSON of every field but the id, the digest and the verdict.

    The verdict is a judgement about the content, not content: re-validating a checkpoint
    with a newer validator never changes what it seals.
    """

    if isinstance(checkpoint, BaseModel):
        fields: dict[str, object] = {
            name: getattr(checkpoint, name)
            for name in type(checkpoint).model_fields
            if name not in _DIGEST_EXCLUDED
        }
    else:
        fields = {name: value for name, value in checkpoint.items() if name not in _DIGEST_EXCLUDED}
    return canonical_digest(fields)


def context_packet_ref(packet: ContextPacket) -> str:
    return f"context_packet:{packet.packet_id}#{packet.packet_digest}"


def parse_context_packet_ref(ref: str) -> tuple[str, str]:
    """``(packet_id, packet_digest)`` of a ``context_packet:<id>#<digest>`` reference."""

    if not ref.startswith("context_packet:") or "#" not in ref:
        raise ValueError("context packet reference must be context_packet:<id>#<digest>")
    packet_id, digest = ref.removeprefix("context_packet:").rsplit("#", 1)
    return packet_id, digest


# --------------------------------------------------------------------------------------
# Ledger facts (captured by the application layer at the safe boundary)
# --------------------------------------------------------------------------------------


class ContinuationFacts(_Contract):
    """Authoritative facts the deterministic reducer and the validator read.

    Captured from the ledger at the safe boundary after new agent actions were frozen:
    nothing here comes from the agent's own account of its work.
    """

    scope: PacketScope
    identities: CheckpointIdentities
    goals_and_criteria: GoalsAndCriteria
    decision_refs: tuple[str, ...] = ()
    artifact_refs: ArtifactRefs = ArtifactRefs()
    known_artifact_refs: tuple[str, ...] | None = None
    """Every artifact ref resolvable in this scope; ``None`` means ``artifact_refs`` only."""
    workspace_snapshot_ref: str = _REF
    workspace_manifest: Mapping[str, str] = Field(default_factory=dict)
    """Restorable path -> content digest of the snapshot (the continuity check reads it)."""
    sandbox_snapshot_ref: str | None = None
    work: WorkState = WorkState()
    verification_dispositions: tuple[VerificationDisposition, ...] = ()
    open_questions: tuple[str, ...] = ()
    open_human_task_refs: tuple[str, ...] = ()
    open_gate_refs: tuple[str, ...] = ()
    """Human gates the next session must still pass (a subset of open Human Tasks or
    policy gates); every one must be carried in ``unresolved.human_task_refs``."""
    queued_command_ids: tuple[str, ...] = ()
    event_cursor: int = Field(ge=0)
    budgets_remaining: BudgetsRemaining = BudgetsRemaining()
    governors_remaining: GovernorsRemaining
    capability_pins: tuple[str, ...] = ()
    model_profile_ref: str = _REF
    lane_profile: str = Field(min_length=1, max_length=64)
    invariants: tuple[str, ...] = ()
    typed_state: TypedStateRef
    pending_actions: tuple[str, ...] = ()
    """Deterministic next actions derived from pending and blocked work."""


class CompactorSynthesis(_Contract):
    """An admitted compacting agent's proposal: untrusted, validated before use.

    Only rationale summaries for known decisions and recommended next actions are ever
    taken from it; any other field it proposes is ignored by construction.
    """

    compactor_ref: str = _REF
    rationale_by_decision: Mapping[str, str] = Field(default_factory=dict)
    recommended_next_actions: tuple[str, ...] = ()


def reduce_checkpoint(
    facts: ContinuationFacts,
    *,
    checkpoint_id: str,
    context_packet_ref: str,
    author: str,
    authored_at: Any,
    synthesis: CompactorSynthesis | None = None,
    supersedes: str | None = None,
) -> CheckpointBody:
    """The deterministic compactor (08 section 7): every field from ledger facts.

    ``synthesis`` contributes only bounded rationale summaries for decisions the ledger
    knows and bounded recommended next actions; the deterministic pending actions always
    stay first. Queued commands, budgets, governors, open Human Tasks and gates come from
    the facts alone, so no summary can drop them.
    """

    rationale = dict(synthesis.rationale_by_decision) if synthesis else {}
    decisions = tuple(
        CheckpointDecision(
            decision_ref=ref,
            rationale_summary=_bounded(rationale.get(ref, ""), MAX_RATIONALE_CHARS),
        )
        for ref in _unique(facts.decision_refs)
    )
    proposed = synthesis.recommended_next_actions if synthesis else ()
    next_actions = _unique(
        (
            *facts.pending_actions,
            *(_bounded(action, MAX_NEXT_ACTION_CHARS) for action in proposed if action.strip()),
        )
    )[:64]
    compactor = (
        CompactorRef(kind=CompactorKind.ADMITTED_AGENT, ref=synthesis.compactor_ref)
        if synthesis is not None
        else CompactorRef(kind=CompactorKind.DETERMINISTIC, ref=DETERMINISTIC_COMPACTOR_REF)
    )
    human_tasks = _unique((*facts.open_human_task_refs, *facts.open_gate_refs))
    return CheckpointBody(
        checkpoint_id=checkpoint_id,
        scope=facts.scope,
        identities=facts.identities,
        goals_and_criteria=facts.goals_and_criteria,
        decisions=decisions,
        artifact_refs=ArtifactRefs(
            inputs=_unique(facts.artifact_refs.inputs),
            outputs=_unique(facts.artifact_refs.outputs),
        ),
        workspace_snapshot_ref=facts.workspace_snapshot_ref,
        sandbox_snapshot_ref=facts.sandbox_snapshot_ref,
        work=facts.work,
        verification_dispositions=facts.verification_dispositions,
        unresolved=Unresolved(questions=_unique(facts.open_questions), human_task_refs=human_tasks),
        queued_commands=_unique(facts.queued_command_ids),
        event_cursor=facts.event_cursor,
        budgets_remaining=facts.budgets_remaining,
        governors_remaining=facts.governors_remaining,
        versions=CheckpointVersions(
            capability_pins=tuple(sorted(set(facts.capability_pins))),
            model_profile_ref=facts.model_profile_ref,
            lane_profile=facts.lane_profile,
        ),
        invariants=_unique(facts.invariants),
        recommended_next_actions=next_actions,
        typed_state=facts.typed_state,
        context_packet_ref=context_packet_ref,
        compactor=compactor,
        author=author,
        authored_at=authored_at,
        supersedes=supersedes,
    )


# --------------------------------------------------------------------------------------
# Validator (08 section 7) and seal
# --------------------------------------------------------------------------------------


def validate_checkpoint(
    candidate: CheckpointBody | Mapping[str, Any],
    facts: ContinuationFacts,
    packet: ContextPacket | None,
    *,
    expected_digest: str | None = None,
    known_checkpoint_ids: Iterable[str] = (),
) -> CheckpointValidatorVerdict:
    """Validate a checkpoint body against the ledger facts and its continuation packet.

    ``candidate`` may be an untrusted mapping (a human correction or an admitted
    compactor's whole proposal): missing fields and schema violations become typed
    reasons, never exceptions. ``expected_digest`` checks a sealed checkpoint's digest.
    """

    findings: list[ValidationFinding] = []
    body = _parse_body(candidate, findings)
    if body is None:
        return CheckpointValidatorVerdict(result="invalid", reasons=tuple(findings))
    if expected_digest is not None and checkpoint_digest(body) != expected_digest:
        findings.append(_finding(CheckpointInvalidReason.DIGEST_MISMATCH, "checkpoint_digest"))
    _check_identity(body, facts, findings)
    _check_mandatory_state(body, facts, findings)
    _check_authority(body, facts, findings)
    _check_references(body, facts, findings, known_checkpoint_ids)
    _check_packet(body, facts, packet, findings)
    if findings:
        return CheckpointValidatorVerdict(result="invalid", reasons=tuple(findings))
    return CheckpointValidatorVerdict(result="valid")


def seal_checkpoint(
    body: CheckpointBody, verdict: CheckpointValidatorVerdict
) -> ContinuationCheckpoint:
    """Seal a validated body. An invalid verdict is sealed too (it is evidence), but a
    checkpoint whose ``validator.result`` is ``invalid`` never seeds a fresh session."""

    return ContinuationCheckpoint.model_validate(
        {
            **body.model_dump(mode="python"),
            "validator": verdict,
            "checkpoint_digest": checkpoint_digest(body),
        }
    )


class CheckpointInvalid(ValueError):
    """``CHECKPOINT_INVALID``: a fresh session must not start from this checkpoint."""

    code = CHECKPOINT_INVALID

    def __init__(self, message: str, reasons: Sequence[ValidationFinding] = ()) -> None:
        super().__init__(message)
        self.reasons = tuple(reasons)


def require_hydratable(checkpoint: ContinuationCheckpoint) -> ContinuationCheckpoint:
    """08 section 8: Mission Control never starts a fresh session from an invalid one."""

    if not checkpoint.valid:
        raise CheckpointInvalid(
            "checkpoint validator result is invalid", checkpoint.validator.reasons
        )
    if checkpoint_digest(checkpoint) != checkpoint.checkpoint_digest:
        raise CheckpointInvalid("checkpoint digest does not match its content")
    return checkpoint


def _parse_body(
    candidate: CheckpointBody | Mapping[str, Any], findings: list[ValidationFinding]
) -> CheckpointBody | None:
    if isinstance(candidate, CheckpointBody):
        return CheckpointBody.model_validate(
            candidate.model_dump(
                mode="python",
                exclude={"validator", "checkpoint_digest"},
            )
        )
    payload = {
        key: value
        for key, value in candidate.items()
        if key not in {"validator", "checkpoint_digest"}
    }
    try:
        return CheckpointBody.model_validate(payload)
    except ValidationError as error:
        for issue in error.errors():
            location = ".".join(str(part) for part in issue["loc"]) or "checkpoint"
            code = (
                CheckpointInvalidReason.MISSING_FIELD
                if issue["type"] == "missing"
                else CheckpointInvalidReason.SCHEMA_INVALID
            )
            findings.append(_finding(code, location[:256], str(issue["msg"])))
        return None


def _check_identity(
    body: CheckpointBody, facts: ContinuationFacts, findings: list[ValidationFinding]
) -> None:
    if body.scope != facts.scope:
        findings.append(_finding(CheckpointInvalidReason.IDENTITY_MISMATCH, "scope"))
    for name in type(body.identities).model_fields:
        if getattr(body.identities, name) != getattr(facts.identities, name):
            findings.append(
                _finding(CheckpointInvalidReason.IDENTITY_MISMATCH, f"identities.{name}")
            )
    if body.typed_state != facts.typed_state:
        code = (
            CheckpointInvalidReason.DIGEST_MISMATCH
            if body.typed_state.state_ref == facts.typed_state.state_ref
            else CheckpointInvalidReason.DANGLING_REFERENCE
        )
        findings.append(_finding(code, "typed_state"))
    if body.event_cursor > facts.event_cursor:
        findings.append(
            _finding(
                CheckpointInvalidReason.DANGLING_REFERENCE,
                "event_cursor",
                "cursor is ahead of the ledger",
            )
        )


def _check_mandatory_state(
    body: CheckpointBody, facts: ContinuationFacts, findings: list[ValidationFinding]
) -> None:
    """CONTEXT-STATE-AND-CONTROL mandatory test: nothing the ledger holds may be dropped."""

    dropped = sorted(set(facts.queued_command_ids) - set(body.queued_commands))
    if dropped:
        findings.append(
            _finding(
                CheckpointInvalidReason.MANDATORY_STATE_DROPPED,
                "queued_commands",
                "dropped: " + ", ".join(dropped)[:900],
            )
        )
    open_tasks = sorted(set(facts.open_human_task_refs) - set(body.unresolved.human_task_refs))
    if open_tasks:
        findings.append(
            _finding(
                CheckpointInvalidReason.MANDATORY_STATE_DROPPED,
                "unresolved.human_task_refs",
                "dropped: " + ", ".join(open_tasks)[:900],
            )
        )
    gates = sorted(set(facts.open_gate_refs) - set(body.unresolved.human_task_refs))
    if gates:
        findings.append(
            _finding(
                CheckpointInvalidReason.UNRESOLVED_GATE,
                "unresolved.human_task_refs",
                "gates not carried: " + ", ".join(gates)[:900],
            )
        )
    for name in type(facts.budgets_remaining).model_fields:
        ledger = getattr(facts.budgets_remaining, name)
        carried = getattr(body.budgets_remaining, name)
        if ledger is not None and carried is None:
            findings.append(
                _finding(
                    CheckpointInvalidReason.MANDATORY_STATE_DROPPED,
                    f"budgets_remaining.{name}",
                    "a bounded budget became unbounded",
                )
            )
    if body.governors_remaining != facts.governors_remaining:
        widened = any(
            getattr(body.governors_remaining, name) > getattr(facts.governors_remaining, name)
            for name in type(facts.governors_remaining).model_fields
        )
        findings.append(
            _finding(
                CheckpointInvalidReason.BUDGET_WIDENED
                if widened
                else CheckpointInvalidReason.MANDATORY_STATE_DROPPED,
                "governors_remaining",
            )
        )
    missing_blocked = sorted(set(facts.work.blocked) - set(body.work.blocked))
    missing_pending = sorted(
        set(facts.work.pending) - set(body.work.pending) - set(body.work.completed)
    )
    if missing_blocked or missing_pending:
        findings.append(
            _finding(
                CheckpointInvalidReason.MANDATORY_STATE_DROPPED,
                "work",
                "pending or blocked work dropped",
            )
        )


def _check_authority(
    body: CheckpointBody, facts: ContinuationFacts, findings: list[ValidationFinding]
) -> None:
    """08 section 4: a transfer cannot expand capabilities, authority, budget or inputs."""

    extra_pins = sorted(set(body.versions.capability_pins) - set(facts.capability_pins))
    if extra_pins:
        findings.append(
            _finding(
                CheckpointInvalidReason.CAPABILITY_WIDENED,
                "versions.capability_pins",
                "not granted: " + ", ".join(extra_pins)[:900],
            )
        )
    if body.versions.model_profile_ref != facts.model_profile_ref:
        findings.append(
            _finding(CheckpointInvalidReason.AUTHORITY_WIDENED, "versions.model_profile_ref")
        )
    if body.versions.lane_profile != facts.lane_profile:
        findings.append(
            _finding(CheckpointInvalidReason.AUTHORITY_WIDENED, "versions.lane_profile")
        )
    for name in type(facts.budgets_remaining).model_fields:
        ledger = getattr(facts.budgets_remaining, name)
        carried = getattr(body.budgets_remaining, name)
        if ledger is not None and carried is not None and carried > ledger:
            findings.append(
                _finding(
                    CheckpointInvalidReason.BUDGET_WIDENED,
                    f"budgets_remaining.{name}",
                    f"{carried} > ledger {ledger}",
                )
            )
    extra_inputs = sorted(set(body.artifact_refs.inputs) - set(facts.artifact_refs.inputs))
    if extra_inputs:
        findings.append(
            _finding(
                CheckpointInvalidReason.AUTHORITY_WIDENED,
                "artifact_refs.inputs",
                "input access widened: " + ", ".join(extra_inputs)[:900],
            )
        )


def _check_references(
    body: CheckpointBody,
    facts: ContinuationFacts,
    findings: list[ValidationFinding],
    known_checkpoint_ids: Iterable[str],
) -> None:
    known = set(
        facts.known_artifact_refs
        if facts.known_artifact_refs is not None
        else (*facts.artifact_refs.inputs, *facts.artifact_refs.outputs)
    )
    dangling = sorted(
        ref for ref in (*body.artifact_refs.inputs, *body.artifact_refs.outputs) if ref not in known
    )
    if dangling:
        findings.append(
            _finding(
                CheckpointInvalidReason.DANGLING_REFERENCE,
                "artifact_refs",
                ", ".join(dangling)[:900],
            )
        )
    unknown_decisions = sorted(
        {decision.decision_ref for decision in body.decisions} - set(facts.decision_refs)
    )
    if unknown_decisions:
        findings.append(
            _finding(
                CheckpointInvalidReason.DANGLING_REFERENCE,
                "decisions",
                ", ".join(unknown_decisions)[:900],
            )
        )
    if body.supersedes is not None and body.supersedes not in set(known_checkpoint_ids):
        findings.append(_finding(CheckpointInvalidReason.DANGLING_REFERENCE, "supersedes"))


def _check_packet(
    body: CheckpointBody,
    facts: ContinuationFacts,
    packet: ContextPacket | None,
    findings: list[ValidationFinding],
) -> None:
    if body.workspace_snapshot_ref != facts.workspace_snapshot_ref:
        findings.append(
            _finding(CheckpointInvalidReason.WORKSPACE_INCONSISTENT, "workspace_snapshot_ref")
        )
    if body.sandbox_snapshot_ref != facts.sandbox_snapshot_ref:
        findings.append(
            _finding(CheckpointInvalidReason.WORKSPACE_INCONSISTENT, "sandbox_snapshot_ref")
        )
    if packet is None:
        findings.append(
            _finding(
                CheckpointInvalidReason.DANGLING_REFERENCE,
                "context_packet_ref",
                "packet not found",
            )
        )
        return
    try:
        packet_id, digest = parse_context_packet_ref(body.context_packet_ref)
    except ValueError as error:
        findings.append(
            _finding(CheckpointInvalidReason.SCHEMA_INVALID, "context_packet_ref", str(error))
        )
        return
    if packet_id != packet.packet_id:
        findings.append(_finding(CheckpointInvalidReason.DANGLING_REFERENCE, "context_packet_ref"))
    if digest != packet.packet_digest:
        findings.append(_finding(CheckpointInvalidReason.DIGEST_MISMATCH, "context_packet_ref"))
    if packet.target.purpose != ContextPurpose.CONTINUATION:
        findings.append(
            _finding(
                CheckpointInvalidReason.WORKSPACE_INCONSISTENT,
                "context_packet.target.purpose",
                f"purpose is {packet.target.purpose.value}",
            )
        )
    if (
        packet.target.run_id != body.identities.run_id
        or packet.target.activation_id != body.identities.activation_id
        or packet.target.node_key != body.identities.node_key
    ):
        findings.append(
            _finding(CheckpointInvalidReason.IDENTITY_MISMATCH, "context_packet.target")
        )
    workspace = [item for item in packet.items if item.tier == ExpansionTier.WORKSPACE]
    if len(workspace) != 1 or workspace[0].workspace is None:
        findings.append(
            _finding(
                CheckpointInvalidReason.WORKSPACE_INCONSISTENT,
                "context_packet.items",
                f"expected exactly one workspace item, got {len(workspace)}",
            )
        )
        return
    restore = workspace[0].workspace
    if restore.snapshot_ref != body.workspace_snapshot_ref:
        findings.append(
            _finding(
                CheckpointInvalidReason.WORKSPACE_INCONSISTENT,
                "context_packet.workspace.snapshot_ref",
            )
        )
    missing_roots = sorted(set(CONTINUATION_RESTORE_ROOTS) - set(restore.restore_paths))
    if missing_roots:
        findings.append(
            _finding(
                CheckpointInvalidReason.WORKSPACE_INCONSISTENT,
                "context_packet.workspace.restore_paths",
                "missing: " + ", ".join(missing_roots),
            )
        )
    outside = sorted(
        path
        for path in facts.workspace_manifest
        if not any(_covered(path, root) for root in restore.restore_paths)
    )
    if outside:
        findings.append(
            _finding(
                CheckpointInvalidReason.WORKSPACE_INCONSISTENT,
                "workspace_manifest",
                "not restorable: " + ", ".join(outside)[:900],
            )
        )


def _covered(path: str, root: str) -> bool:
    prefix = root.removesuffix("**").rstrip("/")
    return path == prefix or path.startswith(prefix + "/")


# --------------------------------------------------------------------------------------
# Failed compaction policy (08 section 8)
# --------------------------------------------------------------------------------------


class CompactionFailurePolicy(_Contract):
    compactor_retries: int = Field(default=1, ge=0, le=10)
    fallback_compactor_ref: str | None = None
    human_review_required: bool = False


class FailureStep(StrEnum):
    RETRY_COMPACTOR = "retry_compactor"
    FALLBACK_COMPACTOR = "fallback_compactor"
    HUMAN_REVIEW = "human_review"
    FAIL = "fail"


class CompactionAttempt(_Contract):
    compactor: CompactorRef
    result: Literal["valid", "invalid", "error"]


def next_failure_step(
    attempts: Sequence[CompactionAttempt], policy: CompactionFailurePolicy
) -> FailureStep:
    """After a failed seal (the source execution is already parked): retry the primary
    compactor within its cap, then the admitted fallback once, then human review when the
    policy requires it, else fail explicitly."""

    failures = [attempt for attempt in attempts if attempt.result != "valid"]
    fallback = policy.fallback_compactor_ref
    primary_failures = [item for item in failures if item.compactor.ref != fallback]
    fallback_failures = [item for item in failures if item.compactor.ref == fallback]
    if len(primary_failures) <= policy.compactor_retries:
        return FailureStep.RETRY_COMPACTOR
    if fallback is not None and not fallback_failures:
        return FailureStep.FALLBACK_COMPACTOR
    if policy.human_review_required:
        return FailureStep.HUMAN_REVIEW
    return FailureStep.FAIL


# --------------------------------------------------------------------------------------
# Governors (08 section 13)
# --------------------------------------------------------------------------------------


class ContinuationGovernorPolicy(_Contract):
    max_transfers: int = Field(default=8, ge=0)
    max_cumulative_tokens: int | None = Field(default=None, ge=0)
    max_cumulative_cost_micros: int | None = Field(default=None, ge=0)
    max_wall_clock_seconds: int | None = Field(default=None, ge=0)
    max_failed_compactions: int = Field(default=3, ge=0)
    max_no_progress_transfers: int = Field(default=2, ge=0)


class ContinuationLedger(_Contract):
    """Cumulative continuation usage of one logical execution."""

    transfers: int = Field(default=0, ge=0)
    cumulative_tokens: int = Field(default=0, ge=0)
    cumulative_cost_micros: int = Field(default=0, ge=0)
    wall_clock_seconds: int = Field(default=0, ge=0)
    failed_compactions: int = Field(default=0, ge=0)
    no_progress_transfers: int = Field(default=0, ge=0)


class GovernorVerdict(_Contract):
    allowed: bool
    exhausted: tuple[str, ...] = ()
    remaining: GovernorsRemaining
    outcome: Literal["continue", "continuation_governor_exhausted"] = "continue"


def evaluate_governors(
    ledger: ContinuationLedger, policy: ContinuationGovernorPolicy
) -> GovernorVerdict:
    """Whether another transfer is allowed; exhaustion is a governed terminal outcome."""

    exhausted: list[str] = []
    if ledger.transfers >= policy.max_transfers:
        exhausted.append("max_transfers")
    if (
        policy.max_cumulative_tokens is not None
        and ledger.cumulative_tokens >= policy.max_cumulative_tokens
    ):
        exhausted.append("max_cumulative_tokens")
    if (
        policy.max_cumulative_cost_micros is not None
        and ledger.cumulative_cost_micros >= policy.max_cumulative_cost_micros
    ):
        exhausted.append("max_cumulative_cost_micros")
    if (
        policy.max_wall_clock_seconds is not None
        and ledger.wall_clock_seconds >= policy.max_wall_clock_seconds
    ):
        exhausted.append("max_wall_clock_seconds")
    if ledger.failed_compactions >= policy.max_failed_compactions:
        exhausted.append("max_failed_compactions")
    if ledger.no_progress_transfers >= policy.max_no_progress_transfers:
        exhausted.append("max_no_progress_transfers")
    remaining = GovernorsRemaining(
        transfers=max(policy.max_transfers - ledger.transfers, 0),
        failed_compactions=max(policy.max_failed_compactions - ledger.failed_compactions, 0),
        no_progress_transfers=max(
            policy.max_no_progress_transfers - ledger.no_progress_transfers, 0
        ),
    )
    return GovernorVerdict(
        allowed=not exhausted,
        exhausted=tuple(exhausted),
        remaining=remaining,
        outcome="continue" if not exhausted else CONTINUATION_GOVERNOR_EXHAUSTED,
    )


def progress_digest(checkpoint: CheckpointBody) -> str:
    """What counts as progress between two transfers: completed work, outputs, typed state."""

    return canonical_digest(
        {
            "completed": sorted(checkpoint.work.completed),
            "outputs": sorted(checkpoint.artifact_refs.outputs),
            "state_digest": checkpoint.typed_state.state_digest,
        }
    )


def record_transfer(
    ledger: ContinuationLedger,
    *,
    previous: CheckpointBody | None,
    current: CheckpointBody,
    tokens: int = 0,
    cost_micros: int = 0,
    wall_clock_seconds: int = 0,
) -> ContinuationLedger:
    """The ledger after one more transfer; a transfer without progress is counted."""

    no_progress = previous is not None and progress_digest(previous) == progress_digest(current)
    return ledger.model_copy(
        update={
            "transfers": ledger.transfers + 1,
            "cumulative_tokens": ledger.cumulative_tokens + tokens,
            "cumulative_cost_micros": ledger.cumulative_cost_micros + cost_micros,
            "wall_clock_seconds": ledger.wall_clock_seconds + wall_clock_seconds,
            "no_progress_transfers": ledger.no_progress_transfers + (1 if no_progress else 0),
        }
    )


# --------------------------------------------------------------------------------------
# Hydration continuity check (SPEC-02 "Hydration from a checkpoint packet")
# --------------------------------------------------------------------------------------


class RestoreMismatch(_Contract):
    path: str
    expected: str | None
    restored: str | None


def verify_restore(
    expected: Mapping[str, str], restored: Mapping[str, str]
) -> tuple[RestoreMismatch, ...]:
    """Compare restored file digests with the snapshot manifest; empty means continuous."""

    mismatches = [
        RestoreMismatch(path=path, expected=expected.get(path), restored=restored.get(path))
        for path in sorted(set(expected) | set(restored))
        if expected.get(path) != restored.get(path)
    ]
    return tuple(mismatches)


def require_continuity(expected: Mapping[str, str], restored: Mapping[str, str]) -> None:
    """Raise ``CHECKPOINT_INVALID`` when the restored workspace differs from the snapshot."""

    mismatches = verify_restore(expected, restored)
    if mismatches:
        raise CheckpointInvalid(
            "restored workspace differs from the checkpoint snapshot: "
            + ", ".join(item.path for item in mismatches[:20]),
            tuple(
                _finding(
                    CheckpointInvalidReason.WORKSPACE_INCONSISTENT,
                    item.path[:256],
                    f"expected {item.expected}, restored {item.restored}",
                )
                for item in mismatches[:20]
            ),
        )


# --------------------------------------------------------------------------------------
# Inline rendering of the bounded checkpoint fields (continuation packet candidates)
# --------------------------------------------------------------------------------------


def checkpoint_sections(body: CheckpointBody) -> dict[str, str]:
    """The bounded checkpoint fields as Markdown sections, one packet candidate each.

    Keys are stable section names; the continuation packet inlines them so the fresh
    session's ``.mission/context.md`` lists the checkpoint fields (SPEC-02 hydration).
    """

    def bullets(values: Iterable[str]) -> str:
        rendered = "\n".join(f"- {value}" for value in values)
        return rendered or "- none"

    goals = body.goals_and_criteria
    budgets = body.budgets_remaining
    return {
        "goals_and_criteria": (
            f"Acceptance state: {goals.acceptance_state}\n\n"
            f"Goals:\n{bullets(goals.goal_refs)}\n\nObjectives:\n{bullets(goals.objective_refs)}"
            f"\n\nSuccess criteria:\n{bullets(goals.criterion_refs)}"
        ),
        "pending_commitments": (
            f"Active:\n{bullets(body.work.active)}\n\nPending:\n{bullets(body.work.pending)}"
            f"\n\nBlocked:\n{bullets(body.work.blocked)}\n\nCompleted:\n"
            f"{bullets(body.work.completed)}\n\nOpen Human Tasks:\n"
            f"{bullets(body.unresolved.human_task_refs)}\n\nOpen questions:\n"
            f"{bullets(body.unresolved.questions)}\n\nQueued commands (held until hydration):\n"
            f"{bullets(body.queued_commands)}"
        ),
        "budget_remaining": (
            f"Tokens: {_bound(budgets.tokens)}; cost (micros): {_bound(budgets.cost_micros)}; "
            f"wall clock (s): {_bound(budgets.wall_clock_seconds)}. Transfers left: "
            f"{body.governors_remaining.transfers}; failed compactions left: "
            f"{body.governors_remaining.failed_compactions}."
        ),
        "loop_state": (
            f"Typed state {body.typed_state.schema_ref} v{body.typed_state.state_version} "
            f"at {body.typed_state.state_ref} ({body.typed_state.state_digest}). Event cursor "
            f"{body.event_cursor}. Source session {body.identities.source_agent_session_ref}."
            f"\n\nInvariants:\n{bullets(body.invariants)}\n\nDecisions:\n"
            + bullets(
                f"{item.decision_ref}: {item.rationale_summary or '(no rationale)'}"
                for item in body.decisions
            )
            + "\n\nVerification:\n"
            + bullets(
                f"{item.subject_ref}: {item.disposition}" for item in body.verification_dispositions
            )
            + f"\n\nRecommended next actions:\n{bullets(body.recommended_next_actions)}"
            + f"\n\nArtifacts (inputs):\n{bullets(body.artifact_refs.inputs)}"
            + f"\n\nArtifacts (outputs):\n{bullets(body.artifact_refs.outputs)}"
        ),
    }


def _bound(value: int | None) -> str:
    return "unbounded" if value is None else str(value)


def _bounded(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    seen: dict[str, None] = {}
    for value in values:
        seen.setdefault(value, None)
    return tuple(seen)


def _finding(code: CheckpointInvalidReason, path: str, detail: str = "") -> ValidationFinding:
    return ValidationFinding(code=code, path=path, detail=detail[:1_024])
