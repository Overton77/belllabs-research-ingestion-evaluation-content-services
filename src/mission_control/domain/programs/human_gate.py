"""Human Gate control activations over the common Human Task rows (SPEC-03, ADR-0038, MP-10).

A manifest ``HumanGateNode`` (or a GoalDirected ``acceptance.human`` review) lowers into one
``HumanGateActivation``: the immutable review packet (refs and digests), the reviewer policy,
the permitted decisions, the deadline and the declared remediation target. Each activation
and review round has exactly one deterministic Human Task identity, stored in the existing
``mission_control.human_task`` row (kind ``human_gate:<TASK KIND>``, the activation as the
inline request packet) and answered once through ``mission_control.human_resolution``.

Pure rules only: who may answer, which answers are admitted, how a timeout policy applies and
what a resolution means to the waiting family. The decision vocabulary is the frozen
``workflow_gate`` row of ``mc.approval_binding.v1`` (``approve | deny | request_changes``;
``cancel`` belongs to the run's stop path, not to a reviewer). The resolution action follows
the durable-controls annex (``docs/knowledge/durable-controls.md``): a REVIEW task resolves
``review_accept`` or ``review_reject`` with the review decision as its payload; any other kind
resolves ``approved`` or ``denied``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.authoring.canonical import (
    sha256_digest,
    stable_json_digest,
    stable_json_dump,
)
from mission_control.domain.execution.approvals import ADMITTED_DECISIONS

HUMAN_GATE_ACTIVATION_SCHEMA: Final = "mc.human_gate_activation.v1"
HUMAN_GATE_RESOLUTION_SCHEMA: Final = "mc.human_gate_resolution.v1"
HUMAN_GATE_TASK_KIND_PREFIX: Final = "human_gate:"
TIMEOUT_POLICY_ACTOR: Final = "policy:on_timeout"
# Canonical event types. `human_task.created` is published as `human_task.opened` by the
# subscription alias projection (application/subscriptions/aliases.py); nothing is appended
# under the public name.
HUMAN_TASK_CREATED: Final = "human_task.created"
HUMAN_TASK_RESOLVED: Final = "human_task.resolved"
HUMAN_TASK_EXPIRED: Final = "human_task.expired"
HUMAN_TASK_CANCELLED: Final = "human_task.cancelled"
HUMAN_TASK_ESCALATED: Final = "human_task.escalated"
# A reviewer entry `r` is satisfied by the principal `r` or by a verified grant `reviewer:r`.
REVIEWER_PERMISSION_PREFIX: Final = "reviewer:"
MAX_FEEDBACK_REFS: Final = 32
MAX_COMMENT_CHARS: Final = 4_000
MAX_REVIEW_ROUNDS: Final = 20

_TASK_NAMESPACE: Final = UUID("3c0b7c56-9f84-4bf0-a6c4-1d2b0f6e8a10")
_DIGEST = r"^sha256:[0-9a-f]{64}$"
_EMBEDDED_DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
_SLUG = r"^[a-z0-9][a-z0-9_-]{0,127}$"

TaskKind = Literal["APPROVAL", "QUESTION", "SELECTION", "REVIEW", "POLICY_OVERRIDE"]
TimeoutPolicy = Literal["keep_waiting", "escalate", "default_answer", "stop"]
GateDecision = Literal["approve", "deny", "request_changes"]
DefaultDecision = Literal["approve", "deny"]
ResolutionAction = Literal["approved", "denied", "review_accept", "review_reject"]
ReviewDecision = Literal["approve", "reject", "request_changes"]
TaskLifecycle = Literal["open", "resolved", "expired", "cancelled"]
GateOutcomeStatus = Literal[
    "accepted", "not_accepted", "changes_requested", "stopped_by_policy", "cancelled"
]
GateFamily = Literal["StageGraph", "GoalDirected"]
ResolutionRejection = Literal[
    "already_resolved",
    "task_expired",
    "task_cancelled",
    "not_reviewer",
    "stale_version",
    "packet_digest_mismatch",
    "decision_not_admitted",
    "feedback_required",
    "deadline_passed",
]

_WORKFLOW_GATE_DECISIONS: Final = ADMITTED_DECISIONS["workflow_gate"]


class HumanGateBindingError(ValueError):
    """A resolution was offered to an activation it does not authorize."""


class _GateContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HumanGateSpec(_GateContract):
    """The lowered Human Gate policy: one per gate node (or GoalDirected review)."""

    gate_key: str = Field(pattern=_SLUG)
    task_kind: TaskKind = "REVIEW"
    prompt: str = Field(min_length=1, max_length=8_192)
    reviewers: tuple[str, ...] = Field(min_length=1, max_length=64)
    packet_sources: tuple[str, ...] = Field(default=(), max_length=64)
    timeout_seconds: int | None = Field(default=None, ge=1)
    on_timeout: TimeoutPolicy = "keep_waiting"
    default_decision: DefaultDecision | None = None
    remediation_target: str | None = Field(default=None, min_length=1, max_length=256)
    max_review_rounds: int = Field(default=1, ge=1, le=MAX_REVIEW_ROUNDS)

    @model_validator(mode="after")
    def _policy_is_explicit(self) -> HumanGateSpec:
        if any(not item for item in self.reviewers):
            raise ValueError("a reviewer entry is non-empty")
        if self.on_timeout == "default_answer" and self.default_decision is None:
            raise ValueError("on_timeout default_answer requires an explicitly declared default")
        if self.on_timeout != "default_answer" and self.default_decision is not None:
            raise ValueError("a default decision applies only under on_timeout default_answer")
        if self.on_timeout != "keep_waiting" and self.timeout_seconds is None:
            raise ValueError(f"on_timeout {self.on_timeout} requires a timeout")
        return self

    @property
    def policy_digest(self) -> str:
        return stable_json_digest(self)

    def permitted_decisions(self, review_round: int) -> tuple[GateDecision, ...]:
        """`request_changes` only with a declared remediation route and a round left."""

        decisions: list[GateDecision] = ["approve", "deny"]
        if self.remediation_target is not None and review_round < self.max_review_rounds:
            decisions.append("request_changes")
        return tuple(item for item in decisions if item in _WORKFLOW_GATE_DECISIONS)


def gate_spec_from_manifest(
    node: Any,
    *,
    remediation_target: str | None = None,
    max_review_rounds: int = 1,
) -> HumanGateSpec:
    """Lower a manifest ``HumanGateNode`` (or its resolved ``DefinitionNode``).

    ``HumanTaskSpec`` has no remediation or default-answer fields (frozen manifest schema);
    the caller supplies a declared remediation route explicitly. ``on_timeout`` names one
    of the annex policies; a node key (route on timeout) is not lowered and fails here.
    """

    task = getattr(node, "task", None)
    if task is None:
        task = (getattr(node, "body", None) or {}).get("task")
    if task is None:
        raise ValueError(f"node {getattr(node, 'key', '?')} is not a human gate")
    if not isinstance(task, dict):
        task = task.model_dump(mode="python", by_alias=True)
    on_timeout_raw = task.get("on_timeout")
    on_timeout = str(on_timeout_raw or "keep_waiting").replace("-", "_")
    if on_timeout not in {"keep_waiting", "escalate", "default_answer", "stop"}:
        raise ValueError(
            f"human gate {node.key}: on_timeout {on_timeout_raw!r} is not one of "
            "keep_waiting | escalate | default_answer | stop"
        )
    if on_timeout == "default_answer":
        raise ValueError(
            f"human gate {node.key}: default_answer needs a declared default decision, which "
            "the mission/v1 and v2 HumanTaskSpec cannot express"
        )
    timeout = task.get("timeout")
    return HumanGateSpec(
        gate_key=node.key,
        task_kind=task.get("kind", "REVIEW"),
        prompt=task["prompt"],
        reviewers=tuple(task["reviewers"]),
        packet_sources=tuple(task.get("packet") or ()),
        timeout_seconds=int(timeout) if timeout is not None else None,
        on_timeout=on_timeout,
        remediation_target=remediation_target,
        max_review_rounds=max_review_rounds,
    )


class ReviewPacketItem(_GateContract):
    """One immutable reference the reviewer is shown, with the digest the approval binds."""

    source: str = Field(min_length=1, max_length=512)
    ref: str = Field(min_length=1, max_length=2_048)
    digest: str = Field(pattern=_DIGEST)


def packet_item(source: str, ref: str) -> ReviewPacketItem:
    """The digest is the content digest the ref embeds, else the digest of the ref itself."""

    embedded = _EMBEDDED_DIGEST.findall(ref)
    return ReviewPacketItem(
        source=source, ref=ref, digest=embedded[-1] if embedded else sha256_digest(ref)
    )


def review_packet_digest(items: tuple[ReviewPacketItem, ...]) -> str:
    return sha256_digest({"packet": [stable_json_dump(item) for item in ordered_packet(items)]})


def ordered_packet(items: tuple[ReviewPacketItem, ...]) -> tuple[ReviewPacketItem, ...]:
    return tuple(sorted(items, key=lambda item: (item.source, item.ref, item.digest)))


class HumanGateActivation(_GateContract):
    """`mc.human_gate_activation.v1`: one gate entry; the Human Task's inline request packet."""

    schema_version: Literal["mc.human_gate_activation.v1"] = HUMAN_GATE_ACTIVATION_SCHEMA
    request_scope: str = Field(min_length=1, max_length=512)
    run_id: str = Field(min_length=1, max_length=512)
    family: GateFamily
    activation_key: str = Field(min_length=1, max_length=1_024)
    execution_epoch: int = Field(ge=1)
    review_round: int = Field(ge=1, le=MAX_REVIEW_ROUNDS)
    spec: HumanGateSpec
    packet: tuple[ReviewPacketItem, ...] = Field(max_length=256)
    packet_digest: str = Field(pattern=_DIGEST)
    policy_digest: str = Field(pattern=_DIGEST)
    permitted_decisions: tuple[GateDecision, ...] = Field(min_length=1)
    opened_at: AwareDatetime
    deadline_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def _bound(self) -> HumanGateActivation:
        if self.packet != ordered_packet(self.packet):
            raise ValueError("the review packet is stored in canonical order")
        if self.packet_digest != review_packet_digest(self.packet):
            raise ValueError("packet_digest does not match the review packet")
        if self.policy_digest != self.spec.policy_digest:
            raise ValueError("policy_digest does not match the gate policy")
        if self.permitted_decisions != self.spec.permitted_decisions(self.review_round):
            raise ValueError("permitted decisions do not follow the gate policy and round")
        if self.review_round > self.spec.max_review_rounds:
            raise ValueError("the review round exceeds the gate's round governor")
        expected_deadline = (
            self.opened_at + timedelta(seconds=self.spec.timeout_seconds)
            if self.spec.timeout_seconds is not None
            else None
        )
        if self.deadline_at != expected_deadline:
            raise ValueError("deadline_at follows opened_at and the gate timeout")
        return self

    @property
    def task_key(self) -> str:
        return (
            f"{HUMAN_GATE_TASK_KIND_PREFIX}{self.run_id}:{self.spec.gate_key}:"
            f"{self.activation_key}:round:{self.review_round}"
        )

    @property
    def human_task_id(self) -> UUID:
        """Deterministic: one Human Task per activation and review round in one scope."""

        return uuid5(_TASK_NAMESPACE, f"{self.request_scope}|{self.task_key}")

    @property
    def kind(self) -> str:
        return f"{HUMAN_GATE_TASK_KIND_PREFIX}{self.spec.task_kind}"

    @property
    def target_ref(self) -> str:
        return f"run:{self.run_id}/gate:{self.spec.gate_key}"

    @property
    def activation_digest(self) -> str:
        return stable_json_digest(self)

    @property
    def request_packet_ref(self) -> str:
        return f"{HUMAN_GATE_ACTIVATION_SCHEMA}@{self.activation_digest}"

    @property
    def workflow_id(self) -> str:
        return human_gate_workflow_id(self.human_task_id)


def human_gate_workflow_id(human_task_id: UUID | str) -> str:
    return f"mc-human-gate:{human_task_id}"


def open_activation(
    *,
    request_scope: str,
    run_id: str,
    family: GateFamily,
    activation_key: str,
    execution_epoch: int,
    review_round: int,
    spec: HumanGateSpec,
    packet: tuple[ReviewPacketItem, ...],
    opened_at: datetime,
) -> HumanGateActivation:
    ordered = ordered_packet(packet)
    return HumanGateActivation(
        request_scope=request_scope,
        run_id=run_id,
        family=family,
        activation_key=activation_key,
        execution_epoch=execution_epoch,
        review_round=review_round,
        spec=spec,
        packet=ordered,
        packet_digest=review_packet_digest(ordered),
        policy_digest=spec.policy_digest,
        permitted_decisions=spec.permitted_decisions(review_round),
        opened_at=opened_at,
        deadline_at=(
            opened_at + timedelta(seconds=spec.timeout_seconds)
            if spec.timeout_seconds is not None
            else None
        ),
    )


class HumanResolutionRequest(_GateContract):
    """The one mutation body (`POST /human-tasks/{id}/resolutions`, MCP parity)."""

    request_id: str = Field(min_length=1, max_length=512)
    expected_task_version: int = Field(ge=1)
    decision: GateDecision
    reviewed_packet_digest: str = Field(pattern=_DIGEST)
    feedback_artifact_refs: tuple[str, ...] = Field(default=(), max_length=MAX_FEEDBACK_REFS)
    comment: str | None = Field(default=None, min_length=1, max_length=MAX_COMMENT_CHARS)

    @model_validator(mode="after")
    def _refs(self) -> HumanResolutionRequest:
        if any(not item or len(item) > 2_048 for item in self.feedback_artifact_refs):
            raise ValueError("feedback artifact refs are non-empty and at most 2048 characters")
        return self


class HumanResolution(_GateContract):
    """`mc.human_gate_resolution.v1`: the attributed answer stored in `human_resolution`."""

    schema_version: Literal["mc.human_gate_resolution.v1"] = HUMAN_GATE_RESOLUTION_SCHEMA
    human_task_id: str = Field(min_length=1)
    request_id: str = Field(min_length=1, max_length=512)
    actor_ref: str = Field(min_length=1, max_length=256)
    decision: GateDecision
    resolution_action: ResolutionAction
    review_decision: ReviewDecision | None = None
    reviewed_packet_digest: str = Field(pattern=_DIGEST)
    feedback_artifact_refs: tuple[str, ...] = ()
    comment: str | None = None
    expected_task_version: int = Field(ge=1)
    decided_at: AwareDatetime
    default_applied: bool = False

    @property
    def answer_digest(self) -> str:
        return stable_json_digest(self)

    @property
    def resolution_ref(self) -> str:
        return f"human-resolution:{self.human_task_id}:{self.answer_digest}"


class HumanTaskView(_GateContract):
    """One Human Gate task as persisted (row columns plus parsed packet and resolution)."""

    human_task_id: str
    task_key: str
    kind: str
    target_ref: str
    lifecycle: TaskLifecycle
    version: int = Field(ge=1)
    deadline_at: AwareDatetime | None = None
    on_timeout: str | None = None
    activation: HumanGateActivation
    resolution: HumanResolution | None = None
    escalated: bool = False
    created_at: AwareDatetime
    updated_at: AwareDatetime

    def public(self) -> dict[str, Any]:
        """Inspection body: state, origin, packet, expiry; no transport handles."""

        return {
            "human_task_id": self.human_task_id,
            "origin": "workflow_gate",
            "kind": self.kind,
            "target_ref": self.target_ref,
            "lifecycle": self.lifecycle,
            "version": self.version,
            "deadline_at": self.deadline_at.isoformat() if self.deadline_at else None,
            "on_timeout": self.on_timeout,
            "escalated": self.escalated,
            "run_id": self.activation.run_id,
            "family": self.activation.family,
            "gate_key": self.activation.spec.gate_key,
            "review_round": self.activation.review_round,
            "max_review_rounds": self.activation.spec.max_review_rounds,
            "prompt": self.activation.spec.prompt,
            "reviewers": list(self.activation.spec.reviewers),
            "permitted_decisions": list(self.activation.permitted_decisions),
            "remediation_target": self.activation.spec.remediation_target,
            "review_packet": [item.model_dump(mode="json") for item in self.activation.packet],
            "packet_digest": self.activation.packet_digest,
            "resolution": (
                self.resolution.model_dump(mode="json") if self.resolution is not None else None
            ),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class ResolutionCheck:
    status: Literal["accept", "duplicate", "reject"]
    resolution: HumanResolution | None = None
    code: ResolutionRejection | None = None


def is_reviewer(spec: HumanGateSpec, actor_id: str, permissions: frozenset[str]) -> bool:
    return any(
        actor_id == reviewer or f"{REVIEWER_PERMISSION_PREFIX}{reviewer}" in permissions
        for reviewer in spec.reviewers
    )


def resolution_action(task_kind: str, decision: GateDecision) -> ResolutionAction:
    if task_kind == "REVIEW":
        return "review_accept" if decision == "approve" else "review_reject"
    return "approved" if decision == "approve" else "denied"


def _review_decision(task_kind: str, decision: GateDecision) -> ReviewDecision | None:
    if task_kind != "REVIEW":
        return None
    return {"approve": "approve", "deny": "reject", "request_changes": "request_changes"}[decision]  # type: ignore[return-value]


def _same_answer(prior: HumanResolution, request: HumanResolutionRequest, actor_id: str) -> bool:
    return (
        prior.request_id == request.request_id
        and prior.actor_ref == actor_id
        and prior.decision == request.decision
        and prior.reviewed_packet_digest == request.reviewed_packet_digest
        and prior.feedback_artifact_refs == request.feedback_artifact_refs
        and prior.comment == request.comment
        and prior.expected_task_version == request.expected_task_version
    )


def decide_resolution(
    task: HumanTaskView,
    request: HumanResolutionRequest,
    *,
    actor_id: str,
    permissions: frozenset[str],
    now: datetime,
) -> ResolutionCheck:
    """Admit at most one attributed resolution; a retry of the same request is a duplicate."""

    activation = task.activation
    if task.resolution is not None:
        if _same_answer(task.resolution, request, actor_id):
            return ResolutionCheck("duplicate", resolution=task.resolution)
        return ResolutionCheck("reject", code="already_resolved")
    if task.lifecycle == "resolved":
        return ResolutionCheck("reject", code="already_resolved")
    if task.lifecycle == "expired":
        return ResolutionCheck("reject", code="task_expired")
    if task.lifecycle == "cancelled":
        return ResolutionCheck("reject", code="task_cancelled")
    if not is_reviewer(activation.spec, actor_id, permissions):
        return ResolutionCheck("reject", code="not_reviewer")
    if request.expected_task_version != task.version:
        return ResolutionCheck("reject", code="stale_version")
    if request.reviewed_packet_digest != activation.packet_digest:
        return ResolutionCheck("reject", code="packet_digest_mismatch")
    if request.decision not in activation.permitted_decisions:
        return ResolutionCheck("reject", code="decision_not_admitted")
    if request.decision == "request_changes" and not (
        request.feedback_artifact_refs or request.comment
    ):
        return ResolutionCheck("reject", code="feedback_required")
    if (
        activation.deadline_at is not None
        and now >= activation.deadline_at
        and activation.spec.on_timeout != "keep_waiting"
        and not task.escalated
    ):
        return ResolutionCheck("reject", code="deadline_passed")
    kind = activation.spec.task_kind
    return ResolutionCheck(
        "accept",
        resolution=HumanResolution(
            human_task_id=task.human_task_id,
            request_id=request.request_id,
            actor_ref=actor_id,
            decision=request.decision,
            resolution_action=resolution_action(kind, request.decision),
            review_decision=_review_decision(kind, request.decision),
            reviewed_packet_digest=request.reviewed_packet_digest,
            feedback_artifact_refs=request.feedback_artifact_refs,
            comment=request.comment,
            expected_task_version=task.version,
            decided_at=now,
        ),
    )


TimeoutDisposition = Literal["none", "keep_waiting", "escalate", "default_answer", "stop"]


def timeout_disposition(task: HumanTaskView, now: datetime) -> TimeoutDisposition:
    """What the gate's timeout policy does now. Timeout is never implicit approval."""

    deadline = task.activation.deadline_at
    if task.lifecycle != "open" or deadline is None or now < deadline:
        return "none"
    policy = task.activation.spec.on_timeout
    if policy == "escalate" and task.escalated:
        return "keep_waiting"
    return policy


def default_resolution(task: HumanTaskView, now: datetime) -> HumanResolution:
    """The explicitly admitted default answer, attributed to the timeout policy."""

    spec = task.activation.spec
    if spec.on_timeout != "default_answer" or spec.default_decision is None:
        raise HumanGateBindingError("the gate admits no default answer")
    decision: GateDecision = spec.default_decision
    return HumanResolution(
        human_task_id=task.human_task_id,
        request_id=f"timeout:{task.human_task_id}",
        actor_ref=TIMEOUT_POLICY_ACTOR,
        decision=decision,
        resolution_action=resolution_action(spec.task_kind, decision),
        review_decision=_review_decision(spec.task_kind, decision),
        reviewed_packet_digest=task.activation.packet_digest,
        expected_task_version=task.version,
        decided_at=now,
        default_applied=True,
    )


class HumanGateOutcome(_GateContract):
    """What the waiting family receives once the task leaves `open`."""

    human_task_id: str
    task_key: str
    gate_key: str
    review_round: int = Field(ge=1)
    status: GateOutcomeStatus
    decision: GateDecision | None = None
    resolution_action: ResolutionAction | None = None
    actor_ref: str | None = None
    resolution_ref: str | None = None
    packet_digest: str = Field(pattern=_DIGEST)
    feedback_artifact_refs: tuple[str, ...] = ()
    comment: str | None = None
    default_applied: bool = False
    remediation_target: str | None = None


_DECISION_STATUS: Final[dict[str, GateOutcomeStatus]] = {
    "approve": "accepted",
    "deny": "not_accepted",
    "request_changes": "changes_requested",
}


def outcome_for(task: HumanTaskView) -> HumanGateOutcome | None:
    """None while the task is open (including `keep_waiting` and escalated tasks)."""

    activation = task.activation
    base: dict[str, Any] = {
        "human_task_id": task.human_task_id,
        "task_key": task.task_key,
        "gate_key": activation.spec.gate_key,
        "review_round": activation.review_round,
        "packet_digest": activation.packet_digest,
        "remediation_target": activation.spec.remediation_target,
    }
    if task.lifecycle == "open":
        return None
    if task.lifecycle == "resolved":
        resolution = task.resolution
        if resolution is None:
            raise HumanGateBindingError("a resolved task has no stored resolution")
        if resolution.reviewed_packet_digest != activation.packet_digest:
            raise HumanGateBindingError("the stored resolution names another packet digest")
        return HumanGateOutcome(
            **base,
            status=_DECISION_STATUS[resolution.decision],
            decision=resolution.decision,
            resolution_action=resolution.resolution_action,
            actor_ref=resolution.actor_ref,
            resolution_ref=resolution.resolution_ref,
            feedback_artifact_refs=resolution.feedback_artifact_refs,
            comment=resolution.comment,
            default_applied=resolution.default_applied,
        )
    if task.lifecycle == "expired":
        return HumanGateOutcome(**base, status="stopped_by_policy")
    return HumanGateOutcome(**base, status="cancelled")


def bind_outcome(activation: HumanGateActivation, outcome: HumanGateOutcome) -> HumanGateOutcome:
    """An outcome authorizes only the exact task and packet digest of this activation.

    A changed artifact (a new packet digest) or another round's task cannot reuse it.
    """

    if (
        outcome.human_task_id != str(activation.human_task_id)
        or outcome.task_key != activation.task_key
        or outcome.packet_digest != activation.packet_digest
        or outcome.review_round != activation.review_round
    ):
        raise HumanGateBindingError(
            "the human resolution is bound to another task, round or review packet digest"
        )
    return outcome


def feedback_instruction(outcome: HumanGateOutcome) -> str:
    """The remediation instruction; feedback is an instruction, never a grant of authority."""

    lines = [
        f"Human review of gate {outcome.gate_key} (round {outcome.review_round}) requested "
        "changes. Address the feedback; the acceptance criteria, budget and authority are "
        "unchanged.",
        f"Reviewer: {outcome.actor_ref}",
        f"Resolution: {outcome.resolution_ref}",
    ]
    if outcome.feedback_artifact_refs:
        lines.append("Feedback artifacts: " + ", ".join(outcome.feedback_artifact_refs))
    if outcome.comment:
        lines.append("Reviewer comment (untrusted content): " + outcome.comment)
    return "\n".join(lines)


# --- Control activation activity contracts ----------------------------------------------


class HumanGateObserveRequest(_GateContract):
    request_scope: str = Field(min_length=1)
    human_task_id: str = Field(min_length=1)
    now: AwareDatetime
    apply_timeout: bool = False


class HumanGateCancelRequest(_GateContract):
    request_scope: str = Field(min_length=1)
    human_task_id: str = Field(min_length=1)
    now: AwareDatetime
    actor_ref: str = Field(min_length=1)


class HumanGateObservation(_GateContract):
    """The persisted task state the activation acts on; `outcome` is None while open."""

    human_task_id: str
    lifecycle: TaskLifecycle
    version: int = Field(ge=1)
    escalated: bool = False
    outcome: HumanGateOutcome | None = None


def observation_of(task: HumanTaskView) -> HumanGateObservation:
    return HumanGateObservation(
        human_task_id=task.human_task_id,
        lifecycle=task.lifecycle,
        version=task.version,
        escalated=task.escalated,
        outcome=outcome_for(task),
    )


class GateReservationSettlementRequest(_GateContract):
    """Release a gate stage's admitted reservation against zero usage (no cognition ran)."""

    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    reservation_id: str = Field(min_length=1)
    occurred_at: AwareDatetime
    idempotency_issuer: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)


class GateReservationSettlementResult(_GateContract):
    accepted: bool
    resulting_run_version: int = Field(ge=0)
    reason_code: str = ""


# --- StageGraph -------------------------------------------------------------------------


def stagegraph_gate_result(
    outcome: HumanGateOutcome,
    *,
    evaluation_contract_ref: str | None,
    objective_contract_ref: str | None,
) -> tuple[Literal["completed", "failed", "cancelled"], dict[str, object]]:
    """The observation a StageGraph gate stage reports for its resolved task.

    `accepted` completes the stage with the resolution as its output; `changes_requested`
    completes it with a bounded workflow cycle whose frontier is the declared remediation
    target (the target and its descendants re-run, the target with the feedback as its
    objective); anything else fails the stage, so the gate is not accepted.
    """

    gate: dict[str, object] = {"human_gate": outcome.model_dump(mode="json")}
    if outcome.status == "accepted":
        return "completed", {**gate, "output_refs": [outcome.resolution_ref]}
    if outcome.status == "changes_requested":
        if (
            outcome.remediation_target is None
            or evaluation_contract_ref is None
            or objective_contract_ref is None
        ):
            raise HumanGateBindingError(
                "request_changes was resolved for a gate without a declared remediation cycle"
            )
        return "completed", {
            **gate,
            "output_refs": [outcome.resolution_ref],
            "evaluation": "cycle",
            "cycle_scope": "workflow",
            "invalidation_frontier": [outcome.remediation_target],
            "next_objective": feedback_instruction(outcome),
            "evaluation_ref": outcome.resolution_ref,
            "evaluation_contract_ref": evaluation_contract_ref,
            "objective_contract_ref": objective_contract_ref,
        }
    if outcome.status == "cancelled":
        return "cancelled", gate
    return "failed", gate


__all__ = [
    "HUMAN_GATE_ACTIVATION_SCHEMA",
    "HUMAN_GATE_RESOLUTION_SCHEMA",
    "HUMAN_GATE_TASK_KIND_PREFIX",
    "HUMAN_TASK_CANCELLED",
    "HUMAN_TASK_CREATED",
    "HUMAN_TASK_ESCALATED",
    "HUMAN_TASK_EXPIRED",
    "HUMAN_TASK_RESOLVED",
    "TIMEOUT_POLICY_ACTOR",
    "HumanGateActivation",
    "HumanGateBindingError",
    "HumanGateOutcome",
    "HumanGateSpec",
    "HumanResolution",
    "HumanResolutionRequest",
    "HumanTaskView",
    "ResolutionCheck",
    "ReviewPacketItem",
    "bind_outcome",
    "decide_resolution",
    "default_resolution",
    "feedback_instruction",
    "gate_spec_from_manifest",
    "human_gate_workflow_id",
    "is_reviewer",
    "open_activation",
    "ordered_packet",
    "outcome_for",
    "packet_item",
    "resolution_action",
    "review_packet_digest",
    "stagegraph_gate_result",
    "timeout_disposition",
]
