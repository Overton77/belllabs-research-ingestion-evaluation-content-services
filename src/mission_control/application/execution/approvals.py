"""Native approval bindings over the one Human Task service (SPEC-03, ADR-0038, MP-11).

A native request (a provider permission callback, a provider user question, an MCP
elicitation forwarded by a lane, or a Mission-Control-owned governed effect) becomes one
durable Human Task of kind ``approval:<origin>`` on the common ``human_task`` /
``human_resolution`` rows. Its inline request packet (``mc.approval_task.v1``) carries the
frozen ``mc.approval_binding.v1`` (``domain/execution/approvals.py``, consumed, never forked)
plus the reviewer policy, a redacted tool preview and the question or elicitation prompt. The
binding is written before anyone waits.

The live native handle is a separate, expiring **correlation** (``ApprovalCorrelation``):
one per native request delivery and connection. A bounded wait expires the correlation, never
the durable task; a resolution is applied to a native request only through a correlation that
is still ``live`` and only after revalidating generation, policy/grants and the Stop Fence.

Task identity is the binding identity, not a transport handle: run, execution, generation,
the stable tool-call identity (or, when the provider offers only a connection-scoped request
id, that id *together with the connection*), the normalized input digest and the policy
digest. So a restarted process cannot answer a new native request with an old approval by ID
coincidence: a different tool call, edited arguments, a new generation or a new connection
for a connection-scoped id is a different task.

Pure contracts, rules and ports; persistence lives in ``adapters/postgres/approvals``.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final, Literal, Protocol
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.authoring.canonical import sha256_digest, stable_json_digest
from mission_control.domain.execution.approvals import (
    ADMITTED_DECISIONS,
    ApprovalBinding,
    ApprovalDecision,
    ApprovalResolutionIntent,
    ElicitationAction,
    NativeApprovalCorrelation,
    ReplayStrategy,
)
from mission_control.domain.execution.lanes import DIGEST_PATTERN, LaneProfileName
from mission_control.domain.policies.stop_fence import EffectKind

APPROVAL_TASK_SCHEMA: Final = "mc.approval_task.v1"
APPROVAL_RESOLUTION_SCHEMA: Final = "mc.approval_resolution.v1"
APPROVAL_TASK_KIND_PREFIX: Final = "approval:"
APPROVAL_TIMEOUT_ACTOR: Final = "policy:on_timeout"
NATIVE_ORIGINS: Final = (
    "provider_permission",
    "provider_question",
    "mcp_elicitation",
    "governed_effect",
)
MAX_PREVIEW_STRING: Final = 512
MAX_PREVIEW_ITEMS: Final = 50
MAX_PREVIEW_DEPTH: Final = 6
MAX_ANSWER_CHARS: Final = 8_000
MAX_FEEDBACK_REFS: Final = 32
MAX_COMMENT_CHARS: Final = 4_000
REDACTED: Final = "[redacted]"

NativeOrigin = Literal[
    "provider_permission", "provider_question", "mcp_elicitation", "governed_effect"
]
ApprovalTimeoutPolicy = Literal["keep_waiting", "expire"]
TaskLifecycle = Literal["open", "resolved", "expired", "cancelled"]
ApprovalResolutionAction = Literal["approved", "denied", "answered", "cancelled"]
CorrelationState = Literal["live", "answered", "expired", "lost", "superseded"]
ReplyAction = Literal["allow", "deny", "cancel", "answer"]
ReplyReason = Literal[
    "human_decision",
    "wait_expired",
    "correlation_superseded",
    "correlation_lost",
    "stale_generation",
    "policy_changed",
    "grant_revoked",
    "stop_fenced",
    "task_expired",
    "task_cancelled",
    "not_replayable",
]
ApprovalRejection = Literal[
    "already_resolved",
    "task_expired",
    "task_cancelled",
    "not_reviewer",
    "stale_version",
    "packet_digest_mismatch",
    "decision_not_admitted",
    "edited_arguments_required",
    "answer_required",
    "elicitation_content_invalid",
    "deadline_passed",
]

_TASK_NAMESPACE: Final = UUID("7d1f3a52-2b8e-4c1d-9a5e-0e6b3c4f8a21")
_DECISION_ORDER: Final[tuple[ApprovalDecision, ...]] = (
    "approve",
    "approve_edited",
    "deny",
    "request_changes",
    "cancel",
)
# Names a form must never collect: URL workflows remain authenticated server flows (SPEC-03).
_SECRET_KEY: Final = re.compile(
    r"(pass(word|wd|phrase)?|secret|token|api[_-]?key|authori[sz]ation|credential|cookie|"
    r"private[_-]?key|session[_-]?(id|key)|bearer)",
    re.IGNORECASE,
)
_ELICITATION_FOR_DECISION: Final[dict[str, ElicitationAction]] = {
    "approve": "accept",
    "deny": "decline",
    "cancel": "cancel",
}


class ApprovalContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --- Digests and the redacted preview -------------------------------------------------------


def normalized_arguments(arguments: Mapping[str, Any]) -> dict[str, Any]:
    """Canonical JSON form of tool arguments (sorted keys, no NaN); refuses non-JSON input."""

    try:
        text = json.dumps(dict(arguments), sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as error:
        raise ValueError(f"tool arguments are not canonical JSON: {error}") from None
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("tool arguments are a JSON object")
    return value


def tool_input_digest(tool_name: str | None, arguments: Mapping[str, Any]) -> str:
    """The digest an approval authorizes: the tool and its exact normalized arguments."""

    return sha256_digest({"tool": tool_name, "input": normalized_arguments(arguments)})


def is_secret_key(name: str) -> bool:
    return bool(_SECRET_KEY.search(name))


def redacted_preview(value: Any, *, depth: int = 0) -> tuple[Any, bool]:
    """A bounded, credential-redacted copy for reviewers; returns (preview, truncated)."""

    if depth >= MAX_PREVIEW_DEPTH:
        return "[depth-limited]", True
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        truncated = False
        for index, (key, item) in enumerate(sorted(value.items(), key=lambda pair: str(pair[0]))):
            if index >= MAX_PREVIEW_ITEMS:
                truncated = True
                break
            name = str(key)
            if is_secret_key(name):
                out[name] = REDACTED
                continue
            out[name], cut = redacted_preview(item, depth=depth + 1)
            truncated = truncated or cut
        return out, truncated
    if isinstance(value, list | tuple):
        items: list[Any] = []
        truncated = len(value) > MAX_PREVIEW_ITEMS
        for item in list(value)[:MAX_PREVIEW_ITEMS]:
            preview, cut = redacted_preview(item, depth=depth + 1)
            items.append(preview)
            truncated = truncated or cut
        return items, truncated
    if isinstance(value, str) and len(value) > MAX_PREVIEW_STRING:
        return value[:MAX_PREVIEW_STRING] + "...[truncated]", True
    if isinstance(value, float) and not math.isfinite(value):
        return str(value), False
    return value, False


class ToolPreview(ApprovalContract):
    """What a reviewer sees of a pending tool call: redacted, bounded, digest-bound."""

    tool_name: str = Field(min_length=1, max_length=256)
    effect_kind: EffectKind = "other"
    arguments: dict[str, Any] = Field(default_factory=dict)
    truncated: bool = False
    input_digest: str = Field(pattern=DIGEST_PATTERN)


def tool_preview(
    tool_name: str, arguments: Mapping[str, Any], *, effect_kind: EffectKind = "other"
) -> ToolPreview:
    normalized = normalized_arguments(arguments)
    preview, truncated = redacted_preview(normalized)
    return ToolPreview(
        tool_name=tool_name,
        effect_kind=effect_kind,
        arguments=preview,
        truncated=truncated,
        input_digest=tool_input_digest(tool_name, normalized),
    )


class QuestionPrompt(ApprovalContract):
    """A provider user question (e.g. Claude ``AskUserQuestion``); the answer is an artifact."""

    question: str = Field(min_length=1, max_length=4_000)
    options: tuple[str, ...] = Field(default=(), max_length=32)
    multi_select: bool = False

    @model_validator(mode="after")
    def _options(self) -> QuestionPrompt:
        if any(not item or len(item) > 512 for item in self.options):
            raise ValueError("question options are non-empty and at most 512 characters")
        if len(set(self.options)) != len(self.options):
            raise ValueError("question options are unique")
        if self.multi_select and not self.options:
            raise ValueError("a multi-select question lists its options")
        return self


class ElicitationPrompt(ApprovalContract):
    """An MCP ``elicitation/create`` request as a reviewer sees it (MCP 2025-11-25).

    Form mode carries a flat requested schema; URL mode carries only the URL the user opens
    out of band. A form may never ask for a credential: URL workflows remain authenticated
    server flows, and the model never collects secrets into an ordinary form.
    """

    mode: Literal["form", "url"] = "form"
    message: str = Field(min_length=1, max_length=4_000)
    requested_schema: dict[str, Any] | None = None
    url: str | None = Field(default=None, min_length=1, max_length=2_048)
    server_name: str | None = Field(default=None, min_length=1, max_length=256)

    @model_validator(mode="after")
    def _mode_shape(self) -> ElicitationPrompt:
        if self.mode == "form":
            if self.requested_schema is None or self.url is not None:
                raise ValueError("a form elicitation carries a requested schema and no URL")
            properties = self.requested_schema.get("properties") or {}
            if not isinstance(properties, Mapping):
                raise ValueError("a form elicitation schema has object properties")
            credential = sorted(name for name in properties if is_secret_key(str(name)))
            if credential:
                raise ValueError(f"a form elicitation may not collect credentials: {credential}")
        else:
            if self.url is None or self.requested_schema is not None:
                raise ValueError("a URL elicitation carries a URL and no schema")
            if not self.url.startswith("https://"):
                raise ValueError("a URL elicitation names an https URL")
        return self

    def property_names(self) -> frozenset[str]:
        properties = (self.requested_schema or {}).get("properties") or {}
        return frozenset(str(name) for name in properties)


# --- The durable approval task --------------------------------------------------------------


class ApprovalTaskPacket(ApprovalContract):
    """`mc.approval_task.v1`: the immutable inline request packet of an approval task."""

    schema_version: Literal["mc.approval_task.v1"] = APPROVAL_TASK_SCHEMA
    request_scope: str = Field(min_length=1, max_length=512)
    run_id: str = Field(min_length=1, max_length=512)
    binding: ApprovalBinding
    effect_kind: EffectKind = "other"
    # Only when the request identity is a connection-scoped id with no stable tool-call ref:
    # then the connection is part of the task identity (never shown to reviewers).
    connection_ref: str | None = Field(default=None, min_length=1, max_length=512)
    reviewers: tuple[str, ...] = Field(min_length=1, max_length=64)
    prompt: str = Field(min_length=1, max_length=8_192)
    preview: ToolPreview | None = None
    question: QuestionPrompt | None = None
    elicitation: ElicitationPrompt | None = None
    on_timeout: ApprovalTimeoutPolicy = "keep_waiting"

    @model_validator(mode="after")
    def _shape(self) -> ApprovalTaskPacket:
        binding = self.binding
        if binding.origin == "workflow_gate":
            raise ValueError("a workflow gate is a Human Gate activation, not an approval task")
        if any(not item for item in self.reviewers):
            raise ValueError("a reviewer entry is non-empty")
        if binding.origin in {"provider_permission", "governed_effect"}:
            if self.preview is None:
                raise ValueError(f"a {binding.origin} task shows the pending tool preview")
        if self.preview is not None:
            if self.preview.input_digest != binding.input_digest:
                raise ValueError("the tool preview is bound to another input digest")
            if binding.tool_name is not None and self.preview.tool_name != binding.tool_name:
                raise ValueError("the tool preview names another tool")
        if (binding.origin == "provider_question") != (self.question is not None):
            raise ValueError("exactly a provider_question task carries a question")
        if (binding.origin == "mcp_elicitation") != (self.elicitation is not None):
            raise ValueError("exactly an mcp_elicitation task carries an elicitation prompt")
        if self.on_timeout == "expire" and binding.deadline is None:
            raise ValueError("on_timeout expire requires a deadline")
        connection_only = binding.native.tool_call_ref is None and binding.native.connection_scoped
        if connection_only and self.connection_ref is None:
            raise ValueError("a connection-scoped request id is bound with its connection")
        if not connection_only and self.connection_ref is not None:
            raise ValueError("a stable tool-call identity is not bound to a connection")
        if connection_only and binding.replay_strategy == "reissue_native_request":
            raise ValueError(
                "a connection-scoped request handle cannot be reissued after its connection"
            )
        if binding.human_task_id != str(self.human_task_id):
            raise ValueError("the binding names another human task than the packet identity")
        return self

    @property
    def origin(self) -> NativeOrigin:
        return self.binding.origin  # type: ignore[return-value]

    @property
    def request_key(self) -> str:
        return approval_request_key(self.binding.native, self.connection_ref)

    @property
    def task_key(self) -> str:
        return approval_task_key(
            origin=self.origin,
            run_id=self.run_id,
            harness_execution_id=self.binding.harness_execution_id,
            generation=self.binding.generation,
            request_key=self.request_key,
            input_digest=self.binding.input_digest,
            policy_digest=self.binding.policy_digest,
        )

    @property
    def human_task_id(self) -> UUID:
        return approval_task_id(self.request_scope, self.task_key)

    @property
    def kind(self) -> str:
        return f"{APPROVAL_TASK_KIND_PREFIX}{self.origin}"

    @property
    def target_ref(self) -> str:
        return f"run:{self.run_id}/approval:{self.origin}"

    @property
    def permitted_decisions(self) -> tuple[ApprovalDecision, ...]:
        admitted = ADMITTED_DECISIONS[self.origin]
        return tuple(item for item in _DECISION_ORDER if item in admitted)

    @property
    def review_digest(self) -> str:
        """What a reviewer attests to having reviewed (the HTTP `reviewed_packet_digest`)."""

        return stable_json_digest(self)

    @property
    def request_packet_ref(self) -> str:
        return f"{APPROVAL_TASK_SCHEMA}@{self.review_digest}"

    def intent(self) -> dict[str, Any]:
        """The identity-defining content; a re-open with other content is a conflict."""

        return self.model_dump(mode="json", exclude={"binding": {"opened_at", "deadline"}})


def approval_request_key(native: NativeApprovalCorrelation, connection_ref: str | None) -> str:
    if native.tool_call_ref is not None:
        return f"call:{native.tool_call_ref}"
    if native.native_request_ref is None:
        raise ValueError("a native approval names its request or tool call")
    if native.connection_scoped:
        if connection_ref is None:
            raise ValueError("a connection-scoped request id is bound with its connection")
        return f"request:{native.native_request_ref}@{connection_ref}"
    return f"request:{native.native_request_ref}"


def approval_task_key(
    *,
    origin: str,
    run_id: str,
    harness_execution_id: str,
    generation: int,
    request_key: str,
    input_digest: str,
    policy_digest: str,
) -> str:
    """Readable prefix plus a digest of the native request key.

    The task key is stored and published (events, outbox); a native transport id (tool-use
    id, JSON-RPC request id, connection) is identity input only and never appears in it.
    """

    request_digest = sha256_digest({"native_request_key": request_key})
    return (
        f"{APPROVAL_TASK_KIND_PREFIX}{origin}:{run_id}:{harness_execution_id}:g{generation}:"
        f"{request_digest}:{input_digest}:{policy_digest}"
    )


def approval_task_id(request_scope: str, task_key: str) -> UUID:
    """Deterministic: one approval task per binding identity in one tenant scope."""

    return uuid5(_TASK_NAMESPACE, f"{request_scope}|{task_key}")


class ApprovalResolution(ApprovalContract):
    """`mc.approval_resolution.v1`: the attributed answer stored in `human_resolution`."""

    schema_version: Literal["mc.approval_resolution.v1"] = APPROVAL_RESOLUTION_SCHEMA
    human_task_id: str = Field(min_length=1)
    request_id: str = Field(min_length=1, max_length=512)
    actor_ref: str = Field(min_length=1, max_length=256)
    origin: NativeOrigin
    decision: ApprovalDecision
    resolution_action: ApprovalResolutionAction
    elicitation_action: ElicitationAction | None = None
    edited_input_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    edited_arguments: dict[str, Any] | None = None
    answer: str | None = Field(default=None, min_length=1, max_length=MAX_ANSWER_CHARS)
    selected: tuple[str, ...] = ()
    elicitation_content: dict[str, Any] | None = None
    reviewed_digest: str = Field(pattern=DIGEST_PATTERN)
    feedback_artifact_refs: tuple[str, ...] = ()
    comment: str | None = None
    expected_task_version: int = Field(ge=1)
    decided_at: AwareDatetime

    @property
    def answer_digest(self) -> str:
        return stable_json_digest(self)

    @property
    def resolution_ref(self) -> str:
        return f"human-resolution:{self.human_task_id}:{self.answer_digest}"

    @property
    def authorizes_effect(self) -> bool:
        return self.decision in {"approve", "approve_edited"}


class ApprovalTaskView(ApprovalContract):
    """One approval-origin Human Task as persisted (row columns, packet and resolution)."""

    human_task_id: str
    task_key: str
    kind: str
    target_ref: str
    lifecycle: TaskLifecycle
    version: int = Field(ge=1)
    deadline_at: AwareDatetime | None = None
    on_timeout: str | None = None
    packet: ApprovalTaskPacket
    resolution: ApprovalResolution | None = None
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @property
    def origin(self) -> NativeOrigin:
        return self.packet.origin

    def bound(self) -> ApprovalBinding:
        """The binding at the task's current version (the frozen intent validator's view)."""

        return self.packet.binding.model_copy(update={"task_version": self.version})

    def public(self) -> dict[str, Any]:
        """Inspection body: state, origin, pending tool preview, prompt and expiry.

        Native session/turn/request ids and the connection are transport handles; they are
        never exposed as user-facing capabilities (SPEC-03 "Transport and authorization").
        """

        packet = self.packet
        binding = packet.binding
        return {
            "human_task_id": self.human_task_id,
            "origin": packet.origin,
            "kind": self.kind,
            "target_ref": self.target_ref,
            "lifecycle": self.lifecycle,
            "version": self.version,
            "deadline_at": self.deadline_at.isoformat() if self.deadline_at else None,
            "on_timeout": self.on_timeout,
            "run_id": packet.run_id,
            "lane_profile": binding.lane_profile,
            "harness_execution_id": binding.harness_execution_id,
            "generation": binding.generation,
            "tool_name": binding.tool_name,
            "input_digest": binding.input_digest,
            "policy_digest": binding.policy_digest,
            "replay_strategy": binding.replay_strategy,
            "prompt": packet.prompt,
            "reviewers": list(packet.reviewers),
            "permitted_decisions": list(packet.permitted_decisions),
            "pending_tool": (
                packet.preview.model_dump(mode="json") if packet.preview is not None else None
            ),
            "question": (
                packet.question.model_dump(mode="json") if packet.question is not None else None
            ),
            "elicitation": (
                packet.elicitation.model_dump(mode="json")
                if packet.elicitation is not None
                else None
            ),
            "review_digest": packet.review_digest,
            # HTTP parity with Human Gate tasks: the same body field names the digest.
            "packet_digest": packet.review_digest,
            "resolution": (
                self.resolution.model_dump(mode="json") if self.resolution is not None else None
            ),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def open_approval_packet(
    *,
    request_scope: str,
    run_id: str,
    origin: NativeOrigin,
    lane_profile: LaneProfileName,
    harness_execution_id: str,
    generation: int,
    native: NativeApprovalCorrelation,
    tool_name: str | None,
    arguments: Mapping[str, Any],
    policy_digest: str,
    reviewers: tuple[str, ...],
    prompt: str,
    opened_at: datetime,
    replay_strategy: ReplayStrategy,
    timeout_seconds: int | None = None,
    on_timeout: ApprovalTimeoutPolicy = "keep_waiting",
    effect_kind: EffectKind = "other",
    connection_ref: str | None = None,
    question: QuestionPrompt | None = None,
    elicitation: ElicitationPrompt | None = None,
    review_packet_ref: str | None = None,
) -> ApprovalTaskPacket:
    """Build the packet and its `mc.approval_binding.v1` with the deterministic task id."""

    input_digest = tool_input_digest(tool_name, arguments)
    connection_only = native.tool_call_ref is None and native.connection_scoped
    bound_connection = connection_ref if connection_only else None
    task_key = approval_task_key(
        origin=origin,
        run_id=run_id,
        harness_execution_id=harness_execution_id,
        generation=generation,
        request_key=approval_request_key(native, bound_connection),
        input_digest=input_digest,
        policy_digest=policy_digest,
    )
    binding = ApprovalBinding(
        human_task_id=str(approval_task_id(request_scope, task_key)),
        origin=origin,
        lane_profile=lane_profile,
        harness_execution_id=harness_execution_id,
        generation=generation,
        native=native,
        tool_name=tool_name,
        input_digest=input_digest,
        policy_digest=policy_digest,
        review_packet_ref=review_packet_ref,
        opened_at=opened_at,
        deadline=(
            opened_at + timedelta(seconds=timeout_seconds) if timeout_seconds is not None else None
        ),
        replay_strategy=replay_strategy,
    )
    return ApprovalTaskPacket(
        request_scope=request_scope,
        run_id=run_id,
        binding=binding,
        effect_kind=effect_kind,
        connection_ref=bound_connection,
        reviewers=reviewers,
        prompt=prompt,
        preview=(
            tool_preview(tool_name, arguments, effect_kind=effect_kind)
            if tool_name is not None
            else None
        ),
        question=question,
        elicitation=elicitation,
        on_timeout=on_timeout,
    )


# --- Resolution: the one mutation, admitted per origin ---------------------------------------


class ApprovalResolutionRequest(ApprovalContract):
    """The resolution body for approval-origin tasks (HTTP/MCP/socket parity).

    Superset of the Human Gate body: ``approve_edited`` carries the edited arguments (their
    digest must differ), a question answer carries ``answer``/``selected``, an elicitation
    accept may carry form ``elicitation_content``; ``cancel`` is distinct from ``deny``.
    """

    request_id: str = Field(min_length=1, max_length=512)
    expected_task_version: int = Field(ge=1)
    decision: ApprovalDecision
    reviewed_digest: str = Field(pattern=DIGEST_PATTERN)
    edited_arguments: dict[str, Any] | None = None
    answer: str | None = Field(default=None, min_length=1, max_length=MAX_ANSWER_CHARS)
    selected: tuple[str, ...] = Field(default=(), max_length=32)
    elicitation_action: ElicitationAction | None = None
    elicitation_content: dict[str, Any] | None = None
    feedback_artifact_refs: tuple[str, ...] = Field(default=(), max_length=MAX_FEEDBACK_REFS)
    comment: str | None = Field(default=None, min_length=1, max_length=MAX_COMMENT_CHARS)

    @model_validator(mode="after")
    def _refs(self) -> ApprovalResolutionRequest:
        if any(not item or len(item) > 2_048 for item in self.feedback_artifact_refs):
            raise ValueError("feedback artifact refs are non-empty and at most 2048 characters")
        return self

    @classmethod
    def from_gate_body(cls, body: Any, *, origin: NativeOrigin) -> ApprovalResolutionRequest:
        """Translate the frozen Human Gate HTTP/socket body (`HumanResolutionRequest`).

        `approve`/`deny`/`request_changes` keep their meaning (request_changes is admitted
        only for workflow gates, so it is refused here by the decision check); on a question
        task the reviewer's comment is the answer; on an elicitation task approve/deny are
        accept/decline. `approve_edited` and `cancel` need this extended body.
        """

        answer = body.comment if origin == "provider_question" else None
        return cls(
            request_id=body.request_id,
            expected_task_version=body.expected_task_version,
            decision=body.decision,
            reviewed_digest=body.reviewed_packet_digest,
            answer=answer,
            feedback_artifact_refs=tuple(body.feedback_artifact_refs),
            comment=None if answer is not None else body.comment,
        )


@dataclass(frozen=True, slots=True)
class ApprovalResolutionCheck:
    status: Literal["accept", "duplicate", "reject"]
    resolution: ApprovalResolution | None = None
    code: ApprovalRejection | None = None
    message: str | None = None


def is_reviewer(reviewers: tuple[str, ...], actor_id: str, permissions: frozenset[str]) -> bool:
    """A reviewer entry `r` is satisfied by the principal `r` or a verified grant `reviewer:r`."""

    return any(actor_id == item or f"reviewer:{item}" in permissions for item in reviewers)


_ACTION_OF: Final[dict[str, ApprovalResolutionAction]] = {
    "approve": "approved",
    "approve_edited": "approved",
    "deny": "denied",
    "cancel": "cancelled",
}


def _same_answer(prior: ApprovalResolution, request: ApprovalResolutionRequest, actor: str) -> bool:
    return (
        prior.request_id == request.request_id
        and prior.actor_ref == actor
        and prior.decision == request.decision
        and prior.reviewed_digest == request.reviewed_digest
        and prior.expected_task_version == request.expected_task_version
        and prior.feedback_artifact_refs == request.feedback_artifact_refs
        and prior.comment == request.comment
        and prior.answer == request.answer
        and prior.selected == request.selected
        and prior.elicitation_content == request.elicitation_content
        and prior.edited_arguments
        == (
            normalized_arguments(request.edited_arguments)
            if request.edited_arguments is not None
            else None
        )
    )


def decide_approval_resolution(
    task: ApprovalTaskView,
    request: ApprovalResolutionRequest,
    *,
    actor_id: str,
    permissions: frozenset[str],
    now: datetime,
) -> ApprovalResolutionCheck:
    """Admit at most one attributed resolution; a retry of the same request is a duplicate."""

    packet = task.packet
    if task.resolution is not None:
        if _same_answer(task.resolution, request, actor_id):
            return ApprovalResolutionCheck("duplicate", resolution=task.resolution)
        return ApprovalResolutionCheck("reject", code="already_resolved")
    if task.lifecycle == "resolved":
        return ApprovalResolutionCheck("reject", code="already_resolved")
    if task.lifecycle == "expired":
        return ApprovalResolutionCheck("reject", code="task_expired")
    if task.lifecycle == "cancelled":
        return ApprovalResolutionCheck("reject", code="task_cancelled")
    if not is_reviewer(packet.reviewers, actor_id, permissions):
        return ApprovalResolutionCheck("reject", code="not_reviewer")
    if request.expected_task_version != task.version:
        return ApprovalResolutionCheck("reject", code="stale_version")
    if request.reviewed_digest != packet.review_digest:
        return ApprovalResolutionCheck("reject", code="packet_digest_mismatch")
    if request.decision not in packet.permitted_decisions:
        return ApprovalResolutionCheck("reject", code="decision_not_admitted")
    if (
        packet.binding.deadline is not None
        and now >= packet.binding.deadline
        and packet.on_timeout == "expire"
    ):
        return ApprovalResolutionCheck("reject", code="deadline_passed")

    edited_arguments: dict[str, Any] | None = None
    edited_digest: str | None = None
    if request.decision == "approve_edited":
        if request.edited_arguments is None:
            return ApprovalResolutionCheck("reject", code="edited_arguments_required")
        try:
            edited_arguments = normalized_arguments(request.edited_arguments)
        except ValueError as error:
            return ApprovalResolutionCheck(
                "reject", code="edited_arguments_required", message=str(error)
            )
        edited_digest = tool_input_digest(packet.binding.tool_name, edited_arguments)
    elif request.edited_arguments is not None:
        return ApprovalResolutionCheck(
            "reject", code="decision_not_admitted", message="only approve_edited edits arguments"
        )

    origin = packet.origin
    answer = request.answer
    selected = request.selected
    if origin == "provider_question":
        question = packet.question
        assert question is not None
        if request.decision == "approve":
            if answer is None and not selected:
                return ApprovalResolutionCheck("reject", code="answer_required")
            if selected and not set(selected) <= set(question.options):
                return ApprovalResolutionCheck(
                    "reject", code="answer_required", message="selection is not an offered option"
                )
            if len(selected) > 1 and not question.multi_select:
                return ApprovalResolutionCheck(
                    "reject", code="answer_required", message="the question takes one option"
                )
    elif answer is not None or selected:
        return ApprovalResolutionCheck(
            "reject", code="decision_not_admitted", message="only a question takes an answer"
        )

    elicitation_action: ElicitationAction | None = None
    content = request.elicitation_content
    if origin == "mcp_elicitation":
        prompt = packet.elicitation
        assert prompt is not None
        elicitation_action = _ELICITATION_FOR_DECISION.get(request.decision)
        if elicitation_action is None or (
            request.elicitation_action is not None
            and request.elicitation_action != elicitation_action
        ):
            return ApprovalResolutionCheck("reject", code="decision_not_admitted")
        if content is not None:
            if elicitation_action != "accept" or prompt.mode != "form":
                return ApprovalResolutionCheck(
                    "reject",
                    code="elicitation_content_invalid",
                    message="only a form accept carries content",
                )
            unknown = set(content) - prompt.property_names()
            secret = sorted(name for name in content if is_secret_key(name))
            if unknown or secret:
                return ApprovalResolutionCheck(
                    "reject",
                    code="elicitation_content_invalid",
                    message="content names fields outside the requested schema or credentials",
                )
        elif elicitation_action == "accept" and prompt.mode == "form":
            return ApprovalResolutionCheck(
                "reject",
                code="elicitation_content_invalid",
                message="a form accept carries the requested content",
            )
    elif request.elicitation_action is not None or content is not None:
        return ApprovalResolutionCheck("reject", code="decision_not_admitted")

    # The frozen `mc.approval_binding.v1` intent validator has the last word.
    intent = ApprovalResolutionIntent(
        human_task_id=task.human_task_id,
        request_id=request.request_id,
        expected_task_version=request.expected_task_version,
        decision=request.decision,
        actor_ref=actor_id,
        edited_input_digest=edited_digest,
        feedback_artifact_refs=request.feedback_artifact_refs,
        elicitation_action=elicitation_action,
        decided_at=now,
    )
    try:
        intent.validate_against(task.bound())
    except ValueError as error:
        return ApprovalResolutionCheck("reject", code="decision_not_admitted", message=str(error))

    action = _ACTION_OF[request.decision]
    if origin == "provider_question" and request.decision == "approve":
        action = "answered"
    return ApprovalResolutionCheck(
        "accept",
        resolution=ApprovalResolution(
            human_task_id=task.human_task_id,
            request_id=request.request_id,
            actor_ref=actor_id,
            origin=origin,
            decision=request.decision,
            resolution_action=action,
            elicitation_action=elicitation_action,
            edited_input_digest=edited_digest,
            edited_arguments=edited_arguments,
            answer=answer,
            selected=selected,
            elicitation_content=content,
            reviewed_digest=request.reviewed_digest,
            feedback_artifact_refs=request.feedback_artifact_refs,
            comment=request.comment,
            expected_task_version=task.version,
            decided_at=now,
        ),
    )


def approval_timed_out(task: ApprovalTaskView, now: datetime) -> bool:
    """Only `on_timeout: expire` closes a task at its deadline; timeout never approves."""

    deadline = task.packet.binding.deadline
    return (
        task.lifecycle == "open"
        and deadline is not None
        and now >= deadline
        and task.packet.on_timeout == "expire"
    )


# --- Translation to native replies ----------------------------------------------------------


class NativeReply(ApprovalContract):
    """A provider-neutral reply the lane adapter maps onto its native callback result.

    ``deny`` (a reviewer refused; the model may continue with the feedback as an
    instruction) and ``cancel`` (the request was dismissed; interrupt the turn) are distinct.
    A system reply (``reason`` other than ``human_decision``) never allows anything.
    """

    action: ReplyAction
    reason: ReplyReason
    interrupt: bool = False
    message: str | None = Field(default=None, max_length=8_192)
    updated_arguments: dict[str, Any] | None = None
    updated_input_digest: str | None = Field(default=None, pattern=DIGEST_PATTERN)
    answer: str | None = None
    selected: tuple[str, ...] = ()
    elicitation_action: ElicitationAction | None = None
    elicitation_content: dict[str, Any] | None = None
    human_task_id: str
    resolution_ref: str | None = None

    @model_validator(mode="after")
    def _system_never_allows(self) -> NativeReply:
        if self.reason != "human_decision" and self.action in {"allow", "answer"}:
            raise ValueError("only an attributed human decision allows or answers")
        if (self.updated_arguments is None) != (self.updated_input_digest is None):
            raise ValueError("edited arguments travel with their digest")
        return self

    @property
    def digest(self) -> str:
        return stable_json_digest(self)


_SYSTEM_MESSAGES: Final[dict[str, str]] = {
    "wait_expired": "the approval wait expired; the request stays pending in Mission Control",
    "correlation_superseded": "a newer delivery of this request owns the approval",
    "correlation_lost": "the native request was lost with its process",
    "stale_generation": "the approval belongs to an earlier execution generation",
    "policy_changed": "the effective policy changed since the approval was requested",
    "grant_revoked": "a grant the approval relied on is no longer held",
    "stop_fenced": "the run is stop-fenced; no new effect is admitted",
    "task_expired": "the approval task expired under its timeout policy",
    "task_cancelled": "the approval task was cancelled",
    "not_replayable": "the recorded decision cannot be replayed into a new native request",
}


def feedback_message(resolution: ApprovalResolution) -> str:
    """Reviewer feedback as an instruction; it never grants authority."""

    lines = [f"Reviewer {resolution.actor_ref} {resolution.resolution_action} the request."]
    if resolution.feedback_artifact_refs:
        lines.append("Feedback artifacts: " + ", ".join(resolution.feedback_artifact_refs))
    if resolution.comment:
        lines.append("Reviewer comment (untrusted content): " + resolution.comment)
    return "\n".join(lines)


def native_reply(packet: ApprovalTaskPacket, resolution: ApprovalResolution) -> NativeReply:
    """Translate an admitted resolution to the provider-neutral reply (deny != cancel)."""

    task_id = resolution.human_task_id
    ref = resolution.resolution_ref
    origin = packet.origin
    if resolution.decision == "cancel":
        return NativeReply(
            action="cancel",
            reason="human_decision",
            interrupt=True,
            message=feedback_message(resolution),
            elicitation_action="cancel" if origin == "mcp_elicitation" else None,
            human_task_id=task_id,
            resolution_ref=ref,
        )
    if resolution.decision == "deny":
        return NativeReply(
            action="deny",
            reason="human_decision",
            interrupt=False,
            message=feedback_message(resolution),
            elicitation_action="decline" if origin == "mcp_elicitation" else None,
            human_task_id=task_id,
            resolution_ref=ref,
        )
    if origin == "provider_question":
        return NativeReply(
            action="answer",
            reason="human_decision",
            answer=resolution.answer,
            selected=resolution.selected,
            message=feedback_message(resolution) if resolution.comment else None,
            human_task_id=task_id,
            resolution_ref=ref,
        )
    if origin == "mcp_elicitation":
        return NativeReply(
            action="allow",
            reason="human_decision",
            elicitation_action="accept",
            elicitation_content=resolution.elicitation_content,
            human_task_id=task_id,
            resolution_ref=ref,
        )
    if resolution.decision == "approve_edited":
        return NativeReply(
            action="allow",
            reason="human_decision",
            updated_arguments=resolution.edited_arguments,
            updated_input_digest=resolution.edited_input_digest,
            human_task_id=task_id,
            resolution_ref=ref,
        )
    return NativeReply(
        action="allow", reason="human_decision", human_task_id=task_id, resolution_ref=ref
    )


def system_reply(packet: ApprovalTaskPacket, reason: ReplyReason) -> NativeReply:
    """A refusal Mission Control issues without a (valid) human decision; never an allow."""

    if reason == "human_decision":
        raise ValueError("a system reply has a system reason")
    return NativeReply(
        action="deny",
        reason=reason,
        interrupt=True,
        message=_SYSTEM_MESSAGES[reason],
        elicitation_action="cancel" if packet.origin == "mcp_elicitation" else None,
        human_task_id=str(packet.human_task_id),
    )


# --- Revalidation and correlations ----------------------------------------------------------


class ApprovalContextState(ApprovalContract):
    """The execution's live context an approval is revalidated against before it applies."""

    generation: int = Field(ge=1)
    policy_digest: str = Field(pattern=DIGEST_PATTERN)
    granted: bool = True


def revalidation_failure(
    binding: ApprovalBinding, state: ApprovalContextState
) -> ReplyReason | None:
    if not state.granted:
        return "grant_revoked"
    if state.generation != binding.generation:
        return "stale_generation"
    if state.policy_digest != binding.policy_digest:
        return "policy_changed"
    return None


class ApprovalCorrelation(ApprovalContract):
    """One live native request handle bound to an approval task (never reused)."""

    correlation_id: str = Field(min_length=1, max_length=64)
    request_scope: str = Field(min_length=1)
    human_task_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    harness_execution_id: str = Field(min_length=1, max_length=512)
    generation: int = Field(ge=1)
    connection_ref: str = Field(min_length=1, max_length=512)
    native: NativeApprovalCorrelation
    input_digest: str = Field(pattern=DIGEST_PATTERN)
    state: CorrelationState = "live"
    opened_at: AwareDatetime
    wait_deadline_at: AwareDatetime | None = None
    closed_at: AwareDatetime | None = None
    close_reason: str | None = Field(default=None, min_length=1, max_length=128)
    reply: NativeReply | None = None
    replayed_from: str | None = Field(default=None, min_length=1)
    version: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def _closed(self) -> ApprovalCorrelation:
        if (self.state == "live") != (self.closed_at is None):
            raise ValueError("exactly a live correlation has no closed_at")
        if self.state == "answered" and self.reply is None:
            raise ValueError("an answered correlation records the reply it sent")
        return self


# --- Ports ----------------------------------------------------------------------------------


class ApprovalTaskRepository(Protocol):
    """Approval-origin tasks on the common human_task/human_resolution rows."""

    async def open_task(self, packet: ApprovalTaskPacket, *, actor_ref: str) -> ApprovalTaskView:
        """Persist task + `human_task.created` event once; idempotent per identity."""
        ...

    async def get_task(
        self, request_scope: str, human_task_id: UUID | str
    ) -> ApprovalTaskView | None: ...

    async def list_tasks(
        self,
        request_scope: str,
        *,
        run_id: str | None = None,
        lifecycle: str | None = None,
        limit: int = 100,
    ) -> tuple[ApprovalTaskView, ...]: ...

    async def resolve_task(
        self,
        request_scope: str,
        human_task_id: UUID | str,
        decide: Callable[[ApprovalTaskView], ApprovalResolutionCheck],
    ) -> tuple[ApprovalResolutionCheck, ApprovalTaskView] | None:
        """Decide under the task row lock; write the one resolution and transition."""
        ...

    async def apply_timeout(
        self, request_scope: str, human_task_id: UUID | str, *, now: datetime
    ) -> ApprovalTaskView | None: ...

    async def cancel_task(
        self, request_scope: str, human_task_id: UUID | str, *, now: datetime, actor_ref: str
    ) -> ApprovalTaskView | None:
        """Stop/cancel path: close an open task without a resolution."""
        ...


class ApprovalCorrelationRepository(Protocol):
    async def open_correlation(self, correlation: ApprovalCorrelation) -> ApprovalCorrelation:
        """Insert a live correlation; older live correlations of the task are superseded."""
        ...

    async def get_correlation(
        self, request_scope: str, correlation_id: str
    ) -> ApprovalCorrelation | None: ...

    async def close_correlation(
        self,
        request_scope: str,
        correlation_id: str,
        *,
        state: CorrelationState,
        at: datetime,
        reason: str,
        reply: NativeReply | None = None,
        replayed_from: str | None = None,
    ) -> ApprovalCorrelation | None:
        """Compare-and-set from `live`; None when the correlation is no longer live."""
        ...

    async def mark_lost(
        self,
        request_scope: str,
        *,
        harness_execution_id: str,
        except_connection_ref: str,
        at: datetime,
    ) -> tuple[ApprovalCorrelation, ...]:
        """Close every live correlation of the execution held by another connection."""
        ...


class ApprovalContextProbe(Protocol):
    """Current generation, effective policy digest and grants of one harness execution.

    Production reads the harness execution's generation (MP-06 state store) and its
    `mc.execution_binding.v2` policy digest; a revoked grant reports `granted=False`.
    """

    async def current(
        self, request_scope: str, *, run_id: str, harness_execution_id: str
    ) -> ApprovalContextState: ...


class ApprovalTaskWake(Protocol):
    """Nudges in-process waiters after a committed resolution (a hint, never authority)."""

    async def resolution_committed(self, task: ApprovalTaskView) -> None: ...


__all__ = [
    "APPROVAL_RESOLUTION_SCHEMA",
    "APPROVAL_TASK_KIND_PREFIX",
    "APPROVAL_TASK_SCHEMA",
    "APPROVAL_TIMEOUT_ACTOR",
    "NATIVE_ORIGINS",
    "ApprovalContextProbe",
    "ApprovalContextState",
    "ApprovalCorrelation",
    "ApprovalCorrelationRepository",
    "ApprovalResolution",
    "ApprovalResolutionCheck",
    "ApprovalResolutionRequest",
    "ApprovalTaskPacket",
    "ApprovalTaskRepository",
    "ApprovalTaskView",
    "ApprovalTaskWake",
    "ElicitationPrompt",
    "NativeOrigin",
    "NativeReply",
    "QuestionPrompt",
    "ToolPreview",
    "approval_request_key",
    "approval_task_id",
    "approval_task_key",
    "approval_timed_out",
    "decide_approval_resolution",
    "feedback_message",
    "is_reviewer",
    "is_secret_key",
    "native_reply",
    "normalized_arguments",
    "open_approval_packet",
    "redacted_preview",
    "revalidation_failure",
    "system_reply",
    "tool_input_digest",
    "tool_preview",
]
