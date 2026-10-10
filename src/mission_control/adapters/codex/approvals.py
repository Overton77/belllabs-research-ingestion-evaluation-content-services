"""Codex approval and user-input server requests over MP-11's `ApprovalBroker` (MP-08).

A server-originated request (`item/commandExecution/requestApproval`,
`item/fileChange/requestApproval`, `item/permissions/requestApproval`,
`item/tool/requestUserInput`, `mcpServer/elicitation/request`) is a pending runtime
operation (SPEC-03 "Native provider requests"). This adapter turns it into MP-11
`NativeApprovalRequest`s, binds them (the approval Human Task and a live correlation are
persisted) BEFORE anything waits or is held, waits bounded, and maps the provider-neutral
`NativeReply` onto the response shapes of the pinned schema (codex-cli 0.162.0).

Binding (per the MP-08 handoff mapping):

- origin per method: command / fileChange / permissions -> `provider_permission`,
  requestUserInput -> `provider_question`, elicitation -> `mcp_elicitation`;
- `native`: `native_session_ref` = threadId, `native_turn_ref` = turnId,
  `native_request_ref` = the JSON-RPC id, `tool_call_ref` = `itemId` (plus `:<approvalId>`
  when the zsh-exec-bridge sends several callbacks for one item), `connection_scoped=True`;
  without an item id (elicitations) the task identity is the request id plus the broker's
  `connection_ref`, which carries the app-server connection epoch (JSON-RPC ids restart with
  every app-server process);
- `replay_strategy = park_for_reconciliation`: a JSON-RPC request handle never survives its
  connection, so a decision is never replayed into a new request (`reissue_native_request`
  would need the drill to prove an item survives `thread/resume`);
- `policy_digest` = the execution's binding digest, the same value `LaneTurnService` records
  as `harness_execution.intended_binding_digest` and `PostgresApprovalContextProbe` compares
  with (NOT `ProviderExecutionBinding.policy_digest`: with that value every resolution would
  revalidate as `policy_changed`); `generation` = the turn request's generation.

Questions: Codex sends `questions[]`; MP-11's `QuestionPrompt` holds one question. Each
question is bound as its own task (`tool_call_ref = <itemId>:q:<questionId>`), all of them
before any wait; they are awaited together and answered together only when every one was
answered; any other outcome refuses the whole request (JSON-RPC -32002). A question flagged
`isSecret` refuses the request without binding anything: a credential is never collected
through a Human Task.

Replies (`NativeReply` -> pinned response):

- command / fileChange: allow -> `{"decision": "accept"}` (never `acceptForSession` or an
  execpolicy/network amendment: they exceed the exact input approved); allow with edited
  arguments (`approve_edited`) -> JSON-RPC error -32001 (edits are not expressible);
  deny without interrupt -> `{"decision": "decline"}` (the turn continues); cancel or any
  system reply (interrupt) -> `{"decision": "cancel"}` (the turn is interrupted);
- permissions: allow -> `{"permissions": <the requested profile>}`; anything else -> -32002
  (the response schema has no refusal variant);
- requestUserInput: answers -> `{"answers": {<id>: {"answers": [answer, *selected]}}}`;
  anything else -> -32002;
- elicitation: `{"action": accept|decline|cancel}`, with `content` on an accept.

Without a broker the lane fails closed: nothing is displayed; command/fileChange are declined,
elicitations declined, permissions and user input refused (-32002).
"""

from __future__ import annotations

import asyncio
import copy
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Final

from mission_control.adapters.codex.protocol import (
    ERR_DECLINED,
    ERR_REFUSED_BY_MISSION_CONTROL,
    R_COMMAND_APPROVAL,
    R_FILE_CHANGE_APPROVAL,
    R_MCP_ELICITATION,
    R_PERMISSIONS_APPROVAL,
    R_USER_INPUT,
)
from mission_control.adapters.codex.transport import InboundEvent
from mission_control.application.execution.approvals import (
    ElicitationPrompt,
    NativeOrigin,
    NativeReply,
    QuestionPrompt,
)
from mission_control.application.execution.approvals_broker import (
    ApprovalBroker,
    ApprovalOutcome,
    BoundApproval,
    NativeApprovalRequest,
    RecoveryItem,
)
from mission_control.domain.execution.approvals import NativeApprovalCorrelation
from mission_control.domain.policies.stop_fence import EffectKind

_LOGGER = logging.getLogger(__name__)
PROFILE: Final = "codex"
DEFAULT_APPROVAL_WAIT_SECONDS: Final = 300.0
DEFAULT_APPROVAL_DEADLINE: Final = timedelta(minutes=30)
# The reviewer role a principal holds directly or through a `reviewer:owner` grant.
DEFAULT_APPROVAL_REVIEWERS: Final = ("owner",)
APPROVAL_ORIGINS: Final[Mapping[str, NativeOrigin]] = {
    R_COMMAND_APPROVAL: "provider_permission",
    R_FILE_CHANGE_APPROVAL: "provider_permission",
    R_PERMISSIONS_APPROVAL: "provider_permission",
    R_USER_INPUT: "provider_question",
    R_MCP_ELICITATION: "mcp_elicitation",
}
TOOL_NAMES: Final[Mapping[str, str]] = {
    R_COMMAND_APPROVAL: "commandExecution",
    R_FILE_CHANGE_APPROVAL: "fileChange",
    R_PERMISSIONS_APPROVAL: "permissions",
    R_USER_INPUT: "requestUserInput",
}
EFFECT_KINDS: Final[Mapping[str, EffectKind]] = {
    R_COMMAND_APPROVAL: "shell",
    R_FILE_CHANGE_APPROVAL: "file",
    R_PERMISSIONS_APPROVAL: "other",
    R_USER_INPUT: "other",
    R_MCP_ELICITATION: "mcp",
}
# Volatile request fields that are not part of what a reviewer approves.
VOLATILE_FIELDS: Final = frozenset({"startedAtMs", "autoResolutionMs", "_meta"})
_ELICITATION_FORM_MODES: Final = frozenset({"form", "openai/form", "openaiForm"})

Gate = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class NativeApprovalResponse:
    """The typed native answer: a JSON-RPC result or an error (refusal)."""

    result: dict[str, Any] | None = None
    error_code: int | None = None
    error_message: str = ""

    @property
    def is_error(self) -> bool:
        return self.error_code is not None


@dataclass(frozen=True)
class ApprovalContext:
    """Where a server request was received: the execution and the app-server connection."""

    request_scope: str
    run_id: str
    harness_execution_id: str
    generation: int
    native_session_ref: str | None
    policy_digest: str
    connection_epoch: str


class ApprovalNotBindable(ValueError):
    """The request cannot be shown to a reviewer safely (e.g. a secret question, an
    elicitation form that collects credentials); it is refused without a task."""


@dataclass(frozen=True)
class ServedApproval:
    """What one served request bound and answered (introspection and evidence)."""

    method: str
    request_id: int | str
    epoch: str
    bound: tuple[BoundApproval, ...]
    outcomes: tuple[ApprovalOutcome, ...]
    response: NativeApprovalResponse
    refused: str | None = None


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def request_arguments(params: Mapping[str, Any]) -> dict[str, Any]:
    """The exact request a reviewer approves: the params without volatile timestamps."""

    return {key: value for key, value in params.items() if key not in VOLATILE_FIELDS}


def tool_call_ref(params: Mapping[str, Any]) -> str | None:
    """`itemId`, plus `:<approvalId>` for zsh-exec-bridge subcommand callbacks (one item, several
    callbacks: the schema's "distinct opaque callback id")."""

    item = _text(params.get("itemId"))
    if item is None:
        return None
    approval = _text(params.get("approvalId"))
    return f"{item}:{approval}" if approval else item


def _prompt(method: str, params: Mapping[str, Any]) -> str:
    reason = _text(params.get("reason"))
    if method == R_COMMAND_APPROVAL:
        text = f"Codex asks to run a command: {params.get('command') or '(not shown)'}"
        if _text(params.get("cwd")):
            text += f"\nWorking directory: {params['cwd']}"
    elif method == R_FILE_CHANGE_APPROVAL:
        text = "Codex asks to apply a file change"
        if _text(params.get("grantRoot")):
            text += f" (and write access under {params['grantRoot']} for the session)"
    elif method == R_PERMISSIONS_APPROVAL:
        text = "Codex asks for additional sandbox permissions"
        if _text(params.get("cwd")):
            text += f" in {params['cwd']}"
    elif method == R_MCP_ELICITATION:
        text = (
            f"MCP server {params.get('serverName') or '(unnamed)'} asks: "
            f"{params.get('message') or ''}"
        )
    else:
        text = "Codex asks a question"
    if reason:
        text += f"\nReason: {reason}"
    return text[:8_192]


def _question_prompt(question: Mapping[str, Any]) -> QuestionPrompt:
    header = _text(question.get("header"))
    body = _text(question.get("question")) or header or "(empty question)"
    text = f"{header}: {body}" if header and header != body else body
    labels: list[str] = []
    for option in question.get("options") or ():
        if isinstance(option, Mapping):
            label = _text(option.get("label"))
            if label and label[:512] not in labels:
                labels.append(label[:512])
    return QuestionPrompt(question=text[:4_000], options=tuple(labels[:32]))


def _elicitation_prompt(params: Mapping[str, Any]) -> ElicitationPrompt:
    mode = params.get("mode")
    message = _text(params.get("message")) or "(no message)"
    server = _text(params.get("serverName"))
    if mode in _ELICITATION_FORM_MODES:
        schema = params.get("requestedSchema")
        if not isinstance(schema, Mapping):
            raise ApprovalNotBindable("the elicitation form carries no object schema")
        return ElicitationPrompt(
            mode="form",
            message=message[:4_000],
            requested_schema=dict(schema),
            server_name=server[:256] if server else None,
        )
    if mode == "url":
        url = _text(params.get("url"))
        return ElicitationPrompt(
            mode="url",
            message=message[:4_000],
            url=url,
            server_name=server[:256] if server else None,
        )
    raise ApprovalNotBindable(f"elicitation mode {mode!r} is not in the pinned schema")


def native_requests(
    event: InboundEvent,
    context: ApprovalContext,
    *,
    reviewers: tuple[str, ...] = DEFAULT_APPROVAL_REVIEWERS,
    deadline: timedelta | None = DEFAULT_APPROVAL_DEADLINE,
) -> tuple[NativeApprovalRequest, ...]:
    """MP-11 requests for one Codex server request (one per question for user input)."""

    method, params = event.method, event.params
    if method not in APPROVAL_ORIGINS:
        raise ApprovalNotBindable(f"{method} is not an approval-class request")
    if event.request_id is None:
        raise ApprovalNotBindable("a server request carries a JSON-RPC id")
    origin = APPROVAL_ORIGINS[method]
    call = tool_call_ref(params)
    base: dict[str, Any] = {
        "request_scope": context.request_scope,
        "run_id": context.run_id,
        "lane_profile": PROFILE,
        "origin": origin,
        "harness_execution_id": context.harness_execution_id,
        "generation": context.generation,
        "effect_kind": EFFECT_KINDS[method],
        "policy_digest": context.policy_digest,
        "reviewers": reviewers,
        "prompt": _prompt(method, params),
        "timeout_seconds": int(deadline.total_seconds()) if deadline is not None else None,
        "on_timeout": "keep_waiting",
        "replay_strategy": "park_for_reconciliation",
    }

    def native(ref: str | None) -> NativeApprovalCorrelation:
        return NativeApprovalCorrelation(
            native_session_ref=_text(params.get("threadId")) or context.native_session_ref,
            native_turn_ref=_text(params.get("turnId")),
            native_request_ref=str(event.request_id)[:1_024],
            tool_call_ref=ref[:1_024] if ref else None,
            connection_scoped=True,
        )

    try:
        if method == R_USER_INPUT:
            questions = [
                item for item in params.get("questions") or () if isinstance(item, Mapping)
            ]
            if not questions:
                raise ApprovalNotBindable("requestUserInput carries no question")
            if any(bool(item.get("isSecret")) for item in questions):
                raise ApprovalNotBindable(
                    "a secret question is never collected through a Human Task"
                )
            ids = [_text(item.get("id")) for item in questions]
            if any(item is None for item in ids) or len(set(ids)) != len(ids):
                raise ApprovalNotBindable("every question names a distinct id")
            return tuple(
                NativeApprovalRequest(
                    **{**base, "prompt": f"{base['prompt']} ({index + 1}/{len(questions)})"},
                    native=native(f"{call or event.request_id}:q:{question['id']}"),
                    tool_name=TOOL_NAMES[method],
                    arguments={
                        "threadId": params.get("threadId"),
                        "turnId": params.get("turnId"),
                        "itemId": params.get("itemId"),
                        "question": request_arguments(question),
                    },
                    question=_question_prompt(question),
                )
                for index, question in enumerate(questions)
            )
        if method == R_MCP_ELICITATION:
            return (
                NativeApprovalRequest(
                    **base,
                    native=native(call),
                    tool_name=None,
                    arguments=request_arguments(params),
                    elicitation=_elicitation_prompt(params),
                ),
            )
        return (
            NativeApprovalRequest(
                **base,
                native=native(call),
                tool_name=TOOL_NAMES[method],
                arguments=request_arguments(params),
            ),
        )
    except ApprovalNotBindable:
        raise
    except ValueError as error:
        raise ApprovalNotBindable(f"the request cannot be bound: {error}"[:512]) from error


def fail_closed_response(method: str, reason: str) -> NativeApprovalResponse:
    """The answer when no broker is composed or the request cannot be bound safely."""

    if method in {R_COMMAND_APPROVAL, R_FILE_CHANGE_APPROVAL}:
        return NativeApprovalResponse(result={"decision": "decline"})
    if method == R_MCP_ELICITATION:
        return NativeApprovalResponse(result={"action": "decline"})
    if method in {R_PERMISSIONS_APPROVAL, R_USER_INPUT}:
        return NativeApprovalResponse(error_code=ERR_DECLINED, error_message=reason[:512])
    return NativeApprovalResponse(
        error_code=ERR_REFUSED_BY_MISSION_CONTROL, error_message=f"{method} is not answered"
    )


def native_response(
    method: str, params: Mapping[str, Any], replies: Sequence[NativeReply]
) -> NativeApprovalResponse:
    """Map broker replies onto the pinned response (one reply; one per question for input)."""

    if not replies:
        return fail_closed_response(method, "no reply")
    if method == R_USER_INPUT:
        questions = [item for item in params.get("questions") or () if isinstance(item, Mapping)]
        if len(questions) != len(replies) or any(reply.action != "answer" for reply in replies):
            return NativeApprovalResponse(
                error_code=ERR_DECLINED, error_message="user input was not answered"
            )
        answers: dict[str, Any] = {}
        for question, reply in zip(questions, replies, strict=True):
            values = [item for item in (reply.answer, *reply.selected) if item]
            answers[str(question["id"])] = {"answers": values}
        return NativeApprovalResponse(result={"answers": answers})
    (reply,) = replies
    if method in {R_COMMAND_APPROVAL, R_FILE_CHANGE_APPROVAL}:
        if reply.action == "allow":
            if reply.updated_arguments is not None:
                return NativeApprovalResponse(
                    error_code=ERR_REFUSED_BY_MISSION_CONTROL,
                    error_message="edited arguments cannot be applied to a native approval",
                )
            return NativeApprovalResponse(result={"decision": "accept"})
        if reply.action == "deny" and not reply.interrupt:
            return NativeApprovalResponse(result={"decision": "decline"})
        if reply.action in {"deny", "cancel"}:
            return NativeApprovalResponse(result={"decision": "cancel"})
        return NativeApprovalResponse(
            error_code=ERR_REFUSED_BY_MISSION_CONTROL,
            error_message=f"{reply.action} is not an approval decision",
        )
    if method == R_PERMISSIONS_APPROVAL:
        if reply.action == "allow" and reply.updated_arguments is None:
            # Grant exactly the requested profile (`PermissionsRequestApprovalResponse`).
            return NativeApprovalResponse(
                result={"permissions": dict(params.get("permissions") or {})}
            )
        if reply.action == "allow":
            return NativeApprovalResponse(
                error_code=ERR_REFUSED_BY_MISSION_CONTROL,
                error_message="edited permissions cannot be applied to a native approval",
            )
        return NativeApprovalResponse(
            error_code=ERR_DECLINED, error_message=f"permissions {reply.action}: {reply.reason}"
        )
    if method == R_MCP_ELICITATION:
        action = reply.elicitation_action
        if action is None:
            action = (
                "accept"
                if reply.action == "allow"
                else ("cancel" if reply.interrupt else "decline")
            )
        result: dict[str, Any] = {"action": action}
        if action == "accept" and reply.elicitation_content is not None:
            result["content"] = dict(reply.elicitation_content)
        return NativeApprovalResponse(result=result)
    return fail_closed_response(method, f"{method} is not answered")


@dataclass
class CodexApprovals:
    """The lane's approval port: an adapter over one worker's `ApprovalBroker`.

    `broker` is the worker's broker (its `connection_ref` the worker session owner); each
    app-server connection gets a view whose `connection_ref` is `<owner>:codex:<epoch>`, so
    a relaunch is another connection to `recover` and a reused JSON-RPC id never collides
    with an earlier task.
    """

    broker: ApprovalBroker | None = None
    wait_seconds: float = DEFAULT_APPROVAL_WAIT_SECONDS
    reviewers: tuple[str, ...] = DEFAULT_APPROVAL_REVIEWERS
    deadline: timedelta | None = DEFAULT_APPROVAL_DEADLINE
    served: list[ServedApproval] = field(default_factory=list)
    recovered: list[RecoveryItem] = field(default_factory=list)
    _views: dict[str, ApprovalBroker] = field(default_factory=dict)

    @property
    def composed(self) -> bool:
        return self.broker is not None

    def connection_ref(self, epoch: str) -> str | None:
        if self.broker is None:
            return None
        return f"{self.broker.connection_ref}:codex:{epoch}"[:512]

    def for_connection(self, epoch: str) -> ApprovalBroker:
        """The broker view for one app-server connection (shared stores, probe, fences, hub)."""

        assert self.broker is not None
        view = self._views.get(epoch)
        if view is None:
            view = copy.copy(self.broker)
            view.connection_ref = self.connection_ref(epoch) or self.broker.connection_ref
            self._views[epoch] = view
        return view

    async def recover(
        self, request_scope: str, harness_execution_id: str, epoch: str
    ) -> tuple[RecoveryItem, ...]:
        """After a (re)launch, before any re-dispatch: every live correlation another
        connection held is `lost` (never reused); the recorded strategy is the plan."""

        if self.broker is None:
            return ()
        items = await self.for_connection(epoch).recover(request_scope, harness_execution_id)
        for item in items:
            _LOGGER.warning(
                "codex approval correlation %s lost with its connection; %s",
                item.correlation.correlation_id,
                item.action,
            )
        self.recovered.extend(items)
        return items

    async def serve(
        self, event: InboundEvent, context: ApprovalContext, *, gate: Gate | None = None
    ) -> NativeApprovalResponse:
        """Bind (persist) every request first, then pass the tool gate (pause), then wait
        bounded and map the replies. Runs as its own task: the event pump never waits here."""

        assert event.request_id is not None
        if self.broker is None:
            if gate is not None:
                await gate()
            response = fail_closed_response(event.method, "no approval broker composed")
            self._record(event, context, (), (), response, "no approval broker composed")
            return response
        try:
            requests = native_requests(
                event, context, reviewers=self.reviewers, deadline=self.deadline
            )
        except ApprovalNotBindable as refused:
            _LOGGER.warning("codex %s refused without a task: %s", event.method, refused)
            if gate is not None:
                await gate()
            response = fail_closed_response(event.method, str(refused))
            self._record(event, context, (), (), response, str(refused))
            return response
        broker = self.for_connection(context.connection_epoch)
        bound: list[BoundApproval] = []
        for request in requests:
            bound.append(await broker.bind(request))
        if gate is not None:
            await gate()
        outcomes = await asyncio.gather(
            *(broker.wait(item, wait_seconds=self.wait_seconds) for item in bound)
        )
        response = native_response(event.method, event.params, [item.reply for item in outcomes])
        self._record(event, context, tuple(bound), tuple(outcomes), response, None)
        return response

    def _record(
        self,
        event: InboundEvent,
        context: ApprovalContext,
        bound: tuple[BoundApproval, ...],
        outcomes: tuple[ApprovalOutcome, ...],
        response: NativeApprovalResponse,
        refused: str | None,
    ) -> None:
        assert event.request_id is not None
        self.served.append(
            ServedApproval(
                method=event.method,
                request_id=event.request_id,
                epoch=context.connection_epoch,
                bound=bound,
                outcomes=outcomes,
                response=response,
                refused=refused,
            )
        )


__all__ = [
    "APPROVAL_ORIGINS",
    "DEFAULT_APPROVAL_DEADLINE",
    "DEFAULT_APPROVAL_REVIEWERS",
    "DEFAULT_APPROVAL_WAIT_SECONDS",
    "EFFECT_KINDS",
    "TOOL_NAMES",
    "ApprovalContext",
    "ApprovalNotBindable",
    "CodexApprovals",
    "NativeApprovalResponse",
    "ServedApproval",
    "fail_closed_response",
    "native_requests",
    "native_response",
    "request_arguments",
    "tool_call_ref",
]
