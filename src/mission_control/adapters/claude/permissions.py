"""Native permission requests bound to `mc.approval_binding.v1` (SPEC-03 human control; MP-07).

`ClaudeAgentOptions.can_use_tool` (`claude_agent_sdk.types.CanUseTool`) is invoked when the
CLI's permission rules evaluate to *ask*; it is not invoked for tools `allowed_tools`, the
permission mode or settings allow rules already admit, and a `PreToolUse` hook that allows
also skips it (`types.ClaudeAgentOptions.can_use_tool` docstring). The request arrives as
`(tool_name, input, types.ToolPermissionContext)` whose `tool_use_id` is always set on the
wire; the answer is `types.PermissionResultAllow(updated_input=...)` or
`types.PermissionResultDeny(message, interrupt)`.

This module binds each request to the frozen `domain/execution/approvals.ApprovalBinding`:
origin `provider_permission`; native correlation = session / turn / `tool_call_ref =
tool_use_id` with `connection_scoped=False`; `input_digest` = MP-11 `tool_input_digest(tool,
input)`; `policy_digest` = the execution's binding digest (what MP-11's
`PostgresApprovalContextProbe` revalidates against); `generation` = the turn request's
generation. It asks a `PermissionBindingPort` for the decision: `approvals.
BrokerPermissionBinding` (MP-11's durable broker) in production, `DenyWithoutGateway` (the
fail-closed default) when no broker is composed.

Why `tool_use_id` is stable and the replay strategy is `reissue_native_request` (pinned
`claude_agent_sdk==0.2.165` source): the SDK reads `tool_use_id` from the permission request
body (`_internal/query.py` `_handle_control_request`, `can_use_tool` branch), separately from
the control protocol's own `request_id` (the connection-scoped handle the callback never
sees); `types.ToolPermissionContext.tool_use_id` documents it as the identifier of *this tool
call within the assistant message*, i.e. the model's `tool_use` block id that the transcript
keeps. The SDK documents re-delivery of the same tool call after the run stopped
(`types.DeferredToolUse`, `PreToolUseHookSpecificOutput.permissionDecision == "defer"`): a
fresh `can_use_tool` for the same `tool_use_id` is a reissue of the same native request. The
MP-11 broker replays a recorded approval into it only when the task identity matches (same
run, execution, generation, tool call, normalized input digest and policy digest) and after
revalidation and Stop Fence admission; a denial or cancel always replays. Nothing here keeps a
connection-scoped handle.

`AskUserQuestion` is not claimed as `provider_question`: the Python SDK source has no
user-question routing (the `can_use_tool` branch is tool-name agnostic) and the answer shape
the CLI expects in `updated_input` is not in the installed Python source, so such a request
is bound as an ordinary `provider_permission` (approve / approve_edited / deny / cancel only).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Protocol, runtime_checkable

from claude_agent_sdk.types import (
    PermissionResult,
    PermissionResultAllow,
    PermissionResultDeny,
    ToolPermissionContext,
)
from pydantic import BaseModel, ConfigDict, Field

from mission_control.adapters.claude.hooks import FrameEmitter
from mission_control.application.execution.approvals import tool_input_digest
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.approvals import (
    ApprovalBinding,
    ApprovalDecision,
    NativeApprovalCorrelation,
    ReplayStrategy,
)
from mission_control.domain.execution.lanes import LaneProfileName

PERMISSION_REQUESTED: Final = "permission.requested"
PERMISSION_RESOLVED: Final = "permission.resolved"
NO_GATEWAY: Final = "no approval gateway is bound to this lane (MP-11); the request is denied"
# See the module docstring: `tool_use_id` is a stable tool-call identity, reissued after defer.
DEFAULT_REPLAY_STRATEGY: Final[ReplayStrategy] = "reissue_native_request"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PermissionRequest(_Strict):
    """One native permission request as the SDK delivered it (secret-free input copy)."""

    tool_name: str = Field(min_length=1, max_length=256)
    tool_input: dict[str, Any] = Field(default_factory=dict)
    tool_use_id: str = Field(min_length=1, max_length=1_024)
    agent_id: str | None = None
    blocked_path: str | None = None
    decision_reason: str | None = None
    title: str | None = None
    display_name: str | None = None
    description: str | None = None
    suggestions: tuple[dict[str, Any], ...] = ()


class PermissionScope(_Strict):
    """Where the bound request lives: the tenant scope and the run key (`LaneExecutionIdentity.
    run_key`, what MP-11's approval tasks and context probe are keyed by)."""

    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)


class PermissionOutcome(_Strict):
    """The human (or policy) decision the port returns for one binding.

    `interrupt` asks the SDK to stop the turn with the denial (`PermissionResultDeny.
    interrupt`): a reviewer's `deny` continues the turn with the feedback as an instruction,
    a `cancel` or any system refusal (wait expired, stale generation, policy changed, Stop
    Fence...) interrupts. `reason` is the broker's reply reason, recorded in the frame only.
    """

    decision: ApprovalDecision
    message: str = Field(default="", max_length=8_192)
    updated_input: dict[str, Any] | None = None
    human_task_ref: str | None = None
    interrupt: bool = False
    reason: str | None = Field(default=None, max_length=128)


class PermissionBindingPort(Protocol):
    """Opens the durable review of one bound native request and returns the decision.

    The production implementation (`approvals.BrokerPermissionBinding`) persists the binding
    as an approval Human Task through MP-11's broker and waits a *bounded* time for the
    decision; the lane never sleeps indefinitely in the SDK callback. A port that cannot
    bind answers `deny`.
    """

    async def resolve(
        self, binding: ApprovalBinding, request: PermissionRequest, *, scope: PermissionScope
    ) -> PermissionOutcome: ...


@runtime_checkable
class RecoveringPermissionPort(Protocol):
    """A port whose native correlations outlive the process (MP-11 `ApprovalBroker.recover`):
    the harness calls `recover` when it resumes a session in a new process, before anything
    is re-dispatched, so correlations another connection held are closed `lost`, never reused.
    """

    async def recover(self, request_scope: str, harness_execution_id: str) -> tuple[Any, ...]: ...


class DenyWithoutGateway:
    """The fail-closed default port: no broker is composed, so every request is denied and
    recorded."""

    def __init__(self) -> None:
        self.denied: list[ApprovalBinding] = []

    async def resolve(
        self, binding: ApprovalBinding, request: PermissionRequest, *, scope: PermissionScope
    ) -> PermissionOutcome:
        del request, scope
        self.denied.append(binding)
        return PermissionOutcome(decision="deny", message=NO_GATEWAY)


def human_task_id(harness_execution_id: str, generation: int, tool_use_id: str) -> str:
    return f"claude-permission:{harness_execution_id}:{generation}:{tool_use_id}"[:512]


def to_permission_result(binding: ApprovalBinding, outcome: PermissionOutcome) -> PermissionResult:
    """Map an admitted decision onto the SDK result; anything not admitted denies."""

    if not binding.admits(outcome.decision):
        return PermissionResultDeny(
            message=f"decision {outcome.decision} is not admitted for {binding.origin}"
        )
    if outcome.decision == "approve":
        return PermissionResultAllow()
    if outcome.decision == "approve_edited":
        if outcome.updated_input is None:
            return PermissionResultDeny(message="approve_edited without the edited input")
        return PermissionResultAllow(updated_input=dict(outcome.updated_input))
    if outcome.decision == "cancel":
        return PermissionResultDeny(message=outcome.message or "cancelled", interrupt=True)
    return PermissionResultDeny(message=outcome.message or "denied", interrupt=outcome.interrupt)


class PermissionBinder:
    """Builds bindings for native requests and serves `can_use_tool` for one session."""

    def __init__(
        self,
        port: PermissionBindingPort,
        *,
        lane_profile: LaneProfileName,
        harness_execution_id: str,
        generation: int,
        policy_digest: str,
        emitter: FrameEmitter,
        session_ref: Callable[[], str | None],
        turn_ref: Callable[[], str | None],
        scope: PermissionScope,
        clock: Callable[[], datetime] | None = None,
        deadline: timedelta | None = None,
        replay_strategy: ReplayStrategy = DEFAULT_REPLAY_STRATEGY,
    ) -> None:
        self._port = port
        self._lane_profile = lane_profile
        self._heid = harness_execution_id
        self._generation = generation
        self._policy_digest = policy_digest
        self._scope = scope
        self._replay: ReplayStrategy = replay_strategy
        self._emitter = emitter
        self._session_ref = session_ref
        self._turn_ref = turn_ref
        self._clock = clock or (lambda: datetime.now(UTC))
        self._deadline = deadline
        self.bindings: list[ApprovalBinding] = []
        self.outcomes: list[tuple[str, PermissionOutcome]] = []

    def binding(self, request: PermissionRequest) -> ApprovalBinding:
        opened = self._clock()
        return ApprovalBinding(
            human_task_id=human_task_id(self._heid, self._generation, request.tool_use_id),
            origin="provider_permission",
            lane_profile=self._lane_profile,
            harness_execution_id=self._heid,
            generation=self._generation,
            native=NativeApprovalCorrelation(
                native_session_ref=self._session_ref(),
                native_turn_ref=self._turn_ref(),
                # The control `request_id` is connection-scoped and never reaches the
                # callback; the tool call is the stable identity (module docstring).
                tool_call_ref=request.tool_use_id,
                connection_scoped=False,
            ),
            tool_name=request.tool_name,
            input_digest=tool_input_digest(request.tool_name, request.tool_input),
            policy_digest=self._policy_digest,
            opened_at=opened,
            deadline=opened + self._deadline if self._deadline is not None else None,
            replay_strategy=self._replay,
        )

    async def can_use_tool(
        self, tool_name: str, tool_input: dict[str, Any], context: ToolPermissionContext
    ) -> PermissionResult:
        """`types.CanUseTool`: bind, record, ask the port, record, answer."""

        request = PermissionRequest(
            tool_name=tool_name,
            tool_input=dict(tool_input),
            tool_use_id=context.tool_use_id or f"unidentified:{sha256_digest(tool_input)[7:23]}",
            agent_id=context.agent_id,
            blocked_path=context.blocked_path,
            decision_reason=context.decision_reason,
            title=context.title,
            display_name=context.display_name,
            description=context.description,
            suggestions=tuple(dataclasses.asdict(item) for item in context.suggestions),
        )
        binding = self.binding(request)
        self.bindings.append(binding)
        await self._emitter.emit(
            PERMISSION_REQUESTED,
            {
                "binding": binding.model_dump(mode="json"),
                "tool_name": request.tool_name,
                "tool_use_id": request.tool_use_id,
                "agent_id": request.agent_id,
                "input": request.tool_input,
                "title": request.title,
                "decision_reason": request.decision_reason,
            },
            tool_call_ref=request.tool_use_id,
        )
        try:
            outcome = await self._port.resolve(binding, request, scope=self._scope)
        except Exception as error:  # the gateway failed: the request is denied, never allowed
            outcome = PermissionOutcome(
                decision="deny",
                message=f"approval gateway failed: {type(error).__name__}"[:1_024],
                interrupt=True,
                reason="gateway_failed",
            )
        self.outcomes.append((binding.human_task_id, outcome))
        result = to_permission_result(binding, outcome)
        await self._emitter.emit(
            PERMISSION_RESOLVED,
            {
                "human_task_id": binding.human_task_id,
                "tool_use_id": request.tool_use_id,
                "decision": outcome.decision,
                "behavior": result.behavior,
                "interrupt": bool(getattr(result, "interrupt", False)),
                "reason": outcome.reason,
                "message": outcome.message,
                "human_task_ref": outcome.human_task_ref,
                "input_digest": binding.input_digest,
            },
            tool_call_ref=request.tool_use_id,
        )
        return result


def request_body(request: PermissionRequest) -> Mapping[str, Any]:
    return request.model_dump(mode="json")


__all__ = [
    "DEFAULT_REPLAY_STRATEGY",
    "NO_GATEWAY",
    "PERMISSION_REQUESTED",
    "PERMISSION_RESOLVED",
    "DenyWithoutGateway",
    "PermissionBinder",
    "PermissionBindingPort",
    "PermissionOutcome",
    "PermissionRequest",
    "PermissionScope",
    "RecoveringPermissionPort",
    "human_task_id",
    "to_permission_result",
]
