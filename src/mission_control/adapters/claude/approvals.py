"""`PermissionBindingPort` over MP-11's durable `ApprovalBroker` (SPEC-03; MP-07 x MP-11).

One `can_use_tool` request becomes one `NativeApprovalRequest` (MP-11 handoff, "Claude"
mapping):

- origin `provider_permission`; `native` = the lane's correlation (session, turn,
  `tool_call_ref = ToolPermissionContext.tool_use_id`, `connection_scoped=False`);
- `generation` = the turn request's generation, `policy_digest` = the execution's binding
  digest (the value `adapters/postgres/approvals/context.PostgresApprovalContextProbe` reads
  back from `harness_execution` to revalidate), `replay_strategy` from the binding
  (`reissue_native_request`; `permissions.py` gives the SDK source behind it);
- `bind` persists the approval task and a live correlation *before* anything waits, then
  `wait(wait_seconds)` waits a bounded time. The bound is checked here (positive, at most the
  broker's `MAX_WAIT_SECONDS`) and by the composition against the segment budget: the SDK
  callback never sleeps indefinitely, and on expiry the correlation (not the task) expires
  with a system deny + interrupt.

The broker's provider-neutral reply maps onto the SDK result exactly as MP-11 specifies:
allow -> `PermissionResultAllow(updated_input=reply.updated_arguments or None)`; a reviewer's
deny -> `PermissionResultDeny(message, interrupt=False)`; cancel or any system reason ->
`PermissionResultDeny(message, interrupt=True)` (`permissions.to_permission_result` builds the
SDK objects from the `PermissionOutcome` returned here).

`recover` delegates to `ApprovalBroker.recover` (every live correlation another connection
held becomes `lost`); the harness calls it when it resumes a session in a new process.
"""

from __future__ import annotations

from typing import Final

from mission_control.adapters.claude.hooks import effect_kind_of
from mission_control.adapters.claude.permissions import (
    PermissionOutcome,
    PermissionRequest,
    PermissionScope,
)
from mission_control.application.execution.approvals import ApprovalTimeoutPolicy, NativeReply
from mission_control.application.execution.approvals_broker import (
    MAX_WAIT_SECONDS,
    ApprovalBroker,
    ApprovalOutcome,
    NativeApprovalRequest,
    RecoveryItem,
)
from mission_control.domain.capabilities.hooks import HookEvent as McHookEvent
from mission_control.domain.execution.approvals import ApprovalBinding

DEFAULT_PROMPT: Final = "Claude requests permission to use {tool}"


class BrokerPermissionBinding:
    """The production `PermissionBindingPort` (and `RecoveringPermissionPort`)."""

    def __init__(
        self,
        broker: ApprovalBroker,
        *,
        wait_seconds: float,
        reviewers: tuple[str, ...],
        timeout_seconds: int | None = None,
        on_timeout: ApprovalTimeoutPolicy = "keep_waiting",
        segment_budget_s: float | None = None,
    ) -> None:
        if not 0 < wait_seconds <= MAX_WAIT_SECONDS:
            raise ValueError(
                f"the approval wait is bounded: 0 < wait_seconds <= {MAX_WAIT_SECONDS:g}"
            )
        if segment_budget_s is not None and wait_seconds >= segment_budget_s:
            raise ValueError(
                f"the approval wait ({wait_seconds:g}s) must stay below the segment budget "
                f"({segment_budget_s:g}s)"
            )
        if not reviewers or any(not item for item in reviewers):
            raise ValueError("a native approval names at least one reviewer")
        self._broker = broker
        self._wait = float(wait_seconds)
        self._reviewers = tuple(reviewers)
        self._timeout = timeout_seconds
        self._on_timeout: ApprovalTimeoutPolicy = on_timeout
        self.outcomes: list[ApprovalOutcome] = []

    @property
    def wait_seconds(self) -> float:
        return self._wait

    @property
    def broker(self) -> ApprovalBroker:
        return self._broker

    def native_request(
        self, binding: ApprovalBinding, request: PermissionRequest, scope: PermissionScope
    ) -> NativeApprovalRequest:
        """The MP-11 request for one bound `can_use_tool` delivery."""

        prompt = request.title or DEFAULT_PROMPT.format(tool=request.tool_name)
        if request.decision_reason:
            prompt = f"{prompt}\nReason: {request.decision_reason}"
        return NativeApprovalRequest(
            request_scope=scope.request_scope,
            run_id=scope.run_id,
            lane_profile=binding.lane_profile,
            origin="provider_permission",
            harness_execution_id=binding.harness_execution_id,
            generation=binding.generation,
            native=binding.native,
            tool_name=request.tool_name,
            arguments=dict(request.tool_input),
            effect_kind=effect_kind_of(request.tool_name, McHookEvent.BEFORE_TOOL),
            policy_digest=binding.policy_digest,
            reviewers=self._reviewers,
            prompt=prompt[:8_192],
            timeout_seconds=self._timeout,
            on_timeout=self._on_timeout,
            replay_strategy=binding.replay_strategy,
        )

    async def resolve(
        self, binding: ApprovalBinding, request: PermissionRequest, *, scope: PermissionScope
    ) -> PermissionOutcome:
        bound = await self._broker.bind(self.native_request(binding, request, scope))
        outcome = await self._broker.wait(bound, wait_seconds=self._wait)
        self.outcomes.append(outcome)
        return permission_outcome(outcome)

    async def recover(
        self, request_scope: str, harness_execution_id: str
    ) -> tuple[RecoveryItem, ...]:
        return await self._broker.recover(request_scope, harness_execution_id)


def permission_outcome(outcome: ApprovalOutcome) -> PermissionOutcome:
    """The broker's reply as the lane's `PermissionOutcome` (deny != cancel; a system reply
    never allows)."""

    reply: NativeReply = outcome.reply
    message = reply.message or ""
    ref = outcome.human_task_id
    if reply.reason == "human_decision" and reply.action == "allow":
        if reply.updated_arguments is not None:
            return PermissionOutcome(
                decision="approve_edited",
                updated_input=dict(reply.updated_arguments),
                message=message,
                human_task_ref=ref,
                reason=reply.reason,
            )
        return PermissionOutcome(
            decision="approve", message=message, human_task_ref=ref, reason=reply.reason
        )
    if reply.reason == "human_decision" and reply.action == "deny":
        return PermissionOutcome(
            decision="deny",
            message=message or "denied by the reviewer",
            human_task_ref=ref,
            interrupt=False,
            reason=reply.reason,
        )
    if reply.reason == "human_decision" and reply.action == "cancel":
        return PermissionOutcome(
            decision="cancel",
            message=message or "cancelled by the reviewer",
            human_task_ref=ref,
            interrupt=True,
            reason=reply.reason,
        )
    # Every system reason (and an `answer`, which a permission request never admits) denies
    # and interrupts the turn.
    return PermissionOutcome(
        decision="deny",
        message=message or f"approval refused: {reply.reason}",
        human_task_ref=ref,
        interrupt=True,
        reason=reply.reason,
    )


__all__ = ["DEFAULT_PROMPT", "BrokerPermissionBinding", "permission_outcome"]
