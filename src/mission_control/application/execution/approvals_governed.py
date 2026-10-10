"""Durable prepare / review / execute for Mission-Control-owned effect tools (SPEC-03, MP-11).

This is an application protocol, not a standard MCP feature. ``prepare`` resolves the tool
and the caller's grant, persists the effect intent with its normalized input digest, checks
the Stop Fence, opens a ``governed_effect`` approval task where policy requires review, and
returns ``pending_approval`` with the intent id and the Human Task reference **without
executing anything**. Review resolves the task through the one Human Task service. An
explicit ``execute`` consumes the approval-bound intent: it re-reads the task, revalidates
generation and policy, claims the intent with a compare-and-set (so concurrent calls never
double-execute), admits the effect through the Stop Fence and records one stable receipt.

Repeated ``prepare`` calls with the same arguments return the same intent and the same task;
repeated ``execute`` calls return the pending state or the existing receipt. Edited arguments
are a new digest, a new intent and a new review; ``execute`` with arguments that differ from
the bound intent is refused (``arguments_changed``). An approval permits at most the admitted
intent, not arbitrary retries or changed arguments. Domain-effect idempotency lives in the
receipt: the executor receives the intent id as its idempotency key.

This fallback governs only the tools routed through it. It cannot govern arbitrary shell or
network writes unless credentials and egress force them through the gateway; see
``approvals_coverage`` for the admission check that rejects an unenforceable combination.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, Protocol
from uuid import UUID, uuid5

from pydantic import AwareDatetime, Field, model_validator

from mission_control.application.execution.approvals import (
    ApprovalContextProbe,
    ApprovalContextState,
    ApprovalContract,
    ApprovalTaskRepository,
    ApprovalTaskView,
    ApprovalTimeoutPolicy,
    ElicitationPrompt,
    approval_timed_out,
    normalized_arguments,
    open_approval_packet,
    tool_input_digest,
)
from mission_control.application.execution.stop_fence import StopFenceRepository
from mission_control.domain.authoring.canonical import sha256_digest, stable_json_digest
from mission_control.domain.execution.approvals import NativeApprovalCorrelation
from mission_control.domain.execution.lanes import DIGEST_PATTERN, LaneProfileName
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.stop_fence import EffectAdmission, EffectKind

GOVERNED_INTENT_SCHEMA: Final = "mc.governed_effect_intent.v1"
GOVERNED_RECEIPT_SCHEMA: Final = "mc.governed_effect_receipt.v1"
# Executing a governed effect is claiming an effect on the run.
GOVERNED_EFFECT_PERMISSION: Final = "workflow_run.claim_effect"
GOVERNED_ACTOR: Final = "mission-control-runtime/governed-gateway"
DEFAULT_CLAIM_LEASE: Final = timedelta(seconds=120)

GovernedIntentState = Literal[
    "pending_approval",
    "ready",
    "executing",
    "executed",
    "denied",
    "cancelled",
    "expired",
    "fenced",
    "stale",
    "in_doubt",
]
TERMINAL_STATES: Final[frozenset[str]] = frozenset(
    {"executed", "denied", "cancelled", "expired", "fenced", "stale", "in_doubt"}
)
GovernedRejectionCode = Literal[
    "unknown_tool",
    "not_permitted",
    "not_found",
    "stop_fenced",
    "arguments_changed",
    "invalid_arguments",
    "elicitation_unsupported",
    "elicitation_mode_unsupported",
    "gate_unenforceable",
]
EffectOutcome = Literal["applied", "noop", "rejected", "failed"]
MissingElicitation = Literal["reject", "review_arguments"]

_INTENT_NAMESPACE: Final = UUID("b4c3e2a1-6f0d-4e8b-9c7a-5d2e1f0a3b94")


def _utcnow() -> datetime:
    return datetime.now(UTC)


class GovernedRejected(Exception):
    """A typed refusal; `code` maps to the MCP error envelope."""

    def __init__(self, code: GovernedRejectionCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class GovernedToolPolicy(ApprovalContract):
    """How one Mission-Control-owned tool is governed."""

    requires_approval: bool = True
    reviewers: tuple[str, ...] = Field(default=(), max_length=64)
    prompt: str = Field(default="Approve this governed effect?", min_length=1, max_length=8_192)
    timeout_seconds: int | None = Field(default=None, ge=1)
    on_timeout: ApprovalTimeoutPolicy = "keep_waiting"
    # Structured input the tool asks the MCP client for (elicitation is negotiated, not assumed).
    elicitation: ElicitationPrompt | None = None
    on_missing_elicitation: MissingElicitation = "reject"

    @model_validator(mode="after")
    def _reviewable(self) -> GovernedToolPolicy:
        if self.requires_approval and not self.reviewers:
            raise ValueError("a tool that requires approval names its reviewers")
        if self.on_missing_elicitation == "review_arguments" and not self.requires_approval:
            raise ValueError("the review_arguments fallback needs a human review")
        return self

    @property
    def digest(self) -> str:
        return stable_json_digest(self)


class GovernedEffectResult(ApprovalContract):
    """What a governed executor reports for one intent (idempotency key = intent id)."""

    outcome: EffectOutcome
    output: dict[str, Any] = Field(default_factory=dict)
    external_ref: str | None = Field(default=None, min_length=1, max_length=1_024)


class GovernedReceipt(ApprovalContract):
    """`mc.governed_effect_receipt.v1`: the one stable receipt of an executed intent."""

    schema_version: Literal["mc.governed_effect_receipt.v1"] = GOVERNED_RECEIPT_SCHEMA
    intent_id: str
    tool_name: str
    input_digest: str = Field(pattern=DIGEST_PATTERN)
    outcome: EffectOutcome
    output: dict[str, Any] = Field(default_factory=dict)
    external_ref: str | None = None
    human_task_id: str | None = None
    resolution_ref: str | None = None
    executed_at: AwareDatetime

    @property
    def receipt_digest(self) -> str:
        return stable_json_digest(self)


class GovernedIntent(ApprovalContract):
    """`mc.governed_effect_intent.v1`: the persisted, approval-bound intent."""

    schema_version: Literal["mc.governed_effect_intent.v1"] = GOVERNED_INTENT_SCHEMA
    intent_id: str
    intent_key: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    harness_execution_id: str = Field(min_length=1, max_length=512)
    generation: int = Field(ge=1)
    lane_profile: LaneProfileName
    tool_name: str = Field(min_length=1, max_length=256)
    effect_kind: EffectKind = "other"
    arguments: dict[str, Any]
    input_digest: str = Field(pattern=DIGEST_PATTERN)
    policy_digest: str = Field(pattern=DIGEST_PATTERN)
    human_task_id: str | None = None
    state: GovernedIntentState
    reason: str | None = Field(default=None, min_length=1, max_length=128)
    claimant_ref: str | None = None
    claimed_at: AwareDatetime | None = None
    receipt: GovernedReceipt | None = None
    version: int = Field(default=1, ge=1)
    created_at: AwareDatetime
    updated_at: AwareDatetime

    @model_validator(mode="after")
    def _consistent(self) -> GovernedIntent:
        if (self.state == "executed") != (self.receipt is not None):
            raise ValueError("exactly an executed intent carries a receipt")
        if self.state == "pending_approval" and self.human_task_id is None:
            raise ValueError("a pending intent names its approval task")
        if self.input_digest != tool_input_digest(self.tool_name, self.arguments):
            raise ValueError("the intent arguments do not match its input digest")
        return self


def governed_intent_key(
    *,
    run_id: str,
    harness_execution_id: str,
    generation: int,
    tool_name: str,
    input_digest: str,
    policy_digest: str,
) -> str:
    return (
        f"governed:{run_id}:{harness_execution_id}:g{generation}:{tool_name}:"
        f"{input_digest}:{policy_digest}"
    )


def governed_intent_id(request_scope: str, intent_key: str) -> str:
    return str(uuid5(_INTENT_NAMESPACE, f"{request_scope}|{intent_key}"))


class GovernedPrepareRequest(ApprovalContract):
    run_id: str = Field(min_length=1, max_length=512)
    harness_execution_id: str = Field(min_length=1, max_length=512)
    generation: int = Field(ge=1)
    lane_profile: LaneProfileName
    tool_name: str = Field(min_length=1, max_length=256)
    arguments: dict[str, Any] = Field(default_factory=dict)


class GovernedEffectState(ApprovalContract):
    """The gateway's answer to prepare/execute/status (stable for repeated calls)."""

    status: GovernedIntentState
    intent_id: str
    tool_name: str
    input_digest: str
    human_task: dict[str, Any] | None = None
    receipt: GovernedReceipt | None = None
    reason: str | None = None

    def public(self) -> dict[str, Any]:
        body = self.model_dump(mode="json")
        if self.receipt is not None:
            body["receipt_digest"] = self.receipt.receipt_digest
        return body


class GovernedExecutor(Protocol):
    async def execute(self, intent: GovernedIntent) -> GovernedEffectResult:
        """Apply the effect once; `intent.intent_id` is the domain idempotency key."""
        ...


class ReconcilingGovernedExecutor(GovernedExecutor, Protocol):
    async def lookup(self, intent: GovernedIntent) -> GovernedEffectResult | None:
        """The recorded outcome for this idempotency key, if the domain has one."""
        ...


@dataclass(frozen=True, slots=True)
class GovernedTool:
    name: str
    executor: GovernedExecutor
    policy: GovernedToolPolicy
    effect_kind: EffectKind = "mcp"
    family: str = "mcp"


class GovernedToolRegistry:
    """Only Mission-Control-owned effect tools are governed here; nothing else is proxied."""

    def __init__(self, tools: Iterable[GovernedTool] = ()) -> None:
        self._tools: dict[str, GovernedTool] = {}
        for tool in tools:
            self.register(tool)

    def register(self, tool: GovernedTool) -> None:
        if tool.name in self._tools:
            raise ValueError(f"governed tool {tool.name} is already registered")
        self._tools[tool.name] = tool

    def get(self, name: str) -> GovernedTool:
        try:
            return self._tools[name]
        except KeyError:
            raise GovernedRejected("unknown_tool", f"{name} is not a governed tool") from None

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._tools))

    def families(self) -> frozenset[str]:
        return frozenset(tool.family for tool in self._tools.values())


class GovernedIntentRepository(Protocol):
    async def prepare(self, intent: GovernedIntent) -> GovernedIntent:
        """Insert once per intent key; a repeat returns the stored intent unchanged."""
        ...

    async def get(self, request_scope: str, intent_id: str) -> GovernedIntent | None: ...

    async def transition(
        self,
        request_scope: str,
        intent_id: str,
        *,
        from_states: frozenset[str],
        to_state: GovernedIntentState,
        at: datetime,
        reason: str | None = None,
        claimant_ref: str | None = None,
    ) -> GovernedIntent | None:
        """Compare-and-set the state; None when the intent is not in `from_states`."""
        ...

    async def record_receipt(
        self, request_scope: str, intent_id: str, receipt: GovernedReceipt, *, at: datetime
    ) -> GovernedIntent | None:
        """Compare-and-set `executing` -> `executed` with the one immutable receipt."""
        ...


@dataclass(frozen=True, slots=True)
class ElicitedInput:
    """The negotiated outcome of a tool's structured-input elicitation (interfaces layer)."""

    action: Literal["accept", "decline", "cancel", "unsupported"]
    content: Mapping[str, Any] | None = None


class GovernedEffectService:
    """prepare -> pending_approval -> (review) -> execute -> receipt, for one tenant scope."""

    def __init__(
        self,
        intents: GovernedIntentRepository,
        tasks: ApprovalTaskRepository,
        registry: GovernedToolRegistry,
        *,
        request_scope: str,
        probe: ApprovalContextProbe,
        fences: StopFenceRepository,
        clock: Callable[[], datetime] = _utcnow,
        claim_lease: timedelta = DEFAULT_CLAIM_LEASE,
        claimant_ref: str = GOVERNED_ACTOR,
    ) -> None:
        self._intents = intents
        self._tasks = tasks
        self._registry = registry
        self.request_scope = request_scope
        self._probe = probe
        self._fences = fences
        self._clock = clock
        self._lease = claim_lease
        self._claimant = claimant_ref

    @property
    def registry(self) -> GovernedToolRegistry:
        return self._registry

    async def prepare(
        self,
        request: GovernedPrepareRequest,
        actor: ActorContext,
        *,
        elicited: ElicitedInput | None = None,
    ) -> GovernedEffectState:
        """Persist the intent (and its review) without executing anything."""

        tool = self._registry.get(request.tool_name)
        self._require_permission(actor)
        arguments = dict(request.arguments)
        policy = tool.policy
        review_arguments = False
        if policy.elicitation is not None:
            outcome = elicited or ElicitedInput("unsupported")
            if outcome.action == "unsupported":
                if policy.on_missing_elicitation == "reject":
                    raise GovernedRejected(
                        "elicitation_unsupported",
                        f"{tool.name} needs client elicitation the caller did not negotiate",
                    )
                missing = policy.elicitation.property_names() - set(arguments)
                if missing:
                    raise GovernedRejected(
                        "invalid_arguments",
                        f"without elicitation the arguments carry {sorted(missing)}",
                    )
                review_arguments = True
            elif outcome.action in {"decline", "cancel"}:
                # Not an intent: nothing was asked to execute. Decline and cancel stay distinct.
                return GovernedEffectState(
                    status="denied" if outcome.action == "decline" else "cancelled",
                    intent_id="",
                    tool_name=tool.name,
                    input_digest=tool_input_digest(tool.name, arguments),
                    reason=f"elicitation_{outcome.action}",
                )
            else:
                arguments.update(dict(outcome.content or {}))
        try:
            normalized = normalized_arguments(arguments)
        except ValueError as error:
            raise GovernedRejected("invalid_arguments", str(error)) from None
        fence = await self._fences.get(self.request_scope, request.run_id)
        if fence is not None and fence.covers(request.generation):
            raise GovernedRejected("stop_fenced", f"run is stop-fenced by {fence.command_id}")
        input_digest = tool_input_digest(tool.name, normalized)
        context = await self._context(request.run_id, request.harness_execution_id, tool)
        if context.generation != request.generation:
            raise GovernedRejected(
                "not_permitted", "the execution generation moved; prepare in the current one"
            )
        if not context.granted:
            raise GovernedRejected("not_permitted", "the execution no longer holds the grant")
        policy_digest = context.policy_digest
        key = governed_intent_key(
            run_id=request.run_id,
            harness_execution_id=request.harness_execution_id,
            generation=request.generation,
            tool_name=tool.name,
            input_digest=input_digest,
            policy_digest=policy_digest,
        )
        intent_id = governed_intent_id(self.request_scope, key)
        existing = await self._intents.get(self.request_scope, intent_id)
        if existing is not None:
            return await self._state(existing)
        now = self._clock()
        needs_review = policy.requires_approval or review_arguments
        task: ApprovalTaskView | None = None
        if needs_review:
            packet = open_approval_packet(
                request_scope=self.request_scope,
                run_id=request.run_id,
                origin="governed_effect",
                lane_profile=request.lane_profile,
                harness_execution_id=request.harness_execution_id,
                generation=request.generation,
                native=NativeApprovalCorrelation(tool_call_ref=intent_id, connection_scoped=False),
                tool_name=tool.name,
                arguments=normalized,
                policy_digest=policy_digest,
                reviewers=policy.reviewers,
                prompt=policy.prompt,
                opened_at=now,
                replay_strategy="reissue_native_request",
                timeout_seconds=policy.timeout_seconds,
                on_timeout=policy.on_timeout,
                effect_kind=tool.effect_kind,
            )
            task = await self._tasks.open_task(packet, actor_ref=GOVERNED_ACTOR)
        intent = await self._intents.prepare(
            GovernedIntent(
                intent_id=intent_id,
                intent_key=key,
                request_scope=self.request_scope,
                run_id=request.run_id,
                harness_execution_id=request.harness_execution_id,
                generation=request.generation,
                lane_profile=request.lane_profile,
                tool_name=tool.name,
                effect_kind=tool.effect_kind,
                arguments=normalized,
                input_digest=input_digest,
                policy_digest=policy_digest,
                human_task_id=task.human_task_id if task is not None else None,
                state="pending_approval" if task is not None else "ready",
                created_at=now,
                updated_at=now,
            )
        )
        return await self._state(intent, task)

    async def status(self, intent_id: str, actor: ActorContext) -> GovernedEffectState:
        self._require_permission(actor)
        return await self._state(await self._load(intent_id))

    async def execute(
        self,
        intent_id: str,
        actor: ActorContext,
        *,
        arguments: Mapping[str, Any] | None = None,
    ) -> GovernedEffectState:
        """Consume the approval-bound intent at most once; repeats return the same state."""

        self._require_permission(actor)
        intent = await self._load(intent_id)
        if arguments is not None:
            try:
                digest = tool_input_digest(intent.tool_name, arguments)
            except ValueError as error:
                raise GovernedRejected("invalid_arguments", str(error)) from None
            if digest != intent.input_digest:
                raise GovernedRejected(
                    "arguments_changed",
                    "the arguments differ from the approved intent; prepare them for review",
                )
        if intent.state in TERMINAL_STATES:
            return await self._state(intent)
        if intent.state == "executing":
            return await self._resume_claimed(intent)
        task: ApprovalTaskView | None = None
        resolution_ref: str | None = None
        if intent.state == "pending_approval":
            assert intent.human_task_id is not None
            task = await self._tasks.get_task(self.request_scope, intent.human_task_id)
            if task is None:
                raise GovernedRejected("not_found", "the intent's approval task is missing")
            now = self._clock()
            if approval_timed_out(task, now):
                task = (
                    await self._tasks.apply_timeout(self.request_scope, task.human_task_id, now=now)
                    or task
                )
            if task.lifecycle == "open":
                return await self._state(intent, task)
            if task.lifecycle in {"expired", "cancelled"}:
                return await self._settle(intent, task.lifecycle, f"task_{task.lifecycle}")  # type: ignore[arg-type]
            resolution = task.resolution
            assert resolution is not None
            if resolution.decision == "deny":
                return await self._settle(intent, "denied", "reviewer_denied")
            if resolution.decision == "cancel":
                return await self._settle(intent, "cancelled", "reviewer_cancelled")
            resolution_ref = resolution.resolution_ref
        # Approved or review-free: the intent's generation, grants and policy must still hold.
        failure = await self._revalidate(intent)
        if failure is not None:
            return await self._settle(intent, "stale", failure, from_state=intent.state)
        claimed = await self._intents.transition(
            self.request_scope,
            intent.intent_id,
            from_states=frozenset({"pending_approval", "ready"}),
            to_state="executing",
            at=self._clock(),
            claimant_ref=self._claimant,
        )
        if claimed is None:
            # Another call claimed or settled it first; report what it recorded.
            return await self._state(await self._load(intent_id))
        verdict = await self._fences.admit_effect(
            EffectAdmission(
                request_scope=self.request_scope,
                run_id=claimed.run_id,
                generation=claimed.generation,
                effect_ref=f"governed:{claimed.intent_id}",
                effect_kind=claimed.effect_kind,
                lane_profile=claimed.lane_profile,
            )
        )
        if not verdict.allowed:
            return await self._settle(claimed, "fenced", "stop_fenced", from_state="executing")
        tool = self._registry.get(claimed.tool_name)
        try:
            result = await tool.executor.execute(claimed)
        except Exception:
            # The effect may or may not have applied: never claim either; reconcile.
            return await self._settle(claimed, "in_doubt", "executor_error", from_state="executing")
        return await self._record(claimed, result, resolution_ref)

    async def _resume_claimed(self, intent: GovernedIntent) -> GovernedEffectState:
        """A claim left by a crashed caller: report it, or reconcile once the lease passed."""

        claimed_at = intent.claimed_at or intent.updated_at
        if self._clock() < claimed_at + self._lease:
            return await self._state(intent)
        tool = self._registry.get(intent.tool_name)
        lookup = getattr(tool.executor, "lookup", None)
        result = await lookup(intent) if lookup is not None else None
        if result is None:
            return await self._settle(
                intent, "in_doubt", "claim_lease_expired", from_state="executing"
            )
        resolution_ref = None
        if intent.human_task_id is not None:
            task = await self._tasks.get_task(self.request_scope, intent.human_task_id)
            if task is not None and task.resolution is not None:
                resolution_ref = task.resolution.resolution_ref
        return await self._record(intent, result, resolution_ref)

    async def _record(
        self, intent: GovernedIntent, result: GovernedEffectResult, resolution_ref: str | None
    ) -> GovernedEffectState:
        receipt = GovernedReceipt(
            intent_id=intent.intent_id,
            tool_name=intent.tool_name,
            input_digest=intent.input_digest,
            outcome=result.outcome,
            output=result.output,
            external_ref=result.external_ref,
            human_task_id=intent.human_task_id,
            resolution_ref=resolution_ref,
            executed_at=self._clock(),
        )
        recorded = await self._intents.record_receipt(
            self.request_scope, intent.intent_id, receipt, at=receipt.executed_at
        )
        return await self._state(recorded or await self._load(intent.intent_id))

    async def _settle(
        self,
        intent: GovernedIntent,
        state: GovernedIntentState,
        reason: str,
        *,
        from_state: str = "pending_approval",
    ) -> GovernedEffectState:
        settled = await self._intents.transition(
            self.request_scope,
            intent.intent_id,
            from_states=frozenset({from_state}),
            to_state=state,
            at=self._clock(),
            reason=reason,
        )
        return await self._state(settled or await self._load(intent.intent_id))

    async def _load(self, intent_id: str) -> GovernedIntent:
        intent = await self._intents.get(self.request_scope, intent_id)
        if intent is None:
            raise GovernedRejected("not_found", "governed intent not found")
        return intent

    async def _state(
        self, intent: GovernedIntent, task: ApprovalTaskView | None = None
    ) -> GovernedEffectState:
        if task is None and intent.human_task_id is not None:
            task = await self._tasks.get_task(self.request_scope, intent.human_task_id)
        return GovernedEffectState(
            status=intent.state,
            intent_id=intent.intent_id,
            tool_name=intent.tool_name,
            input_digest=intent.input_digest,
            human_task=(
                {
                    "human_task_id": task.human_task_id,
                    "lifecycle": task.lifecycle,
                    "version": task.version,
                    "review_digest": task.packet.review_digest,
                }
                if task is not None
                else None
            ),
            receipt=intent.receipt,
            reason=intent.reason,
        )

    async def _revalidate(self, intent: GovernedIntent) -> str | None:
        context = await self._context(
            intent.run_id, intent.harness_execution_id, self._registry.get(intent.tool_name)
        )
        if not context.granted:
            return "grant_revoked"
        if context.generation != intent.generation:
            return "stale_generation"
        if context.policy_digest != intent.policy_digest:
            return "policy_changed"
        return None

    async def _context(
        self, run_id: str, harness_execution_id: str, tool: GovernedTool
    ) -> ApprovalContextState:
        """The live context with the tool's governing policy folded into the policy digest."""

        state = await self._probe.current(
            self.request_scope, run_id=run_id, harness_execution_id=harness_execution_id
        )
        return state.model_copy(
            update={"policy_digest": effective_policy_digest(tool.policy, state.policy_digest)}
        )

    @staticmethod
    def _require_permission(actor: ActorContext) -> None:
        if GOVERNED_EFFECT_PERMISSION not in actor.permissions:
            raise GovernedRejected(
                "not_permitted", f"the principal lacks {GOVERNED_EFFECT_PERMISSION}"
            )


def effective_policy_digest(policy: GovernedToolPolicy, execution_policy_digest: str) -> str:
    """The digest an approval of a governed effect binds: tool policy plus execution policy."""

    return sha256_digest(
        {"tool_policy": policy.digest, "execution_policy": execution_policy_digest}
    )


__all__ = [
    "GOVERNED_EFFECT_PERMISSION",
    "GOVERNED_INTENT_SCHEMA",
    "GOVERNED_RECEIPT_SCHEMA",
    "TERMINAL_STATES",
    "ElicitedInput",
    "GovernedEffectResult",
    "GovernedEffectService",
    "GovernedEffectState",
    "GovernedExecutor",
    "GovernedIntent",
    "GovernedIntentRepository",
    "GovernedPrepareRequest",
    "GovernedReceipt",
    "GovernedRejected",
    "GovernedTool",
    "GovernedToolPolicy",
    "GovernedToolRegistry",
    "ReconcilingGovernedExecutor",
    "effective_policy_digest",
    "governed_intent_id",
    "governed_intent_key",
]
