"""Quota/rate-limit dispositions, finite usage bounds and unknown cost (MP-05).

SPEC-02 "Authentication, subscription usage and limits": token/work/time ceilings are
separate from monetary accounting; usage is tracked as spent, reserved, estimated and
unknown; subscription quota is not a dollar cost of zero; a rate-limit reset may become a
durable wait bounded by the mission deadline; without observable cost the kernel enforces
the known token/turn/time bounds and discloses the unknown billing dimension.

Nothing here schedules. `plan_limit_response` is a pure decision; `wait_for_limit_reset`
waits through an injected `WaitPrimitive` - in a workflow that is
`workflow.wait_condition(predicate, timeout=...)`, so Temporal alone owns the timer and the
same predicate that the operation workflow's cancel handler flips wakes the wait early.
Limit waits never touch mission counters: goal iterations, attempts, turns and budget
consumption are carried unchanged; the wait has its own bounded ledger.

This module is pure (pydantic contracts and decisions over domain vocabularies only), which is
why it lives in the domain layer: the operation workflow consumes `plan_limit_response` /
`wait_for_limit_reset` directly, and workflows may not import application modules.
`mission_control.application.execution.usage_admission` re-exports it unchanged.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime, timedelta
from typing import Final, Literal, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from mission_control.domain.execution.bindings import BillingMode
from mission_control.domain.execution.lanes import LaneProfileName

LimitKind = Literal[
    "approaching_limit",
    "rate_limited",
    "quota_exhausted",
    "overloaded",
    "auth_failed",
    "billing_unavailable",
    "entitlement_missing",
    "unknown",
]
OverageState = Literal["allowed", "rejected", "unknown", "not_applicable"]

LIMIT_RESET_AFTER_DEADLINE: Final = "CAPACITY_RESET_AFTER_DEADLINE"
LIMIT_WAITS_EXHAUSTED: Final = "CAPACITY_WAITS_EXHAUSTED"
LIMIT_QUOTA_EXHAUSTED: Final = "CAPACITY_QUOTA_EXHAUSTED"
LIMIT_AUTH_REJECTED: Final = "AUTH_REJECTED_BY_PROVIDER"
LIMIT_BILLING_UNAVAILABLE: Final = "BILLING_UNAVAILABLE"
LIMIT_ENTITLEMENT_MISSING: Final = "ENTITLEMENT_MISSING"
LIMIT_UNCLASSIFIED: Final = "CAPACITY_UNCLASSIFIED"
LIMIT_WAIT: Final = "CAPACITY_WAIT"

USAGE_BOUND_REQUIRED: Final = "USAGE_BOUND_REQUIRED"
USAGE_COST_CAP_UNENFORCEABLE: Final = "USAGE_COST_CAP_UNENFORCEABLE"
USAGE_BOUND_EXCEEDED: Final = "USAGE_BOUND_EXCEEDED"


class UsageContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --- Provider limit signals ---------------------------------------------------------------------


class ProviderLimitSignal(UsageContract):
    """A provider's quota/rate-limit/auth condition, normalized and secret-free.

    `resets_at` is the provider-stated reset (None: not stated). `source` names the native
    surface the adapter classified (e.g. `claude_sdk.rate_limit_event`); `native_code` is
    the provider's own code string, never a message body.
    """

    lane_profile: LaneProfileName
    kind: LimitKind
    resets_at: AwareDatetime | None = None
    window: str | None = Field(default=None, max_length=64)
    utilization: float | None = Field(default=None, ge=0)
    overage: OverageState = "unknown"
    source: str = Field(min_length=1, max_length=128)
    native_code: str | None = Field(default=None, max_length=128)
    fixture: bool = False


class LimitWaitPolicy(UsageContract):
    """Bounds for capacity waits. `max_waits` and `max_total_wait_s` are finite; the mission
    deadline bounds every individual wait regardless of these."""

    max_waits: int = Field(default=3, ge=0, le=32)
    max_total_wait_s: int = Field(default=6 * 3600, ge=0)
    reset_margin_s: int = Field(default=5, ge=0, le=600)
    fallback_backoff_s: int = Field(default=30, ge=1)
    max_backoff_s: int = Field(default=900, ge=1)
    wait_on_quota_reset: bool = True


class LimitWaitLedger(UsageContract):
    """The wait's own counters. Separate from goal iterations, attempts, turns and budget
    consumption, which a capacity wait never resets or advances."""

    waits_used: int = Field(default=0, ge=0)
    waited_s: int = Field(default=0, ge=0)

    def after_wait(self, waited_s: float) -> LimitWaitLedger:
        return LimitWaitLedger(
            waits_used=self.waits_used + 1, waited_s=self.waited_s + max(0, round(waited_s))
        )


class LimitDecision(UsageContract):
    """What the caller does about a limit signal. `reject` is final for this attempt and
    carries a pointed code; `wait` names an absolute wake time no later than the deadline;
    `continue` is for non-blocking warnings."""

    disposition: Literal["continue", "wait", "reject"]
    code: str
    message: str
    retryable: bool = False
    wait_until: AwareDatetime | None = None
    wait_s: float | None = Field(default=None, ge=0)
    provider_reset_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def _wait_shape(self) -> LimitDecision:
        if (self.disposition == "wait") != (self.wait_until is not None):
            raise ValueError("a wait decision, and only a wait decision, names wait_until")
        return self


def plan_limit_response(
    signal: ProviderLimitSignal,
    *,
    now: datetime,
    deadline: datetime,
    policy: LimitWaitPolicy | None = None,
    ledger: LimitWaitLedger | None = None,
) -> LimitDecision:
    """Decide wait / reject / continue for one signal. Never proposes another auth route.

    Auth failures, billing unavailability and missing entitlements are rejected (no wait
    fixes them; switching to a metered route is not an option this function has). A
    rate limit or an exhausted quota with a stated reset waits until reset + margin when
    that is before the deadline and the wait ledger has room; otherwise it is rejected with
    `CAPACITY_RESET_AFTER_DEADLINE` / `CAPACITY_WAITS_EXHAUSTED`. Overload and unstated
    resets use bounded exponential backoff, also capped by the deadline.
    """

    policy = policy or LimitWaitPolicy()
    ledger = ledger or LimitWaitLedger()

    def reject(code: str, message: str, *, retryable: bool = False) -> LimitDecision:
        return LimitDecision(
            disposition="reject",
            code=code,
            message=message,
            retryable=retryable,
            provider_reset_at=signal.resets_at,
        )

    kind = signal.kind
    if kind == "approaching_limit":
        return LimitDecision(
            disposition="continue",
            code="CAPACITY_WARNING",
            message=f"{signal.lane_profile} is approaching a provider limit ({signal.source})",
            provider_reset_at=signal.resets_at,
        )
    if kind == "auth_failed":
        return reject(
            LIMIT_AUTH_REJECTED,
            f"{signal.lane_profile} rejected the admitted credential "
            f"({signal.native_code or kind}); re-admit auth out of band - no other route is tried",
        )
    if kind == "billing_unavailable":
        return reject(
            LIMIT_BILLING_UNAVAILABLE,
            f"{signal.lane_profile} reports billing unavailable ({signal.native_code or kind})",
        )
    if kind == "entitlement_missing":
        return reject(
            LIMIT_ENTITLEMENT_MISSING,
            f"the account is not entitled to this model/feature on {signal.lane_profile}",
        )
    if kind == "unknown":
        return reject(
            LIMIT_UNCLASSIFIED,
            f"unclassified provider condition {signal.native_code or ''} on {signal.lane_profile}",
        )
    if kind == "quota_exhausted" and (signal.resets_at is None or not policy.wait_on_quota_reset):
        return reject(
            LIMIT_QUOTA_EXHAUSTED,
            f"{signal.lane_profile} quota is exhausted with no admissible reset; overage or "
            "credit purchase is never enabled automatically",
        )

    if ledger.waits_used >= policy.max_waits:
        return reject(
            LIMIT_WAITS_EXHAUSTED,
            f"{ledger.waits_used} capacity waits already used (max {policy.max_waits})",
            retryable=True,
        )
    if signal.resets_at is not None:
        wake = signal.resets_at + timedelta(seconds=policy.reset_margin_s)
    else:
        backoff = min(policy.fallback_backoff_s * 2**ledger.waits_used, policy.max_backoff_s)
        wake = now + timedelta(seconds=backoff)
    wake = max(wake, now)
    wait_s = (wake - now).total_seconds()
    if wake > deadline:
        return reject(
            LIMIT_RESET_AFTER_DEADLINE,
            f"the provider limit resets at {wake.isoformat()}, after the run deadline "
            f"{deadline.isoformat()}",
            retryable=True,
        )
    if ledger.waited_s + wait_s > policy.max_total_wait_s:
        return reject(
            LIMIT_WAITS_EXHAUSTED,
            f"waiting {int(wait_s)}s would exceed the {policy.max_total_wait_s}s wait budget",
            retryable=True,
        )
    return LimitDecision(
        disposition="wait",
        code=LIMIT_WAIT,
        message=f"{signal.lane_profile} {kind}; waiting until {wake.isoformat()}",
        retryable=True,
        wait_until=wake,
        wait_s=wait_s,
        provider_reset_at=signal.resets_at,
    )


class WaitPrimitive(Protocol):
    """Wait until `predicate()` is true or `timeout_s` elapses; True iff the predicate fired.

    In a workflow: `workflow.wait_condition(predicate, timeout=timedelta(seconds=t))`,
    mapping `asyncio.TimeoutError` to False. Tests use a fake clock.
    """

    async def __call__(self, predicate: Callable[[], bool], timeout_s: float) -> bool: ...


class LimitWaitOutcome(UsageContract):
    outcome: Literal["reset_elapsed", "cancelled", "deadline_reached"]
    waited_s: float = Field(ge=0)
    ledger: LimitWaitLedger


async def wait_for_limit_reset(
    decision: LimitDecision,
    *,
    now: Callable[[], datetime],
    deadline: datetime,
    wait: WaitPrimitive,
    cancelled: Callable[[], bool],
    ledger: LimitWaitLedger | None = None,
) -> LimitWaitOutcome:
    """Wait out a `wait` decision, bounded by the deadline, woken by cancellation.

    A cancel that lands during the wait returns `cancelled` at once so the caller's
    existing cancel path (Stop Fence, `lane.cancel`, settlement) reaches pending work; the
    pending provider call is never issued after the fence.
    """

    if decision.disposition != "wait" or decision.wait_until is None:
        raise ValueError("only a wait decision can be waited out")
    ledger = ledger or LimitWaitLedger()
    started = now()
    if cancelled():
        return LimitWaitOutcome(outcome="cancelled", waited_s=0, ledger=ledger)
    wake = min(decision.wait_until, deadline)
    timeout_s = max(0.0, (wake - started).total_seconds())
    fired = await wait(cancelled, timeout_s) if timeout_s > 0 else cancelled()
    waited = max(0.0, (now() - started).total_seconds())
    next_ledger = ledger.after_wait(waited)
    if fired or cancelled():
        return LimitWaitOutcome(outcome="cancelled", waited_s=waited, ledger=next_ledger)
    if decision.wait_until > deadline:
        return LimitWaitOutcome(outcome="deadline_reached", waited_s=waited, ledger=next_ledger)
    return LimitWaitOutcome(outcome="reset_elapsed", waited_s=waited, ledger=next_ledger)


# --- Usage bounds and accounting ----------------------------------------------------------------

UsageDimension = Literal[
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "turns",
    "tool_calls",
    "wall_clock_s",
    "cost_micros_usd",
]
USAGE_DIMENSIONS: Final[tuple[UsageDimension, ...]] = (
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "turns",
    "tool_calls",
    "wall_clock_s",
    "cost_micros_usd",
)
# The kernel can always enforce these itself (it counts turns and the clock).
KERNEL_ENFORCED_DIMENSIONS: Final[frozenset[str]] = frozenset({"turns", "wall_clock_s"})


class UsageBounds(UsageContract):
    """Finite ceilings per dimension. A dollar cap is only a hard cap when the provider or
    account enforces it (`cost_cap_enforced_by`); otherwise declaring one is refused."""

    limits: dict[UsageDimension, int] = Field(default_factory=dict)
    cost_cap_enforced_by: str | None = Field(default=None, min_length=1, max_length=256)

    @model_validator(mode="after")
    def _positive(self) -> UsageBounds:
        for dimension, value in self.limits.items():
            if value < 1:
                raise ValueError(f"usage bound {dimension} must be a positive finite integer")
        return self


class UsageIssue(UsageContract):
    code: str
    pointer: str
    dimension: str
    message: str


def require_finite_bounds(
    bounds: UsageBounds, *, billing_mode: BillingMode, pointer: str = "/environment/budget"
) -> tuple[UsageIssue, ...]:
    """Admission check: turn and wall-clock bounds are mandatory on every route (the kernel
    enforces them itself, so they hold even when tokens and cost are unobservable); a dollar
    cap needs an enforcing provider/account limit."""

    issues: list[UsageIssue] = []
    for dimension in ("turns", "wall_clock_s"):
        if dimension not in bounds.limits:
            issues.append(
                UsageIssue(
                    code=USAGE_BOUND_REQUIRED,
                    pointer=f"{pointer}/{dimension}",
                    dimension=dimension,
                    message=f"a finite {dimension} bound is required on every route",
                )
            )
    if "cost_micros_usd" in bounds.limits and bounds.cost_cap_enforced_by is None:
        issues.append(
            UsageIssue(
                code=USAGE_COST_CAP_UNENFORCEABLE,
                pointer=f"{pointer}/cost_micros_usd",
                dimension="cost_micros_usd",
                message=(
                    f"no enforceable provider/account dollar limit backs this cap on "
                    f"{billing_mode} billing; Mission Control does not claim a hard dollar cap"
                ),
            )
        )
    return tuple(issues)


class UsageBoundExceeded(ValueError):
    def __init__(self, dimension: str, requested: int, bound: int) -> None:
        super().__init__(f"{USAGE_BOUND_EXCEEDED}: {dimension} {requested} exceeds bound {bound}")
        self.code = USAGE_BOUND_EXCEEDED
        self.dimension = dimension


class UsageObservation(UsageContract):
    """One settlement's observed usage. `None` means *not observable*, never zero."""

    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)
    turns: int | None = Field(default=None, ge=0)
    tool_calls: int | None = Field(default=None, ge=0)
    wall_clock_s: int | None = Field(default=None, ge=0)
    cost_micros_usd: int | None = Field(default=None, ge=0)
    estimated: frozenset[UsageDimension] = frozenset()


class UsageAccount(UsageContract):
    """Spent / reserved / estimated / unknown usage of one attempt or mission.

    Monotonic: `spent` never decreases and nothing here resets on session rotation,
    continuation transfer, provider compaction or capacity waits. A dimension whose value
    was not observable is in `unknown` and stays there for the account's life.
    """

    billing_mode: BillingMode
    bounds: UsageBounds = Field(default_factory=UsageBounds)
    spent: dict[UsageDimension, int] = Field(default_factory=dict)
    reserved: dict[UsageDimension, int] = Field(default_factory=dict)
    estimated: dict[UsageDimension, int] = Field(default_factory=dict)
    unknown: tuple[UsageDimension, ...] = ()

    @property
    def cost_state(self) -> Literal["observed", "estimated", "unknown"]:
        if "cost_micros_usd" in self.unknown:
            return "unknown"
        if "cost_micros_usd" in self.estimated:
            return "estimated"
        return "observed" if "cost_micros_usd" in self.spent else "unknown"

    def disclosures(self) -> tuple[str, ...]:
        notes: list[str] = []
        if self.cost_state == "unknown":
            notes.append(
                f"cost unknown on {self.billing_mode} billing"
                + (
                    "; subscription quota is consumed, not a zero dollar cost"
                    if self.billing_mode == "subscription"
                    else ""
                )
            )
        for dimension in self.unknown:
            if dimension != "cost_micros_usd":
                notes.append(f"{dimension} not observable; bounded by kernel-enforced limits")
        return tuple(notes)


def reserve(account: UsageAccount, amounts: Mapping[UsageDimension, int]) -> UsageAccount:
    """Reserve before dispatch; refuse when spent + reserved + amount exceeds a bound."""

    reserved = dict(account.reserved)
    for dimension, amount in amounts.items():
        if amount < 0:
            raise ValueError("a reservation is non-negative")
        bound = account.bounds.limits.get(dimension)
        exposure = account.spent.get(dimension, 0) + reserved.get(dimension, 0) + amount
        if bound is not None and exposure > bound:
            raise UsageBoundExceeded(dimension, exposure, bound)
        reserved[dimension] = reserved.get(dimension, 0) + amount
    return account.model_copy(update={"reserved": reserved})


def settle(
    account: UsageAccount,
    observation: UsageObservation,
    *,
    release: Mapping[UsageDimension, int] | None = None,
) -> UsageAccount:
    """Apply one observation: observed values add to `spent` (or `estimated` when marked),
    unobservable dimensions join `unknown`, and the matching reservation is released."""

    spent = dict(account.spent)
    estimated = dict(account.estimated)
    unknown = set(account.unknown)
    reserved = dict(account.reserved)
    for dimension in USAGE_DIMENSIONS:
        value = getattr(observation, dimension)
        if value is None:
            if (
                dimension == "cost_micros_usd"
                or dimension in reserved
                or dimension in account.bounds.limits
            ):
                unknown.add(dimension)
            continue
        target = estimated if dimension in observation.estimated else spent
        target[dimension] = target.get(dimension, 0) + value
    for dimension, amount in (release or {}).items():
        reserved[dimension] = max(0, reserved.get(dimension, 0) - amount)
    return account.model_copy(
        update={
            "spent": spent,
            "estimated": estimated,
            "reserved": {key: value for key, value in reserved.items() if value},
            "unknown": tuple(sorted(unknown)),
        }
    )


__all__ = [
    "KERNEL_ENFORCED_DIMENSIONS",
    "LIMIT_AUTH_REJECTED",
    "LIMIT_BILLING_UNAVAILABLE",
    "LIMIT_ENTITLEMENT_MISSING",
    "LIMIT_QUOTA_EXHAUSTED",
    "LIMIT_RESET_AFTER_DEADLINE",
    "LIMIT_UNCLASSIFIED",
    "LIMIT_WAIT",
    "LIMIT_WAITS_EXHAUSTED",
    "USAGE_BOUND_EXCEEDED",
    "USAGE_BOUND_REQUIRED",
    "USAGE_COST_CAP_UNENFORCEABLE",
    "USAGE_DIMENSIONS",
    "LimitDecision",
    "LimitKind",
    "LimitWaitLedger",
    "LimitWaitOutcome",
    "LimitWaitPolicy",
    "OverageState",
    "ProviderLimitSignal",
    "UsageAccount",
    "UsageBoundExceeded",
    "UsageBounds",
    "UsageDimension",
    "UsageIssue",
    "UsageObservation",
    "WaitPrimitive",
    "plan_limit_response",
    "require_finite_bounds",
    "reserve",
    "settle",
    "wait_for_limit_reset",
]
