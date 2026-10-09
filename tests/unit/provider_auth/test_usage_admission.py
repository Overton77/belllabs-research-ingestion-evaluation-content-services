"""MP-05 quota/rate-limit dispositions, finite usage bounds and unknown cost.

The bounding tests use a FAKE CLOCK and a fake wait primitive (labelled per test); they prove
the decision and waiting logic, not provider behaviour. Limit payloads are SYNTHETIC
FIXTURES shaped after the documented Claude SDK `RateLimitInfo` and Codex
`RateLimitSnapshot` fields. The real-timer proof is
`tests/integration/temporal/test_mp05_limit_wait.py` (local Temporal).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from mission_control.adapters.provider_auth.limits import (
    classify_claude_assistant_error,
    classify_claude_rate_limit_info,
    classify_codex_error_info,
    classify_codex_rate_limits,
    classify_http_status,
)
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.application.execution.usage_admission import (
    LIMIT_AUTH_REJECTED,
    LIMIT_BILLING_UNAVAILABLE,
    LIMIT_QUOTA_EXHAUSTED,
    LIMIT_RESET_AFTER_DEADLINE,
    LIMIT_WAIT,
    LIMIT_WAITS_EXHAUSTED,
    USAGE_BOUND_REQUIRED,
    USAGE_COST_CAP_UNENFORCEABLE,
    LimitDecision,
    LimitWaitLedger,
    LimitWaitPolicy,
    ProviderLimitSignal,
    UsageAccount,
    UsageBoundExceeded,
    UsageBounds,
    UsageObservation,
    plan_limit_response,
    require_finite_bounds,
    reserve,
    settle,
    wait_for_limit_reset,
)
from mission_control.domain.policies.stop_fence import EffectAdmission, StopFence

T0 = datetime(2026, 10, 8, 21, 0, tzinfo=UTC)
DEADLINE = T0 + timedelta(hours=1)


def _signal(**fields: object) -> ProviderLimitSignal:
    base: dict[str, object] = {
        "lane_profile": "claude_agent_sdk",
        "kind": "rate_limited",
        "source": "fixture",
        "fixture": True,
    }
    return ProviderLimitSignal.model_validate({**base, **fields})


# --- Classification (synthetic fixtures) -----------------------------------------------------


def test_claude_rate_limit_info_classification() -> None:
    reset = int((T0 + timedelta(minutes=10)).timestamp())
    assert classify_claude_rate_limit_info("claude_agent_sdk", {"status": "allowed"}) is None
    warning = classify_claude_rate_limit_info(
        "claude_agent_sdk", {"status": "allowed_warning", "utilization": 0.92}, fixture=True
    )
    assert warning is not None and warning.kind == "approaching_limit"
    rejected = classify_claude_rate_limit_info(
        "claude_agent_sdk",
        {"status": "rejected", "resets_at": reset, "rate_limit_type": "five_hour"},
    )
    assert rejected is not None
    assert rejected.kind == "rate_limited"
    assert rejected.resets_at == T0 + timedelta(minutes=10)
    exhausted = classify_claude_rate_limit_info(
        "claude_agent_sdk",
        {
            "status": "rejected",
            "raw": {"errorCode": "credits_required"},
            "overage_status": "rejected",
        },
    )
    assert exhausted is not None
    assert (exhausted.kind, exhausted.overage) == ("quota_exhausted", "rejected")


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        ("authentication_failed", "auth_failed"),
        ("oauth_org_not_allowed", "auth_failed"),
        ("billing_error", "billing_unavailable"),
        ("rate_limit", "rate_limited"),
        ("overloaded", "overloaded"),
        ("model_not_found", "entitlement_missing"),
        ("something_new", "unknown"),
    ],
)
def test_claude_assistant_error_classification(error: str, kind: str) -> None:
    signal = classify_claude_assistant_error("claude_agent_sdk", error)
    assert signal is not None and signal.kind == kind
    assert classify_claude_assistant_error("claude_agent_sdk", "server_error") is None


def test_codex_rate_limit_snapshot_classification() -> None:
    early = int((T0 + timedelta(minutes=5)).timestamp())
    late = int((T0 + timedelta(minutes=50)).timestamp())
    assert (
        classify_codex_rate_limits("codex", {"primary": {"usedPercent": 40, "resetsAt": early}})
        is None
    )
    both = classify_codex_rate_limits(
        "codex",
        {
            "primary": {"usedPercent": 100, "windowDurationMins": 300, "resetsAt": early},
            "secondary": {"usedPercent": 100, "windowDurationMins": 10080, "resetsAt": late},
        },
    )
    assert both is not None and both.kind == "rate_limited"
    # Waiting for the earlier window alone would be useless: the latest exhausted reset wins.
    assert both.resets_at == T0 + timedelta(minutes=50)
    spend = classify_codex_rate_limits(
        "codex", {"primary": {"usedPercent": 10}, "spendControlReached": True}
    )
    assert spend is not None and spend.kind == "quota_exhausted"
    usage = classify_codex_error_info("codex", "usageLimitExceeded")
    assert usage is not None and usage.kind == "quota_exhausted"
    assert classify_codex_error_info("codex", "other") is None


def test_http_status_classification() -> None:
    limited = classify_http_status("cursor_cloud", 429, retry_after_s=30, now=T0)
    assert limited is not None and limited.resets_at == T0 + timedelta(seconds=30)
    auth = classify_http_status("cursor_cloud", 401)
    assert auth is not None and auth.kind == "auth_failed"
    assert classify_http_status("cursor_cloud", 500) is None


# --- Dispositions ----------------------------------------------------------------------------


def test_reset_before_deadline_waits_until_reset_plus_margin() -> None:
    decision = plan_limit_response(
        _signal(resets_at=T0 + timedelta(minutes=10)), now=T0, deadline=DEADLINE
    )
    assert decision.disposition == "wait" and decision.code == LIMIT_WAIT
    assert decision.wait_until == T0 + timedelta(minutes=10, seconds=5)
    assert decision.wait_until is not None and decision.wait_until <= DEADLINE


def test_reset_after_deadline_rejects_with_pointed_code() -> None:
    decision = plan_limit_response(
        _signal(resets_at=DEADLINE + timedelta(seconds=1)), now=T0, deadline=DEADLINE
    )
    assert decision.disposition == "reject"
    assert decision.code == LIMIT_RESET_AFTER_DEADLINE
    assert decision.wait_until is None and decision.retryable is True


def test_unstated_reset_uses_bounded_backoff_capped_by_deadline() -> None:
    policy = LimitWaitPolicy(fallback_backoff_s=30, max_backoff_s=120, max_waits=10)
    waits = [
        plan_limit_response(
            _signal(kind="overloaded"),
            now=T0,
            deadline=DEADLINE,
            policy=policy,
            ledger=LimitWaitLedger(waits_used=used),
        ).wait_s
        for used in range(5)
    ]
    assert waits == [30, 60, 120, 120, 120]
    near = plan_limit_response(
        _signal(kind="overloaded"), now=T0, deadline=T0 + timedelta(seconds=10), policy=policy
    )
    assert near.code == LIMIT_RESET_AFTER_DEADLINE


def test_wait_budget_is_finite() -> None:
    policy = LimitWaitPolicy(max_waits=2, max_total_wait_s=900)
    used_up = plan_limit_response(
        _signal(resets_at=T0 + timedelta(seconds=60)),
        now=T0,
        deadline=DEADLINE,
        policy=policy,
        ledger=LimitWaitLedger(waits_used=2),
    )
    assert used_up.code == LIMIT_WAITS_EXHAUSTED
    too_long = plan_limit_response(
        _signal(resets_at=T0 + timedelta(seconds=800)),
        now=T0,
        deadline=DEADLINE,
        policy=policy,
        ledger=LimitWaitLedger(waits_used=1, waited_s=200),
    )
    assert too_long.code == LIMIT_WAITS_EXHAUSTED


@pytest.mark.parametrize(
    ("kind", "code"),
    [
        ("auth_failed", LIMIT_AUTH_REJECTED),
        ("billing_unavailable", LIMIT_BILLING_UNAVAILABLE),
        ("quota_exhausted", LIMIT_QUOTA_EXHAUSTED),
    ],
)
def test_non_waitable_conditions_reject_and_never_propose_another_route(
    kind: str, code: str
) -> None:
    decision = plan_limit_response(_signal(kind=kind), now=T0, deadline=DEADLINE)
    assert decision.disposition == "reject" and decision.code == code
    assert "api" not in decision.code.lower()


def test_quota_with_stated_reset_waits_unless_policy_refuses() -> None:
    signal = _signal(kind="quota_exhausted", resets_at=T0 + timedelta(minutes=20))
    assert plan_limit_response(signal, now=T0, deadline=DEADLINE).disposition == "wait"
    refuse = LimitWaitPolicy(wait_on_quota_reset=False)
    decision = plan_limit_response(signal, now=T0, deadline=DEADLINE, policy=refuse)
    assert decision.code == LIMIT_QUOTA_EXHAUSTED


def test_warning_continues() -> None:
    decision = plan_limit_response(_signal(kind="approaching_limit"), now=T0, deadline=DEADLINE)
    assert decision.disposition == "continue"


# --- Bounded waits with cancellation (FAKE CLOCK) --------------------------------------------


class FakeClock:
    def __init__(self, start: datetime) -> None:
        self.value = start

    def now(self) -> datetime:
        return self.value


class FakeWait:
    """Fake `workflow.wait_condition`: advances the fake clock by the timeout, or to the
    scheduled cancel instant when that comes first (and runs the cancel handler there)."""

    def __init__(
        self,
        clock: FakeClock,
        *,
        cancel_after_s: float | None = None,
        on_cancel: Callable[[], None],
    ) -> None:
        self.clock = clock
        self.cancel_after_s = cancel_after_s
        self.on_cancel = on_cancel
        self.timeouts: list[float] = []

    async def __call__(self, predicate: Callable[[], bool], timeout_s: float) -> bool:
        self.timeouts.append(timeout_s)
        if self.cancel_after_s is not None and self.cancel_after_s < timeout_s:
            self.clock.value += timedelta(seconds=self.cancel_after_s)
            self.on_cancel()
            return predicate()
        self.clock.value += timedelta(seconds=timeout_s)
        return predicate()


async def test_fake_clock_wait_elapses_at_reset_and_never_past_deadline() -> None:
    clock = FakeClock(T0)
    cancelled = {"value": False}
    wait = FakeWait(clock, on_cancel=lambda: None)
    decision = plan_limit_response(
        _signal(resets_at=T0 + timedelta(minutes=10)), now=T0, deadline=DEADLINE
    )
    outcome = await wait_for_limit_reset(
        decision, now=clock.now, deadline=DEADLINE, wait=wait, cancelled=lambda: cancelled["value"]
    )
    assert outcome.outcome == "reset_elapsed"
    assert wait.timeouts == [605.0]
    assert clock.value <= DEADLINE
    assert outcome.ledger == LimitWaitLedger(waits_used=1, waited_s=605)


async def test_fake_clock_wait_is_clamped_to_a_deadline_that_moved_in() -> None:
    clock = FakeClock(T0)
    wait = FakeWait(clock, on_cancel=lambda: None)
    decision = LimitDecision(
        disposition="wait", code=LIMIT_WAIT, message="m", wait_until=T0 + timedelta(minutes=30)
    )
    earlier = T0 + timedelta(minutes=5)
    outcome = await wait_for_limit_reset(
        decision, now=clock.now, deadline=earlier, wait=wait, cancelled=lambda: False
    )
    assert outcome.outcome == "deadline_reached"
    assert wait.timeouts == [300.0] and clock.value == earlier


async def test_fake_clock_cancel_during_wait_reaches_pending_work_through_the_stop_fence() -> None:
    """FAKE CLOCK + in-memory Stop Fence. A rate-limited run waits for a 10-minute reset; an
    immediate cancel lands 30 s in. The wait returns `cancelled` at once and the pending
    provider resend is denied by the fence, so cancellation reaches pending work instead of
    sleeping through it."""

    clock = FakeClock(T0)
    fences = InMemoryStopFenceRepository()
    state = {"cancelled": False}

    def cancel_handler() -> None:
        state["cancelled"] = True

    wait = FakeWait(clock, cancel_after_s=30, on_cancel=cancel_handler)
    decision = plan_limit_response(
        _signal(resets_at=T0 + timedelta(minutes=10)), now=T0, deadline=DEADLINE
    )
    outcome = await wait_for_limit_reset(
        decision, now=clock.now, deadline=DEADLINE, wait=wait, cancelled=lambda: state["cancelled"]
    )
    assert outcome.outcome == "cancelled"
    assert outcome.waited_s == 30
    # The workflow's cancel path persists the fence; the pending resend is then denied.
    await fences.persist(
        StopFence(
            request_scope="app:t",
            run_id="run-1",
            generation=1,
            command_id="cmd-cancel",
            reason="operator cancel during capacity wait",
            requested_at=clock.now(),
        )
    )
    verdict = await fences.admit_effect(
        EffectAdmission(
            request_scope="app:t",
            run_id="run-1",
            generation=1,
            effect_ref="resend:turn-2",
            lane_profile="claude_agent_sdk",
        )
    )
    assert verdict.decision == "deny" and verdict.reason_code == "STOP_FENCED"


async def test_cancel_already_requested_skips_the_wait() -> None:
    clock = FakeClock(T0)
    wait = FakeWait(clock, on_cancel=lambda: None)
    decision = plan_limit_response(
        _signal(resets_at=T0 + timedelta(minutes=10)), now=T0, deadline=DEADLINE
    )
    outcome = await wait_for_limit_reset(
        decision, now=clock.now, deadline=DEADLINE, wait=wait, cancelled=lambda: True
    )
    assert outcome.outcome == "cancelled" and wait.timeouts == []


async def test_real_event_loop_cancel_wakes_wait_condition_shaped_primitive() -> None:
    """An asyncio stand-in for `workflow.wait_condition` (real loop, short real timeout)."""

    event = asyncio.Event()

    async def wait(predicate: Callable[[], bool], timeout_s: float) -> bool:
        try:
            await asyncio.wait_for(event.wait(), timeout_s)
        except TimeoutError:
            return predicate()
        return predicate()

    start = datetime.now(UTC)
    decision = LimitDecision(
        disposition="wait", code=LIMIT_WAIT, message="m", wait_until=start + timedelta(seconds=30)
    )
    loop = asyncio.get_running_loop()
    loop.call_later(0.05, event.set)
    began = loop.time()
    outcome = await wait_for_limit_reset(
        decision,
        now=lambda: datetime.now(UTC),
        deadline=start + timedelta(minutes=5),
        wait=wait,
        cancelled=event.is_set,
    )
    assert outcome.outcome == "cancelled"
    assert loop.time() - began < 5


async def test_only_wait_decisions_can_be_waited() -> None:
    clock = FakeClock(T0)
    reject = plan_limit_response(_signal(kind="auth_failed"), now=T0, deadline=DEADLINE)
    with pytest.raises(ValueError):
        await wait_for_limit_reset(
            reject,
            now=clock.now,
            deadline=DEADLINE,
            wait=FakeWait(clock, on_cancel=lambda: None),
            cancelled=lambda: False,
        )


# --- Usage bounds and unknown cost -----------------------------------------------------------


def test_turn_and_time_bounds_are_mandatory_and_dollar_caps_need_enforcement() -> None:
    issues = require_finite_bounds(UsageBounds(), billing_mode="subscription")
    assert [(i.code, i.pointer) for i in issues] == [
        (USAGE_BOUND_REQUIRED, "/environment/budget/turns"),
        (USAGE_BOUND_REQUIRED, "/environment/budget/wall_clock_s"),
    ]
    capped = UsageBounds(limits={"turns": 12, "wall_clock_s": 3600, "cost_micros_usd": 5_000_000})
    (cap,) = require_finite_bounds(capped, billing_mode="api")
    assert cap.code == USAGE_COST_CAP_UNENFORCEABLE
    enforced = capped.model_copy(update={"cost_cap_enforced_by": "provider:workspace-spend-limit"})
    assert require_finite_bounds(enforced, billing_mode="api") == ()
    with pytest.raises(ValueError):
        UsageBounds(limits={"turns": 0})


def test_unobservable_cost_stays_unknown_and_subscription_is_not_zero_dollars() -> None:
    account = UsageAccount(
        billing_mode="subscription",
        bounds=UsageBounds(limits={"turns": 4, "wall_clock_s": 600, "total_tokens": 10_000}),
    )
    account = reserve(account, {"turns": 1, "total_tokens": 2_000})
    account = settle(
        account,
        UsageObservation(turns=1, wall_clock_s=42, total_tokens=None, cost_micros_usd=None),
        release={"turns": 1, "total_tokens": 2_000},
    )
    assert account.spent == {"turns": 1, "wall_clock_s": 42}
    assert "cost_micros_usd" in account.unknown and "total_tokens" in account.unknown
    assert account.cost_state == "unknown"
    assert "cost_micros_usd" not in account.spent  # never recorded as 0
    notes = account.disclosures()
    assert any("not a zero dollar cost" in note for note in notes)
    assert any(note.startswith("total_tokens not observable") for note in notes)
    # Once unknown, a later observation does not launder the dimension back to known.
    account = settle(account, UsageObservation(turns=1, cost_micros_usd=10))
    assert account.cost_state == "unknown"


def test_estimated_cost_is_reported_as_estimated() -> None:
    account = settle(
        UsageAccount(billing_mode="api"),
        UsageObservation(turns=1, cost_micros_usd=1234, estimated=frozenset({"cost_micros_usd"})),
    )
    assert account.cost_state == "estimated"
    assert account.estimated == {"cost_micros_usd": 1234}


def test_reservation_refuses_beyond_a_finite_bound() -> None:
    account = UsageAccount(billing_mode="api", bounds=UsageBounds(limits={"turns": 2}))
    account = settle(
        reserve(account, {"turns": 1}), UsageObservation(turns=1), release={"turns": 1}
    )
    account = reserve(account, {"turns": 1})
    with pytest.raises(UsageBoundExceeded):
        reserve(account, {"turns": 1})


def test_capacity_waits_do_not_touch_mission_counters() -> None:
    """A wait decision and its ledger are separate values; the usage account (and the
    caller's iteration/attempt counters) pass through unchanged."""

    account = settle(UsageAccount(billing_mode="subscription"), UsageObservation(turns=3))
    before = account.model_dump()
    decision = plan_limit_response(
        _signal(resets_at=T0 + timedelta(minutes=1)), now=T0, deadline=DEADLINE
    )
    assert decision.disposition == "wait"
    ledger = LimitWaitLedger().after_wait(65)
    assert ledger.waits_used == 1
    assert account.model_dump() == before
    assert set(LimitDecision.model_fields).isdisjoint({"turns", "iteration", "attempt", "spent"})
