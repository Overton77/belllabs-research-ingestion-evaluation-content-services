"""MP-12: the context policy never invents a percentage and routes compaction honestly."""

from __future__ import annotations

from decimal import Decimal

import pytest

from mission_control.domain.authoring.manifest_v2 import ContinuationPolicy
from mission_control.domain.context.pressure import (
    ContextOccupancy,
    ContextPressurePolicy,
    assess_pressure,
    compaction_route,
    native_unavailable_reason,
)


def _measured(used: int, window: int = 100_000) -> ContextOccupancy:
    return ContextOccupancy.measured(used, window, "provider_context_window")


def test_watermarks_and_reserved_headroom_from_a_measured_occupancy() -> None:
    policy = ContextPressurePolicy()
    none = assess_pressure(policy, _measured(50_000), turns_in_session=1)
    soft = assess_pressure(policy, _measured(72_000), turns_in_session=1)
    hard = assess_pressure(policy, _measured(86_000), turns_in_session=1)
    assert (none.level, soft.level, hard.level) == ("none", "soft", "hard")
    assert {item.basis for item in (none, soft, hard)} == {"occupancy"}
    assert soft.ratio == Decimal("0.7200") and soft.headroom_tokens == 28_000
    # The hard watermark never eats into the reserve: 1 - 0.15 bounds it.
    tight = ContextPressurePolicy(hard_context_ratio=Decimal("0.90"), reserve_ratio=Decimal("0.10"))
    assert tight.effective_hard_ratio == Decimal("0.90")
    reserved = ContextPressurePolicy(
        hard_context_ratio=Decimal("0.84"), reserve_ratio=Decimal("0.16")
    )
    assert reserved.effective_hard_ratio == Decimal("0.84")
    assert assess_pressure(reserved, _measured(84_500), turns_in_session=1).level == "hard"


def test_unknown_occupancy_is_reported_unknown_never_a_guessed_ratio() -> None:
    policy = ContextPressurePolicy()
    unknown = assess_pressure(
        policy, ContextOccupancy.unknown("cursor exposes no window"), turns_in_session=3
    )
    assert (unknown.level, unknown.basis, unknown.ratio, unknown.headroom_tokens) == (
        "none",
        "unknown",
        None,
        None,
    )
    body = unknown.observation()
    assert body["ratio"] == "unknown" and body["headroom_tokens"] == "unknown"
    assert "cursor exposes no window" in unknown.reason


def test_without_occupancy_the_conservative_turn_budget_decides() -> None:
    policy = ContextPressurePolicy(max_session_turns=10)
    early = assess_pressure(policy, ContextOccupancy.unknown("n/a"), turns_in_session=3)
    soft = assess_pressure(policy, ContextOccupancy.unknown("n/a"), turns_in_session=7)
    hard = assess_pressure(policy, ContextOccupancy.unknown("n/a"), turns_in_session=10)
    assert (early.level, soft.level, hard.level) == ("none", "soft", "hard")
    assert {item.basis for item in (early, soft, hard)} == {"turn_budget"}
    assert all(item.ratio is None for item in (early, soft, hard)), "still no percentage"
    assert soft.turn_budget == 10 and soft.observation()["ratio"] == "unknown"


def test_a_cumulative_total_cannot_pose_as_occupancy() -> None:
    with pytest.raises(ValueError, match="names its used tokens"):
        ContextOccupancy(known=True, used_tokens=10, window_tokens=None, source=None)
    with pytest.raises(ValueError, match="carries no token counts"):
        ContextOccupancy(known=False, used_tokens=10)
    with pytest.raises(ValueError, match="soft_context_ratio must be below"):
        ContextPressurePolicy(soft_context_ratio=Decimal("0.9"), hard_context_ratio=Decimal("0.8"))


def test_native_compaction_is_a_qualified_only_path() -> None:
    policy = ContextPressurePolicy()
    soft = assess_pressure(policy, _measured(75_000), turns_in_session=1)
    hard = assess_pressure(policy, _measured(90_000), turns_in_session=1)
    none = assess_pressure(policy, _measured(10_000), turns_in_session=1)
    # Codex documents explicit compaction and a lane that implements it: native at soft.
    assert compaction_route(policy, soft, compaction_control="native", lane_can_compact=True) == (
        "native"
    )
    # Claude / Cursor: unqualified (or undeclared) control -> the sealed checkpoint.
    for control in ("unqualified", "unsupported", None):
        assert (
            compaction_route(policy, soft, compaction_control=control, lane_can_compact=True)
            == "sealed_checkpoint"
        )
    # A declared native control without an implementation is not qualified either.
    assert (
        compaction_route(policy, soft, compaction_control="native", lane_can_compact=False)
        == "sealed_checkpoint"
    )
    # Hard pressure always seals, even where native compaction is qualified.
    assert compaction_route(policy, hard, compaction_control="native", lane_can_compact=True) == (
        "sealed_checkpoint"
    )
    assert compaction_route(policy, none, compaction_control="native", lane_can_compact=True) == (
        "none"
    )
    disabled = ContextPressurePolicy(native_compaction="disabled")
    assert compaction_route(disabled, soft, compaction_control="native", lane_can_compact=True) == (
        "sealed_checkpoint"
    )
    required = ContextPressurePolicy(native_compaction="required")
    assert (
        compaction_route(required, soft, compaction_control="unqualified", lane_can_compact=True)
        == "fail"
    )
    assert "not native" in native_unavailable_reason("unqualified", True)
    assert "no explicit compaction" in native_unavailable_reason("native", False)


def test_the_manifest_v2_block_lowers_into_the_runtime_policy() -> None:
    manifest = ContinuationPolicy(
        max_session_turns=12, max_transfers=3, native_compaction="disabled"
    )
    policy = ContextPressurePolicy.from_manifest(manifest)
    assert policy.soft_context_ratio == Decimal("0.70")
    assert policy.hard_context_ratio == Decimal("0.85")
    assert policy.reserve_ratio == Decimal("0.15")
    assert (policy.max_session_turns, policy.max_transfers, policy.native_compaction) == (
        12,
        3,
        "disabled",
    )
    assert policy.fallback == "sealed_checkpoint"
