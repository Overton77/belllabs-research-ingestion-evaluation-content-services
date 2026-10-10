"""Context pressure policy: watermarks, reserved headroom, turn budgets, honest unknowns.

SPEC-01 "Four independent progress mechanisms" (ADR-0039): the policy uses fresh token
occupancy only when the lane exposes it with a known model window. Soft and hard
watermarks and the reserved instruction/output headroom are tuning inputs (defaults
70 % / 85 % / 15 %), not provider guarantees. Cumulative billed tokens are never
occupancy. When no occupancy is available the policy falls back to conservative
turn-boundary budgets and reports ``unknown``: it never invents a percentage.

At soft pressure the lane prefers a *qualified* native compaction (``compaction_route``)
and remeasures; at hard pressure, or when native compaction is unqualified, unavailable
or failed, the fallback is the sealed Continuation Checkpoint (``sealed_checkpoint``).
Pure: the lane turn service captures the occupancy and persists the assessment as a
non-closing observation frame.
"""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal
from math import ceil
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


NativeCompactionPreference = Literal["preferred", "disabled", "required"]
ContinuationFallback = Literal["sealed_checkpoint", "fail"]
PressureLevel = Literal["none", "soft", "hard"]
PressureBasis = Literal["occupancy", "turn_budget", "unknown"]
CompactionRoute = Literal["native", "sealed_checkpoint", "none", "fail"]
OccupancySource = Literal["provider_context_window", "provider_turn_usage", "lane_estimate"]


class ContextPressurePolicy(_Contract):
    """The runtime shape of the ``mission/v2`` ``continuation`` block."""

    soft_context_ratio: Decimal = Field(default=Decimal("0.70"), gt=0, le=1)
    hard_context_ratio: Decimal = Field(default=Decimal("0.85"), gt=0, le=1)
    reserve_ratio: Decimal = Field(default=Decimal("0.15"), ge=0, lt=1)
    max_session_turns: int | None = Field(default=None, ge=1)
    max_transfers: int | None = Field(default=None, ge=0)
    max_compaction_failures: int = Field(default=2, ge=0)
    native_compaction: NativeCompactionPreference = "preferred"
    fallback: ContinuationFallback = "sealed_checkpoint"

    @model_validator(mode="after")
    def _ordered(self) -> ContextPressurePolicy:
        if self.soft_context_ratio >= self.hard_context_ratio:
            raise ValueError("soft_context_ratio must be below hard_context_ratio")
        if self.hard_context_ratio + self.reserve_ratio > 1:
            raise ValueError("hard_context_ratio plus reserve_ratio cannot exceed 1")
        return self

    @property
    def effective_hard_ratio(self) -> Decimal:
        """The hard watermark never eats into the reserved headroom."""

        return min(self.hard_context_ratio, Decimal(1) - self.reserve_ratio)

    @classmethod
    def from_manifest(cls, policy: Any) -> ContextPressurePolicy:
        """From a ``mission/v2`` ``ContinuationPolicy`` (duck-typed: the manifest module is
        frozen and owned by the integrator; the field names are the contract)."""

        return cls(
            soft_context_ratio=Decimal(str(policy.soft_context_ratio)),
            hard_context_ratio=Decimal(str(policy.hard_context_ratio)),
            reserve_ratio=Decimal(str(policy.reserve_ratio)),
            max_session_turns=policy.max_session_turns,
            max_transfers=policy.max_transfers,
            max_compaction_failures=policy.max_compaction_failures,
            native_compaction=policy.native_compaction,
            fallback=policy.fallback,
        )


class ContextOccupancy(_Contract):
    """What the lane actually knows about the live context window.

    ``known`` carries the tokens occupying the window and the window size the provider
    reports for the model in use; anything the lane cannot read stays ``unknown`` with the
    reason. A cumulative usage total is not an occupancy and must not be passed as one.
    """

    known: bool
    used_tokens: int | None = Field(default=None, ge=0)
    window_tokens: int | None = Field(default=None, ge=1)
    source: OccupancySource | None = None
    reason: str = Field(default="", max_length=256)

    @model_validator(mode="after")
    def _complete(self) -> ContextOccupancy:
        if self.known and (
            self.used_tokens is None or self.window_tokens is None or self.source is None
        ):
            raise ValueError("a known occupancy names its used tokens, window and source")
        if not self.known and (self.used_tokens is not None or self.window_tokens is not None):
            raise ValueError("an unknown occupancy carries no token counts")
        return self

    @classmethod
    def unknown(cls, reason: str) -> ContextOccupancy:
        return cls(known=False, reason=reason[:256])

    @classmethod
    def measured(cls, used_tokens: int, window_tokens: int, source: OccupancySource) -> Any:
        return cls(known=True, used_tokens=used_tokens, window_tokens=window_tokens, source=source)

    @property
    def ratio(self) -> Decimal | None:
        if not self.known:
            return None
        assert self.used_tokens is not None and self.window_tokens is not None
        return (Decimal(self.used_tokens) / Decimal(self.window_tokens)).quantize(
            Decimal("0.0001"), rounding=ROUND_DOWN
        )


class PressureAssessment(_Contract):
    level: PressureLevel
    basis: PressureBasis
    ratio: Decimal | None = None
    """The measured occupancy ratio, or ``None`` (reported as ``unknown``), never guessed."""
    headroom_tokens: int | None = Field(default=None, ge=0)
    turns_in_session: int = Field(ge=0)
    turn_budget: int | None = Field(default=None, ge=1)
    reason: str = Field(max_length=256)

    def observation(self) -> dict[str, object]:
        """The frame body of the observation (strings for decimals, ``unknown`` for none)."""

        return {
            "schema_version": "mc.context_pressure.v1",
            "level": self.level,
            "basis": self.basis,
            "ratio": "unknown" if self.ratio is None else str(self.ratio),
            "headroom_tokens": "unknown" if self.headroom_tokens is None else self.headroom_tokens,
            "turns_in_session": self.turns_in_session,
            "turn_budget": self.turn_budget,
            "reason": self.reason,
        }


def assess_pressure(
    policy: ContextPressurePolicy, occupancy: ContextOccupancy, *, turns_in_session: int
) -> PressureAssessment:
    """Soft/hard from a measured occupancy; otherwise from the session turn budget; otherwise
    ``none`` on an ``unknown`` basis (no occupancy, no budget: nothing is invented)."""

    if occupancy.known:
        ratio = occupancy.ratio
        assert ratio is not None and occupancy.window_tokens is not None
        assert occupancy.used_tokens is not None
        headroom = max(occupancy.window_tokens - occupancy.used_tokens, 0)
        reserve = ceil(policy.reserve_ratio * occupancy.window_tokens)
        level: PressureLevel
        if ratio >= policy.effective_hard_ratio or headroom < reserve:
            level = "hard"
            reason = f"occupancy {ratio} at or above hard watermark {policy.effective_hard_ratio}"
        elif ratio >= policy.soft_context_ratio:
            level = "soft"
            reason = f"occupancy {ratio} at or above soft watermark {policy.soft_context_ratio}"
        else:
            level = "none"
            reason = f"occupancy {ratio} below soft watermark {policy.soft_context_ratio}"
        return PressureAssessment(
            level=level,
            basis="occupancy",
            ratio=ratio,
            headroom_tokens=headroom,
            turns_in_session=turns_in_session,
            turn_budget=policy.max_session_turns,
            reason=reason,
        )
    budget = policy.max_session_turns
    if budget is None:
        return PressureAssessment(
            level="none",
            basis="unknown",
            turns_in_session=turns_in_session,
            reason=f"occupancy unknown ({occupancy.reason or 'not exposed'}); no turn budget",
        )
    soft_turns = max(ceil(policy.soft_context_ratio * budget), 1)
    if turns_in_session >= budget:
        level, reason = "hard", f"turn {turns_in_session} reached the session budget {budget}"
    elif turns_in_session >= soft_turns:
        level = "soft"
        reason = f"turn {turns_in_session} reached the soft turn budget {soft_turns} of {budget}"
    else:
        level, reason = "none", f"turn {turns_in_session} below the soft turn budget {soft_turns}"
    return PressureAssessment(
        level=level,
        basis="turn_budget",
        turns_in_session=turns_in_session,
        turn_budget=budget,
        reason=f"{reason}; occupancy unknown ({occupancy.reason or 'not exposed'})",
    )


def compaction_route(
    policy: ContextPressurePolicy,
    assessment: PressureAssessment,
    *,
    compaction_control: str | None,
    lane_can_compact: bool,
) -> CompactionRoute:
    """Which mechanism answers the assessed pressure.

    Native compaction is taken only when the lane's describe states ``compaction_control ==
    "native"`` *and* the lane implements the explicit operation *and* the policy admits it
    (``preferred`` or ``required``); it answers soft pressure. Hard pressure, soft pressure
    without a qualified native path, and ``required`` native compaction that is unavailable
    all route to the policy fallback. ``none`` means no pressure.
    """

    if assessment.level == "none":
        return "none"
    qualified = compaction_control == "native" and lane_can_compact
    native_admitted = policy.native_compaction != "disabled" and qualified
    if assessment.level == "soft" and native_admitted:
        return "native"
    if policy.native_compaction == "required" and not qualified:
        return "fail"
    return policy.fallback


def native_unavailable_reason(compaction_control: str | None, lane_can_compact: bool) -> str:
    if compaction_control != "native":
        return f"compaction_control is {compaction_control or 'undeclared'}, not native"
    if not lane_can_compact:
        return "the lane exposes no explicit compaction operation"
    return "native compaction available"


__all__ = [
    "CompactionRoute",
    "ContextOccupancy",
    "ContextPressurePolicy",
    "PressureAssessment",
    "PressureBasis",
    "PressureLevel",
    "assess_pressure",
    "compaction_route",
    "native_unavailable_reason",
]
