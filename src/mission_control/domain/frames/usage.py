"""Usage reports with dispositions (SPEC-03 `UsageReport@1`).

Every dimension carries `settled`, `estimated` or `unknown`. `unknown` is never zero: its
value is `None` and it renders as unknown. Lane rules: Deep Agents tokens are settled from
the model end event and cost is estimated from the route's price table (unknown when no
price is configured); Cursor tokens are settled from `TurnEndedUpdate` / `usage`, cost is
estimated until a later `usage` frame carries `charged_cents`, which settles it; Claude
(future) `total_cost_usd` is estimated by definition.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from mission_control.domain.frames.contracts import LaneProfile

USAGE_REPORT_SCHEMA = "mc.usage_report.v1"
TOKEN_DIMENSIONS: tuple[str, ...] = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_tokens",
)
DIMENSIONS: tuple[str, ...] = (*TOKEN_DIMENSIONS, "cost_micros")

# Provider field aliases per dimension (Deep Agents / LangChain usage_metadata, Cursor
# camelCase usage, Claude SDK names).
_ALIASES: dict[str, tuple[str, ...]] = {
    "input_tokens": ("input_tokens", "inputTokens", "prompt_tokens"),
    "cached_input_tokens": (
        "cached_input_tokens",
        "cache_read",
        "cacheReadTokens",
        "cache_read_input_tokens",
    ),
    "output_tokens": ("output_tokens", "outputTokens", "completion_tokens"),
    "reasoning_tokens": ("reasoning_tokens", "reasoning", "reasoningTokens"),
}


class UsageDisposition(StrEnum):
    SETTLED = "settled"
    ESTIMATED = "estimated"
    UNKNOWN = "unknown"


class UsageDimension(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    value: int | None = Field(default=None, ge=0)
    disposition: UsageDisposition
    source_frame_id: str | None = None

    def model_post_init(self, context: Any, /) -> None:
        del context
        if (self.disposition == UsageDisposition.UNKNOWN) != (self.value is None):
            raise ValueError("unknown usage has no value and a known usage has one")


class UsageReport(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["mc.usage_report.v1"] = "mc.usage_report.v1"
    dimensions: dict[str, UsageDimension]

    def value(self, dimension: str) -> int | None:
        item = self.dimensions.get(dimension)
        return item.value if item is not None else None


def _unknown() -> UsageDimension:
    return UsageDimension(value=None, disposition=UsageDisposition.UNKNOWN)


def _number(body: Mapping[str, Any], names: Iterable[str]) -> int | None:
    for name in names:
        value = body.get(name)
        if isinstance(value, bool):
            continue
        if isinstance(value, int | float) and value >= 0:
            return int(value)
        nested = body.get("usage")
        if isinstance(nested, Mapping) and name in nested:
            inner = nested.get(name)
            if isinstance(inner, int | float) and not isinstance(inner, bool) and inner >= 0:
                return int(inner)
    return None


def usage_report(
    lane: LaneProfile,
    usage_bodies: Iterable[tuple[str, Mapping[str, Any]]],
) -> UsageReport:
    """Fold the turn's closing `usage` / `turn_ended` bodies (frame id, body) in arrival order.

    Token dimensions sum across bodies and are settled when any body reports them. Cost
    starts estimated (or unknown) and becomes settled only from a body that carries the
    provider's charged amount (`charged_cents` or `charged_micros`); a later settled cost
    replaces earlier estimates instead of adding to them.
    """

    tokens: dict[str, tuple[int, str]] = {}
    cost: UsageDimension | None = None
    for frame_id, body in usage_bodies:
        for dimension in TOKEN_DIMENSIONS:
            value = _number(body, _ALIASES[dimension])
            if value is None:
                continue
            prior, _source = tokens.get(dimension, (0, frame_id))
            tokens[dimension] = (prior + value, frame_id)
        charged_micros = _number(body, ("charged_micros",))
        charged_cents = _number(body, ("charged_cents",))
        if charged_micros is None and charged_cents is not None:
            charged_micros = charged_cents * 10_000
        if charged_micros is not None:
            cost = UsageDimension(
                value=charged_micros,
                disposition=UsageDisposition.SETTLED,
                source_frame_id=frame_id,
            )
            continue
        if cost is not None and cost.disposition == UsageDisposition.SETTLED:
            continue
        estimated = _number(body, ("estimated_cost_micros", "cost_micros"))
        if estimated is None:
            usd = body.get("total_cost_usd") if isinstance(body, Mapping) else None
            if isinstance(usd, int | float) and not isinstance(usd, bool) and usd >= 0:
                estimated = round(usd * 1_000_000)
        if estimated is not None:
            prior_value = cost.value if cost is not None and cost.value is not None else 0
            cost = UsageDimension(
                value=prior_value + estimated,
                disposition=UsageDisposition.ESTIMATED,
                source_frame_id=frame_id,
            )
    del lane  # lane rules are expressed by what each lane's writer puts in the bodies
    dimensions: dict[str, UsageDimension] = {
        dimension: (
            UsageDimension(
                value=tokens[dimension][0],
                disposition=UsageDisposition.SETTLED,
                source_frame_id=tokens[dimension][1],
            )
            if dimension in tokens
            else _unknown()
        )
        for dimension in TOKEN_DIMENSIONS
    }
    dimensions["cost_micros"] = cost if cost is not None else _unknown()
    return UsageReport(dimensions=dimensions)


def render_dimension(item: UsageDimension | None) -> str:
    """Human rendering; unknown is never shown as zero."""

    if item is None or item.disposition == UsageDisposition.UNKNOWN or item.value is None:
        return "unknown"
    suffix = "" if item.disposition == UsageDisposition.SETTLED else " (estimated)"
    return f"{item.value}{suffix}"
