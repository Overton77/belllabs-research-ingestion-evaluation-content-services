"""Classify native quota / rate-limit / auth conditions into `ProviderLimitSignal`.

Sources (retrieved with `npx ctx7@latest docs`, 2026-10-08):

* Claude Agent SDK (Python) `RateLimitEvent.rate_limit_info`: `status`
  (`allowed | allowed_warning | rejected`), `resets_at` (unix seconds), `rate_limit_type`,
  `utilization`, `overage_status`, `overage_resets_at`; `AssistantMessageError` values
  `authentication_failed | billing_error | rate_limit | invalid_request | server_error |
  unknown` (TypeScript adds `oauth_org_not_allowed`, `account_on_hold`, `overloaded`,
  `model_not_found`, `cloud_credential_error`); `errorCode: "credits_required"` marks an
  exhausted claude.ai subscription. https://code.claude.com/docs/en/agent-sdk/python and
  .../typescript (`/websites/code_claude_en_agent-sdk`).
* Codex app-server `account/rateLimits/read` and `account/rateLimits/updated`:
  `RateLimitSnapshot{primary, secondary, credits, spendControlReached, rateLimitReachedType}`
  with `RateLimitWindow{usedPercent, windowDurationMins, resetsAt}` (camelCase on the wire);
  `CodexErrorInfo::UsageLimitExceeded` for usage-limit/quota errors. Reading rate limits
  requires ChatGPT auth. https://github.com/openai/codex/blob/main/codex-rs/app-server-protocol/src/protocol/v2/account.rs
  (`/openai/codex`).
* HTTP 401/403/429/529 for REST-style surfaces (Cursor Cloud API): `Retry-After` seconds.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, Final

from mission_control.application.execution.usage_admission import (
    LimitKind,
    OverageState,
    ProviderLimitSignal,
)
from mission_control.domain.execution.lanes import LaneProfileName

_CLAUDE_ERROR_KINDS: Final[Mapping[str, LimitKind]] = {
    "authentication_failed": "auth_failed",
    "oauth_org_not_allowed": "auth_failed",
    "account_on_hold": "auth_failed",
    "cloud_credential_error": "auth_failed",
    "billing_error": "billing_unavailable",
    "rate_limit": "rate_limited",
    "overloaded": "overloaded",
    "model_not_found": "entitlement_missing",
}


def _unix(value: Any) -> datetime | None:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return None
    return datetime.fromtimestamp(float(value), UTC)


def _overage(value: Any) -> OverageState:
    if value in ("allowed", "allowed_warning"):
        return "allowed"
    if value == "rejected":
        return "rejected"
    return "unknown"


def classify_claude_rate_limit_info(
    lane_profile: LaneProfileName, info: Mapping[str, Any], *, fixture: bool = False
) -> ProviderLimitSignal | None:
    """`RateLimitInfo` -> signal; `allowed` is no signal at all."""

    status = info.get("status")
    if status == "allowed":
        return None
    raw = info.get("raw")
    error_code = raw.get("errorCode") if isinstance(raw, Mapping) else info.get("errorCode")
    kind: LimitKind
    if status == "allowed_warning":
        kind = "approaching_limit"
    elif status == "rejected":
        kind = "quota_exhausted" if error_code == "credits_required" else "rate_limited"
    else:
        kind = "unknown"
    utilization = info.get("utilization")
    window = info.get("rate_limit_type")
    return ProviderLimitSignal(
        lane_profile=lane_profile,
        kind=kind,
        resets_at=_unix(info.get("resets_at")),
        window=window if isinstance(window, str) else None,
        utilization=float(utilization) if isinstance(utilization, int | float) else None,
        overage=_overage(info.get("overage_status")),
        source="claude_sdk.rate_limit_event",
        native_code=error_code if isinstance(error_code, str) else str(status),
        fixture=fixture,
    )


def classify_claude_assistant_error(
    lane_profile: LaneProfileName, error: str, *, fixture: bool = False
) -> ProviderLimitSignal | None:
    """`AssistantMessage.error` -> signal; non-capacity errors return None."""

    if error in ("invalid_request", "server_error", "max_output_tokens"):
        return None
    return ProviderLimitSignal(
        lane_profile=lane_profile,
        kind=_CLAUDE_ERROR_KINDS.get(error, "unknown"),
        source="claude_sdk.assistant_error",
        native_code=error[:128],
        fixture=fixture,
    )


def classify_codex_rate_limits(
    lane_profile: LaneProfileName,
    snapshot: Mapping[str, Any],
    *,
    warn_at_percent: int = 90,
    fixture: bool = False,
) -> ProviderLimitSignal | None:
    """`RateLimitSnapshot` -> signal for the most constrained window (None: headroom)."""

    windows: list[tuple[str, Mapping[str, Any]]] = [
        (name, window)
        for name in ("primary", "secondary")
        if isinstance(window := snapshot.get(name), Mapping)
    ]
    reached = snapshot.get("rateLimitReachedType")
    spend_control = snapshot.get("spendControlReached") is True
    worst: tuple[str, Mapping[str, Any]] | None = None
    for name, window in windows:
        used = window.get("usedPercent")
        if isinstance(used, int | float) and (
            worst is None or used > float(worst[1].get("usedPercent") or 0)
        ):
            worst = (name, window)
    used_percent = float(worst[1].get("usedPercent") or 0) if worst is not None else 0.0
    exhausted = used_percent >= 100 or reached is not None or spend_control
    if not exhausted and used_percent < warn_at_percent:
        return None
    resets: list[datetime] = []
    for _name, window in windows:
        used = window.get("usedPercent")
        reset = _unix(window.get("resetsAt"))
        if reset is not None and isinstance(used, int | float) and used >= 100:
            resets.append(reset)
    if not resets and worst is not None:
        reset = _unix(worst[1].get("resetsAt"))
        if reset is not None:
            resets.append(reset)
    kind: LimitKind = "approaching_limit"
    if exhausted:
        kind = "quota_exhausted" if spend_control else "rate_limited"
    return ProviderLimitSignal(
        lane_profile=lane_profile,
        kind=kind,
        # The latest reset of the exhausted windows: waiting for an earlier one is useless.
        resets_at=max(resets) if resets else None,
        window=worst[0] if worst is not None else None,
        utilization=used_percent / 100,
        overage="unknown",
        source="codex.account_rate_limits",
        native_code=str(reached)[:128] if reached is not None else None,
        fixture=fixture,
    )


def classify_codex_error_info(
    lane_profile: LaneProfileName, codex_error_info: str, *, fixture: bool = False
) -> ProviderLimitSignal | None:
    # Only the usage-limit variant is confirmed; the wire casing of the enum is not, so both
    # spellings are accepted. Every other variant is left to the adapter's error handling.
    kinds: Mapping[str, LimitKind] = {
        "usageLimitExceeded": "quota_exhausted",
        "UsageLimitExceeded": "quota_exhausted",
    }
    kind = kinds.get(codex_error_info)
    if kind is None:
        return None
    return ProviderLimitSignal(
        lane_profile=lane_profile,
        kind=kind,
        source="codex.error_info",
        native_code=codex_error_info[:128],
        fixture=fixture,
    )


def classify_http_status(
    lane_profile: LaneProfileName,
    status: int,
    *,
    retry_after_s: float | None = None,
    now: datetime | None = None,
    source: str = "http",
    fixture: bool = False,
) -> ProviderLimitSignal | None:
    kind: LimitKind
    if status in (401, 403):
        kind = "auth_failed"
    elif status == 402:
        kind = "billing_unavailable"
    elif status == 429:
        kind = "rate_limited"
    elif status in (503, 529):
        kind = "overloaded"
    else:
        return None
    resets_at = None
    if retry_after_s is not None and retry_after_s >= 0 and kind in ("rate_limited", "overloaded"):
        resets_at = (now or datetime.now(UTC)) + timedelta(seconds=retry_after_s)
    return ProviderLimitSignal(
        lane_profile=lane_profile,
        kind=kind,
        resets_at=resets_at,
        source=source,
        native_code=str(status),
        fixture=fixture,
    )


__all__ = [
    "classify_claude_assistant_error",
    "classify_claude_rate_limit_info",
    "classify_codex_error_info",
    "classify_codex_rate_limits",
    "classify_http_status",
]
