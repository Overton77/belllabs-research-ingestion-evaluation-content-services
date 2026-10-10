"""MP-07 live qualification drill helpers for `claude_agent_sdk` (owner-run, paid; never in CI).

Nothing here runs unless `test_claude_live_qualification.py` is selected with
`MC_LIVE_CLAUDE_QUALIFICATION=1`, a finite owner-approved `MC_PAID_BUDGET_USD`, an explicit
auth route (`MC_CLAUDE_AUTH_ROUTE=api_key|owner_cli_login`) admitted by MP-05 from the owner's
profile document (`MISSION_CONTROL_AUTH_PROFILES_PATH`, `MC_CLAUDE_AUTH_PROFILE`), and a
supported worker host (Linux / WSL 2 / macOS: `adapters/claude/host.host_gate`).

Credentials: the drill never reads, copies or writes a credential. On `api_key` the CLI gets
`ANTHROPIC_API_KEY` through the admitted child environment (`provider_child_environment`) and
its config dir is relocated under the lease; on `owner_cli_login` (Agent SDK docs:
`policy_restricted`) the owner's recorded attestation (`MC_CLAUDE_OWNER_ATTESTATION_REF`) must
equal the admitted profile's `owner_attestation_ref`, the lane keeps the owner's config dir
(where the CLI's own login lives) and copies nothing out of it.

The drill records the CLI's raw stream messages (the shapes `tests/fixtures/provider_frames/
claude/*.jsonl` replay through the SDK's `parse_message`), scrubbed of every bound secret value
and e-mail address, and writes `docs/qualification/lanes/claude_agent_sdk-<date>.md`: the only
evidence allowed to flip `qualified` (in a later, reviewed release).
"""

from __future__ import annotations

import json
import os
import platform
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from claude_agent_sdk.types import ClaudeAgentOptions

from mission_control.adapters.claude.describe import BUNDLED_CLI_VERSION, PROFILE, SDK_VERSION
from mission_control.adapters.claude.host import host_gate
from mission_control.adapters.claude.session import ClaudeClient
from mission_control.adapters.claude.transport import SdkClientFactory
from mission_control.adapters.cursor.frames import scrub

ROOT = Path(__file__).resolve().parents[3]
RECORDINGS = Path(__file__).resolve().parent / "recordings"
RECORDS = ROOT / "docs" / "qualification" / "lanes"
ROUTES: Final = ("api_key", "owner_cli_login")
SECRET_ENV: Final = ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN")

# docs/qualification/lanes/claude_agent_sdk/README.md "What the drill settles".
UNVERIFIED: Final = (
    "client_uuid_is_turn_identity",
    "interrupt_terminal_reason",
    "rate_limit_event_shape",
    "resume_after_process_death",
    "can_use_tool_reaches_the_lane",
    "pretooluse_deny_on_stop_fence",
    "precompact_callback",
    "cost_is_an_estimate",
    "subagent_lifecycle_frames",
    "context_usage_control_request",
)
# The describe controls/features the drill can observe (implemented != qualified).
MATRIX: Final = (
    "start",
    "send_turn",
    "observe",
    "cancel_turn",
    "reattach",
    "usage",
    "end_session",
    "approval_suspension",
    "subordinate_lineage",
    "context_occupancy",
)


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def budget_usd() -> float:
    try:
        value = float(os.environ.get("MC_PAID_BUDGET_USD", "") or "0")
    except ValueError:
        return 0.0
    return value if 0 < value < float("inf") else 0.0


def auth_route() -> str | None:
    route = os.environ.get("MC_CLAUDE_AUTH_ROUTE", "")
    return route if route in ROUTES else None


def missing_preconditions(environ: Mapping[str, str] | None = None) -> list[str]:
    """What the paid drill still needs (names only; values are never read into the list)."""

    env = os.environ if environ is None else environ
    missing: list[str] = []
    if env.get("MC_LIVE_CLAUDE_QUALIFICATION") != "1":
        missing.append("MC_LIVE_CLAUDE_QUALIFICATION=1")
    try:
        budget = float(env.get("MC_PAID_BUDGET_USD", "") or "0")
    except ValueError:
        budget = 0.0
    if not 0 < budget < float("inf"):
        missing.append("MC_PAID_BUDGET_USD (a finite, owner-approved amount)")
    route = env.get("MC_CLAUDE_AUTH_ROUTE", "")
    if route not in ROUTES:
        missing.append("MC_CLAUDE_AUTH_ROUTE (api_key or owner_cli_login; no default)")
    if route == "api_key" and not env.get("ANTHROPIC_API_KEY"):
        missing.append("ANTHROPIC_API_KEY (api_key route)")
    if route == "owner_cli_login" and not env.get("MC_CLAUDE_OWNER_ATTESTATION_REF"):
        missing.append("MC_CLAUDE_OWNER_ATTESTATION_REF (owner_cli_login is policy_restricted)")
    if not env.get("MISSION_CONTROL_AUTH_PROFILES_PATH"):
        missing.append("MISSION_CONTROL_AUTH_PROFILES_PATH (the owner's auth profile document)")
    if not env.get("MC_CLAUDE_AUTH_PROFILE"):
        missing.append("MC_CLAUDE_AUTH_PROFILE (the profile id to admit)")
    gate = host_gate()
    if not gate.supported:
        missing.append(f"a Linux/WSL 2/macOS worker host ({gate.code})")
    return missing


def live_enabled() -> bool:
    return not missing_preconditions()


def secrets() -> tuple[str, ...]:
    return tuple(value for name in SECRET_ENV if (value := os.environ.get(name)))


@dataclass
class Spend:
    """Paid units the drill caused. Cost is the SDK's own client-side estimate
    (`ResultMessage.total_cost_usd`), never a settled bill; an ambiguous effect is unknown."""

    budget_usd: float
    sessions: list[str] = field(default_factory=list)
    turns: list[str] = field(default_factory=list)
    estimated_cost_usd: float = 0.0
    unknown: list[str] = field(default_factory=list)

    def observe_result(self, message: Mapping[str, Any]) -> None:
        cost = message.get("total_cost_usd")
        if isinstance(cost, int | float):
            self.estimated_cost_usd += float(cost)

    @property
    def exhausted(self) -> bool:
        return self.estimated_cost_usd >= self.budget_usd

    def as_record(self) -> dict[str, Any]:
        return {
            "budget_usd": self.budget_usd,
            "spent_units": {"sessions": len(self.sessions), "turns": len(self.turns)},
            "estimated_cost_usd": round(self.estimated_cost_usd, 6),
            "settled_cost": "unknown (the SDK reports an estimate; compare with the Console)",
            "unknown_units": list(self.unknown),
        }


@dataclass
class RecordingClientFactory:
    """The production `SdkClientFactory` with a tee on each client's raw stream: every CLI
    stream message (not control traffic) is kept for the scrubbed recording."""

    inner: SdkClientFactory
    spend: Spend
    auth_route: str
    recordings: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    clients: list[ClaudeClient] = field(default_factory=list)

    @property
    def versions(self) -> Mapping[str, str]:
        return self.inner.versions

    def create(
        self, options: ClaudeAgentOptions, *, environment: Mapping[str, str]
    ) -> ClaudeClient:
        client = self.inner.create(options, environment=environment)
        name = f"session-{len(self.clients) + 1}" + ("-resumed" if options.resume else "")
        records: list[dict[str, Any]] = [
            {
                "_fixture": f"claude_agent_sdk/live/{name}",
                "recorded": True,
                "note": "LIVE recording by the MP-07 drill (scrubbed of secrets and e-mails)",
                "sdk": f"claude-agent-sdk=={SDK_VERSION}",
                "cli_bundled": BUNDLED_CLI_VERSION,
                "auth_route": self.auth_route,
                "host": f"{platform.system()} {platform.release()}",
            }
        ]
        self.recordings[name] = records
        transport = getattr(client, "_custom_transport", None)
        if transport is not None:
            original = transport.read_messages

            def tee() -> AsyncIterator[dict[str, Any]]:
                async def stream() -> AsyncIterator[dict[str, Any]]:
                    async for item in original():
                        kind = str(item.get("type", ""))
                        if not kind.startswith("control"):
                            records.append({"message": dict(item)})
                            if kind == "system" and item.get("subtype") == "init":
                                self.spend.sessions.append(str(item.get("session_id")))
                            if kind == "result":
                                self.spend.observe_result(item)
                        yield item

                return stream()

            transport.read_messages = tee
        else:  # pragma: no cover - the production factory always builds a custom transport
            self.spend.unknown.append(f"{name}: no transport to record")
        self.clients.append(client)
        return client


def write_recording(name: str, records: Sequence[Mapping[str, Any]]) -> Path:
    target = RECORDINGS / _today() / f"{name}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    cleaned = scrub(list(records), secrets=secrets())
    target.write_bytes("".join(json.dumps(item) + "\n" for item in cleaned).encode("utf-8"))
    return target


@dataclass
class DrillOutcome:
    """What the drill observed: checks, the capability matrix, measurements, settlements."""

    spend: Spend
    auth: dict[str, str] = field(default_factory=dict)
    checks: dict[str, bool] = field(default_factory=dict)
    matrix: dict[str, tuple[str, str]] = field(default_factory=dict)
    measurements: dict[str, Any] = field(default_factory=dict)
    unverified: dict[str, tuple[str, str]] = field(default_factory=dict)
    recordings: list[str] = field(default_factory=list)
    approval_url: str | None = None

    @property
    def qualified(self) -> bool:
        return bool(self.checks) and all(self.checks.values()) and not self.spend.unknown

    def observe(self, control: str, status: str, evidence: str) -> None:
        if control not in MATRIX:
            raise ValueError(f"not a drill-observable control: {control}")
        if status not in {"observed", "refuted", "not_exercised"}:
            raise ValueError("status is observed, refuted or not_exercised")
        self.matrix[control] = (status, evidence)

    def settle(self, item: str, status: str, evidence: str) -> None:
        if item not in UNVERIFIED:
            raise ValueError(f"not an UNVERIFIED item: {item}")
        if status not in {"verified", "refuted", "open"}:
            raise ValueError("status is verified, refuted or open")
        self.unverified[item] = (status, evidence)


def write_record(outcome: DrillOutcome, *, root: Path = RECORDS) -> Path:
    """`docs/qualification/lanes/claude_agent_sdk-<date>.md`: the live drill's record."""

    today = _today()
    target = root / f"{PROFILE}-{today}.md"
    spend = outcome.spend.as_record()
    lines = [
        "---",
        "type: Qualification Record",
        f'title: "Lane qualification: {PROFILE} {today}"',
        (
            f'description: "Live drill of the {PROFILE} lane profile (MP-07): auth route, '
            'observed capability matrix, checks, estimated paid units and settled items."'
        ),
        "tags: [mission-control, qualification, lanes, claude]",
        f"profile: {PROFILE}",
        f"date: {today}",
        f"outcome: {'qualified' if outcome.qualified else 'not_qualified'}",
        "evidence: live_drill",
        f"auth_route: {outcome.auth.get('route', 'unknown')}",
        f"budget_usd: {spend['budget_usd']}",
        f"approval: {outcome.approval_url or 'none recorded'}",
        "---",
        "",
        f"# Lane qualification: {PROFILE} ({today})",
        "",
        f"Recorded at {datetime.now(UTC).isoformat()} by `make lane-qualify PROFILE={PROFILE} "
        f"LIVE=1` on {platform.system()} {platform.release()} (`claude-agent-sdk=={SDK_VERSION}`, "
        f"bundled Claude Code {BUNDLED_CLI_VERSION}).",
        "",
        "## Auth route (names and references only; no credential value)",
        "",
        "| field | value |",
        "| --- | --- |",
        *(f"| {name} | {value} |" for name, value in sorted(outcome.auth.items())),
        "",
        "## Observed capability matrix",
        "",
        "| control / feature | observed | evidence |",
        "| --- | --- | --- |",
        *(
            f"| {item} | {outcome.matrix.get(item, ('not_exercised', ''))[0]} | "
            f"{outcome.matrix.get(item, ('not_exercised', 'not exercised'))[1]} |"
            for item in MATRIX
        ),
        "",
        "## Checks",
        "",
        "| check | passed |",
        "| --- | --- |",
        *(f"| {name} | {'yes' if ok else 'NO'} |" for name, ok in sorted(outcome.checks.items())),
        "",
        "## Measurements",
        "",
        "| measurement | value |",
        "| --- | --- |",
        *(f"| {name} | {value} |" for name, value in sorted(outcome.measurements.items())),
        "",
        "## Paid units",
        "",
        f"- spent: {spend['spent_units']}",
        f"- estimated cost (USD, the SDK's own estimate): {spend['estimated_cost_usd']}",
        f"- settled cost: {spend['settled_cost']}",
        f"- unknown: {spend['unknown_units'] or 'none'}",
        "",
        "## UNVERIFIED items (docs/qualification/lanes/claude_agent_sdk/README.md)",
        "",
        "| item | status | evidence |",
        "| --- | --- | --- |",
        *(
            f"| {item} | {outcome.unverified.get(item, ('open', 'not exercised'))[0]} | "
            f"{outcome.unverified.get(item, ('open', 'not exercised'))[1]} |"
            for item in UNVERIFIED
        ),
        "",
        "## Recordings",
        "",
        *(f"- `{path}`" for path in outcome.recordings),
        "",
        "`qualified` flips only through a reviewed release that cites this record "
        "(`describe.py` `qualified=True`, a migration row with `qualification_ref`).",
        "",
    ]
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes("\n".join(lines).encode("utf-8"))
    return target


class Stopwatch:
    def __init__(self) -> None:
        self._start = time.perf_counter()

    def seconds(self) -> float:
        return round(time.perf_counter() - self._start, 3)


__all__ = [
    "MATRIX",
    "RECORDINGS",
    "RECORDS",
    "ROUTES",
    "UNVERIFIED",
    "DrillOutcome",
    "RecordingClientFactory",
    "Spend",
    "Stopwatch",
    "auth_route",
    "budget_usd",
    "live_enabled",
    "missing_preconditions",
    "secrets",
    "write_record",
    "write_recording",
]
