"""MP-08 live qualification drill helpers for the `codex` lane (owner-run, paid; never in CI).

Nothing here runs unless `test_codex_live_qualification.py` is selected with every
precondition of `missing_preconditions()`: `MC_LIVE_CODEX_QUALIFICATION=1`, a finite
owner-approved `MC_PAID_BUDGET_USD`, an explicit auth route (`MC_CODEX_AUTH_ROUTE=api_key|
owner_cli_login`) admitted by MP-05 from the owner's profile document
(`MISSION_CONTROL_AUTH_PROFILES_PATH`, `MC_CODEX_AUTH_PROFILE`), and a Linux / WSL worker
whose `codex --version` is the pinned `codex-cli 0.162.0` (the lane's launcher refuses any
other; `MC_CODEX_BINARY` names the binary when it is not `codex` on PATH).

Credentials: the drill never reads, copies or writes a credential. On `api_key` the app-server
child gets `OPENAI_API_KEY` through the admitted child environment; on `owner_cli_login` the
lane uses the owner's `CODEX_HOME` (`MC_CODEX_OWNER_HOME`, `owner` home mode) where the CLI's
own login lives, and copies nothing out of it.

The drill records every app-server notification and server request it received (the shapes
`tests/fixtures/provider_frames/codex/scripts/*.jsonl` model), scrubbed of every bound secret
value and e-mail address, and writes `docs/qualification/lanes/codex-<date>.md` with the
observed capability matrix: the only evidence allowed to flip `qualified` (in a later,
reviewed release). Codex reports no cost: spend is recorded as turns and estimated tokens,
with the paid amount `unknown` on a subscription route.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from mission_control.adapters.codex.launcher import (
    LaunchedAppServer,
    LaunchSpec,
    SubprocessAppServerLauncher,
)
from mission_control.adapters.codex.protocol import PINNED_CODEX_CLI_VERSION
from mission_control.adapters.cursor.frames import scrub
from mission_control.adapters.cursor.workspace import git

ROOT = Path(__file__).resolve().parents[3]
RECORDINGS = Path(__file__).resolve().parent / "recordings"
RECORDS = ROOT / "docs" / "qualification" / "lanes"
PROFILE: Final = "codex"
ROUTES: Final = ("api_key", "owner_cli_login")
SECRET_ENV: Final = ("OPENAI_API_KEY", "CODEX_API_KEY")
# The drill's hard caps (README "one thread, at most four turns"): never more paid turns.
MAX_TURNS: Final = 4

# docs/qualification/lanes/codex/README.md "UNVERIFIED items the drill settles".
UNVERIFIED: Final = (
    "project_trust",
    "client_user_message_id_echo",
    "limit_error_data",
    "history_after_restart",
    "steer_refusal_shape",
    "collab_agent_tool_call_fields",
    "approval_rpc_coverage",
    "compaction_report",
    "occupancy_reading",
)

# The capability matrix the record carries: what the drill observed per control.
MATRIX: Final = (
    "start",
    "send_turn",
    "observe",
    "approval_suspension",
    "subordinate_visibility",
    "steer",
    "cancel_turn",
    "compaction",
    "context_occupancy",
    "reattach",
    "reconcile_dispatch",
    "usage",
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
    route = os.environ.get("MC_CODEX_AUTH_ROUTE", "")
    return route if route in ROUTES else None


def codex_binary() -> str:
    return os.environ.get("MC_CODEX_BINARY", "") or "codex"


def missing_preconditions(environ: Mapping[str, str] | None = None) -> list[str]:
    """What the paid drill still needs (names only; values are never read into the list)."""

    env = os.environ if environ is None else environ
    missing: list[str] = []
    if env.get("MC_LIVE_CODEX_QUALIFICATION") != "1":
        missing.append("MC_LIVE_CODEX_QUALIFICATION=1")
    try:
        budget = float(env.get("MC_PAID_BUDGET_USD", "") or "0")
    except ValueError:
        budget = 0.0
    if not 0 < budget < float("inf"):
        missing.append("MC_PAID_BUDGET_USD (a finite, owner-approved amount)")
    route = env.get("MC_CODEX_AUTH_ROUTE", "")
    if route not in ROUTES:
        missing.append("MC_CODEX_AUTH_ROUTE (api_key or owner_cli_login; no default)")
    if route == "api_key" and not env.get("OPENAI_API_KEY"):
        missing.append("OPENAI_API_KEY (api_key route)")
    if route == "owner_cli_login" and not env.get("MC_CODEX_OWNER_HOME"):
        missing.append("MC_CODEX_OWNER_HOME (the owner's CODEX_HOME with its `codex login`)")
    if not env.get("MISSION_CONTROL_AUTH_PROFILES_PATH"):
        missing.append("MISSION_CONTROL_AUTH_PROFILES_PATH (the owner's auth profile document)")
    if not env.get("MC_CODEX_AUTH_PROFILE"):
        missing.append("MC_CODEX_AUTH_PROFILE (the profile id to admit)")
    if sys.platform == "win32":
        missing.append("a Linux/WSL worker host (the lane spawns `codex app-server`)")
    binary = env.get("MC_CODEX_BINARY", "") or "codex"
    if shutil.which(binary) is None:
        missing.append(f"`{binary}` on PATH (codex-cli {PINNED_CODEX_CLI_VERSION})")
    if shutil.which("git") is None:
        missing.append("git on PATH (the disposable repository and its worktree leases)")
    return missing


def live_enabled() -> bool:
    return not missing_preconditions()


def secrets() -> tuple[str, ...]:
    return tuple(value for name in SECRET_ENV if (value := os.environ.get(name)))


def disposable_repository(root: Path) -> Path:
    """One small file and one test command (README precondition 3); no AGENTS.md (the
    Host Projection writes it)."""

    root.mkdir(parents=True, exist_ok=True)
    git("init", "--quiet", "--initial-branch=main", cwd=root)
    (root / "README.md").write_text("# codex drill target\n", encoding="utf-8")
    (root / "check.sh").write_text("#!/bin/sh\ntest -f README.md\n", encoding="utf-8")
    git("add", "--all", cwd=root)
    git(
        "-c",
        "user.name=Drill",
        "-c",
        "user.email=drill@localhost",
        "commit",
        "--quiet",
        "-m",
        "base",
        cwd=root,
    )
    return root


@dataclass
class Spend:
    """Paid units the drill caused. Codex reports no cost: turns and estimated tokens are
    recorded; the paid amount stays `unknown` unless the owner's account shows it."""

    budget_usd: float
    turns_started: list[str] = field(default_factory=list)
    steers: int = 0
    compactions: int = 0
    estimated_tokens: int = 0
    unknown: list[str] = field(default_factory=list)

    @property
    def exhausted(self) -> bool:
        return len(self.turns_started) >= MAX_TURNS

    def as_record(self) -> dict[str, Any]:
        return {
            "budget_usd": self.budget_usd,
            "spent_units": {
                "turns": len(self.turns_started),
                "steers": self.steers,
                "compactions": self.compactions,
            },
            "estimated_tokens": self.estimated_tokens,
            "paid_usd": "unknown (Codex reports no cost; see the account)",
            "unknown_units": list(self.unknown),
        }


@dataclass
class RecordingLauncher:
    """The production `SubprocessAppServerLauncher` (pin enforced), keeping every launch so
    the drill can record what each app-server connection delivered."""

    inner: SubprocessAppServerLauncher
    launches: list[LaunchedAppServer] = field(default_factory=list)

    @property
    def versions(self) -> Mapping[str, str]:
        return self.inner.versions

    async def launch(self, spec: LaunchSpec) -> LaunchedAppServer:
        launched = await self.inner.launch(spec)
        self.launches.append(launched)
        return launched

    def records(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = [
            {
                "kind": "meta",
                "recorded": True,
                "synthetic": False,
                "profile": PROFILE,
                "cli": f"codex-cli {self.inner.versions.get('codex_cli')}",
                "pinned": PINNED_CODEX_CLI_VERSION,
                "host": f"{platform.system()} {platform.release()}",
            }
        ]
        for index, launched in enumerate(self.launches):
            for event in launched.connection.stats.events:
                rows.append(
                    {
                        "kind": event.kind,
                        "launch": index,
                        "epoch": launched.connection.epoch,
                        "seq": event.seq,
                        "method": event.method,
                        "request_id": event.request_id,
                        "params": event.params,
                    }
                )
        return rows


def write_recording(name: str, records: Sequence[Mapping[str, Any]]) -> Path:
    target = RECORDINGS / _today() / f"{name}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    cleaned = scrub(list(records), secrets=secrets())
    target.write_bytes("".join(json.dumps(item) + "\n" for item in cleaned).encode("utf-8"))
    return target


@dataclass
class DrillOutcome:
    spend: Spend
    auth: dict[str, str] = field(default_factory=dict)
    checks: dict[str, bool] = field(default_factory=dict)
    measurements: dict[str, Any] = field(default_factory=dict)
    matrix: dict[str, tuple[str, str]] = field(default_factory=dict)
    unverified: dict[str, tuple[str, str]] = field(default_factory=dict)
    recordings: list[str] = field(default_factory=list)

    @property
    def qualified(self) -> bool:
        return bool(self.checks) and all(self.checks.values()) and not self.spend.unknown

    def observe(self, control: str, status: str, evidence: str) -> None:
        if control not in MATRIX:
            raise ValueError(f"not a matrix control: {control}")
        if status not in {"native", "emulated", "unsupported", "failed", "not_exercised"}:
            raise ValueError("status is native, emulated, unsupported, failed or not_exercised")
        self.matrix[control] = (status, evidence[:300])

    def settle(self, item: str, status: str, evidence: str) -> None:
        if item not in UNVERIFIED:
            raise ValueError(f"not an UNVERIFIED item: {item}")
        if status not in {"verified", "refuted", "open"}:
            raise ValueError("status is verified, refuted or open")
        self.unverified[item] = (status, evidence[:300])


def write_record(outcome: DrillOutcome, *, root: Path = RECORDS) -> Path:
    """`docs/qualification/lanes/codex-<date>.md`: the live drill's record."""

    today = _today()
    target = root / f"{PROFILE}-{today}.md"
    spend = outcome.spend.as_record()
    lines = [
        "---",
        "type: Qualification Record",
        f'title: "Lane qualification: {PROFILE} {today}"',
        (
            f'description: "Live drill of the {PROFILE} lane profile (MP-08): checks, the observed '
            'capability matrix, spend and the UNVERIFIED items it settled."'
        ),
        "tags: [mission-control, qualification, lanes, codex]",
        f"profile: {PROFILE}",
        f"date: {today}",
        f"outcome: {'qualified' if outcome.qualified else 'not_qualified'}",
        "evidence: live_drill",
        f"pin: codex-cli {PINNED_CODEX_CLI_VERSION}",
        f"auth_route: {outcome.auth.get('route', 'unknown')}",
        f"budget_usd: {spend['budget_usd']}",
        "---",
        "",
        f"# Lane qualification: {PROFILE} ({today})",
        "",
        f"Recorded at {datetime.now(UTC).isoformat()} by `make lane-qualify PROFILE={PROFILE} "
        f"LIVE=1` on {platform.system()} {platform.release()}.",
        "",
        "## Auth admission (MP-05)",
        "",
        "| field | value |",
        "| --- | --- |",
        *(f"| {name} | {value} |" for name, value in sorted(outcome.auth.items())),
        "",
        "## Observed capability matrix",
        "",
        "| control | observed | evidence |",
        "| --- | --- | --- |",
        *(
            f"| {control} | {outcome.matrix.get(control, ('not_exercised', ''))[0]} | "
            f"{outcome.matrix.get(control, ('not_exercised', 'not exercised'))[1]} |"
            for control in MATRIX
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
        "## Spend",
        "",
        f"- spent: {spend['spent_units']}",
        f"- estimated tokens: {spend['estimated_tokens']}",
        f"- paid: {spend['paid_usd']}",
        f"- unknown: {spend['unknown_units'] or 'none'}",
        "",
        "## UNVERIFIED items (docs/qualification/lanes/codex/README.md)",
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
    "MAX_TURNS",
    "RECORDINGS",
    "RECORDS",
    "UNVERIFIED",
    "DrillOutcome",
    "RecordingLauncher",
    "Spend",
    "Stopwatch",
    "auth_route",
    "budget_usd",
    "codex_binary",
    "disposable_repository",
    "live_enabled",
    "missing_preconditions",
    "secrets",
    "write_record",
    "write_recording",
]
