"""FT-G6 live qualification drill helpers (owner-run, paid; never imported by CI suites).

The drill records real Cursor traffic in the exact shapes the offline fixtures replay
(`fixtures/local/*.jsonl` bridge envelopes, `fixtures/cloud/*.sse` streams plus run records),
scrubbed of the API key, any other bound secret and e-mail addresses, and writes the
qualification record `docs/qualification/lanes/<profile>-<date>.md` that is the only evidence
allowed to flip `lane_profile.qualified` (in a later, reviewed release).

Nothing here runs unless `test_lane_qualification_live.py` is selected with
`MC_LIVE_CURSOR_QUALIFICATION=1`, `CURSOR_API_KEY` and a finite `MC_PAID_BUDGET_USD`
(`make lane-qualify PROFILE=... LIVE=1`). Spent, reserved and unknown units are recorded.
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
from typing import Any

from mission_control.adapters.cursor.bridge import (
    BridgeEvent,
    CursorBridgeLauncher,
    CursorLocalBridge,
    FeatureUnavailable,
    LocalAgentSpec,
    RunState,
    UsageState,
)
from mission_control.adapters.cursor.frames import scrub
from mission_control.adapters.cursor.sse import SseEvent

ROOT = Path(__file__).resolve().parents[3]
RECORDINGS = Path(__file__).resolve().parent / "recordings"
RECORDS = ROOT / "docs" / "qualification" / "lanes"

# SPEC-07 "Qualification fixtures and budget": the UNVERIFIED items the drill settles.
UNVERIFIED = (
    "windows_sandbox",
    "rules_without_setting_sources",
    "run_request_id",
    "concurrent_local_send",
    "cloud_idempotency_window",
    "local_run_git",
    "rest_env_vars_and_metadata",
    "stream_retention",
)


def _today() -> str:
    return datetime.now(UTC).date().isoformat()


def live_enabled(*, cloud: bool = False) -> bool:
    budget = os.environ.get("MC_PAID_BUDGET_USD", "") or "0"
    try:
        finite = 0 < float(budget) < float("inf")
    except ValueError:
        finite = False
    return (
        os.environ.get("MC_LIVE_CURSOR_QUALIFICATION") == "1"
        and bool(os.environ.get("CURSOR_API_KEY"))
        and finite
        and (not cloud or bool(os.environ.get("MC_CURSOR_CLOUD_REPO")))
    )


def repeated_drills_approved() -> str | None:
    """The Linear approval comment URL the repeated latency and busy drills need."""

    url = os.environ.get("MC_LANE_DRILL_APPROVAL_URL", "")
    return url if url.startswith("https://linear.app/") else None


def secrets() -> tuple[str, ...]:
    return tuple(
        value
        for value in (os.environ.get("CURSOR_API_KEY"), os.environ.get("LINEAR_API_KEY"))
        if value
    )


@dataclass
class Spend:
    """Paid units the drill caused: spent (a run that ran), reserved, unknown (ambiguous)."""

    budget_usd: float
    agents_created: list[str] = field(default_factory=list)
    runs_started: list[str] = field(default_factory=list)
    cost_micros_usd: int = 0
    unknown: list[str] = field(default_factory=list)

    def as_record(self) -> dict[str, Any]:
        return {
            "budget_usd": self.budget_usd,
            "spent_units": {"agents": len(self.agents_created), "runs": len(self.runs_started)},
            "settled_cost_micros_usd": self.cost_micros_usd,
            "reserved_units": 0,
            "unknown_units": list(self.unknown),
        }


# --- cursor_local: a recording bridge ------------------------------------------------------------


@dataclass
class RecordingBridge:
    """Proxies the real `SdkLocalBridge` and records the replay-fixture records."""

    inner: CursorLocalBridge
    records: list[dict[str, Any]]
    spend: Spend

    async def create_agent(self, spec: LocalAgentSpec) -> str:
        agent_id = await self.inner.create_agent(spec)
        self.spend.agents_created.append(agent_id)
        self._meta()["agent_id"] = agent_id
        return agent_id

    async def resume_agent(self, agent_id: str, spec: LocalAgentSpec) -> str:
        return await self.inner.resume_agent(agent_id, spec)

    async def send(self, agent_id: str, text: str, *, idempotency_key: str) -> str:
        run_id = await self.inner.send(agent_id, text, idempotency_key=idempotency_key)
        self.spend.runs_started.append(run_id)
        meta = self._meta()
        if "run_id" not in meta:
            meta["run_id"] = run_id
        else:
            meta.setdefault("later_runs", []).append(run_id)
        return run_id

    async def observe(self, run_id: str, *, after_offset: str | None) -> AsyncIterator[BridgeEvent]:
        async for event in self.inner.observe(run_id, after_offset=after_offset):
            record: dict[str, Any] = {
                "kind": "event",
                "offset": event.offset,
                "envelope": dict(event.envelope),
            }
            if run_id != self._meta().get("run_id"):
                record["run"] = run_id
            self.records.append(record)
            yield event

    async def run_state(self, run_id: str) -> RunState:
        state = await self.inner.run_state(run_id)
        record: dict[str, Any] = {
            "kind": "run_state",
            "status": state.status,
            "result": state.result,
            "duration_ms": state.duration_ms,
            "model": state.model,
            "usage": dict(state.usage or {}),
            "git_branches": [dict(item) for item in state.git_branches],
        }
        if run_id != self._meta().get("run_id"):
            record["run"] = run_id
        self.records.append(record)
        return state

    async def cancel(self, run_id: str, *, agent_id: str) -> None:
        await self.inner.cancel(run_id, agent_id=agent_id)

    async def usage(self, agent_id: str) -> UsageState:
        try:
            usage = await self.inner.usage(agent_id)
        except FeatureUnavailable:
            self.records.append({"kind": "usage", "feature_unavailable": True})
            raise
        self.records.append(
            {
                "kind": "usage",
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "total_tokens": usage.total_tokens,
                "cost_micros_usd": usage.cost_micros_usd,
            }
        )
        if usage.cost_micros_usd:
            self.spend.cost_micros_usd += usage.cost_micros_usd
        return usage

    async def close_agent(self, agent_id: str) -> None:
        await self.inner.close_agent(agent_id)

    async def aclose(self) -> None:
        await self.inner.aclose()

    def _meta(self) -> dict[str, Any]:
        return self.records[0]


@dataclass
class RecordingLauncher:
    inner: CursorBridgeLauncher
    spend: Spend
    source: str
    records: list[dict[str, Any]] = field(default_factory=list)
    bridges: list[RecordingBridge] = field(default_factory=list)

    @property
    def versions(self) -> Mapping[str, str]:
        return self.inner.versions

    def sandbox_supported(self) -> bool:
        return self.inner.sandbox_supported()

    async def launch(self, *, workspace: Path, state_root: Path) -> RecordingBridge:
        if not self.records:
            self.records.append(
                {
                    "kind": "meta",
                    "synthetic": False,
                    "recorded": True,
                    "profile": "cursor_local",
                    "sdk": "cursor-sdk==1.0.37",
                    "source": self.source,
                    "host": platform.system(),
                }
            )
        bridge = RecordingBridge(
            await self.inner.launch(workspace=workspace, state_root=state_root),
            self.records,
            self.spend,
        )
        self.bridges.append(bridge)
        return bridge


def write_local_recording(name: str, records: Sequence[Mapping[str, Any]]) -> Path:
    target = RECORDINGS / "local" / _today() / f"{name}.jsonl"
    target.parent.mkdir(parents=True, exist_ok=True)
    cleaned = scrub(list(records), secrets=secrets())
    target.write_bytes("".join(json.dumps(item) + "\n" for item in cleaned).encode("utf-8"))
    return target


def write_cloud_recording(
    name: str, events: Sequence[SseEvent], record: Mapping[str, Any]
) -> tuple[Path, Path]:
    from mission_control.adapters.cursor.sse import render_sse

    folder = RECORDINGS / "cloud" / _today()
    folder.mkdir(parents=True, exist_ok=True)
    stream = folder / f"{name}.sse"
    cleaned_events = [
        SseEvent(
            event=item.event,
            data=json.dumps(scrub(item.json(), secrets=secrets())),
            id=item.id,
        )
        for item in events
    ]
    stream.write_bytes(
        (": recorded live by the FT-G6 drill (scrubbed)\n" + render_sse(cleaned_events)).encode(
            "utf-8"
        )
    )
    run_record = folder / f"{name}.json"
    run_record.write_bytes(
        (json.dumps(scrub(dict(record), secrets=secrets()), indent=2) + "\n").encode("utf-8")
    )
    return stream, run_record


# --- the qualification record ------------------------------------------------------------------


@dataclass
class DrillOutcome:
    profile: str
    spend: Spend
    checks: dict[str, bool] = field(default_factory=dict)
    measurements: dict[str, Any] = field(default_factory=dict)
    unverified: dict[str, tuple[str, str]] = field(default_factory=dict)
    recordings: list[str] = field(default_factory=list)
    approval_url: str | None = None

    @property
    def qualified(self) -> bool:
        return bool(self.checks) and all(self.checks.values()) and not self.spend.unknown

    def settle(self, item: str, status: str, evidence: str) -> None:
        if item not in UNVERIFIED:
            raise ValueError(f"not an UNVERIFIED item: {item}")
        if status not in {"verified", "refuted", "open"}:
            raise ValueError("status is verified, refuted or open")
        self.unverified[item] = (status, evidence)


def write_record(outcome: DrillOutcome, *, root: Path = RECORDS) -> Path:
    """`docs/qualification/lanes/<profile>-<date>.md`: the live drill's record."""

    today = _today()
    target = root / f"{outcome.profile}-{today}.md"
    spend = outcome.spend.as_record()
    lines = [
        "---",
        "type: Qualification Record",
        f'title: "Lane qualification: {outcome.profile} {today}"',
        (
            f'description: "Live drill of the {outcome.profile} lane profile (FT-G6): checks, '
            'measurements, paid units and the UNVERIFIED items it settled."'
        ),
        "tags: [mission-control, qualification, lanes, cursor]",
        f"profile: {outcome.profile}",
        f"date: {today}",
        f"outcome: {'qualified' if outcome.qualified else 'not_qualified'}",
        "evidence: live_drill",
        f"budget_usd: {spend['budget_usd']}",
        f"approval: {outcome.approval_url or 'none (single fixture runs only)'}",
        "---",
        "",
        f"# Lane qualification: {outcome.profile} ({today})",
        "",
        f"Recorded at {datetime.now(UTC).isoformat()} by `make lane-qualify PROFILE="
        f"{outcome.profile} LIVE=1` on {platform.system()} {platform.release()}.",
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
        f"- settled cost (micro USD): {spend['settled_cost_micros_usd']}",
        f"- reserved: {spend['reserved_units']}",
        f"- unknown: {spend['unknown_units'] or 'none'}",
        "",
        "## UNVERIFIED items (SPEC-07 Qualification fixtures and budget)",
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
    "RECORDINGS",
    "RECORDS",
    "UNVERIFIED",
    "DrillOutcome",
    "RecordingLauncher",
    "Spend",
    "Stopwatch",
    "live_enabled",
    "repeated_drills_approved",
    "write_cloud_recording",
    "write_local_recording",
    "write_record",
]
