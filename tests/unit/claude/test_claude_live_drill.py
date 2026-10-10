"""MP-07: the paid live drill's gate, recorder and record writer, offline (nothing is called).

The drill itself (`tests/integration/claude/test_claude_live_qualification.py`) is owner-run
and was not executed; these tests prove it refuses without every explicit precondition, that
its recordings drop control traffic and secrets, and that the record it writes is what the
qualification gate reads (`outcome`, `evidence: live_drill`).
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from mission_control.adapters.claude.host import host_gate
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from tests.integration.claude import live_drill
from tests.integration.claude.live_drill import (
    MATRIX,
    UNVERIFIED,
    DrillOutcome,
    RecordingClientFactory,
    Spend,
    missing_preconditions,
    write_record,
)
from tests.unit.claude.fixtures import PROFILE, fixture_admission

COMPLETE = {
    "MC_LIVE_CLAUDE_QUALIFICATION": "1",
    "MC_PAID_BUDGET_USD": "2.5",
    "MC_CLAUDE_AUTH_ROUTE": "api_key",
    "ANTHROPIC_API_KEY": "sk-ant-FIXTURE-not-a-key",
    "MISSION_CONTROL_AUTH_PROFILES_PATH": "/owner/profiles.json",
    "MC_CLAUDE_AUTH_PROFILE": "claude-sdk-api-key",
}


def _without_host(missing: list[str]) -> list[str]:
    return [item for item in missing if not item.startswith("a Linux/WSL 2/macOS")]


def test_the_drill_refuses_without_every_explicit_precondition() -> None:
    missing = _without_host(missing_preconditions({}))
    assert missing == [
        "MC_LIVE_CLAUDE_QUALIFICATION=1",
        "MC_PAID_BUDGET_USD (a finite, owner-approved amount)",
        "MC_CLAUDE_AUTH_ROUTE (api_key or owner_cli_login; no default)",
        "MISSION_CONTROL_AUTH_PROFILES_PATH (the owner's auth profile document)",
        "MC_CLAUDE_AUTH_PROFILE (the profile id to admit)",
    ]
    assert _without_host(missing_preconditions(COMPLETE)) == []
    for budget in ("inf", "0", "-1", "nan", "two"):
        assert "MC_PAID_BUDGET_USD (a finite, owner-approved amount)" in missing_preconditions(
            {**COMPLETE, "MC_PAID_BUDGET_USD": budget}
        )
    no_key = {key: value for key, value in COMPLETE.items() if key != "ANTHROPIC_API_KEY"}
    assert "ANTHROPIC_API_KEY (api_key route)" in missing_preconditions(no_key)
    login = {**no_key, "MC_CLAUDE_AUTH_ROUTE": "owner_cli_login"}
    assert (
        "MC_CLAUDE_OWNER_ATTESTATION_REF (owner_cli_login is policy_restricted)"
        in missing_preconditions(login)
    )
    attested = {**login, "MC_CLAUDE_OWNER_ATTESTATION_REF": "decision:owner-approval-1"}
    assert _without_host(missing_preconditions(attested)) == []
    assert "MC_CLAUDE_AUTH_ROUTE (api_key or owner_cli_login; no default)" in (
        missing_preconditions({**COMPLETE, "MC_CLAUDE_AUTH_ROUTE": "oauth_token_env"})
    )
    for item in missing_preconditions({**COMPLETE, "ANTHROPIC_API_KEY": "sk-ant-secret-value"}):
        assert "sk-ant-secret-value" not in item, "names only, never a value"


def test_the_host_gate_is_part_of_the_preconditions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        live_drill,
        "host_gate",
        lambda: host_gate(system="Windows", release="11", event_loop="SelectorEventLoop"),
    )
    assert missing_preconditions(COMPLETE) == [
        "a Linux/WSL 2/macOS worker host (LANE_UNSUPPORTED_OS)"
    ]


def test_live_enabled_is_false_without_the_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in COMPLETE.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("MC_LIVE_CLAUDE_QUALIFICATION", raising=False)
    assert not live_drill.live_enabled(), "never without the explicit opt-in flag"


class _Transport:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items

    def read_messages(self) -> AsyncIterator[dict[str, Any]]:
        async def stream() -> AsyncIterator[dict[str, Any]]:
            for item in self.items:
                yield item

        return stream()


class _Client:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self._custom_transport = _Transport(items)


class _Inner:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items

    @property
    def versions(self) -> dict[str, str]:
        return {"claude_agent_sdk": "0.2.165"}

    def create(self, options: Any, *, environment: Any) -> _Client:
        del options, environment
        return _Client(self.items)


async def test_the_recorder_keeps_stream_messages_and_spend_never_control_traffic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from claude_agent_sdk.types import ClaudeAgentOptions

    secret = "sk-ant-FIXTURE-recorded-secret"
    monkeypatch.setenv("ANTHROPIC_API_KEY", secret)
    monkeypatch.setattr(live_drill, "RECORDINGS", tmp_path / "recordings")
    items = [
        {"type": "control_request", "request_id": "req-1", "request": {"subtype": "can_use_tool"}},
        {"type": "system", "subtype": "init", "session_id": "sess-live-1", "uuid": "u-1"},
        {
            "type": "assistant",
            "uuid": "a-1",
            "session_id": "sess-live-1",
            "message": {"content": [{"type": "text", "text": f"owner@example.com {secret}"}]},
        },
        {
            "type": "result",
            "subtype": "success",
            "session_id": "sess-live-1",
            "total_cost_usd": 0.4,
        },
    ]
    spend = Spend(budget_usd=0.5)
    factory = RecordingClientFactory(_Inner(items), spend, "api_key")  # type: ignore[arg-type]
    client = factory.create(ClaudeAgentOptions(), environment={})
    seen = [item async for item in client._custom_transport.read_messages()]  # type: ignore[attr-defined]
    assert seen == items, "the tee passes every message through unchanged"
    (records,) = factory.recordings.values()
    assert records[0]["recorded"] is True and records[0]["auth_route"] == "api_key"
    kinds = [record["message"]["type"] for record in records[1:]]
    assert kinds == ["system", "assistant", "result"], "control traffic is not recorded"
    assert spend.sessions == ["sess-live-1"] and spend.estimated_cost_usd == pytest.approx(0.4)
    assert not spend.exhausted
    spend.observe_result({"total_cost_usd": 0.2})
    assert spend.exhausted, "the drill stops before the next step once the budget is reached"
    path = live_drill.write_recording("session-1", records)
    text = path.read_text(encoding="utf-8")
    assert secret not in text and "owner@example.com" not in text


def _front_matter(path: Path) -> dict[str, str]:
    header = path.read_text("utf-8").split("---", 2)[1]
    values = dict(line.split(":", 1) for line in header.strip().splitlines() if ":" in line)
    return {key.strip(): value.strip().strip('"') for key, value in values.items()}


def test_the_record_is_what_the_qualification_gate_reads(tmp_path: Path) -> None:
    passed = DrillOutcome(Spend(budget_usd=2.0))
    passed.auth = {"route": "api_key", "credential_ref": "env:ANTHROPIC_API_KEY"}
    passed.checks["full_turn_settles_from_closing_facts"] = True
    passed.observe("start", "observed", "system/init")
    passed.settle("interrupt_terminal_reason", "verified", "aborted_streaming")
    path = write_record(passed, root=tmp_path)
    assert path.name.startswith(f"{PROFILE}-")
    header = _front_matter(path)
    assert header["outcome"] == "qualified" and header["evidence"] == "live_drill"
    assert header["auth_route"] == "api_key" and header["profile"] == PROFILE
    text = path.read_text("utf-8")
    assert all(f"| {item} |" in text for item in MATRIX)
    assert all(f"| {item} |" in text for item in UNVERIFIED)
    assert "| start | observed | system/init |" in text
    ambiguous = DrillOutcome(Spend(budget_usd=2.0, unknown=["session?"]))
    ambiguous.checks["full_turn_settles_from_closing_facts"] = True
    assert _front_matter(write_record(ambiguous, root=tmp_path / "b"))["outcome"] == (
        "not_qualified"
    ), "an ambiguous paid effect never qualifies"
    assert _front_matter(write_record(DrillOutcome(Spend(2.0)), root=tmp_path / "c"))[
        "outcome"
    ] == ("not_qualified"), "no check, no qualification"
    with pytest.raises(ValueError):
        passed.settle("not-an-item", "verified", "")
    with pytest.raises(ValueError):
        passed.observe("pause", "observed", "")


def test_the_drill_operations_are_distinct_claude_units_with_bash_not_preallowed() -> None:
    from tests.integration.claude.test_claude_live_qualification import _operation

    admission = fixture_admission(route="api_key")
    turn = _operation(admission, key="turn")
    interrupt = _operation(admission, key="interrupt")
    assert turn.provider_binding is not None and turn.execution_runtime == "claude"
    options = turn.provider_binding.provider_options
    assert options.model_dump()["allowed_tools"] == ("Read", "Task")
    assert turn.provider_binding.auth.profile == admission.profile_id
    ids = {
        LaneExecutionIdentity.of(operation, PROFILE, 1).harness_execution_id
        for operation in (turn, interrupt)
    }
    assert len(ids) == 2
    assert json.loads(json.dumps(turn.model_dump(mode="json")))["lane_profile"] == PROFILE
