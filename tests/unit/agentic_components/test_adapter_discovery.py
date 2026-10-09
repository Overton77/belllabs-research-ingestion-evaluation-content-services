"""MP-03: the recorded Claude Code discovery FIXTURE proves the projected skill, MCP server,
subagent and SessionStart hook load in one exact local CLI build.

Re-record with ``uv run --frozen python -m tests.fixtures.projections.discovery.claude_cli``
(no provider call; see that module). The recording is bound to the projection digest, so any
renderer change that alters the claude_agent_sdk bytes makes this test demand a re-record
instead of silently trusting stale evidence. It proves local discovery only; it qualifies
nothing and says nothing about hosted Claude (Outcome 3).
"""

from __future__ import annotations

import json

from mission_control.application.agentic_components.materialization import projection_digest
from mission_control.application.environments import BootstrapRoute, route_evidence
from mission_control.domain.capabilities.host_support import LaneProfile
from tests.fixtures.projections.discovery import claude_cli

RECORDING = json.loads(claude_cli.RECORDING.read_text(encoding="utf-8"))


def test_recording_is_labelled_and_made_without_a_provider_call() -> None:
    assert RECORDING["schema_version"] == "mc.discovery_recording.v1"
    assert RECORDING["fixture"] is True
    assert RECORDING["evidence"] == "local_cli_discovery"
    assert RECORDING["lane_profile"] == "claude_agent_sdk"
    assert "127.0.0.1:9" in RECORDING["provider_call"]
    assert RECORDING["cli"]["version"].startswith("2.1.")
    assert "--setting-sources" in RECORDING["invocation"]
    assert RECORDING["invocation"][RECORDING["invocation"].index("--setting-sources") + 1] == (
        "project"
    )


def test_recording_matches_the_current_projection() -> None:
    projection = claude_cli.project()
    assert RECORDING["projection_digest"] == projection_digest(projection), (
        "claude_agent_sdk projection changed: re-record "
        "`uv run --frozen python -m tests.fixtures.projections.discovery.claude_cli`"
    )
    assert RECORDING["projected_paths"] == list(projection.paths())


def test_projected_skill_mcp_subagent_and_hook_were_discovered() -> None:
    discovered = RECORDING["discovered"]
    assert "agent-browser" in discovered["skills"]
    assert "discovery-reviewer" in discovered["agents"]
    assert {"name": "discovery-stub", "status": "connected"} in discovered["mcp_servers"]
    assert "mcp__discovery-stub__discovery_echo" in discovered["mcp_tools"]
    assert RECORDING["hooks_fired"] == ["fired-session_start.json"]


def test_recording_is_secret_and_path_free() -> None:
    text = claude_cli.RECORDING.read_text(encoding="utf-8")
    assert "\r\n" not in text
    for needle in ("Users\\", "/Users/", "/home/", "sk-ant", "placeholder-not-a-credential"):
        assert needle not in text


def test_route_evidence_cites_the_recording_but_stays_unqualified() -> None:
    evidence = route_evidence(LaneProfile.CLAUDE_AGENT_SDK, BootstrapRoute.PRE_SESSION_FILES)
    assert evidence is not None and evidence.status == "unqualified"
    assert any(ref.endswith("claude_agent_sdk.recorded.json") for ref in evidence.evidence_refs)
