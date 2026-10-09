"""Per-profile evidence for how mandatory configuration reaches a session before it starts.

A bootstrap route is the mechanism that puts Mission Control's projected configuration
(instructions, skills, MCP servers, hooks, subagents) in front of the provider *before the
agent's first action*. SPEC-02: a setup file in the repository is not proof that a hosted
product loaded it, and a first-prompt instruction asking the agent to install its own controls
never counts as enforcement. Each entry records what the route can inject, its qualification
status and the evidence or missing vendor operations behind that status. Nothing here is
qualified from documentation alone.
"""

from __future__ import annotations

from collections.abc import Mapping
from enum import StrEnum
from typing import Final, Literal

from pydantic import BaseModel, ConfigDict, Field

from mission_control.domain.capabilities.host_support import LaneProfile


class BootstrapRoute(StrEnum):
    # Mission Control writes the Host Projection into its leased workspace before start.
    PRE_SESSION_FILES = "pre_session_files"
    # Deep Agents builds the projection in-process (memory, skills, middleware, subagents).
    IN_PROCESS = "in_process"
    # Projected config committed to an admitted integration branch the hosted product clones.
    INTEGRATION_COMMIT = "integration_commit"
    # The provider's own environment setup/install phase (published environment).
    PROVIDER_ENVIRONMENT_SETUP = "provider_environment_setup"
    # Instructions in the first prompt. Never a bootstrap for mandatory controls.
    FIRST_PROMPT = "first_prompt"


ControlKind = Literal["instructions", "skill", "mcp_server", "hook", "subagent"]
RouteStatus = Literal["qualified", "unqualified", "unsupported"]


class RouteEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    lane_profile: LaneProfile
    route: BootstrapRoute
    status: RouteStatus
    # Control kinds this route can place before the agent starts, if it works as documented.
    injects: frozenset[ControlKind] = frozenset()
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    missing_vendor_operations: tuple[str, ...] = ()
    note: str = Field(min_length=1)


_ALL: Final[frozenset[ControlKind]] = frozenset(
    {"instructions", "skill", "mcp_server", "hook", "subagent"}
)
_CLAUDE_CLOUD_FEASIBILITY: Final = "docs/qualification/lanes/claude_cloud/FEASIBILITY.md"
_CODEX_CLOUD_FEASIBILITY: Final = "docs/qualification/lanes/codex_cloud/FEASIBILITY.md"
_CLAUDE_DISCOVERY: Final = "tests/fixtures/projections/discovery/claude_agent_sdk.recorded.json"
_DESCRIBE: Final = "src/mission_control/application/execution/harness/describe.py"


def _first_prompt(profile: LaneProfile) -> RouteEvidence:
    return RouteEvidence(
        lane_profile=profile,
        route=BootstrapRoute.FIRST_PROMPT,
        status="unsupported",
        evidence_refs=("docs/specs/multi-provider-2026-10/SPEC-02-environments.md",),
        note=(
            "a first-prompt instruction asking the agent to install its own mandatory "
            "controls is not enforcement"
        ),
    )


_ROUTES: Final[tuple[RouteEvidence, ...]] = (
    RouteEvidence(
        lane_profile=LaneProfile.DEEP_AGENTS,
        route=BootstrapRoute.IN_PROCESS,
        status="qualified",
        injects=_ALL,
        evidence_refs=(_DESCRIBE, "tests/fixtures/projections/deep_agents/_projection.json"),
        note="in-process objects built by the qualified Deep Agents lane before the run starts",
    ),
    RouteEvidence(
        lane_profile=LaneProfile.CURSOR_LOCAL,
        route=BootstrapRoute.PRE_SESSION_FILES,
        status="unqualified",
        injects=_ALL,
        evidence_refs=(_DESCRIBE, "tests/fixtures/projections/cursor_local/_projection.json"),
        note="projected files placed by the Cursor lane; the lane describe is unqualified",
    ),
    RouteEvidence(
        lane_profile=LaneProfile.CURSOR_CLOUD,
        route=BootstrapRoute.INTEGRATION_COMMIT,
        status="unqualified",
        injects=_ALL,
        evidence_refs=(_DESCRIBE, "tests/fixtures/projections/cursor_cloud/_projection.json"),
        note=(
            "cloud agents read the repository configuration; session and MCP hooks are skipped "
            "there and no effective-configuration attestation is recorded"
        ),
    ),
    RouteEvidence(
        lane_profile=LaneProfile.CLAUDE_AGENT_SDK,
        route=BootstrapRoute.PRE_SESSION_FILES,
        status="unqualified",
        injects=_ALL,
        evidence_refs=(_CLAUDE_DISCOVERY,),
        note=(
            "local CLI discovery fixture shows the projected skill, MCP server, subagent and "
            "SessionStart hook load in Claude Code 2.1.295; the Python SDK is not pinned and "
            "no live drill ran"
        ),
    ),
    RouteEvidence(
        lane_profile=LaneProfile.CODEX,
        route=BootstrapRoute.PRE_SESSION_FILES,
        status="unqualified",
        injects=_ALL,
        evidence_refs=("tests/fixtures/projections/codex/_projection.json",),
        note=(
            "projected files only; the project .codex/ layer loads for trusted projects, and "
            "no Codex binary was available to record discovery"
        ),
    ),
    RouteEvidence(
        lane_profile=LaneProfile.CLAUDE_CLOUD,
        route=BootstrapRoute.INTEGRATION_COMMIT,
        status="unqualified",
        injects=_ALL,
        evidence_refs=(_CLAUDE_CLOUD_FEASIBILITY,),
        missing_vendor_operations=(
            "per-session configuration injection with an effective-config report",
            "environment list/get with a setup revision or digest",
        ),
        note=(
            "committed .claude/ and .mcp.json reach single-repository sessions only "
            "(documented_partial, not observed); the product reports no effective "
            "configuration, so preparation cannot be attested"
        ),
    ),
    RouteEvidence(
        lane_profile=LaneProfile.CLAUDE_CLOUD,
        route=BootstrapRoute.PROVIDER_ENVIRONMENT_SETUP,
        status="unsupported",
        evidence_refs=(_CLAUDE_CLOUD_FEASIBILITY,),
        missing_vendor_operations=("environment list/get with a setup revision or digest",),
        note="setup scripts and environment variables are edited in the web UI only",
    ),
    RouteEvidence(
        lane_profile=LaneProfile.CODEX_CLOUD,
        route=BootstrapRoute.INTEGRATION_COMMIT,
        status="unqualified",
        injects=frozenset({"instructions", "skill"}),
        evidence_refs=(_CODEX_CLOUD_FEASIBILITY,),
        missing_vendor_operations=(
            "per-task configuration overrides (MCP, hooks, approval, sandbox) or an attestation "
            "of what the task loaded",
        ),
        note=(
            "a task reads repository AGENTS.md and skills only; no per-task MCP, hooks or "
            "approval configuration"
        ),
    ),
    RouteEvidence(
        lane_profile=LaneProfile.CODEX_CLOUD,
        route=BootstrapRoute.PROVIDER_ENVIRONMENT_SETUP,
        status="unqualified",
        injects=frozenset({"instructions", "skill"}),
        evidence_refs=(_CODEX_CLOUD_FEASIBILITY,),
        missing_vendor_operations=(
            "non-interactive environment listing with a revision, publish ID or setup digest",
        ),
        note=(
            "install script and start skill are edited in the UI on the published environment; "
            "no revision or setup digest is exposed, and command hooks do not run under cloud "
            "orchestration"
        ),
    ),
    *(_first_prompt(profile) for profile in LaneProfile),
)

ROUTE_EVIDENCE: Final[Mapping[tuple[LaneProfile, BootstrapRoute], RouteEvidence]] = {
    (item.lane_profile, item.route): item for item in _ROUTES
}

# Profiles whose hosted bootstrap stays Outcome 3 (unqualified) per their feasibility studies;
# `allow_unqualified` never admits them.
OUTCOME_3_PROFILES: Final[frozenset[LaneProfile]] = frozenset(
    {LaneProfile.CLAUDE_CLOUD, LaneProfile.CODEX_CLOUD}
)

if {profile for profile, _ in ROUTE_EVIDENCE} != set(LaneProfile):  # pragma: no cover
    raise RuntimeError("ROUTE_EVIDENCE must cover every LaneProfile")


def route_evidence(profile: LaneProfile | str, route: BootstrapRoute | str) -> RouteEvidence | None:
    return ROUTE_EVIDENCE.get((LaneProfile(profile), BootstrapRoute(route)))


__all__ = [
    "OUTCOME_3_PROFILES",
    "ROUTE_EVIDENCE",
    "BootstrapRoute",
    "ControlKind",
    "RouteEvidence",
    "RouteStatus",
    "route_evidence",
]
