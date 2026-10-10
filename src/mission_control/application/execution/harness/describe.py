"""The seven lane profiles' declared describe matrices (00-ARCHITECTURE section 6, SPEC-07
section 2; multi-provider SPEC-01 for the `.v2` profiles).

These are the declared matrices each lane implementation is held to by the conformance and
describe-honesty tests. The three FT-G1 profiles keep their `mc.lane_describe.v1` shape and
digests. The four MP-01 profiles (`claude_agent_sdk`, `codex`, `claude_cloud`, `codex_cloud`)
are `mc.lane_describe.v2` **stubs**: every control and feature is `unqualified`, so their
delivery semantics and identity maps state design intent (from RESEARCH.md and the hosted
feasibility studies), not proof. A lane flips cells to `native`/`emulated` only with its
harness (MP-04/05/18/19) and to `qualified` only with a recorded drill (SPEC-07 section 12).
"""

from __future__ import annotations

from typing import Final

from mission_control.domain.execution.lanes import (
    LANE_CONTROLS,
    LANE_DESCRIBE_SCHEMA_V2,
    LANE_FEATURES,
    ControlSupport,
    FeatureEvidence,
    LaneDescribe,
    LaneHooks,
    LaneIdentityMap,
    LaneProfileName,
    LaneSubagents,
    LaneUsage,
    unqualified_features,
)

_CURSOR_VERSIONS: Final = {"cursor_sdk": "1.0.37", "bridge": "1.0.37", "protocol": "sdk.v1"}
_CURSOR_LOCAL_HOOKS: Final = (
    "session_start",
    "before_tool",
    "after_tool",
    "after_tool_failure",
    "before_shell",
    "after_shell",
    "after_file_edit",
    "before_prompt",
    "before_compaction",
    "subagent_start",
    "subagent_stop",
    "stop",
    "session_end",
)

DEEP_AGENTS_DESCRIBE: Final = LaneDescribe(
    lane="deep_agents",
    lane_profile="deep_agents",
    versions={"deepagents": "0.7.5", "langgraph": "1.2.10"},
    controls={
        "prepare": "native",
        "start": "native",
        "reattach": "native",
        "send_turn": "native",
        "cancel_turn": "native",
        "observe": "native",
        "snapshot": "emulated",
        "usage": "native",
        "end_session": "native",
        "pause": "native",
        "fork": "emulated",
    },
    delivery_semantics={
        "queue_instruction": "turn_boundary_guaranteed",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "pause_at_tool_gate",
        "hard_pause": "unsupported",
        "resume": "turn_boundary_guaranteed",
        "cancel": "turn_boundary_guaranteed",
        "fork": "emulated",
        "request_continuation": "emulated",
    },
    identity=LaneIdentityMap(
        session_ref="thread_id",
        turn_ref="checkpoint_id",
        effect_ref="tool_call_id",
        cursor="checkpoint_id",
    ),
    hooks=LaneHooks(
        mechanism="middleware",
        events_supported=(
            "session_start",
            "before_model",
            "after_model",
            "before_tool",
            "after_tool",
            "after_tool_failure",
            "before_compaction",
            "stop",
            "session_end",
        ),
        fail_closed=True,
    ),
    instruction_channel=("system_prompt",),
    subagents=LaneSubagents(file=None, inline="SubAgent", readonly_supported_inline=False),
    usage=LaneUsage(tokens="settled_per_turn", cost="estimated"),
    placement="worker_hosted",
    # The first lane: qualified by the Deep Agents acceptance suite (WP-CP-040, parity).
    qualified=True,
)

CURSOR_LOCAL_DESCRIBE_V1: Final = LaneDescribe(
    lane="cursor",
    lane_profile="cursor_local",
    versions=dict(_CURSOR_VERSIONS),
    controls={
        "prepare": "native",
        "start": "native",
        "reattach": "emulated",
        "send_turn": "native",
        "cancel_turn": "native",
        "observe": "native",
        "snapshot": "emulated",
        "usage": "native",
        "end_session": "native",
        "pause": "unsupported",
        "fork": "emulated",
    },
    delivery_semantics={
        "queue_instruction": "wait_then_send",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "unsupported",
        "hard_pause": "unsupported",
        "resume": "wait_then_send",
        "cancel": "turn_boundary_guaranteed",
        "fork": "emulated",
        "request_continuation": "emulated",
    },
    identity=LaneIdentityMap(
        session_ref="agent_id", turn_ref="run_id", effect_ref="call_id", cursor="bridge_offset"
    ),
    hooks=LaneHooks(
        mechanism="command_hooks", events_supported=_CURSOR_LOCAL_HOOKS, fail_closed=True
    ),
    instruction_channel=("AGENTS.md", ".cursor/rules/mc-mission.mdc", "prompt_prefix"),
    subagents=LaneSubagents(
        file=".cursor/agents/*.md", inline="AgentOptions.agents", readonly_supported_inline=False
    ),
    usage=LaneUsage(tokens="settled_per_turn", cost="estimated_then_settled"),
    placement="worker_hosted",
    qualified=False,
)

CURSOR_CLOUD_DESCRIBE_V1: Final = LaneDescribe(
    lane="cursor",
    lane_profile="cursor_cloud",
    versions={**_CURSOR_VERSIONS, "cloud_api": "v1"},
    controls={**CURSOR_LOCAL_DESCRIBE_V1.controls, "reattach": "native"},
    delivery_semantics=dict(CURSOR_LOCAL_DESCRIBE_V1.delivery_semantics),
    identity=LaneIdentityMap(
        session_ref="agent_id", turn_ref="run_id", effect_ref="call_id", cursor="sse_event_id"
    ),
    # Cloud runs fire no sessionStart, sessionEnd or MCP hooks. The cloud VM cannot reach the
    # worker's loopback callback, so no fail-closed Kernel Hook runs there (catalog command
    # hooks only; the Stop Fence reaches a cloud run through `POST .../cancel`): FT-G6 states
    # it honestly.
    hooks=LaneHooks(
        mechanism="command_hooks",
        events_supported=tuple(
            event for event in _CURSOR_LOCAL_HOOKS if event not in {"session_start", "session_end"}
        ),
        fail_closed=False,
    ),
    instruction_channel=("AGENTS.md", ".cursor/rules/mc-mission.mdc"),
    subagents=LaneSubagents(
        file=".cursor/agents/*.md", inline="customSubagents", readonly_supported_inline=False
    ),
    usage=LaneUsage(tokens="settled_per_turn", cost="estimated_then_settled"),
    placement="cloud",
    qualified=False,
)

# --- MP-09: the Cursor profiles as `mc.lane_describe.v2` ----------------------------------------
#
# Controls, delivery and identity are the FT-G1 v1 cells (kept above as `*_V1`, still readable);
# v2 adds per-feature evidence. Implemented cells are `native`/`emulated` with `qualified=False`
# (the convention of every implemented v2 matrix) and cite their offline evidence
# (OVE-55 fixture suites and the MP-09 suites); nothing is qualified until the owner-run drill
# records it. `approval_suspension` has no vendor surface on this pin: cursor-sdk 1.0.37 exposes
# `SDKRequestMessage(request_id)` and no API that answers it, and the cloud run stream carries
# no request event. Cloud agents fire no MCP hooks, so cloud `enforcement_coverage["mcp"]` is
# unsupported. The approval coverage report is `adapters/cursor/qualification.py`.
_CURSOR_SDK_REQUEST: Final = (
    "cursor_sdk==1.0.37 types.py SDKRequestMessage(request_id); no respond API in "
    "_async_run.py/_async_agent.py/_async_client.py"
)
_CURSOR_CLOUD_HOOKS: Final = (
    "../mission-control-general/runtime-facts/CURSOR_SDK_FACTS.md section 7: cloud agents "
    "run command hooks only; no sessionStart/sessionEnd/beforeMCPExecution/afterMCPExecution"
)
_CURSOR_FIXTURES: Final = "tests/unit/harness/test_lane_qualification_fixtures.py"
_CURSOR_HOOK_ROUNDTRIP: Final = "tests/integration/cursor/test_kernel_hook_roundtrip.py"
_CURSOR_HONESTY: Final = "tests/unit/harness/test_describe_honesty.py::request_continuation"
# Patch-based custody and hydrated handover, as the v1 `snapshot`/`fork` cells state them.
_CURSOR_EMULATED: Final = frozenset({"output_custody", "continuation"})


def _cursor_features(*, local: bool) -> dict[str, FeatureEvidence]:
    transport = "cursor-sdk bridge (local)" if local else "Cloud Agents API v1 over httpx"
    os = "Linux|Darwin (WSL 2 on a Windows host)" if local else "any worker host"
    implemented = {
        "launch": True,
        "status": True,
        "observe": True,
        "follow_up": True,
        "cancel": True,
        "usage": True,
        "output_custody": True,
        "environment_selection": True,
        "configuration_materialization": True,
        "approval_suspension": False,
        "subordinate_lineage": local,
        "continuation": True,
    }
    unsupported = set() if local else {"approval_suspension"}
    refs = {
        "approval_suspension": _CURSOR_SDK_REQUEST,
        "subordinate_lineage": _CURSOR_HOOK_ROUNDTRIP if local else _CURSOR_CLOUD_HOOKS,
        "continuation": _CURSOR_HONESTY,
        "usage": (
            "tests/unit/harness/test_cursor_local.py::"
            "test_error_run_fails_and_settles_cost_when_the_provider_reports_it"
            if local
            else "tests/unit/cursor/test_cloud_usage_unknown.py"
        ),
        "observe": (
            "tests/unit/harness/test_cursor_local.py::"
            "test_a_resume_from_a_stored_offset_stores_no_frame_twice"
            if local
            else "tests/unit/cursor/test_cloud_stream_expiry.py"
        ),
        "launch": _CURSOR_FIXTURES
        if local
        else "tests/unit/cursor/test_cloud_capacity_and_busy.py",
        "follow_up": _CURSOR_FIXTURES if local else "tests/unit/cursor/test_cloud_reconcile.py",
        "output_custody": (
            _CURSOR_FIXTURES if local else "tests/unit/cursor/test_cloud_workspace_and_resume.py"
        ),
    }
    cells: dict[str, FeatureEvidence] = {}
    for feature in LANE_FEATURES:
        # The cell's status is how the lane supports it (as the v1 controls state it);
        # `qualified` stays False until the owner-run drill records the profile.
        status: ControlSupport
        if feature in unsupported:
            status = "unsupported"
        elif not implemented[feature]:
            status = "unqualified"
        else:
            status = "emulated" if feature in _CURSOR_EMULATED else "native"
        cells[feature] = FeatureEvidence(
            status=status,
            implemented=implemented[feature] and status != "unsupported",
            account_enabled="unknown",
            qualified=False,
            evidence_ref=refs.get(feature, _CURSOR_FIXTURES),
            transport=transport,
            sdk_language="python",
            sdk_version="1.0.37",
            provider_scope="cursor account bound at launch",
            os=os,
        )
    return cells


CURSOR_LOCAL_DESCRIBE: Final = LaneDescribe.model_validate(
    {
        **CURSOR_LOCAL_DESCRIBE_V1.model_dump(mode="python"),
        "schema_version": LANE_DESCRIBE_SCHEMA_V2,
        "features": _cursor_features(local=True),
        "approval_modes": ("workflow_gate",),
        "compaction_control": "unsupported",  # preCompact is observe-only; no SDK/REST compact
        "subordinate_visibility": "unqualified",
        "enforcement_coverage": {
            "shell": "unqualified",
            "file": "unqualified",
            "mcp": "unqualified",
        },
    }
)
CURSOR_CLOUD_DESCRIBE: Final = LaneDescribe.model_validate(
    {
        **CURSOR_CLOUD_DESCRIBE_V1.model_dump(mode="python"),
        "schema_version": LANE_DESCRIBE_SCHEMA_V2,
        "features": _cursor_features(local=False),
        "approval_modes": ("workflow_gate",),
        "compaction_control": "unsupported",
        "subordinate_visibility": "unqualified",
        "enforcement_coverage": {
            "shell": "unqualified",
            "file": "unqualified",
            "mcp": "unsupported",
        },
    }
)

# --- MP-07 / MP-08: the local Claude and Codex lanes --------------------------------------------
#
# What `adapters/claude` (claude-agent-sdk 0.2.165, bundled Claude Code 2.1.294) and
# `adapters/codex` (codex-cli 0.162.0, app-server protocol v2) implement; every mapping is cited
# in those adapters' `describe.py` modules, which re-export these constants. Implemented cells
# are fixture/real-service proven, never `qualified`: that needs the owner-run bounded drill
# under docs/qualification/lanes/{claude_agent_sdk,codex}/. Both run as stdio subprocesses on
# Linux/WSL workers (the Windows worker's SelectorEventLoop has no subprocess support).

_UNQUALIFIED_CONTROLS: Final = dict.fromkeys(LANE_CONTROLS, "unqualified")
_UNSUPPORTED: Final = FeatureEvidence(status="unsupported")

_CLAUDE_HOOKS: Final = (
    "session_start",
    "before_prompt",
    "before_tool",
    "after_tool",
    "after_tool_failure",
    "before_shell",
    "after_shell",
    "before_mcp",
    "after_file_edit",
    "before_compaction",
    "after_compaction",
    "subagent_start",
    "subagent_stop",
    "stop",
    "session_end",
)
_CODEX_HOOKS: Final = tuple(event for event in _CLAUDE_HOOKS if event != "after_tool_failure")

CLAUDE_SDK_VERSION: Final = "0.2.165"
CLAUDE_BUNDLED_CLI_VERSION: Final = "2.1.294"
CODEX_CLI_VERSION: Final = "0.162.0"
CODEX_APP_SERVER_SCHEMA_SHA256: Final = (
    "0bf5254bede109d4ae03ce2e81372e4c93a30b359c0749ec7dce7a9382a7f857"
)


def _local_feature(
    status: ControlSupport, *, transport: str, sdk_version: str, provider_scope: str, os: str
) -> FeatureEvidence:
    return FeatureEvidence(
        status=status,
        implemented=True,
        account_enabled="unknown",
        qualified=False,
        transport=transport,
        sdk_language="python",
        sdk_version=sdk_version,
        provider_scope=provider_scope,
        os=os,
    )


def _claude(status: ControlSupport = "native") -> FeatureEvidence:
    return _local_feature(
        status,
        transport="stdio_subprocess",
        sdk_version=CLAUDE_SDK_VERSION,
        provider_scope="anthropic:claude_code_local",
        os="linux",
    )


def _codex(status: ControlSupport) -> FeatureEvidence:
    return _local_feature(
        status,
        transport="app-server jsonrpc v2 over stdio",
        sdk_version=f"codex-cli {CODEX_CLI_VERSION}",
        provider_scope="local worker, owner account",
        os="linux (WSL)",
    )


# Approvals: `can_use_tool` binds provider permission requests (MP-11 broker). The Python SDK
# forwards no MCP elicitation (`types.McpSdkServerConfig`) and has no user-question callback,
# so neither `provider_question` nor `mcp_elicitation` is claimed. `PreCompact` is observed
# only (no compaction control request), so compaction stays `unqualified`.
CLAUDE_AGENT_SDK_DESCRIBE: Final = LaneDescribe(
    schema_version=LANE_DESCRIBE_SCHEMA_V2,
    lane="claude",
    lane_profile="claude_agent_sdk",
    versions={
        "claude_agent_sdk": CLAUDE_SDK_VERSION,
        "claude_code_bundled": CLAUDE_BUNDLED_CLI_VERSION,
        "transport": "stdio_subprocess",
    },
    controls={
        "prepare": "native",
        "start": "native",
        "reattach": "emulated",
        "send_turn": "native",
        "cancel_turn": "native",
        "observe": "native",
        "snapshot": "emulated",
        "usage": "native",
        "end_session": "native",
        "pause": "unqualified",
        "fork": "unqualified",
    },
    delivery_semantics={
        "queue_instruction": "turn_boundary_guaranteed",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "pause_at_tool_gate",
        "hard_pause": "unsupported",
        "resume": "turn_boundary_guaranteed",
        "cancel": "turn_boundary_guaranteed",
        "fork": "emulated",
        "request_continuation": "emulated",
    },
    identity=LaneIdentityMap(
        session_ref="session_id",
        turn_ref="user_message_uuid",
        effect_ref="tool_use_id",
        cursor="session_log_ordinal",
    ),
    hooks=LaneHooks(
        mechanism="sdk_callbacks+command_hooks", events_supported=_CLAUDE_HOOKS, fail_closed=True
    ),
    instruction_channel=("CLAUDE.md", "prompt", ".claude/settings.json"),
    subagents=LaneSubagents(
        file=".claude/agents/*.md",
        inline="ClaudeAgentOptions.agents",
        readonly_supported_inline=True,
    ),
    usage=LaneUsage(tokens="settled_per_turn", cost="estimated"),
    placement="worker_hosted",
    qualified=False,
    features={
        "launch": _claude(),
        "status": _claude(),
        "observe": _claude(),
        "follow_up": _claude(),
        "cancel": _claude(),
        "usage": _claude(),
        "output_custody": _claude("emulated"),
        "environment_selection": FeatureEvidence(status="unqualified"),
        "configuration_materialization": _claude(),
        "approval_suspension": _claude(),
        "subordinate_lineage": _claude(),
        # Sealed-checkpoint transfer into a fresh session (no conversation fork claimed).
        "continuation": _claude("emulated"),
    },
    approval_modes=("workflow_gate", "provider_permission", "governed_effect"),
    compaction_control="unqualified",
    subordinate_visibility="lifecycle_only",
    enforcement_coverage={"shell": "native", "file": "native", "mcp": "native"},
)

# Codex: `turn/start` on an active thread would steer it, so queued instructions and resume
# only send on an idle thread (`wait_then_send`); `turn/steer` gives cooperative inject but the
# declared interrupt mode stays `cancel_and_replace` (`LANE_COMMAND_SEMANTICS`). Approval and
# user-input server requests bind to Human Tasks; `thread/compact` is the explicit compaction.
CODEX_DESCRIBE: Final = LaneDescribe(
    schema_version=LANE_DESCRIBE_SCHEMA_V2,
    lane="codex",
    lane_profile="codex",
    versions={
        "codex_cli": CODEX_CLI_VERSION,
        "app_server_protocol": "v2",
        "app_server_schema_sha256": CODEX_APP_SERVER_SCHEMA_SHA256,
    },
    controls={
        "prepare": "native",
        "start": "native",
        "reattach": "emulated",
        "send_turn": "native",
        "cancel_turn": "native",
        "observe": "native",
        "snapshot": "emulated",
        "usage": "native",
        "end_session": "native",
        "pause": "emulated",
        "fork": "unqualified",
    },
    delivery_semantics={
        "queue_instruction": "wait_then_send",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "pause_at_tool_gate",
        "hard_pause": "unsupported",
        "resume": "wait_then_send",
        "cancel": "turn_boundary_guaranteed",
        "fork": "emulated",
        "request_continuation": "emulated",
    },
    identity=LaneIdentityMap(
        session_ref="thread_id",
        turn_ref="turn_id",
        effect_ref="item_id",
        cursor="turn_id@connection_epoch:event_seq",
    ),
    hooks=LaneHooks(mechanism="command_hooks", events_supported=_CODEX_HOOKS, fail_closed=True),
    instruction_channel=("AGENTS.md", ".codex/config.toml", "developer_instructions"),
    subagents=LaneSubagents(
        file=".codex/agents/*.toml", inline=None, readonly_supported_inline=False
    ),
    usage=LaneUsage(tokens="settled_per_turn", cost="estimated"),
    placement="worker_hosted",
    qualified=False,
    features={
        "launch": _codex("native"),
        "status": _codex("native"),
        "observe": _codex("native"),
        "follow_up": _codex("native"),
        "cancel": _codex("native"),
        "usage": _codex("native"),
        "output_custody": _codex("emulated"),
        "environment_selection": _UNSUPPORTED,
        "configuration_materialization": _codex("native"),
        "approval_suspension": _codex("native"),
        "subordinate_lineage": _codex("emulated"),
        "continuation": _codex("emulated"),
    },
    approval_modes=(
        "workflow_gate",
        "provider_permission",
        "provider_question",
        "mcp_elicitation",
        "governed_effect",
    ),
    compaction_control="native",
    subordinate_visibility="lifecycle_only",
    enforcement_coverage={"shell": "native", "file": "native", "mcp": "emulated"},
)

# --- MP-01 v2 stubs: the provider-hosted products -------------------------------------------------
#
# Delivery semantics below are design intent recorded from the hosted feasibility studies
# (revalidated 2026-10-09: Outcome 3 unchanged); the `unqualified`/`unsupported` cells make that
# status machine-readable. No adapter exists: MP-18/19 are evidence-blocked.

# Provider-hosted Claude Code (docs/qualification/lanes/claude_cloud/FEASIBILITY.md): the
# repository's settings-file hooks run in the cloud session, but no loopback callback can be
# reached, so no fail-closed Kernel Hook runs there.
CLAUDE_CLOUD_DESCRIBE: Final = LaneDescribe(
    schema_version=LANE_DESCRIBE_SCHEMA_V2,
    lane="claude",
    lane_profile="claude_cloud",
    versions={"claude_code": "unpinned", "cloud_api": "undocumented"},
    controls=dict(_UNQUALIFIED_CONTROLS),
    delivery_semantics={
        "queue_instruction": "wait_then_send",
        "interrupt_and_inject": "cancel_and_replace",
        "pause": "unsupported",
        "hard_pause": "unsupported",
        "resume": "wait_then_send",
        # No public cancel/interrupt surface exists (FEASIBILITY "Required controls" 4).
        "cancel": "unsupported",
        "fork": "unsupported",
        "request_continuation": "emulated",
    },
    identity=LaneIdentityMap(
        session_ref="cloud_session_id",
        turn_ref="message_uuid",
        effect_ref="tool_use_id",
        cursor="uuid",
    ),
    hooks=LaneHooks(mechanism="command_hooks", events_supported=_CLAUDE_HOOKS, fail_closed=False),
    instruction_channel=("CLAUDE.md", ".claude/settings.json"),
    subagents=LaneSubagents(
        file=".claude/agents/*.md", inline=None, readonly_supported_inline=False
    ),
    usage=LaneUsage(tokens="unavailable", cost="unavailable"),
    placement="cloud",
    qualified=False,
    features={
        **unqualified_features(),
        # Undocumented on every public surface as of 2026-10-08 (FEASIBILITY per-feature table).
        "status": _UNSUPPORTED,
        "cancel": _UNSUPPORTED,
        "approval_suspension": _UNSUPPORTED,
        "subordinate_lineage": _UNSUPPORTED,
    },
    approval_modes=("workflow_gate",),
    compaction_control="unqualified",
    subordinate_visibility="unavailable",
    enforcement_coverage={"shell": "unqualified", "file": "unqualified", "mcp": "unqualified"},
)

# Codex Cloud tasks (docs/qualification/lanes/codex_cloud/FEASIBILITY.md): no per-task
# config, approval, sandbox, MCP or hook overrides; no command/local hooks under cloud
# orchestration; follow-up and observation are not documented.
CODEX_CLOUD_DESCRIBE: Final = LaneDescribe(
    schema_version=LANE_DESCRIBE_SCHEMA_V2,
    lane="codex",
    lane_profile="codex_cloud",
    versions={"codex_cli": "unpinned", "cloud_api": "undocumented"},
    controls=dict(_UNQUALIFIED_CONTROLS),
    delivery_semantics={
        "queue_instruction": "unsupported",
        "interrupt_and_inject": "unsupported",
        "pause": "unsupported",
        "hard_pause": "unsupported",
        "resume": "unsupported",
        # The experimental CLI exposes submit and list only; no cancel (FEASIBILITY item 5).
        "cancel": "unsupported",
        "fork": "unsupported",
        "request_continuation": "unsupported",
    },
    identity=LaneIdentityMap(
        session_ref="task_id", turn_ref="task_id", effect_ref="item_id", cursor="task_id"
    ),
    hooks=LaneHooks(mechanism="none", fail_closed=False),
    instruction_channel=("AGENTS.md",),
    subagents=LaneSubagents(file=None, inline=None, readonly_supported_inline=False),
    usage=LaneUsage(tokens="unavailable", cost="unavailable"),
    placement="cloud",
    qualified=False,
    features={
        **unqualified_features(),
        # Absent from every public surface as of 2026-10-08 (FEASIBILITY "Required controls").
        "observe": _UNSUPPORTED,
        "follow_up": _UNSUPPORTED,
        "cancel": _UNSUPPORTED,
        "usage": _UNSUPPORTED,
        "approval_suspension": _UNSUPPORTED,
        "subordinate_lineage": _UNSUPPORTED,
        "continuation": _UNSUPPORTED,
    },
    approval_modes=("workflow_gate",),
    compaction_control="unsupported",
    subordinate_visibility="unavailable",
    enforcement_coverage={"shell": "unsupported", "file": "unsupported", "mcp": "unsupported"},
)

DECLARED_LANE_MATRICES: Final[dict[LaneProfileName, LaneDescribe]] = {
    "deep_agents": DEEP_AGENTS_DESCRIBE,
    "cursor_local": CURSOR_LOCAL_DESCRIBE,
    "cursor_cloud": CURSOR_CLOUD_DESCRIBE,
    "claude_agent_sdk": CLAUDE_AGENT_SDK_DESCRIBE,
    "codex": CODEX_DESCRIBE,
    "claude_cloud": CLAUDE_CLOUD_DESCRIBE,
    "codex_cloud": CODEX_CLOUD_DESCRIBE,
}

# Deep Agents keeps its digest-stable FT-G1 v1 matrix. The Cursor, Claude and Codex lanes have
# implemented v2 matrices (per-feature evidence, nothing qualified); the hosted products are v2
# stubs with every control unqualified.
V1_LANE_PROFILES: Final[frozenset[LaneProfileName]] = frozenset({"deep_agents"})
V2_IMPLEMENTED_LANE_PROFILES: Final[frozenset[LaneProfileName]] = frozenset(
    {"cursor_local", "cursor_cloud", "claude_agent_sdk", "codex"}
)
V2_STUB_LANE_PROFILES: Final[frozenset[LaneProfileName]] = frozenset(
    {"claude_cloud", "codex_cloud"}
)


def declared_matrix(lane_profile: str) -> LaneDescribe:
    try:
        return DECLARED_LANE_MATRICES[lane_profile]  # type: ignore[index]
    except KeyError as error:
        raise ValueError(f"undeclared lane profile: {lane_profile}") from error
