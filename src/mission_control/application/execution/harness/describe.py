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

CURSOR_LOCAL_DESCRIBE: Final = LaneDescribe(
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

CURSOR_CLOUD_DESCRIBE: Final = LaneDescribe(
    lane="cursor",
    lane_profile="cursor_cloud",
    versions={**_CURSOR_VERSIONS, "cloud_api": "v1"},
    controls={**CURSOR_LOCAL_DESCRIBE.controls, "reattach": "native"},
    delivery_semantics=dict(CURSOR_LOCAL_DESCRIBE.delivery_semantics),
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

# --- MP-01 v2 stubs ---------------------------------------------------------------------------
#
# Delivery semantics below are design intent recorded from docs/research and the hosted
# feasibility studies; the `unqualified` controls/features make that status machine-readable.

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

CLAUDE_AGENT_SDK_DESCRIBE: Final = LaneDescribe(
    schema_version=LANE_DESCRIBE_SCHEMA_V2,
    lane="claude",
    lane_profile="claude_agent_sdk",
    versions={"claude_agent_sdk": "unpinned", "claude_code": "unpinned"},
    controls=dict(_UNQUALIFIED_CONTROLS),
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
        session_ref="session_id", turn_ref="message_uuid", effect_ref="tool_use_id", cursor="uuid"
    ),
    hooks=LaneHooks(mechanism="command_hooks", events_supported=_CLAUDE_HOOKS, fail_closed=True),
    instruction_channel=("CLAUDE.md", "system_prompt_append", ".claude/settings.json"),
    subagents=LaneSubagents(
        file=".claude/agents/*.md",
        inline="ClaudeAgentOptions.agents",
        readonly_supported_inline=True,
    ),
    usage=LaneUsage(tokens="settled_per_turn", cost="settled"),
    placement="worker_hosted",
    qualified=False,
    features=unqualified_features(),
    approval_modes=("workflow_gate", "provider_permission", "mcp_elicitation", "governed_effect"),
    compaction_control="unqualified",
    subordinate_visibility="unqualified",
    enforcement_coverage={"shell": "unqualified", "file": "unqualified", "mcp": "unqualified"},
)

CODEX_DESCRIBE: Final = LaneDescribe(
    schema_version=LANE_DESCRIBE_SCHEMA_V2,
    lane="codex",
    lane_profile="codex",
    versions={"codex_cli": "unpinned", "app_server_protocol": "v2"},
    controls=dict(_UNQUALIFIED_CONTROLS),
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
        session_ref="thread_id", turn_ref="turn_id", effect_ref="item_id", cursor="item_id"
    ),
    hooks=LaneHooks(mechanism="command_hooks", events_supported=_CODEX_HOOKS, fail_closed=True),
    instruction_channel=("AGENTS.md", ".codex/config.toml", "developer_instructions"),
    subagents=LaneSubagents(
        file=".codex/agents/*.toml", inline=None, readonly_supported_inline=False
    ),
    usage=LaneUsage(tokens="settled_per_turn", cost="estimated"),
    placement="worker_hosted",
    qualified=False,
    features=unqualified_features(),
    approval_modes=(
        "workflow_gate",
        "provider_permission",
        "provider_question",
        "mcp_elicitation",
        "governed_effect",
    ),
    compaction_control="unqualified",
    subordinate_visibility="unqualified",
    enforcement_coverage={"shell": "unqualified", "file": "unqualified", "mcp": "unqualified"},
)

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

# The FT-G1 profiles whose v1 matrices are digest-stable; the rest are MP-01 v2 stubs.
V1_LANE_PROFILES: Final[frozenset[LaneProfileName]] = frozenset(
    {"deep_agents", "cursor_local", "cursor_cloud"}
)
V2_STUB_LANE_PROFILES: Final[frozenset[LaneProfileName]] = frozenset(
    {"claude_agent_sdk", "codex", "claude_cloud", "codex_cloud"}
)


def declared_matrix(lane_profile: str) -> LaneDescribe:
    try:
        return DECLARED_LANE_MATRICES[lane_profile]  # type: ignore[index]
    except KeyError as error:
        raise ValueError(f"undeclared lane profile: {lane_profile}") from error
