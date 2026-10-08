"""The three lane profiles' `mc.lane_describe.v1` matrices (00-ARCHITECTURE section 6, SPEC-07
section 2).

These are the declared matrices each lane implementation is held to by the conformance and
describe-honesty tests. The Cursor profiles stay `qualified=False` until a recorded
qualification flips them (SPEC-07 section 12, FT-G6); until their harnesses land (FT-G3,
FT-G5) the registry carries them as stubs whose every control is `unqualified`.
"""

from __future__ import annotations

from typing import Final

from mission_control.domain.execution.lanes import (
    LaneDescribe,
    LaneHooks,
    LaneIdentityMap,
    LaneProfileName,
    LaneSubagents,
    LaneUsage,
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
    # Cloud runs fire no sessionStart, sessionEnd or MCP hooks.
    hooks=LaneHooks(
        mechanism="command_hooks",
        events_supported=tuple(
            event for event in _CURSOR_LOCAL_HOOKS if event not in {"session_start", "session_end"}
        ),
        fail_closed=True,
    ),
    instruction_channel=("AGENTS.md", ".cursor/rules/mc-mission.mdc"),
    subagents=LaneSubagents(
        file=".cursor/agents/*.md", inline="customSubagents", readonly_supported_inline=False
    ),
    usage=LaneUsage(tokens="settled_per_turn", cost="estimated_then_settled"),
    placement="cloud",
    qualified=False,
)

DECLARED_LANE_MATRICES: Final[dict[LaneProfileName, LaneDescribe]] = {
    "deep_agents": DEEP_AGENTS_DESCRIBE,
    "cursor_local": CURSOR_LOCAL_DESCRIBE,
    "cursor_cloud": CURSOR_CLOUD_DESCRIBE,
}


def declared_matrix(lane_profile: str) -> LaneDescribe:
    try:
        return DECLARED_LANE_MATRICES[lane_profile]  # type: ignore[index]
    except KeyError as error:
        raise ValueError(f"undeclared lane profile: {lane_profile}") from error
