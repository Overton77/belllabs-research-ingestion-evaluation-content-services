"""Hook Event vocabulary and its per-lane-profile native mapping (ADR-0026, SPEC-01).

One provider-neutral vocabulary; each lane profile maps an event to its native hook name
and, where the provider has no dedicated event, a matcher over the tool name. ``None``
means the event cannot run on that profile and is reported as ``unsupported_on_lane``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from enum import StrEnum

from pydantic import BaseModel, ConfigDict

from mission_control.domain.capabilities.host_support import LaneProfile


class HookEvent(StrEnum):
    SESSION_START = "session_start"
    SESSION_END = "session_end"
    BEFORE_PROMPT = "before_prompt"
    BEFORE_MODEL = "before_model"
    AFTER_MODEL = "after_model"
    BEFORE_TOOL = "before_tool"
    AFTER_TOOL = "after_tool"
    AFTER_TOOL_FAILURE = "after_tool_failure"
    BEFORE_SHELL = "before_shell"
    AFTER_SHELL = "after_shell"
    BEFORE_MCP = "before_mcp"
    AFTER_FILE_EDIT = "after_file_edit"
    BEFORE_COMPACTION = "before_compaction"
    AFTER_COMPACTION = "after_compaction"
    SUBAGENT_START = "subagent_start"
    SUBAGENT_STOP = "subagent_stop"
    STOP = "stop"


class HookInterpreter(StrEnum):
    SH = "sh"
    BASH = "bash"
    PYTHON = "python"
    NODE = "node"


class HookCallback(StrEnum):
    NONE = "none"
    SERVICE = "service"


class HookDecision(StrEnum):
    ALLOW = "allow"
    DENY = "deny"
    DEFER = "defer"


class NativeHook(BaseModel):
    """Where one Hook Event lands on one lane profile."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    native_event: str
    matcher: str | None = None
    observe_only: bool = False


E = HookEvent
_N = NativeHook

_DEEP_AGENTS: dict[HookEvent, NativeHook] = {
    E.SESSION_START: _N(native_event="before_agent"),
    E.SESSION_END: _N(native_event="after_agent"),
    E.BEFORE_PROMPT: _N(native_event="before_agent"),
    E.BEFORE_MODEL: _N(native_event="before_model"),
    E.AFTER_MODEL: _N(native_event="after_model"),
    E.BEFORE_TOOL: _N(native_event="wrap_tool_call"),
    E.AFTER_TOOL: _N(native_event="wrap_tool_call"),
    E.AFTER_TOOL_FAILURE: _N(native_event="wrap_tool_call"),
    E.BEFORE_SHELL: _N(native_event="wrap_tool_call", matcher="shell|execute"),
    E.AFTER_SHELL: _N(native_event="wrap_tool_call", matcher="shell|execute"),
    E.BEFORE_MCP: _N(native_event="wrap_tool_call", matcher="mcp__.*"),
    E.AFTER_FILE_EDIT: _N(native_event="wrap_tool_call", matcher="write_file|edit_file"),
    E.BEFORE_COMPACTION: _N(native_event="summarization"),
    E.AFTER_COMPACTION: _N(native_event="summarization"),
    E.SUBAGENT_START: _N(native_event="wrap_tool_call", matcher="task"),
    E.SUBAGENT_STOP: _N(native_event="wrap_tool_call", matcher="task"),
    E.STOP: _N(native_event="after_agent"),
}

_CURSOR_LOCAL: dict[HookEvent, NativeHook] = {
    E.SESSION_START: _N(native_event="sessionStart"),
    E.SESSION_END: _N(native_event="sessionEnd"),
    E.BEFORE_PROMPT: _N(native_event="beforeSubmitPrompt"),
    E.AFTER_MODEL: _N(native_event="afterAgentResponse", observe_only=True),
    E.BEFORE_TOOL: _N(native_event="preToolUse"),
    E.AFTER_TOOL: _N(native_event="postToolUse"),
    E.AFTER_TOOL_FAILURE: _N(native_event="postToolUseFailure"),
    E.BEFORE_SHELL: _N(native_event="beforeShellExecution"),
    E.AFTER_SHELL: _N(native_event="afterShellExecution"),
    E.BEFORE_MCP: _N(native_event="beforeMCPExecution"),
    E.AFTER_FILE_EDIT: _N(native_event="afterFileEdit"),
    E.BEFORE_COMPACTION: _N(native_event="preCompact", observe_only=True),
    E.SUBAGENT_START: _N(native_event="subagentStart"),
    E.SUBAGENT_STOP: _N(native_event="subagentStop"),
    E.STOP: _N(native_event="stop"),
}

# Cursor Cloud runs command hooks only and skips session and MCP hooks.
_CURSOR_CLOUD: dict[HookEvent, NativeHook] = {
    event: hook
    for event, hook in _CURSOR_LOCAL.items()
    if event not in {E.SESSION_START, E.SESSION_END, E.BEFORE_MCP}
}

# Claude Code settings-file hooks (`.claude/settings.json`, loaded through `setting_sources`),
# which is how the materialized `.mission/hooks/run.py` reaches the session. Evidence:
# docs/research/2026-10-07-coding-lane-surfaces.md ("Python callback hooks" paragraph).
_CLAUDE: dict[HookEvent, NativeHook] = {
    E.SESSION_START: _N(native_event="SessionStart"),
    E.SESSION_END: _N(native_event="SessionEnd"),
    E.BEFORE_PROMPT: _N(native_event="UserPromptSubmit"),
    E.BEFORE_TOOL: _N(native_event="PreToolUse"),
    E.AFTER_TOOL: _N(native_event="PostToolUse"),
    E.AFTER_TOOL_FAILURE: _N(native_event="PostToolUseFailure"),
    E.BEFORE_SHELL: _N(native_event="PreToolUse", matcher="Bash"),
    E.AFTER_SHELL: _N(native_event="PostToolUse", matcher="Bash"),
    E.BEFORE_MCP: _N(native_event="PreToolUse", matcher="mcp__.*"),
    E.AFTER_FILE_EDIT: _N(native_event="PostToolUse", matcher="Edit|Write"),
    E.BEFORE_COMPACTION: _N(native_event="PreCompact"),
    E.AFTER_COMPACTION: _N(native_event="PostCompact"),
    E.SUBAGENT_START: _N(native_event="SubagentStart"),
    E.SUBAGENT_STOP: _N(native_event="SubagentStop"),
    E.STOP: _N(native_event="Stop"),
}

# The subset of `_CLAUDE` that the pinned Python `claude_agent_sdk.types.HookEvent` union
# also exposes as in-process callbacks (`PreToolUse`, `PostToolUse`, `PostToolUseFailure`,
# `UserPromptSubmit`, `Stop`, `SubagentStop`, `PreCompact`, `Notification`, `SubagentStart`,
# `PermissionRequest`). `SessionStart`, `SessionEnd` and `PostCompact` fire only as
# settings-file hooks; an in-process kernel hook on them is `unsupported_on_lane`
# (multi-provider RESEARCH.md "Claude SDK language mismatch").
CLAUDE_SDK_CALLBACK_EVENTS: frozenset[HookEvent] = frozenset(
    {
        E.BEFORE_PROMPT,
        E.BEFORE_TOOL,
        E.AFTER_TOOL,
        E.AFTER_TOOL_FAILURE,
        E.BEFORE_SHELL,
        E.AFTER_SHELL,
        E.BEFORE_MCP,
        E.AFTER_FILE_EDIT,
        E.BEFORE_COMPACTION,
        E.SUBAGENT_START,
        E.SUBAGENT_STOP,
        E.STOP,
    }
)

# Codex hooks (`hooks.json` / `[hooks]` in config.toml) expose twelve events: PreToolUse,
# PermissionRequest, PostToolUse, PreCompact, PostCompact, SessionStart, SessionEnd,
# UserPromptSubmit, SubagentStart, SubagentStop, Stop, Interrupt
# (codex-rs/protocol/src/protocol.rs `HookEventName`). There is no PostToolUseFailure. The
# canonical shell tool_name is "Bash"; file edits are `apply_patch` (matcher aliases
# Write|Edit) (codex-rs/core/src/tools/hook_names.rs).
_CODEX: dict[HookEvent, NativeHook] = {
    E.SESSION_START: _N(native_event="SessionStart"),
    E.SESSION_END: _N(native_event="SessionEnd"),
    E.BEFORE_PROMPT: _N(native_event="UserPromptSubmit"),
    E.BEFORE_TOOL: _N(native_event="PreToolUse"),
    E.AFTER_TOOL: _N(native_event="PostToolUse"),
    E.BEFORE_SHELL: _N(native_event="PreToolUse", matcher="Bash"),
    E.AFTER_SHELL: _N(native_event="PostToolUse", matcher="Bash"),
    E.BEFORE_MCP: _N(native_event="PreToolUse", matcher="mcp__.*"),
    E.AFTER_FILE_EDIT: _N(native_event="PostToolUse", matcher="apply_patch"),
    E.BEFORE_COMPACTION: _N(native_event="PreCompact"),
    E.AFTER_COMPACTION: _N(native_event="PostCompact"),
    E.SUBAGENT_START: _N(native_event="SubagentStart"),
    E.SUBAGENT_STOP: _N(native_event="SubagentStop"),
    E.STOP: _N(native_event="Stop"),
}

# Provider-hosted Claude Code runs the repository's `.claude/settings.json` hooks, so the
# vocabulary is the settings-file one; the lane profile itself stays unqualified until the
# qualification drill in docs/qualification/lanes/claude_cloud/FEASIBILITY.md runs.
_CLAUDE_CLOUD: dict[HookEvent, NativeHook] = dict(_CLAUDE)

# Codex Cloud tasks accept no command, local or plugin hooks under cloud orchestration
# (docs/qualification/lanes/codex_cloud/FEASIBILITY.md, "Configuration materialization");
# every Hook Event is `unsupported_on_lane` there.
_CODEX_CLOUD: dict[HookEvent, NativeHook] = {}

HOOK_EVENT_MAPPING: Mapping[LaneProfile, Mapping[HookEvent, NativeHook]] = {
    LaneProfile.DEEP_AGENTS: _DEEP_AGENTS,
    LaneProfile.CURSOR_LOCAL: _CURSOR_LOCAL,
    LaneProfile.CURSOR_CLOUD: _CURSOR_CLOUD,
    LaneProfile.CLAUDE_AGENT_SDK: _CLAUDE,
    LaneProfile.CODEX: _CODEX,
    LaneProfile.CLAUDE_CLOUD: _CLAUDE_CLOUD,
    LaneProfile.CODEX_CLOUD: _CODEX_CLOUD,
}

if set(HOOK_EVENT_MAPPING) != set(LaneProfile):  # pragma: no cover - import-time guard
    raise RuntimeError("HOOK_EVENT_MAPPING must cover every LaneProfile")


def native_hook(profile: LaneProfile | str, event: HookEvent | str) -> NativeHook | None:
    return HOOK_EVENT_MAPPING[LaneProfile(profile)].get(HookEvent(event))


def unsupported_events(
    profile: LaneProfile | str, events: Iterable[HookEvent | str]
) -> tuple[HookEvent, ...]:
    mapping = HOOK_EVENT_MAPPING[LaneProfile(profile)]
    return tuple(sorted({HookEvent(event) for event in events if HookEvent(event) not in mapping}))


# Mission Control's kernel hooks, in their fixed composition order (SPEC-01 "Kernel hooks").
KERNEL_HOOK_IDS: tuple[str, ...] = (
    "mc.stop_fence",
    "mc.operation_intent",
    "mc.frame_capture",
    "mc.usage",
)

_GATED = (E.BEFORE_TOOL, E.BEFORE_SHELL, E.BEFORE_MCP, E.SUBAGENT_START)
KERNEL_HOOK_EVENTS: Mapping[str, tuple[HookEvent, ...]] = {
    "mc.stop_fence": _GATED,
    "mc.operation_intent": _GATED,
    "mc.frame_capture": tuple(HookEvent),
    "mc.usage": (E.AFTER_MODEL, E.AFTER_TOOL, E.STOP),
}
