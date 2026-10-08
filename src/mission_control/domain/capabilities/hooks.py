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

_CODEX: dict[HookEvent, NativeHook] = {
    event: (
        _N(native_event="PostToolUse", matcher="apply_patch")
        if event == E.AFTER_FILE_EDIT
        else hook
    )
    for event, hook in _CLAUDE.items()
    if event != E.AFTER_TOOL_FAILURE
}

HOOK_EVENT_MAPPING: Mapping[LaneProfile, Mapping[HookEvent, NativeHook]] = {
    LaneProfile.DEEP_AGENTS: _DEEP_AGENTS,
    LaneProfile.CURSOR_LOCAL: _CURSOR_LOCAL,
    LaneProfile.CURSOR_CLOUD: _CURSOR_CLOUD,
    LaneProfile.CLAUDE_AGENT_SDK: _CLAUDE,
    LaneProfile.CODEX: _CODEX,
}


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
