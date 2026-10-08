"""Cursor hook payloads to `mc.hook_input.v1` (SPEC-07 section 5.4; FT-G3).

`CursorHookMapper` is the `NativeHookMapper` of both Cursor profiles: it reads the documented
stdin payload of each Cursor hook event (research/cursor-platform.md section 2.3), keeps the
raw payload only as a digest, and names the side effect a permission event gates:

| Cursor event | `mc.hook_event` | effect ref |
| --- | --- | --- |
| `preToolUse` | `before_tool` | `tool_use:<tool_use_id>` |
| `beforeShellExecution` | `before_shell` | `shell:<digest(conversation, generation, cwd, cmd)>` |
| `beforeMCPExecution` | `before_mcp` | `mcp:<digest(server, tool, input, generation)>` |
| `subagentStart` | `subagent_start` | `subagent:<subagent_id or tool_call_id>` |

Observation events (`postToolUse`, `afterShellExecution`, `afterMCPExecution`,
`postToolUseFailure`, `afterFileEdit`, `subagentStop`, `preCompact`, `stop`, `sessionStart`,
`sessionEnd`, `beforeSubmitPrompt`, `afterAgentResponse`) carry their fields as excerpts and
digests. The kernel hook script itself is `kernel_hook_script.py`, written into the lease as
`.mission/hooks/kernel.py`.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Final

from mission_control.application.execution.harness.hook_callbacks import HookTokenContext
from mission_control.contracts.hooks import HookInput, HookScope, HookToolCall
from mission_control.contracts.identities import parse_request_scope
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.capabilities.hooks import HookEvent
from mission_control.domain.capabilities.host_support import LaneProfile
from mission_control.domain.policies.stop_fence import EffectKind

KERNEL_HOOK_SCRIPT_SOURCE: Final = Path(__file__).with_name("kernel_hook_script.py")
EXCERPT_CHARS: Final = 2_000

# Cursor's native hook event -> the provider-neutral vocabulary (SPEC-07 section 5.4).
CURSOR_HOOK_EVENTS: Final[dict[str, HookEvent]] = {
    "sessionStart": HookEvent.SESSION_START,
    "beforeSubmitPrompt": HookEvent.BEFORE_PROMPT,
    "preToolUse": HookEvent.BEFORE_TOOL,
    "beforeShellExecution": HookEvent.BEFORE_SHELL,
    "beforeMCPExecution": HookEvent.BEFORE_MCP,
    "postToolUse": HookEvent.AFTER_TOOL,
    "afterShellExecution": HookEvent.AFTER_SHELL,
    "afterMCPExecution": HookEvent.AFTER_TOOL,
    "postToolUseFailure": HookEvent.AFTER_TOOL_FAILURE,
    "afterFileEdit": HookEvent.AFTER_FILE_EDIT,
    "subagentStart": HookEvent.SUBAGENT_START,
    "subagentStop": HookEvent.SUBAGENT_STOP,
    "preCompact": HookEvent.BEFORE_COMPACTION,
    "stop": HookEvent.STOP,
    "sessionEnd": HookEvent.SESSION_END,
    "afterAgentResponse": HookEvent.AFTER_MODEL,
}

_FILE_TOOLS = {"write", "edit", "delete", "edit_file", "write_file", "multiedit"}


def kernel_hook_script() -> bytes:
    """The bytes of `.mission/hooks/kernel.py` (LF-normalized, identical on every checkout)."""

    return KERNEL_HOOK_SCRIPT_SOURCE.read_bytes().replace(b"\r\n", b"\n")


def _first(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value is not None and value != "":
            return value
    return None


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _as_input(value: Any) -> dict[str, Any]:
    value = _json_value(value)
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    return {} if value is None else {"value": value}


def _excerpt(value: Any) -> str | None:
    if value is None:
        return None
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return text[:EXCERPT_CHARS]


def _digest(*parts: Any) -> str:
    joined = "\x1f".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()[:32]


def tool_effect_kind(tool_name: str | None) -> EffectKind:
    name = (tool_name or "").strip()
    lowered = name.lower()
    if lowered in {"shell", "bash", "terminal"}:
        return "shell"
    if lowered.startswith(("mcp:", "mcp__")):
        return "mcp"
    if lowered in _FILE_TOOLS:
        return "file"
    if lowered in {"task", "agent"}:
        return "task"
    return "other"


def _scope(context: HookTokenContext) -> HookScope:
    try:
        parsed = parse_request_scope(context.request_scope)
    except ValueError:
        return HookScope(installation_id=context.request_scope, application_id="unbound")
    return HookScope(
        installation_id=str(parsed.installation_id),
        application_id=parsed.application_id,
        tenant_id=str(parsed.tenant_id),
    )


class CursorHookMapper:
    """`NativeHookMapper` for `cursor_local` and `cursor_cloud`."""

    def tool(self, event: HookEvent, payload: Mapping[str, Any]) -> HookToolCall | None:
        if event in {HookEvent.BEFORE_TOOL, HookEvent.AFTER_TOOL, HookEvent.AFTER_TOOL_FAILURE}:
            name = _first(payload, "tool_name", "toolName")
            if name is None:
                return None
            failure = event == HookEvent.AFTER_TOOL_FAILURE
            return HookToolCall(
                name=str(name),
                call_id=_first(payload, "tool_use_id", "tool_call_id"),
                input=_as_input(_first(payload, "tool_input")),
                side_effect_class=tool_effect_kind(str(name)),
                output_excerpt=_excerpt(
                    _first(payload, "tool_output", "result_json") if not failure else None
                ),
                error=_excerpt(_first(payload, "error_message")) if failure else None,
            )
        if event in {HookEvent.BEFORE_SHELL, HookEvent.AFTER_SHELL}:
            return HookToolCall(
                name="shell",
                input={
                    "command": str(payload.get("command", "")),
                    "cwd": payload.get("cwd"),
                    "sandbox": payload.get("sandbox"),
                },
                side_effect_class="shell",
                output_excerpt=_excerpt(payload.get("output"))
                if event == HookEvent.AFTER_SHELL
                else None,
            )
        if event == HookEvent.BEFORE_MCP:
            name = str(_first(payload, "tool_name") or "mcp")
            return HookToolCall(
                name=name,
                input=_as_input(payload.get("tool_input")),
                side_effect_class="mcp",
            )
        if event == HookEvent.AFTER_FILE_EDIT:
            edits = payload.get("edits") or []
            return HookToolCall(
                name="edit_file",
                input={"file_path": payload.get("file_path"), "edit_count": len(edits)},
                side_effect_class="file",
            )
        if event in {HookEvent.SUBAGENT_START, HookEvent.SUBAGENT_STOP}:
            return HookToolCall(
                name="task",
                call_id=_first(payload, "tool_call_id", "subagent_id"),
                input={
                    "subagent_type": payload.get("subagent_type"),
                    "task": _excerpt(payload.get("task")),
                },
                side_effect_class="task",
            )
        return None

    def details(self, event: HookEvent, payload: Mapping[str, Any]) -> dict[str, Any]:
        if event == HookEvent.SESSION_START:
            return {
                "session_id": payload.get("session_id"),
                "is_background_agent": payload.get("is_background_agent"),
            }
        if event == HookEvent.BEFORE_PROMPT:
            return {"prompt_digest": sha256_digest(str(payload.get("prompt", "")))}
        if event == HookEvent.BEFORE_MCP:
            return {"mcp_server_name": payload.get("mcp_server_name")}
        if event == HookEvent.AFTER_TOOL_FAILURE:
            return {
                "failure_type": payload.get("failure_type"),
                "is_interrupt": payload.get("is_interrupt"),
            }
        if event == HookEvent.SUBAGENT_STOP:
            return {
                "status": payload.get("status"),
                "modified_files": list(payload.get("modified_files") or ())[:100],
            }
        if event == HookEvent.BEFORE_COMPACTION:
            return {
                "trigger": payload.get("trigger"),
                "context_usage_percent": payload.get("context_usage_percent"),
                "message_count": payload.get("message_count"),
            }
        if event == HookEvent.STOP:
            return {"status": payload.get("status"), "loop_count": payload.get("loop_count")}
        if event == HookEvent.SESSION_END:
            return {"reason": payload.get("reason"), "final_status": payload.get("final_status")}
        if event in {HookEvent.AFTER_TOOL, HookEvent.AFTER_SHELL}:
            return {"duration": payload.get("duration")}
        return {}

    def hook_input(
        self, event: HookEvent, payload: Mapping[str, Any], context: HookTokenContext
    ) -> HookInput:
        details = {
            key: value for key, value in self.details(event, payload).items() if value is not None
        }
        details["provider_event"] = _first(payload, "hook_event_name") or event.value
        if _first(payload, "cursor_version"):
            details["cursor_version"] = payload["cursor_version"]
        return HookInput(
            event=event,
            lane_profile=LaneProfile(context.lane_profile),
            scope=_scope(context),
            run_id=context.run_id,
            attempt_no=context.attempt_no,
            generation=context.generation,
            harness_execution_id=str(context.harness_execution_id),
            native_session_ref=_first(payload, "conversation_id", "session_id"),
            native_turn_ref=_first(payload, "generation_id"),
            tool=self.tool(event, payload),
            workspace_root=context.workspace_root,
            provider_payload_digest=sha256_digest(dict(payload)),
            details=details,
        )

    def effect(self, event: HookEvent, payload: Mapping[str, Any]) -> tuple[str, EffectKind] | None:
        if event == HookEvent.BEFORE_TOOL:
            tool_use_id = _first(payload, "tool_use_id", "tool_call_id")
            if tool_use_id is None:
                return None
            return f"tool_use:{tool_use_id}", tool_effect_kind(_first(payload, "tool_name"))
        if event == HookEvent.BEFORE_SHELL:
            return (
                "shell:"
                + _digest(
                    _first(payload, "conversation_id"),
                    _first(payload, "generation_id"),
                    payload.get("cwd"),
                    payload.get("command"),
                ),
                "shell",
            )
        if event == HookEvent.BEFORE_MCP:
            return (
                "mcp:"
                + _digest(
                    payload.get("mcp_server_name"),
                    payload.get("tool_name"),
                    payload.get("tool_input"),
                    _first(payload, "generation_id"),
                ),
                "mcp",
            )
        if event == HookEvent.SUBAGENT_START:
            ref = _first(payload, "subagent_id", "tool_call_id")
            return (f"subagent:{ref}", "task") if ref is not None else None
        return None


__all__ = [
    "CURSOR_HOOK_EVENTS",
    "KERNEL_HOOK_SCRIPT_SOURCE",
    "CursorHookMapper",
    "kernel_hook_script",
    "tool_effect_kind",
]
