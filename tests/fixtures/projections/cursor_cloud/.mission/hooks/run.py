"""Mission Control hook runner for file-based lanes: ``.mission/hooks/run.py`` (SPEC-01).

The Host Projection writes this file verbatim next to each catalog hook's directory. A lane's
native hook system (Cursor ``hooks.json``, Claude ``settings.json``, Codex ``hooks.json``)
calls ``python .mission/hooks/run.py <hook_slug> <mc_event>``; the runner turns the native
stdin payload into ``mc.hook_input.v1``, runs the hook script described by
``.mission/hooks/<hook_slug>/hook.json`` with a scrubbed environment and its timeout, reads
``mc.hook_result.v1`` (exit 2 denies), and answers in the lane's native output shape. Kernel
hooks use ``kernel.py`` (SPEC-07) instead. Standard library only: the workspace's Python runs
it without Mission Control installed.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

SAFE_ENV = ("PATH", "SYSTEMROOT", "SystemRoot", "TEMP", "TMP", "HOME", "USERPROFILE", "LANG")
PERMISSION_EVENTS = {
    "before_tool",
    "before_shell",
    "before_mcp",
    "subagent_start",
    "before_prompt",
}
INTERPRETERS = {"python": sys.executable, "sh": "sh", "bash": "bash", "node": "node"}
CLAUDE_EVENT = {
    "session_start": "SessionStart",
    "session_end": "SessionEnd",
    "before_prompt": "UserPromptSubmit",
    "before_tool": "PreToolUse",
    "before_shell": "PreToolUse",
    "before_mcp": "PreToolUse",
    "after_tool": "PostToolUse",
    "after_shell": "PostToolUse",
    "after_file_edit": "PostToolUse",
    "after_tool_failure": "PostToolUseFailure",
    "before_compaction": "PreCompact",
    "after_compaction": "PostCompact",
    "subagent_start": "SubagentStart",
    "subagent_stop": "SubagentStop",
    "stop": "Stop",
}


def _first(payload: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in payload and payload[key] is not None:
            return payload[key]
    return None


def native_tool(event: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    """Normalize Cursor, Claude and Codex tool payloads into ``mc.hook_input.v1`` ``tool``."""
    name = _first(payload, "tool_name", "toolName")
    tool_input = _first(payload, "tool_input", "toolInput", "input")
    if name is None and event in {"before_shell", "after_shell"}:
        name, tool_input = "shell", {"command": payload.get("command", "")}
    if name is None and event == "after_file_edit":
        name, tool_input = "edit_file", {"file_path": payload.get("file_path", "")}
    if name is None and event == "before_mcp":
        name = str(payload.get("tool_name") or payload.get("server") or "mcp")
    if name is None:
        return None
    tool: dict[str, Any] = {
        "name": str(name),
        "input": tool_input if isinstance(tool_input, dict) else {"value": tool_input},
    }
    call_id = _first(payload, "tool_use_id", "tool_call_id", "toolCallId")
    if call_id is not None:
        tool["call_id"] = str(call_id)
    output = _first(payload, "tool_output", "tool_response", "output")
    if output is not None:
        tool["output_excerpt"] = (output if isinstance(output, str) else json.dumps(output))[:4000]
    error = _first(payload, "error", "error_message")
    if error is not None:
        tool["error"] = str(error)[:4000]
    return tool


def build_input(
    event: str, lane_profile: str, payload: dict[str, Any], context: dict[str, Any]
) -> dict[str, Any]:
    hook_input: dict[str, Any] = {
        "schema_version": "mc.hook_input.v1",
        "event": event,
        "lane_profile": lane_profile,
        "scope": context.get("scope")
        or {"installation_id": "unbound", "application_id": "unbound"},
        "workspace_root": context.get("workspace_root") or str(Path.cwd()),
    }
    for key in (
        "run_id",
        "activation_id",
        "attempt_no",
        "generation",
        "harness_execution_id",
        "callback",
    ):
        if context.get(key) is not None:
            hook_input[key] = context[key]
    session = _first(payload, "session_id", "conversation_id", "agent_id")
    if session is not None:
        hook_input["native_session_ref"] = str(session)
    turn = _first(payload, "generation_id", "turn_id", "run_id")
    if turn is not None:
        hook_input["native_turn_ref"] = str(turn)
    tool = native_tool(event, payload)
    if tool is not None:
        hook_input["tool"] = tool
    return hook_input


def run_script(descriptor: dict[str, Any], directory: Path, stdin: bytes) -> dict[str, Any]:
    """Run the hook script; return a ``mc.hook_result.v1`` dict, resolving failures."""
    fail_closed = bool(descriptor.get("fail_closed", False))
    hook_id = str(descriptor.get("hook_id", directory.name))
    interpreter = INTERPRETERS.get(str(descriptor.get("interpreter")), "")
    entrypoint = (directory / str(descriptor.get("entrypoint", ""))).resolve()
    if not interpreter or directory.resolve() not in entrypoint.parents or not entrypoint.is_file():
        return _failure(hook_id, "hook descriptor is invalid", fail_closed)
    env = {name: os.environ[name] for name in SAFE_ENV if name in os.environ}
    try:
        completed = subprocess.run(
            [interpreter, str(entrypoint)],
            input=stdin,
            capture_output=True,
            env=env,
            timeout=int(descriptor.get("timeout_seconds", 30)),
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        return _failure(hook_id, f"hook could not run: {type(error).__name__}", fail_closed)
    text = completed.stdout.decode("utf-8", "replace").strip()
    parsed: dict[str, Any] | None = None
    if text:
        try:
            candidate = json.loads(text)
            parsed = candidate if isinstance(candidate, dict) else None
        except ValueError:
            parsed = None
    if completed.returncode == 2:
        result = dict(parsed or {})
        result.update(decision="deny", reason=result.get("reason") or f"hook {hook_id} denied")
        return result
    if completed.returncode != 0 or (text and parsed is None):
        return _failure(hook_id, f"hook exited {completed.returncode}", fail_closed)
    result = parsed or {"decision": "allow"}
    if result.get("decision") not in {"allow", "deny", "defer"}:
        return _failure(hook_id, "hook wrote an invalid decision", fail_closed)
    return result


def _failure(hook_id: str, reason: str, fail_closed: bool) -> dict[str, Any]:
    if fail_closed:
        return {"decision": "deny", "reason": f"{hook_id}: {reason}"}
    return {"decision": "allow"}


def native_output(event: str, lane_profile: str, result: dict[str, Any]) -> tuple[str, int]:
    """Translate a merged ``mc.hook_result.v1`` into the lane's native stdout and exit code."""
    decision = result.get("decision", "allow")
    reason = result.get("reason")
    context = result.get("additional_context")
    message = result.get("message")
    if lane_profile in {"cursor_local", "cursor_cloud"}:
        # Headless Cursor does not enforce "ask", so defer becomes deny.
        out: dict[str, Any] = {}
        refused = decision in {"deny", "defer"}
        if event == "before_prompt":
            out["continue"] = not refused
        elif event in PERMISSION_EVENTS:
            out["permission"] = "deny" if refused else "allow"
        if refused and reason:
            out["agent_message"] = reason
        if message:
            out["user_message"] = message
        if result.get("updated_input") is not None:
            out["updated_input"] = result["updated_input"]
        if context:
            out["additional_context"] = context
        return json.dumps(out), 2 if refused and event in PERMISSION_EVENTS else 0
    native = CLAUDE_EVENT.get(event, "")
    specific: dict[str, Any] = {"hookEventName": native}
    out = {}
    if native == "PreToolUse":
        permission = decision
        if decision == "defer" and lane_profile == "codex":
            permission = "deny"  # Codex marks "ask" failed; defer cannot pause there
        specific["permissionDecision"] = permission
        if reason:
            specific["permissionDecisionReason"] = reason
        if result.get("updated_input") is not None:
            specific["updatedInput"] = result["updated_input"]
    elif decision in {"deny", "defer"}:
        out["decision"] = "block"
        out["reason"] = reason or "blocked by Mission Control hook"
    if context:
        specific["additionalContext"] = context
    if message:
        out["systemMessage"] = message
    if len(specific) > 1:
        out["hookSpecificOutput"] = specific
    refused = decision == "deny" or (decision == "defer" and lane_profile == "codex")
    return json.dumps(out), 2 if refused and native != "PreToolUse" else 0


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        sys.stderr.write("usage: run.py <hook_slug> <mc_event>\n")
        return 2
    _, slug, event = argv
    root = Path(__file__).resolve().parent
    directory = (root / slug).resolve()
    lane = "cursor_local"
    try:
        if root not in directory.parents:
            raise ValueError("hook slug escapes .mission/hooks")
        descriptor = json.loads((directory / "hook.json").read_text(encoding="utf-8"))
        context_file = root / "context.json"
        context = (
            json.loads(context_file.read_text(encoding="utf-8")) if context_file.is_file() else {}
        )
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
        if not isinstance(payload, dict):
            payload = {"value": payload}
        lane = str(descriptor.get("lane_profile", "cursor_local"))
        hook_input = build_input(event, lane, payload, context)
        result = run_script(
            descriptor, directory, (json.dumps(hook_input, sort_keys=True) + "\n").encode()
        )
    except (OSError, ValueError) as error:
        result = {"decision": "deny", "reason": f"hook runner failed: {error}"}
    text, code = native_output(event, lane, result)
    sys.stdout.write(text + "\n")
    return code


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
