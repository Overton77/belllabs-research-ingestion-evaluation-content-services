"""The `claude_agent_sdk` describe matrix this adapter implements (MP-07; SPEC-01 "Claude local").

The declared matrix lives in `application/execution/harness/describe.CLAUDE_AGENT_SDK_DESCRIBE`
(integrated from this adapter's MP-07 proposal); this module re-exports it as the harness's
default describe and records, below, the SDK source behind each cell: which controls the
harness in `harness.py` implements natively, which it emulates, and which stay `unqualified`
because no code exists or the SDK offers no surface. Nothing is `qualified`: a qualified cell
needs the owner-run bounded live drill recorded under `docs/qualification/lanes/claude_agent_sdk/`.

Every statement below is backed by the pinned `claude_agent_sdk==0.2.165` source:

- `start`/`send_turn`/`observe`: `claude_agent_sdk.client.ClaudeSDKClient.connect`, `.query`,
  `.receive_messages` (streaming input mode, one subprocess per session).
- `cancel_turn`: `ClaudeSDKClient.interrupt` -> `_internal.query.Query.interrupt`
  (`{"subtype": "interrupt"}` control request); the aborted turn's `ResultMessage` carries
  `terminal_reason` `aborted_streaming` / `aborted_tools` (`types.ResultMessage`).
- `reattach` is *emulated*: the agent loop lives in the worker's subprocess and there is no
  attach-to-running-process API; recovery is `ClaudeAgentOptions.resume=<session_id>` into a
  fresh process (`types.ClaudeAgentOptions.resume`), which is a new connection/generation.
- `usage`: `ResultMessage.usage` (per turn, main loop only) and `total_cost_usd` (a client-side
  estimate, per `types.ResultMessage` / docs cost-tracking) -> tokens settled, cost estimated.
- `snapshot`: emulated through the MP-04 workspace allocator (git patch custody).
- `pause`: the SDK has no pause; the `PreToolUse` `permissionDecision: "defer"` path
  (`types.PreToolUseHookSpecificOutput`) is not implemented here, so the cell stays
  `unqualified` (and `pause` is a typed `unsupported_control` mid-run).
- `fork`: `ClaudeAgentOptions.fork_session` exists for the conversation only; workspace fork
  hydration is not implemented here -> `unqualified`.
- approvals: `ClaudeAgentOptions.can_use_tool` (`types.CanUseTool`) binds provider permission
  requests; MCP elicitation is not forwarded by the Python SDK (`types.McpSdkServerConfig`
  docstring: "Requests ... the server sends to the client (sampling, elicitation, ...) are not
  forwarded yet"), and no user-question callback exists in the Python type surface, so
  `provider_question` and `mcp_elicitation` are not claimed.
- compaction: `PreCompact` is a Python hook event (`types.HookEvent`) and is observed as a
  `before_compaction` frame; there is no `PostCompact` callback and no compaction control
  request in `Query`, so `compaction_control` stays `unqualified` (observation, no control).
- subordinates: `AssistantMessage.parent_tool_use_id` / `Task*Message.tool_use_id` give the
  spawn identity; with `forward_subagent_text` the SDK also forwards text, but only the tool
  and task lifecycle is proven by fixtures here -> `lifecycle_only`.
"""

from __future__ import annotations

from typing import Final

from mission_control.application.execution.harness.describe import (
    CLAUDE_AGENT_SDK_DESCRIBE,
    CLAUDE_BUNDLED_CLI_VERSION,
    CLAUDE_SDK_VERSION,
)

PROFILE: Final = "claude_agent_sdk"
SDK_VERSION: Final = CLAUDE_SDK_VERSION
# `claude_agent_sdk._cli_version.__cli_version__`: the CLI bundled with the pinned SDK.
BUNDLED_CLI_VERSION: Final = CLAUDE_BUNDLED_CLI_VERSION
TRANSPORT: Final = "stdio_subprocess"
SDK_LANGUAGE: Final = "python"
# Local CLI lanes run under WSL/Linux; the Windows worker runs a SelectorEventLoop without
# subprocess support.
WORKER_OS: Final = "linux"
# The settings-file hook vocabulary of Claude Code (`domain/capabilities/hooks._CLAUDE`); the
# in-process subset the Python SDK exposes is what the Kernel Hooks of this lane use (`hooks.py`).
CLAUDE_HOOK_EVENTS: Final = CLAUDE_AGENT_SDK_DESCRIBE.hooks.events_supported
CLAUDE_AGENT_SDK_LANE_DESCRIBE: Final = CLAUDE_AGENT_SDK_DESCRIBE


__all__ = [
    "BUNDLED_CLI_VERSION",
    "CLAUDE_AGENT_SDK_LANE_DESCRIBE",
    "CLAUDE_HOOK_EVENTS",
    "PROFILE",
    "SDK_VERSION",
    "TRANSPORT",
    "WORKER_OS",
]
