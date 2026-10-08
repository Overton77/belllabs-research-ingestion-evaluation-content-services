---
type: Decision Record
title: "Hook scripts are catalog capabilities bound to one provider-neutral hook event vocabulary, projected to Cursor and Claude command hooks and to Deep Agents middleware; kernel hooks always run first"
description: "A hook script declares the mc.hook_event it runs at, reads a mc.hook_input.v1 JSON on stdin and writes a mc.hook_result.v1 JSON; projection writes the lane's native hook file (.cursor/hooks.json, Claude settings hooks, .codex/hooks.json) or wraps the script in a HookScriptMiddleware for Deep Agents; Mission Control's own kernel hooks (stop fence, operation intent, usage, frame capture) are attached before any catalog hook and cannot be removed by a manifest."
tags: [mission-control, adr, decision, capabilities, hooks]
status: accepted
source: fast-track interview 2026-10-07 (requirement 1, hook scripts and middleware); docs/research/2026-10-07-coding-lane-surfaces.md (hook surfaces per provider); expansion/CONTEXT-STATE-AND-CONTROL.md (exact middleware placement); docs/specs/fast-track-2026-10/research/deepagents-middleware.md (wrap_tool_call only; dcode command-hook middleware as template); docs/specs/fast-track-2026-10/research/cursor-platform.md (hooks.json schema; headless local runs auto-approve)
---

# Hook scripts are catalog capabilities bound to one provider-neutral hook event vocabulary, projected to Cursor and Claude command hooks and to Deep Agents middleware; kernel hooks always run first

Every coding host already runs hooks as spawned processes with a JSON contract on stdin and stdout, and Deep Agents exposes the same points as middleware methods. We define one event vocabulary (`session_start`, `session_end`, `before_prompt`, `before_model`, `after_model`, `before_tool`, `after_tool`, `after_tool_failure`, `before_shell`, `after_shell`, `after_file_edit`, `before_compaction`, `after_compaction`, `subagent_start`, `subagent_stop`, `stop`) and one result contract (`decision: allow|deny|defer`, `updated_input`, `additional_context`, `message`). A hook script capability pins a script directory, its event list, matcher, timeout, `fail_closed` flag and side-effect class. Projection maps each event to the lane's native event name and writes the native hook file; events a lane lacks are reported in the Validation Report as `unsupported_on_lane`. For Deep Agents the same script runs from a `HookScriptMiddleware` whose `before_agent`, `before_model`, `after_model` and `wrap_tool_call` methods marshal the identical JSON (there is no `before_tool` method in langchain 1.4; `wrap_tool_call` covers before and after), and compaction events come from Mission Control's own summarization wrapper because the stock middleware exposes no callback. Kernel hooks implement the specification's mandatory guards (stop fence check and Operation Intent before a side effect, usage accounting, provider frame capture) and are composed first, in a fixed order, by Mission Control rather than by the manifest; on Cursor Local they are `fail_closed` command hooks because headless runs auto-approve every tool call.

We rejected letting manifests ship arbitrary provider-specific hook files because they would bypass the catalog's admission and side-effect classification, and we rejected an in-process Python plugin API as the only option because Cursor can only run command hooks.

## Consequences

- A hook script's `deny` is honoured on every lane; `defer` means `pause_at_tool_gate` on Claude and `deny` on Cursor, and `describe` reports that.
- Hook stdin never carries secrets; a hook that needs the service calls back with a task-scoped token.
- Hook invocations are provider frames and appear in the transcript.
