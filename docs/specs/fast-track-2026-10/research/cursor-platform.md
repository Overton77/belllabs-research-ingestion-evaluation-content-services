---
type: Research Note
title: "Cursor platform fact sheet for a Python Mission Control lane (read 2026-10-07)"
description: "Primary-source gaps for cursor_local and cursor_cloud: .cursor/agents subagents, rules, skills, hooks.json schema and payloads, mcp.json, Python SDK 1.0.37 signatures and local store, Cloud Agents API v1 create/stream/artifacts/usage/workers, headless CLI, conversation retrieval, and ten implications for the lane."
tags: [mission-control, research, fast-track]
---

# Cursor platform fact sheet: gaps for a Python Mission Control lane

- **Date:** 2026-10-07. Every source was read on this date.
- **Scope:** This sheet fills gaps 1 to 6 for the two Mission Control placements:
  - **LOCAL:** the `cursor-sdk` Python package plus the `cursor-sdk-bridge` binary, with the agent loop running on our worker.
  - **CLOUD:** Cursor Cloud Agents through REST v1 and the Python SDK cloud runtime.
- **Not covered:** The existing harness-mapping note already covers lifecycle, turn control, the hooks list, transcript events, usage and placement for the cursor-sdk 1.0.37 pins. This sheet does not repeat that material.
- **Evidence levels:**
  - **[DOC]**: the claim comes from cursor.com/docs or the public OpenAPI spec.
  - **[SRC]**: the claim comes from reading the `cursor_sdk-1.0.37-py3-none-win_amd64.whl` wheel from PyPI (uploaded 2026-10-07T20:05Z). That covers its Python sources, its vendored bridge `dist/*.js`, its `proto/sdk/v1/*.proto` files, and its bundled `@cursor/sdk@1.0.37` typings and JS.
  - **UNVERIFIED**: no primary source confirms the claim.
- **Versions:**
  - PyPI `cursor-sdk` latest is **1.0.37** (2026-10-07). 1.0.36 was released 2026-10-05. It needs Python >=3.10 and is under a Proprietary license.
  - The bundled bridge `manifest.json` reports `bridgeVersion 1.0.0`, `sdkVersion 1.0.37` and `protocol sdk.v1`.
  - GitHub `cursor/sdk-bridge` (MIT) publishes matching tags v1.0.36 and v1.0.37. Each release ships standalone tarballs for darwin-arm64/x64, linux-arm64/x64 and win32-x64, plus `SHA256SUMS.txt`.
  - The public SDK release notes were last cached at 18:02Z and only go up to 1.0.35. Release notes for 1.0.36 and 1.0.37 were not available: **UNVERIFIED**.

---

## 1. Cursor subagents (`.cursor/agents/`)

### 1.1 File locations [DOC: cursor.com/docs/subagents]

| Scope | Paths | Notes |
|---|---|---|
| Project | `.cursor/agents/`, `.claude/agents/`, `.codex/agents/` | Applies to the current project only. |
| User | `~/.cursor/agents/`, `~/.claude/agents/`, `~/.codex/agents/` | Applies to all of the user's projects. |

- When names conflict, project subagents win over user subagents.
- At the same scope, `.cursor/` wins over `.claude/` and `.codex/`.
- Background subagents write their output to `~/.cursor/subagents/`.

### 1.2 File format [DOC]

A subagent file is Markdown with YAML frontmatter. The body is the subagent's prompt.

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `name` | string | no | derived from the filename | Identifier. Use lowercase letters and hyphens. |
| `description` | string | no | — | Shown in Task tool hints. The parent reads it to decide whether to delegate. |
| `model` | string | no | `inherit` | Either `inherit` or a model ID. Model ID params are supported, for example `claude-opus-5[effort=high,context=300k]` or `composer-2.5[fast=false]`. |
| `readonly` | boolean | no | `false` | Blocks file edits and state-changing shell commands. |
| `is_background` | boolean | no | `false` | Runs the subagent without blocking the parent. |

- **No per-subagent tool allowlist in the frontmatter.** The only documented fields are the five above. A `tools:` field (as in Claude Code) is **not documented**, so whether it is honored is **UNVERIFIED**.
- **Model fallbacks.** The configured model is replaced with a compatible one when:
  - a team admin blocks it,
  - the plan excludes it, or
  - the account is on a legacy request plan without Max Mode, in which case Composer is used.

### 1.3 Complete example: `.cursor/agents/verifier.md`

```markdown
---
name: verifier
description: Validates completed work. Use proactively after any task is marked done to confirm the implementation is functional and tests pass.
model: inherit
readonly: true
is_background: false
---

You are a skeptical validator. Your job is to verify that work claimed as complete actually works.

When invoked:
1. Identify what was claimed to be completed.
2. Check that the implementation exists and is functional.
3. Run the relevant tests or verification steps.
4. Look for edge cases that may have been missed.

Report:
- What was verified and passed
- What was claimed but is incomplete or broken
- Specific issues that need to be addressed

Do not accept claims at face value. Test everything.
```

### 1.4 How subagents are invoked [DOC]

- **Automatic delegation.** The parent decides based on task complexity, the subagent `description`, and the current context. Phrases like "use proactively" or "always use for" in the description encourage delegation.
- **Explicit invocation.** Use `/name` in the prompt, for example `/verifier confirm the auth flow`, or mention the subagent naturally ("Use the verifier subagent...").
- **Mechanism.** Subagents are exposed through the **Task tool**. The Python SDK docs call it the "`Agent` tool". Parallel delegation means sending several Task calls in one message.
- **Nesting.** Since Cursor 2.5, subagents can nest one level:
  - The main agent and its direct children can launch subagents.
  - Grandchildren cannot launch further subagents.
  - Disallowing `"task"` in the SDK `disallowed_tools` prevents subagents entirely.
- **Foreground vs background.**
  - A foreground subagent blocks until it returns.
  - A background subagent returns immediately.
  - In SDK **local** runs, background subagent results come back as a follow-up turn on the same run. `run.messages()` keeps yielding and `run.wait()` returns after them. This was added in 1.0.31 and applies to local runs only.
- **Isolation.**
  - By default, subagents share the parent's checkout.
  - When asked, each subagent gets its own worktree, branch and VM.
  - `/in-cloud` hands work off to a cloud subagent. This is desktop Agents Window only.
- **Built-in subagents.** `explore`, `bash`/`shell` and `browser`. Hook matchers also see `generalPurpose`. Custom subagent names may not collide with built-ins; the REST docs list `explore`, `shell`, `debug`, `computerUse` and `cursorGuide`.
- **Resume.** Each subagent execution returns an agent ID, and the subagent can be resumed by that ID.
- **Hooks.** `subagentStart` and `subagentStop` gate and observe subagents (see §2.3).

### 1.5 Python SDK: `AgentOptions.agents` [DOC: /docs/sdk/python#subagents; SRC: types.py]

The type is `Mapping[str, AgentDefinition | Mapping[str, Any]]`. It holds **inline definitions keyed by name**, not references to files.

```python
@dataclass(frozen=True)
class AgentDefinition:
    description: str                      # required
    prompt: str                           # required (the subagent's system prompt)
    model: str | ModelSelection | Mapping | Literal["inherit"] | None = None   # None or "inherit" -> parent's model
    mcp_servers: Sequence[str | AgentDefinitionMcpServer | Mapping] | None = None
    # str = name of a server in the parent's mcp_servers; AgentDefinitionMcpServer.inline(cfg) = inline config
```

- **Fields the inline form lacks.** There is no `readonly` and no `is_background`, and the proto `AgentDefinition` has no such fields either [SRC]. To get read-only or background behavior, use a file-based `.cursor/agents/*.md`.
- **Merging with files.** File-based `.cursor/agents/*.md` subagents are also picked up. An inline definition overrides a file definition with the same name [DOC].
- **Local gating.** For local agents, file-based subagents are gated by `local.setting_sources` [DOC: "local.setting_sources (and the file-based MCP and subagent paths it gates) does not apply to cloud agents"]. Include `"project"` in `setting_sources`.
- **Cloud mapping.** For cloud agents, the SDK maps `agents` to the REST `customSubagents` field [SRC: @cursor/sdk createServerAgent]. Cloud agents also pick up `.cursor/agents/*.md` from the cloned repo, because cloud always loads `project`, `team` and `plugins` [DOC: TS SDK "Configuration sources at a glance"].
- **Refresh.** `agent.reload()` re-reads hooks, project MCP and subagents without disposing the agent [DOC].

### 1.6 Cloud support [DOC]

- The docs state: "You can use subagents in the editor, CLI, and Cloud Agents."
- REST `POST /v1/agents` accepts `customSubagents[]` with up to 20 entries:
  - `name`: 1 to 100 characters, unique, and not a built-in name.
  - `description`: 1 to 1000 characters.
  - `prompt`: 1 to 8192 characters.
  - `model`: optional. One of `"inherit"`, a model ID, or a `ModelRef`.
- Cloud subagents use team MCP servers from cursor.com/agents, not local servers.

---

## 2. Rules, skills, plugins, hooks and MCP as configuration materialized into the workspace

### 2.1 Rules: `.cursor/rules/**/*.mdc` [DOC: /docs/rules]

- Files must use the `.mdc` extension. A plain `.md` file in `.cursor/rules` is ignored. Subfolders are allowed.
- Frontmatter fields are `description`, `globs` and `alwaysApply`. Together they set the activation mode:

| `alwaysApply` | `description` | `globs` | Behavior |
|---|---|---|---|
| `true` | — | — | Always included. |
| `false` | — | set | Auto-attached when a matching file is in context. |
| `false` | set | omitted | The agent pulls the rule in when it judges it relevant. |
| `false` | omitted | omitted | Included only on an `@rule-name` mention. |

- `globs` takes a comma-separated string, for example `docs/**/*.md, docs/**/*.mdx`.
- `@file` mentions inside a rule are **not inlined**. The agent reads the file with its tools when it needs it.
- **Precedence** is Team Rules, then Project Rules, then User Rules.
- **AGENTS.md:**
  - Plain Markdown with no frontmatter.
  - Read from the project root and **nested subdirectories**. More specific files win and are combined with their parents.
- **User Rules:**
  - Live in Settings or Customize and are not stored on disk.
  - Used only by Agent (Chat).

```markdown
---
description: Mission Control harness contract for this workspace
alwaysApply: true
---
- Only edit files under `src/` and `tests/`.
- Write the final report to `artifacts/report.md`.
```

### 2.2 Skills: `SKILL.md` (Agent Skills open standard) [DOC: /docs/skills]

- **Discovery roots:**
  - Project: `.agents/skills/` and `.cursor/skills/`.
  - User: `~/.agents/skills/` and `~/.cursor/skills/`.
  - Compatibility: `.claude/skills/`, `.codex/skills/`, and the `~` equivalents of both.
- **Scanning:** Roots are scanned recursively, so category folders are allowed. Nested `.cursor/skills/` folders inside subdirectories are auto-scoped to that subtree.
- **Frontmatter:**

| Field | Required | Meaning |
|---|---|---|
| `name` | **yes** | Lowercase letters, digits and hyphens. Must equal the folder name. |
| `description` | **yes** | Used for relevance. |
| `paths` | no | Glob list or comma-separated string. Legacy `globs` is accepted. |
| `disable-model-invocation` | no | When `true`, the skill only runs on an explicit `/skill-name`. |
| `icon` | no | Custom Mode badge styling. |
| `color` | no | Custom Mode badge styling. |
| `metadata` | no | Arbitrary key-value map. |

- **Optional directories:**
  - `scripts/`: executables, referenced by paths relative to the skill root.
  - `references/`: loaded on demand.
  - `assets/`.
- **Cloud availability:**
  - Cloud agents get **project** skills from the repo.
  - `~/.cursor/skills/` reaches cloud only with "Sync Skills for Cloud Agents" turned on.
  - `~/.agents/skills/` never syncs.
  - Self-hosted workers must have skills in the repo or baked into the worker image.

```markdown
---
name: mc-report
description: Write the Mission Control run report. Use at the end of every task.
paths: "src/**, tests/**"
---
# MC report
Run `python scripts/collect.py` then write `artifacts/report.md` using `references/TEMPLATE.md`.
```

### 2.3 Hooks: `.cursor/hooks.json` [DOC: /docs/hooks]

**Config schema**

```json
{
  "version": 1,
  "hooks": {
    "<eventName>": [
      { "command": ".cursor/hooks/x.sh", "type": "command", "timeout": 30,
        "matcher": "regex", "failClosed": false, "loop_limit": 5 }
    ]
  }
}
```

- `version` is required and must be a positive integer (`1`).
- Per-script options:
  - `command`: required.
  - `type`: `command` (default) or `prompt`. A prompt hook uses `prompt` plus an optional `model` and returns `{ok, reason?}`. `$ARGUMENTS` is replaced with the hook input.
  - `timeout`: in seconds.
  - `matcher`: a regex. An empty string or `*` matches everything.
  - `failClosed`: default `false`.
  - `loop_limit`: default 5 for `stop` and `subagentStop`. `null` removes the limit.

**Where config comes from and how responses merge**

- **Sources:** Enterprise (MDM paths), then Team (dashboard, Enterprise only), then Project (`<root>/.cursor/hooks.json`, trusted workspace only), then User (`~/.cursor/hooks.json`).
- **Merging:**
  - All matching hooks run.
  - For permissions, `deny` beats `ask`, which beats `allow`.
  - `user_message` and `agent_message` values are concatenated.
  - For other fields, such as `followup_message`, the last response wins.
- **Working directory:**
  - Project hooks run from the project root, so use `.cursor/hooks/...` paths.
  - User hooks run from `~/.cursor/`.
- **Reload:** Cursor watches the file and reloads it on change. In the SDK, call `agent.reload()`.

**Matchers per event**

| Event | Matcher is tested against |
|---|---|
| `preToolUse`, `postToolUse`, `postToolUseFailure` | Tool type: `Shell`, `Read`, `Write`, `Grep`, `Delete`, `Task`, or `MCP:<tool>` |
| `subagentStart`, `subagentStop` | Subagent type |
| `beforeShellExecution`, `afterShellExecution` | The full command string |
| `beforeReadFile` | `Read` |
| `afterFileEdit` | `Write` |
| `beforeSubmitPrompt` | `UserPromptSubmit` |
| `stop` | `Stop` |
| `afterAgentResponse` | `AgentResponse` |
| `afterAgentThought` | `AgentThought` |

**Exit codes**

| Exit code | Meaning |
|---|---|
| `0` | Use the JSON on stdout. For permission hooks, invalid JSON or a response that fails the schema **blocks** the action. The permission hooks are `beforeShellExecution`, `beforeMCPExecution`, `beforeReadFile`, `beforeTabFileRead`, `subagentStart` and `preToolUse`. |
| `2` | Block the action. Equivalent to `permission: "deny"`, and compatible with Claude Code. |
| Anything else | The hook failed and the action proceeds (fail-open), unless `failClosed: true` is set. |

**Common stdin fields (every agent hook)**

```json
{ "conversation_id": "", "generation_id": "", "model": "", "model_id": "", "model_params": [{"id":"","value":""}],
  "hook_event_name": "", "cursor_version": "", "workspace_roots": ["<path>"], "user_email": null, "transcript_path": null }
```

**Per-event payloads**

| Event | stdin (in addition to the common fields) | stdout |
|---|---|---|
| `beforeShellExecution` | `{command, cwd, sandbox}` | `{permission: allow\|deny\|ask, user_message?, agent_message?}` |
| `afterShellExecution` | `{command, output, duration, sandbox}` | — |
| `beforeMCPExecution` | `{tool_name, tool_input (JSON string), mcp_server_name}`, plus `url` and `mcp_server_url` for HTTP/SSE servers or `command` for stdio servers | Same as `beforeShellExecution` |
| `afterMCPExecution` | `{tool_name, tool_input, mcp_server_name, mcp_server_url?, result_json, duration}` | — |
| `preToolUse` | `{tool_name, tool_input, tool_use_id, cwd, model, model_id, model_params, agent_message}` | `{permission: allow\|deny, user_message?, agent_message?, updated_input?}`. `ask` is accepted by the schema but not enforced. |
| `postToolUse` | `{tool_name, tool_input, tool_output (JSON string), tool_use_id, cwd, duration, ...}` | `{updated_mcp_tool_output? (MCP only), additional_context?}` |
| `postToolUseFailure` | `{..., error_message, failure_type: error\|timeout\|permission_denied, duration, is_interrupt}` | `{additional_context?}` |
| `subagentStart` | `{subagent_id, subagent_type, task, parent_conversation_id, tool_call_id, subagent_model, is_parallel_worker, git_branch?}` | `{permission: allow\|deny, user_message?}`. `ask` is treated as `deny`. |
| `subagentStop` | `{subagent_type, status: completed\|error\|aborted, task, description, summary, duration_ms, message_count, tool_call_count, loop_count, modified_files[], agent_transcript_path}` | `{followup_message?}`. Only consumed on `completed`. |
| `afterFileEdit` | `{file_path, edits:[{old_string,new_string}]}` | — |
| `beforeReadFile` | `{file_path, content, attachments:[{type: file\|rule, file_path}]}` | `{permission: allow\|deny, user_message?}` |
| `beforeSubmitPrompt` | `{prompt, attachments}` | `{continue: bool, user_message?}` |
| `afterAgentResponse` | `{text}` | — |
| `afterAgentThought` | `{text, duration_ms?}` | — |
| `stop` | `{status: completed\|aborted\|error, loop_count}` | `{followup_message?}`. A non-empty value is auto-submitted as the next user message. |
| `sessionStart` | `{session_id, is_background_agent, composer_mode}` | `{env?: {...}, additional_context?}`. Fire-and-forget: `continue: false` is not enforced. |
| `sessionEnd` | `{session_id, reason, duration_ms, is_background_agent, final_status, error_message?}` | — |
| `preCompact` | `{trigger, context_usage_percent, context_tokens, context_window_size, message_count, messages_to_compact, is_first_compaction}` | `{user_message?}` |
| `workspaceOpen` | `{hook_event_name, cursor_version, workspace_roots, user_email}` | `{pluginPaths?: [...]}` |

**Environment variables available to hooks**

| Variable | When set |
|---|---|
| `CURSOR_PROJECT_DIR` | Always |
| `CURSOR_VERSION` | Always |
| `CURSOR_USER_EMAIL` | When logged in |
| `CURSOR_TRANSCRIPT_PATH` | When transcripts are enabled |
| `CURSOR_CODE_REMOTE` | In remote workspaces, set to `"true"` |
| `CLAUDE_PROJECT_DIR` | Always (alias of the project directory) |

`env` values returned by `sessionStart` are passed to all later hooks in the session.

**Cloud behavior**

- Cloud runs **command** hooks only, never `prompt` hooks.
- It loads them from project `.cursor/hooks.json`, plus team and enterprise hooks on Enterprise plans.
- These events are **not** run in cloud: `sessionStart`, `sessionEnd`, `beforeMCPExecution`, `afterMCPExecution`, the Tab hooks, and `workspaceOpen`.
- Hooks do not run during the early read-only turns of a cloud agent.
- User `~/.cursor/hooks.json` is never used in cloud.
- On self-hosted pools, `sessionStart` and `sessionEnd` fire when a session claims and releases the worker.

**SDK behavior**

- Hooks are file-based only, with no programmatic callback [DOC: /docs/sdk/python#hooks].
- Headless SDK local runs **auto-approve every tool call**. Gate them with hooks (`beforeShellExecution`, `preToolUse`) or with the sandbox [DOC: TS SDK quickstart note].
- Whether local hook loading needs `"project"` in `setting_sources` is **UNVERIFIED**. The docs only say to "add `.cursor/hooks.json` to the repo passed as `local.cwd`".

### 2.4 MCP: `.cursor/mcp.json` [DOC: /docs/mcp]

```json
{
  "mcpServers": {
    "local-stdio": { "type": "stdio", "command": "python", "args": ["${workspaceFolder}/tools/srv.py"],
                     "env": { "API_KEY": "${env:API_KEY}" }, "envFile": "${workspaceFolder}/.env" },
    "remote-http": { "url": "https://api.example.com/mcp",
                     "headers": { "Authorization": "Bearer ${env:MY_SERVICE_TOKEN}" } },
    "remote-oauth": { "url": "https://api.example.com/mcp",
                      "auth": { "CLIENT_ID": "${env:MCP_CLIENT_ID}", "CLIENT_SECRET": "${env:MCP_CLIENT_SECRET}", "scopes": ["read"] } }
  }
}
```

- **Locations:** project `.cursor/mcp.json` and user `~/.cursor/mcp.json`.
- **Transports:** stdio, SSE and Streamable HTTP.
- **`envFile`:** stdio servers only.
- **Interpolation:** applies to the `command`, `args`, `env`, `url`, `headers` and `auth` fields. Supported variables are `${env:NAME}`, `${userHome}`, `${workspaceFolder}`, `${workspaceFolderBasename}`, `${pathSeparator}` and `${/}`.
- **OAuth:**
  - Static OAuth uses the `auth` keys `CLIENT_ID` and `CLIENT_SECRET` (uppercase) and `scopes`.
  - The fixed redirect URLs are `https://www.cursor.com/agents/mcp/oauth/callback` for web and cloud, and `http://localhost:8787/callback` for desktop.
  - The SDK **cannot open a browser for OAuth**. It reuses a login saved by the Cursor app, or uses inline `headers` or `auth`.
- **Python SDK inline types** [SRC]:
  - `HttpMcpServerConfig(url, type="http"|"sse", headers, auth: McpAuth(client_id, client_secret, scopes))`
  - `SseMcpServerConfig`
  - `StdioMcpServerConfig(command, args, env, cwd)`. `cwd` is local only; cloud rejects it.
- **Cloud secrets handling:** HTTP `headers` and `auth` are handled by Cursor's backend and redacted before the VM sees them. Stdio `env` values **enter the VM**.
- **Local precedence:** per-send `mcp_servers` (which fully replaces the others), then create-time `mcp_servers`, then plugins, then project `.cursor/mcp.json`, then user `~/.cursor/mcp.json`.
  - Plugin, project and user servers each load only when `local.setting_sources` includes `"plugins"`, `"project"` or `"user"` respectively.
  - **Without `setting_sources`, only inline servers load.**
- **Cloud precedence:** send, then create, then user and team servers from cursor.com/agents.
- **Resume:** inline MCP servers are **not persisted across resume**. Pass them again.

### 2.5 Plugins [DOC: /docs/plugins]

Two formats are supported:

| Format | Manifest | Can bundle |
|---|---|---|
| **Agent Plugins** (open standard at agent-plugins.org) | Root `plugin.json` with `$schema: https://agent-plugins.org/schemas/1.0.0/plugin.schema.json` | `skills/` and `mcp.json` |
| **Cursor Plugins** | `.cursor-plugin/plugin.json` (with `.cursor-plugin/marketplace.json` for multi-plugin repos) | Rules, skills, agents (subagents), commands, MCP servers, hooks and variables |

- **Plugin root variable:** Use `${CURSOR_PLUGIN_ROOT}`. Cursor does not expand the standard's `${PLUGIN_ROOT}` or `${PLUGIN_DATA}`.
- **Distribution:**
  - The Cursor Marketplace, where every plugin is manually reviewed.
  - Team marketplaces: 1 on the Teams plan, unlimited on Enterprise. Install modes are Default Off, Default On and Required.
  - Local development from `~/.cursor/plugins/local/<name>`. On Enterprise this is admin-gated and off by default.
  - A `workspaceOpen` hook can return `pluginPaths`.
- **SDK and CLI:**
  - In the SDK, plugins load only when `"plugins"` is in `setting_sources` (local). Cloud always loads `plugins`.
  - The CLI has `--plugin-dir <path>`.

### 2.6 `setting_sources` [DOC + SRC]

- **Type:** `LocalAgentOptions.setting_sources: Sequence[SettingSource | str]`.
- **Values:**

| Value | Layer it loads |
|---|---|
| `"project"` | `.cursor/` |
| `"user"` | `~/.cursor/` |
| `"team"` | Dashboard team settings |
| `"mdm"` | Managed settings |
| `"plugins"` | Plugin-provided settings |
| `"all"` | All of the above |

- On the wire it becomes `SETTING_SOURCE_*` [SRC].
- **Cloud** ignores this field and always loads `project`, `team` and `plugins`.
- **What it gates:** file MCP, file subagents and plugins (documented).
- **What it does not gate:** rules, skills and `AGENTS.md`. These load from `cwd` and `dirs` ("Merged with `cwd` so rules, skills, and workspace context load from every path"), and `prewarmLocalWorkspace` resolves "rules, skills, MCP servers, and ignore files". That they load **independently of `setting_sources` is UNVERIFIED**. Set `setting_sources=["project"]` explicitly for deterministic materialization.

---

## 3. Python SDK 1.0.36/1.0.37: exact signatures [SRC: wheel 1.0.37 `types.py`, `_agent.py`, `_client.py`; DOC: /docs/sdk/python]

These shapes come from the 1.0.37 wheel. The 1.0.36 shapes are **assumed identical (UNVERIFIED)**: there is no diff and no release note.

### 3.1 Entry points

```python
Agent.create(options: AgentOptions | Mapping | None = None, *, client=None,
             model=None, api_key=None, name=None, local=None, cloud=None, idempotency_key=None) -> Agent
Agent.resume(agent_id: str, options: AgentOptions | Mapping | None = None, *, client=None) -> Agent   # runtime from prefix: "bc-" = cloud
Agent.prompt(message, options=None, *, client=None) -> RunResult                                       # create+send+wait+close
Agent.list(options=None, *, client=None) -> ListResult[SDKAgentInfo]
Agent.get(agent_id, *, client=None, cwd=None, api_key=None) -> SDKAgentInfo
Agent.list_runs(agent_id, options=None, *, cwd=None, client=None) -> ListResult[Run]
Agent.get_run(run_id, options=None, *, client=None) -> Run
Agent.cancel_run(run_id, *, client=None, agent_id=None) -> None
Agent.archive(agent_id) / Agent.unarchive(agent_id) / Agent.delete(agent_id)   # also on handles and client.agents.*
Agent.messages.list(agent_id)                                                   # == agent.list_messages()
```

**Agent handle**

- Attributes: `agent_id` (`agent-<uuid>` for local, `bc-<uuid>` for cloud), `model`, `client`.
- Methods:
  - `send(message, options=None, *, idempotency_key=None) -> Run`
  - `reload()`
  - `close()`
  - `list_messages(options=None)`
  - `list_artifacts() -> list[SDKArtifact(path, size_bytes, updated_at)]`
  - `download_artifact(path) -> bytes`. Cloud only; it raises on local.
  - `get_usage(*, run_id=None) -> AgentUsage(usage, runs[RunUsage], cost: UsageCost(raw_cost_cents, charged_cents) | None)`. On local it can raise `InternalServerError` with `feature_unavailable` until the account is enabled.
  - `archive()`, `unarchive()`, `delete()`

**Client**

```python
CursorClient.launch_bridge(command=None, *, workspace=None, state_root=None, host=None, port=None, timeout=30,
                           local=None, store_handler: LocalAgentStoreHandler | None = None,
                           client_timeout=..., max_retries=0, http_client=None, allow_api_key_env_fallback=None)
CursorClient.connect(base_url, auth_token, *, timeout=..., unary_timeout=..., stream_timeout=..., max_retries=0, ...)
client.ping() -> str ; client.get_version() -> dict (bridgeVersion, protocolVersion, capabilities) ; client.shutdown(grace_seconds=0)
client.with_options(timeout=..., unary_timeout=..., stream_timeout=..., max_retries=...)
client.agents.list(options=None, *, runtime="local"|"cloud"|"auto", cwd=None, include_archived=None, pr_url=None, cursor=None, limit=None, api_key=None)
client.agents.get(agent_id, *, cwd=None, api_key=None) ; client.agents.list_runs(agent_id, options=None, *, runtime, cwd, cursor, limit, api_key)
client.agents.get_run(run_id, options=None) ; client.agents.cancel_run(run_id, *, agent_id=None)
client.models.list(*, api_key=None) -> list[SDKModel] ; client.repositories.list(*, api_key=None) -> list[SDKRepository]
client.me(*, api_key=None) -> SDKUser
```

- `Cursor.me()`, `Cursor.models.list()` and `Cursor.repositories.list()` are the module-level equivalents.
- Async mirrors exist: `AsyncClient`, `AsyncAgent`, `AsyncRun` and `AsyncCursor`.
- Bridge capabilities advertised by 1.0.37 [SRC `constants.js`]: `agent.create`, `agent.resume`, `agent.send`, `run.observe`, `run.wait`, `run.cancel`, `agent.management`, `cursor.catalog`, `artifacts.chunked`, `agent.usage`.

### 3.2 Option dataclasses (verbatim field lists, all `frozen=True`)

```python
AgentOptions(model=None, api_key=None, name=None, local: LocalAgentOptions|Mapping|None=None,
             cloud: CloudAgentOptions|Mapping|None=None, mcp_servers: Mapping[str, McpServerConfig]|None=None,
             agents: Mapping[str, AgentDefinition|Mapping]|None=None, agent_id=None, idempotency_key=None,
             mode: "agent"|"plan"|None=None, tools: Sequence[str]|None=None, disallowed_tools: Sequence[str]|None=None)

LocalAgentOptions(cwd=None, dirs=None, setting_sources=None, sandbox_options: SandboxOptions|Mapping|None=None,
                  store: LocalAgentStoreConfig|Mapping|None=None, auto_review: bool|None=None,
                  custom_tools: Mapping[str, CustomTool|Mapping]|None=None)

CloudAgentOptions(env: CloudEnvironment|Mapping|None=None, repos: Sequence[CloudRepository|Mapping]|None=None,
                  work_on_current_branch=None, auto_create_pr=None, open_as_cursor_github_app=None,
                  skip_reviewer_request=None, env_vars: Mapping[str,str]|None=None, metadata: Mapping[str,str]|None=None)
CloudRepository(url: str, starting_ref: str|None=None, pr_url: str|None=None)
CloudEnvironment(type: "cloud"|"pool"|"machine" = "cloud", name: str|None=None)   # pool/machine = self-hosted workers

SendOptions(model=None, mcp_servers=None, local: LocalSendOptions|Mapping|None=None, idempotency_key=None,
            on_step: Callable[[ConversationStep],Any]|None=None, on_delta: Callable[[InteractionUpdate],Any]|None=None,
            mode=None, cloud: CloudSendOptions|Mapping|None=None)
LocalSendOptions(force: bool|None=None)          # expire a stuck local run before this send
CloudSendOptions(env_vars: Mapping[str,str]|None=None)   # per-run env vars

SandboxOptions(enabled: bool|None=None)
LocalAgentStoreConfig(type: str, root_dir: str|None=None)   # type: "sqlite" (default) | "jsonl" (root_dir required) | "custom"
CustomTool(execute: Callable[[Mapping, CustomToolContext], Any], description=None, input_schema=None, output_schema=None)
ModelSelection(id: str, params: Sequence[ModelParameterValue]=()) ; ModelParameterValue(id: str, value: str)
UserMessage(text: str, images=None) ; SDKImage(url=None, data=None, mime_type=None, dimension=None)  # .from_file/.from_data/.from_url
```

**Corrections to the task's assumptions:**

- **No `target_branch` or `branch_name` in v1 or the SDK.**
  - `work_on_current_branch=False` (the default) pushes to an auto `cursor/...` branch based on `starting_ref`.
  - `work_on_current_branch=True` pushes to the `starting_ref` branch, or to the PR head when `pr_url` is set.
  - The v0 `target.branchName` field was **not carried into v1**.
- **`custom_tools` lives on `LocalAgentOptions`, not on `AgentOptions`,** and it is local only.
- **`tools` and `disallowed_tools` are local only and are not persisted across resume.**
  - They accept public tool names (`read`, `edit`, `grep`, `glob`, `ls`, `task`, `webSearch`, ...) and the groups `shell` and `mcp`.
  - Deny wins.
  - Disallowing `mcp` also removes custom tools. Disallowing `task` removes subagents.
- **`SendOptions` has no `tools` override.**
- **`env_vars` cannot be combined with a caller-supplied `agent_id`.**
  - Names cannot start with `CURSOR_`.
  - The limits are 50 entries, names up to 255 bytes and values up to 4096 bytes.
  - On REST the field is still in beta and is **silently ignored** if it is not enabled.
- **`metadata` limits:** up to 50 keys; keys up to 255 characters; values up to 4096 bytes. If metadata is not enabled for the account, the request returns `403 feature_unavailable`.
- **`open_as_cursor_github_app`** defaults to `True` for service-account keys and `False` for user keys.

### 3.3 Run handle

- **Fields:**
  - `id`, `agent_id`
  - `status`: `running`, `finished`, `error`, `cancelled` or `expired`
  - `result`, `model`, `duration_ms`
  - `git: RunGitInfo(branches=[RunGitBranchInfo(repo_url, branch, pr_url)])`
  - `created_at`, `usage`
- **Methods:** `stream()`, `messages()`, `events()`, `iter_text()`, `text()`, `wait()`, `cancel()`, `conversation()`, `conversation_json()`, `observe(*, after_offset=None)`, `supports(op)`, `unsupported_reason(op)` and `on_did_change_status(listener) -> unsubscribe`.
- The details are in the existing harness note.

### 3.4 Local persistence: `LocalAgentStoreConfig` and `CURSOR_SDK_BRIDGE_STATE_ROOT` [SRC: bridge `server.js`, `bin/cursor-sdk-bridge.js`, `bridge-local-agent-store.js`, `_local_store.py`]

- **Default state root:** `~/.cursor/sdk-agent-store/<sha256(workspaceRef)[:24]>`.
  - Override it with `--state-root`, `CURSOR_SDK_BRIDGE_STATE_ROOT`, or `launch_bridge(state_root=...)`.
  - The default store is SQLite (`index.db` under the state root).
- **Store types (`local.store.type`):**

| Type | Behavior |
|---|---|
| `"sqlite"` | The default. |
| `"jsonl"` | Requires `rootDir`. |
| `"custom"` | A **host-owned store**. Pass `launch_bridge(store_handler=...)`, implementing the `LocalAgentStoreHandler` protocol with `agents`, `runs`, `run_events` and `checkpoints` substores (sync or async methods). The bridge forwards every store op over a loopback `CallStore` RPC. `run_events.append` carries an `idempotencyKey`, and checkpoint blobs are base64. |

  Any other type is rejected with `InvalidArgument`.
- **Default store for every operation:** `--local-store <json>` or `CURSOR_SDK_LOCAL_STORE`.
- **Other bridge environment variables:**

| Variable | Purpose |
|---|---|
| `CURSOR_SDK_BRIDGE_PORT` | Port to bind |
| `CURSOR_SDK_BRIDGE_HOST` | Host to bind (default `127.0.0.1`) |
| `CURSOR_SDK_BRIDGE_WORKSPACE` | Workspace root |
| `CURSOR_SDK_STORE_CALLBACK_URL`, `CURSOR_SDK_STORE_CALLBACK_AUTH_TOKEN` | Host store callback |
| `CURSOR_SDK_TOOL_CALLBACK_URL`, `CURSOR_SDK_TOOL_CALLBACK_AUTH_TOKEN` | Host custom-tool callback |
| `CURSOR_SDK_BRIDGE_LOG=1` | RPC log, equivalent to `--verbose` |
| `CURSOR_SDK_BRIDGE_SURVIVE_UNCAUGHT=1` | Keep running after uncaught errors |
| `CURSOR_SDK_BRIDGE_DEBUG_STARTUP=1` | Startup tracing |

- **Other bridge flags:** `--max-concurrent-agents` and `--max-message-bytes` are advertised limits.
- **Python logging:** set `CURSOR_SDK_LOG=debug` for SDK logs.
- **Auth token:** generated per bridge process and written to a temp `auth-token` file with mode 0600. Send `Authorization: Bearer` on every RPC [DOC: /docs/sdk/bridge].
- **Wire protocol:** loopback HTTP/1.1 Connect, not gRPC/HTTP2.

### 3.5 Injecting instructions or a system prompt in Python [SRC + DOC]

- **Python 1.0.37 has no `system_prompt`.** It is absent from `AgentOptions` and from the bridge `proto/sdk/v1` `AgentOptions` message, whose fields are model, api_key, name, local, cloud, mcp_servers, agents, agent_id, mode, tools and disallowed_tools.
- **TypeScript 1.0.37 does have `systemPrompt`:**
  - It **replaces** Cursor's harness prompt. Tool schemas, rules and skills still load.
  - It is local only and not persisted on resume.
  - It is gated per account. Without access, the first send fails naming `--system-prompt`.
- **Options for Python, from most to least preferred:**
  1. **Materialize a rule** at `<cwd>/.cursor/rules/mission.mdc` with `alwaysApply: true` [DOC]. This works for local and cloud, and for cloud the rule must be committed to the repo or branch.
  2. **Write `AGENTS.md`** at the workspace root or in a subdirectory [DOC]. This also works in cloud from the repo.
  3. **Return `additional_context` from a `sessionStart` hook** [DOC]. This works locally only, because cloud does not run `sessionStart`.
  4. **Use a skill or Custom Mode** for opt-in instructions.
  5. **Prefix the prompt** text on the first `send` (and repeat it on resume if needed).
  6. **Use a subagent `prompt`.** `AgentDefinition.prompt` is a true system prompt for that subagent [DOC].

### 3.6 Sandbox [DOC: TS SDK sandbox options; SRC]

- `SandboxOptions(enabled: bool | None)` is the only field.
- **Off by default.** Local runs can read and write the cwd, run shell commands, and reach the network unrestricted, and every tool call is auto-approved.
- **When enabled:**
  - Writes are limited to the cwd, temp directories, and paths allowed in `sandbox.json`.
  - Reads are unrestricted.
  - Shell commands run under bubblewrap on Linux, seatbelt on macOS, or the bundled helper.
  - Outbound network is denied except for hosts listed in `.cursor/sandbox.json` or `~/.cursor/sandbox.json`.
- **Unsupported hosts** raise `ConfigurationError` at create.
- **Windows sandbox support is UNVERIFIED** for our worker.
- **`auto_review=True`** routes local tool calls through a classifier. It is best-effort and not a security boundary.
- **Cloud:** `sandbox_options` does not apply, because cloud runs inside a VM.

### 3.7 Example: Mission Control local and cloud construction (Python)

```python
from cursor_sdk import (AgentDefinition, AgentOptions, CloudAgentOptions, CloudEnvironment, CloudRepository,
                        CursorClient, LocalAgentOptions, LocalAgentStoreConfig, SandboxOptions, SendOptions)

with CursorClient.launch_bridge(workspace=ws, state_root=state_dir) as client:
    agent = client.agents.create(AgentOptions(
        model="composer-2.5", name=f"mc-{run_id}", agent_id=None,
        local=LocalAgentOptions(cwd=ws, setting_sources=["project"], sandbox_options=SandboxOptions(enabled=True),
                                store=LocalAgentStoreConfig(type="jsonl", root_dir=store_dir)),
        agents={"verifier": AgentDefinition(description="Verify claimed work; use after edits.",
                                            prompt="You are a skeptical verifier...", model="inherit")},
        disallowed_tools=["webSearch"]))
    run = agent.send(prompt_text, SendOptions(idempotency_key=f"{run_id}:turn1"))

cloud = Agent.create(AgentOptions(model="composer-2.5", idempotency_key=f"{run_id}:create",
    cloud=CloudAgentOptions(repos=[CloudRepository(url=repo, starting_ref="mc/base-123")],
                            auto_create_pr=False, work_on_current_branch=False,
                            env=CloudEnvironment(type="pool", name="mc-pool"),   # omit for Cursor-hosted
                            env_vars={"MC_TOKEN": tok}, metadata={"mc_run_id": run_id})))
```

---

## 4. Cloud Agents API v1 for a durable adapter [DOC: /docs/cloud-agent/api/endpoints; OpenAPI `cloud-agents-openapi.yaml` v1.0.0]

### 4.1 Status, base URL and auth

- **Status:** Public beta; the API may change before GA.
- **Base URL:** `https://api.cursor.com`.
- **Auth:** Basic (`-u KEY:`) or `Authorization: Bearer KEY`. Both user keys and service-account keys work.
- **Image limits:** up to 5 images per prompt, 15 MB each, in png, jpeg, gif or webp format.

### 4.2 Create an agent: `POST /v1/agents`

The request creates the agent and enqueues its first run.

**Body fields**

| Field | Notes |
|---|---|
| `prompt` | Required. `{text, images?: [{data, mimeType} \| {url}]}`. |
| `model` | Optional. `{id, params?: [{id, value}]}`. When omitted, the default resolves to the user default, then the team default, then the system default. |
| `name` | Up to 100 characters. |
| `env` | `{type: cloud\|pool\|machine, name?}`. The pool name defaults to `default`. An unknown pool returns 400. |
| `repos` | Up to 20 entries of `{url, startingRef?, prUrl?}`. Mutually exclusive with a named cloud env. Omit both `repos` and `env` for a no-repo agent. On self-hosted targets, only an any-repo pool accepts more than one repo. |
| `workOnCurrentBranch` | See §4.6. |
| `autoCreatePR` | Open a PR when the run completes. |
| `skipReviewerRequest` | Only applies with `autoCreatePR`. |
| `envVars` | Beta; see the limits in §3.2. |
| `mcpServers` | Up to 50 entries of `{name, type: http\|sse\|stdio, url \| command, args, env, headers, auth}`. |
| `customSubagents` | Up to 20; see §1.6. |
| `mode` | `agent` or `plan`. |
| `agentId` | Client-supplied, in the form `bc-<uuid>`. |

**Response**

```json
{ "agent": {"id","name","status":"ACTIVE","env","repos","workOnCurrentBranch","autoCreatePR","url","createdAt","updatedAt","latestRunId"},
  "run":   {"id":"run-<uuid>","agentId","status":"CREATING","createdAt","updatedAt"} }
```

**Idempotency**

- **Documented:** a client-supplied `agentId`. Re-POSTing the same ID returns `409 agent_id_conflict`. It cannot be combined with `envVars`.
- **Observed in SDK source, not in the public OpenAPI:** the SDK sends an **`Idempotency-Key` header** on `POST /v1/agents` and `POST /v1/agents/{id}/runs` [SRC: `@cursor/sdk` HTTP client `t?.idempotencyKey&&(r["Idempotency-Key"]=...)`]. The bridge proto describes it as "Optional key that makes CreateAgent retries safe for cloud agents."
  - Server dedupe semantics (window, response on replay) are **UNVERIFIED**.
- **Also sent by the SDK but undocumented in the public OpenAPI** [SRC], so their REST support is **UNVERIFIED for direct REST callers**:
  - `metadata`
  - `openAsCursorGithubApp`
  - `runEnvVars`, which carries per-run env vars on the first send

### 4.3 Follow-up runs and run reads

- **`POST /v1/agents/{id}/runs`:**
  - Body: `{prompt: {text, images?}, mcpServers? (replaces create-time servers for this run), mode?}`.
  - Returns `{run}`.
  - Only one active run is allowed. A second request while one is active returns **`409 agent_busy`** (Python raises `AgentBusyError`, which is not retryable).
  - Per-run `envVars` on follow-ups are exposed in the SDK (`CloudSendOptions`). The REST field name is **UNVERIFIED**.
- **`GET /v1/agents/{id}/runs?limit&cursor`** lists runs.
- **`GET /v1/agents/{id}/runs/{runId}`** returns `{id, agentId, status, createdAt, updatedAt, durationMs?, result?, git?}`.
  - Run statuses: `CREATING`, `RUNNING`, `FINISHED`, `ERROR`, `CANCELLED`, `EXPIRED`.
  - Agent statuses (on the agent resource): `ACTIVE`, `IDLE`, `ARCHIVED`.
- **`POST /v1/agents/{id}/runs/{runId}/cancel`:**
  - Terminal; a cancelled run cannot be resumed.
  - Cancelling a run that is not active returns `409 run_not_cancellable`.
- **Agent listing and lifecycle:**
  - `GET /v1/agents?limit<=100&cursor&prUrl&includeArchived` lists agents. `nextCursor` is **omitted** on the last page.
  - `GET /v1/agents/{id}` returns the full agent record.
  - `POST /v1/agents/{id}/archive` and `/unarchive` are idempotent.
  - `DELETE /v1/agents/{id}` is permanent.

### 4.4 SSE stream: `GET /v1/agents/{id}/runs/{runId}/stream`

- Send `Accept: text/event-stream`. The stream covers only the requested run.
- **Events:**

| Event | Payload |
|---|---|
| `status` | `{runId, status}` |
| `assistant` | `{text}` delta |
| `thinking` | `{text}` delta |
| `tool_call` | `{callId, name, status: running\|completed, args?, result?, truncated?: {args?, result?}}` |
| `interaction_update` | SDK `InteractionUpdate` shape: `text-delta`, `tool-call-started/completed`, `step-started/completed`, `turn-ended`, ... |
| `heartbeat` | `{}` |
| `result` | `{runId, status, text?, durationMs?, git?}` |
| `error` | `{code, message}` |
| `done` | `{}` |

- **Resume:**
  - Events carry opaque `id:` lines. The leading `status` event has no ID and is re-sent on every reconnect.
  - Reconnect with a **`Last-Event-ID`** header. An ID from another run returns `400 invalid_last_event_id`.
- **Retention:**
  - The response header **`X-Cursor-Stream-Retention-Seconds`** gives the window.
  - After it, the stream returns **`410 stream_expired`**. At that point, read the terminal state from Get A Run instead.

### 4.5 Artifacts, usage and other endpoints

- **Artifacts** are files the agent writes under the workspace `artifacts/` directory. They are **agent-scoped**, because the workspace persists across runs.
  - `GET /v1/agents/{id}/artifacts` returns `{items: [{path: "artifacts/x.png", sizeBytes, updatedAt}]}`.
  - `GET /v1/agents/{id}/artifacts/download?path=artifacts/x.png` returns `{url (presigned S3, valid 15 min), expiresAt}`.
  - v1 paths are relative. The v0 absolute `/opt/cursor/artifacts/...` form is rejected.
  - The SDK's `download_artifact()` returns the bytes, using the bridge's `artifacts.chunked` capability.
- **Usage:** `GET /v1/agents/{id}/usage?runId=` returns `{totalUsage: {inputTokens, outputTokens, cacheWriteTokens, cacheReadTokens, totalTokens}, runs: [{id, usageUuid?, usage}]}`. A missing run returns `404 run_not_found`. Dollar cost is available through the SDK `get_usage().cost`.
- **Worker tokens:** `POST /v1/sub-tokens` takes `{forUserEmail}` or `{forUserId}` and returns `{accessToken, expiresAt (1 h, not refreshable), userId, teamId}`.
  - It requires an **agent-scoped team service-account key**.
  - Its purpose is to let a worker run as a team member.
- **Metadata endpoints:** `GET /v1/me`, `GET /v1/models` and `GET /v1/repositories`.
- **Environments:** `/v1/environments` (CRUD, builds, secrets) and `/v1/team/secrets` exist in the OpenAPI. They are not detailed here.

### 4.6 Git results and diffs

- **Where git results appear:** `git.branches[]` on run objects and on the `result` SSE event, with entries `{repoUrl (no scheme, e.g. github.com/org/repo), branch?, prUrl?}`.
  - The snapshot is **per agent, not per run**: every run returns the same `git`.
  - Stacked agents produce several entries.
- **Branch behavior:**
  - The default pushes a new `cursor/...` branch from `startingRef`, or from the PR base when `prUrl` is set.
  - `workOnCurrentBranch: true` pushes directly to `startingRef`, or to the PR head when `prUrl` is set.
- **No diff or commit endpoint in v1.** To get the diff, use the SCM with the returned `branch` and `repoUrl`, for example the GitHub compare API or `git fetch`. Commit SHAs are not returned.
- **Diff in local placement:** run `git diff` in our workspace. Whether `run.git` is populated for local runs is **UNVERIFIED**; the docs say "RunGitInfo on cloud".

### 4.7 Self-hosted workers and pools: `/v0/private-workers/*` (current docs, legacy path name)

- **Auth:** the pool's service-account key.
- **Endpoints:**
  - `GET /v0/private-workers?status=all|in_use|idle&scope=all|team_pool|personal&limit&pageToken` lists workers.
  - `GET /v0/private-workers/summary` and `GET /v0/private-workers/{id}` read workers.
  - `GET /v0/private-workers/pools` lists pools.
  - `POST /v0/private-workers/pools` registers a pool with `{scope: user|team, poolName, repoOwner?, repoName?, repoUrl?, workerReadyTimeoutSeconds}`. Omit the repo fields for an any-repo pool.
  - `DELETE /v0/private-workers/pools?scope&pool_name` removes a pool.
  - `GET /v0/private-workers/pending-requests?limit&pageToken&repository&pool` returns `{requests, nextPageToken, streamCursor}`.
- **Watching the queue:** `GET /v0/private-workers/pending-requests/stream?cursor=...` is an SSE stream with events `created`, `claimed`, `claimed_offline`, `expired` and `heartbeat` (about every 20 s).
  - A cursor expires **5 minutes after the list that issued it**, after which the stream returns `410 cursor_expired`.
  - Delivery is best-effort, and the list is the source of truth.
  - A service account can hold at most **4 concurrent streams**.
- **Claims:**
  - `POST /v0/private-workers/claim` with `{id, workerId, sessionToken?}`. Start the worker with `CURSOR_AGENT_WORKER_ID` set to the same ID.
  - `POST /v0/private-workers/tokens` with `{id, workerId}` mints a session token.
  - `POST /v0/private-workers/claims/{id}/release` frees the worker. It returns 400 while a turn is active.
  - "Fail a claim" also exists.
- **Worker CLI:** `agent worker start`. The idle release timeout is set with `--idle-release-timeout` or `CURSOR_WORKER_IDLE_RELEASE_TIMEOUT`.
- **SDK targeting:** `CloudAgentOptions.env=CloudEnvironment(type="pool"|"machine", name=...)` routes the agent to these workers.
- **Secrets on pools:** `envVars` reach pool workers only when the worker runs with `--sync-dashboard-secrets` and a team admin has Secret sync turned on.

### 4.8 Webhooks, rate limits and concurrency

- **Webhooks:**
  - v1 says "Webhooks are coming soon."
  - **Only v0 supports them**, through `POST /v0/agents` with `webhook: {url, secret (32 or more characters)}`.
  - The only event is `statusChange`, fired for `ERROR` and `FINISHED`.
  - Request headers: `X-Webhook-Signature: sha256=<hex HMAC-SHA256 of raw body>`, `X-Webhook-ID`, `X-Webhook-Event`, and `User-Agent: Cursor-Agent-Webhook/1.0`.
  - Payload: `{event, timestamp, id, status, source: {repository, ref}, target: {url, branchName, prUrl}, summary}`.
  - Retries happen "may be retried on error". The exact retry policy is UNVERIFIED.
- **Rate limits:**
  - The Cloud Agents API is listed as "Standard rate limiting". The cross-API default is "20 requests per minute" unless an endpoint documents otherwise. Whether that default applies to each v1 endpoint is **UNVERIFIED**.
  - A 429 response includes `Retry-After`, `X-RateLimit-Limit`, `X-RateLimit-Remaining` and `X-RateLimit-Reset` [OpenAPI].
  - Documented v0-only limits: artifacts at 300/min and 6000/h; `/v0/repositories` at 1/user/min and 30/user/h.
- **Concurrency:**
  - One active run per agent (`409 agent_busy`).
  - The docs also say "You can run as many agents as you want in parallel" (cloud-agent overview).
  - Forum threads report per-plan concurrent-agent caps. The exact numbers are **UNVERIFIED**.

---

## 5. Cursor CLI headless (`agent`, formerly `cursor-agent`) [DOC: /docs/cli/headless, /docs/cli/reference/parameters]

- **Command:** `agent -p/--print "<prompt>"` runs non-interactively and has access to all tools, including write and shell.
- **Flags:**

| Flag | Effect |
|---|---|
| `--output-format text\|json\|stream-json` | Output format. Only works with `--print`. |
| `--stream-partial-output` | Streams text deltas. Requires `stream-json`. |
| `--resume [chatId]` | Resume a chat. `--continue` is an alias for `--resume=-1`. |
| `--model <id>` | Model to use. The docs list `--model`; a `-m` short form is **not listed (UNVERIFIED)**. |
| `--mode plan\|ask`, `--plan` | Agent mode. |
| `-f/--force` (alias `--yolo`) | Applies file changes. Without it, print-mode changes are only proposed. |
| `--sandbox enabled\|disabled` | Sandbox mode. |
| `--approve-mcps` | Auto-approves MCP servers. |
| `--trust` | Trusts the workspace without prompting. Headless only. |
| `--workspace <path>` | Workspace directory. |
| `--plugin-dir <path>` | Load a local plugin directory. |
| `-w/--worktree [name]` | Run in a new worktree. Use with `--worktree-base`. |
| `--api-key` | Auth key; `CURSOR_API_KEY` also works. |
| `-H/--header` | Adds a custom request header. |

- **Commands:** `create-chat` (returns a chat ID), `ls`, `resume`, `models`, `mcp`, `worker start` (self-hosted worker), `acp` (ACP server mode).
- **`stream-json` output:** line-delimited JSON with `type` values `system` (subtype `init`), `assistant`, `tool_call` (subtype `started`/`completed`, with `tool_call.<x>ToolCall`) and `result` (with `duration_ms`).
- **Assessment:**
  - The CLI is still viable for ad-hoc and CI use.
  - For a programmatic Python control plane it is **superseded by the SDK and bridge**. The SDK gives typed runs, `Idempotency-Key`, resume by agent ID, a host-owned store, custom tools, `observe(after_offset)`, usage and cancel.
  - The CLI's remaining unique roles are `agent worker` (self-hosted pools) and ACP.

---

## 6. Conversation retrieval: v0 vs v1

- **v0:**
  - `GET /v0/agents/{id}/conversation` returns `{id, messages: [{id, type: user_message|assistant_message, text}]}`.
  - Text only, with no tool calls. Unavailable after the agent is deleted.
  - v0 is labelled legacy ("remains available during the migration window").
- **v1:**
  - There is **no conversation endpoint**. None appears in the OpenAPI path list, and none is used by `@cursor/sdk` 1.0.37, whose REST paths are only `/v1/agents...`, `/v1/me`, `/v1/models` and `/v1/repositories` [SRC].
  - The v1 equivalents are the per-run SSE stream (subject to retention), Get A Run (`result` text), and the SDK `run.conversation()` / `conversation_json()` (typed `ConversationTurn` list).
  - SDK `agent.list_messages()` for **cloud** agents: the implementation found reads the local checkpoint store, so its cloud behavior is **UNVERIFIED**.
- **Recommendation:** persist events from the stream into Mission Control as they arrive. Do not rely on server-side transcript retrieval.

---

## Implications for a Mission Control Cursor lane

1. **Materialize config as files, not SDK knobs.** Python has no `system_prompt` and hooks are file-only. Instead:
   - write `.cursor/rules/mc-*.mdc` (`alwaysApply: true`), `AGENTS.md`, `.cursor/agents/*.md`, `.cursor/hooks.json` and `.cursor/mcp.json` into the workspace;
   - set `setting_sources=["project"]` (local);
   - commit or push them to the starting branch for cloud.
2. **Use file-based subagents when behavior matters.** Inline `AgentOptions.agents` and REST `customSubagents` lack `readonly` and `is_background`. Keep inline definitions for secret-free, per-run prompts.
3. **Make governance a hook boundary.** Local SDK runs auto-approve every tool call. Ship a fail-closed (`failClosed: true`) `preToolUse` and `beforeShellExecution` policy hook. Enable `SandboxOptions(enabled=True)` where the host supports it; Windows support is UNVERIFIED.
4. **Cloud hook coverage differs from local.** Cloud skips `sessionStart`, `sessionEnd` and the MCP hooks, and does not run hooks in early read-only turns. Do not depend on those events for cloud policy or context injection.
5. **Durable create (cloud).** Prefer a server-minted ID plus `Idempotency-Key` (`idempotency_key=` in the SDK) when `env_vars` are needed. Use a client `agentId` when they are not, and treat `409 agent_id_conflict` as "already created".
6. **Stream durability (cloud).**
   - Persist the last SSE `id`, and resume with `Last-Event-ID`.
   - On `410 stream_expired`, fall back to Get A Run.
   - Remember that `git` is agent-scoped, and attribute it to runs using `latestRunId`.
7. **No v1 webhooks.** Run a poller or streamer per active run inside a Temporal activity with heartbeats. Do not plan on push notifications until v1 webhooks ship.
8. **Diff and branch.**
   - Cloud returns only a branch and PR URL, with no commit SHA or diff. Fetch diffs from the SCM.
   - v1 has no `branchName`. Control the branch through `starting_ref` plus `work_on_current_branch=True` on a pre-created `mc/<run>` branch.
9. **Local persistence ownership.** Pin a per-workspace `state_root`, or use `LocalAgentStoreConfig(type="custom")` with `store_handler=` so that agent, run, event and checkpoint state lands in mission-db. `run_events` carry idempotency keys.
10. **Version pin and re-verify.** Pin `cursor-sdk==1.0.37` and confirm `client.get_version()` reports the expected `bridgeVersion`. Several surfaces are beta, gated or undocumented and need a live check before relying on them:
    - REST `envVars`, `metadata` and `Idempotency-Key`
    - local `get_usage`
    - the TypeScript-only `systemPrompt`

---

## Citations (all read 2026-10-07)

1. Cursor Docs — Subagents: https://cursor.com/docs/subagents
2. Cursor Docs — Python SDK: https://cursor.com/docs/sdk/python
3. Cursor Docs — TypeScript SDK (settingSources table, systemPrompt, sandbox): https://cursor.com/docs/sdk/typescript
4. Cursor Docs — SDK Bridge: https://cursor.com/docs/sdk/bridge
5. Cursor Docs — SDK release notes (cached through 1.0.35): https://cursor.com/docs/release-notes/sdk
6. Cursor Docs — Rules: https://cursor.com/docs/rules
7. Cursor Docs — Agent Skills: https://cursor.com/docs/skills
8. Cursor Docs — Hooks: https://cursor.com/docs/hooks
9. Cursor Docs — MCP: https://cursor.com/docs/mcp
10. Cursor Docs — Plugins: https://cursor.com/docs/plugins
11. Cursor Docs — Cloud Agents API v1 endpoints: https://cursor.com/docs/cloud-agent/api/endpoints
12. Cursor Cloud Agents OpenAPI v1.0.0: https://cursor.com/docs-static/cloud-agents-openapi.yaml
13. Cursor Docs — Cloud Agents API v0 (legacy): https://cursor.com/docs/cloud-agent/api/v0
14. Cursor Docs — Webhooks: https://cursor.com/docs/cloud-agent/api/webhooks
15. Cursor Docs — APIs overview / rate limits: https://cursor.com/docs/api
16. Cursor Docs — Headless CLI: https://cursor.com/docs/cli/headless
17. Cursor Docs — CLI parameters: https://cursor.com/docs/cli/reference/parameters
18. Cursor Docs — Cloud Agents overview (parallelism statement, search excerpt): https://cursor.com/docs/cloud-agent
19. Cursor Changelog 2.4 (subagents, skills): https://cursor.com/changelog/2-4
20. PyPI JSON — cursor-sdk (versions 1.0.29 to 1.0.37, upload times): https://pypi.org/pypi/cursor-sdk/json
21. PyPI wheel inspected — cursor_sdk-1.0.37-py3-none-win_amd64.whl: https://files.pythonhosted.org/packages/b2/12/70f7398bec1de38e4dbaa774cc27365013b0ee4fc0d53941fdfcda9c7ea9/cursor_sdk-1.0.37-py3-none-win_amd64.whl. Read files: `cursor_sdk/types.py`, `_agent.py`, `_client.py`, `_local_store.py`, `_vendor/bridge/dist/{bin/cursor-sdk-bridge.js,server.js,bridge-local-agent-store.js,constants.js}`, `_vendor/bridge/proto/sdk/v1/*.proto`, `_vendor/bridge/node_modules/@cursor/sdk/dist/{esm/options.d.ts,cjs/*.js}`.
22. GitHub — cursor/sdk-bridge releases (v1.0.35 to v1.0.37): https://github.com/cursor/sdk-bridge/releases (via https://api.github.com/repos/cursor/sdk-bridge/releases)
23. Cursor Forum — Cursor 2.4 Subagents thread (file locations): https://forum.cursor.com/t/cursor-2-4-subagents/149403
24. Cursor Forum — concurrent cloud agent limits (anecdotal, not authoritative): https://forum.cursor.com/t/limit-on-concurrent-cloud-agents/159540
