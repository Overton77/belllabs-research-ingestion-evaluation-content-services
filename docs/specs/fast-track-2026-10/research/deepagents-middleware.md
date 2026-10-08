---
type: Research Note
title: "Deep Agents and LangChain v1 middleware fact sheet (deepagents 0.7.23, read 2026-10-07)"
description: "create_deep_agent signature, SubAgent kinds, AgentMiddleware hooks (wrap_tool_call, wrap_model_call, before/after model and agent), built-in and deepagents middleware, backends and pre-loading files, LangGraph checkpoints and forking, MCP adapters, deprecated CLI and the dcode hook precedent, pinned versions, and implications for hook scripts, subagent profiles and checkpoints."
tags: [mission-control, research, fast-track]
---

# Deep Agents + LangChain v1 middleware — primary-source fact sheet

Read date for every source: **2026-10-07**. I verified most claims against the **published wheel source** downloaded from PyPI on that date. I unpacked the wheels into a scratch directory and read them without executing any code. The rest comes from docs.langchain.com pages fetched live (`maxAge: 0`). Anything I inferred rather than read is marked **UNVERIFIED**.

Source key used below:

- `[W:pkg==ver path]`: file inside the PyPI wheel for that exact version (wheel URL listed in section 7).
- `[D:url]`: docs page, read 2026-10-07.

---

## 0. Headline findings (things that changed vs. older mental models)

1. **`create_deep_agent` has no `instructions`/`mcp_servers` kwarg.** The prompt kwarg is `system_prompt`. MCP tools are passed in through `tools=`. `[W:deepagents==0.7.23 deepagents/graph.py:277-297]`
2. **Subagents have three shapes:** `SubAgent` (declarative), `CompiledSubAgent` (pre-built runnable) and `AsyncSubAgent`. `AsyncSubAgent` runs in the **background on a remote Agent Protocol / LangGraph server**, keyed by `graph_id`. `SubAgent` also has an experimental `mode="fork"`, which inherits the parent conversation. `[W:deepagents==0.7.23 deepagents/middleware/subagents.py:75-289, async_subagents.py]`
3. **Deep Agents' `SummarizationMiddleware` does not mutate `messages`.** It records a private `_summarization_event` in state and offloads the evicted history to `/conversation_history/{session_id}.md` on the backend. LangChain's own version instead rewrites `messages` with `RemoveMessage(REMOVE_ALL_MESSAGES)`. `[W:deepagents==0.7.23 deepagents/middleware/summarization.py:1-60, 1795-1810]`
4. **There are no `before_tool`/`after_tool` hooks and no `modify_model_request`.** All tool interception goes through `wrap_tool_call`/`awrap_tool_call`. The model-request hook is `wrap_model_call` plus `ModelRequest.override(...)`. (I grepped the `langchain==1.4.3` source for `modify_model_request`, `before_tool` and `after_tool` and found no matches.)
5. **`langchain==1.4.x` ships `langchain.mcp.MCPAdapter` (beta, FastMCP-based).** The docs changelog says it "replaces the standalone `langchain-mcp-adapters` package". `langchain-mcp-adapters` 0.3.2 is still published and still works. `[W:langchain==1.4.3 langchain/mcp/__init__.py]`, `[D:https://docs.langchain.com/oss/python/releases/changelog]`
6. **`deepagents-cli` is deprecated** (0.3.0 is marked "final release", classifier `Development Status :: 7 - Inactive`). Its replacements:
   - `managed-deepagents` (the `mda` CLI, with a `define_deep_agent(...)` authoring API) for deploying agents.
   - `deepagents-code` (`dcode`) for the interactive terminal agent. `dcode` implements **Claude-compatible command hooks (Hooks v2)** as LangChain middleware.

   `[W:deepagents-cli==0.3.0 METADATA]`, `[W:deepagents-code==0.1.83 deepagents_code/hooks/*]`

---

## 1. `create_deep_agent(...)` and subagents

### 1.1 Signature (deepagents 0.7.23)

```python
# [W:deepagents==0.7.23 deepagents/graph.py:277-297]
def create_deep_agent(
    model: str | BaseChatModel | None = None,          # None is deprecated since 0.5.3 (default claude-sonnet-4-6); removed in 1.0
    tools: Sequence[BaseTool | Callable | dict[str, Any]] | None = None,   # additive to built-ins
    *,
    system_prompt: str | SystemMessage | None = None,  # NOT "instructions"
    middleware: Sequence[AgentMiddleware] = (),
    subagents: Sequence[SubAgent | CompiledSubAgent | AsyncSubAgent] | None = None,
    skills: list[str] | None = None,                   # backend POSIX paths, e.g. ["/skills/user/", "/skills/project/"]
    memory: list[str] | None = None,                   # AGENTS.md paths, e.g. ["/memory/AGENTS.md"]
    permissions: list[FilesystemPermission] | None = None,
    backend: BackendProtocol | None = None,            # default StateBackend()
    interrupt_on: dict[str, bool | InterruptOnConfig] | None = None,
    response_format: ResponseFormat | type | dict | None = None,
    state_schema: type[DeepAgentState] | None = None,  # must subclass DeepAgentState
    context_schema: type[ContextT] | None = None,
    checkpointer: Checkpointer | None = None,
    store: BaseStore | None = None,                    # required if backend uses StoreBackend
    debug: bool = False,
    name: str | None = None,
    cache: BaseCache | None = None,
) -> CompiledStateGraph
```

- Built-in tools are `ls`, `read_file`, `write_file`, `edit_file`, `glob`, `grep`, `execute` and `task`.
  - The `execute` tool is filtered out on every model call when the backend does not implement `SandboxBackendProtocol`.
  - In 0.7.23, `TodoListMiddleware` (`write_todos`) is **not** in the default stack. It is only added as `extra_middleware` by the built-in OpenAI Codex harness profile (`profiles/harness/_openai_codex.py`). To get it otherwise, add `TodoListMiddleware()` via `middleware=`.
- Prompt assembly order is `USER (system_prompt)` → `BASE` → `SUFFIX`. `BASE` and `SUFFIX` come from the active `HarnessProfile`.
- The returned graph is `.with_config({"recursion_limit": 9_999, "metadata": {"ls_integration": "deepagents", "lc_agent_name": name, ...}})`. `[W graph.py:666-675]`

**Default middleware order** `[W graph.py docstring + 552-633]`:

1. Base stack:
   1. `FilesystemMiddleware`
   2. `SubAgentMiddleware` (only if there are sync subagents)
   3. `SummarizationMiddleware` (deepagents variant)
   4. `PatchToolCallsMiddleware`
   5. `AsyncSubAgentMiddleware` (only if there are async subagents)
2. **User `middleware=` is inserted here.**
3. Tail stack:
   1. HarnessProfile `extra_middleware`
   2. `SkillsMiddleware` (if `skills`)
   3. `AnthropicPromptCachingMiddleware(unsupported_model_behavior="ignore")`, plus the Bedrock/Fireworks caching middleware when those packages are installed
   4. `MemoryMiddleware` (if `memory`)
   5. `HumanInTheLoopMiddleware` (if `interrupt_on` or interrupt-mode permissions)
   6. `UnsupportedContentMiddleware`
   7. `_ToolExclusionMiddleware`

If a user middleware has the same `.name` as a built-in, it **replaces the built-in in place**. For example, passing your own `SummarizationMiddleware` swaps out the default one.

### 1.2 `SubAgent` TypedDict

```python
# [W:deepagents==0.7.23 deepagents/middleware/subagents.py:75-197]
class SubAgent(TypedDict):
    name: str                       # required; used as task(subagent_type=...)
    description: str                # required; drives delegation
    system_prompt: NotRequired[str] # empty if omitted; under mode="fork" it is APPENDED to parent prompt
    tools: NotRequired[Sequence[BaseTool | Callable | dict]]   # if key absent -> inherits parent tools
    model: NotRequired[str | BaseChatModel]                    # defaults to parent model
    middleware: NotRequired[list[AgentMiddleware]]             # appended after FS/Summarization/PatchToolCalls
    interrupt_on: NotRequired[dict[str, bool | InterruptOnConfig]]  # overrides inherited top-level interrupt_on
    skills: NotRequired[list[str]]                             # forbidden under mode="fork"
    permissions: NotRequired[list[FilesystemPermission]]       # replaces parent's rules entirely
    response_format: NotRequired[ResponseFormat | type | dict]
    mode: NotRequired[Literal["isolated", "fork"]]             # default "isolated"; "fork" is experimental/beta
```

### 1.3 `CompiledSubAgent`

```python
class CompiledSubAgent(TypedDict):
    name: str
    description: str
    runnable: Runnable          # state must include "messages"
    mode: NotRequired[Literal["isolated", "fork"]]
```

- If `structured_response` is set on the result, the parent JSON-serializes it into the `ToolMessage`. Otherwise the parent uses the last non-empty `AIMessage` text.
- Compiled subagents inherit neither the parent's `interrupt_on` nor its `state_schema`. `[W subagents.py:199-283]`

### 1.4 How `task` delegates

- `SubAgentMiddleware(backend=..., subagents=[...], system_prompt=None, task_description=None, state_schema=None)` registers a single `task` tool.
- `TaskToolSchema` has exactly two fields, `description: str` and `subagent_type: str`. Unknown keys are rejected with a validation error. `[W subagents.py:404-440, 861-]`
- On each call, the tool:
  1. Builds the subagent's input state. Under `isolated`, only the task description goes in as a `HumanMessage`, and the parent's `messages`, `todos`, `structured_response`, skills keys and private keys are excluded.
  2. Calls `subagent.invoke(state, {"configurable": {"ls_agent_type": "subagent"}})` (or `ainvoke`). The parent's callbacks, tags and metadata propagate through `ensure_config`.
  3. Returns `Command(update={**state_update, "messages": [ToolMessage(content, tool_call_id=...)]})`. `[W subagents.py:~700-735]`
- LangSmith traces get the tag `ls_agent_type="subagent"`. `[W subagents.py:500-522]`
- **Parallel subagents:** the tool description tells the model to "launch multiple agents concurrently … using a single message with multiple tool calls". They run in parallel as concurrent tool calls in one `tools` step, and each one blocks that step until it finishes. `[W subagents.py:442-453]`

### 1.5 `general-purpose` default subagent

If you pass no subagent named `general-purpose`, one is auto-added and placed first in the list.

- **Prompt:** `DEFAULT_SUBAGENT_PROMPT`.
- **Tools and model:** the same as the parent's.
- **Middleware:** its own FS, Summarization and PatchToolCalls instances, plus skills if `skills=` was given.
- **Disabling it:** set `HarnessProfile(general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False))`. If you also pass no sync subagents, no `task` tool is exposed.

`[W graph.py:481-550]`

### 1.6 Async / background subagents

```python
# [W:deepagents==0.7.23 deepagents/middleware/async_subagents.py]
class AsyncSubAgent(TypedDict):
    name: str
    description: str
    graph_id: str                       # graph name / assistant id on the remote server
    url: NotRequired[str]               # omit => in-process ASGI transport (ainvoke only)
    headers: NotRequired[dict[str, str]]
```

- These are routed to `AsyncSubAgentMiddleware`. It uses `langgraph_sdk` to start **background runs on an Agent Protocol server** (LangGraph Platform/LangSmith Deployment or self-hosted).
- It exposes five tools: `start_async_task`, `check_async_task`, `update_async_task`, `cancel_async_task` and `list_async_tasks`.
- Tracked tasks are stored in state as `async_tasks: dict[str, AsyncTask]`. Each `AsyncTask` has these fields: `task_id`, `agent_name`, `thread_id`, `run_id`, `status`, `created_at`, `last_checked_at` and `last_updated_at`.
- Auth: the SDK reads `LANGGRAPH_API_KEY`, `LANGSMITH_API_KEY` or `LANGCHAIN_API_KEY` from the environment. For a self-hosted server, pass `headers`.
- Local in-process (non-HTTP) background subagents are **not** offered.

---

## 2. LangChain v1 middleware (`langchain==1.4.3`)

### 2.1 `langchain.agents.middleware.AgentMiddleware`

`[W:langchain==1.4.3 langchain/agents/middleware/types.py:385-880]`

```python
class AgentMiddleware(Generic[StateT, ContextT, ResponseT]):
    state_schema: type[StateT] = _DefaultAgentState   # extend agent state (merged by create_agent)
    tools: Sequence[BaseTool]                         # extra tools contributed by middleware
    trace_policy: TracePolicy | None = None           # e.g. TracePolicy(process_inputs=omit_payload)
    transformers: Sequence[TransformerFactory] = ()   # stream transformers (event-streaming v3)
    @property
    def name(self) -> str: ...                        # defaults to class name; must be unique per agent

    # node-style (return dict state update | None; may include {"jump_to": ...})
    def before_agent(self, state, runtime) -> dict | None      # + abefore_agent
    def before_model(self, state, runtime) -> dict | None      # + abefore_model
    def after_model(self, state, runtime) -> dict | None       # + aafter_model
    def after_agent(self, state, runtime) -> dict | None       # + aafter_agent
    # wrap-style
    def wrap_model_call(self, request: ModelRequest, handler) -> ModelResponse | AIMessage | ExtendedModelResponse   # + awrap_model_call
    def wrap_tool_call(self, request: ToolCallRequest, handler) -> ToolMessage | Command   # + awrap_tool_call
```

- `ModelRequest` has these fields: `model`, `messages` (without the system message), `system_message`, `tool_choice`, `tools`, `response_format`, `state`, `runtime` and `model_settings`. Change it with `request.override(...)`. `[W types.py:88-210]`
- `ToolCallRequest` comes from `langgraph.prebuilt.tool_node`. Its fields are `tool_call`, `tool`, `state` and `runtime: ToolRuntime`. `[W:langgraph-prebuilt==1.1.0]`
- Jumps:
  - `JumpTo = Literal["tools", "model", "end"]`. `[W types.py:68]`
  - Declare them with `@hook_config(can_jump_to=[...])` on class methods, or `@before_model(can_jump_to=[...])` on decorator-style functions.
  - Then return `{"jump_to": "end"}`. `end` goes to the end of the run, or to the first `after_agent` if one exists.
- Wrap hooks update state by returning `ExtendedModelResponse(model_response=..., command=Command(update=...))` from `wrap_model_call`, or a `Command` from `wrap_tool_call`. `[D:https://docs.langchain.com/oss/python/langchain/middleware/custom]`
- Execution order:
  - `before_*` hooks run first to last.
  - `after_*` hooks run last to first.
  - `wrap_*` hooks nest, so the first middleware is the outermost wrapper. `[D: same]`
- Decorators exported from `langchain.agents.middleware` are `before_agent`, `before_model`, `after_model`, `after_agent`, `wrap_model_call`, `wrap_tool_call`, `dynamic_prompt` and `hook_config`. Each node/wrap decorator accepts `state_schema=` (and `can_jump_to=` where it applies).

Minimal hook-script style example (sketch, not run):

```python
from langchain.agents.middleware import AgentMiddleware, hook_config
from langchain_core.messages import ToolMessage

class PolicyHooks(AgentMiddleware):
    def wrap_tool_call(self, request, handler):
        if blocked(request.tool_call):                        # PreToolUse
            return ToolMessage("denied by policy", tool_call_id=request.tool_call["id"], status="error")
        result = handler(request)                              # tool executes
        audit(request.tool_call, result)                       # PostToolUse
        return result
    @hook_config(can_jump_to=["end"])
    def after_model(self, state, runtime):                     # post-model / stop gate
        return {"jump_to": "end"} if should_stop(state) else None
```

### 2.2 Built-in middleware exported by `langchain.agents.middleware` (1.4.3)

Full export list: `AgentMiddleware`, `AgentState`, `ClearToolUsesEdit`, `CodexSandboxExecutionPolicy`, `ContextEditingMiddleware`, `DockerExecutionPolicy`, `ExtendedModelResponse`, `FilesystemFileSearchMiddleware`, `HostExecutionPolicy`, `HumanInTheLoopMiddleware`, `InputAgentState`, `InterruptOnConfig`, `LLMToolEmulator`, `LLMToolSelectorMiddleware`, `ModelCallLimitMiddleware`, `ModelCallResult`, `ModelFallbackMiddleware`, `ModelRequest`, `ModelResponse`, `ModelRetryMiddleware`, `OutputAgentState`, `PIIDetectionError`, `PIIMatch`, `PIIMiddleware`, `ProviderToolSearchMiddleware`, `RedactionRule`, `Runtime`, `ShellToolMiddleware`, `SummarizationMiddleware`, `TodoListMiddleware`, `ToolCallLimitMiddleware`, `ToolCallRequest`, `ToolErrorMiddleware`, `ToolRetryMiddleware`, `TracePolicy`, `TriggerClause`, plus the decorators and `configure_trace_policy` / `omit_payload`. `[W langchain/agents/middleware/__init__.py]`

Constructor signatures, copied from source:

| Middleware | Constructor (1.4.3) |
|---|---|
| `SummarizationMiddleware` | `(model, *, trigger: ContextSize \| TriggerClause \| list[...] \| None = None, keep: ContextSize = ("messages", 20), token_counter=count_tokens_approximately, summary_prompt=DEFAULT_SUMMARY_PROMPT, trim_tokens_to_summarize: int \| None = 4000)`. `ContextSize = ("fraction", float) \| ("tokens", int) \| ("messages", int)`. A dict trigger means AND; a list means OR. It rewrites `messages` in `before_model`. |
| `HumanInTheLoopMiddleware` | `(interrupt_on: dict[str, bool \| InterruptOnConfig], *, description_prefix="Tool execution requires approval", edit_notice=...)`. `InterruptOnConfig = {allowed_decisions: list["approve"\|"edit"\|"reject"\|"respond"], description?: str \| callable, args_schema?: dict, when?: Callable[[ToolCallRequest], bool]}`. It calls `interrupt(HITLRequest)` and reads back `["decisions"]`. Resume with `Command(resume={"decisions": [{"type": "approve"} \| {"type": "edit", "edited_action": {...}} \| {"type": "reject", "message"?: str} \| {"type": "respond", "message": str}]})`. |
| `TodoListMiddleware` | `(*, system_prompt=WRITE_TODOS_SYSTEM_PROMPT, tool_description=WRITE_TODOS_TOOL_DESCRIPTION)`. Adds the `write_todos` tool. |
| `ToolCallLimitMiddleware` | `(*, tool_name=None, thread_limit=None, run_limit=None, exit_behavior: "continue"\|"error"\|"end" = "continue")` |
| `ModelCallLimitMiddleware` | `(*, thread_limit=None, run_limit=None, exit_behavior: "end"\|"error" = "end")` |
| `LLMToolSelectorMiddleware` | `(*, model=None, system_prompt=..., max_tools=None, always_include=None, max_retries=1, on_parsing_failure="error")` |
| `ContextEditingMiddleware` | `(*, edits=None, token_count_method="approximate"\|"model", token_counter=None)`. Pairs with `ClearToolUsesEdit(trigger=100_000, clear_at_least=0, keep=3, clear_tool_inputs=False, exclude_tools=(), placeholder=...)`. |
| `ShellToolMiddleware` | `(workspace_root=None, *, startup_commands=None, shutdown_commands=None, execution_policy: HostExecutionPolicy\|DockerExecutionPolicy\|CodexSandboxExecutionPolicy\|None, redaction_rules=None, tool_description=None, tool_name="shell", shell_command=None, env=None)` |
| `ToolRetryMiddleware` | `(*, max_retries=2, tools=None, retry_on=..., on_failure="continue", backoff_factor=2.0, initial_delay=1.0, max_delay=60.0, jitter=True)`. `ModelRetryMiddleware` has the same signature without `tools`. |
| `ModelFallbackMiddleware` | `(first_model, *additional_models)` |
| `PIIMiddleware` | `(pii_type: "email"\|"credit_card"\|"ip"\|"mac_address"\|"url"\|str, *, strategy: "block"\|"redact"\|"mask"\|"hash" = "redact", detector=None, apply_to_input=True, apply_to_output=False, apply_to_tool_results=False)` |
| `ToolErrorMiddleware` | `(on_error=None, *, aon_error=None, tools=None)`. New in 1.4.x per source. |
| `AnthropicPromptCachingMiddleware` | Lives in `langchain_anthropic.middleware`, not in `langchain`. deepagents adds it unconditionally with `unsupported_model_behavior="ignore"`. `[W deepagents/middleware/_prompt_caching.py]` |

### 2.3 Deep Agents middleware (`deepagents.middleware`, 0.7.23)

Public exports:

- `FilesystemMiddleware`, `FilesystemPermission`
- `SubAgentMiddleware`, `SubAgent`, `CompiledSubAgent`
- `AsyncSubAgentMiddleware`, `AsyncSubAgent`
- `SummarizationMiddleware`, `SummarizationToolMiddleware` (adds a `compact_conversation` tool), `create_summarization_tool_middleware`, `DEEPAGENTS_DEFAULT_SUMMARY_PROMPT`
- `SkillsMiddleware`, `SkillMetadata`, `SkillsState`, `SkillToolResolver`
- `MemoryMiddleware`
- `RubricMiddleware` and its grader types
- `UnsupportedContentMiddleware`

Modules not re-exported from the package root: `patch_tool_calls.PatchToolCallsMiddleware`, `permissions` (a re-export) and private `_*` modules (`_blob_offload`, `_message_eviction`, `_overflow_clip`, `_skill_tools`, `_tool_exclusion`, `_video`, ...). `[W deepagents/middleware/__init__.py]`

- **`FilesystemMiddleware`**:
  - Signature: `(*, backend=None, system_prompt=None, custom_tool_descriptions=None, tool_token_limit_before_evict=20000, human_message_token_limit_before_evict=50000, max_execute_timeout=3600, grep_max_count=1000, tools: list[FsToolName] | "all" | None = None, offload_binary_content=False)`.
  - Large tool results are evicted to files on the backend.
- **`FilesystemPermission(operations=[...], paths=["/secrets/**"], mode="allow"|"deny"|"interrupt")`**:
  - The first matching rule wins.
  - `interrupt` mode auto-installs HITL.
  - Rules are enforced at the tool level, not the backend level.
- **`SummarizationMiddleware`** (deepagents variant):
  - Signature: `(model, *, backend, trigger=None, keep=("messages", 20), token_counter=..., summary_prompt=DEEPAGENTS_DEFAULT_SUMMARY_PROMPT, trim_tokens_to_summarize=4000, truncate_args_settings=None)`.
  - Defaults from `create_summarization_middleware`: if the model profile exposes `max_input_tokens`, then `trigger=("fraction",0.85)` and `keep=("fraction",0.10)`; otherwise `("tokens",170000)` / `("messages",6)`.
  - On a provider `ContextOverflowError` it summarizes and retries.
  - It writes private state `_summarization_event: {cutoff_index, summary_message: HumanMessage, file_path}` and `_summarization_session_id`.
  - The offload file is `/conversation_history/{session_id}.md`, with an append-only section per event. Media goes under `<artifacts_root>/conversation_history/media/`.
  - `[W summarization.py:131-215, 262-300, 547-600, 1772-1830]`
- **`SkillsMiddleware`**:
  - Signature: `(*, backend, sources: Sequence[str | (path, label)], system_prompt=SKILLS_SYSTEM_PROMPT, tools: list | SkillToolResolver | None = None)`.
  - Each skill is `<source>/<skill-name>/SKILL.md` with YAML frontmatter: `name` (at most 64 chars, lowercase alphanumerics and hyphens), `description` (at most 1024 chars), plus optional `license`, `compatibility`, `metadata` and `allowed-tools`.
  - Disclosure is progressive: metadata goes into the system prompt, and the model reads the full file on demand.
  - When two sources define the same name, the later source wins.
  - Skills load in `before_agent` into `state["skills_metadata"]`.
  - `metadata.include_tools` in a skill reveals extra tools after the model reads that skill.
  - With `StateBackend`, pass skill files in through `invoke(files={...})`. With `FilesystemBackend`, paths are relative to `root_dir`.
  - `[W skills.py:1-110, 916-1060]`
- **`MemoryMiddleware`**:
  - Signature: `(*, backend, sources: list[str], add_cache_control=False, system_prompt=MEMORY_SYSTEM_PROMPT)`.
  - Loads AGENTS.md files (agents.md spec) in order, strips HTML comments, and injects the content into the system prompt.
  - `create_deep_agent(memory=[...])` enables it with `add_cache_control=True`.
- **`PatchToolCallsMiddleware`**: in `before_agent`, adds `ToolMessage`s for dangling tool calls (AIMessage tool calls that never got an answer). This matters when you resume or fork a thread mid-tool-call.

---

## 3. Backends (`deepagents.backends`, 0.7.23)

Exports: `BackendProtocol`, `StateBackend`, `FilesystemBackend`, `StoreBackend`, `NamespaceFactory`, `CompositeBackend`, `LocalShellBackend`, `LangSmithSandbox`, `ContextHubBackend`, `DEFAULT_EXECUTE_TIMEOUT`. `SandboxBackendProtocol` and `BaseSandbox` live in `deepagents.backends.protocol` and `deepagents.backends.sandbox`. `[W backends/__init__.py]`

### 3.1 Protocol methods

`[W backends/protocol.py:404-935]`

`BackendProtocol` (abstract base class). Every method has an `a*` async twin:

- `ls(path) -> LsResult`
- `read(file_path, offset=0, limit=2000) -> ReadResult`
- `grep(...) -> GrepResult` (literal matching, not regex, with `max_count`)
- `glob(pattern, path=None) -> GlobResult`
- `write(file_path, content) -> WriteResult` (absolute path; overwrites)
- `edit(file_path, old_string, new_string, replace_all=False) -> EditResult`
- `delete(file_path) -> DeleteResult`
- `upload_files(files: list[tuple[str, bytes]]) -> list[FileUploadResponse]`
- `download_files(paths: list[str]) -> list[FileDownloadResponse]`

`SandboxBackendProtocol(BackendProtocol)` adds:

- an `id` property
- `execute(command, *, timeout=None) -> ExecuteResponse` and `aexecute`. `ExecuteResponse` carries the combined output, the exit code and a truncation flag.

`BaseSandbox` implements every file operation on top of `execute()`, so a new provider only has to implement `execute`. `[D:https://docs.langchain.com/oss/python/deepagents/sandboxes]`

File data shape: `FileData = {content: str (utf-8 or base64), encoding: "utf-8"|"base64", created_at?, modified_at?}`. Use `deepagents.backends.utils.create_file_data(content, created_at=None, encoding="utf-8")` to build one.

### 3.2 Concrete backends

| Backend | Construction | Notes |
|---|---|---|
| `StateBackend()` | no args | Files live in graph state under the `files` key. They are checkpointed per thread and do not cross threads. Only usable **inside** graph execution. To pre-populate, use `agent.invoke({"messages": [...], "files": {...}})`. `[W backends/state.py:38-78]` |
| `FilesystemBackend(root_dir=None, virtual_mode=True, max_file_size_mb=10)` | | With `virtual_mode=True`, paths are anchored at `root_dir` and `..`/`~` are blocked. `False` gives unrestricted host access. `[W filesystem.py:139-180]` |
| `LocalShellBackend(root_dir=None, *, virtual_mode=True, timeout=DEFAULT_EXECUTE_TIMEOUT, max_output_bytes=100_000, env=None, inherit_env=False)` | | Extends `FilesystemBackend` with **unsandboxed** host shell execution. Intended for development use. |
| `StoreBackend(*, namespace: NamespaceFactory, store: BaseStore \| None = None)` | `namespace=lambda rt: (..., "filesystem")` | Persistent across threads via LangGraph `BaseStore`. Wildcards are forbidden in namespaces. |
| `CompositeBackend(default, routes: dict[str, Backend], *, artifacts_root="/")` | `CompositeBackend(default=StateBackend(), routes={"/memories/": StoreBackend(namespace=ns)})` | Routes by longest path prefix. Prefixes must start with `/` and should end with `/`. |
| `LangSmithSandbox(sandbox: langsmith.sandbox.Sandbox)` | `SandboxClient().create_sandbox(name=..., idle_ttl_seconds=3600)` | Needs `pip install "langsmith[sandbox]"`. Default execute timeout is 30 minutes. |
| `ContextHubBackend` | | LangSmith Context Hub-backed store. **UNVERIFIED** semantics; I did not inspect it. |

Partner sandbox packages, versions as of 2026-10-07 `[D:https://docs.langchain.com/oss/python/deepagents/sandboxes]`, `[PyPI]`:

| Package | Class | Created from |
|---|---|---|
| `langchain-daytona` 0.0.8 | `langchain_daytona.DaytonaSandbox` | `sandbox=Daytona().create()` |
| `langchain-modal` 0.0.6 | `langchain_modal.ModalSandbox` | `sandbox=modal.Sandbox.create(app=app)` |
| `langchain-runloop` 0.0.7 | `langchain_runloop.RunloopSandbox` | `devbox=RunloopSDK(...).devbox.create()` |
| `langchain-e2b` | `E2BSandbox` | `sandbox=e2b.Sandbox.create()` |
| `langchain-vercel-sandbox` | `VercelSandbox` | (constructor not recorded) |
| `langchain-agentcore-codeinterpreter` | `AgentCoreSandbox` | `interpreter=CodeInterpreter(...)` |
| `langchain-nvidia-openshell` | `OpenShellSandbox` | (constructor not recorded) |

There is **no first-party `DockerSandbox` backend**. Docker shows up only as `DockerExecutionPolicy` for LangChain's `ShellToolMiddleware`. `langchain-sandbox` (0.0.6, last released 2025-05) is an unrelated, older Pyodide project.

### 3.3 Pre-loading artifacts before a run

- **Sandbox or filesystem backends:** call `backend.upload_files([("/workspace/input.csv", b"..."), ...])` from application code **before** `invoke`. The docs call this "Seeding the sandbox"; it uses provider-native transfer, not shell commands. After the run, pull outputs with `download_files([...])`. `[D:…/deepagents/sandboxes#seeding-the-sandbox]`
- **`StateBackend`:** you cannot call it outside the graph. Seed files in the input instead: `agent.invoke({"messages": [...], "files": {"/skills/x/SKILL.md": create_file_data(text), ...}})`. You can also use `graph.update_state(config, {"files": {...}})` on the thread. **UNVERIFIED** that `update_state` works for this particular use, but it is the standard LangGraph state write. `[W state.py:62-77]`
- **`StoreBackend`:** write to the `BaseStore` directly under the namespace the factory returns. **UNVERIFIED** item key layout; read `store.py` before relying on it.
- **`CompositeBackend`:** calling `upload_files` on the composite routes each path to the correct child backend. **UNVERIFIED** for every child type, especially a `StateBackend` default used outside graph context, which raises.

---

## 4. State, checkpoints, streaming (langgraph 1.2.14, langgraph-checkpoint-postgres 3.1.2)

- **Checkpointer:**

  ```python
  async with AsyncPostgresSaver.from_conn_string(DSN, pipeline=False, serde=None) as cp:
      await cp.setup()  # call once
  ```

  `[W:langgraph-checkpoint-postgres==3.1.2 checkpoint/postgres/aio.py:65-98]`. The package also has a `shallow` module (latest checkpoint only) and `langgraph.store.postgres` (`AsyncPostgresStore`) for `StoreBackend`.
- **Thread and checkpoint addressing:** `config={"configurable": {"thread_id": ..., "checkpoint_id"?: ..., "checkpoint_ns"?: ...}}`.
- **Subgraph (subagent) checkpoints:**
  - They live under a non-empty `checkpoint_ns`. The streaming namespace for a deepagents subagent is `("tools:<pregel_task_id>",)`. `[D:https://docs.langchain.com/oss/python/deepagents/streaming]`
  - **UNVERIFIED:** that declarative subagents are checkpointed into the parent thread. They are compiled with no checkpointer, so per `Checkpointer = None` they inherit the parent's. `[W langgraph/types.py:109-115]`
- **Reading history:** `get_state(config, *, subgraphs=False) -> StateSnapshot` and `get_state_history(config, *, filter=None, before=None, limit=None)`, which returns newest first. Both have `a*` variants. `[W pregel/main.py:1460-1520]`
- **Writing state:** `update_state(config, values, as_node=None, task_id=None) -> RunnableConfig` and `bulk_update_state(config, supersteps)`, with async variants. `[W pregel/main.py:2593-2618]`
- **Time travel:**
  - **Replay:** `graph.invoke(None, past_snapshot.config)` re-executes the nodes after that checkpoint.
  - **Fork:** `fork_cfg = graph.update_state(past_snapshot.config, values={...})`, then `graph.invoke(None, fork_cfg)`.
  - In the docs' words, `update_state` "does not roll back a thread. It creates a new checkpoint that branches from the specified point. The original execution history remains intact." `[D:https://docs.langchain.com/oss/python/langgraph/use-time-travel]`
  - **Fork across threads:** to fork into a *new* `thread_id` (a separate mission branch), copy the state values with `update_state` onto a fresh thread. This is **UNVERIFIED**: LangGraph has no first-class "copy thread" in the OSS checkpointer; LangGraph Platform's `threads.copy` is a server-side feature.
- **Interrupts:**
  - Pause with `langgraph.types.interrupt(value)`.
  - Resume with `graph.invoke(Command(resume=...), config)`.
  - With streaming, interrupts appear as `"__interrupt__"` entries in `updates` chunks.
- **`stream_mode`:** `"values" | "updates" | "checkpoints" | "tasks" | "debug" | "messages" | "custom"`. `[W langgraph/types.py:131-145]`
  - Signature: `stream(input, config, *, context=None, stream_mode=None, print_mode=(), output_keys=None, interrupt_before=None, interrupt_after=None, durability=None, control: RunControl | None = None, subgraphs=False, debug=None, version: "v1"|"v2" = "v1")`.
  - With `version="v2"`, each chunk is a typed `StreamPart` dict: `{"type", "ns", "data"}`.
  - With `subgraphs=True` and v1, chunks are `(namespace, data)`, or `(namespace, mode, data)` when you request multiple modes.
  - `messages` metadata includes `langgraph_node`, `langgraph_triggers`, `langgraph_path` and `langgraph_checkpoint_ns`. `[W pregel/_algo.py:656-659]`
  - **Attributing to subagents:**
    - Prefer `ns[0].startswith("tools:")`, or `langgraph_checkpoint_ns`.
    - `metadata["lc_agent_name"]` is set from `create_agent(name=...)`, and deepagents passes `SubAgent.name` as that name. `[W langchain/agents/factory.py:1897-1902; deepagents subagents.py create_sub_agent]`
    - The pregel task id in `ns` is **not** the `tool_call_id`. `[D:…/deepagents/streaming]`
- **`astream_events`:**
  - Defaults to `version="v2"` on Pregel. An experimental `version="v3"` uses a transformer mux and forbids passing `stream_mode` or `subgraphs`. `[W pregel/main.py:3685-3845]`
  - v2 events have this shape: `{event, name, run_id, parent_ids (root→immediate parent; v2 only), tags, metadata, data}`.
  - Event names: `on_chat_model_start|stream|end`, `on_llm_*`, `on_chain_start|stream|end`, `on_tool_start|end` (plus `on_tool_error`, **UNVERIFIED** in table), `on_retriever_*`, `on_prompt_*` and `on_custom_event`. `[W:langchain-core==1.6.7 runnables/base.py:1383-1440]`
- **`durability`:**

  ```python
  Durability = Literal["sync", "async", "exit"]   # default "async"
  ```

  - `sync`: persist before the next step starts.
  - `async`: persist while the next step runs.
  - `exit`: persist only when the graph exits.
  - `checkpoint_during` is deprecated in favor of `durability`. `[W langgraph/types.py:98-104; pregel/main.py stream docstring]`
- **Long runs:**
  - `create_agent` and `create_deep_agent` both bind `recursion_limit=9_999`. `[W factory.py:1897-1899; deepagents graph.py:668]`
  - Exceeding it raises `GraphRecursionError`. Override per call with `config={"recursion_limit": N}`.
  - The `RemainingSteps` managed value is available to state schemas.
  - `RunControl().request_drain(reason)` asks a run to drain cooperatively, for example on shutdown. `[W langgraph/runtime.py:79-100]`

---

## 5. MCP

- **`langchain-mcp-adapters` 0.3.2:** `MultiServerMCPClient(connections: dict[str, Connection] | None, *, callbacks=None, tool_interceptors=None, tool_name_prefix=False, handle_tool_errors=True)`. `[W client.py:48-126]`
  - Connection shapes `[W sessions.py:82-207]`:
    - `StdioConnection`: `{transport:"stdio", command, args, env?, cwd?, encoding?, session_kwargs?}`
    - `SSEConnection`: `{transport:"sse", url, headers?, timeout?, sse_read_timeout?, auth?: httpx.Auth, httpx_client_factory?}`
    - `StreamableHttpConnection`: `{transport:"streamable_http"` (also accepts `"streamable-http"` or `"http"`)`, url, headers?, timeout?, sse_read_timeout?, terminate_on_close?, auth?, httpx_client_factory?}`
    - `WebsocketConnection`: `{transport:"websocket", url}`
  - `await client.get_tools(server_name=None)` returns tools that each open **a new session per tool call**.
  - For a long-lived session, use `async with client.session("name") as s: tools = await load_mcp_tools(s)`. The tools are valid only while that session is open.
  - `async with MultiServerMCPClient(...)` now raises `NotImplementedError`; context-manager support was removed.
  - Other methods: `get_prompt`, `get_resources`, `get_server_info`.
  - `ToolCallInterceptor` / `MCPToolCallRequest` let you intercept MCP calls, for example to inject auth per call.
- **`langchain.mcp.MCPAdapter`** (beta; ships with `langchain` 1.4.x; install with `pip install "langchain[mcp]"`, which pulls `fastmcp>=4.0.1,<5`):
  - Usage: `async with MCPAdapter(target) as a: tools = await a.list_tools()`.
  - `target` can be an http(s) URL, a `Path`, a FastMCP transport or client, or an `MCPConfig` dict such as `{"mcpServers": {...}}`. A bare string must be an http(s) URL.
  - When a server asks for input mid-call (elicitation), the adapter turns it into a LangGraph `interrupt()`.
  - The deepagents docs now use this adapter in their MCP example. `[W langchain/mcp/adapter.py:125-221]`, `[D:https://docs.langchain.com/oss/python/deepagents/tools]`
- **Native MCP on `create_deep_agent`: none.**
  - The only mention of "mcp" anywhere in `deepagents` 0.7.23 is in `skills.py`.
  - The pattern is to load the tools and pass `tools=`, or per subagent with `SubAgent["tools"]`.
  - Native MCP selection exists only in `managed-deepagents.define_deep_agent(mcp=McpSelection...)` (platform-hosted) and in `dcode`'s own config.

---

## 6. Deep Agents CLI / reusable agent-definition formats

- **`deepagents-cli` 0.3.0 (2026-08-24): deprecated, final release.**
  - It was deployment-only (`deepagents init|deploy|agents|mcp-servers`).
  - Project layout: `agent.json` (name, description, backend `{type: "state"|"sandbox", sandbox_config: {scope: "thread"|"agent", policy_ids, ttl...}}`, `runtime.model`, permissions), `AGENTS.md` (the system prompt), `tools.json` (MCP-server-referenced tools), `skills/<name>/SKILL.md` and `subagents/<name>/`.
  - Superseded by `managed-deepagents` 0.9.0 (`mda` CLI, plus a `define_deep_agent(*, name, model, tools, middleware, subagents, permissions, interrupt_on, response_format, context_schema, instructions, mcp, sandbox, skills, memory, metadata, ...)` authoring API). `[W:deepagents-cli==0.3.0 METADATA]`, `[W:managed-deepagents==0.9.0 define_deep_agent.py:78-110]`
- **`deepagents-code` 0.1.83 (`dcode`), the interactive coding CLI** `[W deepagents_code/_paths.py, agent.py, subagents.py, hooks/*]`:
  - **Profile directory:** `~/.deepagents/<agent>/`. A directory counts as an agent only if it contains `AGENTS.md`.
  - **Memory:** `~/.deepagents/<agent>/AGENTS.md`, plus project-level `AGENTS.md` files.
  - **Skills, lowest to highest precedence:**
    1. built-in
    2. plugins
    3. `~/.deepagents/<agent>/skills`
    4. `~/.agents/skills`
    5. project `.deepagents/skills`
    6. project `.agents/skills`
    7. `~/.claude/skills` (experimental)
    8. project `.claude/skills` (experimental)
  - **Custom subagents:**
    - User-level at `~/.deepagents/<agent>/agents/<name>/AGENTS.md`; project-level at `.deepagents/agents/<name>/AGENTS.md`.
    - Each is YAML frontmatter `{name? (defaults to folder name), description (required), model? "provider:model"}`; the markdown body is the `system_prompt`.
    - Async subagents are configured under `[async_subagents.<name>]` in `~/.deepagents/config.toml`.
  - **Hooks v2** (Claude-compatible envelope):
    - Config files: `.deepagents/hooks.json` (project), `~/.deepagents/hooks.json` (user) and plugin `hooks.json` files.
    - Format: `{"hooks": {"<Event>": [{"matcher": ..., "hooks": [{"type": "command", "command": ..., "timeout"?, "async"?}]}]}}`.
    - Events: `SessionStart`, `UserPromptSubmit`, `SessionEnd`, `PermissionRequest`, `Notification`, `PreToolUse`, `PostToolUse`, `PostToolUseFailure`, `PreCompact`, `Stop`, `SubagentStart`, `SubagentStop`.
    - **Implementation:** `ServerHooksMiddleware(AgentMiddleware)` uses `before_agent`, `before_model`, `after_model`, `wrap_tool_call` and `after_agent`.
      - It emits events through LangGraph `interrupt()` so the client process runs the command and returns a typed decision.
      - `PreCompact` fires when the model calls the compact tool.
    - Legacy hooks are scheduled for removal on 2026-09-01.
  - **Reusability verdict:**
    - The subagent `.md` format (frontmatter `name`, `description`, `model` plus a prompt body) maps almost 1:1 onto `SubAgent` dicts.
    - The `agent.json` / `AGENTS.md` / `skills/` / `subagents/` project layout from the CLI and `mda` is the closest thing to a portable "agent definition".
    - Neither format is an `SDK`-level loader in `deepagents` itself. `create_deep_agent` takes Python objects only.

---

## 7. Pinned versions (PyPI JSON API, read 2026-10-07)

| Package | Latest | Uploaded (UTC) | Python |
|---|---|---|---|
| `deepagents` | **0.7.23** | 2026-10-07 16:45 | >=3.11,<4 |
| `langchain` | **1.4.3** | 2026-09-28 20:17 | >=3.10,<4 |
| `langgraph` | **1.2.14** | 2026-10-06 14:41 | >=3.10 |
| `langchain-core` | **1.6.7** | 2026-10-06 15:46 | >=3.10,<4 |
| `langgraph-checkpoint-postgres` | **3.1.2** | 2026-08-07 20:39 | >=3.10 |
| `langgraph-checkpoint` | 4.2.0 | 2026-08-07 20:05 | >=3.10 |
| `langgraph-prebuilt` | 1.1.0 | 2026-05-12 | |
| `langgraph-sdk` | 0.4.6 | 2026-10-06 | |
| `langchain-mcp-adapters` | **0.3.2** | 2026-08-06 06:15 | >=3.10 |
| `langchain-anthropic` | **1.7.5** | 2026-09-29 15:22 | >=3.10,<4 |
| `langchain-openai` | **1.6.7** | 2026-09-30 15:12 | >=3.10,<4 |
| `langsmith` | 0.14.4 | 2026-10-02 | |
| `deepagents-cli` | 0.3.0 (deprecated) | 2026-08-24 | |
| `deepagents-code` | 0.1.83 | 2026-10-07 21:55 | |
| `managed-deepagents` | 0.9.0 | 2026-10-07 16:38 | |
| `langchain-daytona` / `-modal` / `-runloop` | 0.0.8 / 0.0.6 / 0.0.7 | 2026-07-29 | |

Dependency constraints from the wheel metadata:

- `deepagents` 0.7.23 requires `langchain>=1.4.3,<2`, `langchain-core>=1.6.7,<2`, `langchain-anthropic>=1.7.5,<2`, `langchain-google-genai>=4.4.0,<5` and `langsmith>=0.14.4`.
- `langchain` 1.4.3 requires `langgraph>=1.2.11,<1.3`.
- `langgraph` 1.2.14 requires `langgraph-prebuilt>=1.1.0,<1.2`, `langgraph-checkpoint>=4.1,<5` and `langgraph-sdk>=0.4.6,<0.5`.
- `langgraph-checkpoint-postgres` 3.1.2 requires `psycopg>=3.2` and `psycopg-pool>=3.2`.

Wheel URLs inspected:

- files.pythonhosted.org/…/deepagents-0.7.23-py3-none-any.whl
- …/langchain-1.4.3-py3-none-any.whl
- …/langgraph-1.2.14-py3-none-any.whl
- …/langchain_mcp_adapters-0.3.2-py3-none-any.whl
- …/langgraph_checkpoint_postgres-3.1.2-py3-none-any.whl
- …/deepagents_cli-0.3.0-py3-none-any.whl
- deepagents-code 0.1.83
- managed-deepagents 0.9.0
- langchain-core 1.6.7
- langgraph-prebuilt 1.1.0

Docs pages read (all 2026-10-07):

- https://docs.langchain.com/oss/python/langgraph/use-time-travel
- https://docs.langchain.com/oss/python/deepagents/sandboxes
- https://docs.langchain.com/oss/python/deepagents/streaming
- https://docs.langchain.com/oss/python/deepagents/tools (search excerpt)
- https://docs.langchain.com/oss/python/deepagents/customization (search excerpt)
- https://docs.langchain.com/oss/python/releases/changelog (search excerpt)
- https://docs.langchain.com/oss/python/langchain/middleware/custom
- https://cursor.com/docs/subagents (for the `.cursor/agents/*.md` frontmatter: `name`, `description`, `model` (`inherit`|id), `readonly`, `is_background`; also reads `.claude/agents/` and `.codex/agents/`)

---

## Implications for Mission Control

1. **Hook scripts become one adapter `AgentMiddleware`.** Represent a stored "hook script" capability as `{event, matcher, handler}`, using the Claude/dcode event vocabulary: `PreToolUse`, `PostToolUse`, `PostToolUseFailure`, `PreCompact`, `Stop`, `SubagentStart`/`SubagentStop`, `SessionStart`. For Deep Agents, compile all of them into **one** `MissionHooksMiddleware(AgentMiddleware)`:
   - `wrap_tool_call` handles pre-tool and post-tool.
   - `before_model` / `after_model` handle the model gates.
   - `before_agent` / `after_agent` handle session start and stop.
   - `dcode`'s `ServerHooksMiddleware` is a working precedent for this mapping.
   - For Cursor, project the same records to command hooks.
2. **Mission Control has to build its own "on compaction" hook.**
   - Deep Agents has no compaction callback.
   - Option A: subclass or wrap the deepagents `SummarizationMiddleware`, keeping its `.name` so it replaces the default in place.
   - Option B: detect changes to `_summarization_event` in an `after_model` / `wrap_model_call` layer, or catch the `compact_conversation` tool in `wrap_tool_call` (this is how `dcode` fires `PreCompact`).
3. **Pick sync or async per hook.**
   - Hooks that must block, such as deny or approve, should return a `ToolMessage` / `jump_to: "end"` synchronously, or go through `interrupt()` for a human or out-of-process decision.
   - Audit-only hooks should be fire-and-forget writes to the Mission outbox, not interrupts. Interrupts add a checkpoint round-trip.
4. **Store subagent definitions in one provider-neutral record.** Fields: `name`, `description`, `prompt_body`, `model_ref` (with an `inherit` sentinel), `tools[]` (capability refs), `skills[]`, `permissions[]`, `interrupt_on`, `readonly`, `background`, `context_mode: isolated|fork`.
   - Deep Agents projection: `SubAgent{name, description, system_prompt, model?, tools?, skills?, permissions?, interrupt_on?, mode?}`. Omit `model` and `tools` to inherit. Use `readonly` to derive `FilesystemPermission(mode="deny")` for write operations.
   - Cursor projection: `.cursor/agents/<name>.md` with frontmatter `name`, `description`, `model` (`inherit`|id), `readonly`, `is_background`, and the prompt body as the markdown body.
5. **Background subagents differ by provider.**
   - In Deep Agents, `background: true` is only real as an `AsyncSubAgent{graph_id, url, headers}` against a LangGraph / Agent Protocol server.
   - When no server is configured, the projection should fall back to a sync `SubAgent` (or reject the definition).
   - Cursor's `is_background` has no in-process Deep Agents equivalent.
6. **Materialize artifacts before a stage with one function**, `materialize(backend, manifest)`:
   - Sandbox, Filesystem or LocalShell backends: `backend.upload_files([(abs_path, bytes), ...])` before `ainvoke`.
   - `StateBackend`: put `{"files": {path: create_file_data(text)}}` in the stage's input dict.
   - Persistent mission memory: a `CompositeBackend` route (e.g. `/memories/` → `StoreBackend` on `AsyncPostgresStore`).
   - Record a content hash per file in the stage checkpoint.
7. **Skills and memory are path lists, not objects.**
   - Write each capability skill to `/skills/<scope>/<skill-name>/SKILL.md`, with the frontmatter `name` matching the directory name and at most 64 lowercase/hyphen characters.
   - Pass `skills=["/skills/base/", "/skills/mission/"]`; later sources override earlier ones.
   - Mission AGENTS.md goes to `/memory/AGENTS.md` with `memory=[...]`.
   - The same files can be mirrored to `.cursor/skills` / `.agents/skills` for Cursor and dcode.
8. **MCP servers are resolved by Mission Control, not by deepagents.**
   - Store MCP server definitions in `MultiServerMCPClient` connection-dict form (`transport`, `url`/`command`, `headers`, `env`). The same dict converts to `MCPConfig` for `langchain.mcp.MCPAdapter`.
   - Resolve them to tools at stage start and pass them as `tools=` (or per-subagent `tools`).
   - Mind session lifetime: `get_tools()` opens a session per call; `client.session()` keeps one open for the stage.
9. **Mission checkpoints map onto LangGraph checkpoints.**
   - Use `AsyncPostgresSaver` with `thread_id = run/stage id`.
   - Run with `durability="sync"` for stages whose checkpoints Mission Control must observe immediately (the default is `"async"`).
   - After each step or summarization event, read `aget_state(config)` and record `checkpoint_id`, `_summarization_event.summary_message`, `cutoff_index` and `file_path` as a mission checkpoint. Also pull `/conversation_history/<session>.md` with `download_files` for the archive.
10. **Continuation and forking.**
    - Continue: invoke with `None` or new messages on the same `thread_id`.
    - Replay: invoke with a past `checkpoint_id`.
    - Same-thread fork: `aupdate_state(past.config, values)`, then `ainvoke(None, fork_cfg)`.
    - Cross-thread "branch mission": copy `values` onto a new `thread_id` (OSS has no copy-thread API; **UNVERIFIED** best practice).
    - `PatchToolCallsMiddleware` repairs dangling tool calls after a fork or resume.
11. **Event attribution comes from the stream.**
    - Use `astream(..., stream_mode=["updates","messages","custom"], subgraphs=True, version="v2")`.
    - Attribute events to subagents with `ns` (`tools:<task_id>`) together with `metadata.lc_agent_name`. Join `task` tool calls to subagent runs at completion by `tool_call_id`, because pregel task ids are not tool call ids.
    - Use `astream_events(version="v2")` when you need `run_id` / `parent_ids` lineage for the outbox.
12. **Pin the stack together.** `deepagents==0.7.23`, `langchain==1.4.3`, `langchain-core==1.6.7`, `langgraph==1.2.14`, `langgraph-checkpoint-postgres==3.1.2`, `langchain-anthropic==1.7.5`, `langchain-openai==1.6.7`, and `langchain-mcp-adapters==0.3.2` or `langchain[mcp]`.
    - deepagents is pre-1.0 and ships near-daily, and `mode="fork"` and `langchain.mcp` are beta. Treat upgrades as deliberate.
    - Do **not** adopt `deepagents-cli`; it is deprecated.
