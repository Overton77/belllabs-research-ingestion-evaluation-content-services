# [FT-A5] Hook script contract and Deep Agents HookScriptMiddleware with kernel hooks

Linear: OVE-26

**Epic:** Capabilities (SPEC-01)
**Team:** T1
**Blocked by:** FT-A1
**Status:** ready-for-agent

**What to build:** A Hook Script written once (reads `mc.hook_input.v1` on stdin, writes `mc.hook_result.v1` on stdout, exit 2 denies) runs inside a Deep Agents attempt at the mapped lifecycle points through one `HookScriptMiddleware`, with Mission Control's four Kernel Hooks (stop fence, operation intent, frame capture, usage) composed first and unremovable. A `deny` from a `before_shell` hook stops the shell tool before it runs; `updated_input` rewrites the tool arguments; `additional_context` reaches the model; `defer` becomes a HITL interrupt. Compaction fires `before_compaction` and `after_compaction` from Mission Control's summarization wrapper.

**Spec sections:** SPEC-01 "Hook event vocabulary and lane mapping", "Kernel hooks", "HookScriptMiddleware (Deep Agents)", "Contracts" (`mc.hook_input.v1`, `mc.hook_result.v1`).

**Writable regions:** `src/mission_control/adapters/deep_agents/hooks.py` (new), `src/mission_control/adapters/deep_agents/materializer.py` (middleware order), `src/mission_control/contracts/hooks.py` (new), `scripts/hooks/policy_template/` (the seed template script), `tests/unit/deep_agents/test_hook_middleware.py`, `tests/integration/deep_agents/`.

**Acceptance criteria:**
- [ ] `mc.hook_input.v1` and `mc.hook_result.v1` Pydantic models with JSON Schema export; `decision` limited to `allow|deny|defer`; `reason` required for `deny` and `defer`; `additional_context` capped at 10,000 chars.
- [ ] `HookScriptMiddleware(AgentMiddleware)` implements `before_agent`, `before_model`, `after_model`, `after_agent`, `wrap_tool_call` (and async variants) and maps them to the SPEC-01 events, including `before_shell`/`after_shell` by tool-name matcher, `before_mcp` by MCP tool names, `after_file_edit` by filesystem write tools, `subagent_start`/`subagent_stop` on the `task` tool, `after_tool_failure` on exception.
- [ ] Scripts run as subprocesses in the attempt workspace with the row's interpreter and `timeout_seconds`; results merge as `deny` beats `defer` beats `allow`, `additional_context` concatenates, last `updated_input` wins; `deny` returns a `ToolMessage(status="error")` without invoking the handler; `defer` raises `interrupt()` through `HumanInTheLoopMiddleware` semantics; a timed-out `fail_closed` hook denies.
- [ ] Kernel hooks `mc.stop_fence`, `mc.operation_intent`, `mc.frame_capture`, `mc.usage` are composed first in fixed order by `ExactDeepAgentMaterializer.prepare`; no binding field can omit or reorder them (test asserts the order with a mission that declares three catalog hooks).
- [ ] `MissionSummarizationMiddleware` subclasses the deepagents `SummarizationMiddleware`, keeps its `.name`, replaces the default in place, and emits `before_compaction` and `after_compaction` with the `_summarization_event` fields.
- [ ] Every hook invocation and result is handed to the frame sink port as a Provider Frame (interface only here; persistence is C1), with the hook id, event, decision and digests.
- [ ] Stdin never contains secret values (test scans the serialized input against fixture secrets); scripts that need the service use the `callback` block.
- [ ] The seed template `scripts/hooks/policy_template/` denies `rm -rf /`, `git push --force` and writes outside declared paths, with its own tests.

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/deep_agents/test_hook_middleware.py -q`; `uv run --group biotech pytest tests/integration/deep_agents/test_hooks_local_model.py -q` (a small real Anthropic or OpenAI call is permitted; record it as a fixture).

**Notes:** langchain 1.4.3 has no `before_tool`; `wrap_tool_call` covers both sides. deepagents 0.7.23 exposes no compaction callback; the subclass-by-name replacement is the supported way to intercept it. Pin `deepagents==0.7.23`, `langchain==1.4.3`, `langgraph==1.2.14` in this ticket only if the lock does not already match; otherwise leave pins to T4's G7. The precedent for the mapping is `deepagents-code`'s `ServerHooksMiddleware`; ours runs scripts directly rather than through `interrupt()`.
