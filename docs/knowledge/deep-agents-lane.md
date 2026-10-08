---
type: Concept
title: Deep Agents lane
description: The Deep Agents lane as built - the runtime adapter and its harness wrapper, the exact materializer and sandbox factories, frame recording, hook and compaction middleware, hosted async subordinates, and the Agent Host configuration generator that is not a lane.
tags: [mission-control, deep-agents, lanes, agent-host, implementation]
---

# Deep Agents lane

Deep Agents is the first and only qualified [Lane](../../GLOSSARY.md). The protocol, registry
and `lane.turn` loop that drive it are in [lanes and harness](lanes-and-harness.md).

`adapters/deep_agents/adapter.py::DeepAgentRuntimeAdapter` is the sole production
`create_deep_agent` composition root. `execute(invocation, resolved_secrets)` validates
the exact binding, materializes it, classifies the unit generation from the checkpointer
before any provider work ([context and continuation](context-and-continuation.md)), then
submits, resumes or fails closed; `observe_latest` reads the latest durable checkpoint
and incurred usage of a unit being cancelled without invoking cognition;
`build_hosted_async_subagent_graph` compiles a graph the Agent Server serves.
`application/execution/harness/deep_agents_harness.py::DeepAgentsHarness` wraps that
adapter behind the protocol without changing behavior: `send_turn` starts cognition as a
task, `observe` yields its closing frame, `cancel_turn` cancels the task, `reattach` reads
the latest checkpoint, `snapshot` is emulated from the captured checkpoint.

`adapters/deep_agents/materializer.py::ExactDeepAgentMaterializer.prepare` resolves every
component by digest from an `ExactComponentRegistry` (model factory, sandbox factory,
tools, MCP servers, middleware, subordinate profiles, checkpointer and store), mounts
skill bundles, verifies the installed runtime version and rejects mismatches. A `hosted`
binding leaves checkpointer and store to the Agent Server. Sandbox factories are the
in-memory `StateBackend`, `LangSmithSandbox` and a network-isolated `DockerSandbox`
(`docker_sandbox.py`); an unqualified placement raises `DeepAgentUnsupportedPlacement`
rather than falling back. The LangGraph saver and store bind to the private
`mission_control_runtime` schema (`persistence.py`; [persistence](persistence.md), ADR-0017).

Three adapters make the lane observable and governable. `adapters/deep_agents/frames.py`
(`DeepAgentFrameRecorder`) turns `astream` v2 parts into Provider Frames. `hooks.py`
(`HookScriptMiddleware`) runs the four Kernel Hooks (stop fence, operation intent, frame
capture, usage) first and in fixed order, then catalog hook scripts
([capabilities](capabilities.md)). `compaction.py` (`ObservedSummarizationMiddleware`)
replaces the stock summarization middleware in place so a compaction is emitted as
`before_compaction` and `after_compaction` frames instead of happening silently.

`adapters/deep_agents/async_subagents.py` holds two pieces: `DeepAgentsAsyncSubagentAdapter`
starts, checks, updates, cancels and lists hosted subordinate runs on the Agent Server,
verifies the served graph identity before submission and on reconnect, uses the child
execution id as the provider thread with a spawn key so an existing run is found before
one is created, and records cancellation as `provider_acknowledged` or `ambiguous`;
`BellLabsAsyncSubagentMiddleware` presents the stock tool names to the parent agent while
routing each call through the governed service so the child is reserved and linked
before any provider submission (ADR-0006).

## Agent Host configuration generation

`domain/agentic_components/contracts.py` defines `AgentHost` (`cursor`, `codex`,
`claude_code`, `agent_framework`), `ComponentKind`, `TrustStage`
(`quarantined`, `reviewed`, `qualified`, `accepted`), an `AgenticComponentRelease` that
must carry exactly the typed binding of its kind, `MaterializationRequest`,
`MaterializationPlan` with contiguous steps and `ReadinessReceipt`.
`application/agentic_components/projections.py::render_release_host_files` is the older
MCP-only renderer used by the materialization planner (`materialization.py`), and
`render_host_files` is the Host Projection every lane uses ([capabilities](capabilities.md)).
This generates host configuration; it does not execute mission work.

# Citations

- ADRs: [0005](../adr/0005-deep-agents-first-extend-not-fork.md),
  [0006](../adr/0006-agent-server-required-execution-host-not-scheduler.md),
  [0017](../adr/0017-agent-server-runtime-persistence-separate-database.md).
- Code: [Deep Agents harness](../../src/mission_control/application/execution/harness/deep_agents_harness.py),
  [lane adapter](../../src/mission_control/adapters/deep_agents/adapter.py),
  [materializer](../../src/mission_control/adapters/deep_agents/materializer.py),
  [frame recorder](../../src/mission_control/adapters/deep_agents/frames.py),
  [hook middleware](../../src/mission_control/adapters/deep_agents/hooks.py),
  [compaction observer](../../src/mission_control/adapters/deep_agents/compaction.py),
  [async subordinates](../../src/mission_control/adapters/deep_agents/async_subagents.py),
  [Docker sandbox](../../src/mission_control/adapters/deep_agents/docker_sandbox.py),
  [runtime persistence](../../src/mission_control/adapters/deep_agents/persistence.py),
  [component contracts](../../src/mission_control/domain/agentic_components/contracts.py),
  [host projections](../../src/mission_control/application/agentic_components/projections.py),
  [materialization planner](../../src/mission_control/application/agentic_components/materialization.py),
  [component repository](../../src/mission_control/application/agentic_components/repository.py),
  [filesystem repository](../../src/mission_control/adapters/capabilities/agentic_components/filesystem_repository.py).
- Tests: [Deep Agents frames](../../tests/unit/frames/test_deep_agents_frames.py),
  [hook middleware](../../tests/unit/deep_agents/test_hook_middleware.py),
  [agentic components](../../tests/unit/agentic_components/test_harness.py),
  [Deep Agents operation proof](../../tests/acceptance/control_plane/test_wp_cp_040.py),
  [subordinate submission fence](../../tests/unit/integrations/test_async_subagent_submission_fence.py),
  [canonical Agent Server profiles](../../tests/integration/agent_server/test_canonical_server_local.py).
