---
type: Concept
title: Lanes and the harness protocol
description: The provider-neutral harness every lane must implement, the Deep Agents lane as it exists, the agent-host configuration generator, and the proposed coding lanes that have no code yet.
tags: [mission-control, harness, lanes, deep-agents, agent-host, implementation]
---

# Lanes and the harness protocol

A [Harness](../../GLOSSARY.md) is the protocol; a [Lane](../../GLOSSARY.md) is one
qualified implementation of it; an [Agent Host](../../GLOSSARY.md) is a tool a human
works in that reads generated configuration. A host is where authoring happens; a lane
is where mission work executes. Keep the three apart when reading code, because the
package directory for lanes is called `adapters`.

## The protocol as specified

RUNTIME-CONTRACTS.md defines `AgentHarness` as `describe`, `prepare`, `start`,
`reattach`, `send_turn`, `cancel_turn`, `observe`, `snapshot`, `usage` and `end_session`
(the glossary names the eight without `describe` and `reattach`). Every mutation carries
scope, binding digest, idempotency key, generation, fenced lease and deadline; handles
carry no secret; `describe` reports each control as `native`, `emulated`, `unsupported`
or `unqualified`; `UsageReport` has a settled, estimated or unknown disposition per
dimension; a successful HTTP cancel is not a `CleanupReceipt`. Workflow-types/05 adds
the identity hierarchy (activation, attempt, agent session, session turn, harness
execution), the native identity mapping for `deep_agents`, `cursor_cloud` and
`direct_model`, and the rule that an agent session serves only consecutive attempts of
one activation lineage.

No `AgentHarness` protocol with those method names exists in `src/mission_control`.

## The Deep Agents lane as implemented

`adapters/deep_agents/adapter.py::DeepAgentRuntimeAdapter` is the sole production
`create_deep_agent` composition root. `execute(invocation, resolved_secrets)` validates
the exact binding, materializes it, classifies the unit generation from the checkpointer
before any provider work ([context and continuation](context-and-continuation.md)), then
submits, resumes or fails closed; `observe_latest` reads the latest durable checkpoint
and incurred usage of a unit being cancelled without invoking cognition;
`build_hosted_async_subagent_graph` compiles a graph the Agent Server serves.

`adapters/deep_agents/materializer.py::ExactDeepAgentMaterializer.prepare` resolves every
component by digest from an `ExactComponentRegistry` (model factory, sandbox factory,
tools, MCP servers, middleware, subordinate profiles, checkpointer and store), mounts
skill bundles, verifies the installed runtime version and rejects mismatches. A `hosted`
binding leaves checkpointer and store to the Agent Server. Sandbox factories are the
in-memory `StateBackend`, `LangSmithSandbox` and a network-isolated `DockerSandbox`
(`docker_sandbox.py`); placements are `local_in_worker` or
`remote_langsmith_deployment` (`domain/execution/contracts.py`), and an unqualified
placement raises `DeepAgentUnsupportedPlacement` rather than falling back. The LangGraph
saver and store bind to the private `mission_control_runtime` schema
(`persistence.py`; [persistence](persistence.md), ADR-0017).

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
`claude_code`, `agent_framework`), `ComponentKind` (`plugin`, `mcp_server`, `skill`,
`agent_component`, `sandbox_snapshot`, `workspace_setup`, `diff_codec`), `TrustStage`
(`quarantined`, `reviewed`, `qualified`, `accepted`), an `AgenticComponentRelease` that
must carry exactly the typed binding of its kind, `MaterializationRequest`,
`MaterializationPlan` with contiguous steps (`retrieve`, `verify`, `stage`,
`inject_secrets`, `configure_host`, `provision_workspace`, `start_server`,
`probe_readiness`, `seal_snapshot`) and `ReadinessReceipt`.
`application/agentic_components/projections.py` renders `.codex/config.toml`,
`.cursor/mcp.json` or `.mcp.json` for MCP releases and places skills under
`.agents/skills`, `.cursor/skills` or `.claude/skills`; `materialization.py` compiles a
non-secret replayable plan; `repository.py` is the port with an in-memory implementation
and `adapters/capabilities/agentic_components/filesystem_repository.py` the file-backed
one. This generates host configuration; it does not execute mission work.

## Proposed coding lanes (not accepted, no code)

ADR-0018 (status proposed) wants Claude Code, Codex and Cursor controllable as lanes
behind the same protocol, qualified after Deep Agents in the order Claude Agent SDK, Cursor
Cloud, Codex, with per-profile delivery semantics. ADR-0019 (proposed) wants each coding
lane's first profile worker-hosted as a managed local process with a leased workspace
and cloud placement as a second profile. Both reverse the canonical pack:
RUNTIME-CONTRACTS.md says "first qualify cloud placement" and "local Cursor placement is
not a required lane", and workflow-types/05 says "no Cursor local lane is required".
Searching the kernel for `cursor_cloud`, `claude_code`, `codex` or `direct_model` finds
only the `AgentHost` enum and coordinator surface promotion, so these lanes exist as
decisions awaiting the next interview, not as implementations.

# Citations

- Spec: `../mission-control-general/general-mission-control/RUNTIME-CONTRACTS.md`
  (harness interface); `../mission-control-general/workflow-types/05-EXECUTORS_AND_DURABLE_CONTROLS.md`
  (sections 2 and 3.3); `../mission-control-general/general-mission-control/SPECIFICATION.md`
  (exact agent and sandbox binding; harness continuation and subordinates).
- ADRs: [0005](../adr/0005-deep-agents-first-extend-not-fork.md),
  [0006](../adr/0006-agent-server-required-execution-host-not-scheduler.md),
  [0017](../adr/0017-agent-server-runtime-persistence-separate-database.md),
  [0018](../adr/0018-coding-agents-are-execution-lanes.md),
  [0019](../adr/0019-coding-lanes-worker-hosted-local-first.md).
- Code: [Deep Agents lane](../../src/mission_control/adapters/deep_agents/adapter.py),
  [materializer](../../src/mission_control/adapters/deep_agents/materializer.py),
  [async subordinates](../../src/mission_control/adapters/deep_agents/async_subagents.py),
  [Docker sandbox](../../src/mission_control/adapters/deep_agents/docker_sandbox.py),
  [runtime persistence](../../src/mission_control/adapters/deep_agents/persistence.py),
  [component contracts](../../src/mission_control/domain/agentic_components/contracts.py),
  [host projections](../../src/mission_control/application/agentic_components/projections.py),
  [materialization planner](../../src/mission_control/application/agentic_components/materialization.py),
  [component repository](../../src/mission_control/application/agentic_components/repository.py),
  [filesystem repository](../../src/mission_control/adapters/capabilities/agentic_components/filesystem_repository.py).
- Tests: [agentic components](../../tests/unit/agentic_components/test_harness.py),
  [Deep Agents operation proof](../../tests/acceptance/control_plane/test_wp_cp_040.py),
  [subordinate submission fence](../../tests/unit/integrations/test_async_subagent_submission_fence.py),
  [canonical Agent Server profiles](../../tests/integration/agent_server/test_canonical_server_local.py).
