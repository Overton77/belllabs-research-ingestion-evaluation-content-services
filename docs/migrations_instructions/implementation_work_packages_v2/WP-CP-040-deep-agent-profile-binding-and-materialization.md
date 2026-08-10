---
id: WP-CP-040
title: Implement Deep Agent profile, placement, binding, and capability materialization
status: ready_when_unblocked
implements: [REQ-CP-DA-001, REQ-CP-DA-002, REQ-CP-DA-003, REQ-CP-DA-004, REQ-CP-DA-005, REQ-CP-DA-006, REQ-CP-DA-007, REQ-CP-DA-013, REQ-CP-DA-014, REQ-CP-DA-015, REQ-CP-CS-001, REQ-CP-CS-002, REQ-CP-CS-003, REQ-CP-CS-004, REQ-CP-CS-005, REQ-CP-CS-006, REQ-CP-CS-007]
governed_by: [ADR-0003, ADR-0004, SPEC-CP-DEEP-AGENT-RUNTIME, SPEC-CP-COGNITIVE-SCHEMAS]
contracts: [CON-CP-DEEP-AGENT-PROFILE-V1, CON-CP-DEEP-AGENT-PLACEMENT-V1, CON-CP-DEEP-AGENT-BINDING-V1, CON-CP-COGNITIVE-STATE-SCHEMA-V1, CON-CP-COGNITIVE-CONTEXT-SCHEMA-V1, CON-CP-COGNITIVE-CHANNEL-PACK-V1, CON-CP-WORKSPACE-MANIFEST-V1, CON-CP-ARTIFACT-PROMOTION-V1, CON-CP-SNAPSHOT-V1]
blocked_by: [WP-CP-030]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-CP-040/]
---

# Implement Deep Agent profile, placement, binding, and capability materialization

## Outcome

An exact local Deep Agents 0.7.5 operation is built from a versioned logical profile plus placement profile, executes through `OperationWorkflow`, and materializes exact MCP, Skill, sandbox, model, middleware, tool, context, and workspace inputs without mutable discovery.

## Current implementation baseline

The repository has Deep Agents 0.7.5, partial harness/middleware/context/delegation/capability models, operation assembly/binding versions, and experimental `create_deep_agent` usage. These are inputs, not the canonical schema.

## Requirements implemented

The listed non-async `REQ-CP-DA-*` requirements. Async lifecycle requirements are isolated in WP-CP-045.

## Architectural seams affected

Graph-runtime domain definitions, compiler, operation executor, Deep Agents adapter, LangGraph checkpoint/store integration, workspace/sandbox gateway, MCP and Skill materializers, artifact/snapshot services, API schemas, and tests.

## Authorized implementation slice

- Add canonical profile/placement/binding and workspace/artifact/snapshot contracts under
  `app/domain/operation_execution/` and exact definition refs under `app/domain/control_plane/`.
- Create `app/integrations/agents/deep_agents/adapter.py` and `materializer.py`; these are the only
  production `create_deep_agent` composition roots.
- Replace provider selection in `app/application/operation_execution.py` with the provider-neutral
  executor port and exact binding.
- Port exact MCP, Skill, sandbox, workspace, artifact, snapshot and LangSmith adapters behind that
  port.
- Remove OpenAI Agents SDK imports/dependencies/composition/live scripts and tests from the active
  application and default suite at acceptance.

## Replacement and migrations

Implement the canonical Deep Agent profile, placement, and binding schemas directly. Port only
accepted concepts from `AgentHarnessDefinition`, `OperationAssemblySpecV3`, and
`StageExecutionBindingV2`; do not retain their consumers or stored compatibility. Remove OpenAI
Agents SDK dependencies, fields, imports, composition, live scripts, and required tests from the
active application path. Record exact Deep Agents/LangGraph/runtime dependencies in the binding.

## Acceptance criteria

- [ ] `DeepAgentProfile` and placement contracts validate and digest deterministically.
- [ ] Compiler emits a complete flattened binding including cognitive state/context schema digests.
- [ ] Adapter resolves those digests to `create_deep_agent(state_schema=..., context_schema=...)` and seeds `artifact_index`, `context_manifest`, and `child_result_index`.
- [ ] Local placement invokes Deep Agents 0.7.5 through the provider-neutral executor.
- [ ] One exact MCP server/tool surface, Skill bundle, and sandbox attach correctly.
- [ ] Runtime drift, component collision, unsupported policy, or silent fallback fails closed.
- [ ] Sync subagent receives only delegated capability/workspace ceilings.
- [ ] Artifact promotion and snapshot clone/reauthorization pass.

## Qualification and evidence

Run the non-async portion of `QUAL-CP-DEEP-AGENT-MATERIALIZATION` through a real Temporal activity. Use official Deep Agents documentation as implementation guidance; resolve integration errors in this package and record them in evidence rather than gating on a separate probe.

## Failure and rollback posture

Failed materialization produces typed failure before provider side effects. Rollback disables the
new adapter before production adoption; no prior provider adapter remains registered as fallback.

## Documentation and traceability updates

Record exact package versions, profile/binding schemas, materialization decisions, attachment evidence, and removed OpenAI Agents SDK paths.

## Non-goals

Full governed capability catalogs, remote LangSmith placement promotion, or async-subagent lifecycle.

## Drift guards

No `create_deep_agent` call outside the adapter/composition root; no runtime aliases; no framework types in domain/public contracts.
