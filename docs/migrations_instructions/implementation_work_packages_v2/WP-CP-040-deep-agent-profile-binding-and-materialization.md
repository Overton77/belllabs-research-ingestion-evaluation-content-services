---
id: WP-CP-040
title: Implement Deep Agent profile, placement, binding, and capability materialization
status: draft
implements: [REQ-CP-DA-001, REQ-CP-DA-002, REQ-CP-DA-003, REQ-CP-DA-004, REQ-CP-DA-005, REQ-CP-DA-006, REQ-CP-DA-007, REQ-CP-DA-013, REQ-CP-DA-014, REQ-CP-DA-015]
governed_by: [ADR-0003, SPEC-CP-DEEP-AGENT-RUNTIME]
contracts: [CON-CP-DEEP-AGENT-PROFILE-V1, CON-CP-DEEP-AGENT-PLACEMENT-V1, CON-CP-DEEP-AGENT-BINDING-V1, CON-CP-WORKSPACE-MANIFEST-V1, CON-CP-ARTIFACT-PROMOTION-V1, CON-CP-SNAPSHOT-V1]
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

## Compatibility and migrations

Version current `AgentHarnessDefinition`, `OperationAssemblySpecV3`, and `StageExecutionBindingV2` consumers rather than changing persisted meaning in place. Remove OpenAI Agents SDK target fields/imports from the required path. Record exact Deep Agents/LangGraph/runtime dependencies in placement/binding metadata.

## Acceptance criteria

- [ ] `DeepAgentProfile` and placement contracts validate and digest deterministically.
- [ ] Compiler emits a complete flattened binding.
- [ ] Local placement invokes Deep Agents 0.7.5 through the provider-neutral executor.
- [ ] One exact MCP server/tool surface, Skill bundle, and sandbox attach correctly.
- [ ] Runtime drift, component collision, unsupported policy, or silent fallback fails closed.
- [ ] Sync subagent receives only delegated capability/workspace ceilings.
- [ ] Artifact promotion and snapshot clone/reauthorization pass.

## Qualification and evidence

Run the non-async portion of `QUAL-CP-DEEP-AGENT-MATERIALIZATION` through a real Temporal activity. Use official Deep Agents documentation as implementation guidance; resolve integration errors in this package and record them in evidence rather than gating on a separate probe.

## Failure and rollback posture

Keep the prior exact adapter variant available only for already-admitted compatible runs. New compilation selects the new binding after acceptance. Failed materialization produces typed failure before provider side effects.

## Documentation and traceability updates

Record exact package versions, profile/binding schemas, materialization decisions, attachment evidence, and removed OpenAI Agents SDK paths.

## Non-goals

Full governed capability catalogs, remote LangSmith placement promotion, or async-subagent lifecycle.

## Drift guards

No `create_deep_agent` call outside the adapter/composition root; no runtime aliases; no framework types in domain/public contracts.

