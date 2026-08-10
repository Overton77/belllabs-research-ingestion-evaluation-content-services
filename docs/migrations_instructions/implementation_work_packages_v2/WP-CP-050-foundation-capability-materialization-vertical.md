---
id: WP-CP-050
title: Prove the cohesive control-plane foundation and capability-materialization vertical
status: ready_when_unblocked
implements: [REQ-CP-DEF-005, REQ-CP-DEF-008, REQ-CP-RUN-001, REQ-CP-RUN-005, REQ-CP-EXEC-001, REQ-CP-EXEC-003, REQ-CP-EXEC-006, REQ-CP-EXEC-008, REQ-CP-EXEC-011, REQ-CP-DA-003, REQ-CP-DA-005, REQ-CP-DA-008, REQ-CP-DA-011, REQ-BP-SG-004, REQ-BP-SG-010, REQ-BP-GD-004, REQ-BP-GD-005, REQ-BP-GD-007]
governed_by: [ADR-0003, SPEC-CP-DEFINITIONS, SPEC-CP-RUN-CONTROL, SPEC-CP-DURABLE-EXECUTION, SPEC-CP-DEEP-AGENT-RUNTIME, SPEC-BP-STAGEGRAPH, SPEC-BP-GOAL-DIRECTED]
contracts: [CON-CP-ERC-V1, CON-CP-DEEP-AGENT-BINDING-V1, CON-CP-ASYNC-SUBAGENT-V1, CON-BP-STAGEGRAPH-V1, CON-BP-GOAL-DIRECTED-V1]
blocked_by: [WP-CP-045, WP-BP-010, WP-BP-020]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-CP-050/]
---

# Prove the cohesive control-plane foundation and capability-materialization vertical

## Outcome

One end-to-end executable proof crosses every accepted foundation boundary and leaves the repository ready to implement full governed capability catalogs without reopening architecture.

## Current implementation baseline

This package integrates only accepted outputs of its blockers. It must not build a parallel demo path or replace missing authority with fixtures. Immutable digest-verified MCP, Skill, and sandbox fixtures are permitted solely as temporary catalog inputs.

## Requirements implemented

The listed cross-cutting tracer requirements; all owning package qualifications remain mandatory.

## Architectural seams affected

API/commands, compiler, MongoDB, PostgreSQL, Temporal root/families/operations, Deep Agents 0.7.5, LangGraph checkpointing, MCP, Skills, sandbox/workspace, sync/async subagents, artifacts, LangSmith tracing, and evidence projections.

## Authorized implementation slice

- Add production-shaped StageGraph and GoalDirected fixtures using the same definition compiler,
  admission service, root workflow, operation workflow, Deep Agent adapter, persistence, and
  terminal reducer.
- Add one acceptance manifest under `tests/acceptance/control_plane/` and aggregate evidence under
  `docs/migrations_instructions/evidence_v2/WP-CP-050/`.
- Remove remaining superseded runtime dependencies, settings, launch paths, worker registrations,
  live scripts, and default tests before the final gate.
- Do not introduce a demo-only compiler, in-memory authority, direct provider bypass, or alternate
  workflow registration to make the tracer pass.

## Replacement and migrations

Exercise the exact production-shaped schemas and migrations. No test-only alternate domain contracts or provider bypasses.

## Acceptance criteria

- [ ] StageGraph and GoalDirected runs compile, admit, execute, settle, and terminalize through the same shared foundation.
- [ ] The Deep Agent binding materializes an exact MCP server/tool filter, Agent Skill bundle, and sandbox.
- [ ] Sync and async subagents obey their distinct contracts.
- [ ] StageGraph proves early downstream release.
- [ ] GoalDirected proves independent verification and fresh-session handoff.
- [ ] Messages, cancellation, worker loss, effect idempotency, and Continue-As-New recover correctly.
- [ ] Complete lineage joins run, Temporal, operation, Deep Agent, capability, child, artifact, usage, evidence, and LangSmith trace identifiers.
- [ ] No OpenAI Agents SDK or Agent Server macro-runtime dependency remains.

## Qualification and evidence

Aggregate all foundation and blueprint `QUAL-*` obligations into an evidence manifest with exact revisions, commands, configurations, sanitized outputs, traces, failures, and gate disposition.

## Failure and rollback posture

Any failed boundary keeps the foundation gate unaccepted and identifies the owning package for rework. Do not weaken requirements or substitute another runtime to make the vertical pass.

## Documentation and traceability updates

Attach actual code/migration/test/evidence paths to every traced requirement and identify the next control-plane capability package.

## Non-goals

Complete governed catalogs, remote placement promotion, or production deployment topology.

## Drift guards

The vertical must prove absence of mutable alias reads, provider authority, dual macro schedulers, secret leakage, and unadmitted async-child results.
