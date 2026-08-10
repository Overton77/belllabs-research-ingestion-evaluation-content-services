---
id: WP-CP-030
title: Implement Temporal root, operation, continuity, messaging, cancellation, and linked runs
status: draft
implements: [REQ-CP-EXEC-001, REQ-CP-EXEC-002, REQ-CP-EXEC-003, REQ-CP-EXEC-004, REQ-CP-EXEC-005, REQ-CP-EXEC-006, REQ-CP-EXEC-007, REQ-CP-EXEC-008, REQ-CP-EXEC-009, REQ-CP-EXEC-010, REQ-CP-EXEC-011, REQ-CP-EXEC-012]
governed_by: [ADR-0003, SPEC-CP-DURABLE-EXECUTION]
contracts: [CON-CP-TEMPORAL-IDENTITY-V1, CON-CP-WORKFLOW-MESSAGE-V1, CON-CP-LINKED-RUN-V1, CON-CP-CONTINUATION-V1]
blocked_by: [WP-CP-020]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-CP-030/]
---

# Implement Temporal root, operation, continuity, messaging, cancellation, and linked runs

## Outcome

Every admitted run executes through one root, one family child, and generic durable operation children with authoritative messages, cancellation/recovery, linked runs, semantic snapshots, and Continue-As-New.

## Current implementation baseline

Existing Temporal StageGraph, GoalDirected, operation, linked-run, kernel, and experiment code is prior art. Direct-activity/gather and prototype timeout behavior are not accepted target behavior.

## Requirements implemented

All `REQ-CP-EXEC-*` requirements.

## Architectural seams affected

Temporal contracts/workflows/activities, worker registration and queues, message relay, operation executor port, run-control services, linked-run application services, snapshot/fork services, and replay tests.

## Compatibility and migrations

Introduce versioned workflow types/contracts and replay-safe behavior. Preserve old workflow workers until admitted old executions drain or Continue-As-New through a qualified boundary.

## Acceptance criteria

- [ ] Duplicate start maps to one root.
- [ ] Both family fixtures run through the same root/operation contracts.
- [ ] Workflow code performs no nondeterministic I/O.
- [ ] Messages expose durable ordered receipts.
- [ ] Cancellation reconciles effects and late generations.
- [ ] Linked runs compile/admit independently and require result admission.
- [ ] Continue-As-New preserves active children, messages, identities, and balances.
- [ ] Semantic fork creates a new run/epoch and copies no active child or pending message.

## Qualification and evidence

Run `QUAL-CP-TEMPORAL-REPLAY-RECOVERY` and `QUAL-CP-LINKED-RUN-SEMANTICS` with time skipping, captured-history replay, worker loss, redelivery, and forced continuation.

## Failure and rollback posture

Use worker version compatibility and N/N+1 replay. Never reset product state to roll back workflow code; use repair or deployment rollback with authoritative reconciliation.

## Documentation and traceability updates

Record workflow type/version, task queues, histories, failure injection, and exact test paths.

## Non-goals

Full family semantics or Deep Agent materialization.

## Drift guards

Static/test guards prevent alternate Agent Server macro workflows and database/provider access from workflow code.

