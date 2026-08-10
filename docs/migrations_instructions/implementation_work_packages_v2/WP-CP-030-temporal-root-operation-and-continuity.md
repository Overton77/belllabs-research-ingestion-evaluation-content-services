---
id: WP-CP-030
title: Implement Temporal root, operation, continuity, messaging, cancellation, and linked runs
status: ready
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

## Authorized implementation slice

- Create `app/temporal/workflows/belllabs_run.py`, `operation.py`, `stagegraph.py`,
  `goal_directed.py`, and `linked_run.py`.
- Create `app/temporal/activities/control_plane.py` and `operation.py` as idempotent application
  adapters only.
- Create the single registries in `app/temporal/registration/workflows.py`, `activities.py`, and
  `task_queues.py`; update the active worker composition to use them.
- Replace `app/integrations/temporal_workflow_submission.py` with root-only submission.
- Replace direct family activity execution with typed `OperationWorkflow` children.
- Add contract, time-skipping, replay, worker-loss, redelivery, cancellation, linked-run, fork, and
  forced Continue-As-New tests under the paths frozen by `IMPLEMENTATION_READINESS.md`.

## Replacement and workflow registration

Register new versioned root, family, operation, and linked-run workflows under the exact hierarchy
in `IMPLEMENTATION_READINESS.md`. Replace direct family submission and old worker registration.
There are no production executions to drain and no compatibility worker is required.

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

Captured-history replay and N/N+1 testing apply to the new workflow versions created by this
package. Rollback reverts the new worker deployment before production adoption; it does not keep
the prototype family workflows as a fallback runtime.

## Documentation and traceability updates

Record workflow type/version, task queues, histories, failure injection, and exact test paths.

## Non-goals

Full family semantics or Deep Agent materialization.

## Drift guards

Static/test guards prevent alternate Agent Server macro workflows and database/provider access from workflow code.
