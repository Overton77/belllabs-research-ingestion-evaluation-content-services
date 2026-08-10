---
id: WP-CP-045
title: Implement async-subagent parent-child lifecycle
status: accepted
implements: [REQ-CP-RUN-009, REQ-CP-EXEC-005, REQ-CP-EXEC-006, REQ-CP-EXEC-008, REQ-CP-DA-008, REQ-CP-DA-009, REQ-CP-DA-010, REQ-CP-DA-011, REQ-CP-DA-012]
governed_by: [ADR-0003, SPEC-CP-RUN-CONTROL, SPEC-CP-DURABLE-EXECUTION, SPEC-CP-DEEP-AGENT-RUNTIME]
contracts: [CON-CP-ASYNC-SUBAGENT-V1, CON-CP-WORKFLOW-MESSAGE-V1, CON-CP-BUDGET-LEDGER-V1]
blocked_by: [WP-CP-040]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-CP-045/]
---

# Implement async-subagent parent-child lifecycle

## Outcome

A parent Deep Agent can start, continue alongside, wait for, message, cancel, reconcile, and admit results from durable asynchronous subordinate executions without creating hidden macro workflows or surrendering BellLabs authority.

## Current implementation baseline

The accepted implementation replaces the partial remote-graph-shaped models with canonical
operation-execution contracts, PostgreSQL authority, MongoDB detail records, the wrapped Deep
Agents mechanism, and deterministic provider reconnection. See
[`evidence_v2/WP-CP-045`](../evidence_v2/WP-CP-045/README.md).

## Requirements implemented

The listed cross-owner async lifecycle, authority, messaging, cancellation, admission, and escalation requirements.

## Architectural seams affected

Deep Agent profile/materializer, operation workflow, application parent-child service, PostgreSQL reservations/decisions/messages, MongoDB execution/link documents, provider adapter, callbacks/polling, projections/API, and recovery tests.

## Authorized implementation slice

- Add `AsyncSubagentContract`, `AsyncSubagentExecution`, and `ParentAsyncSubagentLink` to the
  canonical operation-execution contracts and Mongo document models.
- Add authoritative parent-child reservation, lifecycle, message, cancellation, result-admission,
  and settlement commands/facts to run-control contracts and PostgreSQL migrations.
- Extend `OperationWorkflow` and the Deep Agents adapter through typed application ports; no hidden
  workflow type or provider-owned parent mutation is permitted.
- Wrap the exact Deep Agents `0.7.5` `AsyncSubAgentMiddleware` start/check/update/cancel/list tools.
  Persist BellLabs identity and authority before provider submission, bind returned thread/run IDs,
  and converge provider polling into typed facts and result manifests.
- Add the deterministic governance classifier and fixtures for subordinate, operation, and linked
  run routing. `app/domain/operation_execution/delegation.py` is the sole classifier owner;
  blueprint runtimes consume it and may add family-specific fixtures but must not implement a
  second classifier.
- Delete the partial remote-graph-shaped async models and their active consumers after the new
  lifecycle acceptance suite passes.

## Replacement and migrations

Replace the partial remote-graph-shaped async models with the canonical subordinate contracts.
No compatibility reader or record migration is required. Do not enable async spawning until the
Deep Agents `0.7.5` `AsyncSubAgentMiddleware` Agent Protocol adapter and its polling/reconciliation
path satisfy the canonical contract.

## Acceptance criteria

- [x] Spawn persists exact contract/link/reservation before submission.
- [x] `required_blocking`, `degradable_blocking`, `nonblocking`, and `advisory` behave exactly as frozen.
- [x] Parent and child exchange ordered typed messages with durable receipts.
- [x] Retry/callback/poll ambiguity creates one effective child and settlement.
- [x] Cancellation, orphan, late result, and superseded generation are reconciled.
- [x] Child output changes no parent state before explicit admission.
- [x] Governance classifier selects subordinate, operation, or linked-run execution deterministically.

## Qualification and evidence

Run `QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE` with deterministic adapters plus the actual Deep Agents 0.7.5 mechanism used by the implementation. Optimize provider-specific lifecycle behavior later without weakening this contract.

## Failure and rollback posture

Feature-gate new async spawning while keeping already-started children reconcilable. Rollback disables new admissions, continues status/cancellation/reconciliation workers, and preserves every child/link/result record.

## Documentation and traceability updates

Record the actual Deep Agents mechanism, adapter gaps, lifecycle transitions, APIs, tests, and deferred optimizations.

## Non-goals

Perfect provider scheduling, global child optimization, or automatic promotion of async children to Workflow Types.

## Drift guards

Provider task/thread state cannot be parent authority; model output cannot weaken dependency class or admit its own result.
