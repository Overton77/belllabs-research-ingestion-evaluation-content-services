---
id: WP-CP-045
title: Implement async-subagent parent-child lifecycle
status: draft
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

Partial `AsyncSubagentDefinition`, `AsyncTaskKey`, runtime projection, intervention, result-manifest, and feature-maturity contracts exist. They assume a remote graph shape and do not yet constitute the accepted complete lifecycle.

## Requirements implemented

The listed cross-owner async lifecycle, authority, messaging, cancellation, admission, and escalation requirements.

## Architectural seams affected

Deep Agent profile/materializer, operation workflow, application parent-child service, PostgreSQL reservations/decisions/messages, MongoDB execution/link documents, provider adapter, callbacks/polling, projections/API, and recovery tests.

## Compatibility and migrations

Replace or version partial async models through expand-contract. Preserve old records as historical observations. Do not enable an async mode until its exact provider mechanism and fallback behavior satisfy the canonical contract.

## Acceptance criteria

- [ ] Spawn persists exact contract/link/reservation before submission.
- [ ] Required, degradable, nonblocking, and advisory classes behave exactly as frozen.
- [ ] Parent and child exchange ordered typed messages with durable receipts.
- [ ] Retry/callback/poll ambiguity creates one effective child and settlement.
- [ ] Cancellation, orphan, late result, and superseded generation are reconciled.
- [ ] Child output changes no parent state before explicit admission.
- [ ] Governance classifier selects subordinate, operation, or linked-run execution deterministically.

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

