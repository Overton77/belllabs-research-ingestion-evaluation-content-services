---
id: WP-CP-020
title: Implement transactional run control, budgets, effects, settlement, and events
status: draft
implements: [REQ-CP-RUN-001, REQ-CP-RUN-002, REQ-CP-RUN-003, REQ-CP-RUN-004, REQ-CP-RUN-005, REQ-CP-RUN-006, REQ-CP-RUN-007, REQ-CP-RUN-008, REQ-CP-RUN-009, REQ-CP-RUN-010]
governed_by: [ADR-0003, SPEC-CP-RUN-CONTROL]
contracts: [CON-CP-RUN-REQUEST-V1, CON-CP-LIFECYCLE-V1, CON-CP-BUDGET-LEDGER-V1, CON-CP-DOMAIN-EVENT-V1]
blocked_by: [WP-CP-010]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-CP-020/]
---

# Implement transactional run control, budgets, effects, settlement, and events

## Outcome

PostgreSQL/application services transactionally admit exact ERCs and become the sole authority for lifecycle, commands, multidimensional reservations, effects, settlement, terminality, and product events.

## Current implementation baseline

Existing run-control, PostgreSQL authority, journal, outbox, and pre-Stage-3 tests are candidate seams. Their contract versions and incomplete migrations must follow WP-CP-001.

## Requirements implemented

All `REQ-CP-RUN-*` requirements.

## Architectural seams affected

Application services/reducer, PostgreSQL repositories and migrations, API commands/queries, outbox/relay, effect claims, budget ledger, projections, and Temporal start/control relay.

## Compatibility and migrations

Migrate via additive tables/columns and dual-read where necessary. No direct projection writes. Preserve command/event identities and prove rollback against exact migration revisions.

## Acceptance criteria

- [ ] Admission is atomic and idempotent and never starts execution on rejection.
- [ ] Concurrent commands use CAS and stable duplicate results.
- [ ] Lifecycle axes and terminality behave exactly as specified.
- [ ] Reservations prevent oversubscription and reconcile pending liabilities.
- [ ] Effects settle once under retry/ambiguity.
- [ ] Outbox consumers deduplicate and detect gaps.
- [ ] Async-child facts cannot mutate parent authority without reducer decisions.

## Qualification and evidence

Run `QUAL-CP-TRANSACTIONAL-AUTHORITY` against disposable PostgreSQL with transaction-failure and concurrency injection.

## Failure and rollback posture

Failed migrations roll back before new writers are enabled. Relay failure is retried from the durable outbox; it never repeats admission.

## Documentation and traceability updates

Record migration IDs, reducer command/fact schemas, API paths, and evidence.

## Non-goals

Family orchestration or provider runtime execution.

## Drift guards

Tests prevent Temporal, MongoDB, checkpoints, or provider callbacks from directly writing lifecycle/budget state.

