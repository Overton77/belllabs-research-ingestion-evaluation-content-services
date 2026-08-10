---
id: WP-CP-020
title: Implement transactional run control, budgets, effects, settlement, and events
status: accepted
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

## Authorized implementation slice

- Replace shared contracts/reducer in `app/domain/run_control/contracts.py`, `budget.py`, and
  `reducer.py`.
- Replace application coordination in `app/application/run_control.py` and repository ports.
- Add ordered PostgreSQL migrations after the current migration head and replace the PostgreSQL
  repository implementation used by application composition.
- Update `app/api/run_control.py`, outbox relay adapters, and unit/contract/PostgreSQL integration
  tests.
- Temporal may receive emitted start/control messages only after this package commits authoritative
  decisions; Temporal workflow implementation remains WP-CP-030.

## Replacement and migrations

Create the canonical PostgreSQL schema through new ordered migrations and update all application
writers/readers in this package. Prototype tables may be dropped or replaced after their useful
test fixtures are ported. No dual read/write or production backfill is required. No direct
projection writes are allowed.

## Acceptance criteria

- [x] Admission is atomic and idempotent and never starts execution on rejection.
- [x] Concurrent commands use CAS and stable duplicate results.
- [x] Lifecycle axes and terminality behave exactly as specified.
- [x] Reservations prevent oversubscription and reconcile pending liabilities.
- [x] Effects settle once under retry/ambiguity.
- [x] Outbox consumers deduplicate and detect gaps.
- [x] Async-child facts cannot mutate parent authority without reducer decisions.

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
