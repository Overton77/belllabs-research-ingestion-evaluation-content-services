# 02 — Implement transactional Run admission, lifecycle, budgets, and domain events

**What to build:** Establish the single PostgreSQL-authoritative boundary that decides whether a Run Request becomes a Workflow Run and governs every later lifecycle, budget, terminalization, readiness, and durable-event change.

**Blocked by:**
- 01 — Implement versioned Workflow Definitions and Effective Run Configuration compilation

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/2


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Introduce application PostgreSQL migration ownership separately from Temporal's database and make FastAPI lifespan initialize only application resources.
- Organize commands around one transactional application service and lifecycle reducer; API callers, agents, relays, workers, and callbacks must use the same boundary.
- Keep the event envelope transport-neutral. A PostgreSQL outbox relay is sufficient; broker selection must not leak into domain contracts.

## Verification approach

Use disposable PostgreSQL and public command/query seams. Inject transaction failures and concurrent commands, then verify externally visible all-or-nothing state and idempotent results.

## Explicit non-goals

- Definition publication/compilation, blueprint scheduling, runtime adapters, and workflow-specific budget values.
- General event sourcing or treating Temporal status as lifecycle authority.

## Acceptance criteria

- [ ] Application PostgreSQL migrations and disposable integration infrastructure are initialized independently from Temporal persistence.
- [ ] Exact duplicate Run Requests return the prior decision; conflicting identity reuse is rejected; rejected requests create no run, budget account, or start intent.
- [ ] Accepted admission atomically creates a versioned pending projection, immutable request result, baseline multidimensional reservations, and outbox envelopes.
- [ ] One reducer handles typed actor-attributed commands with idempotency and expected-run-version concurrency, appending transitions and updating projections atomically.
- [ ] Phase, wait condition, pause decision, terminal outcome, and purpose-bound readiness remain separate; waits may auto-resume, while pauses require authorized resume.
- [ ] Budgets reserve before dispatch, reconcile actual and pending usage idempotently, preserve parent-child rollup, enforce dimensions independently, and govern soft-limit Continuation Proposals.
- [ ] Only the reducer accepts current Terminalization Proposals and assigns completed, partially_completed, failed, or cancelled exactly once; bounded finalization cannot start new substantive work.
- [ ] A transactional outbox supports stable event identities, at-least-once delivery, per-aggregate ordering, durable cursors, duplicate suppression, gap detection, and recovery.
- [ ] Integration tests prove atomic rollback, concurrent conflict, duplicate redelivery, hard-cap enforcement, cancellation settlement, and projection reconstruction.

## Source basis

- Transactional Run Admission, Lifecycle, and Budgets
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
