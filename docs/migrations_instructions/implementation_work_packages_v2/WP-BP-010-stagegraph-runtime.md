---
id: WP-BP-010
title: Implement canonical StageGraph runtime
status: draft
implements: [REQ-BP-SG-001, REQ-BP-SG-002, REQ-BP-SG-003, REQ-BP-SG-004, REQ-BP-SG-005, REQ-BP-SG-006, REQ-BP-SG-007, REQ-BP-SG-008, REQ-BP-SG-009, REQ-BP-SG-010]
governed_by: [ADR-0003, SPEC-BP-STAGEGRAPH]
contracts: [CON-BP-STAGEGRAPH-V1, CON-BP-STAGE-DECISION-V1]
blocked_by: [WP-CP-030, WP-CP-040]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-BP-010/]
---

# Implement canonical StageGraph runtime

## Outcome

The pure interpreter and Temporal family workflow execute exact acyclic graphs with `all`/`any`/`minimum(k)`, prompt incremental release, fairness, bounded cycles, minimal invalidation/reuse, and obligation-based completion.

## Current implementation baseline

Preserve the existing interpreter and successful Temporal/Deep Agents experiment as prior art. Remove the production frontier `asyncio.gather()` barrier through operation-child launch and incremental reconciliation.

## Requirements implemented

All `REQ-BP-SG-*` requirements.

## Architectural seams affected

Blueprint contracts/compiler, interpreter/kernel, Temporal StageGraph workflow, operation children, reservations, result settlement, stage projections, evaluation/cycle services, and replay tests.

## Compatibility and migrations

Version blueprint/interpreter decision contracts. Run old and new fixtures through parity projections before selecting the new family version for admission.

## Acceptance criteria

- [ ] Structural and join validation is complete.
- [ ] Readiness is pure and deterministic.
- [ ] Downstream `any(1)` starts before a slow sibling completes.
- [ ] Capacity/fairness prevent oversubscription and starvation.
- [ ] Technical retry, stage cycle, and workflow cycle identities are distinct.
- [ ] Minimal invalidation reuses unaffected immutable outputs.
- [ ] Completion requires current accepted obligation evidence.

## Qualification and evidence

Run `QUAL-BP-STAGEGRAPH-PARITY-RECOVERY`, including captured replay, worker loss, waits, cancellation, late results, cycles, and Continue-As-New.

## Failure and rollback posture

Use versioned family workflow/interpreter contracts and retain a rollback worker for already-admitted compatible runs.

## Documentation and traceability updates

Publish blueprint schemas, interpreter decision tables, test paths, histories, and timing evidence.

## Non-goals

Domain-specific Workflow Type topology or arbitrary model-authored graphs.

## Drift guards

No LangGraph checkpoint or model plan may become readiness/completion authority.

