---
id: WP-BP-010
title: Implement canonical StageGraph runtime
status: ready_when_unblocked
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

## Authorized implementation slice

- Replace StageGraph models in `app/domain/control_plane/contracts.py` with the typed dependency,
  join, scheduler, cycle, late-result, obligation, and completion contracts in
  `IMPLEMENTATION_READINESS.md`.
- Replace `app/domain/orchestration/interpreter.py` with a side-effect-free proposal interpreter
  over an exact accepted projection.
- Implement family mechanics only in `app/temporal/workflows/stagegraph.py`, using
  `OperationWorkflow` children and incremental reconciliation.
- Port useful semantic handlers behind exact operation bindings; delete the direct-activity
  StageGraph workflow and Agent Server macro graph at acceptance.
- Add truth-table/property tests, early-release timing, fairness saturation, minimal invalidation,
  replay, recovery, cancellation, late-result, and obligation-completion suites.

## Replacement and migrations

Implement the canonical typed-dependency blueprint and decision contracts. Port useful semantic
fixtures to the new schemas, then remove the parallel dependency maps, ambiguous concurrency field,
direct-activity family path, and Agent Server macro graph. Behavioral comparison is diagnostic
only; exact legacy schema parity is not required.

## Acceptance criteria

- [ ] Structural and join validation is complete.
- [ ] Readiness is pure and deterministic.
- [ ] Downstream `any(1)` starts before a slow sibling completes.
- [ ] Capacity/fairness prevent oversubscription and starvation.
- [ ] Technical retry, stage cycle, and workflow cycle identities are distinct.
- [ ] Minimal invalidation reuses unaffected immutable outputs.
- [ ] Completion requires current accepted obligation evidence.

## Qualification and evidence

Run `QUAL-BP-STAGEGRAPH-SEMANTICS-RECOVERY`, including captured replay, worker loss, waits,
cancellation, late results, cycles, and Continue-As-New against canonical fixtures.

## Failure and rollback posture

Use versioned new family/interpreter contracts and captured replay histories. No legacy rollback
worker is required because there are no production executions to preserve.

## Documentation and traceability updates

Publish blueprint schemas, interpreter decision tables, test paths, histories, and timing evidence.

## Non-goals

Domain-specific Workflow Type topology or arbitrary model-authored graphs.

## Drift guards

No LangGraph checkpoint or model plan may become readiness/completion authority.
