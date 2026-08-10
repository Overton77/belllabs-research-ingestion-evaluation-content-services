---
id: WP-BP-010
title: Implement canonical StageGraph runtime
status: ready
implements: [REQ-BP-SG-001, REQ-BP-SG-002, REQ-BP-SG-003, REQ-BP-SG-004, REQ-BP-SG-005, REQ-BP-SG-006, REQ-BP-SG-007, REQ-BP-SG-008, REQ-BP-SG-009, REQ-BP-SG-010]
governed_by: [ADR-0003, SPEC-BP-STAGEGRAPH]
contracts: [CON-BP-STAGEGRAPH-V2, CON-BP-STAGE-DECISION-V1]
blocked_by: [WP-CP-030, WP-CP-040]
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-BP-010/]
---

# Implement canonical StageGraph runtime

Canonical metadata revision: `c48867a240d09a98db9cdfb4937f55176f30adf1`

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
  join, normalization/canonical-ordering, weighted-group-ring, cycle, slow-sibling, late-result,
  producer-liability, obligation, and completion contracts in `IMPLEMENTATION_READINESS.md`.
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
direct-activity family path, and Agent Server macro graph. Move executable mechanics from the
legacy `app/temporal/stagegraph_workflow.py` module into the frozen
`app/temporal/workflows/stagegraph.py` owner and delete the legacy module at acceptance; a
re-export shim is not the accepted runtime. Behavioral comparison is diagnostic only; exact legacy
schema parity is not required.

## Acceptance criteria

- [ ] Structural and join validation is complete.
- [ ] Pre-publication normalization classifies every collection, applies the complete V2 key
  registry, preserves semantic arrays, rejects duplicate complete keys/non-NFC identifiers, and
  produces digest-stable canonical bytes.
- [ ] Readiness is pure and deterministic.
- [ ] Downstream `any(1)` starts before a slow sibling completes.
- [ ] Initial/resumed weighted-group-ring and per-group candidate cursors advance only with
  authoritative admission and prevent oversubscription/starvation.
- [ ] Every dependency disposition and `all`/`any`/`minimum(k)` satisfied/pending/impossible case
  follows the complete V2 truth table.
- [ ] Slow-sibling action/arrival routing, absolute late-result veto precedence, authored rule
  precedence, and exact admit/reject/quarantine effects are deterministic.
- [ ] Technical retry, stage cycle, and workflow cycle identities are distinct.
- [ ] Minimal invalidation reuses unaffected immutable outputs.
- [ ] Completion requires current accepted obligation evidence and closure of every producer
  liability, including child quiescence, reservations/usage, effects, cancellation, and exactly one
  result disposition.

## Qualification and evidence

Run `QUAL-BP-STAGEGRAPH-SEMANTICS-RECOVERY`, including normalization/digest stability, the complete
set-like key registry and semantic-array ordering, canonical byte ordering, duplicate rejection,
initial/resumed fairness cursors, complete joins/dispositions, captured replay, worker loss, waits,
cancellation, late-result veto precedence, durable liabilities, cycles, and Continue-As-New against
canonical V2 fixtures.

The qualification also includes a credential-gated live acceptance test. It must submit through
the BellLabs API, pass through transactional admission and `BellLabsRunWorkflow`, execute the real
`StageGraphWorkflow`, and run `OperationWorkflow` children with real LLM calls through the
accepted Deep Agents adapter. The graph must contain a branching frontier and a downstream
`any(1)` or equivalent minimum join. Evidence must prove that downstream work starts after the
satisfying result and before an unrelated slow sibling completes. Use a controlled synchronization
gate around the slow sibling while preserving real LLM calls, and prove ordering from Temporal
history events rather than provider latency or wall-clock timing alone. Then record accepted stage
outputs, current obligation evidence, and the run-control reducer's terminal outcome.

The live test complements deterministic unit/property, join-truth-table, API integration, Temporal
time-skipping, replay, recovery, cancellation, and late-result tests; it cannot replace them. Use
the smallest exact Deep Agent binding needed to prove StageGraph semantics. MCP servers, Skills,
executable sandboxes, snapshots, and the complete advanced-capability combination remain outside
this package's live-test requirement and are proved cohesively by `WP-CP-050`.

## Failure and rollback posture

Use versioned new family/interpreter contracts and captured replay histories. No legacy rollback
worker is required because there are no production executions to preserve.

## Documentation and traceability updates

Publish blueprint schemas, interpreter decision tables, test paths, histories, and timing evidence.

## Non-goals

Domain-specific Workflow Type topology or arbitrary model-authored graphs.

## Drift guards

No LangGraph checkpoint or model plan may become readiness/completion authority.
