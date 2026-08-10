---
id: WP-CP-001
title: Establish control-plane authority and replacement boundary
status: accepted
implements: []
governed_by: [ADR-0003]
contracts: []
blocked_by: []
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-CP-001/]
---

# Establish control-plane authority and replacement boundary

## Outcome

The repository has one mechanically checkable authority map, one canonical contract-to-code owner
map, and explicit deletion gates from prototype runtime paths to every canonical `CON-*` surface.
No production compatibility migration is designed or implemented.

## Current implementation baseline

The code contains useful `OperationAssemblySpec`, `StageExecutionBinding`, interpreters,
run-control services, Temporal prototypes, Deep Agents 0.7.5 experiments, and partial
async-subagent records. Treat them as implementation observations. Reuse accepted behavior and
delete superseded executable paths; do not retain old schemas merely because they exist.

## Requirements implemented

This is a governance prerequisite and does not claim product requirements complete. It establishes
the ownership and replacement boundary required by all listed `REQ-*` owners.

## Architectural seams affected

Definition registry, API schema registry, graph-runtime contracts, operation executor, Temporal
workflows, repositories, migrations, tests, evidence ledgers, and documentation pointers.

## Replacement inventory

- Map each canonical `CON-*` contract to its exact domain owner, persistence owner, application
  service, transport/runtime adapter, target path, migration, and test suite.
- Identify only the current executable files, imports, dependencies, settings, worker registration,
  launch paths, and tests that must be replaced or deleted. Do not inventory inert code merely to
  preserve it.
- Mark OpenAI Agents SDK and Agent Server macro-runtime paths `delete_at_replacement_gate`.
- Freeze old Stage 0-8 packages and ledger without deleting evidence.
- Authorize direct replacement of local prototype persistence and fixtures.

## Acceptance criteria

- [x] Every canonical contract has one code owner and one persistence/authority owner.
- [x] Every canonical contract has a planned API/runtime surface and test seam.
- [x] No active code/document pointer treats frozen Stage 0-8 packages as normative.
- [x] Every superseded active runtime responsibility has one replacement package and deletion gate.
- [x] No dual-read, dual-write, compatibility worker, legacy replay, or prototype-data backfill is planned.
- [x] Unrelated worktree changes are identified and preserved.
- [x] The first safe code change for WP-CP-010 is explicit.

## Qualification and evidence

Store the authority/owner matrix, replacement/deletion checklist, link report, changed-path record,
and accepted handoff under `evidence_v2/WP-CP-001/`.

## Failure and rollback posture

This package makes documentation/index changes only. If an authority owner or deletion gate is
missing, retain the new authority documents but keep WP-CP-010 blocked.

## Documentation and traceability updates

Update the document registry, traceability projection, canonical authority index, and package
status. All active work packages must link to `IMPLEMENTATION_READINESS.md`.

## Non-goals

No model/schema migrations, runtime changes, compatibility design, or historical-code archaeology
beyond identifying active replacement targets.

## Drift guards

Search active docs and code comments for claims that Agent Server or OpenAI Agents SDK is required
macro/runtime authority or that compatibility with prototype persistence is required.
