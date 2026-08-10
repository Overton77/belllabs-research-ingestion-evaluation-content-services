---
id: WP-CP-001
title: Establish control-plane authority, supersession, and contract migration inventory
status: ready
implements: []
governed_by: [ADR-0003]
contracts: []
blocked_by: []
github_issue: null
evidence: [docs/migrations_instructions/evidence_v2/WP-CP-001/]
---

# Establish control-plane authority, supersession, and contract migration inventory

## Outcome

The repository has one mechanically checkable authority map and an expand-contract migration plan from current v1/v2/v3 models and frozen Stage 3 artifacts to every canonical `CON-*` surface.

## Current implementation baseline

The code already contains useful `OperationAssemblySpec`, `StageExecutionBinding`, interpreters, run-control services, Temporal prototypes, Deep Agents 0.7.5 dependencies, and partial async-subagent records. The worktree also contains extensive owner changes. Treat all current schemas as implementation observations until mapped; preserve unrelated modifications.

## Requirements implemented

This is a governance prerequisite and does not claim product requirements complete. It establishes the migration map needed by all listed `REQ-*` owners.

## Architectural seams affected

Definition registry, API schema registry, graph-runtime contracts, operation executor, Temporal workflows, repositories, migrations, tests, evidence ledgers, and documentation pointers.

## Compatibility and migrations

- Inventory every current contract version, consumer, stored representation, migration, and fixture.
- Map keep/version/replace/remove disposition to canonical `CON-*` contracts.
- Identify OpenAI Agents SDK assumptions and distinguish deletion from historical preservation.
- Freeze old Stage 0–8 packages and ledger without deleting evidence.
- Define expand-contract ordering and rollback for persisted records.

## Acceptance criteria

- [ ] Every affected current schema and source document has an owner and disposition.
- [ ] Every canonical contract has a planned code owner, persistence owner, API surface, and test seam.
- [ ] No active code/document pointer treats frozen Stage 0–8 packages as normative.
- [ ] Unrelated worktree changes are identified and preserved.
- [ ] The first safe code change for WP-CP-010 is explicit.

## Qualification and evidence

Store the inventory, link report, schema-consumer matrix, changed-path record, and accepted handoff under `evidence_v2/WP-CP-001/`.

## Failure and rollback posture

This package makes documentation/index changes only. If the migration inventory is incomplete, retain the new authority documents but keep WP-CP-010 blocked.

## Documentation and traceability updates

Update the document registry, traceability projection, canonical authority index, and package status.

## Non-goals

No model/schema migrations or runtime changes.

## Drift guards

Search active docs and code comments for claims that Agent Server or OpenAI Agents SDK is required macro/runtime authority.

