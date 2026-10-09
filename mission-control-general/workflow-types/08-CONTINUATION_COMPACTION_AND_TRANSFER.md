---
type: Workflow Specification
title: "Mission Control — Continuation, Compaction, and Transfer"
description: "Scope: preserving logical execution across disposable Agent Sessions"
tags: [mission-control, spec, workflow]
---
# Mission Control — Continuation, Compaction, and Transfer

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Scope:** preserving logical execution across disposable Agent Sessions

## 1. Core law

Agent Sessions are disposable. A long-running logical execution continues by producing a validated Continuation Checkpoint and transferring it into a fresh Agent Session.

The primary path is:

`active logical execution → Compaction → Continuation Checkpoint → validation → Continuation Transfer → fresh Agent Session`

Continuation must not substantially rewrite an existing session's history or pretend that two sessions are one provider-native session.

Agent Session and workflow activation cardinality are many-to-many. One healthy Session may support several Iterations or related actions. One activation may cross Sessions through intra-activation Continuation.

## 2. Policy separation

- **Workflow or cycle policy** decides whether another activation, Revisit, Iteration, or Optimization Round occurs.
- **Continuation Policy** decides whether and how one logical execution crosses an Agent Session boundary.
- **Compaction Policy** decides triggers, algorithm, contents, validation, and failure behavior for producing a checkpoint.
- **Session Policy** decides fresh-session, brief-reuse, and checkpoint-resume behavior.

These policies may coordinate but are not aliases.

## 3. Identity and lineage

Continuation preserves logical execution identity while creating a new Agent Session identity. Revisit, Iteration, Retry, and Optimization Round identities remain distinct even when they consume the same checkpoint lineage.

Every transfer records:

- source logical execution and Agent Session;
- checkpoint identity, schema, digest, author, and validator;
- referenced Artifacts and workspace snapshot;
- target Agent Session and harness execution;
- transfer reason and triggering policy;
- event cursor and unresolved work carried forward.

## 4. Settled constraints

- `compact_and_transfer` is the default long-running Session Policy.
- Brief in-session reuse is permitted only below declared policy thresholds.
- A raw transcript is not a checkpoint.
- A summary without typed state, Artifact references, invariants, and pending work is insufficient.
- Producer, verifier, and evaluator independence cannot be collapsed through checkpoint transfer.
- Session transfer cannot expand capabilities, authority, budget, or input access.
- Compaction and transfer must remain visible through Mission state, events, and lineage.

## 5. Compaction triggers

Compaction Policy may trigger on:

- Context Health Policy soft or hard threshold;
- context-window ratio;
- tool-result volume;
- turn count;
- workflow-system boundary;
- provider-native compaction warning;
- operator or Coordinator request;
- harness migration;
- cache breakpoint; or
- repeated uncertainty or degraded performance.

A soft threshold schedules orderly Compaction. A hard threshold prevents further agent work until transfer succeeds or the execution reaches an explicit terminal outcome.

Context Health Policy is versioned by model, runtime, tool profile, and task class. It reserves capacity for outputs and reasoning and incorporates observed degradation telemetry rather than relying on one universal context percentage.

## 6. Continuation Checkpoint contract

A checkpoint records:

- Mission, Run, Revision, Program Node, activation, logical execution, and source Agent Session identities;
- Goals, Objectives, Success Criteria, and current acceptance state;
- decisions and their rationale;
- immutable input and output Artifact references;
- workspace and sandbox snapshot reference;
- completed, active, pending, and blocked work;
- verification and testing dispositions;
- unresolved questions and Human Tasks;
- queued Commands;
- event cursor;
- remaining budgets and governors;
- capability and profile versions;
- governing invariants and constraints; and
- precise recommended next actions.

It contains no secrets, unbounded tool payloads, or dependence on raw transcript text.

For Goal Loop work, the checkpoint references the current immutable Loop Journal head and bounded Loop State rather than embedding the whole Journal.

## 7. Compactor and validator

A deterministic reducer preserves known structured state. An admitted compacting agent may synthesize semantic decisions and narrative continuity.

The active producer may propose checkpoint content but is not the sole validator for high-risk transfer. Validation checks schema, digests, references, identity, authority, capability bounds, budgets, unresolved gates, and workspace consistency. Complex work additionally requires semantic continuity evaluation.

## 8. Failed Compaction

When a checkpoint fails validation:

1. park the source execution safely;
2. retry the compactor within a separate cap;
3. try an admitted fallback compactor;
4. request human review when policy requires; and
5. cancel or fail explicitly when no valid checkpoint can be produced.

Mission Control never starts a fresh Agent Session from a known-invalid checkpoint.

## 9. Transfer materialization

The full transfer sequence is:

`safe boundary → freeze new agent actions → snapshot structured state and workspace → deterministic reduction → semantic synthesis → validate continuity → seal checkpoint → provision fresh Session → hydrate → continuity check → resume`

The target Agent Session receives:

- stable instruction and tool prefix;
- admitted capabilities;
- validated checkpoint;
- immutable Artifact pointers;
- workspace and sandbox snapshot;
- pending authorized instruction; and
- relevant policy and governor state.

The complete prior transcript is not replayed by default.

Pending Commands remain durably held and are delivered only after the target Session confirms successful hydration.

## 10. Intra-activation and inter-activation continuity

- **Intra-activation Continuation** starts a fresh Agent Session while preserving one logical execution identity.
- **Inter-activation Handoff** supplies a checkpoint to a new Revisit, Iteration, Optimization Round, or other activation.

Inter-activation handoff creates a new activation identity even though continuity lineage is preserved.

## 11. Continuation Decision

Compaction and Transfer is the default response to context degradation, but an authorized Continuation Decision may instead:

- transfer to another admitted model or runtime;
- checkpoint and pause;
- decompose through an existing Action Space;
- propose a Mission Revision;
- invoke a Child Mission within grant;
- request human input; or
- stop because further work is unjustified.

Every decision records its reason, authority, and effect on identity, budget, capabilities, and pending work.

## 12. Memory and cache

A Continuation Checkpoint is execution-scoped handoff state, not long-term memory. Promotion into governed memory is a separate explicit decision.

Where supported, the target session may reconstruct a byte-identical stable prefix and append the checkpoint after the cache boundary. Cache preservation is an optimization and cannot weaken session isolation or checkpoint provenance.

## 13. Governors

Continuation Policy governs:

- maximum transfers;
- cumulative tokens, cost, and wall-clock;
- failed Compactions;
- no-progress transfers; and
- transfer-specific resource usage.

Every transfer records its reason and measurable progress. Exhaustion produces an explicit governed outcome and triggers reconciliation or human review.

## 14. Human correction

A sealed checkpoint is immutable. A human may inspect it and submit a correction that creates a superseding checkpoint with authorship, rationale, and lineage.

## 15. Remaining questions

1. Exact checkpoint schema versions and compatibility rules.
2. Workspace and sandbox snapshot atomicity at the transfer boundary.
3. Delivery and ordering rules for queued Commands during transfer.
4. Which risk levels require semantic continuity evaluation or human review.
5. Provider-native compaction observation and reconciliation with Mission Control checkpoints.
