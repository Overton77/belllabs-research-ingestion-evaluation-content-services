---
type: Decision Record
title: "Session Lane continuation is a persisted phase machine driven by the operation workflow; a fenced handover settles cancelled and an ambiguous one in doubt"
description: "Proposed for the multi-provider packet (SPEC-01, MP-12, integrated 2026-10-09): the operation workflow advances one idempotent phase per activity, the phase lives on the continuation_transfer row, held mailbox entries stay queued until activation, only lanes that register a hydrator and snapshot port can be continued, and the source session is fenced from freeze until activation."
tags: [mission-control, adr, proposed, continuation, temporal]
status: proposed
source: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md (continuation crash recovery); ADR-0039; .scratch/multi-provider-2026-10-08/worktrees/MP-12/.scratch-handoff/MP-12.md; application/context/phases.py; adapters/temporal/continuation_composition.py
---

# Session Lane continuation is a persisted phase machine driven by the operation workflow; a fenced handover settles cancelled and an ambiguous one in doubt

**Status: proposed.** Becomes accepted when the owner authorizes it.

ADR-0039 separates continuation from compaction, goal iteration and Continue-As-New; this record
decides where continuation runs and how it survives a crash. A Session Lane (Cursor, Claude, Codex)
reaches a safe boundary at a turn's terminal frame. There `lane.turn` measures context pressure and,
when a transfer is pending (a `request_continuation` command or a hard/soft-pressure trigger), ends
the segment without settling the unit. The operation workflow then calls `continuation.pending` and
`continuation.advance` until the transfer is activated or ended, behind the
`mp12-operation-continuation` patch. Each advance enters exactly one phase of requested, frozen,
snapshotted, sealed, target prepared, hydrated, verified, activated; the phase is persisted on the
`continuation_transfer` row before the call returns, so a retried activity resumes from it. Only one
target generation activates per source generation of a logical execution. The source session is
fenced from `frozen` until activation or an ended transfer, so `lane.turn` refuses any new native
dispatch with a typed retryable `continuation_in_flight`. Held mailbox entries stay `queued` and the
delivery service refuses to claim them while the fence holds, so a failed target never consumes one.
The first turn after activation goes to the target session with a `continuation:<transfer>`
instruction resolved from the ledger, so a lost workflow-local reference is harmless.

A worker composes one continuation stack (`adapters/temporal/continuation_composition.py`). Only lanes
that register a hydrator and a snapshot port can be continued; the service-level snapshot port
refuses, so a lane without a registration fails the transfer typed instead of inventing a workspace.
Registration never marks a lane's continuation qualified.

We rejected keeping the phase only in workflow state (a worker loss mid-phase would lose which side
effects already happened) and rejected continuing inside one long `lane.turn` activity (it would
hold a cognitive slot across hydration and could not be driven by commands at a boundary).

## Consequences

- A handover dispatch the Stop Fence denies settles the unit `cancelled`: the source turn is
  terminal and nothing reached the target. An ambiguous handover dispatch parks `in_doubt`, because
  the target may have accepted it.
- The phase lives in the `transfer` jsonb today; typed phase columns and a database uniqueness rule
  for one activation per source generation are deferred to a later release (not in 1.2.0). The
  operation workflow serializes activations per harness execution and the session-ownership fence
  prevents two workers from driving the same transfer.
- The Deep Agents governed path has no mid-activity safe boundary; a `request_continuation` on a Deep
  Agents unit is recorded only, and its rotation stays the GoalDirected session policy.
- The context-pressure policy is per worker until an execution binding carries its own.
