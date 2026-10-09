---
type: Decision Record
title: "Goal iteration, provider context compaction, mission continuation and Temporal Continue-As-New are four independent progress mechanisms with separate counters; compaction prefers the qualified native primitive and continuation uses the sealed checkpoint"
description: "Proposed for the multi-provider packet (SPEC-01, MP-12): none of the four resets another's budget or implies the goal moved; mission/v2 continuation policy sets occupancy watermarks; soft pressure serializes a qualified native compaction with turn admission; hard pressure or native failure seals and transfers through an idempotent phase machine; unknown occupancy is reported as unknown."
tags: [mission-control, adr, proposed, continuation, compaction, goal-directed]
status: proposed
source: docs/specs/multi-provider-2026-10/SPEC-01-runtime.md (four independent progress mechanisms); docs/specs/multi-provider-2026-10/RESEARCH.md (Codex thread compaction, Claude Python SDK hook coverage, Temporal Continue-As-New); workflow-types/02-GOAL_LOOP; workflow-types/08-CONTINUATION_COMPACTION_AND_TRANSFER; application/context/continuation.py
---

# Goal iteration, provider context compaction, mission continuation and Temporal Continue-As-New are four independent progress mechanisms with separate counters; compaction prefers the qualified native primitive and continuation uses the sealed checkpoint

**Status: proposed.** Becomes accepted when the owner authorizes it.

A GoalDirected run on a coding agent fills its context window long before a goal iteration ends, and
each provider handles that differently: Codex documents explicit thread compaction, the Claude Python
SDK does not expose every compaction hook its TypeScript sibling has, and Cursor compacts on its own
schedule. We decided these are four mechanisms with four triggers and four kinds of durable state. A
goal iteration advances on evaluated progress against unmet criteria. Native compaction advances a
context epoch inside the same iteration. Mission continuation (`ContinuationService.seal` then
`transfer`) rotates the session generation with a sealed Checkpoint, a transferred Context Packet and a
Workspace Snapshot. Continue-As-New rotates only the Temporal execution. None resets another's budget
or retry counter and none implies the goal moved. The `mission/v2` `continuation` block sets soft and
hard occupancy watermarks, headroom and bounds on turns, transfers and compaction failures. At soft
pressure the lane serializes a qualified native compaction with turn admission and remeasures; at hard
pressure or on native failure it stops at a safe boundary and runs the continuation phase machine
(requested, frozen, snapshotted, sealed, target prepared, hydrated, verified, activated; each phase
idempotent; exactly one target activates while the old generation stays fenced). Where a provider
exposes no occupancy we fall back to conservative turn and session budgets and report `unknown`, never
a guessed percentage. We rejected counting compaction as an iteration (it corrupts governors and
acceptance evidence) and rejected one custom compactor for every provider (it discards the better
native primitive where one exists and still has no control API where none does).

## Consequences

- Mandatory checkpoint content (system intent, exact grants, success criteria, unsettled liabilities,
  accepted artifacts, branch and patch state, mailbox frontier) rejects the transfer when missing rather
  than truncating; summaries are advisory and validated against structured state.
- Cumulative billed tokens are never occupancy; an observed before-compaction hook is not a control API.
- A hosted lane with no usable session rollover fails admission for a mission that requires it, or runs
  a deliberately bounded job whose contract does not require rollover.
- A failed target generation does not consume held mailbox entries; packet, workspace and
  materialization digests are verified before the first turn of the new generation.
