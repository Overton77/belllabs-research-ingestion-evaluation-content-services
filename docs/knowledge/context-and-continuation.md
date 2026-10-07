---
type: Concept
title: Context, state handoff and continuation
description: How context is selected reproducibly, how state moves between activations through a reducer, how checkpoint lineage classifies a unit before dispatch, and which continuation and compaction rules remain specified only.
tags: [mission-control, context, checkpoints, continuation, compaction, implementation]
---

# Context, state handoff and continuation

ADR-0007 separates authoritative state (journal, typed state, command frontier, effect
receipts, acceptance) from advisory memory and from runtime graph checkpoints, which
only recover graph execution. Its consequences are the subjects here:
[Context Selection](../../GLOSSARY.md), [State Handoff](../../GLOSSARY.md),
[Checkpoint](../../GLOSSARY.md) and [Checkpoint Lineage](../../GLOSSARY.md),
[Continuation](../../GLOSSARY.md), [Compaction](../../GLOSSARY.md) and
[In Doubt](../../GLOSSARY.md) units. Snapshots and forks are in [recovery](recovery.md).

## Context selection

Specified (expansion/CONTEXT-STATE-AND-CONTROL.md): an immutable
`mc.context_selection.v1` record holding candidate capture references, selected item
digests, omission reason codes, trust labels and the actual prompt plan; available input
budget is window minus reserved output, overhead, control reserve and margin, and
mandatory facts that exceed it block with `CONTEXT_BUDGET_EXCEEDED`; unknown token counts
use a conservative bound; retrieved content is data with provenance, never instructions.

Implemented: `domain/graph_runtime/definitions.py` has `ContextPolicyDefinition` (one
rule per source kind among `admitted_input`, `artifact`, `evidence`, `catalog`,
`procedural_store`, `prior_checkpoint_summary`, each with an authority label, item and
byte caps; the policy can never store scientific, approval, budget or terminality
authority) and `ContextAssemblySpec` (ordered manifest entries with source digests,
tombstones, contradiction groups and a self-verified assembly digest).
`domain/graph_runtime/contracts.py` adds `SubagentContextSlice` and
`ContextReconstructionResult` (reconstructed, missing and tombstoned entries,
completeness). A `context_selection` ledger record and the token-budget arithmetic were
not found in the code opened here.

## State handoff

Specified: `mc.state_handoff.v1` carries producer identity and generation, base state
version and digest, a typed delta reference, output references and frontiers; the reducer
compares base version and digest, applies the delta as one attributable transition,
rejects conflicts with `STATE_VERSION_CONFLICT`, and merges parallel fields only by
declared policy (`replace_if_base_matches`, `append_unique_by_id`,
`map_union_disjoint_keys`, `reduce_registered`).

Implemented: lifecycle and settlement transitions are owned by
`domain/policies/reducer.py` ([lifecycle](lifecycle.md)); goal handoffs reference a
checkpoint key plus artifact digest (`GoalHandoffReference`); the Deep Agents
materializer composes state channels with `_replace`, `_append_unique_by_id` and
`_merge_by_key` reducers. A `state_handoff` record and the base-version digest check as
specified were not located.

## Checkpoint lineage and in-doubt units

`domain/execution/checkpoint_lineage.py` (`CON-CP-CHECKPOINT-LINEAGE-V1`) is the
implemented core. Each unit generation owns a cognitive namespace; every LangGraph
checkpoint is stamped with unit key, execution generation, invocation id, binding digest
and state-schema digest (`CheckpointInvocationPlan.metadata_stamps`). Before dispatch the
lane classifies the generation as `settled`, `observed_unsettled`, `not_submitted`,
`interrupted`, `terminal_unobserved` or `in_doubt` (`CheckpointClassification`). A
`CheckpointTransitionObservation` links source and result checkpoints with verified
ancestry and an immutable result manifest, accepted by compare-and-set on the namespace
head; `seedable` records whether a later unit may pin to it. Writes presenting a
superseded claim fence or generation are recorded as `LineageWriteRejection`
(`StaleClaimFence`). When no unique safe case exists, `CheckpointLineageInDoubt` carries a
typed reason (`missing_checkpoint`, `ancestry_mismatch`, `schema_mismatch`,
`foreign_descendant`, `multiple_stamped_leaves`, `pending_interrupt`,
`unsettled_effect_claims`, `ambiguous_native_effect` and others) and candidate keys, and a
`UnitReconciliationIncident` stays `operator_required` until a decision such as
`accept_descendant` names an accepted checkpoint. This matches the glossary rule that an
in-doubt unit is reconciled, never duplicated.

## Continuation and compaction

Specified (workflow-types/08): agent sessions are disposable; continuation produces a
validated Continuation Checkpoint (identities, goals and acceptance state, decisions,
artifact references, workspace snapshot, pending work, queued commands, event cursor,
budgets, versions) and transfers it into a fresh session; a raw transcript is not a
checkpoint; compaction triggers on context health thresholds, tool volume, turn count,
boundaries or request; failed compaction parks, retries within a cap, falls back, asks a
human or fails explicitly; transfers are governed by maximum count, cumulative cost and
no-progress limits; a sealed checkpoint is corrected only by a superseding one.

Implemented: the reducer accepts `propose_continuation` and `decide_continuation`
actions under `workflow_run.propose_continuation` and `workflow_run.decide_continuation`
(`domain/policies/contracts.py`); `RedactedCheckpointSummary` exposes status, pending
interrupts and output references without bodies; `ForkFromCheckpointIntervention` cannot
cross request scope. No compaction policy, context health policy, `compact_and_transfer`
session policy or continuation governor was found in the code opened here; runtime
checkpoints are evidence, and the canonical checkpoint manifest of RUNTIME-CONTRACTS.md
is specified only.

# Citations

- Spec: `../mission-control-general/workflow-types/08-CONTINUATION_COMPACTION_AND_TRANSFER.md`;
  `../mission-control-general/general-mission-control/expansion/CONTEXT-STATE-AND-CONTROL.md`;
  `../mission-control-general/general-mission-control/RUNTIME-CONTRACTS.md` (checkpoint
  manifest paragraph).
- ADR: [0007](../adr/0007-authoritative-state-separate-from-advisory-memory.md).
- Code: [checkpoint lineage](../../src/mission_control/domain/execution/checkpoint_lineage.py),
  [graph runtime definitions](../../src/mission_control/domain/graph_runtime/definitions.py),
  [graph runtime contracts](../../src/mission_control/domain/graph_runtime/contracts.py),
  [graph runtime identities](../../src/mission_control/domain/graph_runtime/identities.py),
  [policy contracts](../../src/mission_control/domain/policies/contracts.py),
  [Deep Agents classification](../../src/mission_control/adapters/deep_agents/adapter.py),
  [state reducers in the materializer](../../src/mission_control/adapters/deep_agents/materializer.py).
- Tests: [checkpoint lineage contract](../../tests/unit/operations/test_checkpoint_lineage.py),
  [recovery classification](../../tests/unit/operations/test_checkpoint_recovery_classification.py),
  [goal-directed recovery](../../tests/unit/operations/test_rrm_016_goal_directed_recovery.py).
