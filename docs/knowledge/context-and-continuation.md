---
type: Concept
title: Context, state handoff and continuation
description: The sealed Context Packet and deterministic packer, the stage, iteration, chain and queued-instruction handoffs that use it, the context selection record, checkpoint lineage and in-doubt classification, and the continuation checkpoint and service with their current limitation (sealing and transfer are built but not composed or called by a family workflow).
tags: [mission-control, context, checkpoints, continuation, compaction, implementation]
---

# Context, state handoff and continuation

ADR-0007 separates authoritative state (journal, typed state, command frontier, effect
receipts, acceptance) from advisory memory and from runtime graph checkpoints, which
only recover graph execution. Its consequences are the subjects here:
[Context Packet](../../GLOSSARY.md), [Context Selection](../../GLOSSARY.md),
[State Handoff](../../GLOSSARY.md), [Checkpoint](../../GLOSSARY.md) and
[Checkpoint Lineage](../../GLOSSARY.md), [Continuation](../../GLOSSARY.md),
[Compaction](../../GLOSSARY.md) and [In Doubt](../../GLOSSARY.md) units. Snapshots and
forks are in [recovery](recovery.md).

## Context Packet

Every handoff (stage to stage, iteration to iteration, session to session, mission to
mission, a queued instruction, a fork) produces one sealed `mc.context_packet.v1` and every
attempt consumes one (ADR-0027, SPEC-02). `domain/context/packet.py::pack` is a pure
function: no I/O, no clock, no randomness. Capture (fetching bytes, resolving durable refs,
choosing the tokenizer of the model profile) happens beforehand in
`application/context/pack_service.py::ContextPackService`, through ports.

Items have a source kind (`accepted_output`, `loop_state`, `journal_digest`,
`progress_review`, `human_answer`, `blocker`, `continuation_checkpoint`, `chain_supply`,
`catalog_context`, `operating_contract`, `goals_and_criteria`, `pending_commitments`,
`budget_remaining`, `workspace_map`, `queued_instruction`), a trust label (`authoritative`,
`admitted_input`, `untrusted_content`) and an [Expansion Tier](../../GLOSSARY.md): `inline`,
`reference` (identity, summary and how to fetch), `materialize` (a file in the workspace) or
`workspace` (a restored snapshot, exactly one for the `continuation` and `fork` purposes).
The purposes are `stage_start`, `iteration_start`, `continuation`, `fork`, `chain_link` and
`follow_up_turn`. Ranking is deterministic by source-kind priority; `accepted_output` and
`chain_supply` are acceptance-gated; available input is the window minus output, overhead,
control reserve and margin; a mandatory item that does not fit blocks with
`CONTEXT_BUDGET_EXCEEDED`, a filtered one with `CONTEXT_MANDATORY_ITEM_UNAVAILABLE`, and
nothing is truncated; an unknown tokenizer yields a conservative bound. Retrieved content is
data with provenance, never instructions.

`domain/context/render.py` renders the same packet identically on every lane:
`.mission/context.md`, `.mission/inputs.json`, the single `admitted_input` prompt segment and
the read-only workspace entries, and builds the `mc.context_selection.v1` record whose
`prompt_plan_digest` and `file_plan_digest` are the digests of those renderings. Both schemas
are exported under `contracts/schemas/` (`contracts/context_packet.py`).
`mission_control.context_selection` (migration 0027,
`adapters/postgres/context/selection_repository.py`) holds one immutable row per
`(run, activation, attempt, generation, purpose)`: a replay with the same digest returns the
stored packet and a different digest is an idempotency conflict. Rendered files are staged in
the artifact payload store (`adapters/storage/context_files.py`) and reach the workspace
through the digest-verified durable input path; producer outputs are captured from custody
records (`adapters/postgres/context/artifact_bytes.py`: `workspace-candidate://` and
`artifact://` refs only; a model-emitted string is not a registered output).

## Where a packet is used

- **Stage handoff** (FT-B2): `application/programs/service.py` calls `pack_for_stage` before a
  Stage Graph operation is admitted, so a downstream stage sees accepted outputs of upstream
  stages as `/inputs/...` files with digests.
- **Iteration handoff** (FT-B3): `application/programs/goal_directed.py` calls
  `pack_for_iteration`, replacing the stringified handoff between Goal Loop iterations.
- **Queued instructions and injects** (FT-F1, FT-F2): mailbox entries claimed at a boundary
  become mandatory `queued_instruction` items; an `interrupt_and_inject` seals a
  `follow_up_turn` packet ([events and commands](events-and-commands.md)).
- **Chain link** (FT-D2): the consumer's first packet carries the supplied outputs
  ([mission chains](mission-chains.md)).
- **Fork** (FT-F4): `application/recovery/fork_seed.py` writes the optional instruction and a
  kernel `add_context` with `expand: workspace` into the derived run's own mailbox, which the
  first boundary seals into a `fork` packet ([recovery](recovery.md)).

The production worker composes the packer (`adapters/temporal/deployment_composition.py`).
Proof: unit (`tests/unit/context/`, `tests/unit/operations/test_ft_b2_stage_handoff.py`,
`tests/unit/orchestration/test_ft_b3_goal_iteration_packet.py`), `common_db`
(`tests/integration/postgres/test_ft_b2_context_handoff.py`) and a Deep Agents seeding test
(`tests/integration/deep_agents/test_ft_b2_packet_seeding.py`). No packet has fed a paid
model run: no live mission has run.

## Context policy definitions

Earlier and separate from the packet: `ContextPolicyDefinition` (per-source-kind rules with
authority labels and caps; never scientific, approval, budget or terminality authority) and
`ContextAssemblySpec` in `domain/graph_runtime/definitions.py`, and `SubagentContextSlice` and
`ContextReconstructionResult` in `domain/graph_runtime/contracts.py`. The packet does not
produce them.

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
`_merge_by_key` reducers. The Context Packet carries references and accepted outputs between
activations; a `state_handoff` record with the base-version digest check as specified was
not located.

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
in-doubt unit is reconciled, never duplicated. The lane turn service keeps the rule for
Session Lanes: a lost native turn makes the unit `in_doubt`
([lanes and harness](lanes-and-harness.md)).

## Continuation and compaction

Continuation seals a validated `mc.continuation_checkpoint.v1` (a `purpose = continuation`
packet plus typed state) at a safe boundary and transfers it into a fresh session through a lane
hydrator; compaction is observed on Deep Agents and governed by retry, fallback, human-review and
count, cost and no-progress policies. Detail is in
[continuation checkpoint](continuation-checkpoint.md).

**Current limitation.** `ContinuationService` is not composed in the API or the worker, the
`continuation.seal`, `continuation.transfer` and `continuation.release` activities are registered
on no worker queue, and no family workflow calls them. A `request_continuation` command is
recorded as `session.continuation_requested`, but no worker seals a checkpoint or provisions the
fresh session. The canonical checkpoint manifest of RUNTIME-CONTRACTS.md remains a specification;
runtime (LangGraph) checkpoints are evidence only.

# Citations

- Spec: [SPEC-02](../specs/fast-track-2026-10/SPEC-02-context-packet.md);
  `../mission-control-general/workflow-types/08-CONTINUATION_COMPACTION_AND_TRANSFER.md`;
  `../mission-control-general/general-mission-control/expansion/CONTEXT-STATE-AND-CONTROL.md`;
  `../mission-control-general/general-mission-control/RUNTIME-CONTRACTS.md` (checkpoint
  manifest paragraph).
- ADRs: [0007](../adr/0007-authoritative-state-separate-from-advisory-memory.md),
  [0027](../adr/0027-context-packet-tiers-and-workspace-materialization.md).
- Code: [Context Packet and packer](../../src/mission_control/domain/context/packet.py),
  [pack service](../../src/mission_control/application/context/pack_service.py),
  [checkpoint lineage](../../src/mission_control/domain/execution/checkpoint_lineage.py),
  [graph runtime definitions](../../src/mission_control/domain/graph_runtime/definitions.py),
  [Deep Agents classification](../../src/mission_control/adapters/deep_agents/adapter.py),
  [state reducers in the materializer](../../src/mission_control/adapters/deep_agents/materializer.py).
- Tests: [packer](../../tests/unit/context/test_packer.py),
  [checkpoint lineage contract](../../tests/unit/operations/test_checkpoint_lineage.py),
  [recovery classification](../../tests/unit/operations/test_checkpoint_recovery_classification.py).
