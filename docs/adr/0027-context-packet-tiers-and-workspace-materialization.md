---
type: Decision Record
title: "Context moves between stages, iterations, sessions and missions as one sealed Context Packet with four expansion tiers, built by a deterministic packer and materialized into the workspace"
description: "Every attempt starts from a mc.context_packet.v1: inline items (bytes in the prompt under the model profile budget), references (identity, digest, summary and retrieval instruction), materialized files under /inputs and .mission/, and an optional workspace snapshot restore; the packer is deterministic over accepted outputs, loop state, journal digests, checkpoints and selected catalog context, and the packet is the single thing a stage, iteration, continuation or chain link hands over."
tags: [mission-control, adr, decision, context]
status: accepted
source: fast-track interview 2026-10-07 (requirement 2a); expansion/CONTEXT-STATE-AND-CONTROL.md (stage handoff, context selection and budgets); workflow-types/08; ADR-0007; docs/specs/fast-track-2026-10/research/codebase-map.md (frozen_input_refs never reach the agent; handoff delivered as str(dict))
---

# Context moves between stages, iterations, sessions and missions as one sealed Context Packet with four expansion tiers, built by a deterministic packer and materialized into the workspace

The specification already fixes context selection records and the stage handoff algorithm, but the code hands a stage nothing and hands a Goal Loop iteration a stringified dictionary. We make the Context Packet the one artifact every handoff produces and every attempt consumes. The packer takes the consumer's declared input bindings, the producer's accepted outputs (or the prior iteration's loop state and journal digest, or a continuation checkpoint, or a chain link's supplied outputs), the node's selected catalog context and the model profile's budget, and assigns each item an expansion tier: `inline` when the item is mandatory or small enough to fit the remaining budget, `reference` otherwise (with digest, size, a bounded summary and the exact command or path to fetch it), `materialize` when the binding asks for a file (placed read-only under `/inputs/<binding>` with the manifest in `.mission/inputs.json`), and `workspace` when a continuation or fork restores a snapshot. The packet is sealed with a digest, recorded as `mc.context_selection.v1`, and rendered into `.mission/context.md` (index) plus the prompt's `admitted_input` segment; the same packet renders on every lane. Agents read packets; they never write them. We rejected "expand everything into the prompt" because budgets and provider windows differ per lane, and rejected "references only" because small typed outputs lose nothing by inlining and cost a tool call otherwise.

## Consequences

- `StageGraphOperationPreparationService` and the GoalDirected prompt builder stop composing prompts themselves and call the packer.
- Each input binding may declare `expand: inline|reference|materialize|auto`; `auto` is the packer's budget rule.
- The packet is the hydration input of a fresh session on every lane, which is what makes Cursor forks and continuations possible (ADR-0030).
