---
type: Specification
title: "SPEC-02: Context Packet, Context Packer, stage and iteration handoff, continuation checkpoint"
description: "Specifies mc.context_packet.v1 and the deterministic Context Packer that builds it from accepted outputs, Loop State, journal digests, continuation checkpoints, chain-link supplies and selected catalog context under a model profile budget; the four Expansion Tiers; the renderers that put a packet into .mission/, /inputs, the prompt and each lane's backend; the Stage Graph and Goal Loop handoff fixes; mc.continuation_checkpoint.v1; compaction observation; and hydration of a fresh session. Elaborates ADR-0027."
tags: [mission-control, spec, fast-track, context]
---

# SPEC-02: Context Packet, Context Packer, stage and iteration handoff, continuation checkpoint

Elaborates [ADR-0027](../../adr/0027-context-packet-tiers-and-workspace-materialization.md). Owner requirement 2a (fast-track interview 2026-10-07): "formalize and implement mission control's responsibility of passing context in the Stage Graph from depended-on stage to dependent stage and in the goal directed as the iterations progress, with a smart strategy for fully expanding things in the context of the llm, putting references to artifacts, files (maybe workspaces) to the llm and materializing those in the sandbox."

Normative inputs: `expansion/CONTEXT-STATE-AND-CONTROL.md` (stage handoff algorithm, context selection and budgets, `mc.context_selection.v1`, `mc.state_handoff.v1`), `workflow-types/08` (Continuation Checkpoint contract, transfer materialization), `workflow-types/01` section 3 (Data Binding), `workflow-types/02` section 4 (Loop State, Journal segments), ADR-0007. Code facts: [research/codebase-map.md](research/codebase-map.md) section 3 and gap (c); [research/deepagents-middleware.md](research/deepagents-middleware.md) sections 2.3, 3, 4.

## Problem Statement

A Stage Graph consumer stage today receives nothing from the stages it depends on: `StageGraphInterpreter._input_refs` computes `frozen_input_refs` as a flat tuple of strings and `StageGraphOperationPreparationService.materialize` never reads it, so the agent in `synthesize` cannot see what `collect` produced unless a human copies it into the objective text. A Goal Loop iteration receives its predecessor's handoff as `str(dict)` inside one untrusted prompt segment, with no budget, no digest, no materialized files and no way for a second lane to render it. Continuation after compaction has a specified checkpoint contract but no implementation, and the Deep Agents summarization middleware compacts silently. There is no single object an operator can inspect to answer "what did this attempt get to read?", and nothing that a Cursor agent, which has no system prompt API, could consume as files.

## Solution

One sealed, budgeted object, the **Context Packet** (`mc.context_packet.v1`), is produced by a deterministic **Context Packer** at every handoff and consumed by every attempt on every lane. The packer decides, item by item, how the agent receives it: an **Expansion Tier** of `inline` (bytes in the prompt), `reference` (identity, digest, size, summary and the exact way to fetch it), `materialize` (a read-only file in the workspace) or `workspace` (a restored snapshot). The packet is rendered identically on every lane into `.mission/context.md`, `.mission/inputs.json`, `/inputs/<binding>/...`, one `admitted_input` prompt segment and the lane's backend seeding call. The same packer serves four handoffs: stage to stage (from accepted producer outputs), iteration to iteration (from bounded Loop State and the journal digest), session to session (from a Continuation Checkpoint), and mission to mission across a Chain Link (SPEC-04). The packet is the only thing the agent reads about prior work; agents never write packets. Every packet is recorded as the `mc.context_selection.v1` the specification already requires, and a Continuation Checkpoint is a packet plus typed state sealed together.

## User Stories

1. As a mission author, I want a stage's declared input bindings to be the only thing I write, so that the consumer automatically receives the producer's accepted outputs without me pasting artifact ids into prompts.
2. As a mission author, I want to say `expand: inline` for a small typed summary and `expand: materialize` for a large source manifest, so that the agent reads small facts in the prompt and opens large files from `/inputs`.
3. As a mission author, I want `expand: auto` to choose for me under the model's budget, so that I do not have to know token counts.
4. As an agent in a Stage Graph stage, I want `.mission/context.md` to list every input with its path, digest, size and a one-paragraph summary, so that I know what I have before I start.
5. As an agent in a Goal Loop iteration, I want the previous iteration's Progress Review, unresolved blockers, human answers and accepted artifacts in my packet, so that I continue rather than restart.
6. As an agent on Cursor, which has no system prompt, I want the packet materialized as files in my workspace, so that I receive the same context a Deep Agents attempt receives.
7. As an operator, I want to open a run's packet by id and see what was inlined, referenced, materialized and omitted and why, so that I can audit what the model actually saw.
8. As an operator, I want mandatory items that exceed the budget to block with `CONTEXT_BUDGET_EXCEEDED` rather than be silently dropped, so that a mission never runs on a truncated contract.
9. As an operator, I want an item that was omitted for budget to appear as a reference with retrieval instructions, so that the agent can still fetch it.
10. As Mission Control, I want the packet digest to be deterministic over its inputs, so that a replayed admission produces the same packet and a changed input produces a different digest.
11. As Mission Control, I want the packet to carry no secrets and no raw transcript text, so that it can be stored, forked and shown.
12. As a reducer, I want the producer's accepted outputs, not its provider completion, to be the packer's input, so that a consumer never reads an unaccepted artifact.
13. As a Goal Loop, I want the journal digest and journal head referenced, not embedded, so that the packet stays bounded while lineage stays inspectable.
14. As a continuation, I want a Continuation Checkpoint sealed from the packet plus typed state, so that a fresh session hydrates from one validated object.
15. As a continuation on Deep Agents, I want the summarization event observed and recorded as `before_compaction` and `after_compaction` frames, so that provider compaction is visible in mission state.
16. As a continuation on Cursor, I want the provider's summary frames observed the same way, so that both lanes report compaction identically.
17. As an operator, I want `request_continuation` to force compaction and transfer now, so that I can move a degraded session without cancelling it.
18. As a fork, I want the `workspace` tier to restore the snapshot's files into the new workspace, so that the branch starts from the same bytes.
19. As a chain, I want the supplying mission's accepted outputs, final checkpoint reference and journal digest packed for the consuming mission, so that state crosses missions without shared storage.
20. As a verifier role in a Goal Loop, I want an independent packet containing the executor's outputs but not its reasoning, so that independence is preserved.
21. As a Deep Agents attempt, I want materialized files seeded through the backend before the first model call, so that the agent's `ls /inputs` works on the first turn.
22. As a Deep Agents attempt on `StateBackend`, I want the files seeded in the input dictionary, so that the state-only backend works without a sandbox.
23. As an implementer, I want the packer to be a pure function in the domain layer with no I/O, so that it is unit-testable with fixtures.
24. As an implementer, I want byte retrieval, token counting and file writes to be ports, so that the packer never touches storage.
25. As a producer, I want my promoted artifacts to appear in `RuntimeResult.output_refs`, so that the interpreter's dependency projections carry real artifact references.
26. As an operator, I want a packet's `omitted` list to name reason codes, so that "why didn't the agent see X" has a typed answer.
27. As an application owner, I want the Biotech schema context asset to enter the packet as `reference` with a tool-call instruction, so that large domain context is fetched, not inlined.
28. As a Cursor Cloud attempt, I want the packet index complete on disk before the first send, so that the missing `sessionStart` hook does not leave me blind.

## Contracts

### `mc.context_packet.v1`

```text
ContextPacket@1 {
  schema_version: "mc.context_packet.v1"
  packet_id                         // UUIDv7
  scope: {installation_id, application_id, tenant_id}
  target: {
    mission_id, run_id, revision_id, node_key,
    activation_id, attempt_no, generation,
    purpose: stage_start | iteration_start | continuation | fork | chain_link | follow_up_turn
  }
  producer_refs[]                   // activation ids, iteration ids, checkpoint ids or chain link ids the packet derives from
  budget: {
    model_profile_ref, tokenizer_ref,
    context_window, reserved_output, fixed_overhead, control_reserve, safety_margin,
    available_input,                // computed, see Budget arithmetic
    inline_allocated, inline_remaining,
    counting: exact | conservative_bound
  }
  items[]: ContextItem@1            // deterministic order, see Ordering
  omitted[]: OmittedItem@1
  mandatory_item_ids[]              // subset of items[].item_id
  workspace_snapshot_ref?           // when any item has tier workspace
  packet_digest                     // sha256 over canonical serialization excluding packet_id and sealed_at
  sealed_at
  context_selection_ref             // the mc.context_selection.v1 record this packet is the plan of
}

ContextItem@1 {
  item_id                           // stable: sha256(source_kind, source_ref, binding_name)
  binding_name?                     // consumer input binding this satisfies
  source_kind: accepted_output | loop_state | journal_digest | progress_review | human_answer
             | blocker | continuation_checkpoint | chain_supply | catalog_context | operating_contract
             | goals_and_criteria | pending_commitments | budget_remaining | workspace_map | queued_instruction
  source_ref                        // artifact://, journal://, checkpoint://, catalog://, command:// or state://
  content_digest, bytes, media_type, schema_ref?
  trust: authoritative | admitted_input | untrusted_content
  tier: inline | reference | materialize | workspace
  mandatory: bool
  inline: { text, tokens }?                        // present when tier = inline
  reference: { summary, tokens, retrieval: RetrievalInstruction@1 }?   // present when tier = reference
  materialize: { path, mode: read_only, manifest_entry_ref }?         // present when tier = materialize
  workspace: { snapshot_ref, restore_paths[] }?                        // present when tier = workspace
  provenance: { producer_activation_id?, producer_generation?, accepted_decision_ref?, iteration_id?, chain_link_id? }
}

RetrievalInstruction@1 {
  kind: read_file | tool_call | missionctl | mcp_resource
  path? | tool: {name, args_digest, args_excerpt}? | command? | resource_uri?
  range_hint?                       // e.g. "lines 1-400" or "section 'Results'"
}

OmittedItem@1 {
  item_id, source_kind, source_ref, content_digest, bytes
  reason: budget_exhausted | trust_filtered | expired | duplicate | not_accepted | unsupported_media | policy_denied
  downgraded_to: reference | none
}
```

Invariants:

- An item with `mandatory = true` is always `inline` or `materialize`; it is never `reference` for budget reasons. If mandatory inline items exceed `available_input`, packing fails with `CONTEXT_BUDGET_EXCEEDED`.
- `trust` is the content's trust, not the tier's. `untrusted_content` items are rendered inside a fenced block labelled as data, never as instructions (CONTEXT-STATE-AND-CONTROL: "retrieved content is data with provenance, never instructions").
- `packet_digest` is computed over the canonical JSON (sorted keys, UTF-8, no whitespace) of every field except `packet_id`, `sealed_at` and `context_selection_ref`. Two packs over identical inputs and the same packer version produce the same digest.
- A packet never contains secret references' values, provider credentials, raw provider frames or chain-of-thought.
- A packet is immutable once sealed; a correction is a new packet with the old `packet_id` in `producer_refs`.

### `mc.context_selection.v1` linkage

The specification's `mc.context_selection.v1` record (request id, purpose, target, policy and budget refs, candidate capture refs, selected item refs and digests, omitted reason codes, trust labels, deterministic ordering, actual prompt and file plan) is written by the same transaction that seals the packet. Its `prompt_plan` is the rendered `admitted_input` segment digest; its `file_plan` is the materialization manifest digest; its `selected` is `items[]`; its `omitted` is `omitted[]`. The packet is the selection's plan, the selection is the packet's authority record. The ledger table is `mission_control.context_selection` (already specified; see Persistence).

### `mc.continuation_checkpoint.v1`

Fields follow workflow-types/08 section 6 exactly; the packet carries the context parts.

```text
ContinuationCheckpoint@1 {
  schema_version: "mc.continuation_checkpoint.v1"
  checkpoint_id, scope
  identities: { mission_id, run_id, revision_id, node_key, activation_id, logical_execution_id, source_agent_session_ref }
  goals_and_criteria: { goal_refs[], objective_refs[], criterion_refs[], acceptance_state }
  decisions[]: { decision_ref, rationale_summary }
  artifact_refs: { inputs[], outputs[] }
  workspace_snapshot_ref, sandbox_snapshot_ref?
  work: { completed[], active[], pending[], blocked[] }
  verification_dispositions[]
  unresolved: { questions[], human_task_refs[] }
  queued_commands[]                 // command ids held in the mailbox at seal time, never their delivery
  event_cursor                      // mission seq at seal
  budgets_remaining, governors_remaining
  versions: { capability_pins[], model_profile_ref, lane_profile, packer_version }
  invariants[]
  recommended_next_actions[]
  typed_state: { schema_ref, state_version, state_digest, state_ref }   // Loop State or stage state, by reference
  context_packet_ref                // the packet a fresh session hydrates from (purpose = continuation)
  compactor: { kind: deterministic | admitted_agent, ref }
  validator: { kind, ref, result: valid | invalid, reasons[] }
  author, authored_at, checkpoint_digest
  supersedes?                       // prior checkpoint id when a human correction created this one
}
```

A checkpoint with `validator.result = invalid` is never the source of a fresh session (08 section 8).

## Implementation Decisions

### Budget arithmetic

Per CONTEXT-STATE-AND-CONTROL "Context selection and budgets": the model profile declares `context_window`, `tokenizer_ref`, `max_output`, `tool_schema_allowance`, `control_reserve`, `max_retrieved_content`, `summarization_budget` and `safety_margin`.

```text
available_input = context_window - reserved_output - fixed_overhead - control_reserve - safety_margin
fixed_overhead  = system_prompt_tokens + tool_schema_allowance + skills_metadata_tokens
```

All terms must be non-negative; a negative result is a profile validation error at compile time, not a runtime surprise. Token counts come through a `TokenCounter` port keyed by `tokenizer_ref`; when the tokenizer is unknown the counter returns a conservative bound (`bytes / 2.5`, rounded up) and the packet records `counting: conservative_bound`. Never assume zero.

### Packer algorithm

The Context Packer is a pure function `pack(request: PackRequest) -> ContextPacket | PackFailure` in `domain/context/packet.py`. `PackRequest` holds already-captured candidates (identity, digest, bytes, media type, trust, optional pre-computed summary, optional pre-fetched text), the consumer's bindings, the budget, and the policy; capture (fetching bytes, computing summaries) is done beforehand by the application service through ports, so the packer performs no I/O and no model judgment. Stochastic summaries, when a policy asks for them, are captured candidate artifacts with their own digests before packing.

Steps, in order (mirrors the specification's selection steps):

1. **Authorize candidates.** Drop any candidate whose grant snapshot does not cover the consumer's scope; record `policy_denied`.
2. **Accept-filter.** For `accepted_output` and `chain_supply`, keep only outputs whose producer activation (or supplying goal) has an accepted Completion Decision; record `not_accepted` otherwise. Provisional bindings keep the item but mark `provenance.provisional = true` and the item can never be `mandatory`.
3. **Dedupe** by `(canonical source, version, locator)`; record `duplicate`.
4. **Trust and expiry filter**; record `trust_filtered` or `expired`.
5. **Rank deterministically.** Order key: mandatory first; then by `source_kind` priority (`operating_contract`, `goals_and_criteria`, `pending_commitments`, `budget_remaining`, `queued_instruction`, `human_answer`, `blocker`, `progress_review`, `loop_state`, `accepted_output` / `chain_supply` in binding declaration order, `continuation_checkpoint`, `journal_digest`, `catalog_context`, `workspace_map`); then by binding declaration order; then by `content_digest`. No timestamps, no random tie-breaks.
6. **Assign tiers.**
   - A binding with `expand: materialize` becomes `materialize` at `/inputs/<binding_name>/<file name>` (file name from the artifact metadata, sanitized; collisions get a digest suffix). The item also gets an `inline` index line (path, digest, size, summary) counted against the budget.
   - A binding with `expand: inline` becomes `inline` if `tokens <= inline_remaining`; otherwise, if `mandatory`, fail `CONTEXT_BUDGET_EXCEEDED`; otherwise downgrade to `reference` and record `budget_exhausted` with `downgraded_to: reference`.
   - A binding with `expand: reference` becomes `reference` with a `RetrievalInstruction` chosen by media and lane: files already materialized → `read_file`; artifacts → `missionctl artifact get <ref>` or the MCP resource `mc://.../artifacts/<id>`; catalog context → `tool_call` on the bound capability.
   - `expand: auto` (default): `inline` when `tokens <= min(inline_remaining, auto_inline_cap)` (policy default `auto_inline_cap = 4,000` tokens) and media is text; `materialize` when media is a file type or `bytes > auto_materialize_floor` (policy default 64 KiB) and the lane has a writable workspace; otherwise `reference`.
   - `workspace` is assigned only by the application for `purpose = continuation | fork`; the packer validates that exactly one `workspace` item exists then.
7. **Allocate mandatory items first** (`operating_contract`, `goals_and_criteria`, current action scope, `pending_commitments`, `budget_remaining`, `journal_digest` head reference, `workspace_map`), then fill with ranked optional items until `inline_remaining` is exhausted.
8. **Seal**: compute `packet_digest`, build `omitted[]`, emit `ContextPacket`.

The packer version is part of the digest input (`packer_version` in `versions`), so a packer change produces different digests and never masquerades as the same packet.

### Renderers (`domain/context/render.py`)

Pure functions over a sealed packet:

- `render_context_index(packet) -> str` writes `.mission/context.md`: a header with purpose, target, budget summary; a table of every item (binding, tier, path or retrieval, digest, size, trust, summary); the inline items in order under `## Inline`, each in a fenced block whose info string names the source and trust (`data source=artifact://... trust=untrusted_content`); the omitted list with reasons. The file is bounded by the packet's budget because inline content is already budgeted.
- `render_inputs_manifest(packet) -> dict` writes `.mission/inputs.json`: `[{binding_name, path, artifact_ref, content_digest, bytes, media_type, mode: read_only}]` for `materialize` items plus `{snapshot_ref, restore_paths}` for the `workspace` item.
- `render_prompt_segment(packet) -> PromptSegment` returns one segment of trust class `admitted_input` containing the index header and the inline items; the segment digest is the `prompt_plan` digest in the selection record. Untrusted items are wrapped as data blocks inside this segment, never as a separate instruction segment.
- `render_workspace_entries(packet) -> list[WorkspaceSlotBinding]` maps `materialize` items to `WorkspaceSlotBinding(access="read_only", durable_ref, content_digest)` so the existing `DurableInputManifestEntry` path in `WorkspaceMaterializationService._load_and_verify_inputs` fetches and verifies bytes (`<object_ref>#<sha256>:<size>` format via `_DurableInputsFromPayloads.retrieve`).

Lane seeding (application layer, SPEC-07 owns the lane side):

- Deep Agents sandbox or filesystem backend: `backend.upload_files([(path, bytes), ...])` for `/inputs/**` and `.mission/**` before `invoke` (research 3.3, "Seeding the sandbox").
- Deep Agents `StateBackend`: files go in the invocation input `{"files": {path: create_file_data(text)}}`; binary items are rejected by the text-only backend (`unsupported_media`, downgraded to `reference`), matching the existing capability rule.
- Cursor Local and Cloud: the files are written into the leased workspace (local) or committed to the `mc/<run>` branch (cloud) before the first `send`, so the packet index is complete on disk even though cloud fires no `sessionStart` hook.

### Stage handoff (fixes the gap)

1. `StageGraphInterpreter._input_refs` returns a tuple of `StageInputBinding(consumer_input_slot_id, producer_stage_key, producer_output_slot_id, artifact_ref, accepted_decision_ref, provisional)` instead of a flat tuple of strings. `frozen_input_refs` keeps its string form for identity and digest compatibility (sorted `artifact_ref`s) and gains a sibling `frozen_input_bindings` on `StageOperationAdmissionProposal` and `StageInstanceProjection`.
2. `StageGraphOperationPreparationService.materialize` calls `ContextPackService.pack_for_stage(proposal, template, model_profile)`:
   - capture candidates from `frozen_input_bindings` (artifact metadata and bytes through the artifact repository and payload store), the node's selected catalog context (`ContextPolicyDefinition` rules of kind `catalog`, `admitted_input`, `prior_checkpoint_summary`), the Operating Contract and goals from the Compiled Program;
   - pack; on `PackFailure` the admission proposal is rejected with `CONTEXT_BUDGET_EXCEEDED` before any provider work;
   - add `render_workspace_entries` to `WorkspaceContract.read_mounts` / slot bindings, `render_prompt_segment` to the request's prompt segments (replacing the ad hoc `objective_override` segment, which becomes an item of `source_kind: goals_and_criteria`), and the two `.mission/` files as workspace seed entries;
   - persist the packet and the `context_selection` record in the same application transaction as the admission.
3. The existing `ContextAssemblySpec` and `ContextPolicyDefinition` types stay; the packet's `items[]` is the assembly manifest they describe, so `ContextAssemblySpec.entries` is generated from the packet rather than hand-built.
4. On the producer side, `DeepAgentRuntimeAdapter.execute` fills `RuntimeResult.output_refs` from the artifact promotions it performed (the `artifact://{scope}/{run}/{artifact_id}` durable refs), so `StageOperationResult.output_refs` and the interpreter's `DependencyProjection.evidence_refs` carry registered artifacts rather than model-emitted strings. A model-emitted `output_refs` key in `structured_output` is accepted only if every ref resolves to a promotion of this attempt; others are dropped with a `provenance` warning in the settlement.

### Goal Loop iteration handoff

1. `application/programs/goal_directed.py::_prompt_segments` stops serializing `asdict(handoff)` and calls `ContextPackService.pack_for_iteration(claim, prior_handoff, loop_state, journal_head, role)`.
2. Candidates: bounded Loop State (`inline`, mandatory, from `GoalContinuationState`), the sealed journal head (`reference`, mandatory: `journal://<activation>/<segment_digest>` with `missionctl` retrieval), the prior iteration's Progress Review (`inline`), unresolved blockers and `human_answer` entries (`inline`, mandatory), accepted artifacts from `GoalHandoff.artifact_refs` (`auto`), `workspace_refs` and `snapshot_refs` (as `materialize` of `/goal/HANDOFF.md` and `/goal/checkpoint.json` from the `GoalWorkspaceService` snapshot, keeping those file paths), and the verifier's `verifier_input_refs` for `role = verifier` (`materialize`, with executor reasoning excluded by construction because only registered artifacts are candidates).
3. `_workspace_for` adds the packet's `render_workspace_entries` to the role workspace instead of the single `{role_root}/input` mount; the verifier keeps an independent workspace and packet.
4. `GoalHandoffDraft` content authored by the model remains data: it becomes an `untrusted_content` item, never the packet itself. `GoalHandoffReference` (`checkpoint`, `artifact_ref`, `content_digest`) is populated with the packet's `context_packet_ref` so the schema-only type gains a runtime writer.

### Continuation checkpoint and compaction

1. **Triggers** (08 section 5): context health soft threshold from the model profile's policy, a provider compaction frame, turn count, workflow boundary, operator or coordinator `request_continuation`. Soft threshold schedules; hard threshold blocks further agent work until transfer.
2. **Deep Agents observation.** The stock `SummarizationMiddleware` writes private `_summarization_event {cutoff_index, summary_message, file_path}` and offloads evicted messages to `/conversation_history/{session_id}.md` with no callback (research 2.3). Mission Control ships `ObservedSummarizationMiddleware` in `adapters/deep_agents/compaction.py`: a subclass that keeps the stock `.name` so it replaces the default in `create_deep_agent`'s middleware list, and whose `wrap_model_call` detects a new `_summarization_event` (or whose `wrap_tool_call` catches the `compact_conversation` tool) and emits two Provider Frames through the `FrameSink` (SPEC-03): `before_compaction` (cutoff index, pre-summary token estimate) and `after_compaction` (summary message digest, `file_path`, post-summary token estimate). The offloaded `/conversation_history/<session>.md` is pulled with `download_files` and registered as an artifact of kind `conversation_history` for the archive; it is evidence, never Loop State.
3. **Cursor observation.** `summary-started` / `summary-completed` interaction updates (research: Cursor `InteractionUpdate` kinds) become the same two frame kinds.
4. **Seal.** At the next safe boundary after a trigger, the family workflow runs the `continuation.seal` activity: freeze new agent actions (mailbox holds), snapshot structured state (Loop State or stage state) and workspace (`workspace_snapshot_ref` through the existing `RunSnapshotService.take` at a declared safe boundary), run the deterministic reducer that fills every checkpoint field from ledger facts, optionally run an admitted compacting agent for `decisions[].rationale_summary` and `recommended_next_actions` (its output is an untrusted candidate validated before use), pack a `purpose = continuation` packet whose single `workspace` item restores `/inputs/**`, `/outputs/**` candidates and `.mission/**` and whose inline items are the checkpoint's bounded fields, validate (schema, digests, references, identity, authority, capability bounds, budgets, unresolved gates, workspace consistency), seal, write `session.checkpoint_sealed`.
5. **Failed compaction** follows 08 section 8: park, retry the compactor within `continuation_policy.compactor_retries`, admitted fallback compactor, human review when policy requires, else fail explicitly. A fresh session never starts from an invalid checkpoint.
6. **Transfer.** The lane's `prepare` for the fresh session (SPEC-07) consumes the checkpoint packet: Deep Agents starts a new thread seeded with checkpoint values and the restored files; Cursor creates a new agent in a fresh workspace with the files and the git patch or branch. `session.transferred` is written with source and target session refs; pending mailbox commands are released only after the target reports hydration (a first frame of kind `session_init`).
7. **`request_continuation`** is a command (SPEC-06 owns the command surface) whose boundary delivery sets the trigger; delivery semantics are `turn_boundary_guaranteed` on Deep Agents and `wait_then_send` on Cursor.
8. Governors (08 section 13): `max_transfers`, cumulative tokens, cost and wall-clock, `max_failed_compactions`, `no_progress_transfers`; exhaustion is a governed terminal outcome.

### Hydration from a checkpoint packet

`workspace` tier restore is a list of `(snapshot_ref, restore_paths[])`; the lane restores those paths read-write for `/outputs/**` candidates and read-only for `/inputs/**` and `.mission/**`, then overlays the packet's own `.mission/context.md`, which for a continuation lists the checkpoint fields inline. A continuity check compares restored file digests with the snapshot manifest and fails the transfer on mismatch (`CHECKPOINT_INVALID`).

### Chain link packets

`pack_for_chain_link(link, supplying_run)` (SPEC-04 calls it) uses `chain_supply` candidates: the supplying goal's accepted outputs per the link's typed binding, the supplying run's final checkpoint (`reference`), and its journal digest (`reference`). Nothing else from the supplying mission crosses.

### Worked example: Mission 1, `collect → synthesize`

`collect` (Goal Loop, lane `deep_agents`) accepts with outputs `sources: source_manifest@1` (an artifact of 210 KiB JSON listing 180 PubMed records with abstracts) and `coverage_review: progress_review@1` (2.1 KiB). The stage `synthesize` declares:

```yaml
inputs:
  - { from: collect.sources, as: sources, expand: materialize }
  - { from: collect.coverage_review, as: coverage, expand: inline }
  - { context: biotech.schema_context, search: "muscle aging NAD", expand: reference }
```

Model profile `frontier.long_context`: window 400,000, reserved output 16,000, fixed overhead 11,500 (system prompt 3,200 + tool schemas 7,100 + skills metadata 1,200), control reserve 8,000, safety margin 20,000, so `available_input = 344,500`.

Packer output (`purpose = stage_start`):

| item | source | tier | tokens counted | result |
| --- | --- | --- | --- | --- |
| operating contract, goals and criteria for `synthesize` | compiled program | inline, mandatory | 1,850 | inlined |
| pending commitments (human gate `review` downstream) | ledger | inline, mandatory | 60 | inlined |
| budget remaining (usd 14.20 of 25, tokens 1.3M of 2M) | ledger | inline, mandatory | 40 | inlined |
| `coverage` | `collect.coverage_review` | inline | 620 | inlined, trust `admitted_input` |
| `sources` | `collect.sources` | materialize | 90 (index line) | `/inputs/sources/source_manifest.json`, read-only, digest verified |
| journal head of `collect` | `journal://.../seg-07#sha256:…` | reference, mandatory | 70 | `missionctl journal read <ref>` |
| biotech schema context | `catalog://biotech.schema_context@1.3.0` | reference | 110 | tool call `biotech.schema_context.select(args_digest …)` |
| workspace map | derived | inline, mandatory | 120 | inlined |

`inline_allocated = 2,960`, no omissions. `.mission/context.md` lists the eight rows; `.mission/inputs.json` has one entry; the prompt gains one `admitted_input` segment of about 3,000 tokens; `WorkspaceContract.read_mounts` gains the `sources` entry; the Deep Agents sandbox receives three files through `upload_files` before the first model call. The `context_selection` row and the packet are committed with the admission proposal. Had `coverage` been 300 KiB and `mandatory`, packing would have failed with `CONTEXT_BUDGET_EXCEEDED` naming the item; had it been optional, it would have become a `reference` with `read_file` retrieval after a `materialize` downgrade.

## Persistence

No new migration is owned by this spec; it uses existing and SPEC-03 tables:

- `mission_control.context_selection` (specified; verify it exists in `mig/0003`/`0005`; if absent, T2 adds it in `0027_provider_frames.sql` as `context_selection(selection_id, scope…, run_id, activation_id, attempt_no, generation, purpose, packet_digest, packet jsonb, prompt_plan_digest, file_plan_digest, sealed_at)` with RLS). The packet body is stored as `jsonb` without inline bytes above 64 KiB (those become artifact references); `packet_digest` is unique per `(run_id, activation_id, attempt_no, generation, purpose)`.
- `mission_control.continuation_checkpoint(checkpoint_id, scope…, run_id, activation_id, logical_execution_id, checkpoint jsonb, checkpoint_digest, validator_result, supersedes, sealed_at)` — add in `0027` if not already present from the common component (the `checkpoint_transition` table in `mig/0012` is runtime lineage, not this manifest).
- Artifacts, workspace manifests and `DurableInputManifestEntry` rows are reused unchanged.

## Interfaces

- `missionctl run context RUN_ID --activation ID [--attempt N] --json` and `GET /v1/applications/{app}/runs/{run_id}/activations/{activation_id}/context` return the sealed packet (inline bodies redacted to digests above 4 KiB unless `--full`).
- `missionctl run checkpoint RUN_ID --list|--get ID` and `GET .../runs/{run_id}/checkpoints[/{id}]` return continuation checkpoints.
- `missionctl command send RUN_ID` with `kind: request_continuation` (SPEC-06).
- MCP resource `mc://applications/{app}/runs/{run_id}/activations/{activation_id}/context`.
- Manifest surface (SPEC-05): input binding fields `expand: inline|reference|materialize|auto`, node `context:` entries with `search:` or `pin:` and `expand`.

## Insertion points

| Change | Path |
| --- | --- |
| Packer, tiers, budget, ordering, failure | new `src/mission_control/domain/context/packet.py` (pure) |
| Renderers | new `src/mission_control/domain/context/render.py` |
| Checkpoint contract and validator | new `src/mission_control/domain/context/checkpoint.py` |
| Capture through ports, persistence, pack_for_* entry points | new `src/mission_control/application/context/pack_service.py` (ports: `ArtifactBytesPort`, `TokenCounterPort`, `ContextSelectionRepository`, `CheckpointRepository`) |
| Slot mapping kept | `src/mission_control/domain/programs/interpreter.py::_input_refs`; `StageOperationAdmissionProposal.frozen_input_bindings`, `StageInstanceProjection.frozen_input_bindings` in `domain/programs/contracts.py` |
| Stage preparation uses the packet | `src/mission_control/application/programs/service.py::StageGraphOperationPreparationService.materialize` |
| Iteration prompt and workspace use the packet | `src/mission_control/application/programs/goal_directed.py::_prompt_segments`, `::_workspace_for`, `::_bind_handoff` (fills `GoalHandoffReference`) |
| Producer output refs | `src/mission_control/adapters/deep_agents/adapter.py::execute` (`RuntimeResult.output_refs`) |
| Compaction observation | new `src/mission_control/adapters/deep_agents/compaction.py::ObservedSummarizationMiddleware`; materializer middleware list in `materializer.py` |
| Backend seeding | `adapters/deep_agents/materializer.py::prepare` (`upload_files` or `files` input) |
| Continuation seal activity | new `src/mission_control/adapters/temporal/activities/continuation.py`; family workflows call it at boundaries |
| Read endpoints and CLI | `interfaces/http/mission_control.py` (context, checkpoints), `interfaces/cli/main.py` (`run context`, `run checkpoint`) |
| JSON Schema export | `src/mission_control/contracts/` alongside `mc.command.v1` |

## Testing Decisions

Tests assert external behaviour (packet content, files on the backend, prompt segment, ledger rows), not packer internals.

- Unit (`tests/unit/context/`): golden packets from fixture candidates covering every tier, every omission reason, mandatory overflow (`CONTEXT_BUDGET_EXCEEDED`), deterministic ordering (shuffled input yields identical digest), conservative counting, `auto` thresholds at the boundaries, untrusted items rendered as data blocks, packer version changing the digest. Renderer goldens for `.mission/context.md` and `inputs.json`.
- Unit (`tests/unit/operations/`): checkpoint validator accepts a complete checkpoint and rejects each missing field, digest mismatch, unresolved gate and authority widening; a corrupted summary cannot erase `queued_commands`, `budgets_remaining` or `unresolved.human_task_refs` (CONTEXT-STATE-AND-CONTROL mandatory test).
- Integration (`tests/integration/postgres/`, `common_db`): a two-stage Stage Graph where stage B's packet contains A's accepted artifact as `materialize`; the `DurableInputManifestEntry` is fetched and digest-verified; the `context_selection` row exists with matching digests; an unaccepted producer output is omitted with `not_accepted`. Prior art: `tests/acceptance/mission_control/test_postgres_runtime_parity.py` (source and fork execution).
- Integration (`tests/integration/deep_agents/`): with `StateBackend`, the files appear in the invocation input and `ls /inputs` lists them; with the Docker sandbox, `upload_files` places them before the first model call (local model fixture, as in the GoalDirected parity test).
- Integration: GoalDirected two iterations where iteration 2's packet includes iteration 1's Progress Review inline and the journal head as reference, and the verifier packet contains no executor prompt text. Prior art: `tests/unit/operations/test_rrm_016_goal_directed_recovery.py`.
- Integration: compaction observation emits `before_compaction` and `after_compaction` frames (asserted through SPEC-03's frame repository) when a summarization is forced with `trigger=("messages", 3)`; a `request_continuation` command seals a checkpoint and a fresh thread hydrates with the restored files; `session.transferred` is in the ledger.
- Acceptance: Mission 1 manifest `collect → synthesize` (I1) demonstrates the worked example end to end on the real local stack.

## Tickets

| Ticket | Scope |
| --- | --- |
| [FT-B1](issues/B1-context-packet-contract-and-packer.md) | contract, packer, renderers, JSON Schema, unit goldens |
| [FT-B2](issues/B2-stage-handoff-delivers-packet.md) | slot mapping, stage preparation, read mounts, `output_refs`, integration test |
| [FT-B3](issues/B3-goal-loop-iteration-packet.md) | iteration packet, verifier independence, `GoalHandoffReference` writer |
| [FT-B4](issues/B4-continuation-checkpoint-and-compaction.md) | checkpoint contract and validator, compaction observation, seal activity, hydration, `request_continuation` |

## Out of Scope

Automatic context health policy learning (policies are catalog records with conservative defaults); stochastic summarizers as packer internals (they are captured candidates); memory recall (`mc.memory_recall.v1`) beyond passing admitted knowledge as `catalog_context`; Parallel Swarm member projections and Evaluator Optimizer independent packets (same packer, specified when those systems are built); provider prompt-cache prefix engineering (08 section 12, an optimization later); Cursor-side file writing mechanics (SPEC-07).

## Further Notes

- The packer deliberately does not know lanes; the lane consumes `render_*` outputs. The only lane-aware rule is `materialize` requiring a writable workspace, which the application passes as a capability flag of the lane profile.
- `frozen_input_refs` stays a sorted tuple of refs so existing digests, snapshots and fork reuse frontiers are unchanged; `frozen_input_bindings` is additive.
- The packet is the natural unit for "add_context" (SPEC-06): a queued context item is a `queued_instruction` candidate of the next packet.
- UNVERIFIED (research): whether `update_state` can seed `StateBackend` files on an existing thread; whether `CompositeBackend.upload_files` routes correctly to every child. B2 records the answer in its handoff.

# Citations

- [ADR-0027](../../adr/0027-context-packet-tiers-and-workspace-materialization.md), [ADR-0007](../../adr/0007-authoritative-state-separate-from-advisory-memory.md), [ADR-0028](../../adr/0028-native-event-store-provider-frames-and-materialized-transcript.md), [ADR-0030](../../adr/0030-cursor-lane-two-profiles-reducer-from-frames-hydrated-fork.md).
- `../../../mission-control-general/general-mission-control/expansion/CONTEXT-STATE-AND-CONTROL.md` (stage handoff algorithm, context selection and budgets, exact middleware placement).
- `../../../mission-control-general/workflow-types/08-CONTINUATION_COMPACTION_AND_TRANSFER.md` sections 5 to 13; `01-STAGE_GRAPH.md` section 3; `02-GOAL_LOOP.md` section 4.
- [research/codebase-map.md](research/codebase-map.md) section 3 and gap (c); [research/deepagents-middleware.md](research/deepagents-middleware.md) sections 2.3, 3, 4 and implications 2, 6, 9, 10.
- Code: `src/mission_control/domain/programs/interpreter.py`, `application/programs/service.py`, `application/programs/goal_directed.py`, `domain/graph_runtime/definitions.py`, `adapters/deep_agents/materializer.py`, `application/workspaces/workspace_materialization.py`.
- `docs/knowledge/context-and-continuation.md`.
