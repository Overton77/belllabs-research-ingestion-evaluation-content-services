---
type: Specification Annex
title: "Context, state transfer, messaging and interventions"
description: Normative target contracts supplement workflow types and runtime contracts. The hard seams here require foundational proofs before enabling remote/paid work.
tags: [mission-control, spec, expansion]
---
# Context, state transfer, messaging and interventions

Normative target contracts supplement [workflow types](../../workflow-types/index.md) and [runtime contracts](../RUNTIME-CONTRACTS.md). The hard seams here require foundational proofs before enabling remote/paid work.

## Closed contract inventory

Every envelope includes schema_version, installation/app/tenant, resource identity, creator/actor, timestamp, digest where immutable and request/causation identity. IDs reference authorized records, not arbitrary URLs. JSON Schema seeds are in [schemas.json](schemas.json); generate full strict Pydantic schemas during MC-P001. No arbitrary callable/eval expression is accepted.

| Contract | Required content and invariant |
| --- | --- |
| `mc.context_selection.v1` | request_id, purpose, target revision/node/attempt, policy/budget/tokenizer refs, candidate capture refs, selected item refs/digests/locators/tokens, omitted item reason codes, source grants snapshot, content trust labels, deterministic ordering, actual prompt/file plan; immutable |
| `mc.state_handoff.v1` | producer run/activation/attempt/generation, base state version/digest, node/revision/input/binding digest, typed delta ref/digest, output refs, journal frontier, effect/child/usage frontiers, target state schema, accepted decision refs; cannot copy live ownership |
| `mc.memory_recall.v1` | scoped memory namespace/query, policy and search snapshot, item issuer/digest/locator/time, trust/admission status, expiry/consent, tokens; advisory recall records provenance but does not grant tools |
| `mc.message.v1` | request_id, conversation/target ID, parent/child scope, target generation, client message ID, sender, kind `instruction/context/progress/question/answer/result`, content artifact/ref, delivery boundary `next_turn/next_iteration/after_checkpoint`, deadline, sequence assigned on admission |
| `mc.progress.v1` | attempt/generation, public status summary, completed/remaining declared work, blocker refs, output refs, counters and usage disposition; never private reasoning transcripts |
| `mc.extension_profile.v1` | pinned framework/middleware version/order, hook names, tool wrapper contracts, state/context schemas, permission/budget guards, stream redaction policy and conformance evidence refs |

## Stage handoff algorithm

1. Producer submits outputs and a typed state delta under its fenced generation. Register artifacts before acceptance. Reducer validates output schemas, required evidence and effect/usage/child liabilities. Provider completion is not an accepted producer.
2. Compute downstream readiness from authored joins/dependencies and acceptance. Build input manifest referencing selected accepted outputs. State mapper is a pinned deterministic transformation with typed source/target schemas, not a model rewriting control state.
3. Compare base state version/digest; apply delta with one attributable transition in the app transaction. Duplicate producer submissions recover the same decision. Conflicting deltas reject `STATE_VERSION_CONFLICT` and trigger recomputation from committed state.
4. Seal handoff and context selection; validate grants again at materialization. Child launch reserves resources and persists exact binding/manifest. New stage receives only declared state and context; no shared mutable chat/file directory.
5. Carry-forward across revision checks input/output/mapper/policy/binding digests. Mark eligible/reverify/ineligible with reasons. Revisit invalidates only declared dependent state, keeps original immutable lineage, and starts a new logical cycle identity.

Parallel merges use declared field policies: `replace_if_base_matches`, `append_unique_by_id`, `map_union_disjoint_keys`, or `reduce_registered`. Conflicting writes never use last-writer-wins by default. Imported provisional outputs are labeled and cannot satisfy irreversible/publication gates. Swarm member state remains private until admitted projection/convergence; optimizer evaluator receives an independent context packet.

## Goal Loop iteration algorithm

Loop State is bounded typed data; Loop Journal is append-only sequenced factual entries. Iteration n pins state/version, available authorized Action Space, observation refs, budget and context selection. `observe -> propose -> authorize -> act -> assess -> decide` produces an iteration receipt and typed state delta. Authorization validates all native child/tool launches before execution; action IDs survive technical retry. Assessment is independent where the completion rubric requires it.

Reducer computes Progress Review using committed criterion results, novelty/progress policy, spent/reserved/unknown budgets and no-progress counters. The controller may propose next action/state; it cannot mutate authority or decide its own acceptance. Seal n before releasing n+1. Technical retries do not increment iteration/round/cycle. Quality remediation does. Cancellation or new revision freezes launch, resolves active effect frontiers, applies recorded transition policy then releases the next authorized iteration.

Journal compaction produces a derived summary with source seq range/digests and covered/omitted facts. Original facts and unresolved commitments remain referenced. New sessions hydrate bounded summary plus mandatory unresolved facts and relevant evidence. Summary loss/corruption cannot erase effects, budget reservations or human review obligations. No stale model transcript becomes durable loop state.

## Context selection and budgets

Per-model profile declares context window, tokenizer/version, maximum output, tool-schema allowance, reserved control/instruction space, maximum retrieved content, summarization/hydration budgets and safety margin. Available input budget = context window - reserved output - fixed system/tool overhead - control reserve - safety margin. All terms must be nonnegative; mandatory facts exceeding allowance block `CONTEXT_BUDGET_EXCEEDED` and require a larger admitted profile or approved reduction. Unknown token count uses a conservative bound; never assume zero.

Selection steps: authorize candidate identities -> capture content/digests -> dedupe by canonical source/version/locator -> trust/expiry filter -> deterministic pinned ranking of captured candidates -> allocate mandatory Operating Contract/goals/state/commitments -> include relevant excerpts up to budget -> offload full bytes to scratch/durable artifacts -> record omissions and retrieval instructions. Stochastic retrieval/judgment outputs become captured candidate/ranking artifacts, not deterministic compiler internals.

Always include current action scope, active goal/criterion IDs, pending human/command/effect commitments, budget remaining including unknown usage, journal frontier, selected source citations and workspace map. Citation excerpt selector must resolve against captured immutable bytes. Untrusted retrieved content is data with provenance, never instructions capable of granting permission. Excerpts and file reads are size/range bounded; large CLI outputs return artifact handles and summaries, not full model tool messages.

Memory namespaces: session scratch, mission episodic, tenant personal and app/domain admitted knowledge. Cross-tenant/personal-public leakage denies before retrieval. Recall is advisory, with source, policy, expiry and consent; pending/contradicted memories cannot become accepted evidence. Writes require an explicit memory proposal/admission capability and receipt; automatic save-on-chat is disabled. Deletion/revocation blocks future recall, records retention decisions, and preserves only minimal permitted audit references.

## Exact middleware and extension placement

Use public pinned `create_deep_agent` composition and LangChain middleware interfaces after baseline inspection. The profile declares effective default + custom middleware order, not merely the list supplied to the factory. Golden qualification verifies: before-model context budget/selection and stop checks; wrap-model usage reservation and response accounting; before-tool argument/schema/authority/effect admission; wrap-tool stable intent/receipt/reconciliation and bounded result offload; after-tool artifact custody/safe progress; after-model typed candidate extraction; boundary hooks message dequeue/checkpoint/control reconciliation. Actual hook names/call signatures come from installed source; unavailable hooks use admitted tool/model adapters or block the capability, never monkey patch private state.

Security guards must apply to native default tools, shell/execute, dynamic subagent launch and background tasks. Disable unmanaged fan-out or replace it with the admission wrapper. An observational callback after execution cannot retroactively prevent spend or writes. Async hooks must be awaitable/cancellation-safe; event telemetry uses bounded queues and cannot be the authoritative transaction path. Model/tool retry middleware cannot bypass an uncertain effect claim. Qualify extension interaction with framework summarization, HITL interrupts and filesystem/execute backends.

Installed Deep Agents synchronous `task` transfers nonexcluded parent channels and returns nonexcluded child channels via Command. Replace this default propagation with an admitted compiled subordinate graph and explicit outgoing/returning projections. Child schemas must exclude authority/budget/acceptance mutation channels regardless of field naming. Parent runtime context is not assumed inherited: synchronous invocation lacks explicit context forwarding, while stock async launch transmits user messages. A registered graph input resolver validates the manifest scope/digest/generation and reconstructs immutable server-side context under current grants. Stock async update uses interrupt strategy; queued delivery must wait for a qualified boundary instead. No payload reference may be passed as literal model instructions without dereferencing and validation.

## Parent/subordinate message protocol

Persist parent admission, child dependency/grants/budget and stable launch identity first. Mailboxes use one sequence frontier per target generation. Workers read durable messages at a declared safe boundary, persist accepted/rejected disposition and emit delivery facts. Same client message ID/digest deduplicates; changed digest conflicts. A lost delivery response replays the same identity. Answers bind the question/human task and version; required child outputs still require admission.

Allowed parent messages: bounded new context, question answer, pause/stop intent and scope-preserving instructions. Child messages: summarized progress, blocker/question, artifact/result candidates. Parent and child cannot share raw writable state or secret namespaces. Child grants narrow parent authority; messages cannot widen them. Child cancellation settlement includes usage/effects/late-result policy. Terminal/stale target rejects messages with receipts; authorized link to a new run is explicit. Independent goals use Child Mission Invocation, not a hidden subagent.

## Ordering, urgent stop and feedback

Commands have monotonic per-run admission sequence, target/generation, expected aggregate version, typed payload digest, actor, urgency and deadline. Default ordering is accepted sequence; a stop/cancel fence supersedes undelivered ordinary messages and blocks new effects immediately after its transaction commits. Record superseded message dispositions; do not reorder or fabricate delivered history. Concurrent pause/resume/revision/feedback uses optimistic version checks; only the first compatible transaction proceeds, others return typed conflict/current frontier.

`stop_now` is a semantic cancellation request using command kind `cancel`, payload `urgency=immediate` plus reason. Persist desired stop and launch/effect fence, dispatch native cancel/process signal, fence old generation, observe native state, reconcile irreversible/unknown tool results, then settle. Delivered means native/boundary acknowledgement; settled means liabilities resolved. No hard maximum interruption time is assumed. Profiles publish measured local cancellation latency and remote caveats before advertising immediate control.

Queued instruction is consumed once at `next_turn` or `next_iteration`. Injection at safe boundary uses exact context artifact/digest and validated target generation; structural changes use a revision. Cancel-and-replace mid-turn requires qualified provider cancellation and uncertain-effect settlement before replacement. Pause seals a manifest at a qualified frontier; resume revalidates current grants and context. Fork independently admits new run/session/budget with eligible immutable facts only. No operation undoes a tool already executed.

Review feedback is a Human Task resolution bound to artifact version/digest, rubric/policy and expected task version: approve, reject, request_changes or abstain. Reject/request_changes require a feedback reference and invoke the authored remediation branch/optimizer round, creating a new artifact version and assessment/review. Prior approval is invalid for changed bytes. Human resolutions are one accepted decision per task; competing answers, deadline outcomes and notification deliveries retain receipts. Deadline never fabricates approval.

## Mandatory foundational tests

Stage handoff crash at commit/launch boundaries; two producer deltas conflict deterministically; loop iteration resumes with no duplicate effect; context tokenizer/mandatory overrun; corrupted summary cannot erase obligations; prompt injection and cross-scope memory denial; message gaps/duplicates/late generations; guard applies to native async child/shell tools; stop fence races tool admission; irreversible receipt survives cancellation; approval expires when artifact digest changes. Proof artifacts include fixture inputs, state/journal manifests, failure injections, captured histories and exact dependency/profile hashes.
