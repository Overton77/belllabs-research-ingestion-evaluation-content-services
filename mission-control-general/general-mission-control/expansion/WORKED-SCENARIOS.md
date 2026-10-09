---
type: Specification Annex
title: "Worked knowledge, PDF, coding and retrieval missions"
description: "Expansion fixtures, 2026-10-03. These are implementation-ready scenario contracts to materialize as validated native definition/intent fixtures. Capability names are logical catalog names from Knowledge Services…"
tags: [mission-control, spec, expansion]
---
# Worked knowledge, PDF, coding and retrieval missions

Expansion fixtures, 2026-10-03. These are implementation-ready scenario contracts to materialize as validated native definition/intent fixtures. Capability names are logical catalog names from [Knowledge Services integration](KNOWLEDGE-SERVICES.md), not claims of installed endpoints. Use [workflow semantics](../../workflow-types/index.md) and [runtime contracts](../RUNTIME-CONTRACTS.md). No runtime, paid probe, ingestion or deployment was executed while writing this file.

All scenarios pin application/tenant, goals, revision, exact assets, source/input manifests, schema/rubric versions, budget and deadlines. Run Deep Agents first with required Agent Server asynchronous subordinates in both apps. Cursor SDK Cloud is a later qualified coding adapter; frontier routes are pinned capabilities. Agent Server recovery uses private app-local persistence. Clean break needs no legacy aliases/history migration. Each mission is observed through the same API/CLI/skill; no product-specific scheduler is introduced.

## 1. Knowledge research seed with deterministic ingestion

Human outcome: establish an evidence-backed seed about an engineering tool or a Biotech intervention, with a reviewed proposal and authorized domain update. Engineering uses PostgreSQL named operations; Biotech uses graph catalog/read/apply. The same composition changes only domain pack, rubric, topic and exact capability bindings.

| Stage | Behavior | Inputs and acceptance |
| --- | --- | --- |
| scope | Human Gate if task ambiguity or writes lack authorization | Bounded research question, required facets, allowed sources, target entity and explicit read/write intent |
| discover | Goal Loop | Committed search/capture action space; finite iterations/source/usage ceilings; source bundle and coverage report, including missing facets |
| acquire | Bounded Parallel Swarm when available; Stage Graph children otherwise | Independent sources, exact capture/preparation bindings; convergence deduplicates logical source families and preserves failures |
| verify | Deterministic dispatch + independent domain assessment | Registered locators/claims; sealed mechanical/semantic findings; failure cannot be overruled by draft agent |
| classify | Proposed classification port | Evidence-bound taxonomy proposal with uncertainty; operator/domain policy admission distinct from generation |
| read | Deterministic named query | Persisted current scoped snapshot/concurrency token; unavailable/truncated reads remain explicit |
| plan | Deterministic ingestion executor | Immutable versioned domain intent, evidence and snapshot; registered plan with per-item disposition and touched identities |
| approve | Human Gate according to write policy | Exact intent/plan digest, policy and affected scope; altered plan invalidates reuse |
| apply | Deterministic domain executor | Stable effect identity; canonical receipt and settled costs; response loss enters receipt recovery |
| verify_update | New domain read + Proof Gate | Affected refs agree with plan/receipt; all-required/subset policy determines mission completion |

Draft report approval does not grant apply. If canonical writer is unavailable, compile a separate research/proposal-only mission; its goals must exclude completed ingestion. An authored Evaluator Optimizer can remediate report quality with an independent evaluator and bounded rounds. A Goal Loop may discover missing evidence within its action space; new tools/goals require a revision.

Required artifacts: question/scope manifest, source/capture/locator bundle, sealed verification/admission refs, classification proposal, read snapshot, native ingestion intent, plan, approval, receipt, post-write report and final coverage. Required adversarial proofs: forged evidence, foreign-app reference, stale schema/head, changed plan after approval, identical/changed identity replay, one rejected item among admitted items, lost response and graph commit/control-receipt gap. No unresolved required item/effect can satisfy full ingestion acceptance.

## 2. PDF extraction and evidence report

Human outcome: extract a defined table/claim set from a supplied PDF and produce a cited comparison. A PDF is source data, never instructions granting shell/tools. Original bytes, media type, digest and page identity are registered before parsing.

Stage Graph: `register_input -> capture_prepare -> locate -> extract -> verify -> report -> independent_assess -> human_review -> deliver`. Extraction may spawn bounded Agent Server subordinates per section, with parent-owned deadline/reservation, required/degradable dependency class and durable native handles. Required child settlement blocks completion. Convergence keeps per-section provenance and missing sections; it cannot convert timeout into empty evidence.

Parser binding pins converter, native geometry parser and optional OCR capability separately. Geometry selectors do not imply OCR. Unsupported scanned/table layout returns explicit missing capability/held output rather than fabricated text. A converted representation records transformation and source-page locator mapping; quotations replay against retained bytes/selectors. Numerical assertions include unit, scale, exact decimal/tolerance rule and reported rounding. Semantic relevance cannot override incorrect numeric extraction or ambiguous locator.

Output contract: immutable report, structured extracted rows, source/page/selector/selected-content digests, transformation profile, verification findings and unsupported facets. Human review binds those exact bytes. Default fixture ends at accepted report; graph/SQL ingestion is an optional separately authorized suffix using scenario 1's intent protocol.

Failure fixture: corrupt PDF, representation drift, duplicate quote occurrence, missing OCR, parse timeout, one child lost, parent cancellation and stale child output. Technical retry recovers existing capture/child identity; changed extraction instructions produce bounded new logical work. Success requires every declared required row/claim with replayable support and no unresolved usage/effect.

Source anchors in the Knowledge Services repository: `packages/verification/CAPABILITIES.md`, `apps/verification-executor/examples/CAPABILITIES-ACQUISITION.md`, `packages/verification/src/evidence-selection/`, `services/parser/`, `services/docling/`, and the explicit parser routes in `docs/agents/CODE-MAP.md`. Confirm actual media/profile availability before publishing the fixture; library selector support alone does not establish public extraction support.

## 3. Long-running coding and feature work

Human outcome: implement a bounded feature in an authorized repository, with evidence from independent tests/review. Target binding pins repository issuer, base commit, scope/allowed paths, isolated workspace ownership, test commands, egress and publication policy. Operator dirty worktree is not the mission workspace.

Stage Graph: `specify -> plan_review -> implement -> tests -> independent_review -> human_review -> deliver`. Deep Agents handles first-lane specification/implementation within qualified tools; Cursor Cloud can fill `implement` once its later harness gate passes. Lack of Cursor must not be disguised as passed Cursor qualification. Long execution uses validated continuation, native handle reconciliation and app-local runtime checkpoints; Temporal owns stage release and acceptance.

Knowledge query/retrieval supplies versioned implementation context and replayable citations. It does not authorize repository mutation. Required Agent Server subordinates can investigate modules or tests under bounded read/write grants; concurrent writers use distinct workspaces/branches with explicit patch integration, never race on a shared checkout. Independent evaluator has no authority to alter producer artifacts or self-approve them.

Deliverables: patch artifact, base/head identifiers when commits are explicitly permitted, test commands/environment and results, review findings, changed-file manifest and human resolution. Tests failing semantically trigger a bounded authored Evaluator Optimizer round or revision; infrastructure failure retries under the same effect identity. Commit/push/PR/merge/deploy are independent capabilities. Default mission delivers a patch and instructions; no default merge/deploy or domain ingestion.

Acceptance proofs: unrelated dirty files untouched; forbidden path/secret/network request denied; process death reattaches without duplicate paid launch; cancelled test is not reported as passed; changed patch invalidates prior review; fork gets new workspace/budget and no active child ownership; diagnostic replay makes no domain/repository effects. Long-running retry/recovery obligations remain despite clean-break release policy.

## 4. Retrieval strategy experiment and publication decision

Human outcome: compare retrieval strategies against an immutable evaluation set and recommend a candidate with attributable quality/cost evidence. Research can investigate alternatives, but benchmark claims require deterministic experiment outputs. No automatic vector/index publication follows a good score.

Stage Graph: `freeze_dataset -> candidate_plans -> run_experiments -> compare -> inspect_failures -> independent_assess -> human_review -> deliver_recommendation`. A bounded Parallel Swarm runs candidate configurations; its convergence is the explicit comparison subprogram, not voting by agent opinion. Candidate count, per-query budgets, repeats/seeds, model/provider routes, publication bindings, corpus snapshot and clocks are pinned. The same dataset and metrics apply to each comparable candidate; exclusions are recorded.

Retrieve through the canonical retrieval capability, not read-intent's currently unavailable embedded retrieval operation. Plans name required versus optional features, candidate/final limits, entity scope, query clock and abstention threshold. Unsupported required graph expansion or filter returns typed rejection before provider work. Optional omissions remain in result manifests and comparison eligibility. Preserve packets, support paths, citation replay, contradiction/supersession, truncation and abstention; rank score is not verified evidence.

Independent evaluation measures declared relevance/coverage/citation correctness/abstention plus latency and attributable usage. Confidence intervals or significance are reported only if the chosen admitted statistical method and sample support them. Failed queries, missing packets and unknown costs remain in denominators or explicitly stated exclusion policy; do not silently keep only successful runs. An optional Evaluator Optimizer may refine candidate plans within finite rounds without rewriting the frozen test set.

Publication is a separate human/domain-policy gated stage binding exact evaluated candidate/dataset/configuration digests. It requires its own stable effect receipt and post-publication read. Default completion produces a recommendation and failure analysis. Research recommendation, accepted experiment and active publication are separate facts.

Required artifacts: dataset/query/gold-label manifest, candidate configs, per-query packets/run receipts, evaluation results, comparison methodology/results, failure cases, cost uncertainty and review. Proofs: foreign tenant/publication denied, drifted corpus/config invalidated, unsupported feature explicit, missing citations fail, abstention preserved, response loss recovers same experiment run, and publication cannot be invoked by recommendation-only grants.

Source anchors: Knowledge Services `packages/contracts/src/retrieval.ts`, `packages/application/src/knowledge/retrieval/canonical-retrieval-executor.ts`, `packages/persistence/src/retrieval-evidence.ts`, `packages/evaluation/`, and `skills/knowledge-evaluation/SKILL.md`. Source/test availability is not a claim these experiments were run or deployed.

## Fixture and issue dependency contracts

Each scenario becomes: strict definition fixture, native domain intent/read/retrieval files, catalog assets, deterministic fakes, expected acceptance/rejection outcomes and crash-injection scripts. Generation must validate against the actual locked contracts. No example IDs, endpoints or digests may be accepted as production identities.

| Fixture package | Depends on | Exit evidence |
| --- | --- | --- |
| WF-KNOWLEDGE-SEED | KS-01..KS-05, Goal Loop/Stage Graph, Human/Proof Gates | Same core definition across both app bindings; proposal/apply receipts and no cross-app access |
| WF-PDF-REPORT | Capture/verify/locator bindings, Agent Server settlement, custody | Missing/ambiguous media handling and all required citation replay; no invented OCR |
| WF-CODING | Harness/workspace/capability admission, continuation, independent review | Isolated patch/test/review vertical; later Cursor-specific conformance separately recorded |
| WF-RETRIEVAL-EVAL | Retrieval/evaluation contract, immutable fixtures, budgets | Comparable candidate receipts, explicit exclusions/abstention and separately authorized publication |

Qualification layers are offline contract/fake tests, disposable database/Temporal/Agent Server proofs and separately authorized finite-budget live qualification. Report exact versions, commands/results, failed/skipped checks and deployment status. Paired application success and unresolved evidence gaps must remain visible; no runtime or integration capability is certified merely by these documents.
