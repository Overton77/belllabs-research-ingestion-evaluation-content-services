# 17 — Implement governed Workflow and Mission Memory

**What to build:** Build an application-owned, tenant-isolated memory subsystem for reusable episodic, semantic, and procedural context, with immutable policy, authorization-first retrieval, reproducible injection, proposal-only agent writes, reviewed consolidation, contradiction, and lifecycle governance.

**Blocked by:**
- 01 — Implement versioned Workflow Definitions and Effective Run Configuration compilation
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events
- 06 — Implement provider-neutral Operation Execution Bindings and runtime contracts
- 14 — Implement canonical Conversations, Threads, messages, and forks

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/17


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Keep memory governance records in PostgreSQL and large immutable payloads in object storage; do not repurpose conversation, workspace, or graph stores as memory authority.
- Apply tenant and authorization filters inside candidate generation before scores or content can reach reranking or a model.
- Separate retrieval audit from exact rendered injection and preserve source episodes through every accepted proposal, consolidation, successor, contradiction, and tombstone.

## Verification approach

Use an authorized memory API with transactional PostgreSQL, vector-capable test search, and operation-preparation integration. Seed adversarial cross-scope content and inspect full audits.

## Explicit non-goals

- Canonical scientific knowledge, direct graph promotion, automatic prompt/Skill/policy publication, final embedding models, and workflow-specific thresholds.
- Treating memory volume or one aggregate score as quality.

## Acceptance criteria

- [ ] PostgreSQL stores typed Memory Spaces, immutable policies, versioned items, sources/links, proposals, retrievals, injections, consolidation decisions, and authorization.
- [ ] Items preserve namespace/scope, kind, operational/domain plane, content/reference, provenance/derivation, producer, confidence, review, sensitivity, retention, valid/system time, supersession, contradiction, and tombstone.
- [ ] Effective policy independently declares read/write envelopes, inheritance, consistency position, filters, retrieval/reranking, context budget, review, consolidation, contradiction, retention, and evaluation.
- [ ] Hard tenant, authorization, scope, namespace, validity, sensitivity, and review filters run before exact, lexical, and vector ranking.
- [ ] Every retrieval records policy/corpus position, all candidates and component scores, hard exclusions, reranking, selected order, and rationale.
- [ ] Every injection records exact rendered untrusted representation, labels, order, truncation, item revisions, target binding, token estimate, and digest.
- [ ] Agents/hooks submit idempotent Memory Write Proposals only; authorized decisions create successors without rewriting proposals or source episodes.
- [ ] Bounded consolidation preserves episodes and governs duplicates, successors, contradictions, domain claims, and high-impact procedures through configured review.
- [ ] Retention, expiry, legal hold, sensitivity change, and tombstone exclude unsafe future retrieval while preserving governance-safe historical evidence.
- [ ] Evaluation reports relevance, recall, provenance, leakage, stale resistance, write/consolidation precision, context cost, and downstream impact separately.

## Source basis

- Governed Workflow and Mission Memory
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
