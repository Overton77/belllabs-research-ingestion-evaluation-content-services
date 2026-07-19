# 15 — Implement conversation promotion and SDK session projections

**What to build:** Turn exact conversational evidence into executable meaning only through authorized typed promotion, and build bounded auditable SDK-session context projections without making runtime session history canonical.

**Blocked by:**
- 09 — Implement the OpenAI Agents runtime adapter and bounded delegation
- 14 — Implement canonical Conversations, Threads, messages, and forks

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/15


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Make promotion an application command that records source interaction identities and delegates to the target domain service; it must never update target stores directly.
- Build session projections as immutable selection/compaction/redaction decisions over canonical thread revisions, not copied mutable chat history.
- Keep cross-thread context selection explicit and authorization-checked before rendering model input.

## Verification approach

Promote one existing typed control-plane action end to end, reject unauthorized/injected proposals, and prove multiple compacted/restarted sessions leave canonical history unchanged.

## Explicit non-goals

- Defining every future promotion type, inferring approval from prose, persisting token deltas, and making an SDK session a Conversation Thread.
- Coordinator UI.

## Acceptance criteria

- [ ] A promotion cites exact supporting interactions, requested typed action, proposer, authority context, and policy.
- [ ] Accepted or rejected promotion decisions are immutable, auditable, and do not mutate source turns.
- [ ] Integrated promotions invoke existing Run Request, lifecycle, approval, or other typed application services rather than writing their stores directly.
- [ ] A session projection binds one authorized thread and purpose and records source range, included/excluded items, summaries, redactions, compaction, policy, target Agent Profile, and digest.
- [ ] Session creation uses only the accepted projection; restart, compaction, handoff, model change, or abandonment leaves canonical conversation unchanged.
- [ ] Cross-thread projection requires an explicit authorized context-selection decision and cannot silently cross tenant, purpose, or permission boundaries.
- [ ] Tests prove conversational prompt injection remains inert, rejected promotion remains visible, and session replacement preserves canonical records.

## Source basis

- Canonical Conversations, Session Projections, and Durable Realtime
- Operation Runtime, Workspaces, Artifacts, and Snapshots

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
