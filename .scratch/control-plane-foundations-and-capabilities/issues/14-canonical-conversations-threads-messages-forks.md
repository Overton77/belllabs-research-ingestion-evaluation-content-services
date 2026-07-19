# 14 — Implement canonical Conversations, Threads, messages, and forks

**What to build:** Create PostgreSQL-authoritative durable interaction records so conversations remain stable across clients, SDK sessions, agents, sockets, and workflow execution while retaining exact ordering, participants, scope, and fork lineage.

**Blocked by:**
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/14


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Build transactional conversation services and queries independently of Socket.IO and SDK sessions; transport and model projections consume these canonical records.
- Assign ordering and idempotency in PostgreSQL under principal, tenant, and thread scope, and preserve exact fork position and participant history.
- Use typed payload discriminators so tools, approvals, final outputs, and summaries remain distinguishable without turning message text into authority.

## Verification approach

Use authenticated application-service/API tests with concurrent submissions, exact retries, conflicting identities, stale versions, forks, process restart, and tenant isolation.

## Explicit non-goals

- Chat UI, Socket.IO replay, SDK context compaction, and treating raw messages as Run Requests or approvals.
- Long-term transcript-retention policy.

## Acceptance criteria

- [ ] Authorized APIs create Conversations and Threads with tenant, participants, roles, optional workflow/sandbox scope, and immutable lineage.
- [ ] Fork creation references an exact parent thread and durable turn; later parent messages do not become fork ancestors automatically.
- [ ] Message submission assigns one transactional per-thread order and supports expected-version concurrency.
- [ ] Stable client identity scoped to thread and principal makes exact duplicate submissions return prior results and conflicting reuse fail.
- [ ] Versioned typed records cover text, structured content, tool interactions/results, approvals, final outputs, and durable stream summaries.
- [ ] Accepted messages and final outputs commit before acknowledgement and remain queryable after immediate disconnect or process restart.
- [ ] Raw conversation remains context/evidence and creates no execution state without a separate typed promotion.
- [ ] Authorization and row-level security tests cover participant, thread, and tenant isolation.

## Source basis

- Canonical Conversations, Session Projections, and Durable Realtime
- Human Upgrade System Context

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
