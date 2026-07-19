# 16 — Implement authenticated durable realtime and reconnect

**What to build:** Replace payload-presence Socket.IO acceptance with authenticated, authorized subscriptions and a transport-neutral recovery model based on durable PostgreSQL projections, event records, and scoped cursors.

**Blocked by:**
- 02 — Implement transactional Run admission, lifecycle, budgets, and domain events
- 14 — Implement canonical Conversations, Threads, messages, and forks

**Status:** ready-for-agent

**GitHub:** https://github.com/Overton77/belllabs-research-ingestion-evaluation-content-services/issues/16


**Read first:** [Local control-plane ticket index](INDEX.md)

## Implementation guidance

- Separate connection authentication, subscription authorization, durable replay, and ephemeral streaming into explicit boundaries.
- Back recovery with application event/projection records and scoped consumer cursors; Socket.IO rooms and fan-out adapters are delivery details.
- Apply authorization filtering before payload serialization and recheck it on reconnect and relevant policy or membership changes.

## Verification approach

Use one authenticated ASGI client across HTTP and a real Socket.IO connection. Force disconnects, duplicate/out-of-order replay, gaps, retention expiry, revocation, and process change.

## Explicit non-goals

- Dashboard implementation, permanent broker choice, global ordering, and durable storage of every token delta.
- Using transport acknowledgements as domain command decisions.

## Acceptance criteria

- [ ] Connection validates a credential/trusted session and binds principal, tenant, expiry, and correlation; connection alone grants no subscriptions.
- [ ] Subscription authorization checks tenant, subject, participant/run access, data classification, and event classes before serialization.
- [ ] Policy, membership, credential, or session revocation removes subscriptions and blocks replay from old cursors.
- [ ] Versioned envelopes carry durable identity where applicable, channel, subject, aggregate/version or cursor, times, correlation/causation, and filtered payload/reference.
- [ ] Opaque integrity-protected cursors are policy-scoped and advance only under accepted acknowledgement policy.
- [ ] Reconnect authenticates, reauthorizes, queries current projection, validates cursor, detects gaps, and replays bounded changes or requires typed resynchronization.
- [ ] At-least-once replay supports deduplication and aggregate-version reconciliation; ephemeral token deltas are not required for durable recovery.
- [ ] ASGI/Socket.IO tests cover invalid/revoked credentials, unauthorized subjects, replay duplicates/gaps, retention expiry, process restart, and cross-tenant serialization.

## Source basis

- Canonical Conversations, Session Projections, and Durable Realtime
- Transactional Run Admission, Lifecycle, and Budgets

The source specifications and Human Upgrade System glossary were available locally when this ticket was authored. This ticket is intentionally self-contained for a fresh implementation agent.
