---
type: Decision Record
title: "Provider frames stay native beside a small normalized lifecycle vocabulary, subordinate lineage is a recorded graph, and coordinator callbacks are bounded durable Subscriptions rather than in-session function calls"
description: "Proposed for the multi-provider packet (SPEC-04, MP-13, MP-15): ADR-0028's Native Event Store is kept; only stable lifecycle facts are normalized with native kind and payload preserved; subordinate identity is a graph attached by stable refs with a visibility coverage; a coordinator callback is an ADR-0032 Subscription with a default significance filter, batching, dedupe, rate and recursion caps and causation IDs."
tags: [mission-control, adr, proposed, events, subscriptions, subordinates]
status: proposed
source: docs/specs/multi-provider-2026-10/SPEC-04-realtime.md (event names, subordinate visibility, coordinator callbacks); ADR-0028; ADR-0032; application/frames/; application/subscriptions/
---

# Provider frames stay native beside a small normalized lifecycle vocabulary, subordinate lineage is a recorded graph, and coordinator callbacks are bounded durable Subscriptions rather than in-session function calls

**Status: proposed.** Becomes accepted when the owner authorizes it.

Every provider emits a different event shape, and a coordinator agent that "registers a callback"
would drown in token and tool deltas or, worse, be prompted by unsolicited nested sends. We keep
ADR-0028's Native Event Store exactly as it is and add only a small normalized set of lifecycle facts
(execution started and ended, tool started, completed and failed, approval pending and resolved,
compaction observed, usage, subordinate started and ended) with the native kind, version and a bounded
original payload preserved; an unknown native kind stays visible as unknown and is never promoted to a
closing fact. Subordinate identity is a graph (parent execution, native parent and child refs, spawn
correlation, kind `provider_subagent | agent_server_child | linked_mission`, generation, visibility
`full | lifecycle_only | unavailable`) attached by stable native references, never by message text. A
coordinator callback is an ADR-0032 Subscription with a default significance filter (review required,
blocked or failed, terminal result, significant accepted output, selected child lifecycle), batched
progress, deduplication by event ID, per-run rate and recursion caps and causation IDs so a notification
cannot trigger a command loop; whatever it prompts in the coordinator goes through that coordinator's
own admitted mailbox and budget. We rejected one universal event type (it flattens the provider detail
a dashboard needs) and rejected invoking a function inside a running provider session (not durable, not
authorizable, and an MCP notification cannot be assumed to wake a disconnected client or become model
input).

## Consequences

- `run.completed`, `activation.completed` and `human_task.opened` are a versioned read projection over
  canonical events with stable derived IDs; no second authoritative transition is appended and
  `workflow_run.set_wait` is not aliased into a human task.
- Parent usage totals that already include subordinates are not double counted in recursive views.
- A provider advertising subagents proves neither independent steering nor full event forwarding; the
  coverage is recorded per execution, and late child frames attach by ref or remain explicitly unresolved.
- Webhook delivery is signed, at least once, with delivery IDs, retry schedule, dead-lettering and
  destination egress validation; a failed delivery is never retried by rerunning the provider task.
