---
type: Decision Record
title: "queue_instruction, interrupt_and_inject, immediate cancel, add_context and fork become real commands with per-lane delivery reports; consumers receive mission state through durable subscriptions fed by the outbox"
description: "The single rejection in the mission service is replaced by reducer actions and boundary delivery: queued instructions are held durably and consumed once at the next turn or iteration, injection follows the lane's declared semantics, immediate cancel persists a stop fence before provider cancel, add_context queues a Context Packet addition, and fork snapshots then admits; subscriptions deliver filtered mission events by webhook, SSE stream or MCP notification with at-least-once semantics and a cursor."
tags: [mission-control, adr, decision, commands, subscriptions]
status: accepted
source: fast-track interview 2026-10-07 (requirement 2e); workflow-types/09 sections 7-9; ADR-0008; docs/specs/fast-track-2026-10/research/codebase-map.md (unsupported_control rejection site; delivery_report table)
---

# queue_instruction, interrupt_and_inject, immediate cancel, add_context and fork become real commands with per-lane delivery reports; consumers receive mission state through durable subscriptions fed by the outbox

Agents and operators need to search a run, read its transcript, queue context, interrupt, cancel now and branch. The request contracts exist; the mission service rejects them in one place. We implement them as reducer actions: `queue_instruction` and `add_context` append to a per-generation mailbox and are delivered once at `next_turn` or `next_iteration` by the family boundary; `interrupt_and_inject` dispatches by lane (`cooperative_inject` where native, otherwise `cancel_and_replace`, with the uncertain-effect settlement the specification requires before the replacement turn); `cancel` with `urgency: immediate` persists the Stop Fence, cancels the activity, and settles; `fork` takes a snapshot and admits a new run seeded from it, optionally with a queued instruction. Every command's Delivery Report names what the lane actually did. Subscriptions are rows (`mission_subscription`: scope, filter on event types and node keys, channel webhook or stream ticket or MCP session, cursor) consumed by an outbox relay with at-least-once delivery, dedupe on `event_id`, and a `CURSOR_EXPIRED` resync; the coordinator skill registers one automatically when it starts a run. We rejected provider-specific control endpoints because the harness contract is the one surface the skill teaches.

## Consequences

- Delivery semantics come from the lane's `describe`, recorded at admission so a client sees `requested` and `delivered` separately.
- `missionctl run search`, `run transcript`, `command queue`, `command inject`, `run fork` and `subscribe` are the CLI surface, mirrored by HTTP and MCP.
- A fork never clones in-flight commands; the mailbox of the source stays with the source.
