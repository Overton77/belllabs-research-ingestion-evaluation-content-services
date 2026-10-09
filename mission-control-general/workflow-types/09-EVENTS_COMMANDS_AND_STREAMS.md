---
type: Workflow Specification
title: "Mission Control — Mission Events, Commands, and Streams"
description: "Scope: the normalized Mission Event, the Native Event Store, delivery guarantees, the stream contract, the Command vocabulary and its delivery semantics, and intervention authority"
tags: [mission-control, spec, workflow]
---
# Mission Control — Mission Events, Commands, and Streams

Architecture: [General Mission Control](../general-mission-control/SPECIFICATION.md). Runtime/API binding: [RUNTIME-CONTRACTS.md](../general-mission-control/RUNTIME-CONTRACTS.md). Historical milestone references do not override the [current delivery plan](../general-mission-control/IMPLEMENTATION.md).

**Status:** canonical behavior contract; earlier interview dates below are provenance
**Last updated:** 2026-10-02 (general runtime consolidation)
**Scope:** the normalized Mission Event, the Native Event Store, delivery guarantees, the stream contract, the Command vocabulary and its delivery semantics, and intervention authority

Runtime observations and delivery controls follow the pinned profiles in RUNTIME-CONTRACTS; historical vendor facts do not define current support.

## 1. Purpose

Everything the dashboard, CLI, MCP tools, Agent Skill, and Portals show comes from one canonical stream of **Mission Events** written to the Domain Ledger. Everything an operator or agent does to a running Mission is a **Command** with recorded delivery semantics. Provider-native events are observations that feed adapters; they never become Mission state directly.

## 2. Mission Event envelope (accepted Round 3)

```text
MissionEvent {
  schema_version: "mc.event.v1"
  event_id                   // UUIDv7; canonical Python identity rule
  scope: {installation_id, application_id, tenant_id}
  mission_id
  run_id?, revision_id?
  seq                        // per-Mission monotonic, assigned at durable write; the replay cursor
  occurred_at, recorded_at
  ledger_commit_id
  event_type, event_version  // <aggregate>.<past_tense_verb>, §3
  execution: {
    node_key?, activation_id?, attempt_no?,
    harness_execution_id?, native_session_ref?, native_turn_ref?
  }
  source: {
    kind: mission_control | adapter | human | agent
    actor_ref?               // grant-bearing identity for human/agent sources
    native_event_ref?        // pointer into the Native Event Store
  }
  causation_id?, correlation_id?
  payload                    // typed per `type`; references and digests, never bodies
}
```

## 3. Event type vocabulary (accepted Round 3)

Naming rule: `<aggregate>.<past_tense_verb>`, lower snake_case, one aggregate per family. Payloads carry the shared vocabularies of `00 §6` by their exact names.

| Family | Events | Key payload fields |
|---|---|---|
| `mission` | `created`, `lifecycle_changed`, `closed` | `lifecycle`, `closure_outcome` |
| `run` | `started`, `lifecycle_changed`, `completed` | `lifecycle`, `terminal_outcome` |
| `revision` | `proposal_submitted`, `proposal_validated`, `proposal_resolved`, `committed`, `head_activated`, `superseded` | `proposal_id`, `resolution`, `base_revision_id`, transition impacts |
| `activation` | `lifecycle_changed`, `phase_changed`, `completed` | `lifecycle`, `phase`, `terminal_outcome`, `reason?` |
| `attempt` | `started`, `completed` | `outcome`, `failure_class?` |
| `session` | `started`, `turn_started`, `turn_completed`, `compaction_observed`, `checkpoint_sealed`, `transferred`, `ended` | native refs, usage, checkpoint ref |
| `tool_call` | `completed` | `name`, `status`, `args_digest`, `result_digest` |
| `command` | `accepted`, `queued`, `delivered`, `observed`, `completed` | `command_id`, `delivery_semantics`, `outcome` |
| `human_task` | `opened`, `claimed`, `resolved`, `expired` | `kind`, `resolution`, answer Artifact ref |
| `artifact` | `registered`, `projected` | Artifact ref, digest, producer activation |
| `disposition` | `recorded` | source (`completion_contract`, `evidence_assessment`, `evaluation_report`, `proof_gate`), value |
| `journal` | `entry_appended`, `segment_sealed` | entry kind, segment digest |
| `budget` | `consumed`, `threshold_crossed`, `exhausted` | credit account, amounts |
| `governor` | `threshold_crossed`, `exhausted` | governor name, value, cap |
| `event_wait` | `armed`, `fired`, `expired` | Event Receipt ref |
| `child_mission` | `invoked`, `portal_updated`, `completed`, `spawn_denied` | child id, Portal summary, denial reason |

## 4. Native Event Store (accepted Round 3)

Raw provider events — Deep Agents/Agent Server observations, Cursor Cloud stream events and frontier-provider responses or hook payloads — are stored in a separate **Native Event Store** keyed by `harness_execution_id`.

- Bounded retention, configurable, default 30 days.
- Tool payloads reduced to digests plus a size-capped excerpt.
- Never enters the Mission Event stream; Mission Events point at it through `source.native_event_ref`.
- Exists so the adapter's status mapping (`05 §3.5`) can be audited without polluting the canonical stream.

## 5. Delivery guarantees (accepted Round 3)

- Mission Events are written to the Domain Ledger before any outbound fan-out (outbox pattern).
- Delivery to consumers is **at-least-once**; consumers dedupe on `event_id` and detect gaps on `seq`.
- Adapter ingestion deduplicates scoped native event identity and source generation under the pinned adapter profile. A missing cursor/retention gap requires explicit resync and authoritative observation; it does not permit re-executing a paid request.
- Agent text, thinking, and full tool bodies never become Mission Events. Mission Events carry `session.turn_completed` (usage and result summary reference), `tool_call.completed` (digests), and `journal.entry_appended`. The dashboard's live agent tail is an ephemeral read over the Native Event Store, labeled non-canonical.

## 6. Stream contract (accepted Round 3)

`GET /v1/applications/{application_id}/missions/{id}/events?after_seq=&types=&node_key=` as SSE, plus the required ticket-based WebSocket in [experience/stream contracts](../general-mission-control/expansion/EXPERIENCE-AND-STREAMS.md): replay from `after_seq`, then live; server-side backpressure; heartbeats.

Child Mission events do **not** flow into the parent stream. The parent sees `child_mission.portal_updated`, coalesced on child lifecycle, outcome, and projection changes. A client wanting child detail opens the child's own stream within its grant.

## 7. Commands (accepted Round 3)

A Command is a write that changes execution. Reads (peek, stream) and Mission Invocation are not Commands.

| Command | Effect |
|---|---|
| `queue_instruction` | Deliver an instruction at the next safe Turn boundary. |
| `interrupt_and_inject` | Stop or steer the current Turn per runtime support and inject context or Artifacts. |
| `pause` | Stop releasing new work; running Attempts continue to their next safe boundary (Turn end), seal a Continuation Checkpoint, and park. |
| `hard_pause` | Cancel the Turn now; checkpoint from durable state only; report possible partial side effects. |
| `resume` | Resume release; may carry an instruction (delivered as `queue_instruction`), never new authority or budget without `mission.admin`. |
| `cancel` | Request stop with reason; terminal cancellation follows child/effect/usage settlement; cleanup remains required. |
| `retry` | Semantic retry creates a new Attempt under an authored policy; technical recovery preserves semantic Attempt/effect identity. |
| `rerun` | New activation from chosen immutable inputs. |
| `fork` | New branch from a checkpoint or Artifact set. |
| `request_continuation` | Force Compaction and Transfer now. |
| `request_revision` | Ask a reconciler (human or agent) for a Revision Proposal. |

Command `lifecycle`: `accepted → queued → delivered → observed → completed`  
Command `outcome`: `applied | failed | rejected | expired`  
Public request idempotency uses scoped `request_id`; the admitted `command_id` identifies durable delivery/redelivery. Every Command records `actor_ref` and `grant_ref`. A browser or MCP disconnect never loses a Command.

The table is the full semantic intervention vocabulary. The initial command endpoint supports pause/resume/cancel/queue_instruction/interrupt_and_inject; typed recovery, fork and revision operations use their dedicated routes in RUNTIME-CONTRACTS. Later command capabilities require explicit schemas and availability. A command never bypasses the stronger scopes of a fork, new run, proposal or budget action. `mission.start`, `mission.review`, catalog scopes and attempt-scoped `execution.report` supplement the basic scope table below as specified in the canonical specification.

## 8. Delivery semantics (accepted Round 3)

Every `DeliveryReport` names one of:

`turn_boundary_guaranteed | cooperative_inject | cancel_and_replace | wait_then_send | pause_at_tool_gate | emulated | unsupported`

`emulated` must name the emulation in `DeliveryReport.note` (for example `cancel_turn + checkpoint + transfer`). Each pinned Deep Agents/Agent Server, Cursor SDK Cloud or frontier-provider profile supplies its qualified command matrix. No historical vendor table can assert native support for an unqualified adapter. Plain pause stops releases and requests quiescence; hard pause requires qualified cancellation/recovery and reports uncertain effects.

"Best-effort" and "guaranteed" are never conflated; the dashboard, CLI, MCP tool results, and skill all show the delivered semantics.

## 9. Intervention authority (accepted Round 3)

| Scope | Grants |
|---|---|
| `mission.read` | peek, stream |
| `mission.author` | proposals |
| `mission.command` | all Commands in §7 except budget and authority changes |
| `mission.invoke` | spawn and attach |
| `mission.admin` | budget increase, grant changes, `hard_pause` on side-effecting work, closure |

An instruction delivered by Command can never change authority, capability, or budget. Approval ≠ input ≠ intervention.

## 10. State vocabularies in this document

| Vocabulary | Values |
|---|---|
| Event source kind | `mission_control \| adapter \| human \| agent` |
| Command | `queue_instruction \| interrupt_and_inject \| pause \| hard_pause \| resume \| cancel \| retry \| rerun \| fork \| request_continuation \| request_revision` |
| Command lifecycle | `accepted \| queued \| delivered \| observed \| completed` |
| Command outcome | `applied \| failed \| rejected \| expired` |
| Delivery semantics | `turn_boundary_guaranteed \| cooperative_inject \| cancel_and_replace \| wait_then_send \| pause_at_tool_gate \| emulated \| unsupported` |
| Authority scope | `mission.read \| mission.author \| mission.command \| mission.invoke \| mission.admin` |

## 11. Questions under interview

Payload schemas and adapter qualification are implementation deliverables under the canonical operation/event catalogs; no obsolete milestone specification is an additional authority.
