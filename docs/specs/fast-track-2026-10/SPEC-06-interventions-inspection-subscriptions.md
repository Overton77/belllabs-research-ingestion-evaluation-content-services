---
type: Specification
title: "SPEC-06: Interventions, inspection and subscriptions"
description: "Makes queue_instruction, add_context, interrupt_and_inject, immediate cancel and fork real commands with per-lane delivery reports, enriches run inspection with lane, session, mailbox and delivery facts, and adds durable subscriptions (webhook, SSE stream, MCP notification) fed by the outbox so coordinators and consumers receive mission state without polling. Implements ADR-0032 and the inspection half of owner requirement 2e."
tags: [mission-control, spec, fast-track, commands, subscriptions]
---

# SPEC-06: Interventions, inspection and subscriptions

Decision: [ADR-0032](../../adr/0032-interventions-real-queue-inject-cancel-fork-and-subscriptions.md), with [ADR-0008](../../adr/0008-ordered-commands-with-urgent-stop-fence.md) (ordered commands, stop fence), [ADR-0027](../../adr/0027-context-packet-tiers-and-workspace-materialization.md) (the Context Packet that queued content enters), [ADR-0028](../../adr/0028-native-event-store-provider-frames-and-materialized-transcript.md) (frames and transcript), [ADR-0031](../../adr/0031-temporal-lifecycle-synthesis-observe-activity-updates-seeded-forks.md) (how a cancel reaches a provider). Normative inputs: workflow-types/09 sections 6 to 9, `expansion/CONTEXT-STATE-AND-CONTROL.md` (ordering, urgent stop and feedback), `expansion/EXPERIENCE-AND-STREAMS.md` (stream contract). Code facts: [research/codebase-map.md](research/codebase-map.md) sections 5, 6 and 9 and gap (g).

Vocabulary is `GLOSSARY.md`: Command, Delivery Report, Stop Fence, Intervention, Reducer, Outbox, Subscription, Snapshot, Fork, Context Packet, Generation, Transcript, Run, Grant, Scope.

## Problem Statement

A coordinator agent or an operator watching a Run today can pause, resume, satisfy a wait and request a normal cancellation. Nothing else works. The public request contract already lists `queue_instruction` and `interrupt_and_inject`, and an immediate cancel validates, but one line in the mission service rejects all three as `unsupported_control`. There is no way to hand a running Goal Loop a new instruction for its next iteration, to stop a Cursor agent mid-run and redirect it, to stop everything now with a guarantee that no new side effect lands, or to branch a Run from its last safe Snapshot with a changed instruction. Consumers learn about mission state only by polling `run inspect` and `command list`; nothing pushes events to a webhook, a stream or an MCP session, so the coordinator skill cannot react to a human task or a completion without a busy loop. Inspection itself says little about what the agent is doing: no lane, no session or turn, no pending instructions, no record of what each Command actually did on the lane.

## Solution

Every Intervention in workflow-types/09 section 7 that the owner asked for becomes a real Command admitted by the Reducer, delivered by the family boundary or the lane, and reported with the Delivery Report the lane actually produced:

- `queue_instruction` and `add_context` write a durable mailbox entry per Run and Generation and are consumed exactly once at the next safe boundary into the next Context Packet.
- `interrupt_and_inject` dispatches by lane: `cooperative_inject` where the lane is native, otherwise `cancel_and_replace` with uncertain-effect settlement before the replacement turn, and the injected content rides the replacement turn's packet.
- `cancel` with `urgency: immediate` persists a Stop Fence before anything else, so kernel hooks refuse new effect claims at once, then runs the activity cancel path and settles.
- `fork` takes a Snapshot, admits a new Run seeded from it through the packet's `workspace` tier, optionally with a queued instruction, and records lineage. It never clones the mailbox or active children.

Inspection grows a lane and session view, the mailbox, delivery reports, chain membership and the transcript cursor. A Subscription is a durable row consumed by the Outbox relay that delivers filtered mission events to a webhook, an SSE stream or an MCP session at least once with a cursor; `missionctl events watch` and `missionctl subscribe` are its CLI, and the coordinator skill registers one when it starts a Run.

## User Stories

1. As a coordinator agent, I want to queue an instruction for a running Goal Loop, so that the next iteration reads it without my stopping the loop.
2. As a coordinator agent, I want to queue added context (an artifact or a short note) for the next turn, so that the agent sees new evidence at a safe boundary.
3. As an operator, I want to interrupt a running turn and inject a redirect, so that a Cursor agent heading the wrong way stops and continues with my correction.
4. As an operator, I want the Delivery Report to say whether my interrupt was native or a cancel-and-replace, so that I know whether partial work may exist.
5. As an operator, I want an immediate cancel that first blocks any new side effect, so that no shell command, push or paid call starts after I pressed stop.
6. As an operator, I want to see requested, delivered and settled states separately for an immediate cancel, so that I know when liabilities are resolved.
7. As a coordinator agent, I want to fork a Run from its last Snapshot with a new instruction, so that I can explore an alternative without destroying the original.
8. As a coordinator agent, I want the fork to carry lineage I can search, so that I can find every branch of a Run later.
9. As an operator, I want pause and resume to keep working exactly as before, so that existing runbooks stay valid.
10. As a coordinator agent, I want `run inspect` to tell me the lane profile and the current session and turn, so that I understand what is executing.
11. As a coordinator agent, I want `run inspect` to list pending and consumed mailbox entries, so that I do not queue the same instruction twice.
12. As an operator, I want `run inspect` to list every Delivery Report, so that I can audit what each Command did.
13. As a coordinator agent, I want `run inspect --wait` to return when the Run changes lifecycle or phase, so that I avoid a busy loop.
14. As an operator, I want `run list --query` over Temporal visibility, so that I can find running Cursor runs for a mission without scanning the ledger.
15. As a coordinator agent, I want `run search ID --query` over the Transcript, so that I can find the turn where a tool failed.
16. As a consumer service, I want to register a webhook for selected event types, so that my system reacts to human tasks and completions.
17. As a consumer service, I want webhook deliveries signed with HMAC, so that I can reject forged calls.
18. As a consumer service, I want deliveries to be at least once with an event id, so that I can deduplicate safely.
19. As a dashboard, I want an SSE stream with replay from a sequence and then live events, so that a reconnect never misses an event.
20. As a dashboard, I want heartbeats and an explicit resync signal when my cursor expired, so that I never silently skip a gap.
21. As an MCP host, I want mission notifications in my session, so that the coordinator agent is woken rather than polling.
22. As the coordinator skill, I want a Subscription registered automatically when I start a Run, so that observing is the default.
23. As an operator, I want a Command rejected when my Grant does not cover it, so that an instruction can never widen authority or budget.
24. As an operator, I want a Command rejected with the current version when my `expected_version` is stale, so that two operators cannot race.
25. As an operator, I want an identical retry of a Command to return the same receipt, so that a lost response is safe to retry.
26. As an operator, I want a queued instruction superseded when a cancel lands first, so that the agent never reads an instruction after a stop.
27. As an operator, I want a fork to refuse to clone in-flight Commands, so that the branch starts clean.
28. As a reviewer, I want every intervention visible in the Transcript, so that the record shows what was asked and what happened.
29. As an operator, I want `missionctl command queue` and `command inject` to take a file, so that the content is exact and reviewable.
30. As an operator, I want dead-lettered webhook deliveries listed, so that a broken endpoint is visible rather than silent.

## Implementation Decisions

### Command vocabulary and lifecycle

- The Reducer accepts these kinds for a Run target: `pause`, `resume`, `satisfy_wait`, `cancel` (urgency `normal` or `immediate`), `queue_instruction`, `add_context`, `interrupt_and_inject`, `fork`, `reconcile_unit` (privileged, unchanged). `queue_instruction` and `add_context` are two kinds sharing one mailbox entry shape; they differ in how the packet renders them (an instruction becomes an `admitted_input` segment marked as an instruction; added context becomes a packet item with its own expansion tier).
- Command lifecycle becomes the five-state vocabulary of workflow-types/09: `accepted → queued → delivered → observed → completed`, outcome `applied | failed | rejected | expired`. The code's receipt states (`accepted`, `delivered`, `applied`, `rejected`) are kept as columns of the same record and extended: `queued` is the state of a mailbox-bound Command after acceptance and before the family boundary takes it; `observed` is the state after the lane reported a Delivery Report and before settlement. Existing receipts map forward: `applied` is `completed/applied`; `rejected` is `completed/rejected`. No existing row changes meaning.
- Admission keeps the three sequence spaces (`execution`, `cancel`, `boundary:<family>`) and adds `mailbox:<generation>` so queued content never opens a gap in the boundary sequence. An immediate cancel supersedes every undelivered mailbox entry of the same Run; superseded entries record `superseded_by` and an outcome `expired`, never deletion.
- The single `unsupported_control` rejection in `MissionControlService._action` is removed; each kind maps to a Reducer action (`QueueInstructionAction`, `AddContextAction`, `InterruptAndInjectAction`, `ImmediateCancelAction`, `ForkRunAction`). `BOUNDARY_COMMAND_KINDS` gains `queue_instruction` and `add_context` as family-applicable; `interrupt_and_inject` and immediate `cancel` target the unit boundary; `fork` targets `run_control` and reuses the fork saga.
- Delivery reports are populated from the lane's `describe` at admission (requested semantics) and from what the lane returned at delivery (delivered semantics). The report row records both, plus `native_refs` (session, turn, run id) and an `emulation_note` when `emulated` is reported.

### Mailbox and boundary delivery

- A mailbox entry carries: scope, run, target Generation, kind, `boundary` (`next_turn` or `next_iteration`), content as either an artifact reference with digest or inline text bounded by a configured cap (default 8 KiB), `expand` hint for the packer, actor, admission sequence, deadline, state (`queued | delivered | consumed | superseded | expired`).
- Delivery happens where the family already applies boundary Commands: the StageGraph admission boundary (next operation of the targeted node) and the GoalDirected iteration boundary. The family reads undelivered entries in sequence, hands them to the Context Packer as mandatory items, marks them `delivered`, and the lane marks them `consumed` when the turn that carried them starts. If the turn fails before start the entry returns to `queued`.
- A Generation mismatch (entry targets a Generation that was superseded by a reattach or fork) expires the entry with reason `stale_generation`; it is never silently redirected.
- Delivery Report for mailbox Commands: Deep Agents `turn_boundary_guaranteed`; Cursor Local and Cloud `wait_then_send` because the lane must wait for `IDLE` before the next `send`.

### Interrupt and inject

- The lane's `describe` names the semantics: `cooperative_inject` where native steer exists (no first-wave lane has it; the matrix stays honest), otherwise `cancel_and_replace`.
- `cancel_and_replace` sequence: persist the Command; cancel the running `lane.turn` activity (ADR-0031: activity cancellation, then the idempotent `lane.cancel` activity, then `lane.status` polling to a terminal provider state); settle uncertain effects through the existing in-doubt path (an unsettled effect claim blocks the replacement turn and surfaces `EFFECT_UNCERTAIN`); write the injected content as a mailbox entry with boundary `next_turn`; the family releases the replacement turn, whose packet carries the injected item first. The Delivery Report is `cancel_and_replace` with the cancelled turn's native refs and the replacement turn's refs.
- On Cursor the replacement is a new `send` on the same agent (same Agent Session) when the run was `cancelled` cleanly; a new agent hydrated from the packet when the agent is unusable (SPEC-07 decides).

### Immediate cancel and the Stop Fence

- `cancel` with `urgency: immediate` writes the Stop Fence row (`run_id`, `generation`, `command_id`, `fenced_at`) in the same transaction as the Command admission, before any Temporal interaction. Kernel hooks (ADR-0026) and the effect ledger consult the fence before allowing a side effect or writing an Operation Intent; a fenced claim is rejected with `STOP_FENCED`.
- Then the normal cancel path runs: activity cancel, `lane.cancel`, polling, settlement of children, effects and usage; the Run reaches `completed` with terminal outcome `cancelled` only after settlement. Inspection shows `requested_at`, `fence_persisted_at`, `provider_acknowledged_at`, `settled_at` separately.
- No latency promise is made; the profile publishes measured local cancellation latency as evidence, never as a contract (ADR-0008).

### Request continuation

`request_continuation` (workflow-types/09 §7) is accepted on the same endpoint as the other kinds. Its reducer action (`workflow_run.request_continuation`) is owned by T5 in F1 beside the mailbox actions; the trigger, the sealing of the `mc.continuation_checkpoint.v1` and the fresh-session hydration are owned by T2 in B4 (SPEC-02). The command is delivered at the next safe boundary (`turn_boundary_guaranteed` on Deep Agents, `wait_then_send` on Cursor), records `session.checkpoint_sealed` and `session.transferred`, and reports `emulated` with note `compact_and_transfer` on every lane because no provider exposes a native transfer.

### Fork

- `fork` reuses `RunSnapshotService.take` and `SemanticForkService` (safe-boundary Snapshot, typed patch, independent admission). The fork request may carry `instruction` (becomes the first mailbox entry of the new Run with boundary `next_turn`) and `from_snapshot_id` (defaults to the latest safe Snapshot; a fork on a Run with no safe Snapshot is rejected with `CHECKPOINT_INVALID`).
- The new Run's first Context Packet includes the Snapshot as its `workspace` tier item (SPEC-02), so the lane restores files before the first turn.
- Lineage: `mission_relationship` kind `fork` (existing) plus Temporal search attributes `mc_forked_from_run_id` and `mc_forked_from_snapshot_id` (G7 registers them). The source Run's mailbox, children and in-flight Commands are not copied.

### Inspection

`MissionInspection` (`mc.inspection.v1`) gains optional sections so existing clients keep parsing:

- `lane`: lane profile id, `describe` digest, native identity refs of the current harness execution.
- `sessions`: list of agent sessions with turn count, last turn status, usage disposition.
- `mailbox`: pending and consumed entries (kind, boundary, state, admission sequence, content digest; never bodies).
- `delivery_reports`: every Command with requested and delivered semantics, outcome and native refs.
- `frames_cursor`: the last persisted provider frame ordinal and the transcript cursor to pass to `run transcript --since`.
- `chain`: chain id and the links this Run participates in, when any.
- `subscriptions`: count of active Subscriptions targeting this Run or its mission.

`run inspect --wait SECONDS` returns early on a lifecycle, phase or terminal change, otherwise at the deadline, as today. `run list --query` passes a Temporal visibility query over the registered attributes (`mc_mission_id`, `mc_run_id`, `mc_lane`, `mc_phase`, fork lineage) and returns run ids with their inspection summary. `run search ID --query` is a full-text search over the Transcript materialization (SPEC-03) returning matching entries with their cursor.

### Subscriptions

- A Subscription row carries: scope, target (mission id, or run id), filters (`event_types[]`, `node_keys[]`, optional `kinds` for frames excluded by default), channel (`webhook {url, secret_ref, signature_header}` | `stream_ticket {ticket_id}` | `mcp_session {session_ref}`), `cursor` (last delivered `seq`), state (`active | paused | dead_lettered | closed`), actor and creation time.
- The relay is an Outbox consumer (`ConsumerCursor` per Subscription): it reads committed mission events after the cursor, applies filters, delivers, writes a `subscription_delivery` receipt (event id, attempt, status, response code, latency) and advances the cursor only on success. Delivery is at least once; consumers dedupe on `event_id` and detect gaps on `seq`. Retries use exponential backoff with jitter; after N failures (default 12) the Subscription is `dead_lettered` and a `subscription.dead_lettered` mission event is emitted so the failure is visible in the same stream.
- Webhook bodies are the `mc.event.v1` envelope; the signature header is `X-MC-Signature: sha256=<hmac>` over the raw body with the secret resolved from `secret_ref` at send time (never stored in the row). Payloads carry references and digests, never bodies, per workflow-types/09.
- SSE: `GET /v1/applications/{app}/missions/{id}/events?after_seq=&types=&node_key=` replays from the durable tail then follows live; heartbeat every 30 seconds; a cursor older than retained history returns `CURSOR_EXPIRED` and the client resyncs from inspection. The ticket-based WebSocket of EXPERIENCE-AND-STREAMS remains specified, not in this packet.
- MCP: `mission_subscribe` registers an `mcp_session` Subscription; the server sends notifications `notifications/mission/event` with the envelope; a disconnected session pauses the Subscription and resumes from its cursor on reconnect.
- The coordinator skill (SPEC-08) registers an `mcp_session` or `stream_ticket` Subscription for the Run it starts, filtered to `human_task.*`, `run.*`, `activation.completed`, `command.completed`, `session.transferred`, `budget.threshold_crossed`, `chain_link.*`.

### Authority

workflow-types/09 section 9 scopes map onto the code's existing `workflow_run.*` grants; no grant is renamed:

| Spec scope | Code grant(s) | Covers |
| --- | --- | --- |
| `mission.read` | `workflow_run.read` | inspect, transcript, `run list`, `run search`, subscribe, stream |
| `mission.command` | `workflow_run.control` | pause, resume, satisfy_wait, normal cancel, queue_instruction, add_context, interrupt_and_inject, fork |
| `mission.admin` | `workflow_run.admin` | immediate cancel on side-effecting work, closing a Subscription owned by another actor |
| `mission.invoke` | `workflow_run.launch` | the launch of the forked Run |
| `execution.report` | `workflow_run.relay` | relay and delivery internals, never exposed to coordinators |

An instruction delivered by a Command never changes authority, capability or budget; the packer renders it as data with provenance (ADR-0027).

## Contracts

### `mc.command.v1` payloads (completed)

```text
MissionCommandRequest {
  schema_version: "mc.command.v1", request_id (uuid), expected_version, expected_generation,
  target: {kind: "run", id}, kind, payload, reason
}
kind = pause | resume | satisfy_wait | cancel | queue_instruction | add_context | interrupt_and_inject | fork

QueueInstructionPayload { boundary: next_turn | next_iteration, content: ContentRef | InlineText, deadline?, node_key? }
AddContextPayload      { boundary: next_turn | next_iteration, content: ContentRef, expand: inline | reference | materialize | auto, node_key? }
InterruptAndInjectPayload { content: ContentRef | InlineText, settle_uncertain_effects: true (fixed), node_key? }
CancelPayload          { urgency: normal | immediate }
ForkPayload            { from_snapshot_id?, instruction?: InlineText | ContentRef, changes?: TypedPatch, sponsorship_ref?, approval_refs[] }

ContentRef  { artifact_ref, content_digest, media_type }
InlineText  { text (<= 8 KiB), content_digest }
```

### Command receipt and Delivery Report

```text
CommandReceipt { command_id, request_id, kind, lifecycle: accepted|queued|delivered|observed|completed,
                 outcome?: applied|failed|rejected|expired, admission_sequence, sequence_space,
                 superseded_by?, generation, versions {expected, current} }
DeliveryReport { command_id, requested_semantics, delivered_semantics:
                 turn_boundary_guaranteed|cooperative_inject|cancel_and_replace|wait_then_send|pause_at_tool_gate|emulated|unsupported,
                 emulation_note?, native_refs {lane_profile, session_ref?, turn_ref?, cancelled_turn_ref?, replacement_turn_ref?},
                 observed_outcome: delivered|applied|rejected|emulated|unknown, recorded_at }
```

### `mc.subscription.v1`

```text
Subscription { subscription_id, scope, target: {mission_id} | {run_id}, filters {event_types[], node_keys[]},
               channel: Webhook{url, secret_ref, signature_header} | StreamTicket{ticket_id} | McpSession{session_ref},
               cursor_seq, state: active|paused|dead_lettered|closed, actor_ref, created_at }
SubscriptionDelivery { subscription_id, event_id, seq, attempt, status: delivered|failed|dead_lettered, response_code?, latency_ms, recorded_at }
```

### SSE frame

```text
event: mission_event      data: <mc.event.v1 JSON>        id: <seq>
event: heartbeat          data: {"recorded_at": ...}
event: resync_required    data: {"code": "CURSOR_EXPIRED", "oldest_seq": N}
```

### `MissionInspection` additions

Optional keys `lane`, `sessions`, `mailbox`, `delivery_reports`, `frames_cursor`, `chain`, `subscriptions`, shaped as described above; all reference-only.

## Persistence (migration `0029_command_mailbox_stop_fence_subscriptions.sql`, team T5)

| Table | Columns (all with scope columns and forced RLS like the existing chain) |
| --- | --- |
| `command_mailbox` | `entry_id` uuid pk, `run_id`, `command_id`, `generation` int, `kind` (`queue_instruction|add_context`), `boundary` (`next_turn|next_iteration`), `node_key` null, `content_ref` null, `content_inline` text null (cap enforced), `content_digest`, `expand` null, `admission_sequence` int, `deadline` timestamptz null, `state` (`queued|delivered|consumed|superseded|expired`), `superseded_by` null, `delivered_at`, `consumed_at`, `expired_reason` null; unique `(run_id, generation, admission_sequence)` |
| `stop_fence` | `run_id` pk, `generation`, `command_id`, `fenced_at`, `reason`; one row per Run, insert-only |
| `mission_subscription` | `subscription_id` pk, `mission_id` null, `run_id` null (one required), `event_types` text[], `node_keys` text[], `channel_kind`, `channel` jsonb (no secret values), `cursor_seq` bigint, `state`, `actor_ref`, `created_at`, `updated_at` |
| `subscription_delivery` | `delivery_id` pk, `subscription_id`, `event_id`, `seq`, `attempt` int, `status`, `response_code` null, `latency_ms` null, `recorded_at`; unique `(subscription_id, event_id, attempt)` |
| `delivery_report` (existing) | add nullable `requested_semantics`, `emulation_note`, `replacement_turn_ref` |
| receipts (existing command ledger) | extend the state check to add `queued` and `observed`; add `outcome` nullable; add `sequence_space` value `mailbox:<generation>` |

## Interfaces

| Surface | Addition |
| --- | --- |
| CLI | `command queue RUN --file`, `command inject RUN --file`, `command cancel RUN --urgency immediate --reason`, `run fork RUN --from-snapshot ID --instruction-file FILE`, `run inspect RUN --wait`, `run list --query`, `run search RUN --query`, `subscribe --run RUN | --mission ID --webhook URL --secret-ref REF --events a,b`, `subscribe list|close`, `events watch ID --after-seq` |
| HTTP | `POST /runs/{id}/commands` accepts all kinds; `POST /runs/{id}/forks` accepts `instruction`; `GET /runs/{id}/inspection` enriched; `GET /runs?query=`; `GET /runs/{id}/search?q=`; `POST /subscriptions`, `GET /subscriptions`, `DELETE /subscriptions/{id}`; `GET /missions/{id}/events?after_seq=` (SSE) |
| MCP | `mission_command_send`, `mission_run_fork`, `mission_run_inspect`, `mission_subscribe`, notification `notifications/mission/event` |

## Insertion points

- `application/missions/service.py::MissionControlService._action`: replace the rejection with per-kind actions.
- `domain/policies/contracts.py`: `BOUNDARY_COMMAND_KINDS`, new action records, receipt states; `domain/policies/mailbox.py` and `domain/policies/stop_fence.py` (new).
- `domain/policies/boundary_commands.py`: target mailbox kinds at the family boundary; immediate cancel and interrupt at the unit boundary.
- `adapters/temporal/boundary_commands.py` and the family workflows' `deliver_boundary_command`: read mailbox entries at the admission and iteration boundaries and pass them to the packer (SPEC-02).
- `adapters/postgres/run_control/run_control_repository.py` and `runtime/runtime_execution_repository.py`: delivery report columns.
- `application/recovery/run_forks.py`: fork with instruction and lineage attributes.
- `application/subscriptions/{service,relay}.py` (new) on the Outbox `ConsumerCursor`; `interfaces/http/subscriptions.py`, SSE route in `interfaces/http/mission_control.py`.
- `contracts/contracts.py`: `MissionCommandRequest` payload unions, `MissionInspection` additions.
- `interfaces/cli/main.py`: new command groups; `interfaces/mcp/coordinator_server.py`: new tools.

## Testing Decisions

Good tests observe external behaviour: a Command sent through HTTP produces the expected receipt states, Delivery Report, mission events and Transcript entries on the real local stack (PostgreSQL 17 with pgvector, local Temporal dev server). Unit tests cover the Reducer actions and supersession rules in isolation. Prior art: `tests/unit/run_control/test_boundary_commands.py`, `tests/acceptance/control_plane/test_rrm_007_api.py`, `tests/integration/postgres/test_mission_control_lifecycle_postgres.py`, `tests/unit/run_control/test_mission_control_runtime.py` (fork), `tests/integration/temporal/test_linked_runs.py` (time-skipping Temporal).

Mandatory cases (from CONTEXT-STATE-AND-CONTROL "Mandatory foundational tests"): a queued instruction is consumed exactly once across a worker restart; two Commands with the same `request_id` return one receipt; a stale `expected_version` returns 409 with the current frontier; an immediate cancel's Stop Fence races a tool admission and the admission loses; an irreversible receipt survives cancellation; a fork clones no mailbox entry; a dead-lettered webhook emits its event; an SSE client with an expired cursor receives `resync_required`; late-generation mailbox entries expire with `stale_generation`.

## Out of Scope

Ticket-based WebSocket adapter; `hard_pause`, `retry`, `rerun`, `request_revision` Commands; mid-turn `cooperative_inject` on any first-wave lane (none is native); notification providers beyond webhook, SSE and MCP; dashboard rendering; cross-mission global ordering.

## Scenarios (Mission 3, Cursor Local codebase feature)

1. Operator runs `missionctl command queue RUN --file add-readme.json` (boundary `next_iteration`, text "Also update the README with the new flag"). Events: `command.accepted`, `command.queued`; inspection mailbox shows one `queued` entry. At the iteration boundary: `command.delivered` with Delivery Report `wait_then_send`; the next packet's `.mission/context.md` lists the instruction first; the lane marks `consumed`; `command.completed{applied}`. Transcript shows the instruction entry before the turn's first frame.
2. Operator runs `command inject RUN --file redirect.json`. Events: `command.accepted`; the `lane.turn` activity is cancelled; `lane.cancel` acknowledges; a `tool_call` in flight settles as `uncertain` then `applied`; `command.delivered` with `cancel_and_replace` naming the cancelled and replacement turn refs; replacement turn starts with the redirect in its packet; `command.completed{applied}`.
3. Operator runs `command cancel RUN --urgency immediate --reason "wrong repo"`. The Stop Fence row commits; a `beforeShellExecution` kernel hook callback during the window returns `deny` with `STOP_FENCED` (visible as a frame); the activity is cancelled; `lane.cancel` and `lane.status` settle; `run.completed{cancelled}` after usage settlement. Inspection shows four timestamps.
4. Operator runs `run fork RUN --instruction-file retry-with-tests.json`. A Snapshot at the last safe boundary is taken; a new Run is admitted with `mc_forked_from_run_id`; its first packet restores the workspace snapshot and lists the instruction; `run list --query "mc_forked_from_run_id='RUN'"` returns the branch.

## Tickets

| Id | Title | Blocked by |
| --- | --- | --- |
| FT-F1 | queue_instruction and add_context mailbox with boundary delivery | FT-B1 |
| FT-F2 | interrupt_and_inject per lane with uncertain-effect settlement (team T4, SPEC-07) | FT-F1, FT-G1 |
| FT-F3 | Immediate cancel with persisted stop fence consulted by kernel hooks (team T4, SPEC-07) | FT-G1 |
| FT-F4 | Fork from snapshot with queued instruction on CLI, HTTP and MCP | FT-B1, FT-F1 |
| FT-F5 | Subscriptions: webhook, SSE events watch, MCP notification | none |
| FT-F6 | Inspection enrichment: lane, sessions, mailbox, delivery reports | FT-C2 |

## Further Notes

- Temporal Update ids equal `command_id`; validators reject duplicates and invalid states without history cost; the family carries handled command ids across continue-as-new because server deduplication is per run (research/temporal-lifecycle.md section 1.2). Stay under 10 in-flight and 2,000 total Updates per run.
- Cancel latency is bounded by heartbeat throttling (ADR-0031); report measured values, promise none.
- The ADR-0008 Stop Fence had no persisted form before this spec; the `stop_fence` table is that form.

# Citations

- `docs/adr/0008`, `0027`, `0028`, `0031`, `0032`; `GLOSSARY.md`.
- `../mission-control-general/workflow-types/09-EVENTS_COMMANDS_AND_STREAMS.md` sections 6 to 10; `../mission-control-general/general-mission-control/expansion/CONTEXT-STATE-AND-CONTROL.md` (ordering, urgent stop and feedback; mandatory tests); `../mission-control-general/general-mission-control/expansion/EXPERIENCE-AND-STREAMS.md` (stream backpressure profile).
- `docs/knowledge/events-and-commands.md`; `research/codebase-map.md` sections 5, 6, 9 and gap (g); `research/temporal-lifecycle.md` sections 1.2, 1.3 and 8(c).
