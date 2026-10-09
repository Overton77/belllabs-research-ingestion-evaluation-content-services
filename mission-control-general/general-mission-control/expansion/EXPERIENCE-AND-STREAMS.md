---
type: Specification Annex
title: "Experience, durable review and streaming contracts"
description: "The required release surfaces are canonical Agent Skill plus missionctl, remote MCP server, web dashboard and mobile application. Desktop packaging is deferred. A responsive web prototype is a proof artifact, not…"
tags: [mission-control, spec, expansion]
---
# Experience, durable review and streaming contracts

Status: normative target extension of [SPECIFICATION](../SPECIFICATION.md), authored 2026-10-03. These are proposed implementation contracts, not claims of deployed capability. [Technology evidence](TECHNOLOGY-EVIDENCE.md) records verified source support and qualification gaps. The existing HTTP/SSE, CLI, MCP, application binding, event sequence, command receipt and Human Task contracts remain authoritative; this annex adds required WebSocket, generative UI, MCP Apps/MCP-UI and mobile surfaces. It does not create another kernel or event ledger.

## Product and authority boundaries

The required release surfaces are canonical Agent Skill plus `missionctl`, remote MCP server, web dashboard and mobile application. Desktop packaging is deferred. A responsive web prototype is a proof artifact, not acceptance of the mobile application release. Mobile must pass installed iOS/Android shell or application qualification with authentication, reconnection, notifications, document access and durable reviews; a wrapped web shell is admissible only if those tests pass.

Both product dashboards mount the shared experience through their own app-authenticated integration. Product-specific navigation, templates, educational rubrics and entity links may differ. No UI reads app B using app A credentials or directly writes Mission Control execution tables. Python owns the service and public schema authority; generated TypeScript types and UI builds do not require a TypeScript runtime kernel.

UI, coordinator, CLI and skill have identical authorization and command semantics. The dashboard is not a privileged operator. Application-owned notification adapters and entity navigation never acquire approval or domain-write authority merely because they render a link or card.

## Sandboxed interview to executable workflow

An interview is a bounded authoring agent session attached to a draft. It is recoverable conversational authoring, not a Mission Run and not Temporal program state. Persist it in interview_session/interview_turn rather than inventing an attempt-owned agent_session; authoring budgets/effects have an explicit interview owner variant in the common ledger. Session creation uses an admitted interview capability/environment with the same sandbox, context selection, quota and credential isolation controls as other agent execution. Default grants are Mission Control/catalog read plus draft authoring. The session can search authorized missions and capabilities, inspect receipts and exemplar outputs, and propose complete programs. It cannot start missions, install capabilities or publish artifacts unless the actor has those grants and uses the corresponding admitted handler.

Proposed request extension, relative to the fixed `/v1/applications/{application_id}` prefix:

```text
POST /interviews
InterviewCreate@1 {
  request_id, tenant_id, draft_ref?, environment_profile_ref,
  admitted_agent_config_ref, initial_message_ref, context_request_ref,
  budget_profile_ref
}
-> {session_id, draft_ref, session_version, state, event_cursor}

POST /interviews/{session_id}/turns
InterviewTurnSubmit@1 {
  request_id, expected_session_version, message_ref,
  attachment_refs[], delivery_mode: "next_turn"
}
-> {message_id, delivery_receipt_ref, session_version}
```

These are new operation-catalog entries using existing scoped idempotency and public error envelopes. Authenticated scope determines tenant access; body tenant selectors are verified. Interview events live in an explicitly scoped interview stream until a mission exists; they never use a fictitious mission UUID. Linking a draft/session to a committed mission records a reference, not a renumbering of prior events. Interview stream persistence needs its own contract/migration before enablement.

The session refines: desired outcome, rubric, source/evidence scope, artifact schema, tool/effect authority, stage/goal-loop selection, reviewers, budgets, environment and stop conditions. A proposed program is validated against current capability versions and runtime support before being shown as executable. The UI presents the immutable candidate digest, unresolved warnings, effects needing approval, expected artifacts and allocation ceilings. Commit/start is a separate explicit admitted operation; a chat sentence or generated button label is not an approval token. Lost start responses recover through the existing request receipt rather than starting another run.

Coordinator agents discover, configure, spawn, track, intervene and revise via MCP/CLI/skill using the common operation catalog. Their discovery result includes installation/capability versions, grants and unavailable behaviors. Search relevance is advisory; executable config is assembled only from admitted immutable manifests. A coordinator-generated change produces a draft/revision and a reviewed diff; it does not overwrite the running revision.

## Review, rejection and artifact navigation

Human Tasks are durable process records. Temporal waits for admitted resolutions; notification transport and browser lifetime do not control the gate. Gate creation transaction registers task, immutable review packet, target digest/version, permitted resolutions, assignee policy and event/outbox entry. The task references exactly what is being approved: revision, artifact revision set, effect intent, ingestion manifest or acceptance assessment. Changing that target invalidates the outstanding approval for the changed action and requires a new task/version.

```text
ReviewPacket@1 {
  target_ref, target_digest, target_version, request_summary,
  evidence_refs[], artifact_revision_refs[], rubric_ref,
  allowed_decisions[], effect_authority_ref?, feedback_schema_ref,
  expires_at?, on_timeout, supersedes_task_ref?
}
ReviewDecision@1 {
  request_id, human_task_id, expected_task_version,
  target_digest, decision: "approve" | "reject" | "request_changes" | "abstain",
  feedback_ref?, selected_evidence_refs[]
}
```

`ReviewDecision` is a payload variant of the existing Human Task resolve operation, not a second endpoint that bypasses `human_resolution`. Actor identity and decision time are server-attested. `request_changes` requires structured feedback identifying criteria and desired changes. Concurrent decisions serialize through the task version and single accepted resolution: losers receive the existing version/conflict receipt; repeated identical request IDs recover the winner. Offline decisions remain client drafts, never preapproved queued actions. On reconnect the client reloads the task/version and asks the actor to submit against the current packet.

Feedback is durable input to the next authorized revision activation with provenance. It cannot silently expand domain permissions or budget. Revision creates new immutable artifact registrations and `supersedes` lineage; the rejected bytes, reviews and assessments stay inspectable. Revised output must pass validation/evaluation and a fresh gate when policy requires review. Mission acceptance, artifact approval, knowledge-service admission and app publication remain distinct transitions.

The output page supplies manifest, source citations, PDF/report or patch/test/experiment receipts, acceptance disposition, big-decision records, lineage and authorized app links. A deploy URL is a registered publication receipt with owning application and environment, never an agent's unverified claim. URL policy permits registered HTTPS origins/app links, prevents script/file schemes and requires reauthorization for sensitive artifacts. Signed byte URLs are short-lived and do not appear in traces or durable notification bodies. Preview untrusted PDF/HTML separately from the app origin, with download fallback and content-type verification.

Notifications have durable delivery intents keyed by `(human_task_id, task_version, recipient, channel)`; resolution/supersession can mark an intent obsolete. A worker persists delivery attempt and provider receipt, retries transient failures with the same delivery identity, records unknown outcomes and reconciles. External providers may duplicate notifications after response loss; duplicate delivery never duplicates a decision. In-app inbox is required; mobile push is required for configured devices. Email/chat channels are optional admitted integrations, not assumed connected. Notification bodies use safe summaries and authenticated deep links; clinical/private source details are excluded by policy. Expiration and escalation behavior belongs to the task policy, independent of whether the recipient saw a notification.

## Generative UI and MCP Apps contract

Generative UI is required. The model chooses an approved component plus schema-valid props/action references; it never ships arbitrary generated JavaScript into the trusted dashboard. Initial component catalog: mission proposal/diff, stage/goal progress, capability picker, evidence comparison, artifact/report preview, experiment results, review packet, budget/usage and intervention receipts. Application educational/recommendation components reuse evidence/provenance refs and label uncertainty; public evidence and personal conclusions remain separate.

```text
UiPresentation@1 {
  presentation_id, component_ref, component_version,
  props_schema_ref, props_artifact_ref, source_resource_refs[], as_of_seq,
  allowed_action_refs[], expires_at?, fallback_text
}
UiActionIntent@1 {
  request_id, presentation_id, action_ref, target_ref,
  expected_resource_version, payload_artifact_ref
}
```

The service resolves authorized props/payload artifacts and validates props, resource authorization and registered actions. Resolved `props` is strict against the component's pinned schema; no arbitrary HTML, executable callback or credential field is allowed. The strict seed wire profile in schemas.json carries artifact refs; hosts receive validated bounded data after dereference. Actions map to canonical operation handlers and current grants. A stale card receives `VERSION_CONFLICT`, displays a refreshed diff and does not retry a changed mutation automatically. Partial streamed props are loading previews; they do not enable effects or review submission until validated final data exists.

MCP Apps is the required interoperable rendering protocol; MCP-UI is a required host/framework integration qualification against that protocol. Do not create a separate legacy pre-standard MCP-UI wire dialect. Proposed initial resource `ui://mission-control/mission-detail/v1` is an immutable reviewed HTML/JS/CSS bundle served by the Python MCP resource adapter with `text/html;profile=mcp-app`. UI-enabled tools declare `_meta.ui.resourceUri`, return meaningful text/structured results and expose the same mission inspection/review/intervention handlers. Capability negotiation checks `io.modelcontextprotocol/ui`; unsupported hosts receive the same authorized summary plus canonical dashboard/resource links and ordinary tools.

The dashboard prototype mounts MCP-UI `AppRenderer` (or qualified compatible wrapper) against the Python MCP service. The evidence harness must also exercise the extension SDK bridge with the same bundle to detect wrapper-specific assumptions. Browser host uses a distinct sandbox origin and the MCP Apps handshake/message protocol, validates frame source/session, proxies allowlisted tool/resource calls and caps message sizes/rates. UI resource digest is pinned in deployment config. No app/domain tokens, provider secrets, storage cookies or direct database grants enter the iframe. Direct iframe network access defaults to none; approved origins are minimal and declared in CSP. No nested frames or camera/microphone/location/clipboard permission by default. The host can further restrict requested capabilities and never loosens declared CSP. Opening links requires URL policy; UI-to-model context updates are bounded data and cannot elevate authority.

The mobile application supports approved native components for every critical path and qualifies embedded Apps only in an isolated WebView/host bridge. WebView security, custom scheme handling, origin/message validation, safe area/touch/keyboard behavior, login return routing, token isolation and background/resume behavior must be tested on both mobile platforms. Capability handshake can request mobile context; protocol fields do not prove a mobile host exists. If isolation or bridge conformance is unavailable, render the native component/text fallback and open the authenticated web dashboard for the rich view. Review and stop controls remain usable through fallback. Third-party host support is recorded per host/build/protocol/transport; no claim of blanket ChatGPT/Cursor/mobile Apps parity is permitted.

## Durable events and transport adapters

Keep canonical `mc.event.v1` mission events and per-mission sequence from [RUNTIME-CONTRACTS](../RUNTIME-CONTRACTS.md). Extend the envelope additively only through schema review; the proposed full shape below explains required values, not independent naming authority:

```text
MissionEvent@1 {
  schema_version: "mc.event.v1", scope: {installation_id, application_id, tenant_id},
  mission_id, event_id, event_type, event_version, seq, ledger_commit_id,
  occurred_at, recorded_at, run_id?, revision_id?,
  execution: {node_key?, activation_id?, attempt_no?, harness_execution_id?,
              native_session_ref?, native_turn_ref?, generation?},
  source: {kind, actor_ref?, native_event_ref?},
  causation_id?, correlation_id?, payload_ref
}
```

The seed wire profile uses an authorized `payload_ref`; resolved payload is strict against its registered event schema. The base workflow's bounded inline payload remains an internal admitted representation; MC-P001 can add a reviewed oneOf wire variant if needed, never an arbitrary event blob. Timestamps are UTC strings; sequence/version counters are integers; integer micros in Python/SQL use decimal-digit strings where needed for 64-bit-safe client handling. Durable event types retain `human_task.opened`, `human_task.resolved`, `artifact.registered` and other existing families, plus approved additions `assessment.recorded`, `usage.observed`, `usage.settled`, `reconciliation.required` and public progress summaries. Event types describe facts already admitted, not desired actions. Unknown versions display safely but cannot drive client mutations. UUIDv7 event identity and normalized field names in workflow 09 are the single new-engine wire vocabulary.

Producer observations include native event identity, attempt/generation and adapter binding. Admission deduplicates native identity, rejects mutation by stale generation and assigns canonical sequence in the same app-database transaction as business state/outbox. Native provider offsets or LangGraph stream chunks are never mission sequence. Replayed outbox delivery is at least once. Consumer deduplication uses scoped `(mission_id, seq)` and verifies event identity; conflicting bytes for that key are an integrity incident. Harness diagnostics, raw token streams and logs are not business transitions.

FastAPI SSE and required WebSocket adapter read the same durable events/projections. Socket-local queues or pub/sub notifications only wake readers; the database event tail is recovery authority. An outbox relay crash, API process death or missed wake-up cannot silently lose admitted events. No guaranteed total order across missions is promised. Cross-mission analytics uses an independent ingestion watermark/vector of mission cursors; it never compares two mission sequence numbers as a global clock.

```text
POST /stream-tickets
{request_id, subscriptions: [{mission_id, after_seq}], profile_ref}
-> {ticket, expires_at, endpoint}

WS /streams?ticket=<single-use-short-lived-ticket>
Client: {schema:"mc.stream_control.v1", type:"subscribe", subscriptions:[...]}
Client: {schema:"mc.stream_control.v1", type:"ack", mission_id, through_seq}
Server: {schema:"mc.stream_frame.v1", type:"event", event: MissionEvent@1}
Server: {schema:"mc.stream_frame.v1", type:"heartbeat", recorded_at}
Server: {schema:"mc.stream_frame.v1", type:"resync_required", mission_id,
         retained_floor, snapshot_ref, snapshot_seq, code:"CURSOR_EXPIRED"}
Server: {schema:"mc.stream_frame.v1", type:"auth_expiring", reconnect_required:true}
```

All paths use the application prefix. Ticket issuance intersects subscriptions with current mission-read grants; tickets are hashed server-side, single-use, brief, bound to actor/application/tenant and accepted origins. Query strings and tickets are redacted from logs. Browser cookies require origin/CSRF policy. Recheck authorization on subscription and bounded renewal/long-lived connection interval; revocation closes affected subscriptions. Token expiry requires fresh ticket/reconnect; never put bearer tokens in frame payloads. This ticket is an ephemeral transport credential, not mission state. Commands continue through HTTP/MCP admitted handlers; a socket carrying a command must call the identical handler and return the persisted receipt.

Replay algorithm: authorize; read durable tail `H`; replay `(after_seq,H]` in sequence; then tail `>H`. Read/tail handoff cannot rely exclusively on a live notification subscription. Ack tracks last applied sequence; client reconnects from its own last applied cursor and tolerates duplicate receipt. Client expects next sequence before applying incremental state; on a gap it fetches the missing range rather than advancing. A snapshot is authorization-filtered and includes atomic `snapshot_seq`; apply snapshot then replay `>snapshot_seq`. If history is compacted beyond a cursor, explicit `CURSOR_EXPIRED` triggers resync; neither silent skip nor replay of an unrelated mission is allowed. Filtered display must retain the original cursor frontier, including redacted/omitted-event advance metadata, so clients do not confuse hidden events with gaps.

Persisted projections (`run state`, `mission summary`, `review inbox`, `artifact lineage`, `usage summary`) update idempotently from events with per-stream last-applied cursor. Rebuilding after a crash is deterministic. Projector rollback/restart never executes tools. Each view returns `as_of_seq`/watermark, projection schema version and lag; the UI distinguishes authoritative read from lagging derived view. Search results and analytics cannot authorize actions; handlers reload current authoritative state.

Backpressure profile is versioned deployment configuration, initially proposed for local proof: max frame 24 KiB; larger bodies become artifact/resource refs; at most 100 subscriptions/connection, 256 pending durable frames or 4 MiB buffered, 30-second heartbeat and bounded reconnect jitter. These are design test limits, not vendor quotas or production capacity claims. Slow consumers receive a safe disconnect/resync instruction and recover from their applied cursor. Durable events are never dropped to fit a queue. Ephemeral previews may coalesce/drop with `ephemeral:true`, `stream_id`, monotonic local offset and explicit lost-preview indicator; final persisted message/artifact refs recover the meaningful outcome. Disconnect/push suspension never pauses mission execution by itself.

## Progress, accounting and observability

Public progress is a concise attributable summary: current stage/goal, completed work, next action, blockers, references and timestamp. It contains no hidden chain of thought, private model deliberation, raw prompts or credential-bearing tool logs. Parent/subagent progress records parent/child/attempt identities and visibility classification. Raw `messages`, `updates` or debug streams are filtered/redacted by the adapter before export; custom progress schema is preferred. Private recovery/checkpoint state is not a public event payload. Usage measurements from streamed provider chunks remain observations until normalized/reconciled.

Usage ledger extends existing reservations/settlement, using `(provider, native_request_id, usage_component)` identity plus source observation revision. Record provider/model/version, input/output/cached/reasoning-token components where supplied, sandbox resource duration/units, tool usage, currency, decimal quantity, price-sheet ref/version/effective time and amount or `unknown`. The pricing formula is explicit per component; cached inputs never count twice. Corrections append reconciliation records rather than editing historical receipts. Unknown provider usage remains visible and reserves conservative headroom under policy; it does not become zero because a stream disconnected. No fixed dollar estimates are part of this spec.

Analytics: per-app/tenant outcomes, accepted/rejected artifact revisions, review turnaround, event/projector lag, stale-command conflicts, redelivery/duplicates/gaps, cancellation uncertainty, adapter failures, token/resource allocation versus settled/unknown spend, retrieval/evaluation quality and experiment provenance. Aggregate personal or sensitive evidence only under owning app policy. SQL views/read models are separate from domain knowledge entities; traces correlate request/run/attempt/effect/command/provider handles but do not replace ledger receipts.

## Acceptance and foundational issue slices

These slices are inputs to the canonical dependency-linked issue backlog; its stable IDs/owners govern implementation.

| Slice | Prerequisites | Required acceptance evidence |
| --- | --- | --- |
| MCP Apps/MCP-UI host proof | Pinned dependency inventory, Python MCP read handler, fixture auth | Same bundle renders through MCP-UI and extension bridge; hostile origin/tool/URL/CSP attempts denied; no-token iframe; text/native fallback works |
| Durable event/WS proof | Event/outbox migrations, scoped authorization, authoritative state fixtures | Kill relay/API/projector at each boundary; reconnect reconstructs identical state; expired cursor resync, gap detection and slow-consumer recovery; app A cannot stream app B |
| Durable HITL proof | Existing Human Task transaction contract, review packet schema, notification fixture | Concurrent approve/reject yields one resolution; stale target rejected; notification replay duplicates no effect; process/browser restart preserves wait; revision requires fresh applicable approval |
| Interview and generative dashboard vertical | Catalog/context admission, sandbox interface, author/validate/commit/start, prior proofs | Bounded session produces validated full workflow; unavailable capability blocks start; stale generated card cannot mutate; report and feature outputs have lineage/navigation |
| Mobile qualification | Canonical client schemas, auth/session/deep-link contracts, prior proofs | iOS/Android background/resume, push/deep link, login, PDF access and review parity; WebView attack cases or native fallback; no offline automatic approvals |
| Usage/projection integration | Reservation/settlement handlers, adapter usage fixtures | Duplicate/corrected/unknown native usage accounted; projection rebuild matches golden state; global analytics never misuses per-mission order |

Use deterministic fake providers and notification transports first. Prototype proof fixture has two isolated app/tenant bindings, interleaved streams, rejection/revision/approval and a cancellation-uncertain tool. Models are unnecessary for stream/security/HITL proofs. Frontier-model interview evaluation is a separately authorized bounded experiment after deterministic gates, with declared token/call/sandbox quotas; abort on scope leakage, missing effect authorization, corrupted sequence, unbounded resource use or ambiguous billing. Passing static design review does not authorize metered missions.
