---
type: Specification
title: "SPEC-03: Provider frames, mission-state derivation from closing frames, transcript materialization and run search"
description: "Specifies the Native Event Store (mission_control.provider_frame and mc.provider_frame.v1), the FrameSink port and the writers for Deep Agents, Cursor Local, Cursor Cloud and hook invocations, the writers for harness_execution, agent_session and session_turn, the Reducer rules that derive turn facts and mission events only from closing frames, the Transcript materialization (mc.transcript_entry.v1) with JSONL and Markdown renderers and redaction, and run listing and search through Temporal search attributes and a transcript index. Elaborates ADR-0028."
tags: [mission-control, spec, fast-track, events]
---

# SPEC-03: Provider frames, mission-state derivation, transcript materialization and run search

Elaborates [ADR-0028](../../adr/0028-native-event-store-provider-frames-and-materialized-transcript.md). Owner requirements 2b ("a first iteration of storing mission state as the mission ensues by hooking into the environment and lifecycle of the provider") and 2e ("query for and materialize a full mission's transcript/traces"). Normative inputs: `workflow-types/09` sections 2 to 6 (Mission Event envelope, vocabulary, Native Event Store, delivery guarantees), `workflow-types/05` section 2 (identity hierarchy and native identity mapping), ADR-0007, ADR-0004. Code facts: [research/codebase-map.md](research/codebase-map.md) sections 5, 6 and gap (d); frame envelopes per lane from `docs/research/2026-10-07-coding-lane-surfaces.md` ("Deterministic state accumulation"); Temporal visibility from [research/temporal-lifecycle.md](research/temporal-lifecycle.md) section 4.

## Problem Statement

The specification separates a canonical mission stream from raw provider observations, but the code persists neither. `harness_execution`, `agent_session` and `session_turn` exist in SQL with no Python writer; `native_observation` is used only by async subordinates; `RecordedOperationEventSink` keeps digests in memory and logs them; the inspection and operation code explicitly excludes transcripts; tracing goes to LangSmith, which is outside the ledger and tied to one lane. Consequences: a worker crash loses what the provider said between checkpoints; the reducer cannot derive turn facts, tool effects or usage from evidence; an operator or agent cannot ask "what did this run do" without reading provider dashboards; and a second lane (Cursor) would have nowhere to put its stream. The lifecycle of Deep Agents, Cursor Local and Cursor Cloud must be synthesized into one mission state, and that synthesis needs a durable, provider-neutral record.

## Solution

Every event a lane observes from its provider or from a hook is a **Provider Frame**, persisted verbatim (body digested and excerpted above a size cap) in the **Native Event Store** table `mission_control.provider_frame` by the observing worker, in arrival order, before any derivation. Frames are keyed by `(harness_execution_id, generation, provider_key, arrival_ordinal)`, so a resumed activity re-persists idempotently and a stale generation's frames are fenced. The **Reducer** reads only *closing* frames (turn ended, tool call completed, run result) and writes the mission events of workflow-types/09 (`session.*`, `tool_call.completed`, `attempt.completed`) pointing back by `native_event_ref`. The lane-neutral writers for `harness_execution`, `agent_session` and `session_turn` are added. The **Transcript** is a materialized read view (`mc.transcript_entry.v1`) that joins mission events, frames and artifact references in sequence and renders JSONL (for agents) and Markdown (for humans) with a cursor; it is reachable from CLI, HTTP and MCP. Runs become listable and searchable through Temporal search attributes and a full-text index over transcript entries.

## User Stories

1. As an operator, I want every provider event persisted before it is interpreted, so that a worker crash never loses what the agent did.
2. As an operator, I want frames keyed by harness execution and generation, so that a stale process cannot pollute a resumed attempt.
3. As a reducer, I want to derive an attempt's phase, tool effects, usage and terminal outcome from closing frames only, so that partial deltas never move mission state.
4. As a reducer, I want each derived mission event to point at its frames, so that any state transition can be audited to raw evidence.
5. As a lane implementer, I want one `FrameSink` port with an idempotent `append`, so that Deep Agents, Cursor Local and Cursor Cloud share one persistence path.
6. As a lane implementer, I want the dedupe key per lane documented, so that resuming a stream does not duplicate frames.
7. As a hook script author, I want hook invocations recorded as frames, so that denials and context injections appear in the transcript.
8. As an operator, I want `missionctl run transcript RUN_ID --format md` to show the run in order with tool calls, decisions, human tasks and artifacts, so that I can review a mission without a dashboard.
9. As a coordinator agent, I want `missionctl run transcript RUN_ID --format jsonl --since <cursor>` to tail new entries, so that I can react to a run cheaply.
10. As a coordinator agent, I want an MCP resource for the transcript, so that I can read it without a shell.
11. As an operator, I want frame bodies above a cap stored as digest plus excerpt, so that the ledger stays bounded and full bodies live as artifacts when they matter.
12. As a security reviewer, I want no secret, token or credential in any frame or transcript, so that the store can be shared with agents.
13. As an operator, I want frames retained per application for a configurable period (30 days default) and mission events kept forever, so that cost is bounded without losing canonical state.
14. As an operator, I want usage recorded with a disposition (`settled`, `estimated`, `unknown`), so that Cursor's late billing and Claude's client-side estimate are never treated as settled cost.
15. As an operator, I want `missionctl run list --query "lane='cursor_local' AND phase='executing'"` to answer from Temporal visibility, so that listing active runs does not scan the ledger.
16. As a coordinator agent, I want `missionctl run search RUN_ID --query "pytest failed"` to find transcript entries, so that I can locate what happened without reading everything.
17. As a fork operator, I want the forked run's `mc_forked_from_run_id` attribute set, so that lineage is a visibility query.
18. As a Goal Loop, I want `session.turn_completed` to carry the turn's usage and result summary reference, so that Progress Review has settled numbers.
19. As a Stage Graph, I want `attempt.completed` derived from the run result frame plus the Completion Candidate, so that a provider `FINISHED` is never by itself `accepted`.
20. As an application owner, I want frames and transcripts scoped by installation, application and tenant under RLS, so that one application never reads another's.
21. As a dashboard, I want a non-canonical live tail over frames labelled as such, so that users see text streaming without it becoming state.
22. As an implementer, I want the three unused identity tables to gain writers that run in the same transaction as the first frame of a session or turn, so that identity and evidence never disagree.
23. As an operator, I want compaction observed as `session.compaction_observed` with the summary digest, so that I know when the provider shortened the agent's memory.
24. As an operator, I want a transcript entry for every artifact registration with its digest, so that outputs are visible inline with the work that produced them.
25. As an implementer, I want the reducer rules per lane in one table, so that adding a lane is adding rows, not branches.

## Contracts

### `mc.provider_frame.v1`

```text
ProviderFrame@1 {
  schema_version: "mc.provider_frame.v1"
  frame_id                          // UUIDv7, assigned at write
  scope: {installation_id, application_id, tenant_id}
  run_id, activation_id, attempt_no
  harness_execution_id              // the adapter record binding this attempt to its sessions (05 §2)
  generation                        // execution generation fencing the launch
  lane_profile: deep_agents | cursor_local | cursor_cloud | claude_agent_sdk | codex
  native_session_ref, native_turn_ref?    // per 05 §2 mapping
  provider_key                      // lane-specific dedupe key, see Dedupe keys
  arrival_ordinal                   // worker-assigned, monotonic per (harness_execution_id, generation)
  observed_at                       // worker clock at observation
  provider_timestamp?               // provider clock when present
  kind: FrameKind                   // provider-neutral classification, see below
  closing: bool                     // true when the reducer may read it
  subordinate_ref?                  // subagent attribution: Deep Agents ns/lc_agent_name, Claude parent_tool_use_id, Cursor nested task id
  tool_call_ref?                    // stable tool call id when kind concerns a tool
  body_digest, body_bytes, body_media_type
  body_excerpt                      // first N bytes (cap, default 8 KiB) of the canonical JSON body
  body_artifact_ref?                // when the full body was registered as an artifact
  redactions[]                      // paths redacted (secret patterns) before persistence
  raw_kind                          // the provider's own type string, verbatim
}

FrameKind = session_init | session_state | turn_started | message_delta | message | thinking_delta
          | tool_call_started | tool_call_delta | tool_call_completed | tool_call_failed
          | approval_requested | approval_resolved | hook_invoked | hook_result
          | before_compaction | after_compaction | usage | turn_ended | run_result
          | status | error | heartbeat | unknown
```

`closing = true` exactly for `tool_call_completed`, `tool_call_failed`, `approval_resolved`, `hook_result`, `after_compaction`, `usage`, `turn_ended`, `run_result`, `session_state` and `error`. Deltas and starts are persisted but never read by the reducer.

### Native identity records (writers for existing tables)

- `harness_execution` (05 §2 "Harness Execution"): `harness_execution_id, run_id, activation_id, attempt_no, lane_profile, generation, native_identity jsonb` (`agent_runtime_kind`, `native_session_ref`, `native_turn_refs[]`, `launch_key`, `observation_cursor`), `lifecycle`.
- `agent_session`: `agent_session_id, harness_execution_id, native_session_ref, started_at, ended_at, transferred_to?`.
- `session_turn`: `session_turn_id, agent_session_id, turn_ordinal, native_turn_ref?, started_frame_id, ended_frame_id, usage jsonb (tokens per dimension with disposition), result_summary_ref, stop_reason`.

Columns beyond the existing DDL (`mig/0003`) are added in migration `0027`; existing columns are honoured, not redefined. Writers run inside the frame-append transaction when a `session_init`, `turn_started` or `turn_ended` frame arrives.

### Usage disposition

```text
UsageReport@1 { dimensions: { input_tokens, cached_input_tokens, output_tokens, reasoning_tokens, cost_micros }
                each: { value, disposition: settled | estimated | unknown, source_frame_id } }
```

`unknown` is never zero. Lane rules: Deep Agents tokens `settled` from the model end event, cost `estimated` from the route's price table; Cursor tokens `settled` from `TurnEndedUpdate`/`usage`, cost `estimated` until `get_usage` returns `charged_cents`, then a later `usage` frame flips it to `settled`; Claude (future) `total_cost_usd` is `estimated` by definition.

### `mc.transcript_entry.v1`

```text
TranscriptEntry@1 {
  schema_version: "mc.transcript_entry.v1"
  cursor                            // opaque, monotonic within a run: (mission seq, frame arrival ordinal) encoded
  run_id, activation_id?, attempt_no?, node_key?, iteration_id?, subordinate_ref?
  at                                // recorded_at of the event or observed_at of the frame
  source: mission_event | provider_frame | artifact | human | command
  kind                              // event_type, FrameKind, "artifact.registered", "human_task.resolved", command kind
  role?: system | agent | tool | human | mission_control
  title                             // one line, rendered
  body_excerpt?                     // bounded, redacted
  refs: { event_id?, frame_id?, native_event_ref?, artifact_ref?, tool_call_ref?, command_id?, human_task_id? }
  usage?: UsageReport@1             // on turn_ended entries
  canonical: bool                   // true for mission events, false for frames
}
```

## Implementation Decisions

### Native Event Store table and write path

`mission_control.provider_frame` is written by `PostgresFrameRepository.append(frames: Sequence[ProviderFrame])` in one statement per batch with `ON CONFLICT (harness_execution_id, generation, provider_key) DO NOTHING`; the returned row count tells the writer how many were new. `arrival_ordinal` is assigned by the writer from its in-memory counter seeded by `SELECT max(arrival_ordinal)` for the `(harness_execution_id, generation)` at activity start, so a resumed activity continues the sequence. A frame with a generation older than the harness execution's current generation is rejected with `STALE_GENERATION` and counted, never stored.

Body handling: canonical JSON of the provider payload is digested (`sha256`), redacted (patterns: bearer tokens, `sk-`, `CURSOR_API_KEY`, `TAVILY_API_KEY`, `FIRECRAWL_API_KEY`, `NCBI_API_KEY`, URLs with `?token=`/`?key=`, the configured secret-reference list), then stored as `body_excerpt` up to `frame_excerpt_cap` (application setting, default 8 KiB). A body above the cap whose kind is `tool_call_completed`, `message` or `run_result` is also registered as an artifact of kind `provider_frame_body` through the normal promotion path when the lane profile's `persist_full_bodies` flag is on (default on for `tool_call_completed` with `body_bytes <= 2 MiB`, off above). Deltas are never promoted.

Retention: `mission_control.frame_retention_policy(application_id, retain_days default 30, keep_closing_frames bool default true)`; a scheduled `frames.expire` activity deletes non-closing frames older than `retain_days` and, when `keep_closing_frames` is false, closing frames too. Mission events are never deleted; a transcript rendered after expiry shows `frame_expired` placeholders with the digest.

### FrameSink port and writers

`FrameSink` (application port, `application/frames/sink.py`): `append(frames) -> AppendReceipt(new, duplicate, stale)`, `last_cursor(harness_execution_id, generation) -> ProviderCursor | None`. It replaces `RecordedOperationEventSink` in `adapters/operations/runtime_ports.py`; the in-memory implementation stays for unit tests.

Writers (frame producers) and their dedupe keys, from the coding-lane research and Deep Agents research:

| Lane | Observation surface | `provider_key` | `native_session_ref` / `native_turn_ref` | Notes |
| --- | --- | --- | --- | --- |
| `deep_agents` | `astream(..., stream_mode=["updates","messages","custom"], subgraphs=True, version="v2")`, plus `astream_events(version="v2")` when `run_id`/`parent_ids` lineage is needed | `(thread_id, checkpoint_ns, checkpoint_id, step, message_id or tool_call_id, kind)` joined with `:` | thread id / invocation id (LangGraph run id) | subordinate attribution from `ns[0].startswith("tools:")` and `metadata.lc_agent_name`; `task` tool calls join to subagent runs by `tool_call_id` at completion |
| `cursor_local` | `run.observe(after_offset)` over `RunStreamEvent{kind, offset}` plus `on_delta`/`on_step` callbacks | bridge event `offset` (durable for `ObserveRun`) | `agent_id` / `run_id` | `call_id` is `tool_call_ref`; no native turn id, turn ordinal derived from `TurnEndedUpdate`/`usage` count |
| `cursor_cloud` | SSE `GET /v1/agents/{id}/runs/{runId}/stream` with `Last-Event-ID` | SSE event `id` | `agent_id` / `run_id` | `410 stream_expired` → `GET run` produces a synthetic `run_result` frame with `provider_key = run:<run_id>:final` |
| hooks (all lanes) | kernel and catalog hook invocations (SPEC-01) | `hook:<event>:<tool_call_ref or session>:<invocation ordinal>` | inherited | two frames per invocation: `hook_invoked` (input digest) and `hook_result` (decision) |
| Claude Agent SDK (future) | `receive_messages()` | `message.uuid` (fallback `(session_id, message_id, block index)`) | `session_id` / result `uuid` | listed for completeness; not built in this packet |

Each writer maps the provider's `raw_kind` to a `FrameKind` through a per-lane table in `application/frames/kinds.py`; unmapped kinds become `unknown` with `closing = false` and are counted in a metric so new provider events surface.

### Lifecycle synthesis: provider event → frame → reducer fact → mission event

The Reducer (`domain/policies/reducer.py`, new `apply_frame_facts` action family) consumes `FrameFacts` built by `application/frames/reducer.py::derive(closing_frames)`; it never sees deltas. The table below is the normative mapping; adding a lane adds rows.

| Provider event | Lane | FrameKind | Reducer fact | Mission event (09 §3) |
| --- | --- | --- | --- | --- |
| first `values`/`updates` chunk on a new thread; `system/init` | deep_agents | `session_init` | agent session started, native ids bound | `session.started` |
| `Agent.create` return; `status: CREATING→RUNNING` | cursor_local / cloud | `session_init`, `status` | session started; attempt phase `executing` | `session.started`, `activation.phase_changed{executing}` |
| first `send` acknowledged; `turn/started` | all | `turn_started` | turn ordinal n opened | `session.turn_started` |
| `on_tool_end` / `ToolMessage` in `updates`; `tool_call{status: completed}`; `item/completed` | all | `tool_call_completed` | tool effect settled: name, status, args digest, result digest, exit code when shell | `tool_call.completed` |
| `on_tool_error`; `tool_call{status: error}` | all | `tool_call_failed` | tool effect failed | `tool_call.completed{status: failed}` |
| `request{request_id}`; HITL `__interrupt__`; `session_state_changed{requires_action}` | all | `approval_requested` | phase `awaiting_human` | `activation.phase_changed{awaiting_human}`; Human Task opened by the family when policy maps it |
| hook `preToolUse` deny | all | `hook_result{deny}` | effect denied before claim | `tool_call.completed{status: denied}` |
| `_summarization_event` appeared; `summary-completed` | deep_agents / cursor | `before_compaction`, `after_compaction` | compaction observed, summary digest | `session.compaction_observed` |
| model end event with usage; `TurnEndedUpdate`; `usage` | all | `usage`, `turn_ended` | turn n closed with usage dispositions and result summary ref | `session.turn_completed` |
| `RunResult{status}`; graph run end; `result` SSE | all | `run_result` | execution outcome per 05 §3.5 (`finished→succeeded` subject to Completion Candidate; `error→failed(provider_error)`; `expired→failed(timeout)`; `cancelled→cancelled(cancelled_by_command)` when a command caused it) | `attempt.completed{outcome, failure_class?}`; `session.ended` when the lane ends the session |
| stream gap, `410`, bridge death with unknown state | all | `error{unknown_state}` | unit `in_doubt` | `activation.lifecycle_changed{waiting}` with blocker `in_doubt` (reconciliation path, unchanged) |
| continuation seal (SPEC-02) | mission_control | n/a (ledger fact) | checkpoint sealed, session transferred | `session.checkpoint_sealed`, `session.transferred` |

Rules: (1) a fact is derived only when its closing frame is persisted, so the mission event's `source.native_event_ref` always resolves; (2) `attempt.completed{succeeded}` additionally requires the declared outputs registered in a Completion Candidate (05 §3.4), otherwise `missing_output_policy` applies; (3) frames from a generation other than the current one produce no facts; (4) the reducer is deterministic: replaying the same closing frames in arrival order yields the same events (asserted in tests); (5) events carry references and digests, never bodies (09 §5).

### Transcript materialization

`application/frames/transcript.py::materialize(run_id, since: cursor | None, filters) -> Iterator[TranscriptEntry]` performs a streaming merge of three ordered sources: mission events for the run (by `seq`), closing and non-closing frames for the run's harness executions (by `arrival_ordinal`, interleaved by `observed_at` relative to the nearest mission event), and artifact registrations (already mission events, so no separate source; the artifact source enriches `artifact.registered` entries with digest and media type). Human task resolutions and commands are mission events and render with `role: human` or `mission_control`. Ordering key: `(seq_of_governing_event, arrival_ordinal)` where a frame's governing event is the latest mission event with `recorded_at <= observed_at`; the encoded pair is the `cursor`.

Renderers: `to_jsonl(entries)` (one entry per line, canonical JSON) and `to_markdown(entries)` (a heading per activation, a sub-heading per turn, bullet lines `HH:MM:SS · role · title`, tool calls as collapsed blocks with digest and excerpt, artifacts as links to `mc://` refs, canonical entries marked with `✓`, frames marked with `·`, expired frames as `(frame expired, digest …)`). Both apply the redaction list again at render time (defence in depth) and never include `body_artifact_ref` contents; `--full` on the CLI fetches referenced bodies through the artifact API with the caller's grant.

Filters: `--activation`, `--node`, `--kinds`, `--canonical-only`, `--subordinate`, `--since CURSOR`, `--limit`. A `since` cursor older than retention returns the canonical entries and `frame_expired` placeholders, never an error; a malformed cursor is `CURSOR_EXPIRED` (reuses the public error code).

### Run list and search

- **Search attributes.** Register `mc_mission_id` (Keyword), `mc_run_id` (Keyword), `mc_lane` (Keyword), `mc_phase` (Keyword), `mc_forked_from_run_id` (Keyword), `mc_application_id` (Keyword) with `temporal operator search-attribute create` (dev server: `--search-attribute` flags in `make temporal-up`); ticket G7 owns registration and the `temporalio` upgrade. The mission root sets them at start (`TypedSearchAttributes`), the family upserts `mc_phase` on phase change (`workflow.upsert_search_attributes([MC_PHASE.value_set(...)])`), the fork saga sets `mc_forked_from_run_id` on the new root.
- **`missionctl run list`** calls `GET /v1/applications/{app}/runs?query=` which translates a bounded filter grammar (`lane`, `phase`, `mission_id`, `forked_from`, `status`, `started_after`) into a Temporal list filter joined with the application's installation prefix on `WorkflowId`, calls `client.list_workflows(...)`, and enriches each row from the ledger (`lifecycle`, `terminal_outcome`). The ledger stays the authority; visibility is the index.
- **`missionctl run search RUN_ID --query`** calls `GET .../runs/{id}/transcript/search?q=` which queries `mission_control_search.transcript_document` (a projection row per transcript entry: `run_id, cursor, kind, role, title, body_excerpt, fts tsvector`) with `websearch_to_tsquery`, ranked by `ts_rank_cd`, returning entries with cursors so `run transcript --since` can open the neighbourhood. The projection is rebuilt by the same projection job family as the catalog (SPEC-01) and is never an authorization store.

### Non-canonical live tail

`GET .../runs/{id}/frames/tail` (SSE) streams frames as they are appended, labelled `canonical: false`; it is an ephemeral read over the Native Event Store for dashboards and the coordinator skill's `--follow` mode, and it never substitutes for the mission event stream (09 §5).

## Persistence

Migration `0027_provider_frames.sql` (team T2), additive, RLS-forced with the existing `mc.*` transaction-local scope:

```sql
CREATE TABLE mission_control.provider_frame (
  frame_id uuid PRIMARY KEY,
  installation_id uuid NOT NULL, application_id text NOT NULL, tenant_id uuid NOT NULL,
  run_id uuid NOT NULL, activation_id uuid NOT NULL, attempt_no integer NOT NULL,
  harness_execution_id uuid NOT NULL REFERENCES mission_control.harness_execution(harness_execution_id),
  generation integer NOT NULL,
  lane_profile text NOT NULL CHECK (lane_profile IN ('deep_agents','cursor_local','cursor_cloud','claude_agent_sdk','codex')),
  native_session_ref text NOT NULL, native_turn_ref text,
  provider_key text NOT NULL,
  arrival_ordinal bigint NOT NULL,
  observed_at timestamptz NOT NULL, provider_timestamp timestamptz,
  kind text NOT NULL, closing boolean NOT NULL, raw_kind text NOT NULL,
  subordinate_ref text, tool_call_ref text,
  body_digest text NOT NULL CHECK (body_digest ~ '^sha256:[0-9a-f]{64}$'),
  body_bytes integer NOT NULL, body_media_type text NOT NULL,
  body_excerpt bytea NOT NULL, body_artifact_ref text,
  redactions jsonb NOT NULL DEFAULT '[]'::jsonb,
  UNIQUE (harness_execution_id, generation, provider_key),
  UNIQUE (harness_execution_id, generation, arrival_ordinal)
);
CREATE INDEX provider_frame_run_order ON mission_control.provider_frame (run_id, observed_at, arrival_ordinal);
CREATE INDEX provider_frame_closing ON mission_control.provider_frame (harness_execution_id, generation, arrival_ordinal) WHERE closing;
CREATE INDEX provider_frame_tool_call ON mission_control.provider_frame (run_id, tool_call_ref) WHERE tool_call_ref IS NOT NULL;

CREATE TABLE mission_control.frame_retention_policy (
  application_id text PRIMARY KEY, retain_days integer NOT NULL DEFAULT 30 CHECK (retain_days >= 1),
  keep_closing_frames boolean NOT NULL DEFAULT true, excerpt_cap_bytes integer NOT NULL DEFAULT 8192);

ALTER TABLE mission_control.harness_execution ADD COLUMN IF NOT EXISTS lane_profile text, ADD COLUMN IF NOT EXISTS generation integer,
  ADD COLUMN IF NOT EXISTS native_identity jsonb, ADD COLUMN IF NOT EXISTS observation_cursor jsonb, ADD COLUMN IF NOT EXISTS lifecycle text;
ALTER TABLE mission_control.agent_session ADD COLUMN IF NOT EXISTS transferred_to uuid, ADD COLUMN IF NOT EXISTS ended_at timestamptz;
ALTER TABLE mission_control.session_turn ADD COLUMN IF NOT EXISTS started_frame_id uuid, ADD COLUMN IF NOT EXISTS ended_frame_id uuid,
  ADD COLUMN IF NOT EXISTS usage jsonb, ADD COLUMN IF NOT EXISTS result_summary_ref text, ADD COLUMN IF NOT EXISTS stop_reason text;

-- SPEC-02 dependencies if absent from the common component:
CREATE TABLE IF NOT EXISTS mission_control.context_selection (...);        -- see SPEC-02 Persistence
CREATE TABLE IF NOT EXISTS mission_control.continuation_checkpoint (...);  -- see SPEC-02 Persistence

CREATE TABLE mission_control_search.transcript_document (
  run_id uuid NOT NULL, cursor text NOT NULL, kind text NOT NULL, role text, title text NOT NULL,
  body_excerpt text, fts tsvector GENERATED ALWAYS AS (setweight(to_tsvector('english', coalesce(title,'')),'A') ||
                                                      setweight(to_tsvector('english', coalesce(body_excerpt,'')),'B')) STORED,
  PRIMARY KEY (run_id, cursor));
CREATE INDEX transcript_document_fts ON mission_control_search.transcript_document USING gin (fts);
```

Grants follow the existing six NOLOGIN capability roles: the worker role writes `provider_frame` and the identity tables; the API role reads; the projection role writes `transcript_document`. The exact `ALTER TABLE` list is reconciled against the real `mig/0003` DDL by the implementer (the columns above are the target, not a claim about what exists).

## Interfaces

| Surface | Operation |
| --- | --- |
| CLI | `missionctl run transcript RUN_ID [--format jsonl|md] [--since CURSOR] [--activation ID] [--node KEY] [--kinds a,b] [--canonical-only] [--follow] [--full]`; `missionctl run list [--query FILTER] [--json]`; `missionctl run search RUN_ID --query TEXT [--limit N]`; `missionctl run frames RUN_ID --tail` (debug) |
| HTTP | `GET /v1/applications/{app}/runs/{run_id}/transcript?format=&since=&...` (JSONL streamed or `application/json` array; Markdown with `Accept: text/markdown`); `GET .../runs/{run_id}/transcript/search?q=`; `GET .../runs?query=`; `GET .../runs/{run_id}/frames/tail` (SSE, non-canonical) |
| MCP | resource `mc://applications/{app}/runs/{run_id}/transcript[?since=]`; tool `mission_run_transcript(run_id, since?, format?)`; tool `mission_run_list(query)`; tool `mission_run_search(run_id, query)` |
| Grants | `mission.read` for all reads; `--full` body fetch additionally requires artifact read on the referenced artifacts |

Exit codes follow the CLI convention (0 read ok, 2 invalid request, 3 denied, 5 unavailable, 6 wait timeout for `--follow`).

## Insertion points

| Change | Path |
| --- | --- |
| Frame contract, kinds, closing rule | new `src/mission_control/domain/frames/contracts.py` |
| FrameSink port, append receipt, cursor | new `src/mission_control/application/frames/sink.py` (replaces `RecordedOperationEventSink` in `adapters/operations/runtime_ports.py`) |
| Per-lane kind maps and dedupe keys | new `src/mission_control/application/frames/kinds.py` |
| Deep Agents writer | `src/mission_control/adapters/deep_agents/adapter.py::execute` (`_ModelCallObserver` becomes a frame producer over `astream` v2 with subgraphs) and `adapters/deep_agents/compaction.py` (SPEC-02) |
| Cursor writers | `src/mission_control/adapters/cursor/frames.py` (SPEC-07 implements against this port) |
| Hook frames | kernel hook callback endpoint and `HookScriptMiddleware` (SPEC-01) call the sink |
| Postgres repository | new `src/mission_control/adapters/postgres/frames/{repository,identity,retention}.py` |
| Identity writers | same package; invoked from the sink on `session_init`, `turn_started`, `turn_ended` |
| Fact derivation and reducer action | new `src/mission_control/application/frames/reducer.py`; `domain/policies/reducer.py` gains `apply_frame_facts`; `domain/policies/contracts.py` gains the `session.*`/`tool_call.*` event payload types |
| Transcript materialization and renderers | new `src/mission_control/application/frames/transcript.py`, `domain/frames/render.py` |
| Transcript projection | `adapters/postgres/frames/transcript_projection.py`; projection job registered beside the catalog projection |
| Search attributes | `adapters/temporal/workflows/mission_run.py` (set at start), family workflows (`mc_phase` upsert), `application/recovery/run_forks.py` (`mc_forked_from_run_id`), registration script `scripts/temporal_search_attributes.py` (G7) |
| HTTP and CLI | new `interfaces/http/transcript.py` (router included by `bootstrap/api.py`), `interfaces/cli/main.py` (`run transcript`, `run list`, `run search`, `run frames`) |
| MCP | `interfaces/mcp/coordinator_resources.py`, `coordinator_server.py` (read-only tools) |
| Retention job | `adapters/temporal/activities/frames_expire.py` on a Temporal Schedule per application |
| Inspection | `contracts/contracts.py::MissionInspection` gains `sessions[]` summary (SPEC-06 F6 consumes) |

## Testing Decisions

External behaviour only: rows, events, rendered transcripts, CLI output.

- Unit (`tests/unit/frames/`): kind mapping tables for each lane cover every documented provider event (fixture lists from the research notes); dedupe keys stable across resume; `closing` set exactly per the rule; redaction removes every secret pattern and records paths; excerpt cap honoured; `derive(closing_frames)` is deterministic (shuffled non-closing frames do not change output; replay yields identical facts); 05 §3.5 outcome mapping per lane (`FINISHED` without a Completion Candidate is not `succeeded`).
- Unit: transcript merge ordering with interleaved events and frames, cursor round-trip, `since` beyond retention yields placeholders, Markdown and JSONL goldens.
- Integration (`common_db`): append idempotency (`ON CONFLICT`), stale generation rejected and counted, identity writers create `harness_execution`, `agent_session`, `session_turn` in the same transaction as the first frame, RLS denies cross-scope reads, retention deletes only non-closing frames older than the policy, `transcript_document` projection rebuilds and `run search` ranks a known phrase. Prior art: `tests/integration/postgres/test_mission_control_lifecycle_postgres.py`.
- Integration (`tests/integration/deep_agents/`): a local-model GoalDirected iteration produces `session_init`, `turn_started`, `tool_call_completed`, `usage`, `turn_ended`, `run_result` frames; subordinate frames carry `subordinate_ref` from `ns`; the reducer writes `session.turn_completed` and `tool_call.completed` events whose `native_event_ref` resolve. Prior art: `tests/acceptance/mission_control/test_postgres_runtime_parity.py` (135-event replay).
- Integration (Temporal, `start_time_skipping`): search attributes set at root start and upserted on phase change; `run list --query lane='deep_agents'` returns the run; fork sets `mc_forked_from_run_id`. Prior art: `tests/integration/temporal/test_linked_runs.py`.
- Acceptance: Mission 1 (I1) `missionctl run transcript --format md` renders the full run including the human gate and artifact registrations; Mission 3 (I3) shows hook denials and a queued instruction in order.

## Tickets

| Ticket | Scope |
| --- | --- |
| [FT-C1](issues/C1-provider-frame-store-and-deep-agents-writer.md) | migration 0027, contracts, FrameSink, Postgres repository, identity writers, Deep Agents writer, retention job |
| [FT-C2](issues/C2-reducer-derives-turn-facts-from-closing-frames.md) | kind and dedupe tables, fact derivation, reducer action, `session.*` and `tool_call.*` events, usage dispositions, 05 §3.5 mapping |
| [FT-C3](issues/C3-transcript-materialization-cli-http-mcp.md) | transcript merge, renderers, redaction, cursor, CLI, HTTP, MCP, live tail |
| [FT-C4](issues/C4-run-list-and-search.md) | search attributes set and upsert, `run list`, transcript projection and `run search` |

## Out of Scope

Dashboard rendering of the live tail (consumes the SSE endpoint); LangSmith trace export or correlation beyond storing the LangSmith run id in `native_identity`; Claude Agent SDK and Codex writers (rows in the tables are reserved; ADR-0018 order); cross-run analytics over frames; frame-level access controls finer than scope; replacing `native_observation` for async subordinates (it stays; a follow-up may converge it).

## Further Notes

- The Native Event Store is what makes the lifecycle synthesis honest: every reducer transition cites a frame, and `describe` claims (SPEC-07) are checked against frames in qualification.
- `arrival_ordinal` rather than provider timestamps orders frames because Codex-style deltas carry no sequence numbers and Cursor's `Send` offsets may interleave non-durable events; the provider cursor is for resumption, the ordinal is for order.
- Transcript `cursor` deliberately encodes the governing mission `seq` first so the canonical stream and the transcript advance together; a consumer can switch between `events watch --after-seq` and `run transcript --since` without losing position.
- UNVERIFIED (research): whether LangGraph heartbeats or `astream` v2 guarantee delivery of every `on_tool_end` across subgraph boundaries on cancellation; C1 adds a reconciliation read of the final checkpoint's messages to backfill missing `tool_call_completed` frames with `raw_kind = "checkpoint_backfill"`.

# Citations

- [ADR-0028](../../adr/0028-native-event-store-provider-frames-and-materialized-transcript.md), [ADR-0007](../../adr/0007-authoritative-state-separate-from-advisory-memory.md), [ADR-0004](../../adr/0004-temporal-sole-scheduler-transactional-outbox.md), [ADR-0031](../../adr/0031-temporal-lifecycle-synthesis-observe-activity-updates-seeded-forks.md).
- `../../../mission-control-general/workflow-types/09-EVENTS_COMMANDS_AND_STREAMS.md` sections 2 to 6; `05-EXECUTORS_AND_DURABLE_CONTROLS.md` sections 2, 3.4, 3.5.
- `docs/research/2026-10-07-coding-lane-surfaces.md` ("Transcript as the source of truth" per provider; "Deterministic state accumulation").
- [research/codebase-map.md](research/codebase-map.md) sections 5, 6 and gap (d); [research/deepagents-middleware.md](research/deepagents-middleware.md) section 4 and implication 11; [research/temporal-lifecycle.md](research/temporal-lifecycle.md) section 4; [research/cursor-platform.md](research/cursor-platform.md) sections 3.3, 4.4.
- Code: `src/mission_control/adapters/postgres/run_control/canonical.py::append_events`, `adapters/operations/runtime_ports.py::RecordedOperationEventSink`, `packages/mission-control-db-contract/component/migrations/0003_*.sql` (`harness_execution`, `agent_session`, `session_turn`), `0005_*.sql` (`native_observation`, `delivery_report`), `0022_capability_search_projection.sql` (projection pattern).
- `docs/knowledge/events-and-commands.md`, `docs/knowledge/lanes-and-harness.md`.
