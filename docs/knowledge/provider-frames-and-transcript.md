---
type: Concept
title: Provider frames and the transcript
description: The Native Event Store of provider frames, closing-frame derivation of mission events, usage dispositions, retention, the materialized transcript, and run list and search over Temporal Visibility and a transcript projection.
tags: [mission-control, frames, transcript, native-event-store, search, implementation]
---

# Provider frames and the transcript

A [Provider Frame](../../GLOSSARY.md) is the lane-neutral record of what a provider said; the
[Transcript](../../GLOSSARY.md) is the read view over events and frames. Mission events and
subscriptions are in [events and commands](events-and-commands.md); lanes that write frames are
in [lanes and harness](lanes-and-harness.md) and [cursor lane](cursor-lane.md).

ADR-0028, SPEC-03. A Provider Frame is one raw event a lane observed from its provider or from
a hook, redacted, digested, excerpted under a cap and persisted verbatim before any derivation
in `mission_control.provider_frame` (migration 0027). Frames are keyed by
`(harness_execution_id, generation, provider_key)` and ordered by a writer-assigned
`arrival_ordinal`; an append is idempotent on that key, and a frame of an older generation than
the execution's current one is counted `stale` and never stored
(`adapters/postgres/frames/repository.py`, `application/frames/sink.py`). Kinds are the
lane-neutral `FrameKind` set (`session_init`, `turn_started`, `message`,
`tool_call_started|completed|failed`, `approval_requested|resolved`, `hook_invoked|result`,
`before_compaction`, `after_compaction`, `usage`, `turn_ended`, `run_result`, ...), classified
per lane by tables in `application/frames/kinds.py`; unmapped kinds classify `unknown` and are
counted. Native identity records (`harness_execution`, `agent_session`, `session_turn`) advance
in the same transaction as the frames that cause them (`adapters/postgres/frames/identity.py`).
Writers: Deep Agents (`adapters/deep_agents/frames.py`), Cursor
(`adapters/cursor/frames.py`) and hook callbacks.

Only Closing Frames move state. `application/frames/reducer.py::derive` is pure and reads the
current generation's closing frames in arrival order: first closing frame of a session gives
`session.started`, of a turn `session.turn_started`, a completed or failed tool call
`tool_call.completed{status}` (deduplicated by tool call ref), a denying `hook_result`
`tool_call.completed{status: denied}`, `session_state` awaiting a human
`activation.phase_changed{awaiting_human}`, `after_compaction` `session.compaction_observed`,
`usage` and `turn_ended` the turn's usage disposition (`settled`, `estimated` or `unknown`;
unknown is never zero, `domain/frames/usage.py`). Each event points back by
`source.native_event_ref`. `FrameFactProjector` applies the facts through run control.
Retention: `frame_retention_policy` (default 30 days, keep closing frames, 8 KiB excerpt cap);
`mc.frames_expire.v1` runs `frames.expire` on a daily Temporal Schedule per application, served
on the worker's maintenance queue (`MISSION_CONTROL_FRAMES_EXPIRE_SCHEDULE`, default true).
Mission events are never touched; a transcript shows `frame_expired` placeholders.

The Transcript (`mc.transcript_entry.v1`, `application/frames/transcript.py`) is a read view,
never persisted: it merges mission events by `seq`, frames placed under their governing event,
artifact references and expiry placeholders, with a strictly increasing opaque cursor
(`tc1:...`). Surfaces: `GET /runs/{id}/transcript` (JSONL, JSON or Markdown),
`GET /runs/{id}/frames/tail` (non-canonical SSE tail), `missionctl run transcript [--follow]
[--since]`, `missionctl run frames --tail`, MCP tool `mission_run_transcript` and resource
`mc://applications/{application_id}/runs/{run_id}/transcript`. `--full` bodies answer `501`
because no production `ArtifactBodyReader` is wired. Run list and search (C4):
`GET /runs?query=` over Temporal Visibility with a bounded grammar (`lane`, `phase`,
`mission_id`, `forked_from`, `status`, `started_after`), bound to the installation workflow
prefix and the scope hash and enriched from the ledger (`domain/frames/run_query.py`), and
`GET /runs/{id}/transcript/search` over `mission_control_search.transcript_document` (a
rebuildable `websearch_to_tsquery` index, never an authorization store); CLI `run list`,
`run search`; MCP `mission_run_list`, `mission_run_search`. The `transcript.project` job
activity is registered on no worker (searches refresh their run's projection themselves).
The search attributes `mc_mission_id`, `mc_run_id`, `mc_lane`, `mc_phase` and
`ForkedFromRunId` are registered by `make temporal-search-attributes`.

# Citations

- Spec: [SPEC-03](../specs/fast-track-2026-10/SPEC-03-mission-state-and-transcript.md).
- ADR: [0028](../adr/0028-native-event-store-provider-frames-and-materialized-transcript.md).
- Code: [frame contracts](../../src/mission_control/domain/frames/contracts.py),
  [frame body redaction](../../src/mission_control/domain/frames/body.py),
  [facts](../../src/mission_control/domain/frames/facts.py),
  [usage dispositions](../../src/mission_control/domain/frames/usage.py),
  [run query grammar](../../src/mission_control/domain/frames/run_query.py),
  [frame sink](../../src/mission_control/application/frames/sink.py),
  [frame writer](../../src/mission_control/application/frames/writer.py),
  [kind tables](../../src/mission_control/application/frames/kinds.py),
  [frame reducer](../../src/mission_control/application/frames/reducer.py),
  [transcript](../../src/mission_control/application/frames/transcript.py),
  [run list and search](../../src/mission_control/application/frames/search.py),
  [frame store](../../src/mission_control/adapters/postgres/frames/repository.py),
  [native identity](../../src/mission_control/adapters/postgres/frames/identity.py),
  [retention](../../src/mission_control/adapters/postgres/frames/retention.py),
  [transcript projection](../../src/mission_control/adapters/postgres/frames/transcript_projection.py),
  [expiry activity](../../src/mission_control/adapters/temporal/activities/frames_expire.py),
  [transcript routes](../../src/mission_control/interfaces/http/transcript.py),
  [transcript MCP tools](../../src/mission_control/interfaces/mcp/transcript_tools.py),
  [Deep Agents writer](../../src/mission_control/adapters/deep_agents/frames.py).
- Tests: [contracts and body](../../tests/unit/frames/test_frame_contracts_and_body.py),
  [kinds](../../tests/unit/frames/test_frame_kinds.py),
  [writer](../../tests/unit/frames/test_frame_writer.py),
  [facts](../../tests/unit/frames/test_frame_facts.py),
  [transcript](../../tests/unit/frames/test_transcript.py),
  [run list and search](../../tests/unit/frames/test_run_list_and_search.py),
  [frames in PostgreSQL](../../tests/integration/postgres/test_provider_frames_postgres.py),
  [frame facts in PostgreSQL](../../tests/integration/postgres/test_frame_facts_postgres.py),
  [transcript in PostgreSQL](../../tests/integration/postgres/test_transcript_postgres.py),
  [transcript search in PostgreSQL](../../tests/integration/postgres/test_transcript_search_postgres.py),
  [expiry workflow](../../tests/integration/temporal/test_frames_expire_workflow.py),
  [run list on Temporal](../../tests/integration/temporal/test_ft_c4_run_list.py),
  [Deep Agents frames](../../tests/integration/deep_agents/test_provider_frames_deep_agents.py).
