---
type: Decision Record
title: Provider frames are persisted verbatim in a Native Event Store before any derivation; mission state derives from closing frames; the transcript is a materialized read view
description: "Every frame a lane observes (SDK message, SSE event, hook invocation, LangGraph stream event) is written to mission_control.provider_frame keyed by harness execution, generation and a provider dedupe key with an arrival ordinal, with bodies digested and excerpted under a size cap and a 30-day default retention; the reducer reads only closing frames; missionctl run transcript joins mission events, frames and artifact refs into JSONL or Markdown on demand."
tags: [mission-control, adr, decision, events, transcript]
status: accepted
source: fast-track interview 2026-10-07 (requirement 2b and 2e transcript); workflow-types/09 section 4; docs/research/2026-10-07-coding-lane-surfaces.md (deterministic state accumulation); docs/specs/fast-track-2026-10/research/codebase-map.md (session_turn and harness_execution tables have no writer)
---

# Provider frames are persisted verbatim in a Native Event Store before any derivation; mission state derives from closing frames; the transcript is a materialized read view

The specification separates the canonical mission stream from raw provider events but the code persists neither frames nor turns. We implement the Native Event Store as `mission_control.provider_frame` written by the observing worker in arrival order, before the reducer sees anything, so a worker crash never loses what the provider said. The reducer derives attempt phase, tool effects, usage and terminal outcome only from closing frames (Deep Agents tool and model end events, Cursor `tool_call` completed and `TurnEndedUpdate`, SDK result messages) and writes `session_turn`, `tool_call.completed` and `session.turn_completed` mission events that point at the frames by `native_event_ref`. Hook invocations and lifecycle callbacks are frames too. The transcript is not a table; it is a materialization (`missionctl run transcript`, `GET .../runs/{id}/transcript`) that joins events, frames and artifact references in sequence and renders JSONL for agents and Markdown for humans, with a `--since` cursor. We rejected writing agent text into mission events (it would pollute the canonical stream) and rejected LangSmith traces as the record (external, lane-specific, not in the ledger).

## Consequences

- Frame bodies above the cap are stored as digest plus excerpt; full bodies that matter become artifacts through the normal registration path.
- Retention is per application; expiring frames never deletes mission events.
- Fork, inspection and search over past runs read the same materialization.
