# [FT-C1] Provider frame store and Deep Agents frame writer

Linear: OVE-34

**Epic:** Mission state (SPEC-03)
**Team:** T2
**Blocked by:** None (can start immediately)
**Status:** ready-for-agent

**What to build:** The Native Event Store. Migration `0027_provider_frames.sql` adds `mission_control.provider_frame`, `frame_retention_policy`, the missing columns on `harness_execution`, `agent_session` and `session_turn`, and the `context_selection` and `continuation_checkpoint` tables SPEC-02 needs if the common component lacks them. The `mc.provider_frame.v1` contract with the `FrameKind` vocabulary and the `closing` rule; the `FrameSink` port (`append`, `last_cursor`) replacing the in-memory `RecordedOperationEventSink`; a Postgres repository with `ON CONFLICT DO NOTHING` idempotency, writer-assigned `arrival_ordinal`, stale-generation rejection, redaction and excerpt cap; identity writers that create `harness_execution`, `agent_session` and `session_turn` rows in the same transaction as the first frame of a session or turn; the Deep Agents writer that turns `astream(..., stream_mode=["updates","messages","custom"], subgraphs=True, version="v2")` chunks into frames with subordinate attribution; and a `frames.expire` activity on a Temporal Schedule. Demo: a local-model GoalDirected iteration on the real stack leaves an ordered, deduplicated frame sequence in Postgres; re-running the writer over the same stream adds zero rows.

**Spec sections:** SPEC-03 §Contracts (`mc.provider_frame.v1`, native identity records), §Implementation Decisions (Native Event Store table and write path, FrameSink port and writers), §Persistence

**Writable regions:** `packages/mission-control-db-contract/component/migrations/0027_provider_frames.sql`, `src/mission_control/domain/frames/contracts.py`, `src/mission_control/application/frames/{sink,kinds}.py`, `src/mission_control/adapters/postgres/frames/{repository,identity,retention}.py`, `src/mission_control/adapters/deep_agents/adapter.py` (`_ModelCallObserver` → frame producer), `src/mission_control/adapters/temporal/activities/frames_expire.py`, `tests/unit/frames/`, `tests/integration/postgres/`, `tests/integration/deep_agents/`; shared (integrator): `adapters/operations/runtime_ports.py` (sink replacement)

**Acceptance criteria:**
- [ ] Migration 0027 applies and replays as a no-op with `mission-db` on a disposable PostgreSQL 17 cluster; `mission-db qualify` reports the expected protected-object diff; RLS is forced on every new table and the six capability roles have the grants in SPEC-03 §Persistence.
- [ ] `ProviderFrame` model validates `FrameKind`, `closing` set exactly for the closing kinds, `body_digest` format, and rejects unknown fields; JSON Schema exported.
- [ ] `PostgresFrameRepository.append` is idempotent on `(harness_execution_id, generation, provider_key)` and returns `AppendReceipt(new, duplicate, stale)`; a frame with an older generation is counted as `stale` and not stored; `arrival_ordinal` continues from `max()` on resume.
- [ ] Redaction removes every pattern in the configured list (bearer tokens, `sk-`, the named API key env names, URL `token=`/`key=` params) and records `redactions[]`; bodies above `excerpt_cap_bytes` are truncated in `body_excerpt` with the full digest kept; eligible closing bodies are promoted to `provider_frame_body` artifacts when the lane flag allows.
- [ ] Identity writers create or update `harness_execution` (lane, generation, native identity, observation cursor), `agent_session` and `session_turn` rows in the same transaction as the triggering `session_init` / `turn_started` / `turn_ended` frame.
- [ ] Deep Agents writer maps every documented chunk kind to a `FrameKind` (unmapped → `unknown`, counted), sets `subordinate_ref` from `ns[0].startswith("tools:")` and `metadata.lc_agent_name`, uses the `(thread_id, checkpoint_ns, checkpoint_id, step, message_id|tool_call_id, kind)` dedupe key, and backfills missing `tool_call_completed` frames from the final checkpoint with `raw_kind = "checkpoint_backfill"`.
- [ ] `frames.expire` deletes only non-closing frames older than `retain_days` (and closing ones when `keep_closing_frames = false`); mission events are untouched; a test proves it.
- [ ] Integration test: a local-model GoalDirected iteration (prior art: parity acceptance) produces `session_init`, `turn_started`, `tool_call_completed`, `usage`, `turn_ended`, `run_result` frames in order; replaying the same stream adds zero rows; RLS denies a cross-scope read.
- [ ] `make check` passes; `pytest packages/mission-control-db-contract/tests` passes.

**Verification:** `make check`; `make test-db-contract`; `MISSION_CONTROL_TEST_ADMIN_DSN=... uv run --group biotech pytest -m common_db tests/integration/postgres -k frames -q`; `uv run pytest tests/integration/deep_agents -k frames -q`

**Notes:** Runtime persistence (LangGraph saver and store, Agent Server) uses the local Docker PostgreSQL from `make infra-up`, never a Supabase database (ADR-0017). Reconcile the `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` list against the real `mig/0003` DDL before writing the migration; SPEC-03 lists the target columns, not the current ones. The `context_selection` and `continuation_checkpoint` tables belong in this migration only if absent from the component (check `mig/0003` and `0005`). Do not touch `native_observation` (async subordinates keep it). Cursor writers are G3/G5 against this port; keep `FrameSink` lane-neutral.
