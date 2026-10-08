# [FT-G5] Cursor Cloud lane: idempotent create, branch control, SSE resume, artifacts, usage settlement

Linear: OVE-54

**Epic:** Lanes (SPEC-07)
**Team:** T4
**Blocked by:** FT-G3
**Status:** ready-for-agent

**What to build:** The `cursor_cloud` Lane Profile sharing the Cursor adapter package: `prepare` creates and pushes branch `mc/<run_id>` carrying the projections and packet files; `start` creates the agent with a client-supplied `agent_id` (`409 agent_id_conflict` means reattach) or `idempotency_key` when `env_vars` are needed, `work_on_current_branch=True` on `starting_ref=mc/<run_id>`; `send_turn` returns busy on `409 agent_busy` so the loop applies `wait_then_send`; `observe` consumes the SSE run stream with `Last-Event-ID`, records the retention header, and on `410 stream_expired` synthesizes the terminal frame from `GET .../runs/{runId}`; closing facts include `git.branches[]`; `end_session` fetches the branch diff from the SCM as the patch artifact, downloads cloud artifacts, archives the agent; `usage()` settles tokens from the usage endpoint and cost from `get_usage()`. Proven on recorded SSE fixtures.

**Spec sections:** SPEC-07 §6, §7 (cloud column), §8; Contracts (`mc.cursor_binding.v1` cloud block); 00-ARCHITECTURE.md §6.

**Writable regions:** `src/mission_control/adapters/cursor/{cloud.py,sse.py,scm.py}`, `tests/integration/cursor/fixtures/cloud/`, `tests/unit/harness/test_cursor_cloud.py`. Shared: none beyond registry wiring already done in FT-G3.

**Acceptance criteria:**
- [ ] `prepare` pushes `mc/<run_id>` with `.mission/`, `/inputs/`, `AGENTS.md`, `.cursor/rules`, `.cursor/agents`, `.cursor/hooks.json` (command hooks only), `.cursor/mcp.json`; the branch head digest is recorded on `harness_execution.cloud_branch`.
- [ ] `start` with client `agent_id` is idempotent: a replayed create maps `409 agent_id_conflict` to reattach; `env_vars` with `agent_id` is rejected at binding validation; `metadata` `403 feature_unavailable` is recorded, not fatal.
- [ ] `409 agent_busy` yields `TurnHandle(status="busy")`; the loop polls `lane.status` until `IDLE` within `wall_clock_s`, then sends; exceeding the bound classifies `failed(capacity)`.
- [ ] SSE frames carry `provider_key = sse id`; reconnect with `Last-Event-ID` yields no duplicates; the leading id-less `status` event is deduped by content; `heartbeat` events are not persisted; `410` falls back to the run record and produces one terminal frame.
- [ ] Closing facts map `FINISHED|ERROR|CANCELLED|EXPIRED` per SPEC-07 §8; `git.branches[]` is attributed to the run whose `latestRunId` matches; the diff is fetched from the SCM and registered as the patch artifact.
- [ ] Artifacts are listed and downloaded through presigned URLs and registered with digests; `archive` runs after registration.
- [ ] `usage()` records tokens `settled` from `GET /v1/agents/{id}/usage?runId=` and cost `settled` only when `get_usage().cost` is non-null.
- [ ] `describe()` for `cursor_cloud` matches 00-ARCHITECTURE.md §6 (`reattach: native`, no `session_start`/`session_end`/MCP hooks, `cursor: sse_event_id`).

**Verification:** `make check`; `uv run --group biotech pytest tests/unit/harness/test_cursor_cloud.py -q`; `uv run --group biotech pytest tests/integration/cursor -k cloud -q`.

**Notes:** v1 has no webhooks, no conversation endpoint, no diff or commit SHA, and no branch-name field (use `starting_ref` plus `work_on_current_branch`). Cloud agents ignore `setting_sources` and read project config from the repository. UNVERIFIED: `Idempotency-Key` dedupe window, REST `envVars`/`metadata` acceptance, retention value, per-plan concurrency. Budget: small real fixture runs are permitted by default (see TEAM-WORKSPACE.md, Environment); record spent units in the handoff; larger drills need an approved cap in Linear. `CURSOR_API_KEY` is set; one real cloud agent on a throwaway repository may be recorded here.
