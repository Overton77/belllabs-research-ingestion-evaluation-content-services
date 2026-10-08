# [FT-C4] Run list and search through Temporal search attributes and transcript index

Linear: OVE-37

**Epic:** Mission state (SPEC-03)
**Team:** T2
**Blocked by:** FT-C2, FT-G7
**Status:** ready-for-agent

**What to build:** Runs become findable. The mission root sets typed search attributes `mc_mission_id`, `mc_run_id`, `mc_lane`, `mc_application_id` at start, the family upserts `mc_phase` on phase changes, and the fork saga sets `mc_forked_from_run_id` on the new root (attribute registration itself is G7). `GET .../runs?query=` translates a bounded filter grammar into a Temporal list filter scoped by the installation's workflow id prefix, lists through `client.list_workflows`, and enriches rows from the ledger; `missionctl run list --query` prints them. A `mission_control_search.transcript_document` projection (one row per transcript entry with a generated `fts` column) is maintained by the projection job family and queried by `GET .../runs/{id}/transcript/search?q=` and `missionctl run search RUN_ID --query`, returning entries with cursors that `run transcript --since` can open. Demo: `missionctl run list --query "lane='deep_agents' AND phase='executing'"` lists the running C1 run; `missionctl run search RUN_ID --query "source_manifest"` returns the artifact registration entry with its cursor.

**Spec sections:** SPEC-03 §Implementation Decisions (Run list and search), §Persistence (`transcript_document`), §Interfaces

**Writable regions:** `src/mission_control/adapters/postgres/frames/transcript_projection.py`, `src/mission_control/application/frames/search.py`, `src/mission_control/interfaces/http/transcript.py` (search and list routes), `tests/unit/frames/`, `tests/integration/postgres/`, `tests/integration/temporal/`; shared (integrator-coordinated, additive): `adapters/temporal/workflows/mission_run.py` (set attributes), `adapters/temporal/workflows/{stagegraph,goal_directed}.py` (`mc_phase` upsert), `application/recovery/run_forks.py` (`mc_forked_from_run_id`), `interfaces/cli/main.py` (`run list`, `run search`)

**Acceptance criteria:**
- [ ] Root start passes `TypedSearchAttributes` with the five keys; family workflows call `workflow.upsert_search_attributes([MC_PHASE.value_set(...)])` on every phase change; the fork saga sets `mc_forked_from_run_id`; a Temporal integration test (`start_time_skipping` or `start_local` with `--search-attribute` flags) asserts them through `describe().typed_search_attributes`.
- [ ] The filter grammar accepts `lane`, `phase`, `mission_id`, `forked_from`, `status`, `started_after` and rejects anything else with a typed 422; the generated Temporal filter always includes the installation workflow id prefix so one application cannot list another's runs.
- [ ] `GET /v1/applications/{app}/runs?query=` returns rows enriched with ledger `lifecycle` and `terminal_outcome`; the ledger remains authoritative when visibility lags (a row missing from the ledger is dropped and counted).
- [ ] `transcript_document` is populated by the projection job for every canonical entry and closing-frame entry of a run, rebuilt idempotently, and never consulted for authorization.
- [ ] `run search` uses `websearch_to_tsquery` + `ts_rank_cd`, returns `TranscriptEntry` cursors, respects scope and `mission.read`; a known phrase in a tool result excerpt ranks first in the integration test.
- [ ] `missionctl run list [--query] [--json]` and `missionctl run search RUN_ID --query [--limit]` follow the CLI exit-code convention and `--json` output shape.
- [ ] `make check` passes; Temporal and Postgres integration selections pass.

**Verification:** `make check`; `uv run pytest tests/integration/temporal -k search_attributes -q`; `MISSION_CONTROL_TEST_ADMIN_DSN=... uv run --group biotech pytest -m common_db tests/integration/postgres -k "transcript and search" -q`

**Notes:** Attribute registration (`temporal operator search-attribute create`, dev-server flags in `make temporal-up`) and the `temporalio` 1.34 upgrade are G7; this ticket consumes them. Use `SearchAttributeKey.for_keyword` typed keys; the dict form is deprecated. `ExecutionStatus='Paused'` is accepted by the list filter but the Python enum has no `PAUSED` member (UNVERIFIED how it appears); map it defensively. Keep the projection pattern identical to `0022_capability_search_projection.sql` (generation table + activate) if the integrator asks for rebuild atomicity; otherwise a simple upsert projection is acceptable for this packet.
