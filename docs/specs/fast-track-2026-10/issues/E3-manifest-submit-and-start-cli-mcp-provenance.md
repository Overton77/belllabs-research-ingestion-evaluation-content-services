# [FT-E3] Submit and start on CLI and MCP; provenance stored with the revision

Linear: OVE-43

**Epic:** Manifest (SPEC-05)
**Team:** T3
**Blocked by:** FT-E2, FT-D1
**Status:** ready-for-agent

**What to build:** `missionctl mission submit mission.yml` compiles and, in one application transaction, writes the typed definition rows (`definition_snapshot`, `mission_revision`, `compiled_program`, plus the `goal`, `objective`, `success_criterion`, `program_node` rows), stores the manifest bytes and resolution in `authoring_provenance`, and admits the Run (or, for a chain, writes `mission_chain` and `chain_link` rows and admits the first mission's Run) without starting anything; it is idempotent on `request_id` and returns the mission, revision and run ids (or the chain id and members). `missionctl mission start RUN_ID` is the existing launch and additionally registers the manifest's `controls.subscriptions`. HTTP `POST /missions:submit`, `POST /missions/{id}/runs` and MCP `mission_manifest_submit`, `mission_run_start` mirror the CLI with the same handlers and grants.

**Spec sections:** SPEC-05 §Submit and start, §Persistence, §Interfaces, §Error codes; SPEC-04 §Compile (chain rows at submit); SPEC-06 §Subscriptions (registration at start)

**Writable regions:** `src/mission_control/application/authoring/manifest_service.py` (submit), `src/mission_control/interfaces/http/missions.py`, `src/mission_control/interfaces/mcp/coordinator_server.py` (`mission_manifest_submit`, `mission_run_start`), `tests/integration/postgres/test_manifest_submit.py`, `tests/acceptance/mission_control/test_manifest_lifecycle.py`; integrator-owned: `mission submit|start` CLI verbs, the existing launch handler (subscription registration hook)

**Acceptance criteria:**
- [ ] Submit refuses when the compile has blockers (exit 2 / HTTP 422 with the report) and writes nothing.
- [ ] Submit writes `definition_snapshot`, `mission_revision`, `compiled_program`, `authoring_provenance` and the typed definition rows in one transaction, and admits the Run through the existing admission path (`mission_run` row `pending`, frozen run request digest recorded).
- [ ] For a chain file, submit writes `mission_chain` and `chain_link` rows (state `armed`), admits only the first mission's Run, and returns `chain_id` plus every `mission_id`.
- [ ] Replaying submit with the same `request_id` and manifest digest returns the same ids with HTTP 200; the same `request_id` with a different digest fails `IDEMPOTENCY_CONFLICT` (exit 4 / HTTP 409); a new `request_id` with a digest equal to the current head returns the head with `unchanged: true`.
- [ ] `missionctl mission start RUN_ID` launches the admitted run (existing semantics, `--wait` honoured) and creates the `mission_subscription` rows declared in `controls.subscriptions` in the launch transaction (or records them as pending when FT-F5 has not landed, noted in handoff).
- [ ] Grants: submit requires `mission.author`; start requires `mission.start`; MCP tools are tagged consequential and require the matching grant on the authenticated principal.
- [ ] `authoring_provenance` rows are immutable (update and delete rejected by RLS or trigger) and queryable by `revision_id`.
- [ ] Acceptance: compile → submit → start of `missions/01-research-ingestion-deep-agents.yml` on the real local stack reaches a running Stage Graph whose first released stage is `collect`.

**Verification:** `uv run pytest tests/integration/postgres/test_manifest_submit.py -q -m common_db`; `make infra-up && make temporal-up && uv run --group biotech pytest tests/acceptance/mission_control/test_manifest_lifecycle.py -q -m common_db`; `make check`

**Notes:** Reuse the admission path of `CoordinatorWorkflowLaunchService` (frozen run request → reducer admit → outbox) rather than writing a second one; the Compiled Program from E2's lowering is the input the existing path expects. The `mig/0002` tables have no Python users today; add the writers in `adapters/postgres/control_plane/` and keep them within T3's region. Start remains the only verb that launches agents; chains are the exception handled by FT-D2's reducer, not by this ticket.
