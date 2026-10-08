# [FT-D3] Acceptance: two linked Goal Loops transfer state

Linear: OVE-40

**Epic:** Chains (SPEC-04)
**Team:** T3
**Blocked by:** FT-D2, FT-B3
**Status:** ready-for-agent

**What to build:** On the real local stack, submit the Mission 2 manifest (`missions/02-research-ingestion-cursor-cloud-chain.yml`) with a fixture override that runs both missions on the Deep Agents lane, start the `research` run, drive its Goal Loop to accept `evidence_map`, and prove that the `ingestion` run is released by the chain reducer with a first Context Packet containing the materialized evidence map, the acceptance disposition and the checkpoint and journal references; then prove `chain.completed{accepted}` when `ingestion` accepts, and prove `cancel_downstream` on a second run where `research` is cancelled mid-loop. This is the end-to-end acceptance of SPEC-04 and the chain half of Mission 2 (the Cursor Cloud lane itself is I2).

**Spec sections:** SPEC-04 §Acceptance scenario (Mission 2), §Testing Decisions; SPEC-02 §Iteration packet

**Writable regions:** `tests/acceptance/mission_control/test_chain_two_goal_loops.py`, `tests/fixtures/manifests/`, `docs/specs/fast-track-2026-10/missions/02-research-ingestion-cursor-cloud-chain.yml` (fixture-only corrections), `.scratch/fast-track-2026-10-07/D3/` (proof artifacts)

**Acceptance criteria:**
- [ ] The manifest compiles with zero blockers against the seeded catalog fixture; the lane override is a test fixture, not a manifest edit.
- [ ] After `research` accepts `evidence_map` and completes, `chain_link` rows show both links `released`, `ingestion` has a run, and the outbox intent was delivered exactly once.
- [ ] `ingestion`'s first packet (read from `.mission/context.md` and `inputs.json` in its workspace) contains `/inputs/research.evidence_map/…` with a digest equal to the registered artifact, the acceptance disposition inline, and references to the research checkpoint and journal digest.
- [ ] `ingestion` reaches acceptance with a local model fixture; `chain.completed{accepted}` appears in both streams with one `event_id`.
- [ ] A second scenario cancels `research` during iteration 2: `chain_link.cancelled` or `chain_link.blocked`, no `ingestion` run admitted, `chain.completed{cancelled}`.
- [ ] `missionctl chain inspect` output for both scenarios is saved as proof under `.scratch/fast-track-2026-10-07/D3/`.
- [ ] The test runs under `pytest -m common_db` with the local Temporal dev server and records passed, failed, blocked and unrun separately in the handoff.

**Verification:** `make infra-up && make temporal-up`; `uv run --group biotech pytest tests/acceptance/mission_control/test_chain_two_goal_loops.py -q -m common_db`; `make check`

**Notes:** Runtime persistence (LangGraph saver and store, Agent Server) uses the local Docker PostgreSQL from `make infra-up`, never a Supabase database (ADR-0017). Prior art for a real-stack Goal Loop acceptance: `tests/acceptance/mission_control/test_postgres_runtime_parity.py` (GoalDirected with a local model). One small real Deep Agents run is permitted for the recording; Cursor Cloud is exercised in FT-I2. If the Biotech `kg_ingest` capability is not seeded yet, the fixture catalog supplies a `noop_echo`-backed stand-in with the same kind and schema and the handoff says so.
