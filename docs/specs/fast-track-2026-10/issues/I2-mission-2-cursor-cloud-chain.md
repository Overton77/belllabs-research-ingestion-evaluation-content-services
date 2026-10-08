# [FT-I2] Mission 2 manifest and acceptance run on Cursor Cloud as a chain

Linear: OVE-60

**Epic:** Missions (00-ARCHITECTURE §1, missions/)
**Team:** Integrator
**Blocked by:** FT-G5, FT-D3, FT-I1
**Status:** ready-for-agent

**What to build:** The owner's second mission runs from `missions/02-research-ingestion-cursor-cloud-chain.yml`: a Mission Chain of two Goal Loops on the `cursor_cloud` lane where `research` supplies `ingestion`. Compile, submit and start admit the chain; the Cursor Cloud agent is created idempotently with a client-supplied agent id in a repository workspace with `.mission/`, `AGENTS.md`, rules, skills and MCP projections committed to a pre-created `mc/<run>` branch; SSE frames persist with `Last-Event-ID` resume; when `research.evidence_map` is accepted the chain reducer admits `ingestion` with a Context Packet built from the supplied outputs and the outbox starts its root; usage settles from `get_usage`; a webhook Subscription receives `chain_link.released` and `run.completed`.

**Spec sections:** 00-ARCHITECTURE §1 and §7.4; SPEC-04 (chains), SPEC-07 (Cursor Cloud), SPEC-05 (missions list and links), SPEC-06 (webhook subscription).

**Writable regions:** `docs/specs/fast-track-2026-10/missions/02-research-ingestion-cursor-cloud-chain.yml` (fixture adjustments only), `tests/acceptance/mission_control/test_fast_track_mission_2.py` (new), `.scratch/fast-track-2026-10-07/I2/` (evidence).

**Acceptance criteria:**
- [ ] `mission compile` on the chain file validates both missions and the link; `mission submit` returns one chain id and two mission ids; `chain inspect` shows the link `pending`.
- [ ] `mission start` starts only `research`; `ingestion` is admitted by the chain reducer when the link condition commits (`goal_accepted` by default).
- [ ] The `ingestion` Run's first packet contains `research`'s accepted outputs and final checkpoint reference; `.mission/context.md` in the repository workspace lists them.
- [ ] Cursor Cloud frames are persisted and resumable: a forced stream disconnect resumes from the last SSE id without duplicates in the transcript.
- [ ] `409 agent_busy` on a follow-up is handled as `wait_then_send` in the Delivery Report, never as a new agent.
- [ ] Usage shows `estimated` then `settled` cost after `get_usage`.
- [ ] Webhook receives `chain_link.released` and both `run.completed` events with valid signatures.
- [ ] One real Cursor Cloud chain run on a throwaway repository is the primary evidence (`CURSOR_API_KEY` is set), recorded into replayable fixtures; a repeat needs an approved cap in Linear; evidence saved under `.scratch/fast-track-2026-10-07/I2/`; `make check` passes.

**Verification:** `make infra-up && make temporal-up`; `uv run --group biotech pytest -m common_db tests/acceptance/mission_control/test_fast_track_mission_2.py -q`; CLI sequence `mission compile|submit|start`, `chain inspect`, `run transcript`.

**Notes:** Cursor Cloud needs `CURSOR_API_KEY`, a GitHub-connected repository and the `mc/<run>` branch pre-created by the lane (`starting_ref` plus `work_on_current_branch=True`); v1 has no branch name field and no webhooks (research/cursor-platform.md). Cloud hooks skip `sessionStart`, so the packet index must be complete on disk before the first send. UNVERIFIED items from the research (rate limits, concurrent agent limits) are recorded, not assumed.
