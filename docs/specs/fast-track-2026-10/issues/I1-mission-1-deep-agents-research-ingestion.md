# [FT-I1] Mission 1 manifest and acceptance run on Deep Agents

Linear: OVE-59

**Epic:** Missions (00-ARCHITECTURE §1, missions/)
**Team:** Integrator
**Blocked by:** FT-E3, FT-B2, FT-B3, FT-A6, FT-A7, FT-C3
**Status:** ready-for-agent

**What to build:** The owner's first mission runs end to end from `missions/01-research-ingestion-deep-agents.yml` on the local real stack: `missionctl mission compile` resolves every `search` entry to a pin (PubMed, Tavily, Firecrawl MCP servers; literature skill bundles) and validates lanes; `mission submit` commits the revision and admits the Run; `mission start` launches it; a Subscription registered by the coordinator skill reports progress; the Stage Graph `collect` (nested Goal Loop) hands `synthesize` a Context Packet with the source manifest materialized under `/inputs`; the human gate `review` opens a Human Task that the operator resolves; `ingest` calls the Biotech domain capability through its receipt contract; `run transcript` shows the whole record; outputs are registered artifacts and the Run ends `accepted`.

**Spec sections:** 00-ARCHITECTURE §1 and §7.1 to §7.3; SPEC-05 (manifest), SPEC-02 (packet), SPEC-01 (seeds), SPEC-03 (transcript), SPEC-06 (subscription).

**Writable regions:** `docs/specs/fast-track-2026-10/missions/01-research-ingestion-deep-agents.yml` (fixture adjustments only), `tests/acceptance/mission_control/test_fast_track_mission_1.py` (new), `.scratch/fast-track-2026-10-07/I1/` (evidence). Shared files only with the integrator role.

**Acceptance criteria:**
- [ ] `missionctl mission compile missions/01-...yml --json` returns a Validation Report with zero blockers and a resolution entry per `search`.
- [ ] `mission submit` then `mission start` produce a Run; `run inspect` shows lane `deep_agents`.
- [ ] A Subscription (stream or MCP) receives `activation.completed` for `collect` and `human_task.opened` for `review` without polling.
- [ ] `synthesize`'s workspace contains `/inputs/sources/...` with digests matching `collect`'s registered artifact; `.mission/context.md` lists the packet items.
- [ ] Resolving the Human Task releases `ingest`; the domain receipt is recorded; `run.completed{accepted}` is emitted.
- [ ] `missionctl run transcript RUN --format md` renders the stages, iterations, tool calls (digests) and interventions in order.
- [ ] One real end-to-end run on the local stack with the configured keys (`OPENAI_API_KEY`, `TAVILY_API_KEY`, `FIRECRAWL_API_KEY`; PubMed keyless) is the primary evidence, recorded into replayable fixtures; a second live run needs an approved cap in Linear.
- [ ] Evidence (commands, outputs, event tail, transcript) saved under `.scratch/fast-track-2026-10-07/I1/`; `make check` passes.

**Verification:** `make infra-up && make temporal-up`; `uv run --group biotech pytest -m common_db tests/acceptance/mission_control/test_fast_track_mission_1.py -q`; the CLI sequence above against `make server` and `make worker`.

**Notes:** Runtime persistence (LangGraph saver and store, Agent Server) uses the local Docker PostgreSQL from `make infra-up`, never a Supabase database (ADR-0017). `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, `TAVILY_API_KEY` and `FIRECRAWL_API_KEY` are set; `NCBI_API_KEY` is not (keyless PubMed is enough for one run). Budget: small real fixture runs are permitted by default (see TEAM-WORKSPACE.md, Environment); record spent units in the handoff; larger drills need an approved cap in Linear. Secrets come from `.env` through the host secret mechanism; never print them. The Biotech ingestion capability must exist in the Biotech catalog or the mission ends with a visible blocker, not a fallback (spec rule).
