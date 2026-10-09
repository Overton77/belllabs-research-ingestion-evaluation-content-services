---
type: Agent Handoff
title: "Pre-fixture handoff: close blockers B1-B7 so the owner can run Missions 1 to 3"
description: "Session handoff after the 2026-10-08 fast-track implementation. Lists what is merged, the environment left running, the owner decisions to collect first, and the ordered work that remains before a live run of Missions 1 to 3 (I1, I2, I3): the launch-input author (B1), real capabilities (B2), worker pins (B3/B4), event names (B7), lane qualification and Windows constraints (B5/B6). Each item has code pointers, acceptance checks and process rules for a fresh agent session."
tags: [mission-control, fast-track, handoff, fixtures, blockers]
---

# Pre-fixture handoff (written 2026-10-08)

Start here in a fresh session. The goal is for `missionctl mission start` to launch Missions 1, 2 and 3 on the local real stack. The owner runs the paid fixture runs (I1 OVE-59, I2 OVE-60, I3 OVE-61). This session gets everything else ready and must not run them.

## 1. Where things stand

- `main` and `origin/main` are both at `7c9b755`. All 36 fast-track tickets are merged. Linear: 35 are Done; G6 (OVE-55) is In Review as partial. No `ft/*` worktrees or branches remain.
- The primary checkout `mission-control/` is clean except for two owner items. Do not touch them: the deleted `app/.cursor/rules/user_subagent_preference.mdc` and the untracked `experiments/docs_retrieval/results/`.
- Component release `mission_control` 1.1.0 (migrations 0025 to 0030) is built and locked. It has been proven only on scratch databases. It is **not applied** to either Supabase project.
- Proof artifacts from the implementation agents are in `.scratch/fast-track-2026-10-07-worktrees/<team>/` (gitignored).
- `mission start` currently answers `409 start_unavailable` for every mission. That is blocker B1 below.

Read in this order:
1. [OWNER-FIXTURE-RUNBOOK.md](OWNER-FIXTURE-RUNBOOK.md) sections 0, 1, 2 and 8. They are the source of truth for the stack setup, the per-mission commands and the blocker table this handoff summarises.
2. [00-ARCHITECTURE.md](00-ARCHITECTURE.md) sections 1, 7 and 8, and the three manifests under [missions/](missions/).
3. `docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md`, plus the knowledge concepts `docs/knowledge/mission-manifest.md`, `mission-chains.md`, `lanes-and-harness.md`, `cursor-lane.md`, `deep-agents-lane.md` and `capability-seeds.md`.
4. The Linear handoff comments of the ticket you touch: `uv run --no-sync python docs/specs/fast-track-2026-10/linear_note.py OVE-NN --show`. Ids are A1 OVE-22 to A8 OVE-29, B1 OVE-30 to B4 OVE-33, C1 OVE-34 to C4 OVE-37, D1 OVE-38 to D3 OVE-40, E1 OVE-41 to E3 OVE-43, F1 OVE-44 to F6 OVE-49, G1 OVE-50 to G7 OVE-56, H1 OVE-57, H3 OVE-58, I1 OVE-59 to I4 OVE-62.

## 2. Environment left running

| Resource | State | Note |
| --- | --- | --- |
| Disposable PostgreSQL 17 + pgvector | Docker container `mc-ft-disposable-pg17` on `127.0.0.1:55433`, user `postgres` | Use it as `MISSION_CONTROL_TEST_ADMIN_DSN`. The password is in the container's `POSTGRES_PASSWORD` env (read it with `docker inspect`; do not commit it). After a reboot, `docker start mc-ft-disposable-pg17`. Never `docker volume prune`. |
| `make infra-up` stack | Application PostgreSQL **16** (`:55432`), Redis, Temporal 1.31 (`:7233`), UI (`:8080`) | Release 1.1.0 needs PostgreSQL 17, so fixtures use the disposable PG17 container (or move compose to `pgvector/pgvector:pg17`, an owner decision). The 10 Keyword search-attribute slots on local Temporal are all used. |
| Workspace drift (not caused by this work) | `Biotech/biotech-kg/` was removed. `Biotech/.agents/skills/agent-browser` changed on 2026-10-08. | This causes 7 unit failures (6 in `tests/unit/schema/*`, plus `test_capability_pins_and_runtime_ports::test_workspace_artifacts_verify_against_their_pins_when_present`), and worker startup raises `CapabilityPinError` (B4). |

Current `make check` result on `main`: 1916 passed, 8 failed. The 8 are the 7 environmental failures above plus the pre-existing `test_run_web_research_coordinator_live::test_live_settings_pin_workspace_npx_and_bundled_node` (the uv `node.exe` is missing).

## 3. Ask the owner first (blocks W1)

Collect these answers in one message. Record them in a Linear comment on I1 (OVE-59), or as an ADR if they set policy.

1. **Model profiles.** Which pinned model component does each profile map to: `frontier.default` (all three missions), `frontier.long_context` (Mission 1), and `cursor.default` (Missions 2 and 3, Cursor model id)? AGENTS.md forbids inventing model routes.
2. **Sandbox profiles.** What do `research.standard` (Mission 1, with egress to pubmed, biorxiv, tavily and firecrawl) and `ingestion.standard` (Mission 2, no egress) map to: Deep Agents placement and sandbox backend (local, docker or Daytona), and limits?
3. **Secret refs per lane.** Which secret names each lane may receive (for example `TAVILY_API_KEY` and `FIRECRAWL_API_KEY` for Deep Agents research, and `CURSOR_API_KEY` for Cursor lanes). Names only.
4. **Biotech `kg_ingest`.** Which real app-owned capability implements it? `biotech-knowledge-services` (GraphQL API) is a candidate; the owner decides. The FT-E2 stand-in is a test row only.
5. **Event names (B7).** Should the kernel emit `run.completed`, `activation.completed` and `human_task.opened`, or should subscription filters alias the existing names?
6. **Workspace drift.** Restore `../.agents/skills/agent-browser` to its pinned bytes (bundle `sha256:30722859...`), or re-pin it? Was the removal of `../biotech-kg` intentional? If so, the schema tests need repointing.
7. **Mission 3 host.** Will the worker run under WSL or Linux? A dedicated clone of the target repo is needed instead of the primary checkout.
8. **Budget.** Is a cap approved on OVE-60 for the full Cursor Cloud run (Mission 2: research USD 30 + ingestion USD 15)? What is the finite `MC_PAID_BUDGET_USD` for the lane drills?

## 4. Remaining work, in order

Each item is tracer-bullet sized. Create a Linear issue per item in project Mission Control (team OVE): either write a draft under `issues/` and run `publish_issues.py`, or create it directly. Each item is blocked by the decisions it names.

### W1 — Production launch-input author (B1; blocks every start)

- **Gap.**
  - `LaunchInputPort` (`src/mission_control/application/authoring/manifest_submit.py:186`) is composed as `None`. `bootstrap/manifests.py` takes `launch_inputs: LaunchInputPort | None = None`, and no caller passes one.
  - `ChainLaunchInputPort` (`src/mission_control/application/chains/relay.py:187`) has no production implementation.
  - `ChainIntentRelay` (`relay.py:112`) is composed in neither the API nor the worker, so Mission 2's `ingestion` intent would stay pending.
  - The only author today is the test fixture `StagedLaunchInputs` (`tests/fixtures/manifest_runtime.py:107`, deterministic models). Read it as the reference shape.
- **Build.**
  - A model/sandbox/secret profile registry, configuration-backed and typed, with the profile names from section 3.
  - A production author that turns the compiled manifest (stored `mc.manifest_resolution.v1` + MissionDefinition@1 rows) into the family input (`StageGraphRunInput` / `GoalDirectedRunInput`) with real lane execution templates: model component, prompt segments, MCP servers, skills, capability grant, workspace contract, output schema, and `CursorExecutionBinding` for Cursor lanes.
  - Compose it in `bootstrap/manifests.py` and the API.
  - Compose `ChainIntentRelay` with the chain author on the worker (or the outbox relay process), following ADR-0029.
- **Accept.**
  - `scripts/fast_track_dry_run.py --with-fixture-rows` reaches a started run for all three manifests against local Temporal. The run is started, not executed with paid models; use a deterministic model profile mapping for the proof.
  - The chain test (`tests/acceptance/mission_control/test_chain_two_goal_loops.py`) still passes.
  - `make check` passes.

### W2 — Real capabilities in the catalog (B2)

- **Gap.** The production seeds lack:
  - **Mission 1:** `skill.biotech-literature-review`, the biotech schema context bundle (muscle aging), the Biotech `kg_ingest` tool, hook `citation presence check`, assessment `evidence coverage`.
  - **Mission 2:** a PubMed MCP qualified for `cursor_cloud` (remote endpoint), the `literature verifier` subagent, `claim schema validation`.
  - **Mission 3:** executors `test_run` and `git_snapshot`, subagent `code verifier`, hook `shell command policy`, assessment `code change verification`.
- **Stand-ins.** They live in `tests/fixtures/catalog/fast_track_seeds.json` (built by `tests/fixtures/catalog/fast_track_catalog.py`). Use them as a shape reference only.
- **Build.** Reviewed definitions, as new seed versions under `packages/mission-control-db-contract/seeds/` (generator `seeds/generate_catalog_seeds.py`, plus `seeds/agent_skills.py` for skills), or `missionctl catalog publish` for skills, hooks and subagent profiles. `kg_ingest` must be the real app-owned capability (decision 4).
- **Accept.** `scripts/fast_track_dry_run.py` without `--with-fixture-rows` compiles all three manifests with zero blockers.

### W3 — Worker pins and workspace drift (B3, B4)

- `infra/capability-pins/research-capabilities.json` pins only `model.wp-cp-040`, `mcp.tavily`, `mcp.firecrawl` and the `agent-browser` skill. Extend it, using `scripts/pin_research_capabilities.py` in a reviewed change, for PubMed, the Biotech KG server, the mission skills and the model components chosen in W1.
- Resolve the `agent-browser` drift per decision 6.
- **Accept.** `make worker` starts, and `tests/integration/postgres/test_mission_worker_startup.py` passes on `-m common_db`.

### W4 — Subscription event names (B7)

- The manifests subscribe to `activation.completed`, `human_task.opened` and `run.completed`. The kernel emits `activation.lifecycle_changed`, `workflow_run.set_wait` and `workflow_run.terminalize`. Filters match exact names. Implement decision 5 (`application/subscriptions/`, or the event writers).
- **Accept.** A subscription on each name receives the event in a `common_db` test.

### W5 — Cursor lanes: qualification and host (B5, B6) — owner-run parts marked

- **Owner, paid.** Run `make lane-qualify PROFILE=cursor_local LIVE=1`, then `cursor_cloud`, with a finite `MC_PAID_BUDGET_USD`. The runbook is `docs/qualification/lanes/README.md`. Flip `qualified=True` in `application/execution/harness/describe.py` only through a reviewed change that cites the drill record. Until then, `MISSION_CONTROL_ALLOW_UNQUALIFIED_LANES=true` serves local proof only.
- **Agent.** `cursor_local` cannot launch on a Windows worker, because psycopg forces a SelectorEventLoop and the bridge needs a Proactor or Unix loop. Prepare a WSL worker path, and change Mission 3's `repo.path` (a Windows path to the primary checkout today) to a dedicated clone. This is a fixture edit in `missions/03-codebase-feature-cursor-local.yml`, which is in I3's region.

### W6 — Known gaps (do after W1 to W4; none blocks a first run)

| Gap | Pointer | Note |
| --- | --- | --- |
| `request_continuation` is recorded but never fulfilled | `application/context/continuation.py`, `adapters/temporal/activities/continuation.py`; the activities and hydrators are not registered on the worker, and no family workflow calls seal/transfer | Needs workflow call sites behind `workflow.patched`, plus replay tests (`tests/integration/temporal/test_replay_histories.py`, `test_lane_replay_histories.py`). |
| `run transcript --full` answers 501 | no production `ArtifactBodyReader` | C3 |
| Cursor Cloud fork restore | the lane expects `branch:<branch>@<sha>`, and nothing records it | not needed by I2 |
| `transcript.project` activity not scheduled | C4 | searches refresh their run, so results stay correct |
| Webhook/MCP subscription relay is opt-in | `MISSION_CONTROL_SUBSCRIPTION_RELAY=1` on the API | SSE `events watch` needs no relay |
| `operation.execute` removal (G6 remainder) | `MISSION_CONTROL_LANE_SEGMENT_LOOP` defaults to false for Deep Agents; Cursor lanes always use `lane.turn` | Plan in the OVE-55 handoff: flip the default, keep the old path registered for one release, then remove it. |
| Doc drift | SPEC-07 says `.mission/bin/mc_hook.py`; the code writes `.mission/hooks/kernel.py`. ADR-0021 is still `status: proposed`. `application/frames/kinds.py::DEDUPE_KEY_RULES` docstring says `sse:<event id>`; cloud keys are `sse:<run>:<id>` | Fix the docs to match the code, or ask the owner. |

### W7 — Owner decisions that are not code (record the answers; do not act without approval)

- **Apply 1.1.0 to Supabase.** First run `create extension if not exists pg_trgm with schema extensions` on both projects, then plan/apply with an approval comment. Bindings must then pin `required_component_version="1.1.0"`.
- **A2 bucket policies.** Confirm the JWT claim `mc_capability_role` (`publisher` or `reader`). Applying `mc.storage.capability-bundles` needs approval on OVE-23.
- **D2 security review.** 0028 lets `mission_control_family_writer` INSERT into `mission_run`, `budget_account` and `effect_ledger` for chain-release admission. `tests/integration/postgres/test_pre_stage3_database_authority_integration.py` records the widened matrix.
- **Coordinator skill.** `mc.catalog.approved-assets@1.0.0` is frozen (applied); `@1.0.1` succeeds it. Publishing a coordinator skill revision 2 is the owner's call.
- **Keys.** `NCBI_API_KEY` and `EDGAR_IDENTITY` are unset. Keyless PubMed works for one run.

## 5. Definition of ready for the owner's fixture runs

1. W1 to W4 are merged on `main`. `make check` is green apart from documented environmental failures. The `-m common_db` suites for the touched areas pass on PG17.
2. `scripts/fast_track_dry_run.py`, without fixture rows, compiles, submits and **starts** all three missions against local Temporal with a deterministic model mapping, with zero paid units.
3. The runbook's section 0 table shows `mission start` as ready, and section 1 lists only owner-run items (lane drills, budget caps, Supabase apply).
4. A Linear comment on I1, I2 and I3 states readiness, with the dry-run evidence path.

## 6. Rules and lessons from the implementation run

- **Work.** Use one git worktree per item (`git worktree add ../mission-control-<item> -b <branch> main`). Never stash, reset or clean the primary checkout. Commit on the branch, then fast-forward `main`. Push only when the owner asks.
- **Gate.** Run `make check`. Persistence or Temporal claims need `MISSION_CONTROL_TEST_ADMIN_DSN=<pg17 dsn> uv run --no-sync --group biotech pytest -m common_db <selection>`. Do not run the db-contract package tests that DROP cluster roles on the shared server. Tests built on `tests/fixtures/rrm009_production_harness.py` share a dev server on `:7341` and flake under concurrent load, so rerun them alone before concluding anything.
- **No paid effects** without an approved cap in Linear: no Cursor agents, no model, embedding or search calls, no Supabase writes, no Temporal Cloud.
- **Skills cascade.** Any change under `skills/` requires, in order: `make skills-manifest`; then `uv run --no-sync python packages/mission-control-db-contract/seeds/generate_catalog_seeds.py --write` (it skips the frozen approved-assets 1.0.0); then `uv run --no-sync python -m tests.fixtures.catalog.fast_track_catalog`; then `UPDATE_GOLDENS=1 uv run --no-sync --group biotech pytest tests/unit/authoring/test_manifest_compile.py`.
- **Line endings.**
  - `skills/` and the db-contract component, runtime and seeds are LF-pinned by `.gitattributes`.
  - `0002_authoring.sql` and `0004_capability_artifacts.sql` are `-text` because their applied bytes (CRLF/mixed) must match the live receipts. Never normalise them.
  - New migrations go in a new number after 0030, with a component version bump and `mission-db release-build`. Released files are never edited.
- **Least privilege.** No login role may hold DELETE or TRUNCATE on `mission_control` tables (the authority test enforces it). Use a SECURITY DEFINER function, as frame retention does (`mission_control.expire_provider_frames`).
- **Canonical digests.** New optional fields on recorded contracts use pydantic `exclude_if` or `json_schema_extra={"digest_omit_default": True}`, so existing digests do not change.
- **Clock.** Do not compare fixed datetime constants with the wall clock in tests.

## 7. Kickoff prompt for the fresh session

```text
You are finishing Mission Control's pre-fixture work in C:\Users\Pinda\Proyectos\Biotech\mission-control.
Read docs/specs/fast-track-2026-10/HANDOFF-PRE-FIXTURE.md first and follow it. Start by asking me the
section 3 questions in one message. Then implement W1 to W4 (and the agent parts of W5) in worktrees,
with a Linear issue per item (team OVE, project Mission Control; LINEAR_API_KEY is in .env, never print it).
Do not run paid fixtures or apply anything to Supabase. Finish when section 5 "Definition of ready" holds,
and report passed/failed/blocked/unrun checks separately.
```
