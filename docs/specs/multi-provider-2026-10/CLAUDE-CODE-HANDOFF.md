# Claude Code handoff — multi-provider packet

> **Superseded, 2026-10-10:** read [HANDOFF-2026-10-10.md](HANDOFF-2026-10-10.md) for the current state, remaining work and resume steps. This file and the recovery-audit note below are historical provenance only.

> **Recovery audit, 2026-10-09:** this handoff is historical. Integrated work is now committed/pushed through `efa55f9` in `BellLabs/platform/mission-control`; MP-07/08/09/11/12 retain unintegrated local changes. Read [RECOVERY-HANDOFF-2026-10-09.md](RECOVERY-HANDOFF-2026-10-09.md) before acting on the older checkout, commit, migration or port-script instructions below.

Owner directive, 2026-10-09: continue this implementation in Claude Code. The earlier kickoff line “do not move this implementation to Claude Code” is superseded by that directive. Cursor (Fable 5.1 lead, Opus 5.5 specialists) stopped after integrating wave 2 except MP-10, with integrator wiring still open.

Nothing in this packet has been committed or pushed. Do not commit, push, deploy, run a live migration against the owner database, or make a paid provider call unless the owner asks in the new session.

> **Status 2026-10-09 (Claude Code session, cut off by the usage limit):** the integrator deltas below for MP-06 / MP-14 / MP-22 are applied; MP-10 and MP-15 are ported and composed; migration **0032** (`run_cluster_binding`, stream-hint triggers) is allocated and release-built (fingerprint `sha256:672549cd…`), next slot **0033**; `claude-agent-sdk==0.2.165` is pinned. Wave 3 (MP-07/08/09/11/12) ran as subagents in `.scratch/multi-provider-2026-10-08/worktrees/` and may still be finishing; MP-23 pass 1 docs are in. The ledger's last entry lists the exact next steps. The sections below describe the state at the Cursor stop and are kept as provenance.

## Where the work is

| Item | Value |
| --- | --- |
| Repo | `C:\Users\Pinda\Proyectos\Biotech\mission-control` |
| Branch | `main` |
| HEAD | `7c9b755d75c4417ae364f33c340b9891446a391a` (unchanged) |
| Working tree | about 104 modified, 3 deleted, 81 untracked. This *is* the integrated result. Do not reset, stash, or check out a clean tree. |
| Packet | `docs/specs/multi-provider-2026-10/` (`README.md`, `ARCHITECTURE.md`, `ISSUES.md`, `VALIDATION.md`, `SPEC-01`..`SPEC-04`, `CURSOR-KICKOFF.md`, `TEAM-WORKSPACE.md`) |
| Ledger | `.scratch/multi-provider-2026-10-08/team/LEDGER.md` (append; do not rewrite) |
| Per-ticket handoffs | `.scratch/multi-provider-2026-10-08/team/handoffs/MP-*.md` |
| Claims | `.scratch/multi-provider-2026-10-08/team/claims/` |
| Child worktrees | `.scratch/multi-provider-2026-10-08/worktrees/<ticket>/` on `mp/<ticket>-<slug>` |
| Port script | `C:\Users\Pinda\AppData\Local\Temp\mc-integrate.py <TICKET> [--check]` |
| Linear | overtonbell / OVE / Mission Control, parent **OVE-63** |

The port script copies a child’s unstaged diff and untracked files into this checkout only when the child’s staged base matches the owner’s working copy modulo CRLF. Run `--check` first. It skips `.scratch-handoff/` and it mis-parses renames (`R `): two fixture files are unstaged deletions because of that (`tests/fixtures/projections/codex/.codex/agents/summarizer.md` and `verifier.md`). Confirm they were replaced by regenerated projection fixtures before restoring them. `app/.cursor/rules/user_subagent_preference.mdc` was already deleted before this packet.

## What is integrated (code on `main`, uncommitted)

| Ticket | Linear | State |
| --- | --- | --- |
| MP-01 contracts + migration 0031 | OVE-64 | Integrated. Likely already Done in Linear. |
| MP-02 production launch inputs + chain relay | OVE-65 | Integrated and reviewed. Not account-enabled. |
| MP-03 capability projections | OVE-66 | Integrated. `ProjectionReport.unqualified` applied. |
| MP-04 workspace leases / snapshots | OVE-67 | Integrated. `mc.workspace_artifact_manifest.v1` registered in application code. Cursor cloud rewire still open (MP-09). |
| MP-05 auth / usage admission | OVE-68 | Integrated, including `bootstrap/provider_auth.py` and the settings delta. No route is account-enabled. |
| MP-13 frames, lineage, subscriptions | OVE-76 | Integrated, including artifact-body reader registration in `bootstrap/api.py`. |
| MP-16 hosted Claude Code feasibility | OVE-79 | Outcome 3. Stay unqualified. Docs under `docs/qualification/lanes/claude_cloud/`. No hosted session. |
| MP-17 hosted Codex feasibility | OVE-80 | Outcome 3. Stay unqualified. Docs under `docs/qualification/lanes/codex_cloud/`. |
| MP-06 session ownership, dispatch journal, stop-fence admission, `mp05-capacity-wait` | OVE-69 | **Ported into this checkout. Not marked Done.** Composition wiring below was not applied. |
| MP-14 `/missions` Socket.IO | OVE-77 | **Ported. Not marked Done.** Bootstrap wiring below was not applied. Frozen files were not edited in the child. |
| MP-22 local readiness, example deep_agents bindings, cluster guard | OVE-85 | **Ported, including `bootstrap/preflight.py`. Not marked Done.** Persistence/settings/worker deltas below were not applied. |
| ADR-0035..0040, glossary | — | `proposed` docs. Next free ADR number is **0041**. |

Seven lane profiles exist: `deep_agents`, `cursor_local`, `cursor_cloud`, `claude_agent_sdk`, `codex`, `claude_cloud`, `codex_cloud`. Hosted profiles are unqualified stubs. Do not flip a qualification without a recorded drill. Cloud means the provider-hosted Cursor, Claude Code, or Codex product. An Anthropic model inside Cursor or Claude Code is not the Claude-hosted product.

Migration head is **0031** (`packages/mission-control-db-contract/component/migrations/0031_multi_provider_lanes.sql`). Next free slot is **0032**. Release fingerprint after the 0031 fixes:

`sha256:2cfc77e32f68afbeb368f9286aa252f0a0513db637c7c4eab2f4c5544bbaf31f`

(`manifest.json` and `generated/contract.json` / `contract.md` were regenerated). 0031 does two things beyond the original lane CHECKs:

1. `CREATE OR REPLACE FUNCTION mission_control.capability_host_support_valid` admits all 7 lanes. Without this, capability publish hits `asset_version_host_support_check`.
2. The four new `lane_profile` seeds run between `NO FORCE ROW LEVEL SECURITY` and `FORCE ROW LEVEL SECURITY`. 0030 left the table forced with a SELECT-only policy, so a non-superuser migrator (db-contract cluster B) cannot insert otherwise. Superuser `release-build` does not catch this.

`deployments/biotech/release.lock.json` and `deployments/ai-engineer/release.lock.json` still pin the pre-0031 manifest. Do not `mission-db lock` until the owner accepts release 1.1.0+0031.

## Integrator fix already applied after the MP-06 port

`adapters/temporal/workflows/operation.py` imported `mission_control.application.execution.usage_admission` inside the workflow. The architecture gate forbids that. The pure module was moved to `domain/execution/usage_admission.py`. `application/execution/usage_admission.py` is a re-export shim, including `OverageState`. The workflow now imports the domain module. After that change: ruff clean, mypy clean (528 files), `tests/architecture` + `tests/unit/provider_auth` 105 passed.

## Not integrated

**MP-10 / OVE-73 (human gates) is still only in its worktree** `.scratch/multi-provider-2026-10-08/worktrees/MP-10` (`mp/MP-10-human-gates`, agent `d48fdea4`). There is no `handoffs/MP-10.md`. Unstaged edits: `workflows/goal_directed.py`, `workflows/stagegraph.py`, `application/programs/goal_directed.py`, `application/programs/service.py`, `domain/programs/contracts.py`, `domain/programs/goal_directed_runtime.py`. New modules under `application/human_tasks/`, `domain/programs/human_gate.py`, `human_review.py`, `adapters/postgres/human_tasks/`, `adapters/temporal/workflows/human_gate.py`, HTTP and MCP surfaces, and tests. Scratch logs are in that worktree’s `.scratch-handoff/`. Finish or review it there, write the handoff, `--check` the port, then port. Do not copy it blindly over `main`.

Not started: MP-07 OVE-70 (Claude Agent SDK lane), MP-08 OVE-71 (Codex app-server lane), MP-09 OVE-72 (Cursor local/cloud parity), MP-11 OVE-74 (approval bindings), MP-12 OVE-75 (continuation / compaction), MP-15 OVE-78 (coordinator subscriptions), MP-18 OVE-81 and MP-19 OVE-82 (hosted lanes — stay blocked on Outcome 3), MP-20 OVE-83, MP-21 OVE-84, MP-23 OVE-86 (parity proofs and the release statement).

## Wiring the previous integrator still owed

Do these in this checkout. They are the reason MP-06 / MP-14 / MP-22 are not Done. Exact proposals are in the handoff files; do not invent a second design.

### MP-06 (`handoffs/MP-06.md`)

Ownership and the dispatch journal already run through `PostgresLaneExecutionStateStore` (`native_identity` keys `mc_session_owner` and `mc_dispatch`). What is not wired:

1. `adapters/temporal/deployment_composition.py` around the `LaneTurnService(` call (~line 893): pass `sessions=WorkerSessionManager(owner_ref=default_owner_ref(self._worker_identity))` and `fences=PostgresStopFenceRepository(postgres_pool)`. Without `fences=`, production does not admit new native dispatches against the Stop Fence. `PostgresStopFenceRepository` is already imported and used elsewhere in that file.
2. Settings (frozen for the children, writable for you): `mission_control_session_lease_min_s` (10), `mission_control_session_lease_heartbeats` (2), `mission_control_dispatch_receipt_grace_s` (10). Today those are constants on `WorkerSessionManager` / `DISPATCH_RECEIPT_GRACE_S`.
3. Optional, not required for correctness: migration 0032 moving owner and journal out of `native_identity` into typed columns plus `native_dispatch`. The handoff has the SQL. If you add it, you own 0032, then `mission-db release-build` on the disposable cluster only, and regenerate the contract artifacts.
4. Optional contract delta: `limit_wait_ledger` on `OperationWorkflowRequest` so the MP-05 wait ledger survives continue-as-new. Today it resets; MP-05 bounds still cap each run.
5. Production lanes still do not implement `DispatchReconcilingLane` or `SteeringLane`. That is MP-07 / MP-08 / MP-09. Until then an ambiguous Cursor send parks `in_doubt` (no blind resend).

### MP-14 (`handoffs/MP-14.md`)

New code is in `application/streams/`, `adapters/realtime/stream_source.py`, `stream_hints.py`, `interfaces/socketio/`. Not wired:

1. `bootstrap/api.py`: per tenant, `application.state.mission_control_stream_services[key] = MissionStreamService(PostgresStreamSource(pool, request_scope), request_scope=...)`. Initialize the dict next to the subscription services.
2. New `bootstrap/realtime.py` with `create_asgi_app` as in the handoff (`uvicorn mission_control.bootstrap.realtime:create_asgi_app --factory`). Close the Redis hint client on shutdown. `create_app` must keep working without the socket.
3. Setting `mission_socket_redis_fanout: bool = False`, reusing `redis_url` and `socketio_cors_origins`.
4. Hint publishers are unwired. Correctness does not depend on them (pumps poll). Latency does. Either publish after reducer commit / frame append, or add the proposed 0032 `LISTEN/NOTIFY` (same slot decision as MP-06 — one owner, one migration number).
5. Contract deltas not applied: add `UNAVAILABLE` and `UNSUPPORTED_OPERATION` to the stream error codes. `resolve_human_task` stays a typed refusal until MP-10/11 expose a public handler.

### MP-22 (`handoffs/MP-22.md`)

`preflight.py`, the deep_agents bindings example, `local-run-profile.example.json`, and the readiness/outage tests are in the tree. Not wired:

1. Persist `run_cluster_binding` (proposed 0032 table) and refuse `RunLaunchService.launch` when the stored cluster differs. The file ledger in preflight is the local stand-in only.
2. Settings `local_run_profile_path` and `preflight_workspace_root`.
3. Worker startup: `check_lane_hosts` and `check_pins` before polling.
4. Register goal executor/verifier JSON schemas under `sha256_digest(Model.model_json_schema())` or the example’s structured output runs with `response_format=None`.
5. `preflight.py` is ~1263 lines because it was the only writable source file. A split into `bootstrap/readiness.py` and `application/operations/cluster_guard.py` is reasonable after the behavior is covered.

### MP-05 / MP-02 leftovers

- `mc.auth_admission.v1` persistence is deferred until a consumer needs 0032+.
- MP-07/08/09 child processes must be built with `provider_child_environment` (drops `AuthAdmission.env_unset`).
- There is no owner-authored production `mc.manifest_launch_bindings.v1`. The example is `deployments/examples/manifest-launch-bindings.deep-agents.example.json`. `preflight compose-bindings` fills `OWNER-SELECT:` placeholders. Do not invent production model or sandbox choices.

## Suggested order

1. Read this file, `LEDGER.md`, and `handoffs/MP-06.md`, `MP-14.md`, `MP-22.md`.
2. Apply the wiring in the three lists above. Re-run the real-service modules named below. Then mark OVE-69, OVE-77, OVE-85 Done with evidence, only after the wiring is in this checkout.
3. Finish MP-10 in its worktree, port it, prove it, then OVE-73.
4. Dispatch the unblocked lanes: MP-07, MP-08, MP-09, MP-12 after MP-06 wiring; MP-11 after MP-06 and MP-10; MP-15 after MP-14’s stream service is actually composed. One writer per directory. MP-18 and MP-19 stay blocked.
5. MP-20 / MP-21 / MP-23 and a per-profile release statement. Local deep_agents proof is not all-provider parity. Hosted profiles stay unqualified.
6. A bounded cleanup pass after the vertical slice (naming, dead code, comments). Do not weaken tests to get green.

## Checks already run on this checkout

Commands use `uv run` from the repo. PostgreSQL and Temporal are local Docker, not the owner app database.

Disposable PG 17 container `mc-ft-disposable-pg17` at `127.0.0.1:55433`. Password only via `docker inspect` into `MISSION_CONTROL_TEST_ADMIN_DSN` for one command. Never print it. Never touch `127.0.0.1:55432`.

```text
PW="$(docker inspect mc-ft-disposable-pg17 --format '{{range .Config.Env}}{{println .}}{{end}}' | grep '^POSTGRES_PASSWORD=' | cut -d= -f2- | tr -d '\r')"
MISSION_CONTROL_TEST_ADMIN_DSN="postgresql://postgres:${PW}@127.0.0.1:55433/postgres"
```

Temporal is `127.0.0.1:7233`. Redis pub/sub for the socket test is `redis://127.0.0.1:16379/0` (`MISSION_CONTROL_TEST_REDIS_URL`). db-contract clusters, if you need them: `uv run python packages/mission-control-db-contract/scripts/disposable.py start` (`mcdb-a` 55501, `mcdb-b` 55502).

| When | Result |
| --- | --- |
| Before wave 2 port | `make check`: ruff, format, deptry, architecture clean. `ty` 9 pre-existing warnings in untouched files. `tests/unit` 2183 passed / 11 failed. `make skills-check` ok. |
| Full `tests/integration/postgres` after the 0031 fixes, before wave 2 | 172 passed / 1 failed (`test_mission_worker_startup[True]`, agent-browser pin). |
| db-contract package suite | 62 passed / 2 failed. Both failures exist at HEAD `7c9b755`: tests still expect component version `1.0.0` after commit `95aea03` bumped it to `1.1.0`. Do not “fix” them by reverting the version. |
| After porting MP-06/14/22, before the domain move | ruff clean, format clean (1122 files), mypy 527 files. Targeted units 493 passed and 1 architecture failure (the import fixed above). |
| After the domain move | architecture + `tests/unit/provider_auth` 105 passed; mypy 528 files. Full `tests/unit` was **not** re-run after the wave 2 port. |
| Real PG after the port | 19 passed: `test_mp06_dispatch_journal_postgres`, `test_mission_socket_postgres`, `test_mp22_db_release_preflight`, `test_lane_execution_state`, `test_stop_fence`, `test_subscriptions`. |
| Real Temporal after the port | 72 passed, including `test_mp06_dispatch_recovery`, `test_mp05_limit_wait`, lane turn, replay histories, mailbox, `test_mp22_outage_drill`, `test_mp22_local_profile_start`, `test_manifest_launch_production`. |

The 11 unit failures to expect until the owner acts:

- 6 in `tests/unit/schema/*` because `biotech-kg/typedefs.graphql` and `biotech-kg/src/schema/neo4jbiotechschema.graphql` are absent. Pre-existing at HEAD.
- 5 from workspace skill drift: `tests/unit/capability/test_agent_skill_seeds.py` (3), `tests/unit/control_plane/test_catalog_seed_bundles.py` (1), `tests/unit/integrations/test_capability_pins_and_runtime_ports.py::test_workspace_artifacts_verify_against_their_pins_when_present`.

## Owner decisions (do not take them yourself)

1. **agent-browser pin.** Workspace bundle `C:\Users\Pinda\Proyectos\Biotech\.agents\skills\agent-browser` (outside this repo). MP-22’s later measurement: top-level `SKILL.md` still matches `skill_md_digest` `sha256:328161bf…`. The bundle digest misses pin `sha256:30722859…` because of a nested copy `agent-browser\agent-browser\` (computed bundle `sha256:f65791f5…`). Either move that nested directory out (no re-pin) or authorize a re-pin. This blocks `test_mission_worker_startup[True]` and any live worker that resolves the pin.
2. **CRLF vs LF.** `skills-lock.json` matches CRLF bytes for 52/53 bundles. This checkout has `core.autocrlf=true`. Do not add `.gitattributes` `eol=lf` for `.agents/skills/**` and renormalize; that was tried and reverted (digest failures got worse). Either regenerate the lock and seeds from LF, or keep the Windows lock.
3. **Release locks** for `biotech` and `ai-engineer` after 0031 is accepted.
4. **Production bindings and local-run profile** (18 `OWNER-SELECT:` pointers listed in `handoffs/MP-22.md`). Exporting `OPENAI_API_KEY` is an owner action with an explicit budget. Readiness checks presence only; do not read secret values into the handoff or the ledger.
5. **cursor_local / claude_agent_sdk / codex workers** need Linux, macOS, or WSL. Windows selector loops cannot spawn subprocesses. `claude_cloud` / `codex_cloud` are refused on every host until Outcome 3 is replaced by a real qualification.

## Rules that still bind

- Temporal alone schedules. Reducers own lifecycle and acceptance. Extend `AgentHarness`, the context packet, mailbox, stop fence, capability projections, frame store, human tasks, subscriptions, and the outbox. Do not add a second orchestration runtime.
- Shared contracts, enums, workflow families, bootstrap, migrations, and lockfiles have one owner. You are that owner now.
- Preserve current Deep Agents behavior.
- Do not make a test pass by deleting coverage, weakening an assertion, or treating a fixture lane as a live provider.
- Research output is not medical advice. Do not commit secrets or PHI.
- Repo gate when you are ready: `make check` (`uv run ruff check .`, `uv run mypy`, `uv run --group biotech pytest`). Persistence claims need the disposable PostgreSQL 17 and the local Temporal, not skips.

## First message to paste

```text
Continue the Mission Control multi-provider packet in
C:\Users\Pinda\Proyectos\Biotech\mission-control. Read
docs/specs/multi-provider-2026-10/CLAUDE-CODE-HANDOFF.md and follow it.
The integrated work is uncommitted on main at 7c9b755. Do not reset or stash.
Do not commit, push, or call a paid provider unless I ask.
Start with the unwired MP-06, MP-14, and MP-22 integrator deltas, then MP-10
in its existing worktree.
```
