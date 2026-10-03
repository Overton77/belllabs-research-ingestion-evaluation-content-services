# RRM-010 readiness manifest

**Status: `ready_for_separate_fixture_session`**

Recorded: 2026-10-03 (America/New_York). Branch `wp/rrm-010-readiness`, based on integration `0c5ec35`.

- **Prerequisite revision:** tested code head `7e0b77e` (test-only changes on top of `0c5ec35`; no `app/` change in RRM-010). The commit that adds this manifest changes documentation only. The coordinator adds the integration merge commit.
- **Accepted meta revision:** `biotech-meta` `6c89143`, merged into meta `main` at `a50d833` (AMD-RRM-001).
- **Held:** RRM-011 stays held until the user starts a separate fixture session. **CP-050 stays unaccepted** until its complete tracer evidence exists. This manifest authorizes no fixture launch.

## 1. Ticket dispositions

| Ticket | Disposition | Tested head | Merged into integration |
|---|---|---|---|
| RRM-001 lifecycle contract authority | accepted | meta `6c89143` (meta `main` `a50d833`) | `57c99bd` |
| RRM-002 verification baseline | accepted | `ea0f529` | `ebd1ca1` |
| RRM-003 checkpoint lineage | accepted | `07ac167` | `5b5cb55` |
| RRM-004 checkpoint and settlement recovery | accepted | `d296481` | `fcefd54` |
| RRM-005 inspection and Search Attributes | accepted | `62409d8` | `ed598dd` |
| RRM-006 semantic forks | accepted | `74e088d` | `b54e0cf` |
| RRM-007 boundary interventions | accepted | `0475079` | `aeb0c62` |
| RRM-008 running cancellation | accepted | `4f8543b` | `027cc42` |
| RRM-009 production capability composition | accepted | `6990a8d` | `5006770` |
| RRM-013 async subagents on the Agent Server | accepted | `235ee4a` | `d7d2f01` |
| RRM-015 set-order-stable digests | accepted | `83af2ff` | `2b1da64` |
| RRM-016 GoalDirected through the run-control journal | accepted | `79bd1d3` | `f99ac1d` |
| RRM-018 / RRM-019 GoalDirected multi-iteration | accepted | `d552278` | `468df99` |
| RRM-020 shared GoalDirected workspace | accepted | `cea2d50` | `4bf0010` |
| RRM-021 StageGraph baseline settlement | accepted | `45e9b6d` | `c4e8e30` |
| Clean-code passes CR-1 to CR-5 | merged (behaviour-preserving) | see `CLEANUP.md` | `08def14`, `c850526`, `1bdfd5c`, `b836c20`, `548dff2` |
| RRM-010 stability items | merged | `f520a47` | `94096ff` |
| **RRM-010** readiness gate | implemented; coordinator review pending | `7e0b77e` | (coordinator) |
| RRM-011 company fixtures | **held** | | |
| RRM-012 reference-research journal authority | open, not mission-blocking | | |
| RRM-014 re-admit a unit at generation g+1 | open, not blocking RRM-010 | | |
| RRM-017 patchable fork fields and `cognitive_seed` | open, not blocking RRM-010 | | |

## 2. Agent Server actions (this session)

- **Rebuilt** the `rrm009-agent-server` stack from this worktree at `0c5ec35` (`langgraph up --config langgraph.async_subagents.json --api-version 0.12.0 --no-pull`, `COMPOSE_PROJECT_NAME=rrm009-agent-server`, port 8144). The API container `rrm009-agent-server-langgraph-api-1` was recreated on image `sha256:9dd1ea502a8b…`. `rrm009-agent-server-langgraph-redis-1` and `rrm009-agent-server-postgres` (volume `rrm009-agent-server-pgdata`) were kept. The previous image `8a70a5f007c9` is left dangling, not pruned. RRM-010's test changes are under `tests/`, which `.dockerignore` excludes, so the image is the same for `7e0b77e`.
- **Live claim probe** (status codes only, no secret printed), on both the custom identity route and the Agent Protocol `POST /threads/search`:

  | Bearer | Before the rebuild | After the rebuild |
  |---|---|---|
  | valid 30-minute claim with `jti` | 200 | 200 |
  | forged 7-day claim with `jti` | **200** | **401** |
  | forged 2-hour claim with `jti` | **200** | **401** |
  | claim without `jti` | **200** | **401** |
  | claim with a malformed `jti` | **200** | **401** |
  | raw static secret / no auth | 401 / 401 | 401 / 401 |
  | valid claim, other scope header (Agent Protocol route) | 403 | 403 |

- **Removed** the obsolete `rrm013-agent-server` stack by exact name: containers `rrm013-agent-server-langgraph-api-1`, `rrm013-agent-server-langgraph-redis-1` and `rrm013-agent-server-postgres` (`docker rm -f -v`, which also removed the redis container's anonymous volume). `docker volume ls --filter label=com.docker.compose.project=rrm013-agent-server` listed no labelled volumes; volume `rrm013-agent-server-pgdata` and network `rrm013-agent-server_default` were removed by name. Nothing was pruned. `csi01`, `rrm-app-*` and `rrm009-*` were not touched beyond the rebuild. The image `rrm013-agent-server-langgraph-api` remains; the user can remove it by name.

## 3. Combined technical smoke

`tests/acceptance/control_plane/test_rrm_010_combined_smoke.py` over `tests/fixtures/rrm010_combined_smoke.py`. It is one test on one production stack: `open_production_stack`, the deployment API composition, `ProductionWorkerActivityCompositionFactory` workers, a persistent `start_local` namespace, the disposable application PostgreSQL and Mongo, and the rebuilt `rrm009-agent-server`. Every step goes through the governed facade. Parent cognition is deterministic. The only live model is the hosted async child (`gpt-5.6-luna`).

Command (stack lock held, `rrm009_env.sh` sourced and never printed):
`BELLABS_RUN_RRM_010_LIVE=1 LANGSMITH_TRACING=false TEST_APPLICATION_POSTGRES_DSN=… TEST_MONGODB_URI=… uv run --no-sync --env-file ../biotech-research-ingestion-evaluation-system/.env pytest -q -s tests/acceptance/control_plane/test_rrm_010_combined_smoke.py`
Result: **1 passed in 161 s**.

Provenance note: the passing run was made on the working tree that became `7e0b77e`. Before the commit, one evidence-only line in `run_cancellation_drill` changed: the printed `effects` entry is now a sorted list of `(kind, disposition)` pairs instead of a dict. No assertion changed, and the smoke was not re-run afterwards (coordinator wrap-up).

| Phase | Run IDs | Proven |
|---|---|---|
| Capability availability | | Pins mounted and verified: `mcp.tavily` (`sha256:60d2f3d0…`, 5 tools), `mcp.firecrawl` (`sha256:69e305ec…`, 26 tools), skill `agent-browser` (`sha256:30722859…`), tool `agent_browser_page`, and one checkpointer and one store digest. Readiness reports `search_attributes: verified` and `launch: composed`. `/health/ready` returns `{status, mode}` only. The Agent Server serves `belllabs_async_technical_child`, binding `sha256:764f0ef5…`, deepagents 0.7.5, matching the contract exactly. |
| StageGraph, active real async child, cancellation | run `b9531717-ca57-5704-90c3-4c3a29664121`; child `b369aea7-342f-5f25-8a05-bae5db4fa700`; provider run `01a0fffa-cdca-7910-9f0f-ae7312e15625` | `draft` settles. Its declared wait is released (`accepted → delivered → applied`). `review` spawns a real `required_blocking` child and is held. **Inspection while active:** the run is listed under `phase=active`; every run section is `current`; the child is shown (authority lifecycle `pending`, detail `submitted`, provider run); the unit is `active` with lease `held` and owns the child. **The fork admission classifies the child:** the snapshot is refused `snapshot_not_quiescent` with `async_child_active:b369aea7-…` among its reasons. **Cancel through the facade:** the child is cancelled at the provider (`interrupted`) and `provider_acknowledged`. Its result is rejected, its usage stays `pending_usage` (10 tokens) and the run stays `cancelling`. The privileged reconciliation attributes 2,363 tokens (`settlement_revision 2`) and hints the family. The run is terminal `cancelled` 0.5 s later, with receipts `accepted → delivered → applied`, transition `interrupted`, effects `operation.runtime` cancelled and succeeded and `async_subagent.child` cancelled (all settled), and nothing reserved or pending. Heartbeat is 10 s on `operation.execute` and `operation.cancel`. Root and family replay (134 events). Terminal inspection shows the child `cancelled`/`reject`/settled, and the cancelled unit's checkpoint history (6 entries) is `current`. |
| StageGraph inspection, historical read, fork, wait releases | source `d9d58c11-ae8f-59fe-a10f-d6f643b936f5`; derived `1c2a393f-766a-5f73-a625-1d0c2ed2a59a`; fork `fork-d9d58c11` | While waiting: every inspection section is `current`; the unit is `settled`; checkpoint history has 11 linked, stamped entries. A **historical read** of step 3 (pending `tools`, not the head) returns a redacted summary (`sha256:d4954112…`). Authority is identical before and after the reads. A `stage_settled` snapshot is taken. The fork patches the `review` objective and is idempotent on replay. It is **independently admitted**: the derived run is `pending` at version 1, epoch 1, with its own admission ref, and `draft` is reused by immutable ref (`settled_compatible_outside_frontier`). The derived run is launched with `parent_run_id`, and Visibility `BellLabsParentRunId` lists 1. The derived run's wait is released (`applied`) and it completes. **The parent is unchanged:** the authority digest (run, budget, effects, family head, units, results, transitions, claims, namespaces, saver checkpoints) and its projection are equal before the fork and after the whole derived run. The source's wait is then released (`applied`) and it completes. The draft output is shared, the review output differs. Lineage shows `write_file`, the MCP tool and the sync `task`. Four histories replay (210 events). |
| GoalDirected pause/resume and fork | source `d98c21ff-6ecd-54df-917d-2399bebca61c`; derived `ad833a5f-3540-54a8-ae41-393c2b4f0d4f`; fork `fork-d98c21ff` | A pause is sent while the first executor's model call is held. It is `delivered` while the phase is still `active`, then `applied` at the iteration boundary: phase `paused`, both iteration-1 units settled, the family records `next_goal_iteration 2`. The resume is `applied`. The run completes with 4 accepted settlements and promoted output `artifact:rrm009-goal:2` only. A `goal_verifier_settled` snapshot (iteration 2) is taken and forked with a patched `goal.objective`. Every unit is `not_reusable` (`goal_revision_identity_is_run_bound`) and nothing is reused. The derived run is independently admitted, launched with `parent_run_id`, and completes with its own 4 settlements (disjoint from the parent's). **The parent's authority digest and projection are unchanged.** Four histories replay (306 events). |

Every terminal run: no reservation, nothing reserved, nothing pending settlement, every effect claim settled; completed runs carry accepted settlement and output evidence.

**Live spend (this session):** four smoke invocations, each spawning one hosted child. Attempts 1–3 failed on test expectations before the cancel (see §5), so their children were not cancelled and finished on their own (`wait_seconds(120)`, then a reply). Attempt 4 passed with 2,363 attributed tokens. The total is about 10–15k `gpt-5.6-luna` tokens, almost all input. Exact prices were unavailable; at the mission's assumed USD 2.50/M input and USD 15/M output it is **under USD 0.10**. The claim probes made no model call.

## 4. Gates on `7e0b77e`

Hermetic runs unset both DSNs. DSN runs export them and add `--env-file ../biotech-research-ingestion-evaluation-system/.env`. Common flags: `BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0`, `unset VIRTUAL_ENV`, `uv run --no-sync`, one process at a time, stack lock held.

| Gate | Seed / tracing | Result |
|---|---|---|
| `ruff check app tests scripts` | | All checks passed |
| `mypy app` | | no issues in 384 source files |
| `git diff --check 0c5ec35 HEAD` | | clean |
| H1 full hermetic, one process | `PYTHONHASHSEED=1`, `LANGSMITH_TRACING=false` | **1091 passed, 94 skipped, 2 xfailed** (207 s) |
| H2 full hermetic, one process | `4242`, false | **1091 passed, 94 skipped, 2 xfailed** (193 s) |
| H3 full hermetic, one process | `90210`, false | **1091 passed, 94 skipped, 2 xfailed** (187 s) |
| D1 chunk A `tests --ignore=tests/acceptance` | `2024`, **`LANGSMITH_TRACING` not forced** | 1085 passed, 27 skipped, 2 xfailed (315 s) |
| D1 chunk B `tests/acceptance` | same | 63 passed, 10 skipped (377 s) |
| **D1 total** | | **1148 passed, 37 skipped, 2 xfailed** |
| D2 chunk A | `31337`, false | **aborted at the 595 s limit** after about 133 tests, with one `F` (see §5). Not counted. |
| Replay suites | | Inside H1–H3 and D1: the RRM-007 pre-change histories, RRM-016/RRM-019 post-change histories, the WP-BP-010/020 replays and the RRM-004 restart. The separate replay command was not re-run. |
| Combined smoke (live) | false | 1 passed (161 s) |

**Deltas against the baselines** (hermetic 1091/93/2, DSN 1148/36/2): **+1 skipped** in both, the new combined smoke, which skips without `BELLABS_RUN_RRM_010_LIVE=1`. Pass and xfail counts are unchanged. The refactored drill and the moved RRM-006 digest helper are covered inside these totals (the RRM-009 sync cancellation drill and the RRM-006 demonstrations pass in D1). The repeated-run stability evidence for the RRM-007 waits and the LangSmith isolation is in [`STABILITY.md`](STABILITY.md): three hermetic and two DSN passes, all exact. D1 here is again the run with ambient `LANGSMITH_TRACING`, and it passed.

## 5. Unresolved gates and residuals

**From this session**
1. **D2 chunk A timeout.** Chunk A of the second DSN pass (`PYTHONHASHSEED=31337`, tracing forced) ran only about 133 tests in 595 s (D1's chunk A ran 1,114 tests in 315 s) and printed one `F`. The `F` sits at the collection position of `test_rrm_007_boundary_interventions.py::test_goal_directed_policy_pause_is_durable_across_forced_continue_as_new`. That position is inferred from the progress output, because the summary was lost to the kill. That file passed 6/6 alone with the same seed (18 s), and the test passed in H1–H3 and D1. **Root cause not established**: the coordinator ended gate work at this point. The run's slowness before the failure points to host load or a stalled service, not to the seed. Re-run D2 under the lock before relying on a second DSN pass. The RRM-010 stability checklist item stays open on this point.
2. **In-flight checkpoint history.** For a unit still in its first generation's cognition, the checkpoint history read is `unavailable` with reason `no_recorded_checkpoint_key`. The reader anchors on recorded checkpoint keys and never reads unrecorded saver state. It becomes `current` once a transition is recorded, for example after the cancel. The active async child is visible through the run and unit reads. Inspecting live cognition mid-flight is not supported.
3. **Smoke expectations corrected while developing**, not product defects: the inspection's authority `lifecycle` is the latest recorded run-control fact (`pending` until a terminal fact), while the Mongo detail carries `submitted`/`cancelled`; and a StageGraph run with no accepted stage is `unsupported_boundary`, not `snapshot_not_quiescent`, so the smoke holds the `review` stage after `draft` settles.
4. **Observation (not triaged):** completed GoalDirected runs report `consumed["goal.iterations"] == 0`. The per-iteration reservation is settled without a `goal.iterations` usage record, and no code records that dimension. Stopping is still bounded by the blueprint's iteration limits. The coordinator should decide whether this needs a ticket.
5. Uncancelled hosted children from smoke attempts 1–3 ran to completion on the Agent Server with no BellLabs settlement, because their parent runs' disposable databases were reset. This is technical spend only.

**Open tickets:** RRM-012 (reference-research harness journal authority, strict xfail), RRM-014 (re-admission at generation g+1), RRM-017 (patchable fork-field declarations and `cognitive_seed`).

**Harvested residuals from the accepted evidence**
- *Agent Server and credentials (RRM-009, RRM-013):* the scope-claim key is symmetric, so any worker can mint a claim for any scope (production should use asymmetric signing or per-scope keys). A claim can be replayed within its lifetime (no `jti` cache). The RRM-013 crash-window drills were not re-run against the scope-claim server. The "one LangSmith project, re-check the parent filter" item is open (tracing stayed off). A hosted graph must be compiled through `build_hosted_async_subagent_graph` to keep provider usage attribution. A child's actual usage can exceed its contract limit (2,363 against 10 here); it is recorded, and whether an overage opens an incident is still undecided.
- *Capabilities (RRM-009):* the MCP pins cover the entry module and `package.json`, not the transitive `node_modules`. DNS rebinding between the browser's host check and its own lookup remains. Firecrawl MCP is pinned but was never invoked live. S3 is qualified against MinIO, not AWS. The post-merge `AsyncChildCompletion` admit path was not re-run live. Sync-child usage is under-counted after a crash reconstruction.
- *Composition (RRM-008, RRM-009):* session-generation admission after a sealed head is not composed. The superseded-generation windows under cancellation stay open (RRM-014). `GenericArtifactWorkflow` is outside the cancellation saga.
- *Families:* GoalDirected has no reservation release at a pause (it is vacuous at the iteration boundary). A StageGraph pause does not cancel running operations. The claim-revision staleness window inside the operation boundary remains (RRM-016). Drain GoalDirected runs started before RRM-016 before deploying.
- *Inspection and forks (RRM-005, RRM-006):* the unit list is not paginated. Snapshots list every unit (bounded at 1,024 items). Fork lineage is not shown in the unit read.
- *Environment:* `test_wp_bp_010_recovery.py` is WSL-only and unrun. `start_local` with a database file stands in for a persistent namespace; no production Temporal cluster was used.

## 6. Proven boundaries and what is deferred

**Proven on the production composition (this smoke and the accepted tickets):**
- **Fork boundaries.** (1) StageGraph `stage_settled`: the family head leaves no open producer liability and no stage holding admitted work, at least one stage result is accepted, and every quiescence condition holds. It is proven mid-run at a declared wait, with reuse by immutable ref outside the invalidation frontier. (2) GoalDirected `goal_verifier_settled`: the head is the iteration's verifier with a completed, fenced result, proven on a **terminal** run with no reuse and a fresh derived run. Every fork is independently admitted at epoch 1, the parent's authority is unchanged, and semantic-input patches are limited to the declared patchable paths (a StageGraph stage objective; the GoalDirected objective by family default).
- **Fork refusals.** An active async child (`async_child_active`), an open liability, active stages, an unsettled claim or reservation, in-flight cognition, and a run without an accepted stage (`unsupported_boundary`).
- **Interventions.** The StageGraph declared-wait release (`satisfy_wait`) and the GoalDirected pause (applied at the iteration boundary) and resume, both through the facade with `accepted → delivered → applied` receipts, plus relay redelivery (RRM-009) and Continue-As-New survival (RRM-007). Running cancellation reaches in-process sync children, real async children on the Agent Server during cognition, during the boundary wait and after a worker restart (RRM-009), with usage reconciliation and `applied` receipts.

**Deferred (not supported, never implied):**
- Arbitrary cognitive steering: mid-invocation edits to prompts, state or tool calls of a running Deep Agent.
- Forks from intermediate or arbitrary graph-node checkpoints. Explicit `cognitive_seed` is rejected `cognitive_seed_not_supported` (RRM-017).
- A fork from a GoalDirected durable pause, or from a recorded declared-wait or quiescence record (`quiescence_ref`). A mid-run GoalDirected fork boundary is not proven.
- Workflow Types declaring their own patchable fields (a registry policy stands in; RRM-017).
- Re-admitting a fenced unit at generation g+1 (RRM-014).
- Session-generation admission after a sealed GoalDirected head (fresh-from-handoff continuation; needs family semantics; RRM-014 or a follow-up).
- Restoring sandbox snapshots in a fork, Temporal Reset as a fork, and provider-effect rollback.

## 7. Fixture-session inputs (generic)

The fixture session creates the company definitions; none are created here.
- **Definitions:** one StageGraph Workflow Type (Qualia Life) and one GoalDirected Workflow Type (GenerationLab). Publish them through `ControlPlaneService` and compile an ERC through F1, as `publish_technical_catalog` does. A GoalDirected blueprint should use `workspace_mode="fresh"`, or `shared` now that RRM-020 is accepted. Operation templates carry compiled workspace slots and a `DeepAgentExecutionBinding` on the agent-cognitive queue. Register a `ForkPatchPolicy` per Workflow Type digest for the stage objectives that may be patched. Admission needs an `AdmissionPolicyRegistry` entry for the input contract and invariants. The baseline reservation must equal the launch input's `baseline_reservation`.
- **Capability pins:** `infra/capability-pins/research-capabilities.json` (`belllabs.capability-pins.v1`), verified at worker start and before every MCP launch: skill `skill.agent-browser`, tool `tool.agent-browser-page` (agent-browser 0.33.0), `mcp.tavily` (tavily-mcp 0.2.21), `mcp.firecrawl` (firecrawl-mcp 3.22.4), and the WP-CP-040 checkpointer, store, sandbox and model refs. Re-pin with `scripts/pin_research_capabilities.py` if a package changes; drift is refused. Grant browser egress per operation through `capability_grant.network_hosts` (deny by default).
- **Model:** `gpt-5.6-luna` through pinned model definition `model.wp-cp-040` (Responses API, reasoning low, 2,000 max completion tokens). Sync subagents are `SyncSubagentProfile`s on the parent binding. The async subagent is the hosted `belllabs_async_technical_child` contract (`technical_child_definition().contract(...)`) with `budget_limits` inside the run budget, `REQUIRED_BLOCKING` when the parent needs its result, and a registered result policy (`policy:async-result:technical-child@1` or a fixture-specific one). A company-specific hosted graph must be built through `build_hosted_async_subagent_graph` and served from a rebuilt Agent Server.
- **Credentials by reference only:** `OPENAI_API_KEY`, `TAVILY_API_KEY`, `FIRECRAWL_API_KEY`, `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` (the HMAC signing secret). Node is required at `WEB_RESEARCH_AGENT_BROWSER_NODE` for the stdio MCP servers and the browser.

## 8. Deployment and launch

Prerequisites (RRM-009 runbook, `README.md` "Production-shaped composition"):
1. **Application PostgreSQL** with migrations through **0026** applied by the owner (`APPLICATION_MIGRATION_DATABASE_DIRECT`), a runtime login (`APPLICATION_DATABASE_DIRECT`, member of `belllabs_control_runtime`) and a family-writer login (`APPLICATION_FAMILY_WRITER_DATABASE_DIRECT`).
2. **LangGraph saver and store:** `LANGGRAPH_CHECKPOINT_DATABASE_DIRECT`, `LANGGRAPH_CHECKPOINT_SCHEMA`, and `LANGGRAPH_CHECKPOINT_SETUP=1` once.
3. **Mongo:** `MONGODB_URI`, `MONGODB_DATABASE`. **Object storage:** `S3_BUCKET` with the AWS credential chain (`AWS_ENDPOINT_URL_S3` for an S3-compatible server) or `ARTIFACT_PAYLOAD_ROOT`.
4. **Persistent Temporal namespace:** `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TASK_QUEUE`. The qualifications used `start_local` with a database file.
5. **Agent Server:** the `rrm009-agent-server` stack at `http://127.0.0.1:8144`, rebuilt from `0c5ec35` with signed-claim auth (§2). To rebuild after a change under `app/`: from a checkout of the deployed revision, set `COMPOSE_PROJECT_NAME=rrm009-agent-server` with the stack's secret and Postgres password (the scratch launcher `rrm010_up.py up` / `rrm009_up.py`, which never prints secrets), then run `uv run langgraph up --config langgraph.async_subagents.json --postgres-uri <agent-server PG> --port 8144 --wait --api-version 0.12.0 --no-pull`. Tear down only by exact names.

Launch, each in its own process:

```powershell
uv run python scripts/register_belllabs_search_attributes.py        # administrative, idempotent
$env:COORDINATOR_LAUNCH_ENABLED="1"; uv run python -m app.temporal.worker
$env:RUN_CONTROL_TEMPORAL_ENABLED="1"; uv run uvicorn app.server:asgi_app --host 127.0.0.1 --port 8000
```

Then admit with `POST /run-control/v1/run-requests`, launch with `POST /run-control/v1/runs/{run_id}/launch`, and use `/commands`, `/snapshots`, `/forks` and `/inspection/runs…`.

**Flags:** `ASYNC_SUBAGENT_SPAWNING_ENABLED=true`, `AGENT_SERVER_ENDPOINT`, `ASYNC_SUBAGENT_SUBMITTER_IDENTITY`, `ASYNC_SUBAGENT_COMPLETION_WAIT_SECONDS` (default 120), `OPERATION_JOURNAL_CLAIMED_BY` (deployment-stable), `INSPECTION_CURSOR_KEY` (the same on every replica), `BOUNDARY_RELAY_REQUEST_SCOPES`, `BOUNDARY_RELAY_INTERVAL_SECONDS`, `CAPABILITY_PINS_PATH`, `DEEP_AGENT_SANDBOX_WORKSPACE_ROOT`, the heartbeat settings `OPERATION_HEARTBEAT_TIMEOUT_SECONDS` (30), `OPERATION_ASYNC_CHILDREN_HEARTBEAT_TIMEOUT_SECONDS` (15) and `OPERATION_BOUND_HEARTBEAT_TIMEOUT_SECONDS` (30), `WORKER_GRACEFUL_SHUTDOWN_SECONDS` (10; it must be shorter than every heartbeat timeout), and `LANGSMITH_TRACING` (off by default). Live opt-ins for tests: `BELLABS_RUN_RRM_009_LIVE`, `BELLABS_RUN_RRM_010_LIVE`, `BELLABS_RUN_RRM_013_LIVE`. The privileged child-usage reconciliation route needs the `reconciliation_operator` role.

## 9. Budget per fixture (estimate)

Observed technical spend: one hosted child is about 2.3k tokens. The RRM-009 live capability run, a small cited technical report with the skill, Tavily, the browser, one sync child and one async child, used about 89k tokens, roughly USD 0.25 at the assumed prices. A company fixture with real research is one to two orders of magnitude larger.

**Estimated cost is USD 1 to 10 per fixture** at the assumed prices. Recommended caps:
- `tokens.total` hard cap of about 2–3M per run;
- `model.turns` of about 300;
- GoalDirected `goal.iterations` of at most 4;
- per-child `budget_limits` inside the run budget;
- a session ceiling of USD 10 per fixture.

Stop and ask above that. Check the actual provider prices at session start.

## 10. Merging integration into `main` (not run here)

Verified read-only on 2026-10-03:
- `main` = `687c1b7` is an ancestor of `integration/research-runtime-mission` (`0c5ec35`), so a fast-forward is possible.
- The main checkout has uncommitted edits to `app/api/control_plane.py`, `app/application/control_plane/service.py`, `app/application/runtime/postgres_runtime_execution_repository.py`, `app/temporal/web_research_smoke.py` and `app/temporal/worker.py`. It also has untracked `app/{application,domain,integrations}/agentic_components/`, `tests/unit/agentic_components/`, `scripts/query_agentic_components.py` and five `docs/*` notes.
- `git diff --name-only main integration/research-runtime-mission` overlaps four of the modified files: `app/api/control_plane.py`, `app/application/control_plane/service.py`, `app/temporal/web_research_smoke.py` and `app/temporal/worker.py`. It adds no file at any untracked path.

The user runs these after the coordinator's RRM-010 merge, with the edits set aside first. Do not commit them on `main`, or the fast-forward is lost.

```powershell
cd C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system
git status --short
# Set the edits of the four overlapping files aside on a branch (or stash them with a unique message):
git switch -c user/pre-rrm-merge-edits
git add app/api/control_plane.py app/application/control_plane/service.py app/temporal/web_research_smoke.py app/temporal/worker.py app/application/runtime/postgres_runtime_execution_repository.py
git commit -m "WIP: local control-plane and worker edits before the RRM merge"
git switch main
git merge --ff-only integration/research-runtime-mission
git log --oneline -1                      # the integration head
# Bring the edits back on top and resolve against the merged code:
git switch user/pre-rrm-merge-edits
git rebase main                           # or: git switch main; git merge user/pre-rrm-merge-edits
```

Do not push. Untracked work is unaffected by the fast-forward.

## 11. Stop

RRM-011 stays **held**. CP-050 stays **unaccepted** until its complete tracer evidence exists. No company definition, fixture, report, fork, automation or agent message was created. The fixture session starts only when the user starts it, with [`docs/RESEARCH_RUNTIME_FIXTURE_SESSION_PROMPT.md`](../../../../RESEARCH_RUNTIME_FIXTURE_SESSION_PROMPT.md).
