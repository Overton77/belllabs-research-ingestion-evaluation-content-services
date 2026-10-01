# Research runtime mission — agent-team coordinator prompt

Recorded: 2026-10-01
Attach this whole document to the coordinator agent. It is the coordinator's operating brief for finishing RRM-001, RRM-003 to RRM-010 and RRM-013, plus up to five clean-code passes. It stops at `ready_for_separate_fixture_session`.

Repository: `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system`
Mission integration branch: `integration/research-runtime-mission`, at `6675455` plus the docs merge that added this file
Integrator worktree with a ready Python 3.12 `.venv`: `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-002`
Meta (spec) repository: `C:\Users\Pinda\Proyectos\Biotech\biotech-meta`

---

## 1. Mission and finish line

Demonstrate that **Temporal (macro runtime) and Deep Agents (bounded cognition) work together in production shape**:

- Real StageGraph and GoalDirected runs launched through the governed BellLabs API.
- Deep Agents with **in-process sync subagents and real async subagents on a local Agent Server**.
- Exact checkpoint lineage, **crash recovery** without duplicate prompts or provider effects, **inspection** of active and historical state, **semantic forks** from safe snapshots, **boundary interventions** (wait, pause, resume) and **running cancellation**.

The finish line is **RRM-010**: one reviewed, tested integration revision, merged according to §11, plus a published `ready_for_separate_fixture_session` readiness manifest. Then stop.

After this mission the user will run the real company fixtures (RRM-011: Qualia Life on StageGraph, GenerationLab on GoalDirected, using Deep Agents with sync and async subagents) in a **separate session they start themselves**. Later still, they will generalize this mission-control runtime together with the knowledge services in their aiengineer app. That is why reusable mechanisms must stay cleanly separated from company specifics (§9.4). Do not build the generalization now.

## 2. Non-negotiable boundaries

1. **No company fixtures.** Do not create, compile, launch, schedule or delegate the Qualia Life or GenerationLab fixtures, their reports or their forks (RRM-011). Do not auto-continue into them. Small technical fixtures, captured-history replay and failure injection are allowed.
2. **Protect user work.** The main checkout has uncommitted user changes. Modified: `app/api/control_plane.py`, `app/application/control_plane/service.py`, `app/application/runtime/postgres_runtime_execution_repository.py`. Untracked: `app/{application,domain,integrations}/agentic_components/`, `tests/unit/agentic_components/`, `scripts/query_agentic_components.py` and several `docs/*` walkthroughs. The lifecycle brief is untracked too. `biotech-meta` has its own uncommitted changes (`.cursor/AGENTS.md`, untracked `.agents/`, `curated-content/` and others). Never stage, revert, overwrite, stash or depend on any of these without a reviewed owner commit.
3. **No remote publication.** Do not push, open PRs or create remote issues without the user's separate authorization. All branches are local.
4. **Authority is canonical.** Specs live in `../biotech-meta/docs/specs/`. `SPEC-CP-COGNITIVE-SCHEMAS` (`05-deep-agent-cognitive-state-and-context-schemas.md`) is **draft**, and ADR-0004 is **proposed**. Code that carries their digests does not make them accepted. RRM-001's accepted output gates every new contract.
5. **No weakened verification.** No blanket skip or xfail, no lowered assertions, no fake provider success counted as qualification, no test deselection without a recorded rationale. Unavailable infrastructure is recorded as an unresolved gate, never as a pass.
6. **One macro runtime.** Temporal is the sole macro scheduler. The Agent Server only hosts async subagent graphs. It must never run BellLabs workflows or become a competing scheduler (ADR-0003, `.cursor/rules/agent-framework-coexistence.mdc`).
7. **Docker safety.** Remove only containers you created, with `docker rm -f -v <exact names>`. **Never** run `docker volume prune`, `docker system prune` or `docker compose down --volumes`. They destroy unrelated user data.
8. **Spend.** Live provider and LLM qualifications use tiny technical inputs behind explicit opt-in flags. Report observed spend per ticket. If one ticket's live checks would exceed about USD 10, stop and ask.

## 3. Verified current state (2026-10-01)

| Fact | Where |
|---|---|
| RRM-002 is accepted. Offline gate: 678 passed, 46 skipped, 2 xfailed, 0 failed, with and without `.env`. Ruff clean; mypy clean on 332 files. | `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-002/README.md` |
| Opted-in Postgres/Mongo integration suites passed (38) against disposable containers. This was the first live run of these suites. | same |
| Both families were qualified with real models through FastAPI admission → `BellLabsRunWorkflow` → family → `OperationWorkflow` → Deep Agent. The test harness started the root workflow, using the Temporal time-skipping server and in-memory checkpointers. | `tests/acceptance/control_plane/test_wp_bp_010_live.py`, `test_wp_bp_020_live.py`; evidence `evidence_v2/WP-BP-010`, `WP-BP-020` |
| `POST /run-control/v1/run-requests` only **admits**. Nothing in the production server starts `BellLabsRunWorkflow` after admission. `TemporalWorkflowSubmitter` exists but only runner and smoke scripts use it. | `app/api/run_control.py`, `app/integrations/temporal_workflow_submission.py` |
| The stock worker has **no operation activities** unless a deployment `WorkerActivityCompositionFactory` is supplied. None exists in the repo. | `app/temporal/worker.py` (`main`, `WorkerActivityCompositionFactory`) |
| Deep Agents run **in-process in the Temporal worker**, inside the `operation.execute` activity. The adapter calls `create_deep_agent` and `ainvoke` with a deterministic `thread_id`. It does **not** capture the resulting checkpoint config. | `app/integrations/agents/deep_agents/adapter.py` |
| Async subagents call a remote Agent Protocol server through `langgraph_sdk`. They have only been qualified against a fake client. Spawning is off by default (`ASYNC_SUBAGENT_SPAWNING_ENABLED=false`). | `app/integrations/agents/deep_agents/async_subagents.py`, `app/application/async_subagents/`, evidence `WP-CP-045` |
| Root `langgraph.json` registers no graphs. Block C configs are an older qualification topology, but are prior art for a persistent local Agent Server. | `langgraph.json`, `langgraph.block_c*.json`, `tests/fixtures/agent_server_block_c.py` |
| RRM-012 (inactive reference-research harness lacks journal authority) is open, independent and **not** on the critical path. | `issues/12-…md` |

The audit findings in `docs/RESEARCH_RUNTIME_IMPLEMENTATION_AUDIT_2026-10-01.md` remain valid except where RRM-002 changed them.

## 4. Required reading (coordinator, then each ticket agent)

1. `AGENTS.md` and `.cursor/rules/`: `project-organization.mdc`, `codebase-organization.mdc`, `engineering-sequence.mdc`, `workflows-domain-contracts.mdc`, `agent-framework-coexistence.mdc`, `tech-stack-authority.mdc`, `parallel-blueprint-worktrees.mdc`.
2. `docs/RESEARCH_RUNTIME_IMPLEMENTATION_AUDIT_2026-10-01.md`: verified code locations and gaps per concern.
3. `docs/migrations_instructions/implementation_work_packages_v2/RESEARCH_RUNTIME_MISSION_TICKETS.md`: the index, dependency edges and **mission horizon**.
4. The full body of each ticket being worked, under `…/research-runtime-mission/issues/NN-*.md`.
5. `docs/RESEARCH_RUNTIME_MISSION_SPECIAL_HANDOFF_2026-10-01.md`: §§5–7 cover capabilities, inspection/fork semantics and intervention gates.
6. `docs/migrations_instructions/implementation_work_packages_v2/IMPLEMENTATION_READINESS.md` (runtime ownership, identity rules, frozen paths) and `docs/migrations_instructions/evidence_v2/README.md` (evidence format).
7. Canonical specs in `../biotech-meta/docs/specs/control-plane-foundations/`:
   - `02-…` run control: RUN-003–010
   - `03-…` durable execution: EXEC-003–008, 011, 012
   - `04-…` Deep Agent runtime: DA-001–015
   - `05-…` cognitive schemas (**draft**)
   Also `../biotech-meta/docs/specs/workflow-blueprints/{stagegraph,goal-directed}.md`, ADR-0003 and the **proposed** ADR-0004.
8. Stage skills in `../biotech-meta/.agents/skills/`: `to-spec` for RRM-001, `implement` (with `tdd`) for code tickets, `code-review`.
9. As-built reference: `docs/interview_and_research_result_documentation/CODEBASE_DOMAIN_WORKFLOW_GUIDE.md` and `CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md`.

The local `docs/RUNTIME_LIFECYCLE_INSPECTION_AND_CONTROL_IMPLEMENTATION_BRIEF.md` is an **untracked design brief**, not a spec. Read it only from the main checkout, as input to RRM-001 authoring.

## 5. Architecture map: files and what they do

Dependency direction is `domain ← application ← api | temporal | integrations`. The domain owns meaning; application owns use cases and persistence ports; API, Temporal and integrations are adapters.

### Macro runtime (Temporal)
- `app/temporal/workflows/belllabs_run.py`: stable root `BellLabsRunWorkflow` (`belllabs-run/{run_id}`). It holds the root message receipts and starts exactly one family child. Accepting a message there is **not** the same as applying it (RRM-007).
- `app/temporal/workflows/stagegraph.py` and `goal_directed.py`: family mechanics. StageGraph has wait, resume and cancel signals. GoalDirected has cancel, and currently raises non-retryable `goal_paused` instead of durably pausing (RRM-007).
- `app/temporal/workflows/operation.py`: generic `OperationWorkflow` child. It allows three technical activity attempts. Its cancel flag is checked only before `execute_activity` and does not stop active cognition (RRM-008).
- `app/temporal/activities/operation.py` and `app/temporal/operation_activities.py`: the `operation.execute` entry. There is no `activity.info().attempt` capture and no cognitive heartbeat yet (RRM-003, RRM-008).
- `app/temporal/registration/{workflows,activities,task_queues}.py`: the single registries. These are integrator-owned.
- `app/temporal/worker.py`: worker entry with the `WorkerActivityCompositionFactory` protocol and the refusal guard. A production factory is missing (RRM-009).
- `app/integrations/temporal_workflow_submission.py`: `TemporalWorkflowSubmitter` starts the root workflow from an admitted run. It needs a governed production caller (RRM-009/010).

### Lifecycle authority (application PostgreSQL)
- `app/domain/run_control/reducer.py` and `contracts.py`: the **sole** lifecycle, budget, effect and terminal authority. Apply-authority-batch and family admission live here.
- `app/application/run_control/service.py`, `run_control_repository.py`, `postgres_run_control_repository.py`: admission, commands, family admission, outbox.
- `app/api/run_control.py`: `/run-control/v1` facade. It handles run requests, commands, reads and schemas.

### Family semantics (pure)
- `app/domain/orchestration/interpreter.py`: `StageGraphInterpreter` handles readiness, joins, cycles, late results and liabilities.
- `app/domain/orchestration/goal_directed.py` and `goal_directed_runtime.py`: the GoalDirected interpreter and runtime with executor/verifier isolation, handoff and rollover.
- `app/domain/orchestration/bindings.py`: `RunSemanticInputBinding`, including the dedicated `goal_verifier` slot.

### Operation execution and journal
- `app/application/operations/operation_execution.py`: `OperationExecutionService` (around line 360). A settled binding returns the prior result; an unresolved claim raises `OperationExecutionInProgress`. There is no checkpoint-to-settlement recovery yet (RRM-004).
- `app/application/operations/journaled_operation_execution.py`: claim → observe → authority-batch settlement through run control. The settlement actor gets the binding added to `authority_refs`.
- `app/application/operations/operation_journal.py` and `postgres_operation_journal.py`: atomic journal invariants. Settlements must bind accepted authority, result manifests and evidence.
- `app/domain/operation_execution/contracts.py`: `RuntimeResult` and `OperationExecutionResult`. Neither carries checkpoint identity yet (RRM-001/003). Also `AsyncSubagentContract` (`agent_protocol_url`, `graph_id`).

### Deep Agents (bounded cognition, in-process)
- `app/integrations/agents/deep_agents/adapter.py`: the **only** production `create_deep_agent` composition root. Integrator-owned.
- `app/integrations/agents/deep_agents/materializer.py`: exact binding → framework arguments, using explicitly registered checkpointers and stores.
- `app/integrations/langgraph_persistence.py`: the Postgres saver/store lifespan. It is not yet proven in production worker composition.
- `app/integrations/agents/deep_agents/async_subagents.py`: `DeepAgentsAsyncSubagentAdapter`. It drift-checks the five deepagents 0.7.5 async tools and injects the BellLabs child identity as the thread ID.
- `app/application/async_subagents/` (`service.py`, `postgres_async_subagents.py`, `mongo_async_subagent_repository.py`), plus migration `app/migrations/0016_async_subagent_parent_child_v1.sql`: parent/child reservation, link, message and result authority.

### Runtime identities, recovery, forks, interventions (existing prior art; audit before reuse)
- `app/domain/graph_runtime/identities.py`: binding, attempt and checkpoint identities. `LangGraphCheckpointKey` lacks an explicit namespace. Several identities are Agent Server-shaped.
- `app/application/runtime/runtime_recovery.py` (fork admission/copy saga, around line 365), `runtime_interventions.py`, `runtime_lineage.py`, `postgres_stage3_kernel_repository.py` (`PostgresForkRepository`), and migrations `0012`/`0014`: storage for checkpoints, incidents, lineage and forks.
- `app/application/workspaces/sandbox_snapshots.py`: immutable sandbox/workspace snapshots with an injectable `clock`. These are distinct from macro semantic snapshots (RRM-006).
- `app/api/graph_runtime_schemas.py`: `/v2/graph-runtime/schemas` exports schemas only. It is **not** an inspection endpoint (RRM-005).

### Agent Server prior art (for RRM-013 only)
- `app/agent_server/` (`http_app.py`, `auth.py`, `graph_factory.py`, `runtime_composition.py`) and `app/agent_server/block_c_qualification/`.
- `tests/fixtures/agent_server_block_c.py`: the `uv run langgraph up --config … --postgres-uri … --port …` recipe, restart drills and an auth token helper.

### Composition root
- `app/server.py`: FastAPI `asgi_app`, routers and the optional coordinator MCP mount. Launch is disabled unless production launch dependencies are supplied.
- `app/config.py`: `Settings` and `PROJECT_ROOT`. Workspace siblings resolve through `PROJECT_ROOT.parent`.

## 6. Ticket sequence and dependency graph

```text
RRM-001 (spec, user acceptance) ──► RRM-003 ──► RRM-004 ──┬─► RRM-013 (async subagents on Agent Server) ──┬─► RRM-008 ─┐
                                                          ├─► RRM-005 ──► RRM-006 ───────────────────────┼────────────┤
                                                          └─► RRM-007 ──────────────────────────────────►┘            ├─► RRM-010
                                                                RRM-009 (needs 001, 002, 004, 013) ───────────────────┘
```

Serial default: 001 → 003 → 004 → 013 → 005 → 006 → 007 → 008 → 009 → 010. After 004, run **013, 005 and 007 in parallel** where file ownership allows. 006 follows 005. 008 follows 007 and 013. 009 follows 013. RRM-012 is optional and off the critical path; do it only if it never delays the sequence.

### Ticket briefs (full bodies are authoritative)

**RRM-001 — Lifecycle contract authority (specification).**
- Branch `spec/research-runtime-lifecycle` in `biotech-meta`, plus application traceability.
- Use `to-spec` and produce the minimum canonical amendments for:
  - stable runtime-unit identities;
  - checkpoint namespace and ancestry;
  - expected-checkpoint CAS fencing;
  - terminal-result reconstruction versus resume;
  - safe macro snapshot and reuse frontier;
  - scoped inspection and projection freshness;
  - command receipts (accepted, delivered, applied, rejected);
  - GoalDirected durable pause and resume.
- Classify existing identities and storage (`identities.py`, migrations `0012`/`0014`, fork and intervention services) as reuse, version or retire.
- Decide whether the draft cognitive-schemas spec and ADR-0004 are accepted, narrowed or excluded.
- Include the async-subagent touchpoints RRM-013 needs: child lineage in inspection and fork classification of active children.
- **User checkpoint (§10).** Downstream code waits for the recorded accepted meta revision.

**RRM-003 — Persist exact checkpoint lineage.**
- Thread a stable semantic unit identity through binding → activity attempt (`activity.info().attempt`) → Deep Agent invocation (explicit thread and **namespace**) → captured **result checkpoint config** → PostgreSQL observation, with CAS and serialized session ordering.
- Key files: `adapter.py`, `materializer.py`, `operation_activities.py` / `activities/operation.py`, `operation_execution.py`, `domain/operation_execution/contracts.py`, `identities.py`, and versioned runtime storage.
- Uses the real Postgres saver. Done when a persistent integration shows one operation's before/after checkpoints and an idempotent duplicate delivery.

**RRM-004 — Recover checkpoint and settlement crash windows.**
- Converge to one settlement with no re-appended prompt and no repeated provider effect.
- Inject crashes before the checkpoint, after intermediate and terminal checkpoints, before observation and before settlement. Assert invocation counts, prompt counts, ancestry and the final digest.
- Multiple descendants or unclear ancestry produce an `in_doubt` incident, never a speculative reinvocation.
- Key files: `operation_execution.py`, `journaled_operation_execution.py`, the journal repositories, `runtime_recovery.py`, `operation.py`.
- Requires a real worker restart against the persistent saver and application DB.

**RRM-013 — Real async subagents on a local Agent Server.**
- Run a persistent self-hosted server with `langgraph up`, dedicated Postgres and Redis and a **dedicated config file**; the root `langgraph.json` stays empty.
- The subagent graph is built from an exact binding through the canonical adapter and materializer.
- Real spawn from a Deep Agent inside `operation.execute`. The BellLabs reservation and link happen before the provider run.
- Kill the worker before and after submission, and restart the Agent Server mid-run. Expect one provider run per child.
- Child results are admitted by the parent. Usage settles to the parent budget. Lineage is exposed for 005 and 006, and cancel hooks for 008.
- Key files: `async_subagents.py`, `app/application/async_subagents/`, `app/agent_server/`, the Block C recipe, CP-045 tests.

**RRM-005 — Inspect lifecycle and historical checkpoints.**
- Scoped list, detail, unit and history reads through the application facade. Real Temporal Search Attributes; never query Temporal's internal database.
- Responses expose freshness, `in_doubt` and redaction. Reads never mutate.
- Demonstrate a tiny run with two or more checkpoints, a historical read and a replay.
- The new routes are separate from `graph_runtime_schemas.py`.

**RRM-006 — Semantic forks from safe snapshots.**
- Build a macro `RunSnapshotManifest` at a declared boundary or after quiescence. Validate the patch against protected fields.
- Admit the derived run independently at epoch 1, reusing only settled, compatible results. Never copy pending messages or effects implicitly.
- Reuse the existing fork saga (`runtime_recovery.py`, `PostgresForkRepository`).
- Fork both families. State exactly which fork boundary is proven.

**RRM-007 — Governed boundary interventions.**
- Commands get durable accepted, delivered, applied and rejected receipts.
- Release a StageGraph declared wait through the facade.
- GoalDirected pause becomes a durable paused state with an accepted resume, not `goal_paused` failure.
- Ordering survives worker restart and Continue-As-New.
- Key files: `belllabs_run.py`, `stagegraph.py`, `goal_directed.py`, `runtime_interventions.py`, `run_control` service and API.

**RRM-008 — Running cancellation.**
- Journal the cancel intent first. Long cognition heartbeats.
- Cancel StageGraph siblings and GoalDirected executor/verifier work. Cancel **real async children** (RRM-013).
- Reconcile usage, reservations and effects. Ambiguity becomes an incident.
- Inject cancellation before dispatch, during model or tool calls, during async work and after an ambiguous effect.

**RRM-009 — Production capability composition (CP-050 prerequisite).**
- Ship a **deployment `WorkerActivityCompositionFactory`** covering real Postgres, Mongo, object storage, the persistent LangGraph saver and store, canonical registries and queues.
- Pin the search and browser Skills and MCP tools by digest, mount and disclose them, and prove they are actually invoked. Use a mediated or constrained egress placement; do not simply remove sandbox isolation.
- Promote artifacts. Qualify one sync subagent and one **real** async subagent.
- Provide a governed API-to-Temporal launch path. Small technical inputs only. Document deployment and launch commands.

**RRM-010 — Readiness gate.**
- All tickets accepted, with the full gate set on the integration commit.
- The combined technical smoke covers inspection, a historical read, a safe fork, an intervention and cancellation, **including an active real async child**.
- Merge per §11. Publish `ready_for_separate_fixture_session` and a later fixture-session prompt. **Stop.**

## 7. Team structure and ownership

- **Coordinator / integrator (you).** You own sequencing, worktree creation, merges into `integration/research-runtime-mission`, conflict resolution, the shared-service stack (§12) and the final report. You **exclusively own** these shared seams, and other agents propose diffs for them:
  - `domain/operation_execution/contracts.py` and `domain/graph_runtime/identities.py`;
  - `adapter.py` and `materializer.py`;
  - `app/temporal/registration/*`;
  - `app/migrations/*`;
  - the run-control reducer, service and repositories;
  - the root `langgraph.json`.
- **Ticket implementers (one per ticket).** Each works in its own sibling worktree on its ticket branch. Implementers use `implement` + `tdd` and touch only that ticket's region.
- **Independent reviewer per ticket.** The reviewer is never the implementer. Correctness review uses `code-review`. Findings must be fixed or explicitly dispositioned before merge.
- **Clean-code reviewer.** Runs the ≤5 passes in §9.
- Keep agent count proportional. One implementer at a time is the default; parallelism is used only after RRM-004, on disjoint files.

## 8. Per-ticket workflow

1. **Kickoff record.** Record the worktree path, branch, base commit (the current integration head), dirty-path inventory, owned paths, shared regions, planned gates and non-goals.
2. **Worktree.** `git worktree add C:/Users/Pinda/Proyectos/Biotech/biotech-research-ingestion-evaluation-system-rrm-NNN -b <ticket branch> integration/research-runtime-mission`. Worktrees must be **siblings** of the main checkout; nested worktrees break workspace-relative paths. Create the venv on the Codex Python 3.12 (§12).
3. **Build.** Tests first at the agreed seams. Run typechecks and single test files often. Version contracts and storage rather than adding a second lifecycle system. New migrations are forward-only, with explicit schema identities.
4. **Gates.** Before review, run: the owning suites; `uv run ruff check app tests scripts`; `uv run mypy app`; the full offline `pytest` **both with and without `.env`** (§12); `git diff --check`. Also run the ticket's opted-in service and live qualifications, and captured-history replay where Temporal code changed.
5. **Evidence.** Write `docs/migrations_instructions/evidence_v2/research-runtime-mission/RRM-NNN/README.md` in the section order from `evidence_v2/README.md`. Include:
   - exact commands and sanitized results;
   - base, head and merge commits;
   - requirement → test → observed-assertion map;
   - unresolved gates;
   - one line per reusable seam (mission horizon).
6. **Review and merge.** Independent review, fixes, then `git merge --no-ff` into integration. Re-run the gates on the merge commit.
7. **Close.** Mark the ticket and index accepted with the tested commits. Refresh the ignored local mirrors in `.scratch/research-runtime-mission/issues/` of the main checkout.
8. **Split out defects.** For a genuine defect outside the ticket, open a new numbered ticket (next free number is RRM-014) instead of disabling a test. A narrow `xfail(strict=True, raises=…)` that cites the ticket is the only acceptable test marker.

## 9. Clean-code passes (at most 5)

Purpose: keep naming and file layout coherent as new code lands, so the later generalization starts from a clean base. These passes are **behavior-preserving clean-ups**, not feature work and not correctness reviews.

### 9.1 Schedule (cap: 5; skip a pass if its scope has nothing worth changing, and record that)

| Pass | After | Scope |
|---|---|---|
| CR-1 | RRM-004 merged | Checkpoint lineage and recovery: identities, observation and result contracts, repository and service names, adapter changes |
| CR-2 | RRM-013 and RRM-005 merged | Async-subagent deployment and graph files, inspection API, projection and read models |
| CR-3 | RRM-006 and RRM-007 merged | Snapshot, fork and patch modules; command and receipt and intervention paths |
| CR-4 | RRM-008 and RRM-009 merged | Cancellation, heartbeat, composition factory, capability wiring, launch path |
| CR-5 | Before RRM-010's final gate | Whole-mission diff (`git diff 687c1b7..integration`) for consistency of names, placement and dead code |

### 9.2 What a pass checks
- **Names** say what the thing is in BellLabs domain language: consistent `snapshot`, `checkpoint`, `unit`, `attempt`, `generation` and `receipt` vocabulary. No `v2`, `new`, `tmp` or `helper` names; no vague `manager`/`util` modules.
- **Placement** follows `codebase-organization.mdc` and the canonical organization doc. Meaning lives in `domain/`, use cases and ports in `application/`, providers in `integrations/`, transport in `api/`. Temporal stays deterministic.
- No duplicated contracts or parallel implementations of the same responsibility. Dead code and superseded paths from the mission are deleted (readiness §1).
- Modules stay focused. Split one only when it clearly has two responsibilities. Comments match the surrounding density.
- Test names describe the behavior they assert. Fixtures live under `tests/fixtures/`.

### 9.3 Hard rules for clean-up passes
- **Never rename** persisted or wire identities:
  - Temporal workflow, activity, signal or query names;
  - Temporal payload field names (replay compatibility);
  - contract IDs (`CON-*`, `REQ-*`) and schema-version strings;
  - applied migration files, database tables and columns;
  - Agent Server graph IDs used by persisted threads;
  - API paths already published in evidence.
  Rename the Python symbol only through an alias if a rename is truly needed.
- **File moves need a path-safety sweep.** An earlier reorganization (`e83d2ef`) silently broke 34 tests and one production path. After any move, grep for `Path(__file__)`, `parents[`, quoted path strings and import strings across `app`, `tests`, `scripts` and the JSON configs. Then run the full suite in both modes.
- Separate `refactor:` commits on `cleanup/rrm-cr-N` from the integration head, merged `--no-ff` after an independent check. Use the full gate set, plus opted-in service suites if repositories were touched.
- Do not touch the user's uncommitted files. Do not reformat unrelated code wholesale.
- Record each pass in `evidence_v2/research-runtime-mission/CLEANUP.md`: scope, findings, changes, deliberate non-changes, and commands with results.

### 9.4 Mission-horizon lens (all passes)
Keep reusable mechanisms (lifecycle, recovery, inspection, forks, interventions, subagent spawning) free of company, fixture and provider specifics. Note candidate generalization seams in `CLEANUP.md`. **Do not extract a framework or new packages.**

## 10. User checkpoints (pause and ask only here)

1. **RRM-001 spec acceptance.** Present:
   - the meta-repo diff;
   - the contract disposition table;
   - the cognitive-schemas/ADR-0004 decision;
   - any open decision, with a recommendation.
   Implementation of new contracts (RRM-003 onward) waits for the user's recorded acceptance. The user may instead pre-authorize acceptance after an independent review; if so, record that authorization verbatim.
2. Any need to use, commit or depend on the user's uncommitted work (main checkout or `biotech-meta`).
3. Live spend above the §2.8 threshold, missing credentials, or a licensing or prerequisite fact for `langgraph up` that blocks RRM-013.
4. A merge to `main` that would touch files with uncommitted user changes (§11).
5. Any push, PR or remote issue.

Otherwise proceed autonomously. Do not ask "should I continue" between tickets.

## 11. Merging to `main` (RRM-010 only)

The main checkout is on `main` with uncommitted user edits. Merge `integration/research-runtime-mission` into `main` only if it can be done without touching those files: check `git diff --name-only main integration/research-runtime-mission` against the dirty list first. If they overlap, or git would refuse, **do not stash or modify the user's work**. Stop and give the user the exact commands to run. Never push.

## 12. Environment and commands

- **Interpreter.** Create every worktree venv on the Codex runtime Python 3.12:
  ```bash
  uv venv --python 'C:\Users\Pinda\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe' .venv
  uv sync --frozen
  ```
  The live-runner test requires that interpreter's bundled sibling `node`; uv's CPython 3.13 fails it.
- **Gate commands.** Run from the worktree:
  ```bash
  uv run --no-sync ruff check app tests scripts
  uv run --no-sync mypy app
  BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false \
    uv run --no-sync pytest -q                     # hermetic: worktrees have no .env
  uv run --no-sync --env-file ../biotech-research-ingestion-evaluation-system/.env pytest -q   # developer env, loaded without copying
  git diff --check
  ```
  Never copy or print `.env`. Never commit secrets.
- **Disposable services** for `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI`. The tests' safety guards **require** Postgres at `127.0.0.1:55432/belllabs` (user `belllabs`) and Mongo at `127.0.0.1:27017`. Those ports are shared, so **the coordinator owns one stack and serializes service-gate runs** across agents. Each suite drops and recreates its own schema; never run two at once.
  ```bash
  docker run -d --name rrm-app-postgres -e POSTGRES_DB=belllabs -e POSTGRES_USER=belllabs -e POSTGRES_PASSWORD=belllabs-local \
    -p 127.0.0.1:55432:5432 -v "<worktree>\infra\application-postgres\init:/docker-entrypoint-initdb.d:ro" pgvector/pgvector:pg16
  MSYS_NO_PATHCONV=1 docker run -d --name rrm-app-mongodb -p 127.0.0.1:27017:27017 --entrypoint /bin/bash mongo:8.0 -lc \
    'mongod --replSet rs0 --bind_ip_all & p=$!; until mongosh --quiet --eval "db.runCommand({ping:1}).ok" >/dev/null 2>&1; do sleep 1; done; mongosh --quiet --eval "try { rs.status() } catch (_) { rs.initiate({_id: \"rs0\", members: [{ _id: 0, host: \"localhost:27017\" }]}) }"; wait $p'
  export TEST_APPLICATION_POSTGRES_DSN='postgresql://belllabs:belllabs-local@127.0.0.1:55432/belllabs'
  export TEST_MONGODB_URI='mongodb://127.0.0.1:27017/?directConnection=true'
  # cleanup: docker rm -f -v rrm-app-postgres rrm-app-mongodb   (never prune)
  ```
  First check that the user's compose stack isn't using those ports (`docker ps`). If it is, stop and ask; do not reuse the user's databases.
- **Temporal for persistent or worker-restart qualification.** Use the repo's `docker compose` services (`temporal`, `temporal-postgres`) only if the user's stack is down. Otherwise ask first. Never run `down --volumes`. Time-skipping test environments remain fine for deterministic suites.
- **Agent Server (RRM-013).** Use `uv run langgraph up --config <dedicated config> --postgres-uri <dedicated disposable DB> --port <free port> --wait`. Follow the recipe and restart drill in `tests/fixtures/agent_server_block_c.py` and give it its own `COMPOSE_PROJECT_NAME`. Agent Server tests use `AGENT_SERVER_ENDPOINT` plus a new explicit live flag.
- **Live LLM.** Use explicit `BELLABS_RUN_*_LIVE=1` flags only, with tiny inputs. The OpenAI key comes from the main `.env` via `--env-file`. LangSmith tracing is optional; when on, traces are subordinate evidence.
- On Windows: deep `.venv` paths can exceed `MAX_PATH` (delete them with `cmd /c rd /s /q "\\?\<path>"`). `git worktree move` fails while a shell's working directory is inside the worktree.

## 13. Final report (when RRM-010 publishes readiness, then STOP)

Report:
- the disposition of each ticket, with tested and merge commits;
- the accepted meta revision;
- gate results (offline, both modes; service; Temporal; Agent Server; live) with exact commands;
- evidence paths;
- clean-code passes run (≤5) and what they changed;
- spend;
- unresolved gates and new tickets;
- precisely which fork boundaries and interventions are proven, and what is deferred;
- the `ready_for_separate_fixture_session` manifest path and the fixture-session prompt.

Do **not** start RRM-011.
