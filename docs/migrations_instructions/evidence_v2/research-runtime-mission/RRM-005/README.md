# RRM-005 implementation evidence

Disposition: accepted 2026-10-02 (independent review `approve`; follow-ups in `62409d8`; merged into integration at `ed598dd`; integration fix `9a7e754`)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-005 inspect lifecycle and historical checkpoints. Requirements: REQ-CP-RUN-011 (scoped, non-mutating list/detail/unit/history reads; saver-based historical reads), REQ-CP-RUN-012 (freshness, `in_doubt`, redaction), REQ-CP-EXEC-007 (clarified: Queries are diagnostic only), REQ-CP-EXEC-015 (Search Attributes only). Contracts: `CON-CP-INSPECTION-READ-V1` (`belllabs.inspection-read.v1`), `CON-CP-TEMPORAL-IDENTITY-V1` (Search Attribute table), `CON-CP-CHECKPOINT-LINEAGE-V1` (namespace, stamps, recorded lineage) (AMD-RRM-001, accepted meta `main` `a50d833`).
Base revision and head revision: base `bb964c5` (integration `integration/research-runtime-mission`: RRM-001, 003, 004 and CR-1 merged). Tested code head `ad961ee` on `wp/rrm-005-inspection`; the evidence/ticket commit follows it and changes documentation only. Not merged (the coordinator owns review and merge).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12 (Codex runtime), pytest 8.4.2, ruff 0.15.22, mypy 1.20.2, pydantic 2.13.4, langgraph 1.2.10, langgraph-checkpoint 4.1.1, langgraph-checkpoint-postgres 3.1.1, deepagents 0.7.5, temporalio 1.30.0 (its `start_local` dev server: Temporal CLI 1.9.1, Server 1.32.0), asyncpg 0.31.0, psycopg 3.3.4.

## Worktree provenance

- Worktree: `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-005`, branch `wp/rrm-005-inspection`, created by the coordinator from integration `bb964c5`. It was clean at kickoff.
- The main checkout and the main `biotech-meta` checkout were not touched. No `.env` was copied or printed; the services gate loads it with `--env-file ../biotech-research-ingestion-evaluation-system/.env`.
- Disposable services (coordinator-owned, shared): `rrm-app-postgres` (`127.0.0.1:55432/belllabs`) and `rrm-app-mongodb` (`127.0.0.1:27017`). Every run that set the service DSNs, and every full pytest run, held the shared stack lock (`stack_lock.py acquire/release RRM-005`). No container was created, stopped or removed. The demonstration creates and drops its own saver schema `rrm005_inspection_saver` and its own Mongo database `rrm005_inspection_<random>`.
- Temporal ran only as `WorkflowEnvironment.start_local(search_attributes=…)` (ephemeral in-memory dev server, one at a time) and the time-skipping test server. The user's docker compose Temporal stack was not used, and no Temporal persistence database was read.
- RRM-013 runs concurrently. `app/integrations/agents/deep_agents/async_subagents.py`, `app/application/async_subagents/` and `app/agent_server/` were not edited; async-child lineage is read through this ticket's own read port.

## Implemented contracts and seams

**Envelope (`CON-CP-INSPECTION-READ-V1`).** `app/domain/run_control/inspection.py`: `InspectionRead[T]` (`schema_version = belllabs.inspection-read.v1`, `data`, `sections`). Each `InspectionSection` records `source` (`postgres_authority`, `temporal_visibility`, `checkpointer`, `mongo_detail`, `diagnostic_query`), `observed_at`, `projection_version`, `freshness` (`current`, `stale`, `unavailable`), `reconciliation_state` (`none`, `pending`, `in_doubt`, `operator_required`), `redaction` (`policy_ref = belllabs.inspection-redaction.v1`, `withheld_field_count`) and a typed `reason` for degraded sections. Typed errors (never coerced): `invalid_cursor` (400), `cursor_expired` (410), `inspection_not_found` (404; unknown and out-of-scope are indistinguishable), `checkpoint_not_in_unit_lineage` (404), `incompatible_checkpoint` (409), `inspection_source_unavailable` (503, summary only).

**Service (`RuntimeInspectionService`, `app/application/run_control/inspection.py`).** Provider- and company-neutral. It joins four sources behind narrow ports:
- `InspectionReadRepository` (primary, persisted authority): `list_runs` and `read_run` return one consistent snapshot (`RunSnapshot`: projection, budget, effect ledger, runtime units with generations, leases, attempts, transitions, result observations, every incident revision, rejections, namespace heads, journal claims and settlements, async-child authority). Implementations: `PostgresInspectionReadRepository` and `InMemoryInspectionReadRepository`.
- `TemporalVisibilityReader` → `TemporalVisibilityInspectionReader` (`app/integrations/temporal_visibility.py`): `list_workflows` filtered by `BellLabsRunId` and `BellLabsScopeHash`, with an RPC timeout. No workflow Query is sent.
- `CheckpointHistoryReader` → `LangGraphCheckpointHistoryReader` (`app/integrations/agents/deep_agents/checkpoint_history.py`): `alist` over a namespace's root checkpoints and `aget_tuple` for one checkpoint, by qualified key; it never builds or invokes an agent and never writes.
- `AsyncChildDetailReader`: any object with `get_execution(scope, child_execution_id)`; the existing Mongo detail repository satisfies it structurally. The mapping is thin and tolerant (unknown fields are ignored, lifecycle values are shown as strings).

**Reads never mutate.** No service method writes, settles, reconciles, claims, opens an incident or records an observation. PostgreSQL reads run in a `READ ONLY`, `REPEATABLE READ` transaction with `belllabs.request_scope` set, so the database itself refuses a write and forced RLS confines rows to the scope. `reconcile_unit` remains the only, separate, privileged command.

**Historical reads (RUN-011 order).** (1) scope, run, unit and generation ownership (the unit must belong to the path's run and scope; the namespace is the generation's frozen namespace); (2) the checkpoint must lie on the generation's recorded lineage: with a transition, the parent chain from the recorded result back to (excluding) the expected source; otherwise every root checkpoint stamped with this generation's `belllabs_invocation_id` whose chain reaches the expected source; (3) the stamped binding and state-schema digests must equal the generation's frozen digests, else `incompatible_checkpoint`. Other units' checkpoints in a shared namespace are never part of the lineage.

**Redaction.** Default responses carry keys, digests, refs, counts and statuses only. The redacted state summary (`belllabs.redacted-checkpoint-summary.v1`) needs the distinct permission `workflow_run.read_checkpoint_summary` and exposes exactly the RUN-012 allowlist: state channel names, message count, `artifact_index` keys and count, todo count, presence of a structured response, pending task names and the five stamped digests (never `belllabs_attempt_ref`, never values). LangGraph 1.2 stores `DeltaChannel` state (`messages`, `files`) as ancestor writes, not checkpoint values; the adapter asks the saver for that channel's history (`aget_delta_channel_history`) and folds it to counts only (messages by identity, mappings by key), without any reducer, graph or agent. Pending tasks are derived without the graph: available `branch:to:<node>` triggers the node has not seen, plus the target node of each pending `Send` (its arguments are never read).

**Opaque cursors.** `InspectionCursorCodec`: HMAC-SHA256-signed, base64url tokens bound to kind (`runs`, `checkpoints`), a scope digest, a filter digest (phase set, or unit + generation) and an expiry (15 minutes). A forged, cross-scope, cross-filter, cross-unit, malformed or expired token is a typed error. The key is process-local by default; a deployment supplies one shared key for multi-replica pagination.

**Search Attribute policy (REQ-CP-EXEC-015).**
- `app/domain/orchestration/search_attributes.py`: `SearchAttributePolicy = required | disabled`; the nine attributes (7 Keyword, 2 Int: `BellLabsRunId`, `BellLabsScopeHash` (SHA-256 of the scope, never the raw scope), `BellLabsWorkflowKind`, `BellLabsFamily`, `BellLabsExecutionEpoch`, `BellLabsParentRunId`, `BellLabsUnitKey`, `BellLabsUnitKind`, `BellLabsExecutionGeneration`); value builders; `require_production_search_attribute_policy`; the scope-bound Visibility filter (quotes refused).
- The policy is a defaulted input field of `BellLabsRunInput`, `StageGraphRunInput`, `GoalDirectedRunInput` and `OperationWorkflowRequest` (absent means `disabled`). Workflow code never reads worker configuration. The root passes it into the family input and starts the family with the family attributes; each family passes it into the operation request and starts the `OperationWorkflow` with the operation attributes (unit key, unit kind and generation from the bound runtime unit). Continue-As-New keeps it because the inputs are `replace`d.
- `ensure_workflow_search_attributes` (inside root, family and operation workflows): under `required` it upserts only declared attributes the execution was not started with, deterministically from its own input; under `disabled` it emits nothing, so every captured history replays unchanged.
- `TemporalWorkflowSubmitter` starts the root with its attributes under `required`; `TemporalWorkflowSubmitter.for_production(...)` rejects `disabled`.
- `app/temporal/search_attributes.py`: typed keys, `register_belllabs_search_attributes` (idempotent administrative step through the Operator API; fails on a name/type conflict or a refused registration, e.g. no free slot) and `verify_belllabs_search_attributes` (read-only readiness).
- Generation-boundary upserts: no workflow applies a generation boundary yet (re-admission at `g+1` is RRM-014); `BellLabsExecutionGeneration` is set from the operation request at start.

### API surface

All routes are `GET`, under `/run-control/v1/inspection`, require `request_scope` held by the principal (otherwise 404) and `workflow_run.read` (otherwise 403).

| Route | Data | Sections |
|---|---|---|
| `/runs?request_scope&phase*&limit(1-100)&cursor` | `RunListPage`: run ID, scope, version, phase, terminal outcome, workflow type ref, ERC digest, reconciliation state, `updated_at`; `next_cursor` | `runs` |
| `/runs/{run_id}` | `RunInspection`: the run projection (waits, pauses, `unit_reconciliations`), run reconciliation state, `operator_reconciliation` waits, output refs, budget summary, effect statuses (ambiguous flag), unit summaries (status, reconciliation), async children, Temporal executions | `run`, `budget`, `effects`, `units`, `async_children`, `async_children_detail`, `temporal` |
| `/runs/{run_id}/units/{unit_key}` | `UnitInspection`: structured `CON-CP-RUNTIME-UNIT-V1` identity and `unit_key`; per generation: claim fence, binding ID and digest, cognitive namespace, state-schema digest, lease holder/expiry/state, `superseded`, Activity attempt observations (Temporal workflow, run and activity IDs, attempt, worker identity, fence, dispatching, expected source key), checkpoint transition (source and result qualified keys, ancestry, classification, manifest ref/digest), fenced result observation, every incident revision, stale-write rejections, namespace head and in-flight; journal claims with technical attempts and settlements (status, manifest ref/digest, usage); effect statuses of the unit's bindings; `reconcile_unit` decisions and pending operator waits; async children; Temporal executions joined by `BellLabsUnitKey` | `unit`, `lineage`, `journal`, `effects`, `reconciliation`, `async_children`, `async_children_detail`, `temporal` |
| `/runs/{run_id}/units/{unit_key}/checkpoints?execution_generation&limit&cursor` | `CheckpointHistoryPage`: namespace, checkpointer digest, recorded qualified keys (PostgreSQL), lineage entries oldest first (key, step, source, time, stamped, binding/schema compatibility, roles `result`/`namespace_head`/`incident_candidate`/`accepted_descendant`, pending task names) | `lineage`, `checkpoints` |
| `/runs/{run_id}/units/{unit_key}/checkpoints/{checkpoint_id}/summary?execution_generation` | `RedactedCheckpointStateSummary` (additionally requires `workflow_run.read_checkpoint_summary`) | `lineage`, `checkpoint` |

Roles: `operator`, `scheduler` and `auditor` read; the new role `state_inspector` holds `workflow_run.read` and `workflow_run.read_checkpoint_summary`. No existing role gains the summary permission. Composition: `get_runtime_inspection_service` builds the service over the API's application pool and attaches the optional qualified sources from `app.state` (`temporal_visibility_reader`, `inspection_checkpoint_reader`, `inspection_async_child_details`, `inspection_cursor_key`); each absent source is reported `unavailable`. The schema-export route `/v2/graph-runtime/schemas` is unchanged and is not an inspection endpoint.

## Requirement-to-evidence map

Unit tests are in `tests/unit/run_control/test_runtime_inspection_reads.py` (in-memory authority, production checkpoint reader over a real LangGraph `InMemorySaver`, through the FastAPI facade).

| Requirement | Test → observed assertion |
|---|---|
| RUN-011 scoped list, opaque cursors | `test_run_list_is_scoped_and_paginated_with_bound_opaque_cursors` → pages of 3 and 1 equal the sorted tenant-1 runs; the tenant-2 run never appears; the active run is `operator_required`, the terminal run `none`; the same cursor under tenant-2 or another phase filter → 400 `invalid_cursor`; a tampered, unsigned, three-part or empty cursor → 400; after 16 minutes → 410 `cursor_expired`; `phase=terminal` lists exactly the terminal run with outcome `failed` |
| RUN-011 cross-scope access | `test_cross_scope_access_is_denied_as_absent` → a principal without tenant-1 gets 404 on run, unit and history; tenant-1's run read under tenant-2 → 404 `inspection_not_found`; a unit through another run → 404; the `relay` role → 403 |
| RUN-011 content; RUN-007 `in_doubt` honesty; active and terminal | `test_active_and_terminal_runs_expose_identity_lineage_and_reconciliation` → terminal run: phase `terminal`, outcome `failed`, state `none`. Active run: phase stays `active` with one `operator_reconciliation` wait, state `operator_required`, the effect on the in-doubt binding `ambiguous`, async child lifecycle `in_doubt` shown as recorded. Unit read: StageGraph location, binding ID/digest, namespace, lease `expired`, attempt workflow ID/attempt/fence, transition result `c3`, manifest ref, namespace head `c3`. In-doubt unit: status `in_doubt`, lease `held`, namespace in flight, incident reason `multiple_stamped_leaves`, its effect, its wait, its child |
| RUN-011 reads never mutate | `test_reads_never_mutate_state_or_settle` → nine reads (incl. 404s) return `[200,200,200,200,200,200,404,200,404]`; the deep-copied state of the run-control repository, the lineage repository and every saver checkpoint is equal before and after; the projection still has no `unit_reconciliations` and the operator wait remains |
| RUN-012 / EXEC-015 missing live providers, freshness | `test_missing_live_providers_degrade_sections_and_persisted_reads_succeed` → no sources: 200, `temporal` `unavailable`/`visibility_reader_not_configured`, `async_children_detail` `unavailable`, `run` `current`, history 200 with `checkpointer_not_registered` and the recorded keys still served. Sources down: 200, `temporal_visibility_unavailable`, `detail_read_failed`, `checkpointer_unavailable`; summary 503 `inspection_source_unavailable`. Sources up: `temporal` `current` with 2 executions, the unit read keeps only its own execution, the child gains provider thread/run and `detail_lifecycle=running` (an extra unknown detail field is ignored); terminal authority with a `RUNNING` execution → `stale`/`visibility_lags_authority` |
| RUN-011 history = recorded lineage | `test_checkpoint_history_lists_only_the_units_recorded_lineage` → entries `c1, c3-parent, c3` over two cursor pages; the foreign branch and a later drifted descendant are excluded; `c3` roles `result, namespace_head`; `c3-parent` pending `tools`; section `checkpointer`/`current` with withheld metadata counted; the cursor is refused for another unit; the in-doubt unit's recorded keys are its incident candidates |
| RUN-012 redaction, separate permission | `test_redacted_summary_is_separately_authorized_and_excludes_content` → `operator`+`auditor` get 403; `state_inspector` gets the allowlist (5 state channel names, 3 messages, 1 todo, `artifact_index` keys `[report]`, structured response present, 6 values withheld, pending `tools`, stamped schema digest, no `belllabs_attempt_ref`); the secret, prompt, tool argument and file path appear in none of the summary, run, unit or history responses |
| RUN-011 incompatible checkpoint | `test_incompatible_or_foreign_checkpoints_are_typed_errors` → a lineage checkpoint with a drifted state-schema stamp → 409 `incompatible_checkpoint` (listed with `state_schema_compatible=false`); a foreign-unit checkpoint, an unknown ID and another unit's checkpoint → 404 `checkpoint_not_in_unit_lineage` |
| Redaction allowlist; delta channels | `test_redaction_allowlist_keeps_counts_and_names_only`; `test_delta_channel_counts_are_folded_from_the_saver_history_without_content` → `messages` stored only as ancestor writes counts 3 (a same-ID write replaces), `files` named, no content |
| EXEC-015 policy defaults and production rejection | `test_search_attribute_policy_defaults_to_disabled_and_production_requires_it` → all four inputs default `disabled`; an `OperationWorkflowRequest` payload without the field decodes `disabled`; production rejects `disabled`; the exact 8 operation attribute values, scope hashed (raw scope absent); the 5 root attributes; the scope-bound filter text; an injection attempt in the filter raises. `test_production_submitter_rejects_disabled_search_attributes` |
| EXEC-015 static check | `test_application_code_never_addresses_temporal_persistence` → no Temporal persistence table or DSN name in any `app/` `.py`/`.sql` file |
| PostgreSQL read path, both roles, zero writes, RLS, migration 0022 | `tests/integration/postgres/test_runtime_inspection_postgres.py::test_scoped_reads_under_runtime_and_readonly_roles_never_write` → under `SET ROLE belllabs_control_runtime` and `SET ROLE belllabs_operations_readonly` (non-superuser, no `BYPASSRLS`) the full read set passes: keyset pages, cross-scope cursor rejected, phase filter, cross-scope run → not found, unit statuses, ambiguous effect, budget, async child (`in_doubt` from the latest lifecycle fact), transition/result/head, incidents, lease judged on PostgreSQL's clock, history `c1, c3-parent, c3`, summary without content, foreign checkpoint refused. Both roles return identical read models. An md5 digest of every `belllabs_control` table is unchanged. The read-only role sees one tenant-2 run and zero tenant-2-invisible units, and `UPDATE`/`DELETE` on run, fact and generation tables raise `InsufficientPrivilegeError` |
| EXEC-015 real namespace: registration | `tests/integration/temporal/test_rrm_005_search_attributes.py::test_registration_is_idempotent_read_only_at_readiness_and_fails_on_conflict` → `start_local` registered the attributes (`verify` passes, `register` adds nothing); a new namespace fails readiness, registration adds all 9, then is idempotent; a namespace with `BellLabsRunId` as Int → `SearchAttributeRegistrationError` (conflicting type) |
| EXEC-015 real namespace: root, family, operations, join | `…::test_root_family_and_operations_join_inspection_through_visibility` → a StageGraph run started through `TemporalWorkflowSubmitter.for_production(required)`: Visibility counts 5 executions by `BellLabsRunId`+`BellLabsScopeHash` (kinds `root, family, operation×3`), all `stage_graph`, epoch 1, hashed scope; each unit key lists exactly its `operation/…` workflow with `stage_operation` and generation 1. The 5 histories were started with the attributes and contain 0 upserts; a `required` operation started without attributes upserts exactly once. All 6 histories replay. Inspection joins the persisted units: `temporal` `current`, 5 `COMPLETED` executions, each unit read lists exactly its workflow, matching the attempt's workflow ID; another scope's hash lists nothing. After the dev server is gone, the persisted read succeeds with `temporal` `unavailable`/`temporal_visibility_unavailable` and 3 units |
| EXEC-015 carried through Continue-As-New | `…::test_goal_directed_policy_is_carried_through_continue_as_new` → a 21-iteration GoalDirected family (`required`) continues as new once: Visibility lists 2 family runs (`COMPLETED`, `CONTINUED_AS_NEW`), both `goal_directed`, and 42 operation children, all carrying attributes (children after the Continue-As-New prove the policy survived it); both family histories replay |
| Demonstration (RUN-011/012, EXEC-015, DA-016/017 lineage) | `tests/acceptance/control_plane/test_rrm_005_inspection.py::test_inspect_a_two_checkpoint_technical_run_and_read_a_historical_checkpoint` → see Live runtime qualification |

## Changed paths and migrations

Shared seams edited under the coordinator's RRM-005 authorization (all additive):
- `app/domain/operation_execution/contracts.py`: `OperationWorkflowRequest.search_attribute_policy` (default `disabled`).
- `app/domain/orchestration/contracts.py`: `search_attribute_policy` on `BellLabsRunInput`, `StageGraphRunInput`, `GoalDirectedRunInput` (default `disabled`).
- Workflow inputs and workflows: `app/temporal/workflows/belllabs_run.py`, `stagegraph.py`, `goal_directed.py`, `operation.py` (ensure/upsert under `required`; child starts carry attributes and the policy; no command, signal, query or name changed; under `disabled` the emitted commands are byte-identical).
- `app/temporal/registration/*`: **unchanged** (no new workflow or activity).
- Run-control API: `app/api/run_control.py` (`principal_permissions` made public; new `state_inspector` role). Run-control repository: `InMemoryRunControlRepository.scoped_run_ids` (read-only helper). Reducer and service: **unchanged**.
- `app/server.py`: registers the inspection router.
- `app/migrations/0022_runtime_inspection_reads_v1.sql`: new (below).
- `app/domain/graph_runtime/identities.py`: **unchanged**.

New production paths: `app/domain/run_control/inspection.py`, `app/domain/orchestration/search_attributes.py`, `app/application/run_control/inspection.py`, `app/application/run_control/postgres_inspection_repository.py`, `app/api/runtime_inspection.py`, `app/integrations/temporal_visibility.py`, `app/integrations/agents/deep_agents/checkpoint_history.py`, `app/temporal/search_attributes.py`. Changed: `app/integrations/temporal_workflow_submission.py` (policy, root attributes, `for_production`).

Tests: new `tests/unit/run_control/test_runtime_inspection_reads.py`, `tests/fixtures/runtime_inspection.py`, `tests/integration/postgres/test_runtime_inspection_postgres.py`, `tests/integration/temporal/test_rrm_005_search_attributes.py`, `tests/acceptance/control_plane/test_rrm_005_inspection.py`; `tests/conftest.py` adds the demonstration to the Psycopg selector-loop list.

**Migration `0022_runtime_inspection_reads_v1.sql`** (forward-only; 0021 is left for RRM-013; schema identity `belllabs.inspection-read.v1`). It creates no table and alters none. It grants `belllabs_operations_readonly` `SELECT` on `workflow_runs`, `budget_accounts`, `effect_ledgers`, `async_subagent_authority` and `async_subagent_facts` (message and command payload tables are deliberately not granted) and adds two indexes: `workflow_runs (request_scope, run_id)` for keyset pagination and `async_subagent_authority (request_scope, parent_run_id)`. Every granted table already enables and forces request-scope RLS (0001, 0015, 0016). No role gains `INSERT`, `UPDATE` or `DELETE`. Red check: with 0022 withheld, the read-only role's `read_run` raised `InsufficientPrivilegeError: permission denied for table workflow_runs`; after 0022 it read the run (a one-off script under the stack lock that applied every migration except 0022 into a fresh schema, then 0022).

Deleted owners: none.

## Deterministic verification

All commands ran from the worktree with `unset VIRTUAL_ENV`, one pytest process at a time.

| Command | Result |
|---|---|
| New suites: `pytest tests/unit/run_control/test_runtime_inspection_reads.py` | 13 passed |
| Owning suites, both service DSNs, under the lock: `pytest tests/unit/run_control tests/integration/postgres/test_runtime_inspection_postgres.py tests/integration/postgres/test_checkpoint_lineage_postgres.py tests/integration/postgres/test_run_control_postgres_integration.py tests/integration/temporal/test_rrm_005_search_attributes.py tests/acceptance/control_plane/test_rrm_005_inspection.py` | 70 passed |
| `uv run --no-sync ruff check app tests scripts` | All checks passed! |
| `uv run --no-sync mypy app` | Success: no issues found in 349 source files |
| `BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q -rs` (hermetic, no DSNs, under the lock) | 764 passed, 56 skipped, 2 xfailed, 0 failed |
| The same flags with `TEST_APPLICATION_POSTGRES_DSN`, `TEST_MONGODB_URI` and `uv run --no-sync --env-file ../biotech-research-ingestion-evaluation-system/.env pytest -q -rs` (under the lock) | 796 passed, 24 skipped, 2 xfailed, 0 failed |
| `git diff --check bb964c5` | clean |

Delta against the baseline (hermetic 748 passed, 54 skipped, 2 xfailed; services 778 passed, 24 skipped, 2 xfailed):
- **Hermetic +16 passed:** 13 inspection unit tests and the 3 `start_local` Search Attribute tests. **+2 skipped:** the PostgreSQL inspection test and the demonstration, both DSN-gated; they run in the services gate.
- **Services +18 passed** (16 + those 2) with the same 24 skips (19 Agent Server endpoint, 3 live-provider flags, 1 WSL, 1 pre-existing retirement).
- Nothing was skipped, xfailed, deselected or weakened, and no existing assertion changed.
- Process note: the time-skipping replay suite (below) was started while the services gate was running, so two pytest processes overlapped once on this host; both passed. All other runs were serial.

## Live runtime qualification

No live LLM call. **Spend: USD 0.**

**Real-namespace Search Attribute qualification** (`WorkflowEnvironment.start_local(search_attributes=BELLLABS_SEARCH_ATTRIBUTE_KEYS)`; no lock needed): `uv run --no-sync pytest -q tests/integration/temporal/test_rrm_005_search_attributes.py` → 3 passed in 34 s (registration and conflict; StageGraph root/family/operations with the runtime join and the unreachable-Temporal read; GoalDirected across Continue-As-New).

**Demonstration** (`TEST_APPLICATION_POSTGRES_DSN`, `TEST_MONGODB_URI`, `LANGSMITH_TRACING=false`, under the stack lock): `uv run --no-sync pytest -q -s tests/acceptance/control_plane/test_rrm_005_inspection.py` → 1 passed. One StageGraph unit (`stage:draft`) ran through a real `OperationWorkflow` on a `start_local` dev server with the attributes registered and the policy `required`; cognition was a real `create_deep_agent` graph with the deterministic scripted model (one `write_todos` tool call, then the answer) over the real `AsyncPostgresSaver` (schema `rrm005_inspection_saver`), application PostgreSQL run control, journal and lineage (`compose_postgres_operation_recovery`), and the production Mongo OEB binding store. The inspection facade (FastAPI app, `httpx.ASGITransport`, principal `auditor`+`state_inspector`) then read the run, the unit, the history and one earlier checkpoint. Sanitized record printed by the test (IDs vary per run):

```text
RRM-005 EVIDENCE inspection: {"attempt": [1, 1], "history": [
  {"pending": [], "roles": [], "step": -1},
  {"pending": ["SkillsMiddleware.before_agent"], "roles": [], "step": 0},
  {"pending": ["PatchToolCallsMiddleware.before_agent"], "roles": [], "step": 1},
  {"pending": ["model"], "roles": [], "step": 2},
  {"pending": ["tools"], "roles": [], "step": 3},
  {"pending": ["model"], "roles": [], "step": 4},
  {"pending": [], "roles": ["result", "namespace_head"], "step": 5}],
  "replayed_events": 11, "run_phase": "active", "selected_step": 3,
  "settlement": "completed", "summary_digest": "sha256:a3033bb1…f6c0c",
  "summary_facts": {"channel_names": ["artifact_index", "child_result_index",
  "context_manifest", "files", "messages", "skills_metadata"],
  "has_structured_response": false, "message_count": 2, "todo_count": null},
  "tables_unchanged": true, "temporal_join": ["operation", "COMPLETED"],
  "unit_status": "settled", "workflow_id": "operation/<run>:operation:execution-epoch:1:stage:draft:…:attempt:1"}
```

Asserted: the run list holds exactly the run; the run is `active` with state `none` and one `settled` unit; Visibility joins exactly the unit's `operation` execution (`COMPLETED`) and its Temporal workflow and run IDs equal the recorded Activity attempt observation; attempt 1 at fence 1, dispatching; the journal holds technical attempt 1 and a `completed` settlement whose manifest digest equals the transition's; the result observation links the transition; 7 stamped, binding- and schema-compatible root checkpoints form one parent chain ending at the recorded result (`result`, `namespace_head`); the selected historical checkpoint (step 3, pending `tools`) is not the result; its summary counts 2 messages (the input and the tool call), lists the state channels, repeats the pending task and the stamped unit key, and counts at least every channel as withheld; neither the prompt text, the system prompt nor the answer marker appears in any response; the md5 digest of every `belllabs_control` table **and** every saver table is identical before and after all reads; the captured `OperationWorkflow` history (11 events) replays.

## Replay and recovery artifacts

| Command | Result |
|---|---|
| `uv run --no-sync pytest -q tests/integration/temporal tests/unit/operations/test_operation_execution.py` (time-skipping; includes every pre-existing captured-history replay: StageGraph any-join, Continue-As-New, GoalDirected children, Continue-As-New and cancellation, OperationWorkflow cross-queue, parked in-doubt OperationWorkflow) | 70 passed, 2 skipped (the DSN-gated RRM-004 worker restart and the WSL-only BP-010 recovery); the 3 new start_local tests are included |
| `test_rrm_005_search_attributes.py` | root, family and three operation histories recorded under `required` (started with attributes), one upserting operation history, and both runs of a GoalDirected family across Continue-As-New replay with `Replayer` |
| `test_rrm_005_inspection.py` | the demonstration's `OperationWorkflow` history replays |

Replay compatibility: every input change is a defaulted field; under `disabled` (the default and every existing history) no workflow emits a new command, and the child-start commands are unchanged (`search_attributes=None`). No workflow, activity, signal, query or payload field was renamed.

## Replacement and deletion checks

- Inspection reads replace nothing: `/v2/graph-runtime/schemas` still exports schemas only. The retired `RedactedCheckpointSummary` (graph_runtime, `LangGraphCheckpointKey`-keyed) stays inert; its successor is `RedactedCheckpointStateSummary`, keyed by `QualifiedCheckpointKey` with compatibility checks (disposition row 28). The `_reject_redacted_runtime_payload` allowlist was not reused: it validates inbound runtime payloads, while the summary is built allowlist-first.
- No competing lifecycle store or read-side copy: every read model is computed per request from authority.

## Unresolved risks and drift checks

- **Fork lineage (RUN-011 unit content).** Not shown: forks have no snapshot or seed-checkpoint records yet. RRM-006 adds them; the unit read model has room for a `fork_lineage` section.
- **Command receipts (RUN-011 unit content).** Only accepted `reconcile_unit` decisions and pending operator waits exist and are shown. `accepted → delivered → applied | rejected` receipts are RRM-007's.
- **Artifact refs.** Run-level output refs (accepted output evidence, finalization and readiness refs) and per-unit result manifest refs are shown; per-unit artifact refs live inside the digest-bound result manifest object, which inspection does not fetch.
- **Unit list size.** The run read loads every unit of the run in one snapshot; very large runs will need a paginated unit list (same cursor seam).
- **In-memory leases** are judged on the service clock; PostgreSQL leases on PostgreSQL's clock at the snapshot. A released lease (RRM-004 sets its expiry to the release time) shows as `expired`.
- **Delta-channel counts** fold the saver's delta history with an `add_messages`-equivalent identity rule; a custom reducer with different identity semantics would be counted by the same rule. Counts only, never content.
- **Composition (RRM-009).** The server composes inspection with no qualified runtime source by default (all reported `unavailable`). Production must attach `temporal_visibility_reader = TemporalVisibilityInspectionReader(client)`, `inspection_checkpoint_reader = LangGraphCheckpointHistoryReader({digest: registered saver})`, `inspection_async_child_details` (the Mongo detail repository) and a shared `inspection_cursor_key`; start roots with `TemporalWorkflowSubmitter.for_production(..., search_attribute_policy="required")`; run `register_belllabs_search_attributes` as the administrative step and `verify_belllabs_search_attributes` in worker readiness.
- **`BellLabsParentRunId`.** The builder supports it, but no root input carries a parent yet; fork and linked roots (RRM-006) must pass it.
- **External gates still unrun:** Agent Server endpoint suites, live providers and the WSL-only BP-010 recovery (unchanged).
- **New tickets:** none.

What RRM-006 must know: fork admission should read source units, settled results and checkpoints through `RuntimeInspectionService`/`InspectionReadRepository` (one read-only snapshot) rather than through new queries; a `cognitive_seed` must pass the same three historical-read checks (`_unit_lineage` ownership, lineage, digest compatibility); fork roots must set `BellLabsParentRunId`.

What RRM-007 must know: the unit read shows `reconcile_unit` decisions from the projection; once receipts exist, add them to the `reconciliation` section. The `operator` role does not hold `workflow_run.reconcile_unit` or the summary permission.

What RRM-008 must know: cancellation evidence (`cancelling`, `operator_required`, ambiguous effects, superseded generations, leases) is already visible through the run and unit reads; Temporal status joins come from Visibility, never from Queries. Heartbeat details are not shown (no heartbeat exists yet).

### Reusable seams (mission-horizon lens)

- `InspectionRead[T]` / `InspectionSection`: a source-, freshness- and redaction-labelled read envelope for any read model.
- `InspectionReadRepository` + `PostgresInspectionReadRepository`: one READ ONLY, scope-bound snapshot per read.
- `TemporalVisibilityReader`, `CheckpointHistoryReader`, `AsyncChildDetailReader`: optional qualified-source ports whose outage degrades a section.
- `InspectionCursorCodec`: signed, scope/filter-bound, expiring opaque cursors for any paginated read.
- `summarize_channel_values` and the delta fold: allowlist-first state summaries with counted withholding.
- `_unit_lineage`: recorded-lineage membership plus digest compatibility, the gate any historical or seeded read must pass.
- `BellLabsSearchAttributeValues` + `ensure_workflow_search_attributes` + register/verify: input-carried, replay-safe Visibility identity.

None of these carries company, fixture or provider specifics.

## Review disposition

Independent review verdict: `approve`, with four follow-ups requested before merge. They are applied in `62409d8` (code, tests and the RRM-009 ticket) and the documentation commit that follows it; nothing was amended.

| # | Finding | Disposition |
|---|---|---|
| 1 | `_unit_lineage.chain()` had no cycle guard, and the history reader loaded up to 10,000 checkpoints per namespace. | **Fixed.** The walk keeps a visited set and is bounded by the number of listed checkpoints (the observation list is itself bounded, so `MAX_LINEAGE_WALK` from the integration layer was not imported into the application layer, which would invert the dependency direction). A revisit raises the typed `CheckpointLineageCycle` (`reason = checkpoint_lineage_cycle`): the history section is `unavailable` with that reason and the summary is 503 `inspection_source_unavailable`. `RuntimeSourceUnavailable` now carries a typed `reason`. The reader loads at most `MAX_NAMESPACE_CHECKPOINTS = 2_000` root checkpoints (constructor-overridable) and, when a namespace holds more, reports `namespace_history_exceeds_bound` instead of truncating, because a truncated list would silently break the recorded lineage. Tests: `test_cyclic_checkpoint_parents_are_reported_not_walked_forever` (a result-recorded unit whose chain `c3 → c3-parent → c3` loops, and a transition-less unit whose stamped checkpoints loop: both 200 with the typed reason and no entries; summary 503). Against the previous `chain()` both cases loop forever, since neither ever reaches `None` or the stop key. `test_namespace_history_above_the_bound_is_unavailable_not_truncated` (bound 2 → `namespace_history_exceeds_bound`, no entries; bound 5 → the full lineage `c1, c3-parent, c3`). |
| 2 | Unit reads matched async children by `parent_operation_id ∈ {semantic_operation_id, binding IDs}`, so two units sharing a semantic operation ID could show each other's children. | **Fixed.** A child belongs to a unit generation only through that generation's exact binding (`_owned_by`). The 0016 authority row records `parent_operation_id` but no parent binding (`app/migrations/0016_async_subagent_parent_child_v1.sql`, `async_subagent_authority`); the binding is `AsyncSubagentExecution.parent_binding_id` in the immutable detail document (`app/domain/operation_execution/contracts.py`, `AsyncSubagentExecution`). With the detail, the child must name a binding of the unit at the same generation; without it, the authority's `parent_operation_id` attributes a child only when it is itself one of the unit's binding IDs. A semantic operation ID never attributes a child; such a child stays visible on the run read. `AsyncChildInspection` gains `parent_binding_id`. Test: `test_async_children_are_attributed_by_exact_binding_not_semantic_operation` (two units with the same `semantic_operation_id`, attempts 1 and 2: with the detail, unit 1 sees only its binding-matched child, unit 2 only its authority-binding child; a semantic-only child and a different-generation child are attributed to neither; the run read lists all four; without the detail only the binding-valued reference attributes). The existing fixtures now record the spawning binding as the parent reference. RRM-013 should record the parent binding in the authority row, or keep `parent_binding_id` in the detail, so unit attribution survives a detail outage. |
| 3 | The RRM-009 ticket lacked explicit composition items. | **Done.** Two acceptance items were added to `issues/09-qualify-production-capability-composition.md`: (a) `web_research_coordinator_live.py:704` and every production root starter use `TemporalWorkflowSubmitter.for_production(..., search_attribute_policy="required")`, with the register step and the read-only verify step at readiness, plus a Visibility qualification on the persistent namespace; (b) the server attaches `temporal_visibility_reader`, `inspection_checkpoint_reader`, `inspection_async_child_details` and one shared `inspection_cursor_key` on `app.state`. |
| 4 | Evidence README review section. | This section. |

Post-review gates (tested code head `62409d8`; one pytest process at a time; every DSN or full run under the stack lock):

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed! |
| `uv run --no-sync mypy app` | Success: no issues found in 349 source files |
| New suite: `pytest tests/unit/run_control/test_runtime_inspection_reads.py` | 16 passed (+3 review tests) |
| Owning suites, both DSNs (the list under Deterministic verification) | 73 passed |
| Full pytest, hermetic (`BELLABS_RUN_*_LIVE=0 LANGSMITH_TRACING=false`, no DSNs) | 767 passed, 56 skipped, 2 xfailed, 0 failed |
| Full pytest, both DSNs and `--env-file ../biotech-research-ingestion-evaluation-system/.env` | 799 passed, 24 skipped, 2 xfailed, 0 failed |
| `git diff --check bb964c5` | clean |

The delta against the pre-review head is +3 passed in both full runs (the three review tests); the skips are unchanged.

## Integration merge gates (coordinator)

RRM-015 merged at `2b1da64`, RRM-005 at `ed598dd`. When the two met on integration, RRM-015's static guard flagged the two JSON-mode dumps in `checkpoint_summary_digest` (`app/domain/run_control/inspection.py`). The integrator fixed them in `9a7e754`, switching to `stable_json_dump`, which is value-identical for these set-free contracts. Gates on `9a7e754`:

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed |
| `uv run --no-sync mypy app` | no issues, 349 files |
| Hermetic full pytest | 850 passed, 56 skipped, 2 xfailed, 0 failed |
| Full pytest with the disposable Postgres/Mongo DSNs and `--env-file` | 882 passed, 24 skipped, 2 xfailed, 0 failed |
| `git diff --check` on each merge | clean |

## Final disposition

accepted
