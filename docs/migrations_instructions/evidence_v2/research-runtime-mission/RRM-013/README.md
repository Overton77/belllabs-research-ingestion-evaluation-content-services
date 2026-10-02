# RRM-013 implementation evidence

Disposition: implemented; independent review pending (see Final disposition)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-013 real async subagents on a persistent local Agent Server, spawned from a Deep Agent inside `operation.execute` under BellLabs authority. Requirements: REQ-CP-DA-008 (reservation and link before submission; one provider run per child; `in_doubt` and its exits), REQ-CP-DA-011 (qualified provider checkpoint, attributed or pending usage, admission, late results, cancel acknowledgement), REQ-CP-DA-019 (exact, non-scheduling hosting; served identity), REQ-CP-RUN-009 (child usage settles to the parent budget), REQ-CP-EXEC-016 (active children block snapshots), REQ-CP-RUN-011 (child lineage read). Contracts: `CON-CP-ASYNC-SUBAGENT-V1` (AMD-RRM-001, accepted meta `main` `a50d833`); `QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE` (CP-045 regression). ADR-0003 and `.cursor/rules/agent-framework-coexistence.mdc`: the Agent Server hosts only async subagent graphs.
Base revision and head revision: base integration `a9c3f1d` (RRM-001, RRM-003, RRM-004 accepted); integration `bb964c5` (CR-1) merged into the branch at `44495e9`. Tested code head: see "Head" below. Not merged (the coordinator owns review and merge).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12.14 (Codex runtime); langgraph-cli 0.4.31, langgraph-api 0.12.0, langgraph-sdk 0.4.2, langgraph 1.2.10, deepagents 0.7.5, langchain 1.3.14, langchain-core 1.5.3, langsmith 0.10.15, langgraph-checkpoint-postgres 3.1.1, asyncpg 0.31.0, psycopg 3.3.4, pydantic 2.13.4, pytest 8.4.2, ruff 0.15.22, mypy 1.20.2. Agent Server image: `langchain/langgraph-api:0.12.0-py3.12` (api 0.12.0, sdk 0.4.2 inside the image, matching the repository pins; the untagged `langchain/langgraph-api:3.12` was api 0.15.1 with the constraint `langgraph-sdk>=0.4.4`, which conflicts with the pinned 0.4.2 and failed the build).

## Worktree provenance

- Worktree: `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-013`, branch `wp/rrm-013-async-subagent-agent-server`, created by the coordinator from integration `a9c3f1d`. Clean at kickoff. Integration moved to `bb964c5` (CR-1) during the work and was merged into the branch (no rebase) at `44495e9` without conflicts.
- The main checkout and the main `biotech-meta` checkout were not touched. No `.env` was copied, printed or committed: the Agent Server launcher and the live gate load the main `.env` through python-dotenv into the process environment only (names recorded below, values never).
- Shared disposable services (coordinator-owned, used only under the stack lock `stack_lock.py acquire/release RRM-013`): `rrm-app-postgres` (`127.0.0.1:55432/belllabs`) and `rrm-app-mongodb` (`127.0.0.1:27017`). The live gate drops and recreates `belllabs_control` and its own saver schema `rrm013_live_saver` and uses per-test Mongo databases `rrm013_live_<hex>`, dropped afterwards.
- The Agent Server stack is this ticket's own (see Deployment). The user's `csi01` and the RRM app stack were not touched.

## Deployment (persistent local Agent Server)

Facts re-verified on 2026-10-02:

- `uv run --no-sync langgraph --version` → `LangGraph CLI, version 0.4.31`. The `langgraph up` command prints: "For local dev, requires env var LANGSMITH_API_KEY with access to LangSmith Deployment. For production use, requires a license key in env var LANGGRAPH_CLOUD_LICENSE_KEY." (`langgraph_cli/cli.py:297-298`). `langgraph up --help` on Windows needs `PYTHONUTF8=1` (the help banner contains a non-cp1252 character).
- The server accepted the user's `LANGSMITH_API_KEY` as its license: the container logs report `api_variant=licensed`, `langgraph_api_version=0.12.0`, "Using langgraph_runtime_postgres", `License: {LicenseKey: ***, LangSmithAPIKey: ***}`, a LangSmith trace sink for project `BellLabsBiotech-AsyncSubagents-Local`, and `GET /info` returns `{"version":"0.12.0","langgraph_py_version":"1.2.10","host":{"kind":"self-hosted",…}}`. No `LANGGRAPH_CLOUD_LICENSE_KEY` was needed or used. No user checkpoint was triggered.
- `langgraph dev` and in-memory servers were not used.

Topology (all names exact):

| Item | Value |
|---|---|
| Compose project | `COMPOSE_PROJECT_NAME=rrm013-agent-server` |
| Config | `langgraph.async_subagents.json` (graph `belllabs_async_technical_child` → `app.agent_server.async_subagents.graph:graph`; `api_version` `0.12.0`; auth `app.agent_server.async_subagents.auth:auth`; http `app.agent_server.async_subagents.http_app:app`; env `langgraph.async_subagents.env`) |
| Env file (tracked, references only) | `langgraph.async_subagents.env`: `OPENAI_API_KEY=${OPENAI_API_KEY}`, `LANGSMITH_API_KEY=${LANGSMITH_API_KEY}`, `LANGSMITH_TRACING=${LANGSMITH_TRACING:-true}`, `LANGSMITH_PROJECT=${LANGSMITH_PROJECT_ASYNC_SUBAGENTS:-BellLabsBiotech-AsyncSubagents-Local}`, `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN=${BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN}` |
| Durable Postgres (own, not Compose-managed) | container `rrm013-agent-server-postgres`, image `pgvector/pgvector:pg16`, `127.0.0.1:55433`, database `langgraph`, user `langgraph`, named volume `rrm013-agent-server-pgdata` |
| Compose-managed | `rrm013-agent-server-langgraph-redis-1` (redis), `rrm013-agent-server-langgraph-api-1` (API, port `127.0.0.1:8143`), image built from the worktree (`.dockerignore` excludes `.venv`, `tests`, `docs`) |
| Endpoint | `AGENT_SERVER_ENDPOINT=http://127.0.0.1:8143` |
| Parent credential | reference `environment:BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` (`AsyncSubagentContract.deployment_credential_ref`); the parent sends `Authorization: Bearer <token>` and `x-belllabs-request-scope: <scope>`; the server compares in constant time against its own env value (`app/agent_server/async_subagents/auth.py`); threads and runs are scoped by `metadata.request_scope`; assistants are read-only; Store and crons are disabled |
| Identity route | `GET /belllabs/async-subagents/served-graphs` (same credential) → `{"graphs":[{"graph_id","graph_revision","graph_binding_digest","deepagents_version"}]}` |

Launch and teardown commands (run from the worktree; secrets come from the process environment, never from files in the repo):

```bash
# 1. The dedicated durable Postgres of the Agent Server (password generated locally; never committed)
docker run -d --name rrm013-agent-server-postgres -e POSTGRES_DB=langgraph -e POSTGRES_USER=langgraph \
  -e POSTGRES_PASSWORD="$RRM013_AGENT_SERVER_PG_PASSWORD" -p 127.0.0.1:55433:5432 \
  -v rrm013-agent-server-pgdata:/var/lib/postgresql/data pgvector/pgvector:pg16

# 2. Build and launch (env: OPENAI_API_KEY, LANGSMITH_API_KEY from the main .env; LANGSMITH_TRACING=true;
#    BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN generated locally; COMPOSE_PROJECT_NAME=rrm013-agent-server; PYTHONUTF8=1)
docker pull langchain/langgraph-api:0.12.0-py3.12
uv run --no-sync langgraph up --config langgraph.async_subagents.json \
  --postgres-uri "postgresql://langgraph:<redacted>@host.docker.internal:55433/langgraph?sslmode=disable" \
  --port 8143 --wait --verbose --api-version 0.12.0 --no-pull

# 3. Restart drill (API container only; Postgres and Redis keep their state)
docker restart rrm013-agent-server-langgraph-api-1

# 4. Teardown (coordinator decides when; exact names only, never prune)
docker compose -p rrm013-agent-server down
docker rm -f -v rrm013-agent-server-postgres
docker volume rm rrm013-agent-server-pgdata rrm013-agent-server_langgraph-data   # the second only if Compose created it
```

The image was rebuilt three times during the ticket (constraint fix to the pinned base image; hosted context defaults; streamed usage). Each rebuild takes about four minutes on this host.

## Implemented contracts and seams

**Contracts (`app/domain/operation_execution/contracts.py`, coordinator seam, authorized).**
- `AsyncSubagentLifecycle.IN_DOUBT` (AMD-RRM-001). `AsyncSubagentExecution` carries `in_doubt_reason` (`submission_unobservable`, `multiple_provider_runs`, `graph_identity_mismatch`, `provider_binding_lost`) and `incident_id` exactly while in doubt, plus `result_output_text` (provider evidence, revealed to the parent only after admission). `ACTIVE_ASYNC_SUBAGENT_LIFECYCLES` for EXEC-016.
- `AsyncSubagentContract` gains `graph_revision`, `graph_binding_digest` and `deployment_credential_ref` (reference only; pattern-validated), all inside the contract digest (DA-019).
- `AsyncProviderCheckpointKey` (thread, `checkpoint_ns` as reported, checkpoint id, served graph identity; canonical `ref`) and `AsyncSubagentUsage` (`provider_attributed` | `pending` | `ambiguous` with amounts). `AsyncSubagentResultManifest` carries both as typed fields; `checkpoint_ref` and `usage_ref` are validated to be their derived refs (DA-011). The placeholder refs of CP-045 are gone.
- `ParentAsyncSubagentLink` gains `cancellation_receipt` (`provider_acknowledged` | `ambiguous`), `reconciliation_decision` and `adopted_provider_run_id`.
- `compile_deep_agent_execution_binding` admits a remote placement only with `checkpoint_behavior=remote_managed` and `reconnect_behavior=remote_run_reconnect` (the hosted binding).

**Pure reconciliation rules (`app/domain/operation_execution/async_subagent_reconciliation.py`, new).** `AsyncServedGraphIdentity.matches(contract)`; `AsyncSpawnKeyObservation` and `classify_spawn_key_observation` (exactly one verified run binds; none is `no_provider_run`; more than one or a mismatching identity is `in_doubt`); `AsyncSubagentIncident` (`belllabs.async-subagent-incident.v1`, revisioned, resolved by observation or by exactly one decision); `AsyncProviderRunRecord` (`belllabs.async-provider-run.v1`, bound / duplicate_cancelled / orphaned_cancelled with usage); `classify_async_children_for_fork` (`prohibited` / `snapshot_not_quiescent` naming the active children, else `quiescent`).

**Service (`app/application/async_subagents/service.py`).**
- Order kept from CP-045: Mongo detail, PostgreSQL authority, then (new) the parent run's reservation, effect claim and child registration, all before any provider call.
- RRM-001 §7 #10 fixed: an `admitted` child without a binding is submitted on retry through the same fenced path; a submission error never marks it `orphaned`. The error is classified from provider state: no run → the child stays `admitted` and the error propagates (retryable); one verified run → bound; otherwise `in_doubt`.
- RRM-001 §7 #11 fixed: submission is fenced per child (`acquire_submission_fence` with holder and lease; takeover only after expiry, advancing the fence); a live holder makes a concurrent submitter raise `AsyncSubagentSubmissionInProgress`. Inside the fence the adapter looks a run up by spawn key before creating one and asks the server to reject a concurrent run on the thread.
- DA-019: the served identity is verified before every first submission (`AsyncServedGraphMismatch` fails closed with no provider work) and the stamped identity is re-verified on every observation and on result capture.
- `in_doubt`: `_enter_in_doubt` opens the incident (revision n+1 after a resolved one), mirrors the lifecycle to authority, records the fact, and marks the parent's effect claim `ambiguous`. `reconcile` re-verifies the spawn key on every reconnect for every active lifecycle; observation resolves `in_doubt` to the single verified run or to `orphaned` when no run exists (incident resolution `observation`). `reconcile_in_doubt` implements `adopt_provider_run(run_id)` (every other run cancelled, usage pending) and `orphan_child` (every run cancelled, usage pending, lifecycle `orphaned`), records the decision in the command ledger, resolves the incident once, and replays an exact resend idempotently.
- `cancel` journals the intent first, then reaches the provider run and records `provider_acknowledged` or `ambiguous` (link field and `cancellation` fact), never assuming.
- `settle` settles the child's usage against the parent run exactly once; `decide_result` records late rejections as a `result` fact `late_rejected:<digest>`.

**Parent budget effects (`app/application/async_subagents/parent_effects.py`, new).** `RunControlAsyncChildEffects` over `RunControlService`: `ReserveBudgetAction` (child reservation carved from the run's budget, sponsored by the parent operation's reservation), `ClaimEffectAction` with `effect_kind=async_subagent.child` and `operation_ref = parent binding_id` (so RRM-004's `unsettled_effect_ids` sees an unsettled or ambiguous child), `RegisterAsyncChildAction`, `ObserveEffectAction` + `RecordAsyncChildFactAction` per observation, and at settlement `RecordUsageAction` (attributed amounts consumed, unused reservation released, pending amounts `pending_external`) plus `SettleEffectAction` only when nothing is pending. Commands are idempotent by identity and replay after a crash. Only the child's declared budget dimensions settle; the manifest keeps every reported amount.

**Adapter (`app/integrations/agents/deep_agents/async_subagents.py`).** `DeepAgentsAsyncSubagentAdapter` keeps the drift-checked stock 0.7.5 tool surface for check, update, cancel and list; resolves the deployment credential from a reference; `start` uses the BellLabs child id as thread id, stamps `belllabs_child_execution_id`, `belllabs_parent_run_id`, `belllabs_parent_binding_id`, `belllabs_contract_digest`, `request_scope` and the spawn key on the run, uses `multitask_strategy="reject"`; `observe_spawn_key` lists the thread's runs with the spawn key and reads the stamped served identity; a completed run yields the qualified checkpoint from `threads.get_state` and usage attributed from the AI messages (`tokens.total`, `model.turns`, restricted to the contract's dimensions; `pending` when the provider reported none); `cancel` polls the run for the acknowledgement. `BellLabsAsyncSubagentMiddleware` exposes the five stock tool names and schemas to the parent model; `start_async_task` builds a deterministic spawn request from the tool call id (so a resumed invocation rebuilds the same child), `check_async_task` reconciles and reveals the child's output only after admission. The module evaluates annotations eagerly because `StructuredTool` recognises the injected `ToolRuntime` from the runtime annotation.

**Deep Agent adapter and materializer (coordinator seams, authorized).** `DeepAgentRuntimeAdapter(materializer, async_subagents=factory)` adds the governed middleware when the binding declares async contracts; `build_hosted_async_subagent_graph` compiles the hosted graph from the exact binding (`prepare(hosted=True)`: remote-managed placement, no local checkpointer or store, hosted context defaults from the frozen `cognitive_context_values`) with `ServedGraphIdentityMiddleware` stamping `belllabs_served_graph` into thread state. Both paths share the single `create_deep_agent` call (`_compile`); `test_create_deep_agent_has_one_non_experiment_production_call_site` still holds. `OpenAIExactModelFactory` admits `stream_usage`.

**Hosting (`app/agent_server/async_subagents/`, new).** `bindings.py` builds the exact technical-child binding deterministically (profile `agent.async-technical-child@1`, model `gpt-5.6-luna` with `reasoning_effort=low`, `verbosity=low`, `use_responses_api`, `stream_usage`; one exact tool `wait_seconds` with its schema digest; state backend; base cognitive packs) and exposes `served` identity and `contract(agent_protocol_url=…)`; `graph.py` is the import-safe async graph factory (built once per process through the adapter); `auth.py` and `http_app.py` as above. The root `langgraph.json` is unchanged and graph-free.

**PostgreSQL authority (`app/application/async_subagents/postgres_async_subagents.py`; migration `0021`).** Submission fence (`acquire_submission_fence` by a single conditional `UPDATE … RETURNING`), lifecycle mirror (`record_execution_state`), provider-run records (upsert that never demotes a cancellation back to `bound`), typed incidents in `runtime_reconciliation_incidents` under `incident_type='async_submission_in_doubt'`, decisions in the command ledger, and the read-only `list_children` lineage view (`AsyncChildLineageView`, `inspection.py`).

**Mongo detail** repository is unchanged in shape; the execution payload now carries the new fields.

## Requirement-to-evidence map

| Requirement | Test → observed assertion |
|---|---|
| DA-008 reservation, link, authority and parent claim before submission; real spawn path from `operation.execute` | Live `test_rrm_013_async_subagent_live.py::test_parent_deep_agent_spawns_one_real_child_and_admits_its_result` → the parent `OperationExecutionService.execute` completes with `structured_output["spawned"]` = "Launched async subagent. task_id: <child>"; `list_children` shows one child whose `parent_binding_id` is the parent's binding id, fence 1 released; run control holds the effect claim (`operation_ref` = parent binding, `effect_kind=async_subagent.child`) and the registered async child; the provider holds exactly one run with the spawn key, bound to the child. Offline: `test_async_subagent_submission_fence.py::test_parent_deep_agent_start_async_task_reserves_and_links_before_the_provider` → the fake provider's start sees the execution and reservation already recorded; one `sdk.runs.create`; the same tool call id rebuilds the same child. CP-045 `test_spawn_persists_contract_link_and_reservation_before_provider_submission` → event order `mongo → postgres → verify → start`, fence 1 released |
| DA-008 one provider run per child across the crash windows (RRM-001 §7 #10, #11) | Live `…::test_worker_killed_in_the_submission_window_recovers_with_one_provider_run[before_submit]` and `[after_submit]` (see the matrix). Offline: `test_crash_before_submission_resumes_the_admitted_child_with_one_provider_run` (admitted, no run, fence released; retry submits once, fence 2, no `orphaned` fact), `test_crash_after_submission_before_observation_reconnects_to_the_same_run` (one run; a replacement worker reconciles to the same run), `test_live_fence_holder_blocks_a_concurrent_submitter_until_its_lease_expires` (`AsyncSubagentSubmissionInProgress`, then takeover at fence 2) |
| DA-008 `in_doubt`, never a second spawn; exits by observation, `adopt_provider_run`, `orphan_child` | Live `…::test_duplicate_provider_run_is_in_doubt_until_adopt_provider_run` → a second run with the spawn key makes `reconcile` return `in_doubt` / `multiple_provider_runs`; the incident row is `operator_required` with both candidates; the parent's effect claim is `ambiguous`; a repeated spawn stays `in_doubt` with two runs; `adopt_provider_run` binds the first run, the duplicate is cancelled (`duplicate_cancelled`, usage `pending`), the incident resolves `adopt_provider_run`, the child completes with one bound run. Offline: `test_unobservable_submission_is_in_doubt_with_an_incident_never_orphaned` (`submission_unobservable`, incident, no orphan fact, exit by observation with resolution `observation`), `test_multiple_provider_runs_are_in_doubt_and_adopt_provider_run_cancels_the_rest` (unknown run refused; exact resend idempotent; one cancel), `test_orphan_child_cancels_every_run_and_records_their_usage_as_pending` (every run `interrupted`, `orphaned_cancelled` records with pending usage, a second decision refused) |
| DA-019 served identity verified before submission and on reconnect; dedicated config lists only bound async graphs; root config graph-free | Offline `test_served_graph_mismatch_fails_before_submission` (child stays `admitted`, no `sdk.runs.create`), `test_completed_thread_served_by_another_graph_is_in_doubt_not_admitted` (`graph_identity_mismatch`, no manifest), `test_contract_freezes_the_hosted_graph_identity_in_its_digest`; `test_async_subagent_server_offline.py::test_dedicated_config_lists_only_bound_async_graphs_and_root_stays_graph_free`, `::test_served_identities_are_the_exact_binding_digests`, `::test_hosted_graph_is_built_through_the_canonical_adapter_and_stamps_identity` (checkpointer `None`, `belllabs_served_graph` stamped, hosted context defaults), `::test_identity_route_requires_the_deployment_credential`; `test_agent_server_block_c_unit.py::test_root_langgraph_does_not_select_block_c_auth_or_graphs` unchanged. Live: the served route of the running server returns the exact digest the parent binds (`graph_binding_digest` in the live evidence) |
| DA-011 qualified checkpoint, attributed usage, admission, late results | Live spawn test → manifest `provider_checkpoint` names graph id, revision and binding digest of the served graph, the child thread and a checkpoint id; usage `provider_attributed` with `tokens.total > 0`; `decide_result(admit, parent_open=False)` raises "late or superseded" and records `late_rejected:<digest>`; `check` through the stock tool returns `success`. Offline: CP-045 `test_completed_output_cannot_change_parent_before_explicit_admission_or_when_late`; `test_unreported_usage_is_recorded_pending_never_dropped` |
| RUN-009 child usage settles to the parent budget exactly once; pending stays pending | Live spawn test → after `settle`, the run budget's `async-child-usage:<child>` record consumes the attributed tokens, the effect is `SUCCEEDED`, the lineage view shows `result_decision=admit` and the settlement ref. Offline `test_async_child_parent_budget.py::test_child_is_claimed_as_a_parent_effect_before_submission_and_usage_settles_once` (reservation 10 carved, consumed 7, released 3, effect settled with `usage_settlement_ref`), `::test_in_doubt_child_leaves_its_parent_effect_ambiguous_and_unsettled`, `::test_pending_usage_is_recorded_pending_and_blocks_effect_settlement` (`pending_settlement`, outstanding usage, effect unsettled) |
| DA-011 / EXEC-008 cancel reaches the provider run; acknowledgement or ambiguity recorded | Live `…::test_parent_cancel_reaches_the_provider_run_and_records_the_acknowledgement` → provider run status after cancel and the recorded `cancellation_receipt` agree; `cancellation` fact recorded; one provider run. Offline CP-045 `test_cancellation_is_authorized_before_provider_and_acknowledgement_recorded` |
| Agent Server restart during an active child | Live `…::test_agent_server_restart_during_an_active_child_is_reconciled_from_durable_state` (see the matrix) |
| EXEC-016 fork admission classifies active children | Offline `test_fork_admission_classifies_active_children_as_not_quiescent` → `prohibited` / `snapshot_not_quiescent` naming the running child; `quiescent` after completion and for no children |
| RUN-011 child lineage read (RRM-005 consumer) | `test_async_subagent_postgres_authority.py::test_runtime_role_fences_submission_and_records_in_doubt_lineage` → `list_children` returns BellLabs identity, provider thread and run, graph id/revision/binding digest, lifecycle, fence and holder, decision, result decision, settlement ref and provider-run records |
| Migration 0021 and least privilege | the same Postgres test under `SET ROLE belllabs_control_runtime`: fence takeover semantics (1 → blocked → 2 → stale release ignored → 3), idempotent admit and incident open, decisions ledger `[admit, adopt_provider_run]`, forced RLS hides other scopes, `DELETE` on provider runs and facts raises `InsufficientPrivilegeError`; `test_run_control_postgres_integration.py` applies the migration chain |
| CP-045 regression | `tests/acceptance/control_plane/test_wp_cp_045.py` (all tests pass; fixtures carry the new fields; the fake-client test is offline regression only) |

### Crash-window and restart matrix (live, real Agent Server, real model child)

Filled from the live run (see Live runtime qualification).

## Changed paths and migrations

Shared seams edited under the coordinator's RRM-013 authorization:
- `app/domain/operation_execution/contracts.py` (additive contract changes above).
- `app/domain/operation_execution/materialization.py` (remote placement admission).
- `app/integrations/agents/deep_agents/adapter.py` (governed middleware hook, hosted graph builder, single `_compile` call site, `ServedGraphIdentityMiddleware`).
- `app/integrations/agents/deep_agents/materializer.py` (`prepare(hosted=…)`, optional checkpointer/store for hosted graphs, hosted context defaults, `stream_usage`).
- `app/migrations/0021_async_subagent_submission_fence_v1.sql` (new).
- `app/domain/graph_runtime/identities.py`, `app/temporal/registration/*`, run-control reducer/service/repositories, root `langgraph.json`: **unchanged**.

Owned paths: `app/application/async_subagents/{service,postgres_async_subagents,parent_effects,inspection}.py`, `app/integrations/agents/deep_agents/async_subagents.py`, `app/domain/operation_execution/async_subagent_reconciliation.py`, `app/agent_server/async_subagents/*`, `langgraph.async_subagents.json`, `langgraph.async_subagents.env`.

Tests: new `tests/fixtures/fake_agent_protocol.py`, `tests/fixtures/rrm013_live_stack.py`, `tests/fixtures/rrm013_child_worker.py`, `tests/integration/agent_server/test_rrm_013_async_subagent_live.py`, `tests/integration/postgres/test_async_subagent_postgres_authority.py`, `tests/unit/integrations/test_async_subagent_submission_fence.py`, `tests/unit/operations/test_async_child_parent_budget.py`, `tests/unit/agent_server/test_async_subagent_server_offline.py`; updated `tests/acceptance/control_plane/test_wp_cp_045.py` (new contract fields and typed observations; the fake Agent Protocol client moved to the fixture) and `tests/conftest.py` (selector loop for the live module).

**Migration `0021_async_subagent_submission_fence_v1.sql`** is forward-only and the next free number. Schema identities: `belllabs.async-provider-run.v1`, `belllabs.async-subagent-incident.v1`. It adds the lifecycle mirror, provider binding, submission fence/holder/lease, in_doubt reason and incident columns to `async_subagent_authority` (with shape checks), widens `async_subagent_commands.command_kind` with `adopt_provider_run` and `orphan_child`, creates the insert/update-only `async_subagent_provider_runs` with forced RLS and the `belllabs_control_runtime` / `belllabs_operations_readonly` grants, and adds inspection indexes. Nothing is renamed; the 0016 tables are reused (RRM-001 disposition row 54).

Deleted owners: none. The placeholder usage/checkpoint refs of CP-045 (`ref:async-usage:<run>`, `ref:agent-protocol-thread:<thread>`) are replaced by the typed key and usage.

## Deterministic verification

Filled from the gate runs (see below).

## Live runtime qualification

Filled from the live run.

## Replay and recovery artifacts

Filled from the gate runs.

## Replacement and deletion checks

- `AsyncSubagentService.spawn` no longer returns an admitted child idle and no longer marks a submission error `orphaned` (RRM-001 §7 #10 resolved).
- `DeepAgentsAsyncSubagentAdapter.start` is fenced by the service and looks up before creating; usage and checkpoint refs are real (RRM-001 §7 #11 resolved).
- No competing lifecycle store: the child's authority stays in PostgreSQL (0016/0021), the parent's budget and effect authority in run control, Mongo holds detail, the Agent Server holds only its own threads and runs. The Agent Server registers no BellLabs root, family or operation-workflow graph (ADR-0003).
- Block C stays qualification topology; nothing of it was revived. Its `langgraph.block_c*.json` and fixture are untouched.

## Unresolved risks and drift checks

Filled after the gates.

## Final disposition

implemented; independent review pending
