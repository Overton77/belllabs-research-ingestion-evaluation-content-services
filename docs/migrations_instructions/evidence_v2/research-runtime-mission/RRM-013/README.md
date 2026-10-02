# RRM-013 implementation evidence

Disposition: implemented; independent review pending (see Final disposition)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-013 real async subagents on a persistent local Agent Server, spawned from a Deep Agent inside `operation.execute` under BellLabs authority. Requirements: REQ-CP-DA-008 (reservation and link before submission; one provider run per child; `in_doubt` and its exits), REQ-CP-DA-011 (qualified provider checkpoint, attributed or pending usage, admission, late results, cancel acknowledgement), REQ-CP-DA-019 (exact, non-scheduling hosting; served identity), REQ-CP-RUN-009 (child usage settles to the parent budget), REQ-CP-EXEC-016 (active children block snapshots), REQ-CP-RUN-011 (child lineage read). Contracts: `CON-CP-ASYNC-SUBAGENT-V1` (AMD-RRM-001, accepted meta `main` `a50d833`); `QUAL-CP-ASYNC-SUBAGENT-LIFECYCLE` (CP-045 regression). ADR-0003 and `.cursor/rules/agent-framework-coexistence.mdc`: the Agent Server hosts only async subagent graphs.
Base revision and head revision: base integration `a9c3f1d` (RRM-001, RRM-003, RRM-004 accepted). Integration `bb964c5` (CR-1) merged at `44495e9`; integration `d46f548` (RRM-005, RRM-015) merged at `5e4a0da` (one conflict, `tests/conftest.py`, both selector-loop entries kept). Tested code head: `9c1d344` on `wp/rrm-013-async-subagent-agent-server` (the evidence/ticket commit follows it and changes documentation only). Not merged (the coordinator owns review and merge).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12.14 (Codex runtime); langgraph-cli 0.4.31, langgraph-api 0.12.0, langgraph-sdk 0.4.2, langgraph 1.2.10, deepagents 0.7.5, langchain 1.3.14, langchain-core 1.5.3, langsmith 0.10.15, langgraph-checkpoint-postgres 3.1.1, asyncpg 0.31.0, psycopg 3.3.4, pydantic 2.13.4, pytest 8.4.2, ruff 0.15.22, mypy 1.20.2. Agent Server image: `langchain/langgraph-api:0.12.0-py3.12` (api 0.12.0 and sdk 0.4.2 inside the image, matching the repository pins). The untagged `langchain/langgraph-api:3.12` was api 0.15.1 with the pip constraint `langgraph-sdk>=0.4.4`, which conflicts with the pinned 0.4.2 and failed the first image build; the config therefore pins `api_version` `0.12.0`.

## Worktree provenance

- Worktree: `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-013`, branch `wp/rrm-013-async-subagent-agent-server`, created by the coordinator from integration `a9c3f1d`. Clean at kickoff. Two integration merges, no rebase (see above).
- The main checkout and the main `biotech-meta` checkout were not touched. No `.env` was copied, printed or committed: the Agent Server launcher and the live gate load the main `.env` through python-dotenv into the process environment only. Credentials used by name: `OPENAI_API_KEY`, `LANGSMITH_API_KEY` (main `.env`); `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` and the Agent Server Postgres password, generated locally for this session and kept in the session scratchpad.
- Shared disposable services (coordinator-owned, used only under the stack lock `stack_lock.py acquire/release RRM-013`): `rrm-app-postgres` (`127.0.0.1:55432/belllabs`) and `rrm-app-mongodb` (`127.0.0.1:27017`). The live gate drops and recreates `belllabs_control` and its own saver schema `rrm013_live_saver`, and uses per-test Mongo databases `rrm013_live_<hex>` that it drops afterwards.
- The Agent Server stack is this ticket's own (next section) and is **left running** for RRM-008 and RRM-009. The user's `csi01` and the RRM app stack were not touched. No `prune` or `down --volumes` was run.

## Deployment (persistent local Agent Server)

Facts re-verified on 2026-10-02:

- `uv run --no-sync langgraph --version` → `LangGraph CLI, version 0.4.31`. `langgraph up` prints: "For local dev, requires env var LANGSMITH_API_KEY with access to LangSmith Deployment. For production use, requires a license key in env var LANGGRAPH_CLOUD_LICENSE_KEY." (`langgraph_cli/cli.py:297-298`). On Windows the CLI needs `PYTHONUTF8=1` (its help banner contains a non-cp1252 character).
- The server accepted the user's `LANGSMITH_API_KEY` as its license: the API container logs report `api_variant=licensed`, `langgraph_api_version=0.12.0`, "Using langgraph_runtime_postgres", `"License":{"LicenseKey":"***","LangSmithAPIKey":"***"}`, and a LangSmith trace sink for project `BellLabsBiotech-AsyncSubagents-Local`; `GET /info` returns `{"version":"0.12.0","langgraph_py_version":"1.2.10","host":{"kind":"self-hosted",…}}`. No `LANGGRAPH_CLOUD_LICENSE_KEY` was needed. No user checkpoint was triggered.
- `langgraph dev` and in-memory servers were not used. The server's own Postgres is separate from the application Postgres.

Topology (exact names):

| Item | Value |
|---|---|
| Compose project | `COMPOSE_PROJECT_NAME=rrm013-agent-server` |
| Config | `langgraph.async_subagents.json`: graph `belllabs_async_technical_child` → `app.agent_server.async_subagents.graph:graph`; `api_version` `0.12.0`; auth `app.agent_server.async_subagents.auth:auth` (`disable_studio_auth: true`); http `app.agent_server.async_subagents.http_app:app`; env `langgraph.async_subagents.env` |
| Env file (tracked; variable references only) | `OPENAI_API_KEY=${OPENAI_API_KEY}`, `LANGSMITH_API_KEY=${LANGSMITH_API_KEY}`, `LANGSMITH_TRACING=${LANGSMITH_TRACING:-true}`, `LANGSMITH_PROJECT=${LANGSMITH_PROJECT_ASYNC_SUBAGENTS:-BellLabsBiotech-AsyncSubagents-Local}`, `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN=${BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN}` |
| Durable Postgres (own; not Compose-managed) | container `rrm013-agent-server-postgres`, image `pgvector/pgvector:pg16`, `127.0.0.1:55433`, database `langgraph`, user `langgraph`, named volume `rrm013-agent-server-pgdata` |
| Compose-managed | `rrm013-agent-server-langgraph-redis-1`; `rrm013-agent-server-langgraph-api-1` (API on `127.0.0.1:8143`), image built from the worktree (`.dockerignore` excludes `.venv`, `tests`, `docs`) |
| Endpoint | `AGENT_SERVER_ENDPOINT=http://127.0.0.1:8143` |
| Parent credential | the contract names `deployment_credential_ref = environment:BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`; the parent resolves it and sends `Authorization: Bearer <token>` plus `x-belllabs-request-scope: <scope>`; the server compares in constant time against its own env value (`app/agent_server/async_subagents/auth.py`), scopes threads and runs by `metadata.request_scope`, keeps assistants read-only and disables Store and crons. A wrong token is `401`; the identity route without a token is `401` |
| Identity route | `GET /belllabs/async-subagents/served-graphs` (same credential) → `{"graphs":[{"graph_id":"belllabs_async_technical_child","graph_revision":"agent.async-technical-child@1","graph_binding_digest":"sha256:764f0ef514bde5ae1428f409d66ab35c67808e86d4db3667ef8a318c278199bd","deepagents_version":"0.7.5"}]}` |
| Assistant | `assistants.search(graph_id=…)` → one deployment assistant for `belllabs_async_technical_child` |

Launch and teardown commands (run from the worktree; secrets only in the process environment):

```bash
# 1. The Agent Server's own durable Postgres (password generated locally; never committed)
docker run -d --name rrm013-agent-server-postgres -e POSTGRES_DB=langgraph -e POSTGRES_USER=langgraph \
  -e POSTGRES_PASSWORD="$RRM013_AGENT_SERVER_PG_PASSWORD" -p 127.0.0.1:55433:5432 \
  -v rrm013-agent-server-pgdata:/var/lib/postgresql/data pgvector/pgvector:pg16

# 2. Build and launch. Environment: OPENAI_API_KEY and LANGSMITH_API_KEY (from the main .env, loaded
#    into the process only), LANGSMITH_TRACING=true, BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN (local),
#    COMPOSE_PROJECT_NAME=rrm013-agent-server, PYTHONUTF8=1
docker pull langchain/langgraph-api:0.12.0-py3.12
uv run --no-sync langgraph up --config langgraph.async_subagents.json \
  --postgres-uri "postgresql://langgraph:<redacted>@host.docker.internal:55433/langgraph?sslmode=disable" \
  --port 8143 --wait --verbose --api-version 0.12.0 --no-pull

# 3. Restart drill (API container only; its Postgres and Redis keep their state)
docker restart rrm013-agent-server-langgraph-api-1

# 4. Teardown (the coordinator decides when; exact names only, never prune)
docker compose -p rrm013-agent-server down
docker rm -f -v rrm013-agent-server-postgres
docker volume rm rrm013-agent-server-pgdata
```

The image was rebuilt four times during the ticket (base-image pin; hosted context defaults; streamed usage; usage stamping). A rebuild takes about four minutes on this host; `langgraph up --watch` would rebuild on every change, so it was not used.

## Implemented contracts and seams

**Contracts (`app/domain/operation_execution/contracts.py`; coordinator seam, authorized).**
- `AsyncSubagentLifecycle.IN_DOUBT` (AMD-RRM-001). `AsyncSubagentExecution` carries `in_doubt_reason` (`submission_unobservable`, `multiple_provider_runs`, `graph_identity_mismatch`, `provider_binding_lost`) and `incident_id` exactly while in doubt, plus `result_output_text` (provider evidence, revealed to the parent model only after admission). `ACTIVE_ASYNC_SUBAGENT_LIFECYCLES` for EXEC-016.
- `AsyncSubagentContract` gains `graph_revision`, `graph_binding_digest` and `deployment_credential_ref` (a pattern-validated reference, never a value), all inside the contract digest (DA-019).
- `AsyncProviderCheckpointKey` (`belllabs.async-provider-checkpoint-key.v1`: endpoint, thread, `checkpoint_ns` as reported, checkpoint id, served graph id/revision/binding digest; canonical `ref`) and `AsyncSubagentUsage` (`belllabs.async-subagent-usage.v1`: `provider_attributed` | `pending` | `ambiguous`, attributed and pending amounts). `AsyncSubagentResultManifest` carries both typed fields; `checkpoint_ref` and `usage_ref` are validated to be their derived refs (DA-011). The CP-045 placeholder refs are gone.
- `ParentAsyncSubagentLink` gains `cancellation_receipt` (`provider_acknowledged` | `ambiguous`), `reconciliation_decision` and `adopted_provider_run_id`.
- `compile_deep_agent_execution_binding` (`materialization.py`) admits a remote placement only with `checkpoint_behavior=remote_managed` and `reconnect_behavior=remote_run_reconnect` (the hosted binding).

**Pure reconciliation rules (`app/domain/operation_execution/async_subagent_reconciliation.py`, new).** `AsyncServedGraphIdentity.matches(contract)`; `AsyncSpawnKeyObservation` and `classify_spawn_key_observation` (exactly one verified candidate binds; none is `no_provider_run`; more than one or a mismatching identity is `in_doubt`; runs BellLabs already cancelled through a decision are excluded from the candidates); `AsyncSubagentIncident` (`belllabs.async-subagent-incident.v1`, revisioned, resolved once by observation or by exactly one decision); `AsyncProviderRunRecord` (`belllabs.async-provider-run.v1`: `bound` / `duplicate_cancelled` / `orphaned_cancelled`, with usage); `lifecycle_for_provider_status`; `classify_async_children_for_fork` (`prohibited` / `snapshot_not_quiescent` naming the active children, else `quiescent`).

**Service (`app/application/async_subagents/service.py`).**
- CP-045 order kept: Mongo detail, PostgreSQL authority, then (new) the parent run's reservation, effect claim and child registration, all before any provider call.
- RRM-001 §7 #10 fixed: an `admitted` child without a binding is submitted on retry through the same fenced path; a submission error never marks it `orphaned`. The error is classified from provider state: no run → the child stays `admitted` and the error propagates (retryable); one verified run → bound; otherwise `in_doubt`. The feature gate forbids new children only; an existing child always resumes.
- RRM-001 §7 #11 fixed: submission is fenced per child (`acquire_submission_fence` with holder and lease; takeover only after expiry, advancing the fence; a live holder makes a concurrent submitter raise `AsyncSubagentSubmissionInProgress`). Inside the fence the adapter looks a run up by spawn key before creating one and asks the server to reject a concurrent run on the thread.
- DA-019: the served identity is verified before every first submission (`AsyncServedGraphMismatch` fails closed with no provider work) and re-verified from the stamped thread state on every observation and on result capture.
- `in_doubt`: `_enter_in_doubt` opens the incident (revision n+1 after a resolved one), mirrors the lifecycle to authority, records the fact and marks the parent's effect claim `ambiguous`. `reconcile` re-verifies the spawn key on every reconnect of every active lifecycle; observation resolves `in_doubt` to the single verified run, or to `orphaned` when no run exists (resolution `observation`). `reconcile_in_doubt` implements `adopt_provider_run(run_id)` (every other run cancelled, usage pending) and `orphan_child` (every run cancelled, usage pending, lifecycle `orphaned`), records the decision in the command ledger, resolves the incident once, and replays an exact resend idempotently; any other decision for a resolved incident is refused.
- `cancel` journals the intent first, reaches the provider run through the stock tool, and records `provider_acknowledged` or `ambiguous` (link and `cancellation` fact); it never assumes.
- `settle` settles the child's usage against the parent run exactly once; `decide_result` records a late rejection as a `result` fact `late_rejected:<digest>`.

**Parent budget effects (`app/application/async_subagents/parent_effects.py`, new).** `RunControlAsyncChildEffects` over `RunControlService`: `ReserveBudgetAction` (child reservation carved from the run's budget, sponsored by the parent operation's reservation), `ClaimEffectAction` with `effect_kind=async_subagent.child` and `operation_ref = parent binding_id` (so RRM-004's `unsettled_effect_ids` sees an unsettled or ambiguous child and the parent settles `in_doubt`, not `failed`), `RegisterAsyncChildAction` (terminalization blocks on required/degradable children), `ObserveEffectAction` + `RecordAsyncChildFactAction` per observation; at settlement `RecordUsageAction` (attributed amounts consumed, unused reservation released, pending amounts `pending_external`) plus `SettleEffectAction` only when nothing is pending. Commands are idempotent by identity and replay after a crash. Only the child's declared budget dimensions settle against the run; the manifest and provider-run records keep every reported amount.

**Adapter (`app/integrations/agents/deep_agents/async_subagents.py`).** `DeepAgentsAsyncSubagentAdapter` keeps the drift-checked stock 0.7.5 tool surface for check, update, cancel and list; resolves the deployment credential from its reference; `start` uses the BellLabs child id as the thread id, stamps `belllabs_child_execution_id`, `belllabs_parent_run_id`, `belllabs_parent_binding_id`, `belllabs_contract_digest`, `request_scope` and the spawn key on the run and uses `multitask_strategy="reject"`; `observe_spawn_key` lists the thread's runs with the spawn key and reads the stamped identity; a completed run yields the qualified checkpoint from `threads.get_state` and usage attributed from the hosted graph's `belllabs_provider_usage` stamps (message `usage_metadata` as a fallback; `pending` when nothing was reported); `cancel` polls the run for the acknowledgement. `BellLabsAsyncSubagentMiddleware` exposes the five stock tool names and schemas to the parent model; `start_async_task` builds a deterministic spawn request from the tool-call id (a resumed invocation rebuilds the same child), `check_async_task` reconciles and reveals the child's output only after admission, `list_async_tasks` reconciles each tracked child. The module evaluates annotations eagerly because `StructuredTool` recognises the injected `ToolRuntime` from the runtime annotation.

**Deep Agent adapter and materializer (coordinator seams, authorized).** `DeepAgentRuntimeAdapter(materializer, async_subagents=factory)` adds the governed middleware when the binding declares async contracts; `build_hosted_async_subagent_graph` compiles the hosted graph from the exact binding (`prepare(hosted=True)`: remote-managed placement, no local checkpointer or store, hosted context defaults from the frozen `cognitive_context_values` because the server invokes graphs without a runtime context) with `ServedGraphIdentityMiddleware`, which stamps `belllabs_served_graph` before the agent and `belllabs_provider_usage` after each model call. Both paths share the single `create_deep_agent` call (`_compile`), so `test_create_deep_agent_has_one_non_experiment_production_call_site` still holds. `OpenAIExactModelFactory` admits `stream_usage`.

**Hosting (`app/agent_server/async_subagents/`, new).** `bindings.py` builds the exact technical-child binding deterministically (profile `agent.async-technical-child@1`; model `gpt-5.6-luna` with `reasoning_effort=low`, `verbosity=low`, `use_responses_api`, `stream_usage`; one exact tool `wait_seconds` with its schema digest; state backend; base cognitive packs; `remote_langsmith_deployment` placement with remote-managed checkpoints) and exposes the `served` identity and `contract(agent_protocol_url=…)`; `graph.py` is the import-safe async graph factory (built once per process through the adapter); `auth.py` and `http_app.py` as above. The root `langgraph.json` is unchanged and graph-free. Block C files are untouched.

**PostgreSQL authority (`app/application/async_subagents/postgres_async_subagents.py`; migration 0021).** Submission fence (one conditional `UPDATE … RETURNING`), lifecycle mirror, provider-run records (an upsert that never demotes a cancellation back to `bound`), cancelled-run lookup, typed incidents in `runtime_reconciliation_incidents` under `incident_type='async_submission_in_doubt'`, decisions in the command ledger, `parent_binding_id` on the authority row (RRM-005's recommendation), and the read-only `list_children` lineage view (`AsyncChildLineageView`, `inspection.py`).

## Requirement-to-evidence map

| Requirement | Test → observed assertion |
|---|---|
| DA-008 reservation, link, authority and parent claim before submission; the real spawn path from `operation.execute` | Live `test_rrm_013_async_subagent_live.py::test_parent_deep_agent_spawns_one_real_child_and_admits_its_result` → the parent `OperationExecutionService.execute` (real `create_deep_agent`, scripted parent model whose only tool call is `start_async_task`) completes with `structured_output["spawned"]` = "Launched async subagent. task_id: <child>"; `list_children` shows one child whose `parent_binding_id` equals the parent's binding id (`d12c2317-…`), fence 1 released; run control holds the effect claim (`operation_ref` = parent binding, `effect_kind=async_subagent.child`) and the registered async child; the provider holds exactly one run with the spawn key, bound to the child. Offline: `test_async_subagent_submission_fence.py::test_parent_deep_agent_start_async_task_reserves_and_links_before_the_provider` → the fake provider's start sees the execution and reservation already recorded; one `sdk.runs.create`; the same tool-call id rebuilds the same child. CP-045 `test_spawn_persists_contract_link_and_reservation_before_provider_submission` → order `mongo → postgres → verify → start`, fence 1 released |
| DA-008 one provider run per child across the crash windows (RRM-001 §7 #10, #11) | Live `…::test_worker_killed_in_the_submission_window_recovers_with_one_provider_run[before_submit]` and `[after_submit]` (matrix below). Offline: `test_crash_before_submission_resumes_the_admitted_child_with_one_provider_run`, `test_crash_after_submission_before_observation_reconnects_to_the_same_run`, `test_live_fence_holder_blocks_a_concurrent_submitter_until_its_lease_expires` |
| DA-008 `in_doubt`, never a second spawn; exits by observation, `adopt_provider_run`, `orphan_child` | Live `…::test_duplicate_provider_run_is_in_doubt_until_adopt_provider_run` → a second run created with the spawn key makes `reconcile` return `in_doubt` / `multiple_provider_runs`; the incident row is `operator_required` with both candidates; the parent's effect claim is `ambiguous`; a repeated spawn stays `in_doubt` with two runs; `adopt_provider_run` binds the first run, the duplicate is cancelled (provider status `interrupted`; record `duplicate_cancelled`, usage `pending`), the incident resolves `adopt_provider_run`, the child completes with one bound run. Offline: `test_unobservable_submission_is_in_doubt_with_an_incident_never_orphaned` (exit by observation), `test_multiple_provider_runs_are_in_doubt_and_adopt_provider_run_cancels_the_rest` (unknown run refused; exact resend idempotent; one cancel), `test_orphan_child_cancels_every_run_and_records_their_usage_as_pending` (second decision refused) |
| DA-019 served identity before submission and on reconnect; dedicated config lists only bound async graphs; root config graph-free | Offline `test_served_graph_mismatch_fails_before_submission` (child stays `admitted`, no `sdk.runs.create`), `test_completed_thread_served_by_another_graph_is_in_doubt_not_admitted` (`graph_identity_mismatch`, no manifest), `test_contract_freezes_the_hosted_graph_identity_in_its_digest`; `test_async_subagent_server_offline.py::test_dedicated_config_lists_only_bound_async_graphs_and_root_stays_graph_free`, `::test_compose_generation_uses_external_postgres_and_env_file_references`, `::test_served_identities_are_the_exact_binding_digests`, `::test_hosted_graph_is_built_through_the_canonical_adapter_and_stamps_identity` (checkpointer `None`, identity and usage stamps, hosted context defaults), `::test_identity_route_requires_the_deployment_credential`; `test_agent_server_block_c_unit.py::test_root_langgraph_does_not_select_block_c_auth_or_graphs` unchanged. Live: the running server's identity route returns the exact digest the parent binds, and the manifest's `provider_checkpoint` carries it |
| DA-011 qualified checkpoint, attributed usage, admission, late results | Live spawn test → `provider_checkpoint` = `{graph_id: belllabs_async_technical_child, graph_revision: agent.async-technical-child@1, graph_binding_digest: sha256:764f0ef5…, thread_id: <child>, checkpoint_ns: "", checkpoint_id: 1f1be204-8b62-65c0-8005-cb7e31657d08}`; usage `provider_attributed` `{"tokens.total": 2263}`; `result_output_text` contains `PONG`; `decide_result(admit, parent_open=False)` raises "late or superseded" and the fact `late_rejected:<digest>` is recorded; `check` and `list` through the stock tools return `success`. Offline: CP-045 `test_completed_output_cannot_change_parent_before_explicit_admission_or_when_late`; `test_unreported_usage_is_recorded_pending_never_dropped` |
| RUN-009 child usage settles to the parent budget exactly once; pending stays pending | Live spawn test → after `settle`, `budget.usage_records["async-child-usage:<child>"].actual_amounts["tokens.total"] == 2263`, no pending amounts, the effect is `SUCCEEDED`, the lineage view shows `result_decision=admit`, the settlement ref and one `bound` provider run; a redelivered parent attempt returns the settled result with no second child and no second run. Offline `test_async_child_parent_budget.py::test_child_is_claimed_as_a_parent_effect_before_submission_and_usage_settles_once` (reservation 10 carved, consumed 7, released 3, effect settled with `usage_settlement_ref`), `::test_in_doubt_child_leaves_its_parent_effect_ambiguous_and_unsettled`, `::test_pending_usage_is_recorded_pending_and_blocks_effect_settlement` |
| DA-011 / EXEC-008 cancel reaches the provider run; acknowledgement or ambiguity recorded (RRM-008 hook) | Live `…::test_parent_cancel_reaches_the_provider_run_and_records_the_acknowledgement` → the running child's provider run is `interrupted` after the cancel, the link records `cancellation_receipt=provider_acknowledged`, lifecycle `cancelled`, the `cancellation` fact is recorded, one provider run, the child settles against the parent budget. Offline CP-045 `test_cancellation_is_authorized_before_provider_and_acknowledgement_recorded` |
| Agent Server restart during an active child | Live `…::test_agent_server_restart_during_an_active_child_is_reconciled_from_durable_state` (matrix below) |
| EXEC-016 fork admission classifies active children | Offline `test_fork_admission_classifies_active_children_as_not_quiescent` → `prohibited` / `snapshot_not_quiescent` naming the running child; `quiescent` after completion and for no children |
| RUN-011 child lineage read (RRM-005 consumer) | `test_async_subagent_postgres_authority.py::test_runtime_role_fences_submission_and_records_in_doubt_lineage` → `list_children` returns BellLabs identity, parent binding, provider thread and run, graph id/revision/binding digest, lifecycle, fence and holder, decision, result decision, settlement ref and provider-run records |
| Migration 0021 and least privilege | the same Postgres test under `SET ROLE belllabs_control_runtime`: fence 1 → blocked by the live lease → 2 after expiry → stale release ignored → 3; idempotent admit and incident open; decisions ledger `[admit, adopt_provider_run]`; a second resolution refused; forced RLS hides other scopes; `DELETE` on provider runs and facts raises `InsufficientPrivilegeError`. `test_run_control_postgres_integration.py` applies the chain through 0021 |
| CP-045 regression | `tests/acceptance/control_plane/test_wp_cp_045.py`: 11 passed (fixtures carry the new fields; the fake-client test is offline regression only and satisfies no gate) |
| Tracing (subordinate) | Live spawn test, `trace_correlation`: LangSmith project `BellLabsBiotech-AsyncSubagents-Local` holds the child's root run (`01a0fb09-e5b7-7f70-9e44-5ddd0b2a8c53`, the provider run id) whose metadata `belllabs_parent_binding_id` is the parent's binding id `d12c2317-2897-5aa5-a4fe-93417294c5f3` and `belllabs_child_execution_id` the child id. The parent's own root trace was not found by a `binding_id` metadata filter within 90 s (see Unresolved risks) |

### Crash-window and restart matrix (live; real Agent Server; the child calls the real model)

| Drill | Injection | Provider runs at the loss | Recovery | Provider runs after | Final child state | Record |
|---|---|---|---|---|---|---|
| Worker killed after BellLabs reservation, link, parent claim and fence, before provider submission (`before_submit`) | `CrashWindowProvider` stalls before `start`; the worker process (PID 42508) is killed | 0 (the thread did not exist) | worker 2 (PID 19184) retried the Activity attempt; attempts 2–8 stood down while the lost worker's 20 s unit lease ran, attempt 9 took the lease over, classified the parent `interrupted`, re-executed the tool call, found the child `admitted`, took the submission fence over (fence 2, the lost holder never released it) and submitted once | 1 (`01a0fb0e-ecd6-7cc2-bc86-e5629e90da29`) | `completed`, output `PONG`, admitted and settled | parent model calls `[(42508, 0 tool messages), (19184, 1)]`; `provider_runs_after_recovery: 1`; `submission_fence: 2` |
| Worker killed after provider submission, before the observation was applied (`after_submit`) | `CrashWindowProvider` stalls after `start` returned; worker PID 28628 killed | 1 (status unknown to BellLabs; child `admitted`, no binding) | worker 2 (PID 19184) as above; the spawn-key lookup found the existing run and bound it, no second run was created | 1 (`01a0fb0f-2cd9-7e73-90b0-9d78d42edb7a`, the same run) | `completed`, admitted and settled | model calls `[(28628, 0), (19184, 1)]`; `provider_runs_at_crash: 1`; `provider_runs_after_recovery: 1`; `submission_fence: 2` |
| Agent Server API container restarted while the child was running (`wait_seconds(45)` objective) | `docker restart rrm013-agent-server-langgraph-api-1`; `/ok` back after 11.7 s (11.6 s in the first run) | 1 (`running`) | the server resumed the run from its durable Postgres/Redis state; the parent reconciled through `reconcile` until terminal | 1 (`01a0fb10-f390-7e21-a27e-d9368cf1c160`, `success`) | `completed`, output `PONG`, recorded as a `result` fact | `provider_runs: 1`, `lifecycle: completed`, `restart_to_ready_seconds: 11.7` |
| Duplicate provider run (ambiguity a lost fence could leave) | a second run with the spawn key created directly on the server | 2 | `reconcile` → `in_doubt` / `multiple_provider_runs`, incident `4e9d9906-b91e-5ede-9af2-b80b6632980e` `operator_required`; `adopt_provider_run` cancelled the duplicate (`interrupted`) | 2 on the provider, 1 bound; the duplicate recorded `duplicate_cancelled` with pending usage and excluded from later classification | `completed` on the adopted run | incident `resolved` / `adopt_provider_run` |
| Parent cancel of a running child (`wait_seconds(60)`) | `service.cancel` → stock `cancel_async_task` → `runs.cancel` | 1 (`running`) | provider status `interrupted` within the acknowledgement poll | 1 | `cancelled`; receipt `provider_acknowledged` | `cancellation` fact `provider_acknowledged` |

Ambiguity never produced a second spawn: in every drill the child kept exactly one bound provider run, and the only child with two provider runs is the one whose duplicate was injected on purpose.

## Changed paths and migrations

Shared seams edited under the coordinator's RRM-013 authorization:
- `app/domain/operation_execution/contracts.py` (additive contract changes above).
- `app/domain/operation_execution/materialization.py` (remote placement admission).
- `app/integrations/agents/deep_agents/adapter.py` (governed middleware hook, hosted graph builder, the single `_compile` call site, `ServedGraphIdentityMiddleware` with identity and usage stamps).
- `app/integrations/agents/deep_agents/materializer.py` (`prepare(hosted=…)`, optional checkpointer/store for hosted graphs, hosted context defaults, `stream_usage`).
- `app/migrations/0021_async_subagent_submission_fence_v1.sql` (new).
- `app/domain/graph_runtime/identities.py`, `app/temporal/registration/*`, run-control reducer/service/repositories, root `langgraph.json`: **unchanged**.

Owned paths: `app/application/async_subagents/{service,postgres_async_subagents,parent_effects,inspection}.py`, `app/integrations/agents/deep_agents/async_subagents.py`, `app/domain/operation_execution/async_subagent_reconciliation.py`, `app/agent_server/async_subagents/*`, `langgraph.async_subagents.json`, `langgraph.async_subagents.env`.

Tests: new `tests/fixtures/fake_agent_protocol.py`, `tests/fixtures/rrm013_live_stack.py`, `tests/fixtures/rrm013_child_worker.py`, `tests/integration/agent_server/test_rrm_013_async_subagent_live.py`, `tests/integration/postgres/test_async_subagent_postgres_authority.py`, `tests/unit/integrations/test_async_subagent_submission_fence.py`, `tests/unit/operations/test_async_child_parent_budget.py`, `tests/unit/agent_server/test_async_subagent_server_offline.py`; updated `tests/acceptance/control_plane/test_wp_cp_045.py` (new contract fields and typed observations; the fake client moved to the fixture; the cancellation test additionally asserts the recorded receipt) and `tests/conftest.py` (selector loop for the live module).

**Migration `0021_async_subagent_submission_fence_v1.sql`** is forward-only and was the next free number (RRM-005 took 0022 in parallel; both apply in order). Schema identities: `belllabs.async-provider-run.v1`, `belllabs.async-subagent-incident.v1`. It adds the lifecycle mirror, provider binding, submission fence/holder/lease, in_doubt reason, incident and `parent_binding_id` columns to `async_subagent_authority` (with shape checks), widens `async_subagent_commands.command_kind` with `adopt_provider_run` and `orphan_child`, creates `async_subagent_provider_runs` (insert and update only, forced RLS, `belllabs_control_runtime` SELECT/INSERT/UPDATE, `belllabs_operations_readonly` SELECT) and adds inspection indexes. Nothing is renamed; the 0016 tables are reused (RRM-001 disposition row 54).

Deleted owners: none. The CP-045 placeholder refs are replaced by the typed key and usage.

## Deterministic verification

All commands ran from the worktree with `unset VIRTUAL_ENV`, one pytest process at a time, DSN runs under the stack lock. Tested head `9c1d344` unless noted.

| Command | Result |
|---|---|
| Owning offline suites: `pytest tests/acceptance/control_plane/test_wp_cp_045.py tests/unit/integrations/test_async_subagent_submission_fence.py tests/unit/operations/test_async_child_parent_budget.py tests/unit/agent_server/test_async_subagent_server_offline.py tests/acceptance/control_plane/test_wp_cp_040.py` | 51 passed |
| RRM-015 digest guard plus owning suites after the integration merge: `pytest tests/unit/control_plane/test_digest_set_order_guard.py tests/acceptance/control_plane/test_wp_cp_045.py tests/unit/integrations/test_async_subagent_submission_fence.py tests/unit/operations/test_async_child_parent_budget.py tests/unit/agent_server tests/acceptance/control_plane/test_wp_cp_040.py` | 141 passed, 19 skipped (Block C endpoint suites) |
| Postgres authority and migration chain, both DSNs: `pytest tests/integration/postgres/test_async_subagent_postgres_authority.py tests/integration/postgres/test_run_control_postgres_integration.py` | 2 passed |
| `uv run --no-sync ruff check app tests scripts` | All checks passed! |
| `uv run --no-sync mypy app` | Success: no issues found in 357 source files |
| Hermetic full pytest: `BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q -rs` | 870 passed, 63 skipped, 2 xfailed, 0 failed (118 s). Integration `d46f548`: 850 passed, 56 skipped, 2 xfailed. Delta +20 passed (12 `test_async_subagent_submission_fence.py`, 3 `test_async_child_parent_budget.py`, 5 `test_async_subagent_server_offline.py`), +7 skipped (6 live-gated RRM-013 cases, 1 service-gated Postgres authority test), 0 failed |
| Full pytest with `TEST_APPLICATION_POSTGRES_DSN`, `TEST_MONGODB_URI` and `--env-file ../biotech-research-ingestion-evaluation-system/.env` | 903 passed, 30 skipped, 2 xfailed, 0 failed (223 s). Integration `d46f548`: 882 passed, 24 skipped, 2 xfailed. Delta +21 passed (the 20 above plus the Postgres authority test), +6 skipped (the live-gated RRM-013 cases). Remaining skips: 17 Block C endpoint, 6 RRM-013 live, 3 live-provider flags, 1 WSL-only BP-010 recovery, 1 pre-existing retirement, 2 Block C restart/N+1 drills |
| `git diff --check a9c3f1d HEAD` | working tree clean; across the range the only finding is `CLEANUP.md:45: new blank line at EOF`, which arrived with the CR-1 integration merge (`11f8ac4`), not with this ticket |

Baselines: integration `a9c3f1d` hermetic 748 passed / 54 skipped / 2 xfailed; services 778 / 24 / 2. Integration `d46f548` (RRM-005 and RRM-015 merged) raised both counts; the delta of this ticket against the merged tree is the new RRM-013 tests listed above (offline: 1 + 11 fence/in-doubt + 3 parent-budget + 5 hosting; service-gated: 1 Postgres authority; live-gated: 6, skipped unless opted in) and the 11 CP-045 tests (one renamed: `test_cancellation_is_authorized_before_provider_and_reconciled` → `…_and_acknowledgement_recorded`, strengthened). No test was skipped, xfailed, deselected or weakened by this ticket; the live module skips only without its opt-in flag, like the other live suites.

## Live runtime qualification

Opt-in: `BELLABS_RUN_RRM_013_LIVE=1`, `AGENT_SERVER_ENDPOINT=http://127.0.0.1:8143`, `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`, `TEST_APPLICATION_POSTGRES_DSN`, `TEST_MONGODB_URI`, `LANGSMITH_TRACING=true`, `LANGSMITH_PROJECT=BellLabsBiotech-AsyncSubagents-Local`, with `LANGSMITH_API_KEY` from the main `.env` (loaded into the process by a scratch launcher, never printed). Command, under the stack lock: `uv run --no-sync pytest -q -s tests/integration/agent_server/test_rrm_013_async_subagent_live.py`.

| Run | Head | Result |
|---|---|---|
| 1 (full module) | `2b74407` | 3 passed (cancel, server restart, `after_submit`), 3 failed: usage `pending` (the server's serialized message omits `usage_metadata`), the adopted child re-entered `in_doubt` (the cancelled duplicate still carries the spawn key), the `before_submit` helper raised 404 on a never-created thread. Each was a defect fixed in `888f196` (usage stamping in the hosted graph; cancelled duplicates excluded from classification; the helper treats 404 as zero runs) |
| 2 (full module) | `5e4a0da` | 3 passed (spawn, `before_submit`, `after_submit`), 3 failed because the cancel, in-doubt and restart drills reused fixed binding ids and therefore reconnected to run 1's durable provider threads (the lookup-before-create rule behaving as designed; a harness defect fixed in `9c1d344`) |
| 3 (the three drills) | `9c1d344` | 3 passed (cancel, in-doubt/adopt, restart) |

Every test of the module passed at least once against the real server at head `5e4a0da`/`9c1d344` with the code under test unchanged between those heads except the drill identities. Sanitized evidence records printed by the tests (identities vary per run):

```text
RRM-013 EVIDENCE spawn: {"child_execution_id": "581295f1-8577-5458-988e-df144a957470",
  "elapsed_seconds": 102.8, "graph": {"agent_protocol_url": "http://127.0.0.1:8143",
  "checkpoint_id": "1f1be204-8b62-65c0-8005-cb7e31657d08", "checkpoint_ns": "",
  "graph_binding_digest": "sha256:764f0ef514bde5ae1428f409d66ab35c67808e86d4db3667ef8a318c278199bd",
  "graph_id": "belllabs_async_technical_child", "graph_revision": "agent.async-technical-child@1",
  "thread_id": "581295f1-8577-5458-988e-df144a957470"},
  "manifest_digest": "sha256:7227956972ab0b16f221f230a19d934014d9149bcfe518dea559078029040786",
  "parent_binding_id": "d12c2317-2897-5aa5-a4fe-93417294c5f3", "parent_model_calls": 2,
  "provider_run_id": "01a0fb09-e5b7-7f70-9e44-5ddd0b2a8c53", "provider_runs": 1,
  "trace_correlation": {"available": true, "child_parent_binding_ids": ["d12c2317-2897-5aa5-a4fe-93417294c5f3"],
  "child_root_runs": ["01a0fb09-e5b7-7f70-9e44-5ddd0b2a8c53"], "parent_root_runs": [],
  "project": "BellLabsBiotech-AsyncSubagents-Local"},
  "usage": {"attributed_amounts": {"tokens.total": 2263}, "attribution": "provider_attributed", "pending_amounts": {}}}
RRM-013 EVIDENCE cancel: {"cancellation_receipt": "provider_acknowledged", "lifecycle": "cancelled",
  "provider_run_id": "01a0fb10-6703-70e2-b160-67d2e9830ba3", "provider_status_after_cancel": "interrupted"}
RRM-013 EVIDENCE in_doubt_adopt: {"adopted_run_id": "01a0fb10-7465-7271-8d51-4a7662fca0bf",
  "duplicate_run_id": "01a0fb10-7515-7021-ab33-297043c23620", "duplicate_status_after_adopt": "interrupted",
  "final_lifecycle": "completed", "incident_id": "4e9d9906-b91e-5ede-9af2-b80b6632980e"}
RRM-013 EVIDENCE server_restart: {"lifecycle": "completed", "provider_run_id": "01a0fb10-f390-7e21-a27e-d9368cf1c160",
  "provider_runs": 1, "provider_status": "success", "restart_to_ready_seconds": 11.7}
RRM-013 EVIDENCE crash_window_before_submit: {"model_calls": [{"pid": 42508, "tool_messages": 0}, {"pid": 19184, "tool_messages": 1}],
  "provider_run_id": "01a0fb0e-ecd6-7cc2-bc86-e5629e90da29", "provider_runs_after_recovery": 1,
  "provider_runs_at_crash": 0, "recovery_attempt": 9, "submission_fence": 2}
RRM-013 EVIDENCE crash_window_after_submit: {"model_calls": [{"pid": 28628, "tool_messages": 0}, {"pid": 19184, "tool_messages": 1}],
  "provider_run_id": "01a0fb0f-2cd9-7e73-90b0-9d78d42edb7a", "provider_runs_after_recovery": 1,
  "provider_runs_at_crash": 1, "recovery_attempt": 9, "submission_fence": 2}
```

Parent cognition is the deterministic `ParentSpawnModel` (no model spend). The child is the hosted technical child on `gpt-5.6-luna` with tiny objectives ("Reply with exactly the word PONG." or "Call wait_seconds with seconds=N, then reply with exactly PONG."). The run's `multitask_strategy` is `reject`.

**Spend.** Across the whole ticket the Agent Server recorded 13 child threads and 15 provider runs (10 `success`, 4 `interrupted` by cancellation or duplicate handling, 1 `error` from the pre-fix hosted-context defect). The five threads created after usage stamping landed total 15,961 tokens (stamped); the earlier eight are estimated at a similar 2,000–3,500 tokens each, so the ticket's child usage is roughly 36,000 tokens, plus six local probe calls of about 20 tokens each. At current frontier pricing this is well under USD 1; the USD 10 threshold was never approached, so no checkpoint was triggered.

## Replay and recovery artifacts

| Command | Result |
|---|---|
| `TEST_APPLICATION_POSTGRES_DSN=<disposable> pytest tests/integration/temporal/test_wp_bp_010_temporal.py tests/integration/temporal/test_wp_bp_020_temporal.py tests/integration/temporal/test_linked_runs.py tests/unit/operations/test_operation_execution.py -k "replay or Replayer or replays or routes_bound or parked"` | 6 passed, 53 deselected (47.7 s): the StageGraph any-join, GoalDirected separate-children/Continue-As-New and cancellation histories, the cross-queue `OperationWorkflow`, the parked in_doubt history, and the RRM-004 worker-restart proof (worker 1 killed, worker 2 settles once) |

No Temporal workflow, activity, signal or payload changed in this ticket; the recovery artifacts are the live crash-window and restart records above, and the RRM-004 lease/fence mechanics they exercised (attempts 2–8 standing down behind the lost worker's lease, attempt 9 taking over at claim fence 2).

## Replacement and deletion checks

- `AsyncSubagentService.spawn` no longer returns an admitted child idle and no longer marks a submission error `orphaned` (RRM-001 §7 #10 resolved).
- `DeepAgentsAsyncSubagentAdapter.start` is fenced by the service and looks up before creating; usage and checkpoint refs are real (RRM-001 §7 #11 resolved).
- No competing lifecycle store: the child's authority stays in PostgreSQL (0016/0021), the parent's budget and effect authority in run control, Mongo holds detail, the Agent Server holds only its own threads and runs. The Agent Server registers no BellLabs root, family or operation-workflow graph (ADR-0003); the dedicated config lists exactly the hosted async graphs.
- Block C stays qualification topology; nothing of it was revived. `langgraph.block_c*.json` and `tests/fixtures/agent_server_block_c.py` are untouched.

## Unresolved risks and drift checks

- **Parent-side trace lookup (subordinate evidence).** The child's LangSmith root run carries the parent's binding id and the child id in its metadata, so the correlation from child to parent is recorded. The parent's own root trace was not found by a `has(metadata, {"binding_id": …})` filter within 90 s in the live run; either ingestion lagged or the parent's `trace_deep_agent_execute` metadata is recorded under a different project/key for this harness. Traces are subordinate evidence (ticket), so this is recorded, not asserted. RRM-009's production composition should fix one project for parent and child traces and re-check the parent filter.
- **Provider usage attribution depends on the hosted graph's stamp.** The Agent Server's serialized thread messages omit `usage_metadata`, so the hosted graph records usage in `belllabs_provider_usage`; a graph without the stamp yields `pending` usage (never zero, never dropped). Any future hosted graph must be compiled through `build_hosted_async_subagent_graph` to keep the stamp.
- **Deterministic child identities are durable on the provider.** A child id derives from the parent binding and the tool-call id, and the server keeps threads durably, so re-admitting the same BellLabs identities reconnects to the earlier provider thread. That is the intended lookup-before-create rule; test harnesses (and RRM-010's smoke) must mint unique parent identities per drill, as the live module now does.
- **`interrupted` is overloaded on the provider.** The Agent Protocol reports both a cancelled run and a HITL interrupt as `interrupted`. The adapter maps it to `cancelled` only when BellLabs requested the cancellation (the receipt poll) and to `waiting` otherwise. HITL is out of scope; RRM-008 should keep this distinction when it builds the cross-family cancellation proof.
- **Lease timing in the crash drills.** The parent unit's Activity lease was 20 s and the submission fence lease 20 s; recovery needed 9 Activity attempts of stand-down polling. Production leases come from Temporal's start-to-close timeout (RRM-004); the fence lease default is 120 s (`DEFAULT_SUBMISSION_LEASE`).
- **Hosted model settings.** The technical child uses `use_responses_api` and `stream_usage`; `OpenAIExactModelFactory` now admits `stream_usage` for every exact OpenAI model (an additive allow-list change).
- **External gates still unrun by this ticket:** Block C endpoint suites and the other live provider suites, as before.
- **No new ticket was needed.** No out-of-scope defect was found; RRM-016 stays free.

What RRM-005 must know: the authority row now carries `parent_binding_id` (0021) in addition to the Mongo detail, so unit attribution can use the authority alone; `AsyncChildLineageView` (`app/application/async_subagents/inspection.py`) and `PostgresAsyncSubagentAuthority.list_children` expose provider thread/run, graph identity, fence, decision and provider-run records read-only; incidents of async children sit in `runtime_reconciliation_incidents` under `incident_type='async_submission_in_doubt'` with the child id in the payload.

What RRM-006 must know: `classify_async_children_for_fork(children)` (pure) returns `prohibited` / `snapshot_not_quiescent` with the active child ids for any child in `ACTIVE_ASYNC_SUBAGENT_LIFECYCLES` (`admitted`, `submitted`, `running`, `waiting`, `in_doubt`); the fork admission saga should call it with the parent run's children (from `list_children`) and never copy children implicitly.

What RRM-008 must know: `AsyncSubagentService.cancel` journals the intent, reaches the provider run through the stock `cancel_async_task`, and records `provider_acknowledged` or `ambiguous` on the link and as a `cancellation` fact; `cancel_run` cancels a named run and records pending usage; the acknowledgement poll is 10 × 0.5 s. The cross-family cancellation saga should call `cancel` for every active child of a unit before terminalizing, then `settle` each terminal child (nonblocking children settle without a result decision; blocking ones need `reject` or `admit` first). The parent's effect claim of an unsettled child is what makes the parent `in_doubt` (RRM-004 narrowed rule).

What RRM-009 must know (production composition): construct `AsyncSubagentService(MongoAsyncSubagentDetailRepository(), PostgresAsyncSubagentAuthority(pool), DeepAgentsAsyncSubagentAdapter(secrets=resolved, request_scope=scope), parent_effects=RunControlAsyncChildEffects(run_control, actor=…), allow_new_spawns=settings.async_subagent_spawning_enabled, submitter_identity=<deployment-stable worker identity>)` per operation and pass an `AsyncSubagentMiddlewareFactory` to `DeepAgentRuntimeAdapter(materializer, async_subagents=…)`; the parent's `secret_refs` must include the deployment credential reference; the hosted contract is `technical_child_definition().contract(agent_protocol_url=settings.agent_server_endpoint, budget_limits=…)` with limits inside the run's declared budget dimensions; keep the Agent Server stack described above running or re-create it with the documented commands.

### Reusable seams (mission-horizon lens)

- `classify_spawn_key_observation`, `AsyncSubagentIncident`, `AsyncProviderRunRecord`: a provider-neutral "exactly one remote run per child" rule with typed ambiguity and decisions.
- `acquire_submission_fence` / `release_submission_fence`: a per-child submission lease with fence takeover, the same shape as RRM-004's claim lease.
- `RunControlAsyncChildEffects`: any child work item becomes a reservation, an effect claim and a registered child of its parent run, settled exactly once.
- `AsyncServedGraphIdentity` + `ServedGraphIdentityMiddleware` + the identity route: served-identity verification for any hosted graph.
- `build_hosted_async_subagent_graph` / `prepare(hosted=True)`: exact-binding hosting on any Agent Protocol server.
- `BellLabsAsyncSubagentMiddleware`: a governed replacement of a framework tool surface that keeps the framework's names and schemas.
- `FakeAgentProtocolClient`, `CrashWindowProvider`: offline regression and crash injection at the provider port.

None of these carries company, fixture or provider-model specifics; the only provider-specific code is the Agent Protocol SDK usage inside the adapter and the OpenAI model factory setting.

## Final disposition

implemented; independent review pending
