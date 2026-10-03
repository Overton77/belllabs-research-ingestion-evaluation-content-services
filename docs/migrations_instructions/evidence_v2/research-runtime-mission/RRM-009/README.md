# RRM-009 implementation evidence

Disposition: ready_for_review (implemented; independent review `approve_with_fixes`, every fix made in new commits, see "Review disposition"; CP-050 is **not** accepted by this ticket, it only qualifies the CP-050 capability prerequisite)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-009 qualify the CP-050 capability prerequisite: a production-shaped composition. Requirements: DA-001 to DA-015 and the capability-binding requirements, REQ-CP-DA-004 (persistent saver, clarified), REQ-CP-DA-007 (subordinate usage charged to the parent), REQ-CP-DA-016 (`durability="sync"`), REQ-CP-DA-019, REQ-CP-EXEC-015 (Search Attribute registration and verification), REQ-CP-RUN-011/012 (inspection composition), RRM-007 F6 (boundary delivery relay), RRM-013 N8 (scope-bound Agent Server credential) (AMD-RRM-001, accepted meta `main` `a50d833`).
Base revision and head revision: base `c8221f5` (integration: RRM-016 hardening, RRM-018, RRM-019). Integration `56ffd63` (RRM-008 accepted) was merged in at `dfe33ad` (no rebase). **Tested code head `6990a8d`** (the independent review's fixes; see "Review disposition" for its gates). The integration gates ran on `65bf532`; the earlier gate tables below are history on `6d956e9`. The commit that updates this README, the ticket and the index changes documentation only. Branch `wp/rrm-009-capability-composition`; not merged, not pushed (the coordinator owns review and merge).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12; temporalio 1.30.0 (`start_local` dev server with a database file), deepagents 0.7.5, langgraph 1.2.10, langgraph-checkpoint 4.1.1, langgraph-checkpoint-postgres 3.1.1, langgraph-sdk 0.4.2, langchain-mcp-adapters 0.3.1, pydantic 2.13.4, pymongo 4.17.0, beanie 2.1.0, asyncpg 0.31.0, psycopg 3.3.4, pytest 8.4.2, ruff 0.15.22, mypy 1.20.2. Agent Server: `langchain/langgraph-api` 0.12.0 image built from this worktree (`langgraph up --config langgraph.async_subagents.json --api-version 0.12.0 --no-pull`), served graph `belllabs_async_technical_child`, binding digest `sha256:764f0ef5…`. Model: `gpt-5.6-luna`, pinned model definition `model.wp-cp-040` (reasoning low, verbosity low, Responses API, max 2000 completion tokens). Pinned capabilities (`infra/capability-pins/research-capabilities.json`, schema `belllabs.capability-pins.v1`): skill `skill.agent-browser` (`sha256:30722859…`), tool `tool.agent-browser-page` (agent-browser 0.33.0), MCP `mcp.tavily` (tavily-mcp 0.2.21, 5 tool schemas) and `mcp.firecrawl` (firecrawl-mcp 3.22.4, 26 tool schemas), checkpointer/store/sandbox/model refs of WP-CP-040.

## Worktree provenance

- Worktree `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-009`, branch `wp/rrm-009-capability-composition`, from integration `c8221f5`. The main checkouts were not touched. `.env` was never copied or printed: service and live gates load it with `uv run --env-file ../biotech-research-ingestion-evaluation-system/.env`; the Agent Server secret and its Postgres password live in a scratch env file outside the repository and were never printed or committed.
- Disposable shared stack: `rrm-app-postgres` (`127.0.0.1:55432/belllabs`) and `rrm-app-mongodb` (`127.0.0.1:27017`), serialized by the shared stack lock (`stack_lock.py acquire/release RRM-009`). One pytest process at a time. The demonstrations create the saver schema `rrm009_langgraph` and per-test Mongo databases `rrm009_<12 hex>`. One lock violation happened (see "Incident" below).
- RRM-009's own Agent Server stack (needed because this branch's auth is a signed scope claim; the `rrm013-*` server still runs the pre-RRM-009 static-token auth and answers a claim with 401): compose project `rrm009-agent-server` (`rrm009-agent-server-langgraph-api-1` at `http://127.0.0.1:8144`, `rrm009-agent-server-langgraph-redis-1`) and `rrm009-agent-server-postgres` (`127.0.0.1:55434`, volume `rrm009-agent-server-pgdata`). Left running for RRM-010. The `rrm013-*`, `rrm-app-*` and `csi01` containers were never stopped or removed.
- Migration slot 0025 is used by this ticket (`0025_operation_claim_unit_key_runtime_grant_v1.sql`, grant only); RRM-008 adds no migration. The fourth session adds slot 0026 (`0026_family_writer_terminal_boundary_receipts_v1.sql`, grant only).
- Fourth session: every DSN, `start_local`, container and live command acquired the stack lock and proceeded only on `acquired`; no lock violation. Container `rrm009-minio` (volume `rrm009-minio-data`) existed only for the S3 qualification and was removed by exact name with its volume and image.

### Handoff note (four sessions)

1. **First implementer** (`9ede5cd`, `82b3489`, `5d96c83`): the deployment composition, governed launch, scope-claim auth and pins. Its acceptance test did not pass and its last commit was a WIP preserve at handoff. The second implementer treated all three as an unreviewed draft.
2. **Second implementer** (`6cec836` to `6d956e9`): audited the draft, fixed more than ten defects forward (no amend, see "What the draft got wrong"), added the missing pieces (sanitized lineage, sync-child usage, parent-boundary async completion, grant-scoped egress, live qualification, SET ROLE test, launch baseline check), ran every gate on `2f3a642`, and wrote the runbook and tickets RRM-020/RRM-021 (`9e2e038`). At handoff it committed two Visibility assertions that it could not run because RRM-008 held the stack lock (`6d956e9`).
3. **Third session**: ran the StageGraph qualification with `6d956e9`'s assertions under the lock (passed, no fix needed), re-ran the full gate set on `6d956e9`, dropped the Mongo database the incident leaked, and wrote this evidence and the ticket/index updates. No code changed in this session.
4. **Fourth session** (integration): merged integration (`56ffd63`, RRM-008) and wired every RRM-008 item into the production composition. It fixed three seam defects that the production drills found, proved ticket items 1 (S3) and 6 (subagent cancellation and reconnect), and re-ran every gate. See "Integration with RRM-008".

## Implemented contracts and seams

**Worker composition (`app/temporal/deployment_composition.py`, `app/temporal/worker.py`).** `ProductionWorkerActivityCompositionFactory.build` composes the real application PostgreSQL (runtime login, family-writer login), Mongo immutable definitions and operation-execution bindings, the content-addressed payload store (filesystem CAS under `ARTIFACT_PAYLOAD_ROOT`, or S3 with `S3_BUCKET`), the registered persistent `AsyncPostgresSaver`/store, `compose_postgres_operation_recovery(pool, run_control, nudge=TemporalUnitReconciliationNudge, verifier=LangGraphCheckpointDescendantVerifier)`, `RunControlOperationAuthority`, the deployment-stable `OPERATION_JOURNAL_CLAIMED_BY`, the per-operation `AsyncSubagentService` and middleware factory, `ForkReuseResolver`, `compose_goal_directed_activities(operation_bindings=…)` and `MongoGoalDirectedDocumentRepository`. `create_production_workers` serves the canonical registries on `TEMPORAL_TASK_QUEUE`-derived queues (root, family, agent-cognitive, generic-artifact via `generic_artifact_task_queue`); with `COORDINATOR_LAUNCH_ENABLED` the worker builds the factory itself and calls `verify_belllabs_search_attributes` before opening any store (readiness never mutates the namespace). `DeploymentOperationRuntime` decorates the adapter: it binds the operation's granted `network_hosts` (ContextVar `granted_network_hosts`) around cognition and completes async children at the parent boundary afterwards.

**API composition (`app/api/runtime_composition.py`, `app/server.py`).** `compose_runtime_control` (lifespan, `RUN_CONTROL_TEMPORAL_ENABLED=1`) verifies the Search Attributes and attaches on `app.state`: `temporal_visibility_reader`, `inspection_checkpoint_reader` (`LangGraphCheckpointHistoryReader` over the registered saver), `inspection_async_child_details` (Mongo), `inspection_cursor_key` (settings key, or HMAC-derived from the checkpoint signing key, so every replica agrees), `boundary_command_transport` (`TemporalBoundaryCommandTransport`), `unit_reconciliation_nudge` and `unit_reconciliation_verifier`, the fork services with `fork_patch_policies`, and the governed launch. `BoundaryCommandRelay` (`app/application/run_control/boundary_relay.py`) redelivers pending commands in order for `BOUNDARY_RELAY_REQUEST_SCOPES` every `BOUNDARY_RELAY_INTERVAL_SECONDS` (it reads `runs_with_pending_boundary_commands` under the runtime role and RLS).

**Governed launch (`app/application/run_control/run_launch.py`, `POST /run-control/v1/runs/{run_id}/launch`).** Starts only an input bound to the admitted run, verifies the baseline reservation recorded at admission, starts a fork-derived root with `parent_run_id` only after the fork's materialization (`fork_not_materialized`, retryable) and applies semantic-input patches to derived templates (`StageGraphForkTemplateDerivation`, `app/application/orchestration/fork_templates.py`). Production starters use `TemporalWorkflowSubmitter.for_production(..., search_attribute_policy="required")` (also `app/application/runners/web_research_coordinator_live.py`). The smoke harnesses `schema_grounding_smoke.py` and `web_research_smoke.py` keep plain submitters: they are test-environment harnesses, not production starters.

**Capabilities (`app/integrations/capability_pins.py`, `scripts/pin_research_capabilities.py`).** `CapabilityPins` verifies every mounted skill bundle, tool, MCP module and per-tool schema against the pins file before materialization (drift is refused). MCP servers run as worker-side stdio subprocesses of the pinned module; the browser (`app/integrations/agents/deep_agents/browser_tool.py`) is the pinned agent-browser subprocess with per-call single-host `--allowed-domains`, public hosts by name only (IPv4/IPv6 literals and private hosts refused), and only hosts the operation was granted. Docker sandbox network isolation is untouched; the sandbox stays the state backend.

**Lineage and usage (`app/integrations/agents/deep_agents/capability_lineage.py`, `adapter.py`).** `capability_lineage` records credential references by name, MCP tool filters and schema digests, tool and skill digests, placement (task queue, package versions) and every invocation with argument/result digests, status and observed per-call tokens. It is persisted in the journal's digest-bound output payload (`journaled_operation_execution.py` now carries `event_payloads` and `_restore` re-attaches them; before, the journaled path dropped every runtime event payload). `_ModelCallObserver` charges sync-subagent model calls to the parent's usage (REQ-CP-DA-007; they were uncounted).

**Async children (`app/application/async_subagents/parent_completion.py`).** `AsyncChildCompletion` admits a completed blocking child at the parent operation boundary under a registered result policy (`admit_typed_manifest`, `policy:async-result:technical-child@1` in the qualification), settles its usage once, rejects a failed child (unattributed usage stays pending) and leaves an active child or an unregistered policy undecided.

**Agent Server credential (`app/agent_server/async_subagents/auth.py`, `http_app.py`, `deep_agents/async_subagents.py`).** `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` is an HMAC secret; the parent mints a short-lived claim per request scope (`mint_scope_claim`), the server verifies it (`verify_scope_claim`) and refuses a header scope that differs from the claim. The raw secret is not an accepted bearer. Hosted-child tracing is opt-in (`LANGSMITH_TRACING`, default off).

**Workspaces (`app/domain/operation_execution/materialization.py`).** `slot_ownership_boundary`: a GoalDirected role root (`/goal/{n}/{role}`) is the ownership boundary, so the executor and verifier of one iteration own disjoint roots; other paths keep the two-component rule. Both workspace repositories use it. `WorkspaceCandidateCaptureService` + `ArtifactPromotionService` (`GenericArtifactWorkflow`) promote captured report slots durably.

### Harvested "RRM-009 must compose" checklist (from accepted evidence READMEs)

| From | Item | Status and proof |
|---|---|---|
| RRM-003 | `lineage = CheckpointLineageService(PostgresCheckpointLineageRepository)` with the persistent saver | composed via `compose_postgres_operation_recovery(...).lineage`; the technical StageGraph run writes saver checkpoints in `rrm009_langgraph` (asserted > 0; 45 observed) and inspection serves their history `current` (at least 4 entries) |
| RRM-004 | `compose_postgres_operation_recovery(pool, run_control, nudge=TemporalUnitReconciliationNudge(client), verifier=LangGraphCheckpointDescendantVerifier(registered savers))`; registered persistent `AsyncPostgresSaver`; deployment-stable `journal_claimed_by`; `RunControlOperationAuthority`; Mongo OEB binding store | composed in `ProductionWorkerActivityCompositionFactory.build` (`OPERATION_JOURNAL_CLAIMED_BY`); both technical families settle through the journal under the runtime login (needed migration 0025) |
| RRM-005 | Visibility reader, checkpoint reader, async-child detail, shared cursor key on `app.state`; `for_production(required)`; register (admin) and verify (readiness) | composed in `compose_runtime_control`; StageGraph inspection sections all `current`; `scripts/register_belllabs_search_attributes.py`; worker readiness test; the API refuses to compose before registration (qualification) |
| RRM-006 | `compose_run_fork_services` with `app.state.fork_patch_policies`; `ForkReuseResolver(PostgresForkMaterializationStore, results=payloads, bindings=Mongo OEB)` as `fork_reuse`; fork root started with `parent_run_id` only after materialization; semantic-input patches applied to derived templates | composed; StageGraph fork through `/snapshots` + `/forks` + `/launch`; `BellLabsParentRunId` lists exactly 1; `test_fork_templates.py`, `test_run_launch.py::test_fork_derived_run_waits_for_materialization_and_binds_the_fork_reference` |
| RRM-007 | `<family>.apply_boundary_command` via `create_routed_coordinator_activities`; transport + relay + nudge + verifier; drill | composed; relay drill (pause accepted with Temporal down → `[accepted, delivered, applied]`) |
| RRM-013 | per-operation `AsyncSubagentService(Mongo detail, Postgres authority, adapter(secrets, scope), RunControlAsyncChildEffects, allow_new_spawns, submitter_identity)` + `AsyncSubagentMiddlewareFactory`; parent `secret_refs` include the credential ref; hosted contract within run budget; N8 scope-bound credential; tracing opt-in; `settle_pending_usage` drain note | all composed and qualified live; drain note in the runbook. **Not done:** the "fix one LangSmith project and re-check the parent filter" item (tracing was off in every RRM-009 live run) |
| RRM-015 | digest guard | new digests use `sha256_digest` / `contract_fingerprint` only; the guard passes (it caught `browser_tool.args_schema: type[BaseModel]`, fixed by a narrowed annotation) |
| RRM-016 | `compose_goal_directed_activities` with mandatory `operation_bindings` (→ `RunControlGoalOperationSettlements(run_control, bindings)`); production templates carry compiled slots; drain note | composed; 2-iteration GoalDirected run on the composition |
| RRM-018/019 | `MongoGoalDirectedDocumentRepository` as production `documents`; only final-executor outputs promoted | composed; 2-iteration run on Mongo; promoted outputs asserted to be the final executor's only |

## Integration with RRM-008

Fourth session (2026-10-02, evening): integration `56ffd63` (RRM-008 accepted, merged there at `027cc42`) was merged into this branch with `git merge integration/research-runtime-mission` (no rebase) at `dfe33ad`; the RRM-008 items were wired into the production composition and ticket items 1 and 6 were proven. Commits after the merge: `2f6f4c2` (composition wiring), `59bc928` (terminal receipts of a family-admitted terminalization, migration 0026), `9d65432` (cancellation drills), `cc8778a` (StageGraph saga under a child's pending usage), `89db1a5` (drill proves the hint wake), `c495aea` (S3 qualification), `4667175` (repository runbook), `65bf532` (worker-restart reconnect case), then this documentation commit.

### Merge and conflict resolution

Only two files conflicted textually; both were resolved by keeping both sides:

- `tests/conftest.py`: both tickets' acceptance modules stay on the psycopg selector loop (`test_rrm_009_production_composition.py`, `test_rrm_009_live_capabilities.py`, `test_rrm_008_cancellation_demo.py`).
- `RESEARCH_RUNTIME_MISSION_TICKETS.md`: the RRM-008 row from integration (accepted), the RRM-009 row from this branch.

`adapter.py`, `journaled_operation_execution.py`, `operation_execution.py`, `deployment_composition.py` and `worker.py` merged without textual conflicts (RRM-008 and RRM-009 touched disjoint regions; `agent_cognitive_activities` already returned `(execute, cancel)` and `create_agent_cognitive_worker` registers both). The semantic integration is the wiring below. Right after the merge, ruff and mypy passed and the owning suites of both tickets passed: hermetic `tests/unit/{run_control,operations,orchestration,integrations,workspaces,agent_server} tests/integration/temporal` **507 passed, 21 skipped, 1 xfailed**; DSN (lock) RRM-009 production qualification, RRM-008 demos, RRM-009 grants, digest guard, coordinator runtime, WP-CP-040/045 **124 passed, 1 skipped** (the RRM-008 live case).

### The RRM-008 items, wired and proven

| Item | Wiring | Proof |
|---|---|---|
| `operation.cancel` on the cognitive and generic-artifact workers | Cognitive worker: RRM-008's `(execute, cancel)` surface. Generic artifact worker: `generic_artifact_activities` = the cognitive pair + `artifact.promote` (`app/temporal/artifact_activities.py`). | `tests/unit/operations/test_rrm_009_cancellation_composition.py::test_generic_artifact_worker_serves_the_cancel_beside_the_execute`; every production drill schedules `operation.cancel` on `rrm009-agent-cognitive` and the production worker set serves it. |
| Credential-resolving port so the cancel path can cancel async children | `ProductionAsyncChildCancellation` (`app/temporal/deployment_composition.py`): lists the parent binding's children from PostgreSQL authority; only if there are any, resolves the binding's `secret_refs` with the same `EnvironmentSecretResolver` cognition uses, builds the parent's governed `AsyncSubagentService` with `ProductionAsyncSubagentMiddlewareFactory.service(binding, secrets)` and calls `cancel_children`. Passed as `OperationExecutionService(children=…)`. | `…::test_child_cancellation_resolves_the_operation_credential_only_when_children_exist` (no child: no secret resolved, no provider service); live drills (child cancelled at the provider with the scope-bound claim, `provider_acknowledged`). |
| The relay delivers `cancel` in its own sequence space | `runs_with_pending_boundary_commands` (PostgreSQL) lists accepted `cancel` commands in the `cancel` space beside family commands in the `execution` space (the in-memory adapter already used `pending_delivery`). `deliver_pending` delivers the cancel space first (RRM-008). | `tests/unit/run_control/test_boundary_relay.py::test_relay_delivers_a_cancel_in_its_own_space_before_the_blocked_pause` (cancel sequence 1 in `cancel`, delivered first, then the blocked pause; once); `tests/integration/postgres/test_rrm_009_runtime_grants_postgres.py::test_relay_lists_pending_family_commands_under_the_runtime_role_and_scope` (the cancelled run is listed under `SET ROLE belllabs_control_runtime` and RLS). |
| `heartbeat_timeout_seconds` per operation class; worker drain shorter; cancel-latency bound | `OperationHeartbeatPolicy` (`app/domain/operation_execution/heartbeats.py`): classes `deep_agent`, `deep_agent_async_children`, `bound`; both families' preparers set `OperationWorkflowRequest.heartbeat_timeout_seconds` from it (Activity code, no workflow change). Settings `OPERATION_HEARTBEAT_TIMEOUT_SECONDS` (30), `OPERATION_ASYNC_CHILDREN_HEARTBEAT_TIMEOUT_SECONDS` (15), `OPERATION_BOUND_HEARTBEAT_TIMEOUT_SECONDS` (30), `WORKER_GRACEFUL_SHUTDOWN_SECONDS` (10). `create_production_workers` and the factory refuse a drain that is not shorter than the shortest timeout; the cognitive and generic-artifact workers drain with it. The default policy equals the contract default, so families composed without a policy are unchanged. | `…::test_heartbeat_timeout_is_chosen_per_operation_class`, `::test_cancel_latency_bound_is_the_sdk_heartbeat_throttle`, `::test_worker_drain_must_be_shorter_than_every_heartbeat_timeout`, `::test_deployment_settings_compose_the_policy_and_its_drain`; the drills (settings 20 / 10 / 20, drain 3 s) read the scheduled Activities from history: `operation.execute` and `operation.cancel` carry exactly 20 s (sync-child unit) and 10 s (unit with an async child). |
| Send `liability_reconciled` after `reconcile_usage`, `reconcile_unit` and operator decisions | `FamilyLiabilityHints` (`app/application/run_control/liability_hints.py`) over `TemporalFamilyLiabilityHint`; sent only to a `cancelling` run's family (execution target), a failed send is logged and the family's timer stays the fallback. Callers: the new privileged `POST /run-control/v1/runs/{run_id}/async-children/{child_execution_id}/reconcile-usage` (`AsyncChildUsageReconciliation`, role `reconciliation_operator`, permission `workflow_run.reconcile_async_child`; the API's service has no provider access: `ProviderNotComposed`); an accepted `/reconcile-unit`; accepted operator commands of the liability kinds (`settle_effect`, `settle_pending_usage`, `record_usage`, `record_async_child_fact`, `decide_async_child_fact`). This is the first production caller of `reconcile_usage`. | `…::test_liability_hint_reaches_only_a_cancelling_family`, `::test_usage_reconciliation_is_bound_to_the_run_and_hints_once_settled`, `::test_the_api_reconciliation_never_reaches_the_provider`; live drills: the plain operator gets 403 on the route, the reconciliation operator 200 with `liability_hint_sent: true`, `liability_reconciled` is in the family history, and the run is terminal 0.5 s after the family's refused terminal proposal (the family's own backoff is 30 s). |
| `DeploymentOperationRuntime` forwards to the adapter and never swallows `CancelledError` | `execute` catches nothing; `observe_latest` is forwarded explicitly inside the operation's granted egress; every other attribute through `__getattr__`. | `…::test_deployment_runtime_never_swallows_a_cancel_during_cognition_or_the_child_wait` (both points re-raise, the egress grant does not leak), `::test_deployment_runtime_observes_the_latest_checkpoint_inside_the_granted_egress`; `tests/unit/operations/test_async_child_parent_completion.py::test_deployment_runtime_completes_children_after_cognition_and_delegates_the_rest` (assertion updated to the new explicit signature: `observe_latest(invocation, secrets)` is delegated inside the grant). |
| Settle `cancelled` only on a real, journaled cancel (RRM-008 F1) | The production worker runs `OperationExecutionActivities` (which registers the cancel probe) over `RunControlOperationAuthority` (`verify_cancellation` requires a `cancelling` run); RRM-009 adds no other settlement path and drains workers before the heartbeat timeout. | The restart drill (below): the worker set is shut down mid-model-call; nothing is settled, the run stays `active`, the retried attempt resumes and only the later journaled cancel settles `cancelled`. RRM-008's `test_worker_shutdown_mid_model_call_is_not_a_cancel_and_the_retry_recovers` stays green in the suites. |
| `OperationExecutionService(children=AsyncSubagentService)` | Through `ProductionAsyncChildCancellation` (the service is per parent operation because its provider adapter needs the operation's credential). | As above. |
| RRM-008's run-control `decide_result` | Composed through the same per-operation service; `cancel_children` rejects a blocking child and records the decision in run control before settling. | Live drills: `result_decision reject`, and the run terminalizes (no `unresolved_async_children`). The admit path of `AsyncChildCompletion` after the merge was not re-run live (`test_rrm_009_live_capabilities.py` was last run before the merge). |
| Session-generation admission after a sealed head | **Not composed: unresolved.** | See "Unresolved" below. |

Cancel latency (one delivery round trip plus the SDK heartbeat throttle; temporalio sends at most one heartbeat per `0.8 * heartbeat_timeout`, capped at 60 s; the Activity heartbeats every third of the timeout):

| Operation class | Default `heartbeat_timeout_seconds` | Cancel reaches running cognition within about | Worker loss detected after |
|---|---|---|---|
| `deep_agent` | 30 | 24 s | 30 s |
| `deep_agent_async_children` | 15 | 12 s | 15 s |
| `bound` | 30 | 24 s | 30 s |
| Drain (`WORKER_GRACEFUL_SHUTDOWN_SECONDS`) | 10 | must be < 15 (the shortest timeout) | |

### Defects found by the production drills and fixed

The drills found three defects at the RRM-008/RRM-009 seam. Each is fixed forward in Activity or service code (no workflow code, no new `workflow.patched`), with a test:

1. **A family-admitted terminalization never closed the boundary ledger** (`59bc928`). StageGraph terminalizes through `execute_family_admission`; only the plain `terminalize` path recorded RRM-008 F1's terminal receipts, so a cancelled StageGraph run was terminal `cancelled` while its cancel stayed `delivered` (first sync-child drill: receipts `['accepted', 'delivered']`) and pending family commands were never closed. The family admission now computes the same terminal receipts in the terminalizing commit; the PostgreSQL family commit records them; **migration `0026_family_writer_terminal_boundary_receipts_v1.sql`** grants `belllabs_family_repository_writer` `SELECT` on both insert-only boundary ledgers and `INSERT` on receipts only (grant-only, forced RLS kept, no rows or digests change). Tests: `tests/unit/run_control/test_rrm_009_family_terminal_receipts.py::test_family_admitted_cancelled_terminalization_applies_the_cancel` (fails on the old service: `['accepted']`), `tests/integration/postgres/test_rrm_009_runtime_grants_postgres.py::test_family_writer_closes_the_ledger_of_a_cancelled_run_only_with_migration_0026` (exact privileges; without the grants the terminalizing commit fails `InsufficientPrivilegeError` and rolls back; with them the cancel is `applied` and the pause `rejected(terminal_run)`).
2. **StageGraph failed the family on a cancelled child's pending usage** (`cc8778a`). `decide_result` closed a producer only if every run effect was settled; the cancelled unit's async child keeps unattributed usage pending on its own effect, so the producer stayed open and the family failed non-retryably ("StageGraph cancellation left a producer liability open"; live runs 2, 5, 6). `producer_effects_settled` excludes the cancelled unit's own async-child effects, exactly as `_settle_cancelled` does; the reducer still refuses the `cancelled` terminalization until the usage is reconciled, and the family waits on that refusal. Test: `…::test_a_cancelled_units_pending_child_usage_does_not_hold_its_producer_open` (another unit's child or any other unsettled effect still holds it).
3. **The saga's second terminal proposal reused the refused proposal's identity** (`cc8778a`). After a liability refusal the family proposed again with the same command id and a new payload: `lifecycle command identity was reused with a conflicting payload`, and the family failed (live run 7). A refused proposal keeps its receipt; the next proposal takes the next unused `:retry:{n}` identity; an Activity retry of the same request replays its own receipt. Test: `…::test_a_refused_completion_is_proposed_again_under_a_new_identity`.

RRM-008's own proofs did not reach these paths: its StageGraph demonstrations used the RRM-007 harness (plain terminalization) and its live async child ran under GoalDirected.

### Item 6: subagent cancellation and reconnect inside the production composition

`tests/acceptance/control_plane/test_rrm_009_production_cancellation.py` over `tests/fixtures/rrm009_cancellation.py` (`run_cancellation_drill`): the deployment's API and workers (`open_production_stack`: `compose_runtime_control`, `ProductionWorkerActivityCompositionFactory`, `create_production_workers`, persistent `start_local` namespace, disposable PostgreSQL and Mongo). A StageGraph run is admitted and launched through the facade; its `draft` unit's Deep Agent is held at a chosen point; the cancel enters through `POST /run-control/v1/runs/{run_id}/commands`. Every saga step is asserted.

- **Sync child** (DSN opt-in, deterministic models): the cancel lands while the in-process child's model call is in flight. Unit settled `cancelled` (`failure_code cancelled`), usage `{"model.turns": 1, "tokens.total": 5}` (the parent's completed `task` call; the child's in-flight call is unobservable), transition `interrupted`, effect `operation.runtime cancelled`, reservations empty, model calls parent 1 / child 1 (nothing resumed), `review` never admitted, receipts `accepted → delivered → applied`, terminal `cancelled`, root and family replay.
- **Async child, live** (`BELLABS_RUN_RRM_009_LIVE=1`, the `rrm009-*` Agent Server with signed scope claims, hosted technical child on `gpt-5.6-luna` told to `wait_seconds(120)`, tiny input; the parent model is deterministic so the cancel window is exact). Three cases:
  - `cognition`: the child is `running` and the parent's next model call is in flight.
  - `completion_wait`: the parent's cognition finished; the operation boundary is waiting for the child (`AsyncChildCompletion`).
  - `restart_then_cancel`: the worker set is replaced while the child runs and the parent call is held. The retried attempt resumes the interrupted lineage (the held call is asked again), the worker shutdown settled nothing, the run stays `active`, the parent still has exactly one child and the provider exactly one run (no second spawn); the cancel then reaches the same child. This is the reconnect proof inside the composition.

  In each: child cancelled at the provider (`interrupted`) and acknowledged (`provider_acknowledged`), lifecycle `cancelled`, `result_decision reject`, `usage_disposition pending_usage` (`pending_external_amounts {"tokens.total": 10}`); the run stays `cancelling` and the family is still running after its terminal proposal was refused; the privileged reconciliation (provider thread-state attribution) settles the child (`settlement_revision 2`) and hints the family; the run is terminal `cancelled` 0.5 s later; receipts `accepted → delivered → applied`; effects `{async_subagent.child: cancelled, operation.runtime: cancelled}`, all settled; reservations empty; root and family replay.

Final-head live run (`65bf532`, one process, lock held, `rrm009` stack sourced, tracing off): **4 passed in 141 s** (the sync case runs in the same module).

| Case | Run | Child / provider run | Provider status | Pending, then attributed | Transition | Parent calls | Heartbeat (s) | Terminal after the refused proposal |
|---|---|---|---|---|---|---|---|---|
| sync child | `99e375ca-…` | n/a (in-process child, 1 held call) | n/a | n/a | `interrupted` | 1 | 20 | n/a |
| async, `cognition` | `68f358e2-…` | `fb2e06f6-…` / `01a0fedb-0758-…` | `interrupted` | 10 → 2,282 tokens (revision 2) | `interrupted` | 2 | 10 | 0.7 s |
| async, `completion_wait` | `852595b0-…` | `1f0927bc-…` / `01a0fedb-666f-…` | `interrupted` | 10 → 2,340 tokens | `terminal_unobserved` | 2 | 10 | 0.6 s |
| async, `restart_then_cancel` | `d9b5fda3-…` | `6cf16c35-…` / `01a0fedb-c274-…` | `interrupted` | 10 → 2,359 tokens | `interrupted` | 3 (1 resumed after the restart) | 10 | 0.6 s |

Every case: settlement `cancelled` (`failure_code cancelled`), receipts `accepted → delivered → applied`, terminal outcome `cancelled`, effects settled, reservations empty, root and family replayed. The child's actual usage is recorded on the parent run even though it exceeds the child's 10-token contract limit. Actuals are never dropped, and the incident policy is RRM-010's call, as RRM-008 recorded.

**Live run record and spend (this session, honest count).** Nine development invocations plus the final run spawned 15 hosted child runs. The parent model was deterministic: no parent provider spend and no Tavily call. Runs 1, 2, 5 (2 cases) and 6 failed on defect 2 and run 7 (2 cases) on defect 3, after the child had been spawned and cancelled; runs 3, 4, 8 (2 cases), 9 and the final run (3 async cases) passed. Each child was interrupted after one model turn, and the provider attributed 2,282–2,359 tokens per run, about **35k gpt-5.6-luna tokens in total, almost all input**. Exact prices were not available to this session. At an assumed USD 2.50/M input and USD 15/M output this is **under USD 0.15**, well inside the USD 3 cap.

### Item 1: the S3 object artifact store

`tests/acceptance/control_plane/test_rrm_009_object_store.py::test_s3_object_store_carries_candidates_results_and_promoted_artifacts` runs the production stack with `S3_BUCKET` set against a local S3-compatible server and one governed operation through `POST /runs/{run_id}/operations` (`GenericArtifactWorkflow`). The promoted artifact, the captured workspace candidate, and the journal's result manifest and output payload (with the capability lineage) are content-addressed objects in the bucket (`s3://<bucket>/artifacts/sha256/<digest>`, object metadata `sha256` equals the digest). Retrieval through `S3ArtifactPayloadStore` verifies digest and size, and a wrong digest is refused. Nothing falls back to the filesystem store. The durable reference row exists. The AWS configuration is a dedicated profile in temporary files (path-style addressing, endpoint `AWS_ENDPOINT_URL_S3`); `~/.aws` is never read and no request can reach AWS. Opt-in: `RRM009_S3_ENDPOINT`, `RRM009_S3_ACCESS_KEY`, `RRM009_S3_SECRET_KEY`.

Server: container `rrm009-minio` from `alpine/minio:RELEASE.2025-10-15T17-29-55Z` (`sha256:cf23643a6cf9ce159c57643ceb88279e431262282428c9e0bf3a7ef1a97e84b4`; the official `minio/minio` and `quay.io/minio/minio` repositories refused pulls on this host), `--memory 256m` (about 68 MiB used), `127.0.0.1:19100` (the 58935–59034 range is excluded on this host), volume `rrm009-minio-data`, run as root because the image's `minio` user cannot write a fresh named volume. Credentials were generated into a scratch env file and never printed. Final run on `65bf532`: **1 passed (42 s)**; bucket `rrm009-artifacts-bc73ead4`, artifact `s3://…/artifacts/sha256/5eb812f1…` (`sha256:5eb812f1…`), result manifest `s3://…/artifacts/sha256/908f7684…`, 3 objects. The first qualification run, on `c495aea`, also passed. The container and its volume were removed by exact name afterwards (`docker rm -f -v rrm009-minio`, `docker volume rm rrm009-minio-data`), and so was the image.

Limits of this qualification: MinIO implements the S3 API, not AWS. IAM policies, KMS encryption, real-region endpoints and the AWS credential chain in a deployment are not exercised.

### Server stacks

- **Current:** `rrm009-agent-server` (compose project; `rrm009-agent-server-langgraph-api-1` at `http://127.0.0.1:8144`, `rrm009-agent-server-langgraph-redis-1`, `rrm009-agent-server-postgres` on `127.0.0.1:55434` with volume `rrm009-agent-server-pgdata`). It was built from this branch, carries the signed scope-claim auth and served every live drill above. It was left running.
- **Stale:** `rrm013-agent-server-*` still runs the pre-RRM-009 image (static bearer token) and answers a scope claim with 401. It was not stopped or removed. After RRM-009 merges, rebuild it from integration, or use `rrm009-*`. Rebuild: from the worktree, `langgraph up --config langgraph.async_subagents.json --api-version 0.12.0 --no-pull` with `COMPOSE_PROJECT_NAME` and the Postgres/secret environment of the stack (the RRM-009 launcher is `scratchpad/rrm009_up.py init|postgres|up`, which never prints secrets). `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` is the HMAC signing secret. Teardown by exact names: `docker compose -p rrm009-agent-server down`, `docker rm -f -v rrm009-agent-server-postgres`, `docker volume rm rrm009-agent-server-pgdata`.

### Cleanup recorded in this session

- Mongo (under the lock): the two old leaked databases were dropped by exact name: `rrm009_b8cd764aeb47` and `rrm009_dc8ba5c37292` (GoalDirected and full-model collections from earlier killed runs). Afterwards no `rrm009_*` database remains. Every drill database of this session was dropped by its fixture.
- `rrm009-minio`, its volume and its image were removed by exact name. No `rrm-app-*`, `rrm013-*` or `csi01` container was stopped, removed or pruned.
- No lock violation: every DSN, `start_local`, container and live command acquired `stack_lock.py acquire RRM-009` and proceeded only on `acquired`.

### Unresolved after the integration

1. **Session-generation admission after a sealed head (RRM-008 R3), not composed.** When a GoalDirected unit settles `failed` or `timed_out`, its non-seedable transition seals the session head and every later unit of that session is refused before provider work. The family then fails the run on the unit without a typed result. Continuing needs three things: an operator or family path that admits a new `session_generation` fresh from the handoff (REQ-BP-GD-012); a typed GoalDirected continuation command and its reducer rule; and a family change to resume after a failed unit instead of failing the run. That is new family semantics plus a workflow change under a patch, not composition. Nothing in this composition reuses or branches a sealed session (the GoalDirected technical run uses `fresh` workspaces and never seals). Owner: a follow-up ticket, or RRM-014 with the re-admission work.
2. **Superseded generation under cancellation (RRM-008 R2 residual).** Both windows stay open. (a) A `generation_superseded` result consumed before the cancel has no settler, and the run waits on its liability timer. (b) A superseded generation with an unsettled consequential effect is not re-dispatched after the operator's effect decision. The composition now sends `liability_reconciled` after the operator's settlement commands. That shortens the wait but does not settle the claim. An operator-triggered `operation.cancel` for a named unit is not built. These close with RRM-014's re-admission.
3. **`GenericArtifactWorkflow` is outside the cancellation saga.** It serves `operation.cancel`, but its `operation.execute` declares no heartbeat timeout and no saga delivers a cancel to it. A generic artifact operation runs to its own end (start-to-close 10 min). Changing that is a workflow change (patch) and was not needed for item 6.
4. **Post-merge `AsyncChildCompletion` admit path** (child completes and is admitted, with RRM-008's run-control decision): proven before the merge by the live capability run, and by the unit tests since. Not re-run live after the merge.
5. S3 is qualified against MinIO, not AWS (above).

### Integration gates (final, code head `65bf532`)

One pytest process at a time. Every DSN, `start_local`, container and live run held the stack lock. Hermetic runs unset both DSNs; DSN runs export them and add `--env-file`. The common flags are the ones listed under "Deterministic verification".

| Gate | Command | Result |
|---|---|---|
| ruff | `uv run --no-sync ruff check app tests scripts` | All checks passed |
| mypy | `uv run --no-sync mypy app` | Success: no issues found in 383 source files |
| owning suites, hermetic (both tickets) | `pytest tests/unit/{run_control,operations,orchestration,integrations,workspaces,agent_server} tests/unit/control_plane/test_digest_set_order_guard.py tests/unit/control_plane/test_contract_digest_set_order.py tests/integration/temporal` | **603 passed, 21 skipped, 1 xfailed** (160 s) |
| owning suites, DSN (both tickets) | `pytest tests/acceptance/control_plane/test_rrm_009_production_composition.py` | **3 passed** (226 s) |
| | `pytest tests/acceptance/control_plane/{test_rrm_009_production_cancellation,test_rrm_008_cancellation_demo,test_wp_cp_045,test_wp_cp_040}.py tests/integration/postgres/test_rrm_009_runtime_grants_postgres.py tests/integration/temporal/test_coordinator_temporal_runtime.py` | **42 passed, 4 skipped** (113 s; skips = the three live async cases and RRM-008's live case) |
| replay | the replay command under "Replay and recovery artifacts" (DSNs) | **12 passed, 49 deselected** |
| full hermetic, one process | `env -u … pytest -q -p no:cacheprovider -rs` | **1059 passed, 90 skipped, 3 xfailed, 0 failed** (211 s) |
| full DSN, one process | `pytest -q -p no:cacheprovider -rs` with DSNs and `--env-file` | **1113 passed, 36 skipped, 3 xfailed, 0 failed** (678 s) |
| live cancellation proof | `BELLABS_RUN_RRM_009_LIVE=1 … pytest -s tests/acceptance/control_plane/test_rrm_009_production_cancellation.py` | **4 passed** (141 s) |
| S3 object store | `RRM009_S3_* … pytest -s tests/acceptance/control_plane/test_rrm_009_object_store.py` (container `rrm009-minio`) | **1 passed** (42 s) |
| `git diff --check 56ffd63 HEAD` | | clean |

**Deltas against the integration baseline.**
- Hermetic: 1020/78/2 → **1059/90/3**. The +39 passed are RRM-009's 24 plus this session's 15 hermetic unit tests. The +12 skipped are RRM-009's 6 DSN- or live-gated tests plus this session's postgres test, sync drill, three live drills and S3 test. The +1 xfailed is the strict RRM-020 reproduction.
- DSN: 1067/31/2 → **1113/36/3**. The +46 passed are RRM-009's 29 plus this session's 17. The +5 skipped are the RRM-009 live module, the three live cancellation cases and the S3 test without its container. The +1 xfailed is RRM-020.

**Runs recorded honestly.**
- The full DSN suite no longer fits the 600 s foreground limit; it takes 665–678 s with the production qualifications and drills. It ran as **one process** in the background under the lock, with nothing else running. Two complete runs gave the same counts: `4667175` (1113/35/3, before the restart case was added) and `65bf532` (1113/36/3).
- The first final hermetic run on `65bf532` had **1 failure** after 371 s under host load. `tests/integration/temporal/test_rrm_007_boundary_interventions.py::test_goal_directed_policy_pause_is_durable_across_forced_continue_as_new` timed out (`TimeoutError` in `handle.result`). This is the load-sensitive RRM-007 time-skipping test that RRM-008's evidence already records. It passed 3/3 alone (14–16 s) and in the complete hermetic re-run above (211 s). No test was re-run selectively to produce the gate.
- One owning-suite DSN invocation hit its 590 s `timeout`; it ran the production qualification, the cancellation drills and the RRM-008 demo together. Its buffered output showed one `F` for the second test of the production qualification module (the GoalDirected run), and the cause was not captured. The module then passed 3/3 alone (226 s) and inside both complete DSN runs.

## Requirement-to-evidence map

| Ticket item / requirement | Test | Observed assertion |
|---|---|---|
| Real PostgreSQL, Mongo definitions/bindings, object storage, persistent saver/store through existing ports | `tests/acceptance/control_plane/test_rrm_009_production_composition.py::test_stagegraph_runs_through_the_production_composition_with_fork_relay_and_inspection`, `::test_generic_artifact_operation_promotes_the_captured_report_durably`; `tests/unit/integrations/test_capability_pins_and_runtime_ports.py::test_filesystem_payload_store_is_content_addressed_and_verified` | saver checkpoints in `rrm009_langgraph` asserted > 0 (45 observed for the StageGraph run); inspection checkpoint history `current`; every workspace candidate's `object_ref` exists under the payload root; the promoted artifact has a durable reference row; the CAS store refuses a digest mismatch. **S3** (MinIO, `tests/acceptance/control_plane/test_rrm_009_object_store.py`): the artifact, candidate, result manifest and output payload are bucket objects under their content address and are verified on retrieval; a wrong digest is refused; nothing is written to the filesystem |
| Canonical registries/queues, exact placement, no demo runtime | `…::test_stagegraph_runs_through…` (lineage `placement.task_queue == "rrm009-agent-cognitive"`); `tests/integration/temporal/test_coordinator_temporal_runtime.py::test_production_worker_fails_closed_before_startup_without_real_adapters` | the binding's cognitive queue is served by `create_production_workers`; with launch enabled and an unregistered namespace the worker raises `SearchAttributeRegistrationError` before any store is opened (this replaces the old "requires a deployment factory" refusal: the factory is now built in; a design change, not a weakened assertion) |
| Skills/tools pinned, mounted/disclosed, invoked | `tests/unit/integrations/test_capability_pins_and_runtime_ports.py::test_pin_file_is_exact_and_discloses_no_secret`, `::test_workspace_artifacts_verify_against_their_pins_when_present`, `::test_workspace_locators_cannot_escape_the_workspace`, `::test_pinned_verifier_admits_exact_bindings_and_refuses_drift`; live `tests/acceptance/control_plane/test_rrm_009_live_capabilities.py::test_pinned_capabilities_and_both_subagents_run_in_the_production_composition` | exact pins, no secret value in the file, drift refused; live `invoked` (below) includes the skill read, `tavily_search`, `agent_browser_page`; the disclosed skill bundle digest and MCP tool filter digests equal the pins |
| Mediated/constrained egress, isolation kept | `…::test_browser_tool_reaches_public_hosts_by_name_only`, `…::test_browser_tool_opens_only_hosts_the_operation_was_granted`, `…::test_browser_tool_refuses_a_granted_name_that_resolves_to_a_private_address`; live lineage | **Browser (grant-bound, deny-by-default after the review):** every IP literal refused, including non-canonical IPv4 forms (`2130706433`, `0x7f000001`, `0177.0.0.1`, `127.1`); local names refused; no grant bound means no page; a host outside the operation's `network_hosts` grant is refused, as is a granted name that resolves to a non-global address. The live lineage discloses the grant the browser was bound by. **Tavily MCP (worker-mediated, not grant-bound):** the pinned stdio subprocess reaches its own service endpoint with the worker's network. `network_hosts` does not constrain it; the pin and the worker's launch verification do. Residual: DNS rebinding between the check and the browser's own lookup. |
| Report slots and artifact promotion via governed contracts | `…::test_generic_artifact_operation_promotes_the_captured_report_durably`; live | captured candidate promoted through `GenericArtifactWorkflow`; live report `artifact://tenant-1/75990b37-…/463ca2fa-…` (`sha256:629b9cdd…`) contains "Example Domain" and the child's `CHILD-OK` |
| Sync subagent in-process | `tests/unit/integrations/test_sync_subagent_usage_and_lineage.py::test_sync_subagent_runs_and_its_usage_is_charged_to_the_parent`; technical StageGraph lineage | the materializer hands deepagents typed `FilesystemPermission` rules (the dict form raised `AttributeError: rule.mode`; this test fails on the old code); child usage appears in the parent's usage; technical: `invoked == {"framework": ["write_file"], "mcp": ["lookup_binding_marker"], "sync_subagent": ["task"]}`, one child call per operation; live: one `subordinate` model call charged |
| Async subagent cancellation and reconnect inside the production composition; sync-child cancellation (fourth session) | `tests/acceptance/control_plane/test_rrm_009_production_cancellation.py::test_sync_subagent_cancellation_in_the_production_composition`, `::test_async_subagent_cancellation_in_the_production_composition[cognition|completion_wait|restart_then_cancel]`; hermetic seams in `tests/unit/operations/test_rrm_009_cancellation_composition.py` | see "Item 6" under "Integration with RRM-008" |
| Async subagent on the Agent Server: reservation, dependency/result decision, cancel/reconnect | `tests/unit/operations/test_async_child_parent_completion.py::test_completed_blocking_child_is_admitted_and_settled_once_at_the_boundary`, `::test_active_child_and_unregistered_policy_are_left_undecided_and_unsettled`, `::test_failed_child_is_rejected_and_its_unattributed_usage_stays_pending`, `::test_completion_bounds_are_validated`, `::test_deployment_runtime_completes_children_after_cognition_and_delegates_the_rest`; live; RRM-013 live drills against the scope-claim server | admitted and settled once; active/unregistered left undecided; failed rejected with pending usage; live: reservation and link before submission, `REQUIRED_BLOCKING`, admitted under `policy:async-result:technical-child@1`, `usage_disposition settled`, 2,263 tokens attributed; drills `spawns_one_real_child`, `cancel_reaches`, `restart_during` passed (cancel acknowledged, restart reconnects) |
| Sanitized lineage | `tests/unit/integrations/test_sync_subagent_usage_and_lineage.py::test_credential_references_are_names_only_and_cover_every_mount`; `test_rrm_009_production_composition.py` (lineage read from the journal's digest-bound payloads); `tests/unit/operations/test_operation_execution.py::test_journaled_operation_settles_usage_effect_and_terminalizes` (event payloads persisted and restored); live | `credential_refs == ["environment:OPENAI_API_KEY"]` (technical); every mount's credential appears by reference only; live asserts no credential value appears in any durable payload. After the review: browser URLs are recorded as scheme, host and path only; argument names are kept but no argument digest; unknown subagent names and over-long paths are not echoed (`test_invocation_records_keep_no_argument_value_and_bound_model_chosen_strings`). The "no prompt text" property holds for the lineage record only. The inspection payload persisted beside it carries the pinned Skill instruction text and bundle manifests (reviewed content, not operator input). |
| Small API-to-Temporal qualification, both families, persistent namespace | `…::test_stagegraph_runs_through…`, `…::test_goal_directed_runs_two_iterations_through_the_production_composition` | admission through `/run-control/v1` and `/runs/{id}/launch`; `start_local` with a database file, restarted in the relay drill; StageGraph completes (fork, relay, inspection); GoalDirected runs 2 iterations with an independent verifier and promotes final-executor outputs only; Visibility by `BellLabsRunId` lists exactly 6 executions |
| Runbook, flags, cleanup; replay/recovery | `README.md` "Production-shaped composition (RRM-009)" and the runbook below; replay command below | 12 replay tests passed; no workflow code changed |
| Search Attributes (REQ-CP-EXEC-015) | `…::test_stagegraph_runs_through…`; worker readiness test above | the API refuses to compose before registration; registration is idempotent (run twice); `BellLabsRunId` lists exactly 4 executions for the source and 4 for the derived run; `BellLabsParentRunId = <source>` lists exactly 1; **`BellLabsUnitKey = <unit>` lists exactly 1**; the source run's `BellLabsWorkflowKind` values are exactly `["family", "operation", "operation", "root"]` (the last two assertions were added in `6d956e9` and passed on 2026-10-02 16:13–16:16 EDT) |
| Inspection composition (REQ-CP-RUN-011/012) | `…::test_stagegraph_runs_through…` | sections `temporal`, `run`, `units`, `budget`, `effects`, `async_children`, `async_children_detail` all `current`; the unit's checkpoint history `current` |
| Boundary delivery relay (RRM-007 F6) | `…::test_stagegraph_runs_through…` (relay drill); `tests/unit/run_control/test_boundary_relay.py::test_relay_delivers_what_inline_delivery_could_not_in_order_and_once`, `::test_relay_requires_a_positive_interval`; `tests/integration/postgres/test_rrm_009_runtime_grants_postgres.py::test_relay_lists_pending_family_commands_under_the_runtime_role_and_scope` | pause accepted while Temporal is down → receipts `[accepted, delivered, applied]`, phase `paused`; resume and release → completed; relay redelivers in order, once; the relay read works under `SET ROLE belllabs_control_runtime` and RLS, scoped |
| Agent Server credential (RRM-013 N8) | `tests/unit/agent_server/test_async_subagent_server_offline.py::test_identity_route_requires_the_deployment_credential`; live | claim accepted for its scope; wrong secret, raw secret, tampered claim, expired claim → 401; live: claim + same scope 200, claim + other scope header 403, raw static token 401, no auth 401 |
| Migration 0025 (least privilege) | `tests/integration/postgres/test_rrm_009_runtime_grants_postgres.py::test_runtime_role_opens_a_unit_fenced_claim_only_with_migration_0025` | under `SET ROLE belllabs_control_runtime` an effect claim with `unit_key` is refused without the grant and accepted with it |
| Launch | `tests/unit/run_control/test_run_launch.py::test_launch_starts_only_an_input_bound_to_the_admitted_run`, `::test_fork_derived_run_waits_for_materialization_and_binds_the_fork_reference` | another run's input refused; baseline reservation verified; fork root waits for materialization and carries the fork reference |
| Fork templates | `tests/unit/orchestration/test_fork_templates.py::test_patched_stage_gains_an_admitted_objective_and_the_rest_is_copied`, `::test_patch_naming_a_stage_without_a_template_is_refused` | patched stage gets the admitted objective, others copied; unknown stage refused |
| GoalDirected slot ownership | `tests/unit/workspaces/test_goal_role_slot_ownership.py::test_role_root_is_the_ownership_boundary_and_other_paths_keep_two_components`, `::test_executor_and_verifier_of_one_iteration_own_disjoint_roots`; `::test_shared_goal_workspace_is_materialized_again_at_the_next_iteration` (`xfail(strict=True)`, RRM-020) | role roots disjoint; the shared-workspace case still raises `IdempotencyConflict` (tracked by RRM-020) |

### Capability-invocation proof (live, final run on `2f3a642`)

```text
run 75990b37-…  model gpt-5.6-luna (model.wp-cp-040)  tracing off
invoked = {
  skill_read:     [read_file]            # disclosed skill agent-browser, bundle sha256:30722859…
  mcp:            [tavily_search]        # MCP tool filter digests == pins
  tool:           [agent_browser_page]   # granted network_hosts disclosed in lineage
  sync_subagent:  [task]
  async_subagent: [start_async_task, check_async_task]
  framework:      [write_file]
}
artifact://tenant-1/75990b37-…/463ca2fa-… sha256:629b9cdd…  ("Example Domain", "CHILD-OK")
```

Firecrawl MCP is pinned (module and schema digests verified) but was not invoked live.

### Subagent proofs

- **Sync (in-process).** Technical: one child model call per operation, `sync_subagent: [task]` in the lineage, child usage charged to the parent. Live: one `subordinate` model call charged in the parent's 87,173 tokens.
- **Cancellation and reconnect inside the production composition (fourth session).** Each case was cancelled through the facade to a terminal `cancelled` run with `applied` receipts: the sync child held in its model call, and the hosted async child running during the parent's cognition, during the boundary's wait, and across a worker-set restart. See "Item 6".
- **Async (Agent Server).** Live: hosted technical child `bf67c7e3-…`, provider run `01a0fe17-…`, graph binding `sha256:764f0ef5…`; reservation and link recorded before submission (RRM-013 service); `REQUIRED_BLOCKING`; admitted at the parent boundary by `policy:async-result:technical-child@1`; settled (`usage_disposition settled`, 2,263 tokens `provider_attributed`). Cancellation and reconnect: RRM-013 live drills re-run against the scope-claim server, `BELLABS_RUN_RRM_013_LIVE=1 RRM013_AGENT_SERVER_API_CONTAINER=rrm009-agent-server-langgraph-api-1 … -k "spawns_one_real_child or cancel_reaches or restart_during"` → 3 passed, 3 deselected (296 s). Crash-window drills were not re-run.

## Changed paths and migrations

**Fourth session (integration with RRM-008), `dfe33ad..65bf532`:**
- New files:
  - App: `app/domain/operation_execution/heartbeats.py`, `app/application/run_control/liability_hints.py`, `app/application/async_subagents/usage_reconciliation.py`.
  - Migration: `app/migrations/0026_family_writer_terminal_boundary_receipts_v1.sql`.
  - Tests: `tests/unit/operations/test_rrm_009_cancellation_composition.py`, `tests/unit/run_control/test_rrm_009_family_terminal_receipts.py`, `tests/fixtures/rrm009_cancellation.py`, `tests/acceptance/control_plane/test_rrm_009_production_cancellation.py`, `tests/acceptance/control_plane/test_rrm_009_object_store.py`.
- RRM-009 paths changed:
  - `app/temporal/deployment_composition.py`: child-cancellation port, `observe_latest`, heartbeat policy into the families.
  - `app/temporal/worker.py`: `operation_heartbeat_policy`, the drain check and the graceful shutdown.
  - `app/api/runtime_composition.py`.
  - `app/api/run_control.py`: the reconcile-usage route; hints after `/commands` and `/reconcile-unit`; `reconciliation_operator` gains `workflow_run.reconcile_async_child`.
  - `app/config.py`: four settings.
  - `README.md`.
- **Shared seams (coordinator-owned, listed separately):**
  - `app/application/run_control/service.py`: terminal receipts of a family-admitted terminalization.
  - `app/application/run_control/postgres_run_control_repository.py`: the family commit records receipts; the relay lists the `cancel` space.
  - `app/application/run_control/run_control_repository.py`: docstring only.
  - `app/application/orchestration/service.py`: `producer_effects_settled`, the completion identity on retry, the heartbeat in the StageGraph preparer.
  - `app/application/orchestration/goal_directed.py`: the heartbeat in the GoalDirected preparer.
  - Also `app/temporal/activities/goal_directed.py`, `app/temporal/coordinator_runtime.py`, `app/temporal/operation_activities.py`, `app/temporal/artifact_activities.py`, `app/integrations/temporal_unit_reconciliation.py`.
  - Tests edited: `tests/conftest.py`, `tests/unit/run_control/test_boundary_relay.py`, `tests/integration/postgres/test_rrm_009_runtime_grants_postgres.py`, `tests/unit/operations/test_async_child_parent_completion.py` (explicit `observe_latest` signature), `tests/acceptance/control_plane/test_rrm_009_production_composition.py` (`promote_generic_artifact` extracted; API state names).
- Migration **0026** is grant-only and least-privilege. No workflow code changed, no new `workflow.patched`, no dependency change.

**Earlier sessions:**

`git diff --stat c8221f5 6d956e9`: 59 files, +8,612 / −101.

**Shared-seam edits (coordinator-owned, listed separately):**
- `app/integrations/agents/deep_agents/adapter.py`: workspace-output capture; `_subagent_specs` (drop child permissions beside an executable sandbox); `_ModelCallObserver` and child usage in `_usage`; `capability_lineage` in inspection.
- `app/integrations/agents/deep_agents/materializer.py`: typed `FilesystemPermission` for sync children; `tool_schema_digest` export.
- `app/integrations/agents/deep_agents/async_subagents.py`: scope-claim minting and refresh.
- `app/migrations/0025_operation_claim_unit_key_runtime_grant_v1.sql`: new, `GRANT INSERT (unit_key) ON belllabs_control.operation_effect_claims TO belllabs_control_runtime` (grant only; forced RLS kept; no rows, digests or identities change).
- `app/temporal/registration/task_queues.py`: `generic_artifact_task_queue`.
- `app/temporal/worker.py`: production worker set, readiness verification, built-in factory.
- Run control: `runs_with_pending_boundary_commands` (`postgres_run_control_repository.py`, `run_control_repository.py` in-memory and protocol, `service.py`).
- `app/application/operations/journaled_operation_execution.py`: `output_payload` carries `event_payloads` when present; `_restore` re-attaches them.
- `app/domain/operation_execution/materialization.py`: `slot_ownership_boundary` (used by `app/application/workspaces/mongo_workspace_repository.py` and `workspace_materialization.py`).

**RRM-009 production paths:** `app/temporal/deployment_composition.py`, `app/api/runtime_composition.py`, `app/api/run_control.py` (launch route), `app/server.py`, `app/config.py`, `app/application/run_control/{run_launch.py,boundary_relay.py}`, `app/application/orchestration/{fork_templates.py,mongo_stagegraph_repository.py}`, `app/application/async_subagents/parent_completion.py` (new), `app/application/runners/web_research_coordinator_live.py`, `app/integrations/{capability_pins.py,operation_runtime_ports.py,workspace_candidate_contents.py,mongodb.py}`, `app/integrations/agents/deep_agents/{browser_tool.py,capability_lineage.py}` (`capability_lineage.py` new), `app/models/{stagegraph.py,workspace_materialization.py,__init__.py}`, `app/agent_server/async_subagents/{auth.py,http_app.py}`, `scripts/{pin_research_capabilities.py,register_belllabs_search_attributes.py}`, `infra/capability-pins/research-capabilities.json`, `README.md`.

**Tests:** new `tests/acceptance/control_plane/test_rrm_009_{production_composition,live_capabilities}.py`, `tests/fixtures/rrm009_{production_stack,live_capabilities}.py`, `tests/integration/postgres/test_rrm_009_runtime_grants_postgres.py`, `tests/unit/integrations/{test_capability_pins_and_runtime_ports.py,test_sync_subagent_usage_and_lineage.py}`, `tests/unit/operations/test_async_child_parent_completion.py`, `tests/unit/workspaces/test_goal_role_slot_ownership.py`, `tests/unit/run_control/{test_run_launch.py,test_boundary_relay.py}`, `tests/unit/orchestration/test_fork_templates.py`; edited `tests/conftest.py`, `tests/unit/operations/test_operation_execution.py` (event payloads), `tests/acceptance/control_plane/test_wp_cp_045.py` and `tests/unit/agent_server/test_async_subagent_server_offline.py` (scope-bound adapter/claims), `tests/integration/agent_server/test_rrm_013_async_subagent_live.py` (scope-claim server), `tests/integration/temporal/test_coordinator_temporal_runtime.py` (readiness refusal, see the map).

**Docs:** `docs/…/research-runtime-mission/issues/20-materialize-shared-goal-workspace-across-iterations.md`, `21-settle-the-stagegraph-baseline-reservation.md` (new tickets), this README, the RRM-009 ticket and the index.

## Deterministic verification

All commands ran from the worktree with `unset VIRTUAL_ENV` and `uv run --no-sync`, one pytest process at a time, in the foreground. Every DSN and full run held the stack lock (third session: acquired 16:13, released after the Mongo cleanup at about 16:30 EDT). DSN runs export `TEST_APPLICATION_POSTGRES_DSN` and `TEST_MONGODB_URI` and add `--env-file ../biotech-research-ingestion-evaluation-system/.env`; hermetic runs use `env -u` for both. Common flags: `BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false`. A single full run exceeds the 600 s tool limit, so each full suite ran in two chunks: A = `tests --ignore=tests/acceptance`, B = `tests/acceptance`.

**Final gates on `6d956e9`:**

| Gate | Command | Result |
|---|---|---|
| StageGraph qualification (the `6d956e9` assertions) | `pytest -q -rs tests/acceptance/control_plane/test_rrm_009_production_composition.py -k stagegraph` (DSNs) | 1 passed, 2 deselected (148 s) |
| ruff | `ruff check app tests scripts` | All checks passed |
| mypy | `mypy app` | Success: no issues found in 379 source files |
| hermetic A | `pytest -q -p no:cacheprovider tests --ignore=tests/acceptance` | 954 passed, 69 skipped, 3 xfailed (131 s) |
| hermetic B | `pytest -q -p no:cacheprovider tests/acceptance` | 51 passed, 12 skipped (35 s) |
| **hermetic total** | A + B | **1005 passed, 81 skipped, 3 xfailed, 0 failed** |
| DSN A | `pytest -q -p no:cacheprovider tests --ignore=tests/acceptance` (DSNs, env-file) | 996 passed, 27 skipped, 3 xfailed (234 s) |
| DSN B | `pytest -q -p no:cacheprovider -rs tests/acceptance` (DSNs, env-file) | 59 passed, 4 skipped (252 s); skips = the four live gates (RRM-009, WP-BP-010, WP-BP-020, WP-CP-040) |
| **DSN total** | A + B | **1055 passed, 31 skipped, 3 xfailed, 0 failed** |
| replay | see "Replay and recovery artifacts" | 12 passed, 49 deselected |
| `git diff --check c8221f5 HEAD` | | clean |

Delta against base `c8221f5` (hermetic 981/75/2; DSN 1026/30/2): hermetic +24 passed, +6 skipped (the new DSN- and live-gated tests), +1 xfailed (the strict RRM-020 reproduction); DSN +29 passed, +1 skipped (the RRM-009 live gate), +1 xfailed (RRM-020). The results are identical to the second implementer's gates on `2f3a642`; the only later code change is the two test assertions of `6d956e9`. The owning suites (unit integrations/workspaces/run_launch/boundary_relay/fork_templates/async-child completion/operation execution/agent server/digest guard, the PostgreSQL grants, coordinator runtime, WP-CP-040/045: 245 passed, 19 skipped, 1 xfailed on `c1cd172`) are subsets of the DSN total and were not re-run separately.

## Live runtime qualification

Command (lock held, `rrm009` Agent Server env sourced, never printed):
`BELLABS_RUN_RRM_009_LIVE=1 LANGSMITH_TRACING=false TEST_APPLICATION_POSTGRES_DSN=<redacted> TEST_MONGODB_URI=<redacted> uv run --no-sync --env-file ../biotech-research-ingestion-evaluation-system/.env pytest -q -s tests/acceptance/control_plane/test_rrm_009_live_capabilities.py`

Opt-ins: `BELLABS_RUN_RRM_009_LIVE=1` (this module), `BELLABS_RUN_RRM_013_LIVE=1` (RRM-013 drills). The path is the production composition: `/run-control/v1` admission → `/runs/{id}/launch` → `BellLabsRunWorkflow` → family → `OperationWorkflow` → `DeploymentOperationRuntime` → Deep Agents adapter → `gpt-5.6-luna`, with the pinned skill, Tavily MCP, browser tool, an in-process sync child and a hosted async child on `rrm009-agent-server`.

**Run record (honest count).** Five invocations of the live module:

| # | Head | Outcome |
|---|---|---|
| 1 | before `7e21bd4` | **failed** `generic_artifact_operation_failed` after about 98 s. The typed failure diagnostics were added afterwards; the cause was **not captured** and the run's temp directory was rotated away. Spent model tokens. |
| 2 | before `7e21bd4` | failed before any model call: port 7341 was still held by run #1's leaked dev server (led to the `finally` teardown fix `7e21bd4`). No model spend. |
| 3 | `7e21bd4`–`fb321ad` range | passed |
| 4 | `7e21bd4`–`fb321ad` range | passed |
| 5 | `2f3a642` (final) | passed |

Of the four runs that reached the model, **3 passed and 1 failed with an uncaptured cause**. The live module was not re-run on `6d956e9` (its code and the live module are unchanged since `2f3a642`; `6d956e9` only adds assertions to the technical StageGraph test).

**Final run (`2f3a642`).** Run `75990b37-…`; 10 parent turns plus 1 sync-child turn = 87,173 tokens; async child `bf67c7e3-…` 2,263 tokens (`provider_attributed`), provider run `01a0fe17-…`, graph binding `sha256:764f0ef5…`; report artifact `sha256:629b9cdd…`; terminal `completed`. Each passing run took about 54–90 s. Credential checks against the live server: claim + same scope 200, claim + other scope header 403, raw static token 401, no auth 401.

**Spend (estimate).** About **370k model tokens, ~99 % input**: about 4 model-reaching live runs × about 90k tokens, plus the RRM-013 drills (about 10k), plus about 4 Tavily basic searches. Only the final run's tokens are exact (87,173 + 2,263); the other runs are estimated from it, and run #1's actual usage was not captured, so the total could be off by tens of thousands of tokens either way. Exact `gpt-5.6-luna` prices were not available to the implementing sessions; at an assumed USD 2.50/M input and USD 15/M output the total is **under about USD 1.10**, well under the USD 5 cap. The third session made no live call (USD 0).

**Technical qualification (deterministic model, no provider spend).** StageGraph about 150 s (fork, relay drill with a Temporal restart, inspection, Visibility), GoalDirected about 65 s (2 iterations, `workspace_mode="fresh"`), generic artifact about 25 s. StageGraph runs admit an empty baseline reservation (RRM-021), as the WP-BP-010 live gate does.

## Replay and recovery artifacts

| Command | Result |
|---|---|
| `pytest tests/integration/temporal/{test_rrm_007_replay_pre_change_histories,test_rrm_016_goal_directed_journaled,test_wp_bp_010_temporal,test_wp_bp_020_temporal,test_rrm_004_worker_restart_recovery}.py tests/unit/operations/test_operation_execution.py -k "replay or Replayer or replays or pre_change or post_change"` (DSNs, on `6d956e9`) | 12 passed, 49 deselected |
| Technical StageGraph qualification | the root and family histories of the source and derived runs replay (`_replay` over the production workflow set) |

RRM-009 changes no workflow code (no new `workflow.patched`); every captured-history replay suite is unchanged and passes inside the hermetic and DSN totals.

## Replacement and deletion checks

- The draft's demo-shaped test injection is replaced by `ProductionWorkerActivityCompositionFactory` built inside the worker; the old "launch requires a deployment factory" refusal is gone by design (recorded above).
- The Agent Server's static bearer token is replaced by signed scope claims. An image built before RRM-009 rejects claims and must be rebuilt after merge: the `rrm013-*` stack still runs the old image and was left untouched. The current stack is `rrm009-*`; the rebuild recipe is under "Server stacks".
- No owner was deleted. No competing runtime or store was added: everything composes existing ports.

## Unresolved risks and drift checks

- **Items 1 and 6 are now proven** (fourth session, "Integration with RRM-008"). S3 is qualified against MinIO, not AWS. Three RRM-008 composition items stay unresolved and are listed there: session-generation admission, the superseded-generation windows, and `GenericArtifactWorkflow` outside the saga.
- **Firecrawl MCP** pinned and digest-verified but not invoked live.
- **Live run #1's failure cause is unknown**; later runs passed 3/3.
- **Sync-child usage after a crash.** Child usage is observed live at the model boundary; a terminal reconstruction after a crash (no live invocation) recounts only the parent's checkpointed messages, so child usage is under-counted in that window.
- **Async completion wait inside `operation.execute`** now heartbeats (RRM-008), and a cancel reaches it (the `completion_wait` drill).
- **Child overage** (2,263 tokens against a contract limit of 8,000 here; RRM-008 saw 2,345 against 5) is recorded as consumption; the incident policy is RRM-010's call.
- **RRM-020** (shared GoalDirected workspace across iterations; strict xfail; technical GoalDirected uses `fresh`) and **RRM-021** (StageGraph never settles a baseline reservation → `budget_not_settled`; technical StageGraph admits an empty baseline) are open.
- **RRM-013 crash-window drills** were not re-run against the scope-claim server (spawn, cancel and restart drills were). The RRM-013 "one LangSmith project / parent filter re-check" item was not exercised (tracing off).
- `start_local` with a database file stands in for a persistent namespace (restarted once in the relay drill); no production Temporal cluster was used.
- **Leaked Mongo databases from earlier sessions.** `rrm009_b8cd764aeb47` and `rrm009_dc8ba5c37292` were dropped by exact name under the lock in the fourth session. No `rrm009_*` database remains.

### Incident: an unlocked DB run (2026-10-02 15:51:06–15:52:30 EDT, about 80 s)

While **RRM-008 held the stack lock**, a second-session RRM-009 command ran `test_rrm_009_production_composition.py -k stagegraph` **without the lock**: the shell chain `acquire && …; …` continued after the acquire timed out. In that window it reset `belllabs_control` on the shared PostgreSQL (drop + migrations), created the `rrm009_langgraph` schema, created the Mongo database `rrm009_f5f3768651f4` (not dropped because the process was killed) and started a Temporal dev server on port 7341 (killed). The processes were killed by exact PID; nothing of RRM-008's was touched. **Any RRM-008 DSN run in that window may have been disturbed and should be re-run.**

Cleanup (third session, under the lock): the incident's database was identified by its ObjectId timestamps (19:51:44–19:52:13 UTC = 15:51:44–15:52:13 EDT, StageGraph templates and candidates) and dropped by exact name: `rrm009_f5f3768651f4`. No other database was touched. Prevention: lock commands are written `if python … acquire RRM-009; then …; python … release RRM-009; fi` or run as a separate step whose output must read `acquired`.

### Deployment runbook

Prerequisites:
1. Application PostgreSQL: migrations through 0025 applied as the owner (`APPLICATION_MIGRATION_DATABASE_DIRECT`); the least-privilege runtime login (`APPLICATION_DATABASE_DIRECT`, member of `belllabs_control_runtime`); a family-writer login (`APPLICATION_FAMILY_WRITER_DATABASE_DIRECT`, member of `belllabs_family_repository_writer`). Atomic family admission refuses to run as the owner.
2. LangGraph saver/store: `LANGGRAPH_CHECKPOINT_DATABASE_DIRECT`, `LANGGRAPH_CHECKPOINT_SCHEMA` (default `belllabs_langgraph`); `LANGGRAPH_CHECKPOINT_SETUP=1` once.
3. Mongo (`MONGODB_URI`, `MONGODB_DATABASE`); payload store (`S3_BUCKET` or `ARTIFACT_PAYLOAD_ROOT`).
4. Persistent Temporal namespace (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_TASK_QUEUE`).
5. Agent Server built from a scope-claim revision (`langgraph.async_subagents.json`; `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN` is the HMAC secret).
6. Credentials by reference only (`OPENAI_API_KEY`, `TAVILY_API_KEY`, `FIRECRAWL_API_KEY`, `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`); Node for the stdio MCP servers and the browser tool (`WEB_RESEARCH_AGENT_BROWSER_NODE`); `.agents/skills/agent-browser` and `.tools` laid out beside the checkout (the pins resolve `workspace://`).

Launch (each in its own process):

```powershell
uv run python scripts/register_belllabs_search_attributes.py        # administrative, idempotent
$env:COORDINATOR_LAUNCH_ENABLED="1"; uv run python -m app.temporal.worker
$env:RUN_CONTROL_TEMPORAL_ENABLED="1"; uv run uvicorn app.server:asgi_app --host 127.0.0.1 --port 8000
```

Flags: `BOUNDARY_RELAY_REQUEST_SCOPES`, `BOUNDARY_RELAY_INTERVAL_SECONDS`, `INSPECTION_CURSOR_KEY` (same on every replica), `OPERATION_JOURNAL_CLAIMED_BY` (deployment-stable), `ASYNC_SUBAGENT_SUBMITTER_IDENTITY`, `ASYNC_SUBAGENT_SPAWNING_ENABLED`, `AGENT_SERVER_ENDPOINT`, `ASYNC_SUBAGENT_COMPLETION_WAIT_SECONDS` (default 120), `CAPABILITY_PINS_PATH`, `DEEP_AGENT_SANDBOX_WORKSPACE_ROOT`, `LANGSMITH_TRACING` (default off). Live opt-ins: `BELLABS_RUN_RRM_009_LIVE=1`, `BELLABS_RUN_RRM_013_LIVE=1`.

Running cancellation (fourth session): `OPERATION_HEARTBEAT_TIMEOUT_SECONDS` (30), `OPERATION_ASYNC_CHILDREN_HEARTBEAT_TIMEOUT_SECONDS` (15), `OPERATION_BOUND_HEARTBEAT_TIMEOUT_SECONDS` (30), `WORKER_GRACEFUL_SHUTDOWN_SECONDS` (10, must be shorter than every heartbeat timeout, checked at worker start); migration 0026; the privileged `POST /run-control/v1/runs/{run_id}/async-children/{child_execution_id}/reconcile-usage` (role `reconciliation_operator`). S3: `S3_BUCKET` with the AWS credential chain, `AWS_ENDPOINT_URL_S3` for an S3-compatible server. Live and container opt-ins: `BELLABS_RUN_RRM_009_LIVE=1` (also the async cancellation cases), `RRM009_S3_ENDPOINT` / `RRM009_S3_ACCESS_KEY` / `RRM009_S3_SECRET_KEY`.

Drain before deploy: GoalDirected runs started before RRM-016; `settle_pending_usage` redeliveries from before RRM-013; stop workers within `WORKER_GRACEFUL_SHUTDOWN_SECONDS`.

Cleanup (disposable stacks): stop the three processes; drop `belllabs_control` and the LangGraph schema; delete the dev-server database file; Agent Server teardown by exact names only, never prune: `docker compose -p rrm009-agent-server down`, `docker rm -f -v rrm009-agent-server-postgres`, `docker volume rm rrm009-agent-server-pgdata`. (Left running now for RRM-010.)

### What the draft got wrong (fixed forward by the second implementer)

1. Sync-subagent permissions passed as dicts → `create_deep_agent` `AttributeError` (`rule.mode`) for every binding with a sync child. Fixed with typed rules and a regression test that fails on the old code.
2. GoalDirected executor and verifier of one iteration collided on `/goal/{n}` (`WorkspaceSlotConflict`). Fixed: the role root is the ownership boundary.
3. A `shared` GoalDirected workspace with RRM-016 iteration-rooted slots → `IdempotencyConflict` at iteration 2. **Not fixed**: RRM-020 + strict xfail.
4. StageGraph never settles a baseline reservation → `budget_not_settled`. **Not fixed**: RRM-021.
5. Both StageGraph stages' `/workspace/output` slot in one namespace → review stage `WorkspaceSlotConflict` while the run still "completed". Fixed: one namespace per stage.
6. Relay drill built `PauseAction()` / `ResumeAction()` without decisions. Fixed.
7. Generic artifact binding lacked `artifact.promote`; wrong output contract. Fixed.
8. Settlement facts read from a Mongo document the journaled path never writes, and the journaled path dropped all runtime event payloads. Fixed in the app (payload persistence) and the test reads the journal's digest-bound payloads.
9. Loose assertions (`!= "unavailable"`, `in {202, 409}`, `… or _calls`, a list-shaped `sections`) tightened to exact ones; Visibility asserts exact sets.
10. Digest guard failure on `browser_tool.args_schema: type[BaseModel]`. Fixed (narrowed annotation).
11. Search Attributes were pre-registered by the dev server; the qualification now proves the API refuses before the administrative registration and that registration is idempotent.
12. Migration numbered 0026 on a false premise; renumbered 0025.
13. The worker test's refusal changed from "requires a factory" to a readiness refusal: accepted as a design change.

### RRM-008 seams the coordinator must wire at merge (base has none of these)

**Done in the fourth session.** Items 1–9 are wired and proven as described in "Integration with RRM-008". For item 10, `rrm009-*` is the current server stack and `rrm013-*` is stale (see "Server stacks"). The original list is kept below as the record of what was handed over.

1. `agent_cognitive_activities` returns `(execute, cancel)` on RRM-008: confirm `create_production_workers` / `create_agent_cognitive_worker` registers `operation.cancel` on the cognitive worker and on the generic-artifact worker (which also runs `operation.execute`).
2. `OperationExecutionService(children=…)` in `ProductionWorkerActivityCompositionFactory.build`: the cancel path needs a per-operation provider adapter with the operation's resolved credential. Compose a small port that resolves `binding.secret_refs` (`EnvironmentSecretResolver`), builds the service with `ProductionAsyncSubagentMiddlewareFactory.service(binding, secrets)` and calls `cancel_children`.
3. Cancel delivery through the boundary transport: the API already composes `TemporalBoundaryCommandTransport` and the relay; make the relay (or RRM-008's path) deliver `cancel` in its own sequence space.
4. `heartbeat_timeout_seconds` per operation class on `OperationWorkflowRequest` (family preparers/templates); worker graceful shutdown shorter than it.
5. Send `liability_reconciled` to the family after `reconcile_usage` / `reconcile_unit` / operator decisions (no production caller of `reconcile_usage` exists yet).
6. `DeploymentOperationRuntime` forwards unknown attributes to the adapter (`observe_latest` works); it must not swallow `CancelledError` (it does not).
7. RRM-008's `AsyncSubagentService.decide_result` records the decision in run control, so `AsyncChildCompletion` then also unblocks family terminalization for blocking children (base: link + authority only).
8. Expected textual conflicts: `adapter.py`, `journaled_operation_execution.py`, `operation_execution.py` (RRM-008 only), `deployment_composition.py` / `worker.py`.
9. Settle an operation `cancelled` only on a real cancel request (RRM-008 F1); nothing in RRM-009 settles on shutdown or timeout.
10. Rebuild the `rrm013-*` Agent Server image from integration after merge (static-token auth is gone), or use `rrm009-*`.

### Reusable seams (mission-horizon lens)

- `capability_lineage`: a sanitized per-invocation capability record (credential refs by name, filter/schema/tool/skill digests, placement, argument/result digests, usage).
- `_ModelCallObserver`: subordinate usage observed at the model boundary and charged to the parent.
- `AsyncChildCompletion` + `admit_typed_manifest`: policy-registered child admission and settlement at a parent boundary.
- `DeploymentOperationRuntime`: a runtime decorator for egress scoping and post-cognition steps; `granted_network_hosts`: grant-scoped tool egress.
- `slot_ownership_boundary`: role-root workspace ownership.
- `CapabilityPins` + `scripts/pin_research_capabilities.py`: a digest-pinned capability catalog.
- `BoundaryCommandRelay`, `RunLaunchService` (governed launch with fork lineage), `mint_scope_claim` / `verify_scope_claim`.
- Fourth session:
  - `OperationHeartbeatPolicy`: the heartbeat timeout per operation class, plus the drain check.
  - `ProductionAsyncChildCancellation`: credential-resolving child cancel for any composed operation boundary.
  - `FamilyLiabilityHints` + `TemporalFamilyLiabilityHint`.
  - `AsyncChildUsageReconciliation` and its privileged route.
  - `producer_effects_settled`.
- **RRM-010 can reuse:**
  - `run_cancellation_drill` (`tests/fixtures/rrm009_cancellation.py`) over `open_production_stack`, the core of the combined smoke. It cancels through the facade with a real hosted child running (during cognition, during the boundary's wait, or after a worker restart), then runs the privileged reconciliation and ends with a terminal `cancelled` run and `applied` receipts.
  - `CancellationGate`, `SpawningModel`, `GatedChildModel`, `async_child_binding`, `cancellation_components`, `promote_generic_artifact` and the S3 qualification module.
  - Also: `open_production_stack` (`tests/fixtures/rrm009_production_stack.py`: API + workers, persistent namespace, Search Attribute registration; `components=None` + the rrm009 Agent Server for a combined smoke), the `_admit` / `_launch` / `_send` / `_command` / `_release_wait` / `_operation_payloads` / `_lineages` helpers, the relay drill, the live binding (`tests/fixtures/rrm009_live_capabilities.py`) for an active real async child, and the running `rrm009-agent-server` stack.

None of these carries company, fixture or provider specifics.

## Review disposition

Independent review of RRM-009 (after the RRM-008 integration): verdict **`approve_with_fixes`**. All fixes were made as new commits on `wp/rrm-009-capability-composition`, with no amend. Fix commits: `6f769ce` (items 1, 2 and 6), `5b01385` (item 3), `d5b34fa` (item 4), `6990a8d` (items 5 and 7). The evidence commit follows them.

| # | Finding | Disposition |
|---|---|---|
| 1 (blocking) | The MCP module digest was verified only by the authoring helper `PinnedMCPServer.component()`. The worker checked only the schema digest and launched whatever `command`/`arguments` the binding named. | **Fixed (`6f769ce`).** `PinnedCapabilityAssetVerifier.verify_launch` (`app/integrations/operation_runtime_ports.py`) runs in `verify_servers`, and in `DeploymentOperationRuntime` before both `execute` and `observe_latest`, because the cancel path also materializes the agent and so starts its stdio servers. Each Deep Agent MCP component must satisfy one of two rules. **Pinned:** the component equals the one re-derived from its pin. That re-derivation re-reads the module from disk and checks its digest and the package name and version, and fixes the command (the deployment's `WEB_RESEARCH_AGENT_BROWSER_NODE`), the arguments (the pinned module), the credential refs, the tool filter and the schema digest. **Registered:** the component exactly equals one the deployment registered (`DeploymentCapabilityComponents.mcp_servers`; used only by the qualification harnesses for their Python fixture server). Anything else is refused before launch. Test: `tests/unit/integrations/test_capability_pins_and_runtime_ports.py::test_worker_launches_only_the_pinned_mcp_module_command_and_arguments`. It covers a different command, a different module, and dropped credentials, each keeping the pinned schema digest; an unpinned ref; no node configured; a tampered module file; and registered equality. Also `tests/unit/operations/test_rrm_009_cancellation_composition.py::test_deployment_runtime_verifies_the_mcp_launch_before_any_materialization`. **Residual (documented, not done):** the pin covers the entry module and `package.json`, not the package's transitive `node_modules` tree. Hashing that tree means re-pinning both MCP packages and re-deriving their schema digests, which is a larger, separate change. |
| 2 | Egress failed open when no grant was bound; non-canonical IPv4 forms were accepted. | **Fixed (`6f769ce`).** `agent_browser_page` refuses every page when `GRANTED_NETWORK_HOSTS` is unbound. `_public_host` refuses every inet_aton-style numeric form: each part is parsed as decimal, hex or octal and the whole is normalized to an `ipaddress.IPv4Address`. It also refuses numeric or hex final labels, IPv6 literals, and `.local`/`.localhost`. A granted name is resolved first and refused if any address is non-global; a resolver can be injected for tests. Tests: `…::test_browser_tool_reaches_public_hosts_by_name_only` (nine new forms, including `169.254.169.254`), `…::test_browser_tool_opens_only_hosts_the_operation_was_granted` (unbound case), and `…::test_browser_tool_refuses_a_granted_name_that_resolves_to_a_private_address`. The hosted async child mounts only `wait_seconds` (`app/agent_server/async_subagents/bindings.py`), never the browser. The Tavily MCP is marked worker-mediated, not grant-bound, in the requirement map. **Residual:** DNS rebinding between this check and the browser's own resolution. |
| 3 | The lineage stored the raw model-chosen `requested_url`; there was an unkeyed `arguments_digest`; `subagent_type` was unbounded; the "no prompt text" claim was too broad. | **Fixed (`5b01385`).** The lineage keeps scheme, host and path only (no query, fragment or user info), and path length is bounded. The arguments digest is dropped; it was unused by any consumer and allowed confirming guesses of short arguments. `subagent_type` is recorded only for a bound sync child, otherwise as a fixed placeholder. The Skill read path is bounded. The claim is scoped in the module docstring and the requirement map (see "Sanitized lineage"). Test: `tests/unit/integrations/test_sync_subagent_usage_and_lineage.py::test_invocation_records_keep_no_argument_value_and_bound_model_chosen_strings`. |
| 4 | The scope claim lived 12 h by default (7 d maximum); there was no `jti`; the symmetric-key risk was undocumented. | **Fixed (`d5b34fa`).** Default lifetime 30 min, maximum 1 h, and a random 128-bit `jti`. The verifier refuses a correctly signed claim whose declared lifetime exceeds one hour or that lacks a well-formed `jti`. A design note is in `app/agent_server/async_subagents/auth.py` and is recorded below. Test: `tests/unit/agent_server/test_async_subagent_server_offline.py::test_identity_route_requires_the_deployment_credential` (lifetime bound, forged 7-day claim refused, claim without `jti` refused, distinct `jti`). |
| 5 | `/health/ready` was unauthenticated and disclosed the namespace, relay tenant scopes and checkpointer digests. | **Fixed (`6990a8d`).** It returns `{"status", "mode"}` only. The composition details are logged once by `compose_runtime_control` and kept on `app.state.runtime_control`. The StageGraph qualification asserts the exact public body. |
| 6 | Missing pins yielded empty pins; partial factory builds leaked; a worker-start failure leaked the composition. | **Fixed (`6f769ce`).** `CapabilityPins.from_settings` raises `CapabilityPinError` when the file is absent; `required=False` is for tooling only. Both production callers use the default. `ProductionWorkerActivityCompositionFactory.build` closes its `AsyncExitStack` on any failure. `production_workers_or_close` closes `composition.resources` when `create_production_workers` raises, and `main` uses it. Test: `…::test_composition_resources_are_closed_when_build_or_worker_start_fails`. |
| 7 | `assert reduction.projection.terminal_outcome is not None` in the run-control service. | **Fixed (`6990a8d`).** It now raises `CommandRejected` explicitly. |

**Unresolved gates added by the review:**
- **Symmetric scope-claim key.** Any holder of `BELLABS_ASYNC_SUBAGENT_SERVER_TOKEN`, meaning every parent worker, can mint a claim for any request scope. A compromised worker is therefore not confined to its tenant. The production follow-up is asymmetric signing with issuance bound to worker identity, or per-scope keys. Claims can also be replayed within their lifetime (no `jti` cache).
- **Server image.** The running `rrm009-agent-server` image was built before `d5b34fa`, so it does not yet enforce the one-hour maximum or the `jti` requirement. The new parent claims (30 min, with `jti`) are accepted by it unchanged. Server-side enforcement is proven in process by the offline server test. Rebuild the stack from this branch (recipe under "Server stacks") before relying on it for RRM-010.
- MCP transitive dependencies are not pinned (item 1); DNS rebinding remains (item 2).

### Gates after the review fixes (code head `6990a8d`)

| Gate | Result |
|---|---|
| ruff, mypy | All checks passed; no issues in 383 source files |
| owning suites, hermetic | **608 passed, 21 skipped, 1 xfailed** (154 s) |
| owning suites, DSN | inside the full DSN run below (production qualification 3, cancellation drills, RRM-008 demo, RRM-009 grants, coordinator runtime, WP-CP-040/045) |
| replay | **12 passed, 49 deselected** |
| full hermetic, one process | **1064 passed, 90 skipped, 3 xfailed, 0 failed** (201 s) |
| full DSN, one process (background, lock held, nothing else running) | **1118 passed, 36 skipped, 3 xfailed, 0 failed** (703 s). The first complete run (643 s) had 1 failure: `tests/acceptance/control_plane/test_rrm_007_interventions.py::test_interventions_on_real_temporal_with_postgres_authority` (`boundary_state["paused"]` was `None` on worker 2). This is the race in that test which RRM-008's evidence already records. It passed 3/3 alone (23–26 s), and the complete re-run given here has 0 failures; no test was retried selectively. |
| S3 object store (`rrm009-minio`, recreated and removed again) | **1 passed** (36 s; bucket `rrm009-artifacts-206f9bba`, 3 objects). The container, its volume and its image were removed by exact name afterwards. |
| live cancel proof (the claim path changed) | **4 passed** (140 s): sync child; async `cognition`, `completion_wait` and `restart_then_cancel`. The `-k` filter matched all four cases. Hosted children cost 2,343 + 2,282 + 2,282 attributed tokens (about 7k tokens, under USD 0.03 at the assumed prices). Runs `17e8a089-…` (sync), `5ee23bdf-…`, `1b020c0e-…`, `95eb1359-…`; terminal 0.5–0.6 s after the refused proposal. |
| `git diff --check 56ffd63 HEAD` | clean |

Spend across RRM-009's fourth session including the review re-run: about 42k hosted-child tokens, under USD 0.20 at the assumed prices.

## Final disposition

ready_for_review
