# RRM-009 implementation evidence

Disposition: ready_for_review (implemented; independent review pending; CP-050 is **not** accepted by this ticket, it only qualifies the CP-050 capability prerequisite)
Recorded date: 2026-10-02 (America/New_York)
Qualification identity: RRM-009 qualify the CP-050 capability prerequisite: a production-shaped composition. Requirements: DA-001 to DA-015 and the capability-binding requirements, REQ-CP-DA-004 (persistent saver, clarified), REQ-CP-DA-007 (subordinate usage charged to the parent), REQ-CP-DA-016 (`durability="sync"`), REQ-CP-DA-019, REQ-CP-EXEC-015 (Search Attribute registration and verification), REQ-CP-RUN-011/012 (inspection composition), RRM-007 F6 (boundary delivery relay), RRM-013 N8 (scope-bound Agent Server credential) (AMD-RRM-001, accepted meta `main` `a50d833`).
Base revision and head revision: base `c8221f5` (integration: RRM-016 hardening, RRM-018, RRM-019). Tested code head `6d956e9` (all final gates below ran on it). The commit that adds this README and the ticket/index updates changes documentation only. Branch `wp/rrm-009-capability-composition`; not merged, not pushed (the coordinator owns review and merge).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12; temporalio 1.30.0 (`start_local` dev server with a database file), deepagents 0.7.5, langgraph 1.2.10, langgraph-checkpoint 4.1.1, langgraph-checkpoint-postgres 3.1.1, langgraph-sdk 0.4.2, langchain-mcp-adapters 0.3.1, pydantic 2.13.4, pymongo 4.17.0, beanie 2.1.0, asyncpg 0.31.0, psycopg 3.3.4, pytest 8.4.2, ruff 0.15.22, mypy 1.20.2. Agent Server: `langchain/langgraph-api` 0.12.0 image built from this worktree (`langgraph up --config langgraph.async_subagents.json --api-version 0.12.0 --no-pull`), served graph `belllabs_async_technical_child`, binding digest `sha256:764f0ef5…`. Model: `gpt-5.6-luna`, pinned model definition `model.wp-cp-040` (reasoning low, verbosity low, Responses API, max 2000 completion tokens). Pinned capabilities (`infra/capability-pins/research-capabilities.json`, schema `belllabs.capability-pins.v1`): skill `skill.agent-browser` (`sha256:30722859…`), tool `tool.agent-browser-page` (agent-browser 0.33.0), MCP `mcp.tavily` (tavily-mcp 0.2.21, 5 tool schemas) and `mcp.firecrawl` (firecrawl-mcp 3.22.4, 26 tool schemas), checkpointer/store/sandbox/model refs of WP-CP-040.

## Worktree provenance

- Worktree `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-009`, branch `wp/rrm-009-capability-composition`, from integration `c8221f5`. The main checkouts were not touched. `.env` was never copied or printed: service and live gates load it with `uv run --env-file ../biotech-research-ingestion-evaluation-system/.env`; the Agent Server secret and its Postgres password live in a scratch env file outside the repository and were never printed or committed.
- Disposable shared stack: `rrm-app-postgres` (`127.0.0.1:55432/belllabs`) and `rrm-app-mongodb` (`127.0.0.1:27017`), serialized by the shared stack lock (`stack_lock.py acquire/release RRM-009`). One pytest process at a time. The demonstrations create the saver schema `rrm009_langgraph` and per-test Mongo databases `rrm009_<12 hex>`. One lock violation happened (see "Incident" below).
- RRM-009's own Agent Server stack (needed because this branch's auth is a signed scope claim; the `rrm013-*` server still runs the pre-RRM-009 static-token auth and answers a claim with 401): compose project `rrm009-agent-server` (`rrm009-agent-server-langgraph-api-1` at `http://127.0.0.1:8144`, `rrm009-agent-server-langgraph-redis-1`) and `rrm009-agent-server-postgres` (`127.0.0.1:55434`, volume `rrm009-agent-server-pgdata`). Left running for RRM-010. The `rrm013-*`, `rrm-app-*` and `csi01` containers were never stopped or removed.
- Migration slot 0025 is used by this ticket (`0025_operation_claim_unit_key_runtime_grant_v1.sql`, grant only); RRM-008 adds no migration.

### Handoff note (three sessions)

1. **First implementer** (`9ede5cd`, `82b3489`, `5d96c83`): the deployment composition, governed launch, scope-claim auth and pins. Its acceptance test did not pass and its last commit was a WIP preserve at handoff. The second implementer treated all three as an unreviewed draft.
2. **Second implementer** (`6cec836` to `6d956e9`): audited the draft, fixed more than ten defects forward (no amend, see "What the draft got wrong"), added the missing pieces (sanitized lineage, sync-child usage, parent-boundary async completion, grant-scoped egress, live qualification, SET ROLE test, launch baseline check), ran every gate on `2f3a642`, and wrote the runbook and tickets RRM-020/RRM-021 (`9e2e038`). At handoff it committed two Visibility assertions that it could not run because RRM-008 held the stack lock (`6d956e9`).
3. **Third session** (this README): ran the StageGraph qualification with `6d956e9`'s assertions under the lock (passed, no fix needed), re-ran the full gate set on `6d956e9`, dropped the Mongo database the incident leaked, and wrote this evidence and the ticket/index updates. No code changed in this session.

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

## Requirement-to-evidence map

| Ticket item / requirement | Test | Observed assertion |
|---|---|---|
| Real PostgreSQL, Mongo definitions/bindings, object storage, persistent saver/store through existing ports | `tests/acceptance/control_plane/test_rrm_009_production_composition.py::test_stagegraph_runs_through_the_production_composition_with_fork_relay_and_inspection`, `::test_generic_artifact_operation_promotes_the_captured_report_durably`; `tests/unit/integrations/test_capability_pins_and_runtime_ports.py::test_filesystem_payload_store_is_content_addressed_and_verified` | saver checkpoints in `rrm009_langgraph` asserted > 0 (45 observed for the StageGraph run); inspection checkpoint history `current`; every workspace candidate's `object_ref` exists under the payload root; the promoted artifact has a durable reference row; the CAS store refuses a digest mismatch. **S3 path not qualified** |
| Canonical registries/queues, exact placement, no demo runtime | `…::test_stagegraph_runs_through…` (lineage `placement.task_queue == "rrm009-agent-cognitive"`); `tests/integration/temporal/test_coordinator_temporal_runtime.py::test_production_worker_fails_closed_before_startup_without_real_adapters` | the binding's cognitive queue is served by `create_production_workers`; with launch enabled and an unregistered namespace the worker raises `SearchAttributeRegistrationError` before any store is opened (this replaces the old "requires a deployment factory" refusal: the factory is now built in; a design change, not a weakened assertion) |
| Skills/tools pinned, mounted/disclosed, invoked | `tests/unit/integrations/test_capability_pins_and_runtime_ports.py::test_pin_file_is_exact_and_discloses_no_secret`, `::test_workspace_artifacts_verify_against_their_pins_when_present`, `::test_workspace_locators_cannot_escape_the_workspace`, `::test_pinned_verifier_admits_exact_bindings_and_refuses_drift`; live `tests/acceptance/control_plane/test_rrm_009_live_capabilities.py::test_pinned_capabilities_and_both_subagents_run_in_the_production_composition` | exact pins, no secret value in the file, drift refused; live `invoked` (below) includes the skill read, `tavily_search`, `agent_browser_page`; the disclosed skill bundle digest and MCP tool filter digests equal the pins |
| Mediated/constrained egress, isolation kept | `…::test_browser_tool_reaches_public_hosts_by_name_only`, `…::test_browser_tool_opens_only_hosts_the_operation_was_granted`; live lineage | IP literals (IPv4 and IPv6) and private hosts refused; a host outside the operation's `network_hosts` grant refused; live lineage discloses the egress grant the browser was bound by |
| Report slots and artifact promotion via governed contracts | `…::test_generic_artifact_operation_promotes_the_captured_report_durably`; live | captured candidate promoted through `GenericArtifactWorkflow`; live report `artifact://tenant-1/75990b37-…/463ca2fa-…` (`sha256:629b9cdd…`) contains "Example Domain" and the child's `CHILD-OK` |
| Sync subagent in-process | `tests/unit/integrations/test_sync_subagent_usage_and_lineage.py::test_sync_subagent_runs_and_its_usage_is_charged_to_the_parent`; technical StageGraph lineage | the materializer hands deepagents typed `FilesystemPermission` rules (the dict form raised `AttributeError: rule.mode`; this test fails on the old code); child usage appears in the parent's usage; technical: `invoked == {"framework": ["write_file"], "mcp": ["lookup_binding_marker"], "sync_subagent": ["task"]}`, one child call per operation; live: one `subordinate` model call charged |
| Async subagent on the Agent Server: reservation, dependency/result decision, cancel/reconnect | `tests/unit/operations/test_async_child_parent_completion.py::test_completed_blocking_child_is_admitted_and_settled_once_at_the_boundary`, `::test_active_child_and_unregistered_policy_are_left_undecided_and_unsettled`, `::test_failed_child_is_rejected_and_its_unattributed_usage_stays_pending`, `::test_completion_bounds_are_validated`, `::test_deployment_runtime_completes_children_after_cognition_and_delegates_the_rest`; live; RRM-013 live drills against the scope-claim server | admitted and settled once; active/unregistered left undecided; failed rejected with pending usage; live: reservation and link before submission, `REQUIRED_BLOCKING`, admitted under `policy:async-result:technical-child@1`, `usage_disposition settled`, 2,263 tokens attributed; drills `spawns_one_real_child`, `cancel_reaches`, `restart_during` passed (cancel acknowledged, restart reconnects) |
| Sanitized lineage | `tests/unit/integrations/test_sync_subagent_usage_and_lineage.py::test_credential_references_are_names_only_and_cover_every_mount`; `test_rrm_009_production_composition.py` (lineage read from the journal's digest-bound payloads); `tests/unit/operations/test_operation_execution.py::test_journaled_operation_settles_usage_effect_and_terminalizes` (event payloads persisted and restored); live | `credential_refs == ["environment:OPENAI_API_KEY"]` (technical); every mount's credential appears by reference only; live asserts no credential value appears in any durable payload |
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
- **Async (Agent Server).** Live: hosted technical child `bf67c7e3-…`, provider run `01a0fe17-…`, graph binding `sha256:764f0ef5…`; reservation and link recorded before submission (RRM-013 service); `REQUIRED_BLOCKING`; admitted at the parent boundary by `policy:async-result:technical-child@1`; settled (`usage_disposition settled`, 2,263 tokens `provider_attributed`). Cancellation and reconnect: RRM-013 live drills re-run against the scope-claim server, `BELLABS_RUN_RRM_013_LIVE=1 RRM013_AGENT_SERVER_API_CONTAINER=rrm009-agent-server-langgraph-api-1 … -k "spawns_one_real_child or cancel_reaches or restart_during"` → 3 passed, 3 deselected (296 s). Crash-window drills were not re-run.

## Changed paths and migrations

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
- The Agent Server's static bearer token is replaced by signed scope claims; an image built before RRM-009 (the `rrm013-*` stack) rejects claims and must be rebuilt after merge.
- No owner was deleted. No competing runtime or store was added: everything composes existing ports.

## Unresolved risks and drift checks

- **Ticket items left unchecked (third session's reading; the second implementer's handoff called every item met).** Item 1 (object artifact storage): the S3 path is composed but not qualified. Item 6 (subagents): async cancellation and reconnect were re-proven only by the RRM-013 live drills against the scope-claim server, not inside the production worker composition (its cancel path waits on RRM-008 seams 1–3 below); sync-child cancellation was not exercised. The reviewer decides whether these are acceptable for the prerequisite.
- **S3 artifact store not qualified** (filesystem CAS only). **Firecrawl MCP** pinned and digest-verified but not invoked live.
- **Live run #1's failure cause is unknown**; later runs passed 3/3.
- **Sync-child usage after a crash.** Child usage is observed live at the model boundary; a terminal reconstruction after a crash (no live invocation) recounts only the parent's checkpointed messages, so child usage is under-counted in that window.
- **Async completion wait inside `operation.execute`** has no heartbeat on base; RRM-008 adds heartbeats.
- **Child overage** (2,263 tokens against a contract limit of 8,000 here; RRM-008 saw 2,345 against 5) is recorded as consumption; the incident policy is RRM-010's call.
- **RRM-020** (shared GoalDirected workspace across iterations; strict xfail; technical GoalDirected uses `fresh`) and **RRM-021** (StageGraph never settles a baseline reservation → `budget_not_settled`; technical StageGraph admits an empty baseline) are open.
- **RRM-013 crash-window drills** were not re-run against the scope-claim server (spawn, cancel and restart drills were). The RRM-013 "one LangSmith project / parent filter re-check" item was not exercised (tracing off).
- `start_local` with a database file stands in for a persistent namespace (restarted once in the relay drill); no production Temporal cluster was used.
- **Leaked Mongo databases from earlier sessions.** `rrm009_b8cd764aeb47` and `rrm009_dc8ba5c37292` (GoalDirected documents, created about 12:08 and 12:12 EDT by earlier killed runs, not by the incident) remain on `rrm-app-mongodb`; the coordinator decides whether to drop them.

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

Drain before deploy: GoalDirected runs started before RRM-016; `settle_pending_usage` redeliveries from before RRM-013; after RRM-008, give workers a graceful shutdown shorter than `heartbeat_timeout_seconds`.

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
- **RRM-010 can reuse:** `open_production_stack` (`tests/fixtures/rrm009_production_stack.py`: API + workers, persistent namespace, Search Attribute registration; `components=None` + the rrm009 Agent Server for a combined smoke), the `_admit` / `_launch` / `_send` / `_command` / `_release_wait` / `_operation_payloads` / `_lineages` helpers, the relay drill, the live binding (`tests/fixtures/rrm009_live_capabilities.py`) for an active real async child, and the running `rrm009-agent-server` stack.

None of these carries company, fixture or provider specifics.

## Final disposition

ready_for_review
