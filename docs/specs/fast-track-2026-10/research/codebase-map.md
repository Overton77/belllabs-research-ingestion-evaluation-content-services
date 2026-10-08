---
type: Research Note
title: Codebase map for the fast-track features (read 2026-10-07)
description: "File-path-anchored survey of src/mission_control and the db-contract package for catalog and capabilities, agentic components, execution families and context transfer, harness and lanes, commands and intervention, events and ledger, linked runs, authoring, interfaces and tests, ending with the gaps per requested feature. Evidence for the fast-track ADRs and specifications; it decides nothing."
tags: [mission-control, research, codebase]
---

# Codebase map for the fast-track features

Read on 2026-10-07 at working tree `f90daee` plus uncommitted docs. Paths are relative to the repository root; `src/` means `src/mission_control/` and `mig/` means `packages/mission-control-db-contract/component/migrations/`. Line numbers drift; verify before editing.

## Headline findings

- **Stage inputs are computed but never reach the agent.** The StageGraph interpreter works out each stage's upstream refs (`frozen_input_refs`), but the operation preparation service that builds the operation request ignores them.
- **Search is already hybrid** (full-text plus pgvector fused by RRF), despite ADR-0013 saying "full-text first". The configured public API wires no embeddings, so `catalog search` returns 503 there today, and the projection requires an embedding on every row.
- **Queue and interrupt commands already validate** at the HTTP layer and are rejected in one place, `src/application/missions/service.py` (`MissionControlService._action`, `unsupported_control`).
- **There is no `AgentHarness` lane protocol in code**, and nothing for hooks, plugins or subagent definitions in the host-configuration generator.
- **Linked runs exist only in tests** (no production caller), and there is no YAML manifest or `MissionDefinition` class.

## 1. Catalog and capabilities

Tables (all under forced row-level security):

- `mig/0004_capability_artifacts.sql`: `mission_control.asset_version` with `kind` limited to `skill, tool, mcp_server, plugin, blueprint, profile, schema, policy, workflow_template, operation_binding, hook, model_route`; status `proposed|admitted|revoked|retired`. Also `asset_decision`, `capability_grant`, `execution_binding` (immutable), `artifact`, `artifact_relation`, `evidence_assessment`.
- `mig/0020_catalog_definitions.sql`: forward-only status trigger on `asset_version`; `catalog_head` (drafts), `catalog_record` (contracts `alias-movement/1`, `effective-run-configuration/1`, `compilation-index/1`, `catalog-event/1`), `catalog_alias`, `catalog_projection_job`, `catalog_projection_alert`.
- `mig/0021_capability_bundle_admission.sql`: `capability_bundle_admission`.
- `mig/0023_capability_discovery_custody.sql`: `capability_discovery_record`.
- `mig/0022_capability_search_projection.sql` (schema `mission_control_search`): `projection_generation`, `search_document`, `active_generation`, function `activate_generation()`. `search_document` has a generated `fts tsvector` (weights: `title` and `logical_id` A, `search_text` B, `description` C) with a GIN index, and `embedding extensions.vector(1536) NOT NULL` with an HNSW cosine index, so embeddings are mandatory. A CHECK ties `mcp_tool` rows to a parent `mcp_server`. Columns exist for tags, domains, operation classes and compatible runtimes.

Two kind vocabularies, which differ:

- Python `DefinitionKind` (`src/domain/authoring/contracts.py`): `workflow_type, workflow_implementation, blueprint, control_profile, runtime_profile, workspace_template, evaluation_profile, workflow_configuration, memory_policy, agent_profile, capability_selection, prompt, skill, mcp_server, mcp_tool, plugin_package, model, middleware, sandbox_profile, tool, deep_agent_placement`.
- Mapping to SQL `asset_version.kind`: `ASSET_KIND` in `src/adapters/postgres/control_plane/catalog_assets.py`. Notable: `PLUGIN_PACKAGE→plugin`, `MIDDLEWARE→hook`, `MODEL→model_route`, `PROMPT→schema`, `MCP_TOOL→tool`.
- `CapabilityKind` (same contracts module): `mcp, skill, sandbox, model, middleware, tool`.
- There is no Definition class for `plugin_package`; it is reference-only.

Seeded kinds (actual `asset_version.kind` values):

| Seed file | asset_id | kind |
| --- | --- | --- |
| `seeds/common/mc.catalog.approved-assets-1.0.0.json` | `definition:skill:skill.mission-control-coordinator` | skill |
| same | `definition:prompt:prompt.coordinator.propose-workflow` | schema |
| same | `skill-bundle-manifest:mission-control` | skill |
| `seeds/common/mc.catalog.runtime-profiles-1.0.0.json` | `runtime-persistence:langgraph-postgres`, `agent-server:graph-pins` | profile |
| `seeds/common/mc.catalog.workflow-parity-1.0.0.json` | `workflow-family:stagegraph`, `workflow-family:goal-directed` | policy |
| `seeds/qualification/mc.qualification.parity-1.0.0.json` | `fixture.generic-stage-graph`, `fixture.generic-goal-directed` | blueprint |
| `seeds/{biotech,ai-engineer}/mc.app.bindings-1.0.{0,1}.json` | `app-binding:<app>` | profile |

Only `skill, schema, profile, policy, blueprint` are seeded. No `mcp_server`, `plugin`, `hook` or `tool` is seeded.

How search works:

- `CapabilitySearchService.search` (`src/application/capabilities/capability_search.py`) always runs both branches: `lexical_search` (`websearch_to_tsquery` with `ts_rank_cd`) and `semantic_search` (pgvector `<=>`), both in `src/adapters/postgres/capability/capability_search_repository.py`, fused by `weighted_rrf` (k=50, both weights 1.0).
- Each hit is re-checked against the authoritative definition (source digest compared) and passed through `evaluate_selection`; admission is re-checked against `asset_version.status` at query time (`_filtered_query`).
- Embeddings come from `OpenAICapabilityEmbeddingAdapter` (`src/adapters/capabilities/capability_embeddings.py`). Projection rebuild lives in `catalog_projection*.py` and `domain/coordinator/search_document.py`.

Where the catalog commands live:

- CLI (`src/interfaces/cli/main.py`): `catalog list` → GET `catalog/definitions`; `resolve|search|discover|inspect` → POST `catalog/<action>`; `components` → POST `catalog/components/search`.
- HTTP (`src/interfaces/http/catalog.py`): `/definitions`, `/resolve`, `/search`, `/discover` (source `mcp|skills`), `/inspect`, `/components/search`.
- Service `CatalogService` (`src/application/capabilities/catalog.py`); composition `compose_catalog_service` (`src/bootstrap/catalog.py`). `search` is `None` unless embeddings are passed in; `bootstrap/api.py` passes none and no `components`, so the configured API returns 503 `catalog_search_unavailable` / `component_catalog_unavailable` unless `RuntimeOptions.catalog_factory` overrides it.

How a capability pin is bound into a run:

- `CapabilityPins` (`src/adapters/capabilities/capability_pins.py`) is a deployment-level file (`settings.capability_pins_path`, `infra/capability-pins/research-capabilities.json`) holding `mcp_servers, skills, tools, models, sandboxes, checkpointers, stores`. Skill locators are `workspace://` or `capability-bundles://sha256:…`.
- At worker startup `ProductionWorkerActivityCompositionFactory._build` (`src/adapters/temporal/deployment_composition.py`) loads the pins; for Supabase it calls `PostgresCapabilityBundleAdmissions.resolve_pins` (`src/adapters/postgres/capability_bundles.py`) and `build_deployment_capability_registry`, which builds an `ExactComponentRegistry` keyed by ref digest.
- Per run: the compiler flattens `RuntimeProfileDefinition.operation_assemblies` (each an `OperationAssemblyDefinition`: `deep_agent_profile_ref`, `placement_ref`, `capability_requirements`) into `EffectiveRunConfiguration.flattened_agent_bindings` / `capability_attachment_plan`. The operation template's `DeepAgentExecutionBinding` (`domain/execution/contracts.py`) carries `tools/mcp_servers/skills/sandbox` components, each with an `ExactDefinitionRef`. `PinnedCapabilityAssetVerifier` (`src/adapters/operations/runtime_ports.py`: `verify`, `verify_servers`, `verify_launch`) refuses any component not pinned at that digest.
- StageGraph and GoalDirected do not reference pins directly. `StageOperationSlot.allowed_variants[].operation_contract_ref` is a string; agent and capabilities come from the runtime profile's assemblies and stored operation templates (`PostgresStageGraphOperationTemplateRepository`, `PostgresGoalDirectedDocumentRepository`).

Storage buckets: `capability-bundles` is hard-coded in `src/adapters/supabase_storage/bundles.py` (`SupabaseCapabilityBundleStore`, read-only, never creates buckets). `mission-artifacts` appears only in docs; artifact payloads use `S3ArtifactPayloadStore` or `FilesystemArtifactPayloadStore` (`deployment_composition.py`).

## 2. Agentic components (host configuration generation)

- Contracts (`src/domain/agentic_components/contracts.py`): `AgentHost` {`cursor, codex, claude_code, agent_framework`}; `ComponentKind` {`plugin, mcp_server, skill, agent_component, sandbox_snapshot, workspace_setup, diff_codec`}; `TrustStage` {`quarantined, reviewed, qualified, accepted`}; `AgenticComponentRelease` with exactly one typed binding among `mcp|skill|sandbox|workspace|diff_qualification|agent_component` (`plugin` has no binding type); `ComponentQuery`, `MaterializationRequest`; `MaterializationStep.kind` ∈ {`retrieve, verify, stage, inject_secrets, configure_host, provision_workspace, start_server, probe_readiness, seal_snapshot`}; `MaterializationPlan`, `ReadinessReceipt`.
- Rendering (`src/application/agentic_components/projections.py`): `render_host_files` handles MCP releases only: Codex → `.codex/config.toml` (`[mcp_servers."x"]`, `enabled_tools`), Cursor → `.cursor/mcp.json` (`{"mcpServers": …}`, no `type`), Claude Code → `.mcp.json` (with `type` stdio/http/sse). `skill_target_path`: `.agents/skills`, `.cursor/skills`, `.claude/skills`.
- Planner `MaterializationPlanner.plan` (`materialization.py`); repositories `AgenticComponentRepository` Protocol plus `InMemory…` (`repository.py`) and `FilesystemAgenticComponentRepository` (`src/adapters/capabilities/agentic_components/filesystem_repository.py`).
- Not present: hooks, plugins, subagents. Nothing for `.claude/agents`, `.cursor/agents`, `.cursor/rules`, `hooks.json`, `.claude/settings`, or plugin manifests. `ComponentKind.PLUGIN` exists but nothing renders it. The only "hook" anywhere is the `MIDDLEWARE→"hook"` SQL mapping. Subagents exist only as Deep Agents `SyncSubagentProfile` and `AsyncSubagentContract`.

## 3. Execution families and context transfer

Workflows: `mc.mission_run.v1` (`workflows/mission_run.py`) extends `BellLabsRunWorkflow` (`belllabs_run.py`); `belllabs.stagegraph` (`stagegraph.py`); `GoalDirectedWorkflow` (`goal_directed.py`); `belllabs.operation.v2` / `mc.operation.v1` (`operation.py`); activities `operation.execute` / `operation.cancel`.

How a StageGraph stage receives upstream outputs today:

- `StageGraphInterpreter._input_refs` (`src/domain/programs/interpreter.py`) unions the `DependencyProjection.evidence_refs` of every dependency edge pointing at the consumer into a flat sorted tuple of strings; the `producer_output_slot_id → consumer_input_slot_id` mapping is dropped.
- That tuple becomes `StageOperationAdmissionProposal.frozen_input_refs` and `StageInstanceProjection.frozen_input_refs`.
- Gap: `StageGraphOperationPreparationService.materialize` (`src/application/programs/service.py`) builds the `OperationExecutionRequest` from a static template plus an optional `objective_override` prompt segment. It never reads `frozen_input_refs`. Nothing puts them into the prompt or into `WorkspaceContract.read_mounts`.
- `StageOperationRequest.input_refs` (`domain/programs/contracts.py`) is only used by native semantic handlers (`application/programs/orchestration_routing.py`).
- Outputs travel in `StageOperationResult.output_refs` / `operation_result["output_refs"]`; `stagegraph.py` merges `structured_output` into the observation, so a model-emitted `output_refs` key is accepted; `interpreter.py` turns admitted output refs into dependency `evidence_refs`. The Deep Agents adapter never sets `RuntimeResult.output_refs`; workspace candidates go only into `event_payloads`.

Typed structures (`src/domain/programs/contracts.py`): `StageExecutionIdentity`, `StageCandidateIdentity`, `DependencyProjection`, `StageInstanceProjection`, `StageGraphAcceptedProjection`; activity payloads `StageOperationAdmissionProposal`, `StageResultObservation`, `StageOperationRequest/Result`, `StageGraph{Admission,Result,Cycle,Completion}Activity{Request,Result}`, `StageGraphRunInput`; invalidation `StageInvalidationProposal` (`allowed_input_refs`, `prior_result_refs`, `reused_output_refs`); blueprint slots `StageInputSlot`, `StageOutputSlot`, `StageDependency`, `StageJoin` (`domain/authoring/contracts.py`).

How GoalDirected passes state between iterations:

- `GoalHandoff` (`domain/programs/contracts.py`): `artifact_refs`, `workspace_refs`, `snapshot_refs`, `context_selection_refs`, `continuation_instructions`. The model-authored part is `GoalHandoffDraft` (`goal_directed_runtime.py`), bound by `_bind_handoff` (`application/programs/goal_directed.py`). Also `GoalExecutionClaim.prior_handoff_ref`, `GoalContinuationState`, `GoalDirectedRunInput.continuation_handoff`.
- Delivery to the agent: `_prompt_segments` (`goal_directed.py`) appends one `UNTRUSTED_CONTENT` segment whose content is `str(dict)` of the revision, iteration, role, `handoff` (via `asdict`) and `verifier_input_refs`. `_workspace_for` adds a single read mount `{role_root}/input` of the executor workspace for the verifier only.
- `GoalHandoffReference` (`domain/graph_runtime/contracts.py`: `checkpoint: GoalHandoffCheckpointKey`, `artifact_ref`, `content_digest`) is schema-only; it appears only in `governance.py` and the HTTP schema export, not at runtime.
- File paths `/goal/GOAL.md`, `/goal/HANDOFF.md`, `/goal/checkpoint.json` come from `GoalWorkspaceSnapshotPolicy` and `application/artifacts/goal_workspace.py::GoalWorkspaceService`.

Where the prompt and context are assembled:

- `src/adapters/deep_agents/adapter.py::_prompts`: `system_authority`/`authored_instruction` segments become the system prompt; everything else becomes one user message.
- `ExactDeepAgentMaterializer.prepare` seeds the initial state with `cognitive_context_values`, `initial_artifact_index` and `initial_context_manifest` (`materializer.py`).
- `ContextPolicyDefinition` and `ContextAssemblySpec` (`domain/graph_runtime/definitions.py`; `ContextSourceRule.source_kind` includes `admitted_input`, `artifact`, `prior_checkpoint_summary`) and both `SubagentContextSlice` classes (`domain/execution/contracts.py`, `domain/graph_runtime/contracts.py`) are schema and governance only. Nothing uses them to assemble a prompt.
- The trust class `admitted_input` is used for stage cycle objectives (`service.py`) and fork `stage_objectives` (`programs/fork_templates.py`).

Artifacts: `ArtifactPromotionService.promote` (`application/artifacts/artifact_promotion.py`) writes `ArtifactMetadataRevision` to `artifact_metadata_revision` (`mig/0024`) and a durable ref via `PostgresArtifactDurableReferenceRepository.admit` → `mission_control.artifact`. Ref format `artifact://{scope}/{run}/{artifact_id}` (`artifact_durable_reference`). Workspace manifests: `PostgresWorkspaceManifestRepository`. Read-only inputs materialize through `WorkspaceSlotBinding(access="read_only", durable_ref, content_digest)` → `DurableInputManifestEntry` → `WorkspaceMaterializationService._load_and_verify_inputs` (`workspace_materialization.py`) → `_DurableInputsFromPayloads.retrieve` (`deployment_composition.py`, format `<object_ref>#<sha256>:<size>`). Candidate capture runs through `WorkspaceCandidateCaptureService`; promotion through `GenericArtifactWorkflow` / `ArtifactPromotionActivities`.

## 4. Harness and lanes

Deep Agents adapters:

- `DeepAgentRuntimeAdapter` (`adapter.py`): `execute(invocation, resolved_secrets) -> RuntimeResult`, `observe_latest(...)`, `build_hosted_async_subagent_graph(...)`.
- `ExactDeepAgentMaterializer.prepare(binding, secrets, hosted=, output_schema_digest=)` (`materializer.py`); same file `ExactComponentRegistry`, `ResolvedSkillBundle`, `StateSandboxFactory`, `OpenAIExactModelFactory`, `LangSmithSandboxFactory`.
- `DeepAgentsAsyncSubagentAdapter` (`async_subagents.py`): `stock_tools, verify_served_graph, start, observe_spawn_key, check, update, cancel, cancel_run, list, bind_contract`; `BellLabsAsyncSubagentMiddleware`.
- `DockerSandbox` (`execute, upload_files, download_files`) and `DockerSandboxFactory.workspace_path` (`docker_sandbox.py`); `persistence.py`: `StandalonePersistence`, `StandalonePersistenceLifespan`, `runtime_checkpoint_conninfo`.

Placement enums: `OperationExecutionRequest.execution_runtime: Literal["native","deep_agent"]` (`domain/execution/contracts.py`); `DeepAgentExecutionPlacementProfile.placement: Literal["local_in_worker","remote_langsmith_deployment"]` plus `message_injection_behavior: invoke_only|remote_thread` and cancellation, reconnect and streaming behavior fields; authoring-level `DeepAgentPlacementDefinition.placement: Literal["local_worker","langsmith_remote"]` (`authoring/contracts.py`); `DeepAgentSandboxComponent.backend`: `langsmith|daytona|docker|state`.

Ports a Cursor lane would implement (`src/application/execution/operations/operation_execution.py`): `RuntimePort.execute`, `CancellableRuntimePort.observe_latest`, `SandboxPort.materialize`, `CapabilityAssetPort.verify`, `MCPRuntimePort.verify_servers`; `SecretResolutionPort` and `OperationEventPort` are reusable as they are. The service is constructed with a single `runtime` and has no dispatch by lane; `OperationExecutionService` branches only on `execution_runtime == "deep_agent"` for lineage. `docs/knowledge/lanes-and-harness.md` confirms no `AgentHarness` protocol exists.

Worker wiring: `bootstrap/worker.py::run_worker` → `ProductionWorkerActivityCompositionFactory(client).build` → `production_workers_or_close` → `adapters/temporal/worker.py::create_production_workers`. Queues from `BellLabsTaskQueues.from_base` (`registration/task_queues.py`): `-coordinator-family`, `-agent-cognitive` (operation activities), `-generic-artifact`, root `MissionRunWorkflow`, and the `-linked-runs` worker. The adapter is built in `deployment_composition.py`: `DeploymentOperationRuntime(DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry)))` → `OperationExecutionService(runtime=adapter, sandbox=BindingWorkspaceMaterializer, assets=verifier, mcp=verifier, …)`. Activity selection lives in `registration/activities.py`.

## 5. Commands and intervention

- Command kinds and receipts (`src/domain/policies/contracts.py`): `BoundaryCommandKind = pause|resume|satisfy_wait|cancel|reconcile_unit`; family-applicable subset pause, resume, satisfy_wait. `BoundaryTargetKind = run_control|root|family|unit`. `ReceiptState` accepted→delivered→applied, rejected allowed from accepted or delivered. `BoundaryRejectionReason`. Records `BoundaryCommandRecord`, `BoundaryCommandReceipt`, `BoundaryCommandStatus`, `ApplyBoundaryCommandAction`.
- Pure helpers `domain/policies/boundary_commands.py` (`boundary_target_for`, `run_control_boundary_receipts`); reducer `domain/policies/reducer.py` (`required_action_permissions`).
- Public request (`src/contracts/contracts.py`): `MissionCommandRequest.kind` includes `queue_instruction` and `interrupt_and_inject`, with `InstructionPayload(content_ref, content_digest, boundary: next_turn|next_iteration)`. These pass validation and are then rejected in `src/application/missions/service.py` (`MissionControlService._action` raises `MissionControlRejected("unsupported_control")`). Immediate cancel is rejected the same way.
- Temporal transport: `TemporalBoundaryCommandTransport.deliver` (`src/adapters/temporal/boundary_commands.py`) uses root Update `deliver_message` (`WorkflowMessage kind ∈ control|fact|result|cancel`, `belllabs_run.py`), then family Update `deliver_boundary_command` (`stagegraph.py`), plus `deliver_cancel`.
- Agent Server interventions (unused by the public API): `domain/graph_runtime/contracts.py` defines `append_input`, `respond_to_interrupt`, `fork_from_checkpoint`; routed by `application/recovery/runtime_interventions.py::ExactRuntimeInterventionRouter`; table `runtime_intervention` (`mig/0017`).
- Fork saga `application/recovery/run_forks.py`: `RunSnapshotService.take`, `SemanticForkService.prepare/fork` (typed patch, reuse frontier, independent admission), `RunControlForkAuthority`, `ForkReuseResolver`, `RecordingForkMaterializer`. Facade `application/missions/runtime.py::MissionControlRuntimeService.snapshot/get_snapshot/fork/reconcile`.
- `delivery_report` (`mig/0005`): immutable; `observed_outcome ∈ delivered|applied|rejected|emulated|unknown`, `delivery_semantics`, `native_refs`. Written by `run_control_repository.py` and `runtime_execution_repository.py`.
- `run inspect` returns `MissionInspection` (`mc.inspection.v1`, `contracts/contracts.py`): `run_id, version, execution_generation`, `lifecycle` (pending/running/paused/completed), `phase`, `execution_outcome`, and the full `RunProjection` (`domain/policies/contracts.py`: waits, pauses, readiness, obligation/output evidence, async_children, unit_reconciliations, execution_target). A richer `RunInspection` / `InspectionRead[T]` (`domain/policies/inspection.py`) is served only by the technical API at `/run-control/v1/inspection/*`.

## 6. Events and ledger

- `append_events` (`src/adapters/postgres/run_control/canonical.py`) writes one `ledger_commit`, contiguous `mission_event` rows (sequence numbers allocated under the mission row lock) and `outbox` rows (`delivery_state` `pending`), idempotent on `delivery_key = event_id`. Envelope `DomainEventEnvelope` (`domain/policies/contracts.py`, `aggregate_type="workflow_run"`); also `OutboxRecord`, `ConsumerCursor`.
- Transcript, trace and native-event storage: `native_observation` (`mig/0005`, keyed by `native_event_key`) is used only by async subagents (`adapters/postgres/async_subagents/async_subagents.py`) and inspection. `harness_execution`, `agent_session` and `session_turn` (`mig/0003`) exist in SQL with no Python writer. `RecordedOperationEventSink` (`runtime_ports.py`) keeps digests in memory and logs; nothing is persisted. Operation `event_payloads` (capability lineage, workspace candidates) go into the settlement. The code explicitly excludes transcripts (`inspection_repository.py`, `operation_execution.py`). Tracing goes to LangSmith (`adapters/langsmith/tracing.py`, `trace_behavior="langsmith"`).
- LangGraph checkpoints live in schema `mission_control_runtime` (`deep_agents/persistence.py`; DDL descriptor `packages/mission-control-db-contract/runtime/descriptor.json`; ADR-0017). Lineage is tracked in `checkpoint_transition` (`mig/0012`) via `adapters/postgres/operations/checkpoint_lineage.py`.

## 7. Mission composition (linked runs)

- Contracts `src/domain/composition/contracts.py`: `LinkedRunRequest`, `RunCompositionLink`, `RunDependencyRevision`, `LinkedRunResultAdmissionDecision`, `LinkedChildResultObservation`, `LinkedChildResolution`.
- Service `src/application/programs/linked_runs.py::LinkedRunService` (`request_child`, `revise_dependency`, `decide_result`, `dependency_disposition`, `record_child_terminal`).
- Workflows `belllabs.linked-run` and `belllabs.linked-run-observer` (`adapters/temporal/linked_run_workflow.py`); activities `linked_run_activities.py`; worker wiring in `bootstrap/worker.py` with `DeferredLinkedResultAssessor`.
- Storage `PostgresLinkedRunRepository`; tables `run_composition_link`, `run_dependency_revision`, `linked_result_decision`, `linked_child_terminal` (`mig/0014`) and `mission_relationship` (`kind ∈ composition|fork|dependency`, `mig/0005`).
- Authoring side: `LinkedRunSlotConstraint` (Workflow Type), `LinkedRunSlot` (StageGraph), `allowed_linked_run_slot_ids` (GoalDirected).
- No production caller: `request_child` is called only from tests (`tests/integration/temporal/test_linked_runs.py`, `tests/unit/control_plane/test_contract_digest_set_order.py`). The StageGraph and GoalDirected workflows contain no linked-run dispatch, and there is no HTTP route.
- Cross-run output reference: only `LinkedChildResolution.admitted_output_refs` (strings). No Portal or child-mission concept in code.

## 8. Authoring

- `domain/authoring/`: `contracts.py` (all definitions, `CompileInvocation`, `CompilationRequest`, `EffectiveRunConfiguration`, `FlattenedDeepAgentBinding`), `compiler.py::compile_effective_run_configuration`, `stagegraph_builder.py::build_stagegraph_v2`, `canonical.py`, `extensions.py`, `fixtures.py`, `errors.py`, `identity.py`.
- There is no `MissionDefinition` class. Tables `mission_draft, program_node, goal, objective, success_criterion, revision_proposal, authoring_session` (`mig/0002`) have no Python users. `definition_snapshot`, `mission_revision` and `compiled_program` are written only by `canonical.py` during admission.
- Compile: `src/application/authoring/service.py::ControlPlaneService.compile`, plus `publish`, `save_draft`, `publish_draft`, `move_alias`, `resolve_alias`, `retire`, `retrieve`.
- Coordinator draft `WorkflowDesignDraft` (`src/domain/coordinator/contracts.py`): `proposed_workflow_type`, `blueprint_family`, `proposed_stage_graph`, `requested_assets: RequestedAsset`, `requested_authority`, `workspace_requirements`, `budgets`.
- MCP tools (`interfaces/mcp/coordinator_server.py`): `coordinator_bootstrap, search_capabilities, get_capability, discover_mcp_servers, discover_agent_skills, inspect_external_candidate, validate_workflow_design, prepare_workflow_launch, launch_workflow, get_workflow_result`. Resources `belllabs://workflow-types/…/{contract,input-schema,output-contracts}`, `belllabs://catalog/{kind}/{id}/{rev}[/manifest]`, `belllabs://runs/{id}/{result,launch,bindings}`. Prompts `propose_workflow, review_workflow_design, explain_launch_blocker, summarize_workflow_result`.
- YAML: the only use is `yaml.safe_load` of skill frontmatter (`application/coordinator/coordinator_surface_promotion.py`); `pyyaml` is a dependency. ADR-0022 (proposed) describes `mission.yml`; nothing implements it.

## 9. Interfaces

- `missionctl` (`interfaces/cli/main.py`): `run inspect RUN_ID [--wait]`, `run admit --request-file`, `run {snapshot,fork,reconcile,start} RUN_ID --request-file`, `command send RUN_ID --request-file`, `command list RUN_ID`, `catalog list`, `catalog {resolve,search,discover,inspect,components} --request-file`.
- Public API (`bootstrap/api.py` includes only these two routers): `interfaces/http/mission_control.py` under `/v1/applications/{app}`: GET `runs/{id}/inspection`, GET+POST `runs/{id}/commands`, POST `runs/{id}/snapshots`, GET `runs/{id}/snapshots/{sid}`, POST `runs/{id}/forks`, POST `runs/{id}/reconcile-unit`, POST `run-requests`, POST `runs/{id}/launch`; and `interfaces/http/catalog.py`.
- Technical API (`bootstrap/technical_api.py`): `/control-plane/v1` (definitions, drafts, aliases, compile, effective-run-configurations, retire, schemas); `/run-control/v1` (run-requests, launch, commands, reconcile-unit, async-children reconcile-usage, boundary-commands and redeliver, operations, runs, budget, effects, effect-ledger, transitions, outbox, schemas); `run_forks` (snapshots, forks, `forks/{request_id}`); `/run-control/v1/inspection` (runs, units, checkpoints, checkpoint summary); `/v2/graph-runtime/schemas`.

## 10. Tests and Makefile

- Layout: `tests/unit/{agent_server,agentic_components,capability,config_api,control_plane,coordinator,integrations,mission_control,operations,orchestration,run_control,runtime,schema,workspaces,…}`, `tests/integration/{postgres,temporal,deep_agents,agent_server}`, `tests/acceptance/{mission_control,control_plane}`, `tests/qualification/two_project`, `tests/architecture/test_package_boundaries.py`.
- Markers (`pyproject.toml`): `common_db` (needs `MISSION_CONTROL_TEST_ADMIN_DSN`, a disposable PostgreSQL 17 with pgvector; fails rather than skips) and `block_c_*`. Use `pytestmark = pytest.mark.common_db`.
- PostgreSQL fixtures: `tests/fixtures/mission_control_common_db.py` (`create_common_database`, `common_database`, `CommonDatabase.pool/dsn/scope`, `provision_runtime`); `tests/integration/postgres/runtime_common.py` (`common_db`, `runtime_pool`, `family_pool`, `scoped_request`, `scoped_command`); `tests/integration/postgres/catalog_common.py` (`catalog_db`, `runtime_pool`, `catalog_writer_pool`).
- Temporal: `WorkflowEnvironment.start_time_skipping()` (for example `tests/integration/temporal/test_linked_runs.py`) or `start_local` (`tests/fixtures/mission_control_production_stack.py`).
- Root conftest (`tests/conftest.py`): `in_memory_control_plane_service`, `test_application_postgres_dsn`, and the `_PSYCOPG_SELECTOR_MODULES` list for the Windows event loop.
- Makefile: `check` (lint, fmt-check, typecheck-fast, deps-check, test-arch, test-unit), `ci`, `test`, `test-unit`, `test-integration`, `test-acceptance`, `test-qualification`, `test-db-contract`, `typecheck` (mypy), `infra-up`, `temporal-up`, `server`, `worker`, `mcp-server`, `missionctl`, and `db-*` / `mission-db`.

## Gaps per requested feature

**(a) Capability kinds for skill, MCP server, plugin, hook script and subagent definition.** Exists: SQL `asset_version.kind` allows `skill, mcp_server, plugin, hook`; `DefinitionKind` has `skill, mcp_server, mcp_tool, plugin_package, middleware`; `SkillDefinition` and `MCPServerDefinition` classes; `ComponentKind.PLUGIN`. Missing: Definition classes for plugin, hook script and subagent profile; a `subagent` kind anywhere; any rendering of hooks, plugins or agent files; seeds for these kinds; `MIDDLEWARE→hook` overloads the word "hook". Insertion points: new `DefinitionKind` members and Definition classes in `domain/authoring/contracts.py` (add to the `Definition` union); `ASSET_KIND` in `catalog_assets.py`; a new migration extending the `asset_version.kind` CHECK (ADR-0020: a new migration, not an edit); `CapabilityKind` and the `CapabilityRequirement.validate_policy` mapping; `ComponentKind` plus typed bindings and `render_host_files` / `skill_target_path` cases in `projections.py`; new seed bundles.

**(b) Hybrid search.** Exists: RRF fusion of full-text and pgvector, with source verification. Missing: lexical-only search when no embeddings are available (ADR-0013's intent); embeddings wired into the public API; searching agentic components (`ComponentQuery` is in-memory substring matching); filters for the new kinds. Insertion points: make `semantic_search` optional in `CapabilitySearchService.search`; pass `embeddings` in `bootstrap/api.py`; make `search_document.embedding` nullable in a new migration.

**(c) Context transfer between stages and iterations with artifact refs materialized into the sandbox.** Exists: `frozen_input_refs` computed by the interpreter, `GoalHandoff.artifact_refs`, the `WorkspaceSlotBinding` → `DurableInputManifestEntry` read-only materialization path, `artifact_durable_reference`, and the schema-only `ContextAssemblySpec` / `GoalHandoffReference`. Missing: any use of `frozen_input_refs` in `StageGraphOperationPreparationService.materialize`; mapping each dependency's output slot to its input slot; resolving `artifact://` refs to `<object_ref>#digest:size` mounts; structured handoff delivery (today `str(dict)` in an untrusted segment); a Deep Agents result that emits `output_refs` from promoted artifacts. Insertion points: `materialize()` adds `read_mounts`/`slot_bindings` and an `admitted_input` `PromptSegment` built from a context packet; mirror in `goal_directed.py::_workspace_for` / `_prompt_segments`; keep the slot mapping in `_input_refs`; fill `RuntimeResult.output_refs` in `adapter.py`.

**(d) Provider-frame (transcript) persistence.** Exists: unused `harness_execution`, `agent_session`, `session_turn` tables, `native_observation`, LangGraph checkpoint messages in `mission_control_runtime`. Missing: any writer, a frame or turn store, an `OperationEventPort` that persists anything; policy currently excludes transcripts from read models. Insertion points: a new repository writing `session_turn` / a new frame table from `DeepAgentRuntimeAdapter.execute` callbacks (`_ModelCallObserver`); replace `RecordedOperationEventSink`.

**(e) Linked multi-workflow missions.** Exists: the full `LinkedRunService`, its workflows, tables and slots. Missing: dispatch from the StageGraph or GoalDirected families, from HTTP or from the CLI; a real result assessor (`DeferredLinkedResultAssessor` is used); mapping child outputs into parent stage inputs. Insertion points: the StageGraph interpreter frontier (`linked_run_slots`), a new route in `interfaces/http/mission_control.py`, GoalDirected `scope_expansion_route="linked_run"`.

**(f) YAML manifest.** Exists: ADR-0022 (proposed), `pyyaml`, `CompileInvocation` and the compiler. Missing: the schema, loader, search-to-pin resolution, `missionctl mission compile|submit`, an MCP tool. Insertion points: new `domain/authoring/manifest.py` (pydantic model plus JSON Schema), an application service that resolves searches through `CapabilitySearchService` before `ControlPlaneService.compile`, a CLI group in `cli/main.py`, a coordinator MCP tool.

**(g) Queue, interrupt and fork controls.** Exists: public kinds and `InstructionPayload`, the fork and snapshot facade and routes, Agent Server `append_input` / `respond_to_interrupt` interventions, `delivery_report.observed_outcome='emulated'`, `message_injection_behavior`. Missing: a domain action, reducer handling and boundary delivery for instructions; the rejection sits in `application/missions/service.py`. Insertion points: a `QueueInstructionAction` in `BoundaryCommandAction` / `BOUNDARY_COMMAND_KINDS` plus the reducer; a family `deliver_boundary_command` handler that injects at the next turn or iteration (StageGraph admission boundary; GoalDirected iteration); a per-lane delivery-semantics declaration.

**(h) Cursor lane.** Exists: `AgentHost.CURSOR` `.cursor/mcp.json` and `.cursor/skills` projection, ADR-0018/0019 (proposed), the `RuntimePort` family. Missing: an `AgentHarness` protocol and lane registry; an `execution_runtime` value for Cursor; a placement profile and binding type (today `DeepAgentExecutionBinding` is the only lane binding); runtime dispatch by lane. Insertion points: extend `execution_runtime` and add a `CursorExecutionBinding` in `domain/execution/contracts.py`; add `adapters/cursor/` implementing `RuntimePort` + `CancellableRuntimePort` and using `MaterializationPlanner` for workspace configuration; wire a lane-routing runtime in `deployment_composition.py`.

# Citations

- Working tree `f90daee` (2026-10-07) of `mission-control`, read with the Explore agent; every path above is a citation.
- `docs/knowledge/lanes-and-harness.md`, `docs/knowledge/context-and-continuation.md`, `docs/knowledge/events-and-commands.md` (prior concept notes that the findings above confirm).
