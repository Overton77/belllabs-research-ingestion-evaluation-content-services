# RRM-015 implementation evidence

Disposition: ready_for_review (independent review pending; not merged, the coordinator owns review and merge)
Recorded date: 2026-10-02
Qualification identity: RRM-015 make every contract digest independent of set iteration order. Requirements: REQ-CP-RUN-003/008 and REQ-CP-EXEC-004/005 (idempotency and exact replay), CP-020 operation-journal invariants. No new contract identity, schema version, Temporal name, payload field, table, column or migration.
Base revision and head revision: base `bb964c5` (integration `integration/research-runtime-mission`). Tested code head: `1dd8013` on `wp/rrm-015-set-order-stable-digests` (code commits `8960c53` and `1dd8013`; the evidence and ticket commit that follows changes documentation only).
Framework/package baseline: `uv sync --frozen` from the committed `uv.lock` (no dependency change); CPython 3.12 (Codex runtime venv); pydantic 2.13.4; pydantic-core `to_jsonable_python` for leaf conversion.

## Worktree provenance

- Worktree `C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system-rrm-015`, branch `wp/rrm-015-set-order-stable-digests`, base `bb964c5`. Clean at kickoff (no pre-existing dirty paths).
- The main checkout was not touched. Nothing was pushed. No container was created, stopped or removed. Full pytest runs held the shared-stack lock (`stack_lock.py acquire/release RRM-015`).
- Not edited (RRM-013 owns them): `app/integrations/agents/deep_agents/async_subagents.py`, `app/application/async_subagents/`, `app/agent_server/`. RRM-005 inspection/API paths were not touched.
- Shared files edited, by region: `app/domain/control_plane/canonical.py` (new functions and `contract_fingerprint` body), and one-line call-site changes in the files listed under "Changed paths".

## Implemented contracts and seams

Root cause (RRM-004 diagnosis): `model_dump(mode="json")` lists a `set`/`frozenset` in per-process iteration order (`PYTHONHASHSEED`, insertion history). `sha256_digest` sorts real sets but receives a list after a JSON-mode dump, so `sha256_digest(x.model_dump(mode="json"))` is not an identity when `x` reaches a set.

Canonical forms added to `app/domain/control_plane/canonical.py`:

- `stable_json_dump(model, *, exclude=None)`: JSON-compatible dump with every set emitted as a canonically sorted list (sort key: canonical JSON of the member, the same key `_normalize` uses). For a contract with no set, or sets of at most one member, the result is byte-identical to `model_dump(mode="json")`.
- `stable_json_digest(model, *, exclude=None)`: `sha256_digest(stable_json_dump(...))`. This is the replacement for `sha256_digest(x.model_dump(mode="json", ...))`.
- `stored_payload_matches(stored_json, model)`: order-independent "is this stored JSON exactly this contract" (validate the stored payload and compare models) for exact-replay proofs that compared stored JSON with a fresh JSON dump.
- `contract_fingerprint` (RRM-004) keeps its value but now walks fields instead of `model_dump(mode="python")`. A Python-mode dump turns a `frozenset` of models into a set of dicts and raised `TypeError: unhashable type: 'dict'`; this was a latent defect for any contract holding a set of models.

Why a field walk and not `model_dump(mode="python")` for `stable_json_dump`: the same `unhashable type: 'dict'` failure broke compiling an Effective Run Configuration (it holds sets of models). No model in `app/` uses `computed_field`, `Field(exclude=True)`, custom serializers or aliases, so the field walk matches the JSON dump (checked below).

Shared test helper `tests/fixtures/set_order.py`: `equal_sets_with_different_iteration_order` (the RRM-004 helper, generalized and moved), `with_different_set_orders(model, skip=...)` (two equal copies in which every set is padded with 400 type-compatible probe members and rebuilt in a different insertion order, so iteration orders differ under every seed), `assert_json_dumps_differ` (proves the probe is effective: the JSON dumps of the equal copies differ) and `model_holds_set` (transitive annotation walk including subclasses).

Static guard: `tests/fixtures/digest_sites.py` (ast scan) and `tests/unit/control_plane/test_digest_set_order_guard.py`.

## Requirement-to-evidence map

| Requirement | Test and observed assertion |
|---|---|
| Every fixed site is order-stable (acceptance 2) | `tests/unit/control_plane/test_contract_digest_set_order.py`, one test per site, each using `with_different_set_orders` and `assert_json_dumps_differ`. Each was run against the pre-fix site code (reverse patch) and fails; it passes with the fix. The 11 site tests all fail pre-fix (see "Deterministic verification"). |
| `RuntimeDefinition.digest` | `test_runtime_definition_digest_is_independent_of_set_iteration_order`: `first.digest == second.digest`; a one-member set keeps `sha256_digest(model_dump(mode="json"))`. |
| `WorkflowLaunchProposal.digest` | `test_launch_proposal_digest_is_independent_of_set_iteration_order`: equal digests for the two orderings of the real `launch_fixture` proposal. |
| Stage requirement digest (`compile_structural_graph_assembly`) | `test_structural_compiler_requirement_digest_is_independent_of_set_iteration_order`: a binding digested from one ordering compiles with both orderings. |
| Linked-run request fingerprint | `test_linked_run_request_fingerprint_is_independent_of_set_iteration_order`: a replay with the other ordering returns the same link, one admission, `request_fingerprint == stable_json_digest(...)`. |
| Runtime lifecycle and budget digests (ticket "Blocks RRM-010") | `test_bootstrap_authority_projection_digests_are_independent_of_set_iteration_order`: `lifecycle_projection_digest`, `budget_projection_ref` and `decision_projection_ref` equal for two orderings of a real `RunProjection` and `BudgetState`. |
| Graph admission decision id, reconciliation request digest | `test_graph_admission_and_reconciliation_ids_are_independent_of_set_iteration_order`: `decision_id` and `_reconciliation_request_digest` equal. |
| Snapshot creation identity and clone fingerprint | `test_sandbox_snapshot_identities_are_independent_of_set_iteration_order`: replays with the other ordering return the stored snapshot and clone, one provider capture. |
| Semantic binding template refs (both schema providers) | `test_semantic_binding_operation_template_refs_are_independent_of_set_order`: the frozen plan's `exact_input_refs` equal for both orderings, for the StageGraph and GoalDirected providers. |
| Web research record id and content digest | `test_web_research_record_digests_are_independent_of_set_iteration_order`: `record_id`, `content_digest` and `payload` equal. |
| GoalDirected operation template document digest and payload | `test_goal_operation_template_documents_are_independent_of_set_iteration_order`: one digest and one payload across four persisted documents. |
| Effective Run Configuration persistence | `test_effective_run_configuration_persistence_is_independent_of_set_order`: persisting the second ordering after the first does not raise an ERC digest collision, with an inline payload and with an externalised `payload_ref`. |
| Canonical primitives | `test_stable_json_dump_sorts_sets_and_matches_json_mode_for_set_free_contracts`, `test_stable_json_dump_sorts_sets_nested_in_dicts_and_tuples`, `test_stored_payload_matches_ignores_stored_set_order`, `test_contract_fingerprint_handles_sets_of_models_and_keeps_python_dump_values`. |
| RRM-004 regression kept | `tests/unit/run_control/test_run_control.py::test_command_fingerprint_is_independent_of_set_iteration_order` now imports the shared helper and still passes. |
| Static guard (acceptance 3) | `test_no_unaudited_json_dump_digest_sites` (fails listing 16 sites when the fix is reverted), `test_audit_allowlist_has_no_stale_entries`, `test_audited_models_hold_no_sets` (one case per audited site, resolved from the model classes), `test_scanner_detects_the_banned_patterns` (direct, multi-line, via-variable, nested and fingerprint-named calls are found; python-mode, stable and persist-only calls are not). |

## Audit table

Method: (1) an ast scan of `app/` finds every JSON-mode dump (`model_dump(mode="json"...)` or `model_dump_json`) that flows into a digest-like call, directly or through a local variable; (2) every `sha256_digest`, `hashlib` and `canonical_json*` call was grepped and read; (3) a runtime walk of all `BaseModel` subclasses in `app.*` (137 hold a set transitively through field annotations; subclasses and `Any`-typed members checked separately).

"Persisted" says whether the digest is stored, or compared with a stored value.

### Fixed (JSON-mode dump of a model that holds a set)

| Site | Model | Holds a set | Persisted | Outcome |
|---|---|---|---|---|
| `application/orchestration/linked_runs.py` `request_child` (request fingerprint) | `LinkedRunRequest` | yes (`authority_request_refs`, ...) | yes, `request_fingerprint` of the link row, compared on replay | fixed |
| `application/runtime/postgres_runtime_authority.py` `load` (lifecycle and budget digests) | `RunProjection`, `BudgetState` | yes (`WaitCondition.scope`, `active_*`, ...) | compared with the checkpoint's stored `lifecycle_projection_digest` | fixed |
| `application/runtime/runtime_run_plan.py` (`compile_structural_graph_assembly`, `_v3`: requirement digest) | `StageCapabilityRequirement` | yes (capability ids, delegation modes) | yes, `stage_requirement_ref.digest` in bindings and graph assemblies | fixed |
| `application/reference_research/service.py` `prepare_reference_implementation` (requirement ref digest) | `StageCapabilityRequirement` | yes (always one member there) | yes, in the prepared binding | fixed for agreement with the compiler; value unchanged |
| `application/schema/schema_context_stage_handlers.py` `prepare` (template ref) | `OperationExecutionRequest` | yes | yes, in the frozen plan's `exact_input_refs` | fixed |
| `application/schema/schema_grounding_semantic_handlers.py` `prepare` (template ref) | `OperationExecutionRequest` | yes | yes, same | fixed |
| `application/schema/schema_workspace_binding.py` `decide` (decision id) | `GraphAdmissionRequest` | yes (`GraphCapabilityGrant` sets) | yes, `decision_id` is the compatibility record id | fixed |
| `application/schema/supporting_graph_reconciliation.py` `_reconciliation_request_digest` | `SupportingGraphReconciliationRequest` | yes | yes, reconciliation request digest | fixed |
| `application/workspaces/sandbox_snapshots.py` `create` (creation identity) | `SandboxSnapshotCreateRequest` | yes (`SnapshotCapabilityShape`) | yes, claimed in the snapshot repository | fixed |
| `application/workspaces/sandbox_snapshots.py` `clone_restore` (request fingerprint) | `SnapshotCloneRequest` | yes | yes, claimed per clone | fixed |
| `domain/coordinator/launch.py` `WorkflowLaunchProposal.digest` | `WorkflowLaunchProposal` | yes (`ActorContext` sets) | yes, in the launch ticket (15-minute TTL) | fixed |
| `domain/graph_runtime/definitions.py` `RuntimeDefinition.digest` | `GraphAssemblyDefinition`, `MCPServerDefinition`, `ContextPolicyDefinition`, `MiddlewareStackDefinition` (subclasses with sets) | yes | yes, as definition ref digests | fixed |
| `application/orchestration/mongo_goal_directed_repository.py` `persist_templates` (`document_digest` and payload) | `OperationExecutionRequest` | yes | yes, Mongo document and its exact-replay comparison | fixed |
| `application/web_research/web_research_semantic_handlers.py` `_append` (`content_digest`, `record_id`, payload) | `PublicGoalAdmission` (3-member set), `CitedFinding` (`provider_names`), `CitedSynthesis`, `VerifiedWebResearchResult` | yes | yes, `WebResearchRecordEnvelope` | fixed |
| `application/control_plane/service.py` `_persist_erc` (payload bytes, `payload_ref`, stored record) | `EffectiveRunConfiguration` | yes (sets of models) | yes, ERC record and payload-store address; a second process could raise "ERC digest collision" (reproduced by the regression test) | fixed |
| `application/operations/postgres_operation_journal.py` (transition and event replay proofs); `application/operations/mongo_operation_authority_migration.py` (binding replay proof) | `LifecycleTransitionRecord`, `DomainEventEnvelope`, `OperationExecutionBinding` | yes | compared with stored JSON written by another process | fixed with `stored_payload_matches` (adjacent equality proofs, not digests) |

### Not affected (kept on the JSON dump, allowlisted in the guard with proof)

Each entry lives in `AUDITED_NOT_AFFECTED` in `tests/unit/control_plane/test_digest_set_order_guard.py` with the model classes. `test_audited_models_hold_no_sets` checks from the classes that none holds a set (transitively, including subclasses), so a later set added to an audited model fails the guard.

| Sites | Models | Reason |
|---|---|---|
| journal claim digests (`operation_journal.py` `validate`, `journaled_operation_execution.py` `acquire`) | `OperationEffectClaim` | no set |
| `operation_execution.py` workspace contract digest | `WorkflowWorkspaceContract` | no set |
| `postgres_stage3_kernel_repository.py` (`reserve`, lineage edge), `runtime_decisions.py`, `runtime_lineage.py` | `ForkRequest`, `LineageParentEdge`, `DecisionRequest`, `PersistedExecutionLineage` | no set |
| `runtime_run_plan.py` alias evidence digests (`compile_run_plan`, `_v3`, `_v4`) | `AliasBinding` | no set |
| `reference_research/service.py` resources, fixture, lease request | `ExecutionResourceEnvelopeV2`, `QualiaFixtureInput`, `DaveFixtureInput`, `ResourceLeaseRequest` | no set |
| `run_control_repository.py` family mutation, `reducer.py` evidence frontier, `orchestration/service.py` obligation evidence | `AtomicFamilyMutation`, `AcceptedObligationEvidence`, `AcceptedOutputEvidence` | no set |
| `graph_query.py`, `supporting_graph_reconciliation.py` (`run`, `_terminal_result`, evidence), `schema_grounding_semantic_handlers.py` intent, `schema_catalog_build.py` | `QueryExecutionIntent`, `GraphReconciliationEvidence`, `SchemaCatalogBuildRequest` | no set; `dict[str, Any]` members are JSON payloads |
| `schema_context/validation.py`, `schema_catalog/parser.py` | `SchemaContextSelection`, `SchemaSelectionReview`, `PhysicalSchemaCatalog` | no set |
| workspace manifest and reservation digests (`mongo_workspace_repository.py`, `workspace_materialization.py`, `operation_execution/materialization.py`, `conformance_operation_runtime.py`) | `WorkspaceMaterializationRequest`, `WorkspaceMaterializationManifest`, `WorkspaceMount`, `ExactDefinitionRef` | no set |
| `domain/graph_runtime/contracts.py` submission and intervention digests | `GraphExecutionSubmission`, `InterventionBase` (no subclass holds a set) | no set |
| `domain/coordinator/launch.py` `SemanticBindingPlan`, `domain/operation_execution/journal.py` settlement, `checkpoint_lineage.py` observation | `SemanticBindingPlan`, `OperationJournalSettlement`, `CheckpointTransitionObservation` | no set; `Any` members are JSON payloads |
| `web_research_coordinator_live.py` `_launch_proposal` (selected refs), `web_research_semantic_binding.py` ref checks | `ExactDefinitionRef` | no set |
| `postgres_workflow_result_repository.py`, `goal_directed.py` reconcile | `WorkflowResultRecord`, `OperationWorkflowResult` | no set; `Any` members are JSON payloads |
| `dynamic_research_swarm/{evaluators,repository,temporal_activities}.py` | swarm `GateResult`, `MissionPlan`, `SourceBundle`, `ResearchUnitResult`, `FinalSynthesis` | no set (experiment package; persist only) |
| `web_research_coordinator_live.py` `_run_mounted_mcp_planning`, `web_research_runtime.py` `_tools_snapshot_digest` | third-party MCP `Tool` | not a BellLabs contract |

### Digests that never used a JSON dump (checked, not affected)

- `sha256_digest(model)` and `sha256_digest(model.model_dump(mode="python", ...))` (`operation_execution/contracts.py`, `run_control_repository.authority_state_digest`, `external_candidate_inspection.py`): `_normalize` sorts sets.
- `StageGraphAcceptedProjection.digest` (`asdict` of a dataclass with `frozenset` fields): `asdict` keeps frozensets and `_normalize` sorts them.
- `graph_runtime/definitions.py` `_model_content`, `control_plane` definition and ERC digests: pass model attributes, not JSON dumps.
- `schema_grounding/definitions.py:680` `tuple(frozenset)`: each frozenset has exactly one member, so the order cannot differ. `goal_workspace.py` digests: dataclass dict payloads with no sets.
- `operation_execution.py` `settlement_result_manifest` (`json.dumps` of `OperationSettlement` without payload fields, hashed at the manifest write): `OperationSettlement` holds no set (`Any` members are JSON payloads).
- `app/integrations/agents/deep_agents/async_subagents.py:212` hashes a result string; `app/application/async_subagents/postgres_async_subagents.py` persists an `AsyncSubagentMessage` (no set) and derives no digest. Nothing is deferred to RRM-013; the guard reports any future site in those areas.

### Residual risk, stated

- `Any`-typed members (`dict[str, Any]` payloads) could hold a set only if a caller injected one; they come from validated JSON documents. The guard cannot see them. Fixing them would mean changing the audited sites to `stable_json_digest`, which is value-identical for set-free data.
- Non-digest raw-JSON equality: the `_dump` writers of the Postgres repositories persist sets in iteration order (correct on read through `model_validate`). Comparisons of stored JSON with a fresh JSON dump remain in `postgres_operation_journal.py` (`authority_result`, `command_result`, ledger `entry`) and `postgres_runtime_execution_repository.py` (`intervention`, `binding`); none of those models holds a set today. They are outside "digest" and were not changed; a candidate for the CR-5 whole-mission pass.

## Changed paths and migrations

No storage migration, contract id or schema version changed. No Temporal name or payload field changed. Call-site edits are one line each.

- Canonical: `app/domain/control_plane/canonical.py`.
- Digest sites: `app/application/orchestration/linked_runs.py`, `app/application/orchestration/mongo_goal_directed_repository.py`, `app/application/reference_research/service.py`, `app/application/runtime/postgres_runtime_authority.py`, `app/application/runtime/runtime_run_plan.py`, `app/application/schema/schema_context_stage_handlers.py`, `app/application/schema/schema_grounding_semantic_handlers.py`, `app/application/schema/schema_workspace_binding.py`, `app/application/schema/supporting_graph_reconciliation.py`, `app/application/web_research/web_research_semantic_handlers.py`, `app/application/workspaces/sandbox_snapshots.py`, `app/application/control_plane/service.py`, `app/domain/coordinator/launch.py`, `app/domain/graph_runtime/definitions.py`.
- Replay proofs: `app/application/operations/postgres_operation_journal.py`, `app/application/operations/mongo_operation_authority_migration.py`.
- Tests: `tests/fixtures/set_order.py`, `tests/fixtures/digest_sites.py`, `tests/unit/control_plane/test_contract_digest_set_order.py`, `tests/unit/control_plane/test_digest_set_order_guard.py`; `tests/unit/run_control/test_run_control.py` (moved its helper to the shared fixture).
- No golden digest in any existing test changed. The one edited existing test only swaps its local helper for the shared one.

## Forward-compatibility decisions for persisted digests

Principle: for a contract whose sets have at most one member (or no set), `stable_json_digest` equals the old JSON-dump digest, so values already stored stay valid and no re-derivation is needed. For a contract with a set of two or more members, the old stored digest was one of several seed-dependent values and was never reproducible across processes, so no value is "broken" by the change: a replay already failed in about 1 run in 8 (RRM-004). No dual-read was added because a dual-read would have to enumerate every set permutation to accept every old value.

| Persisted digest | Decision |
|---|---|
| Linked-run `request_fingerprint` (Postgres) | Pre-production, accepted. Unchanged for one-member sets; a pre-upgrade link with a larger set may raise `IdempotencyConflict` on a retry, as it could before. |
| Snapshot `creation_identity`, clone `request_fingerprint` (Mongo) | Pre-production, accepted, same reasoning. |
| Graph admission `decision_id`, web research `record_id`/`content_digest`, reconciliation request digest | Pre-production, accepted. Records written by one process were already unmatched by another when the set had two or more members. |
| GoalDirected template `document_digest` and payload (Mongo) | Pre-production, accepted. The read path verifies `sha256_digest(stored payload)` against the stored digest, which stays self-consistent for every pre-existing document (it hashes the stored list, not a fresh dump); only a re-persist of an old document with a different order can differ, as before. |
| ERC `payload`, `payload_ref`, record | Pre-production, accepted. The ERC `digest` itself (Python-mode `sha256_digest`) is unchanged. A pre-upgrade record with larger sets may differ from a re-save; the re-save was the source of the "ERC digest collision". |
| Runtime `lifecycle_projection_digest` in checkpoints | Fail closed by design: a checkpoint saved before the upgrade with a multi-member set mismatches the fresh digest and goes through the existing bootstrap reconciliation decision (`runtime_bootstrap.py`), not silent acceptance. Unchanged for one-member sets. |
| Stage requirement ref digests, `RuntimeDefinition.digest` refs | Unchanged for one-member sets (all reference fixtures). Pre-upgrade refs with larger sets were not reproducible across processes. |
| Launch proposal digest | Ephemeral: launch tickets expire after 15 minutes. |
| `contract_fingerprint` (RRM-004) | Value unchanged for every contract the old Python-mode dump could represent (proved in `test_contract_fingerprint_handles_sets_of_models_and_keeps_python_dump_values`); RRM-004's stored fingerprints stay valid. |

If the coordinator wants the stricter rule (no accepted break for persisted data), the available option is a one-time re-derivation migration of the rows above; none was written because the mission is pre-production and no row can be proven to be in production use.

## Deterministic verification

Commands (from the worktree, `unset VIRTUAL_ENV`, `uv run --no-sync`):

| Command | Result |
|---|---|
| `uv run --no-sync ruff check app tests scripts` | All checks passed |
| `uv run --no-sync mypy app` | Success: no issues found in 341 source files |
| `BELLABS_RUN_WP_BP_010_LIVE=0 BELLABS_RUN_WP_BP_020_LIVE=0 BELLABS_RUN_WP_CP_040_LIVE=0 LANGSMITH_TRACING=false uv run --no-sync pytest -q` (hermetic, no `.env`) | 823 passed, 54 skipped, 2 xfailed (baseline 748 / 54 / 2; +75 new tests) |
| same environment plus `TEST_APPLICATION_POSTGRES_DSN='postgresql://belllabs:belllabs-local@127.0.0.1:55432/belllabs' TEST_MONGODB_URI='mongodb://127.0.0.1:27017/?directConnection=true'` and `uv run --env-file ../biotech-research-ingestion-evaluation-system/.env` | 853 passed, 24 skipped, 2 xfailed (baseline 778 / 24 / 2; +75) |
| `git diff --check` | clean (only the Windows LF/CRLF notices) |

The first full runs (head `8960c53`) were 822 and 852 passed; the second code commit added one test and the gates were re-run on `1dd8013`. Both full runs held the shared-stack lock and released it.

Red/green proof for the site fixes: the code changes at the sites (all of `8960c53` except `canonical.py`) were reversed with `git apply -R` and then re-applied. Without them, all 11 site tests in `test_contract_digest_set_order.py` fail, `test_runtime_definition_digest_is_independent_of_set_iteration_order` among them, and `test_no_unaudited_json_dump_digest_sites` fails listing the 16 reverted sites. With them, everything passes.

Value compatibility proof (the "old value was seed-dependent" requirement): no golden digest in any existing test changed, because every existing golden uses set-free contracts or one-member sets. `test_stable_json_dump_sorts_sets_and_matches_json_mode_for_set_free_contracts` asserts `stable_json_dump(x) == x.model_dump(mode="json")` and the digests are equal for one-member sets; `test_runtime_definition_digest_is_independent_of_set_iteration_order` asserts the same for `RuntimeDefinition.digest`. For multi-member sets `assert_json_dumps_differ` shows the JSON dumps of equal contracts differ, which is exactly why the old digest was not a stable value.

### Seed sweep

`PYTHONHASHSEED=<seed> uv run --no-sync pytest -q tests/unit/control_plane/test_contract_digest_set_order.py tests/unit/control_plane/test_digest_set_order_guard.py tests/unit/run_control/test_run_control.py`

| Seed | Result |
|---|---|
| 0 | 87 passed |
| 1 | 87 passed |
| 2 | 87 passed |
| 3 | 87 passed |
| 7 | 87 passed |
| 42 | 87 passed |
| 12345 | 87 passed |
| 99999 | 87 passed |

## Live runtime qualification

Not applicable: no provider, model, Temporal workflow or Agent Server behavior changed. The opted-in Postgres and Mongo integration suites ran as part of the 853-test run above against the coordinator's disposable containers (`127.0.0.1:55432`, `127.0.0.1:27017`). No live LLM spend.

## Replay and recovery artifacts

No Temporal code, workflow input, signal, query or payload field changed, so no captured history needed re-recording and replay compatibility is unaffected. The recovery path that RRM-004 fixed (`operation_journal.py`, `journaled_operation_execution.py`, `operation_execution.py`, `run_control/service.py`) is unchanged; `contract_fingerprint` keeps its value.

## Replacement and deletion checks

- Replaced: the 16 JSON-dump digest sites above and the local `_equal_sets_with_different_iteration_order` helper in `tests/unit/run_control/test_run_control.py` (now the shared `tests/fixtures/set_order.py`). No superseded owner remains.
- `grep` of `app/` for `sha256_digest(<x>.model_dump(mode="json"...))`: only audited, set-free sites remain (the guard enforces this).
- No `v2`, `new` or `helper` names were introduced.

## Unresolved risks and drift checks

- Raw-JSON equality comparisons and the `Any` payload caveat are recorded under "Residual risk, stated". No known affected site is left.
- A set-of-models contract now fingerprints correctly, but no current `contract_fingerprint` caller holds one; the new test covers it so a future caller does not hit the old `TypeError`.
- The guard recognises sinks by name (`sha256`, `digest`, `fingerprint`, `canonical_json`, `content_hash`, `reservation_token`) and local-variable flow inside one function. A digest built through a differently named helper in another function would escape it; `test_audited_models_hold_no_sets` and the ticket's audit method (runtime walk of models holding sets) are the backstop.
- Reusable seams (mission horizon): `stable_json_digest`/`stable_json_dump` for any JSON-shaped contract digest; `stored_payload_matches` for exact-replay proofs; `tests/fixtures/set_order.py` `with_different_set_orders` for any future contract identity test; the ast guard in `tests/fixtures/digest_sites.py` for any future "no JSON-dump identity" rule.
- Drift checks: no change to `AGENTS.md`, specs or contract ids; `app/agent_server/`, `async_subagents.py` and `app/application/async_subagents/` untouched.

## Final disposition

ready_for_review
