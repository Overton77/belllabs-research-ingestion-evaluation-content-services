---
type: Removal Guide
title: Clean-break removal and relocation guide
description: "This guide covers the owner-authorized Mission Control organization cleanup. It distinguishes source removal from deleting data. No historical Mongo backfill, record purge, live destructive migration or old…"
tags: [mission-control, removal, evidence]
---
# Clean-break removal and relocation guide

This guide covers the owner-authorized Mission Control organization cleanup.
It distinguishes source removal from deleting data. No historical Mongo backfill,
record purge, live destructive migration or old endpoint/history compatibility is
required by this clean break. Existing user data remains preserved.

## Active replacements

| Previous location or authority | Current replacement | Behavioral consequence |
| --- | --- | --- |
| app/ application package | src/mission_control/{contracts,domain,application,adapters,interfaces,bootstrap} | One import namespace; callers update imports rather than relying on aliases |
| app/api | src/mission_control/interfaces/http | Application-scoped authenticated transport |
| app/temporal | src/mission_control/adapters/temporal | Same governed runtime behavior under adapter ownership |
| app/application/*/postgres_* | src/mission_control/adapters/postgres | Concrete SQL separated from application logic |
| app/migrations | src/mission_control/adapters/postgres/migrations | Ordered SQL preserved and bundled; no applied checksum rewriting |
| app/server.py | src/mission_control/bootstrap/technical_api.py | Lower-level authoring/control/realtime proof facade; not primary public startup |
| mission_control.api, worker, preflight | mission_control.bootstrap.api, worker, preflight | Configured entrypoints live at the composition boundary |
| app/experiments and packaged qualification prototypes | experiments/ | Prototype code is outside the runtime wheel |
| Biotech schema/research implementation in kernel | integrations/biotech/src/biotech_mission_adapters | Optional application integration; no kernel imports of application code |
| Competing LangGraph configuration files | agent_server/langgraph.json | One canonical bounded-graph registration surface |
| Old BellLabs README and active Cursor authority | Root/scoped AGENTS.md and docs/knowledge | General architecture and actual implementation navigation |

The redundant `bootstrap/legacy_preflight.py` is removed. The optional technical
facade preserves tested lower-level contract/realtime surfaces using the same
handlers and engine; it is not an old application fallback or second scheduler.

The versioned [organization evidence](organization/README.md) survives a fresh
checkout. It includes the complete [initial file map](organization/file-map.json),
[initial import map](organization/module-map.json),
[experiment supplement](organization/experiment-relocation.json),
[Biotech extraction](organization/biotech-module-map.json) and
[adapter regrouping](organization/adapter-module-map.json). These record the
physical move; later removals/extraction can supersede an intermediate destination.
Consult the current source and sections below.

## Mongo implementation retirement

PostgreSQL replacements own immutable definitions/configuration, operation/family
bindings, asynchronous details, workspace/artifact metadata, candidate content and
snapshots. Transitional migrations 0027-0036 establish the initial storage chain.
Cleanup adds 0037 catalog projection processing, 0038 external discovery/inspection,
0039 sandbox snapshot contract slots and 0040 snapshot identity indexes. These were
exercised against isolated local PostgreSQL; no live application is implied.

The persistence slice's exact 54 retired paths are versioned in
[the retirement inventory](organization/mongo-removals.json); none remains at its
recorded location. They include Mongo family/operation/workspace/artifact/subordinate
repositories, Beanie document models, obsolete journal-backfill/routing code and
Mongo-only fixtures/proofs. The SQL migration history is preserved.

Shared deterministic fixture models and pure/InMemory contracts remain. Mongo
bootstrap functions were removed from the retained RRM009/RRM013 harness helpers;
removing a helper is not necessary when PostgreSQL acceptance still uses its models.

Standalone sandbox snapshot semantics briefly used a transitional PostgreSQL store.
That store is now retired (see [common component cutover](#common-component-cutover-2026-10-03)):
`adapters/postgres/workspaces/snapshot_repository.py` only raises
`SandboxSnapshotPersistenceRetired`.
PostgreSQL acceptance proofs replace the old Mongo-backed fixture entrypoints;
retired tests are not counted as passing tests. Final post-move test evidence is
recorded separately from the earlier baseline in the implementation status.

Removing source is not permission to drop collections, remove volumes or copy
historic records into the new schema. Archived data must not silently become a
supported new execution history.

## Application-specific extraction

The exact namespace map is the versioned
[Biotech extraction map](organization/biotech-module-map.json). The optional Biotech
integration retains bounded schema/research interfaces and
application-specific composition. Its source must not be imported by the general
kernel. This extraction is not the deferred broad KnowledgeServices generalization
audit and does not certify every domain adapter's persistence backend.

Nine retained domain scripts moved from root `scripts/` to the optional package's
`biotech_mission_adapters.bootstrap.scripts` modules, retaining their basenames:

- `promote_schema_grounding_surface`
- `provision_schema_deployment_evidence`
- `compare_schema_context_runs`
- `diagnose_schema_deployment_snapshot`
- `load_trudiagnostic_graph`
- `reconcile_zero_count_schema_artifacts`
- `run_web_research_coordinator_live`
- `stage_schema_grounding_live_inputs`
- `promote_reviewed_web_capabilities`

Install the optional development group with `uv sync --group biotech` (plain
`uv sync` excludes the domain dependencies). Then invoke the needed module as
`python -m biotech_mission_adapters.bootstrap.scripts.<name>`. Old root-script
aliases are not retained. Their Mongo bootstrap was replaced with explicit scoped
PostgreSQL composition; no live domain script execution is claimed by this move.
The former script `run_web_research_stagegraph_local` was retired because
its fake lifecycle bypassed admission and called a stale operation API. Its
post-rewrite source is recoverable in
`.scratch/organization-checkpoint/removed-domain-scripts/`. Use governed admission
or the deterministic production-stack tests instead.

The assertion-free permanent skip
`tests/unit/schema/test_schema_grounding_services.py::test_goal_semantic_handlers_execute_and_independently_rehydrate_reconciliation`
was removed. It only skipped an already-deleted direct BoundGoal handler path;
actual operation-template/family acceptance tests retain the replacement proof.
Reviewed web-capability fixtures/promotion and reviewed payload resources also
move into the optional package, so research-specific workflow strings do not
remain generic kernel configuration.

The optional package owns its own
`src/biotech_mission_adapters/migrations/0001_scoped_records.sql` with forced RLS
and immutable scoped records. It is outside the general migration chain.
`BiotechSettings` owns Neo4j/schema credentials, and schema resources moved to the
optional package. No live domain operation or Neo4j write was executed.
See [optional integration guide](../integrations/biotech/README.md).

Application policies and extension validators must be explicitly registered by
trusted deployment composition. Missing registration fails closed; a generic API
does not insert permissive Biotech defaults.

## Composition and capability ownership

Trusted wiring moved from application/HTTP modules into bootstrap:
`coordinator_composition.py`, `operation_recovery_composition.py` and
`runtime_control.py`. No import aliases remain at their former application or
interface locations. The former infrastructure catchall is split into storage,
auth, Agent Server,
Deep Agents persistence, capabilities, realtime and operation adapter packages.
For example JWT verification is `adapters/auth/jwt.py`, and realtime transport is
`adapters/realtime/postgres_redis.py`. Framework-neutral payload protocols now live
under `application/ports/payloads.py`; provider transport errors and overwrite
values cross
that boundary as neutral application contracts. Architecture guards are in
`tests/architecture/test_package_boundaries.py`.

Generic discovery and inspection now belong in `application/capabilities`, with
scoped PostgreSQL candidate and projection persistence. The obsolete concrete
`adapters/capabilities/catalog_projection_admin.py`, `catalog_projection_events.py`
and `adapters/document_models/external_capability.py` are removed. Their durable
replacements are `adapters/postgres/capability/projection_events.py` and
`external_candidates.py`; the application projection logic remains framework-neutral. Catalog HTTP/CLI handlers
share the same trusted installation binding and application services. Their
presence does not admit an external candidate or authorize execution.

Seven general catalog scripts remain under root `scripts/`, now using explicitly
configured scoped PostgreSQL rather than Mongo bootstrap:

- `discover_external_capabilities.py`
- `rebuild_capability_search_projection.py`
- `search_capability_catalog.py`
- `process_capability_search_projection_events.py`
- `verify_capability_search_projection.py`
- `promote_coordinator_surface.py`
- `evaluate_coordinator_retrieval.py`

The eighth converted script, `promote_reviewed_web_capabilities.py`, is
application-specific and moved to the optional Biotech package listed above.
Catalog tenant arguments cannot override the configured trusted catalog scope.
The generic `scenario_b_live` runner also uses scoped PostgreSQL; no live execution
or metered discovery is implied by converting these entrypoints.

The canonical `skills/mission-control` directory manifest is now version 0.1.1 and
includes catalog operations. The design/launch helper was renamed from
`.agents/skills/belllabs-workflow-coordinator` to
`.agents/skills/mission-control-coordinator`; its authored asset identity is
`skill.mission-control-coordinator`. Promotion/source references were updated;
there is no old-name alias. Download and verify entire versioned directories,
including references and manifests.

## Agent Server consolidation

Removed root `langgraph.json`, `langgraph.async_subagents.json`,
`langgraph.block_c.json` and `langgraph.block_c_n1.json`; use only
`agent_server/langgraph.json` with an explicit runtime/qualification profile.
Two tracked environment files were consolidated into `agent_server/runtime.env`
containing environment references rather than credential values.

Removed adapter subapps `async_subagents/http_app.py` and
`block_c_qualification/http_app.py`; both now use the parent Agent Server HTTP app.
Authentication modules retain specialized verification/policy helpers, while
`deployment_auth.auth` is the sole registered authentication entrypoint. Profile
checks cover native `threads/create_run` and exact assistant UUIDs as well as graph
names. A disallowed registered graph returns 403 (the old unregistered path could
return 404); supported qualification interrupt/resume behavior is preserved.

Actual local runtime, qualification and N1-only tests pass in
`tests/integration/agent_server/test_canonical_server_local.py`; 40 unit tests pass.
The 19 skipped external PostgreSQL/hosted drills remain unproved. This consolidation
does not turn Agent Server into a mission scheduler or certify production hosting.

## Compose volume identity

The Compose project namespace intentionally remains
`biotech-research-ingestion-evaluation-system` so existing local volume identities
survive the source-directory rename. It is not the product or Python package name.
Changing it requires an explicit volume migration; do not rename it or remove
volumes as an incidental cleanup step. The Supabase project name independently
remains `biotech-research-ingestion`.

## Preserved material and recovery

- Existing unrelated working-tree changes and prior user deletions are preserved.
- Ignored `app/personal_code/` remains user-owned and was not removed.
- Original source checkpoint: `.scratch/organization-checkpoint/source-before-organization.zip`.
- Earlier implementation diff: `.scratch/mission-control-parity-checkpoint/tracked.patch`.
- Pre-documentation rewrite copies:
  `.scratch/mission-control-parity-checkpoint/documentation-before-organization/`.
- The historical discussion/WP trees were deleted by the owner (85 files, pending);
  see [pending owner documentation deletions](#pending-owner-documentation-deletions).
  Previously deleted historical documents were not recreated.

Recovery is selective: inspect the map/archive, extract the required file to a
temporary comparison directory, review it against concurrent work, and restore
only the intended content. Never use git reset/clean, whole-tree extraction over
the current workspace, or database deletion as a recovery shortcut. Do not restart
old and new workers on the same live executions without an explicit compatibility
decision.

## Common component cutover (2026-10-03)

Recorded after the implementation lanes landed; exact paths are in the inventory
below. None of these removals drops a schema, table or row: legacy data in live or
local databases stays where it is under separate persistent-data authorization.

- **Transitional composition retired.** `storage_mode` accepts only
  `production_common`; API, worker and preflight verify the installed common release
  (`bootstrap/common_installation.py`). `transitional_local`, `belllabs.request_scope`
  and `belllabs.catalog_scope` have no production use.
- **Repositories converted.** Every PostgreSQL repository uses `mission_control` /
  `mission_control_search` with transaction-local `mc.*` scope
  ([persistence map](plans/mission-control-two-project-rollout/persistence-map.json)).
- **Legacy runner removed (2026-10-03).** `apply_application_migrations`,
  `apply_capability_search_migrations` and `create_application_migration_pool` in
  `adapters/postgres/connections.py`, `scripts/migrate_application_database.py`,
  `scripts/migrate_capability_search.py` and `bootstrap/installation.py` were removed
  after every test moved to the common fixture. Precondition proof: no caller in
  `src`, `tests` or `scripts`; the independent static scan
  (`tests/qualification/two_project/test_static_architecture.py`). Recovery: the exact
  pre-removal bytes and SHA-256 are in `.scratch/two-project-rollout-checkpoint/retired-legacy-runner/` and in git at
  `f6521c1`. The historical chain itself stays as pinned bytes and is no longer
  executable from this repository.
- **Sandbox snapshot store retired.** The common release defines no sandbox snapshot
  table and nothing in production composition constructed the transitional store;
  `PostgresSandboxSnapshotRepository` now raises `SandboxSnapshotPersistenceRetired`.
  The capability is unavailable rather than silently backed by another store.
- **Runtime event relay removed** from `bootstrap/technical_api.py` (it read
  `belllabs_control.agent_runtime_events`, which had no constructed writer).
- **LangGraph saver/store** moved from `belllabs_langgraph` to `mission_control_runtime`
  (runtime phase of `mission-db`); legacy and business schemas are refused.

### Retired legacy tables

Taken verbatim from `retired_legacy_tables` in the persistence map. These transitional
tables have no destination in the common release; their code paths were removed or
had no production writer. Existing rows are not migrated or deleted.

| Legacy table (`belllabs_control`) | Evidence for retirement |
| --- | --- |
| `agent_runtime_checkpoints` | save/load/complete_checkpoint have no caller (rg 'save_checkpoint/load_checkpoint/complete_checkpoint' src integrations tests: none outside the module); methods removed |
| `agent_runtime_events` | PostgresRedisRuntimeEventBus never constructed (rg PostgresRedisRuntimeEventBus: only its own module); class removed |
| `agent_runtime_messages` | no writer in src (G0) |
| `agent_runtime_sessions` | no writer in src (G0 legacy-tables.json has_live_writer=false) |
| `execution_resource_leases` | only PostgresResourceLeaseJournal writes it; rg PostgresResourceLeaseJournal: only stage3 module and its test; class removed |
| `operation_journal_backfill_batches / _applied_batches / _quarantine` | historical backfill out of scope (G1 section 5); operation_journal_backfill.py no longer exists in src |
| `runtime_async_tasks` | no writer in src (G0) |
| `runtime_checkpoint_observations` | only a retention DELETE in the tests-only PostgresStage3RetentionRepository (G0) |
| `runtime_decision_responses (via DurableDecisionService.answer)` | converted anyway: PostgresDecisionRepository is constructed in production (runtime_authority -> agent_server/http_app.py) |
| `runtime_interrupt_decisions` | no writer in src (G0) |
| `runtime_interrupt_requests` | no writer in src (G0) |
| `runtime_lineage_records (provenance read)` | PostgresExecutionLineageRepository (append/provenance_for_result) constructed only in tests; append_lineage_in_transaction kept and converted (execution_lineage_record) |
| `runtime_repair_audit` | only PostgresRuntimeIncidentRepository writes it; rg: only stage3 module and its test; class removed |
| `runtime_retention_deletion_audit` | only PostgresStage3RetentionRepository writes it; rg: stage3 module and two tests; class removed |
| `schema_migrations` | replaced by mission_control.component_release (lead/database lane) |

## Two-project rollout removal inventory (2026-10-03)

Exact inventory for [plan section 13](plans/mission-control-two-project-rollout/IMPLEMENTATION_PLAN.md#13-removal-guide-clean-code-and-documentation-closure),
observed against HEAD `f6521c1` plus the uncommitted rollout work. Machine-readable
companions: [removal-inventory.json](organization/removal-inventory.json) and
[legacy-belllabs-control-chain.json](organization/legacy-belllabs-control-chain.json).
**"Safe later" is not permission to delete now.** Rows not marked "done" keep their
paths until the named precondition and proof exist; source cleanup never drops
schemas, rows, buckets, collections or volumes.

| Path | Exists / tracked now | Class | Replacement | Unique evidence (hash / location) | Removal precondition | Test / receipt proving it | Disposition |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 54 paths in [mongo-removals.json](organization/mongo-removals.json) (table below) | all absent; none tracked | already-removed | PostgreSQL adapters per path below | `f6521c1` history; source zip `sha256:84f23f73db7446bd3bbeb9d9d2a74c0e16f1739e31d900805e8d577bde649d4c` | met | `removal-inventory.json`: `all_absent=true`, `all_untracked=true`, `replacements_missing=[]` (absence is not test evidence) | done; never recreate or count as passing tests |
| Old `app/` source tree (483 entries in [file-map.json](organization/file-map.json)) | 0 mapped sources exist; only `app/.cursor/rules/user_subagent_preference.mdc` tracked | already-removed | `src/mission_control/**`, `integrations/biotech/**` per the maps | source zip above | met | `removal-inventory.json` `app_sources_present_now=[]`; G0 resolved 159 intermediate destinations, 0 unresolved | done |
| `app/{agent_server,api,application,domain,experiments,integrations,mcp,middleware,migrations,models,temporal}/`, `app/__pycache__/` | removed 2026-10-07; were ignored bytecode only (524 `.pyc`, no source) | regenerable cache | none needed | none (bytecode of removed sources) | met (owner-approved sweep; `app/personal_code` not traversed) | absence of those directories; `app/.cursor` and `app/personal_code` remain | done |
| `app/.cursor/` (14 ignored URL maps + 1 tracked rule) | exist | preserve unique evidence / user material | none | the files | owner decision | n/a | preserve |
| `app/personal_code/` | exists, ignored | preserve (user-owned) | none | user content (not read) | never by cleanup | n/a | preserve |
| `src/mission_control/adapters/infrastructure/` | removed 2026-10-07; was 15 `.pyc` and 0 sources | regenerable cache | split adapter packages ([adapter-module-map.json](organization/adapter-module-map.json)) | none | met in the same owner-approved sweep | directory absent | done |
| Root `langgraph.json`, `langgraph.async_subagents.json`, `langgraph.block_c.json`, `langgraph.block_c_n1.json` | absent, untracked | already-removed | `agent_server/langgraph.json` + `agent_server/runtime.env` (tracked) | `f6521c1` history | met | `removal-inventory.json` `root_langgraph_configs`; `tests/integration/agent_server/test_canonical_server_local.py` | done |
| Six further removals (two Agent Server subapps, `bootstrap/legacy_preflight.py`, catalog projection admin/events, external capability document model) | absent, untracked | already-removed | sections above | `f6521c1` history | met | `removal-inventory.json` `other_removals` | done |
| `src/mission_control/adapters/postgres/migrations/` (0001-0040) | 40 files exist, tracked, unmodified | keep-until-parity | `packages/mission-control-db-contract/component/migrations/` | chain `sha256:8ca2f1504e110e7c17b6757803706d8342e59fdaee37571eb3d609d86f6436e4`; per-file SHA-256 and blob ids in [legacy-belllabs-control-chain.json](organization/legacy-belllabs-control-chain.json) | G3 parity on both disposables (met locally, run `20261003-g3-r1`); zero legacy references in production code; no caller of the legacy runner (still in `adapters/postgres/connections.py`, used by `scripts/migrate_application_database.py` and `scripts/migrate_capability_search.py`) | `tests/qualification/two_project/test_static_architecture.py` (chain pin + production scan; the scan currently fails only on `connections.py`) | keep in place; bytes never edited; never applied to live; runner removal pending |
| Transitional composition branches (`transitional_local`) | removed: `api.py`, `worker.py`, `composition.py`, `preflight.py` accept only `production_common` (`bootstrap/common_installation.py`); every repository converted to `mission_control`/`mission_control_search` | already-removed | `production_common` composition | `f6521c1` history | met; the legacy runner and `bootstrap/installation.py` were removed 2026-10-03 | production scan; poisoned/absent legacy round trip (`test_legacy_poison_round_trip.py`) passes | done |
| `tests/fixtures/mission_control_production_stack.py` | exists, tracked | keep-until-parity | `tests/fixtures/mission_control_common_db.py` + production stack on the common release | `f6521c1` blob | every importing acceptance test runs on the common component | G3 acceptance on both disposables | keep |
| `../biotech-postgres-db-contract/src/biotech_postgres_db_contract/` (`installer`, `deployment`, `fingerprint`, `preflight`, `verification`, `cli`) | exists; sibling has no git repository | replace-then-remove (separately reviewed) | `mission_control_db_contract` (`mission-db`) | archive a copy before editing (no VCS history) | thin `biotech-db` wrapper proven equivalent; integrity/recovery tests ported | DB-lane wrapper tests + `tests/qualification/two_project/test_release_parity.py` | keep until the reviewed wrapper lands |
| `../biotech-postgres-db-contract/component.lock.json` | exists (`status: blocked_missing_common_release`) | replace-then-remove | `deployments/biotech/release.lock.json` from `mission-db lock` | `sha256:0c03c6d41e1a380f4e19853f90fa49b93ca68e912fa765ee28f8ebbfc4d0bae3` | a real accepted pin exists | `load_release` verification of the accepted lock | keep (the blocked lock is truthful) |
| `experiments/` | exists; 34 tracked files | conditional prototype | governed production paths | tracked history | every consumer/proof mapped or deliberately retired | per-consumer mapping | keep; never bulk-delete |
| `src/mission_control/bootstrap/technical_api.py` | exists, tracked; runtime event relay (`belllabs:runtime:*` Redis to `belllabs_control.agent_runtime_events`) removed | conditional prototype | `bootstrap/api.py` public facade | tracked history | remaining proof surfaces mapped | production scan (passes for this file) + facade tests | keep |
| `src/mission_control/bootstrap/runners/scenario_b_live.py` | exists, tracked | conditional prototype | governed admission | tracked history | consumer retired or converted | n/a | keep |
| `.tmp-cleanup-unit/`, `.tmp-worker-poll-20261003/` | exist, untracked; access denied to this account | unclassifiable under current account: preserve | none | unknown (unreadable) | owner/creating account inspects metadata and evidence purpose | none possible now | preserve; never traverse `current` links |
| `.pytest_cache/`, `.ruff_cache/`, `.mypy_cache/`, `__pycache__/` (182 dirs), `pytest-of-Pinda/`, `.uv-cache/`, `packages/mission-control-db-contract/.ruff_cache/` | exist, ignored | regenerable cache | regenerated by tools | none | approved sweep | n/a | keep until approved sweep |
| `.scratch/organization-checkpoint/source-before-organization.zip` | exists, ignored | preserve unique evidence | none | `sha256:84f23f73db7446bd3bbeb9d9d2a74c0e16f1739e31d900805e8d577bde649d4c` | never | n/a | preserve |
| `.scratch/mission-control-parity-checkpoint/` (incl. `tracked.patch`, `documentation-before-organization/`) | exists, ignored | preserve unique evidence | none | `tracked.patch` `sha256:42550895df3e3141f50560d67195d2f39b82029c89351897384e77d223de64db` | never | n/a | preserve |
| Other `.scratch/` trees (247 symlinks incl. pytest `current` links) | exist, ignored | preserve unique evidence | none | G0 scratch reparse-point inventory | owner review | n/a | preserve; never traverse links |
| 85 pending owner deletions under `docs/interview_and_research_result_documentation/` (20) and `docs/migrations_instructions/` (65) | absent on disk; tracked at HEAD; deletion uncommitted | already-removed (user-made) | `docs/knowledge/`, AGENTS.md, this guide | blob ids in [removal-inventory.json](organization/removal-inventory.json) | owner commits the deletion | `python docs/tools/check_links.py` | preserve the deletion; never restore; live references repointed (below) |
| Live `belllabs_control`, `capability_search` (Biotech project: 4 tables, `documents` about 542 rows per the G0 [identity evidence](qualification/two-project/biotech-research-ingestion/g0-readonly-20261003/identity.json)), `belllabs_langgraph` | live data (local and/or Biotech project) | separate persistent-data authorization | `mission_control`, `mission_control_search`, `mission_control_runtime` for new admitted runs only | the live data itself | explicit owner data decision, never part of source cleanup | protected row digests unchanged (`protected-diff.json` per target) | preserve; never dropped or migrated by source cleanup |
| Compose volumes (project `biotech-research-ingestion-evaluation-system`) | exist (local Docker) | separate persistent-data authorization | none | volume contents | explicit volume-migration decision | n/a | preserve; no rename or prune |
| Mongo records/buckets | outside this repository | separate persistent-data authorization | PostgreSQL adapters for new runs | the data | explicit owner decision | n/a | preserve; no purge |

### Retired Mongo persistence paths (validated)

<details>
<summary>All 54 entries of mongo-removals.json with their current replacement</summary>

| Retired path | Now | Replacement (G0-inferred, exists now) |
| --- | --- | --- |
| `src/mission_control/application/programs/mongo_goal_directed_repository.py` | absent, untracked | `src/mission_control/adapters/postgres/orchestration/goal_directed_repository.py` |
| `src/mission_control/application/programs/mongo_stagegraph_repository.py` | absent, untracked | `src/mission_control/adapters/postgres/orchestration/stagegraph_repository.py` |
| `src/mission_control/application/execution/operations/mongo_operation_execution_repository.py` | absent, untracked | `src/mission_control/adapters/postgres/operations/operation_journal.py`; `src/mission_control/adapters/postgres/operations/operation_binding_repository.py` |
| `src/mission_control/application/execution/operations/mongo_operation_authority_migration.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `src/mission_control/application/execution/operations/mongo_operation_journal_backfill.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `src/mission_control/application/execution/operations/operation_journal_backfill.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `src/mission_control/application/execution/operations/operation_journal_read_routing.py` | absent, untracked | `src/mission_control/adapters/postgres/operations/operation_journal.py` |
| `src/mission_control/adapters/postgres/operations/operation_journal_backfill.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `src/mission_control/application/subordinates/mongo_async_subagent_repository.py` | absent, untracked | `src/mission_control/adapters/postgres/async_subagents/async_subagent_detail_repository.py`; `src/mission_control/adapters/postgres/async_subagents/async_subagents.py` |
| `src/mission_control/application/artifacts/mongo_workspace_repository.py` | absent, untracked | `src/mission_control/adapters/postgres/workspaces/workspace_manifest_repository.py` |
| `src/mission_control/application/artifacts/mongo_snapshot_repository.py` | absent, untracked | `src/mission_control/adapters/postgres/workspaces/snapshot_repository.py` |
| `src/mission_control/application/artifacts/mongo_artifact_repository.py` | absent, untracked | `src/mission_control/adapters/postgres/workspaces/artifact_repository.py`; `src/mission_control/adapters/postgres/workspaces/artifact_metadata_repository.py` |
| `src/mission_control/adapters/infrastructure/mongodb.py` | absent, untracked | `src/mission_control/adapters/postgres/connections.py` |
| `src/mission_control/adapters/infrastructure/workspace_candidate_contents.py` | absent, untracked | `src/mission_control/adapters/postgres/workspace_candidate_contents.py` |
| `src/mission_control/adapters/document_models/artifact_promotion.py` | absent, untracked | `src/mission_control/adapters/postgres/workspaces/artifact_repository.py` |
| `src/mission_control/adapters/document_models/control_plane.py` | absent, untracked | `src/mission_control/adapters/postgres/control_plane/definition_repository.py` |
| `src/mission_control/adapters/document_models/goal_directed.py` | absent, untracked | `src/mission_control/adapters/postgres/orchestration/goal_directed_repository.py` |
| `src/mission_control/adapters/document_models/infrastructure.py` | absent, untracked | `src/mission_control/adapters/postgres/documents.py` |
| `src/mission_control/adapters/document_models/operation_execution.py` | absent, untracked | `src/mission_control/adapters/postgres/operations/operation_journal.py` |
| `src/mission_control/adapters/document_models/sandbox_snapshot.py` | absent, untracked | `src/mission_control/adapters/postgres/workspaces/snapshot_repository.py` |
| `src/mission_control/adapters/document_models/stagegraph.py` | absent, untracked | `src/mission_control/adapters/postgres/orchestration/stagegraph_repository.py` |
| `src/mission_control/adapters/document_models/workspace_materialization.py` | absent, untracked | `src/mission_control/adapters/postgres/workspaces/workspace_manifest_repository.py` |
| `tests/integration/temporal/test_rrm_004_worker_restart_recovery.py` | absent, untracked | `tests/acceptance/mission_control/test_postgres_runtime_parity.py` |
| `tests/integration/agent_server/test_rrm_013_async_subagent_live.py` | absent, untracked | `tests/integration/postgres/test_async_subagent_postgres_authority.py` |
| `tests/integration/postgres/test_operation_journal_backfill_integration.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `tests/unit/operations/test_operation_journal_backfill.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `tests/unit/orchestration/test_rrm_018_goal_document_identity.py` | absent, untracked | `tests/integration/temporal/test_rrm_016_goal_directed_journaled.py` |
| `tests/fixtures/mongo_database.py` | absent, untracked | `tests/fixtures/mission_control_production_stack.py` |
| `tests/fixtures/operation_backfill_worker.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `tests/fixtures/rrm004_persistent_stack.py` | absent, untracked | `tests/fixtures/mission_control_production_stack.py` |
| `tests/fixtures/rrm004_restart_worker.py` | absent, untracked | `tests/fixtures/mission_control_production_stack.py` |
| `tests/fixtures/rrm006_fork_stack.py` | absent, untracked | `tests/fixtures/run_forks.py` |
| `tests/fixtures/rrm013_child_worker.py` | absent, untracked | `tests/fixtures/rrm013_live_stack.py` |
| `tests/fixtures/rrm010_combined_smoke.py` | absent, untracked | `tests/fixtures/mission_control_production_stack.py` |
| `tests/integration/mongodb/test_artifact_promotion_mongodb_integration.py` | absent, untracked | `tests/integration/postgres/test_artifact_promotion_postgres_integration.py` |
| `tests/integration/mongodb/test_control_plane_mongodb_integration.py` | absent, untracked | `tests/integration/postgres/test_definition_catalog.py` |
| `tests/integration/mongodb/test_goal_directed_documents_mongodb.py` | absent, untracked | `tests/integration/postgres/test_immutable_runtime_documents.py` |
| `tests/integration/mongodb/test_operation_execution_mongodb_integration.py` | absent, untracked | `tests/integration/postgres/test_operation_journal_stage1.py` |
| `tests/integration/mongodb/test_sandbox_snapshots_mongodb_integration.py` | absent, untracked | `tests/unit/workspaces/test_sandbox_snapshots.py` |
| `tests/integration/mongodb/test_workspace_materialization_mongodb_integration.py` | absent, untracked | `tests/integration/postgres/test_workspace_artifact_documents.py` |
| `tests/integration/mongodb/__init__.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `tests/acceptance/control_plane/test_rrm_005_inspection.py` | absent, untracked | `tests/integration/postgres/test_runtime_inspection_postgres.py` |
| `tests/acceptance/control_plane/test_rrm_006_semantic_forks.py` | absent, untracked | `tests/integration/postgres/test_run_forks_postgres.py` |
| `tests/acceptance/control_plane/test_rrm_008_cancellation_demo.py` | absent, untracked | `tests/integration/temporal/test_rrm_008_family_cancellation.py` |
| `tests/acceptance/control_plane/test_rrm_009_object_store.py` | absent, untracked | `tests/acceptance/mission_control/test_postgres_children_and_artifacts.py` |
| `tests/acceptance/control_plane/test_rrm_009_live_capabilities.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `tests/acceptance/control_plane/test_rrm_009_production_composition.py` | absent, untracked | `tests/acceptance/mission_control/test_postgres_runtime_parity.py` |
| `tests/acceptance/control_plane/test_rrm_009_production_cancellation.py` | absent, untracked | `tests/unit/operations/test_rrm_009_cancellation_composition.py` |
| `tests/acceptance/control_plane/test_rrm_010_combined_smoke.py` | absent, untracked | `tests/acceptance/mission_control/test_postgres_runtime_parity.py` |
| `tests/acceptance/control_plane/test_rrm_016_goal_directed_demo.py` | absent, untracked | `tests/integration/temporal/test_rrm_016_goal_directed_journaled.py` |
| `tests/acceptance/control_plane/test_rrm_020_shared_goal_workspace.py` | absent, untracked | `tests/acceptance/mission_control/test_postgres_children_and_artifacts.py` |
| `tests/fixtures/rrm009_live_capabilities.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `scripts/reorganize_application_packages.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |
| `scripts/reorganize_tests.py` | absent, untracked | none (retired backfill / one-shot / Mongo-only / live-provider proof) |

</details>

Content of every retired path is recoverable from `f6521c1` history and the source
archive above.

### Pending owner documentation deletions

The owner deleted 85 tracked historical documents (uncommitted as of 2026-10-03,
HEAD `f6521c1`). They are user-made, preserved as deletions and must not be
restored; each remains recoverable with `git show f6521c1:<path>` (blob ids in
[removal-inventory.json](organization/removal-inventory.json)). Live references were
repointed or marked historical with that recovery command in `docs/README.md`,
`.cursor/rules/_briefings/01-api-runbook-briefing.md`,
`.cursor/rules/_briefings/02-codebase-organization-briefing.md`,
`src/mission_control/interfaces/mcp/COORDINATOR_ARCHITECTURE_AND_PLAN.md`,
`src/mission_control/interfaces/mcp/implementation_plans/COORDINATOR_SPEC_INDEX.md` and
`src/mission_control/interfaces/mcp/implementation_plans/COORDINATOR_PHASES_2_3_WORKFLOW_CATALOG.md`.
Validate with `python docs/tools/check_links.py`.

## Unsupported behavior and release gates

This source cleanup does not implement a generic mission retry command, expanded
instruction injection or all new workflow types. Existing mapped controls retain
their explicit acceptance/delivery/apply semantics; unsupported requests fail rather
than receiving invented success.

The common component is implemented, qualified on two local disposable databases and
installed in both Supabase projects (owner-approved 2026-10-03; additive only — the
legacy `capability_search` data is untouched and remains under separate persistent-data
authority). Still separately approved: runtime login roles, storage, Agent Server
topology and license, and a verified recovery point. Live Supabase/storage policies, executable read-only mounts, production
hosted Agent Server durability and paid-provider behavior require their own evidence.
See [implementation status](MISSION_CONTROL_IMPLEMENTATION_STATUS.md) and
[operator setup](MISSION_CONTROL_LOCAL_API.md).

## Documentation verification

The standalone `docs/knowledge/` bundle follows the neighboring Biotech catalog's
minimal Open Knowledge Format v0.1 profile. The existing dependency-free validator
passed all eight concepts; a separate check resolved every concept/source/test
Markdown link and checked root/source AGENTS.md files remain below 8 KiB.
These structural checks do not prove runtime behavior. Post-organization
verification passed: fresh locked regression 1,343 passed / 23 skipped / two expected
failures / zero failures, plus the separately passed eleven unchanged end-to-end
cases. Final lint, core/optional typing and both isolated wheel checks passed.
See the implementation status for exact exclusions, the recovered first-run
failures and non-additive focused selections.
