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

Standalone sandbox snapshot semantics now use
`src/mission_control/adapters/postgres/workspaces/snapshot_repository.py`. Clone
creation requires the parent snapshot to be visible in the constructor-bound scope;
immutable metadata and exclusive idempotency claims retain lineage.
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
- Historical discussion/WP/evidence trees remain provenance, with current authority
  clearly separated. Previously deleted historical documents were not recreated.

Recovery is selective: inspect the map/archive, extract the required file to a
temporary comparison directory, review it against concurrent work, and restore
only the intended content. Never use git reset/clean, whole-tree extraction over
the current workspace, or database deletion as a recovery shortcut. Do not restart
old and new workers on the same live executions without an explicit compatibility
decision.

## Unsupported behavior and release gates

This source cleanup does not implement a generic mission retry command, expanded
instruction injection, all new workflow types, or a production common-schema
adapter. Existing mapped controls retain their explicit acceptance/delivery/apply
semantics; unsupported requests fail rather than receiving invented success.

Production common-schema readiness remains blocked on its independent release.
Live Supabase/storage policies, executable read-only mounts, production hosted
Agent Server durability and paid-provider behavior require their own evidence.
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
