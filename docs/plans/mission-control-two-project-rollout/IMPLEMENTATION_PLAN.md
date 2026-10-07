# Two-project Mission Control migration, seeding and parity plan

Status: implementation handoff, not an applied migration or new qualification result.
Prepared 2026-10-03 against commit `f6521c128738b4962f22fb7646882950c0b63d7c`.
Launch instructions: [Claude team prompt](CLAUDE_TEAM_PROMPT.md).
Static evidence: [source inventory](source-inventory.json).
Document/source checks: [validation report](validation.json).

## 1. Decision and authority

Author one independent, reusable `mission-control-db-contract` component in the
Mission Control repository, under **`packages/mission-control-db-contract/`**.
Install exactly the same checksummed common release into **`mission_control`** in
both existing Supabase projects. App identity, bindings and seed admissions differ;
the common table definitions do not. Keep application entity tables with their
existing owners. Do not introduce a third mission database.

This location is a concrete recommendation for the next implementation session,
not an assertion that the package already exists. Its independent release is
justified by two app installers consuming the same schema without installing the
whole agent runtime. The Python runtime remains one distribution.

The owner's latest clarification overrides the old placement of common SQL in
`ai-engineer-db-contract`. That repository contains AI Engineer domain/entity
contracts, migrations and generated application types. It is **not** the required
owner or prerequisite for general Mission Control. No work on that repository is
needed to author the common component. Do not copy its domain tables, migration
head, generated types, helpers, grants or installer chain into Mission Control.

Record this correction in the normative documentation before implementation:
[DATABASE](../../../../mission-control-general/general-mission-control/DATABASE.md)
installation authority and seeds sections,
[CODEBASE-ORGANIZATION](../../../../mission-control-general/general-mission-control/CODEBASE-ORGANIZATION.md),
[SPECIFICATION](../../../../mission-control-general/general-mission-control/SPECIFICATION.md),
and expansion deployment/issue ownership for MC-P002/MC-F002 and any matching
machine-readable issue entries. Make one bounded ownership amendment, not a broad
rewrite of the 44-issue program. Retain all other accepted behavioral contracts.
This planning turn changes none of those source specs.

Scope: modular common migrations, app-local runtime persistence, seed admission,
both existing StageGraph and GoalDirected paths, all currently implemented
lifecycle controls, two-project installation evidence, removal completion and
documentation cleanup. Full expanded Goal Loop semantics, new workflow families,
web/mobile clients, new providers and broad Knowledge Services generalization are
not smuggled into this parity milestone. Record their unmet requirements separately;
do not label parity as completion of the entire expanded specification.

## 2. Observed baseline and limits

`git status --short` showed no tracked modifications at inspection. It did show
untracked `.tmp-cleanup-unit/` and `.tmp-worker-poll-20261003/`, containing test
directories and `current` links. Preserve them until their evidence is classified.
Git also warned about previously missing `.claude/skills` links; do not repair or
restore these as incidental work. Recheck HEAD/status when the team starts.
During final handoff validation, 85 historical documentation deletions appeared
concurrently while HEAD remained unchanged. They were not made or restored by this
planning task. Their exact observed paths are in the validation report. All 71
inventoried runtime/migration source hashes still matched. The next team must
preserve/reconcile that concurrent work before trusting old documentation paths.

Current code facts (paths relative to Mission Control unless prefixed):

| Source | Actual constraint / implication |
| --- | --- |
| `bootstrap/api.py`, `bootstrap/worker.py`, `bootstrap/composition.py` under `src/mission_control` | Reject `production_common`; composition/readiness literals and API response wiring still select `transitional_local`. Merely removing the rejection is unsafe. |
| `application/installations/registry.py` | Digest-sealed `ApplicationBinding`; project ref, app, installation, issuer/audience and required component version; request scope is `mc/{installation}/{app}/{tenant}`. |
| `adapters/postgres/connections.py` | Restricted BellLabs roles plus legacy migration runner; its version-only history is not the new checksummed release protocol. |
| `adapters/postgres/migrations/0001–0040` | Current persisted behavior depends on the full chain, not only 0027–0040. Names include `belllabs_control` and `capability_search`. Do not replay this chain into live common production. |
| `bootstrap/worker.py` | Checkpoint DSN must match the selected installation endpoint; setup is forbidden at worker startup; role and pinned SDK migration versions are checked. |
| `bootstrap/settings.py`, `adapters/deep_agents/persistence.py` | Default recovery schema is `belllabs_langgraph`; pinned saver/store use schema-local tables and optional administrative setup. |
| `../biotech-postgres-db-contract/src/biotech_postgres_db_contract/installer.py` | Integrity checks are reusable, but `source_identity` is hard-coded to `ai-engineer-db-contract`. |
| Same package: `preflight.py`, `component.lock.json` | Target parser permits only `biotech-research-ingestion`; lock is deliberately unavailable. It cannot currently install Blue Ocean. |
| Same package: `deployment.py`, `fingerprint.py` | Existing transaction/advisory lock, ordered checksum-prefix, compatibility, schema/RLS/grant fingerprint and explicit endpoint checks are valuable foundations. Current fingerprint covers only `mission_control`, not domain data or runtime schemas. |
| `integrations/biotech/.../migrations/0001_scoped_records.sql` | Optional `biotech_mission_adapters.records` is a separate domain adapter component. It must not be installed in AI Engineer as part of the common release. |
| `agent_server/langgraph.json` | Sole bounded-graph configuration; local profile proof does not qualify production native persistence in Supabase. |

Read all scoped AGENTS before editing. Source roots are `src/mission_control/`
and `integrations/biotech/src/biotech_mission_adapters/`.

The recorded previous qualification is 1,343 passing tests plus 11 separate
end-to-end cases, 23 skips and two historical expected failures. It is regression
input, not evidence of this new common schema. Reproduce relevant tests on the
new component. The [implementation status](../../MISSION_CONTROL_IMPLEMENTATION_STATUS.md)
and [removal guide](../../REMOVAL_GUIDE.md) preserve exact limits.

Live discovery was **not performed** in this planning turn. No Supabase SQL
connector was available; local credential variable names were inspected without
printing values. Existing deployment templates still contain placeholders and no
verified two-project mapping. Presence of a DSN is not project identity evidence.
The AI Engineer README/package were inspected read-only (domain contract 0.4.16);
their historical deployment records are not proof of either current target.

## 3. Proposed physical ownership

| Namespace / resource | Owner and boundary |
| --- | --- |
| `mission_control` | Common business authority: installation, catalog admissions, mission/program/run, lifecycle, effects, artifacts, accounting and recovery manifests. Identical release in both projects. |
| `mission_control_search` | Common, rebuildable capability/blueprint/plugin projection. Move current `capability_search` repository queries here. Component manifest includes its DDL and fingerprint; projection contents/model generations may differ. Never search domain schema/entity records here. |
| `mission_control_runtime` | Private standalone LangGraph saver/store, separately pinned vendor migration ledger and role. Chosen replacement for `belllabs_langgraph`; not business acceptance authority. |
| `mission_control_agent_server` | Reserved private native Agent Server namespace, **only if supported by the pinned production server**. Do not assume an environment variable or `search_path` makes the server compatible. Qualify supported configuration; stop this lane if unsupported and request a topology decision. |
| `biotech_mission_adapters` | Existing optional Biotech adapter records and its independent migration/role. Biotech-only opt-in; no dependency from common SQL. |
| Existing `public`, `auth`, `storage`, domain schemas, roles, helpers and buckets | Existing owners. Inventory actual schemas; do not rely on a hard-coded list. No alterations except the exact separately reviewed MC bucket/policy additions. |
| Temporal persistence | Remains outside both application databases. No Temporal server tables in these schemas. |

Use fixed validated namespace identifiers, not a caller-supplied schema prefix.
Check namespace/role collisions before creating anything; same-name unknown objects
cause a hold, not `IF NOT EXISTS` acceptance. Common SQL cannot contain foreign
keys, triggers, views, functions or default expressions requiring a domain schema,
app `util` helper, Neo4j, Mongo, or `auth.users`. Actors link by verified external
issuer/subject; membership remains app-owned.

Every common business query uses a schema-qualified identifier. Functions use a
safe fixed `search_path` (`pg_catalog`, with explicitly qualified owned objects),
and have reviewed execution grants. Restrict `SECURITY DEFINER` to a justified
small allowlist; qualify all objects and deny `PUBLIC` execution. No global role
or database `search_path` change. For vendor SQL, configure only the dedicated
runtime connection's private schema, explicitly handle `pg_temp`, deny untrusted
schema CREATE and test shadowing. Do not expose these schemas through PostgREST
by default; FastAPI/MCP/CLI remain the governed facade.

## 4. Release and installer contract

Proposed new tree (not existing CLI promises):

```text
packages/mission-control-db-contract/
  pyproject.toml
  src/mission_control_db_contract/  # shared installer/verifier, no app branching
  component/migrations/            # new common chain, starting 0001
  component/generated/             # MC-only schema exports, generated not hand-edited
  component/manifest.json
  runtime/                         # pinned SDK migration descriptors/provisioner
  seeds/common/                    # authored immutable catalog definitions
  tests/                           # two-database contract/upgrade/crash/role proofs
deployments/biotech/                # authoritative app target/binding/seed manifests
deployments/ai-engineer/             # same structure and release pin
```

Move the reusable installer mechanics out of the Biotech-only package into the
new common package; preserve its integrity and recovery tests. The sibling
`biotech-postgres-db-contract` becomes a thin consumer/wrapper of the common
installer and Biotech manifest, with no editable common SQL copy. AI Engineer
uses the same installer and its own manifest; do not invoke its entity migration
chain. Keep one authoritative copy of each app manifest. Generated/deployed copies
must be checksum-verified artifacts. Runtime Python imports must not resolve
sibling repository directories.

Release manifest fields: component identity/source revision; immutable release
version; minimum and qualified PostgreSQL major versions; required extension
names/versions; ordered migration keys, paths and SHA-256; generated contract
digest; normalized fingerprint algorithm/version and expected outputs for both
owned common schemas; exact reader/writer compatibility sets; admitted predecessor
fingerprints; role/object ownership inventory; runtime component descriptors;
seed compatibility ranges. The app lock exhaustively hashes payload files and the
manifest. Do not create a self-referential manifest hash. Verify publisher/source
provenance independently of byte integrity; never fetch a moving branch at apply.

Use `mission_control.component_release` for immutable applied migration receipts,
and an independently versioned release attestation for manifest/contract digests.
Retain `application_installation` for project/app identity. Unknown history,
changed applied checksum, missing prefix, incompatible writer or unexpected schema
fingerprint rejects. New versions append; they never edit old migration bytes.
Do not invent a production version/checksum in a lock before the actual artifact
and disposable proofs exist.
Preserve historical SQL bytes, including obsolete source-owner comments in the
transitional chain. Correct active authority and new releases, not applied history.

Proposed administrator CLI `mission-db` must ship with tested `--help`, strict
manifest schemas, redacted structured reports and examples. Implement operations:
`inspect`, `plan`, `apply`, `runtime-plan`, `runtime-apply`, `seed-plan`, `seed-apply`,
`verify`, `snapshot`, `compare`, `qualify`. Preserve existing `biotech-db` as a thin
consumer if useful, otherwise document the explicit replacement. These commands
are requirements, not executable commands already available today.

`apply` takes a verified target manifest, release lock/root, expected plan digest
and exact target confirmation. Bind the plan to the before fingerprint and identity;
under the migration lock re-read them to prevent stale-plan application. Migration
SQL plus history and installation version advancement commit atomically in an
explicit dependency order. Use bounded advisory-lock and statement timeouts;
one deployment owner, one target at a time. Reject unexpected concurrent DDL.
Independent databases have no distributed atomic transaction.

Runtime SDK setup is a separate phase: the pinned `langgraph-checkpoint-postgres`
source includes `CREATE INDEX CONCURRENTLY`, which cannot run inside the common
DDL transaction. Use a dedicated session lock and migration connection for this
phase, record per-step progress and inspect invalid indexes after interruption.
Do not blindly retry incomplete concurrent indexes or hand-edit vendor history.
Setup must use the pinned official implementation or a reviewed faithful provisioner
with exact SQL/version checks. Keep startup read-only; no worker `setup()` fallback.
Schema creation, runtime provisioning and seed readiness are distinct states.

Do not run `supabase db reset`, unscoped migration pushes or domain-generated SQL.
For migration/session locks use a verified direct or supported session connection;
qualify pooling behavior before using it. Extension provisioning/role creation is
separate from runtime startup and must pass the explicit access-expansion gate.

## 5. Canonical model and complete write-path conversion

The physical inventory captures 31 SQL-bearing Python modules and all 40 migration
files with source hashes. It is deliberately a static lower bound: delegated
document-store calls, vendor queries, dynamic SQL and non-SQL bytes require the
following semantic matrix and real execution traces. No path is complete merely
because `belllabs_control` was text-replaced.

All canonical business names below are `mission_control.<name>`. Names already in
DATABASE.md are the contract; supporting records labeled **support** below need a
bounded reviewed database-contract addition. Never store all canonical state in a
renamed `immutable_documents` table and claim mission/activation conformance.

| Current path and physical records | Canonical destination and invariant | Required proof |
| --- | --- | --- |
| `bootstrap/{api,worker,composition,installation,preflight}.py`; `mission_installation_identity` | `component_release`, `application_installation`, `tenant`, `actor_binding`, `actor_grant`; sealed binding plus verified project/release and principal before pool selection | Tampered binding, wrong app/project/issuer/version and wrong database fail before mutation; no startup seed |
| `control_plane/definition_repository.py`; catalog heads/records/aliases, catalog events, effective config/compilation docs | `asset_version`, `asset_decision`, `execution_binding`; **support** `catalog_alias`, `catalog_event`, publication/compilation index. Installation/app catalog scope has no fake tenant UUID | Exact replay vs changed digest, revision pinning, retirement/revocation, cross-app catalog denial |
| `coordinator/{launch_ticket_repository,coordinator_audit_repository,workflow_result_repository}.py` | `request_receipt`, `mission`, `mission_draft`, `definition_snapshot`, `mission_revision`, `compiled_program`, `program_node`, goals/objectives/criteria, `mission_run`, `mission_event`; **support** launch ticket/result/audit when distinct semantics require them | Real authored/admitted mission revision linked to run; result visibility does not fabricate acceptance |
| `run_control/run_control_repository.py`; workflow runs, request decisions, family heads/journal/results, lifecycle transitions/results | `mission_run`, `activation`, `attempt`, `request_receipt`, `ledger_commit`, `mission_event`, `completion_candidate`, `completion_decision`; **support** scoped family-admission head/receipt preserving atomic admission | Version conflict/rollback/replay; activation completion separate from candidate and execution completion |
| Same repository + `boundary_activities.py`, `boundary_commands.py`; boundary commands/receipts | `command`, `delivery_report`, `human_task`, `human_resolution`, `event_receipt` as applicable to implemented waits | Accepted/delivered/applied distinct; stale target generation rejected; normal queued cancel/pause/resume/wait preserved; unsupported immediate semantics rejected |
| `operations/operation_journal.py`; journal mutations, effect claims, execution attempts, settlements | `operation_intent`, `operation_receipt`, `attempt`, `completion_decision`, `ledger_commit`; **support** exact mutation/effect/settlement revision records only where necessary | Claim before effect; ambiguous outcome reconciles; duplicate activity never repeats effect or double-settles |
| Journal and run-control budget/effect ledgers | `budget_account`, `budget_entry`; **support** retained effect-ledger aggregate/entries with typed schema | Atomic reservation and settlement, nonnegative availability, integer units, source/dimension uniqueness, real usage only |
| Journal, run-control, artifact repository outboxes/cursors | `outbox`, `mission_event`, `ledger_commit`; **support** `consumer_cursor` | State/events/outbox in one commit; leased delivery and fencing; crash after publish before acknowledgement safely replays |
| `orchestration/orchestration_binding_repository.py`; semantic input bindings | `execution_binding`, `compiled_program`, run input artifact/manifest pins | Frozen request/program/input digests; no mutable template lookup during replay |
| `orchestration/stagegraph_repository.py`, StageGraph activities/workflow; `stagegraph.template/1` and operation binding docs | `asset_version` template definitions, `program_node`, `activation`, `execution_binding`; **support** typed stage/operation detail records | Exact stage-to-node/activation mapping; joins, dependencies, waits, fork reuse and budget baseline retain behavior |
| `orchestration/goal_directed_repository.py`, GoalDirected activities/workflow; `goal.revision/1`, `goal.iteration/1`, `goal.handoff/1`, `goal.verification/1`, `goal.template/1` | `mission_revision` only for an actual mission revision; goal iteration activations, `journal_segment`, `completion_candidate/decision`, `execution_binding`, artifact-backed typed handoff; **support** goal detail envelopes for distinct local revision IDs | Do not conflate GoalDirected internal revision with Mission Revision or iteration with infrastructure retry; two iterations/four operations settle once; handoff/verification rehydrate |
| `operations/operation_binding_repository.py`; `operation.binding/1`, binding-index, claim, settlement docs | `execution_binding`, `operation_intent/receipt`, `attempt`; **support** immutable exact lookup indexes | Invariant-rich translation, stable references/digests, no orphan or competing authority |
| `orchestration/linked_run_repository.py`; composition links/dependencies/results/child terminals | `mission_relationship`, independent child `mission_run`, `execution_binding`, `completion_decision`; **support** dependency revision/result admission records | Required/degradable/nonblocking behavior, separate root budgets and authorization; no async-subordinate conflation |
| `async_subagents/{async_subagents,async_subagent_detail_repository}.py`; authority/facts/commands/messages/provider runs and contract/execution/link details | `subordinate_execution`, `harness_execution`, `native_observation`, `command`, `delivery_report`, `operation_intent/receipt`, reconciliation; **support** typed child contract/message records | Submission ambiguity, lost handles, duplicate callback, cancellation windows, output admission, terminal usage settlement; scoped native IDs |
| `operations/checkpoint_lineage.py`, `runtime/runtime_execution_repository.py`; units/generations/namespaces/transitions/observations/rejections/bindings/attempts/interventions | `activation`, `attempt`, `harness_execution`, `agent_session`, `session_turn`, `continuation_checkpoint`, `native_observation`, `reconciliation_case`; **support** immutable lineage rejection/transition records | Generation fencing and checkpoint descendant checks; provider/checkpoint observations cannot accept a mission |
| `runtime/{run_forks,stage3_kernel_repository}.py`; snapshot manifests, forks, lineage edges, reuse decisions, repairs, decision requests/responses, retention audit, leases | `recovery_request`, `fork_lineage`, `continuation_checkpoint`, `runtime_segment`, `replay_report`, `workspace_lease`, `reconciliation_case`; **support** repair/reuse/retention audit | Sealed checkpoint frontier, unaffected-work reuse only, independent fork admission/budget; replay cannot emit authoritative effects |
| `workspaces/{workspace_manifest_repository,artifact_metadata_repository,artifact_repository}.py`; workspace manifests/reservations, revisions, durable references | `workspace_lease`, `artifact`, `artifact_relation`, `evidence_assessment`; **support** immutable workspace manifest and slot reservation contracts | Content-addressed bytes registered once; scope/slot ownership; promotion commits reference and outbox together |
| `workspace_candidate_contents.py`, `workspaces/snapshot_repository.py`; descriptors and `sandbox.snapshot*` docs | `completion_candidate`, `artifact`, `continuation_checkpoint`; **support** candidate descriptor, sandbox snapshot and exclusive clone/create claim | Full bytes survive restart; duplicate promotion/clone idempotent; digest/scope/parent visibility failures deny |
| `capability_bundles.py`, `capability/external_candidates.py`, `documents.py`; bundle admissions, discovery/inspection document contracts | `asset_version`, `asset_decision`; **support** bundle admission and discovery/inspection evidence custody | Whole directory manifest, immutable version/digest, safe materialization; discovery is not installation or execution permission |
| `capability/{capability_search_repository,projection_events}.py`; search documents/generations/active generations plus processing/alerts | `mission_control_search` projections; common **support** projection job/alert records | Rebuild from admitted catalog, model/dimension pin, leased claims/reclaim/poison handling, search authorization |
| `run_control/inspection_repository.py`, realtime adapters, CLI/HTTP/MCP | Read canonical records above; event projection from `mission_event/outbox`, cursor scoped to principal | Resume/inspect/fork/status agree with committed authority after process restart; no cross-tenant leakage |
| `adapters/deep_agents/persistence.py`, checkpoint verifier and worker checks | Pinned vendor saver/store in `mission_control_runtime`; common checkpoint refs retain ownership | Restricted runtime role cannot mutate business; business role cannot overwrite native checkpoints; no public-schema fallback |
| Agent Server deployment/auth/client/async adapter | Vendor-native private state plus canonical subordinate admission/observation records | Same graph pins, app-bound server/persistence, native interruption/cancel/thread-copy/restart qualified; no second scheduler |
| Optional Biotech repositories/Neo4j interfaces | Leave domain authority outside common schemas; only exact typed external refs in common artifacts/bindings | Entity-schema fingerprints/data checks unchanged; no kernel import of optional package |

Contract integration gate: enumerate every baseline relation/document contract into
`persistence-map.json` with source method, read/write operation, new table/column,
scope, transaction boundary, immutable key, test ID and status. Require **zero
unmapped entries**, including delegated document contracts in migrations 0027,
0030, 0038 and 0039. Inspect all source SQL, not just this static regex inventory.
Explicitly retire unreachable legacy records with evidence rather than blindly
recreating unused tables. All support tables need schema/FK/RLS/generated-contract
coverage and must not become a parallel business ledger.

## 6. Identity, security and transaction requirements

Use contract UUIDv7 identities allocated once and persisted in seed/admission maps;
Python 3.12 has no assumed native UUIDv7 generator. Preserve opaque legacy logical
keys as separate fields where needed, not as fake UUIDs. Full composite FKs include
installation/application/tenant and same-revision constraints. Never join rows on
UUID alone or use cross-project UUID equality as authority. Catalog assets are
installation/app scoped; grants are tenant scoped.

Proposed NOLOGIN roles: `mission_control_owner`, `mission_control_runtime`,
`mission_control_family_writer`, `mission_control_catalog_writer`,
`mission_control_outbox_worker`, `mission_control_readonly`, plus distinct
checkpoint and native-server roles. Freeze actual role matrix during contract
review. Runtime is NOSUPERUSER/NOBYPASSRLS, not table owner, and cannot inherit or
SET ROLE into family/catalog/migration authority. Family writer must not inherit
the general runtime login. Grant separate actual login memberships only after the
access-expansion approval. Existing same-named roles must match reviewed ownership.

Force RLS on tenant and catalog records with fail-closed missing context. Replace
`belllabs.request_scope` with explicit transaction-local MC installation/app/tenant
context and corresponding catalog context. Derive context from verified bindings
and authenticated principals, never client headers. Fix installation/app identity
to the app-bound login/pool; tenant context is trusted backend input, not a database
credential granted to end users. Arbitrary SQL credentials can set custom GUCs;
GUCs alone are not authentication. Test pool reuse, exceptions, nested transactions,
connection recycling and deliberate scope spoofing. Reset after transactions.

Mission admission, revision/program/binding pins, budget reservation, request
receipt, mission event sequence and outbox must share the required atomic commit.
Operation settlement, usage, completion decision and events likewise commit
together. Retain compare-and-swap generation/version predicates, transaction
isolation requirements, unique effect keys and concurrent-race tests. Do not split
existing family-admission atomicity across independent repositories/transactions.

## 7. Minimal seeding and artifact custody

Seeds are a separately versioned component, applied **after** common verification.
Use `installation_seed_receipt` keyed by installation/app/seed key/version and
digest. Resolve stable logical keys to stored UUIDv7 IDs once under lock; a rerun
reuses that mapping. Same version/digest returns the existing receipt. Changed
bytes under an existing key/version fail. Concurrent equal application succeeds
once; differing content has one winner and one conflict. No last-write-wins upsert.

| Seed class | Minimum contents / exclusions |
| --- | --- |
| Installation metadata | Verified app/project/installation identity, component pin and immutable deployment binding; no guessed endpoint/issuer or secret values |
| Catalog vocabulary / policies | Only actual validator/compiler-supported StageGraph and GoalDirected workflow types, exact implementation bindings, required control/evaluation/workspace/persistence profiles and supported permission definitions |
| Executable catalog | Dependency-closed exact versions of approved profiles, blueprints, schemas, skills, tools/MCP/plugin references; admit only those whose pinned implementation/runtime bindings exist |
| App bindings | `biotech` and `ai-engineer` manifests, separate project-local provider/secret references, queues, graph/model/checkpointer/store pins and ceilings; equal common catalog bytes may be reused, app grants never copied |
| Existing tenant/actor mapping | Only owner-approved external tenant and issuer/subject mappings and minimal grants; no automatic Auth user creation, copied memberships, blanket administrator or revived revoked grants |
| Qualification fixtures | Dedicated clearly labeled test tenant and deterministic model/blueprint bundle, opt-in. They are not production provider support or real measured usage |
| Optional Biotech records | Schema installation may be a separately approved component; no research/entity/Neo4j records, source captures or fake evidence seeded |

Proposed stable logical seed keys: `mc.installation`, `mc.catalog.workflow-parity`,
`mc.catalog.runtime-profiles`, `mc.catalog.approved-assets`, `mc.app.bindings` and
optional `mc.qualification.parity`. These identify packages, not fabricated existing
assets. Select actual asset IDs from reviewed source definitions and manifest
dependencies. Preserve `skill.mission-control-coordinator` and the canonical
`skills/mission-control/` bundle where selected; do not invent missing providers.
Blueprint seeding creates reusable asset definitions, not missions, runs, accepted
results, completion receipts or usage rows.
Search projection seeding must not invent embeddings. Reuse only verified compatible
existing vectors or run an explicitly budgeted configured embedding adapter; local
deterministic vectors prove mechanics only. Report semantic search unavailable until
its actual generation/model/dimension/source-set checks pass.

Seed dependency order: installation -> policy/contract definitions -> referenced
assets/profiles -> blueprints/implementation bindings -> app bindings -> expressly
approved tenant/actor grants. Validate the entire bundle first; use one transaction
per dependency-closed seed bundle with receipt, including admission/alias changes.
If catalog publication normally opens its own connection, add a tested unit-of-work
boundary; do not pretend per-item commits are seed atomicity. New seed versions
append assets; alias movement uses expected-old target; revocations stay revoked.

`capability-bundles` and `mission-artifacts` are private app-local byte stores.
Keep `knowledge-artifacts` with its existing domain owner; do not create or seed it
merely for common parity. Whole-directory skills use canonical manifests, immutable
version/digest and path/symlink/reparse/size protections. Upload immutable bytes,
verify digest/ownership, then atomically register/admit refs. Reuse never overwrites
bytes. Missing bytes mean unavailable, not a fabricated admission.

Storage is not in the SQL transaction. Separately reconcile buckets and exact
`storage.objects` policies, with operator-approved allowed differences to baseline.
Do not replace global policies or expose public buckets. Preexisting bucket names
require ownership/settings verification. Scope keys by installation/app/tenant or
authorized installation catalog; bounded signed URLs are not permanent grants.
Partial uploads remain recorded orphans/quarantine; no automatic destructive sweep.
Runtime role credentials and storage service credentials have distinct capabilities.

## 8. Exit from transitional storage

Convert repositories and both production compositions, not only API admission.
Trace `mc.mission_run.v1` -> StageGraph/GoalDirected family -> `mc.operation.v1` ->
operation activities -> journal/bindings/workspaces/subordinate and usage writes.
Convert coordinator, artifact, boundary relay, inspection, reconciliation and
async callback paths too. Retain the existing sole Temporal macro scheduler.

Change API/worker readiness only after real canonical schema/version/role/binding
verification; support `production_common` with accurate observations. Update
preflight, installation tooling, catalog scope, settings and checkpoint binding
checks. Remove hard-coded transient response values and runtime version constants.
No production fallback to `belllabs_control`, old `capability_search`,
`belllabs_langgraph`, Mongo, memory repositories, a filesystem payload stand-in or
the technical facade. Explicit deterministic test providers remain test-only.

Acceptance must run against a database where legacy schemas never existed and
against one where they contain poison/sentinel data and deny all access. Assert
zero successful legacy queries and identical protected fingerprints; missing new
tables must fail readiness, not activate a fallback. Instrument SQL statement
families and object-store calls with values redacted, and verify actual inserted
canonical row relationships. Import-string scans alone are insufficient.

Do not drop existing legacy schemas/data as a prerequisite. The default is a fresh
common execution namespace and new admitted runs. Drain/quiesce any actual existing
runs before changing their deployment; never send a live old history to a different
worker implementation without explicit compatibility qualification. Historical
backfill is excluded unless separately requested and mapped.

## 9. Two-target deployment and non-interference protocol

Expected labels are **biotech-research-ingestion** -> app `biotech`, and
**supabase-blue-ocean** -> app `ai-engineer`. Labels are not authenticated endpoint
identities. Before live inspection/application, obtain an operator-verified mapping
of project ref, organization/account, environment, direct/session host+port+database,
approved login references, API issuer/audience/JWKS and existing tenant/actor refs.
Use approved management metadata plus the connection route and database observation;
freshly inserting an installation row cannot prove which project was reached.
Do not infer the map from a hostname or copy credentials between projects.

For each target capture read-only consistent before evidence: server/version and
endpoint attestation, owned/foreign schemas, tables, columns, defaults, constraints,
indexes (validity), functions, triggers, views, types, sequences, RLS, ACLs/default
ACLs, ownership, roles/memberships, extensions, component history, storage bucket
settings and relevant policies. Existing fingerprint code must be generalized to
multi-schema and protected-object inventories with versioned normalization. Keep
physical differences and intentional target identities separate from logical shape.

Domain protection means **all non-owned schemas/objects**, not a guessed list.
Record protected row counts and deterministic chunked row-content digests for the
approved domain relations using repeatable-read snapshots and stable keys. Hash
inside the database/secure process; publish hashes, not rows or credentials. Include
sequence states where writes would affect them. Sampling is not proof of unchanged
data. On a concurrently written live app, before/after equality cannot establish
non-interference unless writers are quiesced or a reviewed audit/change-attribution
method accounts for legitimate changes. Arrange a bounded approved validation
window or report this gate blocked; do not lock/disable app writers unilaterally.

Require a recovery point with restore procedure and tested disposable restoration.
Backup availability/PITR may depend on the actual project; do not assume or purchase
it. If an export is needed, approve its data scope/destination/access first. Protect
Auth/storage metadata without exporting secret values into reports.

Promotion order: two disposable databases with different protected domain fixtures
-> review accepted release/seed/runtime artifacts -> Biotech live target -> complete
verification and protected-object comparison -> Blue Ocean live target -> same
verification -> shared API routes independently to both. Owner may reverse the two
live targets after verified risk review; never deploy them concurrently. Same release
digest, logical schema fingerprints and generated contract in both; app identity,
seeds/grants, native refs and application data remain distinct.

Before each live mutation, present exact target, plan/release/seed digests, affected
schemas/roles/storage policies, allowed-difference manifest, recovery point and
validation steps for approval. The request to implement does not silently authorize
destructive changes, persistent access expansion, paid runs or unsupported data
changes. No third project creation. Unknown namespace/role ownership, target drift,
unsupported vendor topology or unexpected data changes stop that target and the
second promotion. Continue unrelated local work.

## 10. Recovery and retries

Transactional common DDL/seed failure rolls back its phase and receipt. A crashed
client reconnects read-only and checks receipts, fingerprint and lock state before
retrying. An absent response is not proof nothing committed. New immutable content
can only use a new version; quarantine a divergent seed instead of force-updating.

SDK setup and storage uploads have separate progress records; verify completed
steps and resume supported operations. Invalid runtime indexes, missing objects,
or unknown provider submissions require reconciliation; no blind deletion/relaunch.
No historical backfill by default. If later authorized, use an independent importer
with source snapshot digest, total mapping, exact scope transformation, dry-run
counts/checksums, per-batch replay receipts, quarantine and side-effect suppression.

After a successful release with data, prefer a forward corrective migration or
rollback to a runtime version explicitly supported by the component. Do not issue
DROP-based down migrations as recovery. A backup restore over a populated target
is separately approved destructive work. If Biotech succeeds and Blue Ocean fails,
leave the first verified deployment intact, disable the failed app binding and
report partial promotion; do not erase the first project's new ledger.

## 11. Agent team and dependency gates

Do not launch this team from the planning session. The future Claude Code session
should use a lead plus at most five teammates. One writer per claimed file/tree;
use isolated worktrees where practical. No commits/push/deploy unless authorized.

| Owner | Files / deliverables | Dependencies and review |
| --- | --- | --- |
| Lead / contract integrator | Normative ownership amendment, `contracts`, shared ports, schema mapping, `bootstrap`, app manifests, final evidence index | G0 baseline; owns all cross-lane contract changes and integration; approves no semantic weakening |
| Database and deployment owner | New common package, SQL/generated contracts, installer, sibling thin Biotech wrapper, roles/runtime provisioner; sole live database/storage mutator | After G1 model freeze; independent reviewer before G2 release and each G4 live plan |
| Runtime authority teammate | `adapters/postgres/{run_control,operations,orchestration,runtime,async_subagents}` and delegated documents per ownership ledger; corresponding tests | Frozen ports/table map; no bootstrap edits without lead handoff; real transactions/replay proofs |
| Catalog/artifact teammate | `adapters/postgres/{capability,control_plane,workspaces}`, bundle/candidate repositories, capability/seed payloads and targeted tests | Same contract freeze; seed transaction integration with DB owner, no independent SQL/role edits |
| Runtime persistence teammate | Deep Agents persistence/checkpoint adapter, Agent Server deployment/auth/client, scoped runtime config/tests | Version/topology contract freeze; DB owner executes provisioning; no business-table authority |
| Qualification/docs teammate | Independent matrix, negative/crash/schema-protection tests; `docs`, OKF/AGENTS/removal updates | Can inventory immediately, writes tests after contracts; does not approve own implementation as independent review |

Lead claims shared test fixtures before teammates modify them. Optional Biotech
changes stay in its package and are assigned explicitly. Cross-lane changes are
handed to the owner; do not race on `composition.py`, `connections.py`, manifest
schemas or migration numbering. If team capacity is smaller, execute lanes
sequentially without weakening the single deployment-owner rule.

Dependency gates:

1. **G0 / inventory:** verify HEAD/status, checkpoint dirty work, read authorities,
   refresh source inventory, classify all live identity and existing-state unknowns.
   Initial independent tasks: runtime map, installer gap analysis, native persistence
   support audit, cleanup/evidence inventory. No feature edits before ownership ledger.
2. **G1 / contract freeze:** accepted ownership amendment, canonical table/column and
   support-record map, role matrix, identity/seed schemas, transaction protocol,
   target manifest schema and runtime topology decision. No unresolved dual ledger.
3. **G2 / component and adapter proof:** DB owner authors complete release; teammates
   implement disjoint adapters after contract freeze. Prove fresh install, repeat,
   upgrade, failure rollback, drift denial and exact common fingerprints on two
   disposables with protected domain fixtures. Generate immutable release and pins.
4. **G3 / operational parity:** one canonical production composition, tests below on
   both disposable installs, full code checks, wheels, seed replay, protection proofs.
   No `transitional_local` or native persistence fallback. Review evidence independently.
5. **G4 / concrete live plan:** verified mapping/access, protected snapshots, recovery,
   finite budgets and exact mutations approved. DB owner alone performs phases and
   records receipts. Each target is independent; second depends on first's pass.
6. **G5 / closure:** live verification/status explicitly recorded, removal conditions
   satisfied, clean-code/documentation pass, unresolved gates retained as blockers.
   Do not delete evidence or declare production completion if any mandatory gate failed.

## 12. Pass/fail acceptance and evidence

For **each disposable and each live target**, create
`docs/qualification/two-project/<target>/<run-id>/` (reports only; sensitive backups
outside tracked docs). Required files:

- `identity.json`: verified reference/route evidence and observed installation; no DSN/secrets.
- `release.json`, `plan.json`, `seed-plan.json`: exact artifacts, source/build pins,
  dependency order, allowed mutations and reviewer/approval references.
- `schema-before.json`, `schema-after.json`, `protected-before.json`,
  `protected-after.json`, `protected-diff.json`: versioned fingerprints, counts/digests,
  snapshot method and concurrent-writer disposition.
- `migration-receipts.json`, `runtime-receipts.json`, `seed-receipts.json`,
  `storage-reconciliation.json`: actual results, replay behavior and incomplete phases.
- `persistence-trace.json`, `parity.json`, `junit.xml`, `replay.json`, `roles.json`:
  scope/redacted SQL families, actual run/activation/attempt IDs, assertions and statuses.
- `recovery.json`, `decision.json`: tested recovery, hold/enable decision and exact limits.

Cross-target `comparison.json` proves common release/checksums/generated-contract and
normalized logical schemas match. Do not require seeded row IDs or app policy contents
to match. Sign/digest the evidence index; raw rows, credentials, signed URLs and private
provider reasoning never belong in it.

Mandatory acceptance:

1. Fresh common install twice: first applies, second verified no-op; tampered/extra file,
   unknown applied migration, checksum drift and stale plan all reject. Concurrent
   installers serialize. Inject a middle-migration error and verify no partial receipts.
2. Upgrade from every declared predecessor, crash/resume for vendor/storage phases,
   incompatible runtime rejection and forward recovery. Do not advertise an untested
   downgrade. Compare protected fixtures byte-for-byte before/after.
3. Exact seed replay and two concurrent seeders; changed digest/version conflict,
   revoked grant, conflicting alias and missing asset bytes fail. Dependency closure
   complete; seed failure leaves no partially admitted bundle or misleading receipt.
4. Equal resource/tenant UUIDs in two apps cannot cross; wrong-project tokens, pool
   context leakage, missing context, raw client role, owner fallback and runtime-to-
   family privilege escalation fail. Denied requests produce no unauthorized rows.
5. StageGraph real API -> Temporal -> Deep Agents/local deterministic provider ->
   PostgreSQL: dependency/any-join, waits, pause/resume, restart relay, cancel, semantic
   fork/reuse, inspection/checkpoints and budget settlement. Assert canonical table
   lineage, event/outbox rows and history replay, not only HTTP 200.
6. GoalDirected: two iterations/four operations, immutable local revision/iteration/
   handoff/verification binding, rollover, compaction/recovery and declared termination
   controls supported today; typed results/usage exactly once. Distinguish accepted
   goal completion from model text and future expanded Goal Loop semantics.
7. Artifact promotion replay, crash before/after byte registration, candidate restart,
   sandbox snapshot clone identity and scoped read denial. No duplicate usage/promotion.
8. Async child completion and cancellation during cognition/completion wait; lost launch,
   duplicate callbacks, native restart, parent recovery and output admission. Linked
   independent runs retain different boundaries and budgets. Native checkpoint writes
   remain in the qualified private schema only.
9. Public signed API, CLI, skill and MCP adapters agree with direct database inspection;
   capability/blueprint/plugin searches respect admission and app/tenant grants. Native
   Agent Server lifecycle is subordinate; it never updates business acceptance itself.
10. No legacy schema exists in one proof DB; poison legacy data exists in the other.
    Both pass actual runtime paths with restricted credentials and no fallback queries.
    Remove/mock only external cost-bearing providers in local deterministic fixtures;
    SQL, Temporal, concurrency and actual supported native server remain real.

Reuse and port `tests/acceptance/mission_control/`,
`tests/integration/agent_server/test_canonical_server_local.py`,
`tests/integration/postgres/`, `tests/integration/temporal/` and the sibling installer's
tests. Expand native persistent Agent Server tests where required for live qualification.
Run `uv run ruff check src tests scripts experiments integrations/biotech/src`,
`uv run mypy src/mission_control`, optional-package typing,
`uv run --group biotech pytest`, component tests, lock verification and isolated wheel
imports/resource checks. Use distinct pytest temp directories/mypy caches per agent.
No default skip for a required common-schema proof. Historical optional/live exclusions
must be individually justified; no aggregate pass count hides a blocked production gate.

Local real-stack proof is necessary but not live evidence. Per-target live verification
must use the same installed release, actual restricted roles, verified identity and
approved qualification tenant. Deterministic real-stack runs with zero external provider
calls can verify live DB routing, but must be labeled synthetic qualification, not real
research or paid-provider proof. Any required metered proof gets separate finite budget.

Budgets: maximum six active agents, one deployer, two disposable DBs, one writer per
region. Per lane run focused tests once per change, one full integration at each merge
gate; at most two automatic retries for a diagnosed transient infrastructure failure.
No repeated provider exploration. Bound lock/query/process timeouts, test operation
counts, recursion/iteration/child counts, payload sizes and artifact counts in fixtures.
Before metered work require provider/model, calls/tokens/sandbox/storage ceiling,
currency ceiling, abort rule and approval. No wall-clock delivery estimates.

## 13. Removal guide, clean code and documentation closure

Start from [REMOVAL_GUIDE](../../REMOVAL_GUIDE.md) and the committed
[organization maps](../../organization/README.md). Extend its inventory with exact
path, current existence/tracked status, replacement, unique-evidence hash/location,
removal precondition, test/receipt proving the precondition and disposition.
“Safe later” is not permission to delete now.

| Class | Exact paths / condition |
| --- | --- |
| Already removed | Validate all 54 entries in `docs/organization/mongo-removals.json`, old `app/` source map and the four root LangGraph configurations; do not recreate or count retirement as passing tests |
| Keep until common parity/upgrade evidence | `src/mission_control/adapters/postgres/migrations/`, transitional composition branches and `tests/fixtures/mission_control_production_stack.py`; archive full historical chain/checksums before retiring an executable legacy runner |
| Replace then remove, separately reviewed | Duplicated generic installer mechanics in `../biotech-postgres-db-contract/src/biotech_postgres_db_contract/` after thin wrapper/new package equivalence; old blocked `component.lock.json` only when replaced by a real accepted pin |
| Conditional prototype retirement | `experiments/`, `src/mission_control/bootstrap/technical_api.py`, `src/mission_control/bootstrap/runners/scenario_b_live.py`: retain until every remaining supported consumer/proof is mapped or deliberately retired; do not bulk-delete |
| Regenerable caches, after evidence extraction | `.tmp-cleanup-unit/`, `.tmp-worker-poll-20261003/`, `.pytest_cache/`, `.ruff_cache/`, `.mypy_cache/`, `__pycache__/`; enumerate resolved paths, never traverse `current` links or user-owned junctions |
| Preserve unique evidence / user material | `.scratch/organization-checkpoint/source-before-organization.zip`, `.scratch/mission-control-parity-checkpoint/`, `app/personal_code/`, existing prior deletions, operator `.env`, SQL/Temporal histories, backups, database volumes and all new qualification artifacts |
| Separate persistent-data authorization | Existing `belllabs_control`, `capability_search`, `belllabs_langgraph`, Mongo records/buckets and Compose volumes; no drop/purge or volume rename under source cleanup |

Before deleting anything recoverable, save a manifest and content-addressed archive
outside the deletion set and validate restoration of a sample. For ignored temporary
files inspect metadata and evidence purpose; do not assume the user commit included
them. Only remove scoped source files with an accepted replacement. Never git
reset/clean, wholesale archive extraction or recursive path concatenation across shells.

Final clean-code pass: eliminate production legacy imports/queries/fallback branches,
duplicate migration authorities, stale source-identity gates, unqualified SQL, dead
launch scripts and default-provider/credential fallbacks. Preserve useful tests while
updating their actual scans (assert nonempty inputs). No unsupported feature should
become apparently supported by permissive defaults.

Sync README, operator guide, implementation status, REMOVAL_GUIDE, docs/knowledge
concepts/index/log, scoped AGENTS and active Cursor rules with the final architecture.
Retain historical proof labels. Update canonical ownership references once, including
machine-readable expansion issues, without claiming all expansion work is delivered.
Validate Markdown links, OKF metadata, source/test paths, instruction budgets and
generated contract/manifest consistency. Final report names actual applied versions,
seed receipts, target status, protected-data comparisons, checks passed/failed/skipped,
remaining blockers and tested startup commands. No deployment claim from a plan file.

## 14. Questions that truly block live execution

These do not block implementing the component and local proofs:

1. Verified project refs/endpoints and approved existing connection-secret references
   for both labels, environments and authoritative issuer/audience mapping.
2. Approved existing tenant/actor mappings and smallest role/grant/storage-policy
   additions; permission for any persistent access expansion.
3. Supported production Agent Server persistence topology for the pinned product,
   including its actual schema binding, license/access and recovery configuration.
4. Recovery point/export handling and a quiescent or attributable protected-data
   verification window; exact per-target live plan approval after local evidence.
5. Finite budget only if a provider/sandbox/embedding/live research proof is required.

Do not request secrets in chat. Use existing secure references or owner provisioning.
No permission to edit the AI Engineer entity contract is a prerequisite for common
Mission Control work under this corrected ownership decision.

## References checked for technical boundaries

The code and pinned dependencies above are the primary implementation evidence.
Current official documentation was checked for deployment constraints:

- [PostgreSQL concurrent indexes](https://www.postgresql.org/docs/current/sql-createindex.html):
  concurrent creation requires a separate transaction strategy; inspect invalid indexes.
- [Supabase PostgreSQL connections](https://supabase.com/docs/guides/database/connecting-to-postgres):
  select and qualify the appropriate connection mode rather than assuming interchangeable pooling.
- [Supabase RLS](https://supabase.com/docs/guides/database/postgres/row-level-security):
  privileges and row policies are separate controls; service-role behavior is not runtime isolation proof.
- [Supabase Storage access control](https://supabase.com/docs/guides/storage/security/access-control):
  storage policies require their own scoped verification, separate from business-table grants.
