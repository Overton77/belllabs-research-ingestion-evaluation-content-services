# G1 contract freeze — common Mission Control component

Status: frozen by the lead on 2026-10-03 for the two-project rollout. Changes after
this point go through the lead. Inputs: [implementation plan](IMPLEMENTATION_PLAN.md),
the amended general [DATABASE specification](../../../../mission-control-general/general-mission-control/DATABASE.md)
(ownership amendment D09) and the G0 inventories referenced from
[persistence-map.json](persistence-map.json).

## 1. Ownership (amendment applied)

| Artifact | Owner / path |
| --- | --- |
| Common SQL, generated MC-only contract, release manifest, shared installer `mission-db` | `packages/mission-control-db-contract/` (distribution `mission-control-db-contract`, import `mission_control_db_contract`) |
| App target, binding and seed manifests | `deployments/biotech/`, `deployments/ai-engineer/` (one authoritative copy each) |
| Biotech thin consumer | `../biotech-postgres-db-contract` (`biotech-db` delegates to `mission_control_db_contract`; no SQL) |
| AI Engineer entity tables | `ai-engineer-db-contract` — untouched; not a prerequisite |
| Transitional `belllabs_control` chain | `src/mission_control/adapters/postgres/migrations/` — historical bytes preserved, test-only after cutover, never applied to live |

The amended normative statements are in DATABASE.md (installation authority and seeds),
CODEBASE-ORGANIZATION.md, SPECIFICATION.md, IMPLEMENTATION.md, the general proposal,
EVIDENCE.md D09 and the regenerated MC-P002/MC-F002 issue packet entries.

## 2. Namespaces

| Schema | Content | Installed by |
| --- | --- | --- |
| `mission_control` | Canonical business authority plus explicitly labeled support records | `mission-db apply` (transactional) |
| `mission_control_search` | Rebuildable capability/blueprint/plugin projection | same release, same transaction |
| `mission_control_runtime` | Pinned LangGraph saver/store tables only | `mission-db runtime-apply` (separate phase; concurrent indexes) |
| `mission_control_agent_server` | Reserved; created only if the pinned Agent Server is proven to support it | not created in this release |
| `biotech_mission_adapters` | Optional Biotech records | separate Biotech-only component |

Fixed identifiers only; no caller-supplied prefixes. A pre-existing schema of the same
name without matching receipts holds the install.

## 3. Scope and RLS

- Request scope string stays `mc/{installation_uuid}/{application_id}/{tenant_uuid}`
  (`contracts.identities.parse_request_scope`). Repositories keep their `request_scope: str`
  signatures and convert with `adapters/postgres/scope.py::apply_scope`, which sets
  transaction-local `mc.installation_id`, `mc.application_id`, `mc.tenant_id` (and
  `mc.actor_ref` via `apply_actor`). It refuses to run outside an explicit transaction.
- Every tenant-owned table has `installation_id uuid`, `application_id text`, `tenant_id uuid`
  NOT NULL, a composite FK to `mission_control.tenant (installation_id, application_id,
  tenant_id)`, ENABLE + FORCE RLS and the standard policy comparing all three columns with
  `mission_control.ctx_*()` in USING and WITH CHECK. Missing context denies.
- Installation catalog tables carry `installation_id, application_id` (no fake tenant) with a
  composite FK to `application_installation` and an installation/app policy.
- `belllabs.request_scope` is retired. No production code path may set or read it.

## 4. Keys

- Canonical records: `uuid` primary key allocated in Python by
  `contracts.identities.uuid7()` and persisted once; scoped unique
  `(installation_id, application_id, tenant_id, <id>)`; tenant-local FKs use the whole key.
- Existing opaque logical identifiers (e.g. deterministic `run_id` strings, operation keys,
  document identities) are stored in separate `*_key text` columns with a scoped unique
  constraint. They are never coerced into fake UUIDs.
- Support records may reference a canonical parent by its scoped logical key
  `(installation_id, application_id, tenant_id, <parent>_key)` — still a full-scope FK.
  Joins on UUID or logical key alone are forbidden.
- Digests: `sha256:<64 hex>` checks. Accounting: `bigint` integer units. Timestamps:
  `timestamptz` supplied by the writer or `clock_timestamp()` in writer SQL (no column
  defaults that call app helpers).

## 5. Roles (NOLOGIN capability roles, created/validated by release 0001)

| Role | Purpose | Never |
| --- | --- | --- |
| migration principal (Supabase `postgres`) | owns all component objects; runs `mission-db` phases | used by API/worker |
| `mission_control_runtime` | API/worker business reads/writes per table grants (port of `belllabs_control_runtime` and `belllabs_agent_runtime`) | owner, BYPASSRLS, member of family/catalog roles |
| `mission_control_family_writer` | atomic family admission tables (port of `belllabs_family_repository_writer`) | member of runtime login |
| `mission_control_catalog_writer` | asset admission/seed/catalog projection writes | tenant business writes |
| `mission_control_outbox_worker` | outbox/projection claim, lease and acknowledgement | mission state mutation |
| `mission_control_readonly` | inspection (port of `belllabs_operations_readonly`) | any write |
| `mission_control_checkpointer` | `mission_control_runtime` schema only (runtime phase) | any `mission_control` access |

`belllabs_operation_backfill` is retired (historical backfill is out of scope). Login roles
are granted membership only in disposable tests or after the access-expansion approval.
Startup pool checks assert NOSUPERUSER, NOBYPASSRLS, the expected single capability
membership and no ownership of component objects.

## 6. Transactions

Preserved exactly from the transitional implementation: atomic family admission;
state + events + outbox in one commit; claim-before-effect; one terminal settlement;
compare-and-swap version/generation predicates; `FOR UPDATE SKIP LOCKED` leased claims.
Lock order: request receipt -> mission -> run -> activation -> budget accounts (sorted)
-> effect. Admission additionally writes, in the same transaction, the canonical
`request_receipt`, `mission`, `definition_snapshot`, `mission_revision`, `compiled_program`,
`mission_run`, `budget_account`, `ledger_commit`, `mission_event` rows and `outbox`.

## 7. Seeds

`installation_seed_receipt` + `seed_identity` (release 0001). One transaction per
dependency-closed bundle; same digest replays; changed digest under an existing
key/version conflicts; concurrent equal seeders serialize on an advisory lock keyed by
installation; revocations stay revoked (trigger on `actor_grant`). Seed keys:
`mc.installation`, `mc.catalog.workflow-parity`, `mc.catalog.runtime-profiles`,
`mc.catalog.approved-assets`, `mc.app.bindings`, optional `mc.qualification.parity`.

## 8. Release identity

Component `mission_control` version `1.0.0`, protocol `mission-control-sql/v2`,
fingerprint `mc-pg-catalog-v2` over both owned schemas, qualified PostgreSQL major 17
(both live targets report 17.6). Required extensions: none in `mission_control`;
`vector` (schema `extensions`) for `mission_control_search`.

## 9a. Legacy-to-common table map (frozen destinations)

"Support" records are bounded contract additions in `mission_control`: composite scope,
FK to `tenant`, FK to the canonical parent by scoped key, forced RLS, immutable trigger
where append-only, typed `*_contract` columns for JSONB. They never become a parallel
business ledger: lifecycle, acceptance, events and outbox stay canonical.

| Legacy (`belllabs_control.*` unless noted) | Destination |
| --- | --- |
| `mission_installation_identity`, `schema_migrations` | `application_installation`, `component_release`, `release_attestation` (retired) |
| `run_request_decisions` | `request_receipt` (action `mc.run.admit`, actor_ref = idempotency issuer, request_key = request id) |
| `workflow_runs` | `mission_run` (`run_key` = legacy run id, typed `projection`), created atomically with `mission`, `definition_snapshot` (the admitted request definition), `mission_revision` #1 and `compiled_program` from real admitted values |
| `lifecycle_command_results` | `request_receipt` (action `mc.run.lifecycle_command`) |
| `lifecycle_transitions` | `mission_event` (+ `ledger_commit`) and support `run_lifecycle_transition` for version lookups |
| `budget_accounts`, `budget_ledger` | `budget_account`, `budget_entry` (one row per dimension; integer units) |
| `effect_ledgers`, `effect_ledger_entries` | support `effect_ledger`, `effect_ledger_entry` |
| `outbox`, `consumer_cursors`, `artifact_reference_outbox` | `outbox` (ordered `global_position`, `aggregate_*`), `consumer_cursor` |
| `boundary_commands`, `boundary_command_receipts` | `command`, `delivery_report` |
| `family_admission_heads/journal/results` | support `family_admission_head/journal/result` (family writer) |
| `coordinator_launch_tickets/audit_events/workflow_results` | support `coordinator_launch_ticket/audit_event/workflow_result` |
| `workflow_semantic_input_bindings`, `runtime_execution_bindings` | `execution_binding` (immutable admitted manifest) + support for mutable submission status |
| `run_composition_links` | `mission_relationship` (kind `composition`); dependency revisions, result decisions and child terminals are support |
| `immutable_documents` (goal/stagegraph/operation contracts) | support `runtime_document` (typed contract list; detail envelopes only) |
| `runtime_units`, `runtime_unit_generations` | `activation` (unit), `attempt` (generation, fencing token = claim fence) + support lineage records |
| `operation_effect_claims` | `operation_intent` (claim before effect) |
| `operation_settlements` | support `operation_settlement` revisions + one terminal `operation_receipt` |
| `operation_execution_attempts`, `operation_journal_mutations` | support (technical attempts are observations, not `attempt_no`) |
| `async_subagent_authority` | `subordinate_execution`; facts/commands/messages/provider runs/details support or `command`/`native_observation` |
| `run_snapshot_manifests` | `continuation_checkpoint` (sealed) |
| `runtime_fork_requests`, lineage edges, reuse decisions, repairs | `recovery_request` (kind `fork`), `fork_lineage`, support audit |
| `agent_runtime_*`, `runtime_*` kernel tables | canonical `agent_session`/`session_turn`/`human_task`/`human_resolution`/`reconciliation_case`/`workspace_lease` where semantics match, else support |
| `definition_catalog_heads/records/aliases` | `asset_version` + `asset_decision` for published/retired definitions; support `catalog_head/catalog_record/catalog_alias` |
| `capability_bundle_admissions`, external discovery contracts | `asset_version`/`asset_decision`; support bundle admission and discovery custody |
| `capability_search.*`, `catalog_projection_processing/alerts` | `mission_control_search.*` projection plus support job/alert records |
| `durable_artifact_references`, `artifact_metadata_revisions` | `artifact` (+ support metadata revisions) |
| `workspace_manifests`, `workspace_slot_reservations`, candidate descriptors, sandbox snapshots | support immutable manifests/reservations; candidates also `completion_candidate` |
| `operation_journal_backfill_*` | retired (historical backfill out of scope) |

The executable record of each statement is `persistence-map.json` (method, operation,
destination table, scope, transaction, key, test, status); zero `UNMAPPED` entries at G3.

## 9. Lane ownership (one writer per path)

| Lane | Paths |
| --- | --- |
| Lead | this file, `persistence-map.json`, `contracts/identities.py`, `adapters/postgres/scope.py`, `adapters/postgres/connections.py`, `bootstrap/**`, `deployments/**`, migration `0001`-`0005` (canonical spine), shared test fixtures `tests/fixtures/mission_control_common_db.py` |
| Database/deployment | `packages/mission-control-db-contract/**` except migrations 0001-0005 and `seeds/`; `../biotech-postgres-db-contract/**`; the only live mutator |
| Runtime authority | `adapters/postgres/{run_control,operations,orchestration,runtime,async_subagents,coordinator}/**`, `documents.py`, `adapters/realtime/postgres_redis.py`, migrations `0010`-`0019`, additive changes to canonical `0002`/`0003`/`0005` (recorded in its report), matching tests |
| Catalog/artifacts | `adapters/postgres/{capability,control_plane,workspaces}/**`, `capability_bundles.py`, `workspace_candidate_contents.py`, migrations `0020`-`0029`, additive changes to canonical `0004`, `seeds/` bundle content, matching tests |
| Runtime persistence | `adapters/deep_agents/persistence.py`, checkpoint verifier, `adapters/agent_server/**`, `agent_server/**`, `runtime/` descriptors in the package |
| Qualification/docs | `tests/qualification/two_project/**`, `docs/**` (except this plan folder), OKF/AGENTS/README |
