---
type: Architecture Reference
title: PostgreSQL persistence and installation
description: How the common mission_control component (release 1.1.0, migrations 0001 to 0030) stores scoped application state, what the fast-track migrations add, how an installation upgraded from 1.0.0 is verified, and which releases are applied live versus only proven on scratch databases.
tags: [mission-control, implementation]
---

# PostgreSQL persistence and installation

Application PostgreSQL owns lifecycle, immutable definitions and bindings, command
receipts, workspace/artifact records, candidates and subordinate details. Temporal
runs outside the application databases (Temporal Cloud in production, a separate local database in the dev stack) and keeps its own history there. Runtime connections cannot substitute
a schema-owner credential or perform migrations during startup.

The common component has three namespaces. `mission_control` is the business
authority (installation, tenants, catalog admissions, missions, runs, activations,
attempts, commands, effects, budgets, artifacts, events, outbox, recovery and
bounded support records). `mission_control_search` is a rebuildable
capability/blueprint/plugin projection. `mission_control_runtime` holds only the
pinned LangGraph saver/store, provisioned in a separate runtime phase. The same
checksummed release is installed in every application project; identity, seeds and
data differ. Domain schemas, `auth`, `storage` and `public` keep their owners.

Every business statement is schema-qualified and runs inside an explicit
transaction after `adapters/postgres/scope.py` binds transaction-local
`mc.installation_id`, `mc.application_id`, `mc.tenant_id` (and `mc.actor_ref`).
Tenant tables force row-level security whose policies compare all three columns
with the `mission_control.ctx_*()` functions, so missing context denies. Catalog
tables use installation/application context without a fake tenant. The transitional
`belllabs.request_scope` setting is retired.

Composition checks the persisted installation identity, the complete attested
release and schema fingerprint, writer compatibility and restricted roles before
marking a binding ready (`bootstrap/common_installation.py`). Login roles hold one
NOLOGIN capability role each (runtime, family writer, catalog writer, outbox worker,
readonly, checkpointer); none owns objects or bypasses RLS. There is no transitional,
legacy-schema or `public` fallback.

## Release 1.1.0: the fast-track migrations

Release 1.0.0 (migrations 0001-0005, 0010-0017, 0020-0024) is applied and immutable in both
Supabase projects, so the fast-track additions ship as the additive minor
`mission_control` 1.1.0 (`component/manifest.json`, `component_version: 1.1.0`, minimum
PostgreSQL 17, installer `mission-control-sql/v2`). All six migrations are additive:

| Migration | Adds |
| --- | --- |
| `0025_capability_kinds_and_host_support` | the agent-composition asset kinds, `host_support`, `secret_refs`, `capability_plugin_member` ([capabilities](capabilities.md)) |
| `0026_search_projection_nullable_embedding` | nullable embeddings, `pg_trgm` name surfaces, filters, partial HNSW index |
| `0027_provider_frames` | `provider_frame`, `frame_retention_policy`, `transcript_document`, `context_selection`, `continuation_transfer`, frame expiry function ([events and commands](events-and-commands.md), [context and continuation](context-and-continuation.md)) |
| `0028_mission_chains` | `mission_chain`, `chain_link`, `chain_member_admission`, `authoring_provenance`, transition guards ([mission chains](mission-chains.md)) |
| `0029_command_mailbox_stop_fence_subscriptions` | `command_mailbox`, `command_mailbox_claim`, `stop_fence`, milestones and effect admissions, `mission_subscription`, `subscription_delivery` |
| `0030_lane_bindings` | `lane_profile` reference data, lane columns on `execution_binding` and `harness_execution`, `hook_task_token`, `hook_effect_intent`, workspace lease grants ([lanes and harness](lanes-and-harness.md)) |

Everything stays tenant scoped with forced row-level security except `lane_profile`, which is
installation-independent read-only reference data. `pg_trgm` must exist in schema `extensions`
before the release is applied (`release-spec.json` `required_extensions`, like `vector`), and
this must be created on both Supabase projects first (open owner decision). The family writer's
INSERT on `mission_run`, `budget_account` and `effect_ledger` (chain release admission, 0028) is
pending a security review.

Upgrade path. The release spec admits the 1.0.0 schema fingerprint
(`sha256:ef5a9e71...`) as the compatible previous state, so an installed 1.0.0 upgrades in place.
Two applied migration files keep their exact bytes (`0002_authoring` CRLF, `0004_capability_artifacts`
mixed endings; `.gitattributes` marks them `-text`) because the live receipts hash those bytes and
any normalization is `RECEIPT_DRIFT`. An upgraded installation keeps the 1.0.0 receipts of
0001-0024 beside the 1.1.0 receipts of 0025-0030; readiness now accepts receipts of the pinned or an
earlier semantic version while still requiring the attestation of the pinned version
(`bootstrap/common_installation.py`, `tests/unit/mission_control/test_common_installation_receipts.py`).
Bindings must then pin `required_component_version="1.1.0"`. Seeds: `mc.catalog.approved-assets@1.0.0`
is applied and frozen; `@1.0.1` is its successor and adds only the 0.3.0 router skill manifest (a
revision 2 of the coordinator skill definition is an owner decision); the agent-capability seeds
and the storage seed `mc.storage.capability-bundles@1.0.0` are new
(`packages/mission-control-db-contract/seeds/`).

Evidence and status. Scratch databases on the disposable PostgreSQL 17 server proved a
1.0.0-to-1.1.0 plan, apply, replay `noop` and verify, and a fresh 1.1.0 install with seeds; the
1.1.0 schema fingerprint is `sha256:7da7567a...`. This is local disposable proof. Release 1.1.0 has
not been applied to either live Supabase project.

`mission-db` (`packages/mission-control-db-contract`) is the only installer: release
build and lock, a plan bound to the before-fingerprint, atomic apply with receipts and
attestation, runtime and seed phases, and hash-only protected-object snapshots. The
older `belllabs_control` chain is historical bytes, never applied to live. Historic
Mongo and legacy schema data remain untouched; no backfill or purge is implied.

# Citations

- [Scope binding](../../src/mission_control/adapters/postgres/scope.py).
- [Common installation readiness](../../src/mission_control/bootstrap/common_installation.py).
- [Composition and role verification](../../src/mission_control/bootstrap/composition.py).
- [Independent two-project qualification](../../tests/qualification/two_project/test_release_parity.py).
- [Legacy round trip with poisoned schemas](../../tests/qualification/two_project/test_legacy_poison_round_trip.py).
- Common migrations: `packages/mission-control-db-contract/component/migrations/` (1.1.0: 0025-0030);
  release spec `component/release-spec.json`, manifest `component/manifest.json`.
- [Receipt acceptance for upgraded installations](../../tests/unit/mission_control/test_common_installation_receipts.py),
  [lane bindings](../../tests/integration/postgres/test_lane_bindings.py),
  [mission chain tables](../../tests/integration/postgres/test_mission_chain_tables.py),
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md) (section 9 evidence).
- Historical chain archive: `docs/organization/legacy-belllabs-control-chain.json`.
- Operator commands: `docs/MISSION_CONTROL_LOCAL_API.md`.
