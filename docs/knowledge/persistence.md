---
type: Architecture Reference
title: PostgreSQL persistence and installation
description: How the common mission_control component (release 1.1.0, migrations 0001 to 0032 in the working tree) stores scoped application state, what the fast-track and multi-provider migrations add, how an installation upgraded from 1.0.0 is verified, and which releases are applied live versus only proven on scratch databases or still unlocked.
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
PostgreSQL 17, installer `mission-control-sql/v2`). The multi-provider packet extends the same
unreleased 1.1.0 with 0031 and 0032 rather than a new version, because no 1.1.0 build has been
installed anywhere (integrator decision in the packet ledger). Every migration is additive:

| Migration | Adds |
| --- | --- |
| `0025_capability_kinds_and_host_support` | the agent-composition asset kinds, `host_support`, `secret_refs`, `capability_plugin_member` ([capabilities](capabilities.md)) |
| `0026_search_projection_nullable_embedding` | nullable embeddings, `pg_trgm` name surfaces, filters, partial HNSW index |
| `0027_provider_frames` | `provider_frame`, `frame_retention_policy`, `transcript_document`, `context_selection`, `continuation_transfer`, frame expiry function ([events and commands](events-and-commands.md), [context and continuation](context-and-continuation.md)) |
| `0028_mission_chains` | `mission_chain`, `chain_link`, `chain_member_admission`, `authoring_provenance`, transition guards ([mission chains](mission-chains.md)) |
| `0029_command_mailbox_stop_fence_subscriptions` | `command_mailbox`, `command_mailbox_claim`, `stop_fence`, milestones and effect admissions, `mission_subscription`, `subscription_delivery` |
| `0030_lane_bindings` | `lane_profile` reference data, lane columns on `execution_binding` and `harness_execution`, `hook_task_token`, `hook_effect_intent`, workspace lease grants ([lanes and harness](lanes-and-harness.md)) |
| `0031_multi_provider_lanes` | four unqualified `lane_profile` seeds (`claude_agent_sdk`, `codex`, `claude_cloud`, `codex_cloud`, `mc.lane_describe.v2`) generated from `DECLARED_LANE_MATRICES`; `mc.execution_binding.v2` for the `claude` and `codex` lanes; widened lane CHECKs on frames, continuation, search and execution records; `capability_host_support_valid` replaced to admit all seven profiles; FORCE row-level security lifted only around the seed so a non-superuser migrator can apply it |
| `0032_run_cluster_binding_stream_hints` | `run_cluster_binding` (scope plus `run_key`, FK to `mission_run`, forced RLS, immutable, runtime SELECT and INSERT) for the Temporal cluster guard ([operations](operations.md)); `notify_stream_hint()` AFTER INSERT triggers on `mission_event` and `provider_frame` notifying `mc_stream_hint` with scope and id only ([mission stream](mission-stream.md)) |

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
1.0.0-to-1.1.0 plan, apply, replay `noop` and verify, and a fresh 1.1.0 install with seeds, for the
committed 0001-0030 build (fingerprint `sha256:7da7567a...`). With 0031 and 0032 the working-tree
`component/manifest.json` was rebuilt by `mission-db release-build` on the disposable server:
schema fingerprint `sha256:672549cd...` (`mc-pg-catalog-v2`), source
`git:7c9b755...+inputs:sha256:eb7349e6...`, with `generated/contract.{json,md}` regenerated. The
0032 table and triggers are proven by `test_release_0032_cluster_binding_and_stream_hints.py`, the
0031 seeds and CHECKs by `test_lane_bindings.py`. `deployments/biotech/release.lock.json` and
`deployments/ai-engineer/release.lock.json` still pin the committed 1.1.0 manifest
(`0853a2c0...`, 0001-0030), so readiness reports `RELEASE_LOCK_DRIFT` until the owner accepts
and re-locks. This is local disposable proof. The live Supabase projects hold 1.0.0; no 1.1.0
build has been applied to either. The db-contract package suite has two failures that predate
this work (they still expect version `1.0.0`).

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
- Common migrations: `packages/mission-control-db-contract/component/migrations/` (1.1.0: 0025-0032);
  release spec `component/release-spec.json`, manifest `component/manifest.json`.
- [Receipt acceptance for upgraded installations](../../tests/unit/mission_control/test_common_installation_receipts.py),
  [lane bindings](../../tests/integration/postgres/test_lane_bindings.py),
  [0032 cluster binding and stream hints](../../tests/integration/postgres/test_release_0032_cluster_binding_and_stream_hints.py),
  [host support 0025 with seven profiles](../../tests/integration/postgres/test_catalog_kinds_0025.py),
  [mission chain tables](../../tests/integration/postgres/test_mission_chain_tables.py),
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md) (section 9 evidence).
- Historical chain archive: `docs/organization/legacy-belllabs-control-chain.json`.
- Operator commands: `docs/MISSION_CONTROL_LOCAL_API.md`.
