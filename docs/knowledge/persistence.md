---
type: Architecture Reference
title: PostgreSQL persistence and installation
description: How the common mission_control component (release 1.2.0 in the working tree, migrations 0001 to 0033; locked release 1.1.0 = 0001 to 0030) stores scoped application state, what the fast-track and multi-provider migrations add, how an installation upgraded from 1.0.0 is verified, and which releases are applied live versus only proven on scratch databases or still unlocked.
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

## Releases 1.1.0 and 1.2.0: the fast-track and multi-provider migrations

Release 1.0.0 (migrations 0001-0005, 0010-0017, 0020-0024) is applied and immutable in both
Supabase projects, so the fast-track additions ship as the additive minor
`mission_control` 1.1.0 (`component/manifest.json`, `component_version: 1.1.0`, minimum
PostgreSQL 17, installer `mission-control-sql/v2`) with migrations 0025-0030; the deployment locks
pin that build. The multi-provider migrations 0031-0033 ship as the next minor, **1.2.0**, whose
declared predecessor is the locked 1.1.0 (2026-10-09 recovery decision: a released version never
changes its contents; 0031 and 0032 had first been appended to the working-tree 1.1.0 manifest).
Every migration is additive:

| Migration | Adds |
| --- | --- |
| `0025_capability_kinds_and_host_support` | the agent-composition asset kinds, `host_support`, `secret_refs`, `capability_plugin_member` ([capabilities](capabilities.md)) |
| `0026_search_projection_nullable_embedding` | nullable embeddings, `pg_trgm` name surfaces, filters, partial HNSW index |
| `0027_provider_frames` | `provider_frame`, `frame_retention_policy`, `transcript_document`, `context_selection`, `continuation_transfer`, frame expiry function ([events and commands](events-and-commands.md), [context and continuation](context-and-continuation.md)) |
| `0028_mission_chains` | `mission_chain`, `chain_link`, `chain_member_admission`, `authoring_provenance`, transition guards ([mission chains](mission-chains.md)) |
| `0029_command_mailbox_stop_fence_subscriptions` | `command_mailbox`, `command_mailbox_claim`, `stop_fence`, milestones and effect admissions, `mission_subscription`, `subscription_delivery` |
| `0030_lane_bindings` | `lane_profile` reference data, lane columns on `execution_binding` and `harness_execution`, `hook_task_token`, `hook_effect_intent`, workspace lease grants ([lanes and harness](lanes-and-harness.md)) |
| `0031_multi_provider_lanes` | four unqualified `lane_profile` seeds (`claude_agent_sdk`, `codex`, `claude_cloud`, `codex_cloud`, `mc.lane_describe.v2`) generated from `DECLARED_LANE_MATRICES`; `mc.execution_binding.v2` for the `claude` and `codex` lanes; widened lane CHECKs on frames, continuation, search and execution records; `capability_host_support_valid` replaced to admit all seven profiles; FORCE row-level security lifted only around the seed so a non-superuser migrator can apply it |
| `0033_approvals_coordinator_inbox_lane_describes` | MP-11 `approval_correlation` and `governed_effect_intent` (guard triggers: a closed correlation and a settled intent never change; receipts immutable; no deletes); MP-15 `coordinator_inbox`, `coordinator_notification`, `coordinator_causation` (causation FK to its inbox, immutable); a generated revision of the `lane_profile` describes of `cursor_local`, `cursor_cloud`, `claude_agent_sdk` and `codex` to the implemented `mc.lane_describe.v2` matrices, still unqualified (`scripts/lane_describe_refresh.py --check`); forced RLS, column-limited runtime UPDATE grants |
| `0032_run_cluster_binding_stream_hints` | `run_cluster_binding` (scope plus `run_key`, FK to `mission_run`, forced RLS, immutable, runtime SELECT and INSERT) for the Temporal cluster guard ([operations](operations.md)); `notify_stream_hint()` AFTER INSERT triggers on `mission_event` and `provider_frame` notifying `mc_stream_hint` with scope and id only ([mission stream](mission-stream.md)) |

Everything stays tenant scoped with forced row-level security except `lane_profile`, which is
installation-independent read-only reference data. `pg_trgm` must exist in schema `extensions`
before the release is applied (`release-spec.json` `required_extensions`, like `vector`), and
this must be created on both Supabase projects first (open owner decision). The family writer's
INSERT on `mission_run`, `budget_account` and `effect_ledger` (chain release admission, 0028) is
pending a security review.

Upgrade path. The release spec admits the 1.0.0 schema fingerprint (`sha256:ef5a9e71...`) and
the locked 1.1.0 fingerprint (`sha256:7da7567a...`) as compatible previous states, so an installed
1.0.0 or 1.1.0 upgrades in place.
Two applied migration files keep their exact bytes (`0002_authoring` CRLF, `0004_capability_artifacts`
mixed endings; `.gitattributes` marks them `-text`) because the live receipts hash those bytes and
any normalization is `RECEIPT_DRIFT`. An upgraded installation keeps the 1.0.0 receipts of
0001-0024 beside the 1.1.0 receipts of 0025-0030; readiness now accepts receipts of the pinned or an
earlier semantic version while still requiring the attestation of the pinned version
(`bootstrap/common_installation.py`, `tests/unit/mission_control/test_common_installation_receipts.py`).
Bindings pin the release they were locked against (`required_component_version="1.1.0"` today,
`"1.2.0"` once the owner re-locks). Seeds: `mc.catalog.approved-assets@1.0.0`
is applied and frozen; `@1.0.1` is its successor and adds only the 0.3.0 router skill manifest (a
revision 2 of the coordinator skill definition is an owner decision); the agent-capability seeds
and the storage seed `mc.storage.capability-bundles@1.0.0` are new
(`packages/mission-control-db-contract/seeds/`). `mc.catalog.workflow-parity@1.0.0` is frozen (it is a
dependency of the applied approved-assets 1.0.0); `@1.0.1` carries both families as the worker
registers them now (with the Human Gate workflow) as asset version 2. The agent-skill seeds publish
host support for the five non-hosted Lane Profiles only, so their applied 1.0.0 bytes stay stable.

Evidence and status. Scratch databases on the disposable PostgreSQL 17 server proved a
1.0.0-to-1.1.0 plan, apply, replay `noop` and verify, and a fresh 1.1.0 install with seeds, for the
committed 0001-0030 build (fingerprint `sha256:7da7567a...`). Release 1.2.0 (0001-0033) was built by
`mission-db release-build` on the disposable server on 2026-10-09: schema fingerprint
`sha256:0113df03...` (`mc-pg-catalog-v2`), with `generated/contract.{json,md}` regenerated. The
package suite proves the real 1.1.0-to-1.2.0 upgrade as a non-superuser migrator (pending and
applied are exactly 0031-0033; receipts 1.1.0 for 0001-0030 and 1.2.0 for the rest) and passes
65/65 on the disposable clusters. 0033 is proven by `test_release_0033.py` (RLS, grants, guards,
describe rows, non-superuser apply), 0032 by `test_release_0032_cluster_binding_and_stream_hints.py`
and the 0031 seeds and CHECKs by `test_lane_bindings.py`. `deployments/biotech/release.lock.json`
and `deployments/ai-engineer/release.lock.json` still pin the committed 1.1.0 manifest
(`0853a2c0...`, 0001-0030), so readiness reports `RELEASE_LOCK_DRIFT` until the owner inspects the
installed receipts, re-locks and applies. This is local disposable proof. The live Supabase
projects hold 1.0.0; neither 1.1.0 nor 1.2.0 has been applied to them. The API composes the
coordinator inbox only where `mission_control.coordinator_inbox` exists, so an application database
on an older release keeps every other service.

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
- Common migrations: `packages/mission-control-db-contract/component/migrations/` (1.1.0: 0025-0030;
  1.2.0: 0031-0033);
  release spec `component/release-spec.json`, manifest `component/manifest.json`.
- [Receipt acceptance for upgraded installations](../../tests/unit/mission_control/test_common_installation_receipts.py),
  [lane bindings](../../tests/integration/postgres/test_lane_bindings.py),
  [0032 cluster binding and stream hints](../../tests/integration/postgres/test_release_0032_cluster_binding_and_stream_hints.py),
  [0033 approvals, coordinator inbox and lane describes](../../tests/integration/postgres/test_release_0033.py),
  [host support 0025 with seven profiles](../../tests/integration/postgres/test_catalog_kinds_0025.py),
  [mission chain tables](../../tests/integration/postgres/test_mission_chain_tables.py),
  [owner runbook](../specs/fast-track-2026-10/OWNER-FIXTURE-RUNBOOK.md) (section 9 evidence).
- Historical chain archive: `docs/organization/legacy-belllabs-control-chain.json`.
- Operator commands: `docs/MISSION_CONTROL_LOCAL_API.md`.
