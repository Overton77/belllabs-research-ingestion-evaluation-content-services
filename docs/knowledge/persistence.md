---
type: Architecture Reference
title: PostgreSQL persistence and installation
description: How the common mission_control component stores scoped application state and how installation identity is verified.
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
- Common migrations: `packages/mission-control-db-contract/component/migrations/`.
- Historical chain archive: `docs/organization/legacy-belllabs-control-chain.json`.
- Operator commands: `docs/MISSION_CONTROL_LOCAL_API.md`.
