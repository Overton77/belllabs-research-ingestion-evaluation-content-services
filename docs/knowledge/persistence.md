---
type: Architecture Reference
title: PostgreSQL persistence and installation
description: How scoped application state and verified installation identity are enforced.
tags: [mission-control, implementation]
---

# PostgreSQL persistence and installation

Application PostgreSQL owns lifecycle, immutable definitions and bindings, command
receipts, workspace/artifact records, candidates and subordinate details. Temporal
uses a separate database for its own history. Runtime connections cannot substitute
a schema-owner credential or perform migrations during startup.

Composition checks the persisted installation identity, exact project/application,
component version, database name, migration receipts and restricted roles before
marking a binding ready. Family writers and control-runtime connections are
separate. Tenant scope is applied at every pool acquisition; RLS is part of the
proof, not a replacement for application authority checks.

The local adapter uses the transitional `belllabs_control` schema. Its readiness
always disclaims production common-schema readiness. Production startup fails
closed until the independently released common `mission_control` component and
compatible adapters are available.

The Biotech installation package consumes the common release with checksums and
ordered migrations. It must not author a second editable common SQL copy. Historic
Mongo data remains untouched; no backfill or purge is implied by removing adapters.

# Citations

- [Composition and role verification](../../src/mission_control/bootstrap/composition.py).
- [PostgreSQL scoped documents](../../src/mission_control/adapters/postgres/documents.py).
- [Real composition proof](../../tests/integration/postgres/test_mission_control_composition_postgres.py).
- [Immutable record and snapshot proof](../../tests/integration/postgres/test_immutable_runtime_documents.py).
- Migration source: `src/mission_control/adapters/postgres/migrations/`.
- Installation tooling: `../biotech-postgres-db-contract/`.
