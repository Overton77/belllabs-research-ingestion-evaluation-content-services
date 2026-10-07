# PostgreSQL authority

Repositories enforce scoped identity, immutable content and transaction boundaries.
Apply scope on every acquired connection, including after pool reset. Runtime and
family-writer roles are distinct; do not use schema-owner credentials as a fallback.

Every statement is schema-qualified to mission_control or mission_control_search
and runs inside an explicit transaction after scope.apply_scope (or the catalog
variant); forced RLS denies missing context. Never reference legacy schemas or
fall back to them.

Schema changes belong to packages/mission-control-db-contract (new versioned
migration, never edited bytes). migrations/ here is the historical transitional
chain, pinned by docs/organization/legacy-belllabs-control-chain.json and never
applied to live. Never apply destructive live changes or delete records to make a
test pass. See docs/knowledge/persistence.md and tests/qualification/two_project.
