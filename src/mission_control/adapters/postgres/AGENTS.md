# PostgreSQL authority

Repositories enforce scoped identity, immutable content and transaction boundaries.
Apply scope on every acquired connection, including after pool reset. Runtime and
family-writer roles are distinct; do not use schema-owner credentials as a fallback.

migrations/ contains the ordered transitional application migration history.
Do not rewrite applied SQL checksums. New schema changes need versioned migrations,
idempotency and constraint tests against isolated databases. Never apply destructive
live changes or delete records to make a test pass.

The common production component has a separate source owner and release process.
Local belllabs_control proof does not certify production_common. See
docs/knowledge/persistence.md and tests/integration/postgres.
