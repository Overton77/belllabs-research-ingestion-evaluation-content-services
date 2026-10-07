# `mission_control_runtime` runtime descriptor

`descriptor.json` is the ordered, hashed provisioning plan for the private LangGraph
saver/store schema `mission_control_runtime`. It is the input of the separate
`mission-db runtime-plan` / `runtime-apply` phase (plan §4): it never runs inside the
common DDL transaction, and API/worker startup never runs it (startup is read-only and
only verifies the result, see `src/mission_control/adapters/deep_agents/runtime_persistence_verifier.py`).

| Item | Value |
| --- | --- |
| Component | `mission_control_runtime` |
| Target schema | `mission_control_runtime` (owned by the migration principal) |
| Restricted role | `mission_control_checkpointer` (NOLOGIN; logins become members after access approval) |
| Pins | `langgraph==1.2.10`, `langgraph-checkpoint==4.1.1`, `langgraph-checkpoint-postgres==3.1.1`, `psycopg==3.3.4` |
| Runtime binding | exactly one libpq option `options=-c search_path=mission_control_runtime,pg_temp` |

## Regenerating / checking

The vendor steps are captured by driving the pinned `AsyncPostgresSaver.setup()` and
`AsyncPostgresStore.setup()` against a recording cursor, so their SQL is byte-identical to
what the vendor would run. Never hand-edit vendor steps.

```bash
uv run python packages/mission-control-db-contract/runtime/generate_descriptor.py --check   # default; non-zero on any drift
uv run python packages/mission-control-db-contract/runtime/generate_descriptor.py --write   # only after a reviewed pin change
```

`--check` fails when an installed pin, a vendor source file hash
(`vendor_sources_sha256`) or any step's SQL/sha256/metadata differs from the committed file.

## JSON schema (`mission-control.runtime-descriptor/1`)

```text
{
  descriptor_schema: "mission-control.runtime-descriptor/1",
  component: "mission_control_runtime",
  schema:    "mission_control_runtime",
  role:      "mission_control_checkpointer",
  pins: { "<distribution>": "<exact version>", ... },
  vendor_sources_sha256: { "<path in site-packages>": "<sha256 hex>", ... },
  session: {
    search_path:            "mission_control_runtime, pg_temp",   # SET on the migration session
    runtime_libpq_options:  "-c search_path=mission_control_runtime,pg_temp",
    advisory_lock_sql:      "SELECT pg_catalog.pg_advisory_lock(...)",   # session-level
    advisory_unlock_sql:    "SELECT pg_catalog.pg_advisory_unlock(...)",
    lock_timeout:           "5s"
  },
  steps: [ {
    id:              "prerequisite.schema_role" | "saver.v<N>" | "store.ledger" | "store.v<N>" | "post.grants",
    kind:            "sql" | "vendor",          # vendor = exact vendor SQL, unqualified names
    transactional:   bool,                      # run sql + verify + version insert in ONE transaction
    concurrent:      bool,                      # single CREATE INDEX CONCURRENTLY; autocommit
    sql:             "<exact text>",
    sha256:          "<sha256 hex of sql as UTF-8>",
    records_version: { table, v, sql } | null,  # vendor ledger row; sql is schema-qualified
    precheck:        [ "<SELECT returning one boolean>" ],
    verify:          [ "<SELECT returning one boolean>" ],
    verify_recorded: [ "<SELECT returning one boolean>" ]
  } ],
  final_verify: [ "<SELECT returning one boolean>" ]
}
```

Every check query must return exactly one row whose single column is `true`; no row,
`NULL` or `false` is a failure. All check queries are schema-qualified and read-only.

## Executor contract

1. Use one **direct session** connection (never a transaction pooler) as the migration
   principal. `SET lock_timeout`, `SET search_path TO <session.search_path>` and take the
   session advisory lock for the whole phase. Recompute every step's sha256 before use.
2. For each step in order:
   - If `records_version` is set and that version row already exists, re-run `verify` and
     `verify_recorded`; if they pass the step is already complete, otherwise **hold**.
     Steps without `records_version` are tracked by the executor's own receipts.
   - Run every `precheck`; any failure is a **hold** (for concurrent steps this is the
     "same-named index exists but is invalid/not ready" guard — never retry blindly).
   - `transactional: true`: in one transaction run `sql`, every `verify`, then
     `records_version.sql` and `verify_recorded`. Any failure rolls the whole step back.
   - `concurrent: true`: run `sql` in autocommit; run `verify` (index exists with
     `indisvalid AND indisready`). **Only if verify passes** insert the version row (and
     `verify_recorded`) in a short transaction. If verify fails, stop and report: the
     operator reviews, `DROP INDEX CONCURRENTLY mission_control_runtime.<index>` and re-runs
     that step. Never rely on `IF NOT EXISTS` (it silently accepts the invalid index) and
     never hand-insert vendor ledger rows.
3. After the last step run every `final_verify` (exact relation set, column counts, no
   invalid index anywhere in the schema), write the per-step receipt, release the lock.

The saver's `v0` *is* its ledger (`checkpoint_migrations`), as in the vendor `setup()`; the
store ledger (`store_migrations`) is created by the separate `store.ledger` step whose SQL is
the vendor's setup-local statement. Concurrent steps: `saver.v6`, `saver.v7`, `saver.v8`,
`store.v1`. `prerequisite.schema_role` creates/validates the NOLOGIN role (an existing
same-named role with elevated attributes or memberships aborts), creates the schema (an
existing schema fails the precheck and holds), revokes PUBLIC and grants the role `USAGE`
only. `post.grants` revokes any other grantee on the runtime tables and grants the role
`SELECT, INSERT, UPDATE, DELETE` on `checkpoints`, `checkpoint_blobs`, `checkpoint_writes`,
`store` and `SELECT` only on the two ledgers. The role never gets `CREATE`, ledger writes,
or any access to `mission_control` / `mission_control_search`.

A test-local reference executor implementing exactly this contract lives in
`tests/integration/deep_agents/test_runtime_persistence_postgres.py`.

## Readiness (not provisioning)

The worker (`bootstrap/worker.py::inspect_checkpoint_binding`) connects with the saver's
exact conninfo and fails closed unless: the login is NOSUPERUSER/NOBYPASSRLS and a member of
`mission_control_checkpointer` only; it has USAGE but no CREATE on the runtime schema and no
CREATE on any schema; it has no privilege on business/legacy schemas or tables; exactly the
pinned tables/columns exist; every pinned index is valid and ready; the ledgers equal
`0..9` and `0..3`; unqualified names resolve to the runtime schema; and no same-named
relation exists in `public`, the session temp schema or any schema the login can use.
There is no fallback to `belllabs_langgraph` or `public`.

The Agent Server's native persistence is **not** provisioned here; see
[AGENT_SERVER_TOPOLOGY.md](AGENT_SERVER_TOPOLOGY.md).
