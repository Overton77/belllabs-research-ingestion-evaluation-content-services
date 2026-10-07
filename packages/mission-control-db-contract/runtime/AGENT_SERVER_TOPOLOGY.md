# Agent Server native persistence: topology verdict

Status: **`mission_control_agent_server` is UNSUPPORTED for the pinned Agent Server.** The
schema is not created, configured or granted by any Mission Control release. Live native
persistence qualification is **blocked** on an owner topology decision.

## Evidence (pinned `langgraph-api==0.12.0`, `langgraph.json` `api_version` `0.12.0`)

From the installed package and the G0 audit of the server image's
`langgraph_runtime_postgres` (`agent-server-0.12.0-migrations.tsv`, bytecode dump):

- Configuration exposes `DATABASE_URI` (fallback `POSTGRES_URI`), `REDIS_URI`,
  `MIGRATIONS_PATH` (default `/storage/migrations`), `DB_MIGRATION_BY_CORE_API`,
  `LANGGRAPH_POSTGRES_EXTENSIONS` (`standard` | `lite`) and license settings
  (`LANGGRAPH_CLOUD_LICENSE_KEY`, `LANGSMITH_LICENSE_REQUIRED_CLAIMS`). There is **no
  schema/namespace setting** and no documented `search_path` support.
- The server applies its own migrations at startup (under a Redis lock; a failed lock
  logs "another server is already running migrations. Continuing."). 63 migration files up
  to head `000062` (incl. two `.lite` variants), 23 containing `CREATE INDEX
  CONCURRENTLY`; its ledger is `schema_migrations(version, dirty)`.
- All migration SQL is unqualified. It creates `assistant`, `assistant_versions`,
  `thread`, `thread_ttl`, `run`, `run_event`, `cron`, `checkpoint_delete_queue` **and
  `checkpoints`, `checkpoint_blobs`, `checkpoint_writes`, `store`** - the same names as the
  standalone saver/store, with a different shape. It also runs `CREATE EXTENSION btree_gin`
  (`000017`) and `CREATE EXTENSION ltree` (`000029`) in the standard profile.
- The connection helper parses `DATABASE_URI` with `conninfo_to_dict` and appends
  `-c lock_timeout/statement_timeout/idle_in_transaction_session_timeout` to `options`.
  A caller-supplied `search_path` might therefore survive, but this is undocumented,
  unqualified behaviour: extensions would be created into whatever schema is first, the
  Go `core-api` path (`DB_MIGRATION_BY_CORE_API`) is not covered, and plan §3 forbids
  assuming that an environment variable or `search_path` makes the server compatible.

Consequences: the Agent Server cannot share `mission_control_runtime` (table collisions
with the pinned saver/store) and cannot be confined to a private schema of the business
database by any supported setting. Its startup also performs DDL and needs `CREATE` and
extension privileges that Mission Control runtime roles must never hold.

## Recommended topology (owner decision required)

1. **Preferred: a separate PostgreSQL database per installation** (same server/project or
   a dedicated one) used only by the Agent Server: its own owner and its own login role,
   `CONNECT` to that database only, no access (no `CONNECT`) to the business database,
   and no membership in any `mission_control_*` role. Redis per installation as required
   by the server. Extensions (`btree_gin`, `ltree`, or `LANGGRAPH_POSTGRES_EXTENSIONS=lite`)
   are provisioned there by the Agent Server owner, never in the business database.
2. Alternative: a dedicated, separately owned Postgres instance (managed or self-hosted)
   per installation, if the Supabase project cannot host an extra database.
3. Not acceptable: the business database `public` schema, `mission_control_runtime`, or
   an unqualified `search_path` workaround.

Also required: a **production license decision** (`LANGGRAPH_CLOUD_LICENSE_KEY` /
LangSmith-managed deployment vs. self-hosted license); the local test fixture runs with an
empty `LANGGRAPH_DEPLOYMENT_LICENSE` and is not production evidence.

## Authority boundary (unchanged by any topology)

Native Agent Server state (threads, runs, run status, native checkpoints, store) is
subordinate execution evidence. It never admits, completes or settles a mission, run,
activation or operation; Mission Control records it as a native observation and admits
typed output through its own reducers/settlement in `mission_control`. The same applies
to the standalone saver/store in `mission_control_runtime`.

## Still blocked

- Live native persistence qualification of the Agent Server (plan §12 item 8: native
  interruption/cancel/thread-copy/restart, "native checkpoint writes remain in the
  qualified private schema only") until the topology and license decisions are made and
  a disposable proof of the chosen topology exists.
- Production `DATABASE_URI`/`REDIS_URI` bindings and their secret references per
  installation; none are invented here.
