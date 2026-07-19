# Biotech research ingestion/evaluation backend

The backend now includes versioned workflow-definition compilation and a transactional
Workflow Run control plane alongside the original infrastructure clients.

Current boundaries:

- Application PostgreSQL owns Run Requests, Workflow Run lifecycle projections and transitions,
  budgets, command results, outbox events, and durable consumer cursors.
- Temporal uses a separate PostgreSQL service and owns execution mechanics only.
- MongoDB owns immutable definitions and Effective Run Configuration payloads.
- MongoDB uses Beanie with PyMongo's `AsyncMongoClient` (not Motor) and the database name `belllabsbiotech`.
- Neo4j uses `AsyncGraphDatabase`.
- AWS uses an `aioboto3` async S3 client and the existing AWS profile/credential chain.
- The Agents SDK sandbox probe is one durable Temporal workflow invocation using a Docker sandbox and the low-cost `gpt-5.4-nano` default.

## Local setup

```powershell
uv sync
docker compose up -d
uv run python -m app.preflight
uv run uvicorn app.server:asgi_app --host 127.0.0.1 --port 8000
```

FastAPI docs: <http://127.0.0.1:8000/docs>  
Temporal UI: <http://127.0.0.1:8080>

Run the worker in a second terminal:

```powershell
uv run python -m app.temporal.worker
```

Then invoke the single sandbox-backed Agents SDK probe:

```powershell
uv run python -m app.temporal.run_probe
```

The worker runs on the host and talks to Docker Desktop for isolated sandbox containers. Do not treat this local Compose topology as a production deployment.

The probe is intentionally capped at three model turns. A bootstrap smoke run on 2026-07-18 reached that cap, so the workflow now fails non-retryably instead of letting Temporal replay a bounded model failure indefinitely. Increase or redesign the probe only when implementing the real agent behavior.

## Application PostgreSQL

The local application database is exposed on port `55432` by default so it cannot be confused
with Temporal persistence or a conventional local PostgreSQL instance. FastAPI applies ordered
application migrations at startup when `APPLICATION_DATABASE_DIRECT` or
`APPLICATION_DATABASE_URL` is configured. Production should supply a restricted runtime
credential there and a separate schema-owner credential in
`APPLICATION_MIGRATION_DATABASE_DIRECT`; migrations create the no-login
`belllabs_control_runtime` grant role and force tenant RLS on authoritative tables.

Run the disposable PostgreSQL acceptance path with:

```powershell
$env:TEST_APPLICATION_POSTGRES_DSN="postgresql://belllabs:belllabs-local@127.0.0.1:55432/belllabs"
uv run pytest tests/test_run_control_postgres_integration.py -q
```

The integration test drops its application schema after verification.
