# Biotech research ingestion/evaluation — infrastructure bootstrap

This is deliberately a **pre-emptive setup**. It gives later implementation agents one stable place for async clients, Temporal, sandbox execution, FastAPI, and Socket.IO without inventing ingestion behavior or application schemas early.

Current boundaries:

- Temporal owns the only PostgreSQL tables. Its Docker Compose stack uses a dedicated local PostgreSQL volume.
- `DATABASE_DIRECT` / `DATABASE_URL` and Supabase clients are wired and health-checkable, but this project creates no Supabase/Postgres application tables.
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
