# Briefing: API + Runbook Cursor Rule

**Target rule file:** `.cursor/rules/api-runbook.mdc`  
**Apply mode:** File-specific (intelligent globs), `alwaysApply: false`  
**Audience:** Agents starting the stack, hitting HTTP, or editing API/compose surfaces.

## Purpose

Compact quickstart + API guide: start Docker, run server/worker, know routers, send requests, avoid gotchas. Do not invent a second topology.

## Frontmatter (required)

```yaml
---
description: Local runbook — Compose startup, FastAPI routers, worker, and request patterns
globs: docker-compose*.yml,Makefile,.env.example,README.md,app/server.py,app/preflight.py,app/api/**/*.py,app/temporal/worker.py,app/agent_server/http_app.py,app/mcp/**/*.py,.cursor/scripts/**,.cursor/environment.json,scripts/migrate_application_database.py
alwaysApply: false
---
```

## Facts to encode (authoritative)

### Startup sequence

```bash
uv sync
docker compose up -d          # or: make compose-up
uv run python -m app.preflight
uv run uvicorn app.server:asgi_app --host 127.0.0.1 --port 8000   # make server
# second terminal:
uv run python -m app.temporal.worker   # make worker
```

- `make up` = compose + preflight + server; does **not** start worker.
- ASGI target is `app.server:asgi_app` (Socket.IO wrap), not `api`.
- Cloud: `.cursor/scripts/cloud-install.sh` / `cloud-start.sh`.

### Compose services (host ports)

| Service | Port | Role |
|---------|------|------|
| application-postgres | 55432 | App authority (pgvector) |
| application-mongodb | 27017 | Immutable defs / grounding |
| redis | 16379 | Fan-out / realtime |
| temporal | 7233 | Macro runtime gRPC |
| temporal-ui | 8080 | UI |
| temporal-postgres | unpublished | Temporal-only persistence |

Do not run `docker-compose.temporal.yml` alongside main compose. Never `docker compose down --volumes` unless explicitly asked.

### Router inventory (mounted on `app.server.api`)

| Prefix | File | Purpose |
|--------|------|---------|
| `/health/live`, `/health/ready` | `app/server.py` | Liveness; cheap readiness |
| `/control-plane/v1` | `app/api/control_plane.py` | Definitions, drafts, aliases, compile ERC, retire |
| `/run-control/v1` | `app/api/run_control.py` | Admit runs, commands, budgets, effects, outbox |
| `/schema-grounding/v1` | `app/api/schema_grounding.py` | Read-only grounding records |
| `/v2/graph-runtime` | `app/api/graph_runtime_schemas.py` | Graph-runtime contract schemas |

OpenAPI: `http://127.0.0.1:8000/docs`. Temporal UI: `:8080`.

### Auth gotchas (must state)

- `get_control_plane_principal` stubs to **503** until deployment overrides it.
- Without override, mostly works: `/health/*`, many `*/schemas`, alias resolve, ERC-by-digest.
- Socket.IO / Agent Server / MCP use Supabase/JWT when enabled.
- Agent Server (`app.agent_server.http_app`) is a separate app, not `make server`.

### Worker

- Module: `python -m app.temporal.worker`
- Always: linked-run queue + schema-grounding queue.
- Coordinator launch requires `COORDINATOR_LAUNCH_ENABLED=true` + composition factory or worker refuses to start.

### Env pointers

- Copy names from `.env.example`; never commit `.env`.
- App DB host port **55432**; Redis **16379**; Temporal `localhost:7233`; queue default `biotech-research-ingestion`.
- Two Postgres authorities — never point app DSN at Temporal DB.

### Defer / do not paste

- Full WP migration prose, full SPEC bodies, Deep Agents internals (other rules).
- Keep rule actionable and under ~120 lines; prefer tables + commands over narrative.

### Existing docs to point at (not duplicate)

- `README.md` § Local development
- `.env.example`
- `Makefile`
- As-built: `docs/interview_and_research_result_documentation/CODEBASE_DOMAIN_WORKFLOW_GUIDE.md`
