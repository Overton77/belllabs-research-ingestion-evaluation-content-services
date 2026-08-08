# Agent instructions

## Canonical architecture and reading order

- The application stays in this repository under the canonical Python package `app/`.
- [Canonical application codebase organization](docs/CANONICAL_APPLICATION_CODEBASE_ORGANIZATION.md)
  is the accepted target for paths, ownership, dependency direction, and staged refactoring.
- Read migration guidance in this order: the
  [main index](docs/migrations_instructions/implementation_work_packages/00_MAIN_GOAL_AND_INDEX.md);
  global gates; owner amendments when applicable; canonical organization; the complete active
  package and dependencies; accepted architecture/contract docs; then as-built guidance, code,
  and tests. The active package remains implementation authority if a projected path differs.
- Temporal is the sole production macro-workflow runtime. The BellLabs API is the sole governed
  public facade. Agent Server is bounded to operation, qualification, development, and shared
  support assets; it is not an application root or competing scheduler.
- Preserve `domain <- application <- api/temporal/integrations`. Providers and runtime adapters
  cannot own BellLabs semantics or durable product authority.
- Current/as-built guides and executable code explain current behavior. Historical migration
  plans, experiments, and superseded recommendations are non-normative and cannot override
  accepted documents.

## Cursor Cloud specific instructions

The Cloud Agent environment starts Docker and the default `docker-compose.yml`
services automatically. Before integration work, confirm the stack with:

```bash
docker compose ps --all
docker compose exec application-postgres pg_isready -U belllabs -d belllabs
docker compose exec redis redis-cli ping
docker compose exec temporal nc -z 127.0.0.1 7233
curl --fail --silent http://127.0.0.1:8080/ >/dev/null
```

`application-postgres`, `redis`, `temporal-postgres`, `temporal`, and
`temporal-ui` should be running. `temporal-schema` and
`temporal-create-namespace` are successful one-shot services and should show
exit code 0.

Python dependencies are installed from `uv.lock`. Run checks with:

```bash
uv run ruff check app tests
uv run mypy app
uv run pytest
```

The Compose infrastructure does not need application secrets. FastAPI, the
Temporal worker, external integration tests, and `app.preflight` may need
environment-scoped Cursor secrets. Do not create fake credentials or commit a
`.env` file. Add real values in Cursor's Cloud Agents Secrets settings using
the names in `.env.example`.

The accepted specifications repository is available at `../biotech-meta`.
Treat it as read-only context unless a task explicitly requests changes there.

Do not run `docker compose down --volumes` unless a task explicitly requires
destructive database reset. The application and Temporal PostgreSQL databases
are intentionally separate authorities.
