# Agent instructions

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
