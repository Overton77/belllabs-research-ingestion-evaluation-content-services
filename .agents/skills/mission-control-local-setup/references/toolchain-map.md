# Toolchain map

Where each knob lives and how to verify a change.

## Files

| Concern | File / section |
| --- | --- |
| Dependencies and groups | `pyproject.toml` `[project.dependencies]`, `[dependency-groups]`, `[tool.uv]` (`default-groups = ["dev"]`), `[tool.uv.sources]` |
| Lock | `uv.lock` (revision 5); `make lock`, `make lock-check`, `make lock-upgrade` |
| Interpreter | `.python-version` (3.12); `.venv/Scripts/python.exe` |
| Lint + format | `pyproject.toml` `[tool.ruff]`, `[tool.ruff.lint]` (select/ignore with reasons), `per-file-ignores`, `[tool.ruff.format]` |
| Type gate | `[tool.mypy]` (+ overrides for `temporal.client`, `deepagents.middleware.async_subagents`) |
| Fast types | `[tool.ty.environment]`, `[tool.ty.src]`, `[tool.ty.terminal]`, `[tool.ty.rules]` |
| Dependency drift | `[tool.deptry]`, `package_module_name_map`, `per_rule_ignores` |
| Tests | `[tool.pytest.ini_options]` (markers, timeout, strict flags), `[tool.coverage.*]` |
| Hooks | `.pre-commit-config.yaml` (run by prek); hook at `.git/hooks/pre-commit` |
| Make | `Makefile`; help from `##` comments via `scripts/dev/make_help.py` |
| Compose | `docker-compose.yml`, `docker-compose.temporal.yml`, `infra/application-postgres/init/`, `infra/temporal/{scripts,dynamicconfig}` |
| Env names | `.env.example` (authority for names), `agent_server/runtime.env` (Agent Server), `scripts/dev/env_check.py` |
| Cloud (Cursor) | `.cursor/Dockerfile`, `.cursor/environment.json`, `.cursor/scripts/cloud-{install,start}.sh` |
| Editor (local) | `.vscode/settings.json`, `.vscode/extensions.json` (gitignored) |
| Human guide | `docs/DEVELOPMENT.md`; README "Local operation" / "Checks"; `AGENTS.md` "Verify" |

## Make target catalogue (abridged)

```text
Environment     install install-all lock lock-check lock-upgrade outdated tree hooks doctor env-check
Infrastructure  infra-up infra-down infra-restart infra-ps infra-logs infra-pull infra-config wait
                db-up redis-up temporal-up temporal-only-up temporal-only-down temporal-ui temporal
                psql psql-temporal redis-cli
Processes       preflight server worker agent-server mcp-server health openapi socketio-smoke missionctl
mission-db      mission-db db-inspect db-plan db-verify db-runtime-plan db-seed-plan db-snapshot db-apply
Quality         fmt fmt-check lint lint-fix typecheck typecheck-fast typecheck-watch typecheck-daemon
                deps-check audit links precommit arch check ci
Tests           test test-unit test-unit-fast test-arch test-integration test-acceptance
                test-qualification test-db-contract test-failed coverage
Housekeeping    clean dev up down stop status
```

Variables: `HOST PORT RELOAD AGENT_PORT MCP_PORT MCP_PATH APP DEPLOYMENT_DIR OUT_DIR
SERVICE WAIT_TIMEOUT PYTEST_ARGS ARGS STRICT PLAN_DIGEST CONFIRM_TARGET`.

## Port map

| Endpoint | Address |
| --- | --- |
| Mission Control API | http://127.0.0.1:8000 (`/docs`, `/health/live`, `/health/ready`) |
| Agent Server (`langgraph dev`) | http://127.0.0.1:2024 |
| Coordinator MCP (Streamable HTTP) | http://127.0.0.1:8010/mcp |
| Application PostgreSQL | 127.0.0.1:55432 (`belllabs` / `belllabs-local`, db `belllabs`) |
| Redis | 127.0.0.1:16379 |
| Temporal gRPC | 127.0.0.1:7233 |
| Temporal UI | http://127.0.0.1:8080 |

Ports avoid Windows Hyper-V/WSL excluded ranges (hence 55432 and 16379).

## Verify matrix

| Changed | Run |
| --- | --- |
| ruff config | `make lint fmt-check` (then `make fmt` if intended) |
| mypy config or annotations | `make typecheck` |
| ty config | `make typecheck-fast` |
| dependencies | `make lock-check deps-check test-arch test-unit` |
| pytest config | `make test-arch test-unit` |
| hooks | `make precommit` |
| compose | `make infra-config`; `make infra-up` only when the owner wants containers started |
| Makefile | `make help`, `make -n <target>` dry runs from PowerShell and Git Bash |
| docs | `make links` |

## Audit snapshot

```text
python .agents/skills/mission-control-local-setup/scripts/audit_local_setup.py            # write + print
python .agents/skills/mission-control-local-setup/scripts/audit_local_setup.py --no-write # print only
```

Stdlib only; probes tool versions, lock revision, groups, compose status (if Docker is up),
cache sizes, stale dirs, hook presence, git worktree summary and `.env` name coverage.
Never reads secret values.
