# Developer tooling

How the Python toolchain is set up, what each tool is for, and the `make` targets that
drive it. Operational semantics (installation identity, roles, `mission-db`) stay in the
[operator guide](MISSION_CONTROL_LOCAL_API.md); this page is about the developer loop.

## Quick start

```powershell
make install          # uv sync (runtime + dev + dbcontract + biotech groups)
make hooks            # install the git pre-commit hook (prek)
make infra-up         # docker compose up -d, then wait until healthy
make check            # lint + format check + ty + deptry + architecture + unit tests
```

`make help` lists every target with a one-line description. Variables are overridable
per call (`make server PORT=8100 RELOAD=1`, `make infra-logs SERVICE=temporal`).

## Toolchain

| Tool | Role | Command | Speed |
| --- | --- | --- | --- |
| [uv](https://docs.astral.sh/uv/) | Environment, lockfile, script runner | `uv sync`, `uv run`, `uv lock` | — |
| [ruff](https://docs.astral.sh/ruff/) | Linter and formatter (flake8, isort, pyupgrade, black, bandit subset) | `make lint`, `make fmt` | < 1 s |
| [ty](https://docs.astral.sh/ty/) | Fast type feedback (editor, pre-commit, `--watch`) | `make typecheck-fast` | ~3 s |
| [mypy](https://mypy.readthedocs.io/) | Authoritative type gate (pydantic plugin) | `make typecheck`, `make typecheck-daemon` | ~80 s cold |
| [deptry](https://deptry.com/) | Declared-vs-imported dependency drift | `make deps-check` | ~1 s |
| [prek](https://github.com/j178/prek) | Git hooks (Rust reimplementation of pre-commit) | `make hooks`, `make precommit` | — |
| pytest + asyncio, xdist, timeout, cov; hypothesis | Tests | `make test-unit`, `make coverage` | — |
| [uv-secure](https://pypi.org/project/uv-secure/) | Vulnerability scan of `uv.lock` | `make audit` | network |

### Why two type checkers

mypy with the pydantic plugin is the gate: it understands this codebase fully and runs in
`make ci`. ty is Astral's Rust checker; it finishes in seconds and powers the editor and
the pre-commit hook. Where ty still misreads LangGraph generics or dataclass default
factories, those rules are downgraded to warnings in `pyproject.toml` (`[tool.ty.rules]`)
so the fast loop stays green without hiding the signal. Re-tighten them as ty matures.

### Lint policy

Rule families and the reasons for each ignore are documented inline in
`[tool.ruff.lint]`. Prefer fixing over `# noqa`; when an exception is intentional, the
comment must say why (`# noqa: DTZ001 - naive input is the behaviour under test`).
`ruff format` is the only formatter; nothing else rewrites code.

### Dependency groups

| Group | Installed by | Contents |
| --- | --- | --- |
| (runtime) | `uv sync` | the application |
| `dev` | `uv sync` (default group) | ruff, mypy, ty, deptry, prek, pytest and plugins, hypothesis |
| `dbcontract` | `make install` | `mission-db` installer package |
| `biotech` | `make install` | Neo4j/GraphQL domain adapters; the unit suite imports them, plain `uv sync` stays independent |
| `notebooks` | `uv sync --group notebooks` | Jupyter, ipykernel, polars |

`uv run` syncs the environment to the default groups before running, which removes
optional groups you added by hand. The Makefile therefore uses `uv run --no-sync` and
`make test` adds `--group biotech` explicitly.

## Daily loop

1. Editor: Cursor/VS Code with the recommended extensions (`.vscode/extensions.json`,
   local only) gives ruff-on-save and ty diagnostics inline.
2. Before commit: the prek hook runs ruff (fix + format), ty, deptry, `uv lock` and the
   standard hygiene hooks. `make precommit` runs the same against every file.
3. Before pushing or handing off: `make ci` (adds mypy, lock check and the docs link
   checker). `make test` and the integration/acceptance targets need the compose stack.

## Infrastructure map

| Component | Make target | Host endpoint |
| --- | --- | --- |
| Application PostgreSQL 16 + pgvector | `make db-up`, `make psql` | `127.0.0.1:55432` (`belllabs/belllabs`) |
| Redis 7.4 | `make redis-up`, `make redis-cli` | `127.0.0.1:16379` |
| Temporal 1.31 server | `make temporal-up`, `make temporal ARGS="..."` | `127.0.0.1:7233` |
| Temporal UI | `make temporal-ui` | `http://127.0.0.1:8080` |
| Temporal-only stack | `make temporal-only-up` | same ports; never alongside the main stack |
| Mission Control API | `make server`, `make health`, `make openapi` | `http://127.0.0.1:8000` (`/docs`) |
| Temporal worker | `make worker` | — |
| Agent Server (`langgraph dev`) | `make agent-server` | `http://127.0.0.1:2024` |
| Coordinator MCP (Streamable HTTP) | `make mcp-server` | `http://127.0.0.1:8010/mcp` |

`make dev` starts the compose stack, waits for health (`scripts/dev/wait_for_services.py`)
and prints the per-terminal commands. `make up` chains infra, preflight and the API.
`make infra-down` removes containers but keeps volumes; there is deliberately no target
that deletes volumes.

### Database component targets

`make db-inspect`, `db-plan`, `db-verify`, `db-runtime-plan`, `db-seed-plan` and
`db-snapshot` wrap read-only `mission-db` commands for `APP=biotech` (default) or
`APP=ai-engineer`. `make db-apply` writes to a live target and refuses to run without
`PLAN_DIGEST=...` and `CONFIRM_TARGET=<project_ref>:<installation_id>`; it still requires
owner authorization as described in the operator guide.

## Helper scripts (`scripts/dev/`)

| Script | Purpose |
| --- | --- |
| `make_help.py` | renders `make help` from `##` comments |
| `wait_for_services.py` | waits for compose health; fails fast on a one-shot job error |
| `env_check.py` | compares variable names in `.env` with `.env.example` (never values) |
| `clean_caches.py` | removes tool caches and coverage output, never `.venv` or volumes |

All are stdlib-only so they run identically from PowerShell, Git Bash, WSL and the Cursor
cloud image.
