# Current state of the local setup

Last revised: 2026-10-07 (tooling refresh). Update this file whenever the described state
changes; keep dated notes in `ledger/progress.md`.

## Machine and shells

- Windows 11, Git for Windows (`sh.exe` on PATH, so GNU Make 4.4.1 from Chocolatey runs
  recipes under `sh` from PowerShell, Git Bash and Cursor's terminal alike).
- Docker Desktop 29.x, Compose v5.3.x. No host `psql`, `redis-cli` or `temporal` CLI; the
  Makefile reaches them through `docker compose exec/run`.
- `core.autocrlf=true`: working-copy files may be CRLF; git warns "LF will be replaced by
  CRLF" on files written with LF. Harmless; do not "fix" it by rewriting files.
- Cursor keeps several `uv.exe` processes open while the editor runs. The standalone uv
  installer cannot overwrite the binary in that state; rename-then-install works.

## Python toolchain

| Piece | State |
| --- | --- |
| uv | 0.12.23 (upgraded 2026-10-07 from 0.7.5; old binary kept as `~/.local/bin/uv-0.7.5.exe`). `uv.lock` revision 5. |
| Python | 3.12.14 in `.venv`; `.python-version` pins `3.12`. A stray `.venv-wsl/` (Aug 2026) exists and is ignored. |
| Groups | `dev` (default): ruff, mypy, ty, deptry, prek, pytest + asyncio/xdist/timeout/cov, hypothesis. `dbcontract`, `biotech` (installed by `make install`). `notebooks` opt-in. |
| Direct deps | pydantic, pydantic-core, starlette, pyyaml, typing-extensions were declared in 2026-10 after deptry found them imported but only transitive. |
| ruff | 0.15.22 locked (0.16.x available). ~30 rule families; `force-exclude = true`; excludes `app/`, `experiments/`, `.agents/`, `sandbox-work/`, `graphify-out/`, `internal_hidden_docs/`, `.venv-wsl/`, `.tmp*`. Formatter is the only formatter. |
| mypy | 1.20.2 locked (2.4.x available). Pydantic plugin, `disallow_untyped_defs`, `disallow_incomplete_defs`, `no_implicit_reexport`, `strict_equality`, `warn_redundant_casts`. Clean on 346 files in ~70 s cold. Override: `deepagents.middleware.async_subagents` has `implicit_reexport = true` because tests monkeypatch `get_client` on that module. |
| ty | 0.0.85. ~1 s. `invalid-argument-type`, `invalid-assignment`, `deprecated` ignored (false-positive classes); `error-on-warning = false`; 9 residual warnings. |
| deptry | clean; module map for `socketio`, `storage3`, `langgraph.checkpoint.postgres`; DEP002 ignores for CLI/config-loaded packages. |
| pytest | `--strict-markers --strict-config`, 900 s timeout, xdist available (`make test-unit-fast`). Unit + architecture: 1127 passed, 1 failed (pre-existing seed drift), 19 skipped, 2 xfailed, ~65 s with `-n auto`. |
| hooks | prek installed at `.git/hooks/pre-commit`; 13 hooks all pass on `--all-files` (hygiene, ruff check+format, uv-lock, ty, deptry). |

## Infrastructure

`docker-compose.yml` (project name `biotech-research-ingestion-evaluation-system`, kept for
volume continuity):

| Service | Image | Host port |
| --- | --- | --- |
| application-postgres | pgvector/pgvector:pg16 | 127.0.0.1:55432 |
| redis | redis:7.4-alpine | 127.0.0.1:16379 |
| temporal-postgres | postgres:16 | internal only |
| temporal-schema / temporal-create-namespace | temporalio/admin-tools:1.31.0 | one-shot jobs |
| temporal | temporalio/server:1.31.0 | 127.0.0.1:7233 |
| temporal-ui | temporalio/ui:2.49.1 | 127.0.0.1:8080 |
| temporal-admin-tools | admin-tools (profile `tools`) | interactive |

`docker-compose.temporal.yml` is an isolated Temporal-only stack on the same host ports;
never run both. Application processes: API 8000, Agent Server 2024, Coordinator MCP 8010.

Qualification suites expect disposable PostgreSQL **17** + pgvector clusters named by DSN
environment variables (`MISSION_CONTROL_TEST_ADMIN_DSN`, `MCDB_TEST_ADMIN_DSN_A/B`); the
compose stack is PostgreSQL 16 and is not that proof.

## Makefile and helpers

~70 targets grouped as environment, infrastructure, application processes, mission-db
(read-only; `db-apply` guarded by `PLAN_DIGEST` + `CONFIRM_TARGET`), quality, tests,
housekeeping. `make help` renders from `##` comments via `scripts/dev/make_help.py`.
`scripts/dev/wait_for_services.py` ports the Cursor cloud health wait; `env_check.py`
compares `.env` names with `.env.example`; `clean_caches.py` removes caches only.

## Docs and editor

`docs/DEVELOPMENT.md` is the human guide (linked from README, AGENTS.md, docs index).
`.vscode/settings.json` + `extensions.json` are local-only (gitignored) and apply when
`mission-control` is opened as a workspace folder; the Cursor workspace root is the parent
`Biotech` folder, so they may need to be mirrored there or the folder added to the workspace.

## Known residue (as of 2026-10-07)

- `tests/unit/control_plane/test_catalog_seed_bundles.py` fails: committed
  `common/mc.catalog.approved-assets-1.0.0.json` drifted from its sources (the seeds tree is
  untracked owner WIP). Regeneration with `--write` is an owner decision (new seed version).
- db-contract package tests: 39 pass, 25 blocked without `MCDB_TEST_ADMIN_DSN_A/B`.
- The worktree carries large uncommitted owner work from 2026-10-03 (owner deletions under
  `docs/`, ~230 modified files, untracked `packages/`, `deployments/`, `tests/qualification/`).
  The 2026-10-07 tooling refresh is layered on top and also uncommitted.
- `.env` lacks 65 names present in `.env.example`; most have defaults in `Settings`.
  `make env-check STRICT=1` fails until the owner decides which are required locally.
- Stale local directories: `.venv-wsl/`, `.uv-cache/` (uv 0.7-era project cache),
  `.tmp-cleanup-unit/`, `.tmp-worker-poll-20261003/`, `graphify-out/`. All ignored; none
  removed without the owner.
