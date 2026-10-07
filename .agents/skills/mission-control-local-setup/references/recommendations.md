# Recommendations and paths

Each item: why, the path, the cost, and what could go wrong. Move items to **Closed** with
the date when done; keep them for provenance. Owner decisions are marked **(owner)**.

## Open: Python environment

1. **Climb the mypy strictness ladder.** Counts at 2026-10-07: `warn_return_any` 32,
   `disallow_any_generics` 12, `disallow_untyped_decorators` 4, `warn_unreachable` 5.
   Path: enable one flag per session, fix or narrowly `# type: ignore[code]` with reason,
   run `make typecheck`. End state `strict = true`. Risk: `warn_unreachable` hits may be
   real dead branches in `async_subagents.py`; read before deleting.
2. **Upgrade ruff to 0.16.x and mypy to 2.x.** Path: `uv lock --upgrade-package ruff mypy`,
   bump `rev` in `.pre-commit-config.yaml` to the same ruff version, run `make lint fmt-check
   typecheck`. Cost: possible new rule hits and formatter changes; do it in one commit so the
   reformat is attributable. mypy 2.x is a major; read its changelog first.
3. **Re-tighten ty as it matures.** Re-run with `invalid-argument-type = "warn"` each ty
   bump; when LangGraph `StateGraph` generics and `field(default_factory=frozenset)` stop
   misfiring, restore to `error`. ty is pre-1.0; expect churn.
4. **Add `pytest-randomly` behind an explicit target.** Order-dependence is a real risk in a
   1,100-test suite with module-level fixtures. Path: add to `dev`, run `make test-unit
   PYTEST_ARGS="-p randomly"` in a session dedicated to fixing leaks, then flip the default.
5. **Coverage baseline.** `make coverage` works; no threshold is enforced. Path: record the
   first number in the ledger, add `--cov-fail-under` at ~5 points below it in `make ci`.
6. **Hypothesis for reducers and canonical digests.** The domain reducers, digest set-order
   guards and canonical JSON are property-shaped. Path: one `tests/unit/property/` module per
   invariant; keep `max_examples` modest for CI.
7. **Decide the `.env` baseline (owner).** 65 example names are absent locally. Path: split
   `.env.example` into required vs optional (comment markers), teach `env_check.py` the
   marker, make `STRICT=1` meaningful.
8. **Remove stale environments (owner).** `.venv-wsl/` and `.uv-cache/` are unused by the
   current toolchain; `.tmp-*` dirs are diagnostic leftovers. Path: confirm with the owner,
   then delete; `clean_caches.py` intentionally does not touch them.
9. **`[tool.uv] required-version`.** Pin `>=0.12` once the Cursor cloud image and WSL side
   are confirmed on the new uv, so an old uv cannot rewrite the lock downward.

## Open: infrastructure

10. **PostgreSQL version parity.** Compose runs pgvector pg16; qualification requires PG17 +
    pgvector. Path: move `application-postgres` to `pgvector/pgvector:pg17` **with a volume
    migration plan** (new volume name, dump/restore), or add a second `profiles: ["pg17"]`
    service on another port for disposable proof. Never upgrade a running data directory in
    place. **(owner)**
11. **Compose profiles for partial stacks.** `make temporal-up`/`db-up` start subsets today;
    profiles (`core`, `temporal`, `tools`) would let `docker compose --profile` do it and keep
    `wait_for_services.py` lists in one place. Low risk.
12. **Resource limits and log rotation** for long-running local stacks: `deploy.resources`
    is ignored by plain compose, so use `mem_limit`/`cpus` and `logging.options.max-size`.
13. **Cursor cloud parity.** `.cursor/scripts/cloud-install.sh` runs `uv sync --frozen`
    (no `biotech`) and `cloud-start.sh` re-implements the health wait. Path: call
    `make install` and `make wait` from those scripts so one definition exists.
14. **Disposable PG17 helper.** A `make pg17-disposable` target that runs a throwaway
    `pgvector/pgvector:pg17` container on a guard port and prints the DSN env assignment
    would unblock `MISSION_CONTROL_TEST_ADMIN_DSN` and `MCDB_TEST_ADMIN_DSN_A/B` locally
    without touching the main volumes. Pair with an explicit `make pg17-disposable-down`.

## Open: process

15. **CI workflow.** No `.github/`. Path: one job with `astral-sh/setup-uv`, `uv sync
    --locked --group biotech`, then `make ci`. Needs the owner to agree on pushing to the
    GitHub remote. **(owner)**
16. **Editor settings location.** The Cursor workspace root is the parent `Biotech` folder;
    `.vscode/` under `mission-control/` only applies when that folder is a workspace folder.
    Path: add a `Biotech.code-workspace` with per-folder settings, or add the folder.
17. **Seed-bundle drift (owner).** Decide whether `approved-assets` gets a new seed version
    (`generate_catalog_seeds.py --write`) so the unit suite is green again.

## Closed

- 2026-10-07 uv 0.7.5 -> 0.12.23; lock revision 5; `.python-version`.
- 2026-10-07 ruff rule expansion with documented ignores, `force-exclude`, format applied
  repo-wide (excluding `.agents`, `app`, `experiments`).
- 2026-10-07 mypy tightened (untyped defs, implicit re-export, redundant casts, strict
  equality); 11 annotation fixes; stale `.mypy_cache` removed (it had been crashing mypy).
- 2026-10-07 ty, deptry, prek, pytest-xdist/timeout/cov, hypothesis added; five direct
  imports declared; `notebooks` group split out.
- 2026-10-07 Makefile rewritten (~70 targets), `scripts/dev/` helpers, `.pre-commit-config.yaml`
  with hook installed, `docs/DEVELOPMENT.md`, local `.vscode/`.
