---
name: mission-control-local-setup
description: Maintain and improve the LOCAL developer setup of Mission Control: the uv Python environment, lint/type/test toolchain (ruff, mypy, ty, deptry, prek), the Makefile, Docker Compose infrastructure (PostgreSQL+pgvector, Redis, Temporal) and editor integration. Use when asked to audit, fix, upgrade, speed up or document the local environment, dependency groups, hooks, caches, compose services or make targets. This is an evolving workspace skill with a gitignored ledger for progress and snapshots.
---

# Mission Control local setup

Keep the developer loop fast, reproducible and honest. The skill owns *how the repo is run
locally*; it never owns application behaviour, live databases, or deployment.

```text
snapshot -> read ledger -> pick one improvement -> change -> verify with make -> record
```

## Scope

| In scope | Out of scope |
| --- | --- |
| `pyproject.toml` tooling sections, `uv.lock`, dependency groups | application code semantics, domain rules |
| `Makefile`, `scripts/dev/`, `.pre-commit-config.yaml`, `.python-version` | live Supabase/Temporal Cloud, `mission-db apply` |
| `docker-compose*.yml`, `infra/`, local ports, healthchecks | deleting volumes, migrating data |
| `.vscode/` (local), Cursor cloud scripts under `.cursor/scripts/` | committing, pushing, paid experiments |
| caches, stale local dirs, hook installation | anything under `app/personal_code` or ignored user files |

## Procedure

1. **Snapshot first.** Run `python .agents/skills/mission-control-local-setup/scripts/audit_local_setup.py`.
   It writes `ledger/snapshots/<timestamp>.json` and prints a Markdown summary. Compare with
   the previous snapshot before proposing anything.
2. **Read the ledger.** `ledger/progress.md` (what was done, by date) and the open items in
   [recommendations.md](references/recommendations.md). Do not redo closed items.
3. **Check the worktree state.** `git status --short | cut -c1-2 | sort | uniq -c`. This repo
   carries large uncommitted owner work; never stash, reset or `checkout --` files you did not
   change in the current session. Verify a diff is yours before reverting it.
4. **Pick one improvement** with a stated reason (speed, correctness, reproducibility,
   discoverability). Prefer the smallest change that `make check` can verify.
5. **Change, then verify** with the relevant targets: `make lint fmt-check typecheck-fast
   deps-check` for config changes, `make typecheck` when mypy settings move, `make test-arch
   test-unit` when dependencies or fixtures move, `make infra-config` for compose edits,
   `make precommit` for hook edits, `make lock-check` after any `pyproject.toml` edit.
6. **Record** a dated entry in `ledger/progress.md`: what changed, verification results
   (passed / failed / blocked / unrun, separately), residue and next step. Update
   [current-state.md](references/current-state.md) when the described state changes and
   move items between open and closed in [recommendations.md](references/recommendations.md).

## Invariants

- `uv run --no-sync` inside the Makefile; plain `uv run` syncs to the default `dev` group and
  strips `biotech`/`dbcontract`/`notebooks`. `make install` is the dev baseline.
- mypy is the authoritative type gate; ty is fast feedback. Loosen a ty rule only with a
  comment naming the false-positive class; tighten when a ty release fixes it.
- Every ruff ignore carries a reason in `pyproject.toml`. Prefer a fix to a `# noqa`; a
  `# noqa` carries a reason.
- `.agents/skills/mission-control-coordinator/` is digested into the catalog seed bundles.
  Ruff excludes `.agents` with `force-exclude`; never format or lint-fix that directory.
- No target or script deletes Docker volumes, `.venv`, `.env`, or files under `.scratch/`
  that the owner placed there. Cleaning is limited to tool caches.
- Secrets never enter snapshots, ledgers or output. `scripts/dev/env_check.py` and the audit
  script read variable *names* only.
- Local disposable proof is not live proof. Report blocked checks (missing DSNs, no Docker)
  as blocked, never as passed or skipped.

## Load on demand

- [current-state.md](references/current-state.md): the setup as it stands, with the
  2026-10-07 refresh, verification evidence and known residue.
- [recommendations.md](references/recommendations.md): open improvements, each with a path,
  cost and trade-off; closed items stay for provenance.
- [toolchain-map.md](references/toolchain-map.md): where every knob lives, the make target
  catalogue, the port map and the audit/verify commands.
- `ledger/README.md`: ledger format. `ledger/progress.md` and `ledger/snapshots/` are
  gitignored and machine-local by design.
