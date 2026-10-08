# mission-control-db-contract agent guide

This package owns the common Mission Control SQL (ADR-0003). The runtime never imports it;
`mission-db` is admin tooling that installs, verifies and seeds a release into one application
database. Read the root `GLOSSARY.md` and `docs/knowledge/persistence.md` first.

## Where things are

| Concern | Start |
| --- | --- |
| CLI commands (release-build, lock, inspect, plan, apply, verify, seed-plan/apply, runtime-plan/apply, snapshot, compare, qualify) | src/mission_control_db_contract/cli.py |
| Migrations (the only place SQL changes) | component/migrations/ |
| Generated contract, manifest, fingerprint | component/generated/, component/manifest.json, src/.../fingerprint.py, release.py |
| Seeds per application and qualification | seeds/common, seeds/biotech, seeds/ai-engineer, seeds/qualification |
| Runtime persistence descriptor (LangGraph saver/store schema) | runtime/descriptor.json, runtime/AGENT_SERVER_TOPOLOGY.md |
| App targets (project ref, installation id, DSN env var name, approval) | ../../deployments/<app>/target.toml and release.lock.json |
| Tests (disposable clusters, repeat apply, mismatch, isolation) | tests/ |

## Invariants

A release is immutable once applied: never edit migration bytes or an applied seed; add a new
component version and a new seed version (`mc.app.bindings@1.0.0` is frozen, 1.0.1 is current).
Every apply, runtime-apply and seed-apply writes a receipt; verify compares the persisted
fingerprint and contract digest. Protected objects outside the three schemas must not change.
Runtime persistence targets a separate app-bound database (ADR-0017); the descriptor stays here.

## Verify

`uv run pytest packages/mission-control-db-contract/tests` against a disposable PostgreSQL 17 +
pgvector with `MISSION_CONTROL_TEST_ADMIN_DSN` set; `mission-db qualify` for a two-target proof.
No live apply, seed or role change without owner approval; read
`docs/qualification/two-project/LIVE_PLAN.md` before touching a Supabase project.
