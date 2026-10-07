# Mission Control

General mission execution with governed admission, StageGraph and GoalDirected
programs, durable lifecycle controls, and bounded Deep Agents cognition. Temporal
owns execution; PostgreSQL owns application state.

Start with the [operator guide](docs/MISSION_CONTROL_LOCAL_API.md), the
[knowledge bundle](docs/knowledge/index.md), and the
[implementation evidence](docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md).
The [removal guide](docs/REMOVAL_GUIDE.md) records the clean break, recovery
checkpoints, changed imports and remaining qualification gates.

Two-project rollout: [plan](docs/plans/mission-control-two-project-rollout/IMPLEMENTATION_PLAN.md),
[G1 contract freeze](docs/plans/mission-control-two-project-rollout/G1_CONTRACT_FREEZE.md) and
per-target evidence under `docs/qualification/two-project/`. The common component is
qualified on two local disposable databases and installed live (owner-approved, 2026-10-03) in both Supabase projects — `biotech-research-ingestion` and `supabase-blue-ocean` — as schemas `mission_control`, `mission_control_search` and `mission_control_runtime`; no application traffic uses it yet.

## Source and architecture

One Python distribution imports `mission_control` from `src/mission_control`.
Common database SQL, its release builder and the `mission-db` installer live in
`packages/mission-control-db-contract/` (the sole common SQL owner); per-app target
manifests live in `deployments/<app>/`.
Contracts, domain, application, adapters, interfaces and bootstrap have separate
ownership; scoped AGENTS.md files explain local logic and test navigation.

The accepted general specification is in
[`mission-control-general`](../mission-control-general/general-mission-control/SPECIFICATION.md).
Application domain logic is isolated in the optional `integrations/biotech/`
package. Broad KnowledgeServices generalization remains separate work.

## Local operation

```powershell
make install          # uv sync with the dev, dbcontract and biotech groups
make dev              # docker compose up -d, wait for health, print next steps
make server           # API on http://127.0.0.1:8000 (RELOAD=1 for autoreload)
make worker           # separate terminal; needs the selected application and binding pin
make agent-server     # Agent Server (langgraph dev) on :2024
```

The underlying commands, every infrastructure/database/test target and the tool
rationale are in the [developer tooling guide](docs/DEVELOPMENT.md); `make help` lists
all targets.

These commands require the operator-owned deployment, installation identity,
restricted database roles, accepted runtime options and credentials described in
the operator guide. Startup does not migrate or seed. `missionctl` uses the
authenticated application-scoped API. The canonical downloadable skill is
`skills/mission-control/`; Agent Server configuration is `agent_server/langgraph.json`.

Startup accepts only `storage_mode = "production_common"`: it verifies the
persisted installation identity, the complete attested common release
(`mission_control` + `mission_control_search`), writer compatibility and restricted
pool roles, and fails closed otherwise; there is no transitional fallback. Release 1.0.0 is
installed and verified in both Supabase projects (see the implementation status for
evidence and the remaining, separately approved steps). The Supabase project name remains `biotech-research-ingestion`.

## Checks

```powershell
make check            # ruff lint + format check, ty, deptry, architecture + unit tests
make ci               # adds mypy, uv lock --check and the docs link checker
uv run ruff check .
uv run mypy
uv run --group biotech pytest
# Independent two-project qualification (loopback disposable PostgreSQL 17 + pgvector;
# fails, never skips, without MISSION_CONTROL_TEST_ADMIN_DSN)
uv run --no-sync pytest tests/qualification/two_project
python docs/tools/check_links.py
```

Plain `uv sync` installs the general runtime without Neo4j/GraphQL domain dependencies.
Use `uv sync --group biotech` when developing/testing the optional Biotech integration;
the full test command above enables that group explicitly.

Integration and acceptance suites state their required PostgreSQL/Temporal
services. Deterministic local proof does not certify live provider, Supabase,
storage-policy or operating-system mount behavior. Do not enable paid experiments
without a finite budget, or remove database volumes to repair a test.

No historical Mongo backfill, endpoint aliases or old execution-history support
is required by this clean break. Existing user data and unrelated local work are
preserved; the removal guide distinguishes removed implementation from data.
