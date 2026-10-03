# Mission Control

General mission execution with governed admission, StageGraph and GoalDirected
programs, durable lifecycle controls, and bounded Deep Agents cognition. Temporal
owns execution; PostgreSQL owns application state.

Start with the [operator guide](docs/MISSION_CONTROL_LOCAL_API.md), the
[knowledge bundle](docs/knowledge/index.md), and the
[implementation evidence](docs/MISSION_CONTROL_IMPLEMENTATION_STATUS.md).
The [removal guide](docs/REMOVAL_GUIDE.md) records the clean break, recovery
checkpoints, changed imports and remaining qualification gates.

## Source and architecture

One Python distribution imports `mission_control` from `src/mission_control`.
Contracts, domain, application, adapters, interfaces and bootstrap have separate
ownership; scoped AGENTS.md files explain local logic and test navigation.

The accepted general specification is in
[`mission-control-general`](../mission-control-general/general-mission-control/SPECIFICATION.md).
Application domain logic is isolated in the optional `integrations/biotech/`
package. Broad KnowledgeServices generalization remains separate work.

## Local operation

```powershell
uv sync
docker compose up -d
uv run python -m mission_control.bootstrap.preflight
uv run uvicorn mission_control.bootstrap.api:create_app --factory --host 127.0.0.1 --port 8000
# Separate terminal, with the selected application and binding pin configured:
uv run python -m mission_control.bootstrap.worker
```

These commands require the operator-owned deployment, installation identity,
restricted database roles, accepted runtime options and credentials described in
the operator guide. Startup does not migrate or seed. `missionctl` uses the
authenticated application-scoped API. The canonical downloadable skill is
`skills/mission-control/`; Agent Server configuration is `agent_server/langgraph.json`.

Local execution uses explicit `transitional_local` storage and reports
`production_ready: false`. Production common-schema startup remains blocked on
the independently released common component and qualified adapters. The Supabase
project name remains `biotech-research-ingestion`.

## Checks

```powershell
uv run ruff check src tests
uv run mypy src/mission_control
uv run --group biotech pytest
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
