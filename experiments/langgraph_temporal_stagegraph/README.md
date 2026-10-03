# LangGraph + Temporal + Deep Agents StageGraph experiment

This isolated package proves that a PostgreSQL-checkpointed LangGraph can fan out with `Send`,
acknowledge Temporal workflow starts without awaiting their results, persist an `interrupt()`, resume
from an atomic PostgreSQL outbox wake, satisfy an `any(1)` research join, and launch synthesis before a
controlled slow sibling completes. It does not import or modify the production StageGraph. The slow
activity uses a 45-second delay (rather than the handoff's approximate 20 seconds) because observed
OpenAI latency exceeded 20 seconds; this preserves deterministic ordering instead of testing latency luck.

The scripts use only bounded public biotechnology prompts. Secrets are loaded from the project `.env`
and are never printed or passed in Temporal workflow inputs.

## Topology and setup

From PowerShell:

```powershell
cd C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system
docker compose -f docker-compose.temporal.yml up -d
docker compose up -d application-postgres
uv sync
```

Start the worker in terminal 1:

```powershell
uv run python -m app.experiments.langgraph_temporal_stagegraph.temporal_worker
```

Run the bounded driver in terminal 2:

```powershell
uv run python -m app.experiments.langgraph_temporal_stagegraph.run_experiment
```

The default overall timeout is 120 seconds. Override it with
`STAGEGRAPH_EXPERIMENT_TIMEOUT`; failures point to the Temporal UI and task queue instead of hanging.

Inspect a run without model access:

```powershell
uv run python -m app.experiments.langgraph_temporal_stagegraph.inspect_run RUN_ID
```

Run focused tests:

```powershell
uv run pytest -q tests/experiments/test_langgraph_temporal_stagegraph.py
```

## Restart drill

After the driver prints/records a run ID and reaches an interrupt, stop only the driver. Leave Temporal
and the worker running, then execute:

```powershell
uv run python -m app.experiments.langgraph_temporal_stagegraph.run_experiment --resume RUN_ID
```

The deterministic workflow IDs and persisted PostgreSQL checkpoint prevent completed stages from being
relaunched. Pending outbox wakes resume the same thread.

## Evidence and cleanup

The driver writes `artifacts/latest_report.md`. Temporal executions remain visible at
http://127.0.0.1:8080. Experiment truth is isolated in schema `stagegraph_temporal_experiment`; LangGraph
uses its standard checkpoint tables in application PostgreSQL.

Bounded, non-destructive cleanup (no volumes are deleted):

```powershell
docker compose stop application-postgres
docker compose -f docker-compose.temporal.yml stop
```

To remove only experiment rows later, explicitly drop the `stagegraph_temporal_experiment` schema using
the migration credential. This README intentionally does not automate that destructive action.
