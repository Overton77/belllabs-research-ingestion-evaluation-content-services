# Dynamic research swarm experiment

This isolated experiment turns a research objective into a data-defined StageGraph after an initial
web search. A Deep Agent mission planner emits two or three bounded research units. LangGraph fans the
units out with `Send`; Temporal runs each unit durably; Tavily supplies bounded public source text; each
research agent emits atomic claims and exact evidence quotations. Deterministic gates admit only claims
that faithfully resolve to an immutable source snapshot before a final synthesis agent can see them.

The LLM creates mission-plan data, never executable Python or arbitrary LangGraph node names. Safe
runtime graph injection therefore means validating and persisting a new plan revision executed through
fixed generic node types.

## Process topology

Terminal 1:

```powershell
cd C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system
uv run python -m app.experiments.dynamic_research_swarm.temporal_worker
```

Terminal 2:

```powershell
uv run python -m app.experiments.dynamic_research_swarm.run_experiment
```

Supply another bounded public-research mission:

```powershell
uv run python -m app.experiments.dynamic_research_swarm.run_experiment `
  --objective "Your research objective"
```

Resume a persisted interrupted mission without relaunching completed units:

```powershell
uv run python -m app.experiments.dynamic_research_swarm.run_experiment --resume RUN_ID
```

Required environment variables are `OPENAI_API_KEY`, `TAVILY_API_KEY`, the application PostgreSQL
DSNs, and the existing Temporal settings. Defaults cap the mission at three units, one decomposition
level, three sources per unit, and 240 seconds. These are authorization budgets, independent of any
agent framework recursion limit.

## Verification

```powershell
uv run pytest -q tests/experiments/test_dynamic_research_swarm.py
uv run python -m app.experiments.dynamic_research_swarm.inspect_run RUN_ID
```

The generated report is `artifacts/latest_report.md`. PostgreSQL schema
`dynamic_research_swarm_experiment` contains immutable mission plans, source snapshots, claims, and
evaluation reports. Durable stage execution, result records, outbox wakes, and LangGraph checkpoints
reuse the first experiment's isolated infrastructure.

## Evaluator boundary

The proof deterministically checks source identity, source SHA-256, exact evidence-span inclusion,
numeric equality, numeric-declaration completeness, negation/modality preservation, and token-cosine
support. It verifies fidelity to captured text, not source truth or scientific quality. Production needs
pinned biomedical NLI/embedding models, entity/unit/date ontologies, PDF/table coordinates, source-
quality evaluation, and immutable external artifact storage.
