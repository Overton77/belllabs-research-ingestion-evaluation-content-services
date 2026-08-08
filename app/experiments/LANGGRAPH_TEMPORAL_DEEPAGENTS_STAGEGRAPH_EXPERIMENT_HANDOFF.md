# Handoff: LangGraph + Temporal + Deep Agents durable StageGraph experiment

## Mission

Build a runnable, isolated experiment proving that a LangGraph StageGraph can:

1. determine a ready frontier;
2. use `Send` to launch multiple Temporal-backed stage executions without awaiting their results;
3. checkpoint and pause with `interrupt()`;
4. receive a durable completion notification for one fast stage;
5. resume the same LangGraph thread;
6. admit the fast result and satisfy an `any` join;
7. launch a downstream Deep Agent synthesis stage before a slow sibling finishes;
8. tolerate duplicate completion and wake delivery without duplicate stage execution or settlement.

This is an experiment, not a production migration. Keep all implementation beneath:

```text
app/experiments/langgraph_temporal_stagegraph/
```

Do not modify the production StageGraph, run-control, Agent Server, or legacy Temporal workflow except for a strictly necessary reusable bug fix. Do not commit secrets or print API keys.

## Existing environment

The project already contains compatible dependencies in `pyproject.toml`:

- `deepagents==0.7.4`
- `langgraph==1.2.10`
- `langgraph-checkpoint-postgres==3.1.1`
- `langsmith==0.10.15`
- `temporalio>=1.30,<2`
- `langchain-openai` transitively through the existing LangChain/OpenAI stack

The repository Docker Compose stack already exposes:

- Temporal: `localhost:7233`
- Temporal UI: `http://localhost:8080`
- application PostgreSQL: `localhost:55432`

Relevant environment variables already have documented defaults in `.env.example`:

```text
OPENAI_API_KEY
OPENAI_MODEL
LANGSMITH_API_KEY
LANGSMITH_TRACING
LANGSMITH_PROJECT
APPLICATION_DATABASE_DIRECT
APPLICATION_MIGRATION_DATABASE_DIRECT
TEMPORAL_ADDRESS
TEMPORAL_NAMESPACE
TEMPORAL_TASK_QUEUE
```

Load `.env` without logging its values. Fail immediately with a clear message if `OPENAI_API_KEY` is missing. LangSmith tracing may be optional, but when `LANGSMITH_TRACING=true`, the experiment must attach stable run, stage, attempt, and Temporal IDs as trace metadata.

## Hypothesis to prove

Use this graph:

```text
                         ┌─ fast_research (about 2 seconds) ─┐
START → prepare_inputs ──┤                                   ├─ any(1) → synthesize → END
                         └─ slow_research (about 20 seconds) ┘
```

Both research stages must invoke an OpenAI-backed Deep Agent inside a Temporal activity. The slow stage must include a controlled delay so the ordering is deterministic enough to test.

The proof passes only if:

```text
fast_research admitted_at
    <= synthesize launched_at
    < slow_research completed_at
```

The parent LangGraph `Send` superstep may wait for both Temporal **launch acknowledgements**, but it must not wait for both research results. Each `Send("launch_temporal_stage", ...)` must return after starting or reconnecting to a Temporal workflow.

## Required package layout

Create at least:

```text
app/experiments/langgraph_temporal_stagegraph/
  __init__.py
  README.md
  config.py
  contracts.py
  schema.sql
  repository.py
  temporal_workflows.py
  temporal_activities.py
  temporal_worker.py
  graph_state.py
  graph.py
  wake_dispatcher.py
  run_experiment.py
  inspect_run.py

tests/experiments/
  test_langgraph_temporal_stagegraph.py
```

Small variations are acceptable, but keep Temporal mechanics, graph scheduling, persistence, and experiment driving visibly separate.

## Process topology

Run the proof with two Python processes plus Docker services:

```text
Process 1: Temporal worker
  uv run python -m app.experiments.langgraph_temporal_stagegraph.temporal_worker

Process 2: experiment driver
  uv run python -m app.experiments.langgraph_temporal_stagegraph.run_experiment

Docker:
  Temporal + Temporal PostgreSQL + application PostgreSQL
```

The driver process hosts:

- the compiled LangGraph;
- its PostgreSQL checkpointer;
- the experiment repository;
- a small outbox wake dispatcher.

Do not require the BellLabs Agent Server API for this experiment.

## Persistence model

Use application PostgreSQL for experiment-owned authoritative records and LangGraph checkpoints. Do not use Temporal's PostgreSQL database for application truth.

Create an isolated schema such as:

```sql
CREATE SCHEMA IF NOT EXISTS stagegraph_temporal_experiment;

CREATE TABLE IF NOT EXISTS stagegraph_temporal_experiment.runs (
    run_id text PRIMARY KEY,
    thread_id text NOT NULL UNIQUE,
    status text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS stagegraph_temporal_experiment.stage_attempts (
    attempt_id text PRIMARY KEY,
    run_id text NOT NULL REFERENCES stagegraph_temporal_experiment.runs(run_id),
    stage_id text NOT NULL,
    attempt_number integer NOT NULL,
    status text NOT NULL,
    temporal_workflow_id text UNIQUE,
    temporal_run_id text,
    output_ref text,
    error_type text,
    reserved_at timestamptz NOT NULL,
    launched_at timestamptz,
    completed_at timestamptz,
    admitted_at timestamptz,
    UNIQUE (run_id, stage_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS stagegraph_temporal_experiment.outbox (
    event_id text PRIMARY KEY,
    run_id text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    delivered_at timestamptz,
    delivery_attempts integer NOT NULL DEFAULT 0
);
```

Add an experiment result/artifact table if useful. Store compact JSON or text outputs only for this experiment. Production conclusions must still recommend immutable external artifact references rather than checkpointing large model output.

The completion transaction must atomically:

1. transition the attempt to `READY_TO_RECONCILE`;
2. store the output/result reference;
3. insert an idempotent `WORKFLOW_WAKE_REQUESTED` outbox event.

Use deterministic identities:

```python
def attempt_id(run_id: str, stage_id: str, attempt_number: int = 1) -> str:
    return f"attempt:{run_id}:{stage_id}:{attempt_number}"


def temporal_workflow_id(attempt_id: str) -> str:
    return f"stagegraph-experiment:{attempt_id}"


def completion_event_id(attempt_id: str, output_digest: str) -> str:
    return f"completion:{attempt_id}:{output_digest}"
```

All repository mutations must be idempotent and use compare-and-set conditions where lifecycle state matters.

## Contracts

Use plain dataclasses or Pydantic models that Temporal can serialize predictably:

```python
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class TemporalStageInput:
    run_id: str
    thread_id: str
    attempt_id: str
    stage_id: str
    prompt: str
    delay_seconds: float
    model: str
    trace_headers: dict[str, str]


@dataclass(frozen=True)
class TemporalStageResult:
    attempt_id: str
    stage_id: str
    output_text: str
    output_digest: str


@dataclass(frozen=True)
class CompletionRecord:
    run_id: str
    thread_id: str
    attempt_id: str
    stage_id: str
    temporal_workflow_id: str
    temporal_run_id: str
    disposition: Literal["succeeded", "failed", "cancelled"]
    output_text: str | None
    output_digest: str | None
    error_type: str | None
```

Temporal workflow inputs must not contain secrets. `OPENAI_API_KEY` stays in the Temporal worker environment.

## Temporal activity using Deep Agents and OpenAI

Use the installed `deepagents.create_deep_agent` API. Keep the prompt bounded and inexpensive.

```python
import asyncio
import hashlib

from deepagents import create_deep_agent
from temporalio import activity

from .contracts import TemporalStageInput, TemporalStageResult


@activity.defn
async def execute_deep_agent_stage(request: TemporalStageInput) -> TemporalStageResult:
    activity.heartbeat("activity-started")

    if request.delay_seconds:
        # Sleep in the activity, not the deterministic Temporal workflow.
        remaining = request.delay_seconds
        while remaining > 0:
            interval = min(1.0, remaining)
            await asyncio.sleep(interval)
            remaining -= interval
            activity.heartbeat({"remaining_delay_seconds": remaining})

    agent = create_deep_agent(
        model=request.model,
        tools=[],
        system_prompt=(
            "You are a concise biotechnology research assistant in an architecture "
            "experiment. Return a short, factual response and clearly label uncertainty."
        ),
        name=f"experiment_{request.stage_id}",
    )

    result = await agent.ainvoke(
        {
            "messages": [
                {"role": "user", "content": request.prompt},
            ]
        },
        config={
            "tags": ["stagegraph-temporal-experiment", request.stage_id],
            "metadata": {
                "run_id": request.run_id,
                "attempt_id": request.attempt_id,
                "stage_id": request.stage_id,
                "temporal_workflow_id": activity.info().workflow_id,
            },
        },
    )

    final_message = result["messages"][-1]
    output_text = str(final_message.content)
    output_digest = hashlib.sha256(output_text.encode("utf-8")).hexdigest()

    activity.heartbeat("activity-completed")
    return TemporalStageResult(
        attempt_id=request.attempt_id,
        stage_id=request.stage_id,
        output_text=output_text,
        output_digest=output_digest,
    )
```

If LangSmith distributed trace headers are available, enter a LangSmith tracing context around the agent call. If the installed SDK surface differs, inspect the pinned version and adapt rather than guessing. Never let tracing failure fail the experiment's stage execution.

## Temporal completion activity

For the experiment, the Temporal worker may write directly to the application PostgreSQL repository. That avoids requiring an HTTP callback service while preserving the durable completion boundary.

```python
@activity.defn
async def record_stage_completion(completion: CompletionRecord) -> None:
    repository = get_worker_repository()
    await repository.record_completion_and_wake(completion)
```

`record_completion_and_wake()` must use one database transaction and be safe when Temporal retries the activity.

## Temporal stage workflow

```python
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from .contracts import CompletionRecord, TemporalStageInput
    from .temporal_activities import execute_deep_agent_stage, record_stage_completion


@workflow.defn
class TemporalStageWorkflow:
    @workflow.run
    async def run(self, request: TemporalStageInput) -> None:
        info = workflow.info()

        try:
            result = await workflow.execute_activity(
                execute_deep_agent_stage,
                request,
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                    maximum_attempts=3,
                ),
            )

            completion = CompletionRecord(
                run_id=request.run_id,
                thread_id=request.thread_id,
                attempt_id=request.attempt_id,
                stage_id=request.stage_id,
                temporal_workflow_id=info.workflow_id,
                temporal_run_id=info.run_id,
                disposition="succeeded",
                output_text=result.output_text,
                output_digest=result.output_digest,
                error_type=None,
            )
        except Exception as exc:
            completion = CompletionRecord(
                run_id=request.run_id,
                thread_id=request.thread_id,
                attempt_id=request.attempt_id,
                stage_id=request.stage_id,
                temporal_workflow_id=info.workflow_id,
                temporal_run_id=info.run_id,
                disposition="failed",
                output_text=None,
                output_digest=None,
                error_type=type(exc).__name__,
            )

        await workflow.execute_activity(
            record_stage_completion,
            completion,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_interval=timedelta(seconds=10),
                maximum_attempts=0,
            ),
        )
```

Confirm the precise Temporal Python semantics of `maximum_attempts=0` in the installed version. If it means unlimited attempts, document that; otherwise use the correct explicit policy. Avoid broad exception handling that swallows cancellation. Treat Temporal cancellation separately if needed.

## Temporal worker

```python
import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from .config import settings
from .temporal_activities import execute_deep_agent_stage, record_stage_completion
from .temporal_workflows import TemporalStageWorkflow


async def main() -> None:
    client = await Client.connect(
        settings.temporal_address,
        namespace=settings.temporal_namespace,
    )

    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[TemporalStageWorkflow],
        activities=[execute_deep_agent_stage, record_stage_completion],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
```

Use an experiment-specific task queue, for example:

```text
stagegraph-temporal-deepagents-experiment
```

## LangGraph state

Keep the checkpoint compact:

```python
import operator
from typing import Annotated, TypedDict


class DispatchItem(TypedDict):
    attempt_id: str
    stage_id: str
    prompt: str
    delay_seconds: float


class LaunchReceipt(TypedDict):
    attempt_id: str
    temporal_workflow_id: str
    temporal_run_id: str | None


class ExperimentState(TypedDict):
    run_id: str
    thread_id: str
    dispatch_batch: tuple[DispatchItem, ...]
    launch_receipts: Annotated[tuple[LaunchReceipt, ...], operator.add]
    admitted_outputs: dict[str, str]
    waiting_attempt_ids: tuple[str, ...]
    synthesized_output: str | None
    event_log: Annotated[tuple[dict[str, object], ...], operator.add]
```

Implement conflict-aware reducers for production-shaped behavior. `operator.add` is acceptable only for an initial working slice followed by a deduplication test and a documented replacement.

## Launch node

The launch node must never call `await handle.result()`.

```python
from temporalio.exceptions import WorkflowAlreadyStartedError


async def launch_temporal_stage(state, runtime) -> dict:
    item = state["dispatch_item"]
    workflow_id = f"stagegraph-experiment:{item['attempt_id']}"

    request = TemporalStageInput(
        run_id=state["run_id"],
        thread_id=state["thread_id"],
        attempt_id=item["attempt_id"],
        stage_id=item["stage_id"],
        prompt=item["prompt"],
        delay_seconds=item["delay_seconds"],
        model=runtime.settings.openai_model,
        trace_headers=runtime.current_trace_headers(),
    )

    try:
        handle = await runtime.temporal.start_workflow(
            TemporalStageWorkflow.run,
            request,
            id=workflow_id,
            task_queue=runtime.settings.temporal_task_queue,
        )
        temporal_run_id = handle.result_run_id
    except WorkflowAlreadyStartedError:
        handle = runtime.temporal.get_workflow_handle(workflow_id)
        temporal_run_id = None

    await runtime.repository.bind_temporal_execution(
        attempt_id=item["attempt_id"],
        temporal_workflow_id=workflow_id,
        temporal_run_id=temporal_run_id,
    )

    return {
        "launch_receipts": (
            {
                "attempt_id": item["attempt_id"],
                "temporal_workflow_id": workflow_id,
                "temporal_run_id": temporal_run_id,
            },
        )
    }
```

Inspect the installed Temporal SDK for the exact exception import and handle properties. The semantic requirement is stable even if a name differs.

## Scheduling with `Send`

The first scheduler pass reserves and launches `fast_research` and `slow_research`. A later pass launches `synthesize` once at least one research result is admitted.

```python
from langgraph.types import Send


def dispatch_ready_stages(state: ExperimentState):
    if state["dispatch_batch"]:
        return [
            Send(
                "launch_temporal_stage",
                {
                    "run_id": state["run_id"],
                    "thread_id": state["thread_id"],
                    "dispatch_item": item,
                },
            )
            for item in state["dispatch_batch"]
        ]

    if state["waiting_attempt_ids"]:
        return "wait_for_completion"

    if state["synthesized_output"] is not None:
        return "finish"

    raise RuntimeError("No dispatch, wait, or terminal condition")
```

Do not place database reservations inside a conditional-edge function. Perform reservations idempotently in a normal `prepare_dispatch` node, store the exact batch, and let the routing function remain pure.

## Durable wait and resume

```python
from langgraph.types import interrupt


def wait_for_completion(state: ExperimentState) -> dict:
    wake = interrupt(
        {
            "kind": "temporal_stage_completion",
            "run_id": state["run_id"],
            "waiting_attempt_ids": state["waiting_attempt_ids"],
        }
    )
    return {
        "event_log": (
            {
                "event": "graph_resumed",
                "wake_event_id": wake["wake_event_id"],
            },
        )
    }
```

The edge after this node returns to `reconcile`. Never trust output data in the resume payload. The payload only says that authoritative state may have changed.

## Wake dispatcher

The dispatcher polls the experiment outbox and resumes only interrupted threads. Because this experiment runs without Agent Server, it may call the compiled graph directly.

```python
from langgraph.types import Command


class WakeDispatcher:
    def __init__(self, graph, repository) -> None:
        self._graph = graph
        self._repository = repository
        self._locks: dict[str, asyncio.Lock] = {}

    async def deliver(self, event) -> None:
        lock = self._locks.setdefault(event.run_id, asyncio.Lock())
        async with lock:
            config = {"configurable": {"thread_id": event.thread_id}}
            snapshot = await self._graph.aget_state(config)

            # If the graph is still launching stages, leave the event pending.
            if not any(task.interrupts for task in snapshot.tasks):
                return

            await self._graph.ainvoke(
                Command(
                    resume={
                        "wake_event_id": event.event_id,
                        "reason": "authoritative_state_changed",
                    }
                ),
                config=config,
            )
            await self._repository.mark_outbox_delivered(event.event_id)
```

The real implementation must:

- increment delivery attempts;
- use an inter-process-safe PostgreSQL advisory lock or lease rather than only an in-memory lock;
- leave early wake events pending until the graph reaches its interrupt;
- coalesce multiple events for the same run;
- tolerate a graph that observes the completion during pre-wait reconciliation and therefore never needs that wake;
- mark an event delivered only after successful reconciliation/resume or after proving it obsolete.

## Reconciliation and `any` join

```python
async def reconcile(state: ExperimentState, runtime) -> dict:
    projection = await runtime.repository.load_run_projection(state["run_id"])

    admitted_outputs = dict(state.get("admitted_outputs", {}))
    for attempt in projection.ready_to_reconcile:
        await runtime.repository.admit_success_idempotently(attempt.attempt_id)
        admitted_outputs[attempt.stage_id] = attempt.output_ref

    waiting = tuple(
        attempt.attempt_id
        for attempt in projection.attempts
        if attempt.status not in {"ADMITTED", "FAILED", "CANCELLED"}
    )

    return {
        "admitted_outputs": admitted_outputs,
        "waiting_attempt_ids": waiting,
    }
```

The frontier rule for `synthesize` is deliberately simple in this experiment:

```python
research_inputs = {
    stage_id: output_ref
    for stage_id, output_ref in admitted_outputs.items()
    if stage_id in {"fast_research", "slow_research"}
}

synthesis_ready = bool(research_inputs)  # any(1)
```

When synthesis becomes ready, freeze the chosen admitted input set in its input/prompt record. Do not silently add the slow result to a synthesis attempt that has already launched.

## Driver

The driver must:

1. load `.env` safely;
2. connect to Temporal and application PostgreSQL;
3. run experiment schema setup through the migration credential;
4. initialize the LangGraph PostgreSQL checkpointer once;
5. compile the graph;
6. start the wake dispatcher loop;
7. invoke a new run with a stable `thread_id`;
8. wait until the graph finishes;
9. query and print a redacted timeline;
10. assert all acceptance conditions and exit nonzero on failure.

Example initial prompts:

```python
fast_prompt = (
    "In at most 120 words, identify two plausible mechanisms by which cellular "
    "senescence can affect tissue aging. This is research discussion, not medical advice."
)

slow_prompt = (
    "In at most 120 words, identify two evidence-quality concerns when interpreting "
    "preclinical longevity studies. This is research discussion, not medical advice."
)
```

The synthesis prompt must include only the frozen admitted research outputs and request a response of at most 150 words.

## Commands documented in the experiment README

PowerShell examples:

```powershell
cd C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system

docker compose up -d application-postgres temporal-postgres temporal-schema temporal temporal-create-namespace temporal-ui

uv sync

uv run python -m app.experiments.langgraph_temporal_stagegraph.temporal_worker
```

In a second terminal:

```powershell
cd C:\Users\Pinda\Proyectos\Biotech\biotech-research-ingestion-evaluation-system

uv run python -m app.experiments.langgraph_temporal_stagegraph.run_experiment
```

Add a bounded timeout and cleanup instructions. Do not use commands that delete Docker volumes.

## Required tests

### 1. Pure scheduler test

Prove:

- neither research result admitted → synthesis not ready;
- fast admitted → synthesis ready;
- slow admitted → synthesis ready;
- both admitted → synthesis ready with deterministic frozen-input selection.

### 2. Launch idempotency test

Call the launch node twice with the same attempt identity and verify only one Temporal workflow ID is used.

### 3. Completion idempotency test

Deliver the same `CompletionRecord` twice and verify:

- one terminal attempt transition;
- one logical output;
- one logical wake event;
- no duplicate admission.

### 4. Wake-before-interrupt race

Commit fast completion before the graph enters `interrupt()`. Verify the next reconciliation sees it and synthesis launches without requiring a lost wake.

### 5. Live timing proof

With real Temporal, Deep Agents, and OpenAI:

```text
fast admitted <= synthesis launched < slow completed
```

Also verify Temporal UI shows separate workflow executions for:

- fast research;
- slow research;
- synthesis.

### 6. Checkpoint/resume proof

At minimum, verify the graph reaches a persisted interrupt and resumes on the same thread. Prefer also supporting this manual drill:

1. let the graph reach `interrupt()`;
2. stop the driver process without stopping Temporal;
3. allow a stage to complete;
4. restart the driver in `--resume RUN_ID` mode;
5. deliver the pending outbox event;
6. finish the same graph thread without relaunching completed stages.

### 7. Worker restart proof

Optional but valuable:

1. start the slow activity;
2. stop the Temporal worker;
3. restart it;
4. verify Temporal retries/resumes according to activity semantics;
5. verify only one semantic stage result is admitted.

## Acceptance report

Write a generated report under the experiment directory, for example:

```text
app/experiments/langgraph_temporal_stagegraph/artifacts/latest_report.md
```

Do not commit raw model outputs if they could contain sensitive content. This experiment uses bounded public-research prompts, so short redacted excerpts are acceptable.

The report must contain:

- package versions;
- Docker service health;
- run ID and thread ID;
- Temporal workflow IDs;
- stage lifecycle timeline with UTC timestamps;
- LangGraph interrupt checkpoint evidence;
- wake event delivery evidence;
- duplicate-delivery assertions;
- the exact timing inequality;
- LangSmith trace URL or trace identifiers when available;
- pass/fail for every acceptance criterion;
- limitations and production follow-ups.

Example table:

| Event | Timestamp UTC | Relative seconds |
|---|---:|---:|
| research launches acknowledged | ... | 0.00 |
| graph interrupted | ... | 0.15 |
| fast research completed | ... | 2.50 |
| fast result admitted | ... | 2.65 |
| graph resumed | ... | 2.70 |
| synthesis launched | ... | 2.84 |
| slow research completed | ... | 20.40 |
| synthesis admitted | ... | ... |

Required assertion:

```text
PASS: synthesis launched 17.56 seconds before slow sibling completed.
```

## Failure handling requirements

The experiment must fail clearly rather than hang when:

- Temporal is unavailable;
- application PostgreSQL is unavailable;
- `OPENAI_API_KEY` is absent;
- the Temporal worker is not polling the experiment task queue;
- an OpenAI request exhausts its retries;
- the graph does not reach an interrupt within the expected time;
- the wake event is not delivered within the expected time;
- synthesis fails to launch before the slow sibling completes.

Use explicit timeouts around the overall experiment. Print safe diagnostics and pointers to Temporal UI and the experiment tables.

## Security and cost controls

- Never log or persist API keys.
- Do not send `.env`, repository files, private corpora, or PHI to the model.
- Keep prompts and outputs bounded.
- Use the configured inexpensive `OPENAI_MODEL`; do not hard-code an expensive model.
- Limit each stage agent to one concise invocation unless the experiment explicitly measures multi-step behavior.
- Enable LangSmith input/output hiding according to the existing environment policy.
- Do not expose Temporal or PostgreSQL beyond the existing loopback Compose bindings.

## Non-goals

Do not add these to the first proof:

- production Agent Server API integration;
- arbitrary user-authored graphs;
- MCP, Tavily, or Firecrawl tool calls;
- cancellation of unselected siblings;
- `minimum(n)` joins;
- speculative execution;
- multi-tenant authorization;
- production artifact storage;
- production migrations.

Those are follow-up experiments after the core timing and durability hypothesis passes.

## Completion checklist

- [ ] Experiment package is isolated under `app/experiments/`.
- [ ] Temporal worker runs Deep Agents with OpenAI.
- [ ] LangGraph launches Temporal workflows via `Send` without awaiting results.
- [ ] LangGraph persists an interrupt using PostgreSQL checkpointing.
- [ ] Temporal completion and outbox wake are committed atomically.
- [ ] Wake dispatcher resumes the same LangGraph thread.
- [ ] `any(1)` admits one research result and launches synthesis early.
- [ ] Slow sibling remains running and later settles normally.
- [ ] Duplicate launch, completion, admission, and wake paths are safe.
- [ ] Automated tests pass.
- [ ] Live proof records the required timing inequality.
- [ ] README contains exact setup, run, inspection, and bounded cleanup commands.
- [ ] Report documents evidence, limitations, and production implications.

## Final handoff response expected from the implementing agent

The implementing agent should return:

1. a concise description of what was built;
2. clickable paths to the experiment README, driver, graph, Temporal workflow, tests, and report;
3. commands executed;
4. test results;
5. the measured timing inequality;
6. Temporal and LangSmith evidence;
7. any deviations from this handoff and why;
8. the next architectural decision supported—or not supported—by the experiment.
