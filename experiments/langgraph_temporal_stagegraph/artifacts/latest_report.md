# LangGraph + Temporal + Deep Agents StageGraph experiment

Generated: 2026-08-08T04:00:29.798618+00:00

## Environment

- Run ID: `run-c71f76ba4bf5`
- Thread ID: `thread:run-c71f76ba4bf5`
- Temporal: `localhost:7233` / namespace `default`
- Temporal UI: http://127.0.0.1:8080
- Application PostgreSQL: reachable; experiment schema isolated
- Versions: `{"deepagents": "0.7.4", "langgraph": "1.2.10", "langgraph-checkpoint-postgres": "3.1.1", "langchain-openai": "1.3.4", "openai": "2.44.0", "temporalio": "1.30.0"}`
- LangSmith tracing was enabled; stable run/stage/attempt/Temporal IDs were attached as metadata.

## Timeline

| Event | Timestamp UTC | Evidence |
|---|---:|---|
| fast_research reserved | 2026-08-08T03:59:42.158133+00:00 | ADMITTED |
| fast_research launched | 2026-08-08T03:59:42.259327+00:00 | stagegraph-experiment:attempt:run-c71f76ba4bf5:fast_research:1 |
| fast_research completed | 2026-08-08T03:59:46.379439+00:00 | digest `7b7603600daa` |
| fast_research admitted | 2026-08-08T03:59:46.543641+00:00 | immutable experiment result ref |
| slow_research reserved | 2026-08-08T03:59:42.166074+00:00 | ADMITTED |
| slow_research launched | 2026-08-08T03:59:42.200446+00:00 | stagegraph-experiment:attempt:run-c71f76ba4bf5:slow_research:1 |
| slow_research completed | 2026-08-08T04:00:29.604578+00:00 | digest `23a462bad2c9` |
| slow_research admitted | 2026-08-08T04:00:29.680909+00:00 | immutable experiment result ref |
| synthesize reserved | 2026-08-08T03:59:46.551267+00:00 | ADMITTED |
| synthesize launched | 2026-08-08T03:59:46.578253+00:00 | stagegraph-experiment:attempt:run-c71f76ba4bf5:synthesize:1 |
| synthesize completed | 2026-08-08T03:59:48.719874+00:00 | digest `170f2b1294e1` |
| synthesize admitted | 2026-08-08T03:59:48.855919+00:00 | immutable experiment result ref |

Required inequality:

`2026-08-08T03:59:46.543641+00:00 <= 2026-08-08T03:59:46.578253+00:00 < 2026-08-08T04:00:29.604578+00:00`

**PASS: synthesis launched 43.03 seconds before slow sibling completed.**

## Durable checkpoint and wake evidence

- Persisted interrupt events: 3
- Delivered/obsolete wake events: 3/3
- Delivery attempts: 1, 1, 1
- Temporal workflow IDs: stagegraph-experiment:attempt:run-c71f76ba4bf5:fast_research:1, stagegraph-experiment:attempt:run-c71f76ba4bf5:slow_research:1, stagegraph-experiment:attempt:run-c71f76ba4bf5:synthesize:1

## Temporal and LangSmith evidence

Temporal reported all three executions `COMPLETED`:

- fast research run `019fdf86-d067-7202-9ebb-2e1b5ef68de3`
- slow research run `019fdf86-d066-77d4-bc84-d29de266b7dc`
- synthesis run `019fdf86-e187-7275-b66e-b7c676e2de35`

LangSmith accepted three root traces with stable experiment metadata:

- [fast research trace](https://smith.langchain.com/o/44cdbef8-5da5-5f7b-97d4-d7328bf809a7/projects/p/45a23873-7915-4104-9c85-2bf7adaea137/r/019fdf86-d8b9-7d60-8b4d-2d2929f35a1c?poll=true)
- [slow research trace](https://smith.langchain.com/o/44cdbef8-5da5-5f7b-97d4-d7328bf809a7/projects/p/45a23873-7915-4104-9c85-2bf7adaea137/r/019fdf87-81ad-7fc3-861b-c8722117c455?poll=true)
- [synthesis trace](https://smith.langchain.com/o/44cdbef8-5da5-5f7b-97d4-d7328bf809a7/projects/p/45a23873-7915-4104-9c85-2bf7adaea137/r/019fdf86-e1d2-7903-84b2-080b7e10e7e8?poll=true)

## Acceptance

- PASS: three separate Temporal workflows
- PASS: all stages admitted exactly once
- PASS: persisted interrupt observed
- PASS: wake delivery observed
- PASS: duplicate completion coalesced
- PASS: timing inequality

## Duplicate-delivery assertions

The driver replayed the fast completion transaction twice after settlement. The attempt remained one
logical `ADMITTED` row, its result remained one row, and the deterministic completion event remained
one logical outbox row. Launch IDs use `REJECT_DUPLICATE`, and checkpoint reducers deduplicate receipts.

## Limitations and production follow-ups

- This experiment stores bounded public-research text in PostgreSQL. Production should store immutable
  external artifact references and checkpoint only compact references.
- The dispatcher is in-process but uses a PostgreSQL advisory lock, so competing dispatcher processes
  cannot resume the same run concurrently.
- Cancellation of unselected siblings, arbitrary joins, authorization, and Agent Server integration are
  intentionally out of scope.
- Temporal's `RetryPolicy(maximum_attempts=0)` means unlimited attempts in the Python SDK/server retry
  contract and is used only for the durable completion-recording activity.
- The handoff assumed `langchain-openai` was transitively installed; it was not. The experiment adds
  explicit `langchain-openai 1.3.4`, compatible with the project's `openai <2.45` ceiling.
- A preliminary real run showed that fast-stage model latency could exceed 20 seconds. The controlled
  slow delay is therefore 45 seconds, preserving a deterministic ordering proof.
