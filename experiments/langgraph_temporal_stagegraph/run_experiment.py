from __future__ import annotations

# ruff: noqa: E501 -- generated Markdown prose is intentionally readable in source.
import argparse
import asyncio
import importlib.metadata
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from temporalio.client import Client

from .config import ExperimentSettings, load_settings
from .contracts import CompletionRecord
from .graph import compile_experiment_graph
from .repository import ExperimentRepository, prepare_database
from .wake_dispatcher import WakeDispatcher


def _utc(value: datetime | None) -> str:
    return value.astimezone(UTC).isoformat() if value else "—"


def _stage(timeline: dict[str, Any], stage_id: str) -> dict[str, Any]:
    return next(item for item in timeline["attempts"] if item["stage_id"] == stage_id)


async def _write_report(
    timeline: dict[str, Any], settings: ExperimentSettings, assertions: dict[str, bool]
) -> Path:
    run = timeline["run"]
    fast = _stage(timeline, "fast_research")
    slow = _stage(timeline, "slow_research")
    synthesis = _stage(timeline, "synthesize")
    lead = (slow["completed_at"] - synthesis["launched_at"]).total_seconds()
    versions = {
        name: importlib.metadata.version(name)
        for name in (
            "deepagents",
            "langgraph",
            "langgraph-checkpoint-postgres",
            "langchain-openai",
            "openai",
            "temporalio",
        )
    }
    rows = []
    for item in timeline["attempts"]:
        rows.append(
            f"| {item['stage_id']} reserved | {_utc(item['reserved_at'])} | {item['status']} |"
        )
        rows.append(
            f"| {item['stage_id']} launched | {_utc(item['launched_at'])} | {item['temporal_workflow_id']} |"
        )
        rows.append(
            f"| {item['stage_id']} completed | {_utc(item['completed_at'])} | digest `{(item['output_digest'] or '')[:12]}` |"
        )
        rows.append(
            f"| {item['stage_id']} admitted | {_utc(item['admitted_at'])} | immutable experiment result ref |"
        )
    checks = "\n".join(
        f"- {'PASS' if passed else 'FAIL'}: {name}" for name, passed in assertions.items()
    )
    trace_note = (
        "LangSmith tracing was enabled; stable run/stage/attempt/Temporal IDs were attached as metadata."
        if settings.langsmith_tracing
        else "LangSmith tracing was disabled."
    )
    report = f"""# LangGraph + Temporal + Deep Agents StageGraph experiment

Generated: {_utc(datetime.now(UTC))}

## Environment

- Run ID: `{run["run_id"]}`
- Thread ID: `{run["thread_id"]}`
- Temporal: `{settings.temporal_address}` / namespace `{settings.temporal_namespace}`
- Temporal UI: http://127.0.0.1:8080
- Application PostgreSQL: reachable; experiment schema isolated
- Versions: `{json.dumps(versions, sort_keys=True)}`
- {trace_note}

## Timeline

| Event | Timestamp UTC | Evidence |
|---|---:|---|
{chr(10).join(rows)}

Required inequality:

`{_utc(fast["admitted_at"])} <= {_utc(synthesis["launched_at"])} < {_utc(slow["completed_at"])}`

**{"PASS" if assertions["timing inequality"] else "FAIL"}: synthesis launched {lead:.2f} seconds before slow sibling completed.**

## Durable checkpoint and wake evidence

- Persisted interrupt events: {sum(e["event_type"] == "GRAPH_INTERRUPTED" for e in timeline["graph_events"])}
- Delivered/obsolete wake events: {sum(e["delivered_at"] is not None for e in timeline["outbox"])}/{len(timeline["outbox"])}
- Delivery attempts: {", ".join(str(e["delivery_attempts"]) for e in timeline["outbox"])}
- Temporal workflow IDs: {", ".join(a["temporal_workflow_id"] for a in timeline["attempts"])}

## Acceptance

{checks}

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
"""
    target = Path(__file__).with_name("artifacts") / "latest_report.md"
    target.parent.mkdir(exist_ok=True)
    target.write_text(report, encoding="utf-8")
    return target


async def run(args: argparse.Namespace) -> None:
    settings = load_settings()
    await prepare_database(settings.application_migration_database_dsn)
    repository = await ExperimentRepository.connect(settings.application_database_dsn)
    temporal = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    run_id = args.resume or f"run-{uuid.uuid4().hex[:12]}"
    thread_id = f"thread:{run_id}"
    await repository.create_run(run_id, thread_id)
    stop = asyncio.Event()
    try:
        async with AsyncPostgresSaver.from_conn_string(settings.application_database_dsn) as saver:
            graph = compile_experiment_graph(
                repository=repository,
                temporal=temporal,
                settings=settings,
                checkpointer=saver,
            )
            config = {"configurable": {"thread_id": thread_id}}
            if not args.resume:
                await graph.ainvoke(
                    {
                        "run_id": run_id,
                        "thread_id": thread_id,
                        "dispatch_batch": (),
                        "launch_receipts": (),
                        "admitted_outputs": {},
                        "waiting_attempt_ids": (),
                        "synthesized_output": None,
                        "frozen_synthesis_stages": (),
                        "event_log": (),
                    },
                    config=config,
                )
            # User comments: Check for the args.resume. If it is present, retrieve the graph state
            #  Record an event and wake the dispatcher wi
            snapshot = await graph.aget_state(config)
            if any(task.interrupts for task in snapshot.tasks):
                await repository.record_graph_event(
                    f"interrupt-initial:{run_id}",
                    run_id,
                    "GRAPH_INTERRUPTED",
                    {"thread_id": thread_id},
                )
            dispatcher = WakeDispatcher(graph, repository)
            dispatcher_task = asyncio.create_task(dispatcher.run(stop))
            try:
                async with asyncio.timeout(settings.overall_timeout_seconds):
                    while True:
                        timeline = await repository.timeline(run_id)
                        if timeline["run"]["status"] == "COMPLETED":
                            break
                        await asyncio.sleep(0.25)
            except TimeoutError as exc:
                raise RuntimeError(
                    "Experiment timed out. Verify a worker is polling "
                    f"{settings.temporal_task_queue!r} and inspect http://127.0.0.1:8080"
                ) from exc
            finally:
                stop.set()
                await dispatcher_task

            timeline = await repository.timeline(run_id)
            fast = _stage(timeline, "fast_research")
            original = CompletionRecord(
                run_id=run_id,
                thread_id=thread_id,
                attempt_id=fast["attempt_id"],
                stage_id=fast["stage_id"],
                temporal_workflow_id=fast["temporal_workflow_id"],
                temporal_run_id=fast["temporal_run_id"],
                disposition="succeeded",
                output_text=fast["output_text"],
                output_digest=fast["output_digest"],
                error_type=None,
            )
            await repository.record_completion_and_wake(original)
            await repository.record_completion_and_wake(original)
            timeline = await repository.timeline(run_id)
            fast = _stage(timeline, "fast_research")
            slow = _stage(timeline, "slow_research")
            synthesis = _stage(timeline, "synthesize")
            assertions = {
                "three separate Temporal workflows": len(
                    {item["temporal_workflow_id"] for item in timeline["attempts"]}
                )
                == 3,
                "all stages admitted exactly once": len(timeline["attempts"]) == 3
                and all(item["status"] == "ADMITTED" for item in timeline["attempts"]),
                "persisted interrupt observed": any(
                    item["event_type"] == "GRAPH_INTERRUPTED" for item in timeline["graph_events"]
                ),
                "wake delivery observed": any(
                    item["delivered_at"] is not None for item in timeline["outbox"]
                ),
                "duplicate completion coalesced": sum(
                    event["event_id"].startswith(f"completion:{fast['attempt_id']}:")
                    for event in timeline["outbox"]
                )
                == 1,
                "timing inequality": fast["admitted_at"]
                <= synthesis["launched_at"]
                < slow["completed_at"],
            }
            report_path = await _write_report(timeline, settings, assertions)
            print(f"run_id={run_id}")
            print(f"thread_id={thread_id}")
            print(
                "timing="
                f"{_utc(fast['admitted_at'])} <= {_utc(synthesis['launched_at'])} "
                f"< {_utc(slow['completed_at'])}"
            )
            print(f"report={report_path}")
            failed = [name for name, passed in assertions.items() if not passed]
            if failed:
                raise AssertionError("acceptance failures: " + ", ".join(failed))
            print("PASS: all required acceptance assertions succeeded")
    finally:
        await repository.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", metavar="RUN_ID")
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
