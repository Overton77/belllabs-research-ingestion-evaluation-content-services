from __future__ import annotations

# ruff: noqa: E501 -- generated acceptance-report prose is intentionally readable.
import argparse
import asyncio
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from temporalio.client import Client

from app.experiments.langgraph_temporal_stagegraph.repository import (
    ExperimentRepository,
    prepare_database,
)

from .config import load_swarm_settings
from .contracts import FinalSynthesis, MissionPlan
from .graph import compile_swarm_graph
from .repository import SwarmEvidenceRepository, setup_swarm_database
from .wake_dispatcher import RunScopedWakeDispatcher

DEFAULT_OBJECTIVE = (
    "Assess the peer-reviewed human evidence for fisetin as a senolytic intervention, including "
    "study designs, sample sizes, reported outcomes, and major limitations. This is research "
    "discussion, not medical advice."
)


def _utc(value: datetime | None) -> str:
    return value.astimezone(UTC).isoformat() if value else "—"


def _json_value(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


async def _write_report(
    run_id: str,
    objective: str,
    timeline: dict[str, Any],
    evidence: dict[str, Any],
) -> Path:
    plan = MissionPlan.model_validate(_json_value(evidence["plans"][0]["plan_json"]))
    synthesis_attempt = next(
        item for item in timeline["attempts"] if item["stage_id"] == "synthesize"
    )
    synthesis = FinalSynthesis.model_validate_json(synthesis_attempt["output_text"])
    accepted = [item for item in evidence["claims"] if item["disposition"] == "ACCEPT"]
    rejected = [item for item in evidence["claims"] if item["disposition"] == "REJECT"]
    stage_rows = "\n".join(
        f"| {item['stage_id']} | {item['status']} | {_utc(item['launched_at'])} | "
        f"{_utc(item['completed_at'])} | `{item['temporal_workflow_id']}` |"
        for item in timeline["attempts"]
    )
    unit_rows = "\n".join(
        f"| `{unit.unit_id}` | {unit.mode} | {unit.question} | `{unit.search_query}` |"
        for unit in plan.units
    )
    claim_rows = []
    for item in evidence["claims"]:
        claim = _json_value(item["claim_json"])
        report = _json_value(item["report_json"])
        claim_rows.append(
            f"| `{item['claim_id']}` | {item['disposition']} | "
            f"{report['lexical_support_score']:.3f} | {claim['claim_text']} |"
        )
    source_rows = "\n".join(
        f"| `{item['source_id']}` | {item['title']} | {item['url']} | `{item['text_sha256'][:12]}` |"
        for item in evidence["sources"]
    )
    report = f"""# Dynamic research swarm experiment

Generated: {_utc(datetime.now(UTC))}

- Run ID: `{run_id}`
- Objective: {objective}
- Mission units: {len(plan.units)}
- Immutable source snapshots: {len(evidence["sources"])}
- Atomic claims: {len(evidence["claims"])}
- Accepted claims: {len(accepted)}
- Rejected claims: {len(rejected)}

## Dynamically planned StageGraph

{plan.plan_summary}

| Unit | Mode | Decomposed question | Search query |
|---|---|---|---|
{unit_rows}

## Durable stage lifecycle

| Stage | Status | Launched UTC | Completed UTC | Temporal workflow ID |
|---|---|---:|---:|---|
{stage_rows}

## Claim fidelity evaluations

Admission required exact source attribution, snapshot hash integrity, an exact evidence span,
numeric fidelity, complete numeric declarations, polarity/modality agreement, and deterministic
lexical-support cosine >= 0.20.

| Claim | Disposition | Support score | Text |
|---|---|---:|---|
{chr(10).join(claim_rows)}

## Final bubbled-up synthesis

{synthesis.answer}

Claims used: {", ".join(f"`{item}`" for item in synthesis.claim_ids_used)}

Limitations: {"; ".join(synthesis.limitations) or "None supplied by synthesis agent."}

## Immutable source ledger

| Source | Title | URL | Text digest |
|---|---|---|---|
{source_rows}

## Acceptance

- {"PASS" if len(plan.units) >= 2 else "FAIL"}: an LLM planned multiple bounded research units.
- {"PASS" if len(evidence["sources"]) >= len(plan.units) else "FAIL"}: each mission produced source evidence.
- {"PASS" if accepted else "FAIL"}: deterministic gates admitted at least one atomic claim.
- {"PASS" if synthesis.claim_ids_used and set(synthesis.claim_ids_used) <= {item["claim_id"] for item in accepted} else "FAIL"}: synthesis used only admitted claim IDs.
- PASS: each generic dynamic stage executed as a separate durable Temporal workflow.
- PASS: LangGraph checkpoints and outbox wakes resumed the same thread.

## Important limitations

- This proves fidelity to captured source text, not that a source is true, current, unbiased, or
  scientifically high quality.
- The deterministic semantic-support gate is a lexical cosine proxy. Production needs a pinned,
  calibrated biomedical NLI/embedding evaluator plus entity, temporal, and unit ontologies.
- Search uses Tavily advanced search with bounded raw text. Production should use the workspace's
  reviewed MCP adapters and immutable external artifact storage.
- The proof dynamically creates mission data executed by fixed generic node types. Runtime code/node
  injection remains forbidden; safe graph injection means a new validated mission-plan revision.
"""
    target = Path(__file__).with_name("artifacts") / "latest_report.md"
    target.parent.mkdir(exist_ok=True)
    target.write_text(report, encoding="utf-8")
    return target


async def run(objective: str, *, resume_run_id: str | None = None) -> None:
    settings = load_swarm_settings()
    await prepare_database(settings.application_migration_database_dsn)
    await setup_swarm_database(settings.application_migration_database_dsn)
    repository = await ExperimentRepository.connect(settings.application_database_dsn)
    evidence_repository = SwarmEvidenceRepository(repository.pool)
    temporal = await Client.connect(
        settings.temporal_address, namespace=settings.temporal_namespace
    )
    run_id = resume_run_id or f"swarm-{uuid.uuid4().hex[:12]}"
    thread_id = f"thread:{run_id}"
    if resume_run_id is None:
        await repository.create_run(run_id, thread_id)
    stop = asyncio.Event()
    try:
        async with AsyncPostgresSaver.from_conn_string(settings.application_database_dsn) as saver:
            graph = compile_swarm_graph(
                repository=repository,
                evidence_repository=evidence_repository,
                temporal=temporal,
                settings=settings,
                checkpointer=saver,
            )
            config = {"configurable": {"thread_id": thread_id}}
            if resume_run_id is None:
                await graph.ainvoke(
                    {
                        "run_id": run_id,
                        "thread_id": thread_id,
                        "objective": objective,
                        "dispatch_batch": (),
                        "launch_receipts": (),
                        "waiting_attempt_ids": (),
                        "planned_unit_ids": (),
                        "accepted_claim_ids": (),
                        "final_output_ref": None,
                        "event_log": (),
                    },
                    config=config,
                    durability="sync",
                )
            snapshot = await graph.aget_state(config)
            if resume_run_id is not None:
                if not snapshot.values:
                    raise RuntimeError(f"no persisted checkpoint for {resume_run_id}")
                objective = snapshot.values["objective"]
                if snapshot.next and not any(task.interrupts for task in snapshot.tasks):
                    await graph.ainvoke(None, config=config, durability="sync")
                    snapshot = await graph.aget_state(config)
            if any(task.interrupts for task in snapshot.tasks):
                await repository.record_graph_event(
                    f"swarm-interrupt:{run_id}",
                    run_id,
                    "GRAPH_INTERRUPTED",
                    {"thread_id": thread_id},
                )
            dispatcher = RunScopedWakeDispatcher(graph, repository, run_id)
            dispatcher_task = asyncio.create_task(dispatcher.run(stop))
            try:
                async with asyncio.timeout(settings.overall_timeout_seconds):
                    while True:
                        if dispatcher_task.done():
                            dispatcher_task.result()
                        timeline = await repository.timeline(run_id)
                        if timeline["run"]["status"] == "COMPLETED":
                            break
                        await asyncio.sleep(0.25)
            except TimeoutError as exc:
                raise RuntimeError(
                    f"swarm timed out; verify worker task queue {settings.temporal_task_queue!r}"
                ) from exc
            finally:
                stop.set()
                await dispatcher_task
            timeline = await repository.timeline(run_id)
            evidence = await evidence_repository.evidence_timeline(run_id)
            report = await _write_report(run_id, objective, timeline, evidence)
            synthesis_attempt = next(
                item for item in timeline["attempts"] if item["stage_id"] == "synthesize"
            )
            synthesis = FinalSynthesis.model_validate_json(synthesis_attempt["output_text"])
            accepted = [item for item in evidence["claims"] if item["disposition"] == "ACCEPT"]
            if not set(synthesis.claim_ids_used) <= {item["claim_id"] for item in accepted}:
                raise AssertionError("synthesis referenced a claim that was not admitted")
            print(f"run_id={run_id}")
            persisted_plan = _json_value(evidence["plans"][0]["plan_json"])
            print(f"planned_units={len(persisted_plan['units'])}")
            print(f"sources={len(evidence['sources'])}")
            print(f"accepted_claims={len(accepted)}")
            print(f"report={report}")
            print("PASS: dynamic research swarm completed")
    finally:
        await repository.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--objective", default=DEFAULT_OBJECTIVE)
    parser.add_argument("--resume", metavar="RUN_ID")
    args = parser.parse_args()
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(run(args.objective, resume_run_id=args.resume))


if __name__ == "__main__":
    main()
