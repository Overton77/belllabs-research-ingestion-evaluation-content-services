from __future__ import annotations

import asyncio

from deepagents import create_deep_agent
from temporalio import activity

from .config import load_settings
from .contracts import CompletionRecord, TemporalStageInput, TemporalStageResult, digest_text
from .repository import ExperimentRepository

_worker_repository: ExperimentRepository | None = None


async def get_worker_repository() -> ExperimentRepository:
    global _worker_repository
    if _worker_repository is None:
        settings = load_settings()
        _worker_repository = await ExperimentRepository.connect(settings.application_database_dsn)
    return _worker_repository


@activity.defn
async def execute_deep_agent_stage(request: TemporalStageInput) -> TemporalStageResult:
    activity.heartbeat("activity-started")
    remaining = request.delay_seconds
    while remaining > 0:
        interval = min(1.0, remaining)
        await asyncio.sleep(interval)
        remaining -= interval
        activity.heartbeat({"remaining_delay_seconds": max(0.0, remaining)})

    agent = create_deep_agent(
        model=request.model,
        tools=[],
        system_prompt=(
            "You are a concise biotechnology research assistant in an architecture experiment. "
            "Return a short, factual response, clearly label uncertainty, and do not give "
            "medical advice."
        ),
        name=f"experiment_{request.stage_id}",
    )
    metadata = {
        "run_id": request.run_id,
        "attempt_id": request.attempt_id,
        "stage_id": request.stage_id,
        "temporal_workflow_id": activity.info().workflow_id,
        **request.trace_headers,
    }
    result = await agent.ainvoke(
        {"messages": [{"role": "user", "content": request.prompt}]},
        config={
            "tags": ["stagegraph-temporal-experiment", request.stage_id],
            "metadata": metadata,
        },
    )
    output_text = str(result["messages"][-1].content)
    activity.heartbeat("activity-completed")
    return TemporalStageResult(
        attempt_id=request.attempt_id,
        stage_id=request.stage_id,
        output_text=output_text,
        output_digest=digest_text(output_text),
    )


@activity.defn
async def record_stage_completion(completion: CompletionRecord) -> None:
    repository = await get_worker_repository()
    await repository.record_completion_and_wake(completion)
