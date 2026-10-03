from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from .config import load_settings
from .temporal_activities import execute_deep_agent_stage, record_stage_completion
from .temporal_workflows import TemporalStageWorkflow


async def main() -> None:
    settings = load_settings()
    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[TemporalStageWorkflow],
        activities=[execute_deep_agent_stage, record_stage_completion],
    )
    print(f"Worker polling task queue {settings.temporal_task_queue!r}; Ctrl+C to stop")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
