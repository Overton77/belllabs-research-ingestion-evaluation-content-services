from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from .config import load_swarm_settings
from .temporal_activities import execute_swarm_stage, record_swarm_completion
from .temporal_workflows import SwarmStageWorkflow


async def main() -> None:
    settings = load_swarm_settings()
    client = await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[SwarmStageWorkflow],
        activities=[execute_swarm_stage, record_swarm_completion],
    )
    print(f"Dynamic swarm worker polling {settings.temporal_task_queue!r}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
