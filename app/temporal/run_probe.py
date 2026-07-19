from __future__ import annotations

import asyncio
from uuid import uuid4

from temporalio.contrib.openai_agents import OpenAIAgentsPlugin

from app.config import get_settings
from app.integrations.temporal import create_temporal_client
from app.temporal.workflows import SandboxAgentProbeWorkflow


async def main() -> None:
    settings = get_settings()
    client = await create_temporal_client(settings, plugins=[OpenAIAgentsPlugin()])
    result = await client.execute_workflow(
        SandboxAgentProbeWorkflow.run,
        "Read probe.txt and return its contents.",
        id=f"sandbox-agent-probe-{uuid4()}",
        task_queue=settings.temporal_task_queue,
    )
    expected = "BELL-LABS-BIOTECH-SANDBOX-OK"
    if str(result).strip() != expected:
        raise RuntimeError("Sandbox probe returned an unexpected value")
    print(expected)


if __name__ == "__main__":
    asyncio.run(main())
