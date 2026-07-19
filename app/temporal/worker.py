from __future__ import annotations

import asyncio
import os
from datetime import timedelta

import docker
from agents.sandbox.sandboxes import DockerSandboxClient
from temporalio.contrib.openai_agents import (
    ModelActivityParameters,
    OpenAIAgentsPlugin,
    SandboxClientProvider,
)
from temporalio.worker import Worker

from app.config import get_settings
from app.integrations.temporal import create_temporal_client
from app.temporal.workflows import SandboxAgentProbeWorkflow


async def main() -> None:
    settings = get_settings()
    # PRE-EMPTIVE SETUP: Pydantic reads .env without mutating process env, while
    # the Agents SDK reads OPENAI_API_KEY from the process environment.
    os.environ.setdefault("OPENAI_API_KEY", settings.openai_api_key.get_secret_value())
    docker_client = DockerSandboxClient(docker.from_env())
    plugin = OpenAIAgentsPlugin(
        model_params=ModelActivityParameters(start_to_close_timeout=timedelta(seconds=60)),
        sandbox_clients=[SandboxClientProvider("docker", docker_client)],
    )
    client = await create_temporal_client(settings, plugins=[plugin])
    worker = Worker(
        client,
        task_queue=settings.temporal_task_queue,
        workflows=[SandboxAgentProbeWorkflow],
    )
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
