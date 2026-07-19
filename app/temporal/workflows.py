from __future__ import annotations

from temporalio import workflow
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from agents import MaxTurnsExceeded, ModelSettings, RunConfig, Runner
    from agents.sandbox import Manifest, SandboxAgent, SandboxRunConfig
    from agents.sandbox.entries import File
    from agents.sandbox.sandboxes import DockerSandboxClientOptions
    from temporalio.contrib.openai_agents.workflow import temporal_sandbox_client


@workflow.defn
class SandboxAgentProbeWorkflow:
    """PRE-EMPTIVE SETUP: one minimal durable Agents SDK + Docker sandbox workflow."""

    @workflow.run
    async def run(self, prompt: str) -> str:
        agent = SandboxAgent(
            name="BiotechSandboxProbe",
            model="gpt-5.4-nano",
            instructions=(
                "Use the sandbox to read probe.txt. Return only its exact contents. "
                "Do not access the network or create additional files."
            ),
            default_manifest=Manifest(
                entries={"probe.txt": File(content=b"BELL-LABS-BIOTECH-SANDBOX-OK")}
            ),
            model_settings=ModelSettings(verbosity="low"),
        )
        try:
            result = await Runner.run(
                agent,
                prompt,
                max_turns=3,
                run_config=RunConfig(
                    sandbox=SandboxRunConfig(
                        client=temporal_sandbox_client("docker"),
                        options=DockerSandboxClientOptions(image="python:3.12-slim"),
                    )
                ),
            )
        except MaxTurnsExceeded:
            # Do not let Temporal retry a deliberately bounded model run forever.
            raise ApplicationError(
                "Sandbox agent exceeded its three-turn bootstrap budget",
                non_retryable=True,
            ) from None
        return str(result.final_output)
