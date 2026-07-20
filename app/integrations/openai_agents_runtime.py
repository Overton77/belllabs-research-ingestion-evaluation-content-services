from __future__ import annotations

from collections.abc import Mapping

import docker
from agents import ModelSettings, RunConfig, Runner
from agents.sandbox import Manifest, SandboxAgent, SandboxRunConfig
from agents.sandbox.entries import File
from agents.sandbox.sandboxes import DockerSandboxClient, DockerSandboxClientOptions
from openai import AsyncOpenAI
from openai.types.shared.reasoning import Reasoning

from app.domain.operation_execution.contracts import (
    PromptTrustClass,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
)


class OpenAIAgentsSandboxRuntime:
    """Provider adapter; SDK and Docker types do not cross the runtime port."""

    def __init__(self, *, fixture_asset_contents: Mapping[str, bytes] | None = None) -> None:
        self._fixture_assets = dict(fixture_asset_contents or {})
        self._docker_client: DockerSandboxClient | None = None
        self._effects: dict[str, RuntimeResult] = {}

    async def execute(
        self,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
    ) -> RuntimeResult:
        binding = invocation.binding
        prior = self._effects.get(binding.side_effect_key)
        if prior is not None:
            return prior
        if binding.model_policy.provider != "openai":
            raise ValueError("OpenAI runtime received a non-OpenAI model policy")
        if binding.mcp_servers:
            raise ValueError("MCP runtime mapping is not enabled by this tracer adapter")
        if binding.tools:
            raise ValueError("tool runtime mapping is not enabled by this tracer adapter")
        api_key = self._openai_api_key(resolved_secrets)
        model = binding.model_policy.model
        openai_model = __import__(
            "agents.models.openai_responses", fromlist=["OpenAIResponsesModel"]
        ).OpenAIResponsesModel(model=model, openai_client=AsyncOpenAI(api_key=api_key))

        instruction_segments = [
            segment.content
            for segment in invocation.prompt_segments
            if segment.trust_class
            in {PromptTrustClass.SYSTEM_AUTHORITY, PromptTrustClass.AUTHORED_INSTRUCTION}
        ]
        user_input = "\n\n".join(
            f"[{segment.trust_class.value}]\n{segment.content}"
            for segment in invocation.prompt_segments
            if segment.trust_class
            not in {PromptTrustClass.SYSTEM_AUTHORITY, PromptTrustClass.AUTHORED_INSTRUCTION}
        )
        manifest_entries: dict[str, File] = {
            "binding.txt": File(content=binding.binding_id.encode())
        }
        for asset in (*binding.skills, *binding.plugins):
            content = self._fixture_assets.get(asset.manifest_digest)
            if content is None:
                raise ValueError("bound immutable fixture asset is unavailable")
            manifest_entries[asset.mount_path.lstrip("/")] = File(content=content)
            if asset.ref.kind.value == "skill":
                instruction_segments.append(
                    f"Bound immutable skill {asset.ref.logical_id} "
                    f"({asset.manifest_digest}):\n{content.decode('utf-8')}"
                )
        instructions = "\n\n".join(instruction_segments)

        agent = SandboxAgent(
            name=f"Operation-{binding.operation_id}",
            model=openai_model,
            instructions=instructions,
            default_manifest=Manifest(entries=manifest_entries),
            model_settings=ModelSettings(
                reasoning=(
                    Reasoning(effort=binding.model_policy.reasoning_effort)
                    if binding.model_policy.reasoning_effort is not None
                    else None
                ),
                verbosity=binding.model_policy.verbosity,
                include_usage=True,
            ),
        )
        if self._docker_client is None:
            self._docker_client = DockerSandboxClient(docker.from_env())
        result = await Runner.run(
            agent,
            user_input,
            max_turns=binding.model_policy.max_turns,
            run_config=RunConfig(
                tracing_disabled=True,
                trace_include_sensitive_data=False,
                workflow_name="BellLabs governed operation",
                group_id=binding.run_id,
                sandbox=SandboxRunConfig(
                    client=self._docker_client,
                    options=DockerSandboxClientOptions(
                        image=binding.workspace.image_digest
                    ),
                ),
            ),
        )
        usage = result.context_wrapper.usage
        amounts = {
            "tokens.input": usage.input_tokens,
            "tokens.output": usage.output_tokens,
            "tokens.total": usage.total_tokens,
            "model.turns": len(result.raw_responses),
        }
        runtime_result = RuntimeResult(
            output_text=str(result.final_output),
            usage=RuntimeUsage(amounts=amounts),
            provider_run_id=result.last_response_id,
            event_payloads=(
                {
                    "kind": "openai_agents.operation_completed",
                    "model": model,
                    "sandbox_workspace_id": invocation.workspace.workspace_id,
                },
            ),
        )
        self._effects[binding.side_effect_key] = runtime_result
        return runtime_result

    @staticmethod
    def _openai_api_key(resolved_secrets: Mapping[str, str]) -> str:
        matches = [
            value
            for key, value in resolved_secrets.items()
            if key.endswith(":OPENAI_API_KEY") or key.endswith(":openai_api_key")
        ]
        if len(matches) != 1:
            raise ValueError("exactly one OpenAI API key reference must resolve")
        return matches[0]
