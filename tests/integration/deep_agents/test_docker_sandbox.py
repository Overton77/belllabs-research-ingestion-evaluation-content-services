from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore

from app.domain.operation_execution.contracts import (
    WorkspaceMount,
    workspace_durable_reference,
)
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    DockerSandboxFactory,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
)
from tests.acceptance.control_plane.test_wp_cp_040 import (
    exact_fixture,
    runtime_invocation,
)


class SandboxedFileModel(BaseChatModel):
    calls: int = 0
    workspace_path: str

    @property
    def _llm_type(self) -> str:
        return "docker-sandbox-scripted"

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        self.calls += 1
        tool_result = next(
            (message for message in reversed(messages) if isinstance(message, ToolMessage)),
            None,
        )
        if tool_result is None:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "execute",
                        "args": {
                            "command": (
                                f"mkdir -p {self.workspace_path} && "
                                "printf 'sandboxed-deep-agent' > "
                                f"{self.workspace_path}/result.txt && "
                                f"cat {self.workspace_path}/result.txt"
                            )
                        },
                        "id": "docker-execute-proof",
                        "type": "tool_call",
                    }
                ],
            )
        else:
            message = AIMessage(content=f"observed:{tool_result.content}")
        return ChatResult(generations=[ChatGeneration(message=message)])


@pytest.mark.asyncio
async def test_real_deep_agent_executes_inside_ephemeral_docker_sandbox(
    tmp_path: Path,
) -> None:
    binding, _profile, bundle = exact_fixture(sandbox_backend="docker")
    model = SandboxedFileModel(
        workspace_path=binding.workspace.exclusive_write_paths[0]
    )
    registry = ExactComponentRegistry(
        model_factories={binding.model.ref.digest: lambda _binding, _secrets: model},
        skill_bundles={bundle.bundle_digest: bundle},
        sandbox_factories={
            binding.sandbox.ref.digest: DockerSandboxFactory(
                workspace_root=tmp_path / "workspaces"
            )
        },
        checkpointers={binding.checkpointer_ref.digest: InMemorySaver()},
        stores={binding.store_ref.digest: InMemoryStore()},
    )
    result = await DeepAgentRuntimeAdapter(
        ExactDeepAgentMaterializer(registry)
    ).execute(runtime_invocation(binding), {})

    assert "sandboxed-deep-agent" in result.output_text
    inspection = result.event_payloads[0]
    assert inspection["sandbox_execute_called"] is True
    assert inspection["authority_enforcement"] == (
        "immutable_host_binding_executable_sandbox"
    )
    assert model.calls == 2


@pytest.mark.asyncio
async def test_docker_sandbox_enforces_isolation_and_persists_bound_workspace(
    tmp_path: Path,
) -> None:
    binding, _profile, _bundle = exact_fixture(sandbox_backend="docker")
    factory = DockerSandboxFactory(workspace_root=tmp_path / "workspaces")
    workspace = binding.workspace.exclusive_write_paths[0]

    async with factory(binding, {}) as first:
        container_id = first.id
        assert first.execute("touch /outside-workspace").exit_code != 0
        assert (
            first.execute(
                "python3 -c \"import socket; "
                "socket.create_connection(('1.1.1.1', 53), 1)\""
            ).exit_code
            != 0
        )
        assert first.execute(f"printf persistent > {workspace}/durable.txt").exit_code == 0

    async with factory(binding, {}) as replacement:
        assert replacement.id != container_id
        read = replacement.execute(f"cat {workspace}/durable.txt")
        assert read.exit_code == 0
        assert read.output == "persistent"


@pytest.mark.asyncio
async def test_docker_sandbox_mounts_prior_workspace_read_only(
    tmp_path: Path,
) -> None:
    binding, _profile, _bundle = exact_fixture(sandbox_backend="docker")
    factory = DockerSandboxFactory(workspace_root=tmp_path / "workspaces")
    writable = binding.workspace.exclusive_write_paths[0]
    async with factory(binding, {}) as executor:
        assert executor.execute(f"printf immutable > {writable}/artifact.txt").exit_code == 0

    verifier_workspace = binding.workspace.model_copy(
        update={
            "workspace_id": f"{binding.workspace.workspace_id}:verifier",
            "exclusive_write_paths": ("/verifier/work",),
            "read_mounts": (
                WorkspaceMount(
                    logical_path="/verifier/input",
                    durable_ref=workspace_durable_reference(
                        binding.workspace.namespace_id,
                        binding.workspace.workspace_id,
                    ),
                    content_digest="sha256:" + "a" * 64,
                ),
            ),
        }
    )
    verifier_binding = binding.model_copy(update={"workspace": verifier_workspace})
    async with factory(verifier_binding, {}) as verifier:
        read = verifier.execute("cat /verifier/input/artifact.txt")
        assert read.exit_code == 0
        assert read.output == "immutable"
        assert verifier.execute("printf changed > /verifier/input/artifact.txt").exit_code != 0
