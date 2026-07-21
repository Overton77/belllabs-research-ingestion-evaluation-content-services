from __future__ import annotations

import pytest
from agents import Agent, ImageGenerationTool, WebSearchTool
from agents.mcp import MCPServerStreamableHttp
from agents.sandbox.capabilities import Filesystem, Shell

from app.application.operation_execution import _binding_for
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import MCPServerBinding, ToolBinding
from app.integrations.openai_agents_runtime import (
    OpenAIAgentsSandboxRuntime,
    _ProjectRunHooks,
)
from tests.test_operation_execution import operation_request

DIGEST = "sha256:" + "a" * 64


def tool(tool_id: str) -> ToolBinding:
    return ToolBinding(
        tool_id=tool_id,
        revision=1,
        schema_digest=DIGEST,
        approval_policy="never",
    )


def test_sandbox_tool_bindings_enable_native_filesystem_apply_patch() -> None:
    capabilities = OpenAIAgentsSandboxRuntime._sandbox_capabilities(
        (
            tool("sandbox.filesystem"),
            tool("sandbox.shell"),
        )
    )

    assert [type(capability) for capability in capabilities] == [Filesystem, Shell]


def test_sandbox_tool_bindings_reject_unmapped_tools() -> None:
    runtime = OpenAIAgentsSandboxRuntime()
    with pytest.raises(ValueError, match="no exact runtime tool implementation"):
        runtime._agent_tools((tool("external.database.query"),))


def test_openai_hosted_tools_are_mapped_from_exact_configuration() -> None:
    runtime = OpenAIAgentsSandboxRuntime()
    tools = runtime._agent_tools(
        (
            tool("openai.web_search").model_copy(
                update={"configuration": {"search_context_size": "high"}}
            ),
            tool("openai.image_generation").model_copy(
                update={"configuration": {"quality": "high", "size": "1024x1024"}}
            ),
        )
    )

    assert isinstance(tools[0], WebSearchTool)
    assert tools[0].search_context_size == "high"
    assert isinstance(tools[1], ImageGenerationTool)
    assert tools[1].tool_config["quality"] == "high"


def test_streamable_http_mcp_binding_is_allowlisted() -> None:
    server = OpenAIAgentsSandboxRuntime._mcp_server(
        MCPServerBinding(
            server_id="tavily",
            revision=1,
            transport="streamable_http",
            endpoint_ref="https://example.test/mcp",
            allowed_tools=frozenset({"search"}),
            schema_digest=DIGEST,
            approval_policy="never",
        ),
        network_policy="allowlisted",
        network_hosts=frozenset({"example.test"}),
    )

    assert isinstance(server, MCPServerStreamableHttp)
    assert server.name == "tavily"
    assert server.tool_filter == {"allowed_tool_names": ["search"]}


@pytest.mark.asyncio
async def test_runtime_hooks_continue_durable_sequence_after_recovery() -> None:
    request = operation_request()
    binding = _binding_for(
        request,
        sha256_digest(request.model_dump(mode="json", exclude={"requested_at"})),
    )

    class Sink:
        def __init__(self) -> None:
            self.envelopes = []

        async def latest_sequence(self, _scope: str, _binding_id: str) -> int:
            return 7

        async def publish(self, _scope: str, envelope) -> None:  # type: ignore[no-untyped-def]
            self.envelopes.append(envelope)

        async def publish_ephemeral(self, **_kwargs) -> None:  # type: ignore[no-untyped-def]
            return None

    sink = Sink()
    hooks = _ProjectRunHooks(binding, sink)

    await hooks.on_agent_start(object(), Agent(name="test"))

    assert sink.envelopes[0].sequence == 8
    assert sink.envelopes[0].request_scope == binding.request_scope
