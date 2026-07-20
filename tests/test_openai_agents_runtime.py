from __future__ import annotations

import pytest
from agents.mcp import MCPServerStreamableHttp
from agents.sandbox.capabilities import Filesystem, Shell

from app.domain.operation_execution.contracts import MCPServerBinding, ToolBinding
from app.integrations.openai_agents_runtime import OpenAIAgentsSandboxRuntime

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
    with pytest.raises(ValueError, match="does not support tool bindings"):
        OpenAIAgentsSandboxRuntime._sandbox_capabilities((tool("external.database.query"),))


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
