from __future__ import annotations

from collections.abc import Mapping
from hashlib import sha256
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import urlparse

import docker
from agents import ModelSettings, RunConfig, Runner, ToolCallItem
from agents.mcp import MCPServerSse, MCPServerStreamableHttp, create_static_tool_filter
from agents.sandbox import Manifest, SandboxAgent, SandboxRunConfig
from agents.sandbox.capabilities import (
    Capability,
    Filesystem,
    FilesystemToolSet,
    Shell,
    ShellToolSet,
)
from agents.sandbox.entries import Dir, File
from agents.sandbox.manifest import Environment
from agents.sandbox.sandboxes import DockerSandboxClient, DockerSandboxClientOptions
from agents.sandbox.types import Permissions
from openai import AsyncOpenAI
from openai.types.shared.reasoning import Reasoning

from app.domain.operation_execution.contracts import (
    CapturedWorkspaceCandidate,
    MCPServerBinding,
    OperationExecutionBinding,
    PromptTrustClass,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
    ToolBinding,
)
from app.domain.operation_execution.materialization import (
    verify_workspace_manifest,
)

_FILESYSTEM_TOOL_ID = "sandbox.filesystem"
_SHELL_TOOL_ID = "sandbox.shell"


class WorkspaceCandidateSink(Protocol):
    async def capture(
        self,
        binding: OperationExecutionBinding,
        logical_path: str,
        content: bytes,
    ) -> CapturedWorkspaceCandidate: ...


class OpenAIAgentsSandboxRuntime:
    """Provider adapter; SDK and Docker types do not cross the runtime port."""

    def __init__(
        self,
        *,
        fixture_asset_contents: Mapping[str, bytes] | None = None,
        required_sandbox_tools: frozenset[str] = frozenset(),
        required_sandbox_tool_counts: Mapping[str, int] | None = None,
        required_artifact_paths: tuple[str, ...] = (),
        candidate_sink: WorkspaceCandidateSink | None = None,
    ) -> None:
        self._fixture_assets = dict(fixture_asset_contents or {})
        self._required_sandbox_tools = required_sandbox_tools
        self._required_sandbox_tool_counts = dict(required_sandbox_tool_counts or {})
        self._required_artifact_paths = required_artifact_paths
        self._candidate_sink = candidate_sink
        self.artifacts: dict[str, bytes] = {}
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
        sandbox_environment = {
            key.split(":", maxsplit=1)[1]: value
            for key, value in resolved_secrets.items()
            if key.endswith(":TAVILY_API_KEY")
        }
        manifest_entries: dict[str, File | Dir] = {
            "binding.txt": File(
                content=binding.binding_id.encode(),
                permissions=Permissions(owner=4),
            )
        }
        materialization = invocation.workspace.materialization_manifest
        if binding.workspace.slot_bindings:
            if materialization is None:
                raise ValueError(
                    "compiled workspace slots require a verified materialization manifest"
                )
            verify_workspace_manifest(materialization)
            if (
                materialization.namespace_id != binding.workspace.namespace_id
                or materialization.workspace_id != binding.workspace.workspace_id
                or materialization.slots != binding.workspace.slot_bindings
                or materialization.manifest_digest != invocation.workspace.mount_manifest_digest
                or materialization.revision != invocation.workspace.manifest_revision
            ):
                raise ValueError("runtime workspace does not match the verified materialization")
            workspace_slots = materialization.slots
        else:
            workspace_slots = ()
        for slot in workspace_slots:
            path = slot.logical_path.lstrip("/")
            if slot.access == "exclusive_write":
                manifest_entries[path] = Dir(permissions=Permissions(owner=7))
                continue
            content = self._fixture_assets.get(slot.content_digest or "")
            if content is None:
                raise ValueError(
                    f"bound read-only workspace input is unavailable: {slot.logical_path}"
                )
            if f"sha256:{sha256(content).hexdigest()}" != slot.content_digest:
                raise ValueError(
                    f"bound read-only workspace input digest mismatch: {slot.logical_path}"
                )
            manifest_entries[path] = File(
                content=content,
                permissions=Permissions(owner=4),
            )
        for mount in binding.workspace.read_mounts:
            content = self._fixture_assets.get(mount.content_digest)
            if content is None:
                raise ValueError(
                    f"bound read-only workspace mount is unavailable: {mount.logical_path}"
                )
            if f"sha256:{sha256(content).hexdigest()}" != mount.content_digest:
                raise ValueError(
                    f"bound read-only workspace mount digest mismatch: {mount.logical_path}"
                )
            manifest_entries[mount.logical_path.lstrip("/")] = File(
                content=content,
                permissions=Permissions(owner=4),
            )
        for asset in (*binding.skills, *binding.plugins):
            content = self._fixture_assets.get(asset.manifest_digest)
            if content is None:
                raise ValueError("bound immutable fixture asset is unavailable")
            if f"sha256:{sha256(content).hexdigest()}" != asset.manifest_digest:
                raise ValueError("bound immutable fixture asset digest mismatch")
            manifest_entries[asset.mount_path.lstrip("/")] = File(
                content=content,
                permissions=Permissions(owner=4),
            )
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
            default_manifest=Manifest(
                entries=manifest_entries,
                environment=Environment(value=sandbox_environment),
            ),
            capabilities=self._sandbox_capabilities(binding.tools),
            mcp_servers=[
                self._mcp_server(
                    server,
                    network_policy=binding.workspace.network_policy,
                    network_hosts=binding.capability_grant.network_hosts,
                )
                for server in binding.mcp_servers
            ],
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
                    options=DockerSandboxClientOptions(image=binding.workspace.image_digest),
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
        sandbox_tools = tuple(
            sorted(
                {
                    "apply_patch"
                    for response in result.raw_responses
                    for item in response.output
                    if getattr(item, "type", None) == "apply_patch_call"
                }
                | {
                    item.tool_name
                    for item in result.new_items
                    if isinstance(item, ToolCallItem) and item.tool_name is not None
                }
            )
        )
        sandbox_tool_counts: dict[str, int] = {}
        for response in result.raw_responses:
            for raw_item in response.output:
                if getattr(raw_item, "type", None) == "apply_patch_call":
                    sandbox_tool_counts["apply_patch"] = (
                        sandbox_tool_counts.get("apply_patch", 0) + 1
                    )
        for run_item in result.new_items:
            if isinstance(run_item, ToolCallItem) and run_item.tool_name is not None:
                sandbox_tool_counts[run_item.tool_name] = (
                    sandbox_tool_counts.get(run_item.tool_name, 0) + 1
                )
        missing_required_tools = self._required_sandbox_tools - set(sandbox_tools)
        if missing_required_tools:
            raise RuntimeError(
                "sandbox run did not invoke required tools: "
                + ", ".join(sorted(missing_required_tools))
            )
        insufficient_tools = {
            tool: (sandbox_tool_counts.get(tool, 0), minimum)
            for tool, minimum in self._required_sandbox_tool_counts.items()
            if sandbox_tool_counts.get(tool, 0) < minimum
        }
        if insufficient_tools:
            raise RuntimeError(
                "sandbox tool invocation counts were below required minimums: "
                + ", ".join(
                    f"{tool}={actual}<{minimum}"
                    for tool, (actual, minimum) in sorted(insufficient_tools.items())
                )
            )
        self.artifacts = await self._collect_required_artifacts(result)
        candidate_refs: tuple[str, ...] = ()
        if self._candidate_sink is not None:
            candidates = [
                await self._candidate_sink.capture(binding, path, content)
                for path, content in self.artifacts.items()
            ]
            candidate_refs = tuple(candidate.candidate_id for candidate in candidates)
        runtime_result = RuntimeResult(
            output_text=str(result.final_output),
            output_refs=candidate_refs,
            usage=RuntimeUsage(amounts=amounts),
            provider_run_id=result.last_response_id,
            event_payloads=(
                {
                    "kind": "openai_agents.operation_completed",
                    "model": model,
                    "sandbox_workspace_id": invocation.workspace.workspace_id,
                    "sandbox_tools": sandbox_tools,
                    "sandbox_tool_counts": sandbox_tool_counts,
                    "sandbox_item_types": tuple(type(item).__name__ for item in result.new_items),
                },
            ),
        )
        self._effects[binding.side_effect_key] = runtime_result
        return runtime_result

    async def _collect_required_artifacts(self, result: object) -> dict[str, bytes]:
        if not self._required_artifact_paths:
            return {}
        session = getattr(result, "_sandbox_session", None)
        if session is None:
            raise RuntimeError("sandbox session is unavailable for required artifact collection")
        artifacts: dict[str, bytes] = {}
        for artifact_path in self._required_artifact_paths:
            file = await session.read(Path(artifact_path))
            try:
                artifacts[artifact_path] = file.read()
            finally:
                file.close()
        return artifacts

    @staticmethod
    def _sandbox_capabilities(tools: tuple[ToolBinding, ...]) -> list[Capability]:
        """Map immutable tool bindings to the SDK's sandbox-native capabilities."""
        if not tools:
            raise ValueError("OpenAI sandbox runtime requires explicit tool bindings")

        requested = {tool.tool_id: tool for tool in tools}
        unsupported = set(requested) - {_FILESYSTEM_TOOL_ID, _SHELL_TOOL_ID}
        if unsupported:
            raise ValueError(
                "OpenAI sandbox runtime does not support tool bindings: "
                + ", ".join(sorted(unsupported))
            )

        capabilities: list[Capability] = []
        if filesystem := requested.get(_FILESYSTEM_TOOL_ID):
            requires_approval = OpenAIAgentsSandboxRuntime._requires_approval(filesystem)

            def configure_filesystem(
                toolset: FilesystemToolSet, *, approval: bool = requires_approval
            ) -> None:
                toolset.apply_patch.needs_approval = approval
                toolset.view_image.needs_approval = approval

            capabilities.append(Filesystem(configure_tools=configure_filesystem))
        if shell := requested.get(_SHELL_TOOL_ID):
            requires_approval = OpenAIAgentsSandboxRuntime._requires_approval(shell)

            def configure_shell(
                toolset: ShellToolSet, *, approval: bool = requires_approval
            ) -> None:
                toolset.exec_command.needs_approval = approval
                if toolset.write_stdin is not None:
                    toolset.write_stdin.needs_approval = approval

            capabilities.append(Shell(configure_tools=configure_shell))
        return capabilities

    @staticmethod
    def _requires_approval(binding: ToolBinding) -> bool:
        if binding.approval_policy == "policy":
            raise ValueError(f"tool binding {binding.tool_id} has unresolved approval policy")
        return binding.approval_policy == "always"

    @staticmethod
    def _mcp_server(
        binding: MCPServerBinding,
        *,
        network_policy: str,
        network_hosts: frozenset[str],
    ) -> MCPServerSse | MCPServerStreamableHttp:
        endpoint = urlparse(binding.endpoint_ref)
        if endpoint.scheme not in {"http", "https"} or not endpoint.netloc:
            raise ValueError(
                f"MCP server {binding.server_id} requires an absolute HTTP(S) endpoint_ref"
            )
        if network_policy != "allowlisted" or endpoint.hostname not in network_hosts:
            raise ValueError(
                f"MCP server {binding.server_id} endpoint is not allowed by the workspace grant"
            )

        approval: Literal["always", "never"] = (
            "never" if binding.approval_policy == "never" else "always"
        )
        tool_filter = create_static_tool_filter(allowed_tool_names=sorted(binding.allowed_tools))
        if binding.transport == "sse":
            return MCPServerSse(
                params={"url": binding.endpoint_ref},
                name=binding.server_id,
                tool_filter=tool_filter,
                require_approval=approval,
                client_session_timeout_seconds=binding.timeout_seconds,
                max_retry_attempts=binding.max_retries,
            )
        if binding.transport == "streamable_http":
            return MCPServerStreamableHttp(
                params={"url": binding.endpoint_ref},
                name=binding.server_id,
                tool_filter=tool_filter,
                require_approval=approval,
                client_session_timeout_seconds=binding.timeout_seconds,
                max_retry_attempts=binding.max_retries,
            )
        raise ValueError(
            f"OpenAI sandbox runtime does not support stdio MCP server {binding.server_id}"
        )

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
