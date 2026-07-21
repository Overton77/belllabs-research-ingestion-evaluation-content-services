from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, Protocol, cast
from urllib.parse import urlparse
from uuid import NAMESPACE_URL, uuid5

import docker
from agents import (
    Agent,
    Handoff,
    ImageGenerationTool,
    ModelSettings,
    RunConfig,
    RunHooks,
    Runner,
    RunResultStreaming,
    RunState,
    ToolCallItem,
    ToolExecutionConfig,
    WebSearchTool,
)
from agents.items import TResponseInputItem
from agents.mcp import MCPServerSse, MCPServerStreamableHttp, create_static_tool_filter
from agents.memory import Session
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
from agents.tool import Tool
from openai import AsyncOpenAI
from openai.types.shared.reasoning import Reasoning

from app.domain.operation_execution.contracts import (
    AgentDefinition,
    CapturedWorkspaceCandidate,
    DelegationBinding,
    GuardrailBinding,
    MCPServerBinding,
    OperationExecutionBinding,
    PromptTrustClass,
    RuntimeApprovalRequest,
    RuntimeEventEnvelope,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
    StructuredOutputBinding,
    ToolBinding,
)
from app.domain.operation_execution.delegation import LinkedRunRequired, admit_delegation
from app.domain.operation_execution.errors import UnsupportedRuntimePolicy
from app.domain.operation_execution.materialization import (
    verify_workspace_manifest,
)
from app.integrations.openai_sandbox_snapshots import OpenAIAgentsSnapshotBridge

_FILESYSTEM_TOOL_ID = "sandbox.filesystem"
_SHELL_TOOL_ID = "sandbox.shell"
_WEB_SEARCH_TOOL_ID = "openai.web_search"
_IMAGE_GENERATION_TOOL_ID = "openai.image_generation"


class RuntimeEventSink(Protocol):
    async def latest_sequence(self, request_scope: str, binding_id: str) -> int: ...

    async def publish(self, request_scope: str, envelope: RuntimeEventEnvelope) -> None: ...

    async def publish_ephemeral(
        self, *, request_scope: str, run_id: str, payload: dict[str, object]
    ) -> None: ...


class RuntimeApprovalGateway(Protocol):
    async def request(self, request: RuntimeApprovalRequest) -> object: ...

    async def save_checkpoint(
        self, *, request_scope: str, binding_id: str, state_json: str, status: str
    ) -> None: ...

    async def load_checkpoint(self, request_scope: str, binding_id: str) -> str | None: ...

    async def complete_checkpoint(self, request_scope: str, binding_id: str) -> None: ...


class RuntimeSessionFactory(Protocol):
    def __call__(self, binding: OperationExecutionBinding, session_id: str) -> Session: ...


class OpenAIAgentsComponentRegistry:
    """Exact project-owned implementations for non-hosted SDK components."""

    def __init__(
        self,
        *,
        tools: Mapping[str, Tool | Callable[[ToolBinding], Tool]] | None = None,
        output_types: Mapping[str, type[Any]] | None = None,
        input_guardrails: Mapping[str, object] | None = None,
        output_guardrails: Mapping[str, object] | None = None,
    ) -> None:
        self.tools = dict(tools or {})
        self.output_types = dict(output_types or {})
        self.input_guardrails = dict(input_guardrails or {})
        self.output_guardrails = dict(output_guardrails or {})

    def tool(self, binding: ToolBinding) -> Tool:
        key = f"{binding.tool_id}@{binding.revision}:{binding.schema_digest}"
        registered = self.tools.get(key)
        if registered is None:
            raise ValueError(f"no exact runtime tool implementation: {key}")
        return registered(binding) if callable(registered) else registered

    def output_type(self, binding: StructuredOutputBinding | None) -> type[Any] | None:
        if binding is None:
            return None
        key = f"{binding.schema_id}@{binding.revision}:{binding.schema_digest}"
        value = self.output_types.get(key)
        if value is None:
            raise ValueError(f"no exact structured output implementation: {key}")
        return value

    def guardrails(
        self, bindings: tuple[GuardrailBinding, ...]
    ) -> tuple[list[object], list[object]]:
        inputs: list[object] = []
        outputs: list[object] = []
        for binding in bindings:
            registry = self.input_guardrails if binding.stage == "input" else self.output_guardrails
            key = f"{binding.guardrail_id}@{binding.revision}:{binding.implementation_digest}"
            guardrail = registry.get(key)
            if guardrail is None:
                raise ValueError(f"no exact {binding.stage} guardrail implementation: {key}")
            (inputs if binding.stage == "input" else outputs).append(guardrail)
        return inputs, outputs


class WorkspaceCandidateSink(Protocol):
    async def capture(
        self,
        binding: OperationExecutionBinding,
        logical_path: str,
        content: bytes,
    ) -> CapturedWorkspaceCandidate: ...


class _ProjectRunHooks(RunHooks[Any]):
    def __init__(
        self,
        binding: OperationExecutionBinding,
        sink: RuntimeEventSink | None,
    ) -> None:
        self._binding = binding
        self._sink = sink
        self._sequence = 0
        self._sequence_initialized = False
        self._sequence_lock = asyncio.Lock()
        self.events: list[dict[str, object]] = []

    async def emit(self, event_type: str, payload: dict[str, object]) -> None:
        async with self._sequence_lock:
            if not self._sequence_initialized:
                if self._sink is not None:
                    self._sequence = await self._sink.latest_sequence(
                        self._binding.request_scope,
                        self._binding.binding_id,
                    )
                self._sequence_initialized = True
            self._sequence += 1
            sequence = self._sequence
        envelope = RuntimeEventEnvelope(
            event_id=str(
                uuid5(
                    NAMESPACE_URL,
                    f"{self._binding.request_scope}:{self._binding.side_effect_key}:"
                    f"runtime-event:{sequence}",
                )
            ),
            event_type=event_type,
            request_scope=self._binding.request_scope,
            binding_id=self._binding.binding_id,
            run_id=self._binding.run_id,
            operation_id=self._binding.operation_id,
            sequence=sequence,
            occurred_at=datetime.now(UTC),
            payload=payload,
        )
        serialized = envelope.model_dump(mode="json")
        self.events.append(serialized)
        if self._sink is not None:
            await self._sink.publish(self._binding.request_scope, envelope)

    async def on_agent_start(self, _context: object, agent: Agent[Any]) -> None:
        await self.emit("openai_agents.agent_started", {"agent": agent.name})

    async def on_agent_end(self, _context: object, agent: Agent[Any], _output: Any) -> None:
        await self.emit("openai_agents.agent_completed", {"agent": agent.name})

    async def on_handoff(
        self, _context: object, from_agent: Agent[Any], to_agent: Agent[Any]
    ) -> None:
        await self.emit(
            "openai_agents.handoff",
            {"from_agent": from_agent.name, "to_agent": to_agent.name},
        )

    async def on_llm_start(
        self,
        _context: object,
        agent: Agent[Any],
        _system_prompt: str | None,
        _input_items: list[TResponseInputItem],
    ) -> None:
        await self.emit("openai_agents.llm_started", {"agent": agent.name})

    async def on_llm_end(self, _context: object, agent: Agent[Any], response: object) -> None:
        usage = getattr(response, "usage", None)
        await self.emit(
            "openai_agents.llm_completed",
            {
                "agent": agent.name,
                "response_id": getattr(response, "response_id", None),
                "usage_present": usage is not None,
            },
        )

    async def on_tool_start(self, _context: object, agent: Agent[Any], tool: Tool) -> None:
        await self.emit(
            "openai_agents.tool_started",
            {"agent": agent.name, "tool": getattr(tool, "name", type(tool).__name__)},
        )

    async def on_tool_end(
        self, _context: object, agent: Agent[Any], tool: Tool, result: object
    ) -> None:
        result_digest = sha256(str(result).encode()).hexdigest()
        await self.emit(
            "openai_agents.tool_completed",
            {
                "agent": agent.name,
                "tool": getattr(tool, "name", type(tool).__name__),
                "result_digest": f"sha256:{result_digest}",
                "is_agent_tool": bool(getattr(tool, "_is_agent_tool", False)),
            },
        )


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
        components: OpenAIAgentsComponentRegistry | None = None,
        event_sink: RuntimeEventSink | None = None,
        approval_gateway: RuntimeApprovalGateway | None = None,
        session_factory: RuntimeSessionFactory | None = None,
        snapshot_bridge: OpenAIAgentsSnapshotBridge | None = None,
        approval_timeout_seconds: int = 900,
    ) -> None:
        self._fixture_assets = dict(fixture_asset_contents or {})
        self._required_sandbox_tools = required_sandbox_tools
        self._required_sandbox_tool_counts = dict(required_sandbox_tool_counts or {})
        self._required_artifact_paths = required_artifact_paths
        self._candidate_sink = candidate_sink
        self._components = components or OpenAIAgentsComponentRegistry()
        self._event_sink = event_sink
        self._approval_gateway = approval_gateway
        self._session_factory = session_factory
        self._snapshot_bridge = snapshot_bridge
        self._approval_timeout_seconds = approval_timeout_seconds
        self.artifacts: dict[str, bytes] = {}
        self._docker_client: DockerSandboxClient | None = (
            snapshot_bridge.client if snapshot_bridge is not None else None
        )
        self._effects: dict[str, RuntimeResult] = {}

    async def aclose(self) -> None:
        if self._snapshot_bridge is not None:
            await self._snapshot_bridge.aclose()

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
        if binding.model_policy.fallback_models:
            raise UnsupportedRuntimePolicy(
                "OpenAI runtime does not support bound automatic fallback models"
            )
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
        manifest = Manifest(
            entries=manifest_entries,
            environment=Environment(value=sandbox_environment),
        )

        hooks = _ProjectRunHooks(binding, self._event_sink)
        input_guardrails, output_guardrails = self._components.guardrails(binding.guardrails)
        sdk_tools = self._agent_tools(binding.tools)
        handoffs: list[Agent[Any] | Handoff[Any, Any]] = []
        if self._docker_client is None:
            self._docker_client = DockerSandboxClient(docker.from_env())
        sandbox_options = DockerSandboxClientOptions(image=binding.workspace.image_digest)
        delegation_call_lock = asyncio.Lock()
        delegation_call_count = 0
        for delegation in binding.delegations:
            admission = admit_delegation(binding, delegation)
            if admission.outcome == "linked_run_required":
                raise LinkedRunRequired(admission)
            if admission.outcome != "accepted":
                raise ValueError(f"delegation rejected: {admission.reason_code}")
            if delegation.mode == "handoff" and any(
                tool.tool_id in {_FILESYSTEM_TOOL_ID, _SHELL_TOOL_ID}
                for tool in delegation.agent.tools
            ):
                raise UnsupportedRuntimePolicy(
                    "handoff sandbox tools are unsupported because the SDK cannot "
                    "enforce a child-private sandbox session"
                )
            delegate = self._delegate_agent(
                delegation,
                openai_client=AsyncOpenAI(api_key=api_key),
                fixture_assets=self._fixture_assets,
            )
            if delegation.mode == "handoff":
                handoffs.append(delegate)
            else:
                assert delegation.tool_name is not None
                assert delegation.tool_description is not None
                nested_session = (
                    self._session_factory(
                        binding,
                        f"{binding.session_id}:delegate:{delegation.agent.definition_id}",
                    )
                    if self._session_factory is not None and binding.session_id is not None
                    else None
                )

                async def on_nested_stream(
                    event: object,
                    *,
                    delegate_id: str = delegation.agent.definition_id,
                ) -> None:
                    stream_event = event["event"]  # type: ignore[index]
                    await hooks.emit(
                        "openai_agents.subagent_stream",
                        {
                            "delegate_id": delegate_id,
                            "stream_type": getattr(stream_event, "type", "unknown"),
                        },
                    )

                agent_tool = delegate.as_tool(
                    tool_name=delegation.tool_name,
                    tool_description=delegation.tool_description,
                    max_turns=min(
                        delegation.agent.model_policy.max_turns,
                        delegation.budget_limits.get(
                            "model.turns",
                            delegation.agent.model_policy.max_turns,
                        ),
                    ),
                    hooks=hooks,
                    session=nested_session,
                    needs_approval=delegation.needs_approval,
                    on_stream=on_nested_stream,
                    run_config=RunConfig(
                        trace_include_sensitive_data=False,
                        workflow_name="BellLabs bounded task subagent",
                        group_id=binding.run_id,
                        trace_metadata={
                            "binding_id": binding.binding_id,
                            "delegate_id": delegation.agent.definition_id,
                            "child_workspace_id": delegation.child_workspace_id,
                            "child_namespace_id": delegation.child_namespace_id,
                        },
                        sandbox=SandboxRunConfig(
                            client=self._docker_client,
                            options=sandbox_options,
                        ),
                        tool_execution=ToolExecutionConfig(max_function_tool_concurrency=1),
                    ),
                )
                invoke_delegate = agent_tool.on_invoke_tool

                async def bounded_invoke(
                    context: object,
                    arguments: str,
                    *,
                    invoke: Callable[[Any, str], Awaitable[Any]] = invoke_delegate,
                ) -> Any:
                    nonlocal delegation_call_count
                    async with delegation_call_lock:
                        if delegation_call_count >= binding.delegation_ceiling.max_delegations:
                            raise RuntimeError("operation delegation call ceiling was exhausted")
                        delegation_call_count += 1
                    return await invoke(context, arguments)

                agent_tool.on_invoke_tool = bounded_invoke
                sdk_tools.append(agent_tool)

        agent = SandboxAgent(
            name=f"Operation-{binding.operation_id}",
            model=openai_model,
            instructions=instructions,
            default_manifest=manifest,
            capabilities=self._sandbox_capabilities(binding.tools),
            tools=sdk_tools,
            mcp_servers=[
                self._mcp_server(
                    server,
                    network_policy=binding.workspace.network_policy,
                    network_hosts=binding.capability_grant.network_hosts,
                )
                for server in binding.mcp_servers
            ],
            handoffs=handoffs,
            input_guardrails=input_guardrails,  # type: ignore[arg-type]
            output_guardrails=output_guardrails,  # type: ignore[arg-type]
            output_type=self._components.output_type(binding.output_schema),
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
        # Own the sandbox session so required artifacts can be read after Runner.run and
        # immutable archives can be captured before provider cleanup.
        restored_session = (
            await self._snapshot_bridge.take_restored_session(invocation.workspace, binding)
            if self._snapshot_bridge is not None
            else None
        )
        if binding.workspace.restore_snapshot_id is not None and restored_session is None:
            raise ValueError(
                "bound snapshot restore has no admitted cloned sandbox session"
            )
        archive = (
            self._snapshot_bridge.begin_capture(binding, invocation.workspace)
            if self._snapshot_bridge is not None and restored_session is None
            else None
        )
        session = restored_session or await self._docker_client.create(
            manifest=manifest,
            options=sandbox_options,
            snapshot=archive,
        )
        try:
            if restored_session is not None:
                session.state = session.state.model_copy(update={"manifest": manifest})
                await session.start()
                await session.apply_manifest()
            sdk_session = (
                self._session_factory(binding, binding.session_id)
                if self._session_factory is not None and binding.session_id is not None
                else None
            )
            run_config = RunConfig(
                tracing_disabled=False,
                trace_include_sensitive_data=False,
                workflow_name="BellLabs governed operation",
                group_id=binding.run_id,
                trace_metadata={
                    "binding_id": binding.binding_id,
                    "operation_id": binding.operation_id,
                    "configuration_digest": binding.effective_configuration_digest,
                },
                sandbox=SandboxRunConfig(
                    client=self._docker_client,
                    options=sandbox_options,
                    session=session,
                    archive_limits=(
                        self._snapshot_bridge.archive_limits
                        if self._snapshot_bridge is not None
                        else None
                    ),
                ),
                tool_execution=ToolExecutionConfig(
                    max_function_tool_concurrency=binding.delegation_ceiling.max_concurrency or 1
                ),
            )
            run_input: object = user_input
            if self._approval_gateway is not None:
                checkpoint = await self._approval_gateway.load_checkpoint(
                    binding.request_scope, binding.binding_id
                )
                if checkpoint is not None:
                    run_input = RunState.from_string(agent, checkpoint)
            result = await self._run_with_streaming_and_approvals(
                agent=agent,
                run_input=run_input,
                binding=binding,
                hooks=hooks,
                run_config=run_config,
                sdk_session=sdk_session,
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
            self.artifacts = await self._collect_required_artifacts(session)
            candidate_refs: tuple[str, ...] = ()
            if self._candidate_sink is not None:
                candidates = [
                    await self._candidate_sink.capture(binding, path, content)
                    for path, content in self.artifacts.items()
                ]
                candidate_refs = tuple(candidate.candidate_id for candidate in candidates)
            runtime_result = RuntimeResult(
                output_text=str(result.final_output),
                structured_output=(
                    result.final_output.model_dump(mode="json")
                    if hasattr(result.final_output, "model_dump")
                    else None
                ),
                output_refs=candidate_refs,
                usage=RuntimeUsage(amounts=amounts),
                provider_run_id=result.last_response_id,
                event_payloads=(
                    *hooks.events,
                    {
                        "kind": "openai_agents.operation_completed",
                        "model": model,
                        "sandbox_workspace_id": invocation.workspace.workspace_id,
                        "sandbox_tools": sandbox_tools,
                        "sandbox_tool_counts": sandbox_tool_counts,
                        "sandbox_item_types": tuple(
                            type(item).__name__ for item in result.new_items
                        ),
                    },
                ),
            )
            self._effects[binding.side_effect_key] = runtime_result
            return runtime_result
        finally:
            try:
                await session.aclose()
                if archive is not None and self._snapshot_bridge is not None:
                    self._snapshot_bridge.complete_capture(
                        binding,
                        invocation.workspace,
                        archive,
                        sensitive_values=tuple(
                            value.encode("utf-8")
                            for value in resolved_secrets.values()
                            if value
                        ),
                    )
            finally:
                await self._docker_client.delete(session)

    async def _run_with_streaming_and_approvals(
        self,
        *,
        agent: Agent[Any],
        run_input: object,
        binding: OperationExecutionBinding,
        hooks: _ProjectRunHooks,
        run_config: RunConfig,
        sdk_session: Session | None,
    ) -> RunResultStreaming:
        current_input = run_input
        for _approval_round in range(100):
            result = Runner.run_streamed(
                agent,
                current_input,  # type: ignore[arg-type]
                max_turns=binding.model_policy.max_turns,
                hooks=hooks,
                run_config=run_config,
                session=None if isinstance(current_input, RunState) else sdk_session,
            )
            async for event in result.stream_events():
                if event.type == "raw_response_event":
                    continue
                elif event.type == "agent_updated_stream_event":
                    await hooks.emit(
                        "openai_agents.active_agent_changed",
                        {"agent": event.new_agent.name},
                    )
                elif event.type == "run_item_stream_event":
                    payload: dict[str, object] = {"item_type": event.item.type}
                    await hooks.emit("openai_agents.run_item", payload)

            interruptions = tuple(result.interruptions)
            if not interruptions:
                if self._approval_gateway is not None:
                    await self._approval_gateway.complete_checkpoint(
                        binding.request_scope, binding.binding_id
                    )
                return result
            if self._approval_gateway is None:
                raise ValueError("tool approval is required but no durable gateway is configured")

            state = result.to_state()
            await self._approval_gateway.save_checkpoint(
                request_scope=binding.request_scope,
                binding_id=binding.binding_id,
                state_json=state.to_string(),
                status="awaiting_approval",
            )
            for index, interruption in enumerate(interruptions):
                raw = interruption.raw_item
                raw_arguments = getattr(raw, "arguments", {})
                if isinstance(raw_arguments, str):
                    try:
                        arguments = json.loads(raw_arguments)
                    except json.JSONDecodeError:
                        raw_digest = sha256(raw_arguments.encode()).hexdigest()
                        arguments = {"raw_digest": f"sha256:{raw_digest}"}
                elif isinstance(raw_arguments, dict):
                    arguments = raw_arguments
                else:
                    arguments = {}
                arguments_json = json.dumps(
                    arguments,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
                call_id = getattr(raw, "call_id", None) or str(index)
                approval_id = str(
                    uuid5(
                        NAMESPACE_URL,
                        f"{binding.request_scope}:{binding.side_effect_key}:approval:{call_id}",
                    )
                )
                approval = RuntimeApprovalRequest(
                    approval_id=approval_id,
                    request_scope=binding.request_scope,
                    binding_id=binding.binding_id,
                    run_id=binding.run_id,
                    operation_id=binding.operation_id,
                    tool_name=interruption.tool_name or "unknown",
                    arguments_digest=f"sha256:{sha256(arguments_json.encode()).hexdigest()}",
                    argument_names=tuple(sorted(arguments)),
                    requested_at=datetime.now(UTC),
                    expires_at=datetime.now(UTC)
                    + timedelta(seconds=self._approval_timeout_seconds),
                )
                await hooks.emit(
                    "openai_agents.approval_requested",
                    {
                        "approval_id": approval_id,
                        "tool_name": approval.tool_name,
                        "expires_at": approval.expires_at.isoformat(),
                    },
                )
                decision = await self._approval_gateway.request(approval)
                if getattr(decision, "decision", None) == "approved":
                    state.approve(interruption)
                else:
                    state.reject(
                        interruption,
                        rejection_message=getattr(decision, "reason", None),
                    )
                await hooks.emit(
                    "openai_agents.approval_decided",
                    {
                        "approval_id": approval_id,
                        "decision": getattr(decision, "decision", "rejected"),
                    },
                )
            await self._approval_gateway.save_checkpoint(
                request_scope=binding.request_scope,
                binding_id=binding.binding_id,
                state_json=state.to_string(),
                status="resuming",
            )
            current_input = state
        raise RuntimeError("runtime exceeded the bounded approval round limit")

    def _agent_tools(self, bindings: tuple[ToolBinding, ...]) -> list[Tool]:
        tools: list[Tool] = []
        for binding in bindings:
            if binding.approval_policy == "policy":
                raise ValueError(f"tool binding {binding.tool_id} has unresolved approval policy")
            if binding.tool_id in {_FILESYSTEM_TOOL_ID, _SHELL_TOOL_ID}:
                continue
            if binding.tool_id == _WEB_SEARCH_TOOL_ID:
                if binding.approval_policy != "never":
                    raise UnsupportedRuntimePolicy(
                        "WebSearchTool does not expose a runtime approval gate"
                    )
                allowed = {
                    "user_location",
                    "filters",
                    "search_context_size",
                    "external_web_access",
                }
                if set(binding.configuration) - allowed:
                    raise ValueError("WebSearchTool binding contains unsupported configuration")
                tools.append(WebSearchTool(**binding.configuration))  # type: ignore[arg-type]
                continue
            if binding.tool_id == _IMAGE_GENERATION_TOOL_ID:
                if binding.approval_policy != "never":
                    raise UnsupportedRuntimePolicy(
                        "ImageGenerationTool does not expose a runtime approval gate"
                    )
                allowed = {
                    "action",
                    "background",
                    "input_fidelity",
                    "input_image_mask",
                    "model",
                    "moderation",
                    "output_compression",
                    "output_format",
                    "partial_images",
                    "quality",
                    "size",
                }
                if set(binding.configuration) - allowed:
                    raise ValueError(
                        "ImageGenerationTool binding contains unsupported configuration"
                    )
                tools.append(
                    ImageGenerationTool(
                        tool_config=cast(
                            Any,
                            {"type": "image_generation", **binding.configuration},
                        )
                    )
                )
                continue
            tools.append(self._components.tool(binding))
        return tools

    def _delegate_agent(
        self,
        delegation: DelegationBinding,
        *,
        openai_client: AsyncOpenAI,
        fixture_assets: Mapping[str, bytes],
    ) -> Agent[Any]:
        definition: AgentDefinition = delegation.agent
        model = __import__(
            "agents.models.openai_responses", fromlist=["OpenAIResponsesModel"]
        ).OpenAIResponsesModel(model=definition.model_policy.model, openai_client=openai_client)
        entries: dict[str, File | Dir] = {
            "workspace": Dir(permissions=Permissions(owner=7)),
            "workspace/delegation.json": File(
                content=json.dumps(
                    {
                        "definition_id": definition.definition_id,
                        "workspace_id": delegation.child_workspace_id,
                        "namespace_id": delegation.child_namespace_id,
                    },
                    sort_keys=True,
                ).encode(),
                permissions=Permissions(owner=4),
            ),
        }
        instructions = [definition.instructions]
        for mount in delegation.read_mounts:
            content = fixture_assets.get(mount.content_digest)
            if content is None or f"sha256:{sha256(content).hexdigest()}" != mount.content_digest:
                raise ValueError("delegate read mount is unavailable or has a digest mismatch")
            entries[mount.logical_path.lstrip("/")] = File(
                content=content, permissions=Permissions(owner=4)
            )
        for asset in (*definition.skills, *definition.plugins):
            content = fixture_assets.get(asset.manifest_digest)
            if content is None or f"sha256:{sha256(content).hexdigest()}" != asset.manifest_digest:
                raise ValueError("delegate immutable asset is unavailable or has a digest mismatch")
            entries[asset.mount_path.lstrip("/")] = File(
                content=content, permissions=Permissions(owner=4)
            )
            if asset.ref.kind.value == "skill":
                instructions.append(
                    f"Bound immutable skill {asset.ref.logical_id} "
                    f"({asset.manifest_digest}):\n{content.decode('utf-8')}"
                )
        input_guardrails, output_guardrails = self._components.guardrails(definition.guardrails)
        return SandboxAgent(
            name=definition.name,
            handoff_description=definition.description,
            model=model,
            instructions="\n\n".join(instructions),
            default_manifest=Manifest(entries=entries, environment=Environment(value={})),
            capabilities=self._sandbox_capabilities(definition.tools),
            tools=self._agent_tools(definition.tools),
            mcp_servers=[
                self._mcp_server(
                    server,
                    network_policy=(
                        "allowlisted" if definition.capability_grant.network_hosts else "none"
                    ),
                    network_hosts=definition.capability_grant.network_hosts,
                )
                for server in definition.mcp_servers
            ],
            input_guardrails=input_guardrails,  # type: ignore[arg-type]
            output_guardrails=output_guardrails,  # type: ignore[arg-type]
            output_type=self._components.output_type(definition.output_schema),
            model_settings=ModelSettings(
                reasoning=(
                    Reasoning(effort=definition.model_policy.reasoning_effort)
                    if definition.model_policy.reasoning_effort is not None
                    else None
                ),
                verbosity=definition.model_policy.verbosity,
                include_usage=True,
            ),
        )

    async def _collect_required_artifacts(self, session: object) -> dict[str, bytes]:
        if not self._required_artifact_paths:
            return {}
        if session is None:
            raise RuntimeError("sandbox session is unavailable for required artifact collection")
        artifacts: dict[str, bytes] = {}
        for artifact_path in self._required_artifact_paths:
            file = await session.read(Path(artifact_path))  # type: ignore[attr-defined]
            try:
                artifacts[artifact_path] = file.read()
            finally:
                file.close()
        return artifacts

    @staticmethod
    def _sandbox_capabilities(tools: tuple[ToolBinding, ...]) -> list[Capability]:
        """Map immutable tool bindings to the SDK's sandbox-native capabilities."""
        requested = {
            tool.tool_id: tool
            for tool in tools
            if tool.tool_id in {_FILESYSTEM_TOOL_ID, _SHELL_TOOL_ID}
        }

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
        if binding.approval_policy == "policy":
            raise ValueError(f"MCP server {binding.server_id} has an unresolved approval policy")
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
        raise UnsupportedRuntimePolicy(
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
