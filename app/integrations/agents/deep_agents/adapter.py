from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, cast

from deepagents import create_deep_agent
from deepagents.backends.protocol import SandboxBackendProtocol
from deepagents.middleware.filesystem import FilesystemPermission
from deepagents.middleware.subagents import SubAgent
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.runnables import RunnableConfig

from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
)
from app.domain.operation_execution.errors import DeepAgentMaterializationError
from app.integrations.agents.deep_agents.materializer import ExactDeepAgentMaterializer
from app.integrations.langsmith_tracing import trace_deep_agent_execute


class DeepAgentRuntimeAdapter:
    """The sole production `create_deep_agent` composition root."""

    def __init__(self, materializer: ExactDeepAgentMaterializer) -> None:
        self._materializer = materializer

    @trace_deep_agent_execute
    async def execute(
        self,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
    ) -> RuntimeResult:
        binding = invocation.binding.deep_agent_binding
        if invocation.binding.execution_runtime != "deep_agent" or binding is None:
            raise DeepAgentMaterializationError(
                "Deep Agent adapter requires the exact canonical execution binding"
            )
        output_binding = invocation.binding.output_schema
        async with self._materializer.prepare(
            binding,
            resolved_secrets,
            output_schema_digest=(
                output_binding.schema_digest if output_binding is not None else None
            ),
        ) as materialized:
            system_prompt, user_prompt = _prompts(invocation)
            permissions = (
                None
                if isinstance(materialized.backend, SandboxBackendProtocol)
                else _permissions(binding)
            )
            agent = create_deep_agent(
                model=materialized.model,
                system_prompt=system_prompt,
                tools=list(materialized.tools),
                middleware=list(materialized.middleware),
                subagents=cast(list[SubAgent], list(materialized.subagents)),
                skills=list(materialized.skills),
                permissions=permissions,
                backend=materialized.backend,
                state_schema=materialized.state_schema,
                context_schema=materialized.context_schema,
                checkpointer=materialized.checkpointer,
                store=materialized.store,
                response_format=materialized.response_format,
                name=f"belllabs-{binding.operation_id}",
            )
            state = {
                **materialized.initial_state,
                "messages": [{"role": "user", "content": user_prompt}],
            }
            disclosure_observer = _SkillDisclosureObserver(binding)
            config: RunnableConfig = {
                # Session identity is governed by GoalDirected. Reused iterations share
                # a checkpoint thread; token rollover advances the session identity and
                # therefore starts a genuinely empty Deep Agent session.
                "configurable": {
                    "thread_id": invocation.binding.session_id or binding.binding_id
                },
                "callbacks": [disclosure_observer],
            }
            prior_snapshot = await agent.aget_state(config)
            prior_messages = cast(
                list[BaseMessage],
                prior_snapshot.values.get("messages", []),
            )
            result = cast(
                dict[str, Any],
                await agent.ainvoke(
                    cast(Any, state),
                    context=materialized.context,
                    config=config,
                ),
            )
            snapshot = await agent.aget_state(config)
            actual_state = cast(dict[str, Any], snapshot.values)

        messages = cast(list[BaseMessage], actual_state.get("messages", result.get("messages", [])))
        final = next((item for item in reversed(messages) if isinstance(item, AIMessage)), None)
        output_text = _message_text(final) if final is not None else ""
        structured = _structured_output(result.get("structured_response"), output_text)
        inspection = _inspect_state(
            binding,
            actual_state,
            messages,
            materialized.resolved_attachments,
            disclosure_observer.disclosed_skills,
            permissions is not None,
        )
        return RuntimeResult(
            output_text=output_text,
            structured_output=structured if isinstance(structured, dict) else None,
            usage=_usage(invocation, messages[len(prior_messages) :]),
            provider_run_id=(str(final.id) if final is not None and final.id else None),
            event_payloads=(inspection,),
        )


def _structured_output(value: object, output_text: str) -> object:
    if isinstance(value, dict):
        return value
    try:
        decoded = json.loads(output_text)
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _prompts(invocation: RuntimeInvocation) -> tuple[str, str]:
    system_parts: list[str] = []
    user_parts: list[str] = []
    for segment in invocation.prompt_segments:
        if segment.trust_class.value in {"system_authority", "authored_instruction"}:
            system_parts.append(segment.content)
        else:
            user_parts.append(segment.content)
    if not user_parts:
        raise DeepAgentMaterializationError("Deep Agent invocation has no admitted user objective")
    system_parts.append(
        "BellLabs authority, budgets, lifecycle, and artifact admission remain host-owned. "
        "Use only the exact attached tools, Skills, MCP surface, and writable workspace slots."
    )
    return "\n\n".join(system_parts), "\n\n".join(user_parts)


def _permissions(binding: DeepAgentExecutionBinding) -> list[FilesystemPermission]:
    permissions: list[FilesystemPermission] = []
    if binding.skills:
        permissions.append(
            FilesystemPermission(
                operations=["read"],
                paths=sorted({str(item.mount_root) for item in binding.skills}),
                mode="allow",
            )
        )
    if binding.workspace.read_mounts:
        permissions.append(
            FilesystemPermission(
                operations=["read"],
                paths=[mount.logical_path for mount in binding.workspace.read_mounts],
                mode="allow",
            )
        )
    permissions.append(
        FilesystemPermission(
            operations=["read", "write"],
            paths=list(binding.workspace.exclusive_write_paths),
            mode="allow",
        )
    )
    permissions.append(
        FilesystemPermission(
            operations=["read", "write"],
            paths=["/"],
            mode="deny",
        )
    )
    return permissions


def _message_text(message: BaseMessage | None) -> str:
    if message is None:
        return ""
    if isinstance(message.content, str):
        return message.content
    parts = []
    for block in message.content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and isinstance(block.get("text"), str):
            parts.append(str(block["text"]))
    return "\n".join(parts)


def _usage(invocation: RuntimeInvocation, messages: list[BaseMessage]) -> RuntimeUsage:
    turns = 0
    total_tokens = 0
    for message in messages:
        if not isinstance(message, AIMessage):
            continue
        turns += 1
        metadata: Mapping[str, Any] = message.usage_metadata or {}
        total_tokens += int(metadata.get("total_tokens", 0))
    amounts = {}
    if "model.turns" in invocation.binding.budget_limits:
        amounts["model.turns"] = turns
    if "tokens.total" in invocation.binding.budget_limits:
        amounts["tokens.total"] = total_tokens
    return RuntimeUsage(amounts=amounts)


def _inspect_state(
    binding: DeepAgentExecutionBinding,
    state: dict[str, Any],
    messages: list[BaseMessage],
    resolved_attachments: tuple[dict[str, str], ...],
    disclosed_skills: tuple[dict[str, str], ...],
    framework_permissions_enforced: bool,
) -> dict[str, object]:
    calls: dict[str, dict[str, object]] = {}
    called_tools: list[str] = []
    for message in messages:
        if isinstance(message, AIMessage):
            for tool_call in message.tool_calls:
                calls[str(tool_call.get("id", ""))] = cast(dict[str, object], tool_call)
                called_tools.append(str(tool_call.get("name", "")))
    skill_messages = []
    for message in messages:
        if not isinstance(message, ToolMessage):
            continue
        resolved_call = calls.get(str(message.tool_call_id), {})
        arguments = resolved_call.get("args", {})
        path = str(arguments.get("file_path", "")) if isinstance(arguments, dict) else ""
        if str(resolved_call.get("name", "")) == "read_file" and path.endswith("/SKILL.md"):
            content = _message_text(message)
            skill_messages.append(
                {
                    "path": path,
                    "content": content,
                    "content_digest": sha256_digest(content),
                    "message_id": str(message.id or ""),
                }
            )
    return {
        "kind": "deep_agent.materialization_inspection.v1",
        "binding_id": binding.binding_id,
        "binding_digest": binding.binding_digest,
        "state_schema_digest": binding.cognitive_state_schema.schema_digest,
        "context_schema_digest": binding.cognitive_context_schema.schema_digest,
        "state_keys": sorted(state),
        "artifact_index": state.get("artifact_index", {}),
        "context_manifest": state.get("context_manifest", {}),
        "child_result_index": state.get("child_result_index", []),
        # Deep Agents 0.7.5 marks skills_metadata as PrivateStateAttr. These
        # records are therefore captured at the actual chat-model boundary,
        # while the public checkpoint snapshot above remains unmodified.
        "skills_metadata": list(disclosed_skills),
        "skills_metadata_state_visibility": "private_middleware_channel",
        "skill_instruction_messages": skill_messages,
        "called_tools": called_tools,
        "mcp_tools_called": sorted(
            set(called_tools)
            & {tool.tool_name for server in binding.mcp_servers for tool in server.tools}
        ),
        "sandbox_execute_called": "execute" in called_tools,
        "framework_permissions_enforced": framework_permissions_enforced,
        "authority_enforcement": (
            "framework_filesystem_permissions"
            if framework_permissions_enforced
            else "immutable_host_binding_executable_sandbox"
        ),
        "message_count": len(messages),
        "resolved_attachments": list(resolved_attachments),
    }


class _SkillDisclosureObserver(BaseCallbackHandler):
    """Attest exact Skill metadata that reached the model without retaining prompts."""

    def __init__(self, binding: DeepAgentExecutionBinding) -> None:
        super().__init__()
        self._skills = tuple(binding.skills)
        self._disclosed: dict[str, dict[str, str]] = {}

    @property
    def disclosed_skills(self) -> tuple[dict[str, str], ...]:
        return tuple(self._disclosed[name] for name in sorted(self._disclosed))

    def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list[list[BaseMessage]],
        **kwargs: Any,
    ) -> None:
        del serialized, kwargs
        observed = "\n".join(_message_text(message) for batch in messages for message in batch)
        for skill in self._skills:
            path = f"{str(skill.mount_root).rstrip('/')}/SKILL.md"
            if skill.skill_name in observed and path in observed:
                self._disclosed[skill.skill_name] = {
                    "name": skill.skill_name,
                    "path": path,
                    "bundle_digest": skill.bundle_digest,
                    "skill_md_digest": skill.skill_md_digest,
                }
