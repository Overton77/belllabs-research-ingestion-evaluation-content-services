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
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import QualifiedCheckpointKey
from app.domain.operation_execution.checkpoint_lineage import (
    ROOT_CHECKPOINT_NS,
    STAMP_STATE_SCHEMA_DIGEST,
    CheckpointCapture,
    CheckpointInvocationPlan,
    CheckpointLineageInDoubt,
    IncompatibleCheckpointSchema,
)
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
        plan = _validated_plan(invocation, binding)
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
            checkpointer = cast(BaseCheckpointSaver[Any], materialized.checkpointer)
            # REQ-CP-DA-018 (RRM-003 scope): only `not_submitted` admits a submission. Any
            # other observed checkpointer state fails closed before the model is invoked;
            # resume and terminal reconstruction are the recovery protocol's job.
            source_key = await _classify_not_submitted(checkpointer, plan)
            thread_config: RunnableConfig = {
                "configurable": {
                    "thread_id": plan.namespace,
                    "checkpoint_ns": ROOT_CHECKPOINT_NS,
                }
            }
            # REQ-CP-DA-016/017: exact namespace thread, root checkpoint_ns, submission
            # pinned to the expected source, and scalar stamps copied onto every checkpoint.
            config: RunnableConfig = {
                "configurable": {
                    **thread_config["configurable"],
                    **(
                        {"checkpoint_id": source_key.checkpoint_id}
                        if source_key is not None
                        else {}
                    ),
                },
                "metadata": plan.metadata_stamps(),
                "callbacks": [disclosure_observer],
            }
            prior_messages: list[BaseMessage] = []
            if source_key is not None:
                prior_snapshot = await agent.aget_state(
                    {"configurable": dict(config["configurable"])}
                )
                prior_messages = cast(
                    list[BaseMessage], prior_snapshot.values.get("messages", [])
                )
            result = cast(
                dict[str, Any],
                await agent.ainvoke(
                    cast(Any, state),
                    context=materialized.context,
                    config=config,
                    durability="sync",
                ),
            )
            snapshot = await agent.aget_state(thread_config)
            actual_state = cast(dict[str, Any], snapshot.values)
            messages = cast(
                list[BaseMessage], actual_state.get("messages", result.get("messages", []))
            )
            capture = await _capture_result(
                checkpointer,
                plan,
                source_key,
                snapshot_config=snapshot.config,
                pending=bool(snapshot.next or snapshot.interrupts),
                summary={
                    "state_keys": sorted(actual_state),
                    "message_count": len(messages),
                    "step": (snapshot.metadata or {}).get("step"),
                },
            )

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
            checkpoint=capture,
        )


_MAX_LINEAGE_WALK = 100_000


def _validated_plan(
    invocation: RuntimeInvocation, binding: DeepAgentExecutionBinding
) -> CheckpointInvocationPlan:
    plan = invocation.checkpoint_plan
    if plan is None or binding.cognitive_session_namespace is None:
        raise DeepAgentMaterializationError(
            "Deep Agent invocations require a frozen namespace and checkpoint lineage plan"
        )
    unit = binding.runtime_unit
    if (
        unit is None
        or plan.unit_key != unit.unit_key
        or plan.execution_generation != binding.execution_generation
        or plan.namespace != binding.cognitive_session_namespace
        or plan.binding_digest != binding.binding_digest
        or plan.state_schema_digest != binding.cognitive_state_schema.schema_digest
        or plan.checkpointer_ref_digest != binding.checkpointer_ref.digest
    ):
        raise DeepAgentMaterializationError(
            "checkpoint lineage plan does not match the exact Deep Agent binding"
        )
    return plan


def _checkpoint_config(namespace: str, checkpoint_id: str) -> RunnableConfig:
    return {
        "configurable": {
            "thread_id": namespace,
            "checkpoint_ns": ROOT_CHECKPOINT_NS,
            "checkpoint_id": checkpoint_id,
        }
    }


def _parent_id(item: CheckpointTuple) -> str | None:
    if item.parent_config is None:
        return None
    return cast(str | None, item.parent_config["configurable"].get("checkpoint_id"))


async def _classify_not_submitted(
    checkpointer: BaseCheckpointSaver[Any], plan: CheckpointInvocationPlan
) -> QualifiedCheckpointKey | None:
    source = plan.expected_source
    thread_config: RunnableConfig = {
        "configurable": {"thread_id": plan.namespace, "checkpoint_ns": ROOT_CHECKPOINT_NS}
    }
    if source is None:
        if await checkpointer.aget_tuple(thread_config) is not None:
            raise CheckpointLineageInDoubt(
                "the namespace has root checkpoints but BellLabs records no namespace head"
            )
        return None
    recorded = await checkpointer.aget_tuple(
        _checkpoint_config(plan.namespace, source.checkpoint_id)
    )
    if recorded is None:
        raise CheckpointLineageInDoubt("the recorded source checkpoint is missing")
    if recorded.metadata.get(STAMP_STATE_SCHEMA_DIGEST) != plan.state_schema_digest:
        raise IncompatibleCheckpointSchema(
            "source checkpoint state-schema stamp differs from the binding (REQ-CP-CS-007)"
        )
    if _parent_id(recorded) != source.parent_checkpoint_id:
        raise CheckpointLineageInDoubt("the source checkpoint's ancestry differs from its record")
    async for item in checkpointer.alist(thread_config):
        if _parent_id(item) == source.checkpoint_id:
            raise CheckpointLineageInDoubt(
                "a root checkpoint already descends from the expected source"
            )
    return QualifiedCheckpointKey(
        checkpointer_ref_digest=plan.checkpointer_ref_digest,
        thread_id=plan.namespace,
        checkpoint_ns=ROOT_CHECKPOINT_NS,
        checkpoint_id=source.checkpoint_id,
        parent_checkpoint_id=_parent_id(recorded),
    )


async def _capture_result(
    checkpointer: BaseCheckpointSaver[Any],
    plan: CheckpointInvocationPlan,
    source_key: QualifiedCheckpointKey | None,
    *,
    snapshot_config: RunnableConfig,
    pending: bool,
    summary: dict[str, object],
) -> CheckpointCapture:
    """Capture the result config and verify a fully stamped root lineage to the source."""

    if pending:
        raise CheckpointLineageInDoubt(
            "the invocation ended with pending tasks or interrupts; it is not terminal"
        )
    configurable = snapshot_config.get("configurable", {})
    result_id = configurable.get("checkpoint_id")
    if configurable.get("checkpoint_ns", ROOT_CHECKPOINT_NS) != ROOT_CHECKPOINT_NS or not result_id:
        raise CheckpointLineageInDoubt("no root result checkpoint was captured")
    stamps = plan.metadata_stamps()
    stop_at = source_key.checkpoint_id if source_key is not None else None
    cursor: str | None = str(result_id)
    result_parent: str | None = None
    stamped = 0
    while cursor != stop_at:
        if cursor is None or stamped >= _MAX_LINEAGE_WALK:
            raise CheckpointLineageInDoubt("result checkpoint does not descend from the source")
        item = await checkpointer.aget_tuple(_checkpoint_config(plan.namespace, cursor))
        if item is None:
            raise CheckpointLineageInDoubt("a checkpoint in the result lineage is missing")
        if any(item.metadata.get(key) != value for key, value in stamps.items()):
            raise CheckpointLineageInDoubt(
                "a checkpoint between source and result lacks this invocation's stamps"
            )
        if stamped == 0:
            result_parent = _parent_id(item)
        stamped += 1
        cursor = _parent_id(item)
    if result_parent is None:
        raise CheckpointLineageInDoubt("the result checkpoint has no parent")
    result_key = QualifiedCheckpointKey(
        checkpointer_ref_digest=plan.checkpointer_ref_digest,
        thread_id=plan.namespace,
        checkpoint_ns=ROOT_CHECKPOINT_NS,
        checkpoint_id=str(result_id),
        parent_checkpoint_id=result_parent,
    )
    return CheckpointCapture(
        namespace=plan.namespace,
        invocation_id=plan.invocation_id,
        source_key=source_key,
        result_key=result_key,
        ancestry_verified=True,
        stamped_checkpoint_count=stamped,
        redacted_summary_digest=sha256_digest(
            {
                "namespace": plan.namespace,
                "result_checkpoint_id": result_key.checkpoint_id,
                "parent_checkpoint_id": result_parent,
                "stamped_checkpoint_count": stamped,
                **summary,
            }
        ),
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
