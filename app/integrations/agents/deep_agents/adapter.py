from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from deepagents import create_deep_agent
from deepagents.backends.protocol import SandboxBackendProtocol
from deepagents.middleware.filesystem import FilesystemPermission
from deepagents.middleware.subagents import SubAgent
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langgraph.types import StateSnapshot

from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import QualifiedCheckpointKey
from app.domain.operation_execution.checkpoint_lineage import (
    ROOT_CHECKPOINT_NS,
    STAMP_ATTEMPT_REF,
    STAMP_STATE_SCHEMA_DIGEST,
    CheckpointCapture,
    CheckpointClassification,
    CheckpointInvocationPlan,
    CheckpointLineageError,
    CheckpointLineageInDoubt,
    IncompatibleCheckpointSchema,
)
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
)
from app.domain.operation_execution.errors import (
    DeepAgentMaterializationError,
    RuntimeInvocationFailure,
)
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
            # REQ-CP-DA-018: classify the unit generation from the checkpointer before any
            # provider work, then act exactly as `CON-CP-CHECKPOINT-LINEAGE-V1` prescribes:
            # submit once, resume without input, reconstruct without invocation, or fail
            # closed as `in_doubt` with typed candidates.
            classified = await _classify(checkpointer, agent, plan)
            source_key = classified.source_key
            thread_config: RunnableConfig = {
                "configurable": {
                    "thread_id": plan.namespace,
                    "checkpoint_ns": ROOT_CHECKPOINT_NS,
                }
            }
            prior_messages: list[BaseMessage] = []
            try:
                if source_key is not None:
                    prior_snapshot = await agent.aget_state(
                        _checkpoint_config(plan.namespace, source_key.checkpoint_id)
                    )
                    prior_messages = cast(
                        list[BaseMessage], prior_snapshot.values.get("messages", [])
                    )
                if classified.kind == CheckpointClassification.TERMINAL_UNOBSERVED:
                    # Terminal reconstruction: the result is the stamped leaf's state.
                    assert classified.leaf_snapshot is not None
                    snapshot = classified.leaf_snapshot
                    result = cast(dict[str, Any], dict(snapshot.values))
                else:
                    pinned = (
                        classified.leaf_id
                        if classified.kind == CheckpointClassification.INTERRUPTED
                        else (source_key.checkpoint_id if source_key is not None else None)
                    )
                    # REQ-CP-DA-016/017: exact namespace thread, root checkpoint_ns, pinned
                    # to the expected source (submit) or to the interrupted leaf (resume),
                    # and scalar stamps copied onto every checkpoint.
                    config: RunnableConfig = {
                        "configurable": {
                            **thread_config["configurable"],
                            **({"checkpoint_id": pinned} if pinned is not None else {}),
                        },
                        "metadata": plan.invocation_metadata(),
                        "callbacks": [disclosure_observer],
                    }
                    # A resume never re-appends the submitted input: it continues the
                    # pending tasks of the pinned checkpoint with no input.
                    invoke_input = (
                        None
                        if classified.kind == CheckpointClassification.INTERRUPTED
                        else state
                    )
                    result = cast(
                        dict[str, Any],
                        await agent.ainvoke(
                            cast(Any, invoke_input),
                            context=materialized.context,
                            config=config,
                            durability="sync",
                        ),
                    )
                    # Capture this attempt's own result tip, never the thread's latest
                    # checkpoint (which a superseded attempt may have written).
                    snapshot = await agent.aget_state(
                        await _own_result_config(checkpointer, plan, classified)
                    )
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
                    classification=classified.kind,
                    summary={
                        "state_keys": sorted(actual_state),
                        "message_count": len(messages),
                        "step": (snapshot.metadata or {}).get("step"),
                    },
                )
                final = next(
                    (item for item in reversed(messages) if isinstance(item, AIMessage)), None
                )
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
            except CheckpointLineageError:
                raise
            except Exception as error:
                # REQ-CP-RUN-007 (narrowed): report whether a terminal result exists after the
                # failure, so the boundary settles `failed` only when none does.
                terminal, candidates = await _terminal_result_may_exist(
                    checkpointer, agent, plan
                )
                raise RuntimeInvocationFailure(
                    type(error).__name__,
                    terminal_result_observed=terminal,
                    candidates=candidates,
                ) from error


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


@dataclass(frozen=True)
class _Classified:
    kind: CheckpointClassification
    source_key: QualifiedCheckpointKey | None
    leaf_id: str | None = None
    leaf_snapshot: StateSnapshot | None = None


def _qualified(plan: CheckpointInvocationPlan, item: CheckpointTuple) -> QualifiedCheckpointKey:
    return QualifiedCheckpointKey(
        checkpointer_ref_digest=plan.checkpointer_ref_digest,
        thread_id=plan.namespace,
        checkpoint_ns=ROOT_CHECKPOINT_NS,
        checkpoint_id=str(item.config["configurable"]["checkpoint_id"]),
        parent_checkpoint_id=_parent_id(item),
    )


def _stamped(item: CheckpointTuple, plan: CheckpointInvocationPlan) -> bool:
    return all(item.metadata.get(key) == value for key, value in plan.metadata_stamps().items())


async def _classify(
    checkpointer: BaseCheckpointSaver[Any],
    agent: Any,
    plan: CheckpointInvocationPlan,
) -> _Classified:
    """`CON-CP-CHECKPOINT-LINEAGE-V1` classification of one unit generation.

    Descendants of the expected source are the root-namespace checkpoints whose parent chain
    reaches it (for a new namespace, every root checkpoint). None means `not_submitted`.
    Exactly one stamped lineage with no unstamped or foreign descendant is `interrupted`
    when its leaf has pending tasks and `terminal_unobserved` when it has none. Anything
    else, including a pending LangGraph interrupt, is `in_doubt`. An operator-accepted
    descendant (`accept_descendant`) is classified as if it were the unique leaf; stamped
    descendants it has since gained (a resumed run) are followed to their unique leaf.
    """

    source = plan.expected_source
    thread_config: RunnableConfig = {
        "configurable": {"thread_id": plan.namespace, "checkpoint_ns": ROOT_CHECKPOINT_NS}
    }
    source_key: QualifiedCheckpointKey | None = None
    if source is not None:
        recorded = await checkpointer.aget_tuple(
            _checkpoint_config(plan.namespace, source.checkpoint_id)
        )
        if recorded is None:
            raise CheckpointLineageInDoubt(
                "the recorded source checkpoint is missing", reason="missing_checkpoint"
            )
        if recorded.metadata.get(STAMP_STATE_SCHEMA_DIGEST) != plan.state_schema_digest:
            raise IncompatibleCheckpointSchema(
                "source checkpoint state-schema stamp differs from the binding (REQ-CP-CS-007)"
            )
        if _parent_id(recorded) != source.parent_checkpoint_id:
            raise CheckpointLineageInDoubt(
                "the source checkpoint's ancestry differs from its record",
                reason="ancestry_mismatch",
            )
        source_key = _qualified(plan, recorded)
    items: dict[str, CheckpointTuple] = {}
    async for item in checkpointer.alist(thread_config):
        if len(items) >= _MAX_LINEAGE_WALK:
            raise CheckpointLineageInDoubt(
                "the checkpointer lineage cannot be classified", reason="unclassifiable"
            )
        items[str(item.config["configurable"]["checkpoint_id"])] = item
    parents = {checkpoint_id: _parent_id(item) for checkpoint_id, item in items.items()}

    def descends(checkpoint_id: str, ancestor: str | None) -> bool:
        cursor = parents.get(checkpoint_id)
        for _ in range(len(parents) + 1):
            if cursor == ancestor:
                return True
            if cursor is None:
                return False
            cursor = parents.get(cursor)
        return False

    root_id = source.checkpoint_id if source is not None else None
    descendants = {
        checkpoint_id
        for checkpoint_id in items
        if checkpoint_id != root_id and descends(checkpoint_id, root_id)
    }
    if not descendants:
        return _Classified(CheckpointClassification.NOT_SUBMITTED, source_key)
    stamped = {
        checkpoint_id for checkpoint_id in descendants if _stamped(items[checkpoint_id], plan)
    }
    scope = descendants
    if plan.accepted_leaf is not None:
        accepted = plan.accepted_leaf.checkpoint_id
        path_ok = accepted in stamped and _qualified(plan, items[accepted]) == plan.accepted_leaf
        cursor = parents.get(accepted) if path_ok else None
        while path_ok and cursor != root_id:
            if cursor is None or cursor not in stamped:
                path_ok = False
                break
            cursor = parents.get(cursor)
        if not path_ok:
            raise CheckpointLineageInDoubt(
                "the accepted descendant is not a stamped root-namespace descendant of the "
                "source",
                reason="accepted_descendant_invalid",
            )
        scope = {accepted} | {
            checkpoint_id for checkpoint_id in descendants if descends(checkpoint_id, accepted)
        }
    leaves = sorted(
        checkpoint_id
        for checkpoint_id in scope
        if not any(parents.get(other) == checkpoint_id for other in scope)
    )
    candidates = tuple(
        _qualified(plan, items[checkpoint_id])
        for checkpoint_id in leaves
        if checkpoint_id in stamped
    )
    if scope - stamped:
        raise CheckpointLineageInDoubt(
            "a root checkpoint descending from the source is not this unit generation's",
            reason="foreign_descendant",
            candidates=candidates,
        )
    if len(leaves) != 1:
        raise CheckpointLineageInDoubt(
            "more than one stamped leaf descends from the source",
            reason="multiple_stamped_leaves",
            candidates=candidates,
        )
    leaf_id = leaves[0]
    snapshot = await agent.aget_state(_checkpoint_config(plan.namespace, leaf_id))
    if snapshot.interrupts or any(task.interrupts for task in snapshot.tasks):
        raise CheckpointLineageInDoubt(
            "the stamped leaf holds a pending LangGraph interrupt",
            reason="pending_interrupt",
            candidates=candidates,
        )
    return _Classified(
        (
            CheckpointClassification.INTERRUPTED
            if snapshot.next
            else CheckpointClassification.TERMINAL_UNOBSERVED
        ),
        source_key,
        leaf_id=leaf_id,
        leaf_snapshot=snapshot,
    )


async def _own_result_config(
    checkpointer: BaseCheckpointSaver[Any],
    plan: CheckpointInvocationPlan,
    classified: _Classified,
) -> RunnableConfig:
    """The unique tip this attempt wrote, descending from what it classified and pinned."""

    thread_config: RunnableConfig = {
        "configurable": {"thread_id": plan.namespace, "checkpoint_ns": ROOT_CHECKPOINT_NS}
    }
    if plan.attempt_ref is None:
        return thread_config  # adapter-level harnesses without a lease holder
    own: dict[str, str | None] = {}
    async for item in checkpointer.alist(thread_config):
        if item.metadata.get(STAMP_ATTEMPT_REF) == plan.attempt_ref:
            own[str(item.config["configurable"]["checkpoint_id"])] = _parent_id(item)
    tips = [checkpoint_id for checkpoint_id in own if checkpoint_id not in own.values()]
    if len(tips) != 1:
        raise CheckpointLineageInDoubt(
            "this attempt's invocation has no unique result tip", reason="unclassifiable"
        )
    pinned = (
        classified.leaf_id
        if classified.kind == CheckpointClassification.INTERRUPTED
        else (classified.source_key.checkpoint_id if classified.source_key else None)
    )
    cursor = own[tips[0]]
    while cursor in own:
        cursor = own[cursor]
    if cursor != pinned:
        raise CheckpointLineageInDoubt(
            "the captured result does not descend from the checkpoint this attempt pinned",
            reason="ancestry_mismatch",
        )
    return _checkpoint_config(plan.namespace, tips[0])


async def _terminal_result_may_exist(
    checkpointer: BaseCheckpointSaver[Any], agent: Any, plan: CheckpointInvocationPlan
) -> tuple[bool, tuple[QualifiedCheckpointKey, ...]]:
    """After a failure: whether a terminal stamped result may exist, with its candidates."""

    try:
        classified = await _classify(checkpointer, agent, plan)
    except CheckpointLineageInDoubt as error:
        return True, error.candidates
    except Exception:  # noqa: BLE001 - an unclassifiable lineage is not provably non-terminal
        return True, ()
    if classified.kind != CheckpointClassification.TERMINAL_UNOBSERVED:
        return False, ()
    assert classified.leaf_id is not None
    leaf = await checkpointer.aget_tuple(_checkpoint_config(plan.namespace, classified.leaf_id))
    return True, ((_qualified(plan, leaf),) if leaf is not None else ())


async def _capture_result(
    checkpointer: BaseCheckpointSaver[Any],
    plan: CheckpointInvocationPlan,
    source_key: QualifiedCheckpointKey | None,
    *,
    snapshot_config: RunnableConfig,
    pending: bool,
    summary: dict[str, object],
    classification: CheckpointClassification = CheckpointClassification.NOT_SUBMITTED,
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
        classification=classification,
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
