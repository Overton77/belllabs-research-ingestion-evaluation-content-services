from __future__ import annotations

import base64
import json
from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Annotated, Any, NotRequired, Protocol, cast

from deepagents import create_deep_agent
from deepagents.backends.protocol import BackendProtocol, SandboxBackendProtocol
from deepagents.backends.state import StateBackend
from deepagents.backends.utils import file_data_to_string
from deepagents.middleware.filesystem import FilesystemPermission
from deepagents.middleware.subagents import SubAgent
from langchain.agents.middleware.types import AgentMiddleware
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import BaseCheckpointSaver, CheckpointTuple
from langgraph.runtime import Runtime
from langgraph.types import StateSnapshot
from typing_extensions import TypedDict

from app.application.operations.operation_progress import register_checkpoint_reader
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.identities import QualifiedCheckpointKey
from app.domain.operation_execution.async_subagent_reconciliation import (
    AsyncServedGraphIdentity,
)
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
    AsyncSubagentContract,
    CapturedWorkspaceCandidate,
    DeepAgentExecutionBinding,
    OperationExecutionBinding,
    RuntimeInvocation,
    RuntimeResult,
    RuntimeUsage,
)
from app.domain.operation_execution.errors import (
    DeepAgentMaterializationError,
    RuntimeInvocationFailure,
)
from app.integrations.agents.deep_agents.capability_lineage import capability_lineage
from app.integrations.agents.deep_agents.checkpoint_reads import (
    MAX_LINEAGE_WALK,
    checkpoint_parent_id,
    root_checkpoint_config,
)
from app.integrations.agents.deep_agents.materializer import (
    ExactDeepAgentMaterializer,
    MaterializedDeepAgentArguments,
)
from app.integrations.langsmith_tracing import trace_deep_agent_execute


class AsyncSubagentMiddlewareFactory(Protocol):
    """Builds the governed async-subagent middleware for one parent operation binding."""

    def middleware(
        self,
        binding: OperationExecutionBinding,
        contracts: tuple[AsyncSubagentContract, ...],
        resolved_secrets: Mapping[str, str],
    ) -> AgentMiddleware[Any, Any, Any]: ...


class WorkspaceOutputCapturePort(Protocol):
    """Captures a file the agent wrote into one of its exclusive writable slots (RRM-009).

    REQ-CP-DA-014: capture makes the bytes and descriptor durable as a workspace candidate;
    only the governed promotion decision makes them a consumable artifact.
    """

    async def capture(
        self, binding: OperationExecutionBinding, logical_path: str, content: bytes
    ) -> CapturedWorkspaceCandidate: ...


MAX_CAPTURED_OUTPUT_FILES = 64
MAX_CAPTURED_OUTPUT_BYTES = 4_000_000
MAX_CAPTURE_LISTING_DEPTH = 4


class DeepAgentRuntimeAdapter:
    """The sole production `create_deep_agent` composition root."""

    def __init__(
        self,
        materializer: ExactDeepAgentMaterializer,
        *,
        async_subagents: AsyncSubagentMiddlewareFactory | None = None,
        workspace_outputs: WorkspaceOutputCapturePort | None = None,
    ) -> None:
        self._materializer = materializer
        self._async_subagents = async_subagents
        self._workspace_outputs = workspace_outputs

    async def build_hosted_async_subagent_graph(
        self,
        binding: DeepAgentExecutionBinding,
        resolved_secrets: Mapping[str, str],
        *,
        system_prompt: str,
        served: AsyncServedGraphIdentity,
        stack: AsyncExitStack,
    ) -> Any:
        """Compile the graph an Agent Server serves for one exact async subagent binding.

        REQ-CP-DA-019: the graph is materialized from the exact binding through the canonical
        materializer and compiled here, the only `create_deep_agent` site. The server owns the
        checkpointer, the store and scheduling; the graph stamps its served identity into the
        thread state so the parent can verify it on every observation. Provider lifecycles the
        materializer opens stay open on `stack` for the server process's lifetime.
        """

        if served.graph_binding_digest != binding.binding_digest:
            raise DeepAgentMaterializationError(
                "served graph identity does not name this exact binding"
            )
        materialized = await stack.enter_async_context(
            self._materializer.prepare(binding, resolved_secrets, hosted=True)
        )
        return _compile(
            materialized,
            binding,
            system_prompt=system_prompt,
            extra_middleware=[ServedGraphIdentityMiddleware(served)],
            name=f"belllabs-async-{binding.operation_id}",
        )

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
            if materialized.checkpointer is None or materialized.store is None:
                raise DeepAgentMaterializationError(
                    "local-in-worker cognition requires the registered checkpointer and store"
                )
            # REQ-CP-DA-008: async subagents reach the Agent Server only through the governed
            # middleware, which reserves and links each child before any provider submission.
            governed_async: list[AgentMiddleware[Any, Any, Any]] = (
                [
                    self._async_subagents.middleware(
                        invocation.binding, binding.async_subagents, resolved_secrets
                    )
                ]
                if self._async_subagents is not None and binding.async_subagents
                else []
            )
            permissions = _effective_permissions(materialized, binding)
            agent = _compile(
                materialized,
                binding,
                system_prompt=system_prompt,
                extra_middleware=governed_async,
                name=f"belllabs-{binding.operation_id}",
            )
            state = {
                **materialized.initial_state,
                "messages": [{"role": "user", "content": user_prompt}],
            }
            disclosure_observer = _SkillDisclosureObserver(binding)
            model_calls = _ModelCallObserver()
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
            # RRM-008: the Activity heartbeat reads the latest durable root checkpoint of the
            # namespace while cognition runs (keys only; REQ-CP-EXEC-008 step 3).
            register_checkpoint_reader(_latest_checkpoint_reader(checkpointer, plan))
            try:
                if source_key is not None:
                    prior_snapshot = await agent.aget_state(
                        root_checkpoint_config(plan.namespace, source_key.checkpoint_id)
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
                        "callbacks": [disclosure_observer, model_calls],
                    }
                    # A resume never re-appends the submitted input: it continues the
                    # pending tasks of the pinned checkpoint with no input.
                    invoke_input = (
                        None if classified.kind == CheckpointClassification.INTERRUPTED else state
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
                # RRM-009 (REQ-CP-DA-014): the agent's writable-slot files become durable
                # workspace candidates before the result settles; a capture failure after a
                # terminal checkpoint is classified by the narrowed post-dispatch rule.
                inspection["workspace_candidates"] = await self._capture_workspace_outputs(
                    materialized.backend, invocation.binding, actual_state
                )
                own_messages = messages[len(prior_messages) :]
                # REQ-CP-DA-007: in-process sync subagents spend the parent's reservation, so
                # their model calls (observed at the chat-model boundary; they never enter the
                # parent's messages) are part of the operation's observed usage.
                subordinate_calls = model_calls.calls_outside(messages)
                usage = _usage(invocation, own_messages, subordinate_calls)
                inspection["capability_lineage"] = capability_lineage(
                    binding,
                    own_messages,
                    usage_amounts=usage.amounts,
                    model_calls=(
                        *_message_usage(own_messages),
                        *({**call, "scope": "subordinate"} for call in subordinate_calls),
                    ),
                    disclosed_skills=disclosure_observer.disclosed_skills,
                    operation_secret_refs=invocation.binding.secret_refs,
                )
                return RuntimeResult(
                    output_text=output_text,
                    structured_output=structured if isinstance(structured, dict) else None,
                    usage=usage,
                    provider_run_id=(str(final.id) if final is not None and final.id else None),
                    event_payloads=(inspection,),
                    checkpoint=capture,
                )
            except CheckpointLineageError:
                raise
            except Exception as error:
                # REQ-CP-RUN-007 (narrowed): report whether a terminal result exists after the
                # failure, so the boundary settles `failed` only when none does. RRM-008: the
                # latest durable checkpoint of a unique stamped lineage travels with the
                # failure, so the settlement advances the namespace head over the partial
                # lineage instead of stranding it.
                terminal, candidates, latest = await _terminal_result_may_exist(
                    checkpointer, agent, plan
                )
                raise RuntimeInvocationFailure(
                    type(error).__name__,
                    terminal_result_observed=terminal,
                    candidates=candidates,
                    latest_capture=latest,
                ) from error
            finally:
                register_checkpoint_reader(None)

    async def observe_latest(
        self,
        invocation: RuntimeInvocation,
        resolved_secrets: Mapping[str, str],
    ) -> RuntimeResult:
        """RRM-008 (REQ-CP-EXEC-008 step 3): the latest durable checkpoint of a unit generation
        that is being cancelled, with the usage its completed model calls already incurred.

        No cognition is invoked and nothing is resumed. A unit classified `interrupted` or
        `terminal_unobserved` yields its stamped leaf as the result checkpoint (the partial
        evidence); a `not_submitted` unit yields no checkpoint. Anything ambiguous raises
        `CheckpointLineageInDoubt` exactly as a dispatch would.
        """

        binding = invocation.binding.deep_agent_binding
        if invocation.binding.execution_runtime != "deep_agent" or binding is None:
            raise DeepAgentMaterializationError(
                "Deep Agent adapter requires the exact canonical execution binding"
            )
        plan = _validated_plan(invocation, binding)
        async with self._materializer.prepare(binding, resolved_secrets) as materialized:
            if materialized.checkpointer is None or materialized.store is None:
                raise DeepAgentMaterializationError(
                    "local-in-worker cognition requires the registered checkpointer and store"
                )
            system_prompt, _user_prompt = _prompts(invocation)
            agent = _compile(
                materialized,
                binding,
                system_prompt=system_prompt,
                extra_middleware=[],
                name=f"belllabs-{binding.operation_id}",
            )
            checkpointer = cast(BaseCheckpointSaver[Any], materialized.checkpointer)
            classified = await _classify(checkpointer, agent, plan)
            if classified.kind == CheckpointClassification.NOT_SUBMITTED:
                return RuntimeResult(output_text="", checkpoint=None)
            assert classified.leaf_snapshot is not None
            snapshot = classified.leaf_snapshot
            messages = cast(list[BaseMessage], dict(snapshot.values).get("messages", []))
            prior: list[BaseMessage] = []
            if classified.source_key is not None:
                prior_snapshot = await agent.aget_state(
                    root_checkpoint_config(plan.namespace, classified.source_key.checkpoint_id)
                )
                prior = cast(list[BaseMessage], prior_snapshot.values.get("messages", []))
            capture = await _capture_result(
                checkpointer,
                plan,
                classified.source_key,
                snapshot_config=snapshot.config,
                pending=bool(snapshot.next or snapshot.interrupts),
                classification=classified.kind,
                summary={
                    "state_keys": sorted(dict(snapshot.values)),
                    "message_count": len(messages),
                    "step": (snapshot.metadata or {}).get("step"),
                },
                allow_pending=True,
            )
            final = next(
                (item for item in reversed(messages) if isinstance(item, AIMessage)), None
            )
            return RuntimeResult(
                output_text=_message_text(final) if final is not None else "",
                usage=_usage(invocation, messages[len(prior) :]),
                provider_run_id=(str(final.id) if final is not None and final.id else None),
                checkpoint=capture,
            )

    async def _capture_workspace_outputs(
        self,
        backend: BackendProtocol,
        binding: OperationExecutionBinding,
        state: dict[str, Any],
    ) -> list[dict[str, object]]:
        if self._workspace_outputs is None:
            return []
        captured: list[dict[str, object]] = []
        for logical_path, content in await _slot_files(
            backend, binding.workspace.exclusive_write_paths, state
        ):
            candidate = await self._workspace_outputs.capture(binding, logical_path, content)
            captured.append(
                {
                    "logical_path": candidate.logical_path,
                    "output_slot": candidate.output_slot,
                    "candidate_id": candidate.candidate_id,
                    "content_digest": candidate.content_digest,
                    "size_bytes": candidate.size_bytes,
                    "media_type": candidate.media_type,
                }
            )
        return captured


def _effective_permissions(
    materialized: MaterializedDeepAgentArguments, binding: DeepAgentExecutionBinding
) -> list[FilesystemPermission] | None:
    """Framework filesystem permissions apply only without an executable sandbox."""

    if isinstance(materialized.backend, SandboxBackendProtocol):
        return None
    return _permissions(binding)


def _subagent_specs(materialized: MaterializedDeepAgentArguments) -> list[SubAgent]:
    """Sync subagent specs; their framework permissions follow the parent's rule.

    deepagents refuses filesystem permissions beside an executable sandbox, so a child's
    rules apply only without one (the parent's `_effective_permissions` rule).
    """

    if not isinstance(materialized.backend, SandboxBackendProtocol):
        return cast(list[SubAgent], list(materialized.subagents))
    return [
        cast(SubAgent, {key: value for key, value in spec.items() if key != "permissions"})
        for spec in materialized.subagents
    ]


def _compile(
    materialized: MaterializedDeepAgentArguments,
    binding: DeepAgentExecutionBinding,
    *,
    system_prompt: str,
    extra_middleware: list[AgentMiddleware[Any, Any, Any]],
    name: str,
) -> Any:
    """The only `create_deep_agent` call: local cognition and hosted async graphs share it.

    A hosted graph has no checkpointer or store (the Agent Server owns them) and no structured
    response; local cognition always has the registered pair.
    """

    return create_deep_agent(
        model=materialized.model,
        system_prompt=system_prompt,
        tools=list(materialized.tools),
        middleware=[*materialized.middleware, *extra_middleware],
        subagents=_subagent_specs(materialized),
        skills=list(materialized.skills),
        permissions=_effective_permissions(materialized, binding),
        backend=materialized.backend,
        state_schema=materialized.state_schema,
        context_schema=materialized.context_schema,
        checkpointer=materialized.checkpointer,
        store=materialized.store,
        response_format=materialized.response_format,
        name=name,
    )


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
        parent_checkpoint_id=checkpoint_parent_id(item),
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
            root_checkpoint_config(plan.namespace, source.checkpoint_id)
        )
        if recorded is None:
            raise CheckpointLineageInDoubt(
                "the recorded source checkpoint is missing", reason="missing_checkpoint"
            )
        if recorded.metadata.get(STAMP_STATE_SCHEMA_DIGEST) != plan.state_schema_digest:
            raise IncompatibleCheckpointSchema(
                "source checkpoint state-schema stamp differs from the binding (REQ-CP-CS-007)"
            )
        if checkpoint_parent_id(recorded) != source.parent_checkpoint_id:
            raise CheckpointLineageInDoubt(
                "the source checkpoint's ancestry differs from its record",
                reason="ancestry_mismatch",
            )
        source_key = _qualified(plan, recorded)
    items: dict[str, CheckpointTuple] = {}
    async for item in checkpointer.alist(thread_config):
        if len(items) >= MAX_LINEAGE_WALK:
            raise CheckpointLineageInDoubt(
                "the checkpointer lineage cannot be classified", reason="unclassifiable"
            )
        items[str(item.config["configurable"]["checkpoint_id"])] = item
    parents = {checkpoint_id: checkpoint_parent_id(item) for checkpoint_id, item in items.items()}

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
                "the accepted descendant is not a stamped root-namespace descendant of the source",
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
    snapshot = await agent.aget_state(root_checkpoint_config(plan.namespace, leaf_id))
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
            own[str(item.config["configurable"]["checkpoint_id"])] = checkpoint_parent_id(item)
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
    return root_checkpoint_config(plan.namespace, tips[0])


async def _terminal_result_may_exist(
    checkpointer: BaseCheckpointSaver[Any], agent: Any, plan: CheckpointInvocationPlan
) -> tuple[bool, tuple[QualifiedCheckpointKey, ...], CheckpointCapture | None]:
    """After a failure: whether a terminal stamped result may exist, with its candidates, and
    the latest durable checkpoint of a unique stamped lineage (RRM-008) if there is one."""

    try:
        classified = await _classify(checkpointer, agent, plan)
    except CheckpointLineageInDoubt as error:
        return True, error.candidates, None
    except Exception:  # noqa: BLE001 - an unclassifiable lineage is not provably non-terminal
        return True, (), None
    if classified.kind == CheckpointClassification.NOT_SUBMITTED:
        return False, (), None
    assert classified.leaf_id is not None and classified.leaf_snapshot is not None
    snapshot = classified.leaf_snapshot
    try:
        latest: CheckpointCapture | None = await _capture_result(
            checkpointer,
            plan,
            classified.source_key,
            snapshot_config=snapshot.config,
            pending=bool(snapshot.next or snapshot.interrupts),
            classification=classified.kind,
            summary={
                "state_keys": sorted(dict(snapshot.values)),
                "message_count": len(dict(snapshot.values).get("messages", [])),
                "step": (snapshot.metadata or {}).get("step"),
            },
            allow_pending=True,
        )
    except CheckpointLineageError:
        latest = None
    if classified.kind != CheckpointClassification.TERMINAL_UNOBSERVED:
        return False, (), latest
    leaf = await checkpointer.aget_tuple(root_checkpoint_config(plan.namespace, classified.leaf_id))
    return True, ((_qualified(plan, leaf),) if leaf is not None else ()), latest


def _latest_checkpoint_reader(
    checkpointer: BaseCheckpointSaver[Any], plan: CheckpointInvocationPlan
) -> Any:
    """A reader of the namespace's latest durable root checkpoint, keys only (heartbeat)."""

    thread_config: RunnableConfig = {
        "configurable": {"thread_id": plan.namespace, "checkpoint_ns": ROOT_CHECKPOINT_NS}
    }

    async def read() -> dict[str, str | None] | None:
        latest = await checkpointer.aget_tuple(thread_config)
        if latest is None:
            return None
        return {
            "checkpointer_ref_digest": plan.checkpointer_ref_digest,
            "thread_id": plan.namespace,
            "checkpoint_ns": ROOT_CHECKPOINT_NS,
            "checkpoint_id": str(latest.config["configurable"]["checkpoint_id"]),
            "parent_checkpoint_id": checkpoint_parent_id(latest),
        }

    return read


def _within(path: str, slot: str) -> bool:
    normalized = slot.rstrip("/")
    return path == normalized or path.startswith(normalized + "/")


async def _slot_files(
    backend: BackendProtocol, slots: tuple[str, ...], state: dict[str, Any]
) -> list[tuple[str, bytes]]:
    """Every file under the exclusive writable slots, from the state or the sandbox."""

    files: list[tuple[str, bytes]] = []
    if isinstance(backend, StateBackend):
        state_files = state.get("files", {})
        for path in sorted(state_files):
            if not any(_within(path, slot) for slot in slots):
                continue
            file_data = state_files[path]
            text = file_data_to_string(file_data)
            content = (
                text.encode("utf-8")
                if file_data.get("encoding", "utf-8") == "utf-8"
                else base64.standard_b64decode(text)
            )
            files.append((path, content))
    elif isinstance(backend, SandboxBackendProtocol):
        paths: list[str] = []
        for slot in slots:
            paths.extend(await _list_sandbox_files(backend, slot, depth=0))
        downloaded = (
            await backend.adownload_files(sorted(paths))
            if hasattr(backend, "adownload_files")
            else backend.download_files(sorted(paths))
        )
        for item in downloaded:
            if item.error is None and item.content is not None:
                files.append((item.path, item.content))
    total = sum(len(content) for _path, content in files)
    if len(files) > MAX_CAPTURED_OUTPUT_FILES or total > MAX_CAPTURED_OUTPUT_BYTES:
        raise DeepAgentMaterializationError(
            "writable-slot outputs exceed the capture bound "
            f"({MAX_CAPTURED_OUTPUT_FILES} files, {MAX_CAPTURED_OUTPUT_BYTES} bytes)"
        )
    return files


async def _list_sandbox_files(
    backend: SandboxBackendProtocol, path: str, *, depth: int
) -> list[str]:
    if depth > MAX_CAPTURE_LISTING_DEPTH:
        return []
    listing = await backend.als(path) if hasattr(backend, "als") else backend.ls(path)
    if listing.error is not None or not listing.entries:
        return []
    found: list[str] = []
    for entry in listing.entries:
        entry_path = str(entry["path"])
        if entry.get("is_dir"):
            found.extend(await _list_sandbox_files(backend, entry_path, depth=depth + 1))
        else:
            found.append(entry_path)
    return found


async def _capture_result(
    checkpointer: BaseCheckpointSaver[Any],
    plan: CheckpointInvocationPlan,
    source_key: QualifiedCheckpointKey | None,
    *,
    snapshot_config: RunnableConfig,
    pending: bool,
    summary: dict[str, object],
    classification: CheckpointClassification = CheckpointClassification.NOT_SUBMITTED,
    allow_pending: bool = False,
) -> CheckpointCapture:
    """Capture the result config and verify a fully stamped root lineage to the source.

    `allow_pending` (RRM-008) captures the latest durable checkpoint of an interrupted unit
    as the result checkpoint of a `cancelled` or `failed` settlement: partial evidence is
    preserved and the namespace head advances, but nothing is resumed or promoted.
    """

    if pending and not allow_pending:
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
        if cursor is None or stamped >= MAX_LINEAGE_WALK:
            raise CheckpointLineageInDoubt("result checkpoint does not descend from the source")
        item = await checkpointer.aget_tuple(root_checkpoint_config(plan.namespace, cursor))
        if item is None:
            raise CheckpointLineageInDoubt("a checkpoint in the result lineage is missing")
        if any(item.metadata.get(key) != value for key, value in stamps.items()):
            raise CheckpointLineageInDoubt(
                "a checkpoint between source and result lacks this invocation's stamps"
            )
        if stamped == 0:
            result_parent = checkpoint_parent_id(item)
        stamped += 1
        cursor = checkpoint_parent_id(item)
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


def _usage(
    invocation: RuntimeInvocation,
    messages: list[BaseMessage],
    subordinate_calls: tuple[dict[str, object], ...] = (),
) -> RuntimeUsage:
    turns = 0
    total_tokens = 0
    for message in messages:
        if not isinstance(message, AIMessage):
            continue
        turns += 1
        metadata: Mapping[str, Any] = message.usage_metadata or {}
        total_tokens += int(metadata.get("total_tokens", 0))
    for call in subordinate_calls:
        turns += 1
        total_tokens += int(cast(int, call["total_tokens"]))
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


def _message_usage(messages: list[BaseMessage]) -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "message_id": str(message.id or ""),
            "scope": "operation",
            **_token_counts(message.usage_metadata or {}),
        }
        for message in messages
        if isinstance(message, AIMessage)
    )


def _token_counts(metadata: Mapping[str, Any]) -> dict[str, int]:
    return {
        "input_tokens": int(metadata.get("input_tokens", 0)),
        "output_tokens": int(metadata.get("output_tokens", 0)),
        "total_tokens": int(metadata.get("total_tokens", 0)),
    }


class _ModelCallObserver(BaseCallbackHandler):
    """Every chat-model completion of the invocation, parent and in-process children alike.

    Recorded at the model boundary with the provider-reported token counts and the message
    identity, never the content. `run_inline` keeps the handler on the event loop thread.
    """

    run_inline = True

    def __init__(self) -> None:
        super().__init__()
        self._calls: dict[str, dict[str, object]] = {}

    def on_llm_end(self, response: Any, **kwargs: Any) -> None:
        del kwargs
        for generations in getattr(response, "generations", ()):
            for generation in generations:
                message = getattr(generation, "message", None)
                if not isinstance(message, AIMessage) or not message.id:
                    continue
                self._calls[str(message.id)] = {
                    "message_id": str(message.id),
                    **_token_counts(message.usage_metadata or {}),
                }

    def calls_outside(self, messages: list[BaseMessage]) -> tuple[dict[str, object], ...]:
        """The observed calls whose message never entered the parent's state."""

        parent = {str(message.id) for message in messages if message.id}
        return tuple(
            self._calls[identity] for identity in sorted(self._calls) if identity not in parent
        )


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


def _append_usage(
    existing: list[dict[str, Any]] | None, update: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    merged = list(existing or [])
    seen = {str(item.get("message_id")) for item in merged}
    merged.extend(item for item in update if str(item.get("message_id")) not in seen)
    return merged


class _ServedGraphState(TypedDict):
    belllabs_served_graph: NotRequired[dict[str, Any]]
    belllabs_provider_usage: Annotated[NotRequired[list[dict[str, Any]]], _append_usage]


class ServedGraphIdentityMiddleware(AgentMiddleware[Any, Any, Any]):
    """Stamp the hosted graph's exact identity and its model usage into every thread it serves.

    REQ-CP-DA-019: the parent reads `belllabs_served_graph` from the provider's thread state
    to verify the served identity on every reconnect and to qualify the result checkpoint.
    REQ-CP-DA-011: `belllabs_provider_usage` records each model call's provider-reported
    token usage (keyed by message id) inside the server process, where it is present; the
    Agent Server's serialized message payloads omit `usage_metadata`, so the parent attributes
    usage from this channel and treats its absence as pending, never as zero.
    """

    state_schema = _ServedGraphState

    def __init__(self, served: AsyncServedGraphIdentity) -> None:
        super().__init__()
        self._stamp = served.model_dump(mode="json")

    def before_agent(self, state: Any, runtime: Runtime[Any]) -> dict[str, Any] | None:
        del state, runtime
        return {"belllabs_served_graph": dict(self._stamp)}

    async def abefore_agent(self, state: Any, runtime: Runtime[Any]) -> dict[str, Any] | None:
        del state, runtime
        return {"belllabs_served_graph": dict(self._stamp)}

    def after_model(self, state: Any, runtime: Runtime[Any]) -> dict[str, Any] | None:
        del runtime
        return _usage_stamp(state)

    async def aafter_model(self, state: Any, runtime: Runtime[Any]) -> dict[str, Any] | None:
        del runtime
        return _usage_stamp(state)


def _usage_stamp(state: Any) -> dict[str, Any] | None:
    messages = state.get("messages") if isinstance(state, dict) else None
    if not messages:
        return None
    last = messages[-1]
    if not isinstance(last, AIMessage) or not last.usage_metadata:
        return None
    usage: Mapping[str, Any] = last.usage_metadata
    return {
        "belllabs_provider_usage": [
            {
                "message_id": str(last.id or ""),
                "input_tokens": int(usage.get("input_tokens", 0)),
                "output_tokens": int(usage.get("output_tokens", 0)),
                "total_tokens": int(usage.get("total_tokens", 0)),
            }
        ]
    }
