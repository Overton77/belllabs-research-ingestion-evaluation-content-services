"""RRM-009: a real in-process sync subagent in the canonical adapter, its usage and lineage.

Hermetic: the exact fixture binding, the canonical materializer and `create_deep_agent`, an
in-memory saver and deterministic models. It pins three production-composition findings:

* the materializer hands deepagents typed `FilesystemPermission` rules for each child (a
  mapping failed `create_deep_agent` for every binding with a sync subagent);
* the child's model calls are charged to the parent operation (REQ-CP-DA-007), observed at the
  chat-model boundary because they never enter the parent's messages;
* the sanitized capability lineage records the invocation without argument values.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool

from app.application.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from app.domain.control_plane.contracts import DefinitionKind, SecretRef
from app.domain.operation_execution.contracts import (
    DeepAgentModelComponent,
    SyncSubagentProfile,
)
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
)
from app.integrations.agents.deep_agents.capability_lineage import (
    CAPABILITY_LINEAGE_KIND,
    credential_references,
)
from tests.acceptance.control_plane.test_wp_cp_040 import (
    exact,
    exact_fixture,
    planned_invocation,
    registry,
    unit_bound,
)

CHILD = "technical-child"
SECRET_ARGUMENT = "do-not-persist-this-argument-value"


def _usage(total: int) -> dict[str, int]:
    return {"input_tokens": total - 1, "output_tokens": 1, "total_tokens": total}


class _Scripted(BaseChatModel):
    calls: int = 0

    def bind_tools(self, tools: Sequence[BaseTool | dict[str, Any] | type | Any], **kwargs: Any):  # type: ignore[no-untyped-def,override]
        del tools, kwargs
        return self

    def _generate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs):  # type: ignore[no-untyped-def,override]
        del stop, run_manager, kwargs
        self.calls += 1
        return ChatResult(generations=[ChatGeneration(message=self._reply(messages))])

    async def _agenerate(self, messages: list[BaseMessage], stop=None, run_manager=None, **kwargs):  # type: ignore[no-untyped-def,override]
        return self._generate(messages, stop, run_manager, **kwargs)

    def _reply(self, messages: list[BaseMessage]) -> AIMessage:
        raise NotImplementedError


class ParentModel(_Scripted):
    @property
    def _llm_type(self) -> str:
        return "rrm009-parent"

    def _reply(self, messages: list[BaseMessage]) -> AIMessage:
        if not any(isinstance(item, ToolMessage) for item in messages):
            return AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "task",
                        "args": {"description": SECRET_ARGUMENT, "subagent_type": CHILD},
                        "id": "call-task-1",
                        "type": "tool_call",
                    }
                ],
                usage_metadata=_usage(5),  # type: ignore[arg-type]
            )
        child = next(item for item in messages if isinstance(item, ToolMessage))
        return AIMessage(
            content=json.dumps({"child": str(child.content)}),
            usage_metadata=_usage(7),  # type: ignore[arg-type]
        )


class ChildModel(_Scripted):
    @property
    def _llm_type(self) -> str:
        return "rrm009-child"

    def _reply(self, messages: list[BaseMessage]) -> AIMessage:
        del messages
        return AIMessage(content="CHILD-OK", usage_metadata=_usage(11))  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_sync_subagent_runs_and_its_usage_is_charged_to_the_parent() -> None:
    child_model_ref = exact(DefinitionKind.MODEL, "model.rrm009-unit-child", "child")
    child_prompt_ref = exact(DefinitionKind.PROMPT, "prompt.rrm009-unit-child", "child-prompt")
    child = SyncSubagentProfile(
        name=CHILD,
        description="A bounded in-process technical child.",
        system_prompt_ref=child_prompt_ref,
        model=DeepAgentModelComponent(
            ref=child_model_ref, provider="openai", model_name="fixture-child"
        ),
        state_slice_id="child-state",
        context_slice_id="child-context",
        workspace_id="workspace-child",
        writable_paths=("/workspace/child",),
    )
    binding, _profile, bundle = exact_fixture(sync_subagents=(child,), with_child_slices=True)
    parent, child_model = ParentModel(), ChildModel()
    base = registry(binding, bundle, parent)
    exact_registry = ExactComponentRegistry(
        model_factories={
            **base.model_factories,
            child_model_ref.digest: lambda _binding, _secrets: child_model,
        },
        prompts={child_prompt_ref.digest: "Reply CHILD-OK."},
        skill_bundles=base.skill_bundles,
        sandbox_factories=base.sandbox_factories,
        checkpointers=base.checkpointers,
        stores=base.stores,
    )
    adapter = DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(exact_registry))
    lineage_service = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    invocation = await planned_invocation(unit_bound(binding), lineage_service)
    invocation = invocation.model_copy(
        update={
            "binding": invocation.binding.model_copy(
                update={"secret_refs": (SecretRef(provider="environment", key="OPENAI_API_KEY"),)}
            )
        }
    )

    result = await adapter.execute(invocation, {})

    assert json.loads(result.output_text) == {"child": "CHILD-OK"}
    assert (parent.calls, child_model.calls) == (2, 1)
    # Parent turns 2 (5 + 7 tokens) plus the child's turn (11 tokens).
    expected = {
        key: value
        for key, value in {"model.turns": 3, "tokens.total": 23}.items()
        if key in invocation.binding.budget_limits
    }
    assert expected and result.usage.amounts == expected
    lineage = result.event_payloads[0]["capability_lineage"]
    assert isinstance(lineage, dict)
    assert lineage["kind"] == CAPABILITY_LINEAGE_KIND
    assert lineage["invoked"] == {"sync_subagent": ["task"]}
    (task,) = lineage["invocations"]
    assert task["subagent_type"] == CHILD and task["status"] == "success"
    assert task["argument_names"] == ["description", "subagent_type"]
    scopes = [call["scope"] for call in lineage["usage"]["model_calls"]]
    assert sorted(scopes) == ["operation", "operation", "subordinate"]
    assert lineage["usage"]["amounts"] == expected
    assert lineage["credential_refs"] == ["environment:OPENAI_API_KEY"]
    assert lineage["mounted"]["sync_subagents"][0]["writable_paths"] == ["/workspace/child"]
    assert lineage["placement"]["checkpointer_ref"]["digest"] == binding.checkpointer_ref.digest
    # Sanitized: argument values never enter the record, only their names and digest.
    assert SECRET_ARGUMENT not in json.dumps(lineage)


def test_credential_references_are_names_only_and_cover_every_mount() -> None:
    binding, _profile, _bundle = exact_fixture(
        include_mcp=True,
        sandbox_credentials=(SecretRef(provider="environment", key="SANDBOX_TOKEN"),),
    )
    assert credential_references(
        binding, (SecretRef(provider="environment", key="OPENAI_API_KEY"),)
    ) == ["environment:OPENAI_API_KEY", "environment:SANDBOX_TOKEN"]
