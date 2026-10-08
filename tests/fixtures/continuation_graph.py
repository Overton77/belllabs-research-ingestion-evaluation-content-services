"""A compiled Deep Agents graph with forced summarization (FT-B4), fully offline.

``ScriptedWorker`` writes ``/outputs/report.md``, lists ``/inputs`` and answers; any call
whose only message is a summarization prompt returns a fixed summary. On a continuation
thread (first message from Mission Control's checkpoint packet) it lists ``/inputs`` and
answers with what it saw. No provider is contacted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from deepagents import create_deep_agent
from deepagents.backends import StateBackend
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.base import BaseCheckpointSaver

from mission_control.adapters.deep_agents.compaction import (
    CONTINUATION_MESSAGE_SOURCE,
    ObservedSummarizationMiddleware,
)
from mission_control.adapters.deep_agents.frames import DeepAgentFrameRecorder

TASK = "Sweep the literature and draft the report."
SUMMARY = "SUMMARY: report drafted at /outputs/report.md; inputs listed."


class ScriptedWorker(BaseChatModel):
    calls: list[list[str]] = []
    steps: int = 0

    @property
    def _llm_type(self) -> str:
        return "scripted-continuation-fixture"

    def bind_tools(self, tools: Sequence[Any], **kwargs: Any) -> ScriptedWorker:
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: CallbackManagerForLLMRun | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        kinds = [type(message).__name__ for message in messages]
        self.calls.append(kinds)
        if len(self.calls) > 40:
            raise RuntimeError("scripted worker looped")
        first = messages[0] if messages else None
        if (
            len(messages) == 1
            and isinstance(first, HumanMessage)
            and first.content != TASK
            and (first.additional_kwargs.get("lc_source") != CONTINUATION_MESSAGE_SOURCE)
        ):
            return _reply(AIMessage(content=SUMMARY))
        continuation = any(
            (
                isinstance(message, HumanMessage)
                and message.additional_kwargs.get("lc_source") == CONTINUATION_MESSAGE_SOURCE
            )
            or (isinstance(message, ToolMessage) and message.tool_call_id == "cont-ls")
            for message in messages
        )
        tools = [message for message in messages if isinstance(message, ToolMessage)]
        if continuation:
            if not tools:
                return _reply(
                    AIMessage(
                        content="",
                        tool_calls=[{"name": "ls", "args": {"path": "/inputs"}, "id": "cont-ls"}],
                    )
                )
            return _reply(AIMessage(content=f"continued; /inputs holds {tools[-1].content}"))
        self.steps += 1
        if self.steps == 1:
            return _reply(
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "write_file",
                            "args": {"file_path": "/outputs/report.md", "content": "# draft\n"},
                            "id": "write-1",
                        }
                    ],
                )
            )
        if self.steps == 2:
            return _reply(
                AIMessage(
                    content="",
                    tool_calls=[{"name": "ls", "args": {"path": "/inputs"}, "id": "ls-1"}],
                )
            )
        return _reply(AIMessage(content="report drafted"))


def _reply(message: AIMessage) -> ChatResult:
    return ChatResult(generations=[ChatGeneration(message=message)])


def compaction_agent(
    checkpointer: BaseCheckpointSaver[Any],
) -> tuple[Any, ObservedSummarizationMiddleware, StateBackend, ScriptedWorker]:
    model = ScriptedWorker()
    model.calls = []
    backend = StateBackend()
    middleware = ObservedSummarizationMiddleware.observed(
        model, backend, trigger=("messages", 3), keep=("messages", 2)
    )
    agent = create_deep_agent(
        model=model, backend=backend, middleware=[middleware], checkpointer=checkpointer
    )
    return agent, middleware, backend, model


async def stream_turn(
    agent: Any,
    invoke_input: Any,
    config: Mapping[str, Any],
    recorder: DeepAgentFrameRecorder | None,
) -> dict[str, Any]:
    """The adapter's streamed invocation (``astream`` v2 with subgraphs), recorded."""

    latest: Any = None
    async for part in agent.astream(
        invoke_input,
        config,
        stream_mode=["updates", "messages", "custom", "values"],
        subgraphs=True,
        version="v2",
        durability="sync",
    ):
        if part.get("type") == "values":
            if not part.get("ns"):
                latest = part.get("data")
            continue
        if recorder is not None:
            await recorder.observe(part)
    if recorder is not None:
        await recorder.flush()
    return dict(latest) if isinstance(latest, Mapping) else {}
