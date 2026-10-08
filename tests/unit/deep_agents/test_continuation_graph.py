"""FT-B4 on a real compiled Deep Agents graph, offline (scripted model, InMemorySaver).

Forced summarization (``trigger=("messages", 3)``) yields ``before_compaction`` and
``after_compaction`` Provider Frames through the C1 recorder, the reducer derives one
``compaction_observed`` fact per compaction, the offloaded history is archived as a
``conversation_history`` artifact, a ``provider_compaction`` trigger seals a valid
checkpoint, and a fresh thread hydrated from the checkpoint packet lists the restored
``/inputs`` on its first turn.
"""

from __future__ import annotations

from typing import Any

import pytest
from deepagents.backends.utils import create_file_data
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.checkpoint.memory import InMemorySaver

from mission_control.adapters.deep_agents.compaction import (
    CONVERSATION_HISTORY_KIND,
    DeepAgentsSessionHydrator,
    StateBackendWorkspaceSnapshots,
    archive_conversation_history,
)
from mission_control.adapters.deep_agents.frames import DeepAgentFrameRecorder
from mission_control.application.context.continuation import (
    FrameHydrationConfirmation,
    TransferStatus,
)
from mission_control.application.frames.reducer import DeriveContext, derive
from mission_control.domain.context.checkpoint import ContinuationTriggerKind
from mission_control.domain.frames.contracts import FrameKind, LaneProfile
from mission_control.domain.frames.facts import CompactionObservedFact
from tests.fixtures.continuation import (
    CONT_SCOPE,
    RUN_KEY,
    FakeStaging,
    build_service,
    facts,
    seal_target,
    trigger,
)
from tests.fixtures.continuation_graph import TASK, compaction_agent, stream_turn
from tests.fixtures.provider_frames import harness_start, in_memory_store, opened_writer

SOURCE_THREAD = "thread-collect-1"
MANIFEST = '{"records": 180, "source": "pubmed"}'


class Registrar:
    def __init__(self) -> None:
        self.registered: list[dict[str, Any]] = []

    async def register_conversation_history(self, **kwargs: Any) -> str:
        self.registered.append(kwargs)
        return f"artifact://{CONVERSATION_HISTORY_KIND}/{kwargs['content_digest']}"


async def _record_turn(
    store: Any, agent: Any, invoke_input: Any, thread: str, turn: str
) -> tuple[dict[str, Any], Any]:
    writer, handle = await opened_writer(
        store,
        harness_start(run_key=RUN_KEY, activation_key="unit-collect-1", native_session_ref=thread),
    )
    recorder = DeepAgentFrameRecorder(writer, thread_id=thread, invocation_id=turn)
    await recorder.begin(body={})
    result = await stream_turn(
        agent, invoke_input, {"configurable": {"thread_id": thread}}, recorder
    )
    messages = result.get("messages", [])
    await recorder.finish(
        messages=messages,
        own_messages=messages,
        output_text=str(messages[-1].content) if messages else "",
        structured_keys=(),
        checkpoint_id=None,
    )
    return result, handle


@pytest.mark.asyncio
async def test_forced_summarization_seals_a_checkpoint_and_hydrates_a_fresh_thread() -> None:
    store, run_id = in_memory_store(run_key=RUN_KEY, activation_keys=("unit-collect-1",))
    agent, middleware, backend, _model = compaction_agent(InMemorySaver())
    result, _handle = await _record_turn(
        store,
        agent,
        {
            "messages": [HumanMessage(content=TASK)],
            "files": {"/inputs/sources/source_manifest.json": create_file_data(MANIFEST)},
        },
        SOURCE_THREAD,
        "turn-1",
    )
    assert isinstance(result["messages"][-1], AIMessage)

    # 1. Both compaction frames, persisted through the FrameSink in arrival order.
    frames = await store.frames_for_run(CONT_SCOPE, run_id)
    kinds = [frame.kind for frame in frames]
    assert FrameKind.BEFORE_COMPACTION in kinds and FrameKind.AFTER_COMPACTION in kinds
    first_before = kinds.index(FrameKind.BEFORE_COMPACTION)
    assert first_before < kinds.index(FrameKind.AFTER_COMPACTION)
    before = frames[first_before]
    assert before.raw_kind == "custom.mc.before_compaction" and not before.closing
    assert (
        '"pre_estimate_tokens"' in before.body_excerpt and '"cutoff_index"' in before.body_excerpt
    )
    after = next(frame for frame in frames if frame.raw_kind == "custom.mc.after_compaction")
    assert after.closing
    assert f'"file_path":"/conversation_history/{SOURCE_THREAD}.md"' in after.body_excerpt
    assert '"post_estimate_tokens"' in after.body_excerpt
    # 2. One compaction fact per compaction, though two closing frames observed each one.
    facts_derived = derive(
        frames, DeriveContext(lane=LaneProfile.DEEP_AGENTS, current_generation=1)
    )
    compactions = [fact for fact in facts_derived if isinstance(fact, CompactionObservedFact)]
    assert len(compactions) == len(middleware.observations) >= 1
    assert {fact.summary_digest for fact in compactions} == {
        item.summary_digest for item in middleware.observations
    }

    # 3. The offloaded history is archived as a conversation_history artifact.
    registrar = Registrar()
    archived = await archive_conversation_history(
        middleware.observations, registrar, state_files=result["files"]
    )
    assert [item.file_path for item in archived] == [f"/conversation_history/{SOURCE_THREAD}.md"]
    assert archived[0].kind == CONVERSATION_HISTORY_KIND
    assert registrar.registered[0]["content"].startswith(b"## Summarized at")

    # 4. A provider compaction trigger seals a valid checkpoint from the thread's files.
    staging = FakeStaging()

    async def thread_values(thread: str) -> dict[str, Any]:
        state = await agent.aget_state({"configurable": {"thread_id": thread}})
        return dict(state.values)

    snapshots = StateBackendWorkspaceSnapshots(
        thread_values=thread_values, staging=staging, bytes_source=staging
    )

    async def run_ids(scope: str, run_key: str) -> Any:
        return run_id

    wired = build_service(
        staging=staging,
        snapshots=snapshots,
        pending_commands=("cmd-held-1",),
        confirmation=FrameHydrationConfirmation(store, run_ids),
    )
    service = wired["service"]
    transfer = await service.request(
        trigger(ContinuationTriggerKind.PROVIDER_COMPACTION, f"provider_frame:{after.frame_id}"),
        request_scope=CONT_SCOPE,
        run_key=RUN_KEY,
        activation_key="unit-collect-1",
        logical_execution_id="logical-collect-1",
        lane_profile="deep_agents",
        source_session_ref=SOURCE_THREAD,
    )
    sealed = await service.seal(
        transfer.transfer_id, facts(), seal_target(), request_scope=CONT_SCOPE
    )
    assert sealed.checkpoint is not None and sealed.checkpoint.valid
    snapshot = await snapshots.load(
        request_scope=CONT_SCOPE, snapshot_ref=sealed.checkpoint.workspace_snapshot_ref
    )
    assert snapshot is not None
    assert set(snapshot.manifest) == {
        "/inputs/sources/source_manifest.json",
        "/outputs/report.md",
    }

    # 5. Transfer: a new thread seeded from the checkpoint packet.
    hydrator = DeepAgentsSessionHydrator(graph=agent, backend=backend, bytes_source=staging)
    outcome = await service.transfer(transfer.transfer_id, hydrator, request_scope=CONT_SCOPE)
    target = outcome.transfer.target_session_ref
    assert outcome.transfer.status == TransferStatus.TRANSFERRED
    assert target is not None and target != SOURCE_THREAD and target.startswith(SOURCE_THREAD)
    assert outcome.receipt is not None
    assert {
        path: digest
        for path, digest in outcome.receipt.restored.items()
        if not path.startswith("/.mission/")
    } == dict(snapshot.manifest)
    assert "/.mission/context.md" in outcome.receipt.restored
    actions = wired["events"].actions
    assert [item.event for item in actions] == ["checkpoint_sealed", "transferred"]
    assert actions[-1].source_session_ref == SOURCE_THREAD
    assert actions[-1].target_session_ref == target
    # Held commands wait for the target's first session_init frame.
    assert not outcome.transfer.released

    seeded = await agent.aget_state({"configurable": {"thread_id": target}})
    first = seeded.values["messages"][0]
    assert first.additional_kwargs["checkpoint_id"] == sealed.checkpoint.checkpoint_id
    assert "Pending" in first.content and "draft evidence map" in first.content
    assert seeded.values["files"]["/.mission/context.md"]["content"]

    # 6. The fresh session's first turn reads the restored /inputs.
    continued, _handle = await _record_turn(store, agent, None, target, "turn-2")
    listing = next(message for message in continued["messages"] if isinstance(message, ToolMessage))
    assert "/inputs/sources" in str(listing.content)
    assert "continued; /inputs holds" in str(continued["messages"][-1].content)
    # The source thread's history was never replayed into the new thread.
    assert not any(
        isinstance(message, HumanMessage) and message.content == TASK
        for message in continued["messages"]
    )

    released = await service.release_if_hydrated(transfer.transfer_id, request_scope=CONT_SCOPE)
    assert released.released
    assert wired["mailbox"].released[transfer.transfer_id] == ("cmd-held-1",)
