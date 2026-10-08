"""Deep Agents frame writer: a real `create_deep_agent` run streams frames before derivation.

Offline: deterministic fake chat models, `InMemorySaver`, the in-memory checkpoint lineage
and the in-memory FrameStore with the Postgres semantics (C1 acceptance shape).
"""

from __future__ import annotations

from typing import Any
from uuid import uuid5

import pytest
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage
from langgraph.types import Interrupt

from mission_control.adapters.deep_agents import DeepAgentRuntimeAdapter, ExactDeepAgentMaterializer
from mission_control.adapters.deep_agents.frames import DeepAgentFrameRecorder
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.application.frames.kinds import UnknownKindCounter
from mission_control.application.frames.sink import InMemoryFrameStore
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import (
    MaterializedWorkspace,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    RuntimeInvocation,
)
from mission_control.domain.frames.contracts import FrameKind
from tests.acceptance.control_plane.test_wp_cp_040 import (
    SkillReadingModel,
    exact_fixture,
    registry,
)
from tests.fixtures.checkpoint_lineage import activity_attempt, bind_unit, stage_unit
from tests.fixtures.provider_frames import (
    INSTALLATION,
    SCOPE,
    in_memory_store,
    opened_writer,
)
from tests.unit.operations.test_operation_execution import operation_request


def _unit_binding():  # type: ignore[no-untyped-def]
    binding, _profile, bundle = exact_fixture()
    unit = stage_unit(request_scope=SCOPE, run_id=binding.run_id, operation_id=binding.operation_id)
    return bind_unit(binding, unit), bundle, unit


async def _planned(binding: Any, lineage: CheckpointLineageService) -> RuntimeInvocation:
    payload = operation_request().model_dump(mode="python")
    payload.update(
        request_scope=SCOPE,
        identity=OperationAttemptIdentity(
            run_id=binding.run_id,
            operation_id=binding.operation_id,
            operation_attempt=binding.operation_attempt,
        ),
        execution_runtime="deep_agent",
        native_placement=None,
        deep_agent_binding=binding,
        runtime_unit=binding.runtime_unit,
    )
    request = OperationExecutionRequest.model_validate(payload)
    bound = bind_operation_execution_request(request)
    plan = await lineage.observe_attempt(bound, activity_attempt(1), dispatching=True)
    return RuntimeInvocation(
        binding=bound,
        prompt_segments=request.prompt_segments,
        workspace=MaterializedWorkspace(
            workspace_id=request.workspace.workspace_id,
            namespace_id=request.workspace.namespace_id,
            provider=request.workspace.provider,
            runtime_digest=request.workspace.runtime_digest,
            image_digest=request.workspace.image_digest,
            mount_manifest_digest=sha256_digest("mounts"),
        ),
        checkpoint_plan=plan,
    )


def _store_for(unit: Any) -> InMemoryFrameStore:
    store = InMemoryFrameStore()
    run_id = uuid5(INSTALLATION, unit.belllabs_run_id)
    store.register_run(SCOPE, unit.belllabs_run_id, run_id, {unit.unit_key: uuid5(run_id, "a")})
    return store


@pytest.mark.asyncio
async def test_actual_deep_agent_run_leaves_an_ordered_deduplicated_frame_sequence() -> None:
    binding, bundle, unit = _unit_binding()
    store = _store_for(unit)
    model = SkillReadingModel()
    adapter = DeepAgentRuntimeAdapter(
        ExactDeepAgentMaterializer(registry(binding, bundle, model)), frames=store
    )
    lineage = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    invocation = await _planned(binding, lineage)
    result = await adapter.execute(
        invocation, {"environment:FIXTURE_TOKEN": "fixture-secret-value"}
    )
    assert result.output_text == "SKILL-MD-IN-MESSAGES-040 observed and followed."

    run_id = uuid5(INSTALLATION, unit.belllabs_run_id)
    frames = await store.frames_for_run(SCOPE, run_id)
    kinds = [frame.kind for frame in frames]
    assert kinds[:2] == [FrameKind.SESSION_INIT, FrameKind.TURN_STARTED]
    assert kinds[-2:] == [FrameKind.TURN_ENDED, FrameKind.RUN_RESULT]
    assert kinds.index(FrameKind.TOOL_CALL_STARTED) < kinds.index(FrameKind.TOOL_CALL_COMPLETED)
    assert FrameKind.MESSAGE in kinds
    assert [frame.arrival_ordinal for frame in frames] == list(range(1, len(frames) + 1))
    tool = next(frame for frame in frames if frame.kind == FrameKind.TOOL_CALL_COMPLETED)
    assert tool.tool_call_ref == "skill-read-040" and tool.raw_kind == "updates.tool_message"
    assert all(frame.native_session_ref == binding.cognitive_session_namespace for frame in frames)
    assert all(
        frame.native_turn_ref == invocation.checkpoint_plan.invocation_id for frame in frames
    )  # type: ignore[union-attr]
    run_result = frames[-1]
    assert '"status":"finished"' in run_result.body_excerpt
    assert "fixture-secret-value" not in "".join(frame.body_excerpt for frame in frames)
    handle_id = frames[0].harness_execution_id
    assert store.turns(handle_id)[0]["ended_frame_id"] == frames[-2].frame_id

    # The same unit re-executed is reconstructed from its terminal checkpoint: the writer
    # re-observes the same turn and adds zero rows.
    again = await adapter.execute(invocation, {})
    assert again.checkpoint is not None and again.checkpoint.classification == "terminal_unobserved"
    assert len(await store.frames_for_run(SCOPE, run_id)) == len(frames)
    assert model.calls == 2


class _Writer:
    def __init__(self) -> None:
        self.observations: list[Any] = []

    async def write(self, observations: Any) -> Any:
        self.observations.extend(observations)


def _recorder(counter: UnknownKindCounter | None = None) -> tuple[DeepAgentFrameRecorder, _Writer]:
    writer = _Writer()
    recorder = DeepAgentFrameRecorder(
        writer,  # type: ignore[arg-type]
        thread_id="thread-1",
        invocation_id="turn-1",
        counter=counter or UnknownKindCounter(),
        delta_flush_bytes=8,
    )
    return recorder, writer


@pytest.mark.asyncio
async def test_stream_parts_map_to_frames_with_subordinate_attribution() -> None:
    recorder, writer = _recorder()
    subagent_ns = ("tools:task-123", "model:abc")
    await recorder.observe(
        {
            "type": "messages",
            "ns": subagent_ns,
            "data": (
                AIMessageChunk(content="Searching the literature", id="sub-msg"),
                {"langgraph_step": 2, "lc_agent_name": "researcher"},
            ),
        }
    )
    await recorder.observe(
        {
            "type": "updates",
            "ns": subagent_ns,
            "data": {
                "model": {
                    "messages": [
                        AIMessage(
                            content="",
                            id="sub-msg",
                            tool_calls=[
                                {"name": "pubmed_search", "args": {"q": "rapamycin"}, "id": "c-9"}
                            ],
                            usage_metadata={
                                "input_tokens": 10,
                                "output_tokens": 2,
                                "total_tokens": 12,
                            },
                        )
                    ]
                }
            },
        }
    )
    await recorder.observe(
        {
            "type": "updates",
            "ns": (),
            "data": {
                "tools": {
                    "messages": [
                        ToolMessage(content="boom", tool_call_id="c-8", status="error"),
                    ]
                },
                "SummarizationMiddleware.before_model": {"_summarization_event": {"cut": 4}},
                "__interrupt__": (Interrupt(value={"action": "approve"}, id="int-1"),),
            },
        }
    )
    await recorder.observe(
        {"type": "custom", "ns": (), "data": {"type": "mc.hook_result", "id": "h1"}}
    )
    await recorder.observe({"type": "custom", "ns": (), "data": {"progress": 3}})
    kinds = [item.kind for item in writer.observations]
    assert kinds == [
        FrameKind.MESSAGE_DELTA,
        FrameKind.MESSAGE,
        FrameKind.TOOL_CALL_STARTED,
        FrameKind.USAGE,
        FrameKind.TOOL_CALL_FAILED,
        FrameKind.AFTER_COMPACTION,
        FrameKind.APPROVAL_REQUESTED,
        FrameKind.SESSION_STATE,
        FrameKind.HOOK_RESULT,
        FrameKind.UNKNOWN,
    ]
    sub = writer.observations[:4]
    assert {item.subordinate_ref for item in sub} == {"researcher"}
    assert all(item.subordinate_ref is None for item in writer.observations[4:])
    assert writer.observations[2].tool_call_ref == "c-9"
    assert writer.observations[4].tool_call_ref == "c-8"
    assert recorder.unknown == 1
    keys = [item.provider_key for item in writer.observations]
    assert len(set(keys)) == len(keys)
    assert keys[0].startswith("thread-1:tools:task-123|model:abc::2:sub-msg#0")


@pytest.mark.asyncio
async def test_backfill_closes_tool_results_the_stream_never_delivered() -> None:
    recorder, writer = _recorder()
    await recorder.observe(
        {
            "type": "updates",
            "ns": (),
            "data": {"tools": {"messages": [ToolMessage(content="ok", tool_call_id="seen")]}},
        }
    )
    messages = [
        ToolMessage(content="ok", tool_call_id="seen"),
        ToolMessage(content="late", tool_call_id="missed"),
    ]
    await recorder.finish(
        messages=messages,
        own_messages=[],
        output_text="done",
        structured_keys=(),
        checkpoint_id="cp-1",
    )
    backfill = [item for item in writer.observations if item.raw_kind == "checkpoint_backfill"]
    assert [item.tool_call_ref for item in backfill] == ["missed"]
    assert backfill[0].kind == FrameKind.TOOL_CALL_COMPLETED
    assert [item.kind for item in writer.observations][-2:] == [
        FrameKind.TURN_ENDED,
        FrameKind.RUN_RESULT,
    ]


@pytest.mark.asyncio
async def test_failure_records_error_then_run_result_unless_state_is_unknown() -> None:
    recorder, writer = _recorder()
    await recorder.fail(RuntimeError("model exploded"), unknown_state=False)
    assert [item.kind for item in writer.observations] == [FrameKind.ERROR, FrameKind.RUN_RESULT]
    assert "model exploded" not in str([item.body for item in writer.observations])
    recorder, writer = _recorder()
    await recorder.fail(RuntimeError("lineage"), unknown_state=True)
    assert [item.kind for item in writer.observations] == [FrameKind.ERROR]
    assert writer.observations[0].body["unknown_state"] is True


@pytest.mark.asyncio
async def test_recorder_through_the_real_writer_dedupes_replayed_parts() -> None:
    store, run_id = in_memory_store()
    writer, _handle = await opened_writer(store)
    recorder = DeepAgentFrameRecorder(writer, thread_id="thread-1", invocation_id="turn-1")
    part = {
        "type": "updates",
        "ns": (),
        "data": {"model": {"messages": [AIMessage(content="hi", id="m-1")]}},
    }
    await recorder.observe(part)
    await recorder.observe(part)
    frames = await store.frames_for_run(SCOPE, run_id)
    assert [frame.kind for frame in frames] == [FrameKind.MESSAGE]
    assert recorder.receipt.duplicate == 1
