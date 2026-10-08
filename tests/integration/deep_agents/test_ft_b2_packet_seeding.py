"""FT-B2: Context Packet inputs reach a real Deep Agent before its first model call.

Runs an actual ``create_deep_agent`` graph on the text-only ``StateBackend`` with a scripted
local model (no provider calls): the packet's materialized input and ``.mission/`` files are
seeded into the invocation's ``files`` input, so the agent's first-turn ``ls`` and
``read_file`` see them. Output refs are the attempt's registered captures only.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from typing import Any

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from tests.acceptance.control_plane.test_wp_cp_040 import (
    exact_fixture,
    registry,
    unit_bound,
)
from tests.fixtures.checkpoint_lineage import activity_attempt
from tests.unit.operations.test_operation_execution import operation_request

from mission_control.adapters.deep_agents.adapter import (
    DeepAgentRuntimeAdapter,
    _attempt_output_refs,
)
from mission_control.adapters.deep_agents.materializer import (
    ExactDeepAgentMaterializer,
    seed_backend_files,
)
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from mission_control.application.execution.operations.operation_execution import (
    bind_operation_execution_request,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.context.refs import durable_input_locator, workspace_candidate_ref
from mission_control.domain.execution.contracts import (
    MaterializedWorkspace,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    RuntimeInvocation,
    WorkspaceContract,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)

SOURCES = '{"records": [{"pmid": "41"}]}'
CONTEXT_MD = "# Mission context packet\n\n| binding | ... |\n"


def _digest(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


class Reader:
    def __init__(self, files: dict[str, bytes]) -> None:
        self.objects = {
            durable_input_locator(f"file-artifact://{_digest(c)[7:]}", _digest(c), len(c)): c
            for c in files.values()
        }
        self.requests: list[str] = []

    async def retrieve(self, durable_ref: str) -> bytes:
        self.requests.append(durable_ref)
        return self.objects[durable_ref]


class InputListingModel(BaseChatModel):
    observed: list[str] = []

    @property
    def _llm_type(self) -> str:
        return "ft-b2-input-listing"

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        results = [item for item in messages if isinstance(item, ToolMessage)]
        if not results:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "ls",
                        "args": {"path": "/inputs/sources"},
                        "id": "ls-inputs",
                        "type": "tool_call",
                    },
                    {
                        "name": "read_file",
                        "args": {"file_path": "/.mission/context.md", "limit": 50},
                        "id": "read-index",
                        "type": "tool_call",
                    },
                ],
            )
        else:
            InputListingModel.observed = [str(item.content) for item in results]
            message = AIMessage(content="inputs observed")
        return ChatResult(generations=[ChatGeneration(message=message)])


def _context_slots(owner: WorkspaceOwner) -> tuple[WorkspaceSlotBinding, ...]:
    files = {
        "/inputs/sources/source_manifest.json": SOURCES.encode(),
        "/.mission/context.md": CONTEXT_MD.encode(),
    }
    return tuple(
        WorkspaceSlotBinding(
            slot_name=f"ctx-{index}",
            logical_path=path,
            access="read_only",
            owner=owner,
            durable_ref=durable_input_locator(
                f"file-artifact://{_digest(content)[7:]}", _digest(content), len(content)
            ),
            content_digest=_digest(content),
        )
        for index, (path, content) in enumerate(sorted(files.items()))
    )


def _invocation_with_context_inputs() -> tuple[RuntimeInvocation, Any, Any]:
    binding, _profile, bundle = exact_fixture()
    owner = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="ft-b2")
    workspace = WorkspaceContract.model_validate(
        {
            **binding.workspace.model_dump(mode="python"),
            "workflow_contract_digest": sha256_digest("ft-b2-contract"),
            "slot_bindings": (
                *(
                    WorkspaceSlotBinding(
                        slot_name="output",
                        logical_path=path,
                        access="exclusive_write",
                        owner=owner,
                    )
                    for path in binding.workspace.exclusive_write_paths
                ),
                *_context_slots(owner),
            ),
        }
    )
    binding = unit_bound(
        binding.__class__.create(
            **{
                **binding.model_dump(mode="python", exclude={"binding_digest"}),
                "workspace": workspace,
            }
        )
    )
    payload = operation_request().model_dump(mode="python")
    payload.update(
        identity=OperationAttemptIdentity(
            run_id=binding.run_id,
            operation_id=binding.operation_id,
            operation_attempt=binding.operation_attempt,
        ),
        execution_runtime="deep_agent",
        native_placement=None,
        deep_agent_binding=binding,
        runtime_unit=binding.runtime_unit,
        workspace=workspace,
    )
    request = OperationExecutionRequest.model_validate(payload)
    invocation = RuntimeInvocation(
        binding=bind_operation_execution_request(request),
        prompt_segments=request.prompt_segments,
        workspace=MaterializedWorkspace(
            workspace_id=workspace.workspace_id,
            namespace_id=workspace.namespace_id,
            provider=workspace.provider,
            runtime_digest=workspace.runtime_digest,
            image_digest=workspace.image_digest,
            mount_manifest_digest=sha256_digest("mounts"),
        ),
    )
    return invocation, binding, bundle


@pytest.mark.asyncio
async def test_packet_inputs_are_seeded_before_the_first_model_call() -> None:
    invocation, binding, bundle = _invocation_with_context_inputs()
    reader = Reader(
        {
            "/inputs/sources/source_manifest.json": SOURCES.encode(),
            "/.mission/context.md": CONTEXT_MD.encode(),
        }
    )
    model = InputListingModel()
    adapter = DeepAgentRuntimeAdapter(
        ExactDeepAgentMaterializer(registry(binding, bundle, model)), context_inputs=reader
    )
    lineage = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    plan = await lineage.observe_attempt(invocation.binding, activity_attempt(1), dispatching=True)
    result = await adapter.execute(invocation.model_copy(update={"checkpoint_plan": plan}), {})

    assert result.output_text == "inputs observed"
    listing, index = InputListingModel.observed
    assert "/inputs/sources/source_manifest.json" in listing
    assert "Mission context packet" in index
    assert len(reader.requests) == 2
    assert result.output_refs == ()


@pytest.mark.asyncio
async def test_seeding_refuses_a_digest_mismatch() -> None:
    invocation, binding, bundle = _invocation_with_context_inputs()
    reader = Reader({"x": b"unrelated"})
    reader.objects = dict.fromkeys(_all_refs(invocation), b"tampered")
    adapter = DeepAgentRuntimeAdapter(
        ExactDeepAgentMaterializer(registry(binding, bundle, InputListingModel())),
        context_inputs=reader,
    )
    with pytest.raises(Exception, match="digest mismatch"):
        await adapter._context_seed_files(invocation)


@pytest.mark.asyncio
async def test_bound_context_inputs_without_a_reader_fail_closed() -> None:
    invocation, binding, bundle = _invocation_with_context_inputs()
    adapter = DeepAgentRuntimeAdapter(
        ExactDeepAgentMaterializer(registry(binding, bundle, InputListingModel()))
    )
    with pytest.raises(Exception, match="no durable input reader"):
        await adapter._context_seed_files(invocation)


@pytest.mark.asyncio
async def test_sandbox_backends_receive_uploads_and_state_backends_receive_files() -> None:
    from deepagents.backends import StateBackend
    from deepagents.backends.protocol import FileUploadResponse

    class UploadRecorder:
        def __init__(self) -> None:
            self.uploads: list[tuple[str, bytes]] = []

        async def aupload_files(self, files: list[tuple[str, bytes]]) -> list[Any]:
            self.uploads.extend(files)
            return [FileUploadResponse(path=path, error=None) for path, _ in files]

    files = (("/inputs/a/b.json", b"{}"), ("/.mission/context.md", b"# x"))
    recorder = UploadRecorder()
    assert await seed_backend_files(recorder, files) == {}  # type: ignore[arg-type]
    assert recorder.uploads == list(files)
    state_files = await seed_backend_files(StateBackend(), files)
    assert set(state_files) == {"/inputs/a/b.json", "/.mission/context.md"}
    with pytest.raises(Exception, match="byte-capable"):
        await seed_backend_files(StateBackend(), (("/inputs/x.png", b"\xff\xfe\x00"),))


def test_output_refs_are_the_attempts_captures_and_foreign_refs_are_dropped() -> None:
    captured: list[dict[str, object]] = [
        {"candidate_id": "cand-b", "logical_path": "/outputs/b.json"},
        {"candidate_id": "cand-a", "logical_path": "/outputs/a.json"},
    ]
    emitted = {
        "summary": "done",
        "output_refs": [workspace_candidate_ref("cand-a"), "artifact://made-up"],
    }
    refs, structured, warnings = _attempt_output_refs(captured, emitted, drop_unregistered=True)
    assert refs == (workspace_candidate_ref("cand-a"), workspace_candidate_ref("cand-b"))
    assert structured == {"summary": "done", "output_refs": list(refs)}
    assert warnings == [
        {
            "kind": "provenance",
            "dropped_output_ref": "artifact://made-up",
            "reason": "not_registered_by_this_attempt",
        }
    ]
    refs, structured, warnings = _attempt_output_refs([], {"summary": "no refs"})
    assert (refs, structured, warnings) == ((), {"summary": "no refs"}, [])
    # Default (lenient): the structured output is untouched, the registered refs ride on
    # the result, and the unregistered one is flagged rather than dropped.
    refs, structured, warnings = _attempt_output_refs(captured, emitted)
    assert refs == (workspace_candidate_ref("cand-a"), workspace_candidate_ref("cand-b"))
    assert structured == emitted
    assert warnings == [
        {
            "kind": "provenance",
            "unregistered_output_ref": "artifact://made-up",
            "reason": "not_registered_by_this_attempt",
        }
    ]


def _all_refs(invocation: RuntimeInvocation) -> list[str]:
    return [
        slot.durable_ref
        for slot in invocation.binding.workspace.slot_bindings
        if slot.durable_ref is not None
    ]
