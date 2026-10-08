"""FT-B2: a StageGraph consumer stage receives its producers' accepted outputs as a packet."""

from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from typing import Any

import pytest

from mission_control.application.context.pack_service import (
    ArtifactContent,
    ContextPackRejected,
    ContextPackService,
    StaticModelBudgetProfiles,
)
from mission_control.application.programs.service import (
    StageGraphOperationPreparationService,
    StaticStageGraphOperationTemplateProvider,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.context.packet import (
    ContextPacket,
    ExpansionTier,
    ModelBudgetProfile,
    OmissionReason,
)
from mission_control.domain.context.refs import durable_input_locator, workspace_candidate_ref
from mission_control.domain.context.render import (
    ContextSelectionRecord,
    is_context_input_slot,
)
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    PromptTrustClass,
    WorkspaceContract,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from mission_control.domain.programs.contracts import (
    ExecutionIdentity,
    LateResultFacts,
    StageGraphAdmissionActivityRequest,
    StageInputBinding,
    StageResultObservation,
)
from mission_control.domain.programs.interpreter import StageGraphInterpreter
from tests.integration.temporal.test_wp_bp_010_temporal import (
    DIGEST,
    NOW,
    RecordingOperationBindings,
    _blueprint,
    _operation,
)

RUN_ID = "run-ft-b2"
SCOPE = "mc/0192a4f0-0000-7000-8000-00000000b10e/biotech/6f1e2d3c-0000-5000-8000-000000000001"
FAST_TEXT = '{"sources": [{"pmid": "1"}, {"pmid": "2"}]}'
FAST_REF = workspace_candidate_ref("cand-fast-result")


def _bytes_digest(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


class FakeArtifacts:
    def __init__(self, contents: dict[str, ArtifactContent], *, scope: str | None = SCOPE) -> None:
        self.contents = contents
        self.scope = scope

    async def capture(
        self, source_ref: str, *, request_scope: str, max_text_bytes: int
    ) -> ArtifactContent | None:
        assert self.scope is None or request_scope == self.scope
        content = self.contents.get(source_ref)
        if content is None or content.text is None or content.size_bytes <= max_text_bytes:
            return content
        return replace(content, text=None)


class FakeSelections:
    def __init__(self) -> None:
        self.rows: dict[tuple[object, ...], tuple[ContextPacket, ContextSelectionRecord]] = {}

    async def record(
        self, packet: ContextPacket, selection: ContextSelectionRecord, *, request_scope: str
    ) -> ContextPacket:
        target = packet.target
        key = (
            request_scope,
            target.run_id,
            target.activation_id,
            target.attempt_no,
            target.generation,
            target.purpose,
        )
        stored = self.rows.setdefault(key, (packet, selection))
        if stored[0].packet_digest != packet.packet_digest:
            raise AssertionError("conflicting packet for target")
        return stored[0]

    async def get(self, packet_id: str, *, request_scope: str) -> ContextPacket | None:
        return next((p for p, _ in self.rows.values() if p.packet_id == packet_id), None)


class FakeStaging:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def stage(self, *, request_scope: str, name: str, content: bytes, media_type: str) -> str:
        digest = "sha256:" + hashlib.sha256(content).hexdigest()
        object_ref = f"file-artifact://{digest.removeprefix('sha256:')}"
        self.objects[object_ref] = content
        return durable_input_locator(object_ref, digest, len(content))

    async def retrieve(self, durable_ref: str) -> bytes:
        return self.objects[durable_ref.partition("#")[0]]


def _service(
    contents: dict[str, ArtifactContent] | None = None,
    profile: ModelBudgetProfile | None = None,
) -> tuple[ContextPackService, FakeSelections, FakeStaging]:
    selections = FakeSelections()
    staging = FakeStaging()
    service = ContextPackService(
        artifacts=FakeArtifacts(
            contents
            if contents is not None
            else {
                FAST_REF: ArtifactContent(
                    source_ref=FAST_REF,
                    durable_ref=durable_input_locator(
                        "file-artifact://fast", _bytes_digest(FAST_TEXT), len(FAST_TEXT)
                    ),
                    content_digest=_bytes_digest(FAST_TEXT),
                    size_bytes=len(FAST_TEXT),
                    media_type="application/json",
                    file_name="sources.json",
                    text=FAST_TEXT,
                )
            }
        ),
        selections=selections,
        staging=staging,
        profiles=StaticModelBudgetProfiles(default=profile) if profile else None,
    )
    return service, selections, staging


def _admit(interpreter: StageGraphInterpreter, projection: Any, proposal: Any) -> Any:
    return interpreter.apply_admission(
        projection,
        proposal,
        next_run_version=projection.run_version + 1,
        next_family_version=projection.family_version + 1,
    )


def _downstream_request(output_refs: list[str]) -> StageGraphAdmissionActivityRequest:
    """Admit and accept the `fast` producer, then return the `downstream` admission."""

    graph = _blueprint()
    interpreter = StageGraphInterpreter(graph, effective_max_concurrency=3)
    projection = interpreter.initial_projection(identity=ExecutionIdentity(RUN_ID), run_version=2)
    frontier = interpreter.frontier(projection, available_concurrency=3)
    fast = next(item for item in frontier if item.identity.stage_id == "fast")
    projection = _admit(interpreter, projection, fast)
    observation = StageResultObservation(
        identity=fast.identity,
        operation_result={"output_refs": output_refs},
        child_closed_or_quiesced=True,
        reservations_and_usage_settled=True,
        effects_settled=True,
        cancellation_reconciled=True,
        accepted_order=1,
    )
    decision = interpreter.result_decision(fast.identity, LateResultFacts())
    projection = interpreter.apply_result_decision(
        projection,
        observation,
        decision,
        next_run_version=projection.run_version + 1,
        next_family_version=projection.family_version + 1,
    )
    frontier = interpreter.frontier(projection, available_concurrency=3)
    downstream = next(item for item in frontier if item.identity.stage_id == "downstream")
    return StageGraphAdmissionActivityRequest(
        run_id=RUN_ID,
        request_scope=SCOPE,
        projection=projection,
        proposal=downstream,
        operation=None,
        blueprint=graph.model_dump(mode="json"),
        effective_max_concurrency=3,
        occurred_at=NOW,
        idempotency_issuer="stagegraph-worker",
        correlation_id="stagegraph:ft-b2",
        semantic_input_binding_ref="semantic-input:test",
        effective_configuration_digest=DIGEST,
    )


def _compiled_template(request: StageGraphAdmissionActivityRequest) -> OperationExecutionRequest:
    template = _operation(request).operation
    stage_path = f"/stages/{request.proposal.identity.stage_id}"
    owner = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="compiled")
    workspace = WorkspaceContract.model_validate(
        {
            **template.workspace.model_dump(mode="python"),
            "workflow_contract_digest": DIGEST,
            "slot_bindings": (
                WorkspaceSlotBinding(
                    slot_name="output",
                    logical_path=stage_path,
                    access="exclusive_write",
                    owner=owner,
                ),
            ),
        }
    )
    return template.model_copy(update={"workspace": workspace})


# -- interpreter ---------------------------------------------------------------------------


def test_input_bindings_keep_the_slot_mapping_and_refs_stay_byte_identical():
    request = _downstream_request([FAST_REF, "workspace-candidate://cand-fast-extra"])
    proposal = request.proposal

    assert proposal.frozen_input_refs == tuple(
        sorted([FAST_REF, "workspace-candidate://cand-fast-extra"], key=str.encode)
    )
    assert proposal.frozen_input_bindings == tuple(
        StageInputBinding(
            consumer_input_slot_id="fast-input",
            producer_stage_key="fast",
            producer_output_slot_id="result",
            artifact_ref=ref,
            accepted_decision_ref=proposal.frozen_input_bindings[0].accepted_decision_ref,
        )
        for ref in proposal.frozen_input_refs
    )
    accepted = proposal.frozen_input_bindings[0].accepted_decision_ref
    assert accepted is not None and accepted.startswith("stage-result:run-ft-b2:operation:")


def test_projection_digest_is_unchanged_while_bindings_are_empty():
    graph = _blueprint()
    interpreter = StageGraphInterpreter(graph, effective_max_concurrency=3)
    projection = interpreter.initial_projection(identity=ExecutionIdentity(RUN_ID), run_version=2)
    legacy = asdict(projection)
    legacy.pop("run_version")
    for stage in legacy["stages"].values():
        stage.pop("frozen_input_bindings")
    assert projection.digest == sha256_digest(legacy)

    admitted = _admit(interpreter, projection, _downstream_request([FAST_REF]).proposal)
    assert admitted.digest != projection.digest


def test_admission_carries_bindings_onto_the_stage_instance():
    request = _downstream_request([FAST_REF])
    graph = _blueprint()
    interpreter = StageGraphInterpreter(graph, effective_max_concurrency=3)
    admitted = _admit(interpreter, request.projection, request.proposal)
    stage = admitted.stages[request.proposal.identity.candidate.semantic_prefix]
    assert stage.frozen_input_bindings == request.proposal.frozen_input_bindings
    assert stage.frozen_input_refs == request.proposal.frozen_input_refs


# -- stage preparation ---------------------------------------------------------------------


async def test_preparation_replaces_the_objective_segment_with_one_packet_segment():
    request = _downstream_request([FAST_REF])
    request = replace(
        request, proposal=replace(request.proposal, objective_override="Synthesize the sources.")
    )
    template = _compiled_template(request)
    service, selections, _staging = _service()
    preparation = StageGraphOperationPreparationService(
        templates=StaticStageGraphOperationTemplateProvider(
            {request.proposal.operation_request_key: template}
        ),
        operation_bindings=RecordingOperationBindings(),  # type: ignore[arg-type]
        context_packs=service,
    )

    prepared = await preparation.materialize(request)

    segments = prepared.operation.prompt_segments
    packet_segments = [s for s in segments if s.source_ref.startswith("context-packet:")]
    assert len(packet_segments) == 1
    assert packet_segments[0].trust_class == PromptTrustClass.ADMITTED_INPUT
    assert not any(s.source_ref.startswith("stage-cycle-objective:") for s in segments)
    assert "Synthesize the sources." in packet_segments[0].content
    (packet, selection) = next(iter(selections.rows.values()))
    assert selection.prompt_plan_digest == packet_segments[0].rendered_digest
    assert packet.target.node_key == "downstream"
    sources = next(item for item in packet.items if item.source_ref == FAST_REF)
    assert sources.binding_name == "fast-input"
    assert sources.provenance.accepted_decision_ref is not None


async def test_materialized_outputs_join_the_compiled_slots_as_read_only_inputs():
    request = _downstream_request([FAST_REF])
    big = "x" * 70_000
    contents = {
        FAST_REF: ArtifactContent(
            source_ref=FAST_REF,
            durable_ref=durable_input_locator("file-artifact://big", _bytes_digest(big), len(big)),
            content_digest=_bytes_digest(big),
            size_bytes=len(big),
            media_type="application/json",
            file_name="source_manifest.json",
            text=big,
        )
    }
    service, selections, staging = _service(contents)
    template = _compiled_template(request)
    preparation = StageGraphOperationPreparationService(
        templates=StaticStageGraphOperationTemplateProvider(
            {request.proposal.operation_request_key: template}
        ),
        operation_bindings=RecordingOperationBindings(),  # type: ignore[arg-type]
        context_packs=service,
    )

    prepared = await preparation.materialize(request)

    (packet, _selection) = next(iter(selections.rows.values()))
    item = next(i for i in packet.items if i.source_ref == FAST_REF)
    assert item.tier == ExpansionTier.MATERIALIZE
    slots = {slot.logical_path: slot for slot in prepared.operation.workspace.slot_bindings}
    source_slot = slots["/inputs/fast-input/source_manifest.json"]
    assert is_context_input_slot(source_slot)
    assert source_slot.durable_ref == contents[FAST_REF].durable_ref
    assert source_slot.content_digest == contents[FAST_REF].content_digest
    for path in ("/.mission/context.md", "/.mission/inputs.json"):
        slot = slots[path]
        assert is_context_input_slot(slot)
        object_ref = slot.durable_ref.partition("#")[0]  # type: ignore[union-attr]
        assert "sha256:" + hashlib.sha256(staging.objects[object_ref]).hexdigest() == (
            slot.content_digest
        )
    compiled = [
        slot
        for slot in prepared.operation.workspace.slot_bindings
        if not is_context_input_slot(slot)
    ]
    assert [slot.slot_name for slot in compiled] == ["output"]
    deep = prepared.operation.deep_agent_binding
    assert deep is None  # native fixture; Deep Agents bindings get the same workspace


async def test_uncompiled_workspace_gets_references_and_no_extra_slots():
    request = _downstream_request([FAST_REF])
    big = "y" * 70_000
    contents = {
        FAST_REF: ArtifactContent(
            source_ref=FAST_REF,
            durable_ref=durable_input_locator("file-artifact://y", _bytes_digest(big), len(big)),
            content_digest=_bytes_digest(big),
            size_bytes=len(big),
            media_type="application/json",
        )
    }
    service, selections, _staging = _service(contents)
    template = _operation(request).operation
    preparation = StageGraphOperationPreparationService(
        templates=StaticStageGraphOperationTemplateProvider(
            {request.proposal.operation_request_key: template}
        ),
        operation_bindings=RecordingOperationBindings(),  # type: ignore[arg-type]
        context_packs=service,
    )

    prepared = await preparation.materialize(request)

    assert prepared.operation.workspace.slot_bindings == ()
    (packet, _selection) = next(iter(selections.rows.values()))
    assert next(i for i in packet.items if i.source_ref == FAST_REF).tier == (
        ExpansionTier.REFERENCE
    )


async def test_retry_reproduces_the_identical_packet_and_binding():
    request = _downstream_request([FAST_REF])
    template = _compiled_template(request)
    service, selections, _staging = _service()
    bindings = RecordingOperationBindings()
    preparation = StageGraphOperationPreparationService(
        templates=StaticStageGraphOperationTemplateProvider(
            {request.proposal.operation_request_key: template}
        ),
        operation_bindings=bindings,  # type: ignore[arg-type]
        context_packs=service,
    )
    first = await preparation.materialize(request)
    second = await preparation.materialize(request)
    assert first == second
    assert len(selections.rows) == 1


async def test_unaccepted_output_is_omitted_and_provisional_is_never_mandatory():
    request = _downstream_request([FAST_REF])
    unaccepted = replace(request.proposal.frozen_input_bindings[0], accepted_decision_ref=None)
    request = replace(
        request, proposal=replace(request.proposal, frozen_input_bindings=(unaccepted,))
    )
    service, _selections, _staging = _service()
    sealed = await service.pack_for_stage(request, _compiled_template(request))
    (omission,) = [o for o in sealed.packet.omitted if o.source_ref == FAST_REF]
    assert omission.reason == OmissionReason.NOT_ACCEPTED

    provisional = replace(unaccepted, provisional=True)
    request = replace(
        request, proposal=replace(request.proposal, frozen_input_bindings=(provisional,))
    )
    service, _selections, _staging = _service()
    sealed = await service.pack_for_stage(request, _compiled_template(request))
    item = next(i for i in sealed.packet.items if i.source_ref == FAST_REF)
    assert item.provenance.provisional and not item.mandatory


async def test_unresolvable_model_emitted_refs_stay_references_without_bytes():
    request = _downstream_request(["artifact:model-made-this-up"])
    service, _selections, _staging = _service({})
    sealed = await service.pack_for_stage(request, _compiled_template(request))
    item = next(i for i in sealed.packet.items if i.binding_name == "fast-input")
    assert item.tier == ExpansionTier.REFERENCE
    assert item.bytes == 0


async def test_budget_overflow_rejects_admission_before_any_provider_work():
    request = _downstream_request([FAST_REF])
    request = replace(
        request, proposal=replace(request.proposal, objective_override="word " * 2_000)
    )
    tiny = ModelBudgetProfile(
        model_profile_ref="tiny@1",
        tokenizer_ref="unknown",
        context_window=1_000,
        reserved_output=100,
    )
    service, selections, _staging = _service(profile=tiny)
    bindings = RecordingOperationBindings()
    preparation = StageGraphOperationPreparationService(
        templates=StaticStageGraphOperationTemplateProvider(
            {request.proposal.operation_request_key: _compiled_template(request)}
        ),
        operation_bindings=bindings,  # type: ignore[arg-type]
        context_packs=service,
    )
    with pytest.raises(ContextPackRejected) as raised:
        await preparation.materialize(request)
    assert raised.value.code == "CONTEXT_BUDGET_EXCEEDED"
    assert bindings.bindings == []
    assert selections.rows == {}


def test_context_input_slots_are_recognized_only_when_read_only_and_digest_bound():
    owner = WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id="s")
    bound = WorkspaceSlotBinding(
        slot_name="ctx-sources-0123456789ab",
        logical_path="/inputs/sources/a.json",
        access="read_only",
        owner=owner,
        durable_ref="file-artifact://a#sha256:" + "0" * 64 + ":1",
        content_digest="sha256:" + "0" * 64,
    )
    assert is_context_input_slot(bound)
    assert not is_context_input_slot(bound.model_copy(update={"slot_name": "sources"}))
    writable = WorkspaceSlotBinding(
        slot_name="ctx-out", logical_path="/out", access="exclusive_write", owner=owner
    )
    assert not is_context_input_slot(writable)


async def test_packets_of_a_fork_are_reuse_compatible_with_their_source():
    """REQ-CP-EXEC-012: bindings compare modulo the run id, so packet identity is run-relative."""

    from mission_control.application.execution.operations.operation_execution import (
        bind_operation_execution_request,
    )
    from mission_control.application.recovery.run_forks import reuse_compatibility_digest

    prepared = []
    for run_id in ("0199b8f0-0000-7000-8000-00000000aaaa", "0199b8f0-0000-7000-8000-00000000bbbb"):
        request = _downstream_request([FAST_REF])
        request = replace(
            request,
            run_id=run_id,
            proposal=replace(
                request.proposal,
                identity=replace(request.proposal.identity, run_id=run_id),
            ),
        )
        template = _compiled_template(request)
        service, _selections, _staging = _service()
        preparation = StageGraphOperationPreparationService(
            templates=StaticStageGraphOperationTemplateProvider(
                {request.proposal.operation_request_key: template}
            ),
            operation_bindings=RecordingOperationBindings(),  # type: ignore[arg-type]
            context_packs=service,
        )
        prepared.append((await preparation.materialize(request)).operation)
    source, derived = prepared
    assert source.prompt_segments[-1].source_ref == derived.prompt_segments[-1].source_ref
    assert reuse_compatibility_digest(
        bind_operation_execution_request(source)
    ) == reuse_compatibility_digest(bind_operation_execution_request(derived))
