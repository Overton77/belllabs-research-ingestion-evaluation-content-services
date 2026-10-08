"""FT-B3: each GoalDirected iteration and role starts from a sealed Context Packet.

Deterministic (in-memory run control, journal and lineage; a real ``create_deep_agent`` graph
with the RRM-016 scripted model). Proves: no stringified handoff segment remains; the
executor of iteration 2 receives the prior Progress Review inline (as untrusted data), the
sealed handoff as a mandatory journal reference, Loop State and blockers as mandatory inline
items and the handoff snapshot files under its role root; the verifier receives an
independent packet with only the executor's registered outputs; the handoff records the
packet it was produced from and ``GoalHandoffReference`` is written from it.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from mission_control.adapters.storage.artifact_payloads import InMemoryArtifactPayloadStore
from mission_control.application.context.pack_service import (
    ContextPackRejected,
    ContextPackService,
    StaticModelBudgetProfiles,
    context_packet_ref,
    goal_handoff_reference,
)
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
    InMemoryCheckpointLineageRepository,
)
from mission_control.application.execution.operations.operation_execution import (
    InMemoryOperationBindingRepository,
)
from mission_control.application.programs.goal_directed import _bind_handoff
from mission_control.domain.context.packet import (
    ContextPacket,
    ContextSourceKind,
    ContextTrust,
    ExpansionTier,
    ModelBudgetProfile,
)
from mission_control.domain.context.render import is_context_input_slot
from mission_control.domain.execution.contracts import PromptTrustClass
from mission_control.domain.policies.contracts import CommandStatus
from mission_control.domain.programs.contracts import GoalHandoff
from mission_control.domain.programs.goal_directed_runtime import GoalHandoffDraft
from tests.fixtures.checkpoint_recovery import MemoryOperationJournal
from tests.fixtures.goal_directed_journaled import (
    SCOPE,
    GoalComposition,
    GoalScriptedModel,
    admit_goal_run,
    compose_goal_directed,
    goal_blueprint,
    goal_run_control,
    goal_start_action,
)
from tests.unit.operations.test_ft_b2_stage_handoff import (
    FakeArtifacts,
    FakeSelections,
    FakeStaging,
)
from tests.unit.orchestration.test_rrm_016_goal_directed_settlement import (
    _claim,
    _preparation,
    _run_iteration_one,
)
from tests.unit.run_control.test_run_control import command

DRAFT_SECRET = "PRIVATE-EXECUTOR-REASONING: try the hidden tactic first"


async def _composition(
    profile: ModelBudgetProfile | None = None,
) -> tuple[GoalComposition, str, FakeSelections]:
    run_control = goal_run_control()
    selections = FakeSelections()
    staging = FakeStaging()
    packs = ContextPackService(
        artifacts=FakeArtifacts({}, scope=None),
        selections=selections,
        staging=staging,
        profiles=StaticModelBudgetProfiles(default=profile) if profile else None,
    )
    composition = await compose_goal_directed(
        run_control=run_control,
        journal=MemoryOperationJournal(),
        lineage=CheckpointLineageService(InMemoryCheckpointLineageRepository()),
        results=InMemoryArtifactPayloadStore(),
        bindings=InMemoryOperationBindingRepository(),
        saver=InMemorySaver(),
        model=GoalScriptedModel(),
        blueprint=goal_blueprint(),
        context_packs=packs,
        context_inputs=staging,
    )
    run_id = await admit_goal_run(run_control, "ft-b3-goal-packet")
    started = await run_control.execute(
        command(run_id, 1, "ft-b3-start", goal_start_action(run_id))
    )
    assert started.status == CommandStatus.ACCEPTED
    return composition, run_id, selections


def _packet(selections: FakeSelections, role: str, iteration: int) -> ContextPacket:
    return next(
        packet
        for packet, _selection in selections.rows.values()
        if packet.target.node_key == f"goal/{role}"
        and packet.target.activation_id == f"goal-iteration/{iteration}/{role}"
    )


def _bound_handoff(claim: Any) -> GoalHandoff:
    draft = GoalHandoffDraft(
        accepted_fact_refs=("fact:ft-b3:1",),
        evidence_refs=("evidence:ft-b3:1",),
        artifact_refs=("artifact:ft-b3:draft-report",),
        attempted_tactics=("search PubMed for NAD+ trials",),
        rejected_tactics=(("scrape preprints", "out of scope"),),
        unresolved_obligations=("obligation:human-trials",),
        blockers=("rate limited by the provider",),
        context_selection_refs=("selection:model-claimed",),
        compaction_decision_ref="compaction:ft-b3:1",
        continuation_instructions=f"Continue with human trials. {DRAFT_SECRET}",
    )
    blueprint = goal_blueprint()
    policy = blueprint.session_policy
    return _bind_handoff(
        draft,
        claim=claim,
        operation_identity=f"{claim.identity.semantic_key}:executor",
        actual_usage={"tokens.total": 5},
        remaining_iterations=1,
        protected_fact_classes=tuple(sorted(policy.protected_fact_classes)),
        context_selection_policy_ref=policy.context_selection_policy_ref,
        context_compaction_policy_ref=policy.context_compaction_policy_ref,
        workspace_ref_class="goal-workspace",
        context_packet="context-packet:sha256:" + "a" * 64,
    )


@pytest.mark.asyncio
async def test_iteration_one_runs_both_roles_from_packets_and_the_verifier_is_independent():
    composition, run_id, selections = await _composition()

    observed = await _run_iteration_one(composition, run_id)

    for role in ("executor", "verifier"):
        operation = observed[f"{role}_dispatch"].workflow_request.operation
        refs = [segment.source_ref for segment in operation.prompt_segments]
        assert not any(ref.startswith("goal-context:") for ref in refs)  # no str(dict)
        packet_segments = [
            segment
            for segment in operation.prompt_segments
            if segment.source_ref.startswith("context-packet:")
        ]
        assert len(packet_segments) == 1
        assert packet_segments[0].trust_class == PromptTrustClass.ADMITTED_INPUT
        assert context_packet_ref(operation.prompt_segments) == packet_segments[0].source_ref
        context_slots = [s for s in operation.workspace.slot_bindings if is_context_input_slot(s)]
        assert {slot.logical_path for slot in context_slots} >= {
            f"/goal/1/{role}/.mission/context.md",
            f"/goal/1/{role}/.mission/inputs.json",
        }
    executor_packet = _packet(selections, "executor", 1)
    verifier_packet = _packet(selections, "verifier", 1)
    assert executor_packet.packet_digest != verifier_packet.packet_digest
    # The verifier sees the executor's registered outputs (provisional), nothing it wrote.
    verifier_kinds = {item.source_kind for item in verifier_packet.items}
    assert verifier_kinds <= {
        ContextSourceKind.OPERATING_CONTRACT,
        ContextSourceKind.GOALS_AND_CRITERIA,
        ContextSourceKind.WORKSPACE_MAP,
        ContextSourceKind.ACCEPTED_OUTPUT,
    }
    outputs = [
        i for i in verifier_packet.items if i.source_kind == ContextSourceKind.ACCEPTED_OUTPUT
    ]
    assert [item.source_ref for item in outputs] == ["artifact:rrm016:record"]
    assert all(item.provenance.provisional and not item.mandatory for item in outputs)
    # Every role's packet is recorded with its selection record.
    assert len(selections.rows) == 2


@pytest.mark.asyncio
async def test_iteration_two_packet_carries_review_journal_head_loop_state_and_snapshots():
    composition, run_id, selections = await _composition()
    claim = await _claim(run_id)
    handoff = _bound_handoff(claim)
    request = _preparation(run_id, claim, "executor", 2, 0)
    request = request.model_copy(
        update={"goal_iteration": 2, "handoff_ref": handoff.handoff_id, "handoff": handoff}
    )

    dispatch = await composition.family.execute_iteration(request)

    packet = _packet(selections, "executor", 2)
    by_kind: dict[ContextSourceKind, list[Any]] = {}
    for item in packet.items:
        by_kind.setdefault(item.source_kind, []).append(item)
    (loop_state,) = by_kind[ContextSourceKind.LOOP_STATE]
    assert loop_state.tier == ExpansionTier.INLINE and loop_state.mandatory
    (journal,) = by_kind[ContextSourceKind.JOURNAL_DIGEST]
    assert journal.tier == ExpansionTier.REFERENCE and journal.mandatory
    assert journal.reference is not None
    assert journal.reference.retrieval.command == f"missionctl journal read {journal.source_ref}"
    assert handoff.handoff_digest in journal.source_ref
    (blockers,) = by_kind[ContextSourceKind.BLOCKER]
    assert blockers.mandatory and blockers.tier == ExpansionTier.INLINE
    review = next(i for i in by_kind[ContextSourceKind.PROGRESS_REVIEW] if i.inline is not None)
    assert review.trust == ContextTrust.UNTRUSTED_CONTENT and not review.mandatory
    assert DRAFT_SECRET in review.inline.text
    # The model-authored draft is a fenced data block inside the one admitted_input segment.
    operation = dispatch.workflow_request.operation
    segment = next(
        s for s in operation.prompt_segments if s.source_ref.startswith("context-packet:")
    )
    assert segment.trust_class == PromptTrustClass.ADMITTED_INPUT
    assert "trust=untrusted_content kind=progress_review" in segment.content
    # The handoff snapshot files keep their policy paths under the role root.
    snapshots = {
        item.materialize.path: item
        for item in packet.items
        if item.materialize is not None and item.source_ref.startswith("journal://")
    }
    assert set(snapshots) == {
        "/goal/2/executor/goal/HANDOFF.md",
        "/goal/2/executor/goal/checkpoint.json",
    }
    slots = {slot.logical_path: slot for slot in operation.workspace.slot_bindings}
    for path, item in snapshots.items():
        assert slots[path].content_digest == item.content_digest
    # The handoff's artifacts are candidates bound to the reducer-bound handoff.
    artifact = next(i for i in packet.items if i.source_ref == "artifact:ft-b3:draft-report")
    assert artifact.provenance.accepted_decision_ref == handoff.handoff_id
    assert packet.producer_refs == (handoff.handoff_id,)


@pytest.mark.asyncio
async def test_verifier_packet_contains_no_executor_prompt_or_handoff_text():
    composition, run_id, selections = await _composition()
    claim = await _claim(run_id)
    handoff = _bound_handoff(claim)
    executor = await composition.family.execute_iteration(
        _preparation(run_id, claim, "executor", 2, 0).model_copy(
            update={"goal_iteration": 2, "handoff_ref": handoff.handoff_id, "handoff": handoff}
        )
    )
    del executor
    packet = _packet(selections, "executor", 2)
    from mission_control.domain.context.render import render_context_index

    assert DRAFT_SECRET in render_context_index(packet)
    # A verifier preparation for the same iteration never carries the handoff.
    result = replace(_executor_result(claim, run_id), workspace_id=f"{claim.workspace_namespace}")
    service = composition.family._operations._context_packs
    assert service is not None
    verifier_request = _preparation(run_id, claim, "verifier", 3, 1, result)
    template = composition.templates["verifier"]
    sealed = await service.pack_for_iteration(
        verifier_request, template, role_root="/goal/1/verifier"
    )
    assert DRAFT_SECRET not in sealed.prompt_segment.content
    assert "progress_review" not in {item.source_kind.value for item in sealed.packet.items}


@pytest.mark.asyncio
async def test_mandatory_loop_state_overflow_rejects_the_admission():
    tiny = ModelBudgetProfile(
        model_profile_ref="tiny@1",
        tokenizer_ref="unknown",
        context_window=420,
        reserved_output=10,
    )
    composition, run_id, selections = await _composition(profile=tiny)
    claim = await _claim(run_id)
    handoff = _bound_handoff(claim)
    request = _preparation(run_id, claim, "executor", 2, 0).model_copy(
        update={"goal_iteration": 2, "handoff_ref": handoff.handoff_id, "handoff": handoff}
    )
    with pytest.raises(Exception) as raised:
        await composition.family.execute_iteration(request)
    assert "CONTEXT_BUDGET_EXCEEDED" in str(raised.value)
    assert isinstance(raised.value, ContextPackRejected) or "CONTEXT_BUDGET_EXCEEDED" in getattr(
        raised.value, "type", ""
    )
    assert (await composition.run_control.get_run(SCOPE, run_id)).version == 2
    assert selections.rows == {}


def test_handoff_records_its_packet_and_writes_the_goal_handoff_reference():
    import asyncio

    claim = asyncio.run(_claim("ft-b3-reference-run"))
    handoff = _bound_handoff(claim)
    packet_ref = "context-packet:sha256:" + "a" * 64
    assert handoff.context_selection_refs[0] == packet_ref
    assert "selection:model-claimed" in handoff.context_selection_refs
    reference = goal_handoff_reference(handoff, request_scope=SCOPE)
    assert reference is not None
    assert reference.content_digest == "sha256:" + "a" * 64
    assert reference.artifact_ref == f"context-packet://sha256:{'a' * 64}"
    assert reference.checkpoint.goal_iteration == handoff.source_iteration.goal_iteration
    assert reference.checkpoint.belllabs_run_id == handoff.run_id


def _executor_result(claim: Any, run_id: str) -> Any:
    from mission_control.domain.programs.contracts import GoalExecutionResult

    return GoalExecutionResult(
        identity=claim.identity,
        disposition="completed",
        operation_identity=f"{claim.identity.semantic_key}:executor",
        operation_binding_ref="binding:executor",
        session_id=claim.session_id,
        workspace_id=claim.workspace_namespace,
        writable_paths=("/goal/1/executor/work",),
        output_refs=("artifact:rrm016:record",),
    )
