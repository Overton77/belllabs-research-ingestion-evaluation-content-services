"""RRM-004: checkpoint and settlement crash windows converge to one settlement.

REQ-CP-DA-018 and `CON-CP-CHECKPOINT-LINEAGE-V1` (classification and crash-window tables),
REQ-CP-EXEC-005/014 (claim lease, fence, generation boundary), REQ-CP-RUN-007 (narrowed
post-dispatch rule, `in_doubt`, operator reconciliation). A real Deep Agent graph runs
through the whole operation boundary (see `tests/fixtures/checkpoint_recovery.py`).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import pytest
from langchain_core.messages import HumanMessage, ToolMessage
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import CheckpointTuple
from langgraph.checkpoint.base.id import uuid6

from app.application.operations.journaled_operation_execution import _effect_claim_id
from app.application.operations.operation_execution import (
    OperationExecutionInProgress,
    OperationLeaseExpired,
    bind_operation_execution_request,
)
from app.application.operations.unit_reconciliation import UnitReconciliationRejected
from app.domain.operation_execution.checkpoint_lineage import (
    STAMP_INVOCATION_ID,
    CheckpointClassification,
    CheckpointLineageConflict,
    StaleClaimFence,
    UnitResultObservation,
    submission_invocation_id,
    unit_result_observation_id,
)
from app.domain.operation_execution.contracts import OperationExecutionRequest
from app.domain.run_control.contracts import (
    ClaimEffectAction,
    CommandStatus,
    EffectDisposition,
    PauseAction,
    PauseDecision,
    ReconcileUnitAction,
    ResumeAction,
    ResumeDecision,
    RunPhase,
    operator_reconciliation_condition_id,
)
from tests.fixtures.checkpoint_recovery import (
    RESULT_MARKER,
    RecoveryHarness,
    SimulatedWorkerCrash,
    recovery_harness,
    result_digest,
    run_control_authority,
    stage_recovery_unit,
)
from tests.unit.run_control.test_run_control import command, reconciler_command

# --- helpers -------------------------------------------------------------------------


def _root(namespace: str, checkpoint_id: str | None = None) -> RunnableConfig:
    configurable: dict[str, Any] = {"thread_id": namespace, "checkpoint_ns": ""}
    if checkpoint_id is not None:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


async def _root_checkpoints(harness: RecoveryHarness, namespace: str) -> list[CheckpointTuple]:
    return [item async for item in harness.saver.alist(_root(namespace))]


async def _chain(harness: RecoveryHarness, namespace: str, leaf_id: str) -> list[CheckpointTuple]:
    """Root checkpoints from `leaf_id` back to the first, following parent links."""

    chain: list[CheckpointTuple] = []
    cursor = await harness.saver.aget_tuple(_root(namespace, leaf_id))
    while cursor is not None:
        chain.append(cursor)
        parent = cursor.parent_config
        cursor = (
            await harness.saver.aget_tuple(
                _root(namespace, parent["configurable"]["checkpoint_id"])
            )
            if parent is not None
            else None
        )
    return chain


async def _leaf_id(harness: RecoveryHarness, namespace: str) -> str:
    latest = await harness.saver.aget_tuple(_root(namespace))
    assert latest is not None
    return str(latest.config["configurable"]["checkpoint_id"])


def _namespace(request: OperationExecutionRequest) -> str:
    assert request.deep_agent_binding is not None
    namespace = request.deep_agent_binding.cognitive_session_namespace
    assert namespace is not None
    return namespace


async def _baseline_digest() -> str:
    harness = await recovery_harness()
    request = await harness.request(stage_recovery_unit(harness.run_id))
    result = await harness.run(request)
    assert result.status == "completed"
    return result_digest(result)


async def _final_messages(harness: RecoveryHarness, namespace: str, leaf_id: str) -> list[Any]:
    """The transcript at a checkpoint, read through a graph (never by invoking a model)."""

    from deepagents import create_deep_agent

    from tests.fixtures.checkpoint_recovery import ScriptedRecoveryModel

    reader = create_deep_agent(model=ScriptedRecoveryModel(), checkpointer=harness.saver)
    state = await reader.aget_state(_root(namespace, leaf_id))
    return list(state.values["messages"])


# --- crash windows ---------------------------------------------------------------------

# The scripted graph writes seven root checkpoints: input (1), middleware steps (2-3), the
# model's tool call (4-5), the tool result (6) and the terminal answer (7).
INPUT_CHECKPOINT = 1
AFTER_TOOL_CHECKPOINT = 6
TERMINAL_CHECKPOINT = 7

CRASH_WINDOWS = {
    # window: (classification the recovering attempt applies, model calls in total,
    #          model calls before the crash)
    "before_checkpoint": (CheckpointClassification.NOT_SUBMITTED, 2, 0),
    "after_input_checkpoint": (CheckpointClassification.INTERRUPTED, 2, 0),
    "after_intermediate_checkpoint": (CheckpointClassification.INTERRUPTED, 2, 1),
    "after_terminal_checkpoint": (CheckpointClassification.TERMINAL_UNOBSERVED, 2, 2),
    "before_observation": (CheckpointClassification.TERMINAL_UNOBSERVED, 2, 2),
    "before_settlement": (CheckpointClassification.OBSERVED_UNSETTLED, 2, 2),
    "after_settlement": (CheckpointClassification.SETTLED, 2, 2),
}


def _inject(harness: RecoveryHarness, window: str) -> None:
    if window == "before_checkpoint":
        harness.runtime.crash_before_invocation = True
    elif window == "after_input_checkpoint":
        harness.saver.crash_after(INPUT_CHECKPOINT)
    elif window == "after_intermediate_checkpoint":
        harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
    elif window == "after_terminal_checkpoint":
        harness.saver.crash_after(TERMINAL_CHECKPOINT)
    elif window == "before_observation":
        harness.runtime.crash_after_invocation = True
    elif window == "before_settlement":
        harness.crashable.crash_before_settlement = True


@pytest.mark.asyncio
@pytest.mark.parametrize("window", list(CRASH_WINDOWS))
async def test_crash_window_converges_to_one_settlement_without_reappending_input(
    window: str,
) -> None:
    """REQ-CP-DA-018 crash-window table: each window recovers exactly as prescribed.

    Asserted per window: model invocations, human (prompt) messages seen by every call and
    held in the final state, tool executions (one `ToolMessage` per tool call), ancestry of
    the result to the source through this invocation's stamps only, one transition, one
    settlement, and the final result digest equal to a crash-free run.
    """

    expected_classification, expected_model_calls, calls_before_crash = CRASH_WINDOWS[window]
    baseline = await _baseline_digest()
    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    namespace = _namespace(request)

    if window == "after_settlement":
        first = await harness.run(request)
        recovered = await harness.run(request)
        assert recovered == first, "an authoritative settlement returns unchanged"
    else:
        _inject(harness, window)
        await harness.crash(request)
        assert len(harness.model.calls) == calls_before_crash
        crash_checkpoints = {
            str(item.config["configurable"]["checkpoint_id"])
            for item in await _root_checkpoints(harness, namespace)
        }
        recovered = await harness.run(request)
        assert recovered.status == "completed"
        if expected_classification == CheckpointClassification.INTERRUPTED:
            assert crash_checkpoints, "the crash left durable intermediate checkpoints"
        if expected_classification == CheckpointClassification.TERMINAL_UNOBSERVED:
            assert len(await _root_checkpoints(harness, namespace)) == len(crash_checkpoints)

    # Model and prompt counts: the input is submitted once and never re-appended.
    assert len(harness.model.calls) == expected_model_calls
    assert {human for human, _tools in harness.model.calls} == {1}
    # Final digest equals the crash-free run; the output is restored, not dropped.
    assert result_digest(recovered) == baseline
    assert recovered.structured_output == {"answer": RESULT_MARKER, "tool_results": 1}

    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None
    classification = (
        CheckpointClassification.NOT_SUBMITTED
        if expected_classification
        in {CheckpointClassification.OBSERVED_UNSETTLED, CheckpointClassification.SETTLED}
        else expected_classification
    )
    assert transition.classification == classification
    assert recovered.result_checkpoint == transition.result_key
    assert recovered.checkpoint_transition_id == transition.transition_id
    assert await harness.lineage.get_namespace_head("tenant-1", namespace) == (
        transition.result_key
    )
    assert await harness.lineage.list_transitions("tenant-1", namespace) == (transition,)

    # Ancestry: every root checkpoint from the result to the empty source is this unit
    # generation's submission, so the result lineage is attributable end to end.
    chain = await _chain(harness, namespace, transition.result_key.checkpoint_id)
    assert {item.metadata[STAMP_INVOCATION_ID] for item in chain} == {
        submission_invocation_id(unit.unit_key, 1)
    }
    assert chain[-1].parent_config is None
    messages = await _final_messages(harness, namespace, transition.result_key.checkpoint_id)
    assert sum(isinstance(item, HumanMessage) for item in messages) == 1
    tool_messages = [item for item in messages if isinstance(item, ToolMessage)]
    assert [item.tool_call_id for item in tool_messages] == ["rrm004-write-todos"]

    # One settlement, by the fence holder that settled it.
    binding = bind_operation_execution_request(request)
    claim_id = _effect_claim_id(binding)
    settlement = harness.journal.settlements[claim_id]
    assert settlement.status == "completed"
    assert settlement.result_manifest_digest == transition.result_manifest_digest
    attempts = await harness.lineage.list_attempts("tenant-1", unit.unit_key)
    if window == "after_settlement":
        # A settled replay is answered before any lease or provider work: no observation.
        assert [(item.attempt.attempt, item.claim_fence) for item in attempts] == [(1, 1)]
        assert harness.journal.technical_attempts[claim_id] == [1]
    else:
        assert [(item.attempt.attempt, item.claim_fence) for item in attempts] == [(1, 1), (2, 2)]
        assert all(item.dispatching for item in attempts)
        assert harness.journal.technical_attempts[claim_id] == [2]
    if window == "before_settlement":
        assert transition.claim_fence == 1, "the lost holder's fenced result is the one settled"
    assert await harness.lineage.get_incident("tenant-1", unit.unit_key, 1) is None


# --- in_doubt and operator reconciliation ------------------------------------------------


async def _fork_stamped_sibling(harness: RecoveryHarness, namespace: str, leaf_id: str) -> str:
    """Write a second stamped child of the leaf's parent: two stamped leaves now exist."""

    leaf = await harness.saver.aget_tuple(_root(namespace, leaf_id))
    assert leaf is not None and leaf.parent_config is not None
    sibling = {**leaf.checkpoint, "id": str(uuid6())}
    written = await harness.saver.aput(
        leaf.parent_config, sibling, leaf.metadata, {}
    )
    return str(written["configurable"]["checkpoint_id"])


@pytest.mark.asyncio
async def test_multiple_stamped_leaves_park_in_doubt_until_operator_accepts_one() -> None:
    """DA-018 `in_doubt` (more than one stamped leaf) and `accept_descendant`.

    No model call is made while in doubt; the incident carries both candidates; the run
    keeps its phase with an `operator_reconciliation` wait, and the unit's effect claim has
    an `ambiguous` observation. The accepted leaf is resumed, never the other branch.
    """

    baseline = await _baseline_digest()
    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    namespace = _namespace(request)
    harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
    await harness.crash(request)
    leaf_id = await _leaf_id(harness, namespace)
    sibling_id = await _fork_stamped_sibling(harness, namespace, leaf_id)

    parked = await harness.run(request)
    assert parked.status == "in_doubt"
    assert parked.failure_code == "multiple_stamped_leaves"
    assert len(harness.model.calls) == 1, "in_doubt never invokes the model"
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None and parked.reconciliation_incident_id == incident.incident_id
    assert {item.checkpoint_id for item in incident.candidates} == {leaf_id, sibling_id}
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    assert run.phase == RunPhase.ACTIVE, "the run keeps its phase while the unit is in_doubt"
    wait_id = operator_reconciliation_condition_id(unit.unit_key, 1)
    assert [(item.condition_id, item.kind) for item in run.active_waits] == [
        (wait_id, "operator_reconciliation")
    ]
    binding = bind_operation_execution_request(request)
    effects = await harness.run_control.get_effects("tenant-1", harness.run_id)
    claim = effects.claims[_effect_claim_id(binding)]
    assert claim.disposition == EffectDisposition.AMBIGUOUS and claim.settlement is None

    # A repeated attempt without a decision stays in doubt, still without provider work.
    assert (await harness.run(request)).status == "in_doubt"
    assert len(harness.model.calls) == 1

    # The operator may only accept a checkpoint of the unit's own namespace.
    foreign = incident.candidates[0].model_copy(update={"thread_id": "belllabs/other"})
    with pytest.raises(UnitReconciliationRejected):
        await harness.reconcile(
            request,
            "reconcile-foreign",
            ReconcileUnitAction(
                unit_key=unit.unit_key,
                execution_generation=1,
                incident_id=incident.incident_id,
                decision="accept_descendant",
                accepted_checkpoint=foreign,
            ),
        )
    accepted = next(item for item in incident.candidates if item.checkpoint_id == leaf_id)
    decided = await harness.reconcile(
        request,
        "reconcile-accept",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=incident.incident_id,
            decision="accept_descendant",
            accepted_checkpoint=accepted,
        ),
    )
    assert decided.status == CommandStatus.ACCEPTED
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    assert run.active_waits == () and run.phase == RunPhase.ACTIVE
    assert [item.decision for item in run.unit_reconciliations] == ["accept_descendant"]

    recovered = await harness.run(request)
    assert recovered.status == "completed"
    assert result_digest(recovered) == baseline
    assert len(harness.model.calls) == 2
    assert {human for human, _tools in harness.model.calls} == {1}
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None
    assert transition.classification == CheckpointClassification.INTERRUPTED
    chain_ids = {
        str(item.config["configurable"]["checkpoint_id"])
        for item in await _chain(harness, namespace, transition.result_key.checkpoint_id)
    }
    assert leaf_id in chain_ids and sibling_id not in chain_ids
    resolved = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert resolved is not None and resolved.status == "resolved"
    assert resolved.decision == "accept_descendant"
    effects = await harness.run_control.get_effects("tenant-1", harness.run_id)
    assert effects.claims[_effect_claim_id(binding)].disposition == EffectDisposition.SUCCEEDED


async def _write_foreign_root_checkpoint(harness: RecoveryHarness, namespace: str) -> None:
    """A stray root checkpoint no BellLabs invocation stamped (e.g. an out-of-band write)."""

    from langgraph.checkpoint.base import empty_checkpoint

    await harness.saver.aput(_root(namespace), empty_checkpoint(), {"source": "input"}, {})


@pytest.mark.asyncio
async def test_foreign_descendant_is_in_doubt_and_abandon_settles_failed() -> None:
    """DA-018 foreign/unstamped descendant → `in_doubt`; `abandon_unit` settles `failed`
    with reason `in_doubt_abandoned` and releases the stranded namespace."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    namespace = _namespace(request)
    await _write_foreign_root_checkpoint(harness, namespace)

    parked = await harness.run(request)
    assert parked.status == "in_doubt" and parked.failure_code == "foreign_descendant"
    assert harness.model.calls == []
    assert await harness.lineage.get_namespace_in_flight("tenant-1", namespace) == (
        unit.unit_key,
        1,
    ), "an in_doubt unit keeps its namespace until reconciliation"
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None
    decided = await harness.reconcile(
        request,
        "reconcile-abandon",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=incident.incident_id,
            decision="abandon_unit",
        ),
    )
    assert decided.status == CommandStatus.ACCEPTED
    assert await harness.lineage.get_namespace_in_flight("tenant-1", namespace) is None

    abandoned = await harness.run(request)
    assert abandoned.status == "failed"
    assert abandoned.failure_code == "in_doubt_abandoned"
    assert harness.model.calls == []
    assert await harness.run(request) == abandoned, "the failed settlement is immutable"
    effects = await harness.run_control.get_effects("tenant-1", harness.run_id)
    claim = effects.claims[_effect_claim_id(bind_operation_execution_request(request))]
    assert claim.disposition == EffectDisposition.FAILED


@pytest.mark.asyncio
async def test_start_new_generation_fences_every_late_write_of_the_old_generation() -> None:
    """REQ-CP-EXEC-005: `start_new_generation` crosses a generation boundary. The old
    generation can never settle the unit: its attempts and late results are rejected and
    recorded, and its namespace reservation is released."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    namespace = _namespace(request)
    await _write_foreign_root_checkpoint(harness, namespace)
    await harness.run(request)
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None
    stale_version = (await harness.run_control.get_run("tenant-1", harness.run_id)).version - 1
    stale = await harness.reconciliation.reconcile_unit(
        reconciler_command(
            harness.run_id,
            stale_version,
            "reconcile-stale",
            ReconcileUnitAction(
                unit_key=unit.unit_key,
                execution_generation=1,
                incident_id=incident.incident_id,
                decision="start_new_generation",
            ),
        )
    )
    assert stale.status == CommandStatus.STALE, "a stale run version is rejected"
    decided = await harness.reconcile(
        request,
        "reconcile-new-generation",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=incident.incident_id,
            decision="start_new_generation",
        ),
    )
    assert decided.status == CommandStatus.ACCEPTED
    assert await harness.lineage.get_namespace_in_flight("tenant-1", namespace) is None

    superseded = await harness.run(request)
    assert superseded.status == "in_doubt"
    assert superseded.failure_code == "generation_superseded"
    assert harness.model.calls == []
    assert harness.journal.settlements == {}

    # A late result write of the fenced generation is rejected and recorded, never applied.
    late = UnitResultObservation(
        observation_id=unit_result_observation_id("tenant-1", unit.unit_key, 1),
        request_scope="tenant-1",
        unit_key=unit.unit_key,
        execution_generation=1,
        claim_fence=1,
        binding_id=bind_operation_execution_request(request).binding_id,
        settlement_id="late-settlement",
        status="completed",
        result_manifest_ref="late-manifest",
        result_manifest_digest="sha256:" + "d" * 64,
        result_manifest_size_bytes=1,
        observed_at=harness.clock(),
    )
    with pytest.raises(StaleClaimFence):
        await harness.lineage.record_result(late)
    rejections = await harness.lineage.list_rejections("tenant-1", unit.unit_key)
    assert [(item.reason, item.current_generation) for item in rejections] == [
        ("stale_execution_generation", 2)
    ]
    assert await harness.lineage.get_result("tenant-1", unit.unit_key, 1) is None


@pytest.mark.asyncio
async def test_zombie_holder_late_write_is_rejected_and_recorded() -> None:
    """REQ-CP-EXEC-014: a stale worker cannot apply a late competing observation.

    Holder A stalls inside its second model call. Its lease expires; attempt B takes over
    by advancing the fence, resumes the interrupted lineage, and settles. When A finally
    finishes, its fenced result write is rejected and recorded, and the settlement stays B's.
    """

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    entered, release = harness.model.gate_on(2)
    zombie = asyncio.create_task(harness.run(request))
    await asyncio.wait_for(entered.wait(), timeout=30)
    harness.clock.advance(timedelta(minutes=10))

    recovered = await harness.run(request)
    assert recovered.status == "completed"
    release.set()
    with pytest.raises(CheckpointLineageConflict):
        await zombie
    rejections = await harness.lineage.list_rejections("tenant-1", unit.unit_key)
    assert [(item.reason, item.presented_fence, item.current_fence) for item in rejections] == [
        ("stale_claim_fence", 1, 2)
    ]
    assert len(harness.model.calls) == 3
    assert (await harness.run(request)) == recovered, "the recorded result is immutable"
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None and transition.claim_fence == 2


@pytest.mark.asyncio
async def test_concurrent_recoveries_serialize_on_the_claim_lease() -> None:
    """REQ-CP-EXEC-014: concurrent recoveries of one unit serialize; one resumes."""

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
    await harness.crash(request)
    entered, release = harness.model.gate_on(2)
    first = asyncio.create_task(harness.run(request))
    await asyncio.wait_for(entered.wait(), timeout=30)
    with pytest.raises(OperationExecutionInProgress):
        await harness.run(request)
    release.set()
    assert (await first).status == "completed"
    assert len(harness.model.calls) == 2


# --- the narrowed post-dispatch rule (REQ-CP-RUN-007) -----------------------------------


@pytest.mark.asyncio
async def test_provider_failure_without_terminal_result_or_open_effects_settles_failed() -> None:
    harness = await recovery_harness()
    request = await harness.request(stage_recovery_unit(harness.run_id))
    harness.model.fail_on(2, ValueError("provider rejected the request"))
    result = await harness.run(request)
    assert result.status == "failed"
    assert result.failure_code == "runtime_failed"
    assert result.failure_message == "ValueError at governed operation boundary"
    assert await harness.lineage.get_incident("tenant-1", stage_recovery_unit(
        harness.run_id
    ).unit_key, 1) is None


@pytest.mark.asyncio
async def test_provider_failure_with_an_unsettled_effect_claim_is_in_doubt() -> None:
    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    binding = bind_operation_execution_request(request)

    async def claim_tool_effect() -> None:
        run = await harness.run_control.get_run("tenant-1", harness.run_id)
        claimed = await harness.run_control.execute(
            command(
                harness.run_id,
                run.version,
                "claim-tool-effect",
                ClaimEffectAction(
                    effect_id="tool-effect:send-email",
                    effect_kind="tool.consequential",
                    operation_ref=binding.binding_id,
                    provider_idempotency_key="tool-effect:send-email",
                    reservation_id=request.budget_reservation_id,
                ),
            )
        )
        assert claimed.status == CommandStatus.ACCEPTED

    # A consequential tool effect is claimed during cognition; then the provider fails.
    harness.model.before_call(2, claim_tool_effect)
    harness.model.fail_on(2, ValueError("provider failed after a consequential tool effect"))
    result = await harness.run(request)
    assert result.status == "in_doubt"
    assert result.failure_code == "unsettled_effect_claims"
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None
    assert incident.unsettled_effect_ids == ("tool-effect:send-email",)
    assert harness.journal.settlements == {}, "never settled failed while an effect is open"


@pytest.mark.asyncio
async def test_failure_after_a_terminal_checkpoint_is_in_doubt_then_reconstructed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failure after the terminal checkpoint must not settle `failed` (a terminal result
    exists): it is `in_doubt`; accepting the terminal leaf reconstructs without a model call."""

    import app.integrations.agents.deep_agents.adapter as adapter_module

    baseline = await _baseline_digest()
    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    real_inspect = adapter_module._inspect_state

    def broken_inspect(*args: Any) -> Any:
        raise RuntimeError("post-processing failed after the terminal checkpoint")

    monkeypatch.setattr(adapter_module, "_inspect_state", broken_inspect)
    parked = await harness.run(request)
    assert parked.status == "in_doubt"
    assert parked.failure_code == "terminal_result_after_failure"
    monkeypatch.setattr(adapter_module, "_inspect_state", real_inspect)
    namespace = _namespace(request)
    leaf_id = await _leaf_id(harness, namespace)
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None
    leaf = await harness.saver.aget_tuple(_root(namespace, leaf_id))
    assert leaf is not None and leaf.parent_config is not None
    # The incident records the terminal stamped leaf as the operator's candidate.
    assert incident.candidates == (_key(harness, request, leaf),)
    decided = await harness.reconcile(
        request,
        "reconcile-terminal",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=incident.incident_id,
            decision="accept_descendant",
            accepted_checkpoint=_key(harness, request, leaf),
        ),
    )
    assert decided.status == CommandStatus.ACCEPTED
    recovered = await harness.run(request)
    assert recovered.status == "completed"
    assert result_digest(recovered) == baseline
    assert len(harness.model.calls) == 2, "terminal reconstruction never invokes the model"
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None
    assert transition.classification == CheckpointClassification.TERMINAL_UNOBSERVED
    assert transition.result_key.checkpoint_id == leaf_id


def _key(harness: RecoveryHarness, request: OperationExecutionRequest, item: CheckpointTuple):  # type: ignore[no-untyped-def]
    from app.domain.graph_runtime.identities import QualifiedCheckpointKey

    assert request.deep_agent_binding is not None
    assert item.parent_config is not None
    return QualifiedCheckpointKey(
        checkpointer_ref_digest=request.deep_agent_binding.checkpointer_ref.digest,
        thread_id=_namespace(request),
        checkpoint_id=str(item.config["configurable"]["checkpoint_id"]),
        parent_checkpoint_id=str(item.parent_config["configurable"]["checkpoint_id"]),
    )


@pytest.mark.asyncio
async def test_lost_worker_lease_is_honored_until_it_expires() -> None:
    """A crashed holder's lease is not taken over early: the next attempt stands down."""

    harness = await recovery_harness()
    request = await harness.request(stage_recovery_unit(harness.run_id))
    harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
    attempt = harness.attempt(request)
    with pytest.raises(SimulatedWorkerCrash):
        await harness.service.execute(request, attempt)
    harness.saver.recover()
    with pytest.raises(OperationExecutionInProgress):
        await harness.run(request)
    assert len(harness.model.calls) == 1
    attempts = await harness.lineage.list_attempts("tenant-1", stage_recovery_unit(
        harness.run_id
    ).unit_key)
    assert [(item.attempt.attempt, item.dispatching) for item in attempts] == [
        (1, True),
        (2, False),
    ]


# --- review fix 1: an invalid accepted descendant never strands the unit ------------------


async def _parked_with_two_leaves(
    harness: RecoveryHarness,
) -> tuple[OperationExecutionRequest, str, str, str]:
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    namespace = _namespace(request)
    harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
    await harness.crash(request)
    leaf_id = await _leaf_id(harness, namespace)
    sibling_id = await _fork_stamped_sibling(harness, namespace, leaf_id)
    parked = await harness.run(request)
    assert parked.failure_code == "multiple_stamped_leaves"
    return request, namespace, leaf_id, sibling_id


@pytest.mark.asyncio
async def test_unverifiable_accepted_descendant_is_rejected_before_run_control() -> None:
    """A non-candidate key that is not a stamped descendant (here: no such checkpoint) is
    rejected before run control, so the operator wait stays pending and a valid decision
    still resolves the unit."""

    harness = await recovery_harness()
    request, namespace, leaf_id, _sibling = await _parked_with_two_leaves(harness)
    unit = request.runtime_unit
    assert unit is not None
    incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert incident is not None
    real = next(item for item in incident.candidates if item.checkpoint_id == leaf_id)
    for bogus in (
        real.model_copy(update={"checkpoint_id": "no-such-checkpoint"}),
        real.model_copy(update={"parent_checkpoint_id": "wrong-parent"}),
    ):
        with pytest.raises(UnitReconciliationRejected, match="verified stamped"):
            await harness.reconcile(
                request,
                f"reconcile-bogus-{bogus.checkpoint_id}-{bogus.parent_checkpoint_id}",
                ReconcileUnitAction(
                    unit_key=unit.unit_key,
                    execution_generation=1,
                    incident_id=incident.incident_id,
                    decision="accept_descendant",
                    accepted_checkpoint=bogus,
                ),
            )
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    assert [item.condition_id for item in run.active_waits] == [
        operator_reconciliation_condition_id(unit.unit_key, 1)
    ]
    assert run.unit_reconciliations == ()
    decided = await harness.reconcile(
        request,
        "reconcile-real",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=incident.incident_id,
            decision="accept_descendant",
            accepted_checkpoint=real,
        ),
    )
    assert decided.status == CommandStatus.ACCEPTED
    assert (await harness.run(request)).status == "completed"


@pytest.mark.asyncio
async def test_accepted_descendant_that_fails_at_dispatch_reopens_the_next_incident_revision() -> (
    None
):
    """A verified, non-candidate descendant (the common parent of two stamped leaves) is
    accepted; at dispatch it still has two leaves, so the unit is in doubt again. That opens
    incident revision 2 with its own operator wait, and a second decision completes it."""

    baseline = await _baseline_digest()
    harness = await recovery_harness()
    request, namespace, leaf_id, sibling_id = await _parked_with_two_leaves(harness)
    unit = request.runtime_unit
    assert unit is not None
    first = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert first is not None and first.revision == 1
    leaf = await harness.saver.aget_tuple(_root(namespace, leaf_id))
    assert leaf is not None and leaf.parent_config is not None
    parent = await harness.saver.aget_tuple(leaf.parent_config)
    assert parent is not None
    parent_key = _key(harness, request, parent)
    assert parent_key not in first.candidates
    decided = await harness.reconcile(
        request,
        "reconcile-parent",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=first.incident_id,
            decision="accept_descendant",
            accepted_checkpoint=parent_key,
        ),
    )
    assert decided.status == CommandStatus.ACCEPTED

    reparked = await harness.run(request)
    assert reparked.status == "in_doubt"
    assert reparked.failure_code == "multiple_stamped_leaves"
    second = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
    assert second is not None
    assert (second.revision, second.status) == (2, "operator_required")
    assert reparked.reconciliation_incident_id == second.incident_id != first.incident_id
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    assert [(item.condition_id, item.verification_ref) for item in run.active_waits] == [
        (operator_reconciliation_condition_id(unit.unit_key, 1, 2), second.incident_id)
    ]
    assert len(harness.model.calls) == 1, "no provider work while the decision is unusable"

    accepted = next(item for item in second.candidates if item.checkpoint_id == leaf_id)
    redecided = await harness.reconcile(
        request,
        "reconcile-leaf",
        ReconcileUnitAction(
            unit_key=unit.unit_key,
            execution_generation=1,
            incident_id=second.incident_id,
            decision="accept_descendant",
            accepted_checkpoint=accepted,
        ),
    )
    assert redecided.status == CommandStatus.ACCEPTED
    recovered = await harness.run(request)
    assert recovered.status == "completed"
    assert result_digest(recovered) == baseline
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    assert [item.incident_id for item in run.unit_reconciliations] == [
        first.incident_id,
        second.incident_id,
    ]
    assert run.active_waits == ()
    chain_ids = {
        str(item.config["configurable"]["checkpoint_id"])
        for item in await _chain(harness, namespace, recovered.result_checkpoint.checkpoint_id)  # type: ignore[union-attr]
    }
    assert leaf_id in chain_ids and sibling_id not in chain_ids


# --- review fix 2: the REAL run-control authority admits retries and recovery -------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "window", ["after_intermediate_checkpoint", "after_terminal_checkpoint", "before_settlement"]
)
async def test_real_run_control_authority_admits_recovery_after_a_lost_worker(
    window: str,
) -> None:
    """REQ-CP-EXEC-005/014: the unit's own claim advanced the run past the bound revision.
    The first-binding check would refuse that run version; the continuation check admits the
    same bound attempt, so recovery converges under the production authority."""

    baseline = await _baseline_digest()
    harness = await recovery_harness(real_authority=True)
    request = await harness.request(stage_recovery_unit(harness.run_id))
    _inject(harness, window)
    await harness.crash(request)
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    assert run.version > request.run_control_revision
    authority = run_control_authority(harness.run_control)
    with pytest.raises(ValueError, match="Run Control revision"):
        await authority.verify(request)
    await authority.verify_continuation(request, bind_operation_execution_request(request))

    recovered = await harness.run(request)
    assert recovered.status == "completed"
    assert result_digest(recovered) == baseline
    assert len(harness.model.calls) == 2


@pytest.mark.asyncio
async def test_real_run_control_authority_admits_a_concurrent_retry_as_in_progress() -> None:
    """A redelivered attempt (attempt 2) while attempt 1 still holds the lease passes
    authority and stands down retryably; it is never rejected as non-retryable."""

    harness = await recovery_harness(real_authority=True)
    request = await harness.request(stage_recovery_unit(harness.run_id))
    entered, release = harness.model.gate_on(2)
    first = asyncio.create_task(harness.run(request))
    await asyncio.wait_for(entered.wait(), timeout=30)
    with pytest.raises(OperationExecutionInProgress):
        await harness.run(request)
    release.set()
    result = await first
    assert result.status == "completed"
    assert await harness.run(request) == result


@pytest.mark.asyncio
async def test_real_authority_continuation_fails_closed_for_paused_runs() -> None:
    """A paused run admits no continuation (fail-closed); after resume, recovery converges."""

    harness = await recovery_harness(real_authority=True)
    request = await harness.request(stage_recovery_unit(harness.run_id))
    _inject(harness, "after_intermediate_checkpoint")
    await harness.crash(request)
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    paused = await harness.run_control.execute(
        command(
            harness.run_id,
            run.version,
            "pause-for-continuation",
            PauseAction(
                decision=PauseDecision(
                    decision_id="pause-1",
                    scope=frozenset({"run"}),
                    reason="operator pause",
                    authority_ref="authority:lifecycle",
                ),
                runnable_work_remains=False,
            ),
        )
    )
    assert paused.status == CommandStatus.ACCEPTED and paused.phase == RunPhase.PAUSED
    with pytest.raises(ValueError, match="paused Workflow Run"):
        await harness.run(request)
    assert len(harness.model.calls) == 1
    resumed = await harness.run_control.execute(
        command(
            harness.run_id,
            paused.resulting_run_version,
            "resume-for-continuation",
            ResumeAction(
                decision=ResumeDecision(
                    decision_id="resume-1",
                    pause_decision_id="pause-1",
                    reason="operator resume",
                    authority_ref="authority:lifecycle",
                )
            ),
        )
    )
    assert resumed.phase == RunPhase.ACTIVE
    assert (await harness.run(request)).status == "completed"


@pytest.mark.asyncio
async def test_real_authority_continuation_rejects_terminal_or_foreign_bindings() -> None:
    from types import SimpleNamespace

    harness = await recovery_harness(real_authority=True)
    request = await harness.request(stage_recovery_unit(harness.run_id))
    binding = bind_operation_execution_request(request)

    class TerminalRunControl:
        def __init__(self, phase: RunPhase) -> None:
            self.phase = phase

        async def get_run(self, _scope: str, _run_id: str) -> SimpleNamespace:
            return SimpleNamespace(
                version=request.run_control_revision + 3,
                phase=self.phase,
                effective_configuration_digest=request.effective_configuration_digest,
            )

    for phase in (RunPhase.TERMINAL, RunPhase.CANCELLING, RunPhase.PENDING):
        authority = run_control_authority(TerminalRunControl(phase))  # type: ignore[arg-type]
        with pytest.raises(ValueError, match="cannot continue"):
            await authority.verify_continuation(request, binding)
    authority = run_control_authority(harness.run_control)
    foreign = binding.model_copy(update={"run_control_revision": binding.run_control_revision - 1})
    with pytest.raises(ValueError, match="bound operation attempt"):
        await authority.verify_continuation(request, foreign)


# --- review fix 3: a live holder never outlives its lease ---------------------------------


@pytest.mark.asyncio
async def test_slow_holder_stops_at_its_lease_deadline_before_a_takeover_proceeds() -> None:
    """REQ-CP-EXEC-014: holder A overruns (stalls in its second model call) past its lease
    budget (3 s lease, 1 s safety margin). A cancels its own cognition, releases the lease
    and reports a retryable `OperationLeaseExpired`; it writes nothing. B then takes over,
    resumes A's last durable checkpoint and settles; nothing of A's was rejected because A
    never attempted a late write."""

    baseline = await _baseline_digest()
    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    entered, _never = harness.model.gate_on(2)
    started = asyncio.get_running_loop().time()
    holder_a = asyncio.create_task(
        harness.service.execute(request, harness.attempt(request, lease=timedelta(seconds=3)))
    )
    await asyncio.wait_for(entered.wait(), timeout=30)
    with pytest.raises(OperationLeaseExpired):
        await asyncio.wait_for(holder_a, timeout=30)
    stopped_after = asyncio.get_running_loop().time() - started
    assert 1.5 <= stopped_after < 3.0, "A stops inside its lease, before it could be taken over"
    assert await harness.lineage.get_transition("tenant-1", unit.unit_key, 1) is None
    assert await harness.lineage.get_result("tenant-1", unit.unit_key, 1) is None

    recovered = await harness.run(request)
    assert recovered.status == "completed"
    assert result_digest(recovered) == baseline
    attempts = await harness.lineage.list_attempts("tenant-1", unit.unit_key)
    assert [(item.attempt.attempt, item.claim_fence) for item in attempts] == [(1, 1), (2, 2)]
    assert await harness.lineage.list_rejections("tenant-1", unit.unit_key) == ()
    transition = await harness.lineage.get_transition("tenant-1", unit.unit_key, 1)
    assert transition is not None
    assert transition.classification == CheckpointClassification.INTERRUPTED
    assert transition.claim_fence == 2
    assert len(harness.model.calls) == 3  # A's stalled call was cancelled, never answered
    assert {human for human, _tools in harness.model.calls} == {1}


@pytest.mark.asyncio
async def test_an_attempt_captures_only_its_own_tip_descending_from_its_pin() -> None:
    """The capture follows this attempt's ownership marker, never the thread's latest
    checkpoint: a newer tip written by a superseded attempt is ignored, and an own lineage
    that does not start at the pinned checkpoint is `in_doubt`."""

    from langgraph.checkpoint.base import empty_checkpoint
    from langgraph.checkpoint.memory import InMemorySaver

    from app.domain.operation_execution.checkpoint_lineage import (
        STAMP_ATTEMPT_REF,
        CheckpointInvocationPlan,
        CheckpointLineageInDoubt,
    )
    from app.integrations.agents.deep_agents.adapter import _Classified, _own_result_config

    saver = InMemorySaver()
    namespace = "belllabs/stage/fixture/gen/1"

    async def put(parent: str | None, attempt_ref: str) -> str:
        config: RunnableConfig = _root(namespace, parent) if parent else _root(namespace)
        written = await saver.aput(
            config,
            {**empty_checkpoint(), "id": str(uuid6())},
            {STAMP_ATTEMPT_REF: attempt_ref},
            {},
        )
        return str(written["configurable"]["checkpoint_id"])

    leaf = await put(None, "attempt-a")
    own_first = await put(leaf, "attempt-b")
    own_tip = await put(own_first, "attempt-b")
    await put(leaf, "attempt-a")  # newest checkpoint of the thread: the superseded holder's
    unit = stage_recovery_unit("run-own-tip")
    plan = CheckpointInvocationPlan(
        request_scope="tenant-1",
        unit_key=unit.unit_key,
        execution_generation=1,
        claim_fence=2,
        namespace=namespace,
        invocation_id=submission_invocation_id(unit.unit_key, 1),
        checkpointer_ref_digest="sha256:" + "c" * 64,
        binding_digest="sha256:" + "b" * 64,
        state_schema_digest="sha256:" + "5" * 64,
        attempt_ref="attempt-b",
    )
    resumed = _Classified(CheckpointClassification.INTERRUPTED, None, leaf_id=leaf)
    config = await _own_result_config(saver, plan, resumed)
    assert config["configurable"]["checkpoint_id"] == own_tip
    elsewhere = _Classified(CheckpointClassification.INTERRUPTED, None, leaf_id=own_first)
    with pytest.raises(CheckpointLineageInDoubt, match="does not descend"):
        await _own_result_config(saver, plan, elsewhere)
    with pytest.raises(CheckpointLineageInDoubt, match="no unique result tip"):
        await _own_result_config(
            saver, plan.model_copy(update={"attempt_ref": "attempt-c"}), resumed
        )


# --- re-review regression: a lost wake-up hint is recovered by resending ------------------


@pytest.mark.asyncio
async def test_lost_wake_up_hint_is_recovered_by_resending_the_same_decision() -> None:
    """The decision is accepted and applied, but the hint to the parked `OperationWorkflow`
    fails (Temporal unavailable). Resending the *same* command replays it idempotently and
    re-sends the hint; the workflow wakes and the unit converges. A different decision for
    the resolved revision is still rejected."""

    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker

    from app.application.operations.unit_reconciliation import UnitReconciliationService
    from app.domain.operation_execution.contracts import OperationWorkflowRequest
    from app.integrations.temporal_unit_reconciliation import TemporalUnitReconciliationNudge
    from app.temporal.operation_activities import (
        OperationExecutionActivities,
        parse_operation_result,
    )
    from app.temporal.workflow_sandbox import coordinator_workflow_runner
    from app.temporal.workflows.operation import OperationWorkflow

    harness = await recovery_harness()
    unit = stage_recovery_unit(harness.run_id)
    request = await harness.request(unit)
    namespace = _namespace(request)
    harness.saver.crash_after(AFTER_TOOL_CHECKPOINT)
    await harness.crash(request)
    leaf_id = await _leaf_id(harness, namespace)
    await _fork_stamped_sibling(harness, namespace, leaf_id)
    workflow_request = OperationWorkflowRequest(
        semantic_attempt_id=request.identity.semantic_key,
        operation_kind="bound_operation",
        operation=request,
        timeout_seconds=3600,
    )
    activities = OperationExecutionActivities(harness.service, worker_identity="worker:resend")

    async with await WorkflowEnvironment.start_time_skipping() as environment:

        class FlakyNudge:
            def __init__(self) -> None:
                self.inner = TemporalUnitReconciliationNudge(environment.client)
                self.calls = 0

            async def nudge(self, *, operation_workflow_id: str, decision_id: str) -> None:
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("Temporal unavailable")
                await self.inner.nudge(
                    operation_workflow_id=operation_workflow_id, decision_id=decision_id
                )

        nudge = FlakyNudge()
        reconciliation = UnitReconciliationService(
            run_control=harness.run_control, lineage=harness.lineage, nudge=nudge
        )
        async with (
            Worker(
                environment.client,
                task_queue="rrm004-resend-workflows",
                workflows=[OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
            ),
            Worker(
                environment.client,
                task_queue=workflow_request.activity_task_queue,
                activities=[activities.execute],
            ),
        ):
            handle = await environment.client.start_workflow(
                OperationWorkflow.run,
                workflow_request,
                id=workflow_request.workflow_id,
                task_queue="rrm004-resend-workflows",
            )
            async with asyncio.timeout(60):
                for _ in range(1200):
                    if await harness.lineage.get_incident("tenant-1", unit.unit_key, 1):
                        break
                    await asyncio.sleep(0.05)
            incident = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
            assert incident is not None and incident.reason == "multiple_stamped_leaves"
            assert incident.operation_workflow_id == workflow_request.workflow_id
            run = await harness.run_control.get_run("tenant-1", harness.run_id)
            abandon = reconciler_command(
                harness.run_id,
                run.version,
                "reconcile-abandon-resend",
                ReconcileUnitAction(
                    unit_key=unit.unit_key,
                    execution_generation=1,
                    incident_id=incident.incident_id,
                    decision="abandon_unit",
                ),
            )
            with pytest.raises(RuntimeError, match="Temporal unavailable"):
                await reconciliation.reconcile_unit(abandon)
            resolved = await harness.lineage.get_incident("tenant-1", unit.unit_key, 1)
            assert resolved is not None and resolved.status == "resolved"
            description = await handle.describe()
            assert description.status is not None and description.status.name == "RUNNING"

            other = reconciler_command(
                harness.run_id,
                run.version + 1,
                "reconcile-other-decision",
                ReconcileUnitAction(
                    unit_key=unit.unit_key,
                    execution_generation=1,
                    incident_id=incident.incident_id,
                    decision="start_new_generation",
                ),
            )
            with pytest.raises(UnitReconciliationRejected, match="another decision"):
                await reconciliation.reconcile_unit(other)

            resent = await reconciliation.reconcile_unit(abandon)
            assert resent.status == CommandStatus.ACCEPTED
            workflow_result = await asyncio.wait_for(handle.result(), timeout=120)

    result = parse_operation_result(workflow_result.result or {})
    assert workflow_result.disposition == "failed"
    assert result.failure_code == "in_doubt_abandoned"
    assert nudge.calls == 2
    assert len(harness.model.calls) == 1, "abandon never invokes the model"
    run = await harness.run_control.get_run("tenant-1", harness.run_id)
    assert [item.decision_id for item in run.unit_reconciliations] == [
        "reconcile-abandon-resend"
    ]
    # RRM-007: the governed `reconcile_unit` receipts. Acceptance by run control, delivery
    # once the hint reached the parked operation (the resend), application when the
    # operation boundary acted on the decision; the lost first hint left no receipt.
    status = await harness.run_control.get_boundary_command(
        "tenant-1", harness.run_id, "operator", "reconcile-abandon-resend"
    )
    assert status is not None
    assert [(item.state.value, item.recorded_by) for item in status.receipts] == [
        ("accepted", "run_control"),
        ("delivered", "unit-reconciliation"),
        ("applied", "operation-boundary"),
    ]
    assert status.command.kind == "reconcile_unit"
    assert status.command.target.kind == "unit"
    assert status.receipts[1].transport_ref == workflow_request.workflow_id
