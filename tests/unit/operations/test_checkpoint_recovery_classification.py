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
    ReconcileUnitAction,
    RunPhase,
    operator_reconciliation_condition_id,
)
from tests.fixtures.checkpoint_recovery import (
    RESULT_MARKER,
    RecoveryHarness,
    SimulatedWorkerCrash,
    recovery_harness,
    result_digest,
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
    accepted = incident.expected_source  # None: a fresh namespace
    assert accepted is None
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
