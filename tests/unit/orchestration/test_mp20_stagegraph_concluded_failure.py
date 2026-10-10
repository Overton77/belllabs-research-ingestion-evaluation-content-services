"""MP-20: a Stage Graph whose failed stage leaves every waiting stage unreleasable concludes
without acceptance (SPEC-03 deny, 01-STAGE_GRAPH §6-7); a stalemate never does."""

from __future__ import annotations

from dataclasses import asdict, replace
from typing import Any

from mission_control.application.programs.service import _completion_payload
from mission_control.domain.authoring.stagegraph_builder import (
    StageGraphStageSpec,
    build_stagegraph_v2,
)
from mission_control.domain.programs.contracts import (
    DependencyDisposition,
    ExecutionIdentity,
    LateResultFacts,
    StageGraphCompletionProposal,
    StageResultObservation,
)
from mission_control.domain.programs.interpreter import StageGraphInterpreter

RUN_ID = "run-mp20-concluded"


def _graph(*, optional_tail: bool = False) -> StageGraphInterpreter:
    """`produce` -> `review` -> `consume` -> `report`, plus an independent `side` stage."""

    blueprint = build_stagegraph_v2(
        logical_id="mp20.concluded",
        title="MP-20 concluded failure",
        description="A gate between a producer and its consumers.",
        stages=(
            StageGraphStageSpec("produce", output_slots=("draft",)),
            StageGraphStageSpec("review", depends_on=("produce",), output_slots=("result",)),
            StageGraphStageSpec(
                "consume",
                depends_on=("review",),
                output_slots=("report",),
                obligation_refs=("handed_off",),
            ),
            StageGraphStageSpec("report", depends_on=("consume",), output_slots=("summary",)),
            StageGraphStageSpec("side", output_slots=("note",)),
        ),
        max_concurrency=2,
    )
    if optional_tail:
        blueprint = blueprint.model_validate(
            {
                **blueprint.model_dump(mode="python"),
                "dependencies": tuple(
                    item.model_copy(update={"dependency_class": "optional"})
                    if item.consumer_stage_id == "report"
                    else item
                    for item in blueprint.dependencies
                ),
            }
        )
    return StageGraphInterpreter(blueprint, effective_max_concurrency=2)


def _run(interpreter: StageGraphInterpreter, projection: Any, stage_id: str, outcome: str) -> Any:
    """Admit `stage_id` and decide its result (`completed` or `failed`)."""

    frontier = interpreter.frontier(projection, available_concurrency=2)
    proposal = next(item for item in frontier if item.identity.stage_id == stage_id)
    projection = interpreter.apply_admission(
        projection,
        proposal,
        next_run_version=projection.run_version + 1,
        next_family_version=projection.family_version + 1,
    )
    observation = StageResultObservation(
        identity=proposal.identity,
        operation_result={"output_refs": [f"artifact:{stage_id}"]},
        child_closed_or_quiesced=True,
        reservations_and_usage_settled=True,
        effects_settled=True,
        cancellation_reconciled=True,
        accepted_order=1,
        operation_disposition=outcome,
    )
    decision = interpreter.result_decision(
        proposal.identity, LateResultFacts(), operation_disposition=outcome
    )
    return interpreter.apply_result_decision(
        projection,
        observation,
        decision,
        next_run_version=projection.run_version + 1,
        next_family_version=projection.family_version + 1,
    )


def _denied(interpreter: StageGraphInterpreter) -> Any:
    projection = interpreter.initial_projection(ExecutionIdentity(RUN_ID), run_version=1)
    projection = _run(interpreter, projection, "produce", "completed")
    projection = _run(interpreter, projection, "side", "completed")
    return _run(interpreter, projection, "review", "failed")


def test_a_denied_gate_concludes_the_graph_failed_with_its_dependents_skipped() -> None:
    interpreter = _graph()
    projection = _denied(interpreter)
    assert interpreter.frontier(projection, available_concurrency=2) == ()
    ordinary = interpreter.completion(projection)
    assert not ordinary.can_terminalize  # the pre-MP-20 `stagegraph_blocked` dead end

    concluded = interpreter.failure_completion(projection)

    assert concluded is not None and concluded.failed and concluded.can_terminalize
    assert concluded.skipped_stage_ids == ("consume", "report")
    assert not concluded.required_obligations_accepted
    assert concluded.pending_dependency_ids == ()
    assert concluded.open_producer_liability_ids == ()
    # Outputs stay exactly the accepted ones; the reducer binds them unchanged.
    assert concluded.valid_output_refs == ordinary.valid_output_refs
    assert "artifact:review" not in concluded.valid_output_refs


def test_nothing_concludes_while_a_unit_is_running() -> None:
    interpreter = _graph()
    projection = interpreter.initial_projection(ExecutionIdentity(RUN_ID), run_version=1)
    projection = _run(interpreter, projection, "produce", "completed")
    projection = _run(interpreter, projection, "review", "failed")
    # `side` is still releasable: not concluded.
    assert interpreter.failure_completion(projection) is None
    frontier = interpreter.frontier(projection, available_concurrency=2)
    (side,) = [item for item in frontier if item.identity.stage_id == "side"]
    running = interpreter.apply_admission(
        projection,
        side,
        next_run_version=projection.run_version + 1,
        next_family_version=projection.family_version + 1,
    )
    assert interpreter.failure_completion(running) is None


def test_a_stalemate_without_a_failed_stage_is_never_concluded() -> None:
    interpreter = _graph()
    projection = interpreter.initial_projection(ExecutionIdentity(RUN_ID), run_version=1)
    projection = _run(interpreter, projection, "produce", "completed")
    projection = _run(interpreter, projection, "side", "completed")
    projection = _run(interpreter, projection, "review", "completed")
    # A required dependency that may still resolve (forced back to unresolved): a stalemate.
    stuck = replace(
        projection,
        dependencies={
            key: replace(item, disposition=DependencyDisposition.UNRESOLVED)
            for key, item in projection.dependencies.items()
        },
    )
    assert interpreter.failure_completion(stuck) is None


def test_an_optional_input_of_a_skipped_producer_is_not_concluded() -> None:
    """An optional dependency of a skipped producer would be omitted and so release its
    consumer; the conclusion records no omission, so it keeps the stalemate."""

    interpreter = _graph(optional_tail=True)
    assert interpreter.failure_completion(_denied(interpreter)) is None


def test_the_recorded_completion_is_unchanged_unless_it_concluded_failed() -> None:
    ordinary = StageGraphCompletionProposal(
        required_obligations_accepted=True,
        pending_dependency_ids=(),
        open_producer_liability_ids=(),
        valid_output_refs=("artifact:a",),
    )
    legacy = asdict(ordinary)
    del legacy["failed"], legacy["skipped_stage_ids"]
    assert _completion_payload(ordinary) == legacy
    concluded = replace(
        ordinary, required_obligations_accepted=False, failed=True, skipped_stage_ids=("b",)
    )
    assert _completion_payload(concluded) == asdict(concluded)
    assert concluded.can_terminalize
    assert not replace(concluded, pending_dependency_ids=("x",)).can_terminalize
    assert not replace(concluded, open_producer_liability_ids=("x",)).can_terminalize
