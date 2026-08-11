from __future__ import annotations

from itertools import product

import pytest
from pydantic import ValidationError

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AllowedOperationVariant,
    DependencyClass,
    FairnessGroup,
    LateResultPolicy,
    LateResultRule,
    SlowSiblingPolicy,
    StageDependency,
    StageGraphBlueprint,
    StageInputSlot,
    StageJoin,
    StageNode,
    StageOperationSlot,
    StageOutputSlot,
    WorkflowCyclePolicy,
)
from app.domain.orchestration.contracts import (
    DependencyDisposition,
    DependencyProjection,
    ExecutionIdentity,
    JoinDisposition,
    LateResultFacts,
    ResultDecision,
    StageCandidateIdentity,
    StageExecutionIdentity,
)
from app.domain.orchestration.interpreter import StageGraphInterpreter


def operation_slot(slot_id: str = "execute", *, priority: int = 0) -> StageOperationSlot:
    return StageOperationSlot(
        operation_slot_id=slot_id,
        priority=priority,
        reservation={"tokens.total": 1},
        allowed_variants=(
            AllowedOperationVariant(
                operation_variant_id="default",
                operation_contract_ref=f"operation:{slot_id}@1",
            ),
        ),
    )


def slow_policy() -> SlowSiblingPolicy:
    return SlowSiblingPolicy(
        triggers=("join_released", "accepted_budget_pressure"),
        execution_action="continue",
        arrival_route="evaluate_late_result",
    )


def blueprint(
    *,
    join_kind: str = "all",
    minimum: int | None = None,
    dependency_classes: tuple[DependencyClass, ...] = (
        DependencyClass.REQUIRED,
        DependencyClass.REQUIRED,
    ),
    weights: tuple[int, int] = (1, 1),
) -> StageGraphBlueprint:
    dependencies = tuple(
        StageDependency(
            dependency_id=f"producer-{index}-to-consumer",
            consumer_stage_id="consumer",
            join_id="consumer-inputs",
            producer_stage_id=f"producer-{index}",
            producer_output_slot_id="result",
            consumer_input_slot_id=f"input-{index}",
            dependency_class=dependency_class,
        )
        for index, dependency_class in enumerate(dependency_classes)
    )
    return StageGraphBlueprint(
        logical_id="test.stagegraph-v2",
        title="StageGraph V2 test",
        description="Canonical deterministic StageGraph test fixture.",
        stages=(
            StageNode(
                stage_id="consumer",
                fairness_group_id="consumer-group",
                input_slots=tuple(
                    StageInputSlot(input_slot_id=f"input-{index}")
                    for index in range(len(dependencies))
                ),
                output_slots=(
                    StageOutputSlot(
                        output_slot_id="final",
                        output_contract_ref="contract:final@1",
                    ),
                ),
                operation_slots=(operation_slot(),),
            ),
            *(
                StageNode(
                    stage_id=f"producer-{index}",
                    fairness_group_id="producer-group",
                    output_slots=(
                        StageOutputSlot(
                            output_slot_id="result",
                            output_contract_ref="contract:producer-result@1",
                        ),
                    ),
                    operation_slots=(operation_slot(priority=index),),
                )
                for index in range(len(dependencies))
            ),
        ),
        joins=(
            StageJoin(
                consumer_stage_id="consumer",
                join_id="consumer-inputs",
                kind=join_kind,
                minimum=minimum,
                dependency_ids=tuple(item.dependency_id for item in dependencies),
                slow_sibling_policy=slow_policy(),
            ),
        ),
        dependencies=dependencies,
        fairness_groups=(
            FairnessGroup(group_id="producer-group", weight=weights[0]),
            FairnessGroup(group_id="consumer-group", weight=weights[1]),
        ),
        late_result_policy=LateResultPolicy(
            rules=(
                LateResultRule(
                    rule_id="late-admit",
                    trigger="consumer_already_admitted",
                    decision="admit",
                ),
            )
        ),
        workflow_evaluation_contract_ref="evaluation:test@1",
        workflow_cycle_policy=WorkflowCyclePolicy(
            max_cycles=2,
            evaluation_contract_ref="evaluation:test@1",
            objective_contract_ref="objective:test@1",
            reservation={"workflow.cycles": 1},
        ),
    )


def interpreter(graph: StageGraphBlueprint) -> StageGraphInterpreter:
    return StageGraphInterpreter(graph, effective_max_concurrency=16)


def projection_for(graph: StageGraphBlueprint):
    return interpreter(graph).initial_projection(ExecutionIdentity("run-1"), run_version=1)


def test_normalization_is_order_independent_and_semantic_arrays_preserve_order() -> None:
    graph = blueprint(join_kind="any")
    permuted = graph.model_copy(
        update={
            "stages": tuple(reversed(graph.stages)),
            "dependencies": tuple(reversed(graph.dependencies)),
            "fairness_groups": tuple(reversed(graph.fairness_groups)),
        }
    )
    normalized = StageGraphBlueprint.model_validate(permuted.model_dump(mode="python"))

    assert normalized.stages == graph.stages
    assert normalized.dependencies == graph.dependencies
    assert normalized.fairness_groups == graph.fairness_groups
    assert normalized.joins[0].slow_sibling_policy.triggers == (
        "join_released",
        "accepted_budget_pressure",
    )
    assert sha256_digest(normalized) == sha256_digest(graph)


def test_non_nfc_and_duplicate_complete_identities_are_rejected() -> None:
    with pytest.raises(ValidationError, match="Unicode NFC"):
        FairnessGroup(group_id="e\u0301", weight=1)
    graph = blueprint()
    with pytest.raises(ValidationError, match="dependency identities must be unique"):
        StageGraphBlueprint.model_validate(
            {
                **graph.model_dump(mode="python"),
                "dependencies": (graph.dependencies[0], graph.dependencies[0]),
            }
        )


@pytest.mark.parametrize(
    ("dependency_class", "disposition", "expected"),
    tuple(
        (
            dependency_class,
            disposition,
            (
                None
                if dependency_class == DependencyClass.ADVISORY
                else disposition == DependencyDisposition.FULFILLED
                if dependency_class == DependencyClass.REQUIRED
                else disposition
                in {DependencyDisposition.FULFILLED, DependencyDisposition.DEGRADED}
                if dependency_class == DependencyClass.DEGRADABLE
                else disposition != DependencyDisposition.UNRESOLVED
            ),
        )
        for dependency_class, disposition in product(
            tuple(DependencyClass), tuple(DependencyDisposition)
        )
    ),
)
def test_complete_dependency_truth_table(
    dependency_class: DependencyClass,
    disposition: DependencyDisposition,
    expected: bool | None,
) -> None:
    assert (
        StageGraphInterpreter.dependency_satisfies(dependency_class, disposition)
        is expected
    )


@pytest.mark.parametrize(
    ("kind", "minimum", "dispositions", "expected"),
    (
        ("all", None, ("fulfilled", "fulfilled"), JoinDisposition.SATISFIED),
        ("all", None, ("fulfilled", "unresolved"), JoinDisposition.PENDING),
        ("all", None, ("fulfilled", "failed"), JoinDisposition.IMPOSSIBLE),
        ("any", None, ("fulfilled", "unresolved"), JoinDisposition.SATISFIED),
        ("any", None, ("failed", "unresolved"), JoinDisposition.PENDING),
        ("any", None, ("failed", "cancelled"), JoinDisposition.IMPOSSIBLE),
        ("minimum", 2, ("fulfilled", "fulfilled"), JoinDisposition.SATISFIED),
        ("minimum", 2, ("fulfilled", "unresolved"), JoinDisposition.PENDING),
        ("minimum", 2, ("fulfilled", "failed"), JoinDisposition.IMPOSSIBLE),
    ),
)
def test_join_satisfied_pending_impossible_truth_table(
    kind: str,
    minimum: int | None,
    dispositions: tuple[str, str],
    expected: JoinDisposition,
) -> None:
    graph = blueprint(join_kind=kind, minimum=minimum)
    kernel = interpreter(graph)
    projection = kernel.initial_projection(ExecutionIdentity("run-1"), run_version=1)
    projection = projection.__class__(
        **{
            **projection.__dict__,
            "dependencies": {
                dependency.dependency_id: DependencyProjection(
                    dependency_id=dependency.dependency_id,
                    disposition=DependencyDisposition(disposition),
                )
                for dependency, disposition in zip(
                    graph.dependencies, dispositions, strict=True
                )
            },
        }
    )
    assert kernel.join_disposition(graph.joins[0], projection) == expected


def test_incremental_any_release_does_not_wait_for_slow_sibling() -> None:
    graph = blueprint(join_kind="any")
    kernel = interpreter(graph)
    projection = kernel.initial_projection(ExecutionIdentity("run-1"), run_version=1)
    dependencies = dict(projection.dependencies)
    dependencies[graph.dependencies[0].dependency_id] = DependencyProjection(
        dependency_id=graph.dependencies[0].dependency_id,
        disposition=DependencyDisposition.FULFILLED,
        evidence_refs=("artifact:fast",),
    )
    projection = projection.__class__(
        **{**projection.__dict__, "dependencies": dependencies}
    )

    frontier = kernel.frontier(projection, available_concurrency=3)

    assert any(item.identity.candidate.stage_id == "consumer" for item in frontier)
    assert dependencies[graph.dependencies[1].dependency_id].disposition == (
        DependencyDisposition.UNRESOLVED
    )


def test_weighted_ring_is_deterministic_resumes_and_skips_blocked_candidates() -> None:
    graph = blueprint(join_kind="all", weights=(2, 1))
    kernel = interpreter(graph)
    projection = kernel.initial_projection(ExecutionIdentity("run-1"), run_version=1)

    assert kernel.group_ring == (
        "consumer-group",
        "producer-group",
        "producer-group",
    )
    first = kernel.frontier(
        projection,
        available_concurrency=2,
        blocked_candidate_keys=frozenset(
            {
                next(
                    item.candidate.semantic_prefix
                    for item in projection.stages.values()
                    if item.candidate.stage_id == "producer-0"
                )
            }
        ),
    )
    assert [item.identity.candidate.stage_id for item in first] == ["producer-1"]


def test_late_result_absolute_vetoes_precede_route_and_authored_rule() -> None:
    graph = blueprint()
    kernel = interpreter(graph)
    edge = graph.dependencies[0]
    identity = StageExecutionIdentity(
        run_id="run-1",
        execution_epoch=1,
        candidate=StageCandidateIdentity(
            stage_id=edge.producer_stage_id,
            mapped_instance_presence=0,
            mapped_instance_id="NO_MAPPED_INSTANCE",
            workflow_cycle_ordinal=0,
            stage_cycle_ordinal=0,
            operation_slot_id="execute",
        ),
        semantic_attempt=1,
    )

    terminal = kernel.late_result_decision(
        identity,
        edge,
        LateResultFacts(run_terminal=True, dependency_terminally_disposed=True),
        slow_sibling_route="evaluate_late_result",
    )
    cancelling = kernel.late_result_decision(
        identity,
        edge,
        LateResultFacts(run_cancelling=True, consumer_already_admitted=True),
        slow_sibling_route="evaluate_late_result",
    )
    routed = kernel.late_result_decision(
        identity,
        edge,
        LateResultFacts(consumer_already_admitted=True),
        slow_sibling_route="quarantine",
    )

    assert terminal.decision == ResultDecision.QUARANTINE
    assert terminal.matched_veto == "run_terminal_or_terminalization_started"
    assert cancelling.matched_veto == "run_cancelling"
    assert routed.decision == ResultDecision.QUARANTINE
    assert routed.matched_rule_id is None


def test_minimal_invalidation_reuses_unaffected_output_references() -> None:
    graph = blueprint()
    kernel = interpreter(graph)
    projection = kernel.initial_projection(ExecutionIdentity("run-1"), run_version=1)
    stages = {
        key: instance.__class__(
            **{
                **instance.__dict__,
                "output_refs": (f"artifact:{instance.candidate.stage_id}",),
            }
        )
        for key, instance in projection.stages.items()
    }
    projection = projection.__class__(**{**projection.__dict__, "stages": stages})

    decision = kernel.workflow_invalidation(
        projection,
        invalidation_frontier=("producer-0",),
        next_objective="repair producer zero only",
    )

    assert decision.invalidated_stage_ids == ("consumer", "producer-0")
    assert any("producer-1" in key for key in decision.reused_output_refs)
    assert all("producer-0" not in key for key in decision.reused_output_refs)


def test_completion_requires_obligations_dependencies_and_closed_liabilities() -> None:
    graph = blueprint()
    kernel = interpreter(graph)
    projection = kernel.initial_projection(ExecutionIdentity("run-1"), run_version=1)

    incomplete = kernel.completion(projection)

    assert not incomplete.can_terminalize
    assert set(incomplete.pending_dependency_ids) == {
        item.dependency_id for item in graph.dependencies
    }
