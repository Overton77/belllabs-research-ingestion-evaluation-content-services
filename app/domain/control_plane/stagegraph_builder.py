from __future__ import annotations

from dataclasses import dataclass

from app.domain.control_plane.contracts import (
    AllowedOperationVariant,
    CapacityCeiling,
    CompletionObligationRef,
    LateResultPolicy,
    LateResultRule,
    ObligationMatrixRow,
    SlowSiblingPolicy,
    StageDependency,
    StageGraphBlueprint,
    StageInputSlot,
    StageJoin,
    StageNode,
    StageObligationSlot,
    StageOperationSlot,
    StageOutputSlot,
    WorkflowCyclePolicy,
)


@dataclass(frozen=True)
class StageGraphStageSpec:
    """Compact authoring input that expands only into canonical StageGraph V2 contracts."""

    stage_id: str
    depends_on: tuple[str, ...] = ()
    output_slots: tuple[str, ...] = ()
    obligation_refs: tuple[str, ...] = ()
    reservation: dict[str, int] | None = None
    operation_contract_ref: str = "operation:native-stage@1"


def build_stagegraph_v2(
    *,
    logical_id: str,
    title: str,
    description: str,
    stages: tuple[StageGraphStageSpec, ...],
    max_concurrency: int = 1,
    workflow_evaluation_contract_ref: str | None = None,
    workflow_cycle_policy: WorkflowCyclePolicy | None = None,
) -> StageGraphBlueprint:
    """Expand concise domain authoring into the complete typed V2 graph surface."""
    specs = {item.stage_id: item for item in stages}
    if len(specs) != len(stages):
        raise ValueError("StageGraph stage specs must have unique identities")
    dependencies: list[StageDependency] = []
    joins: list[StageJoin] = []
    nodes: list[StageNode] = []
    obligation_matrix: list[ObligationMatrixRow] = []
    completion_obligations: list[CompletionObligationRef] = []
    for spec in stages:
        inputs = tuple(
            StageInputSlot(input_slot_id=f"from:{producer_id}")
            for producer_id in spec.depends_on
        )
        dependency_ids: list[str] = []
        for producer_id in spec.depends_on:
            producer = specs.get(producer_id)
            if producer is None or not producer.output_slots:
                raise ValueError(
                    f"StageGraph producer {producer_id!r} must declare an output slot"
                )
            dependency_id = f"{producer_id}:to:{spec.stage_id}"
            dependency_ids.append(dependency_id)
            dependencies.append(
                StageDependency(
                    dependency_id=dependency_id,
                    consumer_stage_id=spec.stage_id,
                    join_id=f"{spec.stage_id}:inputs",
                    producer_stage_id=producer_id,
                    producer_output_slot_id=producer.output_slots[0],
                    consumer_input_slot_id=f"from:{producer_id}",
                    dependency_class="required",
                )
            )
        if dependency_ids:
            joins.append(
                StageJoin(
                    consumer_stage_id=spec.stage_id,
                    join_id=f"{spec.stage_id}:inputs",
                    kind="all",
                    dependency_ids=tuple(dependency_ids),
                    slow_sibling_policy=SlowSiblingPolicy(
                        triggers=("join_released",),
                        execution_action="continue",
                        arrival_route="evaluate_late_result",
                    ),
                )
            )
        obligation_slots = tuple(
            StageObligationSlot(
                obligation_slot_id=obligation_ref,
                obligation_ref=obligation_ref,
            )
            for obligation_ref in spec.obligation_refs
        )
        for obligation_ref in spec.obligation_refs:
            obligation_matrix.append(
                ObligationMatrixRow(
                    obligation_scope="stage",
                    owner_stage_id=spec.stage_id,
                    obligation_slot_id=obligation_ref,
                    evidence_slot_id=obligation_ref,
                )
            )
            completion_obligations.append(
                CompletionObligationRef(
                    obligation_scope="stage",
                    owner_stage_id=spec.stage_id,
                    obligation_slot_id=obligation_ref,
                )
            )
        nodes.append(
            StageNode(
                stage_id=spec.stage_id,
                input_slots=inputs,
                output_slots=tuple(
                    StageOutputSlot(
                        output_slot_id=slot_id,
                        output_contract_ref=f"output:{logical_id}:{slot_id}@1",
                    )
                    for slot_id in spec.output_slots
                ),
                obligation_slots=obligation_slots,
                operation_slots=(
                    StageOperationSlot(
                        operation_slot_id="execute",
                        reservation=dict(spec.reservation or {"operation.attempts": 1}),
                        allowed_variants=(
                            AllowedOperationVariant(
                                operation_variant_id="default",
                                operation_contract_ref=spec.operation_contract_ref,
                            ),
                        ),
                    ),
                ),
            )
        )
    return StageGraphBlueprint(
        logical_id=logical_id,
        title=title,
        description=description,
        stages=tuple(nodes),
        joins=tuple(joins),
        dependencies=tuple(dependencies),
        obligation_matrix=tuple(obligation_matrix),
        completion_obligations=tuple(completion_obligations),
        capacity_ceilings=(
            CapacityCeiling(
                scope_kind="workflow",
                scope_id=logical_id,
                dimension_kind="concurrency",
                dimension_id="operation-slots",
                amount=max_concurrency,
            ),
        ),
        late_result_policy=LateResultPolicy(
            rules=(
                LateResultRule(
                    rule_id="consumer-already-admitted:admit",
                    trigger="consumer_already_admitted",
                    decision="admit",
                ),
            )
        ),
        workflow_evaluation_contract_ref=workflow_evaluation_contract_ref,
        workflow_cycle_policy=workflow_cycle_policy,
    )


__all__ = ["StageGraphStageSpec", "build_stagegraph_v2"]
