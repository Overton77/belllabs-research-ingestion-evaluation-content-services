from __future__ import annotations

from collections import defaultdict, deque

from app.domain.control_plane.contracts import StageGraphBlueprint, StageNode
from app.domain.orchestration.contracts import (
    ExecutionIdentity,
    StageExecutionIdentity,
    StageExecutionState,
    StageGraphExecutionState,
    StageOperationRequest,
    StageOperationResult,
    WorkflowEvaluationResult,
)

TERMINAL_STATUSES = frozenset({"completed", "degraded", "skipped", "failed"})
ACCEPTED_STATUSES = frozenset({"completed", "degraded"})


class StageGraphExecutionError(ValueError):
    pass


class StageGraphInterpreter:
    """Pure deterministic scheduler for one frozen, application-authored StageGraph."""

    def __init__(self, blueprint: StageGraphBlueprint, *, effective_max_concurrency: int) -> None:
        if effective_max_concurrency < 1:
            raise StageGraphExecutionError("effective concurrency must be positive")
        oversized = [
            stage.stage_id
            for stage in blueprint.stages
            if stage.concurrency_slots > effective_max_concurrency
        ]
        if oversized:
            raise StageGraphExecutionError(
                f"stages exceed the effective concurrency ceiling: {sorted(oversized)}"
            )
        self.blueprint = blueprint
        self.stages = {stage.stage_id: stage for stage in blueprint.stages}
        self.max_concurrency = min(
            blueprint.max_parallel_stages,
            effective_max_concurrency,
        )
        self.descendants = self._descendants()

    def initial_state(
        self,
        identity: ExecutionIdentity,
        *,
        run_version: int,
    ) -> StageGraphExecutionState:
        return StageGraphExecutionState(
            identity=identity,
            run_version=run_version,
            stages={stage_id: StageExecutionState() for stage_id in self.stages},
        )

    def runnable(self, state: StageGraphExecutionState) -> tuple[str, ...]:
        candidates = [
            stage
            for stage in self.blueprint.stages
            if state.stages[stage.stage_id].status == "pending"
            and self._join_satisfied(stage, state)
        ]
        ordered = sorted(
            candidates,
            key=lambda stage: (
                stage.fairness_priority,
                state.fairness_cursor.get(stage.fairness_group, 0),
                stage.fairness_group,
                stage.stage_id,
            ),
        )
        selected: list[str] = []
        slots = self.max_concurrency
        for stage in ordered:
            if stage.concurrency_slots <= slots:
                selected.append(stage.stage_id)
                slots -= stage.concurrency_slots
        return tuple(selected)

    def operation_request(
        self,
        state: StageGraphExecutionState,
        stage_id: str,
    ) -> StageOperationRequest:
        stage = self.stages[stage_id]
        stage_state = state.stages[stage_id]
        if stage_state.status != "pending" or not self._join_satisfied(stage, state):
            raise StageGraphExecutionError(f"stage is not runnable: {stage_id}")
        if not stage.reservation:
            raise StageGraphExecutionError(
                f"stage dispatch requires a prior non-empty reservation: {stage_id}"
            )
        stage_state.status = "running"
        stage_state.operation_attempt += 1
        state.fairness_cursor[stage.fairness_group] = (
            state.fairness_cursor.get(stage.fairness_group, 0) + 1
        )
        identity = StageExecutionIdentity(
            run_id=state.identity.run_id,
            stage_id=stage_id,
            workflow_cycle=state.workflow_cycle,
            stage_cycle=stage_state.stage_cycle,
            operation_attempt=stage_state.operation_attempt,
            execution_epoch=state.identity.execution_epoch,
        )
        state.schedule_trace.append(identity.semantic_key)
        input_refs = sorted(
            output_ref
            for dependency in stage.depends_on
            if state.stages[dependency].status in ACCEPTED_STATUSES
            for output_ref in state.stages[dependency].output_refs
        )
        if stage_state.stage_cycle:
            input_refs.extend(stage_state.output_refs)
        reservation = dict(stage.reservation)
        reservation["operation.attempts"] = 1
        reservation["concurrency.slots"] = stage.concurrency_slots
        if stage_state.stage_cycle and stage.stage_cycle_policy is not None:
            for dimension, amount in stage.stage_cycle_policy.reservation.items():
                reservation[dimension] = reservation.get(dimension, 0) + amount
        reservation_id = f"reservation:{identity.semantic_key}"
        return StageOperationRequest(
            identity=identity,
            idempotency_key=f"operation:{identity.semantic_key}",
            objective=stage_state.objective,
            input_refs=tuple(input_refs),
            reservation_id=reservation_id,
            reservation=reservation,
            workspace_namespace=(
                f"run/{identity.run_id}/execution-epoch/{identity.execution_epoch}/"
                f"workflow-cycle/{identity.workflow_cycle}/"
                f"stage/{stage_id}/stage-cycle/{identity.stage_cycle}"
            ),
            cycle_evaluation_contract_ref=(
                stage.stage_cycle_policy.evaluation_contract_ref
                if stage.stage_cycle_policy is not None
                else ""
            ),
            cycle_objective_contract_ref=(
                stage.stage_cycle_policy.objective_contract_ref
                if stage.stage_cycle_policy is not None
                else ""
            ),
        )

    def apply_stage_result(
        self,
        state: StageGraphExecutionState,
        result: StageOperationResult,
    ) -> None:
        if result.identity.stage_id not in self.stages:
            raise StageGraphExecutionError(
                f"result references an unknown stage: {result.identity.stage_id}"
            )
        stage = self.stages[result.identity.stage_id]
        current = state.stages[stage.stage_id]
        expected = (
            state.identity.run_id,
            state.workflow_cycle,
            current.stage_cycle,
            current.operation_attempt,
            state.identity.execution_epoch,
        )
        observed = (
            result.identity.run_id,
            result.identity.workflow_cycle,
            result.identity.stage_cycle,
            result.identity.operation_attempt,
            result.identity.execution_epoch,
        )
        if current.status != "running" or observed != expected:
            raise StageGraphExecutionError(
                "stage result does not match the active semantic identity"
            )
        state.lineage.append(result)
        current.output_refs = result.output_refs
        if result.disposition == "waiting":
            if not result.wait_condition_id:
                raise StageGraphExecutionError("waiting result requires a wait condition identity")
            current.status = "waiting"
            current.wait_condition_id = result.wait_condition_id
            return
        if result.disposition == "paused":
            if not result.pause_decision_id:
                raise StageGraphExecutionError("paused result requires a pause decision identity")
            current.status = "paused"
            current.pause_decision_id = result.pause_decision_id
            return
        if (
            stage.stage_cycle_policy is not None
            and result.evaluation_contract_ref != stage.stage_cycle_policy.evaluation_contract_ref
        ):
            raise StageGraphExecutionError(
                f"stage evaluation is not bound to its frozen contract: {stage.stage_id}"
            )
        if result.evaluation == "cycle":
            policy = stage.stage_cycle_policy
            if policy is None or current.stage_cycle >= policy.max_cycles:
                raise StageGraphExecutionError(f"stage cycle limit exceeded: {stage.stage_id}")
            if result.objective_contract_ref != policy.objective_contract_ref:
                raise StageGraphExecutionError(
                    f"stage cycle decision is not bound to frozen contracts: {stage.stage_id}"
                )
            if not result.next_objective:
                raise StageGraphExecutionError("a stage cycle requires a new typed objective")
            current.stage_cycle += 1
            current.objective = result.next_objective
            current.status = "pending"
            return
        if result.evaluation == "degrade":
            if stage.completion_class == "required":
                raise StageGraphExecutionError("a required stage cannot be degraded")
            current.status = "degraded"
            return
        if result.evaluation == "escalate":
            current.status = "failed"
            return
        current.status = result.disposition

    def clear_wait(self, state: StageGraphExecutionState, condition_id: str) -> None:
        stage_state = next(
            (
                item
                for item in state.stages.values()
                if item.status == "waiting" and item.wait_condition_id == condition_id
            ),
            None,
        )
        if stage_state is None:
            raise StageGraphExecutionError(f"unknown active wait: {condition_id}")
        stage_state.status = "pending"
        stage_state.wait_condition_id = ""

    def resume_pause(self, state: StageGraphExecutionState, decision_id: str) -> None:
        stage_state = next(
            (
                item
                for item in state.stages.values()
                if item.status == "paused" and item.pause_decision_id == decision_id
            ),
            None,
        )
        if stage_state is None:
            raise StageGraphExecutionError(f"unknown active pause: {decision_id}")
        stage_state.status = "pending"
        stage_state.pause_decision_id = ""

    def resolve_blocked(self, state: StageGraphExecutionState) -> bool:
        changed = False
        for stage in self.blueprint.stages:
            current = state.stages[stage.stage_id]
            if current.status != "pending" or not stage.depends_on:
                continue
            dependency_states = [state.stages[item].status for item in stage.depends_on]
            if not all(item in TERMINAL_STATUSES for item in dependency_states):
                continue
            if self._join_satisfied(stage, state):
                continue
            if stage.skip_policy == "when_dependencies_unsatisfied":
                current.status = "skipped"
            else:
                current.status = "failed"
            changed = True
        return changed

    def graph_complete(self, state: StageGraphExecutionState) -> bool:
        return all(item.status in TERMINAL_STATUSES for item in state.stages.values())

    def graph_failed(self, state: StageGraphExecutionState) -> bool:
        return any(
            state.stages[stage.stage_id].status == "failed" and stage.completion_class == "required"
            for stage in self.blueprint.stages
        )

    def apply_workflow_evaluation(
        self,
        state: StageGraphExecutionState,
        decision: WorkflowEvaluationResult,
    ) -> dict[str, tuple[str, ...]]:
        if decision.action != "cycle":
            return {}
        self.validate_workflow_evaluation(state, decision)
        frontier = set(decision.invalidation_frontier)
        invalidated = frontier | {
            descendant for stage_id in frontier for descendant in self.descendants[stage_id]
        }
        reused = {
            stage_id: stage_state.output_refs
            for stage_id, stage_state in state.stages.items()
            if stage_id not in invalidated and stage_state.output_refs
        }
        state.workflow_cycle += 1
        state.workflow_objective = decision.next_objective
        for stage_id in invalidated:
            stage_state = state.stages[stage_id]
            stage_state.status = "pending"
            stage_state.stage_cycle = 0
            stage_state.objective = decision.next_objective
            stage_state.output_refs = ()
            stage_state.wait_condition_id = ""
            stage_state.pause_decision_id = ""
        return reused

    def validate_workflow_evaluation(
        self,
        state: StageGraphExecutionState,
        decision: WorkflowEvaluationResult,
    ) -> None:
        policy = self.blueprint.workflow_cycle_policy
        if policy is None or state.workflow_cycle >= policy.max_cycles:
            raise StageGraphExecutionError("workflow cycle limit exceeded")
        if (
            decision.evaluation_contract_ref != policy.evaluation_contract_ref
            or decision.objective_contract_ref != policy.objective_contract_ref
        ):
            raise StageGraphExecutionError(
                "workflow cycle decision is not bound to frozen contracts"
            )
        frontier = set(decision.invalidation_frontier)
        if not frontier or not frontier <= self.stages.keys():
            raise StageGraphExecutionError("workflow cycle has an invalid invalidation frontier")
        if not decision.next_objective:
            raise StageGraphExecutionError("a workflow cycle requires a new typed objective")

    def current_outputs(self, state: StageGraphExecutionState) -> dict[str, tuple[str, ...]]:
        return {
            stage_id: item.output_refs
            for stage_id, item in state.stages.items()
            if item.output_refs and item.status in ACCEPTED_STATUSES
        }

    def _join_satisfied(
        self,
        stage: StageNode,
        state: StageGraphExecutionState,
    ) -> bool:
        if not stage.depends_on:
            return True
        acceptable = 0
        for dependency_id in stage.depends_on:
            dependency_status = state.stages[dependency_id].status
            dependency_class = stage.dependency_classes.get(dependency_id, "required")
            if dependency_status in ACCEPTED_STATUSES:
                acceptable += 1
            elif dependency_class in {"optional", "advisory"} and dependency_status in {
                "skipped",
                "failed",
            }:
                acceptable += 1
            elif dependency_class == "degradable" and dependency_status == "skipped":
                acceptable += 1
        if stage.join_policy == "any":
            return acceptable >= 1
        if stage.join_policy == "minimum":
            return acceptable >= (stage.minimum_dependencies or 1)
        return acceptable == len(stage.depends_on)

    def _descendants(self) -> dict[str, frozenset[str]]:
        direct: dict[str, set[str]] = defaultdict(set)
        for stage in self.blueprint.stages:
            for dependency in stage.depends_on:
                direct[dependency].add(stage.stage_id)
        result: dict[str, frozenset[str]] = {}
        for root in self.stages:
            descendants: set[str] = set()
            queue = deque(direct[root])
            while queue:
                stage_id = queue.popleft()
                if stage_id in descendants:
                    continue
                descendants.add(stage_id)
                queue.extend(direct[stage_id])
            result[root] = frozenset(descendants)
        return result
