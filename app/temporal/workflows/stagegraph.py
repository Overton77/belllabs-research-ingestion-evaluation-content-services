from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from app.domain.control_plane.canonical import sha256_digest
    from app.domain.control_plane.contracts import StageGraphBlueprint
    from app.domain.operation_execution.contracts import (
        OperationWorkflowRequest,
        OperationWorkflowResult,
    )
    from app.domain.orchestration.contracts import (
        CandidateOrderingKey,
        ExecutionIdentity,
        LateResultFacts,
        StageGraphAdmissionActivityRequest,
        StageGraphAdmissionActivityResult,
        StageGraphCompletionActivityRequest,
        StageGraphCompletionActivityResult,
        StageGraphCycleActivityRequest,
        StageGraphCycleActivityResult,
        StageGraphInitializeRequest,
        StageGraphInitializeResult,
        StageGraphResultActivityRequest,
        StageGraphResultActivityResult,
        StageGraphRunInput,
        StageGraphRunResult,
        StageResultObservation,
    )
    from app.domain.orchestration.interpreter import StageGraphInterpreter
    from app.temporal.workflows.operation import OperationWorkflow


@workflow.defn(name="belllabs.stagegraph")
class StageGraphWorkflow:
    """Replay-safe incremental mechanics for canonical StageGraph V2."""

    def __init__(self) -> None:
        self._cancel_requested = False
        self._satisfied_waits: set[str] = set()
        self._resumed_pauses: set[str] = set()
        self._runtime_state: dict[str, Any] = {}

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True

    @workflow.signal
    def satisfy_wait(self, condition_id: str) -> None:
        self._satisfied_waits.add(condition_id)

    @workflow.signal
    def resume_pause(self, decision_id: str) -> None:
        self._resumed_pauses.add(decision_id)

    @workflow.query
    def satisfied_waits(self) -> tuple[str, ...]:
        return tuple(sorted(self._satisfied_waits))

    @workflow.query
    def runtime_state(self) -> dict[str, Any]:
        return self._runtime_state

    @workflow.run
    async def run(self, run_input: StageGraphRunInput) -> StageGraphRunResult:
        if not run_input.correlation_id or not run_input.semantic_input_binding_ref:
            raise ApplicationError(
                "StageGraph execution requires exact correlation and semantic input bindings",
                non_retryable=True,
            )
        blueprint = StageGraphBlueprint.model_validate(run_input.blueprint)
        if sha256_digest(blueprint) != run_input.blueprint_digest:
            raise ApplicationError(
                "frozen StageGraph digest does not match its exact blueprint binding",
                non_retryable=True,
            )
        interpreter = StageGraphInterpreter(
            blueprint,
            effective_max_concurrency=run_input.max_concurrency,
        )
        projection = run_input.initial_projection or interpreter.initial_projection(
            ExecutionIdentity(
                run_id=run_input.run_id,
                execution_epoch=run_input.execution_epoch,
            ),
            run_version=run_input.initial_run_version,
        )
        timeout = timedelta(seconds=run_input.task_timeout_seconds)
        retry = RetryPolicy(maximum_attempts=3)
        if run_input.initial_projection is None:
            initialized = await workflow.execute_activity(
                "stagegraph.initialize",
                StageGraphInitializeRequest(
                    run_id=run_input.run_id,
                    request_scope=run_input.request_scope,
                    expected_run_version=run_input.initial_run_version,
                    initial_projection=projection,
                    occurred_at=workflow.now(),
                    idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                    correlation_id=run_input.correlation_id,
                ),
                result_type=StageGraphInitializeResult,
                start_to_close_timeout=timeout,
                retry_policy=retry,
            )
            if not initialized.accepted:
                raise ApplicationError(
                    f"StageGraph initialization rejected: {initialized.reason_code}",
                    non_retryable=True,
                )
            projection = initialized.projection

        active: dict[
            str,
            tuple[
                Any,
                asyncio.Future[OperationWorkflowResult],
                OperationWorkflowRequest,
                Any,
            ],
        ] = {}
        schedule_trace: list[str] = []
        reused_outputs: dict[str, tuple[str, ...]] = {}
        accepted_order = len(projection.accepted_results)
        cancellation_requested_children: set[str] = set()
        pending_cycle: dict[str, Any] | None = None

        while True:
            if self._cancel_requested:
                for identity, (handle, _task, _request, _stage_identity) in active.items():
                    cancellation_requested_children.add(identity)
                    handle.cancel()

            available = max(
                run_input.max_concurrency - interpreter.running_concurrency(projection),
                0,
            )
            unsatisfied_wait_ids = {
                item.wait_id
                for item in blueprint.waits
                if item.wait_id not in self._satisfied_waits
            }
            blocked_by_wait = frozenset(
                instance.candidate.semantic_prefix
                for instance in projection.stages.values()
                if instance.status in {"ready", "blocked"}
                and any(
                    wait.wait_id in unsatisfied_wait_ids
                    and (
                        wait.scope_kind == "workflow"
                        or (
                            wait.scope_kind == "stage"
                            and wait.scope_id == instance.candidate.stage_id
                        )
                        or (
                            wait.scope_kind == "operation"
                            and wait.scope_id
                            in {
                                instance.candidate.operation_slot_id,
                                (
                                    f"{instance.candidate.stage_id}/"
                                    f"{instance.candidate.operation_slot_id}"
                                ),
                            }
                        )
                    )
                    for wait in blueprint.waits
                )
            )
            frontier = (
                ()
                if self._cancel_requested
                else interpreter.frontier(
                    projection,
                    available_concurrency=available,
                    blocked_candidate_keys=blocked_by_wait,
                )
            )
            self._runtime_state = {
                "unsatisfied_wait_ids": tuple(sorted(unsatisfied_wait_ids)),
                "blocked_candidate_count": len(blocked_by_wait),
                "frontier_count": len(frontier),
                "active_count": len(active),
                "cancel_requested": self._cancel_requested,
            }
            if frontier:
                proposal = frontier[0]
                admitted = await workflow.execute_activity(
                    "stagegraph.admit_operation",
                    StageGraphAdmissionActivityRequest(
                        run_id=run_input.run_id,
                        request_scope=run_input.request_scope,
                        projection=projection,
                        proposal=proposal,
                        operation=None,
                        blueprint=run_input.blueprint,
                        effective_max_concurrency=run_input.max_concurrency,
                        occurred_at=workflow.now(),
                        idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                        correlation_id=run_input.correlation_id,
                        semantic_input_binding_ref=run_input.semantic_input_binding_ref,
                        effective_configuration_digest=(
                            run_input.effective_configuration_digest
                        ),
                    ),
                    result_type=StageGraphAdmissionActivityResult,
                    start_to_close_timeout=timeout,
                    retry_policy=retry,
                )
                if admitted.accepted and admitted.operation is not None:
                    projection = admitted.projection
                    operation = admitted.operation
                    handle = await workflow.start_child_workflow(
                        OperationWorkflow.run,
                        operation,
                        id=operation.workflow_id,
                        task_queue=workflow.info().task_queue,
                        parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
                    )
                    active[proposal.identity.semantic_key] = (
                        handle,
                        asyncio.ensure_future(handle),
                        operation,
                        proposal.identity,
                    )
                    schedule_trace.append(proposal.identity.semantic_key)
                    admitted_stage_id = proposal.identity.candidate.stage_id
                    for join in interpreter.stage_joins[admitted_stage_id]:
                        policy = join.slow_sibling_policy
                        if (
                            "join_released" not in policy.triggers
                            or policy.execution_action != "request_cancel"
                        ):
                            continue
                        unresolved_producers = {
                            interpreter.dependencies[dependency_id].producer_stage_id
                            for dependency_id in join.dependency_ids
                            if projection.dependencies[dependency_id].disposition.value
                            == "unresolved"
                        }
                        for (
                            sibling_handle,
                            _sibling_task,
                            _sibling_request,
                            sibling_identity,
                        ) in active.values():
                            if sibling_identity.candidate.stage_id in unresolved_producers:
                                cancellation_requested_children.add(
                                    sibling_identity.semantic_key
                                )
                                sibling_handle.cancel()
                    continue

            if active:
                task_by_identity = {
                    identity: task
                    for identity, (_handle, task, _request, _stage_identity) in active.items()
                }
                done, _pending = await workflow.wait(
                    tuple(task_by_identity.values()),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                completed_identities = sorted(
                    (
                        identity
                        for identity, task in task_by_identity.items()
                        if task in done
                    ),
                    key=lambda identity: (
                        *CandidateOrderingKey(
                            priority=0,
                            identity=active[identity][3].candidate,
                        ).as_tuple(),
                        active[identity][3].semantic_attempt,
                    ),
                )
                for identity in completed_identities:
                    task = task_by_identity[identity]
                    _handle, _task, operation_request, stage_identity = active.pop(identity)
                    try:
                        operation_result = task.result()
                    except BaseException:
                        operation_result = OperationWorkflowResult(
                            semantic_attempt_id=operation_request.semantic_attempt_id,
                            execution_generation=operation_request.execution_generation,
                            disposition=(
                                "cancelled"
                                if identity in cancellation_requested_children
                                else "failed"
                            ),
                            message_cursor=operation_request.message_cursor,
                            effect_frontier=operation_request.effect_frontier,
                            active_async_child_ids=operation_request.active_async_child_ids,
                        )
                    accepted_order += 1
                    observed_payload = dict(operation_result.result)
                    structured_output = observed_payload.get("structured_output")
                    if isinstance(structured_output, dict):
                        observed_payload.update(structured_output)
                    observation = StageResultObservation(
                        identity=stage_identity,
                        operation_result=observed_payload,
                        child_closed_or_quiesced=True,
                        reservations_and_usage_settled=True,
                        effects_settled=True,
                        cancellation_reconciled=(
                            not self._cancel_requested
                            or operation_result.disposition == "cancelled"
                        ),
                        accepted_order=accepted_order,
                        operation_disposition=operation_result.disposition,
                    )
                    decided = await workflow.execute_activity(
                        "stagegraph.decide_result",
                        StageGraphResultActivityRequest(
                            run_id=run_input.run_id,
                            request_scope=run_input.request_scope,
                            projection=projection,
                            observation=observation,
                            late_facts=LateResultFacts(
                                consumer_already_admitted=any(
                                    instance.candidate.stage_id
                                    in {
                                        edge.consumer_stage_id
                                        for edge in blueprint.dependencies
                                        if edge.producer_stage_id
                                        == stage_identity.candidate.stage_id
                                    }
                                    and instance.status
                                    not in {"blocked", "ready", "structurally_unavailable"}
                                    for instance in projection.stages.values()
                                ),
                                producer_invalidated=(
                                    projection.stages.get(
                                        stage_identity.candidate.semantic_prefix
                                    )
                                    is None
                                    or projection.stages[
                                        stage_identity.candidate.semantic_prefix
                                    ].status
                                    == "invalidated"
                                ),
                                run_cancelling=self._cancel_requested,
                            ),
                            blueprint=run_input.blueprint,
                            effective_max_concurrency=run_input.max_concurrency,
                            occurred_at=workflow.now(),
                            idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                            correlation_id=run_input.correlation_id,
                        ),
                        result_type=StageGraphResultActivityResult,
                        start_to_close_timeout=timeout,
                        retry_policy=retry,
                    )
                    if not decided.accepted:
                        raise ApplicationError(
                            f"StageGraph result rejected: {decided.reason_code}",
                            non_retryable=True,
                        )
                    projection = decided.projection
                    if (
                        decided.proposal.decision.value == "admit"
                        and observed_payload.get("evaluation") == "cycle"
                    ):
                        if pending_cycle is not None:
                            raise ApplicationError(
                                "multiple cycle evaluations require an authored "
                                "precedence decision",
                                non_retryable=True,
                            )
                        frontier_value = observed_payload.get("invalidation_frontier", ())
                        if not isinstance(frontier_value, list | tuple):
                            raise ApplicationError(
                                "StageGraph cycle invalidation frontier is not typed",
                                non_retryable=True,
                            )
                        cycle_scope = observed_payload.get("cycle_scope", "workflow")
                        if cycle_scope not in {"stage", "workflow"}:
                            raise ApplicationError(
                                "StageGraph cycle scope is not typed",
                                non_retryable=True,
                            )
                        pending_cycle = {
                            "cycle_scope": cycle_scope,
                            "stage_id": (
                                stage_identity.candidate.stage_id
                                if observed_payload.get("cycle_scope") == "stage"
                                else None
                            ),
                            "invalidation_frontier": tuple(
                                str(item) for item in frontier_value
                            ),
                            "next_objective": str(
                                observed_payload.get("next_objective", "")
                            ),
                            "evaluation_ref": str(
                                observed_payload.get("evaluation_ref", "")
                            ),
                            "evaluation_contract_ref": str(
                                observed_payload.get("evaluation_contract_ref", "")
                            ),
                            "objective_contract_ref": str(
                                observed_payload.get("objective_contract_ref", "")
                            ),
                        }
                continue

            if blocked_by_wait and not self._cancel_requested:
                wait_ids = frozenset(unsatisfied_wait_ids)

                def wait_released(
                    bound_wait_ids: frozenset[str] = wait_ids,
                ) -> bool:
                    return self._cancel_requested or any(
                        wait_id in self._satisfied_waits for wait_id in bound_wait_ids
                    )

                await workflow.wait_condition(wait_released)
                continue

            if pending_cycle is not None:
                cycled = await workflow.execute_activity(
                    "stagegraph.apply_cycle",
                    StageGraphCycleActivityRequest(
                        run_id=run_input.run_id,
                        request_scope=run_input.request_scope,
                        projection=projection,
                        blueprint=run_input.blueprint,
                        effective_max_concurrency=run_input.max_concurrency,
                        occurred_at=workflow.now(),
                        idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                        correlation_id=run_input.correlation_id,
                        **pending_cycle,
                    ),
                    result_type=StageGraphCycleActivityResult,
                    start_to_close_timeout=timeout,
                    retry_policy=retry,
                )
                if not cycled.accepted:
                    raise ApplicationError(
                        f"StageGraph cycle rejected: {cycled.reason_code}",
                        non_retryable=True,
                    )
                projection = cycled.projection
                reused_outputs.update(cycled.proposal.reused_output_refs)
                pending_cycle = None
                continue

            if run_input.force_continue_as_new and not active:
                workflow.continue_as_new(
                    replace(
                        run_input,
                        initial_projection=projection,
                        initial_run_version=projection.run_version,
                        force_continue_as_new=False,
                    )
                )

            completion = interpreter.completion(projection)
            if completion.can_terminalize:
                terminal = await workflow.execute_activity(
                    "stagegraph.complete",
                    StageGraphCompletionActivityRequest(
                        run_id=run_input.run_id,
                        request_scope=run_input.request_scope,
                        projection=projection,
                        proposal=completion,
                        workflow_type_digest=run_input.workflow_type_digest,
                        occurred_at=workflow.now(),
                        idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                        correlation_id=run_input.correlation_id,
                    ),
                    result_type=StageGraphCompletionActivityResult,
                    start_to_close_timeout=timeout,
                    retry_policy=retry,
                )
                if not terminal.accepted:
                    raise ApplicationError(
                        f"StageGraph terminalization rejected: {terminal.reason_code}",
                        non_retryable=True,
                    )
                return StageGraphRunResult(
                    run_id=run_input.run_id,
                    workflow_cycles=projection.workflow_cycle_ordinal,
                    execution_epoch=run_input.execution_epoch,
                    family_version=projection.family_version,
                    output_refs={
                        stage_id: max(
                            (
                                item
                                for item in projection.stages.values()
                                if item.candidate.stage_id == stage_id and item.output_refs
                            ),
                            key=lambda item: (
                                item.candidate.workflow_cycle_ordinal,
                                item.candidate.stage_cycle_ordinal,
                                item.semantic_attempt,
                            ),
                        ).output_refs
                        for stage_id in sorted(
                            {
                                item.candidate.stage_id
                                for item in projection.stages.values()
                                if item.output_refs
                            },
                            key=lambda item: item.encode("utf-8"),
                        )
                    },
                    reused_output_refs=reused_outputs,
                    schedule_trace=tuple(schedule_trace),
                    completion_proposal=completion,
                )
            raise ApplicationError(
                "StageGraph has no admissible work and no terminal completion proposal",
                type="stagegraph_blocked",
                non_retryable=True,
            )
