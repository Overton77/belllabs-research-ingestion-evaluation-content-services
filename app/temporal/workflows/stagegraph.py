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

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True

    @workflow.signal
    def satisfy_wait(self, condition_id: str) -> None:
        self._satisfied_waits.add(condition_id)

    @workflow.signal
    def resume_pause(self, decision_id: str) -> None:
        self._resumed_pauses.add(decision_id)

    @workflow.run
    async def run(self, run_input: StageGraphRunInput) -> StageGraphRunResult:
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

        while True:
            if self._cancel_requested:
                for handle, _task, _request, _identity in active.values():
                    handle.cancel()

            available = max(run_input.max_concurrency - len(active), 0)
            frontier = interpreter.frontier(
                projection,
                available_concurrency=available,
            )
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
                    continue

            if active:
                task_by_identity = {
                    identity: task
                    for identity, (_handle, task, _request, _stage_identity) in active.items()
                }
                done, _pending = await asyncio.wait(
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
                            disposition="failed",
                            message_cursor=operation_request.message_cursor,
                            effect_frontier=operation_request.effect_frontier,
                            active_async_child_ids=operation_request.active_async_child_ids,
                        )
                    accepted_order += 1
                    observation = StageResultObservation(
                        identity=stage_identity,
                        operation_result=dict(operation_result.result),
                        child_closed_or_quiesced=True,
                        reservations_and_usage_settled=True,
                        effects_settled=True,
                        cancellation_reconciled=(
                            not self._cancel_requested
                            or operation_result.disposition == "cancelled"
                        ),
                        accepted_order=accepted_order,
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
                                    stage_identity.candidate.stage_id
                                    in projection.invalidated_stage_ids
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
                continue

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
                        key: item.output_refs
                        for key, item in projection.stages.items()
                        if item.output_refs
                    },
                    reused_output_refs=reused_outputs,
                    schedule_trace=tuple(schedule_trace),
                    completion_proposal=completion,
                )
            if run_input.force_continue_as_new and not active:
                workflow.continue_as_new(
                    replace(
                        run_input,
                        initial_projection=projection,
                        initial_run_version=projection.run_version,
                        force_continue_as_new=False,
                    )
                )
            raise ApplicationError(
                "StageGraph has no admissible work and no terminal completion proposal",
                type="stagegraph_blocked",
                non_retryable=True,
            )
