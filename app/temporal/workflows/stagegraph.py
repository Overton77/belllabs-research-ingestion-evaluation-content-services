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
        FAMILY_EXECUTION_GENERATION,
        BoundaryCommandAck,
        BoundaryCommandDelivery,
        BoundaryLifecycleOutcome,
        BoundaryLifecycleRequest,
        CandidateOrderingKey,
        ExecutionIdentity,
        FamilyPause,
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
    from app.domain.orchestration.search_attributes import run_search_attributes
    from app.domain.run_control.contracts import ExecutionTarget
    from app.temporal.search_attributes import (
        child_search_attributes,
        ensure_workflow_search_attributes,
        operation_workflow_search_attributes,
    )
    from app.temporal.workflows.operation import OperationWorkflow

# RRM-007 patches: each guards a command sequence that an older history did not emit.
GOVERNED_WAITS_PATCH = "rrm-007-governed-waits"
DECLARED_WAITS_PATCH = "rrm-007-declared-waits"
QUIESCENCE_PATCH = "rrm-007-quiescence"
WAIT_CONDITION_PREFIX = "stagegraph-wait:"
WORKFLOW_PAUSE_SCOPES = frozenset({"run", "workflow"})


def wait_condition_id(wait_id: str) -> str:
    """The run-control wait condition that a declared StageGraph wait is recorded under."""

    return f"{WAIT_CONDITION_PREFIX}{wait_id}"


@workflow.defn(name="belllabs.stagegraph")
class StageGraphWorkflow:
    """Replay-safe incremental mechanics for canonical StageGraph V2.

    RRM-007 (REQ-BP-SG-009, REQ-CP-EXEC-006/007/011): a declared wait is declared to run
    control when it blocks admissible work, so it is inspectable, and is released only by an
    accepted `satisfy_wait` delivered through `deliver_boundary_command` and applied here at
    the admission boundary. Pauses and resumes follow the same path. Satisfied waits,
    applied pauses and delivered-but-unapplied commands are carried across Continue-As-New.
    """

    def __init__(self) -> None:
        self._cancel_requested = False
        self._satisfied_waits: set[str] = set()
        self._declared_waits: set[str] = set()
        self._resumed_pauses: set[str] = set()
        self._active_pauses: dict[str, tuple[str, ...]] = {}
        self._pending_commands: list[BoundaryCommandDelivery] = []
        self._command_acks: dict[str, BoundaryCommandAck] = {}
        self._applied_command_ids: set[str] = set()
        self._quiescent = False
        self._execution_epoch = 1
        self._technical_segment = 1
        self._runtime_state: dict[str, Any] = {}

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True

    @workflow.signal
    def satisfy_wait(self, condition_id: str) -> None:
        # REQ-CP-EXEC-007 (AMD-RRM-001): a raw signal is not a governed release path. It is
        # kept as a hint only; the legacy release survives for histories recorded before the
        # patch so that they replay unchanged.
        if not workflow.patched(GOVERNED_WAITS_PATCH):
            self._satisfied_waits.add(condition_id)

    @workflow.signal
    def resume_pause(self, decision_id: str) -> None:
        # Never a release path; recorded as a hint only.
        self._resumed_pauses.add(decision_id)

    @workflow.update
    def deliver_boundary_command(self, delivery: BoundaryCommandDelivery) -> BoundaryCommandAck:
        """Acknowledge one accepted command: evidence of `delivered`, never of `applied`."""

        prior = self._command_acks.get(delivery.command_id)
        if prior is not None:
            return replace(prior, status="duplicate")
        status: str
        if delivery.execution_generation != FAMILY_EXECUTION_GENERATION:
            status = "stale_generation"
        elif delivery.execution_epoch != self._execution_epoch:
            status = "stale_target"
        elif delivery.command_id in self._applied_command_ids:
            status = "duplicate"
        else:
            status = "delivered"
            self._pending_commands.append(delivery)
        ack = BoundaryCommandAck(
            command_id=delivery.command_id,
            status=status,  # type: ignore[arg-type]
            technical_segment=self._technical_segment,
        )
        self._command_acks[delivery.command_id] = ack
        return ack

    @workflow.query
    def satisfied_waits(self) -> tuple[str, ...]:
        return tuple(sorted(self._satisfied_waits))

    @workflow.query
    def runtime_state(self) -> dict[str, Any]:
        return self._runtime_state

    @workflow.query
    def boundary_state(self) -> dict[str, Any]:
        """Diagnostic only (REQ-CP-EXEC-007); the receipt ledger is the authority."""

        return {
            "pending_command_ids": [item.command_id for item in self._pending_commands],
            "applied_command_ids": sorted(self._applied_command_ids),
            "active_pauses": {key: list(value) for key, value in self._active_pauses.items()},
            "satisfied_wait_ids": sorted(self._satisfied_waits),
            "declared_wait_ids": sorted(self._declared_waits),
            "technical_segment": self._technical_segment,
            "quiescent": self._quiescent,
        }

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
        self._execution_epoch = run_input.execution_epoch
        self._technical_segment = run_input.technical_segment
        self._satisfied_waits.update(run_input.satisfied_wait_ids)
        self._declared_waits.update(run_input.declared_wait_ids)
        self._active_pauses.update(
            {item.decision_id: tuple(item.scope) for item in run_input.active_pauses}
        )
        self._applied_command_ids.update(run_input.applied_boundary_command_ids)
        self._quiescent = run_input.quiescent
        carried = {item.command_id for item in self._pending_commands}
        self._pending_commands.extend(
            item for item in run_input.pending_boundary_commands if item.command_id not in carried
        )
        interpreter = StageGraphInterpreter(
            blueprint,
            effective_max_concurrency=run_input.max_concurrency,
        )
        attribute_policy = run_input.search_attribute_policy
        ensure_workflow_search_attributes(
            attribute_policy,
            run_search_attributes(
                workflow_kind="family",
                run_id=run_input.run_id,
                request_scope=run_input.request_scope,
                family="StageGraph",
                execution_epoch=run_input.execution_epoch,
            ),
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
        boundary_ref = workflow.info().workflow_id
        if run_input.initial_projection is None:
            parent = workflow.info().parent
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
                    execution_target=ExecutionTarget(
                        family="StageGraph",
                        family_workflow_id=boundary_ref,
                        root_workflow_id=parent.workflow_id if parent is not None else None,
                        execution_epoch=run_input.execution_epoch,
                        execution_generation=FAMILY_EXECUTION_GENERATION,
                    ),
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

        def blocked_candidates(
            projection_now: Any,
        ) -> tuple[frozenset[str], set[str], frozenset[str]]:
            """Candidates blocked by unsatisfied waits and by applied pauses."""

            unsatisfied = {
                item.wait_id
                for item in blueprint.waits
                if item.wait_id not in self._satisfied_waits
            }
            by_wait = frozenset(
                instance.candidate.semantic_prefix
                for instance in projection_now.stages.values()
                if instance.status in {"ready", "blocked"}
                and any(
                    wait.wait_id in unsatisfied
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
            by_pause = frozenset(
                instance.candidate.semantic_prefix
                for instance in projection_now.stages.values()
                if instance.status in {"ready", "blocked"}
                and any(
                    _pause_covers(scope, instance.candidate.stage_id)
                    for scope in self._active_pauses.values()
                )
            )
            return by_wait, unsatisfied, by_pause

        def runnable_work_remains(projection_now: Any) -> bool:
            if active:
                return True
            by_wait, _unsatisfied, by_pause = blocked_candidates(projection_now)
            return bool(
                interpreter.frontier(
                    projection_now,
                    available_concurrency=max(
                        run_input.max_concurrency
                        - interpreter.running_concurrency(projection_now),
                        0,
                    ),
                    blocked_candidate_keys=by_wait | by_pause,
                )
            )

        while True:
            if self._cancel_requested:
                for identity, (handle, _task, _request, _stage_identity) in active.items():
                    cancellation_requested_children.add(identity)
                    handle.cancel()

            # RRM-007: apply delivered commands at the admission boundary, in target order.
            # This path exists only in histories that carry deliveries, so it needs no patch.
            applied_any = False
            while self._pending_commands and not self._cancel_requested:
                delivery = min(self._pending_commands, key=lambda item: item.target_sequence)
                self._pending_commands.remove(delivery)
                applicable, rejection = self._decide(delivery, blueprint)
                outcome = await self._apply(
                    run_input,
                    delivery,
                    boundary_ref,
                    applicable=applicable,
                    rejection=rejection,
                    runnable=runnable_work_remains(projection),
                    activity_timeout=timeout,
                )
                self._applied_command_ids.add(delivery.command_id)
                if outcome.accepted:
                    projection = replace(projection, run_version=outcome.resulting_run_version)
                    applied_any = True
                    self._quiescent = not runnable_work_remains(projection)
            if applied_any and run_input.force_continue_as_new and not active:
                # REQ-CP-EXEC-011: handlers quiesce, then the satisfied waits, applied pauses
                # and pending commands continue into the next technical segment.
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(self._continuation(run_input, projection))

            available = max(
                run_input.max_concurrency - interpreter.running_concurrency(projection),
                0,
            )
            blocked_by_wait, unsatisfied_wait_ids, blocked_by_pause = blocked_candidates(
                projection
            )
            frontier = (
                ()
                if self._cancel_requested
                else interpreter.frontier(
                    projection,
                    available_concurrency=available,
                    blocked_candidate_keys=blocked_by_wait | blocked_by_pause,
                )
            )
            self._runtime_state = {
                "unsatisfied_wait_ids": tuple(sorted(unsatisfied_wait_ids)),
                "blocked_candidate_count": len(blocked_by_wait | blocked_by_pause),
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
                    operation_attributes = child_search_attributes(
                        attribute_policy,
                        operation_workflow_search_attributes(
                            operation,
                            family="StageGraph",
                            execution_epoch=run_input.execution_epoch,
                        ),
                    )
                    if operation_attributes is not None:
                        operation = operation.model_copy(
                            update={"search_attribute_policy": attribute_policy}
                        )
                    handle = await workflow.start_child_workflow(
                        OperationWorkflow.run,
                        operation,
                        id=operation.workflow_id,
                        task_queue=workflow.info().task_queue,
                        parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
                        search_attributes=operation_attributes,
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
                tasks = tuple(task_by_identity.values())

                def child_done_or_command_delivered(
                    bound: tuple[asyncio.Future[OperationWorkflowResult], ...] = tasks,
                ) -> bool:
                    # Wake on the first completed child, or on a delivered command so that a
                    # pause or release applies at this admission boundary (new histories only).
                    return (
                        any(task.done() for task in bound)
                        or bool(self._pending_commands)
                        or self._cancel_requested
                    )

                await workflow.wait_condition(child_done_or_command_delivered)
                done = {task for task in tasks if task.done()}
                if not done:
                    continue
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

            if (blocked_by_wait or blocked_by_pause) and not self._cancel_requested:
                wait_ids = frozenset(unsatisfied_wait_ids)
                # REQ-BP-SG-009: a held wait is inspectable through authority while running.
                blocking_waits = tuple(
                    item
                    for item in blueprint.waits
                    if item.wait_id in wait_ids and item.wait_id not in self._declared_waits
                )
                if blocking_waits and workflow.patched(DECLARED_WAITS_PATCH):
                    for declared in blocking_waits:
                        outcome = await self._boundary_fact(
                            run_input,
                            BoundaryLifecycleRequest(
                                command_id=(
                                    f"stagegraph:{run_input.run_id}:epoch:"
                                    f"{run_input.execution_epoch}:wait:{declared.wait_id}"
                                ),
                                action={
                                    "kind": "set_wait",
                                    "condition": {
                                        "condition_id": wait_condition_id(declared.wait_id),
                                        "kind": "approval",
                                        "scope": [f"{declared.scope_kind}:{declared.scope_id}"],
                                        "verification_ref": wait_condition_id(declared.wait_id),
                                        "timeout_policy_ref": "timeout:none",
                                    },
                                    "runnable_work_remains": False,
                                },
                                reason=f"StageGraph declared wait {declared.wait_id} holds",
                                boundary_ref=boundary_ref,
                            ),
                            timeout,
                        )
                        self._declared_waits.add(declared.wait_id)
                        if outcome.accepted:
                            projection = replace(
                                projection, run_version=outcome.resulting_run_version
                            )
                if (
                    self._active_pauses
                    and not self._quiescent
                    and workflow.patched(QUIESCENCE_PATCH)
                ):
                    # REQ-CP-RUN-004: the aggregate phase reflects that no admissible work
                    # remains under the applied scoped pauses.
                    outcome = await self._boundary_fact(
                        run_input,
                        BoundaryLifecycleRequest(
                            command_id=(
                                f"stagegraph:{run_input.run_id}:epoch:"
                                f"{run_input.execution_epoch}:quiescence:"
                                f"{projection.run_version}"
                            ),
                            action={
                                "kind": "observe_quiescence",
                                "boundary_ref": boundary_ref,
                                "runnable_work_remains": False,
                            },
                            reason="StageGraph has no admissible work under its pauses",
                            boundary_ref=boundary_ref,
                        ),
                        timeout,
                    )
                    self._quiescent = True
                    if outcome.accepted:
                        projection = replace(projection, run_version=outcome.resulting_run_version)

                def wait_released(
                    bound_wait_ids: frozenset[str] = wait_ids,
                ) -> bool:
                    return (
                        self._cancel_requested
                        or bool(self._pending_commands)
                        or any(wait_id in self._satisfied_waits for wait_id in bound_wait_ids)
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
                workflow.continue_as_new(self._continuation(run_input, projection))

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

    # --- RRM-007 boundary mechanics ---------------------------------------------------------

    def _decide(
        self, delivery: BoundaryCommandDelivery, blueprint: StageGraphBlueprint
    ) -> tuple[bool, str]:
        """Apply the delivered command to the family's boundary state, if it applies here.

        Returns `(applicable, rejection_reason)`. A release whose wait is not held, a resume
        of an unknown pause, or a pause with an unknown scope is `not_applicable`.
        """

        payload = delivery.payload
        if delivery.kind == "satisfy_wait":
            condition_id = str(payload.get("condition_id", ""))
            wait_id = condition_id.removeprefix(WAIT_CONDITION_PREFIX)
            if (
                not condition_id.startswith(WAIT_CONDITION_PREFIX)
                or wait_id in self._satisfied_waits
                or wait_id not in {item.wait_id for item in blueprint.waits}
            ):
                return False, "not_applicable"
            self._satisfied_waits.add(wait_id)
            return True, ""
        if delivery.kind == "pause":
            decision = payload.get("decision") or {}
            decision_id = str(decision.get("decision_id", ""))
            scope = tuple(sorted(str(item) for item in decision.get("scope", ())))
            known_stages = {item.stage_id for item in blueprint.stages}
            if (
                not decision_id
                or decision_id in self._active_pauses
                or not scope
                or any(not _known_scope(item, known_stages) for item in scope)
            ):
                return False, "not_applicable"
            self._active_pauses[decision_id] = scope
            return True, ""
        decision = payload.get("decision") or {}
        pause_id = str(decision.get("pause_decision_id", ""))
        if pause_id not in self._active_pauses:
            return False, "not_applicable"
        del self._active_pauses[pause_id]
        return True, ""

    async def _apply(
        self,
        run_input: StageGraphRunInput,
        delivery: BoundaryCommandDelivery,
        boundary_ref: str,
        *,
        applicable: bool,
        rejection: str,
        runnable: bool,
        activity_timeout: timedelta,
    ) -> BoundaryLifecycleOutcome:
        if not applicable:
            return await self._boundary_fact(
                run_input,
                BoundaryLifecycleRequest(
                    command_id=f"boundary-reject:{delivery.command_id}",
                    action={},
                    reason=f"StageGraph boundary cannot apply {delivery.kind}",
                    boundary_ref=boundary_ref,
                    boundary_command_id=delivery.command_id,
                    rejection_reason=rejection,
                ),
                activity_timeout,
            )
        return await self._boundary_fact(
            run_input,
            BoundaryLifecycleRequest(
                command_id=f"boundary-apply:{delivery.command_id}",
                action={
                    "kind": "apply_boundary_command",
                    "command_id": delivery.command_id,
                    "action": delivery.payload,
                    "boundary_ref": boundary_ref,
                    "runnable_work_remains": runnable,
                    "boundary_state": {
                        "family": "StageGraph",
                        "technical_segment": self._technical_segment,
                        "satisfied_wait_ids": sorted(self._satisfied_waits),
                        "active_pause_ids": sorted(self._active_pauses),
                    },
                },
                reason=f"StageGraph boundary applied {delivery.kind}",
                boundary_ref=boundary_ref,
                boundary_command_id=delivery.command_id,
            ),
            activity_timeout,
        )

    async def _boundary_fact(
        self,
        run_input: StageGraphRunInput,
        request: BoundaryLifecycleRequest,
        activity_timeout: timedelta,
    ) -> BoundaryLifecycleOutcome:
        return await workflow.execute_activity(
            "stagegraph.apply_boundary_command",
            replace(
                request,
                run_id=run_input.run_id,
                request_scope=run_input.request_scope,
                idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                correlation_id=run_input.correlation_id,
                occurred_at=workflow.now(),
            ),
            result_type=BoundaryLifecycleOutcome,
            start_to_close_timeout=activity_timeout,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    def _continuation(self, run_input: StageGraphRunInput, projection: Any) -> StageGraphRunInput:
        return replace(
            run_input,
            initial_projection=projection,
            initial_run_version=projection.run_version,
            force_continue_as_new=False,
            technical_segment=self._technical_segment + 1,
            satisfied_wait_ids=tuple(sorted(self._satisfied_waits)),
            declared_wait_ids=tuple(sorted(self._declared_waits)),
            active_pauses=tuple(
                FamilyPause(decision_id=key, scope=value)
                for key, value in sorted(self._active_pauses.items())
            ),
            pending_boundary_commands=tuple(
                sorted(self._pending_commands, key=lambda item: item.target_sequence)
            ),
            applied_boundary_command_ids=tuple(sorted(self._applied_command_ids)),
            quiescent=self._quiescent,
        )


def _pause_covers(scope: tuple[str, ...], stage_id: str) -> bool:
    return any(item in WORKFLOW_PAUSE_SCOPES or item == f"stage:{stage_id}" for item in scope)


def _known_scope(item: str, stage_ids: set[str]) -> bool:
    if item in WORKFLOW_PAUSE_SCOPES:
        return True
    prefix, separator, stage_id = item.partition(":")
    return separator == ":" and prefix == "stage" and stage_id in stage_ids
