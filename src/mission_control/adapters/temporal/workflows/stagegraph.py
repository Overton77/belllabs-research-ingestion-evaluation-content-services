from __future__ import annotations

import asyncio
import contextlib
from dataclasses import replace
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from mission_control.adapters.temporal.search_attributes import (
        child_search_attributes,
        ensure_workflow_search_attributes,
        operation_workflow_search_attributes,
        upsert_family_phase,
    )
    from mission_control.adapters.temporal.workflows.human_gate import (
        HumanGateWorkflow,
        HumanGateWorkflowInput,
    )
    from mission_control.adapters.temporal.workflows.operation import (
        MissionOperationWorkflow,
        OperationWorkflow,
        settle_superseded_generation,
        superseded_generation,
    )
    from mission_control.contracts.identities import mission_operation_id
    from mission_control.domain.authoring.canonical import sha256_digest
    from mission_control.domain.authoring.contracts import StageGraphBlueprint
    from mission_control.domain.execution.contracts import (
        OperationWorkflowRequest,
        OperationWorkflowResult,
    )
    from mission_control.domain.policies.contracts import ExecutionTarget
    from mission_control.domain.programs.contracts import (
        FAMILY_EXECUTION_GENERATION,
        BoundaryCommandAck,
        BoundaryCommandDelivery,
        BoundaryLifecycleOutcome,
        BoundaryLifecycleRequest,
        CancelAck,
        CancelDelivery,
        CandidateOrderingKey,
        ExecutionIdentity,
        FamilyPause,
        LateResultFacts,
        StageGraphAdmissionActivityRequest,
        StageGraphAdmissionActivityResult,
        StageGraphBaselineSettlementRequest,
        StageGraphBaselineSettlementResult,
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
    from mission_control.domain.programs.human_gate import (
        GateReservationSettlementRequest,
        GateReservationSettlementResult,
        HumanGateOutcome,
        HumanGateSpec,
        ReviewPacketItem,
        open_activation,
        packet_item,
        stagegraph_gate_result,
    )
    from mission_control.domain.programs.interpreter import StageGraphInterpreter
    from mission_control.domain.programs.search_attributes import run_search_attributes

# RRM-007 patches: each guards a command sequence that an older history did not emit.
GOVERNED_WAITS_PATCH = "rrm-007-governed-waits"
DECLARED_WAITS_PATCH = "rrm-007-declared-waits"
QUIESCENCE_PATCH = "rrm-007-quiescence"
# RRM-008 (REQ-CP-EXEC-008 step 7): under a delivered cancel the family admits nothing new,
# lets every active operation settle through its own saga, rejects delivered-but-unapplied
# commands `superseded`, then proposes terminal `cancelled` and waits for any liability the
# operator must reconcile. Histories recorded before the patch replay unchanged.
CANCELLATION_SAGA_PATCH = "rrm-008-stagegraph-cancellation-saga"
# RRM-008 re-review: an active unit whose generation was superseded before the cancel ends
# `in_doubt` / `generation_superseded` with an unsettled claim; under the saga the family runs
# `operation.cancel` for it once so that the operation boundary settles the claim.
SETTLE_SUPERSEDED_PATCH = "rrm-008-stagegraph-settle-superseded-generation"
# RRM-021 (REQ-CP-RUN-006): a run admitted with a baseline reservation releases it before it
# proposes terminalization (every terminal outcome), so the reducer finds no reservation left.
# Histories recorded before the patch replay unchanged.
SETTLE_BASELINE_PATCH = "rrm-021-settle-stagegraph-baseline"
# RRM-021 review: a family that fails with no admissible work (`stagegraph_blocked`) releases
# the baseline before the failure is raised.
RELEASE_BASELINE_ON_BLOCKED_PATCH = "rrm-021-release-baseline-on-blocked"
# MP-10 (SPEC-03 "Explicit Human Gate"): a stage declared a Human Gate in the run input is
# executed as a `mc.human_gate.v1` control activation child instead of an operation child: it
# holds no activity or cognition slot while the human decides, releases its admitted
# reservation against zero usage, and reports the resolution as the stage result (a
# `request_changes` becomes the declared remediation workflow cycle). Runs without declared
# gates never reach the patch.
STAGEGRAPH_HUMAN_GATE_PATCH = "mp10-stagegraph-human-gate"
LIABILITY_REJECTIONS = frozenset(
    {
        "budget_not_settled",
        "effects_not_settled",
        "unresolved_async_children",
        "unresolved_terminal_dependencies",
        "cancellation_not_settled",
    }
)
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
        self._command_acks: dict[tuple[str, str], BoundaryCommandAck] = {}
        self._applied_command_ids: set[tuple[str, str]] = set()
        self._quiescent = False
        self._execution_epoch = 1
        self._technical_segment = 1
        self._last_delivered_sequence = 0
        self._runtime_state: dict[str, Any] = {}
        self._cancel_acks: dict[str, CancelAck] = {}
        self._cancel_command: tuple[str, str] | None = None
        self._liability_hints = 0

    @workflow.signal
    def request_cancel(self) -> None:
        # The root-internal propagation channel of pre-RRM-008 histories; the governed path
        # is run control -> ledger -> `deliver_cancel` (REQ-CP-EXEC-008 steps 1-2).
        self._cancel_requested = True

    @workflow.signal
    def liability_reconciled(self, reference: str) -> None:
        """A compact wake-up hint: a liability the cancellation saga waits for was
        reconciled. It releases nothing; the saga proposes terminalization again."""

        del reference
        self._liability_hints += 1

    @workflow.update
    def deliver_cancel(self, delivery: CancelDelivery) -> CancelAck:
        """Acknowledge the journaled cancel: evidence of `delivered`; the saga applies it."""

        prior = self._cancel_acks.get(delivery.command_id)
        if prior is not None:
            return replace(prior, status="duplicate")
        status: str
        if delivery.execution_generation != FAMILY_EXECUTION_GENERATION:
            status = "stale_generation"
        elif delivery.execution_epoch != self._execution_epoch:
            status = "stale_target"
        else:
            status = "delivered"
            self._cancel_requested = True
            self._cancel_command = (delivery.idempotency_issuer, delivery.command_id)
        ack = CancelAck(
            command_id=delivery.command_id,
            status=status,  # type: ignore[arg-type]
            technical_segment=self._technical_segment,
        )
        self._cancel_acks[delivery.command_id] = ack
        return ack

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

        key = _command_key(delivery)
        prior = self._command_acks.get(key)
        if prior is not None:
            return replace(prior, status="duplicate")
        status: str
        if delivery.execution_generation != FAMILY_EXECUTION_GENERATION:
            status = "stale_generation"
        elif delivery.execution_epoch != self._execution_epoch:
            status = "stale_target"
        elif key in self._applied_command_ids:
            status = "duplicate"
        elif delivery.target_sequence != self._last_delivered_sequence + 1:
            # F7: the family refuses a non-contiguous sequence; a gap is transient and is
            # not cached, so the redelivery after the missing command is decided again.
            return BoundaryCommandAck(
                command_id=delivery.command_id,
                status="gap",
                technical_segment=self._technical_segment,
                detail=f"expected sequence {self._last_delivered_sequence + 1}",
            )
        else:
            status = "delivered"
            self._last_delivered_sequence = delivery.target_sequence
            self._pending_commands.append(delivery)
        ack = BoundaryCommandAck(
            command_id=delivery.command_id,
            status=status,  # type: ignore[arg-type]
            technical_segment=self._technical_segment,
        )
        self._command_acks[key] = ack
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
            "applied_command_ids": sorted(key[1] for key in self._applied_command_ids),
            "active_pauses": {key: list(value) for key, value in self._active_pauses.items()},
            "satisfied_wait_ids": sorted(self._satisfied_waits),
            "declared_wait_ids": sorted(self._declared_waits),
            "technical_segment": self._technical_segment,
            "quiescent": self._quiescent,
            "last_delivered_sequence": self._last_delivered_sequence,
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
        self._applied_command_ids.update(
            _split_key(item) for item in run_input.applied_boundary_command_ids
        )
        self._quiescent = run_input.quiescent
        self._last_delivered_sequence = max(
            self._last_delivered_sequence, run_input.last_delivered_sequence
        )
        carried = {_command_key(item) for item in self._pending_commands}
        self._pending_commands.extend(
            item
            for item in run_input.pending_boundary_commands
            if _command_key(item) not in carried
        )
        # RRM-008 (REQ-CP-EXEC-011): a cancel delivered before Continue-As-New stays delivered.
        self._cancel_requested = self._cancel_requested or run_input.cancel_requested
        interpreter = StageGraphInterpreter(
            blueprint,
            effective_max_concurrency=run_input.max_concurrency,
        )
        human_gates = _human_gates(run_input, blueprint)
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
        # FT-C4: the family's phase is visible to `run list --query "phase='executing'"`.
        upsert_family_phase(attribute_policy, run_input.run_id, "executing")
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
            if initialized.phase == "cancelling" and workflow.patched(CANCELLATION_SAGA_PATCH):
                # The cancel was accepted before the start fact: run the saga at once.
                self._cancel_requested = True

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
        # MP-10: a gate's `request_changes` cycle is applied before anything else is admitted,
        # so no consumer of the gate runs on a review that was not accepted.
        gate_cycle_pending = False
        liability_hints_seen = self._liability_hints
        baseline_settled = False
        cancellation_backoff = run_input.cancellation_retry_seconds

        def blocked_candidates(
            projection_now: Any,
            satisfied: set[str] | None = None,
            pauses: dict[str, tuple[str, ...]] | None = None,
        ) -> tuple[frozenset[str], set[str], frozenset[str]]:
            """Candidates blocked by unsatisfied waits and by applied pauses."""

            satisfied = self._satisfied_waits if satisfied is None else satisfied
            pauses = self._active_pauses if pauses is None else pauses
            unsatisfied = {
                item.wait_id for item in blueprint.waits if item.wait_id not in satisfied
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
                    _pause_covers(scope, instance.candidate.stage_id) for scope in pauses.values()
                )
            )
            return by_wait, unsatisfied, by_pause

        def runnable_work_remains(
            projection_now: Any,
            satisfied: set[str] | None = None,
            pauses: dict[str, tuple[str, ...]] | None = None,
        ) -> bool:
            if active:
                return True
            by_wait, _unsatisfied, by_pause = blocked_candidates(projection_now, satisfied, pauses)
            return bool(
                interpreter.frontier(
                    projection_now,
                    available_concurrency=max(
                        run_input.max_concurrency - interpreter.running_concurrency(projection_now),
                        0,
                    ),
                    blocked_candidate_keys=by_wait | by_pause,
                )
            )

        while True:
            if self._cancel_requested:
                for identity, (handle, _task, _request, _stage_identity) in active.items():
                    if identity in cancellation_requested_children:
                        continue  # one cancel request per child (the server refuses a second)
                    cancellation_requested_children.add(identity)
                    handle.cancel()

            # RRM-007: apply delivered commands at the admission boundary, in target order.
            # This path exists only in histories that carry deliveries, so it needs no patch.
            applied_any = False
            while self._pending_commands:
                if self._cancel_requested and not workflow.patched(CANCELLATION_SAGA_PATCH):
                    break
                delivery = min(self._pending_commands, key=lambda item: item.target_sequence)
                self._pending_commands.remove(delivery)
                if self._cancel_requested:
                    # RRM-008: a command delivered but not applied is superseded by the cancel.
                    outcome = await self._apply(
                        run_input,
                        delivery,
                        boundary_ref,
                        rejection="superseded",
                        runnable=False,
                        activity_timeout=timeout,
                    )
                    self._mark_handled(delivery, outcome)
                    continue
                # F4: decide purely; the family state changes only when authority applied.
                satisfied, pauses, rejection = self._decide(delivery, blueprint)
                outcome = await self._apply(
                    run_input,
                    delivery,
                    boundary_ref,
                    rejection=rejection,
                    runnable=runnable_work_remains(projection, satisfied, pauses),
                    activity_timeout=timeout,
                )
                self._mark_handled(delivery, outcome)
                if outcome.accepted:
                    self._satisfied_waits, self._active_pauses = satisfied, pauses
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
            blocked_by_wait, unsatisfied_wait_ids, blocked_by_pause = blocked_candidates(projection)
            frontier = (
                ()
                if self._cancel_requested or gate_cycle_pending
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
                        effective_configuration_digest=(run_input.effective_configuration_digest),
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
                    mission_owned = workflow.info().workflow_id.startswith("mc/")
                    if mission_owned and (
                        operation.operation.request_scope != run_input.request_scope
                        or operation.operation.identity.run_id != run_input.run_id
                    ):
                        raise ApplicationError(
                            "operation differs from its admitted parent run",
                            type="InvalidMissionBinding",
                            non_retryable=True,
                        )
                    gate = human_gates.get(proposal.identity.candidate.stage_id)
                    if gate is not None and workflow.patched(STAGEGRAPH_HUMAN_GATE_PATCH):
                        handle, unit = await self._start_human_gate(
                            run_input, blueprint, projection, proposal, operation, gate, timeout
                        )
                    else:
                        handle = await workflow.start_child_workflow(
                            (
                                MissionOperationWorkflow.run
                                if mission_owned
                                else OperationWorkflow.run
                            ),
                            operation,
                            id=(
                                mission_operation_id(
                                    run_input.request_scope,
                                    run_input.run_id,
                                    operation.semantic_attempt_id,
                                )
                                if mission_owned
                                else operation.workflow_id
                            ),
                            task_queue=workflow.info().task_queue,
                            parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
                            search_attributes=operation_attributes,
                        )
                        unit = asyncio.ensure_future(handle)
                    active[proposal.identity.semantic_key] = (
                        handle,
                        unit,
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
                                cancellation_requested_children.add(sibling_identity.semantic_key)
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
                    identities: tuple[str, ...] = tuple(task_by_identity),
                ) -> bool:
                    # Wake on the first completed child, on a delivered command so that a
                    # pause or release applies at this admission boundary (new histories
                    # only), or on a cancel that an active child has not received yet.
                    return (
                        any(task.done() for task in bound)
                        or bool(self._pending_commands)
                        or (
                            self._cancel_requested
                            and any(
                                identity not in cancellation_requested_children
                                for identity in identities
                            )
                        )
                    )

                await workflow.wait_condition(child_done_or_command_delivered)
                done = {task for task in tasks if task.done()}
                if not done:
                    continue
                completed_identities = sorted(
                    (identity for identity, task in task_by_identity.items() if task in done),
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
                    if (
                        self._cancel_requested
                        and superseded_generation(operation_result)
                        and workflow.patched(CANCELLATION_SAGA_PATCH)
                        and workflow.patched(SETTLE_SUPERSEDED_PATCH)
                    ):
                        # A cancelling run admits no new generation: settle the superseded
                        # generation through the operation boundary instead of leaving its
                        # claim as a liability that nothing will ever settle.
                        operation_result = await settle_superseded_generation(
                            operation_request, operation_result
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
                        # RRM-008: under cancellation a unit that settled (cancelled, or
                        # completed/failed before the cancel reached it) is reconciled; only
                        # an in_doubt unit keeps its liability open for the operator.
                        cancellation_reconciled=(
                            not self._cancel_requested or operation_result.disposition != "in_doubt"
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
                                    projection.stages.get(stage_identity.candidate.semantic_prefix)
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
                            "invalidation_frontier": tuple(str(item) for item in frontier_value),
                            "next_objective": str(observed_payload.get("next_objective", "")),
                            "evaluation_ref": str(observed_payload.get("evaluation_ref", "")),
                            "evaluation_contract_ref": str(
                                observed_payload.get("evaluation_contract_ref", "")
                            ),
                            "objective_contract_ref": str(
                                observed_payload.get("objective_contract_ref", "")
                            ),
                        }
                        gate_cycle_pending = "human_gate" in observed_payload
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
                gate_cycle_pending = False
                continue

            if run_input.force_continue_as_new and not active:
                workflow.continue_as_new(self._continuation(run_input, projection))

            completion = interpreter.completion(projection)
            cancelling = self._cancel_requested and workflow.patched(CANCELLATION_SAGA_PATCH)
            if cancelling:
                # REQ-CP-EXEC-008 step 7: every producer liability is closed (each active unit
                # settled through its own saga); unresolved dependencies are cancelled.
                completion = replace(completion, cancelled=True)
                if not completion.can_terminalize:
                    raise ApplicationError(
                        "StageGraph cancellation left a producer liability open: "
                        + ", ".join(completion.open_producer_liability_ids),
                        type="stagegraph_cancellation_unresolved",
                        non_retryable=True,
                    )
            if completion.can_terminalize:
                if self._pending_commands:
                    # F1: drain what was delivered during the final cycle before closing.
                    continue
                if self._active_pauses and not cancelling:
                    # Terminalization requires every pause resumed (reducer rule): the family
                    # holds at this boundary until the resume is delivered.
                    if not self._quiescent and workflow.patched(QUIESCENCE_PATCH):
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
                                reason="StageGraph completed its admissible work under a pause",
                                boundary_ref=boundary_ref,
                            ),
                            timeout,
                        )
                        self._quiescent = True
                        if outcome.accepted:
                            projection = replace(
                                projection, run_version=outcome.resulting_run_version
                            )
                    await workflow.wait_condition(
                        lambda: self._cancel_requested or bool(self._pending_commands)
                    )
                    continue
                if (
                    run_input.baseline_reservation
                    and not baseline_settled
                    and workflow.patched(SETTLE_BASELINE_PATCH)
                ):
                    await self._release_baseline(run_input, timeout, retry)
                    baseline_settled = True
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
                if (
                    not terminal.accepted
                    and cancelling
                    and (terminal.reason_code in LIABILITY_REJECTIONS)
                ):
                    # A liability remains (an async child's pending usage, an in_doubt unit):
                    # the run stays `cancelling`; wait for the reconciliation hint or the
                    # timer, then propose again.
                    def reconciled(waited: int = liability_hints_seen) -> bool:
                        return self._liability_hints > waited

                    with contextlib.suppress(TimeoutError):
                        await workflow.wait_condition(
                            reconciled, timeout=timedelta(seconds=cancellation_backoff)
                        )
                    liability_hints_seen = self._liability_hints
                    cancellation_backoff = min(cancellation_backoff * 2, 3_600)
                    projection = replace(projection, run_version=terminal.resulting_run_version)
                    continue
                if not terminal.accepted:
                    raise ApplicationError(
                        f"StageGraph terminalization rejected: {terminal.reason_code}",
                        non_retryable=True,
                    )
                upsert_family_phase(
                    attribute_policy,
                    run_input.run_id,
                    "cancelled" if completion.cancelled else "completed",
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
            if (
                run_input.baseline_reservation
                and not baseline_settled
                and workflow.patched(RELEASE_BASELINE_ON_BLOCKED_PATCH)
            ):
                # The family fails without proposing terminalization: the admitted baseline
                # must not stay reserved (a later failed terminalization would otherwise be
                # rejected `budget_not_settled`).
                await self._release_baseline(run_input, timeout, retry)
            raise ApplicationError(
                "StageGraph has no admissible work and no terminal completion proposal",
                type="stagegraph_blocked",
                non_retryable=True,
            )

    async def _start_human_gate(
        self,
        run_input: StageGraphRunInput,
        blueprint: StageGraphBlueprint,
        projection: Any,
        proposal: Any,
        operation: OperationWorkflowRequest,
        spec: HumanGateSpec,
        activity_timeout: timedelta,
    ) -> tuple[Any, asyncio.Future[OperationWorkflowResult]]:
        """MP-10: open the gate stage's control activation; no operation child is started."""

        candidate = proposal.identity.candidate
        activation = open_activation(
            request_scope=run_input.request_scope,
            run_id=run_input.run_id,
            family="StageGraph",
            activation_key=f"stage:{operation.semantic_attempt_id}",
            execution_epoch=run_input.execution_epoch,
            review_round=min(candidate.workflow_cycle_ordinal + 1, spec.max_review_rounds),
            spec=spec,
            packet=_stage_review_packet(spec, blueprint, projection, candidate.stage_id),
            opened_at=workflow.now(),
        )
        handle = await workflow.start_child_workflow(
            HumanGateWorkflow.run,
            HumanGateWorkflowInput(
                activation=activation,
                poll_seconds=run_input.human_gate_poll_seconds,
                activity_timeout_seconds=run_input.task_timeout_seconds,
            ),
            id=activation.workflow_id,
            task_queue=workflow.info().task_queue,
            parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
        )
        policy = blueprint.workflow_cycle_policy

        async def settle() -> None:
            settled = await workflow.execute_activity(
                "human_gate.settle_stage_reservation",
                GateReservationSettlementRequest(
                    request_scope=run_input.request_scope,
                    run_id=run_input.run_id,
                    reservation_id=operation.operation.budget_reservation_id,
                    occurred_at=workflow.now(),
                    idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                    correlation_id=run_input.correlation_id,
                ),
                result_type=GateReservationSettlementResult,
                start_to_close_timeout=activity_timeout,
                retry_policy=RetryPolicy(maximum_attempts=3),
            )
            if not settled.accepted:
                raise ApplicationError(
                    f"human gate reservation settlement rejected: {settled.reason_code}",
                    non_retryable=True,
                )

        async def result() -> OperationWorkflowResult:
            try:
                outcome: HumanGateOutcome = await handle
            except (Exception, asyncio.CancelledError):
                # Never on eviction (GeneratorExit): a replayed history settles it again.
                await settle()
                raise
            await settle()
            disposition, payload = stagegraph_gate_result(
                outcome,
                evaluation_contract_ref=policy.evaluation_contract_ref if policy else None,
                objective_contract_ref=policy.objective_contract_ref if policy else None,
            )
            return OperationWorkflowResult(
                semantic_attempt_id=operation.semantic_attempt_id,
                execution_generation=operation.execution_generation,
                disposition=disposition,
                result=payload,
                message_cursor=operation.message_cursor,
                effect_frontier=operation.effect_frontier,
                active_async_child_ids=operation.active_async_child_ids,
            )

        return handle, asyncio.ensure_future(result())

    async def _release_baseline(
        self, run_input: StageGraphRunInput, activity_timeout: timedelta, retry: RetryPolicy
    ) -> None:
        """RRM-021: release the admitted baseline through run control (idempotent)."""

        settlement = await workflow.execute_activity(
            "stagegraph.settle_baseline",
            StageGraphBaselineSettlementRequest(
                run_id=run_input.run_id,
                request_scope=run_input.request_scope,
                occurred_at=workflow.now(),
                idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                correlation_id=run_input.correlation_id,
                baseline_reservation=dict(run_input.baseline_reservation),
            ),
            result_type=StageGraphBaselineSettlementResult,
            start_to_close_timeout=activity_timeout,
            retry_policy=retry,
        )
        if not settlement.accepted:
            raise ApplicationError(
                f"StageGraph baseline settlement rejected: {settlement.reason_code}",
                non_retryable=True,
            )

    # --- RRM-007 boundary mechanics ---------------------------------------------------------

    def _decide(
        self, delivery: BoundaryCommandDelivery, blueprint: StageGraphBlueprint
    ) -> tuple[set[str], dict[str, tuple[str, ...]], str]:
        """Decide a delivered command without touching the family state (F4).

        Returns the satisfied waits and active pauses the command would leave, and the
        rejection reason (empty when applicable). A release whose wait is not held, a resume
        of an unknown pause, or a pause with an unknown scope is `not_applicable`.
        """

        satisfied = set(self._satisfied_waits)
        pauses = dict(self._active_pauses)
        payload = delivery.payload
        if delivery.kind == "satisfy_wait":
            condition_id = str(payload.get("condition_id", ""))
            wait_id = condition_id.removeprefix(WAIT_CONDITION_PREFIX)
            if (
                not condition_id.startswith(WAIT_CONDITION_PREFIX)
                or wait_id in satisfied
                or wait_id not in {item.wait_id for item in blueprint.waits}
            ):
                return satisfied, pauses, "not_applicable"
            satisfied.add(wait_id)
            return satisfied, pauses, ""
        decision = payload.get("decision") or {}
        if delivery.kind == "pause":
            decision_id = str(decision.get("decision_id", ""))
            scope = tuple(sorted(str(item) for item in decision.get("scope", ())))
            known_stages = {item.stage_id for item in blueprint.stages}
            if (
                not decision_id
                or decision_id in pauses
                or not scope
                or any(not _known_scope(item, known_stages) for item in scope)
            ):
                return satisfied, pauses, "not_applicable"
            pauses[decision_id] = scope
            return satisfied, pauses, ""
        pause_id = str(decision.get("pause_decision_id", ""))
        if pause_id not in pauses:
            return satisfied, pauses, "not_applicable"
        del pauses[pause_id]
        return satisfied, pauses, ""

    def _mark_handled(
        self, delivery: BoundaryCommandDelivery, outcome: BoundaryLifecycleOutcome
    ) -> None:
        """A command is handled only once authority applied or terminally rejected it (F2)."""

        if outcome.receipt_state not in {"applied", "rejected"}:
            raise ApplicationError(
                f"boundary command {delivery.command_id} left the ledger in "
                f"{outcome.receipt_state or 'no'} state after {outcome.reason_code}",
                type="boundary_command_unresolved",
                non_retryable=True,
            )
        self._applied_command_ids.add(_command_key(delivery))

    async def _apply(
        self,
        run_input: StageGraphRunInput,
        delivery: BoundaryCommandDelivery,
        boundary_ref: str,
        *,
        rejection: str,
        runnable: bool,
        activity_timeout: timedelta,
    ) -> BoundaryLifecycleOutcome:
        if rejection:
            return await self._boundary_fact(
                run_input,
                BoundaryLifecycleRequest(
                    command_id=f"boundary-reject:{delivery.idempotency_issuer}:{delivery.command_id}",
                    action={},
                    reason=f"StageGraph boundary cannot apply {delivery.kind}",
                    boundary_ref=boundary_ref,
                    boundary_command_id=delivery.command_id,
                    boundary_command_issuer=delivery.idempotency_issuer,
                    rejection_reason=rejection,
                ),
                activity_timeout,
            )
        return await self._boundary_fact(
            run_input,
            BoundaryLifecycleRequest(
                command_id=f"boundary-apply:{delivery.idempotency_issuer}:{delivery.command_id}",
                action={
                    "kind": "apply_boundary_command",
                    "command_id": delivery.command_id,
                    "command_issuer": delivery.idempotency_issuer,
                    "action": delivery.payload,
                    "boundary_ref": boundary_ref,
                    "runnable_work_remains": runnable,
                    "boundary_state": {
                        "family": "StageGraph",
                        "technical_segment": self._technical_segment,
                    },
                },
                reason=f"StageGraph boundary applied {delivery.kind}",
                boundary_ref=boundary_ref,
                boundary_command_id=delivery.command_id,
                boundary_command_issuer=delivery.idempotency_issuer,
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
            cancel_requested=self._cancel_requested,
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
            applied_boundary_command_ids=tuple(
                sorted(_join_key(key) for key in self._applied_command_ids)
            ),
            quiescent=self._quiescent,
            last_delivered_sequence=self._last_delivered_sequence,
        )


def _human_gates(
    run_input: StageGraphRunInput, blueprint: StageGraphBlueprint
) -> dict[str, HumanGateSpec]:
    """The declared gate stages, validated against the frozen blueprint (MP-10)."""

    stage_ids = {stage.stage_id for stage in blueprint.stages}
    gates: dict[str, HumanGateSpec] = {}
    for item in run_input.human_gates:
        spec = item if isinstance(item, HumanGateSpec) else HumanGateSpec.model_validate(item)
        if spec.gate_key not in stage_ids:
            raise ApplicationError(
                f"human gate {spec.gate_key} is not a stage of the frozen blueprint",
                non_retryable=True,
            )
        if spec.remediation_target is not None:
            policy = blueprint.workflow_cycle_policy
            if policy is None or spec.remediation_target not in stage_ids:
                raise ApplicationError(
                    f"human gate {spec.gate_key} declares remediation without a workflow "
                    "cycle policy and a remediation stage in the frozen blueprint",
                    non_retryable=True,
                )
            # The review-round governor never exceeds the blueprint's cycle governor.
            spec = spec.model_copy(
                update={"max_review_rounds": min(spec.max_review_rounds, policy.max_cycles + 1)}
            )
        gates[spec.gate_key] = spec
    return gates


def _stage_review_packet(
    spec: HumanGateSpec, blueprint: StageGraphBlueprint, projection: Any, gate_stage_id: str
) -> tuple[ReviewPacketItem, ...]:
    """The accepted outputs the reviewer sees: the declared packet, else the gate's producers."""

    sources = spec.packet_sources or tuple(
        f"{edge.producer_stage_id}.{edge.producer_output_slot_id}"
        for edge in blueprint.dependencies
        if edge.consumer_stage_id == gate_stage_id
    )
    items: dict[tuple[str, str], ReviewPacketItem] = {}
    for source in sources:
        stage_id = source.split(".", 1)[0]
        produced = [
            instance
            for instance in projection.stages.values()
            if instance.candidate.stage_id == stage_id
            and instance.output_refs
            and instance.status != "invalidated"
        ]
        if not produced:
            continue
        latest = max(
            produced,
            key=lambda item: (
                item.candidate.workflow_cycle_ordinal,
                item.candidate.stage_cycle_ordinal,
                item.semantic_attempt,
            ),
        )
        for ref in latest.output_refs:
            items[(source, ref)] = packet_item(source, ref)
    return tuple(items.values())


def _command_key(delivery: BoundaryCommandDelivery) -> tuple[str, str]:
    return (delivery.idempotency_issuer, delivery.command_id)


def _join_key(key: tuple[str, str]) -> str:
    return f"{key[0]}::{key[1]}"


def _split_key(value: str) -> tuple[str, str]:
    issuer, separator, command_id = value.partition("::")
    return (issuer, command_id) if separator else ("", value)


def _pause_covers(scope: tuple[str, ...], stage_id: str) -> bool:
    return any(item in WORKFLOW_PAUSE_SCOPES or item == f"stage:{stage_id}" for item in scope)


def _known_scope(item: str, stage_ids: set[str]) -> bool:
    if item in WORKFLOW_PAUSE_SCOPES:
        return True
    prefix, separator, stage_id = item.partition(":")
    return separator == ":" and prefix == "stage" and stage_id in stage_ids
