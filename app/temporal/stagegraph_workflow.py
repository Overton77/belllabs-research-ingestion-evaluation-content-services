from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from app.domain.control_plane.canonical import sha256_digest
    from app.domain.control_plane.contracts import StageGraphBlueprint
    from app.domain.orchestration.contracts import (
        ExecutionIdentity,
        LifecycleCommandOutcome,
        LifecycleCommandRequest,
        StageGraphExecutionState,
        StageGraphRunInput,
        StageGraphRunResult,
        StageOperationRequest,
        StageOperationResult,
        WorkflowEvaluationRequest,
        WorkflowEvaluationResult,
    )
    from app.domain.orchestration.interpreter import (
        StageGraphExecutionError,
        StageGraphInterpreter,
    )


@workflow.defn(name="belllabs.stagegraph")
class StageGraphWorkflow:
    """Durable deterministic coordination for one admitted frozen StageGraph."""

    def __init__(self) -> None:
        self._satisfied_waits: set[str] = set()
        self._resumed_pauses: set[str] = set()
        self._authority_ref = "orchestration-authority"
        self._run_id = ""
        self._request_scope = ""
        self._configuration_digest = ""
        self._idempotency_issuer = ""
        self._correlation_id = ""
        self._execution_epoch = 1
        self._blueprint_digest = ""

    @workflow.signal
    def satisfy_wait(self, condition_id: str) -> None:
        self._satisfied_waits.add(condition_id)

    @workflow.signal
    def resume_pause(self, decision_id: str) -> None:
        self._resumed_pauses.add(decision_id)

    @workflow.run
    async def run(self, run_input: StageGraphRunInput) -> StageGraphRunResult:
        if run_input.execution_epoch != 1:
            raise ApplicationError(
                "execution epoch rollover requires the deferred continuity contract",
                non_retryable=True,
            )
        self._authority_ref = run_input.orchestration_authority_ref
        self._run_id = run_input.run_id
        self._request_scope = run_input.request_scope
        self._configuration_digest = run_input.effective_configuration_digest
        self._blueprint_digest = run_input.blueprint_digest
        self._idempotency_issuer = run_input.lifecycle_idempotency_issuer
        self._execution_epoch = run_input.execution_epoch
        self._correlation_id = run_input.correlation_id or (
            f"orchestration:{run_input.run_id}:epoch:{run_input.execution_epoch}"
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
        state = interpreter.initial_state(
            ExecutionIdentity(
                run_id=run_input.run_id,
                execution_epoch=run_input.execution_epoch,
            ),
            run_version=run_input.initial_run_version,
        )
        retry_policy = RetryPolicy(maximum_attempts=3)
        timeout = timedelta(seconds=run_input.task_timeout_seconds)
        reused_outputs: dict[str, tuple[str, ...]] = {}

        start = await self._lifecycle(
            state.run_version,
            LifecycleCommandRequest(
                command_id=(
                    f"orchestration:{run_input.run_id}:epoch:{run_input.execution_epoch}:start"
                ),
                expected_run_version=state.run_version,
                action={"kind": "start"},
                reason="Deterministic StageGraph orchestration started",
                occurred_at=workflow.now(),
            ),
            timeout,
            retry_policy,
        )
        state.run_version = start.resulting_run_version
        lifecycle = start

        while True:
            while not interpreter.graph_complete(state):
                runnable = interpreter.runnable(state)
                if not runnable:
                    if interpreter.resolve_blocked(state):
                        continue
                    if await self._resume_scoped_blocks(
                        interpreter,
                        state,
                        timeout,
                        retry_policy,
                    ):
                        continue
                    raise ApplicationError(
                        "StageGraph has no runnable work and no declared active wait or pause",
                        non_retryable=True,
                    )

                requests = tuple(
                    interpreter.operation_request(state, stage_id) for stage_id in runnable
                )
                reserved_requests: list[StageOperationRequest] = []
                reservation_failed = False
                try:
                    for request in requests:
                        lifecycle = await self._lifecycle(
                            state.run_version,
                            LifecycleCommandRequest(
                                command_id=f"orchestration:{request.reservation_id}",
                                expected_run_version=state.run_version,
                                action={
                                    "kind": "reserve_budget",
                                    "reservation_id": request.reservation_id,
                                    "amounts": request.reservation,
                                },
                                reason="Reserve budget before semantic stage dispatch",
                                evidence_refs=(request.idempotency_key,),
                                occurred_at=workflow.now(),
                            ),
                            timeout,
                            retry_policy,
                        )
                        state.run_version = lifecycle.resulting_run_version
                        reserved_requests.append(request)
                except ApplicationError:
                    for reserved_request in reserved_requests:
                        lifecycle = await self._lifecycle(
                            state.run_version,
                            LifecycleCommandRequest(
                                command_id=(
                                    f"orchestration:release:"
                                    f"{reserved_request.identity.semantic_key}"
                                ),
                                expected_run_version=state.run_version,
                                action={
                                    "kind": "record_usage",
                                    "usage_id": (
                                        f"undispatched:{reserved_request.identity.semantic_key}"
                                    ),
                                    "actual_amounts": {},
                                    "reservation_id": reserved_request.reservation_id,
                                    "release_amounts": reserved_request.reservation,
                                    "pending_external_amounts": {},
                                },
                                reason=(
                                    "Release accepted batch reservation after a later "
                                    "reservation was rejected"
                                ),
                                occurred_at=workflow.now(),
                            ),
                            timeout,
                            retry_policy,
                        )
                        state.run_version = lifecycle.resulting_run_version
                    reservation_failed = True

                if reservation_failed:
                    requests = tuple(replace(request, reservation={}) for request in requests)
                    raw_results: list[StageOperationResult | BaseException] = [
                        StageOperationResult(
                            identity=request.identity,
                            disposition="failed",
                            evaluation="escalate",
                            evaluation_ref=(f"reservation-failure:{request.identity.semantic_key}"),
                            evaluation_contract_ref=(request.cycle_evaluation_contract_ref),
                        )
                        for request in requests
                    ]
                else:
                    raw_results = list(
                        await asyncio.gather(
                            *[
                                workflow.execute_activity(
                                    "stagegraph.execute_operation",
                                    request,
                                    result_type=StageOperationResult,
                                    start_to_close_timeout=timeout,
                                    retry_policy=retry_policy,
                                )
                                for request in requests
                            ],
                            return_exceptions=True,
                        )
                    )
                results: list[StageOperationResult] = []
                for request, raw_result in zip(requests, raw_results, strict=True):
                    if isinstance(raw_result, BaseException):
                        results.append(
                            StageOperationResult(
                                identity=request.identity,
                                disposition="failed",
                                evaluation="escalate",
                                evaluation_ref=(
                                    f"activity-failure:{request.identity.semantic_key}"
                                ),
                                evaluation_contract_ref=(request.cycle_evaluation_contract_ref),
                            )
                        )
                    elif raw_result.pending_external_usage:
                        conservative_usage = {
                            dimension: raw_result.actual_usage.get(dimension, 0)
                            + raw_result.pending_external_usage.get(dimension, 0)
                            for dimension in (
                                raw_result.actual_usage.keys()
                                | raw_result.pending_external_usage.keys()
                            )
                        }
                        results.append(
                            replace(
                                raw_result,
                                disposition="failed",
                                evaluation="escalate",
                                evaluation_ref=(
                                    f"unsupported-pending-settlement:"
                                    f"{request.identity.semantic_key}"
                                ),
                                pending_external_usage={},
                                # Settlement is deferred to F4. Until then, never erase known
                                # liability: conservatively account pending amounts as actual.
                                actual_usage=conservative_usage,
                                evaluation_contract_ref=(request.cycle_evaluation_contract_ref),
                            )
                        )
                    else:
                        results.append(raw_result)
                for result in results:
                    try:
                        interpreter.apply_stage_result(state, result)
                    except StageGraphExecutionError as error:
                        raise ApplicationError(str(error), non_retryable=True) from error
                runnable_work_remains = bool(interpreter.runnable(state)) or any(
                    item.status == "running" for item in state.stages.values()
                )
                for request, result in zip(requests, results, strict=True):
                    lifecycle = await self._report_stage_result(
                        state.run_version,
                        request,
                        result,
                        runnable_work_remains=runnable_work_remains,
                        activity_timeout=timeout,
                        retry_policy=retry_policy,
                        prior=lifecycle,
                    )
                    state.run_version = lifecycle.resulting_run_version

            cycle_policy = blueprint.workflow_cycle_policy
            frozen_evaluation_contract = blueprint.workflow_evaluation_contract_ref or (
                cycle_policy.evaluation_contract_ref if cycle_policy is not None else ""
            )
            frozen_objective_contract = (
                cycle_policy.objective_contract_ref if cycle_policy is not None else ""
            )
            if interpreter.graph_failed(state):
                evaluation = WorkflowEvaluationResult(
                    action="fail",
                    evaluation_ref=f"deterministic:required-stage-failure:{state.workflow_cycle}",
                )
            elif frozen_evaluation_contract:
                try:
                    evaluation = await workflow.execute_activity(
                        "stagegraph.evaluate_workflow",
                        WorkflowEvaluationRequest(
                            run_id=run_input.run_id,
                            workflow_cycle=state.workflow_cycle,
                            objective=state.workflow_objective,
                            current_output_refs=interpreter.current_outputs(state),
                            execution_lineage=tuple(state.lineage),
                            evaluation_contract_ref=frozen_evaluation_contract,
                            objective_contract_ref=frozen_objective_contract,
                        ),
                        result_type=WorkflowEvaluationResult,
                        start_to_close_timeout=timeout,
                        retry_policy=retry_policy,
                    )
                except ActivityError:
                    evaluation = WorkflowEvaluationResult(
                        action="fail",
                        evaluation_ref=(
                            f"workflow-evaluator-activity-failure:{state.workflow_cycle}"
                        ),
                        evaluation_contract_ref=frozen_evaluation_contract,
                    )
                if evaluation.evaluation_contract_ref != frozen_evaluation_contract:
                    raise ApplicationError(
                        "workflow evaluation is not bound to its frozen contract",
                        non_retryable=True,
                    )
            else:
                evaluation = WorkflowEvaluationResult(
                    action="accept",
                    evaluation_ref=(f"deterministic:stage-completion:{state.workflow_cycle}"),
                )
            if evaluation.action != "cycle":
                break

            try:
                interpreter.validate_workflow_evaluation(state, evaluation)
            except StageGraphExecutionError as error:
                raise ApplicationError(str(error), non_retryable=True) from error
            policy = blueprint.workflow_cycle_policy
            if policy is not None and policy.reservation:
                next_cycle = state.workflow_cycle + 1
                reservation_id = (
                    f"reservation:{run_input.run_id}:execution-epoch:"
                    f"{run_input.execution_epoch}:workflow-cycle:{next_cycle}"
                )
                try:
                    lifecycle = await self._lifecycle(
                        state.run_version,
                        LifecycleCommandRequest(
                            command_id=f"orchestration:{reservation_id}",
                            expected_run_version=state.run_version,
                            action={
                                "kind": "reserve_budget",
                                "reservation_id": reservation_id,
                                "amounts": policy.reservation,
                            },
                            reason="Reserve budget before accepted whole-workflow cycle",
                            evidence_refs=(evaluation.evaluation_ref,),
                            occurred_at=workflow.now(),
                        ),
                        timeout,
                        retry_policy,
                    )
                except ApplicationError:
                    evaluation = WorkflowEvaluationResult(
                        action="fail",
                        evaluation_ref=(f"workflow-cycle-reservation-rejected:{next_cycle}"),
                        evaluation_contract_ref=frozen_evaluation_contract,
                    )
                    break
                state.run_version = lifecycle.resulting_run_version
                actual_cycle_usage = (
                    {"workflow.cycles": 1} if "workflow.cycles" in policy.reservation else {}
                )
                pending_cycle_usage: dict[str, int] = {}
                release_cycle_usage = {
                    dimension: amount - actual_cycle_usage.get(dimension, 0)
                    for dimension, amount in policy.reservation.items()
                    if amount > actual_cycle_usage.get(dimension, 0)
                }
                lifecycle = await self._lifecycle(
                    state.run_version,
                    LifecycleCommandRequest(
                        command_id=f"orchestration:usage:{reservation_id}",
                        expected_run_version=state.run_version,
                        action={
                            "kind": "record_usage",
                            "usage_id": f"usage:{reservation_id}",
                            "actual_amounts": actual_cycle_usage,
                            "reservation_id": reservation_id,
                            "release_amounts": release_cycle_usage,
                            "pending_external_amounts": pending_cycle_usage,
                        },
                        reason=(
                            "Reconcile the accepted workflow-cycle reservation; F4 "
                            "supplies provider-observed usage for concrete runtimes"
                        ),
                        evidence_refs=(evaluation.evaluation_ref,),
                        occurred_at=workflow.now(),
                    ),
                    timeout,
                    retry_policy,
                )
                state.run_version = lifecycle.resulting_run_version
            try:
                reused_outputs = interpreter.apply_workflow_evaluation(state, evaluation)
            except StageGraphExecutionError as error:
                raise ApplicationError(str(error), non_retryable=True) from error

        outputs = interpreter.current_outputs(state)
        for stage in blueprint.stages:
            stage_state = state.stages[stage.stage_id]
            if stage_state.status not in {"completed", "degraded"}:
                continue
            evidence_digest = sha256_digest(
                {
                    "stage_id": stage.stage_id,
                    "workflow_cycle": state.workflow_cycle,
                    "output_refs": stage_state.output_refs,
                    "evaluation_ref": evaluation.evaluation_ref,
                }
            )
            for obligation_ref in sorted(stage.obligation_refs):
                lifecycle = await self._lifecycle(
                    state.run_version,
                    LifecycleCommandRequest(
                        command_id=(
                            f"orchestration:epoch:{self._execution_epoch}:"
                            f"obligation-evidence:{stage.stage_id}:"
                            f"{obligation_ref}:{evidence_digest}"
                        ),
                        expected_run_version=state.run_version,
                        action={
                            "kind": "record_obligation_evidence",
                            "evidence": {
                                "obligation_ref": obligation_ref,
                                "evidence_digest": evidence_digest,
                                "accepted_by_authority_ref": self._authority_ref,
                            },
                        },
                        reason="Record final accepted StageGraph obligation evidence",
                        evidence_refs=(evaluation.evaluation_ref,),
                        occurred_at=workflow.now(),
                    ),
                    timeout,
                    retry_policy,
                )
                state.run_version = lifecycle.resulting_run_version
            for output_ref in stage_state.output_refs:
                lifecycle = await self._lifecycle(
                    state.run_version,
                    LifecycleCommandRequest(
                        command_id=(
                            f"orchestration:epoch:{self._execution_epoch}:"
                            f"output-evidence:{stage.stage_id}:"
                            f"{output_ref}:{evidence_digest}"
                        ),
                        expected_run_version=state.run_version,
                        action={
                            "kind": "record_output_evidence",
                            "evidence": {
                                "output_ref": output_ref,
                                "evidence_digest": evidence_digest,
                                "accepted_by_authority_ref": self._authority_ref,
                            },
                        },
                        reason="Record final accepted StageGraph output evidence",
                        evidence_refs=(evaluation.evaluation_ref,),
                        occurred_at=workflow.now(),
                    ),
                    timeout,
                    retry_policy,
                )
                state.run_version = lifecycle.resulting_run_version
        if run_input.baseline_reservation:
            lifecycle = await self._lifecycle(
                state.run_version,
                LifecycleCommandRequest(
                    command_id=(
                        f"orchestration:{run_input.run_id}:epoch:"
                        f"{run_input.execution_epoch}:release-baseline"
                    ),
                    expected_run_version=state.run_version,
                    action={
                        "kind": "record_usage",
                        "usage_id": (
                            f"baseline-release:{run_input.run_id}:{run_input.execution_epoch}"
                        ),
                        "actual_amounts": {},
                        "reservation_id": "baseline",
                        "release_amounts": run_input.baseline_reservation,
                        "pending_external_amounts": {},
                    },
                    reason="Release unused admission baseline before terminalization",
                    occurred_at=workflow.now(),
                ),
                timeout,
                retry_policy,
            )
            state.run_version = lifecycle.resulting_run_version
        terminal = await self._lifecycle(
            state.run_version,
            LifecycleCommandRequest(
                command_id=(
                    f"orchestration:{run_input.run_id}:epoch:"
                    f"{run_input.execution_epoch}:terminal:"
                    f"workflow-cycle:{state.workflow_cycle}"
                ),
                expected_run_version=state.run_version,
                action={
                    "kind": "terminalize",
                    "proposal": {
                        "proposal_id": (
                            f"terminal:{run_input.run_id}:epoch:"
                            f"{run_input.execution_epoch}:{state.workflow_cycle}"
                        ),
                        "obligation_revision": lifecycle.obligation_revision,
                        "evidence_frontier_digest": lifecycle.evidence_frontier_digest,
                        "accepted_obligation_evidence_digest": (
                            lifecycle.accepted_obligation_evidence_digest
                        ),
                        # F4 replaces this explicit orchestration binding placeholder.
                        "proposing_execution_binding_ref": (
                            f"orchestration-binding:{run_input.effective_configuration_digest}"
                        ),
                        "required_obligations_accepted": (lifecycle.required_obligations_accepted),
                        "execution_failure_refs": (
                            (evaluation.evaluation_ref,) if evaluation.action == "fail" else ()
                        ),
                        "degradable_failures": tuple(
                            stage_id
                            for stage_id, item in state.stages.items()
                            if item.status in {"degraded", "skipped", "failed"}
                        ),
                        "valid_output_refs": tuple(
                            output_ref
                            for stage_outputs in outputs.values()
                            for output_ref in stage_outputs
                        ),
                        "cancellation_settled": False,
                        "budget_settled": True,
                        "pending_wait_or_link_ids": (),
                        "proposed_at": workflow.now(),
                    },
                },
                reason="Typed whole-workflow evaluation accepted terminalization",
                evidence_refs=(evaluation.evaluation_ref,),
                occurred_at=workflow.now(),
            ),
            timeout,
            retry_policy,
        )
        state.run_version = terminal.resulting_run_version
        return StageGraphRunResult(
            run_id=run_input.run_id,
            workflow_cycles=state.workflow_cycle,
            execution_epoch=state.identity.execution_epoch,
            stage_cycles={stage_id: item.stage_cycle for stage_id, item in state.stages.items()},
            operation_attempts={
                stage_id: item.operation_attempt for stage_id, item in state.stages.items()
            },
            output_refs=outputs,
            reused_output_refs=reused_outputs,
            schedule_trace=tuple(state.schedule_trace),
            lineage=tuple(state.lineage),
        )

    async def _report_stage_result(
        self,
        run_version: int,
        request: StageOperationRequest,
        result: StageOperationResult,
        runnable_work_remains: bool,
        activity_timeout: timedelta,
        retry_policy: RetryPolicy,
        prior: LifecycleCommandOutcome,
    ) -> LifecycleCommandOutcome:
        outcome = prior
        if request.reservation:
            actual_usage = dict(result.actual_usage)
            pending_usage = dict(result.pending_external_usage)
            if "operation.attempts" in request.reservation:
                actual_usage.setdefault("operation.attempts", 1)
            if request.identity.stage_cycle > 0 and "stage.cycles" in request.reservation:
                actual_usage.setdefault("stage.cycles", 1)
            release_usage = {
                dimension: amount
                - min(
                    actual_usage.get(dimension, 0) + pending_usage.get(dimension, 0),
                    amount,
                )
                for dimension, amount in request.reservation.items()
                if amount > actual_usage.get(dimension, 0) + pending_usage.get(dimension, 0)
            }
            outcome = await self._lifecycle(
                run_version,
                LifecycleCommandRequest(
                    command_id=f"orchestration:usage:{request.identity.semantic_key}",
                    expected_run_version=run_version,
                    action={
                        "kind": "record_usage",
                        "usage_id": f"usage:{request.identity.semantic_key}",
                        "actual_amounts": actual_usage,
                        "reservation_id": request.reservation_id,
                        "release_amounts": release_usage,
                        "pending_external_amounts": pending_usage,
                    },
                    reason="Reconcile observed operation usage with its prior reservation",
                    evidence_refs=(result.evaluation_ref,),
                    occurred_at=workflow.now(),
                ),
                activity_timeout,
                retry_policy,
            )
            run_version = outcome.resulting_run_version
        if result.disposition == "waiting":
            outcome = await self._lifecycle(
                run_version,
                LifecycleCommandRequest(
                    command_id=(
                        f"orchestration:epoch:{self._execution_epoch}:"
                        f"wait:{result.wait_condition_id}"
                    ),
                    expected_run_version=run_version,
                    action={
                        "kind": "set_wait",
                        "condition": {
                            "condition_id": result.wait_condition_id,
                            "kind": "external_result",
                            "scope": [request.identity.stage_id],
                            "verification_ref": result.evaluation_ref,
                            "timeout_policy_ref": "blueprint:stage-timeout-policy",
                        },
                        "runnable_work_remains": runnable_work_remains,
                    },
                    reason="Stage operation reported a declared durable wait",
                    occurred_at=workflow.now(),
                ),
                activity_timeout,
                retry_policy,
            )
            run_version = outcome.resulting_run_version
        elif result.disposition == "paused":
            outcome = await self._lifecycle(
                run_version,
                LifecycleCommandRequest(
                    command_id=(
                        f"orchestration:epoch:{self._execution_epoch}:"
                        f"pause:{result.pause_decision_id}"
                    ),
                    expected_run_version=run_version,
                    action={
                        "kind": "pause",
                        "decision": {
                            "decision_id": result.pause_decision_id,
                            "scope": [request.identity.stage_id],
                            "reason": "Stage operation requested governed intervention",
                            "authority_ref": self._authority_ref,
                        },
                        "runnable_work_remains": runnable_work_remains,
                    },
                    reason="Record accepted scoped pause decision",
                    occurred_at=workflow.now(),
                ),
                activity_timeout,
                retry_policy,
            )
            run_version = outcome.resulting_run_version

        return outcome

    async def _resume_scoped_blocks(
        self,
        interpreter: StageGraphInterpreter,
        state: StageGraphExecutionState,
        activity_timeout: timedelta,
        retry_policy: RetryPolicy,
    ) -> bool:
        waits = {
            item.wait_condition_id for item in state.stages.values() if item.status == "waiting"
        }
        pauses = {
            item.pause_decision_id for item in state.stages.values() if item.status == "paused"
        }
        if not waits and not pauses:
            return False
        await workflow.wait_condition(
            lambda: bool(waits & self._satisfied_waits) or bool(pauses & self._resumed_pauses)
        )
        for condition_id in sorted(waits & self._satisfied_waits):
            interpreter.clear_wait(state, condition_id)
            runnable_work_remains = bool(interpreter.runnable(state)) or any(
                item.status == "running" for item in state.stages.values()
            )
            outcome = await self._lifecycle(
                state.run_version,
                LifecycleCommandRequest(
                    command_id=(
                        f"orchestration:epoch:{self._execution_epoch}:wait-satisfied:{condition_id}"
                    ),
                    expected_run_version=state.run_version,
                    action={
                        "kind": "satisfy_wait",
                        "condition_id": condition_id,
                        "verification_evidence_ref": f"signal:{condition_id}",
                        "runnable_work_remains": runnable_work_remains,
                    },
                    reason="Declared wait condition was verifiably satisfied",
                    occurred_at=workflow.now(),
                ),
                activity_timeout,
                retry_policy,
            )
            state.run_version = outcome.resulting_run_version
            self._satisfied_waits.remove(condition_id)
        for decision_id in sorted(pauses & self._resumed_pauses):
            interpreter.resume_pause(state, decision_id)
            runnable_work_remains = bool(interpreter.runnable(state)) or any(
                item.status == "running" for item in state.stages.values()
            )
            outcome = await self._lifecycle(
                state.run_version,
                LifecycleCommandRequest(
                    command_id=(
                        f"orchestration:epoch:{self._execution_epoch}:pause-resumed:{decision_id}"
                    ),
                    expected_run_version=state.run_version,
                    action={
                        "kind": "resume",
                        "decision": {
                            "decision_id": f"resume:{decision_id}",
                            "pause_decision_id": decision_id,
                            "reason": "Authorized StageGraph pause resume",
                            "authority_ref": self._authority_ref,
                        },
                        "runnable_work_remains": runnable_work_remains,
                    },
                    reason="Record authorized scoped resume decision",
                    occurred_at=workflow.now(),
                ),
                activity_timeout,
                retry_policy,
            )
            state.run_version = outcome.resulting_run_version
            self._resumed_pauses.remove(decision_id)
        return True

    async def _lifecycle(
        self,
        expected_version: int,
        request: LifecycleCommandRequest,
        activity_timeout: timedelta,
        retry_policy: RetryPolicy,
    ) -> LifecycleCommandOutcome:
        if request.expected_run_version != expected_version:
            raise ApplicationError(
                "workflow lifecycle version tracking diverged",
                non_retryable=True,
            )
        request = replace(
            request,
            run_id=self._run_id,
            request_scope=self._request_scope,
            effective_configuration_digest=self._configuration_digest,
            idempotency_issuer=self._idempotency_issuer,
            correlation_id=self._correlation_id,
            blueprint_digest=self._blueprint_digest,
        )
        outcome = await workflow.execute_activity(
            "stagegraph.apply_lifecycle_command",
            request,
            result_type=LifecycleCommandOutcome,
            start_to_close_timeout=activity_timeout,
            # Control-plane command delivery is durable intent. Stable command identities make
            # retries safe, so transient infrastructure outages must not strand F2 state.
            retry_policy=RetryPolicy(maximum_attempts=0),
        )
        if not outcome.accepted:
            raise ApplicationError(
                f"authoritative lifecycle command rejected: {outcome.reason_code}",
                non_retryable=True,
            )
        return outcome
