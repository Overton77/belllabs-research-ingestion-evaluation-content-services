from __future__ import annotations

from dataclasses import asdict, replace
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError, ChildWorkflowError

with workflow.unsafe.imports_passed_through():
    from app.domain.control_plane.canonical import sha256_digest
    from app.domain.control_plane.contracts import GoalDirectedBlueprint
    from app.domain.operation_execution.contracts import OperationWorkflowResult
    from app.domain.orchestration.contracts import (
        GoalContinuationState,
        GoalDirectedExecutionState,
        GoalDirectedRunInput,
        GoalDirectedRunResult,
        GoalExecutionClaim,
        GoalExecutionResult,
        GoalHandoff,
        GoalOperationRole,
        GoalRevision,
        LifecycleCommandOutcome,
        LifecycleCommandRequest,
    )
    from app.domain.orchestration.goal_directed import (
        GoalDirectedExecutionError,
        GoalDirectedInterpreter,
    )
    from app.domain.orchestration.goal_directed_runtime import (
        GoalOperationDispatch,
        GoalOperationPreparationRequest,
        GoalOperationReconciliationRequest,
        GoalOperationReconciliationResult,
    )
    from app.temporal.workflows.operation import OperationWorkflow


CONTINUE_AS_NEW_ITERATIONS = 20


@workflow.defn(name="belllabs.goal-directed")
class GoalDirectedWorkflow:
    """Replay-safe GoalDirected family scheduler over generic operation children."""

    def __init__(self) -> None:
        self._cancel_requested = False
        self._operation_handle: Any | None = None

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True
        if self._operation_handle is not None:
            self._operation_handle.cancel()

    @workflow.run
    async def run(self, run_input: GoalDirectedRunInput) -> GoalDirectedRunResult:
        blueprint = GoalDirectedBlueprint.model_validate(run_input.blueprint)
        if sha256_digest(blueprint) != run_input.blueprint_digest:
            raise ApplicationError(
                "frozen GoalDirected digest does not match its exact blueprint binding",
                non_retryable=True,
            )
        interpreter = GoalDirectedInterpreter(blueprint)
        try:
            state = interpreter.initial_state(run_input)
        except GoalDirectedExecutionError as error:
            raise ApplicationError(str(error), non_retryable=True) from error

        timeout = timedelta(seconds=run_input.task_timeout_seconds)
        run_version = run_input.initial_run_version
        if run_input.continuation_state is None:
            lifecycle = await self._lifecycle(
                run_input,
                LifecycleCommandRequest(
                    command_id=(
                        f"goal:{run_input.run_id}:epoch:{run_input.execution_epoch}:"
                        f"segment:{run_input.technical_segment}:start"
                    ),
                    expected_run_version=run_version,
                    action={"kind": "start"},
                    reason="Start canonical GoalDirected family execution",
                    occurred_at=workflow.now(),
                ),
                timeout,
            )
            run_version = lifecycle.resulting_run_version
        family_version = run_input.family_version

        while state.status == "ready":
            await self._stop_for_cancellation(run_input, run_version, timeout)

            try:
                claimed_state, claim = interpreter.claim_execution(state)
            except GoalDirectedExecutionError as error:
                raise ApplicationError(str(error), non_retryable=True) from error

            executor_dispatch = await self._prepare_operation(
                run_input,
                claimed_state.active_revision,
                claim.identity.iteration.goal_iteration,
                "executor",
                run_version,
                family_version,
                claim.reservation_id,
                claim.reservation,
                claim.session_id,
                claim.workspace_namespace,
                claim.prior_handoff_ref or None,
                (
                    claimed_state.handoffs[-1]
                    if claim.prior_handoff_ref and claimed_state.handoffs
                    else None
                ),
                (),
                timeout,
            )
            run_version = executor_dispatch.resulting_run_version
            family_version = executor_dispatch.resulting_family_version
            executor_result = await self._execute_operation(
                run_input,
                executor_dispatch,
                run_version,
                timeout,
            )
            executor_accepted = await self._reconcile_operation(
                run_input,
                blueprint,
                claim,
                "executor",
                executor_dispatch,
                executor_result,
                None,
                timeout,
            )
            if executor_accepted.execution_result is None:
                raise ApplicationError("executor reconciliation omitted its typed result")
            projected = interpreter.apply_execution_result(
                claimed_state, executor_accepted.execution_result
            )
            run_version = await self._settle_operation(
                run_input,
                run_version,
                claim.reservation_id,
                claim.reservation,
                executor_accepted.execution_result.actual_usage,
                executor_accepted.detail_ref,
                timeout,
            )
            await self._stop_for_cancellation(run_input, run_version, timeout)

            verifier_reservation_id = f"{claim.reservation_id}:verifier"
            verifier_dispatch = await self._prepare_operation(
                run_input,
                claimed_state.active_revision,
                claim.identity.iteration.goal_iteration,
                "verifier",
                run_version,
                family_version,
                verifier_reservation_id,
                claim.reservation,
                f"{claim.session_id}:verifier",
                f"{claim.workspace_namespace}:verifier",
                None,
                None,
                executor_accepted.execution_result.output_refs,
                timeout,
            )
            run_version = verifier_dispatch.resulting_run_version
            family_version = verifier_dispatch.resulting_family_version
            verifier_result = await self._execute_operation(
                run_input,
                verifier_dispatch,
                run_version,
                timeout,
            )
            verifier_accepted = await self._reconcile_operation(
                run_input,
                blueprint,
                claim,
                "verifier",
                verifier_dispatch,
                verifier_result,
                executor_accepted.execution_result,
                timeout,
            )
            if verifier_accepted.verification_result is None:
                raise ApplicationError("verifier reconciliation omitted its typed result")
            try:
                state = interpreter.apply_verification(
                    projected, verifier_accepted.verification_result
                )
            except GoalDirectedExecutionError as error:
                raise ApplicationError(str(error), non_retryable=True) from error
            run_version = await self._settle_operation(
                run_input,
                run_version,
                verifier_reservation_id,
                claim.reservation,
                verifier_accepted.verification_result.actual_usage,
                verifier_accepted.detail_ref,
                timeout,
            )
            await self._stop_for_cancellation(run_input, run_version, timeout)

            if (
                state.status == "ready"
                and state.next_goal_iteration % CONTINUE_AS_NEW_ITERATIONS == 0
            ):
                workflow.continue_as_new(
                    replace(
                        run_input,
                        initial_revision=state.active_revision,
                        initial_run_version=run_version,
                        family_version=family_version,
                        technical_segment=run_input.technical_segment + 1,
                        continuation_handoff=(state.handoffs[-1] if state.handoffs else None),
                        continuation_state=_continuation(state),
                    )
                )

        if state.status == "paused":
            raise ApplicationError(
                "GoalDirected paused by deterministic convergence policy",
                type="goal_paused",
                non_retryable=True,
            )
        await self._stop_for_cancellation(run_input, run_version, timeout)
        terminalization_proposal = state.terminalization_proposal
        if terminalization_proposal is None:
            return interpreter.result(state)
        result = interpreter.result(state)

        final_verification = result.verification_results[-1]
        evidence_digest = sha256_digest(
            {
                "verification": final_verification.verification_ref,
                "outputs": result.output_refs,
                "obligations": final_verification.accepted_obligation_refs,
            }
        )
        for obligation_ref in final_verification.accepted_obligation_refs:
            lifecycle = await self._lifecycle(
                run_input,
                LifecycleCommandRequest(
                    command_id=f"goal:obligation:{obligation_ref}:{evidence_digest}",
                    expected_run_version=run_version,
                    action={
                        "kind": "record_obligation_evidence",
                        "evidence": {
                            "obligation_ref": obligation_ref,
                            "evidence_digest": evidence_digest,
                            "accepted_by_authority_ref": run_input.orchestration_authority_ref,
                        },
                    },
                    reason="Accept independently verified GoalDirected obligation evidence",
                    evidence_refs=(final_verification.verification_ref,),
                    occurred_at=workflow.now(),
                ),
                timeout,
            )
            run_version = lifecycle.resulting_run_version
        for output_ref in result.output_refs:
            lifecycle = await self._lifecycle(
                run_input,
                LifecycleCommandRequest(
                    command_id=f"goal:output:{output_ref}:{evidence_digest}",
                    expected_run_version=run_version,
                    action={
                        "kind": "record_output_evidence",
                        "evidence": {
                            "output_ref": output_ref,
                            "evidence_digest": evidence_digest,
                            "accepted_by_authority_ref": run_input.orchestration_authority_ref,
                        },
                    },
                    reason="Accept independently verified GoalDirected output evidence",
                    evidence_refs=(final_verification.verification_ref,),
                    occurred_at=workflow.now(),
                ),
                timeout,
            )
            run_version = lifecycle.resulting_run_version
        if run_input.baseline_reservation:
            run_version = await self._settle_operation(
                run_input,
                run_version,
                "baseline",
                run_input.baseline_reservation,
                {},
                final_verification.verification_ref,
                timeout,
            )

        proposal = replace(
            terminalization_proposal,
            expected_run_version=run_version,
        )
        result = replace(result, terminalization_proposal=proposal)
        terminal = await self._lifecycle(
            run_input,
            LifecycleCommandRequest(
                command_id=f"goal:terminalization:{proposal.proposal_id}",
                expected_run_version=run_version,
                action={
                    "kind": "terminalize",
                    "proposal": {
                        "proposal_id": proposal.proposal_id,
                        "expected_run_version": run_version,
                        "workflow_type_digest": lifecycle.workflow_type_digest,
                        "obligation_revision": lifecycle.obligation_revision,
                        "evidence_frontier_digest": lifecycle.evidence_frontier_digest,
                        "accepted_obligation_evidence_digest": (
                            lifecycle.accepted_obligation_evidence_digest
                        ),
                        "proposing_execution_binding_ref": (
                            f"goal-revision:{proposal.goal_revision_id}"
                        ),
                        "required_obligations_accepted": (
                            lifecycle.required_obligations_accepted
                        ),
                        "execution_failure_refs": (
                            ()
                            if proposal.proposed_outcome == "complete"
                            else (proposal.verifier_decision_ref,)
                        ),
                        "degradable_failures": proposal.degradation_refs,
                        "valid_output_refs": (
                            proposal.output_refs
                            if proposal.proposed_outcome == "complete"
                            else ()
                        ),
                        "cancellation_settled": True,
                        "budget_settled": True,
                        "effects_settled": proposal.effects_settled,
                        "pending_wait_or_link_ids": (),
                        "proposed_at": workflow.now(),
                    },
                },
                reason="Submit GoalDirected stopping proposal to the lifecycle reducer",
                evidence_refs=(proposal.verifier_decision_ref,),
                occurred_at=workflow.now(),
            ),
            timeout,
        )
        if not terminal.accepted or terminal.terminal_outcome is None:
            raise ApplicationError(
                "run-control reducer did not authorize GoalDirected terminalization",
                non_retryable=True,
            )
        return result

    async def _execute_operation(
        self,
        run_input: GoalDirectedRunInput,
        dispatch: GoalOperationDispatch,
        run_version: int,
        activity_timeout: timedelta,
    ) -> OperationWorkflowResult:
        handle = await workflow.start_child_workflow(
            OperationWorkflow.run,
            dispatch.workflow_request,
            id=f"operation/{dispatch.workflow_request.semantic_attempt_id}",
            parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
        )
        self._operation_handle = handle
        if self._cancel_requested:
            handle.cancel()
        try:
            return await handle
        except ChildWorkflowError as error:
            if not self._cancel_requested:
                raise
            try:
                await self._stop_for_cancellation(
                    run_input,
                    run_version,
                    activity_timeout,
                )
            except ApplicationError as cancellation:
                raise cancellation from error
            raise
        finally:
            self._operation_handle = None

    async def _stop_for_cancellation(
        self,
        run_input: GoalDirectedRunInput,
        run_version: int,
        activity_timeout: timedelta,
    ) -> None:
        if not self._cancel_requested:
            return
        await self._lifecycle(
            run_input,
            LifecycleCommandRequest(
                command_id=f"goal:{run_input.run_id}:epoch:{run_input.execution_epoch}:cancel",
                expected_run_version=run_version,
                action={"kind": "cancel"},
                reason="Reconcile accepted GoalDirected cancellation request",
                occurred_at=workflow.now(),
            ),
            activity_timeout,
        )
        raise ApplicationError(
            "GoalDirected cancellation entered the shared reconciliation saga",
            type="goal_cancelling",
            non_retryable=True,
        )

    async def _prepare_operation(
        self,
        run_input: GoalDirectedRunInput,
        revision: GoalRevision,
        iteration: int,
        role: GoalOperationRole,
        run_version: int,
        family_version: int,
        reservation_id: str,
        reservation: dict[str, int],
        session_id: str,
        workspace_id: str,
        handoff_ref: str | None,
        handoff: GoalHandoff | None,
        verifier_input_refs: tuple[str, ...],
        activity_timeout: timedelta,
    ) -> GoalOperationDispatch:
        activity_name = (
            "goaldirected.prepare_executor"
            if role == "executor"
            else "goaldirected.prepare_verifier"
        )
        return await workflow.execute_activity(
            activity_name,
            GoalOperationPreparationRequest(
                request_scope=run_input.request_scope,
                run_id=run_input.run_id,
                effective_configuration_digest=run_input.effective_configuration_digest,
                semantic_input_binding_ref=run_input.semantic_input_binding_ref,
                goal_revision_id=revision.revision_id,
                goal_revision_digest=revision.canonical_digest,
                goal_revision=revision,
                goal_iteration=iteration,
                operation_role=role,
                operation_attempt=1,
                execution_generation=1,
                expected_run_version=run_version,
                expected_family_version=family_version,
                reservation_id=reservation_id,
                reservation=reservation,
                session_id=session_id,
                workspace_id=workspace_id,
                handoff_ref=handoff_ref,
                handoff=handoff,
                verifier_input_refs=verifier_input_refs,
                decided_at=workflow.now(),
            ),
            result_type=GoalOperationDispatch,
            start_to_close_timeout=activity_timeout,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    async def _reconcile_operation(
        self,
        run_input: GoalDirectedRunInput,
        blueprint: GoalDirectedBlueprint,
        claim: GoalExecutionClaim,
        role: GoalOperationRole,
        dispatch: GoalOperationDispatch,
        result: OperationWorkflowResult,
        executor_result: GoalExecutionResult | None,
        activity_timeout: timedelta,
    ) -> GoalOperationReconciliationResult:
        return await workflow.execute_activity(
            "goaldirected.reconcile_operation",
            GoalOperationReconciliationRequest(
                request_scope=run_input.request_scope,
                goal_revision_id=claim.identity.iteration.goal_revision_id,
                operation_role=role,
                operation_binding_ref=dispatch.operation_binding_ref,
                required_output_contract_refs=tuple(
                    sorted(blueprint.required_output_contracts)
                ),
                operation_request=dispatch.workflow_request,
                claim=claim,
                executor_result=executor_result,
                operation_result=result,
                compaction_failure_action=(
                    blueprint.session_policy.compaction_failure_action
                    if role == "executor"
                    else None
                ),
                verifier_policy_binding_ref=(
                    blueprint.verifier_policy.binding_ref if role == "verifier" else None
                ),
                verifier_rubric_ref=(
                    blueprint.verifier_policy.rubric_ref if role == "verifier" else None
                ),
                verifier_rubric_version=(
                    blueprint.verifier_policy.rubric_version if role == "verifier" else None
                ),
                acceptance_contract_ref=(
                    blueprint.acceptance_contract if role == "verifier" else None
                ),
                acceptance_version=(
                    blueprint.verifier_policy.acceptance_version
                    if role == "verifier"
                    else None
                ),
                recorded_at=workflow.now(),
            ),
            result_type=GoalOperationReconciliationResult,
            start_to_close_timeout=activity_timeout,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    async def _settle_operation(
        self,
        run_input: GoalDirectedRunInput,
        run_version: int,
        reservation_id: str,
        reservation: dict[str, int],
        usage: dict[str, int],
        evidence_ref: str,
        activity_timeout: timedelta,
    ) -> int:
        outcome = await self._lifecycle(
            run_input,
            LifecycleCommandRequest(
                command_id=f"goal:usage:{reservation_id}",
                expected_run_version=run_version,
                action={
                    "kind": "record_usage",
                    "usage_id": f"goal-usage:{reservation_id}",
                    "actual_amounts": usage,
                    "reservation_id": reservation_id,
                    "release_amounts": {
                        dimension: amount - min(usage.get(dimension, 0), amount)
                        for dimension, amount in reservation.items()
                        if amount > usage.get(dimension, 0)
                    },
                    "pending_external_amounts": {},
                },
                reason="Reconcile accepted GoalDirected operation usage",
                evidence_refs=(evidence_ref,),
                occurred_at=workflow.now(),
            ),
            activity_timeout,
        )
        return outcome.resulting_run_version

    async def _lifecycle(
        self,
        run_input: GoalDirectedRunInput,
        request: LifecycleCommandRequest,
        activity_timeout: timedelta,
    ) -> LifecycleCommandOutcome:
        outcome = await workflow.execute_activity(
            "goaldirected.apply_lifecycle_command",
            replace(
                request,
                run_id=run_input.run_id,
                request_scope=run_input.request_scope,
                effective_configuration_digest=run_input.effective_configuration_digest,
                idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                correlation_id=(
                    run_input.correlation_id
                    or f"goal:{run_input.run_id}:epoch:{run_input.execution_epoch}"
                ),
                blueprint_digest=run_input.blueprint_digest,
            ),
            result_type=LifecycleCommandOutcome,
            start_to_close_timeout=activity_timeout,
            retry_policy=RetryPolicy(maximum_attempts=0),
        )
        if not outcome.accepted:
            raise ApplicationError(
                f"authoritative lifecycle command rejected: {outcome.reason_code}",
                non_retryable=True,
            )
        return outcome


def _continuation(state: GoalDirectedExecutionState) -> GoalContinuationState:
    lineage_digest = sha256_digest(
        {
            "previous": state.lineage_digest,
            "execution_results": tuple(
                sha256_digest(asdict(item)) for item in state.execution_results
            ),
            "verification_results": tuple(
                item.verification_digest for item in state.verification_results
            ),
        }
    )
    return GoalContinuationState(
        active_revision=state.active_revision,
        accepted_revisions=state.accepted_revisions,
        next_goal_iteration=state.next_goal_iteration,
        next_agent_run=state.next_agent_run,
        session_generation=state.session_generation,
        session_token_usage=state.session_token_usage,
        workspace_generation=state.workspace_generation,
        handoffs=state.handoffs,
        output_refs=state.output_refs,
        no_progress_iterations=state.no_progress_iterations,
        repeated_blocker_count=state.repeated_blocker_count,
        last_blocker_class=state.last_blocker_class,
        rollover_count=state.rollover_count,
        next_session_mode=state.next_session_mode,
        completed_goal_iterations=(
            state.completed_goal_iterations + len(state.verification_results)
        ),
        completed_agent_runs=state.completed_agent_runs + len(state.execution_results),
        lineage_digest=lineage_digest,
    )


__all__ = ["GoalDirectedWorkflow"]
