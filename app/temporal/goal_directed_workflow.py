from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

with workflow.unsafe.imports_passed_through():
    from app.domain.control_plane.canonical import sha256_digest
    from app.domain.control_plane.contracts import GoalDirectedBlueprint
    from app.domain.orchestration.contracts import (
        GoalDirectedRunInput,
        GoalDirectedRunResult,
        GoalExecutionResult,
        GoalHandoffRequest,
        GoalHandoffResult,
        GoalVerificationRequest,
        GoalVerificationResult,
        LifecycleCommandOutcome,
        LifecycleCommandRequest,
    )
    from app.domain.orchestration.goal_directed import (
        GoalDirectedExecutionError,
        GoalDirectedInterpreter,
    )


@workflow.defn(name="belllabs.goal-directed")
class GoalDirectedWorkflow:
    """Durable Ralph loop over one immutable admitted GoalDirected configuration."""

    def __init__(self) -> None:
        self._run_id = ""
        self._request_scope = ""
        self._configuration_digest = ""
        self._blueprint_digest = ""
        self._idempotency_issuer = ""
        self._correlation_id = ""
        self._execution_epoch = 1

    @workflow.run
    async def run(self, run_input: GoalDirectedRunInput) -> GoalDirectedRunResult:
        if run_input.execution_epoch != 1:
            raise ApplicationError(
                "execution epoch rollover requires the deferred continuity contract",
                non_retryable=True,
            )
        self._run_id = run_input.run_id
        self._request_scope = run_input.request_scope
        self._configuration_digest = run_input.effective_configuration_digest
        self._blueprint_digest = run_input.blueprint_digest
        self._idempotency_issuer = run_input.lifecycle_idempotency_issuer
        self._correlation_id = run_input.correlation_id or (
            f"orchestration:{run_input.run_id}:epoch:{run_input.execution_epoch}"
        )
        self._execution_epoch = run_input.execution_epoch

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
        retry = RetryPolicy(maximum_attempts=3)
        lifecycle = await self._lifecycle(
            LifecycleCommandRequest(
                command_id=(
                    f"orchestration:{run_input.run_id}:epoch:{run_input.execution_epoch}:start"
                ),
                expected_run_version=run_input.initial_run_version,
                action={"kind": "start"},
                reason="Deterministic GoalDirected orchestration started",
                occurred_at=workflow.now(),
            ),
            timeout,
        )
        run_version = lifecycle.resulting_run_version

        while state.status != "terminal":
            try:
                claimed_state, claim = interpreter.claim_execution(state)
            except GoalDirectedExecutionError as error:
                raise ApplicationError(str(error), non_retryable=True) from error

            if claim.reservation:
                lifecycle = await self._lifecycle(
                    LifecycleCommandRequest(
                        command_id=f"orchestration:{claim.reservation_id}",
                        expected_run_version=run_version,
                        action={
                            "kind": "reserve_budget",
                            "reservation_id": claim.reservation_id,
                            "amounts": claim.reservation,
                        },
                        reason="Reserve budget before semantic Goal Iteration dispatch",
                        evidence_refs=(claim.idempotency_key,),
                        occurred_at=workflow.now(),
                    ),
                    timeout,
                )
                run_version = lifecycle.resulting_run_version

            try:
                execution = await workflow.execute_activity(
                    "goaldirected.execute_iteration",
                    claim,
                    result_type=GoalExecutionResult,
                    start_to_close_timeout=timeout,
                    retry_policy=retry,
                )
            except ActivityError:
                execution = GoalExecutionResult(
                    identity=claim.identity,
                    disposition="failed",
                    irrecoverable_failure_ref=(f"activity-failure:{claim.identity.semantic_key}"),
                )

            verification_request = GoalVerificationRequest(
                claim=claim,
                execution_result=execution,
                verifier_ref=blueprint.independent_verifier_ref,
                acceptance_contract_ref=blueprint.acceptance_contract,
                accepted_output_refs=state.output_refs,
            )
            try:
                verification = await workflow.execute_activity(
                    "goaldirected.verify_iteration",
                    verification_request,
                    result_type=GoalVerificationResult,
                    start_to_close_timeout=timeout,
                    retry_policy=retry,
                )
            except ActivityError:
                verification = GoalVerificationResult(
                    identity=claim.identity,
                    action="escalate",
                    verification_ref=(f"verifier-activity-failure:{claim.identity.semantic_key}"),
                    verifier_ref=blueprint.independent_verifier_ref,
                    acceptance_contract_ref=blueprint.acceptance_contract,
                    progress_made=False,
                    irrecoverable_failure_ref=(
                        f"verifier-activity-failure:{claim.identity.semantic_key}"
                    ),
                )

            try:
                projected = interpreter.apply_execution_result(claimed_state, execution)
                projected = interpreter.apply_verification(projected, verification)
            except GoalDirectedExecutionError as error:
                raise ApplicationError(str(error), non_retryable=True) from error

            token_rollover = (
                claimed_state.session_token_usage + execution.actual_usage.get("tokens.total", 0)
                >= claim.fresh_agent_token_threshold
            )
            if projected.status != "terminal" and token_rollover:
                handoff_request = GoalHandoffRequest(
                    claim=claim,
                    execution_result=execution,
                    protected_scope_digest=run_input.protected_scope_digest,
                    verification_ref=verification.verification_ref,
                    unmet_obligations=verification.unmet_obligations,
                )
                try:
                    handoff = await workflow.execute_activity(
                        "goaldirected.prepare_handoff",
                        handoff_request,
                        result_type=GoalHandoffResult,
                        start_to_close_timeout=timeout,
                        retry_policy=retry,
                    )
                except ActivityError:
                    handoff = await workflow.execute_activity(
                        "goaldirected.prepare_handoff",
                        replace(
                            handoff_request,
                            fallback=True,
                            failure_reason="agent handoff activity exhausted retries",
                        ),
                        result_type=GoalHandoffResult,
                        start_to_close_timeout=timeout,
                        retry_policy=RetryPolicy(maximum_attempts=0),
                    )
                combined_usage = dict(execution.actual_usage)
                for dimension, amount in handoff.actual_usage.items():
                    combined_usage[dimension] = combined_usage.get(dimension, 0) + amount
                execution = replace(
                    execution,
                    actual_usage=combined_usage,
                    handoff_checkpoint=handoff.checkpoint,
                )
                try:
                    projected = interpreter.apply_execution_result(claimed_state, execution)
                    projected = interpreter.apply_verification(projected, verification)
                except GoalDirectedExecutionError as error:
                    raise ApplicationError(str(error), non_retryable=True) from error

            if claim.reservation:
                actual_usage = dict(execution.actual_usage)
                for dimension, amount in verification.actual_usage.items():
                    actual_usage[dimension] = actual_usage.get(dimension, 0) + amount
                actual_usage.setdefault("goal.iterations", 1)
                release = {
                    dimension: amount - min(actual_usage.get(dimension, 0), amount)
                    for dimension, amount in claim.reservation.items()
                    if amount > actual_usage.get(dimension, 0)
                }
                lifecycle = await self._lifecycle(
                    LifecycleCommandRequest(
                        command_id=f"orchestration:usage:{claim.identity.semantic_key}",
                        expected_run_version=run_version,
                        action={
                            "kind": "record_usage",
                            "usage_id": f"usage:{claim.identity.semantic_key}",
                            "actual_amounts": actual_usage,
                            "reservation_id": claim.reservation_id,
                            "release_amounts": release,
                            "pending_external_amounts": {},
                        },
                        reason="Reconcile observed Goal Iteration usage",
                        evidence_refs=(verification.verification_ref,),
                        occurred_at=workflow.now(),
                    ),
                    timeout,
                )
                run_version = lifecycle.resulting_run_version
            state = projected

        result = interpreter.result(state)
        if result.stop_reason == "verified_completion":
            evidence_digest = sha256_digest(
                {
                    "run_id": result.run_id,
                    "verification_ref": result.verification_results[-1].verification_ref,
                    "output_refs": result.output_refs,
                }
            )
            for obligation_ref in run_input.required_obligation_refs:
                lifecycle = await self._lifecycle(
                    LifecycleCommandRequest(
                        command_id=(
                            f"orchestration:epoch:{self._execution_epoch}:"
                            f"goal-obligation:{obligation_ref}:{evidence_digest}"
                        ),
                        expected_run_version=run_version,
                        action={
                            "kind": "record_obligation_evidence",
                            "evidence": {
                                "obligation_ref": obligation_ref,
                                "evidence_digest": evidence_digest,
                                "accepted_by_authority_ref": (
                                    run_input.orchestration_authority_ref
                                ),
                            },
                        },
                        reason="Record independently verified GoalDirected obligation evidence",
                        evidence_refs=(result.verification_results[-1].verification_ref,),
                        occurred_at=workflow.now(),
                    ),
                    timeout,
                )
                run_version = lifecycle.resulting_run_version
            for output_ref in result.output_refs:
                lifecycle = await self._lifecycle(
                    LifecycleCommandRequest(
                        command_id=(
                            f"orchestration:epoch:{self._execution_epoch}:"
                            f"goal-output:{output_ref}:{evidence_digest}"
                        ),
                        expected_run_version=run_version,
                        action={
                            "kind": "record_output_evidence",
                            "evidence": {
                                "output_ref": output_ref,
                                "evidence_digest": evidence_digest,
                                "accepted_by_authority_ref": (
                                    run_input.orchestration_authority_ref
                                ),
                            },
                        },
                        reason="Record independently verified GoalDirected output evidence",
                        evidence_refs=(result.verification_results[-1].verification_ref,),
                        occurred_at=workflow.now(),
                    ),
                    timeout,
                )
                run_version = lifecycle.resulting_run_version

        if run_input.baseline_reservation:
            lifecycle = await self._lifecycle(
                LifecycleCommandRequest(
                    command_id=(
                        f"orchestration:{run_input.run_id}:epoch:"
                        f"{run_input.execution_epoch}:release-baseline"
                    ),
                    expected_run_version=run_version,
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
            )
            run_version = lifecycle.resulting_run_version

        final_verification = result.verification_results[-1]
        await self._lifecycle(
            LifecycleCommandRequest(
                command_id=(
                    f"orchestration:{run_input.run_id}:epoch:{run_input.execution_epoch}:"
                    f"terminal:{result.stop_reason}"
                ),
                expected_run_version=run_version,
                action={
                    "kind": "terminalize",
                    "proposal": {
                        "proposal_id": (
                            f"terminal:{run_input.run_id}:epoch:"
                            f"{run_input.execution_epoch}:{result.stop_reason}"
                        ),
                        "obligation_revision": lifecycle.obligation_revision,
                        "evidence_frontier_digest": lifecycle.evidence_frontier_digest,
                        "accepted_obligation_evidence_digest": (
                            lifecycle.accepted_obligation_evidence_digest
                        ),
                        "proposing_execution_binding_ref": (
                            f"orchestration-binding:{run_input.effective_configuration_digest}"
                        ),
                        "required_obligations_accepted": (lifecycle.required_obligations_accepted),
                        "execution_failure_refs": (
                            ()
                            if result.stop_reason == "verified_completion"
                            else (final_verification.verification_ref,)
                        ),
                        "degradable_failures": (
                            ("goal-directed-degraded",) if state.degraded else ()
                        ),
                        "valid_output_refs": (
                            result.output_refs
                            if result.stop_reason == "verified_completion"
                            else ()
                        ),
                        "cancellation_settled": False,
                        "budget_settled": True,
                        "pending_wait_or_link_ids": (),
                        "proposed_at": workflow.now(),
                    },
                },
                reason=f"GoalDirected reducer selected {result.stop_reason}",
                evidence_refs=(final_verification.verification_ref,),
                occurred_at=workflow.now(),
            ),
            timeout,
        )
        return result

    async def _lifecycle(
        self,
        request: LifecycleCommandRequest,
        activity_timeout: timedelta,
    ) -> LifecycleCommandOutcome:
        bound = replace(
            request,
            run_id=self._run_id,
            request_scope=self._request_scope,
            effective_configuration_digest=self._configuration_digest,
            idempotency_issuer=self._idempotency_issuer,
            correlation_id=self._correlation_id,
            blueprint_digest=self._blueprint_digest,
        )
        outcome = await workflow.execute_activity(
            "goaldirected.apply_lifecycle_command",
            bound,
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
