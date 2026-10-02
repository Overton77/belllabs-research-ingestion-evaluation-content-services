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
        FAMILY_EXECUTION_GENERATION,
        BoundaryCommandAck,
        BoundaryCommandDelivery,
        BoundaryLifecycleOutcome,
        BoundaryLifecycleRequest,
        GoalContinuationState,
        GoalDirectedExecutionState,
        GoalDirectedRunInput,
        GoalDirectedRunResult,
        GoalExecutionClaim,
        GoalExecutionResult,
        GoalHandoff,
        GoalOperationRole,
        GoalPausedState,
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
    from app.domain.orchestration.search_attributes import run_search_attributes
    from app.temporal.search_attributes import (
        child_search_attributes,
        ensure_workflow_search_attributes,
        operation_workflow_search_attributes,
    )
    from app.temporal.workflows.operation import OperationWorkflow


CONTINUE_AS_NEW_ITERATIONS = 20
# RRM-007 (REQ-BP-GD-011): a policy-selected pause becomes a durable paused state instead of
# the non-retryable `goal_paused` failure. Histories recorded before the patch replay the
# failure unchanged.
DURABLE_PAUSE_PATCH = "rrm-007-durable-goal-pause"
POLICY_PAUSE_PREFIX = "goal-policy-pause:"


@workflow.defn(name="belllabs.goal-directed")
class GoalDirectedWorkflow:
    """Replay-safe GoalDirected family scheduler over generic operation children.

    RRM-007: delivered pause and resume commands are applied at iteration boundaries only
    (no executor or verifier unit active). A pause binds a durable paused state (revision,
    next iteration, session generation and mode, handoff ref, effect frontier, held
    reservations) and waits, without failing, for an accepted resume, cancellation, or
    Continue-As-New, which carries the paused state into the next technical segment.
    """

    def __init__(self) -> None:
        self._cancel_requested = False
        self._operation_handle: Any | None = None
        self._pending_commands: list[BoundaryCommandDelivery] = []
        self._command_acks: dict[tuple[str, str], BoundaryCommandAck] = {}
        self._applied_command_ids: set[tuple[str, str]] = set()
        self._paused: GoalPausedState | None = None
        self._execution_epoch = 1
        self._technical_segment = 1
        self._last_delivered_sequence = 0

    @workflow.signal
    def request_cancel(self) -> None:
        self._cancel_requested = True
        if self._operation_handle is not None:
            self._operation_handle.cancel()

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
            # F7: a non-contiguous sequence is a transient gap, decided again on redelivery.
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
    def boundary_state(self) -> dict[str, Any]:
        """Diagnostic only (REQ-CP-EXEC-007); the receipt ledger is the authority."""

        return {
            "pending_command_ids": [item.command_id for item in self._pending_commands],
            "applied_command_ids": sorted(key[1] for key in self._applied_command_ids),
            "paused": asdict(self._paused) if self._paused is not None else None,
            "technical_segment": self._technical_segment,
            "last_delivered_sequence": self._last_delivered_sequence,
        }

    @workflow.run
    async def run(self, run_input: GoalDirectedRunInput) -> GoalDirectedRunResult:
        blueprint = GoalDirectedBlueprint.model_validate(run_input.blueprint)
        if sha256_digest(blueprint) != run_input.blueprint_digest:
            raise ApplicationError(
                "frozen GoalDirected digest does not match its exact blueprint binding",
                non_retryable=True,
            )
        self._execution_epoch = run_input.execution_epoch
        self._technical_segment = run_input.technical_segment
        self._applied_command_ids.update(
            _split_key(item) for item in run_input.applied_boundary_command_ids
        )
        self._last_delivered_sequence = max(
            self._last_delivered_sequence, run_input.last_delivered_sequence
        )
        carried = {_command_key(item) for item in self._pending_commands}
        self._pending_commands.extend(
            item
            for item in run_input.pending_boundary_commands
            if _command_key(item) not in carried
        )
        interpreter = GoalDirectedInterpreter(blueprint)
        ensure_workflow_search_attributes(
            run_input.search_attribute_policy,
            run_search_attributes(
                workflow_kind="family",
                run_id=run_input.run_id,
                request_scope=run_input.request_scope,
                family="GoalDirected",
                execution_epoch=run_input.execution_epoch,
            ),
        )
        try:
            state = interpreter.initial_state(run_input)
        except GoalDirectedExecutionError as error:
            raise ApplicationError(str(error), non_retryable=True) from error

        timeout = timedelta(seconds=run_input.task_timeout_seconds)
        boundary_ref = workflow.info().workflow_id
        run_version = run_input.initial_run_version
        if run_input.continuation_state is None:
            parent = workflow.info().parent
            lifecycle = await self._lifecycle(
                run_input,
                LifecycleCommandRequest(
                    command_id=(
                        f"goal:{run_input.run_id}:epoch:{run_input.execution_epoch}:"
                        f"segment:{run_input.technical_segment}:start"
                    ),
                    expected_run_version=run_version,
                    action={
                        "kind": "start",
                        "execution_target": {
                            "family": "GoalDirected",
                            "family_workflow_id": boundary_ref,
                            "root_workflow_id": (
                                parent.workflow_id if parent is not None else None
                            ),
                            "execution_epoch": run_input.execution_epoch,
                            "execution_generation": FAMILY_EXECUTION_GENERATION,
                        },
                    },
                    reason="Start canonical GoalDirected family execution",
                    occurred_at=workflow.now(),
                ),
                timeout,
            )
            run_version = lifecycle.resulting_run_version
        family_version = run_input.family_version
        continuation = run_input.continuation_state
        if continuation is not None and continuation.paused is not None:
            # REQ-CP-EXEC-011: the durable paused state continued into this segment.
            self._paused = continuation.paused
            state = replace(state, status="paused")

        while state.status in {"ready", "paused"}:
            await self._stop_for_cancellation(run_input, run_version, timeout)

            # RRM-007: an iteration boundary. No unit is active here: apply what was delivered
            # (only histories with deliveries reach this code).
            run_version = await self._apply_pending(
                run_input, state, run_version, boundary_ref, timeout, blueprint
            )
            if self._paused is not None:
                if state.status != "paused":
                    state = replace(state, status="paused")
                if run_input.force_continue_as_new:
                    await workflow.wait_condition(workflow.all_handlers_finished)
                    workflow.continue_as_new(
                        self._continuation_input(run_input, state, run_version, family_version)
                    )
                await self._wait_while_paused()
                continue
            if state.status == "paused":
                # Resumed: the interpreter frontier is unchanged; continue from it.
                state = replace(state, status="ready")

            if run_input.force_continue_as_new and (
                state.verification_results or self._paused is not None
            ):
                # REQ-CP-EXEC-011 / REQ-BP-GD-011: a forced continuation at an iteration
                # boundary carries the paused state and the pending commands unchanged.
                await workflow.wait_condition(workflow.all_handlers_finished)
                workflow.continue_as_new(
                    self._continuation_input(run_input, state, run_version, family_version)
                )

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
                None,
                claim.prior_handoff_ref or None,
                (
                    claimed_state.handoffs[-1]
                    if claim.prior_handoff_ref and claimed_state.handoffs
                    else None
                ),
                (),
                timeout,
                claim,
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
            try:
                projected = interpreter.apply_execution_result(
                    claimed_state, executor_accepted.execution_result
                )
            except GoalDirectedExecutionError as error:
                raise ApplicationError(str(error), non_retryable=True) from error
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
                executor_accepted.execution_result.workspace_id,
                None,
                None,
                executor_accepted.execution_result.output_refs,
                timeout,
                claim,
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

            if state.status == "paused":
                if not workflow.patched(DURABLE_PAUSE_PATCH):
                    raise ApplicationError(
                        "GoalDirected paused by deterministic convergence policy",
                        type="goal_paused",
                        non_retryable=True,
                    )
                # REQ-BP-GD-011: a policy-selected pause enters run control as a pause bound
                # to the convergence decision, which this boundary then applies.
                run_version = await self._pause_by_policy(
                    run_input, state, run_version, boundary_ref, timeout, blueprint
                )
                continue

            if (
                state.status == "ready"
                and state.next_goal_iteration % run_input.continue_as_new_iterations == 0
            ):
                workflow.continue_as_new(
                    self._continuation_input(run_input, state, run_version, family_version)
                )

        await self._stop_for_cancellation(run_input, run_version, timeout)
        # F1: drain what was delivered during the final iteration before closing; at this
        # point no pause is active, so every pending command is `not_applicable` here.
        run_version = await self._apply_pending(
            run_input, state, run_version, boundary_ref, timeout, blueprint, closing=True
        )
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
        # Failed/partial convergence may retain produced artifact references for
        # diagnosis, but those artifacts are not authoritatively valid outputs.
        # Promoting them and then proposing an empty terminal output set would
        # contradict the run-control terminalization contract.
        promotable_output_refs = (
            result.output_refs
            if terminalization_proposal.proposed_outcome == "complete"
            else ()
        )
        for output_ref in promotable_output_refs:
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

    # --- RRM-007 boundary mechanics ---------------------------------------------------------

    async def _wait_while_paused(self) -> None:
        """GD-011: wait without failing until a delivery (resume) or cancellation arrives."""

        await workflow.wait_condition(
            lambda: bool(self._pending_commands) or self._cancel_requested
        )

    async def _apply_pending(
        self,
        run_input: GoalDirectedRunInput,
        state: GoalDirectedExecutionState,
        run_version: int,
        boundary_ref: str,
        activity_timeout: timedelta,
        blueprint: GoalDirectedBlueprint,
        *,
        closing: bool = False,
    ) -> int:
        while self._pending_commands and not self._cancel_requested:
            delivery = min(self._pending_commands, key=lambda item: item.target_sequence)
            self._pending_commands.remove(delivery)
            applicable, rejection = self._decide(delivery)
            if closing and applicable:
                applicable, rejection = False, "not_applicable"
            if not applicable:
                outcome = await self._boundary_fact(
                    run_input,
                    BoundaryLifecycleRequest(
                        command_id=(
                            f"boundary-reject:{delivery.idempotency_issuer}:{delivery.command_id}"
                        ),
                        action={},
                        reason=(
                            "GoalDirected boundary is closing"
                            if closing
                            else f"GoalDirected boundary cannot apply {delivery.kind}"
                        ),
                        boundary_ref=boundary_ref,
                        boundary_command_id=delivery.command_id,
                        boundary_command_issuer=delivery.idempotency_issuer,
                        rejection_reason=rejection,
                    ),
                    activity_timeout,
                )
                self._mark_handled(delivery, outcome)
                continue
            if delivery.kind == "pause":
                decision = delivery.payload.get("decision") or {}
                paused = self._paused_state(
                    state, str(decision.get("decision_id", "")), delivery.command_id, run_version,
                    run_input, blueprint,
                )
                outcome = await self._boundary_fact(
                    run_input,
                    BoundaryLifecycleRequest(
                        command_id=(
                            f"boundary-apply:{delivery.idempotency_issuer}:{delivery.command_id}"
                        ),
                        action={
                            "kind": "apply_boundary_command",
                            "command_id": delivery.command_id,
                            "command_issuer": delivery.idempotency_issuer,
                            "action": delivery.payload,
                            "boundary_ref": boundary_ref,
                            "runnable_work_remains": False,
                            "boundary_state": asdict(paused),
                        },
                        reason="GoalDirected boundary applied the pause at an iteration boundary",
                        boundary_ref=boundary_ref,
                        boundary_command_id=delivery.command_id,
                        boundary_command_issuer=delivery.idempotency_issuer,
                    ),
                    activity_timeout,
                )
                self._mark_handled(delivery, outcome)
                if outcome.accepted:
                    run_version = outcome.resulting_run_version
                    self._paused = paused
                continue
            # resume
            assert self._paused is not None
            outcome = await self._boundary_fact(
                run_input,
                BoundaryLifecycleRequest(
                    command_id=(
                        f"boundary-apply:{delivery.idempotency_issuer}:{delivery.command_id}"
                    ),
                    action={
                        "kind": "apply_boundary_command",
                        "command_id": delivery.command_id,
                        "command_issuer": delivery.idempotency_issuer,
                        "action": delivery.payload,
                        "boundary_ref": boundary_ref,
                        "runnable_work_remains": True,
                        # REQ-BP-GD-011: the next iteration must be reservable again.
                        "resume_reservation": dict(self._paused.next_iteration_reservation),
                        "boundary_state": {
                            "resumed_pause_decision_id": self._paused.pause_decision_id,
                            "next_goal_iteration": self._paused.next_goal_iteration,
                            "active_revision_id": self._paused.active_revision_id,
                            "session_generation": self._paused.session_generation,
                        },
                    },
                    reason="GoalDirected boundary applied the resume at its recorded frontier",
                    boundary_ref=boundary_ref,
                    boundary_command_id=delivery.command_id,
                    boundary_command_issuer=delivery.idempotency_issuer,
                ),
                activity_timeout,
            )
            self._mark_handled(delivery, outcome)
            if outcome.accepted:
                run_version = outcome.resulting_run_version
                self._paused = None
            # A rejected resume (for example `insufficient_budget`) keeps the run paused.
        return run_version

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

    def _decide(self, delivery: BoundaryCommandDelivery) -> tuple[bool, str]:
        if delivery.kind == "satisfy_wait":
            return False, "not_applicable"
        decision = delivery.payload.get("decision") or {}
        if delivery.kind == "pause":
            if self._paused is not None or not decision.get("decision_id"):
                return False, "not_applicable"
            return True, ""
        if (
            self._paused is None
            or str(decision.get("pause_decision_id", "")) != self._paused.pause_decision_id
        ):
            return False, "not_applicable"
        return True, ""

    def _paused_state(
        self,
        state: GoalDirectedExecutionState,
        pause_decision_id: str,
        command_id: str,
        run_version: int,
        run_input: GoalDirectedRunInput,
        blueprint: GoalDirectedBlueprint,
    ) -> GoalPausedState:
        last_result = state.execution_results[-1] if state.execution_results else None
        return GoalPausedState(
            pause_decision_id=pause_decision_id,
            command_id=command_id,
            active_revision_id=state.active_revision.revision_id,
            next_goal_iteration=state.next_goal_iteration,
            session_generation=state.session_generation,
            next_session_mode=state.next_session_mode,
            handoff_ref=state.handoffs[-1].handoff_id if state.handoffs else "",
            effect_frontier_refs=(
                last_result.effect_frontier_refs if last_result is not None else ()
            ),
            # At an iteration boundary every iteration reservation is settled; the run-level
            # baseline is kept (RRM-001 §8.4). Nothing is released, so nothing is re-reserved
            # beyond the next iteration's probe. The reservations actually held are recorded
            # by authority on the `applied` receipt (F8); the workflow cannot read the budget.
            held_reservation_ids=(),
            released_reservation_ids=(),
            next_iteration_reservation=dict(blueprint.iteration_reservation),
            paused_at_run_version=run_version,
        )

    async def _pause_by_policy(
        self,
        run_input: GoalDirectedRunInput,
        state: GoalDirectedExecutionState,
        run_version: int,
        boundary_ref: str,
        activity_timeout: timedelta,
        blueprint: GoalDirectedBlueprint,
    ) -> int:
        proposal = state.convergence_proposal
        assert proposal is not None
        decision_id = f"{POLICY_PAUSE_PREFIX}{proposal.proposal_id.removeprefix('sha256:')[:32]}"
        pause_action = {
            "kind": "pause",
            "decision": {
                "decision_id": decision_id,
                "scope": ["run"],
                "reason": f"convergence policy selected pause: {proposal.reason}",
                "authority_ref": run_input.orchestration_authority_ref,
            },
            "runnable_work_remains": False,
        }
        command_id = f"goal:{run_input.run_id}:epoch:{run_input.execution_epoch}:{decision_id}"
        accepted = await self._boundary_fact(
            run_input,
            BoundaryLifecycleRequest(
                command_id=command_id,
                action=pause_action,
                reason="GoalDirected convergence policy proposed a pause",
                boundary_ref=boundary_ref,
                evidence_refs=(proposal.verification_ref,),
                boundary_command_id=command_id,
                boundary_command_issuer=run_input.lifecycle_idempotency_issuer,
            ),
            activity_timeout,
        )
        if not accepted.accepted:
            raise ApplicationError(
                f"run control rejected the policy pause: {accepted.reason_code}",
                non_retryable=True,
            )
        run_version = accepted.resulting_run_version
        # The self-applied command took a place in the run's sequence space; keep the
        # family's contiguity check aligned with it (F7).
        self._last_delivered_sequence = max(
            self._last_delivered_sequence, accepted.target_sequence
        )
        paused = self._paused_state(
            state, decision_id, command_id, run_version, run_input, blueprint
        )
        if accepted.receipt_state != "applied":
            applied = await self._boundary_fact(
                run_input,
                BoundaryLifecycleRequest(
                    command_id=f"boundary-apply:{run_input.lifecycle_idempotency_issuer}:{command_id}",
                    action={
                        "kind": "apply_boundary_command",
                        "command_id": command_id,
                        "command_issuer": run_input.lifecycle_idempotency_issuer,
                        "action": pause_action,
                        "boundary_ref": boundary_ref,
                        "runnable_work_remains": False,
                        "boundary_state": asdict(paused),
                    },
                    reason="GoalDirected boundary applied its policy pause",
                    boundary_ref=boundary_ref,
                    boundary_command_id=command_id,
                    boundary_command_issuer=run_input.lifecycle_idempotency_issuer,
                ),
                activity_timeout,
            )
            if applied.accepted:
                run_version = applied.resulting_run_version
            elif applied.receipt_state != "applied":
                raise ApplicationError(
                    f"run control did not apply the policy pause: {applied.reason_code}",
                    type="boundary_command_unresolved",
                    non_retryable=True,
                )
        self._paused = paused
        self._applied_command_ids.add((run_input.lifecycle_idempotency_issuer, command_id))
        return run_version

    async def _boundary_fact(
        self,
        run_input: GoalDirectedRunInput,
        request: BoundaryLifecycleRequest,
        activity_timeout: timedelta,
    ) -> BoundaryLifecycleOutcome:
        return await workflow.execute_activity(
            "goaldirected.apply_boundary_command",
            replace(
                request,
                run_id=run_input.run_id,
                request_scope=run_input.request_scope,
                idempotency_issuer=run_input.lifecycle_idempotency_issuer,
                correlation_id=(
                    run_input.correlation_id
                    or f"goal:{run_input.run_id}:epoch:{run_input.execution_epoch}"
                ),
                occurred_at=workflow.now(),
            ),
            result_type=BoundaryLifecycleOutcome,
            start_to_close_timeout=activity_timeout,
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

    def _continuation_input(
        self,
        run_input: GoalDirectedRunInput,
        state: GoalDirectedExecutionState,
        run_version: int,
        family_version: int,
    ) -> GoalDirectedRunInput:
        return replace(
            run_input,
            initial_revision=state.active_revision,
            initial_run_version=run_version,
            family_version=family_version,
            technical_segment=run_input.technical_segment + 1,
            force_continue_as_new=False,
            continuation_handoff=(state.handoffs[-1] if state.handoffs else None),
            continuation_state=_continuation(state, self._paused),
            pending_boundary_commands=tuple(
                sorted(self._pending_commands, key=lambda item: item.target_sequence)
            ),
            applied_boundary_command_ids=tuple(
                sorted(_join_key(key) for key in self._applied_command_ids)
            ),
            last_delivered_sequence=self._last_delivered_sequence,
        )

    async def _execute_operation(
        self,
        run_input: GoalDirectedRunInput,
        dispatch: GoalOperationDispatch,
        run_version: int,
        activity_timeout: timedelta,
    ) -> OperationWorkflowResult:
        request = dispatch.workflow_request
        policy = run_input.search_attribute_policy
        operation_attributes = child_search_attributes(
            policy,
            operation_workflow_search_attributes(
                request, family="GoalDirected", execution_epoch=run_input.execution_epoch
            ),
        )
        if operation_attributes is not None:
            request = request.model_copy(update={"search_attribute_policy": policy})
        handle = await workflow.start_child_workflow(
            OperationWorkflow.run,
            request,
            id=f"operation/{request.semantic_attempt_id}",
            parent_close_policy=workflow.ParentClosePolicy.REQUEST_CANCEL,
            search_attributes=operation_attributes,
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
        read_workspace_id: str | None,
        handoff_ref: str | None,
        handoff: GoalHandoff | None,
        verifier_input_refs: tuple[str, ...],
        activity_timeout: timedelta,
        claim: GoalExecutionClaim,
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
                read_workspace_id=read_workspace_id,
                handoff_ref=handoff_ref,
                handoff=handoff,
                verifier_input_refs=verifier_input_refs,
                decided_at=workflow.now(),
                execution_epoch=claim.identity.iteration.execution_epoch,
                agent_run=claim.identity.agent_run,
                session_generation=claim.identity.session_generation,
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
                remaining_iterations=max(
                    blueprint.max_iterations
                    - claim.identity.iteration.goal_iteration,
                    0,
                ),
                protected_fact_classes=(
                    tuple(sorted(blueprint.session_policy.protected_fact_classes))
                    if role == "executor"
                    else ()
                ),
                context_selection_policy_ref=(
                    blueprint.session_policy.context_selection_policy_ref
                    if role == "executor"
                    else None
                ),
                context_compaction_policy_ref=(
                    blueprint.session_policy.context_compaction_policy_ref
                    if role == "executor"
                    else None
                ),
                workspace_ref_class=(
                    sorted(blueprint.handoff_policy.allowed_workspace_ref_classes)[0]
                    if role == "executor"
                    else None
                ),
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
            # Reconciliation is deterministic validation/persistence. Replaying an
            # invalid completed provider payload cannot repair it and obscures the
            # original boundary failure.
            retry_policy=RetryPolicy(maximum_attempts=1),
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
            # Temporal uses zero for unlimited attempts. Lifecycle commands are
            # idempotent but authority/schema defects must fail visibly, not hot-loop.
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        if not outcome.accepted:
            raise ApplicationError(
                f"authoritative lifecycle command rejected: {outcome.reason_code}",
                non_retryable=True,
            )
        return outcome


def _command_key(delivery: BoundaryCommandDelivery) -> tuple[str, str]:
    return (delivery.idempotency_issuer, delivery.command_id)


def _join_key(key: tuple[str, str]) -> str:
    return f"{key[0]}::{key[1]}"


def _split_key(value: str) -> tuple[str, str]:
    issuer, separator, command_id = value.partition("::")
    return (issuer, command_id) if separator else ("", value)


def _continuation(
    state: GoalDirectedExecutionState, paused: GoalPausedState | None = None
) -> GoalContinuationState:
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
        paused=paused,
    )


__all__ = ["GoalDirectedWorkflow"]
