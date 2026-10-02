from __future__ import annotations

from dataclasses import asdict, replace
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, ChildWorkflowError

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
        CancelAck,
        CancelDelivery,
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
        GOAL_ADMISSION_STALE,
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
CLOSING_DRAIN_PATCH = "rrm-007-closing-drain"
# RRM-016: each executor and verifier operation is claimed, observed and settled exactly once
# in run control by the journaled operation boundary; the family consumes that settlement
# and continues from its run version instead of recording the usage itself. Histories
# recorded before the patch replay their own `record_usage` command unchanged.
JOURNALED_SETTLEMENT_PATCH = "rrm-016-journaled-goal-settlement"
# RRM-008 (REQ-CP-EXEC-008; RRM-001 section 7 #5): cancellation completes the saga as workflow
# logic (consume the cancelled unit's settlement, release the baseline, propose terminal
# `cancelled`, wait for liabilities the operator must reconcile) instead of failing the
# family with `goal_cancelling`. Histories recorded before the patch replay the failure.
CANCELLATION_SAGA_PATCH = "rrm-008-goal-cancellation-saga"
# RRM-016 review fix 2: the family's run version comes from the last authority fact it saw;
# an outside command (an API cancel, a late child effect) can move the run before the next
# family command. A stale result is re-read (the activity reports the current version and
# phase) and the command is retried once under a new identity at that version. A run that is
# already `cancelling` is not retried: the family enters its cancellation boundary (the seam
# RRM-008's saga owns). Pre-patch histories fail on a stale result, as they did.
STALE_VERSION_RETRY_PATCH = "rrm-016-stale-version-retry"
# RRM-019 (REQ-BP-GD-004, REQ-BP-GD-010, REQ-CP-RUN-005): a completed run promotes exactly the
# outputs its terminalization proposal names, which are the final executor's outputs that the
# accepting verifier admitted. Earlier iterations' outputs stay immutable lineage refs in the
# result but were never accepted by a verifier, so they are not promoted. Pre-patch histories
# promote the union of every iteration's outputs, as they did.
VERIFIED_TERMINAL_OUTPUTS_PATCH = "rrm-019-verified-terminal-outputs"
STALE_RUN_VERSION = "stale_run_version"
CANCELLING = "cancelling"
POLICY_PAUSE_PREFIX = "goal-policy-pause:"


class _CancellationEntered(Exception):
    """An authority result showed the run already `cancelling`: the saga takes over."""

    def __init__(self, run_version: int) -> None:
        super().__init__("GoalDirected run is cancelling")
        self.run_version = run_version


# Terminalization rejections that name a liability the saga waits for (an async child's
# pending usage, an unsettled effect, an operator reconciliation), never a defect.
LIABILITY_REJECTIONS = frozenset(
    {
        "budget_not_settled",
        "effects_not_settled",
        "unresolved_async_children",
        "unresolved_terminal_dependencies",
        "cancellation_not_settled",
    }
)
# Rejections the saga repairs by re-reading the authoritative digests and version.
STALE_REJECTIONS = frozenset(
    {
        "stale_run_version",
        "stale_control_revision",
        "stale_workflow_type_revision",
        "stale_obligation_revision",
        "stale_evidence_frontier",
        "obligation_evidence_mismatch",
        "obligation_acceptance_mismatch",
    }
)


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
        # Set when an authority result showed the run already `cancelling` (review fix 2).
        self._run_cancelling = False
        self._operation_handle: Any | None = None
        self._pending_commands: list[BoundaryCommandDelivery] = []
        self._command_acks: dict[tuple[str, str], BoundaryCommandAck] = {}
        self._applied_command_ids: set[tuple[str, str]] = set()
        self._paused: GoalPausedState | None = None
        self._execution_epoch = 1
        self._technical_segment = 1
        self._last_delivered_sequence = 0
        self._cancel_acks: dict[str, CancelAck] = {}
        self._cancel_command: tuple[str, str] | None = None
        self._liability_hints = 0

    @workflow.signal
    def request_cancel(self) -> None:
        # The root-internal propagation channel of pre-RRM-008 histories; the governed path
        # is run control -> ledger -> `deliver_cancel` (REQ-CP-EXEC-008 steps 1-2).
        self._cancel_requested = True
        if self._operation_handle is not None:
            self._operation_handle.cancel()

    @workflow.signal
    def liability_reconciled(self, reference: str) -> None:
        """A compact wake-up hint: a liability the cancellation saga waits for (pending child
        usage, an in_doubt unit) was reconciled. It releases nothing by itself; the saga
        proposes terminalization again and the reducer decides (REQ-CP-EXEC-007)."""

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
            if self._operation_handle is not None:
                # Step 2: the cancel reaches the active OperationWorkflow at once; the unit
                # settles through its own saga and returns its disposition.
                self._operation_handle.cancel()
        ack = CancelAck(
            command_id=delivery.command_id,
            status=status,  # type: ignore[arg-type]
            technical_segment=self._technical_segment,
        )
        self._cancel_acks[delivery.command_id] = ack
        return ack

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
        # RRM-008 (REQ-CP-EXEC-011): a cancel delivered before Continue-As-New stays delivered.
        self._cancel_requested = self._cancel_requested or run_input.cancel_requested
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
            if lifecycle.phase == "cancelling" and workflow.patched(CANCELLATION_SAGA_PATCH):
                # The cancel was accepted before this family's start fact (run control was
                # the boundary): the family binds its target and runs the saga at once.
                self._cancel_requested = True
            digests: LifecycleCommandOutcome | None = lifecycle
        else:
            digests = None
        family_version = run_input.family_version
        continuation = run_input.continuation_state
        if continuation is not None and continuation.paused is not None:
            # REQ-CP-EXEC-011: the durable paused state continued into this segment.
            self._paused = continuation.paused
            state = replace(state, status="paused")

        try:
            while state.status in {"ready", "paused"}:
                cancelled = await self._stop_for_cancellation(
                    run_input, run_version, timeout, state=state, digests=digests
                )
                if cancelled is not None:
                    return cancelled

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
                if self._cancel_requested and workflow.patched(CANCELLATION_SAGA_PATCH):
                    return await self._cancellation_saga(
                        run_input,
                        blueprint,
                        claimed_state,
                        run_version,
                        timeout,
                        digests,
                        claim=claim,
                        role="executor",
                        dispatch=executor_dispatch,
                        result=executor_result,
                        executor_result=None,
                        reservation_id=claim.reservation_id,
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
                run_version = await self._consume_settlement(
                    run_input,
                    run_version,
                    executor_accepted,
                    claim.reservation_id,
                    claim.reservation,
                    executor_accepted.execution_result.actual_usage,
                    timeout,
                )
                cancelled = await self._stop_for_cancellation(
                    run_input, run_version, timeout, state=projected, digests=digests
                )
                if cancelled is not None:
                    return cancelled

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
                if self._cancel_requested and workflow.patched(CANCELLATION_SAGA_PATCH):
                    return await self._cancellation_saga(
                        run_input,
                        blueprint,
                        projected,
                        run_version,
                        timeout,
                        digests,
                        claim=claim,
                        role="verifier",
                        dispatch=verifier_dispatch,
                        result=verifier_result,
                        executor_result=executor_accepted.execution_result,
                        reservation_id=verifier_reservation_id,
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
                run_version = await self._consume_settlement(
                    run_input,
                    run_version,
                    verifier_accepted,
                    verifier_reservation_id,
                    claim.reservation,
                    verifier_accepted.verification_result.actual_usage,
                    timeout,
                )
                cancelled = await self._stop_for_cancellation(
                    run_input, run_version, timeout, state=state, digests=digests
                )
                if cancelled is not None:
                    return cancelled

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

            cancelled = await self._stop_for_cancellation(
                run_input, run_version, timeout, state=state, digests=digests
            )
            if cancelled is not None:
                return cancelled
            # F1: drain what was delivered during the final iteration before closing; at this
            # point no pause is active, so every pending command is `not_applicable` here.
            if self._pending_commands and workflow.patched(CLOSING_DRAIN_PATCH):
                run_version = await self._apply_pending(
                    run_input, state, run_version, boundary_ref, timeout, blueprint, closing=True
                )
            terminalization_proposal = state.terminalization_proposal
            if terminalization_proposal is None:
                return interpreter.result(state)
            result = interpreter.result(state)

            final_verification = result.verification_results[-1]
            accepted_output_refs = (
                terminalization_proposal.output_refs
                if workflow.patched(VERIFIED_TERMINAL_OUTPUTS_PATCH)
                else result.output_refs
            )
            evidence_digest = sha256_digest(
                {
                    "verification": final_verification.verification_ref,
                    "outputs": accepted_output_refs,
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
                accepted_output_refs
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
        except _CancellationEntered as entered:
            # RRM-016 review fix 2 meets RRM-008: an authority result reported the run
            # `cancelling`; the saga completes from the family's current state.
            return await self._cancellation_saga(
                run_input, None, state, entered.run_version, timeout, digests
            )

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
                    state,
                    str(decision.get("decision_id", "")),
                    delivery.command_id,
                    run_version,
                    run_input,
                    blueprint,
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
        # The self-issued pause is sequenced in the family's own space (N1): the root's and
        # the family's `execution` contiguity are untouched.
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
            cancel_requested=self._cancel_requested,
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
            if not self._cancel_requested or workflow.patched(CANCELLATION_SAGA_PATCH):
                # Under the saga a cancelled unit completes normally with its disposition;
                # a failed child is the family's failure as before.
                raise
            try:
                await self._stop_for_cancellation(run_input, run_version, activity_timeout)
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
        *,
        state: GoalDirectedExecutionState | None = None,
        digests: LifecycleCommandOutcome | None = None,
    ) -> GoalDirectedRunResult | None:
        if not self._cancel_requested:
            return None
        if state is not None and workflow.patched(CANCELLATION_SAGA_PATCH):
            return await self._cancellation_saga(
                run_input, None, state, run_version, activity_timeout, digests
            )
        if not self._run_cancelling:
            await self._lifecycle(
                run_input,
                LifecycleCommandRequest(
                    command_id=(
                        f"goal:{run_input.run_id}:epoch:{run_input.execution_epoch}:cancel"
                    ),
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

    # --- RRM-008 cancellation saga (REQ-CP-EXEC-008 steps 5-7) ------------------------------

    async def _cancellation_saga(
        self,
        run_input: GoalDirectedRunInput,
        blueprint: GoalDirectedBlueprint | None,
        state: GoalDirectedExecutionState,
        run_version: int,
        activity_timeout: timedelta,
        digests: LifecycleCommandOutcome | None,
        *,
        claim: GoalExecutionClaim | None = None,
        role: GoalOperationRole | None = None,
        dispatch: GoalOperationDispatch | None = None,
        result: OperationWorkflowResult | None = None,
        executor_result: GoalExecutionResult | None = None,
        reservation_id: str | None = None,
    ) -> GoalDirectedRunResult:
        """Complete the cancellation as workflow logic and return the cancelled result.

        1. The unit that was active when the cancel landed is consumed through its own
           journaled settlement (cancelled, failed, or completed if it finished first); the
           family never records usage for it.
        2. Commands delivered but not applied are rejected `superseded`.
        3. The run-level baseline reservation is released.
        4. Terminal `cancelled` is proposed; the reducer decides it only once every
           reservation and effect is settled (REQ-CP-RUN-005). A liability the operator must
           reconcile (an async child's pending usage, an in_doubt unit) keeps the run
           `cancelling`; the saga waits for the hint signal or its timer and proposes again.
        """

        boundary_ref = workflow.info().workflow_id
        if (
            blueprint is not None
            and claim is not None
            and role is not None
            and dispatch is not None
            and result is not None
            and reservation_id is not None
        ):
            if result.disposition == "in_doubt":
                # The unit has no settlement (a pre-saga operation boundary returned it
                # parked, or a generation boundary superseded it). It stays a liability of
                # the run: its reservation and effect are unsettled, so the terminalization
                # below is rejected until the operator's `reconcile_unit` settles it (the
                # reducer rejects `start_new_generation` while the run is cancelling). The
                # saga waits for that; it never fails in place of reconciliation.
                pass
            else:
                accepted = await self._reconcile_operation(
                    run_input,
                    blueprint,
                    claim,
                    role,
                    dispatch,
                    result,
                    executor_result,
                    activity_timeout,
                )
                settlement = accepted.settlement
                if settlement is None:
                    raise ApplicationError(
                        "GoalDirected cancelled operation has no accepted run-control settlement",
                        type="goal_operation_settlement_missing",
                        non_retryable=True,
                    )
                if settlement.reservation_id != reservation_id:
                    raise ApplicationError(
                        "GoalDirected cancelled settlement does not match the admitted operation",
                        type="goal_operation_settlement_mismatch",
                        non_retryable=True,
                    )
                run_version = max(run_version, settlement.settled_run_version)
        # Delivered-but-unapplied commands are superseded by the cancellation.
        while self._pending_commands:
            delivery = min(self._pending_commands, key=lambda item: item.target_sequence)
            self._pending_commands.remove(delivery)
            outcome = await self._boundary_fact(
                run_input,
                BoundaryLifecycleRequest(
                    command_id=(
                        f"boundary-reject:{delivery.idempotency_issuer}:{delivery.command_id}"
                    ),
                    action={},
                    reason="GoalDirected cancellation superseded the delivered command",
                    boundary_ref=boundary_ref,
                    boundary_command_id=delivery.command_id,
                    boundary_command_issuer=delivery.idempotency_issuer,
                    rejection_reason="superseded",
                ),
                activity_timeout,
            )
            self._mark_handled(delivery, outcome)
        if run_input.baseline_reservation:
            for attempt in range(4):
                baseline = await self._lifecycle_outcome(
                    run_input,
                    LifecycleCommandRequest(
                        # A stale result is stored under its command identity: a retry at
                        # the reported version is a new command (RRM-016 `_at_version`).
                        command_id=(
                            "goal:usage:baseline"
                            if attempt == 0
                            else f"goal:usage:baseline:at-version:{run_version}"
                        ),
                        expected_run_version=run_version,
                        action={
                            "kind": "record_usage",
                            "usage_id": "goal-usage:baseline",
                            "actual_amounts": {},
                            "reservation_id": "baseline",
                            "release_amounts": dict(run_input.baseline_reservation),
                            "pending_external_amounts": {},
                        },
                        reason="Release the GoalDirected baseline reservation under cancellation",
                        evidence_refs=("goal-cancellation",),
                        occurred_at=workflow.now(),
                    ),
                    activity_timeout,
                )
                run_version = baseline.resulting_run_version
                digests = baseline
                if baseline.accepted or baseline.reason_code in {
                    "usage_exists",
                    "reservation_missing",
                    "reservation_required",
                }:
                    break  # released now, earlier, or never held
                if baseline.reason_code not in STALE_REJECTIONS:
                    raise ApplicationError(
                        f"baseline release rejected under cancellation: {baseline.reason_code}",
                        type="goal_cancellation_rejected",
                        non_retryable=True,
                    )
        await self._terminalize_cancelled(run_input, run_version, activity_timeout, digests)
        return self._cancelled_result(state)

    async def _terminalize_cancelled(
        self,
        run_input: GoalDirectedRunInput,
        run_version: int,
        activity_timeout: timedelta,
        digests: LifecycleCommandOutcome | None,
    ) -> None:
        seen = self._liability_hints
        backoff = run_input.cancellation_retry_seconds
        attempt = 0
        while True:
            attempt += 1
            if digests is None or digests.workflow_type_digest == "":
                # Probe the authoritative digests: a rejected proposal reports them.
                digests = await self._lifecycle_outcome(
                    run_input,
                    self._cancellation_proposal(run_input, run_version, None, f"probe:{attempt}"),
                    activity_timeout,
                )
                run_version = digests.resulting_run_version
                if digests.accepted and digests.terminal_outcome is not None:
                    return
            outcome = await self._lifecycle_outcome(
                run_input,
                self._cancellation_proposal(run_input, run_version, digests, str(attempt)),
                activity_timeout,
            )
            if outcome.accepted and outcome.terminal_outcome is not None:
                return
            run_version = outcome.resulting_run_version
            digests = outcome
            if outcome.reason_code in STALE_REJECTIONS:
                continue
            if outcome.reason_code not in LIABILITY_REJECTIONS:
                raise ApplicationError(
                    f"run control rejected the cancellation terminal proposal: "
                    f"{outcome.reason_code}",
                    type="goal_cancellation_rejected",
                    non_retryable=True,
                )

            # A liability remains (step 5): wait for its reconciliation hint or the timer.
            def reconciled(waited: int = seen) -> bool:
                return self._liability_hints > waited

            try:
                await workflow.wait_condition(reconciled, timeout=timedelta(seconds=backoff))
            except TimeoutError:
                pass
            seen = self._liability_hints
            backoff = min(backoff * 2, 3_600)

    def _cancellation_proposal(
        self,
        run_input: GoalDirectedRunInput,
        run_version: int,
        digests: LifecycleCommandOutcome | None,
        attempt: str,
    ) -> LifecycleCommandRequest:
        placeholder = sha256_digest({"rrm-008": "digest probe"})
        proposal_id = (
            f"goal-cancellation:{run_input.run_id}:epoch:{run_input.execution_epoch}:"
            f"v{run_version}:{attempt}"
        )
        return LifecycleCommandRequest(
            command_id=f"goal:terminalization:{proposal_id}",
            expected_run_version=run_version,
            action={
                "kind": "terminalize",
                "proposal": {
                    "proposal_id": proposal_id,
                    "expected_run_version": run_version,
                    "workflow_type_digest": (
                        digests.workflow_type_digest if digests is not None else placeholder
                    ),
                    "obligation_revision": (
                        digests.obligation_revision if digests is not None else "unknown"
                    ),
                    "evidence_frontier_digest": (
                        digests.evidence_frontier_digest if digests is not None else placeholder
                    ),
                    "accepted_obligation_evidence_digest": (
                        digests.accepted_obligation_evidence_digest
                        if digests is not None
                        else placeholder
                    ),
                    "proposing_execution_binding_ref": (
                        f"goal-cancellation:{run_input.run_id}:epoch:{run_input.execution_epoch}"
                    ),
                    "required_obligations_accepted": (
                        digests.required_obligations_accepted if digests is not None else False
                    ),
                    "execution_failure_refs": (),
                    "degradable_failures": (),
                    "valid_output_refs": (),
                    "cancellation_settled": True,
                    "budget_settled": True,
                    "effects_settled": True,
                    "pending_wait_or_link_ids": (),
                    "proposed_at": workflow.now(),
                },
            },
            reason="Propose terminal cancelled after the GoalDirected cancellation saga",
            evidence_refs=("goal-cancellation",),
            occurred_at=workflow.now(),
        )

    @staticmethod
    def _cancelled_result(state: GoalDirectedExecutionState) -> GoalDirectedRunResult:
        return GoalDirectedRunResult(
            run_id=state.run_id,
            execution_epoch=state.execution_epoch,
            status="cancelled",
            convergence_proposal=None,
            terminalization_proposal=None,
            goal_iterations=state.completed_goal_iterations + len(state.verification_results),
            agent_runs=state.completed_agent_runs + len(state.execution_results),
            rollover_count=state.rollover_count,
            active_revision_id=state.active_revision.revision_id,
            accepted_revision_ids=tuple(
                revision.revision_id for revision in state.accepted_revisions
            ),
            output_refs=state.output_refs,
            handoffs=state.handoffs,
            execution_results=state.execution_results,
            verification_results=state.verification_results,
            lineage_digest=_continuation(state).lineage_digest,
        )

    async def _lifecycle_outcome(
        self,
        run_input: GoalDirectedRunInput,
        request: LifecycleCommandRequest,
        activity_timeout: timedelta,
    ) -> LifecycleCommandOutcome:
        """A lifecycle command whose rejection is decided by the caller (the saga)."""

        outcome: LifecycleCommandOutcome = await workflow.execute_activity(
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
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return outcome

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
        admission_attempt: int = 1,
    ) -> GoalOperationDispatch:
        activity_name = (
            "goaldirected.prepare_executor"
            if role == "executor"
            else "goaldirected.prepare_verifier"
        )
        try:
            return await self._prepare_activity(
                activity_name,
                run_input,
                revision,
                iteration,
                role,
                run_version,
                family_version,
                reservation_id,
                reservation,
                session_id,
                workspace_id,
                read_workspace_id,
                handoff_ref,
                handoff,
                verifier_input_refs,
                activity_timeout,
                claim,
                admission_attempt,
            )
        except ActivityError as error:
            stale = _admission_stale(error)
            if stale is None or not workflow.patched(STALE_VERSION_RETRY_PATCH):
                raise
            current_version, phase = stale
            if phase == CANCELLING:
                await self._enter_cancellation(run_input, current_version, activity_timeout)
            if admission_attempt != 1:
                raise
            # Nothing was admitted or bound: re-admit once at the run's current version.
            return await self._prepare_operation(
                run_input,
                revision,
                iteration,
                role,
                current_version,
                family_version,
                reservation_id,
                reservation,
                session_id,
                workspace_id,
                read_workspace_id,
                handoff_ref,
                handoff,
                verifier_input_refs,
                activity_timeout,
                claim,
                admission_attempt=2,
            )

    async def _prepare_activity(
        self,
        activity_name: str,
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
        admission_attempt: int,
    ) -> GoalOperationDispatch:
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
                admission_attempt=admission_attempt,
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
                required_output_contract_refs=tuple(sorted(blueprint.required_output_contracts)),
                operation_request=dispatch.workflow_request,
                claim=claim,
                executor_result=executor_result,
                operation_result=result,
                remaining_iterations=max(
                    blueprint.max_iterations - claim.identity.iteration.goal_iteration,
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
                    blueprint.verifier_policy.acceptance_version if role == "verifier" else None
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

    async def _consume_settlement(
        self,
        run_input: GoalDirectedRunInput,
        run_version: int,
        reconciled: GoalOperationReconciliationResult,
        reservation_id: str,
        reservation: dict[str, int],
        usage: dict[str, int],
        activity_timeout: timedelta,
    ) -> int:
        """Continue from the operation's accepted run-control settlement (RRM-016).

        REQ-CP-RUN-006/009: the operation's usage settles once, through its journaled
        settlement; REQ-CP-RUN-007: the effect was claimed, observed and settled by the
        operation boundary. The family records nothing here: it verifies that the
        settlement it consumes is the operation's own and continues from its run version.
        Pre-patch histories replay the family's own usage command.
        """

        if not workflow.patched(JOURNALED_SETTLEMENT_PATCH):
            return await self._settle_operation(
                run_input,
                run_version,
                reservation_id,
                reservation,
                usage,
                reconciled.detail_ref,
                activity_timeout,
            )
        settlement = reconciled.settlement
        if settlement is None:
            raise ApplicationError(
                "GoalDirected operation has no accepted run-control settlement",
                type="goal_operation_settlement_missing",
                non_retryable=True,
            )
        if (
            settlement.reservation_id != reservation_id
            or settlement.usage != usage
            or settlement.settled_run_version < run_version
        ):
            raise ApplicationError(
                "GoalDirected settlement does not match the admitted operation",
                type="goal_operation_settlement_mismatch",
                non_retryable=True,
            )
        return settlement.settled_run_version

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
        outcome = await self._lifecycle_activity(run_input, request, activity_timeout)
        if outcome.accepted:
            return outcome
        is_cancel = request.action.get("kind") == "cancel"
        if (
            is_cancel
            and outcome.phase == CANCELLING
            and workflow.patched(STALE_VERSION_RETRY_PATCH)
        ):
            # The run is already cancelling (an earlier or outside cancel was applied).
            self._run_cancelling = True
            return outcome
        if outcome.reason_code == STALE_RUN_VERSION and workflow.patched(STALE_VERSION_RETRY_PATCH):
            if outcome.phase == CANCELLING:
                await self._enter_cancellation(
                    run_input, outcome.resulting_run_version, activity_timeout
                )
            # The activity read the run's current version: retry once, as a new command.
            outcome = await self._lifecycle_activity(
                run_input, _at_version(request, outcome.resulting_run_version), activity_timeout
            )
            if outcome.accepted:
                return outcome
            if outcome.reason_code == STALE_RUN_VERSION and outcome.phase == CANCELLING:
                await self._enter_cancellation(
                    run_input, outcome.resulting_run_version, activity_timeout
                )
        raise ApplicationError(
            f"authoritative lifecycle command rejected: {outcome.reason_code}",
            non_retryable=True,
        )

    async def _enter_cancellation(
        self,
        run_input: GoalDirectedRunInput,
        run_version: int,
        activity_timeout: timedelta,
    ) -> None:
        """The run is already `cancelling` in run control: enter the family's cancellation
        boundary (`_stop_for_cancellation`, RRM-008's saga) without issuing another cancel."""

        self._cancel_requested = True
        self._run_cancelling = True
        if workflow.patched(CANCELLATION_SAGA_PATCH):
            # RRM-008: the saga runs at the iteration level, where the family state is.
            raise _CancellationEntered(run_version)
        await self._stop_for_cancellation(run_input, run_version, activity_timeout)

    async def _lifecycle_activity(
        self,
        run_input: GoalDirectedRunInput,
        request: LifecycleCommandRequest,
        activity_timeout: timedelta,
    ) -> LifecycleCommandOutcome:
        return await workflow.execute_activity(
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


def _at_version(request: LifecycleCommandRequest, version: int) -> LifecycleCommandRequest:
    """The same lifecycle fact at the run's current version, under a new command identity
    (a stale result is stored under the original one)."""

    action = dict(request.action)
    if action.get("kind") == "terminalize":
        action["proposal"] = {**dict(action["proposal"]), "expected_run_version": version}
    return replace(
        request,
        command_id=f"{request.command_id}:at-version:{version}",
        expected_run_version=version,
        action=action,
    )


def _admission_stale(error: ActivityError) -> tuple[int, str] | None:
    """(current run version, phase) of a stale operation admission, if that is the cause."""

    cause = error.cause
    if (
        isinstance(cause, ApplicationError)
        and cause.type == GOAL_ADMISSION_STALE
        and len(cause.details) >= 2
    ):
        return int(cause.details[0]), str(cause.details[1])
    return None


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
