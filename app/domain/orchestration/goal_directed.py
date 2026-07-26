from __future__ import annotations

from dataclasses import replace
from typing import Literal

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import GoalDirectedBlueprint
from app.domain.orchestration.contracts import (
    GoalAgentRunIdentity,
    GoalDirectedExecutionState,
    GoalDirectedRunInput,
    GoalDirectedRunResult,
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoffCheckpoint,
    GoalIterationIdentity,
    GoalRevision,
    GoalStopReason,
    GoalVerificationResult,
)


class GoalDirectedExecutionError(ValueError):
    """A GoalDirected transition violated its frozen semantic envelope."""


class GoalDirectedInterpreter:
    """Pure deterministic state transitions for one frozen GoalDirected blueprint."""

    def __init__(self, blueprint: GoalDirectedBlueprint) -> None:
        self.blueprint = blueprint

    def initial_state(self, run_input: GoalDirectedRunInput) -> GoalDirectedExecutionState:
        frozen_blueprint = GoalDirectedBlueprint.model_validate(run_input.blueprint)
        if frozen_blueprint != self.blueprint:
            raise GoalDirectedExecutionError(
                "run input GoalDirected blueprint does not match the interpreter"
            )
        if sha256_digest(frozen_blueprint) != run_input.blueprint_digest:
            raise GoalDirectedExecutionError(
                "frozen GoalDirected digest does not match its exact blueprint binding"
            )
        if run_input.execution_epoch < 1:
            raise GoalDirectedExecutionError("execution epoch must be positive")
        revision = run_input.initial_revision
        if revision.revision != 1 or revision.parent_revision_id is not None:
            raise GoalDirectedExecutionError("GoalDirected run requires an initial Goal Revision")
        if revision.protected_scope_digest != run_input.protected_scope_digest:
            raise GoalDirectedExecutionError(
                "initial Goal Revision does not match the launch-bound protected scope"
            )
        return GoalDirectedExecutionState(
            run_id=run_input.run_id,
            execution_epoch=run_input.execution_epoch,
            protected_scope_digest=run_input.protected_scope_digest,
            active_revision=revision,
            accepted_revisions=(revision,),
            next_session_mode=self.blueprint.session_policy.session_mode,
        )

    def claim_execution(
        self,
        state: GoalDirectedExecutionState,
        *,
        operation_class: str | None = None,
    ) -> tuple[GoalDirectedExecutionState, GoalExecutionClaim]:
        if state.status != "ready" or state.active_claim is not None:
            raise GoalDirectedExecutionError(
                "GoalDirected state is not ready for an execution claim"
            )
        selected_operation = operation_class or sorted(self.blueprint.allowed_operation_classes)[0]
        if selected_operation not in self.blueprint.allowed_operation_classes:
            raise GoalDirectedExecutionError(
                "operation class is outside the frozen GoalDirected blueprint: "
                f"{selected_operation}"
            )
        iteration = GoalIterationIdentity(
            run_id=state.run_id,
            goal_iteration=state.next_goal_iteration,
            goal_revision_id=state.active_revision.revision_id,
            execution_epoch=state.execution_epoch,
        )
        identity = GoalAgentRunIdentity(
            iteration=iteration,
            agent_run=state.next_agent_run,
            session_generation=state.session_generation,
        )
        policy = self.blueprint.session_policy
        work_remaining = max(
            policy.fresh_agent_token_threshold - state.session_token_usage,
            0,
        )
        prior_checkpoint = (
            state.handoff_checkpoints[-1].checkpoint_id if state.handoff_checkpoints else ""
        )
        claim = GoalExecutionClaim(
            identity=identity,
            idempotency_key=f"goal-operation:{identity.semantic_key}",
            operation_class=selected_operation,
            objective=state.active_revision.objective,
            protected_scope_digest=state.protected_scope_digest,
            reservation_id=f"reservation:{identity.semantic_key}",
            reservation=dict(self.blueprint.iteration_reservation),
            session_mode=state.next_session_mode,
            session_id=(
                f"goal:{state.run_id}:execution-epoch:{state.execution_epoch}:"
                f"session:{state.session_generation}"
            ),
            workspace_mode=self.blueprint.workspace_policy.workspace_mode,
            workspace_namespace=(
                f"run/{state.run_id}/execution-epoch/{state.execution_epoch}/goal/"
                f"workspace/{state.workspace_generation}"
            ),
            snapshot_mode=self.blueprint.workspace_policy.snapshot_mode,
            prior_checkpoint_id=prior_checkpoint,
            fresh_agent_token_threshold=policy.fresh_agent_token_threshold,
            handoff_token_reserve=policy.handoff_token_reserve,
            token_budget_remaining=work_remaining + policy.handoff_token_reserve,
        )
        return replace(state, status="executing", active_claim=claim), claim

    def apply_execution_result(
        self,
        state: GoalDirectedExecutionState,
        result: GoalExecutionResult,
    ) -> GoalDirectedExecutionState:
        claim = state.active_claim
        if state.status != "executing" or claim is None:
            raise GoalDirectedExecutionError("GoalDirected state has no active execution claim")
        if result.identity != claim.identity:
            raise GoalDirectedExecutionError(
                "agent result does not match the active GoalDirected semantic identity"
            )
        if any(not dimension or amount < 0 for dimension, amount in result.actual_usage.items()):
            raise GoalDirectedExecutionError(
                "GoalDirected execution usage requires non-negative named dimensions"
            )
        checkpoint = result.handoff_checkpoint
        checkpoints = state.handoff_checkpoints
        if checkpoint is not None:
            self._validate_checkpoint(state, result, checkpoint)
            prior = next(
                (item for item in checkpoints if item.checkpoint_id == checkpoint.checkpoint_id),
                None,
            )
            if prior is not None and prior != checkpoint:
                raise GoalDirectedExecutionError(
                    "goal handoff checkpoint identity was reused with conflicting content"
                )
            if prior is None:
                checkpoints = (*checkpoints, checkpoint)
        return replace(
            state,
            status="awaiting_verification",
            active_claim=None,
            pending_result=result,
            execution_results=(*state.execution_results, result),
            handoff_checkpoints=checkpoints,
            output_refs=_ordered_union(state.output_refs, result.output_refs),
            session_token_usage=(
                state.session_token_usage + result.actual_usage.get("tokens.total", 0)
            ),
        )

    def apply_verification(
        self,
        state: GoalDirectedExecutionState,
        verification: GoalVerificationResult,
    ) -> GoalDirectedExecutionState:
        execution = state.pending_result
        if state.status != "awaiting_verification" or execution is None:
            raise GoalDirectedExecutionError(
                "GoalDirected state has no execution awaiting independent verification"
            )
        if verification.identity != execution.identity:
            raise GoalDirectedExecutionError(
                "verification does not match the active GoalDirected semantic identity"
            )
        if verification.verifier_ref != self.blueprint.independent_verifier_ref:
            raise GoalDirectedExecutionError(
                "GoalDirected verification is not bound to the frozen independent verifier"
            )
        if verification.acceptance_contract_ref != self.blueprint.acceptance_contract:
            raise GoalDirectedExecutionError(
                "GoalDirected verification is not bound to the frozen acceptance contract"
            )

        no_progress = 0 if verification.progress_made else state.no_progress_iterations + 1
        blocker = verification.blocker_class or execution.blocker_class
        if blocker:
            repeated_blocker = (
                state.repeated_blocker_count + 1 if blocker == state.last_blocker_class else 1
            )
        else:
            repeated_blocker = 0

        stop_reason = self._stop_reason(
            state=state,
            execution=execution,
            verification=verification,
            no_progress=no_progress,
            repeated_blocker=repeated_blocker,
        )
        verification_results = (*state.verification_results, verification)
        if stop_reason is not None:
            return replace(
                state,
                status="terminal",
                pending_result=None,
                verification_results=verification_results,
                no_progress_iterations=no_progress,
                repeated_blocker_count=repeated_blocker,
                last_blocker_class=blocker,
                stop_reason=stop_reason,
                final_action=verification.action,
                degraded=state.degraded or verification.action == "degrade",
            )

        active_revision = state.active_revision
        accepted_revisions = state.accepted_revisions
        if verification.proposed_revision is not None:
            active_revision = self._accept_revision(state, verification.proposed_revision)
            accepted_revisions = (*accepted_revisions, active_revision)

        policy = self.blueprint.session_policy
        token_rollover = state.session_token_usage >= policy.fresh_agent_token_threshold
        authored_fresh = policy.session_mode in {"fresh", "fresh_from_handoff"}
        fresh_session = token_rollover or authored_fresh
        next_session_mode: Literal["reuse", "fresh", "fresh_from_handoff"]
        if token_rollover:
            next_session_mode = policy.rollover_mode
        elif authored_fresh:
            next_session_mode = policy.session_mode
        else:
            next_session_mode = "reuse"
        workspace_generation = state.workspace_generation
        if self.blueprint.workspace_policy.workspace_mode != "shared":
            workspace_generation += 1
        return replace(
            state,
            status="ready",
            pending_result=None,
            verification_results=verification_results,
            no_progress_iterations=no_progress,
            repeated_blocker_count=repeated_blocker,
            last_blocker_class=blocker,
            active_revision=active_revision,
            accepted_revisions=accepted_revisions,
            next_goal_iteration=state.next_goal_iteration + 1,
            next_agent_run=state.next_agent_run + 1,
            session_generation=state.session_generation + (1 if fresh_session else 0),
            session_token_usage=0 if fresh_session else state.session_token_usage,
            workspace_generation=workspace_generation,
            rollover_count=state.rollover_count + (1 if token_rollover else 0),
            next_session_mode=next_session_mode,
            degraded=state.degraded or verification.action == "degrade",
        )

    def result(self, state: GoalDirectedExecutionState) -> GoalDirectedRunResult:
        if state.status != "terminal" or state.stop_reason is None or state.final_action is None:
            raise GoalDirectedExecutionError("GoalDirected result requires a terminal state")
        return GoalDirectedRunResult(
            run_id=state.run_id,
            execution_epoch=state.execution_epoch,
            status="terminal",
            stop_reason=state.stop_reason,
            final_action=state.final_action,
            goal_iterations=len(state.verification_results),
            agent_runs=len(state.execution_results),
            rollover_count=state.rollover_count,
            active_revision_id=state.active_revision.revision_id,
            accepted_revision_ids=tuple(
                revision.revision_id for revision in state.accepted_revisions
            ),
            output_refs=state.output_refs,
            handoff_checkpoints=state.handoff_checkpoints,
            execution_results=state.execution_results,
            verification_results=state.verification_results,
        )

    def _accept_revision(
        self,
        state: GoalDirectedExecutionState,
        revision: GoalRevision,
    ) -> GoalRevision:
        active = state.active_revision
        if revision.revision_id in {item.revision_id for item in state.accepted_revisions}:
            raise GoalDirectedExecutionError("Goal Revision identity is already accepted")
        if (
            revision.revision != active.revision + 1
            or revision.parent_revision_id != active.revision_id
        ):
            raise GoalDirectedExecutionError(
                "Goal Revision does not extend the active immutable revision"
            )
        if revision.protected_scope_digest != state.protected_scope_digest:
            raise GoalDirectedExecutionError(
                "Goal Revision attempts to change the launch-bound protected scope"
            )
        return revision

    def _validate_checkpoint(
        self,
        state: GoalDirectedExecutionState,
        result: GoalExecutionResult,
        checkpoint: GoalHandoffCheckpoint,
    ) -> None:
        if (
            checkpoint.agent_run_identity != result.identity
            or checkpoint.goal_revision_id != state.active_revision.revision_id
            or checkpoint.protected_scope_digest != state.protected_scope_digest
        ):
            raise GoalDirectedExecutionError(
                "goal handoff checkpoint does not match its run, revision, or protected scope"
            )

    def _stop_reason(
        self,
        *,
        state: GoalDirectedExecutionState,
        execution: GoalExecutionResult,
        verification: GoalVerificationResult,
        no_progress: int,
        repeated_blocker: int,
    ) -> GoalStopReason | None:
        # This ordering is domain behavior. Do not reorder it to mirror provider status.
        if execution.authority_breach_ref or verification.authority_breach_ref:
            return "authority_breach"
        if (
            execution.hard_budget_exhausted_dimensions
            or verification.hard_budget_exhausted_dimensions
        ):
            return "hard_budget_exhausted"
        if verification.action == "verified_completion":
            return "verified_completion"
        if execution.irrecoverable_failure_ref or verification.irrecoverable_failure_ref:
            return "irrecoverable_failure"
        convergence = self.blueprint.convergence_policy
        if no_progress >= convergence.max_no_progress_iterations:
            return "no_progress"
        if repeated_blocker >= convergence.max_repeated_blockers:
            return "repeated_blocker"
        if len(state.verification_results) + 1 >= self.blueprint.max_iterations:
            return "iteration_limit"
        if verification.action == "degrade":
            return "degraded"
        if verification.action == "stop":
            return "verifier_stop"
        if verification.action == "fork":
            return "fork_requested"
        if verification.action == "escalate":
            return "escalation_requested"
        return None


def _ordered_union(left: tuple[str, ...], right: tuple[str, ...]) -> tuple[str, ...]:
    seen = set(left)
    values = list(left)
    for item in right:
        if item in seen:
            continue
        seen.add(item)
        values.append(item)
    return tuple(values)
