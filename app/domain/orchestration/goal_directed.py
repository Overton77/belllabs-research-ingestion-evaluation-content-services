from __future__ import annotations

from dataclasses import asdict, replace
from typing import Literal, cast

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import GoalDirectedBlueprint
from app.domain.orchestration.contracts import (
    GoalAgentRunIdentity,
    GoalConvergenceAction,
    GoalConvergenceFacts,
    GoalConvergenceProposal,
    GoalConvergenceReason,
    GoalDirectedExecutionState,
    GoalDirectedRunInput,
    GoalDirectedRunResult,
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoff,
    GoalIterationIdentity,
    GoalRevision,
    GoalTerminalizationProposal,
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
        if (run_input.technical_segment == 1) != (
            run_input.continuation_state is None
        ):
            raise GoalDirectedExecutionError(
                "GoalDirected technical continuation state is inconsistent"
            )
        revision = run_input.initial_revision
        if revision.envelope_digest != run_input.envelope_digest:
            raise GoalDirectedExecutionError(
                "initial Goal Revision does not match the launch-bound objective envelope"
            )
        self._validate_revision_digest(revision)
        handoffs: tuple[GoalHandoff, ...] = ()
        if run_input.continuation_handoff is not None:
            handoff = run_input.continuation_handoff
            if (
                handoff.run_id != run_input.run_id
                or handoff.execution_epoch != run_input.execution_epoch
                or handoff.goal_revision_id != revision.revision_id
            ):
                raise GoalDirectedExecutionError(
                    "continuation handoff does not match the exact run revision"
                )
            handoffs = (handoff,)
        continuation = run_input.continuation_state
        if continuation is not None:
            if (
                revision != continuation.active_revision
                or continuation.active_revision.envelope_digest != run_input.envelope_digest
                or continuation.active_revision not in continuation.accepted_revisions
                or continuation.next_goal_iteration < 1
                or continuation.next_agent_run < 1
            ):
                raise GoalDirectedExecutionError("GoalDirected continuation state is inconsistent")
            for accepted_revision in continuation.accepted_revisions:
                self._validate_revision_digest(accepted_revision)
            return GoalDirectedExecutionState(
                run_id=run_input.run_id,
                execution_epoch=run_input.execution_epoch,
                envelope_digest=run_input.envelope_digest,
                active_revision=continuation.active_revision,
                accepted_revisions=continuation.accepted_revisions,
                next_goal_iteration=continuation.next_goal_iteration,
                next_agent_run=continuation.next_agent_run,
                session_generation=continuation.session_generation,
                session_token_usage=continuation.session_token_usage,
                workspace_generation=continuation.workspace_generation,
                handoffs=continuation.handoffs,
                output_refs=continuation.output_refs,
                no_progress_iterations=continuation.no_progress_iterations,
                repeated_blocker_count=continuation.repeated_blocker_count,
                last_blocker_class=continuation.last_blocker_class,
                rollover_count=continuation.rollover_count,
                next_session_mode=continuation.next_session_mode,
                request_scope=run_input.request_scope,
                semantic_input_binding_ref=run_input.semantic_input_binding_ref,
                effective_configuration_digest=run_input.effective_configuration_digest,
                blueprint_digest=run_input.blueprint_digest,
                completed_goal_iterations=continuation.completed_goal_iterations,
                completed_agent_runs=continuation.completed_agent_runs,
                lineage_digest=continuation.lineage_digest,
            )
        if revision.revision != 1 or revision.parent_revision_id is not None:
            raise GoalDirectedExecutionError("GoalDirected run requires an initial Goal Revision")
        return GoalDirectedExecutionState(
            run_id=run_input.run_id,
            execution_epoch=run_input.execution_epoch,
            envelope_digest=run_input.envelope_digest,
            active_revision=revision,
            accepted_revisions=(revision,),
            handoffs=handoffs,
            next_session_mode=self.blueprint.session_policy.session_mode,
            request_scope=run_input.request_scope,
            semantic_input_binding_ref=run_input.semantic_input_binding_ref,
            effective_configuration_digest=run_input.effective_configuration_digest,
            blueprint_digest=run_input.blueprint_digest,
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
        prior_checkpoint = state.handoffs[-1].handoff_id if state.handoffs else ""
        claim = GoalExecutionClaim(
            identity=identity,
            idempotency_key=f"goal-operation:{identity.semantic_key}",
            operation_class=selected_operation,
            objective=state.active_revision.objective,
            envelope_digest=state.envelope_digest,
            goal_revision_digest=state.active_revision.canonical_digest,
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
            prior_handoff_ref=prior_checkpoint,
            fresh_agent_token_threshold=policy.fresh_agent_token_threshold,
            handoff_token_reserve=policy.handoff_token_reserve,
            token_budget_remaining=work_remaining + policy.handoff_token_reserve,
            request_scope=state.request_scope,
            semantic_input_binding_ref=state.semantic_input_binding_ref,
            effective_configuration_digest=state.effective_configuration_digest,
            blueprint_digest=state.blueprint_digest,
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
        if (
            not result.operation_identity
            or not result.operation_binding_ref
            or result.session_id != claim.session_id
            or result.workspace_id != claim.workspace_namespace
            or not result.writable_paths
        ):
            raise GoalDirectedExecutionError(
                "executor result is not bound to its exact operation, session, and workspace"
            )
        if any(not dimension or amount < 0 for dimension, amount in result.actual_usage.items()):
            raise GoalDirectedExecutionError(
                "GoalDirected execution usage requires non-negative named dimensions"
            )
        handoff = result.handoff
        handoffs = state.handoffs
        if handoff is not None:
            self._validate_handoff(state, result, handoff)
            prior = next(
                (item for item in handoffs if item.handoff_id == handoff.handoff_id),
                None,
            )
            if prior is not None and prior != handoff:
                raise GoalDirectedExecutionError(
                    "goal handoff identity was reused with conflicting content"
                )
            if prior is None:
                handoffs = (*handoffs, handoff)
        return replace(
            state,
            status="awaiting_verification",
            active_claim=claim,
            pending_result=result,
            execution_results=(*state.execution_results, result),
            handoffs=handoffs,
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
        claim = state.active_claim
        if state.status != "awaiting_verification" or execution is None or claim is None:
            raise GoalDirectedExecutionError(
                "GoalDirected state has no execution awaiting independent verification"
            )
        verifier_policy = self.blueprint.verifier_policy
        if verification.executor_identity != execution.identity:
            raise GoalDirectedExecutionError(
                "verification does not match the active GoalDirected semantic identity"
            )
        if (
            verification.verifier_policy_binding_ref != verifier_policy.binding_ref
            or verification.rubric_ref != verifier_policy.rubric_ref
            or verification.rubric_version != verifier_policy.rubric_version
            or verification.acceptance_version != verifier_policy.acceptance_version
        ):
            raise GoalDirectedExecutionError(
                "GoalDirected verification is not bound to the frozen independent verifier"
            )
        if verification.acceptance_contract_ref != self.blueprint.acceptance_contract:
            raise GoalDirectedExecutionError(
                "GoalDirected verification is not bound to the frozen acceptance contract"
            )
        if (
            verification.verifier_operation_identity == execution.operation_identity
            or verification.verifier_binding_ref == execution.operation_binding_ref
            or verification.verifier_session_id == execution.session_id
            or verification.verifier_workspace_id == execution.workspace_id
            or any(
                _paths_overlap(executor_path, verifier_path)
                for executor_path in execution.writable_paths
                for verifier_path in verification.verifier_writable_paths
            )
        ):
            raise GoalDirectedExecutionError(
                "executor and verifier must use separate operation, binding, session, and workspace"
            )
        if verification.stale_frontier_digest == "":
            raise GoalDirectedExecutionError("verification requires an exact stale frontier")
        if (
            verification.admitted_executor_output_refs != execution.output_refs
            or verification.admitted_executor_evidence_refs != execution.evidence_refs
        ):
            raise GoalDirectedExecutionError(
                "verification is not bound to the admitted executor evidence frontier"
            )
        self._validate_verification_digest(verification)
        if (
            verification.decision == "revision_required"
            and verification.proposed_revision is None
        ):
            raise GoalDirectedExecutionError(
                "revision-required verification omitted its bounded Goal Revision"
            )

        no_progress = 0 if verification.progress_made else state.no_progress_iterations + 1
        blocker = verification.blocker_class or execution.blocker_class
        if blocker:
            repeated_blocker = (
                state.repeated_blocker_count + 1 if blocker == state.last_blocker_class else 1
            )
        else:
            repeated_blocker = 0

        required = self.blueprint.required_obligation_refs
        all_verified = (
            verification.decision == "accepted"
            and not verification.unmet_obligations
            and required <= set(verification.accepted_obligation_refs)
        )
        facts = GoalConvergenceFacts(
            authority_breach=bool(
                execution.authority_breach_ref or verification.authority_breach_ref
            ),
            hard_budget_exhausted=bool(
                execution.hard_budget_exhausted_dimensions
                or verification.hard_budget_exhausted_dimensions
            ),
            all_required_obligations_verified=all_verified,
            irrecoverable_failure=bool(
                execution.irrecoverable_failure_ref or verification.irrecoverable_failure_ref
            ),
            no_progress_threshold_reached=(
                no_progress >= self.blueprint.convergence_policy.max_no_progress_iterations
            ),
            repeated_blocker_threshold_reached=(
                repeated_blocker >= self.blueprint.convergence_policy.max_repeated_blockers
            ),
            iteration_limit_reached=(
                claim.identity.iteration.goal_iteration >= self.blueprint.max_iterations
            ),
            soft_budget_response_required=bool(verification.soft_budget_dimensions),
            bounded_revision=verification.proposed_revision,
            repair_requested=verification.decision == "repair_required",
            scope_expansion_route=verification.scope_expansion_route,
        )
        proposal = self.decide_convergence(state, verification, facts)
        verification_results = (*state.verification_results, verification)
        if proposal.action in {"complete", "partial_or_fail", "fail"}:
            terminalization = self._terminalization_proposal(
                state, execution, verification, proposal
            )
            return replace(
                state,
                status="stopping",
                active_claim=None,
                pending_result=None,
                verification_results=verification_results,
                no_progress_iterations=no_progress,
                repeated_blocker_count=repeated_blocker,
                last_blocker_class=blocker,
                convergence_proposal=proposal,
                terminalization_proposal=terminalization,
            )

        active_revision = state.active_revision
        accepted_revisions = state.accepted_revisions
        if proposal.action == "revise" and verification.proposed_revision is not None:
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
        if token_rollover and state.rollover_count >= policy.max_rollovers:
            raise GoalDirectedExecutionError("GoalDirected context rollover limit is exhausted")
        if fresh_session and not state.handoffs:
            raise GoalDirectedExecutionError("fresh GoalDirected session requires a typed handoff")
        if fresh_session and (
            execution.handoff is None
            or execution.handoff.source_iteration != execution.identity.iteration
        ):
            raise GoalDirectedExecutionError(
                "fresh GoalDirected session requires the current iteration handoff"
            )
        if (
            fresh_session
            and execution.handoff is not None
            and execution.handoff.compaction_status == "failed"
        ):
            compaction_action = policy.compaction_failure_action
            if compaction_action in {"retry", "fresh_from_handoff"}:
                raise GoalDirectedExecutionError(
                    "handoff compaction recovery was not reconciled before interpretation"
                )
            proposal = GoalConvergenceProposal(
                proposal_id=sha256_digest(
                    {
                        "run_id": state.run_id,
                        "iteration": execution.identity.iteration.semantic_key,
                        "verification": verification.verification_ref,
                        "compaction_failure": execution.handoff.compaction_failure_ref,
                        "action": compaction_action,
                    }
                ),
                action="pause" if compaction_action == "pause" else "escalate",
                reason="compaction_failure",
                goal_revision_id=state.active_revision.revision_id,
                source_iteration=execution.identity.iteration,
                verification_ref=verification.verification_ref,
                evidence_refs=(
                    *verification.evidence_refs,
                    execution.handoff.compaction_failure_ref,
                ),
                route_ref=execution.handoff.compaction_failure_ref,
            )
        workspace_generation = state.workspace_generation
        if self.blueprint.workspace_policy.workspace_mode != "shared":
            workspace_generation += 1
        next_status: Literal["ready", "paused", "stopping"]
        if proposal.action == "pause":
            next_status = "paused"
        elif proposal.action in {
            "escalate",
            "fork",
            "linked_run",
            "control_revision",
            "new_run",
        }:
            next_status = "stopping"
        else:
            next_status = "ready"
        return replace(
            state,
            status=next_status,
            active_claim=None,
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
            convergence_proposal=proposal,
        )

    def result(self, state: GoalDirectedExecutionState) -> GoalDirectedRunResult:
        if (
            state.status != "stopping"
            or state.convergence_proposal is None
        ):
            raise GoalDirectedExecutionError(
                "GoalDirected result requires a governed stopping or routing proposal"
            )
        return GoalDirectedRunResult(
            run_id=state.run_id,
            execution_epoch=state.execution_epoch,
            status="stopping",
            convergence_proposal=state.convergence_proposal,
            terminalization_proposal=state.terminalization_proposal,
            goal_iterations=(
                state.completed_goal_iterations + len(state.verification_results)
            ),
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
            lineage_digest=sha256_digest(
                {
                    "previous": state.lineage_digest,
                    "execution_results": tuple(
                        sha256_digest(asdict(item)) for item in state.execution_results
                    ),
                    "verification_results": tuple(
                        item.verification_digest for item in state.verification_results
                    ),
                }
            ),
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
        if revision.envelope_digest != state.envelope_digest:
            raise GoalDirectedExecutionError(
                "Goal Revision attempts to change the launch-bound objective envelope"
            )
        self._validate_revision_digest(revision)
        return revision

    def _validate_handoff(
        self,
        state: GoalDirectedExecutionState,
        result: GoalExecutionResult,
        handoff: GoalHandoff,
    ) -> None:
        if (
            handoff.source_iteration != result.identity.iteration
            or handoff.goal_revision_id != state.active_revision.revision_id
            or handoff.run_id != state.run_id
            or handoff.execution_epoch != state.execution_epoch
        ):
            raise GoalDirectedExecutionError(
                "goal handoff does not match its run, revision, or iteration"
            )
        handoff_payload = asdict(handoff)
        handoff_payload.pop("handoff_digest")
        expected_digest = sha256_digest(handoff_payload)
        if handoff.handoff_digest != expected_digest:
            raise GoalDirectedExecutionError("Goal handoff canonical digest mismatch")
        protected = dict(handoff.protected_context_facts)
        if set(protected) != self.blueprint.session_policy.protected_fact_classes:
            raise GoalDirectedExecutionError("goal handoff omits a protected context fact class")
        if (
            handoff.context_selection_policy_ref
            != self.blueprint.session_policy.context_selection_policy_ref
            or handoff.context_compaction_policy_ref
            != self.blueprint.session_policy.context_compaction_policy_ref
        ):
            raise GoalDirectedExecutionError("goal handoff policy binding mismatch")
        if (
            len(handoff.continuation_instructions.encode("utf-8"))
            > self.blueprint.handoff_policy.max_instruction_bytes
        ):
            raise GoalDirectedExecutionError("goal handoff instructions exceed policy limit")
        if not all(
            (
                handoff.accepted_fact_refs,
                handoff.evidence_refs,
                handoff.artifact_refs,
                handoff.context_selection_refs,
                handoff.compaction_decision_ref,
                handoff.workspace_refs,
                handoff.source_document_digests,
                handoff.source_binding_digests,
                handoff.continuation_instructions,
            )
        ):
            raise GoalDirectedExecutionError(
                "goal handoff omits required continuation evidence or context"
            )
        allowed_workspace_classes = self.blueprint.handoff_policy.allowed_workspace_ref_classes
        if any(
            _reference_class(reference) not in allowed_workspace_classes
            for reference in handoff.workspace_refs
        ):
            raise GoalDirectedExecutionError(
                "goal handoff workspace reference class is not allowed"
            )
        allowed_snapshot_classes = self.blueprint.handoff_policy.allowed_snapshot_ref_classes
        if any(
            _reference_class(reference) not in allowed_snapshot_classes
            for reference in handoff.snapshot_refs
        ):
            raise GoalDirectedExecutionError(
                "goal handoff snapshot reference class is not allowed"
            )
        if any(
            not _is_sha256_reference(digest)
            for digest in (
                *handoff.source_document_digests,
                *handoff.source_binding_digests,
            )
        ):
            raise GoalDirectedExecutionError(
                "goal handoff source digests are not canonical SHA-256 references"
            )

    def decide_convergence(
        self,
        state: GoalDirectedExecutionState,
        verification: GoalVerificationResult,
        facts: GoalConvergenceFacts,
    ) -> GoalConvergenceProposal:
        convergence = self.blueprint.convergence_policy
        action: GoalConvergenceAction
        reason: GoalConvergenceReason
        if facts.authority_breach:
            action = convergence.authority_breach_action
            reason = "authority_breach"
        elif facts.hard_budget_exhausted:
            action = convergence.hard_budget_action
            reason = "hard_budget_exhausted"
        elif facts.all_required_obligations_verified:
            action = "complete"
            reason = "verified_completion"
        elif facts.irrecoverable_failure:
            action = convergence.irrecoverable_failure_action
            reason = "irrecoverable_failure"
        elif facts.no_progress_threshold_reached:
            action = convergence.no_progress_action
            reason = "no_progress"
        elif facts.repeated_blocker_threshold_reached:
            action = convergence.repeated_blocker_action
            reason = "repeated_blocker"
        elif facts.iteration_limit_reached:
            action = convergence.iteration_limit_action
            reason = "iteration_limit"
        elif facts.soft_budget_response_required:
            action = convergence.soft_budget_action
            reason = "soft_budget_response"
        elif facts.scope_expansion_route is not None:
            action = facts.scope_expansion_route
            reason = "scope_expansion"
        elif facts.bounded_revision is not None:
            action = "revise"
            reason = "bounded_revision"
        elif facts.repair_requested:
            action = "repair"
            reason = "repair_requested"
        else:
            action = "continue"
            reason = "continue"
        return GoalConvergenceProposal(
            proposal_id=sha256_digest(
                {
                    "run_id": state.run_id,
                    "iteration": verification.executor_identity.iteration.semantic_key,
                    "verification": verification.verification_ref,
                    "action": action,
                    "reason": reason,
                }
            ),
            action=action,
            reason=reason,
            goal_revision_id=state.active_revision.revision_id,
            source_iteration=verification.executor_identity.iteration,
            verification_ref=verification.verification_ref,
            evidence_refs=verification.evidence_refs,
            route_ref=verification.route_ref,
        )

    def _terminalization_proposal(
        self,
        state: GoalDirectedExecutionState,
        execution: GoalExecutionResult,
        verification: GoalVerificationResult,
        convergence: GoalConvergenceProposal,
    ) -> GoalTerminalizationProposal:
        return GoalTerminalizationProposal(
            proposal_id=sha256_digest(
                {
                    "convergence_proposal_id": convergence.proposal_id,
                    "revision": state.active_revision.revision_id,
                    "verification": verification.verification_ref,
                }
            ),
            expected_run_version=0,
            goal_revision_id=state.active_revision.revision_id,
            verifier_decision_ref=verification.verification_ref,
            obligation_evidence_refs=verification.evidence_refs,
            output_refs=execution.output_refs,
            degradation_refs=(),
            blocker_refs=((verification.blocker_class,) if verification.blocker_class else ()),
            budget_state_digest=sha256_digest(
                {
                    "executor_usage": execution.actual_usage,
                    "verifier_usage": verification.actual_usage,
                }
            ),
            effect_frontier_digest=sha256_digest(execution.effect_frontier_refs),
            stale_frontier_digest=verification.stale_frontier_digest,
            effects_settled=not bool(
                execution.effect_frontier_refs
                or execution.pending_liability_refs
                or verification.effect_refs
            ),
            proposed_outcome=cast(
                Literal["complete", "partial_or_fail", "fail"],
                convergence.action,
            ),
        )

    @staticmethod
    def _validate_verification_digest(verification: GoalVerificationResult) -> None:
        verification_payload = asdict(verification)
        verification_payload.pop("verification_digest")
        expected = sha256_digest(verification_payload)
        if verification.verification_digest != expected:
            raise GoalDirectedExecutionError("Goal verification canonical digest mismatch")

    @staticmethod
    def _validate_revision_digest(revision: GoalRevision) -> None:
        expected = sha256_digest(
            {
                "schema_version": revision.schema_version,
                "revision_id": revision.revision_id,
                "revision": revision.revision,
                "parent_revision_id": revision.parent_revision_id,
                "envelope_digest": revision.envelope_digest,
                "objective": revision.objective,
                "tactical_changes": revision.tactical_changes,
                "evidence_refs": revision.evidence_refs,
                "unmet_obligations": revision.unmet_obligations,
                "proposer": revision.proposer,
                "deciding_authority": revision.deciding_authority,
                "applicability": revision.applicability,
                "tactics": revision.tactics,
                "subgoals": revision.subgoals,
                "coverage_emphasis": revision.coverage_emphasis,
            }
        )
        if revision.canonical_digest != expected:
            raise GoalDirectedExecutionError("Goal Revision canonical digest mismatch")


def _ordered_union(left: tuple[str, ...], right: tuple[str, ...]) -> tuple[str, ...]:
    seen = set(left)
    values = list(left)
    for item in right:
        if item in seen:
            continue
        seen.add(item)
        values.append(item)
    return tuple(values)


def _paths_overlap(left: str, right: str) -> bool:
    normalized_left = left.rstrip("/")
    normalized_right = right.rstrip("/")
    return (
        normalized_left == normalized_right
        or normalized_left.startswith(normalized_right + "/")
        or normalized_right.startswith(normalized_left + "/")
    )


def _reference_class(reference: str) -> str:
    return reference.partition(":")[0]


def _is_sha256_reference(reference: str) -> bool:
    prefix, separator, digest = reference.partition(":")
    return (
        prefix == "sha256"
        and separator == ":"
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
    )
