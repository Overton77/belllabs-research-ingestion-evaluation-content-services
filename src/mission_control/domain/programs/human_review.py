"""GoalDirected human review: a bounded review control action after verified completion.

A Goal Loop whose acceptance includes ``acceptance.human`` (or whose launch declares a review
gate) opens one Human Gate activation on its verified terminal outputs before it proposes
terminalization. Notification delivery never satisfies it; only an attributed resolution does
(SPEC-03 "Explicit Human Gate"). ``request_changes`` continues the loop at the next iteration
with the reviewer feedback as an untrusted instruction for the executor; the goal revision,
acceptance contract and obligations are untouched and every counter carries on, so the run's
budget is never reset.
"""

from __future__ import annotations

from dataclasses import replace

from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.programs.contracts import (
    GoalConvergenceProposal,
    GoalDirectedExecutionState,
)
from mission_control.domain.programs.human_gate import (
    HumanGateBindingError,
    HumanGateOutcome,
    ReviewPacketItem,
    ordered_packet,
    packet_item,
)


def goal_review_packet(state: GoalDirectedExecutionState) -> tuple[ReviewPacketItem, ...]:
    """The verified terminal outputs and the verifier decision the human reviews."""

    proposal = state.terminalization_proposal
    if proposal is None or proposal.proposed_outcome != "complete":
        raise HumanGateBindingError("a goal review needs a verified completion proposal")
    items = [packet_item("goal.output", ref) for ref in proposal.output_refs]
    items.append(packet_item("goal.verification", proposal.verifier_decision_ref))
    return ordered_packet(tuple(items))


def apply_goal_review(
    state: GoalDirectedExecutionState,
    outcome: HumanGateOutcome,
    *,
    max_iterations: int,
    fresh_workspace_per_iteration: bool,
) -> GoalDirectedExecutionState:
    """Apply one review outcome to a verified-complete GoalDirected state.

    - accepted: unchanged; the run terminalizes on the verified proposal.
    - changes_requested: the loop continues at the next iteration with the same goal
      revision, acceptance contract and obligations; iteration, agent-run, no-progress
      and blocker counters carry on (the budget is never reset). With no iteration left
      the review cannot be remediated and the run fails not accepted.
    - anything else: the verified completion is not accepted (`fail`, no valid outputs).
    """

    proposal = state.terminalization_proposal
    convergence = state.convergence_proposal
    if (
        state.status != "stopping"
        or proposal is None
        or proposal.proposed_outcome != "complete"
        or convergence is None
    ):
        raise HumanGateBindingError("a goal review applies to a verified completion only")
    if outcome.status == "accepted":
        return state
    iteration = convergence.source_iteration
    if outcome.status == "changes_requested" and iteration.goal_iteration < max_iterations:
        return replace(
            state,
            status="ready",
            terminalization_proposal=None,
            convergence_proposal=GoalConvergenceProposal(
                proposal_id=sha256_digest(
                    {"review": outcome.resolution_ref, "proposal": convergence.proposal_id}
                ),
                action="repair",
                reason="human_review_changes_requested",
                goal_revision_id=convergence.goal_revision_id,
                source_iteration=iteration,
                verification_ref=convergence.verification_ref,
                evidence_refs=(*convergence.evidence_refs, str(outcome.resolution_ref)),
                route_ref=str(outcome.remediation_target or "goal/executor"),
            ),
            next_goal_iteration=state.next_goal_iteration + 1,
            next_agent_run=state.next_agent_run + 1,
            workspace_generation=(
                state.workspace_generation + 1
                if fresh_workspace_per_iteration
                else state.workspace_generation
            ),
        )
    rejection_ref = outcome.resolution_ref or f"human-task:{outcome.human_task_id}:{outcome.status}"
    return replace(
        state,
        convergence_proposal=GoalConvergenceProposal(
            proposal_id=sha256_digest(
                {"review": rejection_ref, "proposal": convergence.proposal_id}
            ),
            action="fail",
            reason="human_review_rejected",
            goal_revision_id=convergence.goal_revision_id,
            source_iteration=iteration,
            verification_ref=convergence.verification_ref,
            evidence_refs=(*convergence.evidence_refs, rejection_ref),
        ),
        terminalization_proposal=replace(
            proposal,
            proposal_id=sha256_digest({"review": rejection_ref, "proposal": proposal.proposal_id}),
            verifier_decision_ref=rejection_ref,
            output_refs=(),
            blocker_refs=(*proposal.blocker_refs, rejection_ref),
            proposed_outcome="fail",
        ),
    )


__all__ = ["apply_goal_review", "goal_review_packet"]
