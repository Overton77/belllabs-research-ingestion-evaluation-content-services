"""MP-10 GoalDirected review: feedback reaches the next executor without changing criteria
or resetting budgets; a rejection is never an acceptance."""

from __future__ import annotations

import pytest

from mission_control.application.programs.goal_directed import _instantiate_operation_request
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.execution.contracts import (
    OperationExecutionRequest,
    PromptTrustClass,
)
from mission_control.domain.programs.goal_directed_runtime import GoalOperationPreparationRequest
from mission_control.domain.programs.human_gate import (
    HumanGateBindingError,
    HumanGateOutcome,
    review_packet_digest,
)
from mission_control.domain.programs.human_review import apply_goal_review, goal_review_packet
from tests.fixtures.goal_directed_journaled import goal_template_workspace
from tests.unit.operations.test_operation_execution import operation_request
from tests.unit.orchestration.test_rrm_016_goal_directed_settlement import _preparation
from tests.unit.orchestration.test_wp_bp_020_goal_directed import (
    _blueprint,
    _claim,
    _execution,
    _verification,
)


def _verified(max_iterations: int = 3):  # type: ignore[no-untyped-def]
    configured = _blueprint(max_iterations=max_iterations)
    interpreter, state, claim = _claim(configured)
    state = interpreter.apply_execution_result(state, _execution(claim))
    state = interpreter.apply_verification(
        state, _verification(claim, decision="accepted", accepted=True)
    )
    assert state.status == "stopping"
    assert state.terminalization_proposal is not None
    assert state.terminalization_proposal.proposed_outcome == "complete"
    return configured, interpreter, state


def _outcome(state, status: str, decision: str | None) -> HumanGateOutcome:  # type: ignore[no-untyped-def]
    return HumanGateOutcome(
        human_task_id="00000000-0000-0000-0000-000000000001",
        task_key="human_gate:run-goal:goal-review:goal:epoch:1:iteration:1:round:1",
        gate_key="goal-review",
        review_round=1,
        status=status,  # type: ignore[arg-type]
        decision=decision,  # type: ignore[arg-type]
        actor_ref="owner",
        resolution_ref="human-resolution:task:sha256:" + "1" * 64,
        packet_digest=review_packet_digest(goal_review_packet(state)),
        comment="Add the missing cohort." if status == "changes_requested" else None,
        remediation_target="goal/executor",
    )


def test_the_review_packet_is_the_verified_outputs_and_verifier_decision() -> None:
    _configured, _interpreter, state = _verified()
    sources = {item.source for item in goal_review_packet(state)}
    assert sources == {"goal.output", "goal.verification"}
    assert {item.ref for item in goal_review_packet(state)} >= {"artifact:result"}


def test_approval_leaves_the_verified_completion_unchanged() -> None:
    configured, _interpreter, state = _verified()
    reviewed = apply_goal_review(
        state,
        _outcome(state, "accepted", "approve"),
        max_iterations=configured.max_iterations,
        fresh_workspace_per_iteration=False,
    )
    assert reviewed is state


def test_changes_continue_the_loop_without_new_criteria_or_reset_counters() -> None:
    configured, interpreter, state = _verified()
    reviewed = apply_goal_review(
        state,
        _outcome(state, "changes_requested", "request_changes"),
        max_iterations=configured.max_iterations,
        fresh_workspace_per_iteration=False,
    )
    assert reviewed.status == "ready"
    assert reviewed.terminalization_proposal is None
    # Criteria and authority: the same goal revision under the same frozen blueprint.
    assert reviewed.active_revision == state.active_revision
    assert reviewed.accepted_revisions == state.accepted_revisions
    # Budget/counters carry on: the next iteration is 2, history is kept, nothing reset.
    assert reviewed.next_goal_iteration == state.next_goal_iteration + 1
    assert reviewed.next_agent_run == state.next_agent_run + 1
    assert reviewed.verification_results == state.verification_results
    assert reviewed.execution_results == state.execution_results
    assert reviewed.no_progress_iterations == state.no_progress_iterations
    assert reviewed.session_token_usage == state.session_token_usage
    assert reviewed.convergence_proposal is not None
    assert reviewed.convergence_proposal.reason == "human_review_changes_requested"
    _claimed, claim = interpreter.claim_execution(reviewed)
    assert claim.identity.iteration.goal_iteration == 2
    assert claim.goal_revision_digest == state.active_revision.canonical_digest


def test_changes_on_the_last_iteration_fail_rather_than_extend_the_budget() -> None:
    configured, _interpreter, state = _verified(max_iterations=1)
    reviewed = apply_goal_review(
        state,
        _outcome(state, "changes_requested", "request_changes"),
        max_iterations=configured.max_iterations,
        fresh_workspace_per_iteration=False,
    )
    assert reviewed.status == "stopping"
    assert reviewed.terminalization_proposal is not None
    assert reviewed.terminalization_proposal.proposed_outcome == "fail"
    assert reviewed.next_goal_iteration == state.next_goal_iteration


@pytest.mark.parametrize("status", ["not_accepted", "stopped_by_policy", "cancelled"])
def test_anything_but_approval_fails_the_verified_completion(status: str) -> None:
    configured, _interpreter, state = _verified()
    reviewed = apply_goal_review(
        state,
        _outcome(state, status, "deny" if status == "not_accepted" else None),
        max_iterations=configured.max_iterations,
        fresh_workspace_per_iteration=False,
    )
    proposal = reviewed.terminalization_proposal
    assert proposal is not None and proposal.proposed_outcome == "fail"
    assert proposal.output_refs == ()
    assert proposal.verifier_decision_ref.startswith("human-resolution:")
    assert reviewed.convergence_proposal is not None
    assert reviewed.convergence_proposal.reason == "human_review_rejected"


def test_review_applies_to_a_verified_completion_only() -> None:
    configured, interpreter, state = _verified()
    claimed, _ = interpreter.claim_execution(
        apply_goal_review(
            state,
            _outcome(state, "changes_requested", "request_changes"),
            max_iterations=configured.max_iterations,
            fresh_workspace_per_iteration=False,
        )
    )
    with pytest.raises(HumanGateBindingError):
        apply_goal_review(
            claimed,
            _outcome(state, "accepted", "approve"),
            max_iterations=3,
            fresh_workspace_per_iteration=False,
        )


def test_feedback_reaches_the_executor_prompt_as_untrusted_content_only() -> None:
    base_template = operation_request(prompt="MP-10 fixture GoalDirected executor.")
    template = OperationExecutionRequest.model_validate(
        {
            **base_template.model_dump(mode="python"),
            "workspace": goal_template_workspace(base_template.workspace),
        }
    )
    _configured, interpreter, state = _verified()
    outcome = _outcome(state, "changes_requested", "request_changes")
    reviewed = apply_goal_review(
        state, outcome, max_iterations=3, fresh_workspace_per_iteration=False
    )
    _claimed, claim = interpreter.claim_execution(reviewed)
    base = _preparation("run-goal", claim, "executor", 4, 2)
    request = GoalOperationPreparationRequest.model_validate(
        {**base.model_dump(mode="python"), "review_feedback": (outcome,)}
    )
    plain = _instantiate_operation_request(template, base, base.expected_run_version + 1)
    with_feedback = _instantiate_operation_request(
        template, request, request.expected_run_version + 1
    )
    added = with_feedback.prompt_segments[len(plain.prompt_segments) :]
    assert len(added) == 1
    assert added[0].trust_class == PromptTrustClass.UNTRUSTED_CONTENT
    assert "Add the missing cohort." in added[0].content
    assert added[0].rendered_digest == sha256_digest(added[0].content)
    # Feedback changes the instruction only: same reservation, limits and grants.
    assert with_feedback.budget_limits == plain.budget_limits
    assert with_feedback.budget_reservation_id == plain.budget_reservation_id
    assert with_feedback.capability_grant == plain.capability_grant
    # The verifier never receives the reviewer's instruction.
    verifier = GoalOperationPreparationRequest.model_validate(
        {
            **request.model_dump(mode="python"),
            "operation_role": "verifier",
            "read_workspace_id": request.workspace_id,
            "workspace_id": f"{request.workspace_id}:verifier",
            "session_id": f"{request.session_id}:verifier",
            "verifier_input_refs": ("artifact:result",),
        }
    )
    assert not any(
        segment.source_ref.startswith("human-review:")
        for segment in _instantiate_operation_request(template, verifier, 5).prompt_segments
    )
    # An empty feedback tuple is absent from the request dump (digest-neutral).
    assert "review_feedback" not in base.model_dump(mode="json")
