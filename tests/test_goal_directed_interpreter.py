from __future__ import annotations

from dataclasses import replace

import pytest
from pydantic import ValidationError

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    GoalConvergencePolicy,
    GoalDirectedBlueprint,
    GoalSessionRolloverPolicy,
    GoalWorkspaceSnapshotPolicy,
)
from app.domain.orchestration.contracts import (
    GoalAgentRunIdentity,
    GoalDirectedExecutionState,
    GoalDirectedRunInput,
    GoalExecutionResult,
    GoalHandoffCheckpoint,
    GoalRevision,
    GoalVerificationResult,
)
from app.domain.orchestration.goal_directed import (
    GoalDirectedExecutionError,
    GoalDirectedInterpreter,
)

SCOPE_DIGEST = sha256_digest("fixed-goal-scope")


def blueprint(
    *,
    max_iterations: int = 4,
    token_threshold: int = 10,
    handoff_reserve: int = 3,
    no_progress: int = 3,
    repeated_blocker: int = 3,
    workspace_mode: str = "shared",
) -> GoalDirectedBlueprint:
    return GoalDirectedBlueprint(
        logical_id="fixture.goal-directed",
        title="GoalDirected fixture",
        description="Bounded interpreter fixture",
        objective_contract="objective:fixture@1",
        acceptance_contract="acceptance:fixture@1",
        independent_verifier_ref="verifier:fixture@1",
        allowed_operation_classes=frozenset({"research"}),
        session_policy=GoalSessionRolloverPolicy(
            session_mode="reuse",
            fresh_agent_token_threshold=token_threshold,
            handoff_token_reserve=handoff_reserve,
        ),
        workspace_policy=GoalWorkspaceSnapshotPolicy(
            workspace_mode=workspace_mode,  # type: ignore[arg-type]
        ),
        convergence_policy=GoalConvergencePolicy(
            max_no_progress_iterations=no_progress,
            max_repeated_blockers=repeated_blocker,
        ),
        max_iterations=max_iterations,
    )


def initial_revision() -> GoalRevision:
    return GoalRevision(
        revision_id="goal-revision:1",
        revision=1,
        parent_revision_id=None,
        protected_scope_digest=SCOPE_DIGEST,
        objective="Find and verify every company currently run by the subject.",
        evidence_refs=("input:goal",),
        unmet_obligations=("company-coverage",),
        author="workflow-owner",
        deciding_authority="authority:goal-owner",
        applicability="remaining_run",
    )


def initial_state(
    configured: GoalDirectedBlueprint,
) -> tuple[GoalDirectedInterpreter, GoalDirectedExecutionState]:
    interpreter = GoalDirectedInterpreter(configured)
    run_input = GoalDirectedRunInput(
        run_id="run-goal",
        request_scope="tenant-1",
        effective_configuration_digest=sha256_digest("erc"),
        blueprint_digest=sha256_digest(configured),
        blueprint=configured.model_dump(mode="json"),
        protected_scope_digest=SCOPE_DIGEST,
        initial_revision=initial_revision(),
    )
    return interpreter, interpreter.initial_state(run_input)


def execution(
    identity: GoalAgentRunIdentity,
    *,
    tokens: int = 1,
    completion_claim: bool = False,
    blocker_class: str = "",
    authority_breach_ref: str = "",
    hard_budget: tuple[str, ...] = (),
    irrecoverable_failure_ref: str = "",
    checkpoint: GoalHandoffCheckpoint | None = None,
) -> GoalExecutionResult:
    return GoalExecutionResult(
        identity=identity,
        disposition="completed",
        output_refs=(f"artifact:{identity.agent_run}",),
        completion_claim=completion_claim,
        actual_usage={"tokens.total": tokens},
        blocker_class=blocker_class,
        authority_breach_ref=authority_breach_ref,
        hard_budget_exhausted_dimensions=hard_budget,
        irrecoverable_failure_ref=irrecoverable_failure_ref,
        handoff_checkpoint=checkpoint,
    )


def verification(
    identity: GoalAgentRunIdentity,
    *,
    action: str = "continue",
    progress: bool = True,
    blocker_class: str = "",
    authority_breach_ref: str = "",
    hard_budget: tuple[str, ...] = (),
    irrecoverable_failure_ref: str = "",
    revision: GoalRevision | None = None,
) -> GoalVerificationResult:
    return GoalVerificationResult(
        identity=identity,
        action=action,  # type: ignore[arg-type]
        verification_ref=f"verification:{identity.agent_run}",
        verifier_ref="verifier:fixture@1",
        acceptance_contract_ref="acceptance:fixture@1",
        progress_made=progress,
        blocker_class=blocker_class,
        authority_breach_ref=authority_breach_ref,
        hard_budget_exhausted_dimensions=hard_budget,
        irrecoverable_failure_ref=irrecoverable_failure_ref,
        proposed_revision=revision,
    )


def test_blueprint_defaults_keep_existing_goal_fixtures_valid_and_bounded() -> None:
    configured = GoalDirectedBlueprint(
        logical_id="generic.goal",
        title="Generic goal",
        description="Existing construction remains valid",
        objective_contract="objective:generic@1",
        acceptance_contract="acceptance:generic@1",
        max_iterations=2,
    )

    assert configured.independent_verification_required is True
    assert configured.session_policy.session_mode == "reuse"
    assert configured.workspace_policy.workspace_mode == "shared"
    assert configured.iteration_reservation == {"goal.iterations": 1}
    assert configured.protected_scope_policy.protected_fields == {
        "objective",
        "acceptance",
        "invariants",
        "admitted_inputs",
        "authority",
        "budget",
        "prohibited_work",
    }

    with pytest.raises(ValidationError, match="goal.iterations"):
        configured.model_copy(
            update={"iteration_reservation": {}},
        ).__class__.model_validate(
            {**configured.model_dump(mode="json"), "iteration_reservation": {}}
        )


def test_token_threshold_rolls_to_fresh_agent_with_handoff_in_shared_workspace() -> None:
    interpreter, state = initial_state(blueprint())
    state, claim = interpreter.claim_execution(state, operation_class="research")
    checkpoint = GoalHandoffCheckpoint(
        checkpoint_id="handoff:1",
        agent_run_identity=claim.identity,
        goal_revision_id="goal-revision:1",
        protected_scope_digest=SCOPE_DIGEST,
        instructions="Continue by checking corporate ownership and current operating status.",
        state_refs=("state:coverage-ledger",),
        artifact_refs=("artifact:1",),
        workspace_ref=claim.workspace_namespace,
    )
    state = interpreter.apply_execution_result(
        state,
        execution(claim.identity, tokens=10, checkpoint=checkpoint),
    )
    state = interpreter.apply_verification(state, verification(claim.identity))

    assert state.status == "ready"
    assert state.rollover_count == 1
    assert state.session_generation == 2
    assert state.session_token_usage == 0
    assert state.next_session_mode == "fresh_from_handoff"
    assert state.workspace_generation == 1

    state, next_claim = interpreter.claim_execution(state)
    assert next_claim.session_mode == "fresh_from_handoff"
    assert next_claim.prior_checkpoint_id == checkpoint.checkpoint_id
    assert next_claim.workspace_namespace == claim.workspace_namespace
    assert next_claim.identity.iteration.goal_iteration == 2


def test_agent_completion_claim_never_bypasses_independent_verification() -> None:
    interpreter, state = initial_state(blueprint())
    state, claim = interpreter.claim_execution(state)
    state = interpreter.apply_execution_result(
        state,
        execution(claim.identity, completion_claim=True),
    )
    state = interpreter.apply_verification(
        state,
        verification(claim.identity, action="continue", progress=True),
    )
    assert state.status == "ready"
    assert state.stop_reason is None

    state, claim = interpreter.claim_execution(state)
    state = interpreter.apply_execution_result(state, execution(claim.identity))
    state = interpreter.apply_verification(
        state,
        verification(claim.identity, action="verified_completion", progress=True),
    )
    result = interpreter.result(state)
    assert result.stop_reason == "verified_completion"
    assert result.goal_iterations == 2


@pytest.mark.parametrize(
    (
        "action",
        "authority_breach",
        "hard_budget",
        "irrecoverable",
        "progress",
        "blocker",
        "no_progress_limit",
        "blocker_limit",
        "max_iterations",
        "expected",
    ),
    [
        (
            "verified_completion",
            "authority:breach",
            ("tokens.total",),
            "failure:irrecoverable",
            False,
            "source-blocked",
            1,
            1,
            1,
            "authority_breach",
        ),
        (
            "verified_completion",
            "",
            ("tokens.total",),
            "failure:irrecoverable",
            False,
            "source-blocked",
            1,
            1,
            1,
            "hard_budget_exhausted",
        ),
        (
            "verified_completion",
            "",
            (),
            "failure:irrecoverable",
            False,
            "source-blocked",
            1,
            1,
            1,
            "verified_completion",
        ),
        (
            "continue",
            "",
            (),
            "failure:irrecoverable",
            False,
            "source-blocked",
            1,
            1,
            1,
            "irrecoverable_failure",
        ),
        ("continue", "", (), "", False, "", 1, 3, 1, "no_progress"),
        (
            "continue",
            "",
            (),
            "",
            True,
            "source-blocked",
            3,
            1,
            1,
            "repeated_blocker",
        ),
        ("continue", "", (), "", True, "", 3, 3, 1, "iteration_limit"),
        ("continue", "", (), "", True, "", 3, 3, 2, None),
    ],
)
def test_stop_precedence_is_deterministic(
    action: str,
    authority_breach: str,
    hard_budget: tuple[str, ...],
    irrecoverable: str,
    progress: bool,
    blocker: str,
    no_progress_limit: int,
    blocker_limit: int,
    max_iterations: int,
    expected: str | None,
) -> None:
    configured = blueprint(
        max_iterations=max_iterations,
        no_progress=no_progress_limit,
        repeated_blocker=blocker_limit,
    )
    interpreter, state = initial_state(configured)
    state, claim = interpreter.claim_execution(state)
    state = interpreter.apply_execution_result(
        state,
        execution(
            claim.identity,
            authority_breach_ref=authority_breach,
            hard_budget=hard_budget,
            irrecoverable_failure_ref=irrecoverable,
            blocker_class=blocker,
        ),
    )
    state = interpreter.apply_verification(
        state,
        verification(
            claim.identity,
            action=action,
            progress=progress,
            blocker_class=blocker,
        ),
    )

    if expected is None:
        assert state.status == "ready"
        assert state.stop_reason is None
    else:
        assert state.status == "terminal"
        assert state.stop_reason == expected


def test_goal_revision_requires_exact_parent_and_protected_scope() -> None:
    interpreter, state = initial_state(blueprint())
    state, claim = interpreter.claim_execution(state)
    state = interpreter.apply_execution_result(state, execution(claim.identity))
    accepted = GoalRevision(
        revision_id="goal-revision:2",
        revision=2,
        parent_revision_id="goal-revision:1",
        protected_scope_digest=SCOPE_DIGEST,
        objective="Verify every candidate and resolve remaining operating-status uncertainty.",
        evidence_refs=("verification:1",),
        unmet_obligations=("operating-status",),
        author="goal-agent",
        deciding_authority="authority:independent-verifier",
        applicability="next_iteration",
        tactics=("triangulate official sites and corporate registries",),
    )
    state = interpreter.apply_verification(
        state,
        verification(claim.identity, action="repair", revision=accepted),
    )
    assert state.active_revision == accepted
    assert tuple(item.revision_id for item in state.accepted_revisions) == (
        "goal-revision:1",
        "goal-revision:2",
    )

    state, claim = interpreter.claim_execution(state)
    state = interpreter.apply_execution_result(state, execution(claim.identity))
    expansion = replace(
        accepted,
        revision_id="goal-revision:3",
        revision=3,
        parent_revision_id="goal-revision:2",
        protected_scope_digest=sha256_digest("broader-scope"),
    )
    with pytest.raises(GoalDirectedExecutionError, match="protected scope"):
        interpreter.apply_verification(
            state,
            verification(claim.identity, action="repair", revision=expansion),
        )


def test_execution_and_verification_require_exact_semantic_identity_and_contracts() -> None:
    interpreter, state = initial_state(blueprint())
    state, claim = interpreter.claim_execution(state)
    wrong_identity = replace(claim.identity, agent_run=claim.identity.agent_run + 1)
    with pytest.raises(GoalDirectedExecutionError, match="semantic identity"):
        interpreter.apply_execution_result(state, execution(wrong_identity))

    state = interpreter.apply_execution_result(state, execution(claim.identity))
    with pytest.raises(GoalDirectedExecutionError, match="independent verifier"):
        interpreter.apply_verification(
            state,
            replace(verification(claim.identity), verifier_ref="verifier:self"),
        )
