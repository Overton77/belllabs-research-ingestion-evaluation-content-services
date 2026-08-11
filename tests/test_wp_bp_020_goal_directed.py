from __future__ import annotations

from dataclasses import asdict, replace

import pytest

from app.application.goal_directed import _recover_handoff_compaction
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    GoalDirectedBlueprint,
    GoalSessionRolloverPolicy,
)
from app.domain.control_plane.fixtures import GENERIC_GOAL_DIRECTED
from app.domain.operation_execution.delegation import AsyncDelegationBoundary
from app.domain.orchestration.contracts import (
    GoalContinuationState,
    GoalConvergenceFacts,
    GoalDirectedExecutionState,
    GoalDirectedRunInput,
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoff,
    GoalRevision,
    GoalVerificationResult,
)
from app.domain.orchestration.goal_directed import (
    GoalDirectedExecutionError,
    GoalDirectedInterpreter,
)
from app.domain.orchestration.goal_directed_runtime import route_goal_async_subgoal

ENVELOPE_DIGEST = sha256_digest("wp-bp-020-envelope")
STALE_FRONTIER_DIGEST = sha256_digest("wp-bp-020-stale-frontier")


def _blueprint(**updates: object) -> GoalDirectedBlueprint:
    payload = GENERIC_GOAL_DIRECTED.model_dump(mode="python")
    payload.update(updates)
    return GoalDirectedBlueprint.model_validate(payload)


def _revision(
    *,
    revision: int = 1,
    parent_revision_id: str | None = None,
    envelope_digest: str = ENVELOPE_DIGEST,
) -> GoalRevision:
    values = {
        "schema_version": "belllabs.goal-revision.v1",
        "revision_id": f"goal-revision:{revision}",
        "revision": revision,
        "parent_revision_id": parent_revision_id,
        "envelope_digest": envelope_digest,
        "objective": "Produce one independently verified bounded result.",
        "tactical_changes": (() if revision == 1 else ("triangulate another source",)),
        "evidence_refs": ("input:goal",),
        "unmet_obligations": ("fixture-obligation",),
        "proposer": "application:test",
        "deciding_authority": "authority:test",
        "applicability": "remaining_run",
        "tactics": (),
        "subgoals": (),
        "coverage_emphasis": (),
    }
    return GoalRevision(canonical_digest=sha256_digest(values), **values)  # type: ignore[arg-type]


def _run_input(
    configured: GoalDirectedBlueprint,
    *,
    revision: GoalRevision | None = None,
    continuation: GoalContinuationState | None = None,
) -> GoalDirectedRunInput:
    active_revision = revision or _revision()
    return GoalDirectedRunInput(
        run_id="run-goal",
        request_scope="tenant-1",
        effective_configuration_digest=sha256_digest("erc"),
        blueprint_digest=sha256_digest(configured),
        blueprint=configured.model_dump(mode="json"),
        envelope_digest=ENVELOPE_DIGEST,
        initial_revision=active_revision,
        required_obligation_refs=("fixture-obligation",),
        required_output_contract_refs=("fixture-output",),
        semantic_input_binding_ref="semantic-input:test",
        technical_segment=2 if continuation is not None else 1,
        continuation_state=continuation,
    )


def _claim(
    configured: GoalDirectedBlueprint,
) -> tuple[GoalDirectedInterpreter, GoalDirectedExecutionState, GoalExecutionClaim]:
    interpreter = GoalDirectedInterpreter(configured)
    state = interpreter.initial_state(_run_input(configured))
    claimed, claim = interpreter.claim_execution(state)
    return interpreter, claimed, claim


def _handoff(claim: GoalExecutionClaim) -> GoalHandoff:
    draft = GoalHandoff(
        schema_version="belllabs.goal-handoff.v1",
        handoff_id=f"handoff:{claim.identity.iteration.semantic_key}",
        handoff_digest="pending",
        run_id=claim.identity.iteration.run_id,
        execution_epoch=claim.identity.iteration.execution_epoch,
        goal_revision_id=claim.identity.iteration.goal_revision_id,
        source_iteration=claim.identity.iteration,
        accepted_fact_refs=("fact:accepted",),
        evidence_refs=("evidence:accepted",),
        artifact_refs=("artifact:result",),
        unresolved_obligations=("fixture-obligation",),
        remaining_budget={"goal.iterations": 1},
        remaining_iterations=1,
        protected_context_facts=(("objective", claim.objective),),
        context_selection_policy_ref="context-selection:fixture@1",
        context_compaction_policy_ref="context-compaction:fixture@1",
        context_selection_refs=("context-selection:fixture@1",),
        compaction_decision_ref="compaction:accepted",
        workspace_refs=(f"fixture-workspace:{claim.workspace_namespace}",),
        source_document_digests=(sha256_digest("document"),),
        source_binding_digests=(claim.goal_revision_digest,),
        continuation_instructions="Continue from accepted facts and resolve the obligation.",
    )
    payload = asdict(draft)
    payload.pop("handoff_digest")
    return replace(draft, handoff_digest=sha256_digest(payload))


def _failed_handoff(claim: GoalExecutionClaim) -> GoalHandoff:
    draft = replace(
        _handoff(claim),
        handoff_digest="pending",
        compaction_status="failed",
        compaction_failure_ref="compaction-failure:fixture",
    )
    payload = asdict(draft)
    payload.pop("handoff_digest")
    return replace(draft, handoff_digest=sha256_digest(payload))


def _execution(
    claim: GoalExecutionClaim,
    *,
    handoff: GoalHandoff | None = None,
    tokens: int = 0,
    authority_breach: bool = False,
    hard_budget: bool = False,
    irrecoverable: bool = False,
) -> GoalExecutionResult:
    return GoalExecutionResult(
        identity=claim.identity,
        disposition="completed",
        operation_identity=f"{claim.identity.iteration.semantic_key}:executor",
        operation_binding_ref="binding:executor",
        session_id=claim.session_id,
        workspace_id=claim.workspace_namespace,
        writable_paths=("/goal/executor/work",),
        output_refs=("artifact:result",),
        completion_claim=True,
        actual_usage={"tokens.total": tokens},
        authority_breach_ref=("authority:breach" if authority_breach else ""),
        hard_budget_exhausted_dimensions=(("tokens.total",) if hard_budget else ()),
        irrecoverable_failure_ref=("failure:irrecoverable" if irrecoverable else ""),
        accepted_fact_refs=("fact:accepted",),
        evidence_refs=("evidence:accepted",),
        handoff=handoff,
        output_contract_ref="fixture-output",
    )


def _verification(
    claim: GoalExecutionClaim,
    *,
    decision: str = "rejected",
    progress: bool = True,
    accepted: bool = False,
    blocker: str = "",
    authority_breach: bool = False,
    hard_budget: bool = False,
    irrecoverable: bool = False,
    proposed_revision: GoalRevision | None = None,
    scope_expansion_route: str | None = None,
) -> GoalVerificationResult:
    values = {
        "schema_version": "belllabs.goal-verification.v1",
        "verification_id": f"verification:{claim.identity.semantic_key}",
        "executor_identity": claim.identity,
        "verifier_operation_identity": (
            f"{claim.identity.iteration.semantic_key}:independent-verifier"
        ),
        "verifier_binding_ref": "binding:verifier",
        "verifier_policy_binding_ref": "verifier:fixture@1",
        "verifier_session_id": f"{claim.session_id}:verifier",
        "verifier_workspace_id": f"{claim.workspace_namespace}:verifier",
        "verifier_writable_paths": ("/goal/verifier/work",),
        "decision": decision,
        "verification_ref": f"verification-ref:{claim.identity.semantic_key}",
        "rubric_ref": "rubric:fixture@1",
        "rubric_version": 1,
        "acceptance_contract_ref": GENERIC_GOAL_DIRECTED.acceptance_contract,
        "acceptance_version": 1,
        "progress_made": progress,
        "accepted_obligation_refs": (("fixture-obligation",) if accepted else ()),
        "findings": (),
        "evidence_refs": ("evidence:verified",),
        "admitted_executor_output_refs": ("artifact:result",),
        "admitted_executor_evidence_refs": ("evidence:accepted",),
        "unmet_obligations": (() if accepted else ("fixture-obligation",)),
        "obligation_applicability": (("fixture-obligation", True),),
        "stale_frontier_digest": STALE_FRONTIER_DIGEST,
        "blocker_class": blocker,
        "authority_breach_ref": ("authority:breach" if authority_breach else ""),
        "hard_budget_exhausted_dimensions": (("tokens.total",) if hard_budget else ()),
        "soft_budget_dimensions": (),
        "irrecoverable_failure_ref": ("failure:irrecoverable" if irrecoverable else ""),
        "proposed_revision": proposed_revision,
        "scope_expansion_route": scope_expansion_route,
        "route_ref": ("route:accepted" if scope_expansion_route else ""),
        "actual_usage": {"tokens.total": 1},
        "effect_refs": (),
        "output_contract_ref": "fixture-output",
    }
    draft = GoalVerificationResult(  # type: ignore[arg-type]
        verification_digest="pending",
        **values,
    )
    payload = asdict(draft)
    payload.pop("verification_digest")
    return replace(draft, verification_digest=sha256_digest(payload))


def test_executor_completion_claim_cannot_bypass_independent_verifier() -> None:
    interpreter, state, claim = _claim(_blueprint(max_iterations=2))
    state = interpreter.apply_execution_result(state, _execution(claim))

    assert state.status == "awaiting_verification"
    with pytest.raises(GoalDirectedExecutionError, match="stopping or routing proposal"):
        interpreter.result(state)

    state = interpreter.apply_verification(
        state,
        _verification(claim, decision="accepted", accepted=True),
    )
    result = interpreter.result(state)
    assert result.convergence_proposal.reason == "verified_completion"
    assert result.terminalization_proposal.proposed_outcome == "complete"


def test_terminalization_does_not_claim_unsettled_effects_are_settled() -> None:
    interpreter, state, claim = _claim(_blueprint(max_iterations=2))
    execution = replace(
        _execution(claim),
        effect_frontier_refs=("effect:pending",),
        pending_liability_refs=("liability:pending",),
    )
    state = interpreter.apply_execution_result(state, execution)
    state = interpreter.apply_verification(
        state,
        _verification(claim, decision="accepted", accepted=True),
    )
    assert state.terminalization_proposal is not None
    assert state.terminalization_proposal.effects_settled is False


def test_verifier_must_have_separate_operation_binding_session_and_workspace() -> None:
    interpreter, state, claim = _claim(_blueprint())
    execution = _execution(claim)
    state = interpreter.apply_execution_result(state, execution)
    verification = _verification(claim)
    forged = replace(
        verification,
        verifier_operation_identity=execution.operation_identity,
    )

    with pytest.raises(GoalDirectedExecutionError, match="separate operation"):
        interpreter.apply_verification(state, forged)


def test_revision_accepts_tactical_change_and_rejects_envelope_expansion() -> None:
    configured = _blueprint(max_iterations=3)
    interpreter, state, claim = _claim(configured)
    state = interpreter.apply_execution_result(state, _execution(claim))
    revision_two = _revision(revision=2, parent_revision_id="goal-revision:1")
    state = interpreter.apply_verification(
        state,
        _verification(
            claim,
            decision="revision_required",
            proposed_revision=revision_two,
        ),
    )
    assert state.active_revision == revision_two

    state, next_claim = interpreter.claim_execution(state)
    state = interpreter.apply_execution_result(state, _execution(next_claim))
    expansion = _revision(
        revision=3,
        parent_revision_id="goal-revision:2",
        envelope_digest=sha256_digest("expanded-envelope"),
    )
    with pytest.raises(GoalDirectedExecutionError, match="objective envelope"):
        interpreter.apply_verification(
            state,
            _verification(
                next_claim,
                decision="revision_required",
                proposed_revision=expansion,
            ),
        )


def test_revision_required_without_a_bounded_revision_fails_closed() -> None:
    interpreter, state, claim = _claim(_blueprint())
    state = interpreter.apply_execution_result(state, _execution(claim))
    with pytest.raises(GoalDirectedExecutionError, match="omitted its bounded Goal Revision"):
        interpreter.apply_verification(
            state,
            _verification(claim, decision="revision_required"),
        )


@pytest.mark.parametrize(
    ("signals", "reason"),
    [
        ({"authority_breach": True, "hard_budget": True, "accepted": True}, "authority_breach"),
        ({"hard_budget": True, "accepted": True, "irrecoverable": True}, "hard_budget_exhausted"),
        ({"accepted": True, "irrecoverable": True}, "verified_completion"),
        ({"irrecoverable": True}, "irrecoverable_failure"),
    ],
)
def test_convergence_precedence_is_fixed(
    signals: dict[str, bool],
    reason: str,
) -> None:
    interpreter, state, claim = _claim(_blueprint(max_iterations=2))
    state = interpreter.apply_execution_result(
        state,
        _execution(
            claim,
            authority_breach=signals.get("authority_breach", False),
            hard_budget=signals.get("hard_budget", False),
            irrecoverable=signals.get("irrecoverable", False),
        ),
    )
    state = interpreter.apply_verification(
        state,
        _verification(
            claim,
            decision=("accepted" if signals.get("accepted") else "rejected"),
            accepted=signals.get("accepted", False),
        ),
    )
    assert state.convergence_proposal is not None
    assert state.convergence_proposal.reason == reason


@pytest.mark.parametrize(
    ("facts", "reason"),
    [
        (
            GoalConvergenceFacts(
                no_progress_threshold_reached=True,
                repeated_blocker_threshold_reached=True,
                iteration_limit_reached=True,
            ),
            "no_progress",
        ),
        (
            GoalConvergenceFacts(
                repeated_blocker_threshold_reached=True,
                iteration_limit_reached=True,
            ),
            "repeated_blocker",
        ),
        (
            GoalConvergenceFacts(
                iteration_limit_reached=True,
                soft_budget_response_required=True,
            ),
            "iteration_limit",
        ),
        (
            GoalConvergenceFacts(
                soft_budget_response_required=True,
                bounded_revision=_revision(revision=2, parent_revision_id="goal-revision:1"),
            ),
            "soft_budget_response",
        ),
        (
            GoalConvergenceFacts(
                bounded_revision=_revision(revision=2, parent_revision_id="goal-revision:1"),
                repair_requested=True,
            ),
            "bounded_revision",
        ),
        (GoalConvergenceFacts(repair_requested=True), "repair_requested"),
        (GoalConvergenceFacts(), "continue"),
    ],
)
def test_remaining_convergence_precedence_is_total(
    facts: GoalConvergenceFacts,
    reason: str,
) -> None:
    interpreter, state, claim = _claim(_blueprint(max_iterations=4))
    proposal = interpreter.decide_convergence(state, _verification(claim), facts)
    assert proposal.reason == reason


def test_fresh_rollover_requires_current_typed_handoff_and_preserves_protected_fact() -> None:
    session_policy = GoalSessionRolloverPolicy(
        session_mode="reuse",
        fresh_agent_token_threshold=1,
        handoff_token_reserve=0,
        rollover_mode="fresh_from_handoff",
        context_selection_policy_ref="context-selection:fixture@1",
        context_compaction_policy_ref="context-compaction:fixture@1",
        protected_fact_classes=frozenset({"objective"}),
        max_rollovers=1,
        compaction_failure_action="pause",
    )
    configured = _blueprint(max_iterations=2, session_policy=session_policy)
    interpreter, state, claim = _claim(configured)
    state = interpreter.apply_execution_result(state, _execution(claim, tokens=1))
    with pytest.raises(GoalDirectedExecutionError, match="typed handoff"):
        interpreter.apply_verification(state, _verification(claim))

    interpreter, state, claim = _claim(configured)
    handoff = _handoff(claim)
    state = interpreter.apply_execution_result(
        state,
        _execution(claim, tokens=1, handoff=handoff),
    )
    state = interpreter.apply_verification(state, _verification(claim))
    assert state.next_session_mode == "fresh_from_handoff"
    assert state.session_generation == 2
    assert state.rollover_count == 1


def test_repeated_rollover_stops_at_the_frozen_limit() -> None:
    session_policy = GoalSessionRolloverPolicy(
        session_mode="reuse",
        fresh_agent_token_threshold=1,
        handoff_token_reserve=0,
        rollover_mode="fresh_from_handoff",
        context_selection_policy_ref="context-selection:fixture@1",
        context_compaction_policy_ref="context-compaction:fixture@1",
        protected_fact_classes=frozenset({"objective"}),
        max_rollovers=1,
        compaction_failure_action="pause",
    )
    configured = _blueprint(max_iterations=3, session_policy=session_policy)
    interpreter, state, claim = _claim(configured)
    state = interpreter.apply_execution_result(
        state,
        _execution(claim, tokens=1, handoff=_handoff(claim)),
    )
    state = interpreter.apply_verification(state, _verification(claim))
    state, claim = interpreter.claim_execution(state)
    state = interpreter.apply_execution_result(
        state,
        _execution(claim, tokens=1, handoff=_handoff(claim)),
    )
    with pytest.raises(GoalDirectedExecutionError, match="rollover limit"):
        interpreter.apply_verification(state, _verification(claim))


@pytest.mark.parametrize("action", ["retry", "fresh_from_handoff"])
def test_compaction_recovery_is_deterministic_and_non_effectful(action: str) -> None:
    _, _, claim = _claim(_blueprint())
    failed = _failed_handoff(claim)
    first = _recover_handoff_compaction(failed, action=action)  # type: ignore[arg-type]
    second = _recover_handoff_compaction(failed, action=action)  # type: ignore[arg-type]
    assert first == second
    assert first.compaction_status == "accepted"
    assert first.compaction_attempt == failed.compaction_attempt + 1
    assert first.compaction_failure_ref == ""
    assert first.compaction_decision_ref.startswith("goal-compaction:")
    assert first.handoff_id == failed.handoff_id


@pytest.mark.parametrize(
    ("action", "expected_status"),
    [("pause", "paused"), ("escalate", "stopping")],
)
def test_failed_compaction_applies_the_frozen_failure_action(
    action: str,
    expected_status: str,
) -> None:
    session_policy = GoalSessionRolloverPolicy(
        session_mode="reuse",
        fresh_agent_token_threshold=1,
        handoff_token_reserve=0,
        rollover_mode="fresh_from_handoff",
        context_selection_policy_ref="context-selection:fixture@1",
        context_compaction_policy_ref="context-compaction:fixture@1",
        protected_fact_classes=frozenset({"objective"}),
        max_rollovers=1,
        compaction_failure_action=action,  # type: ignore[arg-type]
    )
    interpreter, state, claim = _claim(
        _blueprint(max_iterations=2, session_policy=session_policy)
    )
    state = interpreter.apply_execution_result(
        state,
        _execution(claim, tokens=1, handoff=_failed_handoff(claim)),
    )
    state = interpreter.apply_verification(state, _verification(claim))
    assert state.status == expected_status
    assert state.convergence_proposal is not None
    assert state.convergence_proposal.reason == "compaction_failure"
    assert state.convergence_proposal.action == action


def test_handoff_and_verification_digests_fail_closed() -> None:
    interpreter, state, claim = _claim(_blueprint())
    forged_handoff = replace(_handoff(claim), handoff_digest=sha256_digest("forged"))
    with pytest.raises(GoalDirectedExecutionError, match="handoff canonical digest"):
        interpreter.apply_execution_result(
            state,
            _execution(claim, handoff=forged_handoff),
        )

    state = interpreter.apply_execution_result(state, _execution(claim))
    forged_verification = replace(
        _verification(claim),
        verification_digest=sha256_digest("forged"),
    )
    with pytest.raises(GoalDirectedExecutionError, match="verification canonical digest"):
        interpreter.apply_verification(state, forged_verification)


def test_handoff_policy_bindings_are_exact() -> None:
    interpreter, state, claim = _claim(_blueprint())
    draft = replace(
        _handoff(claim),
        context_compaction_policy_ref="context-compaction:unbound@1",
    )
    payload = asdict(draft)
    payload.pop("handoff_digest")
    forged_policy = replace(draft, handoff_digest=sha256_digest(payload))
    with pytest.raises(GoalDirectedExecutionError, match="handoff policy binding"):
        interpreter.apply_execution_result(
            state,
            _execution(claim, handoff=forged_policy),
        )


def test_handoff_requires_continuation_evidence_and_context() -> None:
    interpreter, state, claim = _claim(_blueprint())
    draft = replace(_handoff(claim), source_document_digests=())
    payload = asdict(draft)
    payload.pop("handoff_digest")
    incomplete = replace(draft, handoff_digest=sha256_digest(payload))
    with pytest.raises(GoalDirectedExecutionError, match="continuation evidence"):
        interpreter.apply_execution_result(
            state,
            _execution(claim, handoff=incomplete),
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"workspace_refs": ("undeclared-workspace:run-goal",)}, "workspace reference class"),
        ({"snapshot_refs": ("undeclared-snapshot:snapshot-1",)}, "snapshot reference class"),
        ({"source_document_digests": ("not-a-digest",)}, "canonical SHA-256"),
    ],
)
def test_handoff_context_references_stay_inside_frozen_policy(
    changes: dict[str, tuple[str, ...]],
    message: str,
) -> None:
    interpreter, state, claim = _claim(_blueprint())
    draft = replace(_handoff(claim), **changes)
    payload = asdict(draft)
    payload.pop("handoff_digest")
    invalid = replace(draft, handoff_digest=sha256_digest(payload))
    with pytest.raises(GoalDirectedExecutionError, match=message):
        interpreter.apply_execution_result(
            state,
            _execution(claim, handoff=invalid),
        )


def test_continue_as_new_accepts_a_later_active_revision_without_resetting_identity() -> None:
    configured = _blueprint(max_iterations=3)
    revision_one = _revision()
    revision_two = _revision(revision=2, parent_revision_id=revision_one.revision_id)
    continuation = GoalContinuationState(
        active_revision=revision_two,
        accepted_revisions=(revision_one, revision_two),
        next_goal_iteration=3,
        next_agent_run=3,
        session_generation=2,
        session_token_usage=0,
        workspace_generation=1,
        handoffs=(),
        output_refs=("artifact:prior",),
        no_progress_iterations=0,
        repeated_blocker_count=0,
        last_blocker_class="",
        rollover_count=1,
        next_session_mode="reuse",
        completed_goal_iterations=2,
        completed_agent_runs=2,
        lineage_digest=sha256_digest("prior-lineage"),
    )
    state = GoalDirectedInterpreter(configured).initial_state(
        _run_input(configured, revision=revision_two, continuation=continuation)
    )
    assert state.active_revision == revision_two
    assert state.next_goal_iteration == 3
    assert state.output_refs == ("artifact:prior",)
    assert state.completed_goal_iterations == 2
    assert state.lineage_digest == sha256_digest("prior-lineage")


@pytest.mark.parametrize(
    ("boundary", "expected_route"),
    [
        (AsyncDelegationBoundary(), "subordinate"),
        (AsyncDelegationBoundary(independent_settlement=True), "operation"),
        (AsyncDelegationBoundary(independent_authority=True), "linked_run"),
    ],
)
def test_async_subgoals_consume_the_canonical_classifier(
    boundary: AsyncDelegationBoundary,
    expected_route: str,
) -> None:
    routed = route_goal_async_subgoal(
        _blueprint(),
        subgoal_class="fixture_subgoal",
        boundary=boundary,
    )
    assert routed.route == expected_route


def test_scope_expansion_becomes_a_typed_route_proposal_not_a_revision() -> None:
    interpreter, state, claim = _claim(_blueprint(max_iterations=2))
    state = interpreter.apply_execution_result(state, _execution(claim))
    state = interpreter.apply_verification(
        state,
        _verification(claim, scope_expansion_route="linked_run"),
    )
    assert state.status == "stopping"
    assert state.convergence_proposal is not None
    assert state.convergence_proposal.action == "linked_run"
    assert state.convergence_proposal.route_ref == "route:accepted"
    assert state.terminalization_proposal is None
    result = interpreter.result(state)
    assert result.convergence_proposal.action == "linked_run"
    assert result.terminalization_proposal is None
