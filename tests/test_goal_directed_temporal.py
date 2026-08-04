from __future__ import annotations

from collections import Counter

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    GoalDirectedBlueprint,
    GoalSessionRolloverPolicy,
    GoalWorkspaceSnapshotPolicy,
)
from app.domain.orchestration.contracts import (
    GoalDirectedRunInput,
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoffCheckpoint,
    GoalHandoffRequest,
    GoalHandoffResult,
    GoalRevision,
    GoalVerificationRequest,
    GoalVerificationResult,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
)
from app.temporal.goal_directed_workflow import GoalDirectedWorkflow
from app.temporal.workflow_sandbox import coordinator_workflow_runner

DIGEST = "sha256:" + "d" * 64
SCOPE = sha256_digest("temporal-goal-scope")


def temporal_blueprint() -> GoalDirectedBlueprint:
    return GoalDirectedBlueprint(
        logical_id="fixture.temporal-goal",
        title="Temporal GoalDirected fixture",
        description="Forces a fresh agent handoff before verified completion.",
        objective_contract="objective:temporal-goal@1",
        acceptance_contract="acceptance:temporal-goal@1",
        independent_verifier_ref="verifier:temporal-goal@1",
        allowed_operation_classes=frozenset({"research"}),
        session_policy=GoalSessionRolloverPolicy(
            session_mode="reuse",
            fresh_agent_token_threshold=2,
            handoff_token_reserve=1,
            rollover_mode="fresh_from_handoff",
        ),
        workspace_policy=GoalWorkspaceSnapshotPolicy(
            workspace_mode="shared",
            snapshot_mode="on_rollover",
        ),
        iteration_reservation={"goal.iterations": 1, "tokens.total": 10},
        max_iterations=3,
    )


def run_input(run_id: str) -> GoalDirectedRunInput:
    configured = temporal_blueprint()
    return GoalDirectedRunInput(
        run_id=run_id,
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        blueprint_digest=sha256_digest(configured),
        blueprint=configured.model_dump(mode="json"),
        protected_scope_digest=SCOPE,
        initial_revision=GoalRevision(
            revision_id="goal-revision:1",
            revision=1,
            parent_revision_id=None,
            protected_scope_digest=SCOPE,
            objective="Produce one independently verified report.",
            evidence_refs=("input:goal",),
            unmet_obligations=("report",),
            author="workflow-owner",
            deciding_authority="authority:goal-owner",
            applicability="remaining_run",
        ),
        required_obligation_refs=("report",),
    )


class FakeGoalDirectedActivities:
    def __init__(self, *, fail_handoff: bool = False) -> None:
        self.execution_claims: list[GoalExecutionClaim] = []
        self.verifications: list[GoalVerificationRequest] = []
        self.handoff_requests: list[GoalHandoffRequest] = []
        self.lifecycle_requests: list[LifecycleCommandRequest] = []
        self._first_execution_failed = False
        self._fail_handoff = fail_handoff

    @activity.defn(name="goaldirected.execute_iteration")
    async def execute_iteration(self, claim: GoalExecutionClaim) -> GoalExecutionResult:
        self.execution_claims.append(claim)
        if claim.identity.agent_run == 1 and not self._first_execution_failed:
            self._first_execution_failed = True
            raise ApplicationError("simulated retryable worker loss")
        return GoalExecutionResult(
            identity=claim.identity,
            disposition="completed",
            output_refs=(f"artifact:report:{claim.identity.agent_run}",),
            completion_claim=True,
            actual_usage={"tokens.total": 3, "model.turns": 1},
            temporal_activity_attempt=activity.info().attempt,
        )

    @activity.defn(name="goaldirected.verify_iteration")
    async def verify_iteration(
        self,
        request: GoalVerificationRequest,
    ) -> GoalVerificationResult:
        self.verifications.append(request)
        complete = request.claim.identity.agent_run == 2
        return GoalVerificationResult(
            identity=request.claim.identity,
            action="verified_completion" if complete else "continue",
            verification_ref=(f"verification:{request.claim.identity.semantic_key}"),
            verifier_ref=request.verifier_ref,
            acceptance_contract_ref=request.acceptance_contract_ref,
            progress_made=True,
            evidence_refs=request.execution_result.output_refs,
            unmet_obligations=() if complete else ("independent-second-pass",),
        )

    @activity.defn(name="goaldirected.prepare_handoff")
    async def prepare_handoff(self, request: GoalHandoffRequest) -> GoalHandoffResult:
        self.handoff_requests.append(request)
        if self._fail_handoff and not request.fallback:
            raise ApplicationError("simulated handoff generation failure")
        checkpoint = GoalHandoffCheckpoint(
            checkpoint_id=f"checkpoint:{request.claim.identity.semantic_key}",
            agent_run_identity=request.claim.identity,
            goal_revision_id=request.claim.identity.iteration.goal_revision_id,
            protected_scope_digest=request.protected_scope_digest,
            instructions=(
                "Resume from accepted workspace state."
                if not request.fallback
                else "System fallback: inspect frozen goal and accepted outputs."
            ),
            state_refs=request.execution_result.output_refs,
            artifact_refs=request.execution_result.output_refs,
            workspace_ref=request.claim.workspace_namespace,
        )
        return GoalHandoffResult(
            checkpoint=checkpoint,
            actual_usage={} if request.fallback else {"tokens.total": 1},
            fallback_used=request.fallback,
        )

    @activity.defn(name="goaldirected.apply_lifecycle_command")
    async def apply_lifecycle_command(
        self,
        request: LifecycleCommandRequest,
    ) -> LifecycleCommandOutcome:
        self.lifecycle_requests.append(request)
        return LifecycleCommandOutcome(
            accepted=True,
            resulting_run_version=request.expected_run_version + 1,
            phase="terminal" if request.action["kind"] == "terminalize" else "active",
            reason_code="accepted",
            evidence_frontier_digest=DIGEST,
            obligation_revision=DIGEST,
            accepted_obligation_evidence_digest=DIGEST,
            required_obligations_accepted=True,
        )

    @property
    def activity_functions(self) -> list[object]:
        return [
            self.execute_iteration,
            self.verify_iteration,
            self.prepare_handoff,
            self.apply_lifecycle_command,
        ]


@pytest.mark.asyncio
async def test_temporal_goal_loop_retries_hands_off_and_replays() -> None:
    activities = FakeGoalDirectedActivities()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="goal-directed-acceptance",
            workflows=[GoalDirectedWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.activity_functions,
        ):
            result = await environment.client.execute_workflow(
                GoalDirectedWorkflow.run,
                run_input("run-goal-temporal"),
                id="run-goal-temporal",
                task_queue="goal-directed-acceptance",
            )
            history = await environment.client.get_workflow_handle(
                "run-goal-temporal"
            ).fetch_history()
        await Replayer(
            workflows=[GoalDirectedWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(history)

    counts = Counter(claim.identity.agent_run for claim in activities.execution_claims)
    assert counts == {1: 2, 2: 1}
    first_attempts = [
        claim for claim in activities.execution_claims if claim.identity.agent_run == 1
    ]
    assert len({claim.idempotency_key for claim in first_attempts}) == 1
    assert result.stop_reason == "verified_completion"
    assert result.goal_iterations == 2
    assert result.agent_runs == 2
    assert result.rollover_count == 1
    assert result.execution_results[0].temporal_activity_attempt == 2
    assert len(result.handoff_checkpoints) == 1
    assert activities.execution_claims[-1].session_mode == "fresh_from_handoff"
    assert (
        activities.execution_claims[-1].workspace_namespace
        == activities.execution_claims[0].workspace_namespace
    )
    assert activities.execution_claims[-1].prior_checkpoint_id
    assert len(activities.verifications) == 2
    assert any(
        request.action["kind"] == "record_obligation_evidence"
        for request in activities.lifecycle_requests
    )
    assert activities.lifecycle_requests[-1].action["kind"] == "terminalize"


@pytest.mark.asyncio
async def test_temporal_goal_loop_uses_typed_system_fallback_handoff() -> None:
    activities = FakeGoalDirectedActivities(fail_handoff=True)
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="goal-directed-fallback",
            workflows=[GoalDirectedWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.activity_functions,
        ):
            result = await environment.client.execute_workflow(
                GoalDirectedWorkflow.run,
                run_input("run-goal-fallback"),
                id="run-goal-fallback",
                task_queue="goal-directed-fallback",
            )

    assert result.stop_reason == "verified_completion"
    assert result.rollover_count == 1
    assert sum(not item.fallback for item in activities.handoff_requests) == 3
    assert activities.handoff_requests[-1].fallback
    assert result.handoff_checkpoints[0].instructions.startswith("System fallback")
