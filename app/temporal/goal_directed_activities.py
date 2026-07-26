from __future__ import annotations

from dataclasses import replace

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from app.application.orchestration import (
    GoalHandoffPreparer,
    GoalIndependentVerifier,
    GoalIterationExecutor,
    RunControlLifecycleGateway,
)
from app.domain.orchestration.contracts import (
    GoalExecutionClaim,
    GoalExecutionResult,
    GoalHandoffRequest,
    GoalHandoffResult,
    GoalVerificationRequest,
    GoalVerificationResult,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
)
from app.temporal.goal_directed_workflow import GoalDirectedWorkflow


class GoalDirectedActivities:
    """All nondeterministic GoalDirected work lives behind these activities."""

    def __init__(
        self,
        *,
        executor: GoalIterationExecutor,
        verifier: GoalIndependentVerifier,
        handoffs: GoalHandoffPreparer,
        lifecycle: RunControlLifecycleGateway,
    ) -> None:
        self._executor = executor
        self._verifier = verifier
        self._handoffs = handoffs
        self._lifecycle = lifecycle

    @activity.defn(name="goaldirected.execute_iteration")
    async def execute_iteration(self, claim: GoalExecutionClaim) -> GoalExecutionResult:
        result = await self._executor.execute(claim)
        return replace(result, temporal_activity_attempt=activity.info().attempt)

    @activity.defn(name="goaldirected.prepare_handoff")
    async def prepare_handoff(self, request: GoalHandoffRequest) -> GoalHandoffResult:
        return await self._handoffs.prepare(request)

    @activity.defn(name="goaldirected.verify_iteration")
    async def verify_iteration(
        self,
        request: GoalVerificationRequest,
    ) -> GoalVerificationResult:
        return await self._verifier.verify(request)

    @activity.defn(name="goaldirected.apply_lifecycle_command")
    async def apply_lifecycle_command(
        self,
        request: LifecycleCommandRequest,
    ) -> LifecycleCommandOutcome:
        return await self._lifecycle.execute(request)


def create_goal_directed_worker(
    client: Client,
    *,
    task_queue: str,
    activities: GoalDirectedActivities,
) -> Worker:
    return Worker(
        client,
        task_queue=task_queue,
        workflows=[GoalDirectedWorkflow],
        activities=[
            activities.execute_iteration,
            activities.prepare_handoff,
            activities.verify_iteration,
            activities.apply_lifecycle_command,
        ],
    )
