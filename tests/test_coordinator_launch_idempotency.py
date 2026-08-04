from __future__ import annotations

from datetime import timedelta

import pytest

from app.application.coordinator_launch import (
    CoordinatorWorkflowLaunchService,
)
from app.application.orchestration_binding_repository import (
    InMemoryRunSemanticInputBindingRepository,
    RunSemanticInputBindingService,
)
from app.domain.coordinator.launch import (
    LaunchIdempotencyConflict,
    LaunchTicketState,
    LaunchTicketUnavailable,
    WorkflowSubmission,
)
from app.domain.run_control.contracts import (
    AdmissionDecision,
    DecisionStatus,
)
from tests.test_coordinator_launch_preparation import (
    NOW,
    SCOPE,
    FixtureSemanticBindingProvider,
    launch_fixture,
)


class IdempotentAdmission:
    def __init__(self) -> None:
        self.calls = 0

    async def admit(self, request):
        self.calls += 1
        return AdmissionDecision(
            request_scope=request.request_scope,
            idempotency_issuer=request.idempotency_issuer,
            request_id=request.request_id,
            request_fingerprint=request.effective_configuration_digest,
            status=DecisionStatus.ACCEPTED,
            run_id="run-stable-1",
            reason_code="accepted",
            reason="already or newly admitted",
            recorded_at=request.requested_at,
        )


class RecordingDispatcher:
    def __init__(self) -> None:
        self.calls = []

    async def prepare(self, request_scope, run_id, *, initial_goal=None, **_kwargs):
        value = (request_scope, run_id, initial_goal)
        self.calls.append(value)
        return value

    async def prepare_bound(
        self,
        request_scope,
        run_id,
        *,
        semantic_binding,
        binding_service,
        initial_goal=None,
        **_kwargs,
    ):
        await binding_service.freeze(semantic_binding)
        value = (
            request_scope,
            run_id,
            initial_goal,
            semantic_binding.binding_id,
        )
        self.calls.append(value)
        return value


class IdempotentSubmission:
    def __init__(self) -> None:
        self.calls = []

    async def submit(self, workflow_input, *, workflow_id, blueprint_family):
        self.calls.append((workflow_input, workflow_id, blueprint_family))
        return WorkflowSubmission(
            workflow_id=workflow_id,
            temporal_run_id="temporal-stable-1",
        )


@pytest.mark.asyncio
async def test_repeated_prepare_and_launch_return_same_ticket_and_run() -> None:
    preparation, tickets, proposal, context = await launch_fixture("StageGraph")
    first_ticket = await preparation.prepare(proposal, context)
    second_ticket = await preparation.prepare(proposal, context)
    assert first_ticket.ticket_id == second_ticket.ticket_id

    admission = IdempotentAdmission()
    dispatcher = RecordingDispatcher()
    submissions = IdempotentSubmission()
    launch = CoordinatorWorkflowLaunchService(
        tickets=tickets,
        admission=admission,
        dispatcher=dispatcher,
        submissions=submissions,
        semantic_bindings=FixtureSemanticBindingProvider(),
        binding_service=RunSemanticInputBindingService(
            InMemoryRunSemanticInputBindingRepository()
        ),
    )
    first = await launch.launch(first_ticket.ticket_id, context)
    second = await launch.launch(
        first_ticket.ticket_id,
        context.model_copy(update={"observed_at": NOW + timedelta(minutes=16)}),
    )
    assert first == second
    assert first.run_id == "run-stable-1"
    assert first.workflow_id == "belllabs:run-stable-1:epoch:1"
    assert first.result_resource_uri == "belllabs://runs/run-stable-1/result"
    private = await tickets.get(first_ticket.ticket_id, request_scope=SCOPE)
    assert private is not None
    assert private.state == LaunchTicketState.CONSUMED
    assert private.consumed_run_id == first.run_id
    assert len({call[1] for call in submissions.calls}) == 1


@pytest.mark.asyncio
async def test_changed_goal_under_same_idempotency_identity_is_rejected() -> None:
    preparation, _tickets, proposal, context = await launch_fixture(
        "GoalDirected",
        initial_goal="First protected goal",
    )
    await preparation.prepare(proposal, context)
    changed = proposal.model_copy(update={"initial_goal": "Changed protected goal"})
    with pytest.raises(LaunchIdempotencyConflict, match="changed proposal"):
        await preparation.prepare(changed, context)


@pytest.mark.asyncio
async def test_expired_ticket_is_cas_transitioned_and_cannot_launch() -> None:
    preparation, tickets, proposal, context = await launch_fixture("StageGraph")
    public = await preparation.prepare(proposal, context)
    launch = CoordinatorWorkflowLaunchService(
        tickets=tickets,
        admission=IdempotentAdmission(),
        dispatcher=RecordingDispatcher(),
        submissions=IdempotentSubmission(),
        semantic_bindings=FixtureSemanticBindingProvider(),
        binding_service=RunSemanticInputBindingService(
            InMemoryRunSemanticInputBindingRepository()
        ),
    )
    expired_context = context.model_copy(
        update={"observed_at": NOW + timedelta(minutes=16)}
    )
    with pytest.raises(LaunchTicketUnavailable, match="expired"):
        await launch.launch(public.ticket_id, expired_context)
    private = await tickets.get(public.ticket_id, request_scope=SCOPE)
    assert private is not None and private.state == LaunchTicketState.EXPIRED


@pytest.mark.asyncio
async def test_launch_fails_closed_without_exact_semantic_binding_provider() -> None:
    preparation, tickets, proposal, context = await launch_fixture("StageGraph")
    public = await preparation.prepare(proposal, context)
    launch = CoordinatorWorkflowLaunchService(
        tickets=tickets,
        admission=IdempotentAdmission(),
        dispatcher=RecordingDispatcher(),
        submissions=IdempotentSubmission(),
    )

    with pytest.raises(LaunchTicketUnavailable, match="semantic binding provider"):
        await launch.launch(public.ticket_id, context)


@pytest.mark.asyncio
async def test_consumed_ticket_rejects_a_conflicting_run_identity() -> None:
    preparation, tickets, proposal, context = await launch_fixture("StageGraph")
    public = await preparation.prepare(proposal, context)
    await tickets.consume(
        public.ticket_id,
        request_scope=SCOPE,
        run_id="run-one",
        consumed_at=NOW,
    )
    with pytest.raises(LaunchIdempotencyConflict, match="different Workflow Run"):
        await tickets.consume(
            public.ticket_id,
            request_scope=SCOPE,
            run_id="run-two",
            consumed_at=NOW,
        )
