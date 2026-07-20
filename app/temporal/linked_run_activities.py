from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Protocol

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Worker

from app.application.linked_runs import LinkedRunService
from app.domain.composition.contracts import (
    LinkedChildResolution,
    LinkedChildResultObservation,
    LinkedResultAdmissionProposal,
    LinkedRunExecutionBinding,
    ResultEvidenceAssessment,
)
from app.domain.run_control.contracts import ActorContext


class LinkedRunDecisionPort(Protocol):
    async def execution_binding(
        self,
        request_scope: str,
        link_id: str,
        dependency_revision_id: str | None,
    ) -> LinkedRunExecutionBinding: ...

    async def resolve(
        self, observation: LinkedChildResultObservation
    ) -> LinkedChildResolution: ...


class LinkedResultAssessmentPort(Protocol):
    async def assess(
        self,
        observation: LinkedChildResultObservation,
        exact_output_ref: str,
    ) -> LinkedResultAdmissionProposal: ...


class DeferredLinkedResultAssessor:
    """Safe production fallback until workflow-specific admission policies are bound."""

    async def assess(
        self,
        _observation: LinkedChildResultObservation,
        exact_output_ref: str,
    ) -> LinkedResultAdmissionProposal:
        return LinkedResultAdmissionProposal(
            outcome="defer",
            assessment=ResultEvidenceAssessment(
                intended_purpose_satisfied=False,
                exact_version_compatible=False,
                ready=False,
                provenance_valid=False,
                permissions_valid=False,
                evaluation_evidence_valid=False,
                evidence_refs=(f"pending-assessment:{exact_output_ref}",),
            ),
            reason="workflow-specific result-admission policy is not yet bound",
        )


class LinkedRunDecisionGateway:
    """Persists exact parent-side decisions through the composition authority."""

    def __init__(
        self,
        service: LinkedRunService,
        assessor: LinkedResultAssessmentPort,
        *,
        actor: ActorContext,
        authority_ref: str,
    ) -> None:
        self._service = service
        self._assessor = assessor
        self._actor = actor
        self._authority_ref = authority_ref

    async def execution_binding(
        self,
        request_scope: str,
        link_id: str,
        dependency_revision_id: str | None,
    ) -> LinkedRunExecutionBinding:
        binding = await self._service.execution_binding(request_scope, link_id)
        if dependency_revision_id != binding.dependency_revision_id:
            raise ValueError(
                "linked-run execution request is not bound to the latest accepted "
                "dependency revision"
            )
        return binding

    async def resolve(
        self, observation: LinkedChildResultObservation
    ) -> LinkedChildResolution:
        await self._service.record_child_terminal(observation)
        if observation.status != "completed":
            disposition = await self._service.dependency_disposition(
                observation.link.request_scope,
                observation.link.link_id,
                child_terminal=True,
                acceptable_result_admitted=False,
            )
            return LinkedChildResolution(
                link_id=observation.link.link_id,
                child_status=observation.status,
                failure_ref=observation.failure_ref,
                disposition=(
                    "degraded"
                    if disposition.degradation_required_on_failure
                    else "failed"
                ),
                reason=(
                    f"governed dependency policy degraded child {observation.status}"
                    if disposition.degradation_required_on_failure
                    else f"governed dependency policy did not admit child "
                    f"{observation.status}"
                ),
            )

        decisions = []
        for output_ref in observation.exact_output_refs:
            proposal = await self._assessor.assess(observation, output_ref)
            decisions.append(
                await self._service.decide_result(
                    observation.link.request_scope,
                    observation.link.link_id,
                    output_ref,
                    proposal.outcome,
                    proposal.assessment,
                    condition_refs=proposal.condition_refs,
                    authority_ref=self._authority_ref,
                    actor=self._actor,
                    decided_at=datetime.now(UTC),
                    reason=proposal.reason,
                )
            )
        outcomes = {decision.outcome for decision in decisions}
        admitted = tuple(
            decision.exact_output_ref
            for decision in decisions
            if decision.outcome in {"admit", "conditionally_admit"}
        )
        if not decisions or outcomes == {"admit"}:
            resolution = "admitted"
        elif outcomes <= {"admit", "conditionally_admit"}:
            resolution = "conditionally_admitted"
        elif "defer" in outcomes:
            resolution = "deferred"
        else:
            resolution = "rejected"
        return LinkedChildResolution(
            link_id=observation.link.link_id,
            child_status=observation.status,
            disposition=resolution,
            decision_ids=tuple(decision.decision_id for decision in decisions),
            admitted_output_refs=admitted,
            reason="parent authority resolved every exact child output",
        )


class LinkedRunActivities:
    """Nondeterministic parent-side authority boundary for child observations."""

    def __init__(self, decisions: LinkedRunDecisionPort) -> None:
        self._decisions = decisions

    @activity.defn(name="linked_run.resolve_execution_binding")
    async def resolve_execution_binding(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        binding = await self._decisions.execution_binding(
            str(payload["request_scope"]),
            str(payload["link_id"]),
            (
                str(payload["dependency_revision_id"])
                if payload.get("dependency_revision_id") is not None
                else None
            ),
        )
        return binding.model_dump(mode="json")

    @activity.defn(name="linked_run.resolve_child_observation")
    async def resolve_child_observation(
        self, payload: dict[str, Any]
    ) -> dict[str, Any]:
        observation = LinkedChildResultObservation.model_validate(payload)
        resolution = await self._decisions.resolve(observation)
        return resolution.model_dump(mode="json")


def create_linked_run_worker(
    client: Client,
    *,
    task_queue: str,
    activities: LinkedRunActivities,
) -> Worker:
    from app.temporal.linked_run_workflow import (
        LinkedRunObserverWorkflow,
        LinkedRunWorkflow,
    )

    return Worker(
        client,
        task_queue=task_queue,
        workflows=[LinkedRunWorkflow, LinkedRunObserverWorkflow],
        activities=[
            activities.resolve_execution_binding,
            activities.resolve_child_observation,
        ],
    )
