from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from app.application.control_plane import ControlPlaneService
from app.application.run_control import RunControlService
from app.domain.composition.contracts import (
    DependencyAssessment,
    LinkedChildResultObservation,
    LinkedChildTerminalRecord,
    LinkedRunCancellationRequest,
    LinkedRunDependencyDisposition,
    LinkedRunExecutionBinding,
    LinkedRunRequest,
    LinkedRunResultAdmissionDecision,
    ResultEvidenceAssessment,
    RunCompositionLink,
    RunDependencyClass,
    RunDependencyRevision,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind, DefinitionSelector
from app.domain.run_control.contracts import ActorContext, DecisionStatus, RunPhase, RunRequest
from app.domain.run_control.errors import AdmissionRejected, IdempotencyConflict


class LinkedRunRepository(Protocol):
    async def get_link(
        self, request_scope: str, request_identity: str
    ) -> RunCompositionLink | None: ...

    async def get_link_by_id(self, request_scope: str, link_id: str) -> RunCompositionLink: ...

    async def commit_link(self, link: RunCompositionLink) -> RunCompositionLink: ...

    async def list_parent_links(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[RunCompositionLink, ...]: ...

    async def commit_dependency_revision(
        self, request_scope: str, revision: RunDependencyRevision
    ) -> RunDependencyRevision: ...

    async def list_dependency_revisions(
        self, request_scope: str, link_id: str
    ) -> tuple[RunDependencyRevision, ...]: ...

    async def commit_result_decision(
        self, request_scope: str, decision: LinkedRunResultAdmissionDecision
    ) -> LinkedRunResultAdmissionDecision: ...

    async def list_result_decisions(
        self, request_scope: str, link_id: str
    ) -> tuple[LinkedRunResultAdmissionDecision, ...]: ...

    async def commit_terminal_record(
        self, request_scope: str, record: LinkedChildTerminalRecord
    ) -> LinkedChildTerminalRecord: ...


class InMemoryLinkedRunRepository:
    """Conformance authority preserving immutable composition decisions."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._links: dict[str, RunCompositionLink] = {}
        self._links_by_id: dict[str, RunCompositionLink] = {}
        self._revisions: dict[str, list[RunDependencyRevision]] = {}
        self._decisions: dict[str, list[LinkedRunResultAdmissionDecision]] = {}
        self._terminal_records: dict[str, LinkedChildTerminalRecord] = {}

    async def get_link(
        self, request_scope: str, request_identity: str
    ) -> RunCompositionLink | None:
        link = self._links.get(request_identity)
        return deepcopy(link) if link is not None and link.request_scope == request_scope else None

    async def get_link_by_id(self, request_scope: str, link_id: str) -> RunCompositionLink:
        try:
            link = self._links_by_id[link_id]
        except KeyError as error:
            raise KeyError(f"run composition link not found: {link_id}") from error
        if link.request_scope != request_scope:
            raise KeyError(f"run composition link not found: {link_id}")
        return deepcopy(link)

    async def commit_link(self, link: RunCompositionLink) -> RunCompositionLink:
        async with self._lock:
            prior = self._links.get(link.request_identity)
            if prior is not None:
                if prior.request_fingerprint != link.request_fingerprint:
                    raise IdempotencyConflict(
                        "linked request identity was reused with a conflicting fingerprint"
                    )
                return deepcopy(prior)
            if link.link_id in self._links_by_id:
                raise IdempotencyConflict("run composition link identity already exists")
            self._links[link.request_identity] = deepcopy(link)
            self._links_by_id[link.link_id] = deepcopy(link)
            self._revisions[link.link_id] = []
            self._decisions[link.link_id] = []
            return deepcopy(link)

    async def list_parent_links(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[RunCompositionLink, ...]:
        return tuple(
            deepcopy(link)
            for link in sorted(self._links.values(), key=lambda item: item.link_id)
            if link.request_scope == request_scope and link.parent_run_id == parent_run_id
        )

    async def commit_dependency_revision(
        self, request_scope: str, revision: RunDependencyRevision
    ) -> RunDependencyRevision:
        await self.get_link_by_id(request_scope, revision.link_id)
        async with self._lock:
            revisions = self._revisions[revision.link_id]
            prior = next(
                (item for item in revisions if item.revision_id == revision.revision_id),
                None,
            )
            if prior is not None:
                if prior != revision:
                    raise IdempotencyConflict(
                        "dependency revision identity was reused with conflicting content"
                    )
                return deepcopy(prior)
            expected = len(revisions) + 2
            if revision.revision != expected:
                raise ValueError(f"expected dependency revision {expected}")
            revisions.append(deepcopy(revision))
            return deepcopy(revision)

    async def list_dependency_revisions(
        self, request_scope: str, link_id: str
    ) -> tuple[RunDependencyRevision, ...]:
        await self.get_link_by_id(request_scope, link_id)
        return tuple(deepcopy(self._revisions[link_id]))

    async def commit_result_decision(
        self, request_scope: str, decision: LinkedRunResultAdmissionDecision
    ) -> LinkedRunResultAdmissionDecision:
        link = await self.get_link_by_id(request_scope, decision.link_id)
        if (
            decision.parent_run_id != link.parent_run_id
            or decision.child_run_id != link.child_run_id
        ):
            raise IdempotencyConflict(
                "linked result decision run identities do not match its composition link"
            )
        async with self._lock:
            decisions = self._decisions[decision.link_id]
            prior = next(
                (item for item in decisions if item.decision_id == decision.decision_id), None
            )
            if prior is not None:
                if prior != decision:
                    raise IdempotencyConflict(
                        "result decision identity was reused with conflicting content"
                    )
                return deepcopy(prior)
            if any(item.exact_output_ref == decision.exact_output_ref for item in decisions):
                raise IdempotencyConflict("exact child output already has an admission decision")
            decisions.append(deepcopy(decision))
            return deepcopy(decision)

    async def list_result_decisions(
        self, request_scope: str, link_id: str
    ) -> tuple[LinkedRunResultAdmissionDecision, ...]:
        await self.get_link_by_id(request_scope, link_id)
        return tuple(deepcopy(self._decisions[link_id]))

    async def commit_terminal_record(
        self, request_scope: str, record: LinkedChildTerminalRecord
    ) -> LinkedChildTerminalRecord:
        link = await self.get_link_by_id(request_scope, record.link_id)
        if record.child_run_id != link.child_run_id:
            raise IdempotencyConflict("terminal child does not match its composition link")
        prior = self._terminal_records.get(record.link_id)
        if prior is not None:
            if prior != record:
                raise IdempotencyConflict("linked child already has a conflicting terminal record")
            return deepcopy(prior)
        self._terminal_records[record.link_id] = deepcopy(record)
        return deepcopy(record)


class LinkedRunService:
    def __init__(
        self,
        control_plane: ControlPlaneService,
        run_control: RunControlService,
        repository: LinkedRunRepository,
    ) -> None:
        self._control_plane = control_plane
        self._run_control = run_control
        self._repository = repository

    async def request_child(self, request: LinkedRunRequest) -> RunCompositionLink:
        identity = self._request_identity(request)
        fingerprint = sha256_digest(request.model_dump(mode="json", exclude={"requested_at"}))
        prior = await self._repository.get_link(request.request_scope, identity)
        if prior is not None:
            if prior.request_fingerprint != fingerprint:
                raise IdempotencyConflict(
                    "linked request identity was reused with a conflicting fingerprint"
                )
            return prior

        parent = await self._run_control.get_run(request.request_scope, request.parent_run_id)
        if parent.phase in {RunPhase.CANCELLING, RunPhase.TERMINAL}:
            raise AdmissionRejected("terminal or cancelling parent cannot sponsor linked work")
        parent_configuration = await self._control_plane.retrieve_for_admission(
            parent.effective_configuration_digest
        )
        slot = next(
            (
                item
                for item in parent_configuration.linked_run_slots
                if item.slot_id == request.slot_id
            ),
            None,
        )
        if slot is None:
            raise AdmissionRejected("linked request does not target a frozen parent slot")
        if request.target_workflow_type_ref not in slot.allowed_child_workflow_types:
            raise AdmissionRejected("child Workflow Type is not allowed by the frozen slot")
        if request.dependency_class.value != slot.dependency_class:
            raise AdmissionRejected("linked request changes the frozen dependency class")
        if not request.authority_request_refs <= request.actor.authority_refs:
            raise AdmissionRejected("linked request exceeds caller authority")
        if (
            request.compilation.context.actor_id != request.actor.actor_id
            or request.compilation.context.authority_subject_id != request.actor.actor_id
            or request.compilation.context.authority_scope != request.request_scope
        ):
            raise AdmissionRejected("child compilation actor or tenant scope is not bound")

        parent_budget = await self._run_control.get_budget(
            request.request_scope, request.parent_run_id
        )
        child_budget = request.child_budget.model_copy(
            update={"parent_account_id": parent_budget.account_id}
        )
        self._validate_budget_ceiling(child_budget, slot.budget_reservation_ceiling.dimensions)
        compilation = request.compilation.model_copy(
            update={
                "workflow_type": DefinitionSelector(exact=request.target_workflow_type_ref),
                "parent_authority": slot.delegation_ceiling,
            }
        )
        child_configuration = await self._control_plane.compile(compilation)
        compiled_workflow_ref = next(
            (
                ref
                for ref in child_configuration.source_refs
                if ref.kind == DefinitionKind.WORKFLOW_TYPE
            ),
            None,
        )
        if compiled_workflow_ref != request.target_workflow_type_ref:
            raise AdmissionRejected(
                "compiled child does not match the requested exact Workflow Type revision"
            )

        decision = await self._run_control.admit(
            RunRequest(
                request_scope=request.request_scope,
                idempotency_issuer=f"linked-run:{request.parent_run_id}",
                request_id=identity,
                actor=request.actor,
                effective_configuration_digest=child_configuration.digest,
                workflow_type_ref=request.target_workflow_type_ref,
                input_manifest=child_configuration.input_manifest,
                budget_envelope=child_budget,
                requested_at=request.requested_at,
                correlation_id=request.correlation_id,
                causation_id=request.causation_id,
                parent_run_id=request.parent_run_id,
                sponsorship_ref=f"linked-slot:{request.slot_id}:{request.request_revision}",
                approval_refs=request.permission_assessment_refs,
                delegation_authority_refs=request.authority_request_refs,
                admission_evidence_refs=(f"linked-request:{identity}",),
            )
        )
        if decision.status != DecisionStatus.ACCEPTED or decision.run_id is None:
            raise AdmissionRejected(f"child run admission rejected: {decision.reason}")
        child_budget_state = await self._run_control.get_budget(
            request.request_scope, decision.run_id
        )
        return await self._repository.commit_link(
            RunCompositionLink(
                link_id=_stable_id("run-composition-link", identity),
                request_identity=identity,
                request_fingerprint=fingerprint,
                request_scope=request.request_scope,
                parent_run_id=request.parent_run_id,
                child_run_id=decision.run_id,
                slot_id=request.slot_id,
                request_revision=request.request_revision,
                target_workflow_type_ref=request.target_workflow_type_ref,
                child_effective_configuration_digest=child_configuration.digest,
                dependency_class=request.dependency_class,
                linked_budget_account_id=child_budget_state.account_id,
                result_admission_policy=slot.result_admission_policy,
                cancellation_policy=slot.cancellation_policy,
                created_at=request.requested_at,
            )
        )

    async def revise_dependency(
        self,
        request_scope: str,
        link_id: str,
        dependency_class: RunDependencyClass,
        assessment: DependencyAssessment,
        *,
        authority_ref: str,
        actor: ActorContext,
        decided_at: datetime,
    ) -> RunDependencyRevision:
        if (
            "workflow_run.revise_dependency" not in actor.permissions
            or authority_ref not in actor.authority_refs
        ):
            raise AdmissionRejected("actor lacks dependency revision authority")
        link = await self._repository.get_link_by_id(request_scope, link_id)
        revisions = await self._repository.list_dependency_revisions(request_scope, link_id)
        prior_class = revisions[-1].dependency_class if revisions else link.dependency_class
        class_changed = prior_class != dependency_class
        if class_changed and (
            not assessment.readiness_reassessment_required
            or not (
                assessment.affected_obligation_refs
                or assessment.affected_artifact_refs
                or assessment.affected_output_refs
                or assessment.affected_evaluation_refs
            )
        ):
            raise AdmissionRejected(
                "dependency class changes require an explicit affected-state reassessment"
            )
        revision_number = len(revisions) + 2
        revision = RunDependencyRevision(
            revision_id=_stable_id("run-dependency-revision", link_id, str(revision_number)),
            link_id=link_id,
            revision=revision_number,
            prior_dependency_class=prior_class,
            dependency_class=dependency_class,
            assessment=assessment,
            authority_ref=authority_ref,
            decided_by=actor.actor_id,
            decided_at=decided_at,
        )
        return await self._repository.commit_dependency_revision(request_scope, revision)

    async def decide_result(
        self,
        request_scope: str,
        link_id: str,
        exact_output_ref: str,
        outcome: str,
        assessment: ResultEvidenceAssessment,
        *,
        condition_refs: tuple[str, ...] = (),
        authority_ref: str,
        actor: ActorContext,
        decided_at: datetime,
        reason: str,
    ) -> LinkedRunResultAdmissionDecision:
        if (
            "workflow_run.admit_linked_result" not in actor.permissions
            or authority_ref not in actor.authority_refs
        ):
            raise AdmissionRejected("actor lacks linked result-admission authority")
        link = await self._repository.get_link_by_id(request_scope, link_id)
        parent = await self._run_control.get_run(link.request_scope, link.parent_run_id)
        late = parent.phase == RunPhase.TERMINAL
        effective_outcome = "defer" if late else outcome
        decision = LinkedRunResultAdmissionDecision(
            decision_id=_stable_id("linked-result-decision", link_id, exact_output_ref),
            link_id=link_id,
            parent_run_id=link.parent_run_id,
            child_run_id=link.child_run_id,
            exact_output_ref=exact_output_ref,
            outcome=effective_outcome,
            assessment=assessment,
            condition_refs=condition_refs if not late else (),
            late_result=late,
            authority_ref=authority_ref,
            decided_by=actor.actor_id,
            decided_at=decided_at,
            reason=reason,
        )
        return await self._repository.commit_result_decision(request_scope, decision)

    async def dependency_disposition(
        self,
        request_scope: str,
        link_id: str,
        *,
        child_terminal: bool,
        acceptable_result_admitted: bool,
    ) -> LinkedRunDependencyDisposition:
        link = await self._repository.get_link_by_id(request_scope, link_id)
        revisions = await self._repository.list_dependency_revisions(request_scope, link_id)
        dependency = revisions[-1].dependency_class if revisions else link.dependency_class
        blocking = dependency in {
            RunDependencyClass.REQUIRED_BLOCKING,
            RunDependencyClass.DEGRADABLE_BLOCKING,
        }
        unresolved = not acceptable_result_admitted
        return LinkedRunDependencyDisposition(
            link_id=link_id,
            blocks_parent_completion=blocking and unresolved,
            wait_required=blocking and not child_terminal and unresolved,
            degradation_required_on_failure=(
                dependency == RunDependencyClass.DEGRADABLE_BLOCKING
                and child_terminal
                and unresolved
            ),
            parent_may_complete=(not blocking)
            or acceptable_result_admitted
            or (dependency == RunDependencyClass.DEGRADABLE_BLOCKING and child_terminal),
        )

    async def execution_binding(
        self, request_scope: str, link_id: str
    ) -> LinkedRunExecutionBinding:
        link = await self._repository.get_link_by_id(request_scope, link_id)
        revisions = await self._repository.list_dependency_revisions(request_scope, link_id)
        latest = revisions[-1] if revisions else None
        return LinkedRunExecutionBinding(
            link=link,
            effective_dependency_class=(
                latest.dependency_class if latest is not None else link.dependency_class
            ),
            dependency_revision_id=(latest.revision_id if latest is not None else None),
        )

    async def record_child_terminal(
        self, observation: LinkedChildResultObservation
    ) -> LinkedChildTerminalRecord:
        record = LinkedChildTerminalRecord(
            terminal_record_id=_stable_id("linked-terminal", observation.link.link_id),
            link_id=observation.link.link_id,
            child_run_id=observation.link.child_run_id,
            status=observation.status,
            exact_output_refs=observation.exact_output_refs,
            failure_ref=observation.failure_ref,
            observed_at=observation.observed_at,
        )
        return await self._repository.commit_terminal_record(observation.link.request_scope, record)

    async def cancellation_requests(
        self, request_scope: str, parent_run_id: str
    ) -> tuple[LinkedRunCancellationRequest, ...]:
        links = await self._repository.list_parent_links(request_scope, parent_run_id)
        results: list[LinkedRunCancellationRequest] = []
        for link in links:
            revisions = await self._repository.list_dependency_revisions(
                request_scope, link.link_id
            )
            dependency = revisions[-1].dependency_class if revisions else link.dependency_class
            blocking = dependency in {
                RunDependencyClass.REQUIRED_BLOCKING,
                RunDependencyClass.DEGRADABLE_BLOCKING,
            }
            requested = blocking or link.cancellation_policy == "request_cancel"
            results.append(
                LinkedRunCancellationRequest(
                    cancellation_request_id=_stable_id(
                        "linked-cancellation", parent_run_id, link.link_id
                    ),
                    link_id=link.link_id,
                    child_run_id=link.child_run_id,
                    requested=requested,
                    reason=(
                        "parent cancellation requests governed child cancellation"
                        if requested
                        else "declared nonblocking continuation policy allows child to continue"
                    ),
                )
            )
        return tuple(results)

    @staticmethod
    def _request_identity(request: LinkedRunRequest) -> str:
        return _stable_id(
            "linked-run-request",
            request.request_scope,
            request.parent_run_id,
            request.slot_id,
            str(request.request_revision),
            request.target_workflow_type_ref.logical_id,
            str(request.target_workflow_type_ref.revision),
        )

    @staticmethod
    def _validate_budget_ceiling(child_budget: object, ceilings: dict[str, int]) -> None:
        from app.domain.run_control.contracts import BudgetEnvelope

        budget = BudgetEnvelope.model_validate(child_budget)
        dimensions = {item.dimension: item for item in budget.dimensions}
        for dimension, ceiling in ceilings.items():
            selected = dimensions.get(dimension)
            if selected is None or selected.hard_cap is None or selected.hard_cap > ceiling:
                raise AdmissionRejected(
                    f"child budget for {dimension} exceeds the linked slot ceiling"
                )


def _stable_id(*parts: str) -> str:
    return str(uuid5(NAMESPACE_URL, ":".join(parts)))
