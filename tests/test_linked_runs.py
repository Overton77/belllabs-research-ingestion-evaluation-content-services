from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from temporalio import workflow
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.application.linked_runs import (
    InMemoryLinkedRunRepository,
    LinkedRunService,
)
from app.domain.composition.contracts import (
    DependencyAssessment,
    LinkedChildResultObservation,
    LinkedResultAdmissionProposal,
    LinkedRunContinuationState,
    LinkedRunRequest,
    ResultEvidenceAssessment,
    RunDependencyClass,
)
from app.domain.control_plane.contracts import (
    AuthorityCeiling,
    BudgetCeiling,
    CompilationContext,
    CompileInvocation,
    DefinitionKind,
    DefinitionSelector,
    EnvironmentAvailability,
    ExactDefinitionRef,
    LinkedRunSlotConstraint,
    RunInputManifestRef,
)
from app.domain.orchestration.contracts import (
    StageGraphRunInput,
    StageGraphRunResult,
)
from app.domain.run_control.contracts import (
    ActorContext,
    AdmissionDecision,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    BudgetState,
    DecisionStatus,
    RunPhase,
    RunRequest,
)
from app.domain.run_control.errors import AdmissionRejected, IdempotencyConflict
from app.temporal.linked_run_activities import (
    LinkedRunActivities,
    LinkedRunDecisionGateway,
)
from app.temporal.linked_run_workflow import (
    LinkedRunObserverWorkflow,
    LinkedRunWorkflow,
)

NOW = datetime(2026, 7, 19, 21, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64
CHILD_DIGEST = "sha256:" + "b" * 64
PARENT_ACCOUNT = "budget-account:parent"
PARENT_AUTHORITY = "workflow_run.parent:parent-run:sponsor"
DECISION_AUTHORITY = "authority:composition"


@workflow.defn(name="belllabs.stagegraph", sandboxed=False)
class FixtureChildWorkflow:
    @workflow.run
    async def run(self, run_input: StageGraphRunInput) -> StageGraphRunResult:
        if run_input.blueprint.get("logical_id") == "child.fail":
            raise ApplicationError("fixture child failed", non_retryable=True)
        if run_input.blueprint.get("logical_id") == "child.cancel":
            await workflow.wait_condition(lambda: False)
        return StageGraphRunResult(
            run_id=run_input.run_id,
            workflow_cycles=0,
            execution_epoch=run_input.execution_epoch,
            stage_cycles={"child": 0},
            operation_attempts={"child": 1},
            output_refs={"child": ("artifact:child:exact-v1",)},
            reused_output_refs={},
            schedule_trace=("child",),
            lineage=(),
        )


class FixtureResultAssessor:
    def __init__(self) -> None:
        self.assessed_refs: list[str] = []

    async def assess(
        self,
        _observation: LinkedChildResultObservation,
        exact_output_ref: str,
    ) -> LinkedResultAdmissionProposal:
        self.assessed_refs.append(exact_output_ref)
        return LinkedResultAdmissionProposal(
            outcome="admit",
            assessment=ResultEvidenceAssessment(
                intended_purpose_satisfied=True,
                exact_version_compatible=True,
                ready=True,
                provenance_valid=True,
                permissions_valid=True,
                evaluation_evidence_valid=True,
                evidence_refs=(f"evaluation:{exact_output_ref}",),
            ),
            reason="fixture assessor accepted exact output evidence",
        )


def ref(kind: DefinitionKind, name: str, digest: str = DIGEST) -> ExactDefinitionRef:
    return ExactDefinitionRef(kind=kind, logical_id=name, revision=1, digest=digest)


CHILD_WORKFLOW = ref(DefinitionKind.WORKFLOW_TYPE, "child.workflow")
INPUT_MANIFEST = RunInputManifestRef(manifest_id="child-input", revision=1, digest=DIGEST)


def actor() -> ActorContext:
    return ActorContext(
        actor_id="composition-worker",
        authority_refs=frozenset({PARENT_AUTHORITY, DECISION_AUTHORITY}),
        permissions=frozenset(
            {
                "workflow_run.admit",
                "workflow_run.revise_dependency",
                "workflow_run.admit_linked_result",
            }
        ),
    )


def authority() -> AuthorityCeiling:
    return AuthorityCeiling(
        capabilities=frozenset({"sandbox.execute"}),
        budgets=BudgetCeiling(dimensions={"units": 10}),
        max_concurrency=1,
    )


def compile_invocation() -> CompileInvocation:
    return CompileInvocation(
        workflow_type=DefinitionSelector(exact=CHILD_WORKFLOW),
        blueprint=DefinitionSelector(exact=ref(DefinitionKind.BLUEPRINT, "child.graph")),
        control_profile=DefinitionSelector(
            exact=ref(DefinitionKind.CONTROL_PROFILE, "child.control")
        ),
        runtime_profile=DefinitionSelector(
            exact=ref(DefinitionKind.RUNTIME_PROFILE, "child.runtime")
        ),
        workspace_template=DefinitionSelector(
            exact=ref(DefinitionKind.WORKSPACE_TEMPLATE, "child.workspace")
        ),
        evaluation_profile=DefinitionSelector(
            exact=ref(DefinitionKind.EVALUATION_PROFILE, "child.evaluation")
        ),
        input_manifest=INPUT_MANIFEST,
        caller_authority=authority(),
        environment=EnvironmentAvailability(
            capabilities=frozenset({"sandbox.execute"}),
            runtime_bindings=frozenset({"python-3.12"}),
        ),
        context=CompilationContext(
            compilation_id="compile-child-request-1",
            compiled_at=NOW,
            actor_id=actor().actor_id,
            authority_subject_id=actor().actor_id,
            authority_scope="tenant-1",
        ),
    )


def linked_request(
    dependency: RunDependencyClass = RunDependencyClass.REQUIRED_BLOCKING,
    *,
    purpose: str = "produce exact child evidence",
) -> LinkedRunRequest:
    return LinkedRunRequest(
        request_scope="tenant-1",
        parent_run_id="parent-run",
        slot_id="child_work",
        request_revision=1,
        target_workflow_type_ref=CHILD_WORKFLOW,
        compilation=compile_invocation(),
        child_budget=BudgetEnvelope(
            dimensions=(
                BudgetDimensionLimit(
                    dimension="units",
                    applicability=BudgetApplicability.BOUNDED,
                    hard_cap=5,
                ),
            ),
            baseline_reservations={"units": 5},
        ),
        dependency_class=dependency,
        purpose=purpose,
        actor=actor(),
        requested_at=NOW,
        authority_request_refs=frozenset({PARENT_AUTHORITY}),
        permission_assessment_refs=("permission-assessment:child",),
        causation_id="stage:request-child",
        correlation_id="parent-run:composition",
    )


class FakeControlPlane:
    def __init__(self, dependency: RunDependencyClass) -> None:
        self.compilations: list[CompileInvocation] = []
        self.compiled_workflow_ref = CHILD_WORKFLOW
        self.slot = LinkedRunSlotConstraint(
            slot_id="child_work",
            allowed_child_workflow_types=frozenset({CHILD_WORKFLOW}),
            dependency_class=dependency.value,
            wait_policy=(
                "wait"
                if dependency
                in {
                    RunDependencyClass.REQUIRED_BLOCKING,
                    RunDependencyClass.DEGRADABLE_BLOCKING,
                }
                else "continue"
            ),
            cancellation_policy=(
                "request_cancel"
                if dependency == RunDependencyClass.REQUIRED_BLOCKING
                else "allow_continue"
            ),
            result_admission_policy="linked-result:exact-evidence@1",
            delegation_ceiling=authority(),
            budget_reservation_ceiling=BudgetCeiling(dimensions={"units": 5}),
        )

    async def retrieve_for_admission(self, _digest: str) -> SimpleNamespace:
        return SimpleNamespace(linked_run_slots=(self.slot,))

    async def compile(self, invocation: CompileInvocation) -> SimpleNamespace:
        self.compilations.append(invocation)
        return SimpleNamespace(
            digest=CHILD_DIGEST,
            workflow_type=SimpleNamespace(logical_id=CHILD_WORKFLOW.logical_id),
            source_refs=(self.compiled_workflow_ref,),
            input_manifest=INPUT_MANIFEST,
        )


class FakeRunControl:
    def __init__(self) -> None:
        self.parent_phase = RunPhase.ACTIVE
        self.admissions: list[RunRequest] = []
        self.parent_budget = BudgetState(
            account_id=PARENT_ACCOUNT,
            run_id="parent-run",
            limits=(
                BudgetDimensionLimit(
                    dimension="units",
                    applicability=BudgetApplicability.BOUNDED,
                    hard_cap=10,
                ),
            ),
        )
        self.child_budget = BudgetState(
            account_id="budget-account:child",
            run_id="child-run",
            parent_account_id=PARENT_ACCOUNT,
            limits=(
                BudgetDimensionLimit(
                    dimension="units",
                    applicability=BudgetApplicability.BOUNDED,
                    hard_cap=5,
                ),
            ),
        )

    async def get_run(self, _scope: str, run_id: str) -> SimpleNamespace:
        if run_id == "parent-run":
            return SimpleNamespace(
                phase=self.parent_phase,
                effective_configuration_digest=DIGEST,
            )
        return SimpleNamespace(phase=RunPhase.TERMINAL)

    async def get_budget(self, _scope: str, run_id: str) -> BudgetState:
        return self.parent_budget if run_id == "parent-run" else self.child_budget

    async def admit(self, request: RunRequest) -> AdmissionDecision:
        self.admissions.append(request)
        return AdmissionDecision(
            request_scope="tenant-1",
            idempotency_issuer="linked-run:parent-run",
            request_id=request.request_id,
            request_fingerprint=DIGEST,
            status=DecisionStatus.ACCEPTED,
            run_id="child-run",
            reason_code="accepted",
            reason="independently compiled and admitted",
            recorded_at=NOW,
        )


def service_fixture(
    dependency: RunDependencyClass = RunDependencyClass.REQUIRED_BLOCKING,
) -> tuple[
    LinkedRunService,
    FakeControlPlane,
    FakeRunControl,
    InMemoryLinkedRunRepository,
]:
    control = FakeControlPlane(dependency)
    run_control = FakeRunControl()
    repository = InMemoryLinkedRunRepository()
    service = LinkedRunService(control, run_control, repository)  # type: ignore[arg-type]
    return service, control, run_control, repository


@pytest.mark.asyncio
async def test_linked_request_retry_returns_one_child_reservation_and_link() -> None:
    service, control, run_control, _repository = service_fixture()
    request = linked_request()

    first = await service.request_child(request)
    replayed = await service.request_child(request)

    assert first == replayed
    assert first.child_run_id == "child-run"
    assert first.linked_budget_account_id == "budget-account:child"
    assert len(control.compilations) == 1
    assert control.compilations[0].parent_authority == authority()
    assert len(run_control.admissions) == 1
    admitted = run_control.admissions[0]
    assert admitted.budget_envelope.parent_account_id == PARENT_ACCOUNT

    with pytest.raises(IdempotencyConflict):
        await service.request_child(linked_request(purpose="conflicting payload"))


@pytest.mark.asyncio
async def test_linked_request_rejects_mismatched_exact_child_revision() -> None:
    service, control, _run_control, _repository = service_fixture()
    control.compiled_workflow_ref = CHILD_WORKFLOW.model_copy(
        update={"revision": 2, "digest": "sha256:" + "d" * 64}
    )

    with pytest.raises(AdmissionRejected, match="exact Workflow Type revision"):
        await service.request_child(linked_request())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dependency", "terminal", "admitted", "blocks", "waits", "degrades", "may_complete"),
    [
        (RunDependencyClass.REQUIRED_BLOCKING, False, False, True, True, False, False),
        (RunDependencyClass.DEGRADABLE_BLOCKING, True, False, True, False, True, True),
        (RunDependencyClass.DEGRADABLE_NONBLOCKING, False, False, False, False, False, True),
        (RunDependencyClass.DETACHED_ADVISORY, False, False, False, False, False, True),
    ],
)
async def test_dependency_classes_have_declared_completion_behavior(
    dependency: RunDependencyClass,
    terminal: bool,
    admitted: bool,
    blocks: bool,
    waits: bool,
    degrades: bool,
    may_complete: bool,
) -> None:
    service, _control, _run_control, _repository = service_fixture(dependency)
    link = await service.request_child(linked_request(dependency))

    disposition = await service.dependency_disposition(
        "tenant-1",
        link.link_id,
        child_terminal=terminal,
        acceptable_result_admitted=admitted,
    )

    assert disposition.blocks_parent_completion is blocks
    assert disposition.wait_required is waits
    assert disposition.degradation_required_on_failure is degrades
    assert disposition.parent_may_complete is may_complete


@pytest.mark.asyncio
async def test_dependency_revision_result_admission_late_delivery_and_cancellation() -> None:
    service, _control, run_control, repository = service_fixture()
    link = await service.request_child(linked_request())
    revision = await service.revise_dependency(
        "tenant-1",
        link.link_id,
        RunDependencyClass.DEGRADABLE_NONBLOCKING,
        DependencyAssessment(
            affected_obligation_refs=("obligation:child",),
            affected_artifact_refs=("artifact:parent-draft",),
            affected_evaluation_refs=("evaluation:parent",),
            readiness_reassessment_required=True,
            reason="authorized parent policy revision",
        ),
        authority_ref=DECISION_AUTHORITY,
        actor=actor(),
        decided_at=NOW,
    )
    assert revision.revision == 2
    assert revision.prior_dependency_class == RunDependencyClass.REQUIRED_BLOCKING

    assessment = ResultEvidenceAssessment(
        intended_purpose_satisfied=True,
        exact_version_compatible=True,
        ready=True,
        provenance_valid=True,
        permissions_valid=True,
        evaluation_evidence_valid=True,
        evidence_refs=("evaluation:child-output",),
    )
    admitted = await service.decide_result(
        "tenant-1",
        link.link_id,
        "artifact:child:exact-v1",
        "admit",
        assessment,
        authority_ref=DECISION_AUTHORITY,
        actor=actor(),
        decided_at=NOW,
        reason="exact child result satisfies the parent purpose",
    )
    assert admitted.outcome == "admit"
    assert not admitted.late_result

    run_control.parent_phase = RunPhase.TERMINAL
    late = await service.decide_result(
        "tenant-1",
        link.link_id,
        "artifact:child:late-v2",
        "admit",
        assessment,
        authority_ref=DECISION_AUTHORITY,
        actor=actor(),
        decided_at=NOW,
        reason="preserve late result without parent mutation",
    )
    assert late.outcome == "defer"
    assert late.late_result
    assert len(await repository.list_result_decisions("tenant-1", link.link_id)) == 2

    cancellations = await service.cancellation_requests("tenant-1", "parent-run")
    assert len(cancellations) == 1
    assert cancellations[0].requested


@pytest.mark.asyncio
async def test_revised_dependency_controls_cancellation_and_decisions_are_link_bound() -> None:
    service, _control, _run_control, repository = service_fixture(
        RunDependencyClass.DEGRADABLE_BLOCKING
    )
    link = await service.request_child(linked_request(RunDependencyClass.DEGRADABLE_BLOCKING))
    await service.revise_dependency(
        "tenant-1",
        link.link_id,
        RunDependencyClass.DEGRADABLE_NONBLOCKING,
        DependencyAssessment(
            affected_output_refs=("output:advisory-child",),
            readiness_reassessment_required=True,
            reason="retain advisory work without blocking parent cancellation",
        ),
        authority_ref=DECISION_AUTHORITY,
        actor=actor(),
        decided_at=NOW,
    )
    cancellations = await service.cancellation_requests("tenant-1", "parent-run")
    assert not cancellations[0].requested

    assessment = ResultEvidenceAssessment(
        intended_purpose_satisfied=True,
        exact_version_compatible=True,
        ready=True,
        provenance_valid=True,
        permissions_valid=True,
        evaluation_evidence_valid=True,
    )
    valid = await service.decide_result(
        "tenant-1",
        link.link_id,
        "artifact:child:bound",
        "admit",
        assessment,
        authority_ref=DECISION_AUTHORITY,
        actor=actor(),
        decided_at=NOW,
        reason="create a valid decision fixture",
    )
    with pytest.raises(IdempotencyConflict, match="do not match"):
        await repository.commit_result_decision(
            "tenant-1",
            valid.model_copy(
                update={
                    "decision_id": "wrong-link-runs",
                    "exact_output_ref": "artifact:child:wrong-link",
                    "child_run_id": "different-child",
                }
            ),
        )


def test_continue_as_new_contract_preserves_semantic_state() -> None:
    state = LinkedRunContinuationState(
        run_id="parent-run",
        next_execution_epoch=2,
        workflow_cycle=3,
        semantic_counters={"operation_attempt": 7, "stage_cycle": 2},
        pending_wait_ids=("wait:child",),
        link_ids=("link:child",),
        accepted_dependency_revision_ids=("dependency-revision:2",),
        accepted_result_decision_ids=("result-decision:1",),
        reservation_ids=("reservation:child",),
        authority_ref=DECISION_AUTHORITY,
    )

    replayed = LinkedRunContinuationState.model_validate_json(state.model_dump_json())
    assert replayed == state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("initial_dependency", "effective_dependency", "expected_child_status"),
    [
        (
            RunDependencyClass.REQUIRED_BLOCKING,
            RunDependencyClass.REQUIRED_BLOCKING,
            "completed",
        ),
        (
            RunDependencyClass.DEGRADABLE_NONBLOCKING,
            RunDependencyClass.DEGRADABLE_NONBLOCKING,
            "completed",
        ),
        (
            RunDependencyClass.REQUIRED_BLOCKING,
            RunDependencyClass.DEGRADABLE_NONBLOCKING,
            "cancelled",
        ),
    ],
)
async def test_temporal_mapping_executes_admitted_child_as_distinct_workflow(
    initial_dependency: RunDependencyClass,
    effective_dependency: RunDependencyClass,
    expected_child_status: str,
) -> None:
    service, _control, _run_control, repository = service_fixture(initial_dependency)
    link = await service.request_child(linked_request(initial_dependency))
    if initial_dependency != effective_dependency:
        await service.revise_dependency(
            "tenant-1",
            link.link_id,
            effective_dependency,
            DependencyAssessment(
                affected_output_refs=("output:child",),
                readiness_reassessment_required=True,
                reason="exercise accepted Temporal dependency revision",
            ),
            authority_ref=DECISION_AUTHORITY,
            actor=actor(),
            decided_at=NOW,
        )
    execution_binding = await service.execution_binding("tenant-1", link.link_id)
    assert execution_binding.effective_dependency_class == effective_dependency
    child_input = StageGraphRunInput(
        run_id=link.child_run_id,
        request_scope=link.request_scope,
        effective_configuration_digest=link.child_effective_configuration_digest,
        blueprint_digest=DIGEST,
        blueprint={
            "logical_id": (
                "child.cancel" if expected_child_status == "cancelled" else "child.graph"
            ),
            "title": "Child graph",
            "description": "Minimal child workflow fixture",
            "family": "StageGraph",
            "stages": [{"stage_id": "child"}],
        },
    )
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        assessor = FixtureResultAssessor()
        decisions = LinkedRunDecisionGateway(
            service,
            assessor,
            actor=actor(),
            authority_ref=DECISION_AUTHORITY,
        )
        activities = LinkedRunActivities(decisions)
        continuation = LinkedRunContinuationState(
            run_id="parent-run",
            next_execution_epoch=2,
            workflow_cycle=0,
            semantic_counters={"operation_attempt": 1},
            pending_wait_ids=("wait:unrelated",),
            link_ids=(link.link_id,),
            accepted_dependency_revision_ids=(),
            accepted_result_decision_ids=(),
            reservation_ids=(link.linked_budget_account_id,),
            authority_ref=DECISION_AUTHORITY,
        )
        async with Worker(
            environment.client,
            task_queue="linked-run-composition",
            workflows=[
                LinkedRunWorkflow,
                LinkedRunObserverWorkflow,
                FixtureChildWorkflow,
            ],
            activities=[
                activities.resolve_execution_binding,
                activities.resolve_child_observation,
            ],
        ):
            result = await environment.client.execute_workflow(
                LinkedRunWorkflow.run,
                {
                    "request_scope": link.request_scope,
                    "link_id": link.link_id,
                    "dependency_revision_id": execution_binding.dependency_revision_id,
                    "child_task_queue": "linked-run-composition",
                    "child_input": {
                        "run_id": child_input.run_id,
                        "request_scope": child_input.request_scope,
                        "effective_configuration_digest": (
                            child_input.effective_configuration_digest
                        ),
                        "blueprint_digest": child_input.blueprint_digest,
                        "blueprint": child_input.blueprint,
                    },
                    "continuation_state": continuation.model_dump(mode="json"),
                    "force_continue_as_new": True,
                },
                id="linked-run-parent-fixture",
                task_queue="linked-run-composition",
            )
            if effective_dependency == RunDependencyClass.DEGRADABLE_NONBLOCKING:
                assert result["disposition"] == "launched_nonblocking"
                observer = environment.client.get_workflow_handle(result["observer_workflow_id"])
                result = await observer.result()

    assert result["child_status"] == expected_child_status
    assert result["disposition"] == (
        "admitted" if expected_child_status == "completed" else "failed"
    )
    assert result["execution_epoch"] == 2
    assert LinkedRunContinuationState.model_validate(result["continuation_state"]) == continuation
    assert result["child_run_id"] == link.child_run_id
    expected_outputs = ["artifact:child:exact-v1"] if expected_child_status == "completed" else []
    assert result["admitted_output_refs"] == expected_outputs
    assert assessor.assessed_refs == expected_outputs
    assert len(await repository.list_result_decisions("tenant-1", link.link_id)) == (
        1 if expected_child_status == "completed" else 0
    )
    assert "child_result" not in result


@pytest.mark.asyncio
async def test_degradable_child_failure_requires_governed_resolution() -> None:
    dependency = RunDependencyClass.DEGRADABLE_BLOCKING
    service, _control, _run_control, repository = service_fixture(dependency)
    link = await service.request_child(linked_request(dependency))
    decisions = LinkedRunDecisionGateway(
        service,
        FixtureResultAssessor(),
        actor=actor(),
        authority_ref=DECISION_AUTHORITY,
    )
    activities = LinkedRunActivities(decisions)
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="linked-run-degradation",
            workflows=[
                LinkedRunWorkflow,
                LinkedRunObserverWorkflow,
                FixtureChildWorkflow,
            ],
            activities=[
                activities.resolve_execution_binding,
                activities.resolve_child_observation,
            ],
        ):
            result = await environment.client.execute_workflow(
                LinkedRunWorkflow.run,
                {
                    "request_scope": link.request_scope,
                    "link_id": link.link_id,
                    "dependency_revision_id": None,
                    "child_task_queue": "linked-run-degradation",
                    "child_input": {
                        "run_id": link.child_run_id,
                        "request_scope": link.request_scope,
                        "effective_configuration_digest": (
                            link.child_effective_configuration_digest
                        ),
                        "blueprint_digest": DIGEST,
                        "blueprint": {
                            "logical_id": "child.fail",
                            "title": "Failing child",
                            "description": "Exercise governed degradation",
                            "family": "StageGraph",
                            "stages": [{"stage_id": "child"}],
                        },
                    },
                },
                id="linked-run-degradation-parent",
                task_queue="linked-run-degradation",
            )

    assert result["disposition"] == "degraded"
    assert result["admitted_output_refs"] == []
    assert await repository.list_result_decisions("tenant-1", link.link_id) == ()
