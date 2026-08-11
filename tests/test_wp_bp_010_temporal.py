from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.application.orchestration import (
    StageGraphOperationPreparationService,
    StaticStageGraphOperationTemplateProvider,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AllowedOperationVariant,
    DefinitionKind,
    ExactDefinitionRef,
    FairnessGroup,
    LateResultPolicy,
    LateResultRule,
    SlowSiblingPolicy,
    StageCyclePolicy,
    StageDependency,
    StageGraphBlueprint,
    StageGraphWait,
    StageInputSlot,
    StageJoin,
    StageNode,
    StageOperationSlot,
    StageOutputSlot,
    WorkflowCyclePolicy,
)
from app.domain.operation_execution.contracts import (
    CapabilityGrant,
    ModelPolicy,
    NativeOperationExecutionPlacement,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
    PromptSegment,
    PromptTrustClass,
    WorkspaceContract,
)
from app.domain.orchestration.contracts import (
    ExecutionIdentity,
    StageGraphAdmissionActivityRequest,
    StageGraphAdmissionActivityResult,
    StageGraphCompletionActivityRequest,
    StageGraphCompletionActivityResult,
    StageGraphCycleActivityRequest,
    StageGraphCycleActivityResult,
    StageGraphInitializeRequest,
    StageGraphInitializeResult,
    StageGraphResultActivityRequest,
    StageGraphResultActivityResult,
    StageGraphRunInput,
)
from app.domain.orchestration.interpreter import StageGraphInterpreter
from app.domain.run_control.contracts import RunOutcome
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 11, 12, 0, tzinfo=UTC)
QUEUE = "wp-bp-010-temporal"


def _exact(kind: DefinitionKind, logical_id: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(kind=kind, logical_id=logical_id, revision=1, digest=DIGEST)


def _slot() -> StageOperationSlot:
    return StageOperationSlot(
        operation_slot_id="execute",
        reservation={"operation.attempts": 1},
        allowed_variants=(
            AllowedOperationVariant(
                operation_variant_id="default",
                operation_contract_ref="operation:temporal-fixture@1",
            ),
        ),
    )


def _blueprint(
    *,
    cancel_slow_sibling: bool = False,
    workflow_cycles: int = 0,
    stage_cycles: int = 0,
    workflow_wait: bool = False,
) -> StageGraphBlueprint:
    dependencies = (
        StageDependency(
            dependency_id="fast-to-downstream",
            consumer_stage_id="downstream",
            join_id="first-result",
            producer_stage_id="fast",
            producer_output_slot_id="result",
            consumer_input_slot_id="fast-input",
            dependency_class="required",
        ),
        StageDependency(
            dependency_id="slow-to-downstream",
            consumer_stage_id="downstream",
            join_id="first-result",
            producer_stage_id="slow",
            producer_output_slot_id="result",
            consumer_input_slot_id="slow-input",
            dependency_class="required",
        ),
    )
    return StageGraphBlueprint(
        logical_id="wp-bp-010-temporal",
        title="Temporal incremental StageGraph",
        description="A deterministic fast/slow any-join fixture.",
        stages=(
            StageNode(
                stage_id="fast",
                output_slots=(
                    StageOutputSlot(
                        output_slot_id="result",
                        output_contract_ref="output:fast@1",
                    ),
                ),
                operation_slots=(_slot(),),
            ),
            StageNode(
                stage_id="slow",
                output_slots=(
                    StageOutputSlot(
                        output_slot_id="result",
                        output_contract_ref="output:slow@1",
                    ),
                ),
                operation_slots=(_slot(),),
            ),
            StageNode(
                stage_id="downstream",
                input_slots=(
                    StageInputSlot(input_slot_id="fast-input"),
                    StageInputSlot(input_slot_id="slow-input"),
                ),
                output_slots=(
                    StageOutputSlot(
                        output_slot_id="result",
                        output_contract_ref="output:downstream@1",
                    ),
                ),
                operation_slots=(_slot(),),
                stage_cycle_policy=(
                    StageCyclePolicy(
                        max_cycles=stage_cycles,
                        evaluation_contract_ref="evaluation:temporal-fixture@1",
                        objective_contract_ref="objective:temporal-fixture@1",
                        reservation={"stage.cycles": 1},
                    )
                    if stage_cycles
                    else None
                ),
            ),
        ),
        joins=(
            StageJoin(
                consumer_stage_id="downstream",
                join_id="first-result",
                kind="any",
                dependency_ids=tuple(item.dependency_id for item in dependencies),
                slow_sibling_policy=SlowSiblingPolicy(
                    triggers=("join_released",),
                    execution_action=(
                        "request_cancel" if cancel_slow_sibling else "continue"
                    ),
                    arrival_route="evaluate_late_result",
                ),
            ),
        ),
        dependencies=dependencies,
        fairness_groups=(FairnessGroup(group_id="default", weight=1),),
        late_result_policy=LateResultPolicy(
            rules=(
                LateResultRule(
                    rule_id="admit-late-sibling",
                    trigger="consumer_already_admitted",
                    decision="admit",
                ),
            )
        ),
        workflow_evaluation_contract_ref=(
            "evaluation:temporal-fixture@1" if workflow_cycles else None
        ),
        workflow_cycle_policy=(
            WorkflowCyclePolicy(
                max_cycles=workflow_cycles,
                evaluation_contract_ref="evaluation:temporal-fixture@1",
                objective_contract_ref="objective:temporal-fixture@1",
                reservation={"workflow.cycles": 1},
            )
            if workflow_cycles
            else None
        ),
        waits=(
            (
                StageGraphWait(
                    scope_kind="workflow",
                    scope_id="wp-bp-010-temporal",
                    wait_id="release-workflow",
                ),
            )
            if workflow_wait
            else ()
        ),
    )


def _operation(request: StageGraphAdmissionActivityRequest) -> OperationWorkflowRequest:
    proposal = request.proposal
    identity = OperationAttemptIdentity(
        run_id=request.run_id,
        operation_id=proposal.identity.operation_id,
        operation_attempt=proposal.identity.semantic_attempt,
    )
    content = f"Execute StageGraph stage {proposal.identity.stage_id}."
    operation = OperationExecutionRequest(
        identity=identity,
        request_scope=request.request_scope,
        effective_configuration_digest=request.effective_configuration_digest,
        run_control_revision=request.projection.run_version + 1,
        operation_contract_ref="operation:temporal-fixture@1",
        prompt_segments=(
            PromptSegment(
                source_ref="prompt:stagegraph-temporal@1",
                source_revision=1,
                trust_class=PromptTrustClass.SYSTEM_AUTHORITY,
                content=content,
                rendered_digest=sha256_digest(content),
            ),
        ),
        model_policy=ModelPolicy(provider="fixture", model="deterministic", max_turns=1),
        agent_profile_ref=_exact(DefinitionKind.AGENT_PROFILE, "fixture.stagegraph"),
        capability_grant=CapabilityGrant(capabilities=frozenset()),
        workspace=WorkspaceContract(
            namespace_id=f"run/{request.run_id}",
            workspace_id=f"workspace:{proposal.identity.semantic_key}",
            provider="fixture",
            template_ref=_exact(DefinitionKind.WORKSPACE_TEMPLATE, "fixture.workspace"),
            exclusive_write_paths=(f"/stages/{proposal.identity.stage_id}",),
            runtime_digest=DIGEST,
            image_digest=DIGEST,
            package_digest=DIGEST,
            environment_digest=DIGEST,
        ),
        native_placement=NativeOperationExecutionPlacement.create(
            placement_id="native.stagegraph.fixture",
            revision=1,
            task_queue=QUEUE,
            qualification_refs=("QUAL-BP-STAGEGRAPH-SEMANTICS-RECOVERY",),
        ),
        budget_reservation_id=proposal.reservation_id,
        budget_limits=proposal.reservation,
        tracing_policy_ref="tracing:test@1",
        sensitive_data_policy_ref="sensitive:test@1",
        snapshot_policy_ref="snapshot:test@1",
        requested_at=NOW,
        idempotency_key=f"stagegraph:{identity.semantic_key}",
    )
    return OperationWorkflowRequest(
        semantic_attempt_id=identity.semantic_key,
        execution_generation=proposal.identity.execution_generation,
        operation_kind="bound_operation",
        operation=operation,
        timeout_seconds=120,
    )


class FakeStageGraphActivities:
    def __init__(
        self, *, cycle_once: bool = False, cycle_scope: str = "workflow"
    ) -> None:
        self.slow_started = asyncio.Event()
        self.initialized = asyncio.Event()
        self.slow_release = asyncio.Event()
        self.slow_completed = asyncio.Event()
        self.slow_cancelled = asyncio.Event()
        self.downstream_started = asyncio.Event()
        self.admission_order: list[str] = []
        self.result_decisions: list[tuple[str, str]] = []
        self._cycle_once = cycle_once
        self._cycle_scope = cycle_scope
        self._cycle_emitted = False

    @activity.defn(name="stagegraph.initialize")
    async def initialize(self, request: StageGraphInitializeRequest) -> StageGraphInitializeResult:
        self.initialized.set()
        return StageGraphInitializeResult(
            accepted=True,
            projection=request.initial_projection.__class__(
                **{**request.initial_projection.__dict__, "run_version": 2}
            ),
            reason_code="accepted",
        )

    @activity.defn(name="stagegraph.admit_operation")
    async def admit(
        self, request: StageGraphAdmissionActivityRequest
    ) -> StageGraphAdmissionActivityResult:
        interpreter = StageGraphInterpreter(
            StageGraphBlueprint.model_validate(request.blueprint),
            effective_max_concurrency=request.effective_max_concurrency,
        )
        operation = _operation(request)
        projection = interpreter.apply_admission(
            request.projection,
            request.proposal,
            next_run_version=request.projection.run_version + 1,
            next_family_version=request.projection.family_version + 1,
        )
        self.admission_order.append(request.proposal.identity.stage_id)
        return StageGraphAdmissionActivityResult(
            accepted=True,
            projection=projection,
            operation=operation,
            reason_code="accepted",
        )

    @activity.defn(name="stagegraph.decide_result")
    async def decide(
        self, request: StageGraphResultActivityRequest
    ) -> StageGraphResultActivityResult:
        interpreter = StageGraphInterpreter(
            StageGraphBlueprint.model_validate(request.blueprint),
            effective_max_concurrency=request.effective_max_concurrency,
        )
        proposal = interpreter.result_decision(
            request.observation.identity,
            request.late_facts,
            operation_disposition=request.observation.operation_disposition,
        )
        self.result_decisions.append(
            (request.observation.identity.semantic_key, proposal.decision.value)
        )
        projection = interpreter.apply_result_decision(
            request.projection,
            request.observation,
            proposal,
            next_run_version=request.projection.run_version + 1,
            next_family_version=request.projection.family_version + 1,
        )
        return StageGraphResultActivityResult(
            accepted=True,
            projection=projection,
            proposal=proposal,
            reason_code="accepted",
        )

    @activity.defn(name="stagegraph.complete")
    async def complete(
        self, request: StageGraphCompletionActivityRequest
    ) -> StageGraphCompletionActivityResult:
        assert request.proposal.can_terminalize
        return StageGraphCompletionActivityResult(
            accepted=True,
            terminal_outcome=RunOutcome.COMPLETED,
            resulting_run_version=request.projection.run_version + 1,
            reason_code="accepted",
        )

    @activity.defn(name="stagegraph.apply_cycle")
    async def apply_cycle(
        self, request: StageGraphCycleActivityRequest
    ) -> StageGraphCycleActivityResult:
        interpreter = StageGraphInterpreter(
            StageGraphBlueprint.model_validate(request.blueprint),
            effective_max_concurrency=request.effective_max_concurrency,
        )
        if request.cycle_scope == "stage":
            assert request.stage_id is not None
            proposal = interpreter.stage_invalidation(
                request.projection,
                stage_id=request.stage_id,
                next_objective=request.next_objective,
            )
            projection = interpreter.apply_stage_invalidation(
                request.projection,
                proposal,
                next_run_version=request.projection.run_version + 1,
                next_family_version=request.projection.family_version + 1,
            )
        else:
            proposal = interpreter.workflow_invalidation(
                request.projection,
                invalidation_frontier=request.invalidation_frontier,
                next_objective=request.next_objective,
            )
            projection = interpreter.apply_workflow_invalidation(
                request.projection,
                proposal,
                next_run_version=request.projection.run_version + 1,
                next_family_version=request.projection.family_version + 1,
            )
        return StageGraphCycleActivityResult(
            accepted=True,
            projection=projection,
            proposal=proposal,
            reason_code="accepted",
        )

    @activity.defn(name="operation.execute")
    async def execute_operation(self, request: dict[str, Any]) -> dict[str, Any]:
        operation_id = str(request["identity"]["operation_id"])
        if ":stage:slow:" in operation_id:
            self.slow_started.set()
            try:
                await self.slow_release.wait()
            except asyncio.CancelledError:
                self.slow_cancelled.set()
                raise
            self.slow_completed.set()
            stage_id = "slow"
        elif ":stage:downstream:" in operation_id:
            self.downstream_started.set()
            stage_id = "downstream"
        else:
            await self.slow_started.wait()
            stage_id = "fast"
        result: dict[str, Any] = {"output_refs": [f"artifact:{stage_id}"]}
        if stage_id == "downstream" and self._cycle_once and not self._cycle_emitted:
            self._cycle_emitted = True
            result.update(
                {
                    "evaluation": "cycle",
                    "evaluation_ref": "evaluation:evidence:cycle-1",
                    "evaluation_contract_ref": "evaluation:temporal-fixture@1",
                    "objective_contract_ref": "objective:temporal-fixture@1",
                    "next_objective": "Repair only the downstream result.",
                    "invalidation_frontier": ["downstream"],
                    "cycle_scope": self._cycle_scope,
                }
            )
        return result

    @property
    def functions(self) -> list[object]:
        return [
            self.initialize,
            self.admit,
            self.decide,
            self.apply_cycle,
            self.complete,
            self.execute_operation,
        ]


def _run_input(graph: StageGraphBlueprint) -> StageGraphRunInput:
    return StageGraphRunInput(
        run_id="run-wp-bp-010-temporal",
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        workflow_type_digest=DIGEST,
        blueprint_digest=sha256_digest(graph),
        blueprint=graph.model_dump(mode="json"),
        initial_run_version=1,
        max_concurrency=3,
        task_timeout_seconds=10,
        correlation_id="stagegraph:temporal-test",
        semantic_input_binding_ref="semantic-input:test",
    )


class RecordingOperationBindings:
    def __init__(self) -> None:
        self.bindings: list[object] = []

    async def create_binding(self, binding: Any, *, request_scope: str) -> Any:
        self.bindings.append(binding)
        return binding


@pytest.mark.asyncio
async def test_real_materializer_persists_exact_operation_child_intent() -> None:
    graph = _blueprint()
    interpreter = StageGraphInterpreter(graph, effective_max_concurrency=3)
    projection = interpreter.initial_projection(
        identity=ExecutionIdentity("run-wp-bp-010-temporal"),
        run_version=2,
    )
    proposal = replace(
        interpreter.frontier(projection, available_concurrency=3)[0],
        objective_override="Repair only the accepted stage evidence.",
    )
    request = StageGraphAdmissionActivityRequest(
        run_id="run-wp-bp-010-temporal",
        request_scope="tenant-1",
        projection=projection,
        proposal=proposal,
        operation=None,
        blueprint=graph.model_dump(mode="json"),
        effective_max_concurrency=3,
        occurred_at=NOW,
        idempotency_issuer="stagegraph-worker",
        correlation_id="stagegraph:materializer-test",
        semantic_input_binding_ref="semantic-input:test",
        effective_configuration_digest=DIGEST,
    )
    template = _operation(request).operation
    repository = RecordingOperationBindings()
    service = StageGraphOperationPreparationService(
        templates=StaticStageGraphOperationTemplateProvider(
            {proposal.operation_request_key: template}
        ),
        operation_bindings=repository,  # type: ignore[arg-type]
    )

    prepared = await service.materialize(request)

    assert prepared.semantic_attempt_id == proposal.identity.semantic_key
    assert prepared.operation.run_control_revision == projection.run_version + 1
    assert prepared.operation.budget_reservation_id == proposal.reservation_id
    assert prepared.operation.prompt_segments[-1].content == proposal.objective_override
    assert len(repository.bindings) == 1


@pytest.mark.asyncio
async def test_incremental_any_join_runs_downstream_before_slow_sibling_and_replays() -> None:
    activities = FakeStageGraphActivities()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    workflow_id = "family/run-wp-bp-010-temporal/1"
    async with environment:
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            handle = await environment.client.start_workflow(
                StageGraphWorkflow.run,
                _run_input(_blueprint()),
                id=workflow_id,
                task_queue=QUEUE,
            )
            await asyncio.wait_for(activities.downstream_started.wait(), timeout=20)
            assert not activities.slow_completed.is_set()
            activities.slow_release.set()
            result = await handle.result()
            history = await handle.fetch_history()

        await Replayer(
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(history)

    assert activities.admission_order.index("downstream") < len(
        activities.admission_order
    )
    assert result.output_refs["downstream"] == ("artifact:downstream",)
    assert result.completion_proposal.can_terminalize


@pytest.mark.asyncio
async def test_slow_sibling_cancellation_is_reconciled_before_completion() -> None:
    activities = FakeStageGraphActivities()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            result = await environment.client.execute_workflow(
                StageGraphWorkflow.run,
                _run_input(_blueprint(cancel_slow_sibling=True)),
                id="family/run-wp-bp-010-slow-cancel/1",
                task_queue=QUEUE,
            )

    assert activities.slow_cancelled.is_set()
    assert not activities.slow_completed.is_set()
    assert result.output_refs["downstream"] == ("artifact:downstream",)
    assert result.completion_proposal.can_terminalize


@pytest.mark.asyncio
async def test_continue_as_new_preserves_accepted_projection_and_semantic_identity() -> None:
    activities = FakeStageGraphActivities()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    run_input = _run_input(_blueprint())
    run_input = run_input.__class__(**{**run_input.__dict__, "force_continue_as_new": True})
    async with environment:
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            handle = await environment.client.start_workflow(
                StageGraphWorkflow.run,
                run_input,
                id="family/run-wp-bp-010-continuation/1",
                task_queue=QUEUE,
            )
            await asyncio.wait_for(activities.downstream_started.wait(), timeout=20)
            activities.slow_release.set()
            result = await handle.result()

    assert activities.admission_order.count("fast") == 1
    assert activities.admission_order.count("slow") == 1
    assert activities.admission_order.count("downstream") == 1
    assert result.execution_epoch == 1
    assert result.output_refs["downstream"] == ("artifact:downstream",)


@pytest.mark.asyncio
async def test_bounded_workflow_cycle_reuses_producers_and_advances_cycle_identity() -> None:
    activities = FakeStageGraphActivities(cycle_once=True)
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            handle = await environment.client.start_workflow(
                StageGraphWorkflow.run,
                _run_input(_blueprint(workflow_cycles=1)),
                id="family/run-wp-bp-010-cycle/1",
                task_queue=QUEUE,
            )
            await asyncio.wait_for(activities.downstream_started.wait(), timeout=20)
            activities.slow_release.set()
            result = await handle.result()

    assert activities.admission_order.count("fast") == 1
    assert activities.admission_order.count("slow") == 1
    assert activities.admission_order.count("downstream") == 2
    assert result.workflow_cycles == 1
    assert any("workflow-cycle:1" in item for item in result.schedule_trace)
    assert any(
        "workflow-cycle:1" in identity and decision == "admit"
        for identity, decision in activities.result_decisions
    )
    assert result.reused_output_refs


@pytest.mark.asyncio
async def test_bounded_stage_cycle_has_distinct_identity_and_preserves_workflow_ordinal() -> None:
    activities = FakeStageGraphActivities(cycle_once=True, cycle_scope="stage")
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")
    async with environment:
        async with Worker(
            environment.client,
            task_queue=QUEUE,
            workflows=[StageGraphWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            handle = await environment.client.start_workflow(
                StageGraphWorkflow.run,
                _run_input(_blueprint(stage_cycles=1)),
                id="family/run-wp-bp-010-stage-cycle/1",
                task_queue=QUEUE,
            )
            await asyncio.wait_for(activities.downstream_started.wait(), timeout=20)
            activities.slow_release.set()
            result = await handle.result()

    assert activities.admission_order.count("downstream") == 2
    assert result.workflow_cycles == 0
    assert any("stage-cycle:1" in item for item in result.schedule_trace)
    assert any(
        "stage-cycle:1" in identity and decision == "admit"
        for identity, decision in activities.result_decisions
    )
    assert result.reused_output_refs
