from __future__ import annotations

import asyncio
from dataclasses import asdict, replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from app.application.goal_directed import (
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    GoalDirectedBlueprint,
)
from app.domain.control_plane.fixtures import GENERIC_GOAL_DIRECTED
from app.domain.operation_execution.contracts import (
    CapabilityGrant,
    ModelPolicy,
    NativeOperationExecutionPlacement,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    OperationWorkflowRequest,
    OperationWorkflowResult,
    PromptSegment,
    PromptTrustClass,
    StructuredOutputBinding,
    WorkspaceContract,
)
from app.domain.orchestration.contracts import (
    GoalDirectedRunInput,
    GoalExecutionResult,
    GoalRevision,
    GoalVerificationResult,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
)
from app.domain.orchestration.goal_directed import GoalDirectedInterpreter
from app.domain.orchestration.goal_directed_runtime import (
    GoalOperationDispatch,
    GoalOperationPreparationRequest,
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
)
from app.domain.run_control.contracts import ActorContext, RunOutcome
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.operation import OperationWorkflow

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 10, 20, 0, tzinfo=UTC)


def _exact(kind: DefinitionKind, logical_id: str) -> ExactDefinitionRef:
    return ExactDefinitionRef(kind=kind, logical_id=logical_id, revision=1, digest=DIGEST)


def _revision() -> GoalRevision:
    values = {
        "schema_version": "belllabs.goal-revision.v1",
        "revision_id": "goal-revision:1",
        "revision": 1,
        "parent_revision_id": None,
        "envelope_digest": sha256_digest("temporal-envelope"),
        "objective": "Produce one independently verified bounded result.",
        "tactical_changes": (),
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
    *,
    blueprint: GoalDirectedBlueprint = GENERIC_GOAL_DIRECTED,
    run_id: str = "run-wp-bp-020-temporal",
) -> GoalDirectedRunInput:
    revision = _revision()
    return GoalDirectedRunInput(
        run_id=run_id,
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        blueprint_digest=sha256_digest(blueprint),
        blueprint=blueprint.model_dump(mode="json"),
        envelope_digest=revision.envelope_digest,
        initial_revision=revision,
        initial_run_version=1,
        task_timeout_seconds=10,
        required_obligation_refs=("fixture-obligation",),
        required_output_contract_refs=("fixture-output",),
        semantic_input_binding_ref="semantic-input:test",
    )


def _operation(request: GoalOperationPreparationRequest) -> OperationExecutionRequest:
    operation_id = f"goal-iteration/{request.goal_iteration}/{request.operation_role}"
    identity = OperationAttemptIdentity(
        run_id=request.run_id,
        operation_id=operation_id,
        operation_attempt=request.operation_attempt,
    )
    content = f"Execute exact GoalDirected role {request.operation_role}."
    return OperationExecutionRequest(
        identity=identity,
        request_scope=request.request_scope,
        effective_configuration_digest=request.effective_configuration_digest,
        run_control_revision=request.expected_run_version,
        operation_contract_ref=f"operation:{request.operation_role}@1",
        prompt_segments=(
            PromptSegment(
                source_ref=f"prompt:{request.operation_role}@1",
                source_revision=1,
                trust_class=PromptTrustClass.SYSTEM_AUTHORITY,
                content=content,
                rendered_digest=sha256_digest(content),
            ),
        ),
        model_policy=ModelPolicy(
            provider="fixture",
            model="deterministic",
            max_turns=1,
        ),
        output_schema=StructuredOutputBinding(
            schema_id=f"fixture-{request.operation_role}-output",
            revision=1,
            schema_digest=DIGEST,
        ),
        session_id=request.session_id,
        agent_profile_ref=_exact(DefinitionKind.AGENT_PROFILE, "fixture.agent"),
        capability_grant=CapabilityGrant(capabilities=frozenset()),
        workspace=WorkspaceContract(
            namespace_id=f"run/{request.run_id}",
            workspace_id=request.workspace_id,
            provider="fixture",
            template_ref=_exact(DefinitionKind.WORKSPACE_TEMPLATE, "fixture.workspace"),
            exclusive_write_paths=(
                f"/goal/{request.goal_iteration}/{request.operation_role}/work",
            ),
            runtime_digest=DIGEST,
            image_digest=DIGEST,
            package_digest=DIGEST,
            environment_digest=DIGEST,
        ),
        native_placement=NativeOperationExecutionPlacement.create(
            placement_id=f"native.goal.{request.operation_role}",
            revision=1,
            task_queue="wp-bp-020-temporal",
            qualification_refs=("QUAL-BP-GOAL-DIRECTED-CONVERGENCE",),
        ),
        budget_reservation_id=request.reservation_id,
        budget_limits=request.reservation,
        tracing_policy_ref="tracing:test@1",
        sensitive_data_policy_ref="sensitive:test@1",
        snapshot_policy_ref="snapshot:test@1",
        requested_at=NOW,
        idempotency_key=(
            f"goal:{identity.semantic_key}:generation:{request.execution_generation}"
        ),
    )


class FakeGoalDirectedActivities:
    def __init__(
        self,
        *,
        slow_operation: bool = False,
        complete_at_iteration: int = 1,
        scope_expansion_route: str | None = None,
    ) -> None:
        self.prepared_roles: list[str] = []
        self.lifecycle_kinds: list[str] = []
        self.operation_started = asyncio.Event()
        self._slow_operation = slow_operation
        self._complete_at_iteration = complete_at_iteration
        self._scope_expansion_route = scope_expansion_route

    async def _prepare(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        self.prepared_roles.append(request.operation_role)
        operation = _operation(request)
        workflow_request = OperationWorkflowRequest(
            semantic_attempt_id=operation.identity.semantic_key,
            execution_generation=request.execution_generation,
            operation_kind="bound_operation",
            operation=operation,
        )
        return GoalOperationDispatch(
            workflow_request=workflow_request,
            operation_binding_ref=f"binding:{request.operation_role}",
            operation_request_digest=sha256_digest(workflow_request),
            resulting_run_version=request.expected_run_version + 1,
            resulting_family_version=request.expected_family_version + 1,
        )

    @activity.defn(name="goaldirected.prepare_executor")
    async def prepare_executor(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        return await self._prepare(request)

    @activity.defn(name="goaldirected.prepare_verifier")
    async def prepare_verifier(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        return await self._prepare(request)

    @activity.defn(name="operation.execute")
    async def execute_operation(self, request: dict[str, Any]) -> dict[str, Any]:
        self.operation_started.set()
        if self._slow_operation:
            await asyncio.sleep(60)
        return {"operation_id": str(request["identity"])}

    @activity.defn(name="goaldirected.reconcile_operation")
    async def reconcile(
        self, request: GoalOperationReconciliationRequest
    ) -> GoalOperationReconciliationResult:
        operation = request.operation_request.operation
        if request.operation_role == "executor":
            result = GoalExecutionResult(
                identity=request.claim.identity,
                disposition="completed",
                operation_identity=operation.identity.semantic_key,
                operation_binding_ref=request.operation_binding_ref,
                session_id=operation.session_id or "",
                workspace_id=operation.workspace.workspace_id,
                writable_paths=operation.workspace.exclusive_write_paths,
                output_refs=("artifact:verified-result",),
                completion_claim=True,
                actual_usage={"goal.iterations": 1},
                evidence_refs=("evidence:executor",),
                output_contract_ref="fixture-output",
            )
            return GoalOperationReconciliationResult(
                operation_role="executor",
                execution_result=result,
                detail_ref="goal-iteration:1",
            )

        accepted = self._scope_expansion_route is None and (
            request.claim.identity.iteration.goal_iteration >= self._complete_at_iteration
        )
        values = {
            "schema_version": "belllabs.goal-verification.v1",
            "verification_id": "verification:1",
            "executor_identity": request.claim.identity,
            "verifier_operation_identity": operation.identity.semantic_key,
            "verifier_binding_ref": request.operation_binding_ref,
            "verifier_policy_binding_ref": (
                GENERIC_GOAL_DIRECTED.verifier_policy.binding_ref
            ),
            "verifier_session_id": operation.session_id or "",
            "verifier_workspace_id": operation.workspace.workspace_id,
            "verifier_writable_paths": operation.workspace.exclusive_write_paths,
            "decision": ("accepted" if accepted else "rejected"),
            "verification_ref": "verification-ref:1",
            "rubric_ref": GENERIC_GOAL_DIRECTED.verifier_policy.rubric_ref,
            "rubric_version": 1,
            "acceptance_contract_ref": GENERIC_GOAL_DIRECTED.acceptance_contract,
            "acceptance_version": 1,
            "progress_made": True,
            "accepted_obligation_refs": (("fixture-obligation",) if accepted else ()),
            "findings": (),
            "evidence_refs": ("evidence:verifier",),
            "admitted_executor_output_refs": ("artifact:verified-result",),
            "admitted_executor_evidence_refs": ("evidence:executor",),
            "unmet_obligations": (() if accepted else ("fixture-obligation",)),
            "obligation_applicability": (("fixture-obligation", True),),
            "stale_frontier_digest": sha256_digest("stale-frontier"),
            "blocker_class": "",
            "authority_breach_ref": "",
            "hard_budget_exhausted_dimensions": (),
            "soft_budget_dimensions": (),
            "irrecoverable_failure_ref": "",
            "proposed_revision": None,
            "scope_expansion_route": self._scope_expansion_route,
            "route_ref": (
                "route:governed-expansion" if self._scope_expansion_route else ""
            ),
            "actual_usage": {"goal.iterations": 1},
            "effect_refs": (),
            "output_contract_ref": "fixture-output",
        }
        draft = GoalVerificationResult(  # type: ignore[arg-type]
            verification_digest="pending",
            **values,
        )
        payload = asdict(draft)
        payload.pop("verification_digest")
        verification = replace(draft, verification_digest=sha256_digest(payload))
        return GoalOperationReconciliationResult(
            operation_role="verifier",
            verification_result=verification,
            detail_ref="goal-verification:1",
        )

    @activity.defn(name="goaldirected.apply_lifecycle_command")
    async def lifecycle(
        self, request: LifecycleCommandRequest
    ) -> LifecycleCommandOutcome:
        kind = str(request.action["kind"])
        self.lifecycle_kinds.append(kind)
        return LifecycleCommandOutcome(
            accepted=True,
            resulting_run_version=request.expected_run_version + 1,
            phase=("terminal" if kind == "terminalize" else "active"),
            reason_code="accepted",
            evidence_frontier_digest=DIGEST,
            obligation_revision=DIGEST,
            accepted_obligation_evidence_digest=DIGEST,
            required_obligations_accepted=True,
            workflow_type_digest=DIGEST,
            terminal_outcome=(RunOutcome.COMPLETED if kind == "terminalize" else None),
        )

    @property
    def functions(self) -> list[object]:
        return [
            self.prepare_executor,
            self.prepare_verifier,
            self.execute_operation,
            self.reconcile,
            self.lifecycle,
        ]


class RecordingGoalDocuments:
    def __init__(self) -> None:
        self.revisions: list[GoalRevision] = []
        self.verifications: list[GoalVerificationResult] = []

    async def persist_revision(
        self,
        request_scope: str,
        run_id: str,
        revision: GoalRevision,
        recorded_at: datetime,
    ) -> str:
        self.revisions.append(revision)
        return "goal-revision:recorded"

    async def persist_verification(
        self,
        request_scope: str,
        run_id: str,
        goal_revision_id: str,
        verification: GoalVerificationResult,
        recorded_at: datetime,
    ) -> str:
        self.verifications.append(verification)
        return "goal-verification:recorded"


class FixedGoalTemplateProvider:
    def __init__(self, template: OperationExecutionRequest) -> None:
        self._template = template

    async def get_template(self, **_: object) -> OperationExecutionRequest:
        return self._template


class RecordingOperationBindings:
    def __init__(self) -> None:
        self.bindings: list[object] = []

    async def create_binding(self, binding, *, request_scope: str):
        self.bindings.append(binding)
        return binding


class AcceptingFamilyRunControl:
    def __init__(self) -> None:
        self.mutations: list[object] = []

    async def execute_family_admission(self, command, mutation):
        self.mutations.append(mutation)
        return SimpleNamespace(
            command_result=SimpleNamespace(resulting_run_version=3),
            family_receipt=SimpleNamespace(family_version=2),
        )


@pytest.mark.asyncio
async def test_real_preparer_persists_revision_before_atomic_operation_admission() -> None:
    run_input = _run_input()
    interpreter = GoalDirectedInterpreter(GENERIC_GOAL_DIRECTED)
    _, claim = interpreter.claim_execution(interpreter.initial_state(run_input))
    request = GoalOperationPreparationRequest(
        request_scope=run_input.request_scope,
        run_id=run_input.run_id,
        effective_configuration_digest=run_input.effective_configuration_digest,
        semantic_input_binding_ref=run_input.semantic_input_binding_ref,
        goal_revision_id=run_input.initial_revision.revision_id,
        goal_revision_digest=run_input.initial_revision.canonical_digest,
        goal_revision=run_input.initial_revision,
        goal_iteration=1,
        operation_role="executor",
        operation_attempt=1,
        execution_generation=1,
        expected_run_version=2,
        expected_family_version=1,
        reservation_id=claim.reservation_id,
        reservation=claim.reservation,
        session_id=claim.session_id,
        workspace_id=claim.workspace_namespace,
        decided_at=NOW,
    )
    documents = RecordingGoalDocuments()
    bindings = RecordingOperationBindings()
    run_control = AcceptingFamilyRunControl()
    service = GoalDirectedOperationPreparationService(
        templates=FixedGoalTemplateProvider(_operation(request)),
        operation_bindings=bindings,  # type: ignore[arg-type]
        run_control=run_control,  # type: ignore[arg-type]
        documents=documents,  # type: ignore[arg-type]
        actor=ActorContext(
            actor_id="goal-directed-worker",
            permissions=frozenset({"workflow_run.goal_directed"}),
        ),
    )
    dispatch = await service.prepare(request)

    assert documents.revisions == [run_input.initial_revision]
    assert len(bindings.bindings) == 1
    assert len(run_control.mutations) == 1
    assert dispatch.resulting_run_version == 3
    assert dispatch.resulting_family_version == 2
    assert dispatch.workflow_request.operation.identity.run_id == run_input.run_id


@pytest.mark.asyncio
async def test_real_reconciler_binds_verifier_authority_and_recomputes_digest() -> None:
    run_input = _run_input()
    interpreter = GoalDirectedInterpreter(GENERIC_GOAL_DIRECTED)
    state, claim = interpreter.claim_execution(interpreter.initial_state(run_input))
    execution = GoalExecutionResult(
        identity=claim.identity,
        disposition="completed",
        operation_identity=f"{claim.identity.iteration.semantic_key}:executor",
        operation_binding_ref="binding:executor",
        session_id=claim.session_id,
        workspace_id=claim.workspace_namespace,
        writable_paths=("/goal/executor/work",),
        output_refs=("artifact:verified-result",),
    )
    state = interpreter.apply_execution_result(state, execution)
    preparation = GoalOperationPreparationRequest(
        request_scope=run_input.request_scope,
        run_id=run_input.run_id,
        effective_configuration_digest=run_input.effective_configuration_digest,
        semantic_input_binding_ref=run_input.semantic_input_binding_ref,
        goal_revision_id=run_input.initial_revision.revision_id,
        goal_revision_digest=run_input.initial_revision.canonical_digest,
        goal_revision=run_input.initial_revision,
        goal_iteration=1,
        operation_role="verifier",
        operation_attempt=1,
        execution_generation=1,
        expected_run_version=2,
        expected_family_version=1,
        reservation_id="reservation:verifier",
        reservation={"goal.iterations": 1},
        session_id=f"{claim.session_id}:verifier",
        workspace_id=f"{claim.workspace_namespace}:verifier",
        verifier_input_refs=execution.output_refs,
        decided_at=NOW,
    )
    operation = _operation(preparation)
    workflow_request = OperationWorkflowRequest(
        semantic_attempt_id=operation.identity.semantic_key,
        operation_kind="bound_operation",
        operation=operation,
    )
    provider_values = {
        "schema_version": "belllabs.goal-verification.v1",
        "verification_id": "verification:provider-observation",
        "executor_identity": claim.identity,
        "verifier_operation_identity": "provider:untrusted-operation",
        "verifier_binding_ref": "provider:untrusted-binding",
        "verifier_policy_binding_ref": "provider:untrusted-policy",
        "verifier_session_id": "provider:untrusted-session",
        "verifier_workspace_id": "provider:untrusted-workspace",
        "verifier_writable_paths": ("/provider/untrusted",),
        "decision": "accepted",
        "verification_ref": "verification-ref:provider",
        "rubric_ref": "provider:untrusted-rubric",
        "rubric_version": 99,
        "acceptance_contract_ref": "provider:untrusted-acceptance",
        "acceptance_version": 99,
        "progress_made": True,
        "accepted_obligation_refs": ("fixture-obligation",),
        "findings": (),
        "evidence_refs": ("evidence:verifier",),
        "unmet_obligations": (),
        "obligation_applicability": (("fixture-obligation", True),),
        "stale_frontier_digest": sha256_digest("stale-frontier"),
        "blocker_class": "",
        "authority_breach_ref": "",
        "hard_budget_exhausted_dimensions": (),
        "soft_budget_dimensions": (),
        "irrecoverable_failure_ref": "",
        "proposed_revision": None,
        "scope_expansion_route": None,
        "route_ref": "",
        "actual_usage": {"goal.iterations": 1},
        "effect_refs": (),
        "output_contract_ref": "fixture-output",
    }
    provider_result = GoalVerificationResult(  # type: ignore[arg-type]
        verification_digest=sha256_digest("provider-untrusted-digest"),
        **provider_values,
    )
    documents = RecordingGoalDocuments()
    service = GoalDirectedOperationResultService(documents)  # type: ignore[arg-type]
    reconciled = await service.reconcile(
        GoalOperationReconciliationRequest(
            request_scope=run_input.request_scope,
            goal_revision_id=run_input.initial_revision.revision_id,
            operation_role="verifier",
            operation_binding_ref="binding:persisted-verifier",
            required_output_contract_refs=("fixture-output",),
            operation_request=workflow_request,
            claim=claim,
            executor_result=execution,
            operation_result=OperationWorkflowResult(
                semantic_attempt_id=workflow_request.semantic_attempt_id,
                execution_generation=1,
                disposition="completed",
                result=asdict(provider_result),
                message_cursor=0,
                effect_frontier=("effect:operation-frontier",),
                active_async_child_ids=("child:still-active",),
            ),
            verifier_policy_binding_ref=GENERIC_GOAL_DIRECTED.verifier_policy.binding_ref,
            verifier_rubric_ref=GENERIC_GOAL_DIRECTED.verifier_policy.rubric_ref,
            verifier_rubric_version=GENERIC_GOAL_DIRECTED.verifier_policy.rubric_version,
            acceptance_contract_ref=GENERIC_GOAL_DIRECTED.acceptance_contract,
            acceptance_version=GENERIC_GOAL_DIRECTED.verifier_policy.acceptance_version,
            recorded_at=NOW,
        )
    )
    verification = reconciled.verification_result
    assert verification is not None
    assert verification.verification_id.startswith("goal-verification:")
    assert verification.verification_id != "verification:provider-observation"
    assert verification.verification_ref.startswith(f"{verification.verification_id}@sha256:")
    assert verification.verifier_binding_ref == "binding:persisted-verifier"
    assert (
        verification.verifier_policy_binding_ref
        == GENERIC_GOAL_DIRECTED.verifier_policy.binding_ref
    )
    assert verification.effect_refs == (
        "effect:operation-frontier",
        "async-child:child:still-active",
    )
    accepted = interpreter.apply_verification(state, verification)
    assert accepted.convergence_proposal is not None
    assert accepted.convergence_proposal.action == "complete"


@pytest.mark.asyncio
async def test_goal_workflow_runs_separate_operation_children_and_replays() -> None:
    activities = FakeGoalDirectedActivities()
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="wp-bp-020-temporal",
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            result = await environment.client.execute_workflow(
                GoalDirectedWorkflow.run,
                _run_input(),
                id="family/run-wp-bp-020-temporal/1",
                task_queue="wp-bp-020-temporal",
            )
            history = await environment.client.get_workflow_handle(
                "family/run-wp-bp-020-temporal/1"
            ).fetch_history()

        await Replayer(
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(history)

    assert activities.prepared_roles == ["executor", "verifier"]
    execution = result.execution_results[0]
    verification = result.verification_results[0]
    assert execution.operation_identity != verification.verifier_operation_identity
    assert execution.operation_binding_ref != verification.verifier_binding_ref
    assert execution.session_id != verification.verifier_session_id
    assert execution.workspace_id != verification.verifier_workspace_id
    assert result.convergence_proposal.action == "complete"
    assert activities.lifecycle_kinds[-1] == "terminalize"


@pytest.mark.asyncio
async def test_goal_workflow_continue_as_new_preserves_semantic_iteration() -> None:
    blueprint = GoalDirectedBlueprint.model_validate(
        {
            **GENERIC_GOAL_DIRECTED.model_dump(mode="python"),
            "max_iterations": 25,
        }
    )
    activities = FakeGoalDirectedActivities(complete_at_iteration=25)
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="wp-bp-020-temporal",
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            result = await environment.client.execute_workflow(
                GoalDirectedWorkflow.run,
                _run_input(
                    blueprint=blueprint,
                    run_id="run-wp-bp-020-continuation",
                ),
                id="family/run-wp-bp-020-continuation/1",
                task_queue="wp-bp-020-temporal",
            )
            history = await environment.client.get_workflow_handle(
                "family/run-wp-bp-020-continuation/1"
            ).fetch_history()

        await Replayer(
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(history)

    assert activities.prepared_roles.count("executor") == 25
    assert activities.prepared_roles.count("verifier") == 25
    assert activities.lifecycle_kinds.count("start") == 1
    assert result.goal_iterations == 25
    assert result.agent_runs == 25
    assert result.lineage_digest
    assert result.verification_results[-1].executor_identity.iteration.goal_iteration == 25
    assert result.convergence_proposal.action == "complete"


@pytest.mark.asyncio
async def test_goal_workflow_returns_governed_scope_expansion_proposal() -> None:
    activities = FakeGoalDirectedActivities(scope_expansion_route="linked_run")
    blueprint = GoalDirectedBlueprint.model_validate(
        {
            **GENERIC_GOAL_DIRECTED.model_dump(mode="python"),
            "max_iterations": 2,
        }
    )
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="wp-bp-020-temporal",
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            result = await environment.client.execute_workflow(
                GoalDirectedWorkflow.run,
                _run_input(
                    blueprint=blueprint,
                    run_id="run-wp-bp-020-route",
                ),
                id="family/run-wp-bp-020-route/1",
                task_queue="wp-bp-020-temporal",
            )

    assert result.convergence_proposal.action == "linked_run"
    assert result.convergence_proposal.route_ref == "route:governed-expansion"
    assert result.terminalization_proposal is None
    assert "terminalize" not in activities.lifecycle_kinds


@pytest.mark.asyncio
async def test_goal_workflow_cancels_active_operation_and_replays_reconciliation() -> None:
    activities = FakeGoalDirectedActivities(slow_operation=True)
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
        async with Worker(
            environment.client,
            task_queue="wp-bp-020-temporal",
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
            activities=activities.functions,
        ):
            handle = await environment.client.start_workflow(
                GoalDirectedWorkflow.run,
                _run_input(),
                id="family/run-wp-bp-020-cancel/1",
                task_queue="wp-bp-020-temporal",
            )
            await asyncio.wait_for(activities.operation_started.wait(), timeout=10)
            await handle.signal(GoalDirectedWorkflow.request_cancel)
            with pytest.raises(WorkflowFailureError):
                await handle.result()
            history = await handle.fetch_history()

        await Replayer(
            workflows=[GoalDirectedWorkflow, OperationWorkflow],
            workflow_runner=coordinator_workflow_runner(),
        ).replay_workflow(history)

    assert "cancel" in activities.lifecycle_kinds
    # Cancellation enters the shared reconciliation saga with reservations retained;
    # terminalization is forbidden until that saga settles budget/effects.
    assert "terminalize" not in activities.lifecycle_kinds
    assert activities.lifecycle_kinds.count("cancel") == 1
