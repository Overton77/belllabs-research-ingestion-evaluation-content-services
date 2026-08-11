from __future__ import annotations

import asyncio
import json
import os
from dataclasses import asdict
from typing import Any, cast

import httpx
import pytest
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from temporalio import activity
from temporalio.api.enums.v1 import EventType
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.api.control_plane import ControlPlanePrincipal, get_control_plane_principal
from app.api.run_control import get_run_control_service, router
from app.application.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    RunControlOperationBudgetAuthority,
    bind_operation_execution_request,
)
from app.application.orchestration.service import (
    StageGraphDecisionService,
    StageGraphOperationPreparationService,
    StaticStageGraphOperationTemplateProvider,
    orchestration_lifecycle_actor,
    register_stagegraph_family_mutations,
)
from app.application.run_control.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
    RunControlService,
)
from app.application.run_control.run_control_repository import InMemoryRunControlRepository
from app.config import Settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    CompletionObligationRef,
    ObligationMatrixRow,
    StageGraphBlueprint,
    WorkflowObligationSlot,
)
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    OperationExecutionRequest,
    RuntimeInvocation,
)
from app.domain.orchestration.contracts import BellLabsRunInput, StageGraphRunInput
from app.domain.run_control.contracts import (
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    DecisionStatus,
    RunPhase,
    VerifiedRunConfiguration,
)
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    OpenAIExactModelFactory,
    StateSandboxFactory,
)
from app.temporal.orchestration_activities import StageGraphActivities
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from app.temporal.workflows.stagegraph import StageGraphWorkflow
from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture
from tests.unit.operations.test_operation_execution import operation_request
from tests.unit.run_control.test_run_control import request as run_request
from tests.integration.temporal.test_wp_bp_010_temporal import _blueprint

LIVE_QUEUE = "wp-bp-010-live-family"
COGNITIVE_QUEUE = "agent-cognitive"
OBLIGATION_REF = "live-stagegraph-evidence"


class LiveConfigurationVerifier:
    async def verify(self, request: Any) -> VerifiedRunConfiguration:
        return VerifiedRunConfiguration(
            effective_configuration_digest=request.effective_configuration_digest,
            workflow_type_ref=request.workflow_type_ref,
            input_manifest=request.input_manifest,
            effective_budget_ceilings={
                item.dimension: cast(int, item.hard_cap)
                for item in request.budget_envelope.dimensions
                if item.applicability == BudgetApplicability.BOUNDED
            },
            max_concurrency=2,
            input_admission_contract="contract:input@1",
            invariant_refs=frozenset({"contract:invariant@1"}),
            obligation_revision="stagegraph-live:1",
            required_obligation_refs=frozenset({OBLIGATION_REF}),
        )


class LiveDeepAgentActivity:
    def __init__(
        self,
        adapter: DeepAgentRuntimeAdapter,
        budget: RunControlOperationBudgetAuthority,
        openai_key: str,
    ) -> None:
        self._adapter = adapter
        self._budget = budget
        self._secrets = {"environment:OPENAI_API_KEY": openai_key}
        self.slow_release = asyncio.Event()
        self.slow_model_completed = asyncio.Event()
        self.slow_returned = asyncio.Event()
        self.downstream_started = asyncio.Event()
        self.real_model_stages: list[str] = []

    @activity.defn(name="operation.execute")
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = OperationExecutionRequest.model_validate(payload)
        operation_id = request.identity.operation_id
        stage_id = next(
            stage
            for stage in ("fast", "slow", "downstream")
            if f":stage:{stage}:" in operation_id
        )
        if stage_id == "downstream":
            self.downstream_started.set()
        binding = bind_operation_execution_request(request)
        result = await self._adapter.execute(
            RuntimeInvocation(
                binding=binding,
                prompt_segments=request.prompt_segments,
                workspace=MaterializedWorkspace(
                    workspace_id=request.workspace.workspace_id,
                    namespace_id=request.workspace.namespace_id,
                    provider=request.workspace.provider,
                    runtime_digest=request.workspace.runtime_digest,
                    image_digest=request.workspace.image_digest,
                    mount_manifest_digest=sha256_digest(
                        f"wp-bp-010-live:{stage_id}:mounts"
                    ),
                ),
                resolved_secret_names=("environment:OPENAI_API_KEY",),
            ),
            self._secrets,
        )
        self.real_model_stages.append(stage_id)
        if stage_id == "slow":
            self.slow_model_completed.set()
            await self.slow_release.wait()
            self.slow_returned.set()
        await self._budget.reconcile(
            binding=binding,
            settlement_id=f"live-settlement:{binding.binding_id}",
            usage=result.usage,
        )
        response = result.model_dump(mode="json")
        response["output_refs"] = [f"artifact:wp-bp-010-live:{stage_id}"]
        if stage_id == "downstream":
            response["obligation_refs"] = [OBLIGATION_REF]
        return response


class UnusedLifecycleGateway:
    async def execute(self, _request: Any) -> Any:
        raise AssertionError("legacy StageGraph lifecycle activity must not execute")


def _live_run_request() -> Any:
    base = run_request(request_id="wp-bp-010-live", hard_cap=100_000)
    bounded = {
        "tokens.total": 100_000,
        "model.turns": 40,
        "operation.attempts": 6,
        "concurrency.slots": 2,
        "external.openai": 20,
    }
    dimensions = tuple(
        BudgetDimensionLimit(
            dimension=item.dimension,
            applicability=BudgetApplicability.BOUNDED,
            hard_cap=bounded[item.dimension],
        )
        if item.dimension in bounded
        else item
        for item in base.budget_envelope.dimensions
    )
    return base.model_copy(
        update={
            "budget_envelope": BudgetEnvelope(
                dimensions=dimensions,
                baseline_reservations={},
            )
        }
    )


def _live_blueprint() -> StageGraphBlueprint:
    base = _blueprint()
    stages = tuple(
        stage.model_copy(
            update={
                "operation_slots": tuple(
                    slot.model_copy(
                        update={
                            "reservation": {
                                "tokens.total": 20_000,
                                "model.turns": 12,
                                "operation.attempts": 1,
                            }
                        }
                    )
                    for slot in stage.operation_slots
                )
            }
        )
        for stage in base.stages
    )
    return StageGraphBlueprint.model_validate(
        {
            **base.model_dump(mode="python"),
            "stages": stages,
            "workflow_obligation_slots": (
                WorkflowObligationSlot(
                    obligation_slot_id="live-completion",
                    obligation_ref=OBLIGATION_REF,
                ),
            ),
            "obligation_matrix": (
                ObligationMatrixRow(
                    obligation_scope="workflow",
                    obligation_slot_id="live-completion",
                    evidence_slot_id=OBLIGATION_REF,
                ),
            ),
            "completion_obligations": (
                CompletionObligationRef(
                    obligation_scope="workflow",
                    obligation_slot_id="live-completion",
                ),
            ),
        }
    )


def _child_history_order(history: Any) -> tuple[int, int]:
    downstream_started = -1
    slow_completed = -1
    for index, event in enumerate(history.events):
        if event.event_type == EventType.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
            workflow_id = (
                event.start_child_workflow_execution_initiated_event_attributes.workflow_id
            )
            if ":stage:downstream:" in workflow_id:
                downstream_started = index
        if event.event_type == EventType.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:
            workflow_id = (
                event.child_workflow_execution_completed_event_attributes.workflow_execution.workflow_id
            )
            if ":stage:slow:" in workflow_id:
                slow_completed = index
    return downstream_started, slow_completed


@pytest.mark.asyncio
async def test_live_api_root_stagegraph_incremental_deep_agents_vertical() -> None:
    if os.getenv("BELLABS_RUN_WP_BP_010_LIVE") != "1":
        pytest.skip("set BELLABS_RUN_WP_BP_010_LIVE=1 for external qualification")
    openai_key = Settings().openai_api_key.get_secret_value().strip()
    if not openai_key:
        pytest.skip("OpenAI credentials are required")

    policies = AdmissionPolicyRegistry()
    policies.register("contract:input@1", lambda _request, _configuration: None)
    policies.register("contract:invariant@1", lambda _request, _configuration: None)
    families = FamilyAdmissionRegistry()
    register_stagegraph_family_mutations(families)
    repository = InMemoryRunControlRepository()
    run_control = RunControlService(
        repository,
        LiveConfigurationVerifier(),
        policies,
        families,
    )
    application = FastAPI()
    application.include_router(router)
    application.state.run_control_service = run_control
    principal = ControlPlanePrincipal(
        actor_id="operator",
        roles=frozenset({"operator"}),
        tenant_scopes=frozenset({"tenant-1"}),
        authority_refs=frozenset({"authority:lifecycle"}),
        sponsorship_refs=frozenset({"sponsorship:test"}),
        approval_refs=frozenset({"approval:test"}),
    )
    application.dependency_overrides[get_control_plane_principal] = lambda: principal
    application.dependency_overrides[get_run_control_service] = lambda: run_control
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://belllabs.test",
    ) as client:
        response = await client.post(
            "/run-control/v1/run-requests",
            json=_live_run_request().model_dump(mode="json"),
        )
    assert response.status_code == 201, response.text
    admission = response.json()
    assert admission["status"] == DecisionStatus.ACCEPTED
    run_id = cast(str, admission["run_id"])

    graph = _live_blueprint()
    deep_binding, _profile, bundle = exact_fixture(
        model_name="gpt-5.6-luna",
        model_settings={
            "reasoning_effort": "low",
            "verbosity": "low",
            "use_responses_api": True,
        },
        sandbox_backend="state",
    )
    from app.domain.control_plane.contracts import SecretRef as BoundSecretRef

    templates: dict[str, OperationExecutionRequest] = {}
    for stage_id in ("fast", "slow", "downstream"):
        base_operation = operation_request(
            prompt=(
                f"WP-BP-010 live StageGraph stage {stage_id}. "
                "Return a concise confirmation that this real model call completed."
            )
        )
        templates[f"{stage_id}/execute/default"] = OperationExecutionRequest.model_validate(
            {
                **base_operation.model_dump(mode="python"),
                "execution_runtime": "deep_agent",
                "native_placement": None,
                "deep_agent_binding": deep_binding,
                "secret_refs": (
                    BoundSecretRef(provider="environment", key="OPENAI_API_KEY"),
                ),
            }
        )
    bindings = InMemoryOperationBindingRepository()
    stage_activities = StageGraphActivities(
        decision_service=StageGraphDecisionService(run_control, repository),
        operation_materializer=StageGraphOperationPreparationService(
            templates=StaticStageGraphOperationTemplateProvider(templates),
            operation_bindings=bindings,
        ),
        lifecycle_gateway=cast(Any, UnusedLifecycleGateway()),
    )
    registry = ExactComponentRegistry(
        model_factories={deep_binding.model.ref.digest: OpenAIExactModelFactory()},
        skill_bundles={bundle.bundle_digest: bundle},
        sandbox_factories={deep_binding.sandbox.ref.digest: StateSandboxFactory()},
        checkpointers={deep_binding.checkpointer_ref.digest: InMemorySaver()},
        stores={deep_binding.store_ref.digest: InMemoryStore()},
    )
    cognitive = LiveDeepAgentActivity(
        DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry)),
        RunControlOperationBudgetAuthority(
            run_control,
            actor=orchestration_lifecycle_actor(),
        ),
        openai_key,
    )
    stage_input = StageGraphRunInput(
        run_id=run_id,
        request_scope="tenant-1",
        effective_configuration_digest=_live_run_request().effective_configuration_digest,
        workflow_type_digest=_live_run_request().workflow_type_ref.digest,
        blueprint_digest=sha256_digest(graph),
        blueprint=graph.model_dump(mode="json"),
        initial_run_version=1,
        max_concurrency=2,
        task_timeout_seconds=300,
        semantic_input_binding_ref="semantic-input:wp-bp-010-live",
        correlation_id=f"wp-bp-010-live:{run_id}",
    )
    root_input = BellLabsRunInput(
        schema_version="belllabs.temporal-root.v1",
        run_id=run_id,
        request_scope="tenant-1",
        effective_configuration_digest=stage_input.effective_configuration_digest,
        workflow_type_digest=stage_input.workflow_type_digest,
        family="StageGraph",
        family_input=asdict(stage_input),
        family_task_queue=LIVE_QUEUE,
    )

    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with (
            Worker(
                environment.client,
                task_queue=LIVE_QUEUE,
                workflows=[BellLabsRunWorkflow, StageGraphWorkflow, OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
                activities=[
                    stage_activities.initialize,
                    stage_activities.admit_operation,
                    stage_activities.decide_result,
                    stage_activities.apply_cycle,
                    stage_activities.complete_stagegraph,
                ],
            ),
            Worker(
                environment.client,
                task_queue=COGNITIVE_QUEUE,
                activities=[cognitive.execute],
            ),
        ):
            root_handle = await environment.client.start_workflow(
                BellLabsRunWorkflow.run,
                root_input,
                id=root_input.workflow_id,
                task_queue=LIVE_QUEUE,
            )
            await asyncio.wait_for(cognitive.slow_model_completed.wait(), timeout=300)
            await asyncio.wait_for(cognitive.downstream_started.wait(), timeout=300)
            assert not cognitive.slow_returned.is_set()
            cognitive.slow_release.set()
            result = await root_handle.result()
            family_handle = environment.client.get_workflow_handle(
                root_input.family_workflow_id
            )
            history = await family_handle.fetch_history()

    downstream_started, slow_completed = _child_history_order(history)
    assert 0 <= downstream_started < slow_completed
    assert set(cognitive.real_model_stages) == {"fast", "slow", "downstream"}
    run = await run_control.get_run("tenant-1", run_id)
    assert run.phase == RunPhase.TERMINAL
    assert OBLIGATION_REF in {
        item.obligation_ref for item in run.accepted_obligation_evidence
    }
    assert {
        "artifact:wp-bp-010-live:fast",
        "artifact:wp-bp-010-live:downstream",
    } <= {item.output_ref for item in run.accepted_output_evidence}
    completion = cast(dict[str, Any], result)["completion_proposal"]
    assert completion["required_obligations_accepted"] is True
    assert completion["pending_dependency_ids"] == []
    assert completion["open_producer_liability_ids"] == []
    print(
        "WP_BP_010_LIVE_EVIDENCE="
        + json.dumps(
            {
                "root_workflow_id": root_input.workflow_id,
                "family_workflow_id": root_input.family_workflow_id,
                "run_id": run_id,
                "model": deep_binding.model.model_name,
                "real_model_stages": sorted(cognitive.real_model_stages),
                "downstream_start_history_index": downstream_started,
                "slow_completion_history_index": slow_completed,
                "terminal_outcome": run.terminal_outcome,
                "accepted_obligations": sorted(
                    item.obligation_ref for item in run.accepted_obligation_evidence
                ),
                "accepted_outputs": sorted(
                    item.output_ref for item in run.accepted_output_evidence
                ),
            },
            sort_keys=True,
            default=str,
        )
    )
