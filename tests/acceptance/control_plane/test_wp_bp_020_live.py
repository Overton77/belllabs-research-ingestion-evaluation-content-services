from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import Any, cast

import httpx
import pytest
from fastapi import FastAPI
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from pydantic import TypeAdapter
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.api.control_plane import ControlPlanePrincipal, get_control_plane_principal
from app.api.run_control import get_run_control_service, router
from app.application.operations.operation_execution import (
    InMemoryOperationBindingRepository,
    bind_operation_execution_request,
)
from app.application.orchestration.goal_directed import (
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
    configure_goal_directed_family_admissions,
)
from app.application.orchestration.service import (
    RunControlLifecycleGateway,
    orchestration_lifecycle_actor,
)
from app.application.run_control.run_control_repository import InMemoryRunControlRepository
from app.application.run_control.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
    RunControlService,
)
from app.config import Settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import GoalDirectedBlueprint, SecretRef
from app.domain.operation_execution.contracts import (
    MaterializedWorkspace,
    OperationExecutionRequest,
    OperationExecutionResult,
    RuntimeInvocation,
    StructuredOutputBinding,
)
from app.domain.orchestration.contracts import (
    BellLabsRunInput,
    GoalDirectedRunInput,
    GoalRevision,
    LifecycleCommandOutcome,
    LifecycleCommandRequest,
)
from app.domain.orchestration.goal_directed_runtime import (
    GoalExecutorObservation,
    GoalVerifierObservation,
)
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    DecisionStatus,
    RunPhase,
    VerifiedRunConfiguration,
)
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    DockerSandboxFactory,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    OpenAIExactModelFactory,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from tests.acceptance.control_plane.test_wp_bp_020_sandbox_rollover import (
    Documents,
    SandboxRolloverActivities,
    Templates,
)
from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture
from tests.unit.operations.test_operation_execution import operation_request
from tests.unit.run_control.test_run_control import request as run_request

QUEUE = "wp-bp-020-live-family"
COGNITIVE_QUEUE = "agent-cognitive"
OBLIGATION_REF = "fixture-obligation"
OUTPUT_CONTRACT = "fixture-output"
LIVE_MODEL = "gpt-5.6-luna"
BIOTECH_ARTIFACT = "company=Moderna, product=Spikevax, modality=mRNA vaccine."


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
            max_concurrency=1,
            input_admission_contract="contract:input@1",
            invariant_refs=frozenset({"contract:invariant@1"}),
            obligation_revision="goal-directed-live:1",
            required_obligation_refs=frozenset({OBLIGATION_REF}),
        )


class ExactBindingVerifier:
    def __init__(self, configuration_digest: str, blueprint_digest: str) -> None:
        self._configuration_digest = configuration_digest
        self._blueprint_digest = blueprint_digest

    async def verify(self, configuration_digest: str, blueprint_digest: str) -> None:
        if (
            configuration_digest != self._configuration_digest
            or blueprint_digest != self._blueprint_digest
        ):
            raise ValueError("live lifecycle binding drifted")


def _live_run_request() -> Any:
    base = run_request(request_id="wp-bp-020-live", hard_cap=200_000)
    bounded = {
        "tokens.total": 200_000,
        "model.turns": 40,
        "goal.iterations": 4,
        "operation.attempts": 8,
        "concurrency.slots": 1,
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
        update={"budget_envelope": BudgetEnvelope(dimensions=dimensions)}
    )


def _live_blueprint() -> GoalDirectedBlueprint:
    from app.domain.control_plane.fixtures import GENERIC_GOAL_DIRECTED

    session = GENERIC_GOAL_DIRECTED.session_policy.model_copy(
        update={
            "fresh_agent_token_threshold": 1,
            "handoff_token_reserve": 1,
            "max_rollovers": 1,
        }
    )
    return GoalDirectedBlueprint.model_validate(
        {
            **GENERIC_GOAL_DIRECTED.model_dump(mode="python"),
            "max_iterations": 2,
            "session_policy": session,
            "authority_ceiling": {
                **GENERIC_GOAL_DIRECTED.authority_ceiling.model_dump(mode="python"),
                "budgets": {
                    "dimensions": {
                        "goal.iterations": 1,
                        "tokens.total": 30_000,
                        "model.turns": 10,
                    }
                },
            },
            "iteration_reservation": {
                "goal.iterations": 1,
                "tokens.total": 30_000,
                "model.turns": 10,
            },
        }
    )


def _live_revision() -> GoalRevision:
    values = {
        "schema_version": "belllabs.goal-revision.v1",
        "revision_id": "goal-revision:wp-bp-020-live-biotech",
        "revision": 1,
        "parent_revision_id": None,
        "envelope_digest": sha256_digest("wp-bp-020-live-biotech-envelope"),
        "objective": (
            "Create and independently verify a sandbox record linking Moderna to "
            "Spikevax and identifying it as an mRNA vaccine."
        ),
        "tactical_changes": (),
        "evidence_refs": ("input:wp-bp-020-live-biotech",),
        "unmet_obligations": (OBLIGATION_REF,),
        "proposer": "application:qualification",
        "deciding_authority": "authority:qualification",
        "applicability": "remaining_run",
        "tactics": (),
        "subgoals": (),
        "coverage_emphasis": (),
    }
    return GoalRevision(canonical_digest=sha256_digest(values), **values)  # type: ignore[arg-type]


class LiveGoalActivities(SandboxRolloverActivities):
    def __init__(
        self,
        *,
        run_control: RunControlService,
        configuration_digest: str,
        blueprint: GoalDirectedBlueprint,
        openai_key: str,
        model_name: str,
        workspace_root: Path,
    ) -> None:
        self.documents = Documents()
        self.models = []
        self.operation_ids: list[str] = []
        self.provider_run_ids: list[str] = []
        self._secrets = {"environment:OPENAI_API_KEY": openai_key}
        deep_binding, _profile, bundle = exact_fixture(
            model_name=model_name,
            model_settings={
                "reasoning_effort": "low",
                "verbosity": "low",
                "max_completion_tokens": 2_000,
                "use_responses_api": True,
            },
            sandbox_backend="docker",
        )
        schemas = {
            "executor": TypeAdapter(GoalExecutorObservation).json_schema(),
            "verifier": TypeAdapter(GoalVerifierObservation).json_schema(),
        }
        template_values: dict[str, OperationExecutionRequest] = {}
        schema_registry: dict[str, dict[str, Any]] = {}
        for role in ("executor", "verifier"):
            schema_digest = sha256_digest(f"goal-{role}-live-observation")
            schema_registry[schema_digest] = schemas[role]
            instruction = (
                "You are the executor. Use the execute tool before answering. On iteration 1, "
                "write exactly 'company=Moderna, product=Spikevax, modality=mRNA vaccine.' "
                "to /goal/1/executor/work/artifact.txt, then return completion_claim=false. "
                "Return a concise complete handoff draft with artifact and evidence refs; the host "
                "will bind protected facts, policies, workspace, snapshots, and source digests. "
                "On iteration 2, "
                "read /goal/2/executor/work/artifact.txt, return completion_claim=true, preserve "
                "output_refs=[\"artifact:/goal/1/executor/work/artifact.txt\"], and omit handoff. "
                "Always cite an evidence ref and use output_contract_ref fixture-output. "
                "After the tool result, immediately submit the required structured response; "
                "never finish with plain text."
                if role == "executor"
                else
                "You are an independent verifier. Use the execute tool before answering and read "
                "the shared artifact. On iteration 1, read /goal/1/verifier/work/artifact.txt "
                "and return decision=\"rejected\", accepted_obligation_refs=[], "
                "unmet_obligations=[\"fixture-obligation\"]. On iteration 2, read "
                "/goal/2/verifier/input/artifact.txt; if it contains Moderna, Spikevax, and mRNA "
                "vaccine, return decision=\"accepted\", "
                "accepted_obligation_refs=[\"fixture-obligation\"], unmet_obligations=[], and "
                "obligation_applicability=[[\"fixture-obligation\", true]]. Always use "
                "output_contract_ref=\"fixture-output\". After the tool result, immediately "
                "submit the required structured response; never finish with plain text."
            )
            base = operation_request(prompt=instruction)
            template_values[role] = OperationExecutionRequest.model_validate(
                {
                    **base.model_dump(mode="python"),
                    "execution_runtime": "deep_agent",
                    "native_placement": None,
                    "deep_agent_binding": deep_binding,
                    "secret_refs": (SecretRef(provider="environment", key="OPENAI_API_KEY"),),
                    "output_schema": StructuredOutputBinding(
                        schema_id=f"goal-{role}-observation",
                        revision=1,
                        schema_digest=schema_digest,
                    ),
                }
            )
        bindings = InMemoryOperationBindingRepository()
        self.preparer = GoalDirectedOperationPreparationService(
            templates=Templates(template_values),
            operation_bindings=bindings,
            run_control=run_control,
            documents=self.documents,
            actor=ActorContext(
                actor_id="goal-directed-live-worker",
                permissions=frozenset(
                    {
                        "workflow_run.goal_directed",
                        "workflow_run.reserve_budget",
                    }
                ),
                authority_refs=frozenset({"authority:goal-directed-live-worker"}),
            ),
        )
        self.reconciler = GoalDirectedOperationResultService(self.documents)
        self._lifecycle = RunControlLifecycleGateway(
            run_control,
            ExactBindingVerifier(configuration_digest, sha256_digest(blueprint)),
            orchestration_lifecycle_actor(),
        )
        registry = ExactComponentRegistry(
            model_factories={deep_binding.model.ref.digest: OpenAIExactModelFactory()},
            skill_bundles={bundle.bundle_digest: bundle},
            sandbox_factories={
                deep_binding.sandbox.ref.digest: DockerSandboxFactory(
                    workspace_root=workspace_root
                )
            },
            checkpointers={deep_binding.checkpointer_ref.digest: InMemorySaver()},
            stores={deep_binding.store_ref.digest: InMemoryStore()},
            structured_output_schemas=schema_registry,
        )
        self.adapter = DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry))

    @activity.defn(name="operation.execute")
    async def execute(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = OperationExecutionRequest.model_validate(payload)
        binding = bind_operation_execution_request(request)
        result = await self.adapter.execute(
            RuntimeInvocation(
                binding=binding,
                prompt_segments=request.prompt_segments,
                workspace=MaterializedWorkspace(
                    workspace_id=request.workspace.workspace_id,
                    namespace_id=request.workspace.namespace_id,
                    provider=request.workspace.provider,
                    runtime_digest=request.workspace.runtime_digest,
                    image_digest=request.workspace.image_digest,
                    mount_manifest_digest=sha256_digest("goal-directed-live-mounts"),
                ),
                resolved_secret_names=("environment:OPENAI_API_KEY",),
            ),
            self._secrets,
        )
        self.operation_ids.append(request.identity.semantic_key)
        if result.provider_run_id:
            self.provider_run_ids.append(result.provider_run_id)
        return OperationExecutionResult(
            binding_id=binding.binding_id,
            semantic_attempt_key=binding.semantic_attempt_key,
            status="completed",
            output_text=result.output_text,
            structured_output=result.structured_output,
            output_refs=result.output_refs,
            usage=result.usage,
        ).model_dump(mode="json")

    @activity.defn(name="goaldirected.apply_lifecycle_command")
    async def lifecycle(
        self, request: LifecycleCommandRequest
    ) -> LifecycleCommandOutcome:
        return await self._lifecycle.execute(request)


@pytest.mark.asyncio
async def test_live_api_root_goal_directed_rollover_deep_agents_docker_vertical(
    tmp_path: Path,
) -> None:
    if os.getenv("BELLABS_RUN_WP_BP_020_LIVE") != "1":
        pytest.skip("set BELLABS_RUN_WP_BP_020_LIVE=1 for external qualification")
    settings = Settings()
    openai_key = settings.openai_api_key.get_secret_value().strip()
    if not openai_key:
        pytest.skip("OpenAI credentials are required")

    policies = AdmissionPolicyRegistry()
    policies.register("contract:input@1", lambda _request, _configuration: None)
    policies.register("contract:invariant@1", lambda _request, _configuration: None)
    families = FamilyAdmissionRegistry()
    configure_goal_directed_family_admissions(families)
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
    request = _live_run_request()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=application),
        base_url="http://belllabs.test",
    ) as client:
        response = await client.post(
            "/run-control/v1/run-requests",
            json=request.model_dump(mode="json"),
        )
    assert response.status_code == 201, response.text
    admission = response.json()
    assert admission["status"] == DecisionStatus.ACCEPTED
    run_id = cast(str, admission["run_id"])

    blueprint = _live_blueprint()
    revision = _live_revision()
    run_input = GoalDirectedRunInput(
        run_id=run_id,
        request_scope="tenant-1",
        effective_configuration_digest=request.effective_configuration_digest,
        blueprint_digest=sha256_digest(blueprint),
        blueprint=blueprint.model_dump(mode="json"),
        envelope_digest=revision.envelope_digest,
        initial_revision=revision,
        initial_run_version=1,
        task_timeout_seconds=90,
        required_obligation_refs=(OBLIGATION_REF,),
        required_output_contract_refs=(OUTPUT_CONTRACT,),
        semantic_input_binding_ref="semantic-input:wp-bp-020-live",
    )
    root_input = BellLabsRunInput(
        schema_version="belllabs.temporal-root.v1",
        run_id=run_id,
        request_scope="tenant-1",
        effective_configuration_digest=request.effective_configuration_digest,
        workflow_type_digest=request.workflow_type_ref.digest,
        family="GoalDirected",
        family_input=asdict(run_input),
        family_task_queue=QUEUE,
    )
    activities = LiveGoalActivities(
        run_control=run_control,
        configuration_digest=request.effective_configuration_digest,
        blueprint=blueprint,
        openai_key=openai_key,
        model_name=LIVE_MODEL,
        workspace_root=tmp_path / "live-goal-workspaces",
    )

    async with await WorkflowEnvironment.start_time_skipping() as environment:
        async with (
            Worker(
                environment.client,
                task_queue=QUEUE,
                workflows=[BellLabsRunWorkflow, GoalDirectedWorkflow, OperationWorkflow],
                workflow_runner=coordinator_workflow_runner(),
                activities=[
                    activities.prepare_executor,
                    activities.prepare_verifier,
                    activities.reconcile,
                    activities.lifecycle,
                ],
            ),
            Worker(
                environment.client,
                task_queue=COGNITIVE_QUEUE,
                activities=[activities.execute],
            ),
        ):
            root_handle = await environment.client.start_workflow(
                BellLabsRunWorkflow.run,
                root_input,
                id=root_input.workflow_id,
                task_queue=QUEUE,
            )
            result_task = asyncio.create_task(root_handle.result())
            done, _pending = await asyncio.wait({result_task}, timeout=120)
            if not done:
                root_history = await root_handle.fetch_history()
                family_history = await environment.client.get_workflow_handle(
                    root_input.family_workflow_id
                ).fetch_history()
                pytest.fail(
                    "live GoalDirected workflow exceeded 120 seconds\n"
                    + _history_tail("root", root_history.events)
                    + "\n"
                    + _history_tail("family", family_history.events)
                )
            try:
                result = await result_task
            except Exception as exc:
                root_history = await root_handle.fetch_history()
                family_history = await environment.client.get_workflow_handle(
                    root_input.family_workflow_id
                ).fetch_history()
                pytest.fail(
                    "live GoalDirected workflow failed: "
                    + _failure_chain(exc)
                    + "\n"
                    + _history_tail("root", root_history.events)
                    + "\n"
                    + _history_tail("family", family_history.events)
                )

    run = await run_control.get_run("tenant-1", run_id)
    print("WP_BP_020_LIVE_RESULT=" + json.dumps(result, sort_keys=True))
    assert run.phase == RunPhase.TERMINAL
    assert result["rollover_count"] == 1
    assert result["goal_iterations"] == 2
    assert result["convergence_proposal"]["action"] == "complete"
    assert len(activities.operation_ids) == 4
    assert any("/executor" in item for item in activities.operation_ids)
    assert any("/verifier" in item for item in activities.operation_ids)
    artifact_paths = list((tmp_path / "live-goal-workspaces").rglob("artifact.txt"))
    assert artifact_paths
    assert artifact_paths[0].read_text(encoding="utf-8") == BIOTECH_ARTIFACT
    print(
        "WP_BP_020_LIVE_EVIDENCE="
        + json.dumps(
            {
                "api_request_id": request.request_id,
                "run_id": run_id,
                "root_workflow_id": root_input.workflow_id,
                "family_workflow_id": root_input.family_workflow_id,
                "operation_ids": activities.operation_ids,
                "provider_run_ids": activities.provider_run_ids,
                "model": LIVE_MODEL,
                "sandbox": "docker",
                "rollover_count": result["rollover_count"],
                "terminal_outcome": run.terminal_outcome,
            },
            sort_keys=True,
        )
    )


def _history_tail(label: str, events: Sequence[Any]) -> str:
    return label + " history tail:\n" + "\n".join(
        f"{event.event_id}: event_type={event.event_type}: {str(event)[:1_000]}"
        for event in events[-12:]
    )


def _failure_chain(exc: BaseException) -> str:
    failures: list[str] = []
    current: BaseException | None = exc
    while current is not None:
        failures.append(f"{type(current).__name__}: {current}")
        current = current.__cause__
    return " <- ".join(failures)
