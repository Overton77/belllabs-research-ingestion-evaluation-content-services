from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.store.memory import InMemoryStore
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.application.operations.operation_execution import bind_operation_execution_request
from app.application.orchestration.goal_directed import (
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import GoalDirectedBlueprint
from app.domain.control_plane.fixtures import GENERIC_GOAL_DIRECTED
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
    GoalOperationDispatch,
    GoalOperationPreparationRequest,
    GoalOperationReconciliationRequest,
    GoalOperationReconciliationResult,
)
from app.domain.run_control.contracts import ActorContext, RunOutcome
from app.integrations.agents.deep_agents import (
    DeepAgentRuntimeAdapter,
    DockerSandboxFactory,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
)
from app.temporal.workflow_sandbox import coordinator_workflow_runner
from app.temporal.workflows.belllabs_run import BellLabsRunWorkflow
from app.temporal.workflows.goal_directed import GoalDirectedWorkflow
from app.temporal.workflows.operation import OperationWorkflow
from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture
from tests.unit.operations.test_operation_execution import operation_request

DIGEST = "sha256:" + "a" * 64
QUEUE = "wp-bp-020-sandbox-rollover"


def _revision() -> GoalRevision:
    values = {
        "schema_version": "belllabs.goal-revision.v1",
        "revision_id": "goal-revision:1",
        "revision": 1,
        "parent_revision_id": None,
        "envelope_digest": sha256_digest("sandbox-rollover-envelope"),
        "objective": "Create and independently verify a sandbox filesystem artifact.",
        "tactical_changes": (),
        "evidence_refs": ("input:sandbox-rollover",),
        "unmet_obligations": ("fixture-obligation",),
        "proposer": "application:qualification",
        "deciding_authority": "authority:qualification",
        "applicability": "remaining_run",
        "tactics": (),
        "subgoals": (),
        "coverage_emphasis": (),
    }
    return GoalRevision(canonical_digest=sha256_digest(values), **values)  # type: ignore[arg-type]


def _blueprint() -> GoalDirectedBlueprint:
    session = GENERIC_GOAL_DIRECTED.session_policy.model_copy(
        update={
            "fresh_agent_token_threshold": 5,
            "handoff_token_reserve": 2,
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
                        "tokens.total": 100,
                        "model.turns": 10,
                    }
                },
            },
            "iteration_reservation": {
                "goal.iterations": 1,
                "tokens.total": 100,
                "model.turns": 10,
            },
        }
    )


def _run_input() -> GoalDirectedRunInput:
    blueprint = _blueprint()
    revision = _revision()
    return GoalDirectedRunInput(
        run_id="run-wp-bp-020-sandbox-rollover",
        request_scope="tenant-1",
        effective_configuration_digest=DIGEST,
        blueprint_digest=sha256_digest(blueprint),
        blueprint=blueprint.model_dump(mode="json"),
        envelope_digest=revision.envelope_digest,
        initial_revision=revision,
        initial_run_version=1,
        task_timeout_seconds=120,
        required_obligation_refs=("fixture-obligation",),
        required_output_contract_refs=("fixture-output",),
        semantic_input_binding_ref="semantic-input:sandbox-rollover",
    )


def _placeholder_identity(run_id: str, iteration: int) -> dict[str, object]:
    return {
        "iteration": {
            "run_id": run_id,
            "goal_iteration": iteration,
            "goal_revision_id": "goal-revision:1",
            "execution_epoch": 1,
        },
        "agent_run": iteration,
        "session_generation": iteration,
    }


def _handoff(run_id: str, iteration: int) -> dict[str, object]:
    del run_id, iteration
    return {
        "schema_version": "belllabs.goal-handoff-draft.v1",
        "accepted_fact_refs": ["fact:sandbox-artifact-created"],
        "evidence_refs": ["evidence:sandbox-execute"],
        "artifact_refs": ["workspace-file:artifact.txt"],
        "attempted_tactics": ["write-and-read-file"],
        "rejected_tactics": [],
        "unresolved_obligations": ["fixture-obligation"],
        "blockers": [],
        "effect_frontier_refs": [],
        "pending_liability_refs": [],
        "context_selection_refs": ["context:objective"],
        "compaction_decision_ref": "compaction:accepted",
        "compaction_status": "accepted",
        "compaction_attempt": 1,
        "compaction_failure_ref": "",
        "continuation_instructions": (
            "Start with an empty model session. Rehydrate this typed handoff, then "
            "read artifact.txt from the governed sandbox workspace."
        ),
    }


def _executor_payload(run_id: str, iteration: int) -> dict[str, object]:
    return {
        "schema_version": "belllabs.goal-executor-observation.v1",
        "disposition": "completed",
        "output_refs": [f"artifact:sandbox:{iteration}"],
        "completion_claim": iteration == 2,
        "accepted_fact_refs": ["fact:sandbox-artifact-created"],
        "evidence_refs": [f"evidence:executor:{iteration}"],
        "handoff": _handoff(run_id, iteration) if iteration == 1 else None,
        "output_contract_ref": "fixture-output",
    }


def _verifier_payload(run_id: str, iteration: int) -> dict[str, object]:
    del run_id
    accepted = iteration == 2
    return {
        "schema_version": "belllabs.goal-verifier-observation.v1",
        "decision": "accepted" if accepted else "rejected",
        "progress_made": True,
        "accepted_obligation_refs": ["fixture-obligation"] if accepted else [],
        "findings": [],
        "evidence_refs": [f"evidence:verifier:{iteration}"],
        "unmet_obligations": [] if accepted else ["fixture-obligation"],
        "obligation_applicability": [["fixture-obligation", True]],
        "output_contract_ref": "fixture-output",
    }


class GoalSandboxModel(BaseChatModel):
    role: str
    iteration: int
    run_id: str
    workspace_path: str
    observations: list[dict[str, object]]

    @property
    def _llm_type(self) -> str:
        return "wp-bp-020-sandbox-scripted"

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        tool_result = next(
            (message for message in reversed(messages) if isinstance(message, ToolMessage)),
            None,
        )
        if tool_result is None:
            prior_ai_outputs = sum(
                isinstance(message, AIMessage) and bool(message.content)
                for message in messages
            )
            rendered = "\n".join(str(message.content) for message in messages)
            self.observations.append(
                {
                    "role": self.role,
                    "iteration": self.iteration,
                    "prior_ai_outputs": prior_ai_outputs,
                    "typed_handoff_present": (
                        self.iteration == 1 or "goal-handoff:" in rendered
                    ),
                }
            )
            if self.role == "executor" and self.iteration == 1:
                command = (
                    f"mkdir -p {self.workspace_path} && "
                    f"printf 'durable-sandbox-artifact' > {self.workspace_path}/artifact.txt && "
                    f"cat {self.workspace_path}/artifact.txt"
                )
            elif self.role == "executor":
                command = f"cat {self.workspace_path}/artifact.txt"
            else:
                command = (
                    f"mkdir -p {self.workspace_path} && "
                    f"printf 'verified-{self.iteration}' > "
                    f"{self.workspace_path}/verification.txt && "
                    f"cat {self.workspace_path}/verification.txt"
                )
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "execute",
                        "args": {"command": command},
                        "id": f"sandbox-{self.role}-{self.iteration}",
                        "type": "tool_call",
                    }
                ],
                usage_metadata={"input_tokens": 2, "output_tokens": 3, "total_tokens": 5},
            )
        else:
            if self.role == "executor" and self.iteration == 2:
                assert "durable-sandbox-artifact" in str(tool_result.content)
            payload = (
                _executor_payload(self.run_id, self.iteration)
                if self.role == "executor"
                else _verifier_payload(self.run_id, self.iteration)
            )
            message = AIMessage(
                content=json.dumps(payload, sort_keys=True),
                usage_metadata={"input_tokens": 2, "output_tokens": 3, "total_tokens": 5},
            )
        return ChatResult(generations=[ChatGeneration(message=message)])


class Documents:
    def __init__(self) -> None:
        self.handoffs: list[object] = []

    async def persist_revision(self, *_args: object) -> str:
        return "revision:persisted"

    async def persist_iteration(self, *_args: object) -> str:
        return "iteration:persisted"

    async def persist_handoff(self, _scope: str, handoff: object, _at: datetime) -> str:
        self.handoffs.append(handoff)
        return "handoff:persisted"

    async def persist_verification(self, *_args: object) -> str:
        return "verification:persisted"


class Bindings:
    async def create_binding(self, binding: object, *, request_scope: str):
        del request_scope
        return binding


class FamilyAdmissions:
    async def execute_family_admission(self, command: object, mutation: object):
        return SimpleNamespace(
            command_result=SimpleNamespace(
                resulting_run_version=command.expected_run_version + 1
            ),
            family_receipt=SimpleNamespace(
                family_version=mutation.expected_family_version + 1
            ),
        )


class Templates:
    def __init__(self, values: dict[str, OperationExecutionRequest]) -> None:
        self._values = values

    async def get_template(self, *, operation_role: str, **_kwargs: object):
        return self._values[operation_role]


class SandboxRolloverActivities:
    def __init__(self, *, workspace_root: Path) -> None:
        self.documents = Documents()
        self.models: list[GoalSandboxModel] = []
        deep_binding, _profile, bundle = exact_fixture(sandbox_backend="docker")
        template_values: dict[str, OperationExecutionRequest] = {}
        for role in ("executor", "verifier"):
            base = operation_request(prompt=f"Execute GoalDirected {role} in the sandbox.")
            template = OperationExecutionRequest.model_validate(
                {
                    **base.model_dump(mode="python"),
                    "execution_runtime": "deep_agent",
                    "native_placement": None,
                    "deep_agent_binding": deep_binding,
                    "output_schema": StructuredOutputBinding(
                        schema_id=f"goal-{role}-output",
                        revision=1,
                        schema_digest=sha256_digest(f"goal-{role}-output-schema"),
                    ),
                }
            )
            template_values[role] = template
        self.preparer = GoalDirectedOperationPreparationService(
            templates=Templates(template_values),  # type: ignore[arg-type]
            operation_bindings=Bindings(),  # type: ignore[arg-type]
            run_control=FamilyAdmissions(),  # type: ignore[arg-type]
            documents=self.documents,  # type: ignore[arg-type]
            actor=ActorContext(
                actor_id="goal-sandbox-worker",
                permissions=frozenset({"workflow_run.goal_directed"}),
            ),
        )
        self.reconciler = GoalDirectedOperationResultService(self.documents)  # type: ignore[arg-type]
        sandbox_factory = DockerSandboxFactory(workspace_root=workspace_root)

        def model_factory(binding: object, _secrets: object) -> BaseChatModel:
            operation_id = binding.operation_id
            parts = operation_id.split("/")
            iteration = int(parts[1])
            role = parts[2]
            model = GoalSandboxModel(
                role=role,
                iteration=iteration,
                run_id=binding.run_id,
                workspace_path=binding.workspace.exclusive_write_paths[0],
                observations=[],
            )
            self.models.append(model)
            return model

        registry = ExactComponentRegistry(
            model_factories={deep_binding.model.ref.digest: model_factory},
            skill_bundles={bundle.bundle_digest: bundle},
            sandbox_factories={deep_binding.sandbox.ref.digest: sandbox_factory},
            checkpointers={deep_binding.checkpointer_ref.digest: InMemorySaver()},
            stores={deep_binding.store_ref.digest: InMemoryStore()},
        )
        self.adapter = DeepAgentRuntimeAdapter(ExactDeepAgentMaterializer(registry))

    @activity.defn(name="goaldirected.prepare_executor")
    async def prepare_executor(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        return await self.preparer.prepare(request)

    @activity.defn(name="goaldirected.prepare_verifier")
    async def prepare_verifier(
        self, request: GoalOperationPreparationRequest
    ) -> GoalOperationDispatch:
        return await self.preparer.prepare(request)

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
                    mount_manifest_digest=sha256_digest("sandbox-rollover-mounts"),
                ),
            ),
            {},
        )
        return OperationExecutionResult(
            binding_id=binding.binding_id,
            semantic_attempt_key=binding.semantic_attempt_key,
            status="completed",
            output_text=result.output_text,
            structured_output=result.structured_output,
            output_refs=result.output_refs,
            usage=result.usage,
        ).model_dump(mode="json")

    @activity.defn(name="goaldirected.reconcile_operation")
    async def reconcile(
        self, request: GoalOperationReconciliationRequest
    ) -> GoalOperationReconciliationResult:
        return await self.reconciler.reconcile(request)

    @activity.defn(name="goaldirected.apply_lifecycle_command")
    async def lifecycle(self, request: LifecycleCommandRequest) -> LifecycleCommandOutcome:
        kind = str(request.action["kind"])
        return LifecycleCommandOutcome(
            accepted=True,
            resulting_run_version=request.expected_run_version + 1,
            phase="terminal" if kind == "terminalize" else "active",
            reason_code="accepted",
            evidence_frontier_digest=DIGEST,
            obligation_revision=DIGEST,
            accepted_obligation_evidence_digest=DIGEST,
            required_obligations_accepted=True,
            workflow_type_digest=DIGEST,
            terminal_outcome=RunOutcome.COMPLETED if kind == "terminalize" else None,
        )

    @property
    def functions(self) -> list[object]:
        return [
            self.prepare_executor,
            self.prepare_verifier,
            self.execute,
            self.reconcile,
            self.lifecycle,
        ]
@pytest.mark.asyncio
async def test_temporal_goal_rollover_uses_fresh_deep_agent_typed_handoff_and_sandbox(
    tmp_path: Path,
) -> None:
    activities = SandboxRolloverActivities(workspace_root=tmp_path / "workspaces")
    run_input = _run_input()
    root_input = BellLabsRunInput(
        schema_version="belllabs.temporal-root.v1",
        run_id=run_input.run_id,
        request_scope=run_input.request_scope,
        effective_configuration_digest=run_input.effective_configuration_digest,
        workflow_type_digest=DIGEST,
        family="GoalDirected",
        family_input=asdict(run_input),
        family_task_queue=QUEUE,
    )
    try:
        environment = await WorkflowEnvironment.start_time_skipping()
    except RuntimeError as error:
        pytest.skip(f"Temporal test server is unavailable: {error}")

    async with environment:
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
                task_queue="agent-cognitive",
                activities=[activities.execute],
            ),
        ):
            result = await environment.client.execute_workflow(
                BellLabsRunWorkflow.run,
                root_input,
                id=root_input.workflow_id,
                task_queue=QUEUE,
            )

    assert result["rollover_count"] == 1
    assert result["goal_iterations"] == 2
    assert len(result["handoffs"]) == 1
    assert result["convergence_proposal"]["action"] == "complete"
    observations = [item for model in activities.models for item in model.observations]
    executor_observations = [item for item in observations if item["role"] == "executor"]
    assert [item["iteration"] for item in executor_observations] == [1, 2]
    assert executor_observations[1]["prior_ai_outputs"] == 0
    assert executor_observations[1]["typed_handoff_present"] is True
    assert all(item["prior_ai_outputs"] == 0 for item in observations)
    assert activities.documents.handoffs
    assert list((tmp_path / "workspaces").rglob("artifact.txt"))
