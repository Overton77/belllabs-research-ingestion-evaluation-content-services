"""RRM-016: the governed GoalDirected composition, shared by the RRM-016 proofs.

One composition: the real `GoalDirectedOperationPreparationService` admits each executor and
verifier operation atomically with its reservation; the production `operation.execute`
activity runs `OperationExecutionService` with the real `RunControlOperationAuthority`, the
journaled coordinator (claim -> fenced result -> observation -> one authority settlement) and
checkpoint lineage, over a real `create_deep_agent` graph driven by a deterministic scripted
model; the real `GoalDirectedOperationResultService` consumes the run-control settlement
through `RunControlGoalOperationSettlements`, and the family records no usage of its own.

Storage is pluggable: in-memory run control and journal for the deterministic suites,
application PostgreSQL (run control, journal, lineage), `AsyncPostgresSaver`
and the immutable PostgreSQL
binding store for the real-Temporal demonstration.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from langchain_core.messages import AIMessage, BaseMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.store.memory import InMemoryStore
from pydantic import PrivateAttr

from mission_control.adapters.deep_agents import (
    DeepAgentRuntimeAdapter,
    ExactComponentRegistry,
    ExactDeepAgentMaterializer,
    ResolvedSkillBundle,
    StateSandboxFactory,
)
from mission_control.adapters.operations.conformance import (
    ConformanceAssetVerifier,
    ConformanceBudgetAuthority,
    ConformanceEventSink,
    ConformanceSandbox,
    ConformanceSecretResolver,
)
from mission_control.adapters.temporal.activities.goal_directed import (
    GoalDirectedActivities,
    compose_goal_directed_activities,
)
from mission_control.application.execution.boundary_interventions import (
    BoundaryCommandApplicationService,
)
from mission_control.application.execution.operations.checkpoint_lineage import (
    CheckpointLineageService,
)
from mission_control.application.execution.operations.journaled_operation_execution import (
    JournaledOperationExecutionCoordinator,
    ResultPayloadStore,
)
from mission_control.application.execution.operations.operation_execution import (
    OperationBindingRepository,
    OperationExecutionService,
    RunControlOperationAuthority,
)
from mission_control.application.execution.operations.operation_journal import (
    OperationJournalService,
)
from mission_control.application.execution.run_control_repository import (
    InMemoryRunControlRepository,
    RunControlRepository,
)
from mission_control.application.execution.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
    RunControlService,
)
from mission_control.application.programs.goal_directed import (
    GoalDirectedDocumentRepository,
    GoalDirectedOperationPreparationService,
    GoalDirectedOperationResultService,
    GoalOperationTemplateProvider,
    InMemoryGoalOperationTemplateRepository,
    RunControlGoalOperationSettlements,
    configure_goal_directed_family_admissions,
)
from mission_control.application.programs.service import (
    RunControlLifecycleGateway,
    orchestration_lifecycle_actor,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.authoring.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
    GoalDirectedBlueprint,
    SecretRef,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
)
from mission_control.domain.authoring.fixtures import GENERIC_GOAL_DIRECTED
from mission_control.domain.execution.contracts import (
    DeepAgentExecutionBinding,
    OperationExecutionRequest,
    OperationExecutionResult,
    StructuredOutputBinding,
    WorkspaceContract,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.programs.contracts import (
    GoalDirectedRunInput,
    GoalExecutionResult,
    GoalHandoff,
    GoalRevision,
    GoalVerificationResult,
)
from mission_control.domain.programs.goal_directed_runtime import (
    GoalOperationReconciliationRequest,
    GoalOperationSettlement,
)
from tests.fixtures.checkpoint_recovery import ScriptedRecoveryModel
from tests.unit.operations.test_operation_execution import (
    MCP_DIGEST,
    SKILL_DIGEST,
    operation_request,
)
from tests.unit.run_control.test_run_control import ConfigurationVerifier, actor
from tests.unit.run_control.test_run_control import request as run_request

SCOPE = "tenant-1"
DIGEST = "sha256:" + "a" * 64
OBLIGATION = "fixture-obligation"
OUTPUT_CONTRACT = "fixture-output"
SEMANTIC_INPUT = "semantic-input:rrm-016"
TOKENS_PER_OPERATION = 10  # two scripted model calls of 5 tokens each

# The compiled workflow workspace contract: one exclusive writable slot. A GoalDirected unit
# binds it under its role-scoped root, so the executor writes `/goal/{i}/executor/work` and
# the verifier `/goal/{i}/verifier/work` (REQ-CP-DA-013, REQ-BP-GD-004).
GOAL_WORK_CONTRACT = WorkflowWorkspaceContract(
    slots=(
        WorkspaceSlot(
            name="work",
            path="/work",
            access="exclusive_write",
            purpose="GoalDirected role work",
        ),
    )
)
SYSTEM_PROMPT_REF = ExactDefinitionRef(
    kind=DefinitionKind.PROMPT, logical_id="system", revision=1, digest=DIGEST
)


def goal_template_workspace(workspace: WorkspaceContract) -> WorkspaceContract:
    """A template workspace bound to the exact compiled contract (before role rebasing)."""

    return workspace.model_copy(
        update={
            "workflow_contract_digest": sha256_digest(GOAL_WORK_CONTRACT.model_dump(mode="json")),
            "slot_bindings": (
                WorkspaceSlotBinding(
                    slot_name="work",
                    logical_path="/work",
                    access="exclusive_write",
                    owner=WorkspaceOwner(kind=WorkspaceOwnerKind.RUN, owner_id="goal-template"),
                ),
            ),
            "exclusive_write_paths": ("/work",),
        }
    )


class GoalControlPlane:
    """The admitted run's exact effective configuration, as F1 would return it."""

    def __init__(self, template: OperationExecutionRequest) -> None:
        self._template = template

    async def retrieve_for_admission(self, digest: str) -> SimpleNamespace:
        del digest
        return SimpleNamespace(
            effective_authority=SimpleNamespace(
                capabilities=self._template.capability_grant.capabilities
            ),
            source_refs=(self._template.workspace.template_ref, SYSTEM_PROMPT_REF),
            workflow_workspace_contract=GOAL_WORK_CONTRACT,
        )


def goal_authority(
    run_control: RunControlService, template: OperationExecutionRequest
) -> RunControlOperationAuthority:
    return RunControlOperationAuthority(run_control, GoalControlPlane(template))  # type: ignore[arg-type]


def goal_templates(
    binding: DeepAgentExecutionBinding,
    *,
    workspace: Callable[[WorkspaceContract], WorkspaceContract] = goal_template_workspace,
    secret_refs: tuple[SecretRef, ...] = (),
) -> dict[str, OperationExecutionRequest]:
    """Executor and verifier templates: exact Deep Agent binding, compiled workspace slots,
    the configured system prompt and an admitted objective."""

    templates: dict[str, OperationExecutionRequest] = {}
    for role in ("executor", "verifier"):
        base = operation_request(prompt=f"RRM-016 technical GoalDirected {role}.")
        template_workspace = workspace(base.workspace)
        templates[role] = OperationExecutionRequest.model_validate(
            {
                **base.model_dump(mode="python"),
                "secret_refs": (*base.secret_refs, *secret_refs),
                "execution_runtime": "deep_agent",
                "native_placement": None,
                "deep_agent_binding": DeepAgentExecutionBinding.create(
                    **{
                        **binding.model_dump(mode="python", exclude={"binding_digest"}),
                        "workspace": template_workspace,
                    }
                ),
                "workspace": template_workspace,
                "output_schema": StructuredOutputBinding(
                    schema_id=f"goal-{role}-observation",
                    revision=1,
                    schema_digest=sha256_digest(f"goal-{role}-observation-schema"),
                ),
            }
        )
    return templates


def goal_family_registry() -> FamilyAdmissionRegistry:
    registry = FamilyAdmissionRegistry()
    configure_goal_directed_family_admissions(registry)
    return registry


def goal_run_control(repository: RunControlRepository | None = None) -> RunControlService:
    """Run control with the GoalDirected family admission and a required obligation."""

    policies = AdmissionPolicyRegistry()
    policies.register("contract:input@1", lambda _request, _configuration: None)
    policies.register("contract:invariant@1", lambda _request, _configuration: None)
    return RunControlService(
        repository or InMemoryRunControlRepository(),
        ConfigurationVerifier(frozenset({OBLIGATION})),
        policies,
        goal_family_registry(),
    )


def goal_blueprint(*, max_iterations: int = 2) -> GoalDirectedBlueprint:
    reservation = {"goal.iterations": 1, "tokens.total": 15}
    return GoalDirectedBlueprint.model_validate(
        {
            **GENERIC_GOAL_DIRECTED.model_dump(mode="python"),
            "max_iterations": max_iterations,
            "authority_ceiling": {
                **GENERIC_GOAL_DIRECTED.authority_ceiling.model_dump(mode="python"),
                "budgets": {"dimensions": reservation},
            },
            "iteration_reservation": reservation,
        }
    )


def goal_revision(run_id: str) -> GoalRevision:
    values = {
        "schema_version": "belllabs.goal-revision.v1",
        "revision_id": "goal-revision:1",
        "revision": 1,
        "parent_revision_id": None,
        "envelope_digest": sha256_digest({"rrm016": "goal-envelope", "run": run_id}),
        "objective": "Produce one independently verified technical record.",
        "tactical_changes": (),
        "evidence_refs": ("input:rrm-016",),
        "unmet_obligations": (OBLIGATION,),
        "proposer": "application:qualification",
        "deciding_authority": "authority:qualification",
        "applicability": "remaining_run",
        "tactics": (),
        "subgoals": (),
        "coverage_emphasis": (),
    }
    return GoalRevision(canonical_digest=sha256_digest(values), **values)  # type: ignore[arg-type]


def goal_run_input(
    run_id: str, blueprint: GoalDirectedBlueprint, *, baseline: dict[str, int]
) -> GoalDirectedRunInput:
    revision = goal_revision(run_id)
    return GoalDirectedRunInput(
        run_id=run_id,
        request_scope=SCOPE,
        effective_configuration_digest=DIGEST,
        blueprint_digest=sha256_digest(blueprint),
        blueprint=blueprint.model_dump(mode="json"),
        envelope_digest=revision.envelope_digest,
        initial_revision=revision,
        initial_run_version=1,
        task_timeout_seconds=60,
        baseline_reservation=baseline,
        required_obligation_refs=(OBLIGATION,),
        required_output_contract_refs=(OUTPUT_CONTRACT,),
        semantic_input_binding_ref=SEMANTIC_INPUT,
    )


def goal_start_action(run_id: str) -> Any:
    """The family's `start` fact: it binds the run to its GoalDirected execution target,
    which the run-control authority requires of a GoalDirected unit (review fix 1)."""

    from mission_control.domain.policies.contracts import ExecutionTarget, StartAction

    return StartAction(
        execution_target=ExecutionTarget(
            family="GoalDirected", family_workflow_id=f"family/{run_id}/1"
        )
    )


async def admit_goal_run(run_control: RunControlService, request_id: str) -> str:
    """Admit a run whose budget bounds `goal.iterations` (the iteration reservation)."""

    from mission_control.domain.policies.contracts import BudgetApplicability

    base = run_request(request_id=request_id)
    dimensions = tuple(
        item.model_copy(update={"applicability": BudgetApplicability.BOUNDED, "hard_cap": 10})
        if item.dimension == "goal.iterations"
        else item
        for item in base.budget_envelope.dimensions
    )
    admitted = await run_control.admit(
        base.model_copy(
            update={
                "budget_envelope": base.budget_envelope.model_copy(
                    update={"dimensions": dimensions}
                )
            }
        )
    )
    assert admitted.run_id is not None
    return admitted.run_id


# --- Deterministic cognition ---------------------------------------------------------------

_ROLE = re.compile(r"'operation_role': '(executor|verifier)'")
_ITERATION = re.compile(r"'goal_iteration': (\d+)")


class GoalScriptedModel(ScriptedRecoveryModel):
    """Two turns per operation: `write_todos`, then the role's typed observation.

    The role and iteration are read from the goal-context segment of the latest human
    message (the operation's own input). In a shared executor session earlier iterations'
    messages stay in the thread, so a turn is decided by the messages after that input.
    The verifier rejects every iteration before `accept_at` and accepts it there.
    """

    accept_at: int = 2
    # One stable output record across iterations (default); `False` gives each iteration its
    # own output ref (the RRM-019 case: only the verified final output is promoted).
    stable_output_ref: bool = True
    # Tokens each scripted call reports (RRM-008 drives a budget violation by raising it).
    tokens_per_call: int = 5
    # RRM-008 review F3: like a real provider, refuse a prompt in which an AI message's
    # `tool_calls` are not all answered by tool messages (an OpenAI 400).
    validate_tool_pairing: bool = True
    _turns: list[dict[str, Any]] = PrivateAttr(default_factory=list)

    @property
    def turns(self) -> list[dict[str, Any]]:
        return self._turns

    def _observe(self, messages: list[BaseMessage]) -> tuple[int, int]:
        if self.validate_tool_pairing:
            unanswered: set[str] = set()
            for item in messages:
                if isinstance(item, AIMessage) and item.tool_calls:
                    unanswered |= {str(call["id"]) for call in item.tool_calls}
                elif isinstance(item, ToolMessage):
                    unanswered.discard(str(item.tool_call_id))
            if unanswered:
                raise AssertionError(
                    f"unmatched tool_calls reached the model (a provider rejects this): "
                    f"{sorted(unanswered)}"
                )
        human_indexes = [index for index, item in enumerate(messages) if item.type == "human"]
        last_human = human_indexes[-1]
        content = str(messages[last_human].content)
        role_match = _ROLE.search(content)
        iteration_match = _ITERATION.search(content)
        assert role_match is not None and iteration_match is not None, content
        tools = sum(isinstance(item, ToolMessage) for item in messages[last_human:])
        human = len(human_indexes)
        self._log.append((human, tools))
        self._turns.append(
            {
                "role": role_match.group(1),
                "iteration": int(iteration_match.group(1)),
                "human_messages": human,
                "system_has_goal_context": any(
                    item.type == "system" and "goal_revision_id" in str(item.content)
                    for item in messages
                ),
            }
        )
        index = len(self._log)
        if index in self._fail_on:
            raise self._fail_on[index]
        return index, tools

    def _reply(self, tools: int) -> ChatResult:  # type: ignore[override]
        usage = {"input_tokens": 2, "output_tokens": 3, "total_tokens": self.tokens_per_call}
        turn = self.turns[-1]
        if tools == 0:
            message = AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "write_todos",
                        "args": {"todos": [{"content": "verify", "status": "completed"}]},
                        "id": f"rrm016-{turn['role']}-{turn['iteration']}",
                        "type": "tool_call",
                    }
                ],
                usage_metadata=usage,
            )
        else:
            payload = (
                executor_payload(turn["iteration"], self.accept_at, self.stable_output_ref)
                if turn["role"] == "executor"
                else verifier_payload(turn["iteration"], self.accept_at)
            )
            message = AIMessage(content=json.dumps(payload), usage_metadata=usage)
        return ChatResult(generations=[ChatGeneration(message=message)])


def executor_payload(
    iteration: int, accept_at: int, stable_output_ref: bool = True
) -> dict[str, object]:
    return {
        "schema_version": "belllabs.goal-executor-observation.v1",
        "disposition": "completed",
        # One stable record across iterations: the family promotes every iteration's output
        # refs while the terminal proposal names only the last executor's (RRM-019).
        "output_refs": [
            "artifact:rrm016:record" if stable_output_ref else f"artifact:rrm016:{iteration}"
        ],
        "completion_claim": iteration >= accept_at,
        "accepted_fact_refs": [f"fact:rrm016:{iteration}"],
        "evidence_refs": [f"evidence:executor:{iteration}"],
        "handoff": None,
        "output_contract_ref": OUTPUT_CONTRACT,
    }


def verifier_payload(iteration: int, accept_at: int) -> dict[str, object]:
    accepted = iteration >= accept_at
    return {
        "schema_version": "belllabs.goal-verifier-observation.v1",
        "decision": "accepted" if accepted else "rejected",
        "progress_made": True,
        "accepted_obligation_refs": [OBLIGATION] if accepted else [],
        "findings": [],
        "evidence_refs": [f"evidence:verifier:{iteration}"],
        "unmet_obligations": [] if accepted else [OBLIGATION],
        "obligation_applicability": [[OBLIGATION, True]],
        "output_contract_ref": OUTPUT_CONTRACT,
    }


def deep_agent_registry(
    binding: DeepAgentExecutionBinding,
    bundle: ResolvedSkillBundle,
    model: GoalScriptedModel,
    saver: BaseCheckpointSaver[Any],
) -> ExactComponentRegistry:
    return ExactComponentRegistry(
        model_factories={binding.model.ref.digest: lambda _binding, _secrets: model},
        skill_bundles={bundle.bundle_digest: bundle},
        sandbox_factories={binding.sandbox.ref.digest: StateSandboxFactory()},
        checkpointers={binding.checkpointer_ref.digest: saver},
        stores={binding.store_ref.digest: InMemoryStore()},
    )


# --- Family documents and the composition ----------------------------------------------------


class RecordingGoalDocuments:
    def __init__(self) -> None:
        self.revisions: list[GoalRevision] = []
        self.iterations: list[GoalExecutionResult] = []
        self.verifications: list[GoalVerificationResult] = []
        self.handoffs: list[GoalHandoff] = []

    async def persist_revision(
        self, request_scope: str, run_id: str, revision: GoalRevision, recorded_at: datetime
    ) -> str:
        del request_scope, run_id, recorded_at
        self.revisions.append(revision)
        return revision.revision_id

    async def persist_iteration(
        self,
        request_scope: str,
        result: GoalExecutionResult,
        goal_revision_id: str,
        recorded_at: datetime,
    ) -> str:
        del request_scope, goal_revision_id, recorded_at
        self.iterations.append(result)
        return f"goal-iteration:{result.operation_identity}"

    async def persist_handoff(
        self, request_scope: str, handoff: GoalHandoff, recorded_at: datetime
    ) -> str:
        del request_scope, recorded_at
        self.handoffs.append(handoff)
        return handoff.handoff_id

    async def persist_verification(
        self,
        request_scope: str,
        run_id: str,
        goal_revision_id: str,
        verification: GoalVerificationResult,
        recorded_at: datetime,
    ) -> str:
        del request_scope, run_id, goal_revision_id, recorded_at
        self.verifications.append(verification)
        return verification.verification_id


class ExactLifecycleBinding:
    def __init__(self, blueprint: GoalDirectedBlueprint) -> None:
        self._blueprint_digest = sha256_digest(blueprint)

    async def verify(self, configuration_digest: str, blueprint_digest: str) -> None:
        if configuration_digest != DIGEST or blueprint_digest != self._blueprint_digest:
            raise ValueError("GoalDirected lifecycle binding drifted")


def goal_worker_actor() -> ActorContext:
    return ActorContext(
        actor_id="rrm016-goal-worker",
        permissions=frozenset({"workflow_run.goal_directed", "workflow_run.reserve_budget"}),
        authority_refs=frozenset({"authority:rrm016-goal-worker"}),
    )


@dataclass
class GoalComposition:
    """The family activities and the operation boundary over one durable store set."""

    run_control: RunControlService
    service: OperationExecutionService
    family: GoalDirectedActivities
    # `RecordingGoalDocuments` (in memory) unless a durable repository was composed (RRM-018).
    documents: Any
    templates: dict[str, OperationExecutionRequest]
    model: GoalScriptedModel
    binding: DeepAgentExecutionBinding
    extras: dict[str, Any] = field(default_factory=dict)


async def compose_goal_directed(
    *,
    run_control: RunControlService,
    journal: Any,
    lineage: CheckpointLineageService,
    results: ResultPayloadStore,
    bindings: OperationBindingRepository,
    saver: BaseCheckpointSaver[Any],
    model: GoalScriptedModel,
    blueprint: GoalDirectedBlueprint,
    claimed_by: str = "operation-runtime:rrm-016",
    template_provider: GoalOperationTemplateProvider | None = None,
    documents: GoalDirectedDocumentRepository | None = None,
    binding: DeepAgentExecutionBinding | None = None,
    async_subagents: Any = None,
    children: Any = None,
    secrets: Mapping[str, str] | None = None,
    template_secret_refs: tuple[SecretRef, ...] = (),
) -> GoalComposition:
    """Compose the production GoalDirected activities and operation boundary.

    RRM-008: `binding` (an exact binding that declares async subagent contracts),
    `async_subagents` (the governed middleware factory), `children` (the async child
    cancellation port) and `secrets`/`template_secret_refs` (the deployment credential)
    compose an executor whose Deep Agent spawns a real async child.
    """

    from tests.acceptance.control_plane.test_wp_cp_040 import exact_fixture

    fixture_binding, _profile, bundle = exact_fixture()
    binding = binding or fixture_binding
    templates = goal_templates(binding, secret_refs=template_secret_refs)
    if template_provider is None:
        repository = InMemoryGoalOperationTemplateRepository()
        await repository.persist_templates(
            request_scope=SCOPE,
            semantic_input_binding_ref=SEMANTIC_INPUT,
            executor=templates["executor"],
            verifier=templates["verifier"],
            recorded_at=templates["executor"].requested_at,
        )
        template_provider = repository
    adapter = DeepAgentRuntimeAdapter(
        ExactDeepAgentMaterializer(deep_agent_registry(binding, bundle, model, saver)),
        async_subagents=async_subagents,
    )
    assets = ConformanceAssetVerifier(
        mcp_schema_digests={"fixture-mcp": MCP_DIGEST},
        asset_manifest_digests={"skill:fixture.skill:1": SKILL_DIGEST},
    )
    service = OperationExecutionService(
        # The production run-control authority, not an accepting fake.
        authority=goal_authority(run_control, templates["executor"]),
        bindings=bindings,
        runtime=adapter,
        sandbox=ConformanceSandbox(),
        assets=assets,
        mcp=assets,
        secrets=ConformanceSecretResolver(
            {"environment:OPENAI_API_KEY": "unused", **dict(secrets or {})}
        ),
        events=ConformanceEventSink(),
        # The journal settles usage; this port is never reached with a journal composed.
        budget=ConformanceBudgetAuthority(),
        journal=JournaledOperationExecutionCoordinator(
            journal=OperationJournalService(journal),
            run_control=run_control,
            results=results,
            actor=actor(),
        ),
        journal_claimed_by=claimed_by,
        lineage=lineage,
        children=children,
    )
    documents = documents or RecordingGoalDocuments()
    family = compose_goal_directed_activities(
        run_control=run_control,
        operation_bindings=bindings,  # type: ignore[arg-type]
        templates=template_provider,
        documents=documents,
        lifecycle=RunControlLifecycleGateway(
            run_control, ExactLifecycleBinding(blueprint), orchestration_lifecycle_actor()
        ),
        actor=goal_worker_actor(),
        boundary=BoundaryCommandApplicationService(run_control, orchestration_lifecycle_actor()),
    )
    return GoalComposition(
        run_control=run_control,
        service=service,
        family=family,
        documents=documents,
        templates=templates,
        model=model,
        binding=binding,
    )


# --- Fixtures whose operation child is not the governed boundary ---------------------------


class FixtureGoalSettlements:
    """The settlement port for fixtures with fake run control (no journal to read).

    It returns the settlement shape the family consumes, derived from the operation result.
    The governed path, reading accepted run-control settlements, is
    `RunControlGoalOperationSettlements` (proved in the RRM-016 suites).
    """

    def __init__(self) -> None:
        self.version = 100

    async def observe_terminal(
        self,
        request: GoalOperationReconciliationRequest,
        provider_result: OperationExecutionResult,
    ) -> GoalOperationSettlement:
        return await self.observe(request, provider_result)

    async def observe(
        self,
        request: GoalOperationReconciliationRequest,
        provider_result: OperationExecutionResult,
    ) -> GoalOperationSettlement:
        self.version += 3
        operation = request.operation_request.operation
        return GoalOperationSettlement(
            binding_id=request.operation_binding_ref,
            settlement_id=f"settlement:{request.operation_binding_ref}",
            effect_claim_id=f"effect:{request.operation_binding_ref}",
            reservation_id=operation.budget_reservation_id,
            usage=dict(provider_result.usage.amounts),
            pending_external_usage=dict(provider_result.usage.pending_external_amounts),
            settlement_payload_digest=DIGEST,
            settled_run_version=max(self.version, operation.run_control_revision + 3),
        )


def governed_result_service(
    documents: Any, run_control: RunControlService, bindings: Any
) -> GoalDirectedOperationResultService:
    """The governed result service; `bindings` is the operation binding store (required)."""

    return GoalDirectedOperationResultService(
        documents, RunControlGoalOperationSettlements(run_control, bindings)
    )


def preparer(
    *,
    run_control: RunControlService,
    templates: GoalOperationTemplateProvider,
    bindings: Any,
    documents: Any,
) -> GoalDirectedOperationPreparationService:
    return GoalDirectedOperationPreparationService(
        templates=templates,
        operation_bindings=bindings,
        run_control=run_control,
        documents=documents,
        actor=goal_worker_actor(),
    )


def turns_by_operation(model: GoalScriptedModel) -> list[tuple[str, int, int]]:
    """(role, iteration, human messages) for every model call, in call order."""

    return [(item["role"], item["iteration"], item["human_messages"]) for item in model.turns]


__all__: Sequence[str] = (
    "DIGEST",
    "GOAL_WORK_CONTRACT",
    "OBLIGATION",
    "SCOPE",
    "TOKENS_PER_OPERATION",
    "FixtureGoalSettlements",
    "GoalComposition",
    "GoalScriptedModel",
    "RecordingGoalDocuments",
    "admit_goal_run",
    "compose_goal_directed",
    "goal_authority",
    "goal_blueprint",
    "goal_run_control",
    "goal_run_input",
    "goal_start_action",
    "goal_templates",
    "goal_template_workspace",
    "governed_result_service",
    "preparer",
    "turns_by_operation",
)
