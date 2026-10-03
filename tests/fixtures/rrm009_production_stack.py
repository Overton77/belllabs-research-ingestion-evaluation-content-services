"""RRM-009 production-shaped stack: the real API composition plus the deployment worker factory.

Nothing here replaces a production component with a fake. The API is `app.server.api`
composed by `initialize_run_control_resources` and `compose_runtime_control`; the workers are
`create_production_workers` over `ProductionWorkerActivityCompositionFactory`; the catalog is
published through the real `ControlPlaneService` into the disposable MongoDB and compiled into
an Effective Run Configuration the real admission verifier and operation authority read. Only
the model is deterministic: a technical model registered under the exact fixture model
digest, exactly as a deployment registers a provider factory.

Technical inputs only (RRM-009 §2): a two-stage StageGraph with a declared wait and a
two-iteration GoalDirected run. No company fixture, no research run.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.parse import quote

import asyncpg
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langchain_core.tools import BaseTool

from app.application.control_plane.control_plane_repository import BeanieDefinitionRepository
from app.application.control_plane.service import ControlPlaneService
from app.application.run_control.service import AdmissionPolicyRegistry
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AllowedOperationVariant,
    AuthorityCeiling,
    BudgetCeiling,
    CompilationContext,
    CompileInvocation,
    ControlProfileDefinition,
    DefinitionKind,
    DefinitionSelector,
    EffectiveRunConfiguration,
    EnvironmentAvailability,
    EvaluationProfileDefinition,
    ExactDefinitionRef,
    GoalDirectedBlueprint,
    LateResultPolicy,
    LateResultRule,
    PublishRequest,
    RunInputManifestRef,
    RuntimeProfileDefinition,
    SecretRef,
    SlowSiblingPolicy,
    StageDependency,
    StageGraphBlueprint,
    StageGraphWait,
    StageInputSlot,
    StageJoin,
    StageNode,
    StageOperationSlot,
    StageOutputSlot,
    WorkflowTypeDefinition,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
    WorkspaceTemplateDefinition,
)
from app.domain.control_plane.fixtures import GENERIC_GOAL_DIRECTED
from app.domain.operation_execution.contracts import (
    DeepAgentExecutionBinding,
    DeepAgentModelComponent,
    OperationExecutionRequest,
    PromptSegment,
    PromptTrustClass,
    StructuredOutputBinding,
    SyncSubagentProfile,
    WorkspaceContract,
    WorkspaceOwner,
    WorkspaceOwnerKind,
    WorkspaceSlotBinding,
)
from app.domain.orchestration.contracts import (
    GoalDirectedRunInput,
    GoalRevision,
    StageGraphRunInput,
)
from app.domain.run_control.contracts import (
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    RunRequest,
)
from app.integrations.agents.deep_agents.materializer import ResolvedSkillBundle
from app.integrations.control_plane_payloads import InMemoryPayloadStore
from app.temporal.deployment_composition import DeploymentCapabilityComponents
from app.temporal.registration.task_queues import BellLabsTaskQueues
from tests.acceptance.control_plane.test_wp_cp_040 import exact, exact_fixture
from tests.unit.operations.test_operation_execution import operation_request
from tests.unit.run_control.test_run_control import request as run_request

SCOPE = "tenant-1"
OPERATOR = "operator"
WAIT_ID = "release-review"
GOAL_OBLIGATION = "fixture-obligation"
GOAL_OUTPUT_CONTRACT = "fixture-output"
INPUT_CONTRACT = "contract:rrm009-input@1"
INVARIANT = "contract:rrm009-invariant@1"
CHILD_NAME = "technical-child"
CHILD_MARKER = "RRM009-CHILD-OK"
ANSWER_MARKER = "RRM009-OK"
MCP_CODE = "RRM009"
REPORT_PATH = "/workspace/output/report.md"
LANGGRAPH_SCHEMA = "rrm009_langgraph"
APP_LOGIN = "belllabs_app"
APP_PASSWORD = "belllabs-app-local"  # the disposable stack's init script value
FAMILY_WRITER_LOGIN = "rrm009_family_writer"
FAMILY_WRITER_PASSWORD = "rrm009-family-writer-local"
TOKENS_PER_CALL = 5
TASK_QUEUE = "rrm009"
# The placement's task queue must be the deployment's agent-cognitive queue: the family
# dispatches `operation.execute` on the binding's own queue (REQ-CP-DA-004 placement).
AGENT_COGNITIVE_QUEUE = BellLabsTaskQueues.from_base(TASK_QUEUE).agent_cognitive
NODE_EXECUTABLE = Path("C:/Program Files/nodejs/node.exe")

TECHNICAL_CEILINGS = {
    "tokens.total": 400,
    "model.turns": 80,
    "operation.attempts": 12,
    "goal.iterations": 6,
}
MAX_CONCURRENCY = 2
# `artifact.promote` is the governed promotion authority the generic artifact path verifies
# on the producer binding (`ArtifactPromotionService._validate_authority`).
CAPABILITIES = frozenset(
    {"model.invoke", "sandbox.execute", "mcp.call", "subagent.task", "artifact.promote"}
)


def _login_dsn(owner_dsn: str, login: str, password: str) -> str:
    """The same server and database as the owner DSN, under a least-privilege login."""

    host_and_db = owner_dsn.split("@", 1)[1]
    return f"postgresql://{login}:{quote(password)}@{host_and_db}"


async def prepare_disposable_identities(owner_dsn: str) -> None:
    """The dedicated family-writer login the production worker requires (migration 0017)."""

    connection = await asyncpg.connect(owner_dsn)
    try:
        await connection.execute(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{FAMILY_WRITER_LOGIN}') THEN
                    CREATE ROLE {FAMILY_WRITER_LOGIN} LOGIN PASSWORD '{FAMILY_WRITER_PASSWORD}'
                        NOSUPERUSER NOCREATEDB NOCREATEROLE INHERIT NOBYPASSRLS;
                END IF;
            END
            $$;
            GRANT belllabs_family_repository_writer TO {FAMILY_WRITER_LOGIN};
            GRANT USAGE ON SCHEMA belllabs_control TO {FAMILY_WRITER_LOGIN};
            GRANT belllabs_control_runtime TO {APP_LOGIN};
            DROP SCHEMA IF EXISTS {LANGGRAPH_SCHEMA} CASCADE;
            CREATE SCHEMA {LANGGRAPH_SCHEMA};
            """
        )
    finally:
        await connection.close()


def runtime_environment(
    *,
    owner_dsn: str,
    mongo_uri: str,
    mongo_database: str,
    temporal_address: str,
    task_queue: str,
    payload_root: Path,
    workspace_root: Path,
) -> dict[str, str]:
    """The deployment's environment: the exact names the runbook documents."""

    return {
        "APPLICATION_DATABASE_DIRECT": _login_dsn(owner_dsn, APP_LOGIN, APP_PASSWORD),
        "APPLICATION_MIGRATION_DATABASE_DIRECT": owner_dsn,
        "APPLICATION_FAMILY_WRITER_DATABASE_DIRECT": _login_dsn(
            owner_dsn, FAMILY_WRITER_LOGIN, FAMILY_WRITER_PASSWORD
        ),
        "LANGGRAPH_CHECKPOINT_DATABASE_DIRECT": owner_dsn,
        "LANGGRAPH_CHECKPOINT_SCHEMA": LANGGRAPH_SCHEMA,
        "LANGGRAPH_CHECKPOINT_SETUP": "1",
        "MONGODB_URI": mongo_uri,
        "MONGODB_DATABASE": mongo_database,
        "TEMPORAL_ADDRESS": temporal_address,
        "TEMPORAL_NAMESPACE": "default",
        "TEMPORAL_TASK_QUEUE": task_queue,
        "RUN_CONTROL_TEMPORAL_ENABLED": "1",
        "COORDINATOR_LAUNCH_ENABLED": "1",
        "BOUNDARY_RELAY_REQUEST_SCOPES": json.dumps([SCOPE]),
        "BOUNDARY_RELAY_INTERVAL_SECONDS": "1",
        "ARTIFACT_PAYLOAD_ROOT": str(payload_root),
        "DEEP_AGENT_SANDBOX_WORKSPACE_ROOT": str(workspace_root),
        "OPERATION_JOURNAL_CLAIMED_BY": "operation-runtime:rrm-009",
        "ASYNC_SUBAGENT_SUBMITTER_IDENTITY": "belllabs-async-submitter:rrm-009",
        "WEB_RESEARCH_AGENT_BROWSER_NODE": str(NODE_EXECUTABLE),
        "LANGSMITH_TRACING": "false",
    }


# --- Catalog ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class TechnicalCatalog:
    family: str
    erc: EffectiveRunConfiguration
    workflow_ref: ExactDefinitionRef
    workspace_ref: ExactDefinitionRef
    blueprint: StageGraphBlueprint | GoalDirectedBlueprint

    @property
    def blueprint_digest(self) -> str:
        return sha256_digest(self.blueprint)


def technical_control_plane() -> ControlPlaneService:
    from app.domain.control_plane.extensions import ExtensionRegistry

    return ControlPlaneService(
        BeanieDefinitionRepository(), ExtensionRegistry(), InMemoryPayloadStore()
    )


def _stage_slot() -> StageOperationSlot:
    return StageOperationSlot(
        operation_slot_id="execute",
        reservation={"tokens.total": 40, "model.turns": 8},
        allowed_variants=(
            AllowedOperationVariant(
                operation_variant_id="default",
                operation_contract_ref="operation:rrm009-technical@1",
            ),
        ),
    )


def stage_blueprint() -> StageGraphBlueprint:
    """`draft` -> declared wait `release-review` -> `review`."""

    dependency = StageDependency(
        dependency_id="draft-to-review",
        consumer_stage_id="review",
        join_id="draft-ready",
        producer_stage_id="draft",
        producer_output_slot_id="result",
        consumer_input_slot_id="draft-input",
        dependency_class="required",
    )
    return StageGraphBlueprint(
        logical_id="rrm009-technical-stagegraph",
        title="RRM-009 technical StageGraph",
        description="Draft, a declared wait, then review; technical inputs only.",
        stages=(
            StageNode(
                stage_id="draft",
                output_slots=(
                    StageOutputSlot(output_slot_id="result", output_contract_ref="output:draft@1"),
                ),
                operation_slots=(_stage_slot(),),
            ),
            StageNode(
                stage_id="review",
                input_slots=(StageInputSlot(input_slot_id="draft-input"),),
                output_slots=(
                    StageOutputSlot(output_slot_id="result", output_contract_ref="output:review@1"),
                ),
                operation_slots=(_stage_slot(),),
            ),
        ),
        joins=(
            StageJoin(
                consumer_stage_id="review",
                join_id="draft-ready",
                kind="all",
                dependency_ids=(dependency.dependency_id,),
                slow_sibling_policy=SlowSiblingPolicy(
                    triggers=("join_released",),
                    execution_action="continue",
                    arrival_route="evaluate_late_result",
                ),
            ),
        ),
        dependencies=(dependency,),
        late_result_policy=LateResultPolicy(
            rules=(
                LateResultRule(
                    rule_id="admit-late",
                    trigger="consumer_already_admitted",
                    decision="admit",
                ),
            )
        ),
        waits=(StageGraphWait(scope_kind="stage", scope_id="review", wait_id=WAIT_ID),),
    )


def goal_blueprint(*, max_iterations: int = 2) -> GoalDirectedBlueprint:
    reservation = {"goal.iterations": 1, "tokens.total": 40, "model.turns": 8}
    return GoalDirectedBlueprint.model_validate(
        {
            **GENERIC_GOAL_DIRECTED.model_dump(mode="python"),
            "logical_id": "rrm009-technical-goal-directed",
            "title": "RRM-009 technical GoalDirected",
            "description": "Executor then independent verifier; technical inputs only.",
            "max_iterations": max_iterations,
            "authority_ceiling": {
                **GENERIC_GOAL_DIRECTED.authority_ceiling.model_dump(mode="python"),
                "budgets": {"dimensions": reservation},
            },
            "iteration_reservation": reservation,
            # A fresh workspace per iteration. With the default `shared` mode the second
            # iteration's executor re-materializes the same workspace identity with its
            # iteration-rooted slots and the governed materializer refuses it (RRM-020).
            "workspace_policy": {
                **GENERIC_GOAL_DIRECTED.workspace_policy.model_dump(mode="python"),
                "workspace_mode": "fresh",
            },
        }
    )


def stage_workspace_contract() -> WorkflowWorkspaceContract:
    return WorkflowWorkspaceContract(
        slots=(
            WorkspaceSlot(
                name="output",
                path="/workspace/output",
                access="exclusive_write",
                purpose="report and output slot",
            ),
        )
    )


def goal_workspace_contract() -> WorkflowWorkspaceContract:
    return WorkflowWorkspaceContract(
        slots=(
            WorkspaceSlot(
                name="work", path="/work", access="exclusive_write", purpose="role work slot"
            ),
        )
    )


def _ceiling(ceilings: Mapping[str, int] = TECHNICAL_CEILINGS) -> AuthorityCeiling:
    return AuthorityCeiling(
        capabilities=CAPABILITIES,
        budgets=BudgetCeiling(dimensions=dict(ceilings)),
        max_concurrency=MAX_CONCURRENCY,
    )


async def publish_technical_catalog(
    control_plane: ControlPlaneService,
    *,
    family: str,
    now: datetime,
    ceilings: Mapping[str, int] = TECHNICAL_CEILINGS,
) -> TechnicalCatalog:
    """Publish one technical Workflow Type per family and compile its ERC through F1."""

    tag = "stagegraph" if family == "StageGraph" else "goal-directed"
    blueprint: StageGraphBlueprint | GoalDirectedBlueprint = (
        stage_blueprint() if family == "StageGraph" else goal_blueprint()
    )
    contract = stage_workspace_contract() if family == "StageGraph" else goal_workspace_contract()

    async def publish(definition: Any) -> Any:
        return await control_plane.publish(
            PublishRequest(
                definition=definition,
                actor_id="publisher",
                published_at=now,
                expected_head_revision=0,
            )
        )

    published_blueprint = await publish(blueprint)
    control = await publish(
        ControlProfileDefinition(
            logical_id=f"rrm009.{tag}.control",
            title=f"RRM-009 {family} control",
            description="Technical qualification control profile.",
            blueprint_ref=published_blueprint.ref,
            selected_variants=frozenset({"default"} if family == "StageGraph" else ()),
            authority_ceiling=_ceiling(ceilings),
            overlayable_fields=frozenset(),
        )
    )
    runtime = await publish(
        RuntimeProfileDefinition(
            logical_id=f"rrm009.{tag}.runtime",
            title=f"RRM-009 {family} runtime",
            description="Technical qualification runtime profile; templates carry the binding.",
            binding="python-3.12",
        )
    )
    workspace = await publish(
        WorkspaceTemplateDefinition(
            logical_id=f"rrm009.{tag}.workspace",
            title=f"RRM-009 {family} workspace",
            description="The governed writable slot of the technical operations.",
            slots=contract.slots,
        )
    )
    evaluation = await publish(
        EvaluationProfileDefinition(
            logical_id=f"rrm009.{tag}.evaluation",
            title=f"RRM-009 {family} evaluation",
            description="Technical qualification gate placeholder.",
            gate_contract_refs=frozenset({"contract:rrm009-evaluation@1"}),
        )
    )
    workflow = await publish(
        WorkflowTypeDefinition(
            logical_id=f"rrm009.{tag}.workflow",
            title=f"RRM-009 {family} technical workflow",
            description="A technical qualification Workflow Type, not a product workflow.",
            purpose="Qualify the production composition end to end",
            input_admission_contract=INPUT_CONTRACT,
            invariants=frozenset({INVARIANT}),
            obligations=frozenset() if family == "StageGraph" else frozenset({GOAL_OBLIGATION}),
            allowed_blueprints=frozenset({published_blueprint.ref}),
            allowed_control_profiles=frozenset({control.ref}),
            allowed_runtime_profiles=frozenset({runtime.ref}),
            allowed_workspace_templates=frozenset({workspace.ref}),
            allowed_evaluation_profiles=frozenset({evaluation.ref}),
            authority_ceiling=_ceiling(ceilings),
            workspace_contract=contract,
        )
    )
    manifest_digest = sha256_digest({"rrm009": "technical-input-manifest", "family": family})
    erc = await control_plane.compile(
        CompileInvocation(
            workflow_type=DefinitionSelector(exact=workflow.ref),
            blueprint=DefinitionSelector(exact=published_blueprint.ref),
            control_profile=DefinitionSelector(exact=control.ref),
            runtime_profile=DefinitionSelector(exact=runtime.ref),
            workspace_template=DefinitionSelector(exact=workspace.ref),
            evaluation_profile=DefinitionSelector(exact=evaluation.ref),
            input_manifest=RunInputManifestRef(
                manifest_id=f"rrm009-{tag}-manifest", revision=1, digest=manifest_digest
            ),
            caller_authority=_ceiling(ceilings),
            environment=EnvironmentAvailability(
                capabilities=CAPABILITIES, runtime_bindings=frozenset({"python-3.12"})
            ),
            context=CompilationContext(
                compilation_id=f"rrm009-{tag}-compilation",
                compiled_at=now,
                actor_id=OPERATOR,
                authority_subject_id=OPERATOR,
                authority_scope=SCOPE,
            ),
        )
    )
    return TechnicalCatalog(
        family=family,
        erc=erc,
        workflow_ref=workflow.ref,
        workspace_ref=workspace.ref,
        blueprint=blueprint,
    )


def technical_admission_policies() -> AdmissionPolicyRegistry:
    policies = AdmissionPolicyRegistry()
    policies.register(INPUT_CONTRACT, lambda _request, _configuration: None)
    policies.register(INVARIANT, lambda _request, _configuration: None)
    return policies


def baseline_reservation(family: str) -> dict[str, int]:
    """The admitted baseline reservation. GoalDirected settles its baseline at closing;
    StageGraph never settles one, so a StageGraph run admitted with a baseline cannot
    terminalize (`budget_not_settled`, RRM-021) and its technical runs admit none, as the
    WP-BP-010 live gate does."""

    return {"tokens.total": 20} if family == "GoalDirected" else {}


def admission_request(catalog: TechnicalCatalog, request_id: str) -> RunRequest:
    """A run request bound to the compiled ERC, with hard caps at the effective ceilings."""

    base = run_request(request_id=request_id)
    ceilings = catalog.erc.effective_authority.budgets.dimensions
    dimensions = tuple(
        BudgetDimensionLimit(
            dimension=item.dimension,
            applicability=BudgetApplicability.BOUNDED,
            hard_cap=ceilings[item.dimension],
        )
        if item.dimension in ceilings
        else (
            item.model_copy(update={"hard_cap": MAX_CONCURRENCY})
            if item.dimension == "concurrency.slots"
            else item
        )
        for item in base.budget_envelope.dimensions
    )
    return base.model_copy(
        update={
            "effective_configuration_digest": catalog.erc.digest,
            "workflow_type_ref": catalog.workflow_ref,
            "input_manifest": catalog.erc.input_manifest,
            "budget_envelope": BudgetEnvelope(
                dimensions=dimensions, baseline_reservations=baseline_reservation(catalog.family)
            ),
        }
    )


# --- Exact binding and templates -------------------------------------------------------------


@dataclass(frozen=True)
class TechnicalBinding:
    binding: DeepAgentExecutionBinding
    bundle: ResolvedSkillBundle
    child_model_ref: ExactDefinitionRef
    child_prompt_ref: ExactDefinitionRef

    def components(self, model_log: list[dict[str, Any]]) -> DeploymentCapabilityComponents:
        """The qualification's exact components: deterministic parent and child models."""

        return DeploymentCapabilityComponents(
            model_factories={
                self.binding.model.ref.digest: lambda bound, _secrets: TechnicalModel(
                    run_id=bound.run_id, operation_id=bound.operation_id, log=model_log
                ),
                self.child_model_ref.digest: lambda bound, _secrets: ChildModel(
                    run_id=bound.run_id, operation_id=bound.operation_id, log=model_log
                ),
            },
            prompts={
                self.child_prompt_ref.digest: (
                    "You are the technical child. Reply with the marker you are asked for."
                )
            },
            skill_bundles={self.bundle.bundle_digest: self.bundle},
            # The qualification MCP server (a Python stdio module, not a pinned package) is
            # registered exactly; the worker refuses any other launch (RRM-009 review).
            mcp_servers={server.ref.digest: server for server in self.binding.mcp_servers},
        )


def technical_binding() -> TechnicalBinding:
    """The exact fixture binding plus one in-process sync subagent and the qualification MCP."""

    child_model_ref = exact(DefinitionKind.MODEL, "model.rrm009-child", "child-model")
    child_prompt_ref = exact(DefinitionKind.PROMPT, "prompt.rrm009-child", "child-prompt")
    child = SyncSubagentProfile(
        name=CHILD_NAME,
        description="An in-process technical child with a bounded capability slice.",
        system_prompt_ref=child_prompt_ref,
        model=DeepAgentModelComponent(
            ref=child_model_ref, provider="openai", model_name="fixture-child-model"
        ),
        tool_refs=(),
        skill_refs=(),
        state_slice_id="child-state",
        context_slice_id="child-context",
        workspace_id="workspace-child",
        writable_paths=("/workspace/child",),
        # Bounded by the parent's reservation: the fixture profile's delegation ceiling
        # declares no per-child dimension (REQ-CP-DA-007).
        budget_limits={},
    )
    binding, _profile, bundle = exact_fixture(
        include_mcp=True, sync_subagents=(child,), with_child_slices=True
    )
    return TechnicalBinding(
        binding=binding,
        bundle=bundle,
        child_model_ref=child_model_ref,
        child_prompt_ref=child_prompt_ref,
    )


def _workspace(
    template_ref: ExactDefinitionRef,
    *,
    namespace: str,
    workspace_id: str,
    contract: WorkflowWorkspaceContract,
    owner: WorkspaceOwner,
) -> WorkspaceContract:
    base = operation_request().workspace
    slot = contract.slots[0]
    return base.model_copy(
        update={
            "namespace_id": namespace,
            "workspace_id": workspace_id,
            "template_ref": template_ref,
            "workflow_contract_digest": sha256_digest(contract.model_dump(mode="json")),
            "slot_bindings": (
                WorkspaceSlotBinding(
                    slot_name=slot.name,
                    logical_path=slot.path,
                    access="exclusive_write",
                    owner=owner,
                ),
            ),
            "exclusive_write_paths": (slot.path,),
        }
    )


def _segments(objective: str, *, source: str) -> tuple[PromptSegment, ...]:
    """Admitted, non-privileged segments only: the minimal catalog carries no prompt."""

    return (
        PromptSegment(
            source_ref=source,
            source_revision=1,
            trust_class=PromptTrustClass.ADMITTED_INPUT,
            content=objective,
            rendered_digest=sha256_digest(objective),
        ),
    )


def _template(
    technical: TechnicalBinding,
    *,
    objective: str,
    workspace: WorkspaceContract,
    output_schema: StructuredOutputBinding | None = None,
) -> OperationExecutionRequest:
    base = operation_request()
    return OperationExecutionRequest.model_validate(
        {
            **base.model_dump(mode="python"),
            "prompt_segments": _segments(objective, source="input:rrm009@1"),
            "mcp_servers": (),
            "skills": (),
            "capability_grant": {
                "capabilities": sorted(CAPABILITIES),
                "tool_ids": [],
                "mcp_server_ids": [],
                "data_scope_refs": [],
                "network_hosts": [],
            },
            "workspace": workspace,
            "execution_runtime": "deep_agent",
            "native_placement": None,
            "deep_agent_binding": DeepAgentExecutionBinding.create(
                **{
                    **technical.binding.model_dump(mode="python", exclude={"binding_digest"}),
                    "task_queue": AGENT_COGNITIVE_QUEUE,
                    "workspace": workspace,
                    "capability_grant": {
                        "capabilities": sorted(CAPABILITIES),
                        "tool_ids": [],
                        "mcp_server_ids": [],
                        "data_scope_refs": [],
                        "network_hosts": [],
                    },
                }
            ),
            "budget_limits": {"model.turns": 8, "tokens.total": 40},
            "secret_refs": (SecretRef(provider="environment", key="OPENAI_API_KEY"),),
            "output_schema": output_schema,
        }
    )


def stage_templates(
    technical: TechnicalBinding, catalog: TechnicalCatalog
) -> dict[str, OperationExecutionRequest]:
    templates: dict[str, OperationExecutionRequest] = {}
    for stage in ("draft", "review"):
        templates[f"{stage}/execute/default"] = _template(
            technical,
            objective=f"RRM-009 technical stage {stage}. Produce the stage record.",
            # One namespace per stage: both stages bind the Workflow Type's exclusive
            # `/workspace/output` slot, and two workspaces never own one slot in a namespace.
            workspace=_workspace(
                catalog.workspace_ref,
                namespace=f"workspace-namespace:{{run_id}}:stage:{stage}",
                workspace_id=f"workspace:{{run_id}}:stage:{stage}",
                contract=stage_workspace_contract(),
                owner=WorkspaceOwner(kind=WorkspaceOwnerKind.STAGE, owner_id=f"stage:{stage}"),
            ),
        )
    return templates


def goal_templates(
    technical: TechnicalBinding, catalog: TechnicalCatalog
) -> dict[str, OperationExecutionRequest]:
    templates: dict[str, OperationExecutionRequest] = {}
    for role in ("executor", "verifier"):
        templates[role] = _template(
            technical,
            objective=f"RRM-009 technical GoalDirected {role}.",
            workspace=_workspace(
                catalog.workspace_ref,
                namespace="workspace-namespace:{run_id}",
                workspace_id=f"workspace:{{run_id}}:{role}",
                contract=goal_workspace_contract(),
                owner=WorkspaceOwner(kind=WorkspaceOwnerKind.RUN, owner_id="goal-template"),
            ),
            output_schema=StructuredOutputBinding(
                schema_id=f"goal-{role}-observation",
                revision=1,
                schema_digest=sha256_digest(f"goal-{role}-observation-schema"),
            ),
        )
    return templates


# --- Family inputs ---------------------------------------------------------------------------


def stage_input(
    catalog: TechnicalCatalog, run_id: str, binding_ref: str, version: int
) -> StageGraphRunInput:
    return StageGraphRunInput(
        run_id=run_id,
        request_scope=SCOPE,
        effective_configuration_digest=catalog.erc.digest,
        workflow_type_digest=catalog.workflow_ref.digest,
        blueprint_digest=catalog.blueprint_digest,
        blueprint=catalog.blueprint.model_dump(mode="json"),
        initial_run_version=version,
        max_concurrency=1,
        task_timeout_seconds=180,
        semantic_input_binding_ref=binding_ref,
        correlation_id=f"rrm009:{run_id}",
        baseline_reservation=baseline_reservation("StageGraph"),
    )


def goal_revision(run_id: str, objective: str) -> GoalRevision:
    values = {
        "schema_version": "belllabs.goal-revision.v1",
        "revision_id": "goal-revision:1",
        "revision": 1,
        "parent_revision_id": None,
        "envelope_digest": sha256_digest({"rrm009": "goal-envelope", "run": run_id}),
        "objective": objective,
        "tactical_changes": (),
        "evidence_refs": ("input:rrm009-technical",),
        "unmet_obligations": (GOAL_OBLIGATION,),
        "proposer": "application:qualification",
        "deciding_authority": "authority:qualification",
        "applicability": "remaining_run",
        "tactics": (),
        "subgoals": (),
        "coverage_emphasis": (),
    }
    return GoalRevision(canonical_digest=sha256_digest(values), **values)  # type: ignore[arg-type]


def goal_input(
    catalog: TechnicalCatalog, run_id: str, binding_ref: str, version: int, objective: str
) -> GoalDirectedRunInput:
    revision = goal_revision(run_id, objective)
    return GoalDirectedRunInput(
        run_id=run_id,
        request_scope=SCOPE,
        effective_configuration_digest=catalog.erc.digest,
        blueprint_digest=catalog.blueprint_digest,
        blueprint=catalog.blueprint.model_dump(mode="json"),
        envelope_digest=revision.envelope_digest,
        initial_revision=revision,
        initial_run_version=version,
        task_timeout_seconds=180,
        baseline_reservation=baseline_reservation("GoalDirected"),
        required_obligation_refs=(GOAL_OBLIGATION,),
        required_output_contract_refs=(GOAL_OUTPUT_CONTRACT,),
        semantic_input_binding_ref=binding_ref,
    )


# --- Deterministic cognition -----------------------------------------------------------------


def _text(messages: Sequence[BaseMessage], kinds: tuple[type, ...]) -> str:
    return "\n".join(str(item.content) for item in messages if isinstance(item, kinds))


def _digest16(text: str) -> str:
    return sha256(text.encode("utf-8")).hexdigest()[:16]


def call_usage() -> dict[str, int]:
    return {"input_tokens": 2, "output_tokens": 3, "total_tokens": TOKENS_PER_CALL}


class _LoggedModel(BaseChatModel):
    run_id: str
    operation_id: str
    log: Any  # a shared list; pydantic would copy a validated `list`

    def bind_tools(
        self,
        tools: Sequence[BaseTool | dict[str, Any] | type | Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> BaseChatModel:
        del tools, tool_choice, kwargs
        return self

    def _record(self, kind: str, messages: list[BaseMessage]) -> tuple[int, int]:
        tools = sum(isinstance(item, ToolMessage) for item in messages)
        humans = sum(isinstance(item, HumanMessage) for item in messages)
        self.log.append(
            {
                "model": kind,
                "run_id": self.run_id,
                "operation": self.operation_id,
                "tools": tools,
                "humans": humans,
                "pid": os.getpid(),
            }
        )
        return tools, humans

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        return self._reply(messages)

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        del stop, run_manager, kwargs
        return self._reply(messages)

    def _reply(self, messages: list[BaseMessage]) -> ChatResult:
        raise NotImplementedError


class ChildModel(_LoggedModel):
    """The sync subagent's cognition: one answer carrying the child marker."""

    @property
    def _llm_type(self) -> str:
        return "rrm-009-child"

    def _reply(self, messages: list[BaseMessage]) -> ChatResult:
        self._record("child", messages)
        return ChatResult(
            generations=[
                ChatGeneration(message=AIMessage(content=CHILD_MARKER, usage_metadata=call_usage()))
            ]
        )


class TechnicalModel(_LoggedModel):
    """The parent's cognition, by tool-message count since its latest input:

    0: `task` (the in-process sync subagent); 1: `write_file` into the writable slot;
    2: `lookup_binding_marker` (the exact MCP tool); 3: the typed answer. GoalDirected
    operations answer with their role's observation; StageGraph operations with an
    artifact ref derived from their admitted objective.
    """

    @property
    def _llm_type(self) -> str:
        return "rrm-009-technical"

    def _reply(self, messages: list[BaseMessage]) -> ChatResult:
        human_indexes = [index for index, item in enumerate(messages) if item.type == "human"]
        since_input = messages[human_indexes[-1] :] if human_indexes else messages
        tools = sum(isinstance(item, ToolMessage) for item in since_input)
        self._record("parent", messages)
        if tools == 0:
            call = {
                "name": "task",
                "args": {
                    "description": f"Reply with exactly {CHILD_MARKER}.",
                    "subagent_type": CHILD_NAME,
                },
            }
        elif tools == 1:
            objective = _text(since_input, (HumanMessage,))
            call = {
                "name": "write_file",
                "args": {
                    "file_path": REPORT_PATH,
                    "content": f"# RRM-009 report\n\nobjective-digest: {_digest16(objective)}\n",
                },
            }
        elif tools == 2:
            call = {"name": "lookup_binding_marker", "args": {"code": MCP_CODE}}
        else:
            return ChatResult(
                generations=[
                    ChatGeneration(
                        message=AIMessage(
                            content=json.dumps(self._answer(since_input), sort_keys=True),
                            usage_metadata=call_usage(),
                        )
                    )
                ]
            )
        message = AIMessage(
            content="",
            tool_calls=[
                {
                    **call,
                    "id": f"rrm009-{call['name']}-{_digest16(self.operation_id)}",
                    "type": "tool_call",
                }
            ],
            usage_metadata=call_usage(),
        )
        return ChatResult(generations=[ChatGeneration(message=message)])

    def _answer(self, since_input: list[BaseMessage]) -> dict[str, Any]:
        tool_text = _text(since_input, (ToolMessage,))
        facts = {
            "child": CHILD_MARKER in tool_text,
            "mcp": f"MCP-BOUND::{MCP_CODE}::EXACT" in tool_text,
        }
        if ":stage:" in self.operation_id:
            stage = self.operation_id.split(":stage:", 1)[1].split(":", 1)[0]
            admitted = _text(since_input, (HumanMessage,))
            return {
                "answer": ANSWER_MARKER,
                "facts": facts,
                "output_refs": [f"artifact:rrm009:{stage}:{_digest16(admitted)}"],
            }
        role = self.operation_id.rsplit("/", 1)[-1]
        iteration = int(self.operation_id.split("goal-iteration/", 1)[1].split("/", 1)[0])
        if role == "executor":
            return {
                "schema_version": "belllabs.goal-executor-observation.v1",
                "disposition": "completed",
                "output_refs": [f"artifact:rrm009-goal:{iteration}"],
                "completion_claim": iteration >= 2,
                "accepted_fact_refs": [f"fact:rrm009:{iteration}"],
                "evidence_refs": [f"evidence:rrm009-goal:executor:{iteration}"],
                "handoff": None,
                "output_contract_ref": GOAL_OUTPUT_CONTRACT,
            }
        accepted = iteration >= 2
        return {
            "schema_version": "belllabs.goal-verifier-observation.v1",
            "decision": "accepted" if accepted else "rejected",
            "progress_made": True,
            "accepted_obligation_refs": [GOAL_OBLIGATION] if accepted else [],
            "findings": [],
            "evidence_refs": [f"evidence:rrm009-goal:verifier:{iteration}"],
            "unmet_obligations": [] if accepted else [GOAL_OBLIGATION],
            "obligation_applicability": [[GOAL_OBLIGATION, True]],
            "output_contract_ref": GOAL_OUTPUT_CONTRACT,
        }


def utc_now() -> datetime:
    return datetime.now(UTC)
