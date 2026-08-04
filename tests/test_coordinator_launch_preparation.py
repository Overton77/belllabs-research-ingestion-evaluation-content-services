from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import InMemoryDefinitionRepository
from app.application.coordinator_launch import (
    CoordinatorLaunchPreparationService,
    InMemoryLaunchTicketRepository,
)
from app.application.coordinator_results import (
    CoordinatorResultService,
    InMemoryWorkflowResultRepository,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    CompilationContext,
    CompileInvocation,
    DefinitionSelector,
    EnvironmentAvailability,
    GoalDirectedBlueprint,
    PublishRequest,
    RunInputManifestRef,
    StageGraphBlueprint,
    WorkflowImplementationBindingDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.coordinator.launch import (
    AdmissionPreviewDecision,
    BlueprintFamily,
    LaunchRequestContext,
    LaunchTicketUnavailable,
    PreparedLaunchTicket,
    RunAdmissionSpec,
    SemanticBindingPlan,
    StageGraphResultDetails,
    WorkflowLaunchProposal,
    WorkflowResultRecord,
)
from app.domain.orchestration.bindings import (
    GoalOperationHandlerBinding,
    RunSemanticInputBinding,
    SemanticHandlerBinding,
    SemanticInputPayload,
    StageHandlerBinding,
)
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    RunOutcome,
    RunPhase,
)
from app.domain.schema_grounding.definitions import (
    register_schema_grounding_extensions,
    schema_grounding_definitions,
)
from app.integrations.control_plane_payloads import InMemoryPayloadStore

NOW = datetime(2026, 7, 25, 16, 0, tzinfo=UTC)
POLICY = sha256_digest("policy-v1")
ENVIRONMENT = sha256_digest("environment-v1")
SCOPE = "tenant-1/research"
TENANT = "tenant-1"
ACTOR = ActorContext(
    actor_id="coordinator-1",
    authority_refs=frozenset({"authority:coordinator"}),
    permissions=frozenset({"workflow_run.admit"}),
)


class AcceptingPreview:
    async def preview(self, _request):
        return AdmissionPreviewDecision(
            accepted=True,
            reason_code="accepted",
            reason="admission preview accepted",
        )


class FixtureSemanticBindingProvider:
    async def prepare(self, proposal, configuration):
        family = (
            BlueprintFamily.STAGE_GRAPH
            if isinstance(configuration.selected_blueprint, StageGraphBlueprint)
            else BlueprintFamily.GOAL_DIRECTED
        )
        return SemanticBindingPlan.create(
            plan_ref=f"semantic-plan:{proposal.idempotency_key}",
            blueprint_family=family,
            exact_input_refs=(configuration.input_manifest.manifest_id,),
            payload={"fixture": proposal.idempotency_key},
        )

    async def author(self, plan, ticket, *, run_id):
        handler = SemanticHandlerBinding(
            handler_id="fixture.handler",
            handler_revision=1,
            input=SemanticInputPayload.from_value(
                schema_ref="schema:fixture:v1",
                value=plan.payload,
            ),
            output_contract_ref="schema:fixture-output:v1",
        )
        values = {
            "request_scope": ticket.request_scope,
            "run_id": run_id,
            "blueprint_family": ticket.blueprint_family.value,
            "effective_configuration_digest": ticket.effective_configuration_digest,
            "blueprint_digest": ticket.blueprint_ref.digest,
            "created_at": ticket.prepared_at,
        }
        if ticket.blueprint_family == BlueprintFamily.STAGE_GRAPH:
            return RunSemanticInputBinding.create(
                **values,
                stage_handlers=(
                    StageHandlerBinding(stage_id="fixture", handler=handler),
                ),
            )
        return RunSemanticInputBinding.create(
            **values,
            goal_operation_handlers=(
                GoalOperationHandlerBinding(
                    operation_class="fixture",
                    handler=handler,
                ),
            ),
            goal_verifier=handler,
            goal_handoff=handler,
        )


async def launch_fixture(
    family: str,
    *,
    idempotency_key: str = "launch-1",
    initial_goal: str | None = None,
    semantic_bindings=None,
):
    repository = InMemoryDefinitionRepository()
    extensions = ExtensionRegistry()
    register_schema_grounding_extensions(extensions)
    control_plane = ControlPlaneService(
        repository,
        extensions,
        InMemoryPayloadStore(),
    )
    published = {}
    revisions = {}
    implementations: list[object] = []
    for definition in schema_grounding_definitions():
        key = (definition.kind, definition.logical_id)
        record = await control_plane.publish(
            PublishRequest(
                definition=definition,
                actor_id="fixture-publisher",
                published_at=NOW,
                expected_head_revision=revisions.get(key, 0),
            )
        )
        revisions[key] = record.ref.revision
        published[definition.logical_id] = record
        if isinstance(definition, WorkflowImplementationBindingDefinition):
            implementations.append(record)

    if family == "StageGraph":
        workflow = published["schema-context-selection"]
        blueprint = published["schema-context-selection-v1"]
        control = published["schema-context-selection-control-v1"]
        runtime = published["schema-context-selection-runtime-v1"]
        workspace = published["schema-context-selection-workspace-v1"]
        evaluation = published["schema-context-selection-evaluation-v1"]
        configuration = published["schema-context-selection-official-v1"]
    else:
        workflow = published["supporting-graph-reconciliation"]
        blueprint = published["supporting-graph-reconciliation-goal-directed-v1"]
        control = published[
            "supporting-graph-reconciliation-goal-directed-control-v1"
        ]
        runtime = published["supporting-graph-reconciliation-runtime-v1"]
        workspace = published["supporting-graph-reconciliation-workspace-v1"]
        evaluation = published["supporting-graph-reconciliation-evaluation-v1"]
        configuration = published["supporting-graph-reconciliation-official-v1"]

    selected_blueprint = blueprint.definition
    assert isinstance(selected_blueprint, StageGraphBlueprint | GoalDirectedBlueprint)
    authority = control.definition.authority_ceiling
    invocation = CompileInvocation(
        workflow_type=DefinitionSelector(exact=workflow.ref),
        blueprint=DefinitionSelector(exact=blueprint.ref),
        control_profile=DefinitionSelector(exact=control.ref),
        runtime_profile=DefinitionSelector(exact=runtime.ref),
        workspace_template=DefinitionSelector(exact=workspace.ref),
        evaluation_profile=DefinitionSelector(exact=evaluation.ref),
        workflow_configuration=DefinitionSelector(exact=configuration.ref),
        input_manifest=RunInputManifestRef(
            manifest_id=f"{family.lower()}-input",
            revision=1,
            digest=sha256_digest({"family": family, "input": "fixture"}),
        ),
        caller_authority=authority,
        parent_authority=authority,
        environment=EnvironmentAvailability(
            capabilities=authority.capabilities,
            runtime_bindings=frozenset({runtime.definition.binding}),
            secret_refs=runtime.definition.required_secrets,
        ),
        context=CompilationContext(
            compilation_id=f"compile-{idempotency_key}",
            compiled_at=NOW,
            actor_id=ACTOR.actor_id,
            authority_subject_id=ACTOR.actor_id,
            authority_scope=SCOPE,
        ),
    )
    proposal = WorkflowLaunchProposal(
        request_scope=SCOPE,
        tenant_scope=TENANT,
        compilation=invocation,
        admission=RunAdmissionSpec(
            actor=ACTOR,
            budget_envelope=BudgetEnvelope(
                dimensions=(
                    BudgetDimensionLimit(
                        dimension="workflow.cycles",
                        applicability=BudgetApplicability.BOUNDED,
                        hard_cap=4,
                    ),
                )
            ),
            requested_at=NOW,
            correlation_id=f"correlation-{idempotency_key}",
            sponsorship_ref="sponsorship:fixture",
            approval_refs=("approval:fixture",),
            delegation_authority_refs=ACTOR.authority_refs,
            admission_evidence_refs=("fixture:admission",),
        ),
        initial_goal=initial_goal,
        policy_snapshot_digest=POLICY,
        environment_snapshot_digest=ENVIRONMENT,
        idempotency_issuer=ACTOR.actor_id,
        idempotency_key=idempotency_key,
    )
    context = LaunchRequestContext(
        caller_id=ACTOR.actor_id,
        tenant_scope=TENANT,
        request_scope=SCOPE,
        approval_refs=("approval:fixture",),
        policy_snapshot_digest=POLICY,
        environment_snapshot_digest=ENVIRONMENT,
        observed_at=NOW,
    )
    tickets = InMemoryLaunchTicketRepository()
    service = CoordinatorLaunchPreparationService(
        compiler=control_plane,
        admission=AcceptingPreview(),
        tickets=tickets,
        semantic_bindings=semantic_bindings or FixtureSemanticBindingProvider(),
        ttl=timedelta(minutes=15),
    )
    return service, tickets, proposal, context


@pytest.mark.asyncio
async def test_stagegraph_rejects_initial_goal_and_public_ticket_redacts_private_input() -> None:
    service, _tickets, proposal, context = await launch_fixture(
        "StageGraph",
        initial_goal="not allowed",
    )
    with pytest.raises(LaunchTicketUnavailable, match="does not accept"):
        await service.prepare(proposal, context)

    service, tickets, proposal, context = await launch_fixture("StageGraph")
    public = await service.prepare(proposal, context)
    payload = public.model_dump(mode="json")
    assert "initial_goal" not in payload
    assert "frozen_run_request" not in payload
    assert "semantic_binding_plan" not in payload
    assert public.semantic_binding_plan_ref
    assert public.semantic_binding_plan_digest
    assert public.initial_goal_digest is None
    private = await tickets.get(public.ticket_id, request_scope=SCOPE)
    assert private is not None
    assert private.frozen_run_request.effective_configuration_digest == (
        public.effective_configuration_digest
    )
    assert private.semantic_binding_plan is not None
    assert private.semantic_binding_plan.plan_digest == (
        public.semantic_binding_plan_digest
    )
    assert (
        PreparedLaunchTicket.model_validate(private.model_dump(mode="json"))
        == private
    )


@pytest.mark.asyncio
async def test_goaldirected_requires_goal_and_freezes_only_its_digest_publicly() -> None:
    service, _tickets, proposal, context = await launch_fixture("GoalDirected")
    with pytest.raises(LaunchTicketUnavailable, match="non-empty"):
        await service.prepare(proposal, context)

    goal = "Reconcile supporting graph evidence within the admitted protected scope."
    service, tickets, proposal, context = await launch_fixture(
        "GoalDirected",
        initial_goal=goal,
    )
    public = await service.prepare(proposal, context)
    assert public.initial_goal_digest == sha256_digest(goal)
    assert goal not in public.model_dump_json()
    private = await tickets.get(public.ticket_id, request_scope=SCOPE)
    assert private is not None and private.initial_goal == goal


@pytest.mark.asyncio
async def test_preparation_rejects_changed_policy_environment_caller_or_tenant() -> None:
    service, _tickets, proposal, context = await launch_fixture("StageGraph")
    for update in (
        {"caller_id": "another-caller"},
        {"tenant_scope": "another-tenant"},
        {"policy_snapshot_digest": sha256_digest("changed-policy")},
        {"environment_snapshot_digest": sha256_digest("changed-environment")},
    ):
        with pytest.raises(RuntimeError):
            await service.prepare(proposal, context.model_copy(update=update))


@pytest.mark.asyncio
async def test_result_polling_returns_phase_then_exact_typed_terminal_result() -> None:
    class Runs:
        phase = RunPhase.ACTIVE

        async def get_run(self, request_scope, run_id):
            return SimpleNamespace(
                request_scope=request_scope,
                run_id=run_id,
                phase=self.phase,
            )

    runs = Runs()
    records = InMemoryWorkflowResultRepository()
    service = CoordinatorResultService(runs=runs, results=records)
    _preparation, _tickets, _proposal, context = await launch_fixture("StageGraph")
    pending = await service.get_workflow_result("run-result-1", context)
    assert pending.phase == "active" and pending.result is None

    result = WorkflowResultRecord(
        run_id="run-result-1",
        tenant_scope=TENANT,
        request_scope=SCOPE,
        blueprint_family=BlueprintFamily.STAGE_GRAPH,
        terminal_outcome=RunOutcome.COMPLETED,
        output_contract_results={"output:fixture": {"accepted": True}},
        artifact_refs=("artifact:fixture",),
        evidence_refs=("evidence:fixture",),
        operation_binding_refs=("binding:fixture",),
        usage_summary={"tokens.total": 10},
        family_result=StageGraphResultDetails(
            execution_epoch=1,
            workflow_cycles=0,
            stage_cycles={"research": 1},
            operation_attempts={"research": 1},
            output_refs={"report": ("artifact:fixture",)},
        ),
        completed_at=NOW,
    )
    await records.save(result)
    runs.phase = RunPhase.TERMINAL
    completed = await service.get_workflow_result("run-result-1", context)
    assert completed.result == result
