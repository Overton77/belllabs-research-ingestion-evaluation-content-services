from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from time import perf_counter
from typing import Any, Literal, cast
from uuid import uuid4

from app.application.capability_search import CapabilitySearchService
from app.application.control_plane import ControlPlaneService
from app.application.control_plane_repository import BeanieDefinitionRepository
from app.application.coordinator_composition import CoordinatorLaunchProductionInputs
from app.application.coordinator_launch import UnavailableRuntimePlanPreparer
from app.application.coordinator_results import CoordinatorResultService
from app.application.coordinator_semantic_bindings import (
    WorkflowSemanticBindingProviderRouter,
)
from app.application.mongo_operation_execution_repository import (
    create_semantic_operation_binding_repository,
)
from app.application.orchestration import (
    F1OrchestrationBindingVerifier,
    GoalDirectedLaunchService,
    RunControlLifecycleGateway,
    StageGraphLaunchService,
    WorkflowLaunchDispatcher,
    orchestration_lifecycle_actor,
)
from app.application.orchestration_binding_repository import (
    RunSemanticInputBindingService,
)
from app.application.postgres_capability_search_repository import (
    PostgresCatalogSearchRepository,
)
from app.application.postgres_launch_ticket_repository import (
    PostgresLaunchTicketRepository,
)
from app.application.postgres_orchestration_binding_repository import (
    PostgresRunSemanticInputBindingRepository,
)
from app.application.postgres_run_control_repository import (
    PostgresRunControlRepository,
)
from app.application.postgres_workflow_result_repository import (
    PostgresWorkflowResultRepository,
)
from app.application.run_control import (
    REQUIRED_SHARED_BUDGET_DIMENSIONS,
    AdmissionPolicyRegistry,
    F1RunConfigurationVerifier,
    RunControlService,
    run_identity_for,
)
from app.application.schema_authority_issuance import (
    SchemaAuthorityIssuanceService,
)
from app.application.schema_catalog import (
    CATALOG_GENERATOR_VERSION,
    parse_schema_catalog,
)
from app.application.schema_catalog_build import SchemaCatalogBuildService
from app.application.schema_context_stage_handlers import (
    SchemaContextBindingPlanInput,
    SchemaContextSemanticBindingProvider,
)
from app.application.schema_grounding_admission import (
    register_schema_grounding_admission_policies,
)
from app.application.schema_grounding_repository import (
    BeanieSchemaGroundingRecordRepository,
)
from app.application.schema_grounding_semantic_handlers import (
    SupportingGraphBindingPlanInput,
    SupportingGraphSemanticBindingProvider,
)
from app.application.schema_workspace_binding import SchemaGraphAdmissionService
from app.application.semantic_operation_bindings import (
    SemanticOperationBindingRepository,
    SemanticOperationBindingTemplates,
    SemanticOperationExecutionBindingService,
)
from app.application.supporting_graph_reconciliation import (
    SupportingGraphReconciliationWorkflow,
)
from app.config import Settings
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    CompilationContext,
    CompileInvocation,
    DefinitionKind,
    DefinitionSelector,
    EnvironmentAvailability,
    ExactDefinitionRef,
    PublishedDefinition,
    RunInputManifestRef,
    RuntimeProfileDefinition,
    SkillDefinition,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkspaceTemplateDefinition,
)
from app.domain.control_plane.extensions import ExtensionRegistry
from app.domain.coordinator.contracts import (
    AuthorizationState,
    CapabilitySearchRequest,
)
from app.domain.coordinator.launch import (
    AdmissionPreviewDecision,
    BlueprintFamily,
    GoalDirectedResultDetails,
    LaunchRequestContext,
    RunAdmissionSpec,
    StageGraphResultDetails,
    WorkflowLaunchProposal,
    WorkflowResultRecord,
)
from app.domain.operation_execution.contracts import (
    CapabilityGrant,
    ImmutableAssetBinding,
    ModelPolicy,
    OperationAttemptIdentity,
    OperationExecutionRequest,
    PromptSegment,
    PromptTrustClass,
    WorkspaceContract,
)
from app.domain.run_control.contracts import (
    ActorContext,
    BudgetApplicability,
    BudgetDimensionLimit,
    BudgetEnvelope,
    RunOutcome,
)
from app.domain.schema_context.contracts import (
    QueryExecutionIntent,
    SchemaContextSelectionRequest,
    SchemaOperationProjection,
)
from app.domain.schema_grounding.contracts import (
    DurableObjectRef,
    GraphAdmissionRequest,
    SchemaAuthorityIssuanceRequest,
    SchemaCatalogBuildRequest,
    SupportingGraphReconciliationRequest,
)
from app.domain.schema_grounding.definitions import (
    register_schema_grounding_extensions,
)
from app.integrations.capability_embeddings import OpenAICapabilityEmbeddingAdapter
from app.integrations.catalog_projection_admin import list_published_definition_refs
from app.integrations.control_plane_payloads import S3PayloadStore
from app.integrations.langsmith_tracing import configure_langsmith_tracing
from app.integrations.mongodb import create_mongodb
from app.integrations.neo4j import create_neo4j
from app.integrations.neo4j_schema_deployment import (
    Neo4jLiveSchemaDeploymentReader,
    schema_authority_issuer_identities,
)
from app.integrations.postgres import (
    create_application_postgres_pool,
    create_postgres_pool,
)
from app.integrations.schema_agent_sandbox import SandboxAgentHarness
from app.integrations.schema_catalog_payloads import schema_catalog_payload_store
from app.integrations.schema_grounding_payloads import schema_grounding_input_store
from app.integrations.schema_neo4j_executor import Neo4jBoundedReadExecutorFactory
from app.integrations.temporal import create_temporal_client
from app.integrations.temporal_workflow_submission import TemporalWorkflowSubmitter
from app.temporal.coordinator_runtime import (
    SchemaGroundingCoordinatorRuntimeDependencies,
    coordinator_task_queues,
    create_coordinator_workers,
    create_schema_grounding_coordinator_runtime,
)
from app.temporal.goal_directed_workflow import GoalDirectedWorkflow
from app.temporal.stagegraph_workflow import StageGraphWorkflow

SCENARIO_A_QUERY = (
    "select a purpose-bound schema context from an admitted report with deterministic "
    "validation and independent review"
)
SCENARIO_C_QUERY = (
    "perform bounded adaptive supporting graph reconciliation with independent "
    "verification and deterministic convergence"
)


class ReadOnlyAdmissionPreview:
    def __init__(
        self,
        verifier: F1RunConfigurationVerifier,
        policies: AdmissionPolicyRegistry,
    ) -> None:
        self._verifier = verifier
        self._policies = policies

    async def preview(self, request: Any) -> AdmissionPreviewDecision:
        try:
            configuration = await self._verifier.verify(request)
            await self._policies.validate(request, configuration)
        except Exception as error:
            return AdmissionPreviewDecision(
                accepted=False,
                reason_code=type(error).__name__,
                reason="read-only schema-grounding admission preview rejected",
            )
        return AdmissionPreviewDecision(
            accepted=True,
            reason_code="accepted",
            reason="read-only schema-grounding admission preview accepted",
        )


async def run_live_schema_grounding_coordinator(
    args: Any,
    *,
    artifact_root: Path,
) -> dict[str, Any]:
    base_settings = Settings()
    settings = base_settings.model_copy(
        update={"s3_bucket": args.artifact_bucket or base_settings.s3_bucket}
    )
    if not settings.s3_bucket:
        raise RuntimeError("Scenario A/C live execution requires configured S3_BUCKET")
    configure_langsmith_tracing(settings)
    mongo_client, _ = await create_mongodb(settings)
    capability_pool = await create_postgres_pool(settings)
    application_pool = await create_application_postgres_pool(settings)
    neo4j = await create_neo4j(settings)
    harness = SandboxAgentHarness(model=args.model, image=settings.sandbox_image)
    try:
        definitions = BeanieDefinitionRepository()
        control_plane = _control_plane(definitions, settings)
        refs = await list_published_definition_refs()
        records = tuple([await definitions.get(ref) for ref in refs])
        search = CapabilitySearchService(
            search=PostgresCatalogSearchRepository(capability_pool),
            definitions=definitions,
            embeddings=OpenAICapabilityEmbeddingAdapter(settings),
            embedding_model_id=settings.capability_embedding_model,
            embedding_dimensions=settings.capability_embedding_dimensions,
        )
        workflow_a, search_a = await _retrieve_workflow(
            search,
            definitions=definitions,
            query=SCENARIO_A_QUERY,
            tenant_scope=args.tenant_scope,
            expected_logical_id="schema-context-selection",
        )
        workflow_c, search_c = await _retrieve_workflow(
            search,
            definitions=definitions,
            query=SCENARIO_C_QUERY,
            tenant_scope=args.tenant_scope,
            expected_logical_id="supporting-graph-reconciliation",
        )
        implementation_a = _implementation_for(
            records,
            workflow_a.ref,
            blueprint_logical_id="schema-context-selection-v1",
        )
        implementation_c = _implementation_for(
            records,
            workflow_c.ref,
            blueprint_logical_id="supporting-graph-reconciliation-goal-directed-v1",
        )
        runtime_a, workspace_a = _runtime_workspace(records, implementation_a)
        runtime_c, workspace_c = _runtime_workspace(records, implementation_c)
        profile_a = _current_record(
            records,
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.schema-context-selection-v1",
        )
        profile_c = _current_record(
            records,
            DefinitionKind.AGENT_PROFILE,
            "agent-profile.supporting-graph-reconciliation-v1",
        )

        schema_bytes, overlay_bytes, report_bytes = await asyncio.gather(
            asyncio.to_thread(args.schema.resolve(strict=True).read_bytes),
            asyncio.to_thread(args.semantic_overlay.resolve(strict=True).read_bytes),
            asyncio.to_thread(args.report.resolve(strict=True).read_bytes),
        )
        schema_store = schema_grounding_input_store(
            settings,
            settings.s3_bucket,
            "schema",
        )
        overlay_store = schema_grounding_input_store(
            settings,
            settings.s3_bucket,
            "semantic_overlay",
        )
        report_store = schema_grounding_input_store(
            settings,
            settings.s3_bucket,
            "report",
        )
        schema_address, overlay_address, report_address = await asyncio.gather(
            schema_store.put(schema_bytes),
            overlay_store.put(overlay_bytes),
            report_store.put(report_bytes),
        )
        schema_ref = _durable(schema_address, "application/graphql")
        overlay_ref = _durable(overlay_address, "application/json")
        report_ref = _durable(report_address, "text/markdown; charset=utf-8")
        catalog = parse_schema_catalog(
            schema_bytes,
            schema_ref.uri,
            semantic_overlay=args.semantic_overlay.resolve(strict=True),
        )

        now = datetime.now(UTC)
        identity_suffix = uuid4().hex
        actor = ActorContext(
            actor_id=args.actor_id,
            authority_refs=frozenset({"authority:coordinator-schema-grounding-live"}),
            permissions=frozenset({"workflow_run.admit"}),
        )
        build_request = SchemaCatalogBuildRequest(
            build_id=f"schema-catalog-live-{identity_suffix}",
            idempotency_key=f"schema-catalog-live-{identity_suffix}",
            request_scope=args.request_scope,
            schema_definition_ref=schema_ref.uri,
            schema_definition_digest=schema_ref.digest,
            semantic_overlay_ref=overlay_ref.uri,
            semantic_overlay_revision="1",
            semantic_overlay_digest=overlay_ref.digest,
            catalog_schema_version="1",
            generator_version=CATALOG_GENERATOR_VERSION,
            normalization_policy_version="graphql-sdl-normalization-v1",
            publication_target=f"s3://{settings.s3_bucket}/schema-grounding/catalog-builds",
            actor_id=actor.actor_id,
            authority_ref="authority:schema-catalog-build-service",
            requested_at=now,
        )
        grounding_records = BeanieSchemaGroundingRecordRepository()
        catalog_payloads = schema_catalog_payload_store(settings)
        catalog_builds = SchemaCatalogBuildService(grounding_records, catalog_payloads)
        build_record = await catalog_builds.build(
            build_request,
            schema_definition=schema_bytes,
            semantic_overlay=overlay_bytes,
        )
        if (
            build_record.catalog_digest != catalog.catalog_digest
            or build_record.resource_manifest_digest is None
        ):
            raise RuntimeError("durable catalog build differs from deterministic live inputs")

        selection_request = SchemaContextSelectionRequest(
            request_id=f"schema-selection-live-{identity_suffix}",
            purpose="pre_ingestion_graph_reconciliation",
            intended_operations=("read", "exact_lookup", "bounded_traversal"),
            schema_definition_ref=schema_ref.uri,
            schema_definition_digest=schema_ref.digest,
            catalog_digest=catalog.catalog_digest,
            report_ref=report_ref.uri,
            report_digest=report_ref.digest,
            coverage_obligations=(
                "organization_identity",
                "offered_products",
                "lab_tests_panels_platforms",
                "biomarker_sample",
                "legacy_state_snapshot_mapping",
                "provenance_boundary",
            ),
            workspace_ref="belllabs://schema-workspaces/{run_id}/selection",
            created_at=now,
        )
        operation_repository = create_semantic_operation_binding_repository(settings)
        freezer = SemanticOperationExecutionBindingService(operation_repository)
        provider_a = SchemaContextSemanticBindingProvider(
            SchemaContextBindingPlanInput(
                build_request=build_request,
                selection_request=selection_request,
                schema_definition=schema_ref,
                semantic_overlay=overlay_ref,
                report=report_ref,
                operation_bindings=_operation_templates(
                    ("semantic_selector", "independent_reviewer"),
                    records=records,
                    profile=profile_a,
                    runtime=runtime_a,
                    workspace=workspace_a,
                    request_scope=args.request_scope,
                    model=args.model,
                    created_at=now,
                ),
                created_at=now,
            ),
            freezer,
        )

        projection = SchemaOperationProjection.model_validate_json(
            args.projection.resolve(strict=True).read_text(encoding="utf-8")
        )
        intents = tuple(
            QueryExecutionIntent.model_validate_json(
                path.resolve(strict=True).read_text(encoding="utf-8")
            )
            for path in args.intent
        )
        _verify_projection_inputs(
            projection,
            intents,
            schema_digest=schema_ref.digest,
            catalog_digest=catalog.catalog_digest,
        )
        idempotency_key_a = f"scenario-a-live:{identity_suffix}"
        idempotency_key_c = f"scenario-c-live:{identity_suffix}"
        run_id_c = run_identity_for(
            args.request_scope,
            actor.actor_id,
            idempotency_key_c,
        )
        authority_request = SchemaAuthorityIssuanceRequest(
            request_scope=args.request_scope,
            run_id=run_id_c,
            environment="production",
            database=args.database,
            deployment_id=args.deployment_id,
            schema_definition_ref=catalog.source_ref,
            schema_definition_digest=schema_ref.digest,
            catalog_build_id=build_record.build_id,
            catalog_digest=catalog.catalog_digest,
            resource_manifest_digest=build_record.resource_manifest_digest,
            workspace_id=f"schema-workspace:{run_id_c}",
            slot_name="graph_query_runtime",
            profile="graph-query-runtime",
            purpose="read_query_reconciliation",
            workspace_read_only=True,
            requested_graph_access="read",
            query_kinds=frozenset(intent.query_kind for intent in intents),
            allowed_node_labels=frozenset(
                label for intent in intents for label in intent.labels
            ),
            allowed_relationship_types=frozenset(
                relationship
                for intent in intents
                for relationship in intent.relationship_types
            ),
            maximum_limit=max(intent.limit for intent in intents),
            maximum_traversal_depth=max(intent.max_depth for intent in intents),
            secret_ref="secret:neo4j-readonly:v1",
            budget_reservation_id=f"reservation:{run_id_c}:graph-reads",
            sensitive_data_policy_ref="policy:sensitive-data:v1",
            requested_at=now,
        )
        authority_bundle = await SchemaAuthorityIssuanceService(
            deployment_reader=Neo4jLiveSchemaDeploymentReader(neo4j),
            records=grounding_records,
            identities=schema_authority_issuer_identities(settings),
        ).issue(authority_request)
        graph_admission = GraphAdmissionRequest(
            request_scope=args.request_scope,
            run_id=run_id_c,
            purpose="read_query_reconciliation",
            environment="production",
            database=args.database,
            deployment_id=authority_bundle.deployment_manifest.deployment_id,
            catalog_build_id=build_record.build_id,
            schema_definition_ref=catalog.source_ref,
            schema_definition_digest=schema_ref.digest,
            catalog_digest=catalog.catalog_digest,
            resource_manifest_digest=build_record.resource_manifest_digest,
            projection_id=projection.projection_id,
            projection_digest=projection.projection_digest,
            deployment_manifest=authority_bundle.deployment_manifest,
            workspace_binding=authority_bundle.workspace_binding,
            graph_capability=authority_bundle.graph_capability,
            requested_at=now,
        )
        reconciliation_request = SupportingGraphReconciliationRequest(
            reconciliation_id=f"supporting-graph-live-{identity_suffix}",
            request_scope=args.request_scope,
            run_id=run_id_c,
            question="Which existing TruDiagnostic graph facts match the admitted report?",
            admission=graph_admission,
            projection=projection,
            intents=intents,
            maximum_intents=len(intents),
            created_at=now,
        )
        provider_c = SupportingGraphSemanticBindingProvider(
            SupportingGraphBindingPlanInput(
                request=reconciliation_request,
                minimum_successful_intents=1,
                handoff_instructions="Resume only from immutable admitted graph evidence.",
                operation_bindings=_operation_templates(
                    ("goal_iteration",),
                    records=records,
                    profile=profile_c,
                    runtime=runtime_c,
                    workspace=workspace_c,
                    request_scope=args.request_scope,
                    model=args.model,
                    created_at=now,
                ),
                created_at=now,
            ),
            freezer,
        )
        providers = WorkflowSemanticBindingProviderRouter(
            {
                "schema-context-selection": provider_a,
                "supporting-graph-reconciliation": provider_c,
            }
        )

        policies = AdmissionPolicyRegistry()
        register_schema_grounding_admission_policies(policies)
        verifier = F1RunConfigurationVerifier(control_plane)
        run_control = RunControlService(
            PostgresRunControlRepository(application_pool),
            verifier,
            policies,
        )
        tickets = PostgresLaunchTicketRepository(application_pool)
        temporal = await create_temporal_client(settings)
        base_queue = args.task_queue or f"{settings.temporal_task_queue}-schema-live"
        queues = coordinator_task_queues(base_queue)
        semantic_repository = PostgresRunSemanticInputBindingRepository(application_pool)
        dispatcher = WorkflowLaunchDispatcher(
            stagegraph=StageGraphLaunchService(run_control, control_plane),
            goal_directed=GoalDirectedLaunchService(run_control, control_plane),
            run_control=run_control,
            control_plane=control_plane,
        )
        launch_inputs = CoordinatorLaunchProductionInputs(
            compiler=control_plane,
            admission_preview=ReadOnlyAdmissionPreview(verifier, policies),
            admission=run_control,
            tickets=tickets,
            dispatcher=dispatcher,
            submissions=TemporalWorkflowSubmitter(
                temporal,
                stagegraph_task_queue=queues.stagegraph,
                goal_directed_task_queue=queues.goal_directed,
            ),
            semantic_bindings=providers,
            runtime_plans=UnavailableRuntimePlanPreparer(),
            binding_service=RunSemanticInputBindingService(semantic_repository),
        )
        preparation, launcher = launch_inputs.build()
        proposal_a, context_a = _proposal(
            workflow_a,
            implementation_a,
            runtime_a,
            actor=actor,
            tenant_scope=args.tenant_scope,
            request_scope=args.request_scope,
            idempotency_key=idempotency_key_a,
            now=now,
            input_digest=sha256_digest(
                {
                    "build": build_request,
                    "selection": selection_request,
                }
            ),
            admission_evidence=(
                f"schema-definition:{schema_ref.digest}",
                f"schema-catalog-build:{build_record.build_id}",
                f"semantic-overlay:{overlay_ref.digest}",
                "sensitive-data-policy:policy:sensitive-data:v1",
            ),
        )
        proposal_c, context_c = _proposal(
            workflow_c,
            implementation_c,
            runtime_c,
            actor=actor,
            tenant_scope=args.tenant_scope,
            request_scope=args.request_scope,
            idempotency_key=idempotency_key_c,
            now=now,
            input_digest=sha256_digest(reconciliation_request),
            initial_goal=(
                "Reconcile the admitted TruDiagnostic supporting graph using only "
                "projection-bound bounded read intents; stop after independent verification."
            ),
            admission_evidence=(
                f"schema-definition:{schema_ref.digest}",
                f"schema-catalog-build:{build_record.build_id}",
                "schema-deployment-manifest:"
                f"{authority_bundle.deployment_manifest.manifest_id}",
                f"schema-workspace-binding:{authority_bundle.workspace_binding.binding_id}",
                f"graph-capability:{authority_bundle.graph_capability.grant_id}",
                "sensitive-data-policy:policy:sensitive-data:v1",
            ),
        )
        prepare_started_a = perf_counter()
        public_a = await preparation.prepare(proposal_a, context_a)
        prepare_latency_a_ms = int((perf_counter() - prepare_started_a) * 1000)
        prepare_started_c = perf_counter()
        public_c = await preparation.prepare(proposal_c, context_c)
        prepare_latency_c_ms = int((perf_counter() - prepare_started_c) * 1000)
        lifecycle = RunControlLifecycleGateway(
            run_control,
            F1OrchestrationBindingVerifier(control_plane),
            orchestration_lifecycle_actor(),
        )
        reconciliation = SupportingGraphReconciliationWorkflow(
            admission=SchemaGraphAdmissionService(grounding_records),
            executor_factory=Neo4jBoundedReadExecutorFactory(settings),
            records=grounding_records,
        )
        runtime = create_schema_grounding_coordinator_runtime(
            application_postgres_pool=application_pool,
            dependencies=SchemaGroundingCoordinatorRuntimeDependencies(
                lifecycle=lifecycle,
                records=grounding_records,
                catalog_builds=catalog_builds,
                sources=schema_store,
                catalog_payloads=catalog_payloads,
                selector=harness,
                reviewer=harness,
                reconciliations=reconciliation,
                operation_bindings=operation_repository,
            ),
        )
        workers = create_coordinator_workers(
            temporal,
            task_queues=queues,
            activities=runtime.activities,
        )
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(workers.stagegraph)
            await stack.enter_async_context(workers.goal_directed)
            handle_a = await launcher.launch(public_a.ticket_id, context_a)
            if handle_a.run_id != run_identity_for(
                args.request_scope,
                actor.actor_id,
                idempotency_key_a,
            ):
                raise RuntimeError("Scenario A admission returned an unexpected run identity")
            result_a = await temporal.get_workflow_handle_for(
                StageGraphWorkflow.run,
                handle_a.workflow_id,
            ).result()
            handle_c = await launcher.launch(public_c.ticket_id, context_c)
            if handle_c.run_id != run_id_c:
                raise RuntimeError("Scenario C admission returned an unexpected run identity")
            result_c = await temporal.get_workflow_handle_for(
                GoalDirectedWorkflow.run,
                handle_c.workflow_id,
            ).result()

        results = PostgresWorkflowResultRepository(application_pool)
        record_a = await results.save(
            await _stage_result_record(
                result_a,
                handle=handle_a,
                tenant_scope=args.tenant_scope,
                request_scope=args.request_scope,
                operation_repository=operation_repository,
            )
        )
        record_c = await results.save(
            await _goal_result_record(
                result_c,
                handle=handle_c,
                tenant_scope=args.tenant_scope,
                request_scope=args.request_scope,
                operation_repository=operation_repository,
            )
        )
        result_service = CoordinatorResultService(runs=run_control, results=results)
        retrieved_a, retrieved_c = await asyncio.gather(
            result_service.get_workflow_result(handle_a.run_id, context_a),
            result_service.get_workflow_result(handle_c.run_id, context_c),
        )
        mongo_audit_a, mongo_audit_c, transitions_a, transitions_c = await asyncio.gather(
            grounding_records.list_for_run(args.request_scope, handle_a.run_id),
            grounding_records.list_for_run(args.request_scope, handle_c.run_id),
            run_control.list_transitions(args.request_scope, handle_a.run_id),
            run_control.list_transitions(args.request_scope, handle_c.run_id),
        )
        artifact = {
            "mode": "live-schema-grounding-coordinator",
            "s3_bucket": settings.s3_bucket,
            "durable_inputs": {
                "schema": schema_ref.model_dump(mode="json"),
                "overlay": overlay_ref.model_dump(mode="json"),
                "report": report_ref.model_dump(mode="json"),
                "catalog_bundle": (
                    build_record.bundle.model_dump(mode="json")
                    if build_record.bundle is not None
                    else None
                ),
            },
            "scenario_a": {
                "search_request": search_a.model_dump(mode="json"),
                "workflow_ref": workflow_a.ref.model_dump(mode="json"),
                "ticket_id": public_a.ticket_id,
                "run_id": handle_a.run_id,
                "workflow_id": handle_a.workflow_id,
                "temporal_run_id": handle_a.temporal_run_id,
                "semantic_binding_plan_ref": public_a.semantic_binding_plan_ref,
                "operation_binding_refs": list(record_a.operation_binding_refs),
                "typed_result": retrieved_a.model_dump(mode="json"),
                "metrics": {
                    "prepare_latency_ms": prepare_latency_a_ms,
                    "operator_correction_count": 0,
                },
                "audit_refs": _audit_refs(mongo_audit_a, transitions_a),
            },
            "scenario_c": {
                "search_request": search_c.model_dump(mode="json"),
                "workflow_ref": workflow_c.ref.model_dump(mode="json"),
                "ticket_id": public_c.ticket_id,
                "run_id": handle_c.run_id,
                "workflow_id": handle_c.workflow_id,
                "temporal_run_id": handle_c.temporal_run_id,
                "semantic_binding_plan_ref": public_c.semantic_binding_plan_ref,
                "authority_bundle": authority_bundle.model_dump(mode="json"),
                "operation_binding_refs": list(record_c.operation_binding_refs),
                "typed_result": retrieved_c.model_dump(mode="json"),
                "metrics": {
                    "prepare_latency_ms": prepare_latency_c_ms,
                    "operator_correction_count": 0,
                },
                "audit_refs": _audit_refs(mongo_audit_c, transitions_c),
            },
        }
        artifact_path = artifact_root / f"schema-grounding-{identity_suffix}.json"
        await asyncio.to_thread(
            artifact_path.write_text,
            __import__("json").dumps(artifact, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        artifact["artifact_path"] = str(artifact_path)
        return artifact
    finally:
        harness.close()
        await neo4j.close()
        await application_pool.close()
        await capability_pool.close()
        await mongo_client.close()


def _control_plane(
    definitions: BeanieDefinitionRepository,
    settings: Settings,
) -> ControlPlaneService:
    assert settings.s3_bucket is not None
    extensions = ExtensionRegistry()
    register_schema_grounding_extensions(extensions)
    return ControlPlaneService(
        definitions,
        extensions,
        S3PayloadStore(settings, settings.s3_bucket),
        externalize_above_bytes=15_000_000,
    )


async def _retrieve_workflow(
    search: CapabilitySearchService,
    *,
    definitions: BeanieDefinitionRepository,
    query: str,
    tenant_scope: str,
    expected_logical_id: str,
) -> tuple[PublishedDefinition, CapabilitySearchRequest]:
    request = CapabilitySearchRequest(
        query=query,
        kinds=frozenset({DefinitionKind.WORKFLOW_TYPE}),
        tenant_scope=tenant_scope,
        limit=10,
    )
    response = await search.search(request)
    for hit in response.hits:
        ref = hit.exact_ref
        if (
            ref is None
            or hit.candidate_id is not None
            or hit.authorization_state != AuthorizationState.SELECTABLE
        ):
            continue
        if ref.logical_id != expected_logical_id:
            continue
        record = await definitions.get(ref)
        if record.ref != ref or sha256_digest(record.definition) != ref.digest:
            raise RuntimeError("retrieved Workflow Type failed exact Mongo rehydration")
        return record, request
    raise RuntimeError(
        "natural-language catalog retrieval did not return the required current "
        f"Workflow Type: {expected_logical_id}"
    )


def _implementation_for(
    records: tuple[PublishedDefinition, ...],
    workflow_ref: ExactDefinitionRef,
    *,
    blueprint_logical_id: str,
) -> PublishedDefinition:
    matches = tuple(
        record
        for record in records
        if record.retired_at is None
        and isinstance(
            record.definition,
            WorkflowImplementationBindingDefinition,
        )
        and record.definition.workflow_type_ref == workflow_ref
        and record.definition.blueprint_ref.logical_id == blueprint_logical_id
    )
    if len(matches) != 1:
        raise RuntimeError(
            "catalog must contain exactly one implementation for "
            f"{workflow_ref.logical_id}/{blueprint_logical_id}"
        )
    return matches[0]


def _current_record(
    records: tuple[PublishedDefinition, ...],
    kind: DefinitionKind,
    logical_id: str,
) -> PublishedDefinition:
    matches = tuple(
        record
        for record in records
        if record.retired_at is None
        and record.ref.kind == kind
        and record.ref.logical_id == logical_id
    )
    if not matches:
        raise RuntimeError(f"catalog head is unavailable: {kind.value}:{logical_id}")
    return max(matches, key=lambda record: record.ref.revision)


def _runtime_workspace(
    records: tuple[PublishedDefinition, ...],
    implementation: PublishedDefinition,
) -> tuple[PublishedDefinition, PublishedDefinition]:
    definition = implementation.definition
    if not isinstance(definition, WorkflowImplementationBindingDefinition):
        raise TypeError("implementation record has the wrong definition kind")
    runtime = _current_record(
        records,
        DefinitionKind.RUNTIME_PROFILE,
        definition.runtime_profile_ref.logical_id,
    )
    workspace = _current_record(
        records,
        DefinitionKind.WORKSPACE_TEMPLATE,
        definition.workspace_template_ref.logical_id,
    )
    if runtime.ref != definition.runtime_profile_ref:
        raise RuntimeError("implementation runtime is not the current exact catalog head")
    if workspace.ref != definition.workspace_template_ref:
        raise RuntimeError("implementation workspace is not the current exact catalog head")
    return runtime, workspace


def _durable(address: Any, media_type: str) -> DurableObjectRef:
    return DurableObjectRef(
        uri=address.uri,
        digest=address.digest,
        size_bytes=address.size,
        media_type=media_type,
        version_id=address.version_id,
    )


def _operation_templates(
    operation_ids: tuple[str, ...],
    *,
    records: tuple[PublishedDefinition, ...],
    profile: PublishedDefinition,
    runtime: PublishedDefinition,
    workspace: PublishedDefinition,
    request_scope: str,
    model: str,
    created_at: datetime,
) -> SemanticOperationBindingTemplates:
    if not isinstance(profile.definition, AgentProfileDefinition):
        raise TypeError("schema operation profile has the wrong definition kind")
    if not isinstance(runtime.definition, RuntimeProfileDefinition):
        raise TypeError("schema runtime has the wrong definition kind")
    if not isinstance(workspace.definition, WorkspaceTemplateDefinition):
        raise TypeError("schema workspace has the wrong definition kind")
    runtime_digest = sha256_digest(runtime.definition)
    settings = profile.definition.model_policy.settings
    reasoning = settings.get("reasoning_effort", "low")
    if reasoning not in {"minimal", "low", "medium", "high"}:
        raise ValueError("schema Agent Profile has an unsupported reasoning effort")
    max_turns = settings.get("max_turns", 20)
    if not isinstance(max_turns, int):
        raise ValueError("schema Agent Profile max_turns must be an integer")
    writable = tuple(
        slot.path
        for slot in workspace.definition.slots
        if slot.access == "exclusive_write"
    )
    prompt = "Execute only the exact admitted schema operation contract."
    operations = {
        operation_id: OperationExecutionRequest(
            identity=OperationAttemptIdentity(
                run_id="{run_id}",
                operation_id=operation_id,
                operation_attempt=1,
            ),
            request_scope=request_scope,
            effective_configuration_digest="sha256:" + "0" * 64,
            run_control_revision=1,
            operation_contract_ref=f"operation-contract:{operation_id}:v1",
            prompt_segments=(
                PromptSegment(
                    source_ref=next(iter(profile.definition.prompt_refs)).logical_id,
                    source_revision=next(iter(profile.definition.prompt_refs)).revision,
                    trust_class=PromptTrustClass.SYSTEM_AUTHORITY,
                    content=prompt,
                    rendered_digest=sha256_digest(prompt),
                ),
            ),
            model_policy=ModelPolicy(
                provider=profile.definition.model_policy.provider,
                model=model,
                reasoning_effort=cast(
                    Literal["minimal", "low", "medium", "high"],
                    reasoning,
                ),
                max_turns=max_turns,
            ),
            skills=tuple(
                ImmutableAssetBinding(
                    ref=skill_ref,
                    manifest_digest=_skill_manifest_digest(records, skill_ref),
                    mount_path=f"/skills/{skill_ref.logical_id}/SKILL.md",
                )
                for skill_ref in sorted(
                    profile.definition.skill_refs,
                    key=lambda ref: ref.logical_id,
                )
            ),
            agent_profile_ref=profile.ref,
            capability_grant=CapabilityGrant(
                capabilities=profile.definition.maximum_capability_request.capabilities,
                data_scope_refs=frozenset({"schema-catalog:exact", "graph-projection:exact"}),
            ),
            workspace=WorkspaceContract(
                namespace_id=f"schema-grounding/{{run_id}}/{operation_id}",
                workspace_id=f"schema-workspace:{{run_id}}:{operation_id}",
                provider=runtime.definition.binding,
                template_ref=workspace.ref,
                exclusive_write_paths=writable,
                network_policy="none",
                runtime_digest=runtime_digest,
                image_digest=runtime_digest,
                package_digest=runtime_digest,
                environment_digest=runtime_digest,
            ),
            budget_reservation_id=f"reservation:{{run_id}}:{operation_id}",
            budget_limits={"operation.attempts": 1, "model.turns": max_turns},
            tracing_policy_ref="tracing:schema-grounding-live@1",
            sensitive_data_policy_ref="policy:sensitive-data:v1",
            snapshot_policy_ref="snapshot:schema-grounding-live@1",
            requested_at=created_at,
            idempotency_key=f"schema-grounding:{{run_id}}:{operation_id}:1",
        )
        for operation_id in operation_ids
    }
    return SemanticOperationBindingTemplates(operations=operations)


def _skill_manifest_digest(
    records: tuple[PublishedDefinition, ...],
    ref: ExactDefinitionRef,
) -> str:
    matches = tuple(record for record in records if record.ref == ref)
    if len(matches) != 1 or not isinstance(matches[0].definition, SkillDefinition):
        raise RuntimeError(
            f"schema Agent Profile skill is unavailable by exact ref: {ref.logical_id}"
        )
    return matches[0].definition.manifest_digest


def _verify_projection_inputs(
    projection: SchemaOperationProjection,
    intents: tuple[QueryExecutionIntent, ...],
    *,
    schema_digest: str,
    catalog_digest: str,
) -> None:
    if not intents:
        raise ValueError("Scenario C requires at least one bounded query intent")
    if (
        projection.source_schema_digest != schema_digest
        or projection.purpose != "read_query_reconciliation"
    ):
        raise ValueError("Scenario C projection differs from the exact durable catalog")
    if not catalog_digest.startswith("sha256:"):
        raise ValueError("Scenario C catalog digest is not content addressed")
    for sequence, intent in enumerate(intents, start=1):
        if (
            intent.sequence != sequence
            or intent.projection_id != projection.projection_id
            or intent.projection_digest != projection.projection_digest
            or intent.schema_definition_digest != schema_digest
        ):
            raise ValueError("Scenario C intent is not ordered and bound to the exact projection")


def _proposal(
    workflow: PublishedDefinition,
    implementation: PublishedDefinition,
    runtime: PublishedDefinition,
    *,
    actor: ActorContext,
    tenant_scope: str,
    request_scope: str,
    idempotency_key: str,
    now: datetime,
    input_digest: str,
    admission_evidence: tuple[str, ...],
    initial_goal: str | None = None,
) -> tuple[WorkflowLaunchProposal, LaunchRequestContext]:
    if not isinstance(workflow.definition, WorkflowTypeDefinition):
        raise TypeError("workflow record has the wrong definition kind")
    if not isinstance(runtime.definition, RuntimeProfileDefinition):
        raise TypeError("runtime record has the wrong definition kind")
    authority = workflow.definition.authority_ceiling
    invocation = CompileInvocation(
        workflow_type=DefinitionSelector(exact=workflow.ref),
        implementation=DefinitionSelector(exact=implementation.ref),
        input_manifest=RunInputManifestRef(
            manifest_id=f"schema-grounding-live:{idempotency_key}",
            revision=1,
            digest=input_digest,
        ),
        caller_authority=authority,
        parent_authority=authority,
        environment=EnvironmentAvailability(
            capabilities=authority.capabilities,
            runtime_bindings=frozenset({runtime.definition.binding}),
            secret_refs=runtime.definition.required_secrets,
        ),
        context=CompilationContext(
            compilation_id=f"schema-grounding-live:{uuid4()}",
            compiled_at=now,
            actor_id=actor.actor_id,
            authority_subject_id=actor.actor_id,
            authority_scope=request_scope,
        ),
    )
    ceiling = authority.budgets.dimensions
    budget = BudgetEnvelope(
        dimensions=tuple(
            BudgetDimensionLimit(
                dimension=dimension,
                applicability=(
                    BudgetApplicability.BOUNDED
                    if dimension in ceiling
                    else BudgetApplicability.NOT_APPLICABLE
                ),
                hard_cap=(
                    min(ceiling[dimension], authority.max_concurrency)
                    if dimension == "concurrency.slots" and dimension in ceiling
                    else ceiling.get(dimension)
                ),
            )
            for dimension in sorted(REQUIRED_SHARED_BUDGET_DIMENSIONS)
        )
    )
    policy = sha256_digest("schema-grounding-live-policy-v1")
    environment = sha256_digest("schema-grounding-live-environment-v1")
    proposal = WorkflowLaunchProposal(
        request_scope=request_scope,
        tenant_scope=tenant_scope,
        compilation=invocation,
        admission=RunAdmissionSpec(
            actor=actor,
            budget_envelope=budget,
            requested_at=now,
            correlation_id=idempotency_key,
            sponsorship_ref="sponsorship:schema-grounding-live",
            approval_refs=("approval:user-schema-grounding-live",),
            delegation_authority_refs=actor.authority_refs,
            admission_evidence_refs=admission_evidence,
        ),
        initial_goal=initial_goal,
        policy_snapshot_digest=policy,
        environment_snapshot_digest=environment,
        idempotency_issuer=actor.actor_id,
        idempotency_key=idempotency_key,
    )
    context = LaunchRequestContext(
        caller_id=actor.actor_id,
        tenant_scope=tenant_scope,
        request_scope=request_scope,
        approval_refs=("approval:user-schema-grounding-live",),
        policy_snapshot_digest=policy,
        environment_snapshot_digest=environment,
        observed_at=now,
    )
    return proposal, context


async def _operation_binding_refs(
    repository: SemanticOperationBindingRepository,
    request_scope: str,
    run_id: str,
    operation_ids: tuple[str, ...],
) -> tuple[str, ...]:
    refs = []
    for operation_id in operation_ids:
        binding_id = f"{run_id}:operation:{operation_id}:attempt:1"
        binding = await repository.get_binding_by_id(
            binding_id,
            request_scope=request_scope,
        )
        if binding is None:
            raise RuntimeError(f"durable OEB is unavailable after launch: {binding_id}")
        refs.append(binding.binding_id)
    return tuple(refs)


async def _stage_result_record(
    result: Any,
    *,
    handle: Any,
    tenant_scope: str,
    request_scope: str,
    operation_repository: SemanticOperationBindingRepository,
) -> WorkflowResultRecord:
    evidence = tuple(
        ref
        for stage_refs in result.output_refs.values()
        for ref in stage_refs
    )
    final_refs = result.output_refs.get("accept_selection", ())
    if len(final_refs) != 1:
        raise RuntimeError("Scenario A did not produce one accepted selection")
    return WorkflowResultRecord(
        run_id=handle.run_id,
        tenant_scope=tenant_scope,
        request_scope=request_scope,
        blueprint_family=BlueprintFamily.STAGE_GRAPH,
        terminal_outcome=RunOutcome.COMPLETED,
        output_contract_results={
            "schema:accepted-schema-context-selection:v1": {
                "record_ref": final_refs[0],
            }
        },
        evidence_refs=evidence,
        operation_binding_refs=await _operation_binding_refs(
            operation_repository,
            request_scope,
            handle.run_id,
            ("semantic_selector", "independent_reviewer"),
        ),
        family_result=StageGraphResultDetails(
            execution_epoch=result.execution_epoch,
            workflow_cycles=result.workflow_cycles,
            stage_cycles=result.stage_cycles,
            operation_attempts=result.operation_attempts,
            output_refs=result.output_refs,
            reused_output_refs=result.reused_output_refs,
            schedule_trace=result.schedule_trace,
        ),
        completed_at=datetime.now(UTC),
    )


async def _goal_result_record(
    result: Any,
    *,
    handle: Any,
    tenant_scope: str,
    request_scope: str,
    operation_repository: SemanticOperationBindingRepository,
) -> WorkflowResultRecord:
    if result.final_action != "verified_completion" or not result.output_refs:
        raise RuntimeError("Scenario C did not reach independently verified completion")
    return WorkflowResultRecord(
        run_id=handle.run_id,
        tenant_scope=tenant_scope,
        request_scope=request_scope,
        blueprint_family=BlueprintFamily.GOAL_DIRECTED,
        terminal_outcome=RunOutcome.COMPLETED,
        output_contract_results={
            "schema:supporting-graph-reconciliation-record:v1": {
                "record_refs": list(result.output_refs),
            }
        },
        evidence_refs=result.output_refs,
        operation_binding_refs=await _operation_binding_refs(
            operation_repository,
            request_scope,
            handle.run_id,
            ("goal_iteration",),
        ),
        family_result=GoalDirectedResultDetails(
            execution_epoch=result.execution_epoch,
            stop_reason=result.stop_reason,
            final_verifier_action=result.final_action,
            goal_iterations=result.goal_iterations,
            agent_runs=result.agent_runs,
            rollover_count=result.rollover_count,
            active_revision_id=result.active_revision_id,
            accepted_revision_ids=result.accepted_revision_ids,
            handoff_checkpoints=tuple(asdict(item) for item in result.handoff_checkpoints),
            execution_results=tuple(asdict(item) for item in result.execution_results),
            verification_results=tuple(asdict(item) for item in result.verification_results),
        ),
        completed_at=datetime.now(UTC),
    )


def _audit_refs(
    records: tuple[Any, ...],
    transitions: tuple[Any, ...],
) -> dict[str, list[dict[str, Any]]]:
    return {
        "mongo_schema_grounding": [
            {
                "record_type": record.record_type,
                "record_id": record.record_id,
                "content_digest": record.content_digest,
            }
            for record in records
        ],
        "postgres_run_control_transitions": [
            {
                "resulting_version": transition.resulting_version,
                "resulting_phase": transition.resulting_phase.value,
                "transition_digest": sha256_digest(transition),
            }
            for transition in transitions
        ],
    }
