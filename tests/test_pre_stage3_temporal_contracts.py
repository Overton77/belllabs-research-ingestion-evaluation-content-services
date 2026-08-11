from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from app.api.graph_runtime_schemas import graph_runtime_contract_schemas
from app.application.runtime_resources import (
    InMemoryResourceLeaseJournal,
    ResourceCapacity,
    ResourceExhausted,
)
from app.application.runtime_run_plan import compile_structural_graph_assembly_v3
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    ExactDefinitionRef,
)
from app.domain.control_plane.stagegraph_builder import (
    StageGraphStageSpec,
    build_stagegraph_v2,
)
from app.domain.graph_runtime.contracts import RuntimeCapabilityReadiness
from app.domain.graph_runtime.definitions import (
    CapabilityManifestDefinition,
    CapabilityMaturityRecord,
    CompatibilityManifestRef,
    ContentAddressedRef,
    ExecutionLineageEnvelopeV2,
    ExecutionResourceEnvelopeRef,
    ExecutionResourceEnvelopeV2,
    GraphAssemblySpecV3,
    OperationAssemblyRef,
    OperationAssemblySpec,
    OperationAssemblySpecV3,
    RunPlanV4,
    RuntimeDefinitionKind,
    StageCapabilityRequirement,
    StageCapabilityRequirementRef,
    StageExecutionBindingV2,
    TemporalExecutionProfileRef,
)
from app.domain.graph_runtime.kernel import (
    OperationFailureClass,
    OperationFailureClassV2,
    ResourceKindV2,
    ResourceLeaseRequestV2,
)

DIGEST = "sha256:" + "a" * 64


def ref(
    kind: RuntimeDefinitionKind,
    logical_id: str,
    digest: str = DIGEST,
    *,
    schema_version: str = "1",
) -> ContentAddressedRef:
    return ContentAddressedRef(
        kind=kind,
        logical_id=logical_id,
        schema_version=schema_version,
        digest=digest,
    )


def requirement() -> StageCapabilityRequirement:
    return StageCapabilityRequirement(
        stage_id="collect",
        operation_contract_ref="operation:collect@1",
        required_capability_ids=frozenset({"literature_search"}),
        input_contract_ref="contract:input@1",
        output_contract_ref="contract:output@1",
        context_purpose="research",
        effect_class="read_only",
        resource_class_ref="resource:default@1",
        verification_contract_ref="verification:collect@1",
        degradation_contract_ref="degradation:collect@1",
        speculation_policy_ref="policy:speculation:disabled",
    )


def v3_fixture() -> tuple[
    StageCapabilityRequirement,
    OperationAssemblySpecV3,
    StageExecutionBindingV2,
    CapabilityManifestDefinition,
    ContentAddressedRef,
]:
    stage_requirement = requirement()
    manifest = CapabilityManifestDefinition(
        logical_id="capability.manifest.temporal",
        title="Temporal capability manifest",
        description="Exact capability posture for the pre-Stage 3 contract.",
        capabilities=(
            CapabilityMaturityRecord(
                capability_id="literature_search",
                maturity="stable",
                required_for_migration=True,
                feature_flag="LITERATURE_SEARCH_ENABLED",
                enabled=True,
                fallback="reject",
            ),
        ),
    )
    manifest_ref = ref(
        RuntimeDefinitionKind.CAPABILITY_MANIFEST,
        manifest.logical_id,
        manifest.digest,
        schema_version=manifest.schema_version,
    )
    temporal_profile_ref = TemporalExecutionProfileRef(
        logical_id="temporal.operation.default",
        schema_version="belllabs.temporal-execution-profile.v1",
        digest=DIGEST,
    )
    resource_ref = ExecutionResourceEnvelopeRef(
        logical_id="resource.default",
        digest=DIGEST,
    )
    implementation_ref = ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "operation.collect")
    assembly = OperationAssemblySpecV3.create(
        operation_assembly_id="assembly.collect.temporal.v3",
        operation_contract_ref=stage_requirement.operation_contract_ref,
        implementation_kind="native",
        adapter_variant="native_exact",
        implementation_ref=implementation_ref,
        model_policy_ref=manifest_ref,
        prompt_manifest_ref=manifest_ref,
        middleware_manifest_ref=manifest_ref,
        tool_manifest_ref=manifest_ref,
        mcp_manifest_ref=manifest_ref,
        skill_manifest_ref=manifest_ref,
        context_assembly_ref=manifest_ref,
        delegation_policy_ref=manifest_ref,
        workspace_policy_ref=manifest_ref,
        sandbox_profile_ref=manifest_ref,
        verifier_ref=manifest_ref,
        resource_envelope_ref=resource_ref,
        effect_policy_ref=manifest_ref,
        fallback_policy_ref=manifest_ref,
        trace_redaction_policy_ref=manifest_ref,
        capability_manifest_ref=manifest_ref,
        temporal_execution_profile_ref=temporal_profile_ref,
        compatibility_manifest_ref=CompatibilityManifestRef(
            logical_id="compatibility.stagegraph",
            schema_version="belllabs.compatibility-manifest.v1",
            digest=DIGEST,
        ),
    )
    binding = StageExecutionBindingV2(
        stage_id="collect",
        stage_requirement_ref=StageCapabilityRequirementRef(
            logical_id="stage-requirement:collect:default",
            digest=sha256_digest(stage_requirement.model_dump(mode="json")),
        ),
        operation_assembly_ref=OperationAssemblyRef(
            logical_id=assembly.operation_assembly_id,
            digest=assembly.operation_assembly_digest,
        ),
        operation_assembly_digest=assembly.operation_assembly_digest,
        input_projection_ref="projection:input@1",
        output_projection_ref="projection:output@1",
        resource_envelope_ref=resource_ref,
        temporal_execution_profile_ref=temporal_profile_ref,
        compatibility_key="stagegraph-temporal-v1",
    )
    return stage_requirement, assembly, binding, manifest, manifest_ref


def test_temporal_contracts_are_versioned_without_mutating_published_v2() -> None:
    assert OperationAssemblySpec.model_fields["schema_version"].default == (
        "belllabs.operation-assembly.v2"
    )
    assert "adapter_variant" not in OperationAssemblySpec.model_fields
    assert "temporal_execution_profile_ref" not in OperationAssemblySpec.model_fields
    assert "temporal_execution_profile" not in {item.value for item in RuntimeDefinitionKind}
    assert "stale_execution_generation" not in {
        item.value for item in OperationFailureClass
    }
    with pytest.raises(ValidationError, match="Extra inputs"):
        OperationAssemblySpec.model_validate(
            {
                "schema_version": "belllabs.operation-assembly.v2",
                "adapter_variant": "native_exact",
            }
        )

    schemas = graph_runtime_contract_schemas()
    assert sha256_digest(schemas["operation_assembly_spec"]) == (
        "sha256:d8dcb93c0c9e47c16a63c8adab397bb493ed2a42354028b90081cf83c8aa754d"
    )
    assert sha256_digest(schemas["run_plan_v3"]) == (
        "sha256:b97ee980bc34118a947b0b56d514b8f8578658fde063cfb7f1e60d26068a0f4d"
    )
    assert sha256_digest(schemas["operation_execution_outcome"]) == (
        "sha256:b53b176f42d9cb4ee6a4068907b41e783b116ac8c5ab2fe6562fa35f51119fd1"
    )
    assert "operation_assembly_spec_v3" in schemas
    assert "stage_execution_binding_v2" in schemas
    assert "execution_resource_envelope_v2" in schemas
    assert "execution_lineage_envelope_v2" in schemas
    assert "graph_assembly_spec_v3" in schemas
    assert "run_plan_v4" in schemas


def test_v3_compiler_freezes_temporal_profile_and_fails_closed_on_drift() -> None:
    stage_requirement, assembly, binding, manifest, manifest_ref = v3_fixture()
    readiness = RuntimeCapabilityReadiness(
        capability_id="literature_search",
        maturity="stable",
        enabled=True,
        ready=True,
        reason="qualified",
        fallback="reject",
    )
    effective_configuration = SimpleNamespace(
        effective_authority=SimpleNamespace(
            capabilities=frozenset({"literature_search"})
        )
    )
    compiled, unavailable = compile_structural_graph_assembly_v3(
        blueprint=build_stagegraph_v2(
            logical_id="blueprint.collect.temporal",
            title="Collect",
            description="One exact Temporal operation stage.",
            stages=(StageGraphStageSpec(stage_id="collect"),),
        ),
        effective_configuration=effective_configuration,
        graph_assembly_ref=ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "stagegraph.v3"),
        state_schema_digest=DIGEST,
        reducer_registry_digest=DIGEST,
        operation_registry_digest=DIGEST,
        requirements=(stage_requirement,),
        bindings=(binding,),
        assemblies={binding.operation_assembly_ref.logical_id: assembly},
        compatibility_manifest_digest=DIGEST,
        capability_manifest_ref=manifest_ref,
        capability_manifest=manifest,
        capability_readiness=(readiness,),
    )
    assert isinstance(compiled, GraphAssemblySpecV3)
    assert unavailable == ()
    assert (
        compiled.stage_execution_bindings[0].temporal_execution_profile_ref
        == assembly.temporal_execution_profile_ref
    )

    drifted_binding = binding.model_copy(
        update={
            "temporal_execution_profile_ref": binding.temporal_execution_profile_ref.model_copy(
                update={"logical_id": "temporal.operation.other"}
            )
        }
    )
    with pytest.raises(ValueError, match="Temporal execution profiles differ"):
        compile_structural_graph_assembly_v3(
            blueprint=build_stagegraph_v2(
                logical_id="blueprint.collect.temporal",
                title="Collect",
                description="One exact Temporal operation stage.",
                stages=(StageGraphStageSpec(stage_id="collect"),),
            ),
            effective_configuration=effective_configuration,
            graph_assembly_ref=ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "stagegraph.v3"),
            state_schema_digest=DIGEST,
            reducer_registry_digest=DIGEST,
            operation_registry_digest=DIGEST,
            requirements=(stage_requirement,),
            bindings=(drifted_binding,),
            assemblies={binding.operation_assembly_ref.logical_id: assembly},
            compatibility_manifest_digest=DIGEST,
            capability_manifest_ref=manifest_ref,
            capability_manifest=manifest,
            capability_readiness=(readiness,),
        )

    with pytest.raises(ValueError, match="wrong definition kind"):
        compile_structural_graph_assembly_v3(
            blueprint=build_stagegraph_v2(
                logical_id="blueprint.collect.temporal",
                title="Collect",
                description="One exact Temporal operation stage.",
                stages=(StageGraphStageSpec(stage_id="collect"),),
            ),
            effective_configuration=effective_configuration,
            graph_assembly_ref=ref(RuntimeDefinitionKind.STATE_SCHEMA, "not-a-graph"),
            state_schema_digest=DIGEST,
            reducer_registry_digest=DIGEST,
            operation_registry_digest=DIGEST,
            requirements=(stage_requirement,),
            bindings=(binding,),
            assemblies={binding.operation_assembly_ref.logical_id: assembly},
            compatibility_manifest_digest=DIGEST,
            capability_manifest_ref=manifest_ref,
            capability_manifest=manifest,
            capability_readiness=(readiness,),
        )

    wrong_requirement = binding.model_copy(
        update={
            "stage_requirement_ref": binding.stage_requirement_ref.model_copy(
                update={"logical_id": "stage-requirement:other:default"}
            )
        }
    )
    with pytest.raises(ValueError, match="stage requirement reference"):
        compile_structural_graph_assembly_v3(
            blueprint=build_stagegraph_v2(
                logical_id="blueprint.collect.temporal",
                title="Collect",
                description="One exact Temporal operation stage.",
                stages=(StageGraphStageSpec(stage_id="collect"),),
            ),
            effective_configuration=effective_configuration,
            graph_assembly_ref=ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "stagegraph.v3"),
            state_schema_digest=DIGEST,
            reducer_registry_digest=DIGEST,
            operation_registry_digest=DIGEST,
            requirements=(stage_requirement,),
            bindings=(wrong_requirement,),
            assemblies={binding.operation_assembly_ref.logical_id: assembly},
            compatibility_manifest_digest=DIGEST,
            capability_manifest_ref=manifest_ref,
            capability_manifest=manifest,
            capability_readiness=(readiness,),
        )

    stale_assembly = assembly.model_copy(update={"adapter_variant": "changed_without_digest"})
    with pytest.raises(ValidationError, match="operation assembly digest mismatch"):
        compile_structural_graph_assembly_v3(
            blueprint=build_stagegraph_v2(
                logical_id="blueprint.collect.temporal",
                title="Collect",
                description="One exact Temporal operation stage.",
                stages=(StageGraphStageSpec(stage_id="collect"),),
            ),
            effective_configuration=effective_configuration,
            graph_assembly_ref=ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "stagegraph.v3"),
            state_schema_digest=DIGEST,
            reducer_registry_digest=DIGEST,
            operation_registry_digest=DIGEST,
            requirements=(stage_requirement,),
            bindings=(binding,),
            assemblies={binding.operation_assembly_ref.logical_id: stale_assembly},
            compatibility_manifest_digest=DIGEST,
            capability_manifest_ref=manifest_ref,
            capability_manifest=manifest,
            capability_readiness=(readiness,),
        )


def test_resource_lineage_failure_and_run_plan_cover_temporal_hierarchy() -> None:
    resources = ExecutionResourceEnvelopeV2(
        tenant_limit_ref="limit:tenant",
        environment_limit_ref="limit:environment",
        workflow_run_slots=1,
        family_scheduler_slots=1,
        stage_slots=2,
        operation_workflow_slots=2,
        operation_worker_slots=1,
        model_call_slots=1,
        tool_call_slots=1,
        mcp_call_slots=0,
        sync_subagent_slots=0,
        async_child_slots=0,
        linked_run_slots=0,
        deadline="deadline:run",
        lease_ttl="PT5M",
        resumption_reserve=1,
        release_policy="release:durable-wait",
    )
    assert resources.operation_workflow_slots == 2

    lineage = ExecutionLineageEnvelopeV2(
        request_scope="tenant-a",
        belllabs_run_id="run-a",
        execution_epoch=1,
        technical_segment=2,
        workflow_implementation_ref="workflow:stagegraph@1",
        graph_assembly_digest=DIGEST,
        workflow_cycle=0,
        stage_id="collect",
        stage_cycle=0,
        semantic_operation_attempt_id="attempt-1",
        execution_generation=1,
        runtime_attempt_id="runtime-1",
        operation_binding_id="binding-1",
        operation_assembly_digest=DIGEST,
        temporal_namespace_ref="namespace:default",
        root_workflow_id="belllabs-run-a",
        root_temporal_run_id="temporal-root-run",
        family_workflow_id="belllabs-run-a:stagegraph",
        family_temporal_run_id="temporal-family-run",
        operation_workflow_id="belllabs-run-a:collect:attempt-1",
        operation_temporal_run_id="temporal-operation-run",
        activity_id="execute-operation",
        activity_attempt=1,
        task_queue_id="agent-cognitive",
        worker_build_id="worker-n",
        agent_invocation_id="agent-invocation-1",
        agent_thread_id="agent-thread-1",
        agent_checkpoint_ref="checkpoint:1",
        result_manifest_ref="result:1",
        effect_claim_ids=("effect:1",),
        effect_settlement_refs=("effect-settlement:1",),
    )
    assert lineage.technical_segment == 2
    assert lineage.operation_workflow_id
    assert OperationFailureClassV2.STALE_EXECUTION_GENERATION.value == (
        "stale_execution_generation"
    )
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    lease = ResourceLeaseRequestV2(
        lease_id="lease-temporal",
        request_scope="tenant-a",
        semantic_identity="run-a:collect",
        envelope_digest=DIGEST,
        resources=(
            ResourceKindV2.TENANT,
            ResourceKindV2.ENVIRONMENT,
            ResourceKindV2.WORKFLOW_RUN,
            ResourceKindV2.FAMILY_SCHEDULER,
            ResourceKindV2.STAGE,
            ResourceKindV2.OPERATION_WORKFLOW,
            ResourceKindV2.OPERATION_WORKER,
            ResourceKindV2.RESUMPTION,
        ),
        requested_at=now,
        deadline=now + timedelta(minutes=5),
        ttl_seconds=300,
    )
    assert ResourceKindV2.FAMILY_SCHEDULER in lease.resources
    assert ResourceKindV2.OPERATION_WORKFLOW in lease.resources

    stage_requirement, assembly, binding, _manifest, _manifest_ref = v3_fixture()
    graph = GraphAssemblySpecV3(
        graph_assembly_ref=ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "stagegraph.v3"),
        state_schema_digest=DIGEST,
        reducer_registry_digest=DIGEST,
        operation_registry_digest=DIGEST,
        stage_requirements=(stage_requirement,),
        stage_execution_bindings=(binding,),
        compatibility_manifest_digest=DIGEST,
    )
    plan = RunPlanV4.create(
        plan_id="plan-temporal-v4",
        effective_run_configuration_digest=DIGEST,
        semantic_binding_ref="semantic:1",
        workflow_implementation_ref=ExactDefinitionRef(
            kind=DefinitionKind.WORKFLOW_IMPLEMENTATION,
            logical_id="workflow.stagegraph",
            revision=1,
            digest=DIGEST,
        ),
        graph_assembly=graph,
        alias_evidence_digest=DIGEST,
    )
    assert plan.graph_assembly.stage_execution_bindings[0].operation_assembly_digest == (
        assembly.operation_assembly_digest
    )
    with pytest.raises(ValidationError, match="RunPlan v4 digest mismatch"):
        RunPlanV4.model_validate(
            {**plan.model_dump(mode="json"), "plan_digest": "sha256:" + "b" * 64}
        )


@pytest.mark.asyncio
async def test_v2_resource_journal_enforces_family_and_operation_workflow_capacity() -> None:
    now = datetime(2026, 8, 8, 12, 0, tzinfo=UTC)
    journal = InMemoryResourceLeaseJournal(
        ResourceCapacity(
            limits={
                ResourceKindV2.FAMILY_SCHEDULER: 1,
                ResourceKindV2.OPERATION_WORKFLOW: 1,
            }
        )
    )
    first = ResourceLeaseRequestV2(
        lease_id="lease-family-a",
        request_scope="tenant-a",
        semantic_identity="run-a:family",
        envelope_digest=DIGEST,
        resources=(
            ResourceKindV2.FAMILY_SCHEDULER,
            ResourceKindV2.OPERATION_WORKFLOW,
        ),
        requested_at=now,
        deadline=now + timedelta(minutes=5),
        ttl_seconds=300,
    )
    acquired = await journal.acquire(first, now=now)
    assert acquired.request == first

    second = first.model_copy(
        update={
            "lease_id": "lease-family-b",
            "semantic_identity": "run-b:family",
        }
    )
    with pytest.raises(ResourceExhausted, match="family_scheduler"):
        await journal.acquire(second, now=now)
