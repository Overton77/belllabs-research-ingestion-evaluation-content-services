from __future__ import annotations

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    EffectiveRunConfiguration,
    ExactDefinitionRef,
    StageGraphBlueprint,
)
from app.domain.graph_runtime.definitions import (
    ContentAddressedRef,
    GraphAssemblySpec,
    GraphAssemblySpecV2,
    OperationAssemblySpec,
    RunPlan,
    RunPlanV3,
    StageCapabilityRequirement,
    StageExecutionBinding,
)


def compile_run_plan(
    *,
    plan_id: str,
    effective_configuration: EffectiveRunConfiguration,
    semantic_binding_ref: str,
    workflow_implementation_ref: ExactDefinitionRef,
    graph_assembly: GraphAssemblySpec,
    harness_ref: ContentAddressedRef,
    delegation_policy_ref: ContentAddressedRef,
    context_assembly_ref: ContentAddressedRef,
    execution_environment_ref: ContentAddressedRef,
    capability_manifest_ref: ContentAddressedRef,
    evaluation_profile_ref: ContentAddressedRef,
) -> RunPlan:
    """Freezes exact runtime mechanics without changing the accepted ERC digest."""

    if workflow_implementation_ref.kind != DefinitionKind.WORKFLOW_IMPLEMENTATION:
        raise ValueError("RunPlan requires an exact Workflow Implementation reference")
    implementation_source = next(
        (
            ref
            for ref in effective_configuration.source_refs
            if ref.kind == DefinitionKind.WORKFLOW_IMPLEMENTATION
        ),
        None,
    )
    if implementation_source is not None and implementation_source != workflow_implementation_ref:
        raise ValueError("RunPlan Workflow Implementation differs from the compiled ERC")
    if graph_assembly.graph_assembly_ref.kind.value != "graph_assembly":
        raise ValueError("RunPlan requires an exact graph assembly submanifest")
    expected = {
        "harness": (harness_ref, "agent_harness"),
        "delegation": (delegation_policy_ref, "delegation_policy"),
        "context": (context_assembly_ref, "context_assembly"),
        "environment": (execution_environment_ref, "execution_environment"),
        "capability": (capability_manifest_ref, "capability_manifest"),
        "evaluation": (evaluation_profile_ref, "evaluation_profile"),
    }
    for name, (ref, kind) in expected.items():
        if ref.kind.value != kind:
            raise ValueError(f"RunPlan {name} reference has the wrong definition kind")
    alias_evidence_digest = sha256_digest(
        [item.model_dump(mode="json") for item in effective_configuration.alias_evidence]
    )
    values = {
        "plan_id": plan_id,
        "effective_run_configuration_digest": effective_configuration.digest,
        "semantic_binding_ref": semantic_binding_ref,
        "workflow_implementation_ref": workflow_implementation_ref,
        "graph_assembly": graph_assembly,
        "harness_ref": harness_ref,
        "delegation_policy_ref": delegation_policy_ref,
        "context_assembly_ref": context_assembly_ref,
        "execution_environment_ref": execution_environment_ref,
        "capability_manifest_ref": capability_manifest_ref,
        "evaluation_profile_ref": evaluation_profile_ref,
        "alias_evidence_digest": alias_evidence_digest,
    }
    return RunPlan.create(**values)


def compile_structural_graph_assembly(
    *,
    blueprint: StageGraphBlueprint,
    graph_assembly_ref: ContentAddressedRef,
    state_schema_digest: str,
    reducer_registry_digest: str,
    operation_registry_digest: str,
    requirements: tuple[StageCapabilityRequirement, ...],
    bindings: tuple[StageExecutionBinding, ...],
    assemblies: dict[str, OperationAssemblySpec],
    allowed_capability_ids: frozenset[str],
    disabled_capability_ids: frozenset[str] = frozenset(),
    compatibility_manifest_digest: str,
) -> tuple[GraphAssemblySpecV2, tuple[str, ...]]:
    """Purely freeze structural stage coverage; never discover runtime capabilities."""

    expected = {
        (stage.stage_id, variant)
        for stage in blueprint.stages
        for variant in (stage.variant_names or frozenset({"default"}))
    }
    requirement_by_key = {(item.stage_id, item.variant_name): item for item in requirements}
    binding_by_key = {(item.stage_id, item.variant_name): item for item in bindings}
    if set(requirement_by_key) != expected:
        raise ValueError(
            "stage requirements do not cover every declared stage variant exactly once"
        )
    if set(binding_by_key) != expected:
        raise ValueError(
            "stage execution bindings do not cover every declared stage variant exactly once"
        )

    unavailable: list[str] = []
    for key in sorted(expected):
        requirement = requirement_by_key[key]
        binding = binding_by_key[key]
        requirement_digest = sha256_digest(requirement.model_dump(mode="json"))
        if binding.stage_requirement_ref.digest != requirement_digest:
            raise ValueError("stage requirement reference digest drift")
        assembly = assemblies.get(binding.operation_assembly_ref.logical_id)
        if assembly is None:
            raise ValueError("stage binding refers to an unknown exact operation assembly")
        if (
            binding.operation_assembly_ref.digest != assembly.operation_assembly_digest
            or binding.operation_assembly_digest != assembly.operation_assembly_digest
        ):
            raise ValueError("stage binding operation assembly digest drift")
        if assembly.operation_contract_ref != requirement.operation_contract_ref:
            raise ValueError("stage binding operation contract is incompatible with requirement")
        missing = (requirement.required_capability_ids - allowed_capability_ids) | (
            requirement.required_capability_ids & disabled_capability_ids
        )
        if missing:
            unavailable.append(f"{key[0]}:{key[1]}:{','.join(sorted(missing))}")

    return (
        GraphAssemblySpecV2(
            graph_assembly_ref=graph_assembly_ref,
            state_schema_digest=state_schema_digest,
            reducer_registry_digest=reducer_registry_digest,
            operation_registry_digest=operation_registry_digest,
            stage_requirements=requirements,
            stage_execution_bindings=bindings,
            compatibility_manifest_digest=compatibility_manifest_digest,
        ),
        tuple(unavailable),
    )


def compile_run_plan_v3(
    *,
    plan_id: str,
    effective_configuration: EffectiveRunConfiguration,
    semantic_binding_ref: str,
    workflow_implementation_ref: ExactDefinitionRef,
    graph_assembly: GraphAssemblySpecV2,
) -> RunPlanV3:
    """Freeze v3 per-stage bindings without reinterpreting the published v2 plan."""

    if workflow_implementation_ref.kind != DefinitionKind.WORKFLOW_IMPLEMENTATION:
        raise ValueError("RunPlan v3 requires an exact Workflow Implementation reference")
    implementation_source = next(
        (
            ref
            for ref in effective_configuration.source_refs
            if ref.kind == DefinitionKind.WORKFLOW_IMPLEMENTATION
        ),
        None,
    )
    if implementation_source is not None and implementation_source != workflow_implementation_ref:
        raise ValueError("RunPlan v3 Workflow Implementation differs from the compiled ERC")
    return RunPlanV3.create(
        plan_id=plan_id,
        effective_run_configuration_digest=effective_configuration.digest,
        semantic_binding_ref=semantic_binding_ref,
        workflow_implementation_ref=workflow_implementation_ref,
        graph_assembly=graph_assembly,
        alias_evidence_digest=sha256_digest(
            [item.model_dump(mode="json") for item in effective_configuration.alias_evidence]
        ),
    )
