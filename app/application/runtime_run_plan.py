from __future__ import annotations

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    DefinitionKind,
    EffectiveRunConfiguration,
    ExactDefinitionRef,
    StageGraphBlueprint,
)
from app.domain.graph_runtime.contracts import RuntimeCapabilityReadiness
from app.domain.graph_runtime.definitions import (
    CapabilityManifestDefinition,
    ContentAddressedRef,
    GraphAssemblySpec,
    GraphAssemblySpecV2,
    OperationAssemblySpec,
    RunPlan,
    RunPlanV3,
    StageCapabilityRequirement,
    StageExecutionBinding,
    UnavailableStageSurface,
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
    effective_configuration: EffectiveRunConfiguration | None = None,
    graph_assembly_ref: ContentAddressedRef,
    state_schema_digest: str,
    reducer_registry_digest: str,
    operation_registry_digest: str,
    requirements: tuple[StageCapabilityRequirement, ...],
    bindings: tuple[StageExecutionBinding, ...],
    assemblies: dict[str, OperationAssemblySpec],
    compatibility_manifest_digest: str,
    allowed_capability_ids: frozenset[str] | None = None,
    disabled_capability_ids: frozenset[str] = frozenset(),
    capability_manifest_ref: ContentAddressedRef | None = None,
    capability_manifest: CapabilityManifestDefinition | None = None,
    capability_readiness: tuple[RuntimeCapabilityReadiness, ...] = (),
) -> tuple[GraphAssemblySpecV2, tuple[UnavailableStageSurface, ...]]:
    """Freeze exact stage coverage and predict unavailable required surfaces without I/O."""

    if (capability_manifest_ref is None) != (capability_manifest is None):
        raise ValueError("capability manifest ref and content must be supplied together")
    if capability_manifest is not None and capability_manifest_ref is not None:
        if effective_configuration is None:
            raise ValueError(
                "capability manifest structural compilation requires an effective configuration"
            )
        if (
            capability_manifest_ref.kind.value != "capability_manifest"
            or capability_manifest_ref.logical_id != capability_manifest.logical_id
            or capability_manifest_ref.schema_version
            != capability_manifest.schema_version
            or capability_manifest_ref.digest != capability_manifest.digest
        ):
            raise ValueError("capability manifest reference is not exact")
        allowed_capability_ids, readiness_by_id = _effective_capability_ids(
            effective_configuration=effective_configuration,
            manifest=capability_manifest,
            readiness=capability_readiness,
            disabled_capability_ids=disabled_capability_ids,
        )
    else:
        if allowed_capability_ids is None:
            raise ValueError(
                "legacy structural compilation requires explicit allowed capability IDs"
            )
        readiness_by_id = {}

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

    unavailable: list[UnavailableStageSurface] = []
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
        if binding.resource_envelope_ref != assembly.resource_envelope_ref:
            raise ValueError("stage binding and operation assembly resource envelopes differ")
        if assembly.compatibility_manifest_ref.digest != compatibility_manifest_digest:
            raise ValueError("operation assembly compatibility manifest differs from graph")
        if (
            capability_manifest_ref is not None
            and assembly.capability_manifest_ref != capability_manifest_ref
        ):
            raise ValueError("operation assembly capability manifest differs from graph")
        _validate_delegation(requirement, assembly)
        for capability_id in sorted(requirement.required_capability_ids):
            prediction = _unavailable_surface(
                stage_id=key[0],
                variant_name=key[1],
                capability_id=capability_id,
                allowed_capability_ids=allowed_capability_ids,
                disabled_capability_ids=disabled_capability_ids,
                readiness=readiness_by_id.get(capability_id),
            )
            if prediction is not None:
                unavailable.append(prediction)

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


def _effective_capability_ids(
    *,
    effective_configuration: EffectiveRunConfiguration,
    manifest: CapabilityManifestDefinition,
    readiness: tuple[RuntimeCapabilityReadiness, ...],
    disabled_capability_ids: frozenset[str],
) -> tuple[frozenset[str], dict[str, RuntimeCapabilityReadiness]]:
    records = {record.capability_id: record for record in manifest.capabilities}
    readiness_by_id = {item.capability_id: item for item in readiness}
    if len(readiness_by_id) != len(readiness):
        raise ValueError("capability readiness identities must be unique")
    if unknown := readiness_by_id.keys() - records.keys():
        raise ValueError(f"capability readiness contains unknown IDs: {sorted(unknown)}")
    for capability_id, readiness_fact in readiness_by_id.items():
        record = records[capability_id]
        if (
            readiness_fact.maturity != record.maturity
            or readiness_fact.enabled != record.enabled
        ):
            raise ValueError("capability readiness differs from the pinned maturity manifest")
    allowed: set[str] = set()
    for capability_id, record in records.items():
        maybe_readiness = readiness_by_id.get(capability_id)
        if (
            capability_id in effective_configuration.effective_authority.capabilities
            and capability_id not in disabled_capability_ids
            and record.maturity != "policy_disabled"
            and record.enabled
            and maybe_readiness is not None
            and maybe_readiness.ready
            and maybe_readiness.enabled
        ):
            allowed.add(capability_id)
    return frozenset(allowed), readiness_by_id


def _unavailable_surface(
    *,
    stage_id: str,
    variant_name: str,
    capability_id: str,
    allowed_capability_ids: frozenset[str],
    disabled_capability_ids: frozenset[str],
    readiness: RuntimeCapabilityReadiness | None,
) -> UnavailableStageSurface | None:
    if capability_id in allowed_capability_ids and capability_id not in disabled_capability_ids:
        return None
    if capability_id in disabled_capability_ids:
        maturity = readiness.maturity if readiness is not None else "policy_disabled"
        reason_code = "feature_disabled"
        fallback = readiness.fallback if readiness is not None else "reject"
        detail = "capability is explicitly disabled"
    elif readiness is None:
        maturity = "policy_disabled"
        reason_code = "capability_unavailable"
        fallback = "reject"
        detail = "no pinned capability maturity/readiness record exists"
    else:
        maturity = readiness.maturity
        fallback = readiness.fallback
        if maturity == "policy_disabled":
            reason_code = "feature_disabled"
            detail = "capability is policy disabled"
        elif capability_id in disabled_capability_ids or not readiness.enabled:
            reason_code = "feature_disabled"
            detail = "capability feature flag is disabled"
        elif not readiness.ready and maturity in {
            "beta",
            "preview",
            "entitlement_dependent",
        }:
            reason_code = "maturity_not_promoted"
            detail = readiness.reason
        elif not readiness.ready:
            reason_code = "readiness_unavailable"
            detail = readiness.reason
        else:
            reason_code = "authority_denied"
            detail = "capability is outside the frozen effective authority"
    return UnavailableStageSurface(
        stage_id=stage_id,
        variant_name=variant_name,
        capability_id=capability_id,
        reason_code=reason_code,
        maturity=maturity,
        fallback=fallback,
        detail=detail,
    )


def _validate_delegation(
    requirement: StageCapabilityRequirement,
    assembly: OperationAssemblySpec,
) -> None:
    allowed = requirement.delegation_modes_allowed
    if assembly.synchronous_subagent_refs and "sync" not in allowed:
        raise ValueError("operation assembly enables synchronous delegation without authority")
    if (
        assembly.async_subagent_target_refs
        or assembly.implementation_kind == "async_child"
    ) and "async" not in allowed:
        raise ValueError("operation assembly enables async delegation without authority")
    if assembly.implementation_kind == "linked_run" and "linked_run" not in allowed:
        raise ValueError("operation assembly enables linked runs without authority")


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
