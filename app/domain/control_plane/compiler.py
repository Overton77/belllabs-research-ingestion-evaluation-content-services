from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AgentProfileDefinition,
    AuthorityCeiling,
    AvailableCapability,
    BudgetCeiling,
    CapabilityDefinition,
    CapabilityKind,
    CapabilityRequirement,
    CompilationRequest,
    ControlProfileDefinition,
    Definition,
    EffectiveRunConfiguration,
    ExactDefinitionRef,
    FlattenedDeepAgentBinding,
    GoalDirectedBlueprint,
    MCPServerDefinition,
    MCPToolDefinition,
    ModelPolicy,
    OperationAssemblyDefinition,
    OverlayDecision,
    OverlayDecisionStatus,
    ProfileComponent,
    ResolvedCapabilityAttachment,
    ResolvedDefinitions,
    SkillDefinition,
    StageGraphBlueprint,
    WorkflowImplementationBindingDefinition,
)
from app.domain.control_plane.errors import CompilationRejected

COMPILER_VERSION = "control-plane-definitions/1"


def _ref_order(ref: ExactDefinitionRef) -> tuple[str, int, str]:
    return (ref.logical_id, ref.revision, ref.digest)


@dataclass(frozen=True)
class _FlattenedProfile:
    components: tuple[ProfileComponent, ...]
    prompt_refs: tuple[ExactDefinitionRef, ...]
    skill_refs: tuple[ExactDefinitionRef, ...]
    mcp_server_refs: tuple[ExactDefinitionRef, ...]
    tool_refs: tuple[ExactDefinitionRef, ...]
    model_policy: ModelPolicy
    guardrail_refs: tuple[str, ...]
    output_schema_ref: str | None
    maximum_capability_request: AuthorityCeiling
    model_ref: ExactDefinitionRef | None
    middleware_refs: tuple[ExactDefinitionRef, ...]
    sandbox_profile_ref: ExactDefinitionRef | None
    requirements: tuple[CapabilityRequirement, ...]


@dataclass(frozen=True)
class _CapabilityFact:
    ref: ExactDefinitionRef
    capability_kinds: frozenset[CapabilityKind]
    maturity: str
    attachment_targets: frozenset[str]
    compatible_compiler_versions: frozenset[str]
    conflicts_with: frozenset[ExactDefinitionRef]


def _merge_exact_refs(
    current: dict[tuple[str, str], ExactDefinitionRef],
    refs: Iterable[ExactDefinitionRef],
    label: str,
) -> None:
    for ref in refs:
        key = (ref.kind.value, ref.logical_id)
        prior = current.get(key)
        if prior is not None and prior != ref:
            raise CompilationRejected(f"Deep Agent {label} revision collision: {ref.logical_id}")
        current[key] = ref


def _profile_authority_intersection(
    left: AuthorityCeiling, right: AuthorityCeiling
) -> AuthorityCeiling:
    return AuthorityCeiling(
        capabilities=left.capabilities & right.capabilities,
        budgets=_minimum_budget((left.budgets, right.budgets)),
        max_concurrency=min(left.max_concurrency, right.max_concurrency),
    )


def _flatten_agent_profile(
    profile_ref: ExactDefinitionRef,
    profiles: dict[ExactDefinitionRef, AgentProfileDefinition],
    *,
    stack: tuple[ExactDefinitionRef, ...] = (),
) -> _FlattenedProfile:
    if profile_ref in stack:
        raise CompilationRejected("Deep Agent profile composition contains a cycle")
    try:
        profile = profiles[profile_ref]
    except KeyError as exc:
        raise CompilationRejected(
            f"Deep Agent profile revision was not explicitly resolved: {profile_ref.logical_id}"
        ) from exc
    components: dict[str, ExactDefinitionRef] = {}
    prompt_refs: dict[tuple[str, str], ExactDefinitionRef] = {}
    skill_refs: dict[tuple[str, str], ExactDefinitionRef] = {}
    mcp_server_refs: dict[tuple[str, str], ExactDefinitionRef] = {}
    tool_refs: dict[tuple[str, str], ExactDefinitionRef] = {}
    model_policy: ModelPolicy | None = None
    guardrail_refs: set[str] = set()
    output_schema_ref: str | None = None
    maximum_authority: AuthorityCeiling | None = None
    model_ref: ExactDefinitionRef | None = None
    sandbox_ref: ExactDefinitionRef | None = None
    middleware: set[ExactDefinitionRef] = set()
    requirements: dict[str, CapabilityRequirement] = {}
    for parent_ref in sorted(profile.parent_profile_refs, key=_ref_order):
        parent = _flatten_agent_profile(parent_ref, profiles, stack=stack + (profile_ref,))
        for component in parent.components:
            prior_component_ref = components.get(component.slot)
            if prior_component_ref is not None and prior_component_ref != component.ref:
                raise CompilationRejected(
                    f"Deep Agent profile component collision at slot {component.slot!r}"
                )
            components[component.slot] = component.ref
        _merge_exact_refs(prompt_refs, parent.prompt_refs, "prompt")
        _merge_exact_refs(skill_refs, parent.skill_refs, "Skill")
        _merge_exact_refs(mcp_server_refs, parent.mcp_server_refs, "MCP server")
        _merge_exact_refs(tool_refs, parent.tool_refs, "tool")
        if model_policy is not None and model_policy != parent.model_policy:
            raise CompilationRejected("Deep Agent model policy composition is incompatible")
        model_policy = parent.model_policy
        guardrail_refs.update(parent.guardrail_refs)
        if output_schema_ref is not None and output_schema_ref != parent.output_schema_ref:
            raise CompilationRejected("Deep Agent output schema composition is incompatible")
        output_schema_ref = parent.output_schema_ref or output_schema_ref
        maximum_authority = (
            parent.maximum_capability_request
            if maximum_authority is None
            else _profile_authority_intersection(
                maximum_authority, parent.maximum_capability_request
            )
        )
        if parent.model_ref is not None:
            if model_ref is not None and model_ref != parent.model_ref:
                raise CompilationRejected("Deep Agent profile model composition is incompatible")
            model_ref = parent.model_ref
        if parent.sandbox_profile_ref is not None:
            if sandbox_ref is not None and sandbox_ref != parent.sandbox_profile_ref:
                raise CompilationRejected("Deep Agent profile sandbox composition is incompatible")
            sandbox_ref = parent.sandbox_profile_ref
        middleware.update(parent.middleware_refs)
        for requirement in parent.requirements:
            requirement_id = requirement.requirement_id
            prior_requirement = requirements.get(requirement_id)
            if prior_requirement is not None and prior_requirement != requirement:
                raise CompilationRejected(
                    f"Deep Agent capability composition collision: {requirement_id}"
                )
            requirements[requirement_id] = requirement
    for component in profile.components:
        prior_component_ref = components.get(component.slot)
        if prior_component_ref is not None and prior_component_ref != component.ref:
            raise CompilationRejected(
                f"Deep Agent profile component collision at slot {component.slot!r}"
            )
        components[component.slot] = component.ref
    _merge_exact_refs(prompt_refs, profile.prompt_refs, "prompt")
    _merge_exact_refs(skill_refs, profile.skill_refs, "Skill")
    _merge_exact_refs(mcp_server_refs, profile.mcp_server_refs, "MCP server")
    _merge_exact_refs(tool_refs, profile.tool_refs, "tool")
    if model_policy is not None and model_policy != profile.model_policy:
        raise CompilationRejected("Deep Agent model policy composition is incompatible")
    model_policy = profile.model_policy
    guardrail_refs.update(profile.guardrail_refs)
    if output_schema_ref is not None and output_schema_ref != profile.output_schema_ref:
        raise CompilationRejected("Deep Agent output schema composition is incompatible")
    output_schema_ref = profile.output_schema_ref or output_schema_ref
    maximum_authority = (
        profile.maximum_capability_request
        if maximum_authority is None
        else _profile_authority_intersection(maximum_authority, profile.maximum_capability_request)
    )
    if profile.model_ref is not None:
        if model_ref is not None and model_ref != profile.model_ref:
            raise CompilationRejected("Deep Agent profile model composition is incompatible")
        model_ref = profile.model_ref
    if profile.sandbox_profile_ref is not None:
        if sandbox_ref is not None and sandbox_ref != profile.sandbox_profile_ref:
            raise CompilationRejected("Deep Agent profile sandbox composition is incompatible")
        sandbox_ref = profile.sandbox_profile_ref
    middleware.update(profile.middleware_refs)
    for requirement in profile.capability_requirements:
        prior_requirement = requirements.get(requirement.requirement_id)
        if prior_requirement is not None and prior_requirement != requirement:
            raise CompilationRejected(
                f"Deep Agent capability composition collision: {requirement.requirement_id}"
            )
        requirements[requirement.requirement_id] = requirement
    flattened = tuple(
        ProfileComponent(slot=slot, ref=components[slot]) for slot in sorted(components)
    )
    assert model_policy is not None and maximum_authority is not None
    return _FlattenedProfile(
        components=flattened,
        prompt_refs=tuple(sorted(prompt_refs.values(), key=_ref_order)),
        skill_refs=tuple(sorted(skill_refs.values(), key=_ref_order)),
        mcp_server_refs=tuple(sorted(mcp_server_refs.values(), key=_ref_order)),
        tool_refs=tuple(sorted(tool_refs.values(), key=_ref_order)),
        model_policy=model_policy,
        guardrail_refs=tuple(sorted(guardrail_refs)),
        output_schema_ref=output_schema_ref,
        maximum_capability_request=maximum_authority,
        model_ref=model_ref,
        middleware_refs=tuple(sorted(middleware, key=_ref_order)),
        sandbox_profile_ref=sandbox_ref,
        requirements=tuple(requirements[key] for key in sorted(requirements)),
    )


def _published_capability_facts(
    definitions: ResolvedDefinitions,
) -> dict[ExactDefinitionRef, _CapabilityFact]:
    facts: dict[ExactDefinitionRef, _CapabilityFact] = {}
    supported = (CapabilityDefinition, SkillDefinition, MCPServerDefinition, MCPToolDefinition)
    for record in definitions.published_records:
        definition = record.definition
        if isinstance(definition, supported):
            if isinstance(definition, CapabilityDefinition):
                capability_kinds = frozenset({definition.capability_kind})
            elif isinstance(definition, SkillDefinition):
                capability_kinds = frozenset({CapabilityKind.SKILL})
            elif isinstance(definition, MCPServerDefinition):
                capability_kinds = frozenset({CapabilityKind.MCP})
            else:
                capability_kinds = frozenset({CapabilityKind.MCP, CapabilityKind.TOOL})
            facts[record.ref] = _CapabilityFact(
                ref=record.ref,
                capability_kinds=capability_kinds,
                maturity=definition.maturity,
                attachment_targets=definition.attachment_targets,
                compatible_compiler_versions=definition.compatible_compiler_versions,
                conflicts_with=definition.conflicts_with,
            )
    return facts


def _authority_is_declared(requested: AuthorityCeiling, effective: AuthorityCeiling) -> bool:
    return (
        requested.capabilities <= effective.capabilities
        and requested.max_concurrency <= effective.max_concurrency
        and all(
            amount <= effective.budgets.dimensions.get(dimension, -1)
            for dimension, amount in requested.budgets.dimensions.items()
        )
    )


def _compile_agent_bindings(
    assemblies: tuple[OperationAssemblyDefinition, ...],
    profiles: dict[ExactDefinitionRef, AgentProfileDefinition],
    available: tuple[AvailableCapability, ...],
    definitions: ResolvedDefinitions,
    authority_ceiling: AuthorityCeiling,
    effective_authority: AuthorityCeiling,
) -> tuple[
    tuple[FlattenedDeepAgentBinding, ...],
    tuple[ResolvedCapabilityAttachment, ...],
]:
    available_refs = {item.ref for item in available}
    if len(available_refs) != len(available):
        raise CompilationRejected("environment contains duplicate exact capability revisions")
    facts = _published_capability_facts(definitions)
    selected: list[ExactDefinitionRef] = []
    bindings: list[FlattenedDeepAgentBinding] = []
    attachments: list[ResolvedCapabilityAttachment] = []
    for assembly in sorted(assemblies, key=lambda item: item.assembly_id):
        flattened = _flatten_agent_profile(assembly.deep_agent_profile_ref, profiles)
        if not _authority_is_declared(flattened.maximum_capability_request, authority_ceiling):
            raise CompilationRejected(
                "Deep Agent profile maximum capability request exceeds effective authority"
            )
        bindings.append(
            FlattenedDeepAgentBinding(
                assembly_id=assembly.assembly_id,
                profile_ref=assembly.deep_agent_profile_ref,
                placement_ref=assembly.placement_ref,
                flattened_components=flattened.components,
                prompt_refs=flattened.prompt_refs,
                skill_refs=flattened.skill_refs,
                mcp_server_refs=flattened.mcp_server_refs,
                tool_refs=flattened.tool_refs,
                model_policy=flattened.model_policy,
                guardrail_refs=flattened.guardrail_refs,
                output_schema_ref=flattened.output_schema_ref,
                maximum_capability_request=_profile_authority_intersection(
                    flattened.maximum_capability_request, effective_authority
                ),
                model_ref=flattened.model_ref,
                middleware_refs=flattened.middleware_refs,
                sandbox_profile_ref=flattened.sandbox_profile_ref,
            )
        )
        requirements = {item.requirement_id: item for item in flattened.requirements}
        for item in assembly.capability_requirements:
            prior = requirements.get(item.requirement_id)
            if prior is not None and prior != item:
                raise CompilationRejected(f"operation capability collision: {item.requirement_id}")
            requirements[item.requirement_id] = item
        for requirement_id in sorted(requirements):
            requirement = requirements[requirement_id]
            candidates = [
                facts[ref]
                for ref in sorted(requirement.allowed_refs, key=_ref_order)
                if ref in available_refs
                and ref in facts
                and requirement.capability_kind in facts[ref].capability_kinds
                and facts[ref].maturity in {"qualified", "accepted"}
                and COMPILER_VERSION in facts[ref].compatible_compiler_versions
                and requirement.attachment_target in facts[ref].attachment_targets
            ]
            status = "accepted"
            reason = "exact authored capability revision selected"
            selected_ref: ExactDefinitionRef | None = candidates[0].ref if candidates else None
            if (
                selected_ref is None
                and requirement.when_unavailable == "degrade"
                and requirement.degraded_ref is not None
            ):
                degraded = facts.get(requirement.degraded_ref)
                if (
                    degraded is not None
                    and degraded.ref in available_refs
                    and requirement.capability_kind in degraded.capability_kinds
                    and degraded.maturity in {"qualified", "accepted"}
                    and COMPILER_VERSION in degraded.compatible_compiler_versions
                    and (requirement.attachment_target in degraded.attachment_targets)
                ):
                    selected_ref = degraded.ref
                    status = "degraded"
                    reason = "authored exact degradation selected"
            if selected_ref is None and requirement.when_unavailable == "omit":
                attachments.append(
                    ResolvedCapabilityAttachment(
                        requirement_id=requirement.requirement_id,
                        capability_kind=requirement.capability_kind,
                        attachment_target=requirement.attachment_target,
                        status="omitted",
                        reason="authored optional omission policy applied",
                    )
                )
                continue
            if selected_ref is None:
                raise CompilationRejected(
                    f"exact capability unavailable or incompatible: {requirement.requirement_id}"
                )
            candidate = facts[selected_ref]
            if any(
                prior in candidate.conflicts_with or selected_ref in facts[prior].conflicts_with
                for prior in selected
            ):
                raise CompilationRejected(
                    f"capability conflict for requirement: {requirement.requirement_id}"
                )
            selected.append(selected_ref)
            attachments.append(
                ResolvedCapabilityAttachment(
                    requirement_id=requirement.requirement_id,
                    capability_kind=requirement.capability_kind,
                    selected_ref=selected_ref,
                    attachment_target=requirement.attachment_target,
                    status=status,
                    reason=reason,
                )
            )
    return tuple(bindings), tuple(attachments)


def _reject(field: str, requested: object, reason: str) -> None:
    decision = OverlayDecision(
        field=field,
        status=OverlayDecisionStatus.REJECTED,
        requested=requested,
        reason=reason,
    )
    raise CompilationRejected(reason, decisions=(decision,))


def _minimum_budget(ceilings: Iterable[BudgetCeiling]) -> BudgetCeiling:
    values = [ceiling.dimensions for ceiling in ceilings]
    keys = {key for value in values for key in value}
    return BudgetCeiling(
        dimensions={
            key: min(value[key] for value in values if key in value) for key in sorted(keys)
        }
    )


def _intersect_authority(
    workflow: AuthorityCeiling,
    control: AuthorityCeiling,
    caller: AuthorityCeiling,
    parent: AuthorityCeiling | None,
    available: frozenset[str],
) -> AuthorityCeiling:
    ceilings = [workflow, control, caller]
    if parent is not None:
        ceilings.append(parent)
    capabilities = set(available)
    for ceiling in ceilings:
        capabilities.intersection_update(ceiling.capabilities)
    return AuthorityCeiling(
        capabilities=frozenset(capabilities),
        budgets=_minimum_budget(ceiling.budgets for ceiling in ceilings),
        max_concurrency=min(ceiling.max_concurrency for ceiling in ceilings),
    )


def _declared_variants(
    blueprint: StageGraphBlueprint | GoalDirectedBlueprint,
) -> frozenset[str]:
    if isinstance(blueprint, StageGraphBlueprint):
        return frozenset(variant for stage in blueprint.stages for variant in stage.variant_names)
    return blueprint.variant_names


def _validate_exact_ref(ref: ExactDefinitionRef, definition: Definition) -> None:
    if definition.kind != ref.kind or definition.logical_id != ref.logical_id:
        raise CompilationRejected(
            f"exact reference identity does not match resolved definition: {ref.logical_id}"
        )
    actual_digest = sha256_digest(definition)
    if actual_digest != ref.digest:
        raise CompilationRejected(
            f"exact reference digest does not match resolved definition: {ref.logical_id}"
        )


def _validate_implementation_binding(
    request: CompilationRequest,
    definitions: ResolvedDefinitions,
    binding: WorkflowImplementationBindingDefinition,
) -> None:
    exact_bindings = (
        (binding.workflow_type_ref, request.workflow_type_ref, "Workflow Type"),
        (binding.blueprint_ref, request.blueprint_ref, "blueprint"),
        (binding.control_profile_ref, request.control_profile_ref, "control profile"),
        (binding.runtime_profile_ref, request.runtime_profile_ref, "runtime profile"),
        (
            binding.workspace_template_ref,
            request.workspace_template_ref,
            "workspace template",
        ),
        (
            binding.evaluation_profile_ref,
            request.evaluation_profile_ref,
            "evaluation profile",
        ),
        (
            binding.workflow_configuration_ref,
            request.workflow_configuration_ref,
            "workflow configuration",
        ),
    )
    for bound, selected, label in exact_bindings:
        if bound != selected:
            raise CompilationRejected(f"Workflow Implementation selects a different {label}")
    if definitions.control_profile.blueprint_ref != request.blueprint_ref:
        raise CompilationRejected(
            "Workflow Implementation control profile selects a different blueprint"
        )
    configuration = definitions.workflow_configuration
    if (
        configuration is not None
        and configuration.workflow_type_logical_id != definitions.workflow_type.logical_id
    ):
        raise CompilationRejected(
            "Workflow Implementation configuration targets a different Workflow Type"
        )

    realized_obligations = {item.obligation_ref for item in binding.obligation_realizations}
    missing_obligations = definitions.workflow_type.obligations - realized_obligations
    if missing_obligations:
        raise CompilationRejected(
            "Workflow Implementation does not realize required obligations: "
            f"{sorted(missing_obligations)}"
        )
    realized_outputs = {item.output_contract_ref for item in binding.output_contract_realizations}
    missing_outputs = definitions.workflow_type.output_contracts - realized_outputs
    if missing_outputs:
        raise CompilationRejected(
            f"Workflow Implementation does not realize required outputs: {sorted(missing_outputs)}"
        )

    blueprint = definitions.blueprint
    if isinstance(blueprint, StageGraphBlueprint):
        stages = {stage.stage_id: stage for stage in blueprint.stages}
        for realization in binding.obligation_realizations:
            stage = stages.get(realization.realization_ref)
            if realization.realization_kind != "stage" or stage is None:
                raise CompilationRejected(
                    "StageGraph obligation realization must name an existing stage"
                )
            if realization.obligation_ref not in stage.obligation_refs:
                raise CompilationRejected(
                    "StageGraph stage does not declare its realized obligation"
                )
        for output_realization in binding.output_contract_realizations:
            if output_realization.output_slot not in blueprint.declared_output_slots:
                raise CompilationRejected(
                    "StageGraph output realization names an undeclared output slot"
                )
    else:
        goal_contracts = {
            blueprint.objective_contract,
            blueprint.acceptance_contract,
        }
        if any(
            realization.realization_kind != "goal_acceptance"
            or realization.realization_ref not in goal_contracts
            for realization in binding.obligation_realizations
        ):
            raise CompilationRejected(
                "GoalDirected obligations must bind to its objective or acceptance contract"
            )


def _validate_bindings(
    request: CompilationRequest, definitions: ResolvedDefinitions
) -> list[OverlayDecision]:
    workflow = definitions.workflow_type
    resolved_pairs: list[tuple[ExactDefinitionRef, Definition]] = [
        (request.workflow_type_ref, definitions.workflow_type),
        (request.blueprint_ref, definitions.blueprint),
        (request.control_profile_ref, definitions.control_profile),
        (request.runtime_profile_ref, definitions.runtime_profile),
        (request.workspace_template_ref, definitions.workspace_template),
        (request.evaluation_profile_ref, definitions.evaluation_profile),
    ]
    if request.implementation_ref is not None:
        if definitions.implementation_binding is None:
            raise CompilationRejected(
                "Workflow Implementation reference and definition must both be present"
            )
        resolved_pairs.insert(
            1,
            (request.implementation_ref, definitions.implementation_binding),
        )
    elif definitions.implementation_binding is not None:
        raise CompilationRejected(
            "Workflow Implementation reference and definition must both be present"
        )
    if (
        request.workflow_configuration_ref is not None
        and definitions.workflow_configuration is not None
    ):
        resolved_pairs.append(
            (request.workflow_configuration_ref, definitions.workflow_configuration)
        )
    elif request.workflow_configuration_ref is not None or definitions.workflow_configuration:
        raise CompilationRejected(
            "workflow-specific configuration reference and definition must both be present"
        )
    published_by_ref = {record.ref: record for record in definitions.published_records}
    if len(published_by_ref) != len(definitions.published_records):
        raise CompilationRejected("resolved publication evidence contains duplicate references")
    for ref, definition in resolved_pairs:
        record = published_by_ref.get(ref)
        if record is None or record.definition != definition:
            raise CompilationRejected(
                f"resolved publication evidence does not match exact revision: {ref.logical_id}"
            )
        _validate_exact_ref(ref, definition)
    for ref, record in published_by_ref.items():
        _validate_exact_ref(ref, record.definition)
    source_ref_set = {ref for ref, _definition in resolved_pairs}
    if any(evidence.target not in source_ref_set for evidence in request.alias_evidence):
        raise CompilationRejected("alias resolution evidence targets an unselected revision")
    if definitions.implementation_binding is not None:
        _validate_implementation_binding(
            request,
            definitions,
            definitions.implementation_binding,
        )
    else:
        checks = (
            (request.blueprint_ref, workflow.allowed_blueprints, "blueprint"),
            (request.control_profile_ref, workflow.allowed_control_profiles, "control profile"),
            (request.runtime_profile_ref, workflow.allowed_runtime_profiles, "runtime profile"),
            (
                request.workspace_template_ref,
                workflow.allowed_workspace_templates,
                "workspace template",
            ),
            (
                request.evaluation_profile_ref,
                workflow.allowed_evaluation_profiles,
                "evaluation profile",
            ),
        )
        for selected, allowed, label in checks:
            if selected not in allowed:
                raise CompilationRejected(f"{label} is not allowed by the Workflow Type")
        if request.workflow_configuration_ref is None:
            if workflow.allowed_workflow_configurations:
                raise CompilationRejected(
                    "Workflow Type requires a workflow-specific configuration"
                )
        else:
            if request.workflow_configuration_ref not in workflow.allowed_workflow_configurations:
                raise CompilationRejected(
                    "workflow-specific configuration is not allowed by the Workflow Type"
                )
            assert definitions.workflow_configuration is not None
            if (
                definitions.workflow_configuration.workflow_type_logical_id
                != request.workflow_type_ref.logical_id
            ):
                raise CompilationRejected(
                    "workflow-specific configuration targets a different Workflow Type"
                )
    if definitions.control_profile.blueprint_ref != request.blueprint_ref:
        raise CompilationRejected("control profile selects a different blueprint")

    declared = _declared_variants(definitions.blueprint)
    if not definitions.control_profile.selected_variants <= declared:
        raise CompilationRejected("control profile selects undeclared blueprint variants")

    required_capabilities = (
        definitions.runtime_profile.required_capabilities
        | definitions.workspace_template.required_capabilities
        | definitions.evaluation_profile.required_capabilities
    )
    missing = required_capabilities - request.environment.capabilities
    if missing:
        raise CompilationRejected(
            f"required environment capabilities unavailable: {sorted(missing)}"
        )
    if definitions.runtime_profile.binding not in request.environment.runtime_bindings:
        raise CompilationRejected(
            f"runtime binding unavailable: {definitions.runtime_profile.binding}"
        )
    available_secrets = set(request.environment.secret_refs)
    missing_secrets = set(definitions.runtime_profile.required_secrets) - available_secrets
    if missing_secrets:
        raise CompilationRejected("one or more required secret references are unavailable")
    decisions: list[OverlayDecision] = []
    requirements = (
        definitions.runtime_profile.capability_requirements
        + definitions.workspace_template.capability_requirements
        + definitions.evaluation_profile.capability_requirements
    )
    for requirement in requirements:
        if requirement.capability in request.environment.capabilities:
            continue
        if requirement.when_unavailable == "reject":
            raise CompilationRejected(
                f"required environment capability unavailable: {requirement.capability}"
            )
        status = (
            OverlayDecisionStatus.DEGRADED
            if requirement.when_unavailable == "degrade"
            else OverlayDecisionStatus.OMITTED
        )
        decisions.append(
            OverlayDecision(
                field=f"environment.capability.{requirement.capability}",
                status=status,
                requested=requirement.capability,
                effective=None,
                reason=requirement.decision_reason,
            )
        )
    return decisions


def compile_effective_run_configuration(
    request: CompilationRequest,
    definitions: ResolvedDefinitions,
) -> EffectiveRunConfiguration:
    """Compile exact, already-resolved definitions without I/O, clocks, or mutable reads."""
    decisions = _validate_bindings(request, definitions)
    control: ControlProfileDefinition = definitions.control_profile
    authority = _intersect_authority(
        definitions.workflow_type.authority_ceiling,
        control.authority_ceiling,
        request.caller_authority,
        request.parent_authority,
        request.environment.capabilities,
    )
    required_authority = (
        definitions.runtime_profile.required_capabilities
        | definitions.workspace_template.required_capabilities
        | definitions.evaluation_profile.required_capabilities
    )
    unauthorized_required = required_authority - authority.capabilities
    if unauthorized_required:
        raise CompilationRejected(
            f"required capabilities exceed effective authority: {sorted(unauthorized_required)}"
        )
    contract_slots = {
        slot.name: slot for slot in definitions.workflow_type.workspace_contract.slots
    }
    template_slots = {slot.name: slot for slot in definitions.workspace_template.slots}
    incompatible_slots = [
        name
        for name, contract_slot in contract_slots.items()
        if template_slots.get(name) != contract_slot
    ]
    if incompatible_slots:
        raise CompilationRejected(
            f"workspace template does not satisfy contract slots: {sorted(incompatible_slots)}"
        )
    overlay = request.overlay

    capabilities = authority.capabilities
    if overlay.requested_capabilities is None:
        decisions.append(
            OverlayDecision(
                field="capabilities",
                status=OverlayDecisionStatus.OMITTED,
                effective=sorted(capabilities),
                reason="no per-run capability overlay supplied",
            )
        )
    else:
        if "capabilities" not in control.overlayable_fields:
            _reject("capabilities", overlay.requested_capabilities, "capabilities are fixed")
        if not overlay.requested_capabilities <= capabilities:
            _reject(
                "capabilities",
                overlay.requested_capabilities,
                "requested capabilities exceed an authority or environment ceiling",
            )
        capabilities = overlay.requested_capabilities
        decisions.append(
            OverlayDecision(
                field="capabilities",
                status=OverlayDecisionStatus.ACCEPTED,
                requested=sorted(overlay.requested_capabilities),
                effective=sorted(capabilities),
                reason="request is within all intersected ceilings",
            )
        )
    if not required_authority <= capabilities:
        _reject(
            "capabilities",
            overlay.requested_capabilities,
            "capability overlay removes a required runtime, workspace, or evaluation capability",
        )

    budgets = authority.budgets
    if overlay.budget_ceilings is None:
        decisions.append(
            OverlayDecision(
                field="budgets",
                status=OverlayDecisionStatus.OMITTED,
                effective=budgets.dimensions,
                reason="no per-run budget overlay supplied",
            )
        )
    else:
        if "budgets" not in control.overlayable_fields:
            _reject("budgets", overlay.budget_ceilings, "budgets are fixed")
        for dimension, requested in overlay.budget_ceilings.items():
            ceiling = budgets.dimensions.get(dimension)
            if requested < 0 or ceiling is None or requested > ceiling:
                _reject(
                    "budgets",
                    overlay.budget_ceilings,
                    f"budget dimension {dimension!r} exceeds its effective ceiling",
                )
        budgets = BudgetCeiling(dimensions=dict(sorted(overlay.budget_ceilings.items())))
        decisions.append(
            OverlayDecision(
                field="budgets",
                status=OverlayDecisionStatus.ACCEPTED,
                requested=overlay.budget_ceilings,
                effective=budgets.dimensions,
                reason="budget request strengthens the effective ceilings",
            )
        )

    max_concurrency = authority.max_concurrency
    if overlay.max_concurrency is None:
        decisions.append(
            OverlayDecision(
                field="max_concurrency",
                status=OverlayDecisionStatus.OMITTED,
                effective=max_concurrency,
                reason="no per-run concurrency overlay supplied",
            )
        )
    else:
        if "max_concurrency" not in control.overlayable_fields:
            _reject("max_concurrency", overlay.max_concurrency, "max_concurrency is fixed")
        if overlay.max_concurrency > max_concurrency:
            _reject(
                "max_concurrency",
                overlay.max_concurrency,
                "requested concurrency exceeds an effective ceiling",
            )
        max_concurrency = overlay.max_concurrency
        decisions.append(
            OverlayDecision(
                field="max_concurrency",
                status=OverlayDecisionStatus.ACCEPTED,
                requested=overlay.max_concurrency,
                effective=max_concurrency,
                reason="concurrency request strengthens the effective ceiling",
            )
        )

    variants = control.selected_variants
    if overlay.selected_variants is None:
        decisions.append(
            OverlayDecision(
                field="variants",
                status=OverlayDecisionStatus.OMITTED,
                effective=sorted(variants),
                reason="control-profile variants retained",
            )
        )
    else:
        if "variants" not in control.overlayable_fields:
            _reject("variants", overlay.selected_variants, "variants are fixed")
        declared = _declared_variants(definitions.blueprint)
        if not overlay.selected_variants <= declared:
            _reject(
                "variants",
                overlay.selected_variants,
                "overlay selects an undeclared blueprint variant",
            )
        variants = overlay.selected_variants
        decisions.append(
            OverlayDecision(
                field="variants",
                status=OverlayDecisionStatus.ACCEPTED,
                requested=sorted(overlay.selected_variants),
                effective=sorted(variants),
                reason="all selected variants are blueprint-declared",
            )
        )

    workflow_extensions = (
        definitions.workflow_configuration.extensions
        if definitions.workflow_configuration is not None
        else ()
    )
    allowed_overlay_extensions = {
        (identity.namespace, identity.schema_version, identity.discriminator)
        for identity in definitions.workflow_type.allowed_overlay_extensions
    }
    for extension in overlay.extensions:
        identity = (
            extension.namespace,
            extension.schema_version,
            extension.discriminator,
        )
        if identity not in allowed_overlay_extensions:
            _reject(
                "extensions",
                extension,
                f"overlay extension is not allowed by the Workflow Type: {identity}",
            )
    requested_extensions = (
        definitions.workflow_type.required_extensions + workflow_extensions + overlay.extensions
    )
    identities = [
        (extension.namespace, extension.schema_version, extension.discriminator)
        for extension in requested_extensions
    ]
    if len(identities) != len(set(identities)):
        raise CompilationRejected("duplicate executable extension identity")
    extensions = requested_extensions
    effective_authority = AuthorityCeiling(
        capabilities=capabilities,
        budgets=budgets,
        max_concurrency=max_concurrency,
    )
    profiles_by_ref = {
        record.ref: record.definition
        for record in definitions.published_records
        if isinstance(record.definition, AgentProfileDefinition)
    }
    flattened_bindings, attachment_plan = _compile_agent_bindings(
        definitions.runtime_profile.operation_assemblies,
        profiles_by_ref,
        request.environment.exact_capabilities,
        definitions,
        authority,
        effective_authority,
    )
    primary_source_refs = (
        (request.workflow_type_ref,)
        + ((request.implementation_ref,) if request.implementation_ref is not None else ())
        + (
            request.blueprint_ref,
            request.control_profile_ref,
            request.runtime_profile_ref,
            request.workspace_template_ref,
            request.evaluation_profile_ref,
        )
        + (
            (request.workflow_configuration_ref,)
            if request.workflow_configuration_ref is not None
            else ()
        )
    )
    source_refs = primary_source_refs + tuple(
        sorted(
            (
                record.ref
                for record in definitions.published_records
                if record.ref not in set(primary_source_refs)
            ),
            key=lambda ref: (ref.kind.value, *_ref_order(ref)),
        )
    )
    payload = {
        "schema_version": "1",
        "compiler_version": COMPILER_VERSION,
        "context": request.context,
        "source_refs": source_refs,
        "alias_evidence": request.alias_evidence,
        "input_manifest": request.input_manifest,
        "workflow_type": definitions.workflow_type,
        "selected_blueprint": definitions.blueprint,
        "selected_variants": variants,
        "control_profile": control,
        "runtime_profile": definitions.runtime_profile,
        "workspace_template": definitions.workspace_template,
        "workflow_workspace_contract": definitions.workflow_type.workspace_contract,
        "evaluation_profile": definitions.evaluation_profile,
        "workflow_specific_configuration": definitions.workflow_configuration,
        "effective_authority": effective_authority,
        "linked_run_slots": definitions.workflow_type.linked_run_slots,
        "extensions": extensions,
        "overlay_decisions": tuple(decisions),
        "operation_assemblies": definitions.runtime_profile.operation_assemblies,
        "flattened_agent_bindings": flattened_bindings,
        "capability_attachment_plan": attachment_plan,
    }
    return EffectiveRunConfiguration(digest=sha256_digest(payload), **payload)
