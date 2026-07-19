from __future__ import annotations

from collections.abc import Iterable

from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AuthorityCeiling,
    BudgetCeiling,
    CompilationRequest,
    ControlProfileDefinition,
    Definition,
    EffectiveRunConfiguration,
    ExactDefinitionRef,
    GoalDirectedBlueprint,
    OverlayDecision,
    OverlayDecisionStatus,
    ResolvedDefinitions,
    StageGraphBlueprint,
)
from app.domain.control_plane.errors import CompilationRejected
from app.domain.control_plane.extensions import ExtensionRegistry

COMPILER_VERSION = "control-plane-f1/1"


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
    if set(published_by_ref) != {ref for ref, _definition in resolved_pairs}:
        raise CompilationRejected("resolved publication evidence contains unexpected references")
    source_ref_set = {ref for ref, _definition in resolved_pairs}
    if any(evidence.target not in source_ref_set for evidence in request.alias_evidence):
        raise CompilationRejected("alias resolution evidence targets an unselected revision")
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
            raise CompilationRejected("Workflow Type requires a workflow-specific configuration")
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
    extension_registry: ExtensionRegistry,
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
    extensions = extension_registry.validate_all(requested_extensions)
    effective_authority = AuthorityCeiling(
        capabilities=capabilities,
        budgets=budgets,
        max_concurrency=max_concurrency,
    )
    source_refs = (
        request.workflow_type_ref,
        request.blueprint_ref,
        request.control_profile_ref,
        request.runtime_profile_ref,
        request.workspace_template_ref,
        request.evaluation_profile_ref,
    ) + (
        (request.workflow_configuration_ref,)
        if request.workflow_configuration_ref is not None
        else ()
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
    }
    return EffectiveRunConfiguration(digest=sha256_digest(payload), **payload)
