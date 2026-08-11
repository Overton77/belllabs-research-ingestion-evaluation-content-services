"""Executable Stage 0–2 Q/D reference verticals over the accepted generic seams."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, TypeAdapter

from app.application.control_plane.service import ControlPlaneService
from app.application.operations.operation_executor import (
    CancellationContext,
    CompletedOperationOutcome,
    ExactStageExecutionBinding,
    OperationExecutor,
    OperationExecutorConformanceHarness,
    StageOperationRequest,
)
from app.application.operations.operation_journal import OperationJournalMutation, OperationJournalService
from app.application.runtime.runtime_run_plan import (
    compile_run_plan_v4,
    compile_structural_graph_assembly_v3,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import (
    AuthorityCeiling,
    BudgetCeiling,
    CompilationContext,
    CompileInvocation,
    ControlProfileDefinition,
    DefinitionSelector,
    EnvironmentAvailability,
    EvaluationProfileDefinition,
    ExactDefinitionRef,
    ObligationRealization,
    OutputContractRealization,
    PublishedDefinition,
    PublishRequest,
    RunInputManifestRef,
    RuntimeProfileDefinition,
    StageGraphBlueprint,
    WorkflowImplementationBindingDefinition,
    WorkflowTypeDefinition,
    WorkflowWorkspaceContract,
    WorkspaceSlot,
    WorkspaceTemplateDefinition,
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
    OperationAssemblySpecV3,
    RunPlanV4,
    RuntimeDefinitionKind,
    StageCapabilityRequirement,
    StageCapabilityRequirementRef,
    StageExecutionBindingV2,
    TemporalExecutionProfileRef,
)
from app.domain.graph_runtime.kernel import (
    ResourceKind,
    ResourceLeaseRecord,
    ResourceLeaseRequest,
    ResourceLeaseStatus,
)
from app.domain.operation_execution.journal import (
    OperationEffectClaim,
    OperationJournalSettlement,
    OperationTechnicalAttempt,
)
from app.domain.reference_research.contracts import (
    DAVE_FAMILY_ID,
    QUALIA_FAMILY_ID,
    CompanyRelationshipClass,
    DaveCompanyClaim,
    DaveFixtureInput,
    DaveOwnershipResult,
    QualiaCatalogResult,
    QualiaFixtureInput,
    QualiaProductClaim,
    ReferenceFixture,
)

REFERENCE_BLUEPRINT_VERSION = "stage0-2.v1"
REFERENCE_IMPLEMENTATION_VERSION = "fixture-native.v1"
DETERMINISTIC_CAPABILITY = "deterministic_fixture"
REFERENCE_FIXTURE_ADAPTER: TypeAdapter[ReferenceFixture] = TypeAdapter(ReferenceFixture)


class ImmutableManifestStore:
    """Small content-addressed artifact/evidence store used by deterministic CI execution."""

    def __init__(self) -> None:
        self._payloads: dict[str, bytes] = {}

    def put(self, value: BaseModel | dict[str, object]) -> tuple[str, str, int]:
        serializable = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
        payload = json.dumps(
            serializable,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        digest = sha256_digest(value)
        ref = f"manifest:{digest}"
        prior = self._payloads.get(ref)
        if prior is not None and prior != payload:
            raise ValueError("content-addressed manifest collision")
        self._payloads[ref] = payload
        return ref, digest, len(payload)

    def get_json(self, ref: str) -> bytes:
        return self._payloads[ref]


class ReferenceOperationManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    family_id: str
    stage_id: str
    as_of: datetime
    result: dict[str, object] | None = None
    evidence_refs: tuple[str, ...] = ()


class ReferenceExecutionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    family_id: str
    blueprint_ref: ExactDefinitionRef
    implementation_ref: ExactDefinitionRef
    graph_assembly_digest: str
    run_plan_digest: str
    result_manifest_ref: str
    result_manifest_digest: str
    settlement_refs: tuple[str, ...]
    lineage: tuple[ExecutionLineageEnvelopeV2, ...]


@dataclass(frozen=True)
class PreparedReferenceImplementation:
    family_id: str
    fixture_digest: str
    blueprint_record: PublishedDefinition
    implementation_record: PublishedDefinition
    effective_configuration_digest: str
    graph_assembly: GraphAssemblySpecV3
    run_plan: RunPlanV4
    assemblies: dict[str, OperationAssemblySpecV3]


def load_reference_fixture(payload: bytes) -> QualiaFixtureInput | DaveFixtureInput:
    """Load strict sanitized fixture bytes; no provider or filesystem authority is implied."""

    return REFERENCE_FIXTURE_ADAPTER.validate_json(payload)


def reference_blueprint(family_id: str) -> StageGraphBlueprint:
    if family_id == QUALIA_FAMILY_ID:
        stage_ids = (
            "normalize_request",
            "discover_sources",
            "extract_candidates",
            "verify_current_offer",
            "normalize_deduplicate",
            "ambiguity_review",
            "publish_result",
        )
        title = "Qualia Life current supplement products — Stage 0–2 fixture"
        description = (
            "Immutable deterministic increment with explicit as-of/source authority, inclusion "
            "policy, verification, deduplication, ambiguity review, evidence, and canonical URLs."
        )
    elif family_id == DAVE_FAMILY_ID:
        stage_ids = (
            "normalize_request",
            "discover_companies",
            "collect_evidence",
            "verify_relationships",
            "targeted_follow_up",
            "ambiguity_review",
            "publish_result",
        )
        title = "Dave Asprey current company ownership — Stage 0–2 fixture"
        description = (
            "Immutable deterministic increment separating ownership/control from founding, "
            "investing, advising, endorsement, former association, and unresolved evidence."
        )
    else:
        raise ValueError(f"unsupported reference family: {family_id}")
    stages = tuple(
        StageGraphStageSpec(
            stage_id=stage_id,
            depends_on=((stage_ids[index - 1],) if index else ()),
            obligation_refs=(f"obligation:{family_id}:{stage_id}@1",),
            output_slots=("typed_result",) if stage_id == "publish_result" else (
                f"{stage_id}:result",
            ),
        )
        for index, stage_id in enumerate(stage_ids)
    )
    return build_stagegraph_v2(
        logical_id=f"{family_id}.blueprint.{REFERENCE_BLUEPRINT_VERSION}",
        title=title,
        description=description,
        stages=stages,
        max_concurrency=1,
    )


async def prepare_reference_implementation(
    service: ControlPlaneService,
    *,
    family_id: str,
    fixture_digest: str,
    now: datetime,
) -> PreparedReferenceImplementation:
    """Publish, load, compile, and freeze one reference implementation through real seams."""

    blueprint = reference_blueprint(family_id)
    blueprint_record = await _publish(service, blueprint, now)
    authority = AuthorityCeiling(
        capabilities=frozenset({DETERMINISTIC_CAPABILITY}),
        budgets=BudgetCeiling(dimensions={"fixture.records": 32}),
        max_concurrency=1,
    )
    control_record = await _publish(
        service,
        ControlProfileDefinition(
            logical_id=f"{family_id}.control.{REFERENCE_BLUEPRINT_VERSION}",
            title=f"{family_id} fixture control",
            description="Exact single-worker fixture control profile.",
            blueprint_ref=blueprint_record.ref,
            authority_ceiling=authority,
        ),
        now,
    )
    runtime_record = await _publish(
        service,
        RuntimeProfileDefinition(
            logical_id=f"{family_id}.runtime.{REFERENCE_IMPLEMENTATION_VERSION}",
            title=f"{family_id} fixture runtime",
            description="Deterministic in-process operation execution; no provider fallback.",
            binding="belllabs.reference-fixture.in-process.v1",
            required_capabilities=frozenset({DETERMINISTIC_CAPABILITY}),
        ),
        now,
    )
    output_slot = WorkspaceSlot(
        name="result",
        path="/reference/result",
        access="exclusive_write",
        purpose="content-addressed typed result and evidence manifests",
    )
    workspace_record = await _publish(
        service,
        WorkspaceTemplateDefinition(
            logical_id=f"{family_id}.workspace.{REFERENCE_BLUEPRINT_VERSION}",
            title=f"{family_id} fixture workspace",
            description="Compact manifest-only reference workspace.",
            slots=(output_slot,),
        ),
        now,
    )
    evaluation_record = await _publish(
        service,
        EvaluationProfileDefinition(
            logical_id=f"{family_id}.evaluation.{REFERENCE_BLUEPRINT_VERSION}",
            title=f"{family_id} fixture evaluation",
            description="Deterministic schema, evidence, ambiguity, and lineage gates.",
            gate_contract_refs=frozenset({f"contract:{family_id}:fixture-invariants@1"}),
        ),
        now,
    )
    obligations = frozenset(
        slot.obligation_ref for stage in blueprint.stages for slot in stage.obligation_slots
    )
    output_contract = f"contract:{family_id}:typed-result@1"
    workflow_record = await _publish(
        service,
        WorkflowTypeDefinition(
            logical_id=family_id,
            title=blueprint.title,
            description=blueprint.description,
            purpose="Produce a time-indexed evidence result; research is not medical advice.",
            non_goals=frozenset(
                {
                    "Medical advice",
                    "Treat founding or affiliation as ownership",
                    "Treat changing live-web answers as deterministic fixtures",
                }
            ),
            input_admission_contract=f"contract:{family_id}:input@1",
            invariants=frozenset(
                {
                    "explicit-as-of",
                    "accepted-source-evidence",
                    "ambiguity-remains-unknown",
                    "no-provider-transcript-authority",
                }
            ),
            obligations=obligations,
            output_contracts=frozenset({output_contract}),
            allowed_blueprints=frozenset({blueprint_record.ref}),
            allowed_control_profiles=frozenset({control_record.ref}),
            allowed_runtime_profiles=frozenset({runtime_record.ref}),
            allowed_workspace_templates=frozenset({workspace_record.ref}),
            allowed_evaluation_profiles=frozenset({evaluation_record.ref}),
            authority_ceiling=authority,
            workspace_contract=WorkflowWorkspaceContract(slots=(output_slot,)),
        ),
        now,
    )
    implementation = WorkflowImplementationBindingDefinition(
        logical_id=f"{family_id}.implementation.{REFERENCE_IMPLEMENTATION_VERSION}",
        title=f"{family_id} deterministic Stage 0–2 implementation",
        description="Exact fixture implementation over reusable BellLabs contracts.",
        workflow_type_ref=workflow_record.ref,
        blueprint_ref=blueprint_record.ref,
        control_profile_ref=control_record.ref,
        runtime_profile_ref=runtime_record.ref,
        workspace_template_ref=workspace_record.ref,
        evaluation_profile_ref=evaluation_record.ref,
        obligation_realizations=tuple(
            ObligationRealization(
                obligation_ref=stage.obligation_slots[0].obligation_ref,
                realization_kind="stage",
                realization_ref=stage.stage_id,
            )
            for stage in blueprint.stages
        ),
        output_contract_realizations=(
            OutputContractRealization(
                output_contract_ref=output_contract,
                output_slot="typed_result",
            ),
        ),
        conformance_evidence_refs=frozenset(
            {"evidence:reference-stage0-2-fixture", f"fixture:{fixture_digest}"}
        ),
    )
    implementation_record = await _publish(service, implementation, now)
    erc = await service.compile(
        CompileInvocation(
            workflow_type=DefinitionSelector(exact=workflow_record.ref),
            implementation=DefinitionSelector(exact=implementation_record.ref),
            input_manifest=RunInputManifestRef(
                manifest_id=f"fixture:{family_id}", revision=1, digest=fixture_digest
            ),
            caller_authority=authority,
            environment=EnvironmentAvailability(
                capabilities=frozenset({DETERMINISTIC_CAPABILITY}),
                runtime_bindings=frozenset({"belllabs.reference-fixture.in-process.v1"}),
            ),
            context=CompilationContext(
                compilation_id=f"compile:{family_id}:{REFERENCE_IMPLEMENTATION_VERSION}",
                compiled_at=now,
                actor_id="reference-fixture-compiler",
                authority_subject_id="reference-fixture-compiler",
                authority_scope="reference-fixtures",
            ),
        )
    )
    resources = ExecutionResourceEnvelopeV2(
        tenant_limit_ref="limit:reference-fixtures",
        environment_limit_ref="limit:ci",
        workflow_run_slots=1,
        family_scheduler_slots=1,
        stage_slots=1,
        operation_workflow_slots=1,
        operation_worker_slots=1,
        model_call_slots=0,
        tool_call_slots=0,
        mcp_call_slots=0,
        sync_subagent_slots=0,
        async_child_slots=0,
        linked_run_slots=0,
        budget_reservation_refs=("budget:fixture.records:32",),
        deadline="PT30S",
        lease_ttl="PT30S",
        resumption_reserve=1,
        release_policy="release:all-on-operation-completion",
    )
    resource_ref = ExecutionResourceEnvelopeRef(
        logical_id=f"{family_id}.resources.{REFERENCE_IMPLEMENTATION_VERSION}",
        digest=sha256_digest(resources.model_dump(mode="json")),
    )
    capability_manifest = CapabilityManifestDefinition(
        logical_id=f"{family_id}.capabilities.{REFERENCE_IMPLEMENTATION_VERSION}",
        title="Reference fixture capabilities",
        description="Only deterministic fixture execution is selectable.",
        capabilities=(
            CapabilityMaturityRecord(
                capability_id=DETERMINISTIC_CAPABILITY,
                maturity="stable",
                required_for_migration=True,
                feature_flag="REFERENCE_FIXTURE_ENABLED",
                enabled=True,
                fallback="reject",
            ),
        ),
    )
    capability_ref = ContentAddressedRef(
        kind=RuntimeDefinitionKind.CAPABILITY_MANIFEST,
        logical_id=capability_manifest.logical_id,
        schema_version=capability_manifest.schema_version,
        digest=capability_manifest.digest,
    )
    compatibility_digest = sha256_digest(
        {"family_id": family_id, "blueprint": blueprint_record.ref, "version": "stage0-2.v1"}
    )
    compatibility_ref = CompatibilityManifestRef(
        logical_id=f"{family_id}.compatibility.{REFERENCE_BLUEPRINT_VERSION}",
        schema_version="belllabs.compatibility-manifest.v1",
        digest=compatibility_digest,
    )
    temporal_profile_ref = TemporalExecutionProfileRef(
        logical_id="temporal.operation.reference-fixture",
        schema_version="belllabs.temporal-execution-profile.v1",
        digest=sha256_digest(
            {"queue_class": "native", "activity": "reference_fixture", "timeouts": "PT30S"}
        ),
    )
    requirements: list[StageCapabilityRequirement] = []
    bindings: list[StageExecutionBindingV2] = []
    assemblies: dict[str, OperationAssemblySpecV3] = {}
    for stage in blueprint.stages:
        requirement = StageCapabilityRequirement(
            stage_id=stage.stage_id,
            operation_contract_ref=f"contract:{family_id}:{stage.stage_id}@1",
            required_capability_ids=frozenset({DETERMINISTIC_CAPABILITY}),
            input_contract_ref=f"contract:{family_id}:fixture-input@1",
            output_contract_ref=(
                output_contract
                if stage.stage_id == "publish_result"
                else f"contract:{family_id}:{stage.stage_id}-manifest@1"
            ),
            context_purpose="reference_fixture",
            effect_class="pure",
            resource_class_ref=resource_ref.logical_id,
            verification_contract_ref=f"contract:{family_id}:fixture-invariants@1",
            degradation_contract_ref="policy:no-degradation",
            speculation_policy_ref="policy:speculation:disabled",
        )
        operation_id = f"{family_id}.operation.{stage.stage_id}.{REFERENCE_IMPLEMENTATION_VERSION}"
        assembly = OperationAssemblySpecV3.create(
            operation_assembly_id=operation_id,
            operation_contract_ref=requirement.operation_contract_ref,
            implementation_kind="native",
            adapter_variant="local_exact",
            implementation_ref=_content_ref("operation_registry", operation_id),
            model_policy_ref=_content_ref("state_schema", "disabled-model-policy"),
            prompt_manifest_ref=_content_ref("state_schema", "empty-prompt-manifest"),
            middleware_manifest_ref=_content_ref("state_schema", "empty-middleware-manifest"),
            tool_manifest_ref=_content_ref("state_schema", "empty-tool-manifest"),
            mcp_manifest_ref=_content_ref("state_schema", "empty-mcp-manifest"),
            skill_manifest_ref=_content_ref("state_schema", "empty-skill-manifest"),
            context_assembly_ref=_content_ref("state_schema", "reference-fixture-context"),
            delegation_policy_ref=_content_ref("state_schema", "delegation-disabled"),
            workspace_policy_ref=_content_ref("state_schema", "reference-manifest-workspace"),
            sandbox_profile_ref=_content_ref("state_schema", "sandbox-disabled"),
            verifier_ref=_content_ref("state_schema", "deterministic-invariant-verifier"),
            resource_envelope_ref=resource_ref,
            effect_policy_ref=_content_ref("state_schema", "pure-operation"),
            fallback_policy_ref=_content_ref("state_schema", "fallback-reject"),
            trace_redaction_policy_ref=_content_ref("state_schema", "no-payload-tracing"),
            capability_manifest_ref=capability_ref,
            temporal_execution_profile_ref=temporal_profile_ref,
            compatibility_manifest_ref=compatibility_ref,
        )
        requirement_ref = StageCapabilityRequirementRef(
            logical_id=f"stage-requirement:{stage.stage_id}:default",
            digest=sha256_digest(requirement.model_dump(mode="json")),
        )
        assembly_ref = OperationAssemblyRef(
            logical_id=operation_id,
            digest=assembly.operation_assembly_digest,
        )
        binding = StageExecutionBindingV2(
            stage_id=stage.stage_id,
            stage_requirement_ref=requirement_ref,
            operation_assembly_ref=assembly_ref,
            operation_assembly_digest=assembly.operation_assembly_digest,
            input_projection_ref=f"projection:{family_id}:{stage.stage_id}:input@1",
            output_projection_ref=f"projection:{family_id}:{stage.stage_id}:output@1",
            resource_envelope_ref=resource_ref,
            temporal_execution_profile_ref=temporal_profile_ref,
            compatibility_key=f"{family_id}|{REFERENCE_IMPLEMENTATION_VERSION}|{stage.stage_id}",
        )
        requirements.append(requirement)
        bindings.append(binding)
        assemblies[operation_id] = assembly
    graph, unavailable = compile_structural_graph_assembly_v3(
        blueprint=blueprint,
        effective_configuration=erc,
        graph_assembly_ref=_content_ref("graph_assembly", f"{family_id}.graph.stage0-2.v1"),
        state_schema_digest=sha256_digest({"family_id": family_id, "state": "manifest-refs-only"}),
        reducer_registry_digest=sha256_digest({"reducers": ["append_unique_manifest_ref"]}),
        operation_registry_digest=sha256_digest(sorted(assemblies)),
        requirements=tuple(requirements),
        bindings=tuple(bindings),
        assemblies=assemblies,
        compatibility_manifest_digest=compatibility_digest,
        capability_manifest_ref=capability_ref,
        capability_manifest=capability_manifest,
        capability_readiness=(
            RuntimeCapabilityReadiness(
                capability_id=DETERMINISTIC_CAPABILITY,
                maturity="stable",
                enabled=True,
                ready=True,
                reason="checked-in sanitized fixture executor is available",
                fallback="reject",
            ),
        ),
    )
    if unavailable:
        raise ValueError(
            f"reference fixture compilation predicted unavailable surfaces: {unavailable}"
        )
    run_plan = compile_run_plan_v4(
        plan_id=f"plan:{family_id}:{REFERENCE_IMPLEMENTATION_VERSION}",
        effective_configuration=erc,
        semantic_binding_ref=f"semantic-binding:{family_id}:{REFERENCE_BLUEPRINT_VERSION}",
        workflow_implementation_ref=implementation_record.ref,
        graph_assembly=graph,
    )
    return PreparedReferenceImplementation(
        family_id=family_id,
        fixture_digest=fixture_digest,
        blueprint_record=blueprint_record,
        implementation_record=implementation_record,
        effective_configuration_digest=erc.digest,
        graph_assembly=graph,
        run_plan=run_plan,
        assemblies=assemblies,
    )


class DeterministicReferenceOperationExecutor:
    def __init__(
        self,
        fixture: QualiaFixtureInput | DaveFixtureInput,
        store: ImmutableManifestStore,
    ) -> None:
        self._fixture = fixture
        self._store = store

    async def execute(
        self,
        stage_request: StageOperationRequest,
        exact_stage_execution_binding: ExactStageExecutionBinding,
        execution_resource_lease: ResourceLeaseRecord,
        cancellation_context: CancellationContext,
    ) -> CompletedOperationOutcome:
        if cancellation_context.requested:
            raise ValueError("deterministic reference executor was invoked after cancellation")
        if execution_resource_lease.status != ResourceLeaseStatus.ACQUIRED:
            raise ValueError("reference executor requires an acquired resource lease")
        result: dict[str, object] | None = None
        if stage_request.operation_id == "publish_result":
            typed = classify_reference_fixture(self._fixture)
            result = cast(dict[str, object], typed.model_dump(mode="json"))
        evidence_refs = tuple(f"evidence:{source.source_id}" for source in self._fixture.sources)
        manifest = ReferenceOperationManifest(
            family_id=self._fixture.family_id,
            stage_id=stage_request.operation_id,
            as_of=self._fixture.as_of,
            result=result,
            evidence_refs=evidence_refs,
        )
        manifest_ref, _digest, _size = self._store.put(manifest)
        return CompletedOperationOutcome(
            result_manifest_ref=manifest_ref,
            evidence_refs=evidence_refs,
            usage_refs=(f"usage:{stage_request.semantic_attempt_id}:records",),
        )


async def execute_reference_fixture(
    *,
    prepared: PreparedReferenceImplementation,
    fixture: QualiaFixtureInput | DaveFixtureInput,
    journal: OperationJournalService,
    store: ImmutableManifestStore,
    run_id: str,
    now: datetime,
    executor: OperationExecutor | None = None,
) -> ReferenceExecutionResult:
    """Execute every semantic operation over exact bindings and journal each accepted outcome."""

    fixture_digest = sha256_digest(fixture.model_dump(mode="json"))
    if fixture.family_id != prepared.family_id:
        raise ValueError("fixture family differs from the frozen implementation")
    if fixture_digest != prepared.fixture_digest:
        raise ValueError("fixture digest differs from the frozen implementation input")
    selected_executor = executor or DeterministicReferenceOperationExecutor(fixture, store)
    harness = OperationExecutorConformanceHarness()
    lineages: list[ExecutionLineageEnvelopeV2] = []
    settlement_refs: list[str] = []
    result_ref = ""
    result_digest = ""
    for index, binding in enumerate(prepared.graph_assembly.stage_execution_bindings, start=1):
        assembly = prepared.assemblies[binding.operation_assembly_ref.logical_id]
        semantic_attempt = f"{run_id}:{binding.stage_id}:semantic-attempt:1"
        lease_request = ResourceLeaseRequest(
            lease_id=f"lease-{run_id}-{index}",
            request_scope="reference-fixtures",
            semantic_identity=semantic_attempt,
            envelope_digest=binding.resource_envelope_ref.digest,
            resources=(
                ResourceKind.TENANT,
                ResourceKind.WORKFLOW_RUN,
                ResourceKind.STAGE,
                ResourceKind.OPERATION_WORKER,
            ),
            requested_at=now,
            deadline=now + timedelta(seconds=30),
            ttl_seconds=30,
        )
        lease = ResourceLeaseRecord(
            request=lease_request,
            status=ResourceLeaseStatus.ACQUIRED,
            acquired_at=now,
            expires_at=now + timedelta(seconds=30),
            canonical_digest=sha256_digest(lease_request.model_dump(mode="json")),
        )
        outcome = await harness.assert_conforms(
            selected_executor,
            StageOperationRequest(
                request_scope="reference-fixtures",
                operation_id=binding.stage_id,
                semantic_attempt_id=semantic_attempt,
                input_manifest_ref=f"fixture:{fixture_digest}",
                input_digest=fixture_digest,
            ),
            ExactStageExecutionBinding(
                binding_ref=binding.operation_assembly_ref.logical_id,
                operation_assembly_digest=assembly.operation_assembly_digest,
            ),
            lease,
            CancellationContext(
                cancellation_id=f"cancel-{run_id}-{index}",
                cascade_policy_ref="policy:cooperative-cascade",
            ),
        )
        if outcome.kind != "completed":
            raise ValueError(f"reference fixture operation did not complete: {outcome.kind}")
        payload = store.get_json(outcome.result_manifest_ref)
        manifest_digest = "sha256:" + outcome.result_manifest_ref.removeprefix("manifest:sha256:")
        claim_id = f"claim-{run_id}-{index}"
        settlement_id = f"settlement-{run_id}-{index}"
        claim = OperationEffectClaim(
            effect_claim_id=claim_id,
            request_scope="reference-fixtures",
            belllabs_run_id=run_id,
            operation_contract_digest=sha256_digest(assembly.operation_contract_ref),
            idempotency_key=semantic_attempt,
            request_digest=sha256_digest(
                {
                    "fixture_digest": fixture_digest,
                    "assembly_digest": assembly.operation_assembly_digest,
                }
            ),
            semantic_binding_id=binding.operation_assembly_ref.logical_id,
            semantic_binding_digest=assembly.operation_assembly_digest,
            semantic_attempt_key=semantic_attempt,
            claimed_by="reference-fixture-executor",
            claimed_at=now,
        )
        attempt = OperationTechnicalAttempt(
            operation_attempt_id=f"runtime-{run_id}-{index}",
            request_scope="reference-fixtures",
            effect_claim_id=claim_id,
            technical_attempt=1,
            provider="deterministic-reference-fixture",
            disposition="succeeded",
            idempotency_supported=True,
            retry_class="safe",
            usage={"fixture.records": 1},
            started_at=now,
            finished_at=now,
        )
        settlement = OperationJournalSettlement.create(
            settlement_id=settlement_id,
            request_scope="reference-fixtures",
            effect_claim_id=claim_id,
            settlement_revision=1,
            status="completed",
            usage={"fixture.records": 1},
            result_manifest_ref=outcome.result_manifest_ref,
            result_manifest_digest=manifest_digest,
            result_manifest_size_bytes=len(payload),
            detail={"schema_version": "reference-stage0-2.v1"},
            settled_at=now,
        )
        await journal.commit(
            OperationJournalMutation(
                request_scope="reference-fixtures",
                belllabs_run_id=run_id,
                expected_run_version=1,
                claim=claim,
                attempt=attempt,
                settlement=settlement,
            )
        )
        result_ref = outcome.result_manifest_ref
        result_digest = manifest_digest
        settlement_refs.append(f"settlement:{settlement_id}")
        lineages.append(
            ExecutionLineageEnvelopeV2(
                request_scope="reference-fixtures",
                belllabs_run_id=run_id,
                execution_epoch=1,
                technical_segment=1,
                workflow_implementation_ref=str(prepared.implementation_record.ref.logical_id),
                graph_assembly_digest=prepared.graph_assembly.graph_assembly_ref.digest,
                workflow_cycle=0,
                stage_id=binding.stage_id,
                stage_cycle=0,
                semantic_operation_attempt_id=semantic_attempt,
                execution_generation=1,
                runtime_attempt_id=attempt.operation_attempt_id,
                operation_binding_id=binding.operation_assembly_ref.logical_id,
                operation_assembly_digest=assembly.operation_assembly_digest,
                input_manifest_digest=fixture_digest,
                result_manifest_ref=result_ref,
                evidence_refs=outcome.evidence_refs,
                usage_settlement_refs=(f"usage-settlement:{settlement_id}",),
                effect_claim_ids=(claim_id,),
                effect_settlement_refs=(f"effect-settlement:{settlement_id}",),
            )
        )
    return ReferenceExecutionResult(
        family_id=fixture.family_id,
        blueprint_ref=prepared.blueprint_record.ref,
        implementation_ref=prepared.implementation_record.ref,
        graph_assembly_digest=prepared.graph_assembly.graph_assembly_ref.digest,
        run_plan_digest=prepared.run_plan.plan_digest,
        result_manifest_ref=result_ref,
        result_manifest_digest=result_digest,
        settlement_refs=tuple(settlement_refs),
        lineage=tuple(lineages),
    )


def reconstruct_typed_result(
    store: ImmutableManifestStore,
    execution: ReferenceExecutionResult,
) -> QualiaCatalogResult | DaveOwnershipResult:
    manifest = ReferenceOperationManifest.model_validate_json(
        store.get_json(execution.result_manifest_ref)
    )
    if manifest.result is None:
        raise ValueError("terminal manifest has no typed result")
    if execution.family_id == QUALIA_FAMILY_ID:
        return QualiaCatalogResult.model_validate(manifest.result)
    return DaveOwnershipResult.model_validate(manifest.result)


async def reconstruct_typed_result_from_journal(
    journal: OperationJournalService,
    store: ImmutableManifestStore,
    execution: ReferenceExecutionResult,
) -> QualiaCatalogResult | DaveOwnershipResult:
    """Rebuild the final semantic result from accepted settlement and artifact authority."""

    if not execution.lineage or not execution.lineage[-1].effect_claim_ids:
        raise ValueError("terminal execution lineage has no accepted effect claim")
    claim_id = execution.lineage[-1].effect_claim_ids[0]
    settlement = await journal.get_settlement("reference-fixtures", claim_id)
    if settlement is None or settlement.status != "completed":
        raise ValueError("terminal operation has no accepted completed settlement")
    if (
        settlement.result_manifest_ref != execution.result_manifest_ref
        or settlement.result_manifest_digest != execution.result_manifest_digest
    ):
        raise ValueError("settled result manifest differs from execution lineage")
    payload = store.get_json(settlement.result_manifest_ref)
    if len(payload) != settlement.result_manifest_size_bytes:
        raise ValueError("settled result manifest size differs from artifact content")
    return reconstruct_typed_result(store, execution)


def classify_reference_fixture(
    fixture: QualiaFixtureInput | DaveFixtureInput,
) -> QualiaCatalogResult | DaveOwnershipResult:
    if isinstance(fixture, QualiaFixtureInput):
        sources = {source.source_id: source for source in fixture.sources}
        products: list[QualiaProductClaim] = []
        review: list[str] = []
        for candidate in fixture.candidates:
            authorities = {sources[source_id].authority for source_id in candidate.source_refs}
            accepted_authority = bool(authorities & set(fixture.source_authority_policy))
            if (
                not accepted_authority
                or candidate.canonical_product_url is None
                or (candidate.availability == "unknown")
                or candidate.seller != "Qualia Life"
            ):
                classification: Literal["included", "excluded", "unknown_requires_review"] = (
                    "unknown_requires_review"
                )
                reason = "insufficient_official_current_offer_evidence"
                confidence = 0.25
                review.append(candidate.record_id)
            elif candidate.historical:
                classification, reason, confidence = "excluded", "historical_product", 0.95
            elif candidate.item_kind == "bundle":
                classification, reason, confidence = "excluded", "bundle_excluded_by_policy", 0.95
            elif candidate.availability in {"out_of_stock", "unavailable"}:
                classification, reason, confidence = (
                    "excluded",
                    "unavailable_excluded_by_policy",
                    0.95,
                )
            elif candidate.item_kind == "supplement" and candidate.availability == "available":
                classification, reason, confidence = (
                    "included",
                    "official_current_supplement_offer",
                    0.99,
                )
            else:
                classification, reason, confidence = (
                    "excluded",
                    "non_supplement_excluded_by_policy",
                    0.9,
                )
            products.append(
                QualiaProductClaim(
                    record_id=candidate.record_id,
                    name=candidate.name,
                    canonical_product_url=candidate.canonical_product_url,
                    classification=classification,
                    reason_code=reason,
                    availability=candidate.availability,
                    observed_at=fixture.as_of,
                    evidence_refs=tuple(f"evidence:{item}" for item in candidate.source_refs),
                    confidence=confidence,
                )
            )
        return QualiaCatalogResult(
            as_of=fixture.as_of,
            products=tuple(products),
            review_required_record_ids=tuple(review),
        )
    companies: list[DaveCompanyClaim] = []
    review_ids: list[str] = []
    for company in fixture.companies:
        if company.explicit_current_control_evidence and company.asserted_relationship == (
            CompanyRelationshipClass.CURRENTLY_OWNS_OR_CONTROLS
        ):
            status: Literal["affirmed", "not_current", "unknown_requires_review"] = "affirmed"
            confidence = 0.95
            limitations: tuple[str, ...] = ()
        elif company.explicit_former_evidence or company.asserted_relationship == (
            CompanyRelationshipClass.FORMER_OR_HISTORICAL_ASSOCIATION
        ):
            status, confidence, limitations = "not_current", 0.9, ()
        else:
            status, confidence = "unknown_requires_review", 0.35
            limitations = (
                "No accepted affirmative current-control evidence; founding, affiliation, "
                "investing, advising, or public silence is not dispositive.",
            )
            review_ids.append(company.record_id)
        relationship = company.asserted_relationship
        if company.contrary_source_refs:
            relationship = CompanyRelationshipClass.CONFLICTING_OR_INSUFFICIENT_EVIDENCE
            status, confidence = "unknown_requires_review", 0.2
            if company.record_id not in review_ids:
                review_ids.append(company.record_id)
        companies.append(
            DaveCompanyClaim(
                record_id=company.record_id,
                company=company.company,
                relationship_class=relationship,
                current_status=status,
                observed_at=fixture.as_of,
                evidence_refs=tuple(f"evidence:{item}" for item in company.source_refs),
                contrary_evidence_refs=tuple(
                    f"evidence:{item}" for item in company.contrary_source_refs
                ),
                confidence=confidence,
                unresolved_limitations=limitations,
            )
        )
    return DaveOwnershipResult(
        as_of=fixture.as_of,
        companies=tuple(companies),
        review_required_record_ids=tuple(review_ids),
    )


async def _publish(
    service: ControlPlaneService, definition: Any, now: datetime
) -> PublishedDefinition:
    return await service.publish(
        PublishRequest(
            definition=definition,
            actor_id="reference-blueprint-publisher",
            published_at=now.astimezone(UTC),
            expected_head_revision=0,
        )
    )


def _content_ref(kind: str, logical_id: str) -> ContentAddressedRef:
    return ContentAddressedRef(
        kind=RuntimeDefinitionKind(kind),
        logical_id=logical_id,
        schema_version="reference-stage0-2.v1",
        digest=sha256_digest({"kind": kind, "logical_id": logical_id, "version": "1"}),
    )
