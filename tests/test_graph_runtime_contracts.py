from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.api.graph_runtime_schemas import graph_runtime_contract_schemas
from app.application.mongo_operation_authority_migration import select_authority_version
from app.application.runtime_run_plan import compile_structural_graph_assembly
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import StageGraphBlueprint, StageNode
from app.domain.graph_runtime.contracts import ProviderNeutralAttemptMetadata
from app.domain.graph_runtime.definitions import (
    CapabilityManifestDefinition,
    CapabilityMaturityRecord,
    ContentAddressedRef,
    DelegationModePolicy,
    DelegationPolicyDefinition,
    GraphAssemblyDefinition,
    MiddlewareBinding,
    MiddlewareStackDefinition,
    OperationAssemblySpec,
    RuntimeDefinitionKind,
    StageCapabilityRequirement,
    StageExecutionBinding,
)
from app.domain.graph_runtime.governance import (
    build_field_governance,
    validate_field_governance,
)
from app.domain.graph_runtime.identities import (
    AgentThreadKey,
    ExecutionEpochKey,
    RuntimeTransportAttemptKey,
    SemanticOperationAttemptKey,
)

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 5, 20, 0, tzinfo=UTC)


def ref(kind: RuntimeDefinitionKind, name: str, digest: str = DIGEST) -> ContentAddressedRef:
    return ContentAddressedRef(
        kind=kind,
        logical_id=name,
        schema_version="1",
        digest=digest,
    )


def test_provider_qualified_identity_grammar_rejects_ambiguous_children() -> None:
    epoch = ExecutionEpochKey(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        execution_epoch=2,
    )
    assert epoch.canonical_key.endswith("run:run-1:execution-epoch:2")
    attempt = SemanticOperationAttemptKey(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        operation_id="search",
        semantic_attempt=1,
        stage_id="research",
        stage_cycle=0,
    )
    assert ":stage:research:cycle:0:" in attempt.canonical_key

    with pytest.raises(ValidationError, match="child threads require"):
        AgentThreadKey(
            **epoch.model_dump(),
            agent_server_thread_id="thread-1",
            relationship="async_subagent",
        )
    with pytest.raises(ValidationError, match="both stage and goal"):
        SemanticOperationAttemptKey(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            operation_id="search",
            semantic_attempt=1,
            stage_id="research",
            stage_cycle=0,
            goal_iteration=1,
        )


def test_middleware_duplicates_conflicts_and_unknown_fields_are_rejected() -> None:
    first = MiddlewareBinding(
        middleware_id="planning.primary",
        implementation_ref=ref(RuntimeDefinitionKind.MIDDLEWARE_STACK, "planning"),
        phase="before_agent",
        core_capability="planning",
        configuration_digest=DIGEST,
    )
    with pytest.raises(ValidationError, match="core middleware"):
        MiddlewareStackDefinition(
            logical_id="middleware.test",
            title="Test",
            description="Test stack",
            ordered_middleware=(
                first,
                first.model_copy(update={"middleware_id": "planning.secondary"}),
            ),
        )
    with pytest.raises(ValidationError, match="Extra inputs"):
        MiddlewareBinding.model_validate(
            {
                **first.model_dump(mode="json"),
                "run_id": "ambiguous-and-forbidden",
            }
        )


def test_delegation_modes_and_feature_maturity_are_independent_and_default_off() -> None:
    policies = tuple(
        DelegationModePolicy(
            mode=mode,
            enabled=False,
            maturity=maturity,
            fallback_mode="linked_run" if mode != "linked_run" else "reject",
            max_concurrency=0,
            max_depth=0,
            capacity_policy_ref=f"capacity:{mode}@1",
            result_admission_policy_ref=f"result:{mode}@1",
        )
        for mode, maturity in (
            ("sync_subagent", "stable"),
            ("dynamic_interpreter", "beta"),
            ("async_subagent", "preview"),
            ("linked_run", "stable"),
        )
    )
    definition = DelegationPolicyDefinition(
        logical_id="delegation.default-off",
        title="Default-off delegation",
        description="Distinct delegation tracks",
        continuity_mode="bounded_context_slice",
        modes=policies,
    )
    assert len(definition.modes) == 4
    with pytest.raises(ValidationError, match="all four"):
        definition.model_copy(update={"modes": policies[:-1]}).model_validate(
            definition.model_copy(update={"modes": policies[:-1]}).model_dump()
        )

    manifest = CapabilityManifestDefinition(
        logical_id="capabilities.stage1",
        title="Stage 1 capability posture",
        description="Migration features remain disabled",
        capabilities=(
            CapabilityMaturityRecord(
                capability_id="deepagents_async_subagents",
                maturity="preview",
                required_for_migration=True,
                feature_flag="DEEPAGENTS_ASYNC_SUBAGENTS_ENABLED",
                enabled=False,
                fallback="linked_belllabs_run_or_sync_subagent",
                promotion_gate="Stage 6 lifecycle proof",
            ),
        ),
    )
    assert not manifest.capabilities[0].enabled


def test_graph_assembly_digest_field_governance_and_schema_export() -> None:
    assembly_values = {
        "schema_version": "belllabs.graph-runtime.v1",
        "logical_id": "graph.stagegraph.v1",
        "title": "StageGraph assembly",
        "description": "Frozen runtime assembly",
        "kind": RuntimeDefinitionKind.GRAPH_ASSEMBLY,
        "graph_factory_ref": ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "factory"),
        "state_schema_ref": ref(RuntimeDefinitionKind.STATE_SCHEMA, "state"),
        "reducer_registry_ref": ref(RuntimeDefinitionKind.REDUCER_REGISTRY, "reducers"),
        "operation_registry_ref": ref(RuntimeDefinitionKind.OPERATION_REGISTRY, "operations"),
        "harness_ref": ref(RuntimeDefinitionKind.AGENT_HARNESS, "harness"),
        "context_policy_ref": ref(RuntimeDefinitionKind.CONTEXT_POLICY, "context"),
        "delegation_policy_ref": ref(RuntimeDefinitionKind.DELEGATION_POLICY, "delegation"),
        "execution_environment_ref": ref(
            RuntimeDefinitionKind.EXECUTION_ENVIRONMENT, "environment"
        ),
        "evaluation_profile_ref": ref(
            RuntimeDefinitionKind.EVALUATION_PROFILE, "evaluation"
        ),
        "capability_manifest_ref": ref(
            RuntimeDefinitionKind.CAPABILITY_MANIFEST, "capabilities"
        ),
        "checkpoint_compatibility_key": "stagegraph-state-v1",
        "prohibited_state_fields": frozenset(
            {"secrets", "credentials", "checkpoint_body", "raw_private_corpus"}
        ),
        "maximum_state_bytes": 1_000_000,
    }
    assembly = GraphAssemblyDefinition.create(
        **assembly_values,
        graph_family="StageGraph",
        graph_id="stagegraph",
    )
    rebuilt = GraphAssemblyDefinition.create(
        **assembly_values,
        graph_family="StageGraph",
        graph_id="stagegraph",
    )
    assert assembly.graph.graph_assembly_digest == rebuilt.graph.graph_assembly_digest

    appendix = build_field_governance()
    validate_field_governance(appendix)
    schemas = graph_runtime_contract_schemas()
    assert "runtime_intervention" in schemas
    assert "field_governance" in schemas


def test_provider_neutral_attempt_prevents_exactly_once_illusion() -> None:
    with pytest.raises(ValidationError, match="cannot be blindly retried"):
        ProviderNeutralAttemptMetadata(
            attempt_key=RuntimeTransportAttemptKey(
                request_scope="tenant-1",
                belllabs_run_id="run-1",
                execution_epoch=1,
                runtime_attempt=1,
                submission_id="submission-1",
            ),
            provider="external-provider",
            idempotency_supported=False,
            consequential=True,
            retry_class="safe",
        )


def test_mongo_authority_dual_read_and_rollback_window_are_explicit() -> None:
    assert (
        select_authority_version(
            requested_schema_version=None,
            v2_available=True,
            rollback_window_open=True,
        )
        == "v2"
    )
    assert (
        select_authority_version(
            requested_schema_version="1",
            v2_available=True,
            rollback_window_open=True,
        )
        == "legacy"
    )
    with pytest.raises(ValueError, match="rollback window"):
        select_authority_version(
            requested_schema_version="1",
            v2_available=True,
            rollback_window_open=False,
        )


def test_v2_structural_compiler_requires_exact_coverage_and_reports_disabled_surfaces() -> None:
    implementation_ref = ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "operation.collect")
    manifest_ref = ref(RuntimeDefinitionKind.CAPABILITY_MANIFEST, "disabled.manifest")
    assembly = OperationAssemblySpec.create(
        operation_assembly_id="assembly.collect.v1",
        operation_contract_ref="operation:collect@1",
        implementation_kind="native",
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
        resource_envelope_ref=manifest_ref,
        effect_policy_ref=manifest_ref,
        fallback_policy_ref=manifest_ref,
        trace_redaction_policy_ref=manifest_ref,
        capability_manifest_ref=manifest_ref,
        compatibility_manifest_ref=manifest_ref,
    )
    requirement = StageCapabilityRequirement(
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
    binding = StageExecutionBinding(
        stage_id="collect",
        stage_requirement_ref=manifest_ref.model_copy(
            update={"digest": sha256_digest(requirement.model_dump(mode="json"))}
        ),
        operation_assembly_ref=implementation_ref.model_copy(
            update={"digest": assembly.operation_assembly_digest}
        ),
        operation_assembly_digest=assembly.operation_assembly_digest,
        input_projection_ref="projection:input@1",
        output_projection_ref="projection:output@1",
        resource_envelope_ref=manifest_ref,
        compatibility_key="stagegraph-v2",
    )
    blueprint = StageGraphBlueprint(
        logical_id="blueprint.collect",
        title="Collect",
        description="One exact stage",
        stages=(StageNode(stage_id="collect"),),
    )
    compiled, unavailable = compile_structural_graph_assembly(
        blueprint=blueprint,
        graph_assembly_ref=ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "stagegraph.v2"),
        state_schema_digest=DIGEST,
        reducer_registry_digest=DIGEST,
        operation_registry_digest=DIGEST,
        requirements=(requirement,),
        bindings=(binding,),
        assemblies={binding.operation_assembly_ref.logical_id: assembly},
        allowed_capability_ids=frozenset({"literature_search"}),
        disabled_capability_ids=frozenset({"literature_search"}),
        compatibility_manifest_digest=DIGEST,
    )
    assert compiled.schema_version == "belllabs.graph-assembly-spec.v2"
    assert unavailable == ("collect:default:literature_search",)
    with pytest.raises(ValueError, match="cover every declared"):
        compile_structural_graph_assembly(
            blueprint=blueprint,
            graph_assembly_ref=ref(RuntimeDefinitionKind.GRAPH_ASSEMBLY, "stagegraph.v2"),
            state_schema_digest=DIGEST,
            reducer_registry_digest=DIGEST,
            operation_registry_digest=DIGEST,
            requirements=(),
            bindings=(),
            assemblies={},
            allowed_capability_ids=frozenset(),
            compatibility_manifest_digest=DIGEST,
        )
