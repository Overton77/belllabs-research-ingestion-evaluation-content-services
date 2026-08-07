from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from pydantic import BaseModel, ConfigDict

from app.domain.graph_runtime import contracts, definitions, identities
from app.domain.operation_execution import journal


class FieldGovernance(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    writer: str
    readers: tuple[str, ...]
    authority_class: Literal[
        "domain_authority",
        "runtime_fact",
        "immutable_definition",
        "derived_projection",
        "evidence",
    ]
    mutation_rule: Literal[
        "immutable",
        "append_only",
        "optimistic_version",
        "replace_derived",
    ]
    retention: str
    sensitivity: Literal["public", "internal", "sensitive_ref", "redacted"]
    trace_policy: Literal["include", "redact", "digest_only", "exclude"]
    compatibility_behavior: Literal[
        "exact",
        "additive_optional",
        "versioned_migration",
        "derived_rebuild",
    ]


_IDENTITY_MODELS: tuple[type[BaseModel], ...] = (
    identities.BellLabsRunKey,
    identities.ExecutionEpochKey,
    identities.GraphIdentity,
    identities.DeploymentIdentity,
    identities.AgentThreadKey,
    identities.AgentRunKey,
    identities.LangGraphCheckpointKey,
    identities.GoalHandoffCheckpointKey,
    identities.SemanticOperationAttemptKey,
    identities.RuntimeTransportAttemptKey,
    identities.SubagentProfileKey,
    identities.AsyncTaskKey,
    identities.LinkedBellLabsRunKey,
)

_DEFINITION_MODELS: tuple[type[BaseModel], ...] = (
    definitions.ContentAddressedRef,
    definitions.MiddlewareBinding,
    definitions.MiddlewareStackDefinition,
    definitions.AgentHarnessDefinition,
    definitions.ContextSourceRule,
    definitions.ContextPolicyDefinition,
    definitions.ContextManifestEntry,
    definitions.ContextAssemblySpec,
    definitions.SyncDictionarySubagent,
    definitions.SyncCompiledGraphSubagent,
    definitions.AsyncSubagentDefinition,
    definitions.DelegationModePolicy,
    definitions.DelegationPolicyDefinition,
    definitions.MCPServerDefinition,
    definitions.PromptContextBinding,
    definitions.InterpreterProfileDefinition,
    definitions.SandboxSnapshotPolicy,
    definitions.SandboxProfileDefinition,
    definitions.ExecutionEnvironmentDefinition,
    definitions.EvaluationProfileDefinition,
    definitions.CapabilityMaturityRecord,
    definitions.CapabilityManifestDefinition,
    definitions.GraphAssemblyDefinition,
    definitions.NativeOperationImplementation,
    definitions.HarnessOperationImplementation,
    definitions.CompiledGraphOperationImplementation,
    definitions.StageImplementationBinding,
    definitions.GraphAssemblySpec,
    definitions.RunPlan,
    definitions.StageCapabilityRequirement,
    definitions.OperationAssemblySpec,
    definitions.StageExecutionBinding,
    definitions.ExecutionResourceEnvelope,
    definitions.ExecutionLineageEnvelope,
    definitions.UnavailableStageSurface,
    definitions.GraphAssemblySpecV2,
    definitions.RunPlanV3,
)

_RUNTIME_MODELS: tuple[type[BaseModel], ...] = (
    contracts.ActorRef,
    contracts.Correlation,
    contracts.GraphExecutionSubmission,
    contracts.GraphExecutionReceipt,
    contracts.RuntimeExecutionBinding,
    contracts.RuntimeExecutionAttempt,
    contracts.RuntimeExecutionProjection,
    contracts.AppendInputIntervention,
    contracts.SatisfyWaitIntervention,
    contracts.ResumePauseIntervention,
    contracts.RespondToInterruptIntervention,
    contracts.UpdateAsyncTaskIntervention,
    contracts.CancelAsyncTaskIntervention,
    contracts.CancelRunIntervention,
    contracts.ForkFromCheckpointIntervention,
    contracts.PrivilegedOperatorReconcileIntervention,
    contracts.InterventionReceipt,
    contracts.DurableInterruptEnvelope,
    contracts.DurableInterruptResponse,
    contracts.RuntimeAsyncTaskProjection,
    contracts.BellLabsStreamEvent,
    contracts.ForkRequest,
    contracts.ForkReceipt,
    contracts.RedactedCheckpointSummary,
    contracts.RuntimeCapabilityReadiness,
    contracts.GraphRuntimeHealth,
    contracts.ProviderNeutralAttemptMetadata,
    contracts.SubagentContextSlice,
    contracts.SubagentResultManifest,
    contracts.ContextReconstructionResult,
    contracts.GoalHandoffReference,
    contracts.BellLabsSuccessEnvelope,
    contracts.BellLabsErrorDetail,
    contracts.BellLabsErrorEnvelope,
    journal.OperationEffectClaim,
    journal.OperationTechnicalAttempt,
    journal.OperationJournalSettlement,
    journal.OperationClaimResult,
)

GOVERNED_MODELS = _IDENTITY_MODELS + _DEFINITION_MODELS + _RUNTIME_MODELS


def build_field_governance(
    models: Iterable[type[BaseModel]] = GOVERNED_MODELS,
) -> dict[str, FieldGovernance]:
    appendix: dict[str, FieldGovernance] = {}
    for model in models:
        for field_name in model.model_fields:
            key = f"{model.__name__}.{field_name}"
            appendix[key] = _policy_for(model, field_name)
    return appendix


def validate_field_governance(
    appendix: dict[str, FieldGovernance],
    models: Iterable[type[BaseModel]] = GOVERNED_MODELS,
) -> None:
    expected = {
        f"{model.__name__}.{field_name}"
        for model in models
        for field_name in model.model_fields
    }
    missing = expected - appendix.keys()
    unknown = appendix.keys() - expected
    if missing or unknown:
        raise ValueError(
            f"field governance mismatch; missing={sorted(missing)}, unknown={sorted(unknown)}"
        )


def field_governance_schema() -> dict[str, object]:
    appendix = build_field_governance()
    validate_field_governance(appendix)
    return {
        "schema_version": "belllabs.field-governance.v1",
        "fields": {
            key: value.model_dump(mode="json")
            for key, value in sorted(appendix.items())
        },
    }


def _policy_for(model: type[BaseModel], field_name: str) -> FieldGovernance:
    definition = model in _DEFINITION_MODELS
    identity = model in _IDENTITY_MODELS
    projection = model.__name__.endswith(("Projection", "Receipt", "Result", "Health"))
    payload = "payload" in field_name or "summary" in field_name
    secret_ref = "secret" in field_name or "credential" in field_name
    digest = "digest" in field_name

    if definition:
        authority_class = "immutable_definition"
        writer = "control_plane_compiler"
        mutation_rule = "immutable"
        compatibility = "versioned_migration"
    elif identity:
        authority_class = "domain_authority"
        writer = "belllabs_control_plane"
        mutation_rule = "immutable"
        compatibility = "exact"
    elif projection:
        authority_class = "derived_projection"
        writer = "runtime_projection_service"
        mutation_rule = "replace_derived"
        compatibility = "derived_rebuild"
    else:
        authority_class = "runtime_fact"
        writer = "runtime_coordination_service"
        mutation_rule = (
            "optimistic_version"
            if field_name in {"status", "version", "updated_at"}
            else "append_only"
        )
        compatibility = "additive_optional"

    return FieldGovernance(
        writer=writer,
        readers=("belllabs_control_plane", "runtime_adapter", "operations"),
        authority_class=authority_class,
        mutation_rule=mutation_rule,
        retention="policy_ref:runtime-contract-default",
        sensitivity="sensitive_ref" if secret_ref else "redacted" if payload else "internal",
        trace_policy=(
            "exclude"
            if secret_ref
            else "digest_only"
            if payload or digest
            else "include"
        ),
        compatibility_behavior=compatibility,
    )
