from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, TypeAdapter

from app.agent_server.common_state import CommonStateMetadata
from app.application.operation_executor import OperationExecutionOutcome
from app.domain.graph_runtime.contracts import (
    BellLabsErrorEnvelope,
    BellLabsStreamEvent,
    BellLabsSuccessEnvelope,
    ContextReconstructionResult,
    DurableInterruptEnvelope,
    DurableInterruptResponse,
    ForkReceipt,
    ForkRequest,
    GraphExecutionReceipt,
    GraphExecutionSubmission,
    GraphRuntimeHealth,
    InterventionReceipt,
    RedactedCheckpointSummary,
    RuntimeAsyncTaskProjection,
    RuntimeExecutionProjection,
    RuntimeIntervention,
    SubagentContextSlice,
    SubagentResultManifest,
)
from app.domain.graph_runtime.definitions import (
    AgentHarnessDefinition,
    CapabilityManifestDefinition,
    ContextAssemblySpec,
    ContextPolicyDefinition,
    DelegationPolicyDefinition,
    EvaluationProfileDefinition,
    ExecutionEnvironmentDefinition,
    ExecutionLineageEnvelope,
    ExecutionResourceEnvelope,
    GraphAssemblyDefinition,
    GraphAssemblySpec,
    GraphAssemblySpecV2,
    InterpreterProfileDefinition,
    MCPServerDefinition,
    MiddlewareStackDefinition,
    OperationAssemblySpec,
    PromptContextBinding,
    RunPlan,
    RunPlanV3,
    SandboxProfileDefinition,
    StageCapabilityRequirement,
    StageExecutionBinding,
    UnavailableStageSurface,
)
from app.domain.graph_runtime.governance import field_governance_schema
from app.domain.graph_runtime.kernel import (
    CancellationContext,
    DecisionRequest,
    DecisionResponse,
    LineageParentEdge,
    ProviderQualifiedLineageRecord,
    ResourceLeaseRecord,
    ResourceLeaseRequest,
    WaitLeaseProjection,
)
from app.domain.operation_execution.journal import (
    OperationClaimResult,
    OperationEffectClaim,
    OperationJournalSettlement,
    OperationTechnicalAttempt,
)

router = APIRouter(prefix="/v2/graph-runtime", tags=["graph-runtime-contracts"])


def graph_runtime_contract_schemas() -> dict[str, object]:
    """Schema-export foundation for the BellLabs v2 runtime-neutral API."""

    models: dict[str, type[BaseModel]] = {
        "graph_execution_submission": GraphExecutionSubmission,
        "graph_execution_receipt": GraphExecutionReceipt,
        "runtime_execution_projection": RuntimeExecutionProjection,
        "intervention_receipt": InterventionReceipt,
        "durable_interrupt_envelope": DurableInterruptEnvelope,
        "durable_interrupt_response": DurableInterruptResponse,
        "runtime_async_task_projection": RuntimeAsyncTaskProjection,
        "stream_event": BellLabsStreamEvent,
        "fork_request": ForkRequest,
        "fork_receipt": ForkReceipt,
        "redacted_checkpoint_summary": RedactedCheckpointSummary,
        "runtime_health": GraphRuntimeHealth,
        "subagent_context_slice": SubagentContextSlice,
        "subagent_result_manifest": SubagentResultManifest,
        "context_reconstruction_result": ContextReconstructionResult,
        "error_envelope": BellLabsErrorEnvelope,
        "success_envelope": BellLabsSuccessEnvelope[dict[str, object]],
        "graph_assembly_definition": GraphAssemblyDefinition,
        "agent_harness_definition": AgentHarnessDefinition,
        "middleware_stack_definition": MiddlewareStackDefinition,
        "context_policy_definition": ContextPolicyDefinition,
        "context_assembly_spec": ContextAssemblySpec,
        "delegation_policy_definition": DelegationPolicyDefinition,
        "mcp_server_definition": MCPServerDefinition,
        "prompt_context_binding": PromptContextBinding,
        "interpreter_profile_definition": InterpreterProfileDefinition,
        "sandbox_profile_definition": SandboxProfileDefinition,
        "execution_environment_definition": ExecutionEnvironmentDefinition,
        "evaluation_profile_definition": EvaluationProfileDefinition,
        "capability_manifest_definition": CapabilityManifestDefinition,
        "graph_assembly_spec": GraphAssemblySpec,
        "run_plan": RunPlan,
        "stage_capability_requirement": StageCapabilityRequirement,
        "operation_assembly_spec": OperationAssemblySpec,
        "stage_execution_binding": StageExecutionBinding,
        "execution_resource_envelope": ExecutionResourceEnvelope,
        "execution_lineage_envelope": ExecutionLineageEnvelope,
        "unavailable_stage_surface": UnavailableStageSurface,
        "graph_assembly_spec_v2": GraphAssemblySpecV2,
        "run_plan_v3": RunPlanV3,
        "common_state_metadata": CommonStateMetadata,
        "decision_request": DecisionRequest,
        "decision_response": DecisionResponse,
        "provider_qualified_lineage_record": ProviderQualifiedLineageRecord,
        "lineage_parent_edge": LineageParentEdge,
        "resource_lease_request": ResourceLeaseRequest,
        "resource_lease_record": ResourceLeaseRecord,
        "wait_lease_projection": WaitLeaseProjection,
        "cancellation_context": CancellationContext,
        "operation_effect_claim": OperationEffectClaim,
        "operation_technical_attempt": OperationTechnicalAttempt,
        "operation_journal_settlement": OperationJournalSettlement,
        "operation_claim_result": OperationClaimResult,
    }
    schemas: dict[str, object] = {
        name: model.model_json_schema() for name, model in models.items()
    }
    schemas["runtime_intervention"] = TypeAdapter(RuntimeIntervention).json_schema()
    schemas["operation_execution_outcome"] = TypeAdapter(
        OperationExecutionOutcome
    ).json_schema()
    schemas["field_governance"] = field_governance_schema()
    return schemas


@router.get("/schemas")
async def get_graph_runtime_contract_schemas() -> dict[str, object]:
    return graph_runtime_contract_schemas()
