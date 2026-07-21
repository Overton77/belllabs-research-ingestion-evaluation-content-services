from __future__ import annotations

from app.application.operation_execution import _binding_for
from app.domain.control_plane.canonical import sha256_digest
from app.domain.control_plane.contracts import DefinitionKind
from app.domain.operation_execution.contracts import (
    AgentDefinition,
    CapabilityGrant,
    DelegationBinding,
    DelegationCeiling,
    ModelPolicy,
    OperationExecutionRequest,
)
from app.domain.operation_execution.delegation import admit_delegation
from tests.test_operation_execution import exact, operation_request


def _delegation(*, workflow_boundary: bool = False) -> DelegationBinding:
    return DelegationBinding(
        mode="task_subagent",
        agent=AgentDefinition(
            definition_id="specialist@1",
            revision=1,
            name="Specialist",
            description="Perform one bounded specialist task.",
            instructions="Use only the capabilities in this immutable definition.",
            model_policy=ModelPolicy(
                provider="openai",
                model="gpt-5-mini",
                max_turns=1,
            ),
            capability_grant=CapabilityGrant(
                capabilities=frozenset({"model.invoke"}),
            ),
            requested_workflow_type_ref=(
                exact(DefinitionKind.WORKFLOW_TYPE, "deep-research") if workflow_boundary else None
            ),
        ),
        tool_name="run_specialist",
        tool_description="Run the bounded specialist.",
        budget_limits={"model.turns": 1},
        child_workspace_id="workspace:delegate:specialist",
        child_namespace_id="namespace:delegate:specialist",
    )


def _request(delegation: DelegationBinding) -> OperationExecutionRequest:
    base = operation_request()
    candidate = base.model_copy(
        update={
            "capability_grant": base.capability_grant.model_copy(
                update={"capabilities": frozenset({"model.invoke", "mcp.call"})}
            ),
            "delegations": (delegation,),
            "delegation_ceiling": DelegationCeiling(
                allowed_modes=frozenset({"task_subagent"}),
                max_depth=1,
                max_concurrency=1,
                max_delegations=1,
                allowed_models=frozenset({"gpt-5-mini"}),
                budget_limits={"model.turns": 1},
            ),
        }
    )
    return OperationExecutionRequest.model_validate(candidate.model_dump(mode="python"))


def test_task_subagent_is_admitted_inside_intersected_operation_authority() -> None:
    request = _request(_delegation())
    binding = _binding_for(
        request,
        sha256_digest(request.model_dump(mode="json", exclude={"requested_at"})),
    )

    admission = admit_delegation(binding, request.delegations[0])

    assert admission.outcome == "accepted"
    assert admission.reason_code == "operation_local"


def test_recognized_workflow_type_is_routed_to_linked_run() -> None:
    request = _request(_delegation(workflow_boundary=True))
    binding = _binding_for(
        request,
        sha256_digest(request.model_dump(mode="json", exclude={"requested_at"})),
    )

    admission = admit_delegation(binding, request.delegations[0])

    assert admission.outcome == "linked_run_required"
    assert admission.target_workflow_type_ref is not None
    assert admission.target_workflow_type_ref.logical_id == "deep-research"
