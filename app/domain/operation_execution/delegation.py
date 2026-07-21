from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from app.domain.control_plane.contracts import ExactDefinitionRef
from app.domain.operation_execution.contracts import (
    DelegationBinding,
    OperationExecutionBinding,
)


class DelegationAdmission(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: Literal["accepted", "linked_run_required", "rejected"]
    reason_code: str
    target_workflow_type_ref: ExactDefinitionRef | None = None


class LinkedRunRequired(ValueError):
    """Operation-local delegation attempted to cross a Workflow Type boundary."""

    def __init__(self, admission: DelegationAdmission) -> None:
        self.admission = admission
        super().__init__(admission.reason_code)


def admit_delegation(
    binding: OperationExecutionBinding,
    delegation: DelegationBinding,
) -> DelegationAdmission:
    """Recheck immutable delegation bounds before constructing any SDK agent."""
    agent = delegation.agent
    if agent.requested_workflow_type_ref is not None:
        return DelegationAdmission(
            outcome="linked_run_required",
            reason_code="recognized_workflow_type_requires_linked_run",
            target_workflow_type_ref=agent.requested_workflow_type_ref,
        )
    ceiling = binding.delegation_ceiling
    if delegation.mode not in ceiling.allowed_modes:
        return DelegationAdmission(outcome="rejected", reason_code="mode_exceeds_ceiling")
    if agent.model_policy.model not in ceiling.allowed_models:
        return DelegationAdmission(outcome="rejected", reason_code="model_exceeds_ceiling")
    if not agent.capability_grant.capabilities <= binding.capability_grant.capabilities:
        return DelegationAdmission(outcome="rejected", reason_code="authority_exceeds_parent")
    if not agent.capability_grant.tool_ids <= (
        binding.capability_grant.tool_ids & ceiling.tool_ids
    ):
        return DelegationAdmission(outcome="rejected", reason_code="tools_exceed_ceiling")
    if not agent.capability_grant.mcp_server_ids <= (
        binding.capability_grant.mcp_server_ids & ceiling.mcp_server_ids
    ):
        return DelegationAdmission(outcome="rejected", reason_code="mcp_exceeds_ceiling")
    if not agent.capability_grant.data_scope_refs <= (
        binding.capability_grant.data_scope_refs & ceiling.data_scope_refs
    ):
        return DelegationAdmission(outcome="rejected", reason_code="data_exceeds_ceiling")
    if not agent.capability_grant.network_hosts <= (
        binding.capability_grant.network_hosts & ceiling.network_hosts
    ):
        return DelegationAdmission(outcome="rejected", reason_code="network_exceeds_ceiling")
    if any(
        value > binding.budget_limits.get(dimension, 0)
        or value > ceiling.budget_limits.get(dimension, 0)
        for dimension, value in delegation.budget_limits.items()
    ):
        return DelegationAdmission(outcome="rejected", reason_code="budget_exceeds_ceiling")
    return DelegationAdmission(outcome="accepted", reason_code="operation_local")
