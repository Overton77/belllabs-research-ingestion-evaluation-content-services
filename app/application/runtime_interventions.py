from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict

from app.application.runtime_execution_bindings import (
    RuntimeExecutionBindingRepository,
    RuntimeInterventionRepository,
)
from app.domain.graph_runtime.contracts import (
    InterventionReceipt,
    PrivilegedOperatorReconcileIntervention,
    RuntimeIntervention,
)
from app.domain.graph_runtime.identities import LangGraphCheckpointKey


class PrivilegedRepairAuthorization(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    command_id: str
    repair_authorization_ref: str
    operator_actor_id: str
    approved: bool


class RuntimeInterventionAuthorization(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    command_id: str
    request_scope: str
    actor_id: str
    approved: bool


class RuntimeInterventionAuthority(Protocol):
    async def current_version(
        self,
        request_scope: str,
        belllabs_run_id: str,
    ) -> int: ...

    async def current_checkpoint(
        self,
        request_scope: str,
        belllabs_run_id: str,
        execution_epoch: int,
    ) -> LangGraphCheckpointKey | None: ...

    async def authorize_intervention(
        self,
        intervention: RuntimeIntervention,
    ) -> RuntimeInterventionAuthorization: ...

    async def authorize_privileged_repair(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
    ) -> PrivilegedRepairAuthorization: ...


class RuntimeInterventionClient(Protocol):
    async def apply(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt: ...


class RuntimeInterventionService:
    """Rechecks BellLabs authority before forwarding typed runtime interventions."""

    def __init__(
        self,
        *,
        bindings: RuntimeExecutionBindingRepository,
        commands: RuntimeInterventionRepository,
        authority: RuntimeInterventionAuthority,
        client: RuntimeInterventionClient,
    ) -> None:
        self._bindings = bindings
        self._commands = commands
        self._authority = authority
        self._client = client

    async def apply(self, intervention: RuntimeIntervention) -> InterventionReceipt:
        prior = await self._commands.get_intervention(
            intervention.epoch.request_scope,
            intervention.command_id,
        )
        if prior is not None:
            prior_intervention, receipt = prior
            if prior_intervention != intervention:
                raise ValueError("intervention command identity has conflicting intent")
            return receipt
        binding = await self._bindings.get_binding(intervention.epoch)
        if binding is None:
            raise LookupError("runtime binding not found for intervention epoch")
        current_version = await self._authority.current_version(
            intervention.epoch.request_scope,
            intervention.epoch.belllabs_run_id,
        )
        if current_version != intervention.expected_belllabs_version:
            raise ValueError("intervention expected BellLabs version is stale")
        authorization = await self._authority.authorize_intervention(intervention)
        if (
            not isinstance(authorization, RuntimeInterventionAuthorization)
            or not authorization.approved
            or authorization.command_id != intervention.command_id
            or authorization.request_scope != intervention.epoch.request_scope
            or authorization.actor_id != intervention.actor.actor_id
        ):
            raise PermissionError(
                "runtime intervention lacks matching scope and actor authorization"
            )
        if intervention.expected_checkpoint is not None:
            current_checkpoint = await self._authority.current_checkpoint(
                intervention.epoch.request_scope,
                intervention.epoch.belllabs_run_id,
                intervention.epoch.execution_epoch,
            )
            if current_checkpoint != intervention.expected_checkpoint:
                raise ValueError("intervention expected LangGraph checkpoint is stale")
        if isinstance(intervention, PrivilegedOperatorReconcileIntervention):
            repair_authorization = await self._authority.authorize_privileged_repair(
                intervention
            )
            if (
                not isinstance(repair_authorization, PrivilegedRepairAuthorization)
                or not repair_authorization.approved
                or repair_authorization.command_id != intervention.command_id
                or repair_authorization.repair_authorization_ref
                != intervention.repair_authorization_ref
                or repair_authorization.operator_actor_id != intervention.actor.actor_id
            ):
                raise PermissionError(
                    "privileged reconciliation lacks a matching explicit approval"
                )
        created = await self._commands.reserve(
            intervention,
            binding_id=binding.binding_id,
        )
        if not created:
            prior = await self._commands.get_intervention(
                intervention.epoch.request_scope,
                intervention.command_id,
            )
            if prior is None:
                raise RuntimeError("reserved intervention is unavailable for reconciliation")
            return prior[1]
        receipt = await self._client.apply(intervention, binding_id=binding.binding_id)
        if (
            receipt.command_id != intervention.command_id
            or receipt.binding_id != binding.binding_id
        ):
            raise ValueError("runtime intervention receipt does not match the command")
        return await self._commands.record(intervention, receipt)
