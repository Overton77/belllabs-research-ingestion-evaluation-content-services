from __future__ import annotations

from typing import Protocol

from pydantic import BaseModel, ConfigDict

from app.application.runtime_execution_bindings import (
    RuntimeExecutionBindingRepository,
    RuntimeInterventionRepository,
)
from app.application.runtime_repairs import PrivilegedRuntimeRepairService
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


class DenyByDefaultRuntimeInterventionAuthority:
    """Concrete fail-closed default for uncomposed production services."""

    async def current_version(
        self,
        request_scope: str,
        belllabs_run_id: str,
    ) -> int:
        del request_scope, belllabs_run_id
        raise PermissionError("runtime intervention authority is not configured")

    async def current_checkpoint(
        self,
        request_scope: str,
        belllabs_run_id: str,
        execution_epoch: int,
    ) -> LangGraphCheckpointKey | None:
        del request_scope, belllabs_run_id, execution_epoch
        raise PermissionError("runtime intervention authority is not configured")

    async def authorize_intervention(
        self,
        intervention: RuntimeIntervention,
    ) -> RuntimeInterventionAuthorization:
        return RuntimeInterventionAuthorization(
            command_id=intervention.command_id,
            request_scope=intervention.epoch.request_scope,
            actor_id=intervention.actor.actor_id,
            approved=False,
        )

    async def authorize_privileged_repair(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
    ) -> PrivilegedRepairAuthorization:
        return PrivilegedRepairAuthorization(
            command_id=intervention.command_id,
            repair_authorization_ref=intervention.repair_authorization_ref,
            operator_actor_id=intervention.actor.actor_id,
            approved=False,
        )


class RuntimeInterventionClient(Protocol):
    async def apply(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt: ...

    async def reconcile(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt | None: ...


class BoundRuntimeActionClient(Protocol):
    async def apply(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt: ...

    async def reconcile(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt | None: ...


class ExactRuntimeInterventionRouter:
    """Routes an accepted command only to its persisted endpoint/assistant/graph tuple."""

    def __init__(
        self,
        *,
        bindings: RuntimeExecutionBindingRepository,
        clients: dict[tuple[str, str, str, str], BoundRuntimeActionClient],
    ) -> None:
        self._bindings = bindings
        self._clients = clients

    async def apply(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt:
        binding = await self._bindings.get_binding(intervention.epoch)
        if binding is None or binding.binding_id != binding_id:
            raise LookupError("persisted runtime binding is unavailable for intervention")
        deployment = binding.deployment
        if deployment is None or binding.graph_id is None:
            raise ValueError("runtime intervention requires an exact Agent Server route")
        key = (
            deployment.deployment_endpoint_id,
            deployment.deployment_revision,
            deployment.assistant_id,
            binding.graph_id,
        )
        try:
            client = self._clients[key]
        except KeyError as error:
            raise LookupError(
                "no intervention client is registered for the exact persisted route"
            ) from error
        return await client.apply(intervention, binding_id=binding_id)

    async def reconcile(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> InterventionReceipt | None:
        binding = await self._bindings.get_binding(intervention.epoch)
        if binding is None or binding.binding_id != binding_id:
            raise LookupError("persisted runtime binding is unavailable for intervention")
        deployment = binding.deployment
        if deployment is None or binding.graph_id is None:
            raise ValueError("runtime intervention requires an exact Agent Server route")
        key = (
            deployment.deployment_endpoint_id,
            deployment.deployment_revision,
            deployment.assistant_id,
            binding.graph_id,
        )
        try:
            client = self._clients[key]
        except KeyError as error:
            raise LookupError(
                "no intervention client is registered for the exact persisted route"
            ) from error
        return await client.reconcile(intervention, binding_id=binding_id)


class RuntimeInterventionService:
    """Rechecks BellLabs authority before forwarding typed runtime interventions."""

    def __init__(
        self,
        *,
        bindings: RuntimeExecutionBindingRepository,
        commands: RuntimeInterventionRepository,
        client: RuntimeInterventionClient,
        authority: RuntimeInterventionAuthority | None = None,
        repair_service: PrivilegedRuntimeRepairService | None = None,
    ) -> None:
        self._bindings = bindings
        self._commands = commands
        self._authority = authority or DenyByDefaultRuntimeInterventionAuthority()
        self._client = client
        self._repair_service = repair_service

    async def apply(self, intervention: RuntimeIntervention) -> InterventionReceipt:
        pending_receipt: InterventionReceipt | None = None
        prior = await self._commands.get_intervention(
            intervention.epoch.request_scope,
            intervention.command_id,
        )
        if prior is not None:
            prior_intervention, receipt = prior
            if prior_intervention != intervention:
                raise ValueError("intervention command identity has conflicting intent")
            if receipt.status != "reconciliation_required":
                return receipt
            pending_receipt = receipt
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
            repair_authorization = await self._authority.authorize_privileged_repair(intervention)
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
            if self._repair_service is None:
                raise RuntimeError("privileged repair service is not configured")
        if pending_receipt is not None:
            if isinstance(intervention, PrivilegedOperatorReconcileIntervention):
                assert self._repair_service is not None
                reconciled = await self._repair_service.reconcile_reserved(
                    intervention,
                    binding,
                )
            else:
                reconciled = await self._client.reconcile(
                    intervention,
                    binding_id=binding.binding_id,
                )
            if reconciled is None:
                return pending_receipt
            self._validate_receipt(intervention, binding.binding_id, reconciled)
            return await self._commands.record(intervention, reconciled)
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
        if isinstance(intervention, PrivilegedOperatorReconcileIntervention):
            assert self._repair_service is not None
            receipt = await self._repair_service.apply_reserved(intervention, binding)
        else:
            receipt = await self._client.apply(
                intervention,
                binding_id=binding.binding_id,
            )
        self._validate_receipt(intervention, binding.binding_id, receipt)
        return await self._commands.record(intervention, receipt)

    @staticmethod
    def _validate_receipt(
        intervention: RuntimeIntervention,
        binding_id: str,
        receipt: InterventionReceipt,
    ) -> None:
        if receipt.command_id != intervention.command_id or receipt.binding_id != binding_id:
            raise ValueError("runtime intervention receipt does not match the command")
