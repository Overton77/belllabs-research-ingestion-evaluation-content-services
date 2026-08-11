from __future__ import annotations

from typing import Protocol

from langgraph.types import Overwrite
from pydantic import BaseModel, ConfigDict, Field

from app.application.runtime.runtime_reconciliation import RuntimeRepairAuditRecord
from app.domain.graph_runtime.contracts import (
    InterventionReceipt,
    PrivilegedOperatorReconcileIntervention,
    RuntimeExecutionBinding,
)
from app.domain.graph_runtime.identities import DIGEST_PATTERN


class PrivilegedRepairObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    before_digest: str = Field(pattern=DIGEST_PATTERN)
    after_digest: str = Field(pattern=DIGEST_PATTERN)


class PrivilegedRepairClient(Protocol):
    async def apply_overwrite(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
        binding: RuntimeExecutionBinding,
        overwrite: Overwrite,
    ) -> PrivilegedRepairObservation: ...

    async def reconcile_overwrite(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
        binding: RuntimeExecutionBinding,
    ) -> PrivilegedRepairObservation | None: ...


class RepairAuditRepository(Protocol):
    async def record_repair_audit(
        self,
        record: RuntimeRepairAuditRecord,
    ) -> RuntimeRepairAuditRecord: ...


class PrivilegedRuntimeRepairService:
    """Applies one compact Overwrite only after outer authority checks and reservation."""

    def __init__(
        self,
        *,
        client: PrivilegedRepairClient,
        audit: RepairAuditRepository,
    ) -> None:
        self._client = client
        self._audit = audit

    async def apply_reserved(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
        binding: RuntimeExecutionBinding,
    ) -> InterventionReceipt:
        observation = await self._client.apply_overwrite(
            intervention,
            binding,
            Overwrite(
                {
                    "command_id": intervention.command_id,
                    "reconciliation_action": intervention.reconciliation_action,
                    "evidence_refs": intervention.evidence_refs,
                }
            ),
        )
        return await self._audit_and_receipt(intervention, binding, observation)

    async def reconcile_reserved(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
        binding: RuntimeExecutionBinding,
    ) -> InterventionReceipt | None:
        observation = await self._client.reconcile_overwrite(intervention, binding)
        if observation is None:
            return None
        return await self._audit_and_receipt(intervention, binding, observation)

    async def _audit_and_receipt(
        self,
        intervention: PrivilegedOperatorReconcileIntervention,
        binding: RuntimeExecutionBinding,
        observation: PrivilegedRepairObservation,
    ) -> InterventionReceipt:
        assert intervention.expected_checkpoint is not None
        await self._audit.record_repair_audit(
            RuntimeRepairAuditRecord(
                request_scope=intervention.epoch.request_scope,
                audit_id=f"repair-{intervention.command_id}",
                command_id=intervention.command_id,
                actor_id=intervention.actor.actor_id,
                reason=intervention.reason,
                expected_belllabs_version=intervention.expected_belllabs_version,
                expected_checkpoint_id=(intervention.expected_checkpoint.langgraph_checkpoint_id),
                before_digest=observation.before_digest,
                after_digest=observation.after_digest,
                evidence_refs=intervention.evidence_refs,
                recorded_at=intervention.requested_at,
            )
        )
        return InterventionReceipt(
            command_id=intervention.command_id,
            status="accepted",
            binding_id=binding.binding_id,
            resulting_belllabs_version=intervention.expected_belllabs_version,
            reason_code="privileged_repair_applied_and_audited",
            recorded_at=intervention.requested_at,
        )
