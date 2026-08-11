from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.domain.graph_runtime.contracts import RuntimeExecutionBinding
from app.domain.graph_runtime.identities import DIGEST_PATTERN, ExecutionEpochKey


class BootstrapContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AuthoritativeRuntimeProjection(BootstrapContract):
    binding: RuntimeExecutionBinding
    lifecycle_version: int = Field(ge=1)
    lifecycle_projection_ref: str = Field(min_length=1)
    lifecycle_projection_digest: str = Field(pattern=DIGEST_PATTERN)
    budget_projection_ref: str = Field(min_length=1)
    decision_projection_ref: str = Field(min_length=1)
    cancellation_requested: bool = False
    terminal: bool = False


class CheckpointRuntimeProjection(BootstrapContract):
    binding_id: str = Field(min_length=1)
    binding_version: int = Field(ge=1)
    lifecycle_version: int = Field(ge=1)
    lifecycle_projection_digest: str = Field(pattern=DIGEST_PATTERN)
    run_plan_digest: str = Field(pattern=DIGEST_PATTERN)
    graph_assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    deployment_endpoint_id: str | None = None
    deployment_revision: str | None = None
    graph_id: str | None = None


class BootstrapRequest(BootstrapContract):
    epoch: ExecutionEpochKey
    runtime_binding_ref: str = Field(min_length=1)
    run_plan_digest: str = Field(pattern=DIGEST_PATTERN)
    graph_assembly_digest: str = Field(pattern=DIGEST_PATTERN)
    state_schema_digest: str = Field(pattern=DIGEST_PATTERN)
    checkpoint_projection: CheckpointRuntimeProjection | None = None


class BootstrapDecision(BootstrapContract):
    action: Literal["ready", "rebuild_projection", "interrupt_for_reconciliation"]
    binding_id: str = Field(min_length=1)
    binding_version: int = Field(ge=1)
    lifecycle_version: int = Field(ge=1)
    lifecycle_projection_ref: str = Field(min_length=1)
    lifecycle_projection_digest: str = Field(pattern=DIGEST_PATTERN)
    budget_projection_ref: str = Field(min_length=1)
    decision_projection_ref: str = Field(min_length=1)
    cancellation_requested: bool
    terminal: bool
    reason_code: str = Field(min_length=1)
    decision_id: str | None = Field(default=None, min_length=1)


class BootstrapAuthority(Protocol):
    async def load(self, epoch: ExecutionEpochKey) -> AuthoritativeRuntimeProjection: ...


class BootstrapDecisionBridge(Protocol):
    async def persist_reconciliation_decision(
        self,
        request: BootstrapRequest,
        current: AuthoritativeRuntimeProjection,
        reason_code: str,
    ) -> str: ...


class RuntimeBootstrapReconciler:
    """First-node authority check; provider/checkpoint status never grants advancement."""

    def __init__(
        self,
        authority: BootstrapAuthority,
        decision_bridge: BootstrapDecisionBridge | None = None,
    ) -> None:
        self._authority = authority
        self._decision_bridge = decision_bridge

    async def reconcile(self, request: BootstrapRequest) -> BootstrapDecision:
        current = await self._authority.load(request.epoch)
        binding = current.binding
        if binding.epoch != request.epoch or binding.binding_id != request.runtime_binding_ref:
            return await self._interrupt(
                request,
                current,
                "runtime_binding_identity_mismatch",
            )
        if (
            binding.run_plan_digest != request.run_plan_digest
            or binding.graph_assembly_digest != request.graph_assembly_digest
            or binding.state_schema_digest != request.state_schema_digest
        ):
            return await self._interrupt(request, current, "frozen_runtime_digest_mismatch")
        checkpoint = request.checkpoint_projection
        if checkpoint is None:
            return self._decision(current, "rebuild_projection", "checkpoint_projection_missing")
        if not self._checkpoint_route_matches(checkpoint, binding):
            return await self._interrupt(request, current, "checkpoint_route_incompatible")
        if checkpoint.binding_version > binding.version:
            return await self._interrupt(
                request,
                current,
                "checkpoint_binding_version_ahead",
            )
        if checkpoint.lifecycle_version > current.lifecycle_version:
            return await self._interrupt(
                request,
                current,
                "checkpoint_lifecycle_version_ahead",
            )
        if (
            checkpoint.binding_version < binding.version
            or checkpoint.lifecycle_version < current.lifecycle_version
            or checkpoint.lifecycle_projection_digest != current.lifecycle_projection_digest
        ):
            return self._decision(current, "rebuild_projection", "checkpoint_projection_stale")
        return self._decision(current, "ready", "authoritative_projection_matches")

    @staticmethod
    def _checkpoint_route_matches(
        checkpoint: CheckpointRuntimeProjection,
        binding: RuntimeExecutionBinding,
    ) -> bool:
        deployment = binding.deployment
        return (
            checkpoint.binding_id == binding.binding_id
            and checkpoint.run_plan_digest == binding.run_plan_digest
            and checkpoint.graph_assembly_digest == binding.graph_assembly_digest
            and checkpoint.state_schema_digest == binding.state_schema_digest
            and checkpoint.deployment_endpoint_id
            == (deployment.deployment_endpoint_id if deployment else None)
            and checkpoint.deployment_revision
            == (deployment.deployment_revision if deployment else None)
            and checkpoint.graph_id == binding.graph_id
        )

    @staticmethod
    def _decision(
        current: AuthoritativeRuntimeProjection,
        action: Literal["ready", "rebuild_projection"],
        reason_code: str,
    ) -> BootstrapDecision:
        return BootstrapDecision(
            action=action,
            binding_id=current.binding.binding_id,
            binding_version=current.binding.version,
            lifecycle_version=current.lifecycle_version,
            lifecycle_projection_ref=current.lifecycle_projection_ref,
            lifecycle_projection_digest=current.lifecycle_projection_digest,
            budget_projection_ref=current.budget_projection_ref,
            decision_projection_ref=current.decision_projection_ref,
            cancellation_requested=current.cancellation_requested,
            terminal=current.terminal,
            reason_code=reason_code,
        )

    async def _interrupt(
        self,
        request: BootstrapRequest,
        current: AuthoritativeRuntimeProjection,
        reason_code: str,
    ) -> BootstrapDecision:
        base = RuntimeBootstrapReconciler._decision(
            current,
            "ready",
            reason_code,
        )
        decision_id = None
        if self._decision_bridge is not None:
            decision_id = await self._decision_bridge.persist_reconciliation_decision(
                request,
                current,
                reason_code,
            )
        return base.model_copy(
            update={
                "action": "interrupt_for_reconciliation",
                "decision_id": decision_id,
            }
        )
