from __future__ import annotations

from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.domain.graph_runtime.contracts import RuntimeExecutionBinding
from app.domain.graph_runtime.identities import AgentRunKey, LangGraphCheckpointKey


class ReconciliationObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    binding_id: str = Field(min_length=1)
    submission_id: str = Field(min_length=1)
    provider_run: AgentRunKey | None = None
    latest_checkpoint: LangGraphCheckpointKey | None = None
    provider_status: str | None = None
    evidence_refs: tuple[str, ...] = ()


class ReconciliationDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    binding_id: str
    action: Literal[
        "bind_observed_run",
        "remain_pending",
        "mark_failed_before_dispatch",
        "operator_required",
    ]
    reason: str = Field(min_length=1)
    evidence_refs: tuple[str, ...]


class RuntimeReconciliationRepository(Protocol):
    async def list_pending(
        self,
        request_scope: str,
        *,
        limit: int,
    ) -> tuple[RuntimeExecutionBinding, ...]: ...

    async def apply_decision(
        self,
        decision: ReconciliationDecision,
    ) -> RuntimeExecutionBinding: ...


class RuntimeProviderInspector(Protocol):
    async def inspect_submission(
        self,
        binding: RuntimeExecutionBinding,
    ) -> ReconciliationObservation: ...


class RuntimeReconciliationService:
    """Reconciles ambiguous provider effects by metadata, never by blind resubmission."""

    def __init__(
        self,
        *,
        repository: RuntimeReconciliationRepository,
        inspector: RuntimeProviderInspector,
    ) -> None:
        self._repository = repository
        self._inspector = inspector

    async def reconcile_pending(
        self,
        request_scope: str,
        *,
        limit: int = 100,
    ) -> tuple[ReconciliationDecision, ...]:
        decisions: list[ReconciliationDecision] = []
        for binding in await self._repository.list_pending(request_scope, limit=limit):
            observation = await self._inspector.inspect_submission(binding)
            if (
                observation.binding_id != binding.binding_id
                or observation.submission_id != binding.submission_id
            ):
                decision = ReconciliationDecision(
                    binding_id=binding.binding_id,
                    action="operator_required",
                    reason="provider observation does not match immutable submission metadata",
                    evidence_refs=observation.evidence_refs,
                )
                await self._repository.apply_decision(decision)
                decisions.append(decision)
                continue
            if observation.provider_run is not None:
                if binding.deployment is None or (
                    observation.provider_run.deployment_endpoint_id
                    != binding.deployment.deployment_endpoint_id
                ):
                    decision = ReconciliationDecision(
                        binding_id=binding.binding_id,
                        action="operator_required",
                        reason="observed provider run belongs to another deployment",
                        evidence_refs=observation.evidence_refs,
                    )
                    await self._repository.apply_decision(decision)
                    decisions.append(decision)
                    continue
            if observation.provider_run is not None:
                action = "bind_observed_run"
                reason = "provider run found by immutable submission metadata"
            elif observation.provider_status == "not_found_definitive":
                action = "mark_failed_before_dispatch"
                reason = "provider proved that the submission was never accepted"
            elif observation.provider_status in {None, "unknown", "temporarily_unavailable"}:
                action = "remain_pending"
                reason = "provider effect remains ambiguous; blind retry is prohibited"
            else:
                action = "operator_required"
                reason = "provider observation conflicts with the frozen runtime binding"
            decision = ReconciliationDecision(
                binding_id=binding.binding_id,
                action=action,
                reason=reason,
                evidence_refs=observation.evidence_refs,
            )
            await self._repository.apply_decision(decision)
            decisions.append(decision)
        return tuple(decisions)
