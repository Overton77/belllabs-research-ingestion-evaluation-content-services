from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from app.domain.graph_runtime.contracts import RuntimeExecutionBinding
from app.domain.graph_runtime.identities import DIGEST_PATTERN, AgentRunKey, LangGraphCheckpointKey


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


class RuntimeIncidentType(StrEnum):
    BINDING_WITHOUT_THREAD = "binding_without_thread"
    THREAD_WITHOUT_INITIAL_RUN = "thread_without_initial_run"
    PROVIDER_ACTIVE_WHILE_BELL_LABS_STOPPED = "provider_active_while_belllabs_stopped"
    BELL_LABS_ACTIVE_WITHOUT_RUNTIME = "belllabs_active_without_runtime"
    UNSETTLED_ACCEPTED_OPERATION = "unsettled_accepted_operation"
    STALE_DECISION = "stale_decision"
    ORPHAN_RUNTIME_RESOURCE = "orphan_runtime_resource"
    TERMINAL_WITHOUT_TYPED_RESULT = "terminal_without_typed_result"
    INCOMPATIBLE_CHECKPOINT_ROUTE = "incompatible_checkpoint_route"
    OUTBOX_CURSOR_DRIFT = "outbox_cursor_drift"
    EXPIRED_RESOURCE_LEASE = "expired_resource_lease"
    LINEAGE_GAP_OR_COLLISION = "lineage_gap_or_collision"
    MISSING_ASSEMBLY_OR_CONTEXT_DIGEST = "missing_assembly_or_context_digest"


class RuntimeIncidentObservation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    incident_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    binding_id: str | None = None
    incident_type: RuntimeIncidentType
    identity_digest: str = Field(pattern=DIGEST_PATTERN)
    observed_version: int = Field(ge=0)
    expected_version: int = Field(ge=0)
    evidence_refs: tuple[str, ...] = ()
    proposed_action: Literal[
        "create_thread",
        "bind_observed_run",
        "interrupt_runtime",
        "mark_runtime_missing",
        "settle_observed_usage",
        "expire_decision",
        "close_orphan",
        "request_typed_result",
        "repair_cursor",
        "expire_lease",
        "operator_required",
    ]
    ambiguous_effect: bool = False
    compatible: bool = True
    observed_at: datetime


class RuntimeIncidentDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    incident_id: str
    request_scope: str
    action: str = Field(min_length=1)
    disposition: Literal["automatic", "operator_required", "retry_scheduled"]
    before_version: int = Field(ge=0)
    after_version: int | None = Field(default=None, ge=0)
    actor_ref: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    evidence_refs: tuple[str, ...] = ()
    retry_at: datetime | None = None


class RuntimeRepairAuditRecord(BaseModel):
    """Immutable evidence written after an authorized repair is observed."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request_scope: str = Field(min_length=1)
    audit_id: str = Field(min_length=1)
    incident_id: str | None = Field(default=None, min_length=1)
    command_id: str = Field(min_length=1)
    actor_id: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    expected_belllabs_version: int = Field(ge=1)
    expected_checkpoint_id: str | None = Field(default=None, min_length=1)
    before_digest: str = Field(pattern=DIGEST_PATTERN)
    after_digest: str = Field(pattern=DIGEST_PATTERN)
    evidence_refs: tuple[str, ...] = Field(min_length=1)
    recorded_at: datetime


class RuntimeIncidentRepository(Protocol):
    async def reserve_incident(self, observation: RuntimeIncidentObservation) -> bool: ...

    async def record_incident_decision(
        self,
        observation: RuntimeIncidentObservation,
        decision: RuntimeIncidentDecision,
    ) -> RuntimeIncidentDecision: ...


class RuntimeIncidentReconciler:
    """Fail-closed policy for the complete Stage 3 inconsistency catalog."""

    _SAFE_ACTIONS = frozenset(
        {
            "create_thread",
            "bind_observed_run",
            "interrupt_runtime",
            "mark_runtime_missing",
            "settle_observed_usage",
            "expire_decision",
            "close_orphan",
            "request_typed_result",
            "repair_cursor",
            "expire_lease",
        }
    )
    _ALWAYS_OPERATOR = frozenset(
        {
            RuntimeIncidentType.INCOMPATIBLE_CHECKPOINT_ROUTE,
            RuntimeIncidentType.LINEAGE_GAP_OR_COLLISION,
            RuntimeIncidentType.MISSING_ASSEMBLY_OR_CONTEXT_DIGEST,
        }
    )

    def __init__(self, repository: RuntimeIncidentRepository) -> None:
        self._repository = repository

    async def reconcile(
        self,
        observation: RuntimeIncidentObservation,
        *,
        actor_ref: str = "service:runtime-reconciler",
    ) -> RuntimeIncidentDecision:
        created = await self._repository.reserve_incident(observation)
        if not created:
            return await self._repository.record_incident_decision(
                observation,
                self._decision(observation, actor_ref=actor_ref),
            )
        decision = self._decision(observation, actor_ref=actor_ref)
        return await self._repository.record_incident_decision(observation, decision)

    def _decision(
        self,
        observation: RuntimeIncidentObservation,
        *,
        actor_ref: str,
    ) -> RuntimeIncidentDecision:
        operator_required = (
            observation.ambiguous_effect
            or not observation.compatible
            or observation.incident_type in self._ALWAYS_OPERATOR
            or observation.proposed_action not in self._SAFE_ACTIONS
            or observation.observed_version != observation.expected_version
        )
        if operator_required:
            return RuntimeIncidentDecision(
                incident_id=observation.incident_id,
                request_scope=observation.request_scope,
                action="operator_required",
                disposition="operator_required",
                before_version=observation.observed_version,
                actor_ref=actor_ref,
                reason="unsafe, ambiguous, incompatible, or stale reconciliation requires review",
                evidence_refs=observation.evidence_refs,
            )
        return RuntimeIncidentDecision(
            incident_id=observation.incident_id,
            request_scope=observation.request_scope,
            action=observation.proposed_action,
            disposition="automatic",
            before_version=observation.observed_version,
            after_version=observation.observed_version + 1,
            actor_ref=actor_ref,
            reason="idempotent version-checked automatic repair",
            evidence_refs=observation.evidence_refs,
        )
