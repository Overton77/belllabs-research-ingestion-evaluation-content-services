from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, uuid5

from app.application.workspaces.artifact_promotion import ArtifactPayloadAddress
from app.application.operations.operation_journal import OperationJournalMutation, OperationJournalService
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    OperationExecutionBinding,
    OperationSettlement,
)
from app.domain.operation_execution.journal import (
    OperationClaimResult,
    OperationEffectClaim,
    OperationJournalSettlement,
    OperationTechnicalAttempt,
)
from app.domain.run_control.contracts import (
    AcceptedOperationSettlementEvidence,
    ActorContext,
    ApplyAuthorityBatchAction,
    BudgetState,
    ClaimEffectAction,
    CommandResult,
    CommandStatus,
    DomainEventEnvelope,
    EffectDisposition,
    EffectSettlementOutcome,
    LifecycleCommand,
    ObserveEffectAction,
    RecordOperationSettlementEvidenceAction,
    RecordUsageAction,
    RunProjection,
    SettleEffectAction,
    SettlePendingUsageAction,
)


class JournalRunControlReader(Protocol):
    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState: ...

    async def execute(self, command: LifecycleCommand) -> CommandResult: ...

    async def get_command_result(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> CommandResult | None: ...


class ResultPayloadStore(Protocol):
    async def stage(
        self,
        *,
        artifact_id: str,
        content: bytes,
        content_digest: str,
        media_type: str,
    ) -> ArtifactPayloadAddress: ...

    async def retrieve(self, address: ArtifactPayloadAddress) -> bytes: ...


class JournaledOperationExecutionCoordinator:
    """Coordinates Mongo OEB authority with the PostgreSQL effect journal."""

    def __init__(
        self,
        *,
        journal: OperationJournalService,
        run_control: JournalRunControlReader,
        results: ResultPayloadStore,
        actor: ActorContext,
    ) -> None:
        self._journal = journal
        self._run_control = run_control
        self._results = results
        self._actor = actor

    async def acquire(
        self,
        binding: OperationExecutionBinding,
        *,
        claimed_by: str,
    ) -> OperationClaimResult:
        claim = _claim_for(binding, claimed_by=claimed_by)
        authority_command, claimed = await self._execute_replayable(
            LifecycleCommand(
                command_id=_claim_authority_command_id(claim),
                idempotency_issuer="operation-journal",
                request_scope=binding.request_scope,
                run_id=binding.run_id,
                expected_run_version=binding.run_control_revision,
                actor=self._actor,
                action=ClaimEffectAction(
                    effect_id=claim.effect_claim_id,
                    effect_kind="operation.runtime",
                    operation_ref=binding.binding_id,
                    provider_idempotency_key=binding.side_effect_key,
                    reservation_id=binding.budget_reservation_id,
                    claim_payload_digest=sha256_digest(
                        claim.model_dump(mode="json")
                    ),
                ),
                reason="Claim consequential operation effect before provider dispatch",
                evidence_refs=(binding.binding_id,),
                occurred_at=claim.claimed_at,
                correlation_id=f"operation:{binding.semantic_attempt_key}",
                causation_id=binding.binding_id,
            )
        )
        if claimed.status != CommandStatus.ACCEPTED:
            return OperationClaimResult(
                status="shadow_denied",
                reason=f"run-control effect claim rejected: {claimed.reason_code}",
            )
        return await self._journal.commit(
            OperationJournalMutation(
                request_scope=binding.request_scope,
                belllabs_run_id=binding.run_id,
                expected_run_version=claimed.resulting_run_version,
                claim=claim,
                authority_command=authority_command,
                authority_result=claimed,
            )
        )

    async def get_settlement(
        self,
        binding: OperationExecutionBinding,
    ) -> OperationSettlement | None:
        claim_id = _effect_claim_id(binding)
        settlement = await self._journal.get_settlement(
            binding.request_scope,
            claim_id,
        )
        if settlement is None or settlement.result_manifest_ref is None:
            return None
        assert settlement.result_manifest_digest is not None
        assert settlement.result_manifest_size_bytes is not None
        content = await self._results.retrieve(
            ArtifactPayloadAddress(
                object_ref=settlement.result_manifest_ref,
                content_digest=settlement.result_manifest_digest,
                size_bytes=settlement.result_manifest_size_bytes,
            )
        )
        replay = OperationSettlement.model_validate_json(content)
        if replay.binding_id != binding.binding_id:
            raise ValueError("result manifest belongs to another operation binding")
        return replay

    async def settle(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim,
        settlement: OperationSettlement,
        *,
        started_at: datetime,
    ) -> OperationSettlement:
        expected_claim_id = _effect_claim_id(binding)
        if (
            claim.effect_claim_id != expected_claim_id
            or claim.request_scope != binding.request_scope
            or claim.semantic_binding_id != binding.binding_id
            or claim.semantic_binding_digest != sha256_digest(binding)
            or settlement.binding_id != binding.binding_id
        ):
            raise ValueError("claim, binding, and settlement authority do not match")
        replay_manifest = settlement.model_dump(
            mode="json",
            exclude={"output_text", "structured_output", "event_payloads"},
        )
        content = json.dumps(
            replay_manifest,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        content_digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
        address = await self._results.stage(
            artifact_id=settlement.settlement_id,
            content=content,
            content_digest=content_digest,
            media_type="application/vnd.belllabs.operation-settlement+json",
        )
        current_run = await self._run_control.get_run(binding.request_scope, binding.run_id)
        release_amounts = {
            dimension: limit
            - min(
                limit,
                settlement.usage.amounts.get(dimension, 0)
                + settlement.usage.pending_external_amounts.get(dimension, 0),
            )
            for dimension, limit in binding.budget_limits.items()
            if limit
            > settlement.usage.amounts.get(dimension, 0)
            + settlement.usage.pending_external_amounts.get(dimension, 0)
        }
        disposition = (
            EffectDisposition.SUCCEEDED
            if settlement.status == "completed"
            else EffectDisposition.CANCELLED
            if settlement.status == "cancelled"
            else EffectDisposition.FAILED
        )
        _observation_command, observation = await self._execute_replayable(
            LifecycleCommand(
                command_id=f"operation-effect-observation:{settlement.settlement_id}",
                idempotency_issuer="operation-journal",
                request_scope=binding.request_scope,
                run_id=binding.run_id,
                expected_run_version=current_run.version,
                actor=self._actor,
                action=ObserveEffectAction(
                    effect_id=claim.effect_claim_id,
                    observation_id=f"observation:{settlement.settlement_id}",
                    disposition=disposition,
                    provider_effect_ref=settlement.provider_run_id,
                    evidence_refs=(address.object_ref,),
                ),
                reason="Reconcile provider completion as an observed effect fact",
                evidence_refs=(binding.binding_id, address.object_ref),
                occurred_at=settlement.settled_at,
                correlation_id=f"operation:{binding.semantic_attempt_key}",
                causation_id=claim.effect_claim_id,
            )
        )
        _require_accepted(observation, "effect observation")
        pending_external = any(settlement.usage.pending_external_amounts.values())
        authority_actor = self._actor.model_copy(
            update={
                "authority_refs": self._actor.authority_refs
                | {binding.binding_id}
            }
        )
        journal_settlement = OperationJournalSettlement.create(
            settlement_id=settlement.settlement_id,
            request_scope=binding.request_scope,
            effect_claim_id=claim.effect_claim_id,
            settlement_revision=1,
            status=(
                "reconciliation_required" if pending_external else settlement.status
            ),
            usage=settlement.usage.amounts,
            released_usage=release_amounts,
            pending_external_usage=settlement.usage.pending_external_amounts,
            result_manifest_ref=address.object_ref,
            result_manifest_digest=address.content_digest,
            result_manifest_size_bytes=address.size_bytes,
            failure_code=settlement.failure_code,
            detail={"schema_version": "1"},
            settled_at=settlement.settled_at,
        )
        usage_action = RecordUsageAction(
            usage_id=settlement.settlement_id,
            authority_ref=binding.binding_id,
            actual_amounts=settlement.usage.amounts,
            reservation_id=binding.budget_reservation_id,
            release_amounts=release_amounts,
            pending_external_amounts=settlement.usage.pending_external_amounts,
        )
        settlement_evidence = RecordOperationSettlementEvidenceAction(
            evidence=AcceptedOperationSettlementEvidence(
                settlement_id=journal_settlement.settlement_id,
                settlement_payload_digest=journal_settlement.settlement_digest,
                accepted_by_authority_ref=binding.binding_id,
            )
        )
        authority_action = (
            ApplyAuthorityBatchAction(actions=(usage_action, settlement_evidence))
            if pending_external
            else ApplyAuthorityBatchAction(
                actions=(
                    usage_action,
                    SettleEffectAction(
                        effect_id=claim.effect_claim_id,
                        settlement_id=settlement.settlement_id,
                        observation_id=f"observation:{settlement.settlement_id}",
                        outcome=EffectSettlementOutcome(disposition.value),
                        usage_settlement_ref=settlement.settlement_id,
                        evidence_refs=(address.object_ref,),
                    ),
                    settlement_evidence,
                )
            )
        )
        authority_command, usage_result = await self._execute_replayable(
            LifecycleCommand(
                command_id=(
                    f"operation-authority-settlement:{settlement.settlement_id}:revision:1"
                ),
                idempotency_issuer="operation-journal",
                request_scope=binding.request_scope,
                run_id=binding.run_id,
                expected_run_version=observation.resulting_run_version,
                actor=authority_actor,
                action=authority_action,
                reason=(
                    "Record pending operation usage before later reconciliation"
                    if pending_external
                    else "Atomically settle operation usage and consequential effect"
                ),
                evidence_refs=(binding.binding_id, address.object_ref),
                occurred_at=settlement.settled_at,
                correlation_id=f"operation:{binding.semantic_attempt_key}",
                causation_id=claim.effect_claim_id,
            )
        )
        _require_accepted(usage_result, "operation authority settlement")
        technical_attempt = OperationTechnicalAttempt(
            operation_attempt_id=str(
                uuid5(NAMESPACE_URL, f"operation-technical:{settlement.settlement_id}")
            ),
            request_scope=binding.request_scope,
            effect_claim_id=claim.effect_claim_id,
            technical_attempt=1,
            provider="runtime_adapter",
            provider_attempt_id=settlement.provider_run_id,
            disposition=(
                "succeeded"
                if settlement.status == "completed"
                else "cancelled"
                if settlement.status == "cancelled"
                else "failed"
            ),
            idempotency_supported=True,
            retry_class="claim_then_reconcile",
            usage=settlement.usage.amounts,
            started_at=started_at,
            finished_at=settlement.settled_at,
            failure_code=settlement.failure_code,
        )
        await self._journal.commit(
            OperationJournalMutation(
                request_scope=binding.request_scope,
                belllabs_run_id=binding.run_id,
                expected_run_version=usage_result.resulting_run_version,
                claim=claim,
                attempt=technical_attempt,
                settlement=journal_settlement,
                authority_command=authority_command,
                authority_result=usage_result,
            )
        )
        return settlement

    async def settle_pending_usage(
        self,
        binding: OperationExecutionBinding,
        claim: OperationEffectClaim,
        settlement: OperationSettlement,
        *,
        actual_amounts: dict[str, int],
        release_amounts: dict[str, int],
        reconciled_at: datetime,
    ) -> OperationSettlement:
        if (
            claim.effect_claim_id != _effect_claim_id(binding)
            or claim.semantic_binding_id != binding.binding_id
            or settlement.binding_id != binding.binding_id
        ):
            raise ValueError("pending settlement authority does not match operation binding")
        prior = await self._journal.get_settlement(
            binding.request_scope,
            claim.effect_claim_id,
        )
        if (
            prior is None
            or prior.settlement_id != settlement.settlement_id
            or prior.status != "reconciliation_required"
            or not prior.pending_external_usage
        ):
            raise ValueError("operation has no matching pending usage settlement")
        if prior.result_manifest_ref is None:
            raise ValueError("pending operation settlement has no result manifest")
        final_settlement = OperationJournalSettlement.create(
            settlement_id=settlement.settlement_id,
            request_scope=binding.request_scope,
            effect_claim_id=claim.effect_claim_id,
            settlement_revision=2,
            status=settlement.status,
            usage={
                dimension: prior.usage.get(dimension, 0)
                + actual_amounts.get(dimension, 0)
                for dimension in prior.usage.keys() | actual_amounts.keys()
            },
            released_usage={
                dimension: prior.released_usage.get(dimension, 0)
                + release_amounts.get(dimension, 0)
                for dimension in prior.released_usage.keys()
                | release_amounts.keys()
            },
            pending_external_usage={},
            result_manifest_ref=prior.result_manifest_ref,
            result_manifest_digest=prior.result_manifest_digest,
            result_manifest_size_bytes=prior.result_manifest_size_bytes,
            failure_code=settlement.failure_code,
            detail={"schema_version": "1"},
            settled_at=reconciled_at,
        )
        current_run = await self._run_control.get_run(binding.request_scope, binding.run_id)
        pending_settlement_id = f"pending:{settlement.settlement_id}"
        authority_command, authority_result = await self._execute_replayable(
            LifecycleCommand(
                command_id=(
                    f"operation-authority-settlement:{settlement.settlement_id}:revision:2"
                ),
                idempotency_issuer="operation-journal",
                request_scope=binding.request_scope,
                run_id=binding.run_id,
                expected_run_version=current_run.version,
                actor=self._actor.model_copy(
                    update={
                        "authority_refs": self._actor.authority_refs
                        | {binding.binding_id}
                    }
                ),
                action=ApplyAuthorityBatchAction(
                    actions=(
                        SettlePendingUsageAction(
                            settlement_id=pending_settlement_id,
                            usage_id=settlement.settlement_id,
                            actual_amounts=actual_amounts,
                            pending_release_amounts=release_amounts,
                        ),
                        SettleEffectAction(
                            effect_id=claim.effect_claim_id,
                            settlement_id=settlement.settlement_id,
                            observation_id=f"observation:{settlement.settlement_id}",
                            outcome=EffectSettlementOutcome(
                                (
                                    EffectDisposition.SUCCEEDED
                                    if settlement.status == "completed"
                                    else EffectDisposition.CANCELLED
                                    if settlement.status == "cancelled"
                                    else EffectDisposition.FAILED
                                ).value
                            ),
                            usage_settlement_ref=pending_settlement_id,
                            evidence_refs=(prior.result_manifest_ref,),
                        ),
                        RecordOperationSettlementEvidenceAction(
                            evidence=AcceptedOperationSettlementEvidence(
                                settlement_id=final_settlement.settlement_id,
                                settlement_payload_digest=(
                                    final_settlement.settlement_digest
                                ),
                                accepted_by_authority_ref=binding.binding_id,
                            )
                        ),
                    )
                ),
                reason="Atomically reconcile pending operation usage and effect",
                evidence_refs=(
                    binding.binding_id,
                    prior.result_manifest_ref,
                ),
                occurred_at=reconciled_at,
                correlation_id=f"operation:{binding.semantic_attempt_key}",
                causation_id=claim.effect_claim_id,
            )
        )
        _require_accepted(authority_result, "pending operation authority settlement")
        await self._journal.commit(
            OperationJournalMutation(
                request_scope=binding.request_scope,
                belllabs_run_id=binding.run_id,
                expected_run_version=authority_result.resulting_run_version,
                claim=claim,
                settlement=final_settlement,
                prior_settlement=prior,
                authority_command=authority_command,
                authority_result=authority_result,
            )
        )
        return settlement

    async def _execute_replayable(
        self,
        command: LifecycleCommand,
    ) -> tuple[LifecycleCommand, CommandResult]:
        prior = await self._run_control.get_command_result(
            command.request_scope,
            command.run_id,
            command.idempotency_issuer,
            command.command_id,
        )
        if prior is not None:
            command = command.model_copy(
                update={"expected_run_version": prior.resulting_run_version - 1}
            )
            fingerprint = sha256_digest(
                command.model_dump(mode="json", exclude={"occurred_at"})
            )
            if (
                prior.status != CommandStatus.ACCEPTED
                or prior.command_fingerprint != fingerprint
            ):
                raise RuntimeError(
                    "stored operation authority result conflicts with exact replay command: "
                    f"stored={prior.command_fingerprint}, rebuilt={fingerprint}"
                )
            return command, prior
        result = await self._run_control.execute(command)
        return command, result


def _effect_claim_id(binding: OperationExecutionBinding) -> str:
    identity = f"operation-effect:{binding.request_scope}:{binding.binding_id}"
    return str(uuid5(NAMESPACE_URL, identity))


def _claim_authority_command_id(claim: OperationEffectClaim) -> str:
    identity = (
        f"operation-claim-authority:{claim.request_scope}:"
        f"{claim.operation_contract_digest}:{claim.idempotency_key}"
    )
    return f"operation-effect-claim:{uuid5(NAMESPACE_URL, identity)}"


def _require_accepted(result: CommandResult, subject: str) -> None:
    if result.status != CommandStatus.ACCEPTED:
        raise RuntimeError(f"{subject} rejected by run control: {result.reason_code}")


def _claim_for(
    binding: OperationExecutionBinding,
    *,
    claimed_by: str,
) -> OperationEffectClaim:
    return OperationEffectClaim(
        effect_claim_id=_effect_claim_id(binding),
        request_scope=binding.request_scope,
        belllabs_run_id=binding.run_id,
        operation_contract_digest=sha256_digest(binding.operation_contract_ref),
        idempotency_key=binding.side_effect_key,
        request_digest=binding.request_fingerprint,
        semantic_binding_id=binding.binding_id,
        semantic_binding_digest=sha256_digest(binding),
        semantic_attempt_key=binding.semantic_attempt_key,
        claimed_by=claimed_by,
        claimed_at=binding.bound_at,
    )


def _runtime_outbox_events(
    *,
    binding: OperationExecutionBinding,
    settlement: OperationSettlement,
    claim: OperationEffectClaim,
    result_manifest_ref: str,
    aggregate_version: int,
    actor: ActorContext,
    correlation_id: str,
) -> tuple[DomainEventEnvelope, ...]:
    events = []
    event_count = len(settlement.event_payloads)
    for index, payload in enumerate(settlement.event_payloads, start=1):
        events.append(
            DomainEventEnvelope(
                event_id=str(
                    uuid5(
                        NAMESPACE_URL,
                        f"operation-event:{binding.request_scope}:"
                        f"{settlement.settlement_id}:{index}",
                    )
                ),
                event_type="operation.runtime_event_recorded",
                aggregate_id=binding.run_id,
                aggregate_version=aggregate_version,
                sequence=index + 1,
                is_version_final=index == event_count,
                occurred_at=settlement.settled_at,
                recorded_at=settlement.settled_at,
                actor=actor,
                correlation_id=correlation_id,
                causation_id=claim.effect_claim_id,
                payload={
                    "operation_id": binding.operation_id,
                    "event_index": index - 1,
                    "event_payload_digest": sha256_digest(payload),
                    "result_manifest_ref": result_manifest_ref,
                },
            )
        )
    return tuple(events)
