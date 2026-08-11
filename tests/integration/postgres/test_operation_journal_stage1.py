from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import asyncpg
import pytest
from pydantic import ValidationError

from app.application.operations.journaled_operation_execution import (
    _claim_authority_command_id,
)
from app.application.operations.operation_journal import (
    InMemoryAtomicOperationJournalRepository,
    OperationJournalMutation,
    OperationJournalService,
)
from app.application.operations.postgres_operation_journal import (
    PostgresAtomicOperationJournalRepository,
)
from app.application.run_control.postgres_run_control_repository import PostgresRunControlRepository
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.journal import (
    OperationEffectClaim,
    OperationJournalSettlement,
    OperationTechnicalAttempt,
)
from app.domain.run_control.contracts import (
    AcceptedOperationSettlementEvidence,
    ActorContext,
    ApplyAuthorityBatchAction,
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
    ReserveBudgetAction,
    RunPhase,
    SettleEffectAction,
    SettlePendingUsageAction,
)
from app.domain.run_control.errors import IdempotencyConflict, RunVersionConflict
from app.integrations.postgres import apply_application_migrations
from tests.unit.run_control.test_run_control import actor, command, request, service

NOW = datetime(2026, 8, 5, 20, 0, tzinfo=UTC)
DIGEST = "sha256:" + "a" * 64


def claim(*, run_id: str = "run-1", request_digest: str = DIGEST) -> OperationEffectClaim:
    return OperationEffectClaim(
        effect_claim_id="claim-1",
        request_scope="tenant-1",
        belllabs_run_id=run_id,
        operation_contract_digest=DIGEST,
        idempotency_key="effect-key-1",
        request_digest=request_digest,
        semantic_binding_id="mongo-binding-1",
        semantic_binding_digest=DIGEST,
        semantic_attempt_key=f"{run_id}:operation:search:semantic-attempt:1",
        claimed_by="operation-worker",
        claimed_at=NOW,
    )


def attempt() -> OperationTechnicalAttempt:
    return OperationTechnicalAttempt(
        operation_attempt_id="technical-attempt-1",
        request_scope="tenant-1",
        effect_claim_id="claim-1",
        technical_attempt=1,
        provider="fixture-provider",
        disposition="failed",
        idempotency_supported=True,
        retry_class="safe",
        usage={"tokens.total": 7},
        started_at=NOW,
        finished_at=NOW,
        failure_code="fixture_failure",
    )


def settlement() -> OperationJournalSettlement:
    values = {
        "settlement_id": "settlement-1",
        "request_scope": "tenant-1",
        "effect_claim_id": "claim-1",
        "settlement_revision": 1,
        "status": "failed",
        "usage": {"tokens.total": 7},
        "result_manifest_ref": "artifact:settlement-1",
        "result_manifest_digest": DIGEST,
        "result_manifest_size_bytes": 7,
        "failure_code": "fixture_failure",
        "detail": {"schema_version": "1"},
        "settled_at": NOW,
    }
    return OperationJournalSettlement.create(**values)


def test_legacy_settlement_digest_is_explicit_and_new_writes_cannot_downgrade() -> None:
    legacy_payload = {
        "settlement_id": "legacy-settlement",
        "request_scope": "tenant-1",
        "effect_claim_id": "claim-1",
        "settlement_revision": 1,
        "status": "reconciliation_required",
        "usage": {"tokens.total": 2},
        "pending_external_usage": {"tokens.total": 5},
        "result_manifest_ref": "artifact:legacy",
        "result_manifest_digest": DIGEST,
        "result_manifest_size_bytes": 7,
        "failure_code": None,
        "detail": {"schema_version": "1"},
        "settled_at": NOW,
    }
    legacy_digest = sha256_digest(legacy_payload)
    restored = OperationJournalSettlement.model_validate(
        {**legacy_payload, "settlement_digest": legacy_digest}
    )
    assert restored.digest_version == "legacy-v1"
    assert restored.released_usage == {}
    with pytest.raises(ValidationError, match="digest_version is required"):
        OperationJournalSettlement.model_validate(
            {
                **legacy_payload,
                "released_usage": {},
                "settlement_digest": legacy_digest,
            }
        )
    with pytest.raises(ValidationError, match="digest mismatch"):
        OperationJournalSettlement.model_validate(
            {
                **legacy_payload,
                "settlement_digest": "sha256:" + "b" * 64,
            }
        )
    with pytest.raises(ValueError, match="complete-v2"):
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            expected_run_version=2,
            claim=claim(),
            settlement=restored,
            authority_command=authority_command(run_id="run-1"),
            authority_result=authority_result(run_id="run-1"),
        ).validate()


def authority_result(
    *,
    run_id: str,
    settlement_id: str = "settlement-1",
    revision: int = 1,
    version: int = 2,
    fingerprint: str | None = None,
) -> CommandResult:
    command = authority_command(
        run_id=run_id,
        settlement_id=settlement_id,
        revision=revision,
        resulting_version=version,
    )
    return CommandResult(
        command_id=(
            f"operation-authority-settlement:{settlement_id}:revision:{revision}"
        ),
        idempotency_issuer="operation-journal",
        run_id=run_id,
        command_fingerprint=fingerprint
        or sha256_digest(command.model_dump(mode="json", exclude={"occurred_at"})),
        status=CommandStatus.ACCEPTED,
        resulting_run_version=version,
        phase=RunPhase.ACTIVE,
        reason_code="accepted",
        reason="operation authority accepted",
        recorded_at=NOW,
    )


def authority_command(
    *,
    run_id: str,
    settlement_id: str = "settlement-1",
    revision: int = 1,
    resulting_version: int = 2,
) -> LifecycleCommand:
    return LifecycleCommand(
        command_id=(
            f"operation-authority-settlement:{settlement_id}:revision:{revision}"
        ),
        idempotency_issuer="operation-journal",
        request_scope="tenant-1",
        run_id=run_id,
        expected_run_version=resulting_version - 1,
        actor=ActorContext(
            actor_id="operation-journal",
            authority_refs=frozenset(),
            permissions=frozenset(),
        ),
        action=ApplyAuthorityBatchAction(
            actions=(
                RecordUsageAction(
                    usage_id=settlement_id,
                    authority_ref="mongo-binding-1",
                    reservation_id="reservation-1",
                    actual_amounts={"tokens.total": 7},
                ),
                SettleEffectAction(
                    effect_id="claim-1",
                    settlement_id=settlement_id,
                    observation_id=f"observation:{settlement_id}",
                    outcome=EffectSettlementOutcome.FAILED,
                    usage_settlement_ref=settlement_id,
                    evidence_refs=("artifact:settlement-1",),
                ),
                RecordOperationSettlementEvidenceAction(
                    evidence=AcceptedOperationSettlementEvidence(
                        settlement_id=settlement_id,
                        settlement_payload_digest=settlement().settlement_digest,
                        accepted_by_authority_ref="mongo-binding-1",
                    )
                ),
            )
        ),
        reason="test authority settlement",
        evidence_refs=("mongo-binding-1", "artifact:settlement-1"),
        occurred_at=NOW,
        correlation_id="operation:test",
        causation_id="claim-1",
    )


def authority_event(
    command: LifecycleCommand,
    result: CommandResult,
) -> DomainEventEnvelope:
    action = command.action
    is_batch = isinstance(action, ApplyAuthorityBatchAction)
    payload = {"command_id": command.command_id}
    if is_batch:
        payload["authority_batch_digest"] = sha256_digest(action)
    return DomainEventEnvelope(
        event_id=f"event:{command.command_id}",
        event_type=(
            "workflow_run.apply_authority_batch"
            if is_batch
            else "workflow_run.record_usage"
            if isinstance(action, RecordUsageAction)
            else "workflow_run.claim_effect"
        ),
        aggregate_id=command.run_id,
        aggregate_version=result.resulting_run_version,
        sequence=1,
        occurred_at=NOW,
        recorded_at=NOW,
        actor=command.actor,
        correlation_id=command.correlation_id,
        causation_id=command.causation_id,
        payload=payload,
    )


def pending_settlement(revision: int = 1) -> OperationJournalSettlement:
    return OperationJournalSettlement.create(
        settlement_id="pending-settlement",
        request_scope="tenant-1",
        effect_claim_id="claim-1",
        settlement_revision=revision,
        status="reconciliation_required" if revision == 1 else "failed",
        usage={"tokens.total": 2 if revision == 1 else 7},
        pending_external_usage={"tokens.total": 5} if revision == 1 else {},
        result_manifest_ref="artifact:pending-settlement",
        result_manifest_digest=DIGEST,
        result_manifest_size_bytes=7,
        failure_code="fixture_failure" if revision == 2 else None,
        detail={"schema_version": "1"},
        settled_at=NOW,
    )


def pending_authority_command(
    *,
    run_id: str,
    revision: int,
) -> LifecycleCommand:
    action = (
        ApplyAuthorityBatchAction(
            actions=(
                RecordUsageAction(
                    usage_id="pending-settlement",
                    authority_ref="mongo-binding-1",
                    reservation_id="reservation-1",
                    actual_amounts={"tokens.total": 2},
                    pending_external_amounts={"tokens.total": 5},
                ),
                RecordOperationSettlementEvidenceAction(
                    evidence=AcceptedOperationSettlementEvidence(
                        settlement_id="pending-settlement",
                        settlement_payload_digest=pending_settlement(
                            revision
                        ).settlement_digest,
                        accepted_by_authority_ref="mongo-binding-1",
                    )
                ),
            )
        )
        if revision == 1
        else ApplyAuthorityBatchAction(
            actions=(
                SettlePendingUsageAction(
                    settlement_id="pending:pending-settlement",
                    usage_id="pending-settlement",
                    actual_amounts={"tokens.total": 5},
                ),
                SettleEffectAction(
                    effect_id="claim-1",
                    settlement_id="pending-settlement",
                    observation_id="observation:pending-settlement",
                    outcome=EffectSettlementOutcome.FAILED,
                    usage_settlement_ref="pending:pending-settlement",
                    evidence_refs=("artifact:pending-settlement",),
                ),
                RecordOperationSettlementEvidenceAction(
                    evidence=AcceptedOperationSettlementEvidence(
                        settlement_id="pending-settlement",
                        settlement_payload_digest=pending_settlement(
                            revision
                        ).settlement_digest,
                        accepted_by_authority_ref="mongo-binding-1",
                    )
                ),
            )
        )
    )
    return LifecycleCommand(
        command_id=(
            f"operation-authority-settlement:pending-settlement:revision:{revision}"
        ),
        idempotency_issuer="operation-journal",
        request_scope="tenant-1",
        run_id=run_id,
        expected_run_version=revision,
        actor=ActorContext(
            actor_id="operation-journal",
            authority_refs=frozenset({"mongo-binding-1"}),
            permissions=frozenset(),
        ),
        action=action,
        reason="pending authority settlement",
        evidence_refs=("mongo-binding-1", "artifact:pending-settlement"),
        occurred_at=NOW,
        correlation_id="operation:test",
        causation_id="claim-1",
    )


def result_for_command(command: LifecycleCommand) -> CommandResult:
    return CommandResult(
        command_id=command.command_id,
        idempotency_issuer=command.idempotency_issuer,
        run_id=command.run_id,
        command_fingerprint=sha256_digest(
            command.model_dump(mode="json", exclude={"occurred_at"})
        ),
        status=CommandStatus.ACCEPTED,
        resulting_run_version=command.expected_run_version + 1,
        phase=RunPhase.ACTIVE,
        reason_code="accepted",
        reason="operation authority accepted",
        recorded_at=NOW,
    )


def claim_authority_command(
    *,
    run_id: str,
    claim_value: OperationEffectClaim | None = None,
) -> LifecycleCommand:
    claim_value = claim_value or claim(run_id=run_id)
    return LifecycleCommand(
        command_id=_claim_authority_command_id(claim_value),
        idempotency_issuer="operation-journal",
        request_scope="tenant-1",
        run_id=run_id,
        expected_run_version=1,
        actor=ActorContext(
            actor_id="operation-journal",
            authority_refs=frozenset(),
            permissions=frozenset(),
        ),
        action=ClaimEffectAction(
            effect_id=claim_value.effect_claim_id,
            effect_kind="operation.runtime",
            operation_ref="mongo-binding-1",
            provider_idempotency_key="effect-key-1",
            reservation_id="reservation-1",
            claim_payload_digest=sha256_digest(claim_value.model_dump(mode="json")),
        ),
        reason="claim operation effect",
        occurred_at=NOW,
        correlation_id="operation:test",
        causation_id="mongo-binding-1",
    )


@pytest.mark.asyncio
async def test_claim_attempt_usage_and_settlement_are_idempotent_but_conflicts_durable() -> None:
    repository = InMemoryAtomicOperationJournalRepository()
    journal = OperationJournalService(repository)
    accepted_command = authority_command(run_id="run-1")
    accepted_result = authority_result(run_id="run-1")
    repository.seed_authority_proof(
        accepted_command,
        accepted_result,
        authority_event(accepted_command, accepted_result),
    )
    mutation = OperationJournalMutation(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        expected_run_version=2,
        claim=claim(),
        attempt=attempt(),
        settlement=settlement(),
        authority_command=accepted_command,
        authority_result=accepted_result,
    )

    first = await journal.commit(mutation)
    replay = await journal.commit(mutation)

    assert first.status == "acquired"
    assert replay.status == "existing"
    assert (await repository.get_settlement("tenant-1", "claim-1")).usage == {
        "tokens.total": 7
    }
    with pytest.raises(IdempotencyConflict, match="conflicting request"):
        await journal.commit(
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id="run-1",
                expected_run_version=1,
                claim=claim(request_digest="sha256:" + "b" * 64),
            )
        )
    revised_values = settlement().model_dump(exclude={"settlement_digest"})
    revised_values.update(
        {
            "settlement_id": "settlement-2",
            "settlement_revision": 2,
            "status": "completed",
            "failure_code": None,
        }
    )
    revised_command = authority_command(
        run_id="run-1",
        settlement_id="settlement-2",
        revision=2,
    )
    revised_result = authority_result(
        run_id="run-1",
        settlement_id="settlement-2",
        revision=2,
    )
    repository.seed_authority_proof(
        revised_command,
        revised_result,
        authority_event(revised_command, revised_result),
    )
    with pytest.raises(ValueError, match="prior operation settlement"):
        await journal.commit(
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id="run-1",
                expected_run_version=2,
                claim=claim(),
                settlement=OperationJournalSettlement.create(**revised_values),
                prior_settlement=settlement(),
                authority_command=revised_command,
                authority_result=revised_result,
            )
        )


@pytest.mark.asyncio
async def test_claim_retry_reuses_persisted_generated_identity() -> None:
    repository = InMemoryAtomicOperationJournalRepository()
    journal = OperationJournalService(repository)
    first = await journal.commit(
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            expected_run_version=1,
            claim=claim(),
        )
    )
    replayed_claim = claim().model_copy(
        update={
            "effect_claim_id": "regenerated-claim-id",
            "claimed_at": NOW.replace(second=1),
        }
    )
    replay = await journal.commit(
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            expected_run_version=1,
            claim=replayed_claim,
        )
    )

    assert first.status == "acquired"
    assert replay.status == "existing"
    assert replay.claim is not None
    assert replay.claim.effect_claim_id == "claim-1"


@pytest.mark.asyncio
async def test_claim_authority_proof_rejects_regenerated_claim_identity() -> None:
    repository = InMemoryAtomicOperationJournalRepository()
    original_claim = claim()
    original_command = claim_authority_command(
        run_id="run-1",
        claim_value=original_claim,
    )
    original_result = result_for_command(original_command)
    repository.seed_authority_proof(
        original_command,
        original_result,
        authority_event(original_command, original_result),
    )
    mutation = OperationJournalMutation(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        expected_run_version=2,
        claim=original_claim,
        authority_command=original_command,
        authority_result=original_result,
    )
    assert (await repository.commit(mutation)).status == "acquired"
    assert (await repository.commit(mutation)).status == "existing"

    regenerated_claim = original_claim.model_copy(
        update={"effect_claim_id": "regenerated-claim-id"}
    )
    regenerated_command = claim_authority_command(
        run_id="run-1",
        claim_value=regenerated_claim,
    )
    regenerated_result = result_for_command(regenerated_command)
    repository.seed_authority_proof(
        regenerated_command,
        regenerated_result,
        authority_event(regenerated_command, regenerated_result),
    )
    with pytest.raises(IdempotencyConflict, match="regenerated identity"):
        await repository.commit(
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id="run-1",
                expected_run_version=2,
                claim=regenerated_claim,
                authority_command=regenerated_command,
                authority_result=regenerated_result,
            )
        )


@pytest.mark.asyncio
async def test_concurrent_claim_key_collision_mutates_run_authority_once() -> None:
    run_service, _ = service()
    admitted = await run_service.admit(request(request_id="concurrent-claim-key"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    first_claim = claim(run_id=run_id)
    second_claim = first_claim.model_copy(
        update={"effect_claim_id": "regenerated-claim-id"}
    )

    def executable(claim_value: OperationEffectClaim) -> LifecycleCommand:
        draft = claim_authority_command(
            run_id=run_id,
            claim_value=claim_value,
        )
        return draft.model_copy(
            update={
                "actor": actor(),
                "action": draft.action.model_copy(
                    update={"reservation_id": "baseline"}
                ),
            }
        )

    first_command = executable(first_claim)
    second_command = executable(second_claim)
    assert first_command.command_id == second_command.command_id
    outcomes = await asyncio.gather(
        run_service.execute(first_command),
        run_service.execute(second_command),
        return_exceptions=True,
    )
    accepted = [
        item
        for item in outcomes
        if isinstance(item, CommandResult) and item.status == CommandStatus.ACCEPTED
    ]
    conflicts = [item for item in outcomes if isinstance(item, IdempotencyConflict)]
    assert len(accepted) == 1
    assert len(conflicts) == 1
    projection = await run_service.get_run("tenant-1", run_id)
    effects = await run_service.get_effects("tenant-1", run_id)
    assert projection.version == 2
    assert len(effects.claims) == 1

    winner_command = (
        first_command
        if accepted[0].command_fingerprint
        == result_for_command(first_command).command_fingerprint
        else second_command
    )
    winner_claim = (
        first_claim if winner_command is first_command else second_claim
    )
    winner_event = next(
        record.envelope
        for record in await run_service.pending_outbox("tenant-1")
        if record.envelope.aggregate_version == 2
    )
    journal_repository = InMemoryAtomicOperationJournalRepository()
    journal_repository.seed_authority_proof(
        winner_command,
        accepted[0],
        winner_event,
    )
    journal_mutation = OperationJournalMutation(
        request_scope="tenant-1",
        belllabs_run_id=run_id,
        expected_run_version=2,
        claim=winner_claim,
        authority_command=winner_command,
        authority_result=accepted[0],
    )
    assert (await journal_repository.commit(journal_mutation)).status == "acquired"
    assert (await journal_repository.commit(journal_mutation)).status == "existing"


@pytest.mark.asyncio
async def test_stale_claim_acquisition_requires_exact_authority_proof() -> None:
    run_service, _ = service()
    admitted = await run_service.admit(request(request_id="stale-claim-proof"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    projection = await run_service.get_run("tenant-1", run_id)
    budget = await run_service.get_budget("tenant-1", run_id)
    command_value = claim_authority_command(run_id=run_id)
    result_value = result_for_command(command_value)
    mutation = OperationJournalMutation(
        request_scope="tenant-1",
        belllabs_run_id=run_id,
        expected_run_version=result_value.resulting_run_version,
        claim=claim(run_id=run_id),
        authority_command=command_value,
        authority_result=result_value,
    )
    repository = InMemoryAtomicOperationJournalRepository()
    repository.seed_run(projection.model_copy(update={"version": 3}), budget)
    repository.seed_authority_proof(
        command_value,
        result_value,
        authority_event(command_value, result_value),
    )
    assert (await repository.commit(mutation)).status == "acquired"

    unproven = InMemoryAtomicOperationJournalRepository()
    unproven.seed_run(projection.model_copy(update={"version": 3}), budget)
    with pytest.raises(RunVersionConflict):
        await unproven.commit(
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id=run_id,
                expected_run_version=2,
                claim=claim(run_id=run_id),
            )
        )


@pytest.mark.asyncio
async def test_journal_only_settlement_survives_later_run_version_and_binds_authority() -> None:
    run_service, _run_repository = service()
    admitted = await run_service.admit(request(request_id="journal-liveness"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    projection = await run_service.get_run("tenant-1", run_id)
    budget = await run_service.get_budget("tenant-1", run_id)
    repository = InMemoryAtomicOperationJournalRepository()
    repository.seed_run(projection, budget)
    journal = OperationJournalService(repository)
    operation_claim = claim(run_id=run_id)
    await journal.commit(
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id=run_id,
            expected_run_version=1,
            claim=operation_claim,
        )
    )
    repository.seed_run(
        projection.model_copy(update={"version": 3}),
        budget,
    )
    accepted_authority = authority_result(run_id=run_id, version=2)
    accepted_command = authority_command(run_id=run_id)
    repository.seed_authority_proof(
        accepted_command,
        accepted_authority,
        authority_event(accepted_command, accepted_authority),
    )
    mutation = OperationJournalMutation(
        request_scope="tenant-1",
        belllabs_run_id=run_id,
        expected_run_version=2,
        claim=operation_claim,
        settlement=settlement(),
        authority_command=accepted_command,
        authority_result=accepted_authority,
    )

    committed = await journal.commit(mutation)
    replay = await journal.commit(mutation)

    assert committed.status == "existing"
    assert replay.status == "existing"
    assert await journal.get_settlement("tenant-1", "claim-1") == settlement()
    forged_command = authority_command(run_id=run_id).model_copy(
        update={"reason": "forged authority intent"}
    )
    forged_result = accepted_authority.model_copy(
        update={
            "command_fingerprint": sha256_digest(
                forged_command.model_dump(mode="json", exclude={"occurred_at"})
            )
        }
    )
    repository.seed_authority_proof(
        forged_command,
        forged_result,
        authority_event(forged_command, forged_result),
    )
    with pytest.raises(IdempotencyConflict, match="conflicting intent"):
        await journal.commit(
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id=run_id,
                expected_run_version=2,
                claim=operation_claim,
                settlement=settlement(),
                authority_command=forged_command,
                authority_result=forged_result,
            )
        )
    with pytest.raises(ValueError, match="strict revalidation"):
        await journal.commit(
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id=run_id,
                expected_run_version=2,
                claim=operation_claim,
                settlement=settlement().model_copy(
                    update={"settlement_id": "unrelated-settlement"}
                ),
                authority_command=authority_command(run_id=run_id),
                authority_result=accepted_authority,
            )
        )


def test_journal_authority_proof_rejects_every_settlement_payload_substitution() -> None:
    command_value = authority_command(run_id="run-1")
    result_value = authority_result(run_id="run-1")
    base = settlement()
    base_values = base.model_dump(exclude={"settlement_digest"})
    substitutions = (
        {"usage": {"tokens.total": 8}},
        {"released_usage": {"tokens.total": 1}},
        {"pending_external_usage": {"tokens.total": 1}},
        {"status": "completed", "failure_code": None},
        {
            "result_manifest_ref": "artifact:substituted",
            "result_manifest_digest": "sha256:" + "b" * 64,
        },
        {"settlement_id": "settlement:substituted"},
    )
    for update in substitutions:
        values = {**base_values, **update}
        substituted = OperationJournalSettlement.create(**values)
        with pytest.raises(ValueError):
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id="run-1",
                expected_run_version=2,
                claim=claim(),
                settlement=substituted,
                authority_command=command_value,
                authority_result=result_value,
            ).validate()

    malformed = base.model_copy(update={"usage": {"tokens.total": -1}})
    with pytest.raises(ValueError, match="strict revalidation"):
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            expected_run_version=2,
            claim=claim(),
            settlement=malformed,
            authority_command=command_value,
            authority_result=result_value,
        ).validate()


@pytest.mark.asyncio
async def test_in_memory_settlement_revision_chain_matches_postgres_rules() -> None:
    run_service, _ = service()
    admitted = await run_service.admit(request(request_id="journal-revision-chain"))
    assert admitted.run_id is not None
    run_id = admitted.run_id
    projection = await run_service.get_run("tenant-1", run_id)
    budget = await run_service.get_budget("tenant-1", run_id)
    repository = InMemoryAtomicOperationJournalRepository()
    repository.seed_run(projection, budget)
    journal = OperationJournalService(repository)
    operation_claim = claim(run_id=run_id)
    await journal.commit(
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id=run_id,
            expected_run_version=1,
            claim=operation_claim,
        )
    )
    first_command = pending_authority_command(run_id=run_id, revision=1)
    first_result = result_for_command(first_command)
    repository.seed_run(projection.model_copy(update={"version": 2}), budget)
    repository.seed_authority_proof(
        first_command,
        first_result,
        authority_event(first_command, first_result),
    )
    first_settlement = pending_settlement(1)
    first_mutation = OperationJournalMutation(
        request_scope="tenant-1",
        belllabs_run_id=run_id,
        expected_run_version=2,
        claim=operation_claim,
        settlement=first_settlement,
        authority_command=first_command,
        authority_result=first_result,
    )
    assert (await journal.commit(first_mutation)).status == "existing"
    assert (await journal.commit(first_mutation)).status == "existing"

    second_command = pending_authority_command(run_id=run_id, revision=2)
    second_result = result_for_command(second_command)
    repository.seed_run(projection.model_copy(update={"version": 3}), budget)
    repository.seed_authority_proof(
        second_command,
        second_result,
        authority_event(second_command, second_result),
    )
    second_settlement = pending_settlement(2)
    second_mutation = OperationJournalMutation(
        request_scope="tenant-1",
        belllabs_run_id=run_id,
        expected_run_version=3,
        claim=operation_claim,
        settlement=second_settlement,
        prior_settlement=first_settlement,
        authority_command=second_command,
        authority_result=second_result,
    )
    assert (await journal.commit(second_mutation)).status == "existing"
    assert await journal.get_settlement("tenant-1", "claim-1") == second_settlement

    with pytest.raises(ValueError, match="strict revalidation"):
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id=run_id,
            expected_run_version=3,
            claim=operation_claim,
            settlement=second_settlement.model_copy(
                update={"settlement_revision": 3}
            ),
            prior_settlement=first_settlement,
            authority_command=second_command,
            authority_result=second_result,
        ).validate()


@pytest.mark.asyncio
async def test_shadow_execution_cannot_acquire_consequential_claim() -> None:
    repository = InMemoryAtomicOperationJournalRepository()
    result = await OperationJournalService(repository).commit(
        OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id="run-1",
            expected_run_version=1,
            claim=claim().model_copy(update={"claim_mode": "shadow"}),
        )
    )
    assert result.status == "shadow_denied"
    assert await repository.get_claim("tenant-1", "claim-1") is None


@pytest.mark.asyncio
async def test_postgres_journal_crash_rolls_back_claim_attempt_and_settlement(
    test_application_postgres_dsn: str,
) -> None:
    pool = await asyncpg.create_pool(
        dsn=test_application_postgres_dsn,
        min_size=1,
        max_size=4,
    )
    try:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await apply_application_migrations(pool)
        async with pool.acquire() as connection:
            primary_key = await connection.fetchval(
                """
                SELECT pg_get_constraintdef(oid)
                FROM pg_constraint
                WHERE conrelid = 'belllabs_control.operation_settlements'::regclass
                  AND contype = 'p'
                """
            )
        assert primary_key == (
            "PRIMARY KEY (request_scope, settlement_id, settlement_revision)"
        )
        run_service, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        admitted = await run_service.admit(request())
        assert admitted.run_id is not None
        operation_claim = claim(run_id=admitted.run_id)
        claim_command = command(
            admitted.run_id,
            1,
            _claim_authority_command_id(operation_claim),
            ClaimEffectAction(
                effect_id="claim-1",
                effect_kind="operation.runtime",
                operation_ref="mongo-binding-1",
                provider_idempotency_key="effect-key-1",
                reservation_id="baseline",
                claim_payload_digest=sha256_digest(
                    operation_claim.model_dump(mode="json")
                ),
            ),
        )
        claimed = await run_service.execute(claim_command)
        claim_repository = PostgresAtomicOperationJournalRepository(pool)
        claim_mutation = OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id=admitted.run_id,
            expected_run_version=claimed.resulting_run_version,
            claim=operation_claim,
            authority_command=claim_command,
            authority_result=claimed,
        )
        assert (await claim_repository.commit(claim_mutation)).status == "acquired"
        assert (await claim_repository.commit(claim_mutation)).status == "existing"
        with pytest.raises(ValueError, match="claim authority"):
            OperationJournalMutation(
                request_scope="tenant-1",
                belllabs_run_id=admitted.run_id,
                expected_run_version=claimed.resulting_run_version,
                claim=operation_claim.model_copy(
                    update={"effect_claim_id": "regenerated-claim-id"}
                ),
                authority_command=claim_command,
                authority_result=claimed,
            ).validate()
        observed = await run_service.execute(
            command(
                admitted.run_id,
                claimed.resulting_run_version,
                "postgres-journal-effect-observation",
                ObserveEffectAction(
                    effect_id="claim-1",
                    observation_id="observation:settlement-1",
                    disposition=EffectDisposition.FAILED,
                ),
            )
        )
        accepted_command = LifecycleCommand(
            command_id="operation-authority-settlement:settlement-1:revision:1",
            idempotency_issuer="operation-journal",
            request_scope="tenant-1",
            run_id=admitted.run_id,
            expected_run_version=observed.resulting_run_version,
            actor=actor(),
            action=ApplyAuthorityBatchAction(
                actions=(
                    RecordUsageAction(
                        usage_id="settlement-1",
                        authority_ref="mongo-binding-1",
                        reservation_id="baseline",
                        actual_amounts={"tokens.total": 7},
                        release_amounts={"tokens.total": 13},
                    ),
                    SettleEffectAction(
                        effect_id="claim-1",
                        settlement_id="settlement-1",
                        observation_id="observation:settlement-1",
                        outcome=EffectSettlementOutcome.FAILED,
                        usage_settlement_ref="settlement-1",
                    ),
                )
            ),
            reason="postgres journal authority",
            occurred_at=NOW,
            correlation_id="operation:test",
            causation_id="claim-1",
        )
        accepted_authority = await run_service.execute(accepted_command)
        mutation = OperationJournalMutation(
            request_scope="tenant-1",
            belllabs_run_id=admitted.run_id,
            expected_run_version=accepted_authority.resulting_run_version,
            claim=operation_claim,
            attempt=attempt(),
            settlement=settlement(),
            authority_command=accepted_command,
            authority_result=accepted_authority,
        )
        advanced = await run_service.execute(
            command(
                admitted.run_id,
                accepted_authority.resulting_run_version,
                "advance-before-journal-settlement",
                ReserveBudgetAction(
                    reservation_id="later-unrelated-reservation",
                    amounts={"tokens.total": 1},
                ),
            )
        )
        assert advanced.resulting_run_version == 5

        async def fail_before_commit(boundary: str) -> None:
            if boundary == "operation_journal":
                raise RuntimeError("injected operation journal crash")

        failing = PostgresAtomicOperationJournalRepository(
            pool,
            before_commit=fail_before_commit,
        )
        with pytest.raises(RuntimeError, match="injected"):
            await failing.commit(mutation)
        assert await failing.get_claim("tenant-1", "claim-1") is None

        repository = PostgresAtomicOperationJournalRepository(pool)
        assert (await repository.commit(mutation)).status == "acquired"
        assert (await repository.commit(mutation)).status == "existing"
        persisted = await repository.get_settlement("tenant-1", "claim-1")
        assert persisted == settlement()
        with pytest.raises(IdempotencyConflict, match="conflicting immutable intent"):
            await repository.commit(
                OperationJournalMutation(
                    request_scope="tenant-1",
                    belllabs_run_id=admitted.run_id,
                    expected_run_version=1,
                    claim=claim(
                        run_id=admitted.run_id,
                        request_digest="sha256:" + "b" * 64,
                    ),
                )
            )
        revised_values = settlement().model_dump(exclude={"settlement_digest"})
        revised_values.update(
            {
                "settlement_id": "settlement-2",
                "settlement_revision": 2,
                "status": "completed",
                "failure_code": None,
            }
        )
        with pytest.raises(ValueError, match="prior operation settlement"):
            await repository.commit(
                OperationJournalMutation(
                    request_scope="tenant-1",
                    belllabs_run_id=admitted.run_id,
                    expected_run_version=2,
                    claim=operation_claim,
                    settlement=OperationJournalSettlement.create(**revised_values),
                    prior_settlement=settlement(),
                    authority_command=authority_command(
                        run_id=admitted.run_id,
                        settlement_id="settlement-2",
                        revision=2,
                    ),
                    authority_result=authority_result(
                        run_id=admitted.run_id,
                        settlement_id="settlement-2",
                        revision=2,
                    ),
                )
            )

        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SET LOCAL ROLE belllabs_control_runtime")
            await connection.execute(
                "SELECT set_config('belllabs.request_scope', 'tenant-2', true)"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM belllabs_control.operation_effect_claims"
                )
                == 0
            )
    finally:
        async with pool.acquire() as connection:
            await connection.execute("DROP SCHEMA IF EXISTS belllabs_control CASCADE")
        await pool.close()
