from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from pydantic import Field, ValidationError

from app.api.run_control import (
    compose_api_run_control_service,
    configure_family_admission_registry,
)
from app.application.run_control.service import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
    RunControlService,
)
from app.application.run_control.run_control_repository import (
    InMemoryRunControlRepository,
    authority_state_digest,
    upgrade_legacy_operation_pending_usage,
)
from app.domain.control_plane.canonical import canonical_json, sha256_digest
from app.domain.operation_execution.journal import OperationJournalSettlement
from app.domain.run_control.contracts import (
    MAX_AUTHORITY_BATCH_IDENTITY_SUMMARY_BYTES,
    AcceptedObligationEvidence,
    AcceptedOutputEvidence,
    ApplyAuthorityBatchAction,
    BudgetState,
    ClaimEffectAction,
    CommandStatus,
    ConsumerApplyStatus,
    EffectDisposition,
    EffectLedgerState,
    EffectSettlementOutcome,
    ObserveEffectAction,
    RecordObligationEvidenceAction,
    RecordOutputEvidenceAction,
    RecordUsageAction,
    ReserveBudgetAction,
    SettleEffectAction,
    SettlePendingUsageAction,
    StartAction,
)
from app.domain.run_control.errors import (
    CommandRejected,
    IdempotencyConflict,
    RunControlNotFound,
)
from app.domain.run_control.family_admission import (
    AtomicFamilyMutation,
    FamilyAdmissionReceipt,
    FamilyVersionConflict,
)
from app.domain.run_control.reducer import (
    ReductionRejected,
    reduce_lifecycle,
    required_action_permissions,
)
from app.temporal.worker import compose_worker_run_control_service
from tests.unit.run_control.test_run_control import ConfigurationVerifier, command, request


class TestFamilyMutation(AtomicFamilyMutation):
    __test__ = False
    candidate_ref: str = Field(min_length=1)
    cursor: tuple[str, ...] = ()


def validate_test_family_batch(
    mutation: AtomicFamilyMutation,
    batch: ApplyAuthorityBatchAction,
) -> str | None:
    if not isinstance(mutation, TestFamilyMutation):
        return "unexpected mutation type"
    output_refs = tuple(
        action.evidence.output_ref
        for action in batch.actions
        if isinstance(action, RecordOutputEvidenceAction)
    )
    if output_refs != (mutation.candidate_ref,):
        return "accepted output evidence does not match the family mutation candidate"
    return None


def registered_family_registry() -> FamilyAdmissionRegistry:
    families = FamilyAdmissionRegistry()
    families.register(
        TestFamilyMutation,
        family_kind="test_family",
        mutation_kind="candidate_admitted",
        required_permission="workflow_run.reserve_budget",
        allowed_action_kinds=frozenset({"reserve_budget", "apply_authority_batch"}),
        allowed_batch_action_kinds=frozenset(
            {
                "record_usage",
                "settle_pending_usage",
                "observe_effect",
                "settle_effect",
                "record_obligation_evidence",
                "record_output_evidence",
            }
        ),
        required_batch_action_kinds=frozenset(
            {
                "record_usage",
                "settle_pending_usage",
                "observe_effect",
                "settle_effect",
                "record_obligation_evidence",
                "record_output_evidence",
            }
        ),
        batch_binding_validator=validate_test_family_batch,
    )
    return families


def admission_policies() -> AdmissionPolicyRegistry:
    policies = AdmissionPolicyRegistry()
    policies.register("contract:input@1", lambda _request, _configuration: None)
    policies.register("contract:invariant@1", lambda _request, _configuration: None)
    return policies


def family_service(
    repository: InMemoryRunControlRepository | None = None,
    *,
    register: bool = True,
) -> tuple[RunControlService, InMemoryRunControlRepository]:
    repository = repository or InMemoryRunControlRepository()
    policies = admission_policies()
    families = registered_family_registry() if register else FamilyAdmissionRegistry()
    return (
        RunControlService(repository, ConfigurationVerifier(), policies, families),
        repository,
    )


def family_mutation(
    run_id: str,
    *,
    mutation_id: str = "family-mutation-1",
    expected_family_version: int = 0,
    candidate_ref: str = "output:accepted",
    request_scope: str = "tenant-1",
) -> TestFamilyMutation:
    return TestFamilyMutation(
        family_kind="test_family",
        mutation_kind="candidate_admitted",
        mutation_id=mutation_id,
        request_scope=request_scope,
        run_id=run_id,
        expected_family_version=expected_family_version,
        exact_operation_request_ref="operation-request:sha256-test",
        decided_at=request().requested_at,
        candidate_ref=candidate_ref,
        cursor=("group:a", "candidate:a"),
    )


def authority_batch() -> ApplyAuthorityBatchAction:
    return ApplyAuthorityBatchAction(
        actions=(
            RecordUsageAction(
                usage_id="accepted-usage",
                authority_ref="operation:1",
                reservation_id="effect-reservation",
                actual_amounts={"tokens.total": 3},
                pending_external_amounts={"tokens.total": 5},
                release_amounts={"tokens.total": 2},
            ),
            SettlePendingUsageAction(
                settlement_id="accepted-usage-settlement",
                usage_id="accepted-usage",
                actual_amounts={"tokens.total": 5},
            ),
            ObserveEffectAction(
                effect_id="effect-1",
                observation_id="effect-observation-1",
                disposition=EffectDisposition.SUCCEEDED,
                evidence_refs=("evidence:effect-observed",),
            ),
            SettleEffectAction(
                effect_id="effect-1",
                settlement_id="effect-settlement-1",
                observation_id="effect-observation-1",
                outcome=EffectSettlementOutcome.SUCCEEDED,
                usage_settlement_ref="accepted-usage-settlement",
                evidence_refs=("evidence:effect-settled",),
            ),
            RecordObligationEvidenceAction(
                evidence=AcceptedObligationEvidence(
                    obligation_ref="obligation:required",
                    evidence_digest="sha256:" + "d" * 64,
                    accepted_by_authority_ref="authority:lifecycle",
                )
            ),
            RecordOutputEvidenceAction(
                evidence=AcceptedOutputEvidence(
                    output_ref="output:accepted",
                    evidence_digest="sha256:" + "e" * 64,
                    accepted_by_authority_ref="authority:lifecycle",
                )
            ),
        )
    )


async def admitted_family_run(
    repository: InMemoryRunControlRepository | None = None,
) -> tuple[RunControlService, InMemoryRunControlRepository, str]:
    run_service, repository = family_service(repository)
    admitted = await run_service.admit(request())
    assert admitted.run_id is not None
    return run_service, repository, admitted.run_id


async def prepared_authority_batch_run(
    repository: InMemoryRunControlRepository | None = None,
) -> tuple[RunControlService, InMemoryRunControlRepository, str]:
    run_service, repository, run_id = await admitted_family_run(repository)
    reserved = await run_service.execute(
        command(
            run_id,
            1,
            "reserve-for-authority-batch",
            ReserveBudgetAction(
                reservation_id="effect-reservation",
                amounts={"tokens.total": 10},
            ),
        )
    )
    assert reserved.status == CommandStatus.ACCEPTED
    claimed = await run_service.execute(
        command(
            run_id,
            2,
            "claim-for-authority-batch",
            ClaimEffectAction(
                effect_id="effect-1",
                effect_kind="external_write",
                operation_ref="operation:1",
                provider_idempotency_key="provider-effect-1",
                reservation_id="effect-reservation",
            ),
        )
    )
    assert claimed.status == CommandStatus.ACCEPTED
    return run_service, repository, run_id


@pytest.mark.asyncio
async def test_success_reservation_family_state_replay_and_outbox_order() -> None:
    run_service, repository, run_id = await admitted_family_run()
    lifecycle = command(
        run_id,
        1,
        "admit-candidate",
        ReserveBudgetAction(
            reservation_id="operation:test:1",
            amounts={"tokens.total": 10},
        ),
    )
    mutation = family_mutation(run_id)

    receipt = await run_service.execute_family_admission(lifecycle, mutation)
    replay = await run_service.execute_family_admission(
        lifecycle.model_copy(update={"occurred_at": lifecycle.occurred_at + timedelta(hours=1)}),
        mutation.model_copy(update={"decided_at": mutation.decided_at + timedelta(hours=1)}),
    )

    assert replay == receipt
    assert receipt.command_result.status == CommandStatus.ACCEPTED
    assert receipt.family_receipt is not None
    assert receipt.family_receipt.family_version == 1
    assert (await run_service.get_budget("tenant-1", run_id)).reservations[
        "operation:test:1"
    ] == {"tokens.total": 10}
    assert (
        await repository.get_family_head(
            "tenant-1", run_id, "test_family", TestFamilyMutation
        )
        == mutation
    )
    outbox = await run_service.pending_outbox("tenant-1")
    combined = [record.envelope for record in outbox if record.envelope.aggregate_version == 2]
    assert [event.event_type for event in combined] == [
        "workflow_run.reserve_budget",
        "workflow_run.family_admission_committed",
    ]
    assert [event.sequence for event in combined] == [1, 2]
    assert [event.is_version_final for event in combined] == [False, True]
    assert set(combined[-1].payload) == {
        "family_kind",
        "mutation_kind",
        "mutation_id",
        "mutation_fingerprint",
        "family_version",
        "operation_request_ref_digest",
    }


@pytest.mark.asyncio
async def test_replay_conflicts_stale_family_and_command_collisions() -> None:
    run_service, repository, run_id = await admitted_family_run()
    first_command = command(
        run_id,
        1,
        "combined",
        ReserveBudgetAction(reservation_id="first", amounts={"tokens.total": 10}),
    )
    first_mutation = family_mutation(run_id)
    await run_service.execute_family_admission(first_command, first_mutation)

    with pytest.raises(IdempotencyConflict):
        await run_service.execute_family_admission(
            first_command, first_mutation.model_copy(update={"candidate_ref": "candidate:b"})
        )
    with pytest.raises(IdempotencyConflict, match="reused by another command"):
        await run_service.execute_family_admission(
            command(
                run_id,
                2,
                "same-mutation-new-command",
                ReserveBudgetAction(reservation_id="same-mutation", amounts={"tokens.total": 1}),
            ),
            first_mutation,
        )
    with pytest.raises(IdempotencyConflict, match="conflicting content"):
        await run_service.execute_family_admission(
            command(
                run_id,
                2,
                "conflicting-mutation-new-command",
                ReserveBudgetAction(
                    reservation_id="conflicting-mutation",
                    amounts={"tokens.total": 1},
                ),
            ),
            first_mutation.model_copy(update={"candidate_ref": "candidate:b"}),
        )
    with pytest.raises(IdempotencyConflict):
        await run_service.execute(first_command)

    version_before = (await run_service.get_run("tenant-1", run_id)).version
    with pytest.raises(FamilyVersionConflict):
        await run_service.execute_family_admission(
            command(
                run_id,
                version_before,
                "stale-family",
                ReserveBudgetAction(reservation_id="stale", amounts={"tokens.total": 5}),
            ),
            family_mutation(
                run_id,
                mutation_id="stale-family-mutation",
                expected_family_version=0,
            ),
        )
    assert (await run_service.get_run("tenant-1", run_id)).version == version_before
    assert "stale" not in (await run_service.get_budget("tenant-1", run_id)).reservations

    plain = command(
        run_id,
        version_before,
        "plain-first",
        ReserveBudgetAction(reservation_id="plain", amounts={"tokens.total": 1}),
    )
    await run_service.execute(plain)
    with pytest.raises(IdempotencyConflict):
        await run_service.execute_family_admission(
            plain,
            family_mutation(
                run_id,
                mutation_id="plain-collision",
                expected_family_version=1,
            ),
        )
    assert len(repository._family_journal) == 1


@pytest.mark.asyncio
async def test_parent_rollup_authority_change_is_re_read_and_reduced() -> None:
    class PausingRepository(InMemoryRunControlRepository):
        def __init__(self) -> None:
            super().__init__()
            self.target_run_id: str | None = None
            self.stale_budget_read = asyncio.Event()
            self.resume_family_read = asyncio.Event()
            self.paused = False

        async def get_effects(self, request_scope: str, run_id: str):  # type: ignore[no-untyped-def]
            if run_id == self.target_run_id and not self.paused:
                self.paused = True
                self.stale_budget_read.set()
                await self.resume_family_read.wait()
            return await super().get_effects(request_scope, run_id)

    repository = PausingRepository()
    run_service, _ = family_service(repository)
    parent = await run_service.admit(request(request_id="authority-parent"))
    assert parent.run_id is not None
    parent_run_id = parent.run_id
    repository.target_run_id = parent_run_id
    parent_budget = await run_service.get_budget("tenant-1", parent_run_id)
    child_request = request(request_id="authority-child")
    child_request = child_request.model_copy(
        update={
            "parent_run_id": parent_run_id,
            "actor": child_request.actor.model_copy(
                update={
                    "authority_refs": child_request.actor.authority_refs
                    | {f"workflow_run.parent:{parent_run_id}:sponsor"}
                }
            ),
            "budget_envelope": child_request.budget_envelope.model_copy(
                update={"parent_account_id": parent_budget.account_id}
            ),
        }
    )

    family_task = asyncio.create_task(
        run_service.execute_family_admission(
            command(
                parent_run_id,
                1,
                "authority-family",
                ReserveBudgetAction(
                    reservation_id="authority-family",
                    amounts={"tokens.total": 10},
                ),
            ),
            family_mutation(parent_run_id),
        )
    )
    await repository.stale_budget_read.wait()
    child = await run_service.admit(child_request)
    assert child.run_id is not None
    repository.resume_family_read.set()
    receipt = await family_task

    assert receipt.command_result.status == CommandStatus.ACCEPTED
    final_budget = await run_service.get_budget("tenant-1", parent_run_id)
    assert final_budget.reserved["tokens.total"] == 50
    assert final_budget.reservations["authority-family"] == {"tokens.total": 10}


@pytest.mark.asyncio
async def test_plain_command_retries_after_concurrent_parent_rollup() -> None:
    class PausingRepository(InMemoryRunControlRepository):
        def __init__(self) -> None:
            super().__init__()
            self.target_run_id: str | None = None
            self.stale_budget_read = asyncio.Event()
            self.resume_command_read = asyncio.Event()
            self.paused = False

        async def get_effects(self, request_scope: str, run_id: str):  # type: ignore[no-untyped-def]
            if run_id == self.target_run_id and not self.paused:
                self.paused = True
                self.stale_budget_read.set()
                await self.resume_command_read.wait()
            return await super().get_effects(request_scope, run_id)

    repository = PausingRepository()
    run_service, _ = family_service(repository)
    parent = await run_service.admit(request(request_id="plain-authority-parent"))
    assert parent.run_id is not None
    parent_run_id = parent.run_id
    repository.target_run_id = parent_run_id
    parent_budget = await run_service.get_budget("tenant-1", parent_run_id)
    child_request = request(request_id="plain-authority-child")
    child_request = child_request.model_copy(
        update={
            "parent_run_id": parent_run_id,
            "actor": child_request.actor.model_copy(
                update={
                    "authority_refs": child_request.actor.authority_refs
                    | {f"workflow_run.parent:{parent_run_id}:sponsor"}
                }
            ),
            "budget_envelope": child_request.budget_envelope.model_copy(
                update={"parent_account_id": parent_budget.account_id}
            ),
        }
    )

    command_task = asyncio.create_task(
        run_service.execute(
            command(
                parent_run_id,
                1,
                "plain-authority-command",
                ReserveBudgetAction(
                    reservation_id="plain-authority-command",
                    amounts={"tokens.total": 10},
                ),
            )
        )
    )
    await repository.stale_budget_read.wait()
    child = await run_service.admit(child_request)
    assert child.run_id is not None
    repository.resume_command_read.set()
    result = await command_task

    assert result.status == CommandStatus.ACCEPTED
    final_budget = await run_service.get_budget("tenant-1", parent_run_id)
    assert final_budget.reserved["tokens.total"] == 50
    assert final_budget.reservations["plain-authority-command"] == {
        "tokens.total": 10
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("combined", [False, True])
async def test_stale_rejection_is_reduced_again_and_never_replayed(
    combined: bool,
) -> None:
    class PausingRepository(InMemoryRunControlRepository):
        def __init__(self) -> None:
            super().__init__()
            self.target_run_id: str | None = None
            self.stale_budget_read = asyncio.Event()
            self.resume_command_read = asyncio.Event()
            self.paused = False

        async def get_effects(self, request_scope: str, run_id: str):  # type: ignore[no-untyped-def]
            if run_id == self.target_run_id and not self.paused:
                self.paused = True
                self.stale_budget_read.set()
                await self.resume_command_read.wait()
            return await super().get_effects(request_scope, run_id)

    repository = PausingRepository()
    run_service, _ = family_service(repository)
    parent = await run_service.admit(request(request_id=f"rejection-parent-{combined}"))
    assert parent.run_id is not None
    parent_run_id = parent.run_id
    parent_budget = await run_service.get_budget("tenant-1", parent_run_id)
    child_request = request(request_id=f"rejection-child-{combined}")
    child_request = child_request.model_copy(
        update={
            "parent_run_id": parent_run_id,
            "actor": child_request.actor.model_copy(
                update={
                    "authority_refs": child_request.actor.authority_refs
                    | {f"workflow_run.parent:{parent_run_id}:sponsor"}
                }
            ),
            "budget_envelope": child_request.budget_envelope.model_copy(
                update={"parent_account_id": parent_budget.account_id}
            ),
        }
    )
    child = await run_service.admit(child_request)
    assert child.run_id is not None
    repository.target_run_id = parent_run_id
    lifecycle = command(
        parent_run_id,
        1,
        f"stale-rejection-{combined}",
        ReserveBudgetAction(
            reservation_id=f"stale-rejection-{combined}",
            amounts={"tokens.total": 70},
        ),
    )
    mutation = family_mutation(
        parent_run_id,
        mutation_id=f"stale-rejection-{combined}",
    )
    if combined:
        task = asyncio.create_task(
            run_service.execute_family_admission(lifecycle, mutation)
        )
    else:
        task = asyncio.create_task(run_service.execute(lifecycle))

    await repository.stale_budget_read.wait()
    released = await run_service.execute(
        command(
            child.run_id,
            1,
            f"release-child-{combined}",
            RecordUsageAction(
                usage_id=f"release-child-{combined}",
                reservation_id="baseline",
                actual_amounts={},
                release_amounts={"tokens.total": 20},
            ),
        )
    )
    assert released.status == CommandStatus.ACCEPTED
    repository.resume_command_read.set()
    result_or_receipt = await task
    result = (
        result_or_receipt.command_result
        if isinstance(result_or_receipt, FamilyAdmissionReceipt)
        else result_or_receipt
    )
    assert result.status == CommandStatus.ACCEPTED
    assert (
        await run_service.get_budget("tenant-1", parent_run_id)
    ).reservations[f"stale-rejection-{combined}"] == {"tokens.total": 70}

    if combined:
        replay_receipt = await run_service.execute_family_admission(lifecycle, mutation)
        assert replay_receipt == result_or_receipt
        assert replay_receipt.command_result.status == CommandStatus.ACCEPTED
    else:
        replay_result = await run_service.execute(lifecycle)
        assert replay_result == result
        assert replay_result.status == CommandStatus.ACCEPTED


@pytest.mark.asyncio
async def test_concurrent_plain_commands_return_accepted_and_stable_stale_results() -> None:
    run_service, _repository, run_id = await admitted_family_run()
    commands = (
        command(
            run_id,
            1,
            "plain-concurrent-one",
            ReserveBudgetAction(
                reservation_id="plain-concurrent-one", amounts={"tokens.total": 1}
            ),
        ),
        command(
            run_id,
            1,
            "plain-concurrent-two",
            ReserveBudgetAction(
                reservation_id="plain-concurrent-two", amounts={"tokens.total": 1}
            ),
        ),
    )
    results = await asyncio.gather(*(run_service.execute(item) for item in commands))
    assert {result.status for result in results} == {
        CommandStatus.ACCEPTED,
        CommandStatus.STALE,
    }
    stale_index = next(
        index
        for index, result in enumerate(results)
        if result.status == CommandStatus.STALE
    )
    assert await run_service.execute(commands[stale_index]) == results[stale_index]


@pytest.mark.asyncio
async def test_concurrent_admissions_return_one_accepted_and_one_stable_stale_receipt() -> None:
    run_service, _repository, run_id = await admitted_family_run()
    commands = (
        command(
            run_id,
            1,
            "concurrent-one",
            ReserveBudgetAction(reservation_id="concurrent-one", amounts={"tokens.total": 1}),
        ),
        command(
            run_id,
            1,
            "concurrent-two",
            ReserveBudgetAction(reservation_id="concurrent-two", amounts={"tokens.total": 1}),
        ),
    )
    mutations = (
        family_mutation(run_id, mutation_id="concurrent-one"),
        family_mutation(run_id, mutation_id="concurrent-two"),
    )
    receipts = await asyncio.gather(
        *(
            run_service.execute_family_admission(lifecycle, mutation)
            for lifecycle, mutation in zip(commands, mutations, strict=True)
        )
    )

    assert {receipt.command_result.status for receipt in receipts} == {
        CommandStatus.ACCEPTED,
        CommandStatus.STALE,
    }
    stale_index = next(
        index
        for index, receipt in enumerate(receipts)
        if receipt.command_result.status == CommandStatus.STALE
    )
    assert (
        await run_service.execute_family_admission(
            commands[stale_index], mutations[stale_index]
        )
        == receipts[stale_index]
    )


@pytest.mark.asyncio
async def test_family_admission_commit_rejects_all_cross_bound_identity_tampering() -> None:
    class CapturingRepository(InMemoryRunControlRepository):
        last_commit = None

        async def commit_family_admission(self, commit):  # type: ignore[no-untyped-def]
            self.last_commit = commit
            return await super().commit_family_admission(commit)

    repository = CapturingRepository()
    run_service, _repository, run_id = await admitted_family_run(repository)
    await run_service.execute_family_admission(
        command(
            run_id,
            1,
            "invariant-source",
            ReserveBudgetAction(reservation_id="invariant-source", amounts={"tokens.total": 1}),
        ),
        family_mutation(run_id),
    )
    commit = repository.last_commit
    assert commit is not None

    with pytest.raises(ValueError, match="fingerprint does not match"):
        replace(commit, family_mutation_fingerprint="sha256:" + "f" * 64)
    with pytest.raises(ValueError, match="identities differ"):
        replace(
            commit,
            command=replace(commit.command, request_scope="tenant-2"),
        )
    with pytest.raises(ValueError, match="run-control mutation"):
        replace(
            commit,
            command=replace(commit.command, projection=None),
            receipt=commit.receipt,
        )
    assert commit.receipt.family_receipt is not None
    invalid_family_receipt = commit.receipt.family_receipt.model_copy(
        update={"family_version": commit.receipt.family_receipt.family_version + 1}
    )
    with pytest.raises(ValueError, match="accepted family receipt"):
        replace(
            commit,
            receipt=commit.receipt.model_copy(
                update={"family_receipt": invalid_family_receipt}
            ),
        )


@pytest.mark.asyncio
async def test_registry_permission_action_scope_and_run_binding_rejections() -> None:
    run_service, _repository, run_id = await admitted_family_run()
    reserve = command(
        run_id,
        1,
        "validation",
        ReserveBudgetAction(reservation_id="validation", amounts={"tokens.total": 1}),
    )

    unregistered, _ = family_service(register=False)
    with pytest.raises(CommandRejected, match="no exact"):
        await unregistered.execute_family_admission(reserve, family_mutation(run_id))

    unauthorized = reserve.model_copy(
        update={
            "actor": reserve.actor.model_copy(
                update={
                    "permissions": reserve.actor.permissions
                    - {"workflow_run.reserve_budget"}
                }
            )
        }
    )
    with pytest.raises(CommandRejected, match="lacks"):
        await run_service.execute_family_admission(unauthorized, family_mutation(run_id))
    with pytest.raises(CommandRejected, match="not allowed"):
        await run_service.execute_family_admission(
            command(run_id, 1, "bad-action", StartAction()),
            family_mutation(run_id, mutation_id="bad-action"),
        )
    with pytest.raises(CommandRejected, match="scope and run"):
        await run_service.execute_family_admission(
            reserve,
            family_mutation(run_id, request_scope="tenant-2"),
        )
    with pytest.raises(CommandRejected, match="scope and run"):
        await run_service.execute_family_admission(
            reserve,
            family_mutation("another-run"),
        )
    with pytest.raises(CommandRejected, match="registered policy"):
        await run_service.execute_family_admission(
            reserve,
            family_mutation(run_id).model_copy(update={"family_kind": "forged_family"}),
        )
    with pytest.raises(CommandRejected, match="registered policy"):
        await run_service.execute_family_admission(
            reserve,
            family_mutation(run_id).model_copy(update={"mutation_kind": "forged_mutation"}),
        )


@pytest.mark.asyncio
async def test_api_and_worker_composition_receive_public_family_registry_hook() -> None:
    registry = registered_family_registry()
    application = FastAPI()
    configure_family_admission_registry(application, registry)
    api_repository = InMemoryRunControlRepository()
    api_service = compose_api_run_control_service(
        application,
        api_repository,
        ConfigurationVerifier(),
        admission_policies(),
    )
    api_admitted = await api_service.admit(request(request_id="api-family-hook"))
    assert api_admitted.run_id is not None
    api_receipt = await api_service.execute_family_admission(
        command(
            api_admitted.run_id,
            1,
            "api-family-hook",
            ReserveBudgetAction(
                reservation_id="api-family-hook",
                amounts={"tokens.total": 1},
            ),
        ),
        family_mutation(api_admitted.run_id, mutation_id="api-family-hook"),
    )
    assert api_receipt.command_result.status == CommandStatus.ACCEPTED

    worker_repository = InMemoryRunControlRepository()
    worker_service = compose_worker_run_control_service(
        worker_repository,
        ConfigurationVerifier(),
        admission_policies(),
        registry,
    )
    worker_admitted = await worker_service.admit(request(request_id="worker-family-hook"))
    assert worker_admitted.run_id is not None
    worker_receipt = await worker_service.execute_family_admission(
        command(
            worker_admitted.run_id,
            1,
            "worker-family-hook",
            ReserveBudgetAction(
                reservation_id="worker-family-hook",
                amounts={"tokens.total": 1},
            ),
        ),
        family_mutation(worker_admitted.run_id, mutation_id="worker-family-hook"),
    )
    assert worker_receipt.command_result.status == CommandStatus.ACCEPTED

    empty_application = FastAPI()
    empty_repository = InMemoryRunControlRepository()
    empty_service = compose_api_run_control_service(
        empty_application,
        empty_repository,
        ConfigurationVerifier(),
        admission_policies(),
    )
    empty_admitted = await empty_service.admit(request(request_id="api-empty-family-hook"))
    assert empty_admitted.run_id is not None
    with pytest.raises(CommandRejected, match="no exact"):
        await empty_service.execute_family_admission(
            command(
                empty_admitted.run_id,
                1,
                "api-empty-family-hook",
                ReserveBudgetAction(
                    reservation_id="api-empty-family-hook",
                    amounts={"tokens.total": 1},
                ),
            ),
            family_mutation(
                empty_admitted.run_id,
                mutation_id="api-empty-family-hook",
            ),
        )

    application.state.run_control_service = api_service
    with pytest.raises(RuntimeError, match="before run control"):
        configure_family_admission_registry(application, registry)


@pytest.mark.asyncio
async def test_family_reads_hide_missing_and_cross_scope_runs_consistently() -> None:
    run_service, repository, run_id = await admitted_family_run()
    for scope, target_run in (
        ("tenant-2", run_id),
        ("tenant-1", "missing-run"),
    ):
        with pytest.raises(RunControlNotFound):
            await repository.get_family_admission_receipt(
                scope, target_run, "operator", "missing-command"
            )
        with pytest.raises(RunControlNotFound):
            await repository.get_family_head(
                scope, target_run, "test_family", TestFamilyMutation
            )
    assert (
        await repository.get_family_admission_receipt(
            "tenant-1", run_id, "operator", "missing-command"
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary",
    ["family_admission.after_run_control", "family_admission.after_family"],
)
async def test_failure_boundaries_roll_back_every_store(boundary: str) -> None:
    async def fail(observed: str) -> None:
        if observed == boundary:
            raise RuntimeError("injected family admission failure")

    repository = InMemoryRunControlRepository(before_commit=fail)
    run_service, repository, run_id = await admitted_family_run(repository)
    before_outbox = await run_service.pending_outbox("tenant-1")
    before_transitions = await repository.list_transitions("tenant-1", run_id)
    before_ledger = await repository.list_budget_ledger("tenant-1", run_id)
    with pytest.raises(RuntimeError, match="injected"):
        await run_service.execute_family_admission(
            command(
                run_id,
                1,
                "rollback",
                ReserveBudgetAction(reservation_id="rollback", amounts={"tokens.total": 10}),
            ),
            family_mutation(run_id),
        )
    assert (await run_service.get_run("tenant-1", run_id)).version == 1
    assert "rollback" not in (await run_service.get_budget("tenant-1", run_id)).reservations
    assert await run_service.pending_outbox("tenant-1") == before_outbox
    assert await repository.list_transitions("tenant-1", run_id) == before_transitions
    assert await repository.list_budget_ledger("tenant-1", run_id) == before_ledger
    assert await repository.list_effect_ledger("tenant-1", run_id) == ()
    assert repository._commands == {}
    assert repository._family_heads == {}
    assert repository._family_journal == {}
    assert repository._family_results == {}


def test_family_mutations_inherit_payload_and_sensitive_data_bounds() -> None:
    with pytest.raises(ValidationError, match="raw secrets"):
        TestFamilyMutation.model_validate(
            {
                **family_mutation("run").model_dump(),
                "api_key": "secret-value",
            }
        )
    with pytest.raises(ValidationError, match="8192"):
        TestFamilyMutation.model_validate(
            {
                **family_mutation("run").model_dump(),
                "candidate_ref": "x" * 8193,
            }
        )
    with pytest.raises(ValidationError, match="serialized bytes"):
        TestFamilyMutation.model_validate(
            {
                **family_mutation("run").model_dump(),
                "cursor": tuple("x" * 7000 for _ in range(10)),
            }
        )
    with pytest.raises(ValidationError, match="string_pattern_mismatch"):
        TestFamilyMutation.model_validate(
            {
                **family_mutation("run").model_dump(),
                "exact_operation_request_ref": "https://example.test/private/material",
            }
        )


def test_migration_has_private_repository_dml_and_no_attachment_function() -> None:
    migration = (
        Path(__file__).parents[1]
        / "app"
        / "migrations"
        / "0017_atomic_family_admission_v1.sql"
    ).read_text(encoding="utf-8")
    assert "CREATE FUNCTION" not in migration
    assert "commit_family_admission(" not in migration
    assert "belllabs_family_repository_writer" in migration
    assert "FROM PUBLIC, belllabs_control_runtime" in migration
    assert "GRANT belllabs_family_repository_writer TO belllabs_app" not in migration
    repository = (
        Path(__file__).parents[1]
        / "app"
        / "application"
        / "postgres_run_control_repository.py"
    ).read_text(encoding="utf-8")
    assert "family_writer_pool: asyncpg.Pool | None = None" in repository
    assert "self._family_writer_pool.acquire()" in repository
    assert "SET LOCAL ROLE belllabs_family_repository_writer" not in repository


def test_operation_settlement_revision_key_uses_forward_migration() -> None:
    migration = (
        Path(__file__).parents[1]
        / "app"
        / "migrations"
        / "0018_operation_settlement_revisions_v1.sql"
    ).read_text(encoding="utf-8")
    assert "DROP CONSTRAINT IF EXISTS operation_settlements_pkey" in migration
    assert (
        "PRIMARY KEY (request_scope, settlement_id, settlement_revision)"
        in migration
    )
    assert "pending_candidates" in migration
    assert "'{usage_records}'" in migration
    assert "'{outstanding_usage_ids}'" in migration
    original = (
        Path(__file__).parents[1]
        / "app"
        / "migrations"
        / "0012_graph_runtime_operation_journal.sql"
    ).read_text(encoding="utf-8")
    assert "PRIMARY KEY (request_scope, settlement_id)" in original


@pytest.mark.asyncio
async def test_authority_batch_commits_one_version_all_authority_and_ordered_outbox() -> None:
    run_service, repository, run_id = await prepared_authority_batch_run()
    batch = authority_batch()
    lifecycle = command(run_id, 3, "authority-batch", batch)
    mutation = family_mutation(run_id, mutation_id="authority-batch")

    receipt = await run_service.execute_family_admission(lifecycle, mutation)
    replay = await run_service.execute_family_admission(
        lifecycle.model_copy(update={"occurred_at": lifecycle.occurred_at + timedelta(hours=1)}),
        mutation.model_copy(update={"decided_at": mutation.decided_at + timedelta(hours=1)}),
    )

    assert replay == receipt
    assert receipt.command_result.status == CommandStatus.ACCEPTED
    assert receipt.command_result.resulting_run_version == 4
    assert receipt.family_receipt is not None
    assert receipt.family_receipt.family_version == 1
    projection = await run_service.get_run("tenant-1", run_id)
    assert projection.version == 4
    assert projection.phase.value == "pending"
    assert [item.obligation_ref for item in projection.accepted_obligation_evidence] == [
        "obligation:required"
    ]
    assert [item.output_ref for item in projection.accepted_output_evidence] == ["output:accepted"]
    budget = await run_service.get_budget("tenant-1", run_id)
    assert budget.consumed["tokens.total"] == 8
    assert budget.pending_settlement["tokens.total"] == 0
    assert "effect-reservation" not in budget.reservations
    usage_settlement = budget.usage_settlements["accepted-usage-settlement"]
    assert usage_settlement.usage_id == "accepted-usage"
    assert usage_settlement.reservation_id == "effect-reservation"
    assert usage_settlement.source_pending_amounts == {"tokens.total": 5}
    assert usage_settlement.provenance_digest == sha256_digest(
        {
            "settlement_id": "accepted-usage-settlement",
            "usage_id": "accepted-usage",
            "reservation_id": "effect-reservation",
            "authority_ref": "operation:1",
            "settled_amounts": {"tokens.total": 5},
            "released_amounts": {},
            "source_pending_amounts": {"tokens.total": 5},
        }
    )
    effects = await run_service.get_effects("tenant-1", run_id)
    assert effects.claims["effect-1"].settlement is not None
    transitions = await repository.list_transitions("tenant-1", run_id)
    assert [(item.prior_version, item.resulting_version) for item in transitions] == [
        (0, 1),
        (1, 2),
        (2, 3),
        (3, 4),
    ]
    budget_entries = await repository.list_budget_ledger("tenant-1", run_id)
    assert [item.kind.value for item in budget_entries[-4:]] == [
        "consumption",
        "pending_settlement",
        "release",
        "settlement",
    ]
    assert [item.kind for item in await repository.list_effect_ledger("tenant-1", run_id)][-2:] == [
        "observation",
        "settlement",
    ]
    combined = [
        record.envelope
        for record in await run_service.pending_outbox("tenant-1")
        if record.envelope.aggregate_version == 4
    ]
    assert [event.event_type for event in combined] == [
        "workflow_run.apply_authority_batch",
        "workflow_run.family_admission_committed",
    ]
    assert [event.sequence for event in combined] == [1, 2]
    assert [event.is_version_final for event in combined] == [False, True]
    batch_event = combined[0]
    assert batch_event.payload["authority_batch_digest"] == sha256_digest(batch)
    assert batch_event.payload["action_identity_summary"] == batch.canonical_identity_summary()
    assert batch_event.payload["action_identity_summary"] == (
        {
            "action_kind": "record_usage",
            "usage_id_digest": sha256_digest("accepted-usage"),
        },
        {
            "action_kind": "settle_pending_usage",
            "settlement_id_digest": sha256_digest("accepted-usage-settlement"),
        },
        {
            "action_kind": "observe_effect",
            "effect_id_digest": sha256_digest("effect-1"),
            "observation_id_digest": sha256_digest("effect-observation-1"),
        },
        {
            "action_kind": "settle_effect",
            "effect_id_digest": sha256_digest("effect-1"),
            "observation_id_digest": sha256_digest("effect-observation-1"),
            "settlement_id_digest": sha256_digest("effect-settlement-1"),
            "usage_settlement_ref_digest": sha256_digest(
                "accepted-usage-settlement"
            ),
        },
        {
            "action_kind": "record_obligation_evidence",
            "obligation_ref_digest": sha256_digest("obligation:required"),
        },
        {
            "action_kind": "record_output_evidence",
            "output_ref_digest": sha256_digest("output:accepted"),
        },
    )
    records = await run_service.pending_outbox("tenant-1")
    for record in records:
        applied = await run_service.apply_consumer_event(
            "tenant-1",
            "authority-batch-consumer",
            record.envelope,
        )
        assert applied.status == ConsumerApplyStatus.APPLIED
    redelivery = await run_service.apply_consumer_event(
        "tenant-1",
        "authority-batch-consumer",
        batch_event,
    )
    assert redelivery.status == ConsumerApplyStatus.DUPLICATE
    replayed_batch_event = next(
        record.envelope
        for record in await run_service.pending_outbox("tenant-1")
        if record.envelope.event_id == batch_event.event_id
    )
    assert replayed_batch_event == batch_event


@pytest.mark.asyncio
async def test_authority_batch_rejection_rolls_back_and_replays_without_family_advance() -> None:
    run_service, repository, run_id = await prepared_authority_batch_run()
    invalid = authority_batch().model_copy(
        update={
            "actions": (
                authority_batch().actions[0],
                SettlePendingUsageAction(
                    settlement_id="accepted-usage-settlement",
                    usage_id="accepted-usage",
                    actual_amounts={"tokens.total": 6},
                ),
                *authority_batch().actions[2:],
            )
        }
    )
    lifecycle = command(run_id, 3, "rejected-authority-batch", invalid)
    mutation = family_mutation(run_id, mutation_id="rejected-authority-batch")
    before_budget = await run_service.get_budget("tenant-1", run_id)
    before_effects = await run_service.get_effects("tenant-1", run_id)

    receipt = await run_service.execute_family_admission(lifecycle, mutation)
    replay = await run_service.execute_family_admission(lifecycle, mutation)

    assert replay == receipt
    assert receipt.command_result.status == CommandStatus.REJECTED
    assert receipt.command_result.reason_code == "usage_settlement_provenance_mismatch"
    assert receipt.family_receipt is None
    assert (await run_service.get_run("tenant-1", run_id)).version == 3
    assert await run_service.get_budget("tenant-1", run_id) == before_budget
    assert await run_service.get_effects("tenant-1", run_id) == before_effects
    assert len(await repository.list_transitions("tenant-1", run_id)) == 3
    assert await repository.get_family_head(
        "tenant-1", run_id, "test_family", TestFamilyMutation
    ) is None


@pytest.mark.asyncio
async def test_authority_batch_requires_nested_permissions_and_detects_collision() -> None:
    run_service, repository, run_id = await prepared_authority_batch_run()
    lifecycle = command(run_id, 3, "permission-batch", authority_batch())
    unauthorized = lifecycle.model_copy(
        update={
            "actor": lifecycle.actor.model_copy(
                update={
                    "permissions": lifecycle.actor.permissions
                    - {"workflow_run.accept_output_evidence"}
                }
            )
        }
    )
    with pytest.raises(CommandRejected, match="accept_output_evidence"):
        await run_service.execute_family_admission(
            unauthorized, family_mutation(run_id, mutation_id="permission-batch")
        )
    assert (await run_service.get_run("tenant-1", run_id)).version == 3
    assert repository._family_results == {}

    mutation = family_mutation(run_id, mutation_id="permission-batch")
    await run_service.execute_family_admission(lifecycle, mutation)
    changed_output = authority_batch().actions[-1].model_copy(
        update={
            "evidence": authority_batch().actions[-1].evidence.model_copy(
                update={"evidence_digest": "sha256:" + "f" * 64}
            )
        }
    )
    collision = lifecycle.model_copy(
        update={
            "action": authority_batch().model_copy(
                update={"actions": (*authority_batch().actions[:-1], changed_output)}
            )
        }
    )
    with pytest.raises(IdempotencyConflict, match="lifecycle command"):
        await run_service.execute_family_admission(collision, mutation)
    assert (await run_service.get_run("tenant-1", run_id)).version == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "boundary",
    ["family_admission.after_run_control", "family_admission.after_family"],
)
async def test_authority_batch_transaction_failure_rolls_back_every_store(
    boundary: str,
) -> None:
    async def fail(observed: str) -> None:
        if observed == boundary:
            raise RuntimeError("injected authority batch failure")

    repository = InMemoryRunControlRepository(before_commit=fail)
    run_service, repository, run_id = await prepared_authority_batch_run(repository)
    before_projection = await run_service.get_run("tenant-1", run_id)
    before_budget = await run_service.get_budget("tenant-1", run_id)
    before_effects = await run_service.get_effects("tenant-1", run_id)
    before_transitions = await repository.list_transitions("tenant-1", run_id)
    before_budget_ledger = await repository.list_budget_ledger("tenant-1", run_id)
    before_effect_ledger = await repository.list_effect_ledger("tenant-1", run_id)
    before_outbox = await run_service.pending_outbox("tenant-1")

    with pytest.raises(RuntimeError, match="injected authority batch failure"):
        await run_service.execute_family_admission(
            command(run_id, 3, f"rollback-authority-batch-{boundary}", authority_batch()),
            family_mutation(
                run_id,
                mutation_id=f"rollback-authority-batch-{boundary}",
            ),
        )

    assert await run_service.get_run("tenant-1", run_id) == before_projection
    assert await run_service.get_budget("tenant-1", run_id) == before_budget
    assert await run_service.get_effects("tenant-1", run_id) == before_effects
    assert await repository.list_transitions("tenant-1", run_id) == before_transitions
    assert await repository.list_budget_ledger("tenant-1", run_id) == before_budget_ledger
    assert await repository.list_effect_ledger("tenant-1", run_id) == before_effect_ledger
    assert await run_service.pending_outbox("tenant-1") == before_outbox
    assert repository._family_heads == {}
    assert repository._family_journal == {}
    assert repository._family_results == {}


def test_authority_batch_is_non_empty_bounded_closed_unique_and_ordered() -> None:
    with pytest.raises(ValidationError, match="at least 1"):
        ApplyAuthorityBatchAction(actions=())
    with pytest.raises(ValidationError, match="at most 64"):
        ApplyAuthorityBatchAction(
            actions=tuple(
                RecordUsageAction(
                    usage_id=f"usage-{index:02d}",
                    reservation_id="baseline",
                    actual_amounts={},
                )
                for index in range(65)
            )
        )
    with pytest.raises(ValidationError, match="canonical deterministic ordering"):
        ApplyAuthorityBatchAction(actions=tuple(reversed(authority_batch().actions)))
    duplicate_output = authority_batch().actions[-1]
    with pytest.raises(ValidationError, match="identities must be unique"):
        ApplyAuthorityBatchAction(actions=(duplicate_output, duplicate_output))
    with pytest.raises(ValidationError, match="record_output_evidence"):
        ApplyAuthorityBatchAction.model_validate(
            {
                "actions": [
                    ReserveBudgetAction(
                        reservation_id="forbidden",
                        amounts={"tokens.total": 1},
                    ).model_dump(mode="json")
                ]
            }
        )


@pytest.mark.asyncio
async def test_malformed_copied_batches_and_unknown_permissions_fail_closed() -> None:
    run_service, repository, run_id = await prepared_authority_batch_run()
    mutation = family_mutation(run_id, mutation_id="malformed-batch")
    malformed_actions = (
        authority_batch().model_copy(update={"actions": ()}),
        authority_batch().model_copy(update={"actions": (StartAction(),)}),
    )
    for index, malformed in enumerate(malformed_actions):
        malformed_command = command(
            run_id,
            3,
            f"malformed-batch-{index}",
            authority_batch(),
        ).model_copy(update={"action": malformed})
        with pytest.raises(CommandRejected, match="strict contract revalidation"):
            await run_service.execute_family_admission(
                malformed_command,
                mutation.model_copy(update={"mutation_id": f"malformed-batch-{index}"}),
            )
    with pytest.raises(ValueError, match="no permission mapping"):
        required_action_permissions(SimpleNamespace(kind="unknown_action"))
    with pytest.raises(ValueError, match="no permission mapping"):
        required_action_permissions(SimpleNamespace(kind=None))
    with pytest.raises(ReductionRejected) as rejected:
        reduce_lifecycle(
            await run_service.get_run("tenant-1", run_id),
            await run_service.get_budget("tenant-1", run_id),
            await run_service.get_effects("tenant-1", run_id),
            malformed_command,
            "sha256:" + "f" * 64,
        )
    assert rejected.value.code == "invalid_lifecycle_command"
    assert (await run_service.get_run("tenant-1", run_id)).version == 3
    assert repository._family_results == {}


@pytest.mark.asyncio
async def test_family_mutation_is_strictly_revalidated_before_policy_and_persistence() -> None:
    class OtherFamilyMutation(TestFamilyMutation):
        pass

    run_service, repository, run_id = await admitted_family_run()
    lifecycle = command(
        run_id,
        1,
        "strict-family-mutation",
        ReserveBudgetAction(
            reservation_id="strict-family-mutation",
            amounts={"tokens.total": 1},
        ),
    )
    malformed = (
        family_mutation(run_id).model_copy(update={"family_kind": "INVALID FAMILY"}),
        family_mutation(run_id).model_copy(update={"candidate_ref": "x" * 70_000}),
    )
    for mutation in malformed:
        with pytest.raises(CommandRejected, match="strict exact-type"):
            await run_service.execute_family_admission(lifecycle, mutation)
    with pytest.raises(CommandRejected, match="no exact family mutation registration"):
        await run_service.execute_family_admission(
            lifecycle,
            OtherFamilyMutation.model_validate(family_mutation(run_id).model_dump()),
        )
    assert (await run_service.get_run("tenant-1", run_id)).version == 1
    assert repository._family_results == {}

    exact = family_mutation(run_id, mutation_id="strict-family-mutation")
    receipt = await run_service.execute_family_admission(lifecycle, exact)
    replay = await run_service.execute_family_admission(lifecycle, exact)
    assert replay == receipt
    assert receipt.command_result.status == CommandStatus.ACCEPTED


def test_batch_family_policy_requires_nested_constraints_and_reference_binding() -> None:
    with pytest.raises(ValueError, match="must bind nested actions and references"):
        families = FamilyAdmissionRegistry()
        families.register(
            TestFamilyMutation,
            family_kind="test_family",
            mutation_kind="candidate_admitted",
            required_permission="workflow_run.reserve_budget",
            allowed_action_kinds=frozenset({"apply_authority_batch"}),
        )
    with pytest.raises(ValueError, match="subset"):
        families = FamilyAdmissionRegistry()
        families.register(
            TestFamilyMutation,
            family_kind="test_family",
            mutation_kind="candidate_admitted",
            required_permission="workflow_run.reserve_budget",
            allowed_action_kinds=frozenset({"apply_authority_batch"}),
            allowed_batch_action_kinds=frozenset({"record_output_evidence"}),
            required_batch_action_kinds=frozenset({"record_usage"}),
            batch_binding_validator=validate_test_family_batch,
        )


@pytest.mark.asyncio
async def test_batch_family_policy_rejects_omissions_and_mutation_reference_mismatch() -> None:
    run_service, repository, run_id = await prepared_authority_batch_run()
    omitted_output = ApplyAuthorityBatchAction(actions=authority_batch().actions[:-1])
    with pytest.raises(CommandRejected, match="omits actions required"):
        await run_service.execute_family_admission(
            command(run_id, 3, "policy-omission", omitted_output),
            family_mutation(run_id, mutation_id="policy-omission"),
        )
    with pytest.raises(CommandRejected, match="binding failed"):
        await run_service.execute_family_admission(
            command(run_id, 3, "policy-reference-mismatch", authority_batch()),
            family_mutation(
                run_id,
                mutation_id="policy-reference-mismatch",
                candidate_ref="output:not-accepted",
            ),
        )
    assert (await run_service.get_run("tenant-1", run_id)).version == 3
    assert repository._family_results == {}


@pytest.mark.asyncio
async def test_effect_settlement_rejects_spoofed_usage_settlement_reference() -> None:
    run_service, repository, run_id = await prepared_authority_batch_run()
    actions = list(authority_batch().actions)
    actions[3] = actions[3].model_copy(
        update={"usage_settlement_ref": "settlement:spoofed"}
    )
    spoofed = ApplyAuthorityBatchAction(actions=tuple(actions))
    lifecycle = command(run_id, 3, "spoofed-settlement", spoofed)
    mutation = family_mutation(run_id, mutation_id="spoofed-settlement")

    receipt = await run_service.execute_family_admission(lifecycle, mutation)
    replay = await run_service.execute_family_admission(lifecycle, mutation)

    assert replay == receipt
    assert receipt.command_result.status == CommandStatus.REJECTED
    assert receipt.command_result.reason_code == "usage_settlement_not_found"
    assert (await run_service.get_run("tenant-1", run_id)).version == 3
    assert (await run_service.get_budget("tenant-1", run_id)).settlement_ids == frozenset()
    assert (
        await run_service.get_effects("tenant-1", run_id)
    ).claims["effect-1"].settlement is None
    assert await repository.get_family_head(
        "tenant-1", run_id, "test_family", TestFamilyMutation
    ) is None


@pytest.mark.asyncio
async def test_pending_usage_settlement_rejects_empty_and_unrelated_provenance() -> None:
    run_service, _repository, run_id = await prepared_authority_batch_run()
    unrelated_reservation = await run_service.execute(
        command(
            run_id,
            3,
            "reserve-unrelated-usage",
            ReserveBudgetAction(
                reservation_id="unrelated-reservation",
                amounts={"tokens.total": 2},
            ),
        )
    )
    assert unrelated_reservation.status == CommandStatus.ACCEPTED
    unrelated_usage = await run_service.execute(
        command(
            run_id,
            4,
            "record-unrelated-usage",
            RecordUsageAction(
                usage_id="unrelated-usage",
                reservation_id="unrelated-reservation",
                actual_amounts={"tokens.total": 2},
            ),
        )
    )
    assert unrelated_usage.status == CommandStatus.ACCEPTED
    observed = await run_service.execute(
        command(
            run_id,
            5,
            "observe-for-unrelated-settlement",
            authority_batch().actions[2],
        )
    )
    assert observed.status == CommandStatus.ACCEPTED
    forged = await run_service.execute(
        command(
            run_id,
            6,
            "settle-with-unrelated-usage",
            authority_batch().actions[3].model_copy(
                update={"usage_settlement_ref": "unrelated-usage"}
            ),
        )
    )
    assert forged.status == CommandStatus.REJECTED
    assert forged.reason_code == "usage_settlement_provenance_mismatch"

    pending_usage = await run_service.execute(
        command(
            run_id,
            6,
            "record-pending-for-empty-settlement",
            RecordUsageAction(
                usage_id="pending-for-empty-settlement",
                authority_ref="operation:1",
                reservation_id="effect-reservation",
                actual_amounts={"tokens.total": 3},
                pending_external_amounts={"tokens.total": 5},
                release_amounts={"tokens.total": 2},
            ),
        )
    )
    assert pending_usage.status == CommandStatus.ACCEPTED
    collision = await run_service.execute(
        command(
            run_id,
            7,
            "colliding-pending-settlement",
            SettlePendingUsageAction(
                settlement_id="unrelated-usage",
                usage_id="pending-for-empty-settlement",
                actual_amounts={"tokens.total": 5},
            ),
        )
    )
    assert collision.status == CommandStatus.REJECTED
    assert collision.reason_code == "settlement_identity_collision"
    empty = await run_service.execute(
        command(
            run_id,
            7,
            "empty-pending-settlement",
            SettlePendingUsageAction(
                settlement_id="empty-pending-settlement",
                usage_id="pending-for-empty-settlement",
                actual_amounts={},
                pending_release_amounts={},
            ),
        )
    )
    assert empty.status == CommandStatus.REJECTED
    assert empty.reason_code == "empty_usage_settlement"
    budget = await run_service.get_budget("tenant-1", run_id)
    assert "empty-pending-settlement" not in budget.settlement_ids
    assert "empty-pending-settlement" not in budget.usage_settlements
    assert (
        await run_service.get_effects("tenant-1", run_id)
    ).claims["effect-1"].settlement is None


@pytest.mark.asyncio
async def test_each_usage_amount_map_rejects_negative_values_independently() -> None:
    run_service, _repository, run_id = await admitted_family_run()
    hidden_negative = await run_service.execute(
        command(
            run_id,
            1,
            "negative-actual-hidden-by-pending",
            RecordUsageAction(
                usage_id="negative-actual-hidden-by-pending",
                reservation_id="baseline",
                actual_amounts={"tokens.total": -1},
                pending_external_amounts={"tokens.total": 2},
            ),
        )
    )
    assert hidden_negative.status == CommandStatus.REJECTED
    assert hidden_negative.reason_code == "invalid_budget_amount"


@pytest.mark.asyncio
async def test_legacy_pending_usage_requires_unambiguous_journal_effect_evidence() -> None:
    run_service, repository, run_id = await prepared_authority_batch_run()
    observed = await run_service.execute(
        command(
            run_id,
            3,
            "observe-legacy-operation-effect",
            authority_batch().actions[2],
        )
    )
    assert observed.status == CommandStatus.ACCEPTED
    legacy_budget = (await run_service.get_budget("tenant-1", run_id)).model_copy(
        update={
            "reserved": {"tokens.total": 0},
            "reservations": {},
            "consumed": {"tokens.total": 3},
            "pending_settlement": {"tokens.total": 5},
            "usage_ids": frozenset(),
            "usage_records": {},
            "outstanding_usage_ids": frozenset(),
        }
    )
    legacy_settlement = OperationJournalSettlement.create(
        settlement_id="legacy-operation-usage",
        request_scope="tenant-1",
        effect_claim_id="effect-1",
        settlement_revision=1,
        status="reconciliation_required",
        usage={"tokens.total": 3},
        released_usage={"tokens.total": 2},
        pending_external_usage={"tokens.total": 5},
        result_manifest_ref="artifact:legacy-operation",
        result_manifest_digest="sha256:" + "a" * 64,
        result_manifest_size_bytes=7,
        detail={"schema_version": "1"},
        settled_at=request().requested_at,
    )
    effects = await run_service.get_effects("tenant-1", run_id)
    upgraded = upgrade_legacy_operation_pending_usage(
        legacy_budget,
        effects,
        legacy_settlement,
    )
    assert upgraded.outstanding_usage_ids == {"legacy-operation-usage"}
    assert (
        upgraded.usage_records["legacy-operation-usage"].authority_ref
        == "operation:1"
    )
    repository._budgets[run_id] = upgraded
    settled = await run_service.execute(
        command(
            run_id,
            4,
            "settle-upgraded-legacy-usage",
            ApplyAuthorityBatchAction(
                actions=(
                    SettlePendingUsageAction(
                        settlement_id="pending:legacy-operation-usage",
                        usage_id="legacy-operation-usage",
                        actual_amounts={"tokens.total": 5},
                    ),
                    SettleEffectAction(
                        effect_id="effect-1",
                        settlement_id="legacy-effect-settlement",
                        observation_id="effect-observation-1",
                        outcome=EffectSettlementOutcome.SUCCEEDED,
                        usage_settlement_ref="pending:legacy-operation-usage",
                    ),
                )
            ),
        )
    )
    assert settled.status == CommandStatus.ACCEPTED

    with pytest.raises(ValueError, match="absent or ambiguous"):
        upgrade_legacy_operation_pending_usage(
            upgraded,
            effects,
            legacy_settlement,
        )
    with pytest.raises(ValueError, match="totals do not match"):
        upgrade_legacy_operation_pending_usage(
            legacy_budget.model_copy(
                update={"pending_settlement": {"tokens.total": 6}}
            ),
            effects,
            legacy_settlement,
        )


@pytest.mark.asyncio
async def test_effect_settlement_requires_exact_operation_authority_and_one_usage_settlement(
) -> None:
    run_service, _repository, run_id = await prepared_authority_batch_run()
    wrong_authority_usage = await run_service.execute(
        command(
            run_id,
            3,
            "same-reservation-wrong-authority",
            RecordUsageAction(
                usage_id="same-reservation-wrong-authority",
                authority_ref="operation:other",
                reservation_id="effect-reservation",
                actual_amounts={"tokens.total": 1},
            ),
        )
    )
    observed = await run_service.execute(
        command(
            run_id,
            wrong_authority_usage.resulting_run_version,
            "observe-wrong-authority",
            authority_batch().actions[2],
        )
    )
    mismatch = await run_service.execute(
        command(
            run_id,
            observed.resulting_run_version,
            "settle-wrong-authority",
            authority_batch().actions[3].model_copy(
                update={
                    "usage_settlement_ref": "same-reservation-wrong-authority"
                }
            ),
        )
    )
    assert mismatch.status == CommandStatus.REJECTED
    assert mismatch.reason_code == "usage_settlement_authority_mismatch"

    pending = await run_service.execute(
        command(
            run_id,
            observed.resulting_run_version,
            "record-single-settlement-usage",
            RecordUsageAction(
                usage_id="single-settlement-usage",
                authority_ref="operation:1",
                reservation_id="effect-reservation",
                actual_amounts={"tokens.total": 2},
                pending_external_amounts={"tokens.total": 4},
                release_amounts={"tokens.total": 3},
            ),
        )
    )
    first = await run_service.execute(
        command(
            run_id,
            pending.resulting_run_version,
            "first-usage-settlement",
            SettlePendingUsageAction(
                settlement_id="first-usage-settlement",
                usage_id="single-settlement-usage",
                actual_amounts={"tokens.total": 4},
            ),
        )
    )
    assert first.status == CommandStatus.ACCEPTED
    second = await run_service.execute(
        command(
            run_id,
            first.resulting_run_version,
            "second-usage-settlement",
            SettlePendingUsageAction(
                settlement_id="second-usage-settlement",
                usage_id="single-settlement-usage",
                actual_amounts={"tokens.total": 4},
            ),
        )
    )
    assert second.status == CommandStatus.REJECTED
    assert second.reason_code == "usage_already_settled"


@pytest.mark.asyncio
async def test_usage_settlement_authorizes_only_one_exact_effect() -> None:
    run_service, _repository, run_id = await admitted_family_run()
    await run_service.execute(
        command(
            run_id,
            1,
            "shared-effect-reservation",
            ReserveBudgetAction(
                reservation_id="shared-effect-reservation",
                amounts={"tokens.total": 10},
            ),
        )
    )
    for version, effect_id in ((2, "shared-effect-1"), (3, "shared-effect-2")):
        claimed = await run_service.execute(
            command(
                run_id,
                version,
                f"claim-{effect_id}",
                ClaimEffectAction(
                    effect_id=effect_id,
                    effect_kind="external_write",
                    operation_ref="operation:shared",
                    provider_idempotency_key=f"provider:{effect_id}",
                    reservation_id="shared-effect-reservation",
                ),
            )
        )
        assert claimed.status == CommandStatus.ACCEPTED
    usage = await run_service.execute(
        command(
            run_id,
            4,
            "shared-effect-usage",
            RecordUsageAction(
                usage_id="shared-effect-usage",
                authority_ref="operation:shared",
                reservation_id="shared-effect-reservation",
                actual_amounts={"tokens.total": 1},
                release_amounts={"tokens.total": 9},
            ),
        )
    )
    assert usage.status == CommandStatus.ACCEPTED
    for effect_id, version in (("shared-effect-1", 5), ("shared-effect-2", 7)):
        observed = await run_service.execute(
            command(
                run_id,
                version,
                f"observe-{effect_id}",
                ObserveEffectAction(
                    effect_id=effect_id,
                    observation_id=f"observation:{effect_id}",
                    disposition=EffectDisposition.SUCCEEDED,
                ),
            )
        )
        settled = await run_service.execute(
            command(
                run_id,
                observed.resulting_run_version,
                f"settle-{effect_id}",
                SettleEffectAction(
                    effect_id=effect_id,
                    settlement_id=f"settlement:{effect_id}",
                    observation_id=f"observation:{effect_id}",
                    outcome=EffectSettlementOutcome.SUCCEEDED,
                    usage_settlement_ref="shared-effect-usage",
                ),
            )
        )
        if effect_id == "shared-effect-1":
            assert settled.status == CommandStatus.ACCEPTED
        else:
            assert settled.status == CommandStatus.REJECTED
            assert settled.reason_code == "usage_settlement_already_applied"


def test_authority_batch_and_lifecycle_command_have_aggregate_byte_ceilings() -> None:
    oversized_actions = tuple(
        RecordOutputEvidenceAction(
            evidence=AcceptedOutputEvidence(
                output_ref=f"output:{index:02d}:" + "x" * 2000,
                evidence_digest="sha256:" + "e" * 64,
                accepted_by_authority_ref="authority:lifecycle",
            )
        )
        for index in range(40)
    )
    with pytest.raises(ValidationError, match="authority batch exceeds 65536"):
        ApplyAuthorityBatchAction(actions=oversized_actions)

    run_id = "run:bytes"
    copied = command(run_id, 1, "oversized-command", authority_batch()).model_copy(
        update={
            "evidence_refs": tuple(
                f"evidence:{index:02d}:" + "y" * 4000 for index in range(20)
            )
        }
    )
    with pytest.raises(ValidationError, match="lifecycle command exceeds 65536"):
        type(copied).model_validate(copied.model_dump(mode="python"))

    maximum_summary_batch = ApplyAuthorityBatchAction(
        actions=tuple(
            RecordOutputEvidenceAction(
                evidence=AcceptedOutputEvidence(
                    output_ref=f"output:{index:02d}",
                    evidence_digest="sha256:" + "e" * 64,
                    accepted_by_authority_ref="authority:lifecycle",
                )
            )
            for index in range(64)
        )
    )
    summary = maximum_summary_batch.canonical_identity_summary()
    assert len(summary) == 64
    assert (
        len(canonical_json(summary))
        <= MAX_AUTHORITY_BATCH_IDENTITY_SUMMARY_BYTES
    )
    assert "output:00" not in canonical_json(summary).decode("utf-8")


@pytest.mark.asyncio
async def test_authority_digest_is_order_independent_for_plain_and_combined_cas() -> None:
    class ReorderedAuthorityReadRepository(InMemoryRunControlRepository):
        async def get_budget(self, request_scope: str, run_id: str) -> BudgetState:
            state = await super().get_budget(request_scope, run_id)
            return state.model_copy(
                update={
                    "usage_ids": frozenset(reversed(sorted(state.usage_ids))),
                    "settlement_ids": frozenset(
                        reversed(sorted(state.settlement_ids))
                    ),
                    "usage_records": dict(reversed(tuple(state.usage_records.items()))),
                    "usage_settlements": dict(
                        reversed(tuple(state.usage_settlements.items()))
                    ),
                }
            )

        async def get_effects(
            self, request_scope: str, run_id: str
        ) -> EffectLedgerState:
            state = await super().get_effects(request_scope, run_id)
            return state.model_copy(
                update={"claims": dict(reversed(tuple(state.claims.items())))}
            )

    repository = ReorderedAuthorityReadRepository()
    run_service, repository, run_id = await prepared_authority_batch_run(repository)
    identifiers = tuple(f"identity-{index:02d}" for index in range(40))
    stored_budget = repository._budgets[run_id]
    left_budget = stored_budget.model_copy(
        update={
            "usage_ids": frozenset(identifiers),
            "settlement_ids": frozenset(f"settlement-{item}" for item in identifiers),
        }
    )
    right_budget = left_budget.model_copy(
        update={
            "usage_ids": frozenset(reversed(identifiers)),
            "settlement_ids": frozenset(
                reversed(tuple(f"settlement-{item}" for item in identifiers))
            ),
        }
    )
    claim = repository._effects[run_id].claims["effect-1"]
    second_claim = claim.model_copy(
        update={
            "effect_id": "effect-2",
            "provider_idempotency_key": "provider-effect-2",
        }
    )
    left_effects = EffectLedgerState(
        run_id=run_id,
        claims={"effect-1": claim, "effect-2": second_claim},
    )
    right_effects = EffectLedgerState(
        run_id=run_id,
        claims={"effect-2": second_claim, "effect-1": claim},
    )
    assert left_budget == right_budget
    assert left_effects == right_effects
    assert authority_state_digest(left_budget) == authority_state_digest(right_budget)
    assert authority_state_digest(left_effects) == authority_state_digest(right_effects)
    repository._budgets[run_id] = left_budget
    repository._effects[run_id] = left_effects

    plain = await run_service.execute(
        command(
            run_id,
            3,
            "ordered-authority-plain",
            ReserveBudgetAction(
                reservation_id="ordered-authority-plain",
                amounts={"tokens.total": 1},
            ),
        )
    )
    assert plain.status == CommandStatus.ACCEPTED
    combined = await run_service.execute_family_admission(
        command(
            run_id,
            4,
            "ordered-authority-combined",
            ReserveBudgetAction(
                reservation_id="ordered-authority-combined",
                amounts={"tokens.total": 1},
            ),
        ),
        family_mutation(run_id, mutation_id="ordered-authority-combined"),
    )
    assert combined.command_result.status == CommandStatus.ACCEPTED
