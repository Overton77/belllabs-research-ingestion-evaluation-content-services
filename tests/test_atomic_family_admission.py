from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest
from fastapi import FastAPI
from pydantic import Field, ValidationError

from app.api.run_control import (
    compose_api_run_control_service,
    configure_family_admission_registry,
)
from app.application.run_control import (
    AdmissionPolicyRegistry,
    FamilyAdmissionRegistry,
    RunControlService,
)
from app.application.run_control_repository import InMemoryRunControlRepository
from app.domain.run_control.contracts import (
    CommandStatus,
    RecordUsageAction,
    ReserveBudgetAction,
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
from app.temporal.worker import compose_worker_run_control_service
from tests.test_run_control import ConfigurationVerifier, command, request


class TestFamilyMutation(AtomicFamilyMutation):
    __test__ = False
    candidate_ref: str = Field(min_length=1)
    cursor: tuple[str, ...] = ()


def registered_family_registry() -> FamilyAdmissionRegistry:
    families = FamilyAdmissionRegistry()
    families.register(
        TestFamilyMutation,
        family_kind="test_family",
        mutation_kind="candidate_admitted",
        required_permission="workflow_run.reserve_budget",
        allowed_action_kinds=frozenset({"reserve_budget"}),
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
    candidate_ref: str = "candidate:a",
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


async def admitted_family_run(
    repository: InMemoryRunControlRepository | None = None,
) -> tuple[RunControlService, InMemoryRunControlRepository, str]:
    run_service, repository = family_service(repository)
    admitted = await run_service.admit(request())
    assert admitted.run_id is not None
    return run_service, repository, admitted.run_id


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
