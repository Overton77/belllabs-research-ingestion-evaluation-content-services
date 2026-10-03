from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from mission_control.application.recovery.runtime_recovery import (
    ForkAdmission,
    ForkAdmissionObservation,
    ForkMaterialization,
    ForkMaterializationObservation,
    InMemoryForkRepository,
    RecoveryMode,
    RuntimeForkService,
    apply_terminal_runtime_observation,
    build_cancellation_plan,
    decide_recovery_mode,
)
from mission_control.domain.authoring.canonical import sha256_digest
from mission_control.domain.graph_runtime.contracts import (
    ActorRef,
    CancelRunIntervention,
    Correlation,
    RuntimeExecutionBinding,
    RuntimeExecutionStatus,
)
from mission_control.domain.graph_runtime.identities import (
    AgentThreadKey,
    DeploymentIdentity,
    ExecutionEpochKey,
)
from mission_control.domain.policies.errors import IdempotencyConflict
from mission_control.domain.policies.forks import (
    ForkRejected,
    RunForkPatch,
    RunForkRequest,
    admission_request_ref,
    lineage_for,
)
from tests.unit.run_control.test_run_control import request as run_request

DIGEST = "sha256:" + "a" * 64
NOW = datetime(2026, 8, 6, 20, 0, tzinfo=UTC)


def binding(*, status: RuntimeExecutionStatus = RuntimeExecutionStatus.RUNNING):
    epoch = ExecutionEpochKey(
        request_scope="tenant-1",
        belllabs_run_id="run-1",
        execution_epoch=1,
    )
    return RuntimeExecutionBinding(
        binding_id="binding-1",
        epoch=epoch,
        submission_id="submission-1",
        submission_idempotency_key="submission-1",
        submission_digest=DIGEST,
        run_plan_digest=DIGEST,
        graph_assembly_digest=DIGEST,
        state_schema_digest=DIGEST,
        runtime_provider="langgraph_agent_server",
        deployment=DeploymentIdentity(
            assistant_id="assistant-n",
            deployment_id="deployment-n",
            deployment_revision="revision-n",
            deployment_endpoint_id="endpoint-n",
        ),
        agent_thread=AgentThreadKey(
            **epoch.model_dump(),
            agent_server_thread_id="thread-parent",
            relationship="parent",
        ),
        graph_id="stagegraph",
        status=status,
        created_at=NOW,
        updated_at=NOW,
    )


def cancellation(current: RuntimeExecutionBinding) -> CancelRunIntervention:
    values = {
        "kind": "cancel_run",
        "command_id": "cancel-1",
        "idempotency_key": "cancel-1",
        "epoch": current.epoch,
        "expected_belllabs_version": 3,
        "expected_checkpoint": None,
        "actor": ActorRef(
            actor_id="operator-1",
            actor_type="operator",
            authority_ref="authority:operator@1",
        ),
        "reason": "cancel accepted",
        "correlation": Correlation(correlation_id="correlation-1"),
        "requested_at": NOW,
        "cancellation_mode": "graceful",
    }
    return CancelRunIntervention(**values, request_digest=sha256_digest(values))


def test_cancellation_cascades_runtime_resources_but_not_linked_run_authority() -> None:
    current = binding()
    plan = build_cancellation_plan(
        cancellation(current),
        current,
        runtime_resource_refs=("operation:1", "mcp-session:1", "sandbox:1"),
        linked_run_refs=("linked-run:2",),
    )

    assert plan.cancellation.requested
    assert plan.cascade_runtime_resources == (
        "operation:1",
        "mcp-session:1",
        "sandbox:1",
    )
    assert plan.linked_runs_requiring_commands == ("linked-run:2",)


def test_late_provider_success_cannot_overwrite_terminal_cancellation() -> None:
    current = binding(status=RuntimeExecutionStatus.CANCELLING)

    unsettled = apply_terminal_runtime_observation(
        current,
        observed_status=RuntimeExecutionStatus.COMPLETED,
        cancellation_settled=False,
        observed_at=NOW,
    )
    settled = apply_terminal_runtime_observation(
        unsettled,
        observed_status=RuntimeExecutionStatus.COMPLETED,
        cancellation_settled=True,
        observed_at=NOW,
    )

    assert unsettled.status == RuntimeExecutionStatus.CANCELLING
    assert settled.status == RuntimeExecutionStatus.CANCELLED
    assert not settled.active


def test_recovery_modes_distinguish_retry_fork_replay_epoch_and_rollback() -> None:
    assert decide_recovery_mode(RecoveryMode.TECHNICAL_RETRY).creates_new_thread is False
    assert decide_recovery_mode(RecoveryMode.FORK).creates_new_belllabs_run
    assert not decide_recovery_mode(RecoveryMode.DIAGNOSTIC_REPLAY).effect_claims_allowed
    assert not decide_recovery_mode(RecoveryMode.EPOCH_ROLLOVER).allowed
    assert not decide_recovery_mode(RecoveryMode.ROLLBACK).allowed


SOURCE_RUN = "run-1"
DERIVED_RUN = "run-2"
SNAPSHOT_ID = "run-snapshot:" + "1" * 64
SNAPSHOT_DIGEST = "sha256:" + "2" * 64
PATCHED_ERC = "sha256:" + "e" * 64


def fork_request(
    *,
    effective_configuration_digest: str = DIGEST,
    requested_at: datetime = NOW,
    reason: str = "branch from a settled boundary",
) -> RunForkRequest:
    target = run_request(request_id="fork-1").model_copy(
        update={
            "effective_configuration_digest": effective_configuration_digest,
            "requested_at": requested_at,
        }
    )
    patch = RunForkPatch.create(
        source_snapshot_id=SNAPSHOT_ID,
        source_snapshot_digest=SNAPSHOT_DIGEST,
        target_admission_request_ref=admission_request_ref(target),
    )
    return RunForkRequest(
        request_id="fork-1",
        idempotency_key="fork-1",
        request_scope="tenant-1",
        source_run_id=SOURCE_RUN,
        source_execution_epoch=1,
        snapshot_id=SNAPSHOT_ID,
        snapshot_digest=SNAPSHOT_DIGEST,
        patch=patch,
        target=target,
        derived_run_id=DERIVED_RUN,
        actor_id="operator",
        reason=reason,
        requested_at=requested_at,
    )


class ForkAuthority:
    def __init__(self, *, epoch: int = 1) -> None:
        self.calls = 0
        self._epoch = epoch

    async def admit_fork(self, request):  # type: ignore[no-untyped-def]
        self.calls += 1
        return ForkAdmission(
            request_id=request.request_id,
            target_epoch=ExecutionEpochKey(
                request_scope=request.request_scope,
                belllabs_run_id=request.derived_run_id,
                execution_epoch=self._epoch,
            ),
            admission_ref=f"admission:{request.request_id}",
            budget_reservation_ref="budget:fork-1",
            admitted_effective_configuration_digest=request.target.effective_configuration_digest,
        )

    async def reconcile_fork_admission(self, _request):  # type: ignore[no-untyped-def]
        return ForkAdmissionObservation(status="ambiguous")


class ForkMaterializer:
    def __init__(self) -> None:
        self.calls = 0

    async def materialize(self, request, admission):  # type: ignore[no-untyped-def]
        self.calls += 1
        return ForkMaterialization(
            request_id=request.request_id,
            target_run_id=admission.target_epoch.belllabs_run_id,
            lineage=lineage_for(request, admission_ref=admission.admission_ref),
        )

    async def reconcile_materialization(self, _request, _admission):  # type: ignore[no-untyped-def]
        return ForkMaterializationObservation(status="ambiguous")


def _service(
    authority: ForkAuthority | None = None, materializer: ForkMaterializer | None = None
) -> tuple[RuntimeForkService, InMemoryForkRepository, ForkAuthority, ForkMaterializer]:
    repository = InMemoryForkRepository()
    authority = authority or ForkAuthority()
    materializer = materializer or ForkMaterializer()
    return (
        RuntimeForkService(repository=repository, authority=authority, materializer=materializer),
        repository,
        authority,
        materializer,
    )


@pytest.mark.asyncio
async def test_fork_admits_new_run_at_epoch_one_once_with_one_durable_receipt() -> None:
    service, repository, authority, materializer = _service()
    request = fork_request()

    first = await service.fork(request)
    replay = await service.fork(request)
    later = await service.fork(fork_request(requested_at=NOW + timedelta(hours=2)))

    assert first == replay == later
    assert first.target_run_id == DERIVED_RUN
    assert first.target_execution_epoch == 1
    assert first.lineage.derived_run_id == DERIVED_RUN
    assert first.lineage.source_run_id == SOURCE_RUN
    assert first.lineage.snapshot_digest == SNAPSHOT_DIGEST
    assert first.recorded_at == NOW
    assert authority.calls == materializer.calls == 1
    assert await repository.get("tenant-1", "fork-1") == first


@pytest.mark.asyncio
async def test_patched_configuration_is_admitted_and_epoch_must_be_one() -> None:
    # RRM-001 §7 #12: a fork never requires the source run plan or ERC to be unchanged...
    service, _repository, _authority, _materializer = _service()
    patched = await service.fork(fork_request(effective_configuration_digest=PATCHED_ERC))
    assert patched.admitted_effective_configuration_digest == PATCHED_ERC

    # ...and an admission that does not target epoch 1 is refused (no receipt).
    epoch_two, repository, _authority, materializer = _service(ForkAuthority(epoch=2))
    with pytest.raises(ValueError, match="execution epoch 1"):
        await epoch_two.fork(fork_request())
    assert await repository.get("tenant-1", "fork-1") is None
    assert materializer.calls == 0


@pytest.mark.asyncio
async def test_conflicting_fork_intent_is_rejected() -> None:
    service, _repository, _authority, _materializer = _service()
    await service.fork(fork_request())

    with pytest.raises(IdempotencyConflict, match="conflicting intent"):
        await service.fork(fork_request(reason="a different intent"))


class TimeoutAfterMaterializer(ForkMaterializer):
    def __init__(self) -> None:
        super().__init__()
        self.recorded = None

    async def materialize(self, request, admission):  # type: ignore[no-untyped-def]
        self.recorded = await super().materialize(request, admission)
        raise TimeoutError("materialization result was lost")

    async def reconcile_materialization(self, _request, _admission):  # type: ignore[no-untyped-def]
        assert self.recorded is not None
        return ForkMaterializationObservation(status="materialized", materialization=self.recorded)


@pytest.mark.asyncio
async def test_fork_recovers_persisted_admission_after_materialization_timeout() -> None:
    materializer = TimeoutAfterMaterializer()
    service, _repository, authority, _ = _service(materializer=materializer)

    with pytest.raises(TimeoutError, match="result was lost"):
        await service.fork(fork_request())
    recovered = await service.fork(fork_request())

    assert recovered.target_run_id == DERIVED_RUN
    assert authority.calls == 1
    assert materializer.calls == 1


class AmbiguousMaterializer(ForkMaterializer):
    def __init__(self) -> None:
        super().__init__()
        self.resolved = False

    async def materialize(self, request, admission):  # type: ignore[no-untyped-def]
        if not self.resolved:
            self.calls += 1
            raise ConnectionError("materialization outcome unknown")
        return await super().materialize(request, admission)

    async def reconcile_materialization(self, request, admission):  # type: ignore[no-untyped-def]
        if not self.resolved:
            return ForkMaterializationObservation(status="ambiguous")
        return ForkMaterializationObservation(status="definitively_missing")


@pytest.mark.asyncio
async def test_ambiguous_materialization_fails_closed_without_a_receipt() -> None:
    materializer = AmbiguousMaterializer()
    service, repository, authority, _ = _service(materializer=materializer)

    with pytest.raises(ConnectionError, match="outcome unknown"):
        await service.fork(fork_request())
    for _attempt in range(2):
        with pytest.raises(ForkRejected) as caught:
            await service.fork(fork_request())
        assert caught.value.code == "fork_materialization_ambiguous"
    assert await repository.get("tenant-1", "fork-1") is None

    # Once the outcome is known (definitively absent), the saga re-materializes once.
    materializer.resolved = True
    receipt = await service.fork(fork_request())
    assert receipt.target_run_id == DERIVED_RUN
    assert authority.calls == 1
    assert materializer.calls == 2


class SlowForkAuthority(ForkAuthority):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def admit_fork(self, request):  # type: ignore[no-untyped-def]
        self.entered.set()
        await self.release.wait()
        return await super().admit_fork(request)


@pytest.mark.asyncio
async def test_concurrent_fork_retries_serialize_admission_and_materialization() -> None:
    authority = SlowForkAuthority()
    service, _repository, _, materializer = _service(authority)
    request = fork_request()

    first_task = asyncio.create_task(service.fork(request))
    await authority.entered.wait()
    second_task = asyncio.create_task(service.fork(request))
    await asyncio.sleep(0)
    authority.release.set()
    first, second = await asyncio.gather(first_task, second_task)

    assert first == second
    assert authority.calls == 1
    assert materializer.calls == 1


class TimeoutAfterAdmissionAuthority(ForkAuthority):
    def __init__(self) -> None:
        super().__init__()
        self.admission = None

    async def admit_fork(self, request):  # type: ignore[no-untyped-def]
        self.admission = await super().admit_fork(request)
        raise TimeoutError("admission result was lost")

    async def reconcile_fork_admission(self, _request):  # type: ignore[no-untyped-def]
        assert self.admission is not None
        return ForkAdmissionObservation(
            status="admitted",
            admission=self.admission,
        )


@pytest.mark.asyncio
async def test_fork_recovers_admission_claim_after_timeout() -> None:
    authority = TimeoutAfterAdmissionAuthority()
    service, _repository, _, materializer = _service(authority)

    with pytest.raises(TimeoutError, match="result was lost"):
        await service.fork(fork_request())
    recovered = await service.fork(fork_request())

    assert recovered.target_run_id == DERIVED_RUN
    assert authority.calls == 1
    assert materializer.calls == 1


class DefinitivelyMissingAdmissionAuthority(ForkAuthority):
    async def admit_fork(self, request):  # type: ignore[no-untyped-def]
        if self.calls == 0:
            self.calls += 1
            raise ConnectionError("admission failed before commit")
        return await super().admit_fork(request)

    async def reconcile_fork_admission(self, _request):  # type: ignore[no-untyped-def]
        return ForkAdmissionObservation(status="definitively_missing")


@pytest.mark.asyncio
async def test_fork_reclaims_admission_only_after_definitive_absence() -> None:
    authority = DefinitivelyMissingAdmissionAuthority()
    service, _repository, _, materializer = _service(authority)

    with pytest.raises(ConnectionError, match="before commit"):
        await service.fork(fork_request())
    recovered = await service.fork(fork_request())

    assert recovered.target_run_id == DERIVED_RUN
    assert authority.calls == 2
    assert materializer.calls == 1
