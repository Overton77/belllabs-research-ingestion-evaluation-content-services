from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.application.runtime_recovery import (
    ForkAdmission,
    ForkAdmissionObservation,
    ForkRuntimeObservation,
    InMemoryForkRepository,
    RecoveryMode,
    RuntimeForkService,
    apply_terminal_runtime_observation,
    build_cancellation_plan,
    decide_recovery_mode,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.graph_runtime.contracts import (
    ActorRef,
    CancelRunIntervention,
    Correlation,
    ForkRequest,
    RuntimeExecutionBinding,
    RuntimeExecutionStatus,
)
from app.domain.graph_runtime.identities import (
    AgentThreadKey,
    BellLabsRunKey,
    DeploymentIdentity,
    ExecutionEpochKey,
    LangGraphCheckpointKey,
)

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


def fork_request(current: RuntimeExecutionBinding) -> ForkRequest:
    assert current.deployment is not None
    assert current.agent_thread is not None
    return ForkRequest(
        request_id="fork-1",
        idempotency_key="fork-1",
        source_epoch=current.epoch,
        source_checkpoint=LangGraphCheckpointKey(
            deployment_endpoint_id=current.deployment.deployment_endpoint_id,
            agent_server_thread_id=current.agent_thread.agent_server_thread_id,
            langgraph_checkpoint_id="checkpoint-1",
        ),
        target_run=BellLabsRunKey(
            request_scope=current.epoch.request_scope,
            belllabs_run_id="run-2",
        ),
        run_plan_digest=current.run_plan_digest,
        actor=ActorRef(
            actor_id="operator-1",
            actor_type="operator",
            authority_ref="authority:operator@1",
        ),
        reason="branch accepted recovery",
        requested_at=NOW,
    )


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


class ForkAuthority:
    def __init__(self) -> None:
        self.calls = 0

    async def admit_fork(self, request):  # type: ignore[no-untyped-def]
        self.calls += 1
        return ForkAdmission(
            request_id=request.request_id,
            target_epoch=ExecutionEpochKey(
                request_scope=request.target_run.request_scope,
                belllabs_run_id=request.target_run.belllabs_run_id,
                execution_epoch=1,
            ),
            budget_reservation_ref="budget:fork-1",
            admitted_run_plan_digest=request.run_plan_digest,
        )

    async def reconcile_fork_admission(self, _request):  # type: ignore[no-untyped-def]
        return ForkAdmissionObservation(status="ambiguous")


class ForkRuntime:
    def __init__(self) -> None:
        self.calls = 0

    async def copy_checkpoint(self, request, _source, admission):  # type: ignore[no-untyped-def]
        self.calls += 1
        return AgentThreadKey(
            **admission.target_epoch.model_dump(),
            agent_server_thread_id="thread-child",
            relationship="fork",
            parent_belllabs_run_id=request.source_epoch.belllabs_run_id,
        )

    async def reconcile_checkpoint_copy(self, _request, _source, _admission):  # type: ignore[no-untyped-def]
        return ForkRuntimeObservation(status="ambiguous")


@pytest.mark.asyncio
async def test_fork_admits_new_run_budget_and_thread_once_and_keeps_parent_immutable() -> None:
    repository = InMemoryForkRepository()
    authority = ForkAuthority()
    runtime = ForkRuntime()
    service = RuntimeForkService(
        repository=repository,
        authority=authority,
        runtime=runtime,
    )
    parent = binding()
    request = fork_request(parent)

    first = await service.fork(request, parent)
    replay = await service.fork(request, parent)

    assert first == replay
    assert first.target_epoch.belllabs_run_id == "run-2"
    assert first.target_thread.agent_server_thread_id == "thread-child"
    assert first.target_thread.parent_belllabs_run_id == "run-1"
    assert authority.calls == runtime.calls == 1
    assert parent == binding()


class TimeoutAfterForkRuntime(ForkRuntime):
    def __init__(self) -> None:
        super().__init__()
        self.created_thread = None

    async def copy_checkpoint(self, request, source, admission):  # type: ignore[no-untyped-def]
        self.created_thread = await super().copy_checkpoint(request, source, admission)
        raise TimeoutError("copy result was lost")

    async def reconcile_checkpoint_copy(self, _request, _source, _admission):  # type: ignore[no-untyped-def]
        assert self.created_thread is not None
        return ForkRuntimeObservation(
            status="copied",
            target_thread=self.created_thread,
        )


@pytest.mark.asyncio
async def test_fork_recovers_persisted_admission_after_copy_timeout() -> None:
    repository = InMemoryForkRepository()
    authority = ForkAuthority()
    runtime = TimeoutAfterForkRuntime()
    service = RuntimeForkService(
        repository=repository,
        authority=authority,
        runtime=runtime,
    )
    parent = binding()
    request = fork_request(parent)

    with pytest.raises(TimeoutError, match="result was lost"):
        await service.fork(request, parent)
    recovered = await service.fork(request, parent)

    assert recovered.target_thread.agent_server_thread_id == "thread-child"
    assert authority.calls == 1
    assert runtime.calls == 1


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
async def test_concurrent_fork_retries_serialize_admission_and_copy() -> None:
    repository = InMemoryForkRepository()
    authority = SlowForkAuthority()
    runtime = ForkRuntime()
    service = RuntimeForkService(
        repository=repository,
        authority=authority,
        runtime=runtime,
    )
    parent = binding()
    request = fork_request(parent)

    first_task = asyncio.create_task(service.fork(request, parent))
    await authority.entered.wait()
    second_task = asyncio.create_task(service.fork(request, parent))
    await asyncio.sleep(0)
    authority.release.set()
    first, second = await asyncio.gather(first_task, second_task)

    assert first == second
    assert authority.calls == 1
    assert runtime.calls == 1


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
    repository = InMemoryForkRepository()
    authority = TimeoutAfterAdmissionAuthority()
    runtime = ForkRuntime()
    service = RuntimeForkService(
        repository=repository,
        authority=authority,
        runtime=runtime,
    )
    parent = binding()
    request = fork_request(parent)

    with pytest.raises(TimeoutError, match="result was lost"):
        await service.fork(request, parent)
    recovered = await service.fork(request, parent)

    assert recovered.target_epoch.belllabs_run_id == "run-2"
    assert authority.calls == 1
    assert runtime.calls == 1


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
    repository = InMemoryForkRepository()
    authority = DefinitivelyMissingAdmissionAuthority()
    runtime = ForkRuntime()
    service = RuntimeForkService(
        repository=repository,
        authority=authority,
        runtime=runtime,
    )
    parent = binding()
    request = fork_request(parent)

    with pytest.raises(ConnectionError, match="before commit"):
        await service.fork(request, parent)
    recovered = await service.fork(request, parent)

    assert recovered.target_epoch.belllabs_run_id == "run-2"
    assert authority.calls == 2
    assert runtime.calls == 1
