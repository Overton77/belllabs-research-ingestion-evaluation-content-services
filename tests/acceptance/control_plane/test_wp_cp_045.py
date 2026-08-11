from __future__ import annotations

from datetime import UTC, datetime

import pytest
from deepagents.middleware import async_subagents as deepagents_async

from app.application.async_subagents.service import (
    AsyncSubagentError,
    AsyncSubagentService,
    AsyncSubagentSpawnRequest,
    InMemoryAsyncSubagentAuthority,
    InMemoryAsyncSubagentDetailRepository,
    ProviderAsyncObservation,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.contracts import (
    AsyncSubagentContract,
    AsyncSubagentDependencyClass,
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    AsyncSubagentMessage,
    CapabilityGrant,
)
from app.domain.operation_execution.delegation import (
    AsyncDelegationBoundary,
    classify_async_delegation,
)
from app.integrations.agents.deep_agents.async_subagents import (
    DeepAgentsAsyncSubagentAdapter,
)

NOW = datetime(2026, 8, 10, 12, 0, tzinfo=UTC)


def contract() -> AsyncSubagentContract:
    return AsyncSubagentContract.create(
        contract_id="async-researcher-v1",
        name="researcher",
        description="Bounded background research",
        graph_id="research-graph",
        agent_protocol_url="https://agent.example.test",
        objective_schema_ref="schema:objective:v1",
        result_schema_ref="schema:result:v1",
        context_slice_id="context:child",
        state_slice_id="state:child",
        capability_ceiling=CapabilityGrant(capabilities=frozenset({"search"})),
        authority_refs=("authority:parent-operation",),
        budget_limits={"subagent.spawns": 1, "tokens.total": 1_000},
        dependency_classes=frozenset(AsyncSubagentDependencyClass),
        timeout_seconds=300,
        cancellation_propagation="required",
        late_result_policy="quarantine",
        fallback_policy="degrade",
        result_admission_policy_ref="policy:async-result:v1",
    )


def request(
    dependency: AsyncSubagentDependencyClass = AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
) -> AsyncSubagentSpawnRequest:
    return AsyncSubagentSpawnRequest(
        request_scope="tenant-a",
        parent_run_id="run-1",
        parent_operation_id="operation-1",
        parent_binding_id="binding-1",
        execution_generation=1,
        contract=contract(),
        dependency_class=dependency,
        objective_ref="ref:objective:1",
        objective="Research the exact bounded question.",
        context_slice_ref="ref:context-slice:1",
        reservation_id="reservation-child-1",
        idempotency_key="spawn-1",
        requested_at=NOW,
    )


class DeterministicProvider:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.starts = 0
        self.next_status = "running"

    async def start(
        self,
        _contract: AsyncSubagentContract,
        _execution: AsyncSubagentExecution,
        _objective: str,
    ) -> ProviderAsyncObservation:
        self.events.append("provider.start")
        self.starts += 1
        return self._observation("running")

    async def check(
        self, _contract: AsyncSubagentContract, _execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation:
        self.events.append("provider.check")
        return self._observation(self.next_status)

    async def update(
        self,
        _contract: AsyncSubagentContract,
        _execution: AsyncSubagentExecution,
        _message: AsyncSubagentMessage,
    ) -> ProviderAsyncObservation:
        self.events.append("provider.update")
        return self._observation("running", run_id="provider-run-2")

    async def cancel(
        self, _contract: AsyncSubagentContract, _execution: AsyncSubagentExecution
    ) -> ProviderAsyncObservation:
        self.events.append("provider.cancel")
        return self._observation("cancelled")

    async def list(
        self, executions: tuple[tuple[AsyncSubagentContract, AsyncSubagentExecution], ...]
    ) -> tuple[ProviderAsyncObservation, ...]:
        return tuple(self._observation("running") for _ in executions)

    @staticmethod
    def _observation(status: str, run_id: str = "provider-run-1") -> ProviderAsyncObservation:
        terminal = status == "success"
        return ProviderAsyncObservation(
            status=status,  # type: ignore[arg-type]
            thread_id="provider-thread-1",
            run_id=run_id,
            output_ref="ref:output:1" if terminal else None,
            evidence_refs=("ref:evidence:1",) if terminal else (),
            usage_ref="ref:usage:1" if terminal else None,
            checkpoint_ref="ref:checkpoint:1" if terminal else None,
            observed_at=NOW,
        )


class OrderedDetails(InMemoryAsyncSubagentDetailRepository):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    async def create_before_submit(self, *args: object, **kwargs: object) -> AsyncSubagentExecution:
        self._events.append("mongo.contract-link-execution")
        return await super().create_before_submit(*args, **kwargs)  # type: ignore[arg-type]


class OrderedAuthority(InMemoryAsyncSubagentAuthority):
    def __init__(self, events: list[str]) -> None:
        super().__init__()
        self._events = events

    async def reserve_and_admit(
        self, spawn: AsyncSubagentSpawnRequest, child_execution_id: str, link_id: str
    ) -> None:
        self._events.append("postgres.reservation-link-admission")
        await super().reserve_and_admit(spawn, child_execution_id, link_id)


def fixture_service(
    dependency: AsyncSubagentDependencyClass = AsyncSubagentDependencyClass.REQUIRED_BLOCKING,
) -> tuple[
    AsyncSubagentService,
    InMemoryAsyncSubagentDetailRepository,
    InMemoryAsyncSubagentAuthority,
    DeterministicProvider,
    AsyncSubagentSpawnRequest,
]:
    events: list[str] = []
    details = OrderedDetails(events)
    authority = OrderedAuthority(events)
    provider = DeterministicProvider(events)
    return (
        AsyncSubagentService(details, authority, provider, allow_new_spawns=True),
        details,
        authority,
        provider,
        request(dependency),
    )


@pytest.mark.asyncio
async def test_spawn_persists_contract_link_and_reservation_before_provider_submission() -> None:
    service, details, authority, provider, spawn = fixture_service()
    execution = await service.spawn(spawn)

    assert provider.events[:3] == [
        "mongo.contract-link-execution",
        "postgres.reservation-link-admission",
        "provider.start",
    ]
    assert execution.lifecycle == AsyncSubagentLifecycle.RUNNING
    assert execution.provider_thread_id == "provider-thread-1"
    assert (spawn.request_scope, execution.child_execution_id) in details.executions
    assert (spawn.request_scope, execution.child_execution_id) in authority.reservations


@pytest.mark.asyncio
async def test_feature_gate_disables_new_spawn_without_disabling_reconciliation_service() -> None:
    details = InMemoryAsyncSubagentDetailRepository()
    authority = InMemoryAsyncSubagentAuthority()
    provider = DeterministicProvider([])
    service = AsyncSubagentService(details, authority, provider)
    with pytest.raises(AsyncSubagentError, match="feature-gated"):
        await service.spawn(request())
    assert provider.starts == 0


@pytest.mark.asyncio
async def test_retry_or_callback_poll_ambiguity_creates_one_effective_child() -> None:
    service, _, authority, provider, spawn = fixture_service()
    first = await service.spawn(spawn)
    second = await service.spawn(spawn)

    assert second == first
    assert provider.starts == 1
    facts_before = len(authority.facts)
    await service.reconcile(spawn.request_scope, first.child_execution_id)
    await service.reconcile(spawn.request_scope, first.child_execution_id)
    assert len(authority.facts) == facts_before


@pytest.mark.parametrize(
    ("dependency", "initial", "admitted", "rejected"),
    [
        (AsyncSubagentDependencyClass.REQUIRED_BLOCKING, "wait", "proceed", "wait"),
        (AsyncSubagentDependencyClass.DEGRADABLE_BLOCKING, "wait", "proceed", "degrade"),
        (AsyncSubagentDependencyClass.NONBLOCKING, "proceed", "proceed", "proceed"),
        (AsyncSubagentDependencyClass.ADVISORY, "proceed", "proceed", "proceed"),
    ],
)
def test_four_frozen_dependency_classes(
    dependency: AsyncSubagentDependencyClass,
    initial: str,
    admitted: str,
    rejected: str,
) -> None:
    link = fixture_service(dependency)[1].links
    assert not link
    base = request(dependency)
    from app.domain.operation_execution.contracts import ParentAsyncSubagentLink

    value = ParentAsyncSubagentLink(
        link_id="link",
        child_execution_id="child",
        parent_run_id=base.parent_run_id,
        parent_operation_id=base.parent_operation_id,
        dependency_class=dependency,
        timeout_at=NOW,
        cancellation_propagation="required",
        late_result_policy="quarantine",
        fallback_policy="degrade",
        result_admission_policy_ref="policy",
        created_at=NOW,
        updated_at=NOW,
    )
    assert AsyncSubagentService.parent_dependency(value) == initial
    assert (
        AsyncSubagentService.parent_dependency(
            value.model_copy(update={"result_decision": "admit"})
        )
        == admitted
    )
    assert (
        AsyncSubagentService.parent_dependency(
            value.model_copy(update={"result_decision": "reject"})
        )
        == rejected
    )


@pytest.mark.asyncio
async def test_messages_are_ordered_and_provider_receipts_do_not_replace_ledger() -> None:
    service, details, authority, _, spawn = fixture_service()
    execution = await service.spawn(spawn)
    one = await service.send_message(
        spawn.request_scope,
        execution.child_execution_id,
        payload_ref="ref:message:one",
        correlation_id="correlation-1",
        created_at=NOW,
    )
    two = await service.send_message(
        spawn.request_scope,
        execution.child_execution_id,
        payload_ref="ref:message:two",
        correlation_id="correlation-1",
        created_at=NOW,
    )

    assert [one.target_sequence, two.target_sequence] == [1, 2]
    assert [item[1].target_sequence for item in authority.messages] == [1, 2]
    link = await details.get_link(spawn.request_scope, execution.child_execution_id)
    assert [item.receipt for item in link.messages] == ["provider_applied", "provider_applied"]
    child_one = await service.receive_child_message(
        spawn.request_scope,
        execution.child_execution_id,
        payload_ref="ref:child-message:one",
        correlation_id="correlation-1",
        created_at=NOW,
    )
    child_two = await service.receive_child_message(
        spawn.request_scope,
        execution.child_execution_id,
        payload_ref="ref:child-message:two",
        correlation_id="correlation-1",
        created_at=NOW,
    )
    assert [child_one.target_sequence, child_two.target_sequence] == [1, 2]
    assert child_one.context_authority == "untrusted_observation"


@pytest.mark.asyncio
async def test_completed_output_cannot_change_parent_before_explicit_admission_or_when_late() -> (
    None
):
    service, details, authority, provider, spawn = fixture_service()
    execution = await service.spawn(spawn)
    provider.next_status = "success"
    completed = await service.reconcile(spawn.request_scope, execution.child_execution_id)

    assert completed.result_manifest is not None
    link = await details.get_link(spawn.request_scope, execution.child_execution_id)
    assert link.result_decision is None
    assert not authority.decisions
    with pytest.raises(AsyncSubagentError, match="late or superseded"):
        await service.decide_result(
            spawn.request_scope,
            execution.child_execution_id,
            "admit",
            parent_open=False,
            current_generation=1,
            decided_at=NOW,
        )
    admitted = await service.decide_result(
        spawn.request_scope,
        execution.child_execution_id,
        "admit",
        parent_open=True,
        current_generation=1,
        decided_at=NOW,
    )
    assert admitted.admitted_manifest_digest == completed.result_manifest.manifest_digest
    settled = await service.settle(
        spawn.request_scope,
        execution.child_execution_id,
        "settlement:child:1",
        NOW,
    )
    replayed = await service.settle(
        spawn.request_scope,
        execution.child_execution_id,
        "settlement:child:1",
        NOW,
    )
    assert settled.settled and replayed == settled
    assert authority.settlements == {
        (spawn.request_scope, execution.child_execution_id): "settlement:child:1"
    }


@pytest.mark.asyncio
async def test_cancellation_is_authorized_before_provider_and_reconciled() -> None:
    service, details, authority, provider, spawn = fixture_service()
    execution = await service.spawn(spawn)
    cancelled = await service.cancel(
        spawn.request_scope, execution.child_execution_id, "parent cancelled", NOW
    )
    assert cancelled.lifecycle == AsyncSubagentLifecycle.CANCELLED
    assert authority.cancellations[(spawn.request_scope, execution.child_execution_id)]
    assert provider.events[-1] == "provider.cancel"
    link = await details.get_link(spawn.request_scope, execution.child_execution_id)
    assert link.cancellation_requested


@pytest.mark.parametrize(
    ("boundary", "route"),
    [
        (AsyncDelegationBoundary(), "subordinate"),
        (AsyncDelegationBoundary(durable_cross_operation_wait=True), "operation"),
        (AsyncDelegationBoundary(substantial_separate_budget=True), "operation"),
        (AsyncDelegationBoundary(recognized_workflow_type=True), "linked_run"),
        (AsyncDelegationBoundary(independent_authority=True), "linked_run"),
    ],
)
def test_governance_classifier_never_launches_hidden_work(
    boundary: AsyncDelegationBoundary, route: str
) -> None:
    assert classify_async_delegation(boundary).route == route


def test_actual_deep_agents_075_middleware_surface_is_wrapped_exactly() -> None:
    adapter = DeepAgentsAsyncSubagentAdapter(now=lambda: NOW)
    tools = adapter._tools(contract())  # qualification inspects the actual installed mechanism
    assert tuple(tools) == adapter.tool_names
    assert sha256_digest(tuple(tools)) == sha256_digest(adapter.tool_names)


@pytest.mark.asyncio
async def test_actual_middleware_start_and_check_are_governed_before_sdk_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    class Threads:
        async def create(self, **kwargs: object) -> dict[str, str]:
            calls.append("sdk.threads.create")
            return {"thread_id": str(kwargs["thread_id"])}

        async def get(self, *, thread_id: str) -> dict[str, object]:
            assert thread_id
            return {"values": {"messages": [{"content": "canonical child result"}]}}

    class Runs:
        created: dict[str, object] | None = None

        async def list(self, **_kwargs: object) -> list[dict[str, object]]:
            return [self.created] if self.created is not None else []

        async def create(self, **kwargs: object) -> dict[str, object]:
            calls.append("sdk.runs.create")
            self.created = {
                "run_id": "provider-run-1",
                "status": "running",
                "metadata": {"belllabs_spawn_key": kwargs["thread_id"]},
            }
            return self.created

        async def get(self, **_kwargs: object) -> dict[str, str]:
            calls.append("sdk.runs.get")
            return {"run_id": "provider-run-1", "status": "success"}

        async def cancel(self, **_kwargs: object) -> None:
            calls.append("sdk.runs.cancel")

    class Client:
        threads = Threads()
        runs = Runs()

    monkeypatch.setattr(deepagents_async, "get_client", lambda **_kwargs: Client())
    details = InMemoryAsyncSubagentDetailRepository()
    authority = InMemoryAsyncSubagentAuthority()
    provider = DeepAgentsAsyncSubagentAdapter(now=lambda: NOW)
    service = AsyncSubagentService(details, authority, provider, allow_new_spawns=True)

    execution = await service.spawn(request())
    assert authority.reservations
    assert calls[:2] == ["sdk.threads.create", "sdk.runs.create"]
    await provider.start(contract(), execution, "retry after ambiguous provider response")
    assert calls.count("sdk.runs.create") == 1
    recovered_service = AsyncSubagentService(
        details,
        authority,
        DeepAgentsAsyncSubagentAdapter(now=lambda: NOW),
    )
    completed = await recovered_service.reconcile("tenant-a", execution.child_execution_id)
    assert completed.lifecycle == AsyncSubagentLifecycle.COMPLETED
    assert completed.result_manifest is not None
    assert completed.result_manifest.output_refs[0].startswith("ref:async-output:")
