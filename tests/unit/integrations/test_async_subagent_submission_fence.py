"""REQ-CP-DA-008 submission fence, in_doubt and its exits, against the fake Agent Protocol.

Offline regression (RRM-013). The fake client stands in for the server so the fence, the
spawn-key lookup, the typed ambiguity and the operator decisions are deterministic; the live
proof is `tests/integration/agent_server/test_rrm_013_async_subagent_live.py`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from mission_control.adapters.deep_agents.async_subagents import DeepAgentsAsyncSubagentAdapter
from mission_control.application.subordinates.parent_effects import (
    RunControlAsyncChildEffects,
    async_child_effect_id,
    async_child_usage_id,
)
from mission_control.application.subordinates.service import (
    AsyncServedGraphMismatch,
    AsyncSubagentDecisionRejected,
    AsyncSubagentError,
    AsyncSubagentService,
    AsyncSubagentSubmissionInProgress,
    InMemoryAsyncSubagentAuthority,
    InMemoryAsyncSubagentDetailRepository,
)
from mission_control.domain.execution.async_subagent_reconciliation import (
    AsyncProviderRunRecord,
    AsyncServedGraphIdentity,
    aggregate_child_usage,
    classify_async_children_for_fork,
)
from mission_control.domain.execution.contracts import (
    AsyncSubagentExecution,
    AsyncSubagentLifecycle,
    AsyncSubagentUsage,
)
from tests.acceptance.control_plane.test_wp_cp_045 import (
    GRAPH_BINDING_DIGEST,
    NOW,
    SERVED,
    contract,
    request,
)
from tests.fixtures.fake_agent_protocol import FakeAgentProtocolClient, install
from tests.fixtures.rrm013_live_stack import reconciler
from tests.unit.operations.test_async_child_parent_budget import admitted_parent, spawn
from tests.unit.run_control.test_run_control import actor

SECRETS = {"environment:AGENT_SERVER_TOKEN": "offline-token"}


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> datetime:
        return self.now


def governed(
    client: FakeAgentProtocolClient,
    *,
    details: InMemoryAsyncSubagentDetailRepository | None = None,
    authority: InMemoryAsyncSubagentAuthority | None = None,
    clock: Clock | None = None,
    submitter: str = "worker-1",
    allow_new_spawns: bool = True,
) -> tuple[
    AsyncSubagentService, InMemoryAsyncSubagentDetailRepository, InMemoryAsyncSubagentAuthority
]:
    details = details or InMemoryAsyncSubagentDetailRepository()
    authority = authority or InMemoryAsyncSubagentAuthority()
    clock = clock or Clock()
    service = AsyncSubagentService(
        details,
        authority,
        DeepAgentsAsyncSubagentAdapter(now=clock, secrets=SECRETS, request_scope="tenant-a"),
        allow_new_spawns=allow_new_spawns,
        submitter_identity=submitter,
        submission_lease=timedelta(seconds=30),
        now=clock,
    )
    return service, details, authority


@pytest.mark.asyncio
async def test_crash_before_submission_resumes_the_admitted_child_with_one_provider_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-001 section 7 #10: an admitted, unsubmitted child is submitted on retry, not idle."""

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    clock = Clock()
    service, details, authority = governed(client, clock=clock)
    client.fail_before_create = ConnectionError("worker lost before the provider accepted")
    with pytest.raises(ConnectionError):
        await service.spawn(request())
    child_id = next(iter(details.executions.values())).child_execution_id
    admitted = await details.get_execution("tenant-a", child_id)
    assert admitted.lifecycle == AsyncSubagentLifecycle.ADMITTED
    assert admitted.provider_run_id is None
    assert client.runs_of(child_id) == []
    fence = authority.fences[("tenant-a", child_id)]
    assert fence["fence"] == 1 and fence["holder"] is None  # released on the way out

    resumed = await service.spawn(request())
    assert resumed.lifecycle == AsyncSubagentLifecycle.RUNNING
    assert resumed.child_execution_id == child_id
    assert len(client.runs_of(child_id)) == 1
    assert authority.fences[("tenant-a", child_id)]["fence"] == 2
    assert not any(ref == "orphaned" for _s, _c, _k, ref in authority.facts)


@pytest.mark.asyncio
async def test_crash_after_submission_before_observation_reconnects_to_the_same_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    clock = Clock()
    service, details, authority = governed(client, clock=clock)
    # The provider created the run, then the worker was lost before observing the response:
    # the spawn classifies from durable provider state and binds the single run.
    client.fail_after_create = ConnectionError("worker lost after the provider accepted")
    bound = await service.spawn(request())
    assert bound.lifecycle == AsyncSubagentLifecycle.RUNNING
    assert len(client.runs_of(bound.child_execution_id)) == 1
    assert bound.provider_run_id == client.runs_of(bound.child_execution_id)[0]["run_id"]
    assert client.calls.count("sdk.runs.create") == 1

    # A replacement worker that reconciles instead of re-spawning sees the same binding.
    other, _, _ = governed(client, details=details, authority=authority, submitter="worker-2")
    reconciled = await other.reconcile("tenant-a", bound.child_execution_id)
    assert reconciled.provider_run_id == bound.provider_run_id
    assert client.calls.count("sdk.runs.create") == 1


@pytest.mark.asyncio
async def test_live_fence_holder_blocks_a_concurrent_submitter_until_its_lease_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    clock = Clock()
    details = InMemoryAsyncSubagentDetailRepository()
    authority = InMemoryAsyncSubagentAuthority()
    service, _, _ = governed(client, details=details, authority=authority, clock=clock)
    # Worker 1 is lost while holding the fence (its release never happens).
    client.fail_before_create = ConnectionError("lost")
    with pytest.raises(ConnectionError):
        await service.spawn(request())
    child_id = next(iter(details.executions.values())).child_execution_id
    authority.fences[("tenant-a", child_id)].update(
        {"holder": "worker-1", "lease_expires_at": clock.now + timedelta(seconds=30)}
    )
    other, _, _ = governed(
        client, details=details, authority=authority, clock=clock, submitter="worker-2"
    )
    with pytest.raises(AsyncSubagentSubmissionInProgress):
        await other.spawn(request())
    assert client.runs_of(child_id) == []
    clock.now += timedelta(seconds=31)
    resumed = await other.spawn(request())
    assert resumed.lifecycle == AsyncSubagentLifecycle.RUNNING
    assert len(client.runs_of(child_id)) == 1
    assert authority.fences[("tenant-a", child_id)]["fence"] == 2


@pytest.mark.asyncio
async def test_unobservable_submission_is_in_doubt_with_an_incident_never_orphaned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, _details, authority = governed(client)
    client.fail_after_create = ConnectionError("response lost")
    client.fail_list_after_create = ConnectionError("server unreachable")
    in_doubt = await service.spawn(request())
    assert in_doubt.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
    assert in_doubt.in_doubt_reason == "submission_unobservable"
    incident = authority.incidents[("tenant-a", in_doubt.child_execution_id)]
    assert incident.status == "operator_required" and incident.incident_id == in_doubt.incident_id
    assert not any(ref.startswith("orphaned") for _s, _c, _k, ref in authority.facts)

    # A second spawn with the same idempotency key never creates a second run.
    assert (await service.spawn(request())).lifecycle == AsyncSubagentLifecycle.IN_DOUBT
    assert client.calls.count("sdk.runs.create") == 1

    # REQ-CP-DA-008 observation exit: exactly one verified run binds the child.
    client.fail_list = None
    observed = await service.reconcile("tenant-a", in_doubt.child_execution_id)
    assert observed.lifecycle == AsyncSubagentLifecycle.RUNNING
    assert observed.incident_id is None
    assert (
        authority.incidents[("tenant-a", in_doubt.child_execution_id)].resolution == "observation"
    )


@pytest.mark.asyncio
async def test_multiple_provider_runs_are_in_doubt_and_adopt_provider_run_cancels_the_rest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, _details, authority = governed(client)
    running = await service.spawn(request())
    child_id = running.child_execution_id
    foreign = client.add_foreign_run(child_id, child_id)

    in_doubt = await service.reconcile("tenant-a", child_id)
    assert in_doubt.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
    assert in_doubt.in_doubt_reason == "multiple_provider_runs"
    assert set(authority.incidents[("tenant-a", child_id)].candidate_run_ids) == {
        running.provider_run_id,
        foreign,
    }
    with pytest.raises(AsyncSubagentError, match="does not carry"):
        await service.reconcile_in_doubt(
            "tenant-a",
            child_id,
            "adopt_provider_run",
            actor=reconciler(),
            decision_id="decision-1",
            run_id="run-unknown",
            reason="operator",
            decided_at=NOW,
        )
    adopted = await service.reconcile_in_doubt(
        "tenant-a",
        child_id,
        "adopt_provider_run",
        actor=reconciler(),
        decision_id="decision-1",
        run_id=running.provider_run_id,
        reason="operator adopts the original run",
        decided_at=NOW,
    )
    assert adopted.lifecycle == AsyncSubagentLifecycle.RUNNING
    assert adopted.provider_run_id == running.provider_run_id
    statuses = {run["run_id"]: run["status"] for run in client.runs_of(child_id)}
    assert statuses[foreign] == "interrupted" and statuses[running.provider_run_id] == "running"
    duplicates = [
        record for _scope, record in authority.provider_runs if record.provider_run_id == foreign
    ]
    assert duplicates[0].disposition == "duplicate_cancelled"
    assert duplicates[0].usage.attribution == "pending"
    incident = authority.incidents[("tenant-a", child_id)]
    assert incident.status == "resolved" and incident.adopted_run_id == running.provider_run_id
    replay = await service.reconcile_in_doubt(
        "tenant-a",
        child_id,
        "adopt_provider_run",
        actor=reconciler(),
        decision_id="decision-1",
        run_id=running.provider_run_id,
        reason="resend",
        decided_at=NOW,
    )
    assert replay.lifecycle == AsyncSubagentLifecycle.RUNNING
    assert client.calls.count("sdk.runs.cancel") == 1


@pytest.mark.asyncio
async def test_orphan_child_cancels_every_run_and_records_their_usage_as_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, _details, authority = governed(client)
    running = await service.spawn(request())
    child_id = running.child_execution_id
    foreign = client.add_foreign_run(child_id, child_id)
    await service.reconcile("tenant-a", child_id)
    orphaned = await service.reconcile_in_doubt(
        "tenant-a",
        child_id,
        "orphan_child",
        actor=reconciler(),
        decision_id="decision-orphan",
        reason="operator orphans the child",
        decided_at=NOW,
    )
    assert orphaned.lifecycle == AsyncSubagentLifecycle.ORPHANED
    assert {run["status"] for run in client.runs_of(child_id)} == {"interrupted"}
    cancelled = {record.provider_run_id: record for _s, record in authority.provider_runs}
    assert cancelled[foreign].disposition == "orphaned_cancelled"
    assert cancelled[running.provider_run_id].disposition == "orphaned_cancelled"
    assert all(record.usage.attribution == "pending" for record in cancelled.values())
    assert ("tenant-a", child_id, "lifecycle", "orphaned:orphan_child") in authority.facts
    with pytest.raises(AsyncSubagentError, match="already resolved by another decision"):
        await service.reconcile_in_doubt(
            "tenant-a",
            child_id,
            "orphan_child",
            actor=reconciler(),
            decision_id="decision-other",
            reason="again",
            decided_at=NOW,
        )


@pytest.mark.asyncio
async def test_served_graph_mismatch_fails_before_submission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(
        served=AsyncServedGraphIdentity(
            graph_id="research-graph",
            graph_revision="agent.research@2",
            graph_binding_digest=GRAPH_BINDING_DIGEST,
            deepagents_version="0.7.5",
        )
    )
    install(monkeypatch, client)
    service, details, _authority = governed(client)
    with pytest.raises(AsyncServedGraphMismatch):
        await service.spawn(request())
    child = next(iter(details.executions.values()))
    assert child.lifecycle == AsyncSubagentLifecycle.ADMITTED
    assert "sdk.runs.create" not in client.calls


@pytest.mark.asyncio
async def test_completed_thread_served_by_another_graph_is_in_doubt_not_admitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, _details, _authority = governed(client)
    running = await service.spawn(request())
    client.served = SERVED.model_copy(update={"graph_revision": "agent.research@9"})
    client.complete(running.child_execution_id, "foreign result")
    observed = await service.reconcile("tenant-a", running.child_execution_id)
    assert observed.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
    assert observed.in_doubt_reason == "graph_identity_mismatch"
    assert observed.result_manifest is None


@pytest.mark.asyncio
async def test_unreported_usage_is_recorded_pending_never_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED, tokens_per_turn=None)
    install(monkeypatch, client)
    service, _details, _authority = governed(client)
    running = await service.spawn(request())
    client.complete(running.child_execution_id, "done")
    completed = await service.reconcile("tenant-a", running.child_execution_id)
    assert completed.result_manifest is not None
    assert completed.result_manifest.usage.attribution == "pending"
    assert completed.result_manifest.usage.pending_amounts == {"model.turns": 1}


@pytest.mark.asyncio
async def test_fork_admission_classifies_active_children_as_not_quiescent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, details, _authority = governed(client)
    running = await service.spawn(request())
    classified = classify_async_children_for_fork(tuple(details.executions.values()))
    assert classified.admission == "prohibited"
    assert classified.reason == "snapshot_not_quiescent"
    assert classified.active_child_execution_ids == (running.child_execution_id,)
    client.complete(running.child_execution_id, "done")
    await service.reconcile("tenant-a", running.child_execution_id)
    quiescent = classify_async_children_for_fork(tuple(details.executions.values()))
    assert quiescent.admission == "quiescent" and quiescent.active_child_execution_ids == ()
    assert classify_async_children_for_fork(()).admission == "quiescent"


def test_contract_freezes_the_hosted_graph_identity_in_its_digest() -> None:
    base = contract()
    drifted = contract().model_copy(update={"graph_revision": "agent.research@2"})
    with pytest.raises(ValueError, match="digest mismatch"):
        type(base).model_validate(drifted.model_dump(mode="python"))
    assert base.deployment_credential_ref == "environment:AGENT_SERVER_TOKEN"
    assert base.graph_binding_digest == GRAPH_BINDING_DIGEST


@pytest.mark.asyncio
async def test_parent_deep_agent_start_async_task_reserves_and_links_before_the_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real spawn path offline: `operation.execute` cognition calls the governed tool.

    The parent Deep Agent is a real `create_deep_agent` graph compiled through the adapter; its
    `start_async_task` is the BellLabs-governed tool with the stock 0.7.5 name and schema. The
    child is reserved, linked and claimed before the fake provider sees the submission.
    """

    from langgraph.checkpoint.memory import InMemorySaver
    from langgraph.store.memory import InMemoryStore

    from mission_control.adapters.deep_agents import (
        DeepAgentRuntimeAdapter,
        ExactComponentRegistry,
        ExactDeepAgentMaterializer,
        StateSandboxFactory,
    )
    from mission_control.adapters.deep_agents.async_subagents import (
        BellLabsAsyncSubagentMiddleware,
    )
    from mission_control.application.execution.operations.checkpoint_lineage import (
        CheckpointLineageService,
        InMemoryCheckpointLineageRepository,
    )
    from mission_control.domain.execution.contracts import DeepAgentExecutionBinding
    from tests.acceptance.control_plane.test_wp_cp_040 import (
        exact_fixture,
        planned_invocation,
        unit_bound,
    )
    from tests.fixtures.rrm013_live_stack import ParentSpawnModel

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    base, _profile, bundle = exact_fixture()
    agreement = contract()
    binding = DeepAgentExecutionBinding.create(
        **{
            **base.model_dump(mode="python", exclude={"binding_digest", "async_subagents"}),
            "async_subagents": (agreement,),
        }
    )
    model = ParentSpawnModel(subagent_type=agreement.name)
    details = InMemoryAsyncSubagentDetailRepository()
    authority = InMemoryAsyncSubagentAuthority()
    provider = DeepAgentsAsyncSubagentAdapter(secrets=SECRETS, request_scope="tenant-1")
    service = AsyncSubagentService(details, authority, provider, allow_new_spawns=True)
    events: list[str] = []
    original_start = provider.start

    async def observed_start(*args: object, **kwargs: object) -> object:
        events.append(("provider.start", tuple(details.executions), tuple(authority.reservations)))
        return await original_start(*args, **kwargs)

    monkeypatch.setattr(provider, "start", observed_start)

    class Factory:
        def middleware(
            self, op_binding: object, contracts: object, secrets: object
        ) -> BellLabsAsyncSubagentMiddleware:
            del secrets
            return BellLabsAsyncSubagentMiddleware(
                service=service,
                adapter=provider,
                binding=op_binding,  # type: ignore[arg-type]
                contracts=contracts,  # type: ignore[arg-type]
            )

    adapter = DeepAgentRuntimeAdapter(
        ExactDeepAgentMaterializer(
            ExactComponentRegistry(
                model_factories={binding.model.ref.digest: lambda _b, _s: model},
                skill_bundles={bundle.bundle_digest: bundle},
                sandbox_factories={binding.sandbox.ref.digest: StateSandboxFactory()},
                checkpointers={binding.checkpointer_ref.digest: InMemorySaver()},
                stores={binding.store_ref.digest: InMemoryStore()},
            )
        ),
        async_subagents=Factory(),
    )
    lineage = CheckpointLineageService(InMemoryCheckpointLineageRepository())
    invocation = await planned_invocation(unit_bound(binding), lineage)
    result = await adapter.execute(invocation, {})

    children = list(details.executions.values())
    assert len(children) == 1
    child = children[0]
    assert result.structured_output is not None
    assert str(result.structured_output["spawned"]).endswith(child.child_execution_id)
    assert child.parent_binding_id == invocation.binding.binding_id
    assert child.lifecycle == AsyncSubagentLifecycle.RUNNING
    # Reservation, link and execution existed before the provider start.
    assert len(events) == 1
    assert child.child_execution_id in {key[1] for key in events[0][1]}
    assert child.child_execution_id in {key[1] for key in events[0][2]}
    assert len(client.runs_of(child.child_execution_id)) == 1
    assert client.calls.count("sdk.runs.create") == 1
    # The same tool call (a resumed invocation) rebuilds the same child identity.
    middleware = Factory().middleware(invocation.binding, (agreement,), {})
    spawn = middleware.spawn_request(
        description=model.objective, contract=agreement, tool_call_id="rrm013-start-async-task"
    )
    assert spawn.idempotency_key == "rrm013-start-async-task"
    assert (await service.spawn(spawn)).child_execution_id == child.child_execution_id
    assert client.calls.count("sdk.runs.create") == 1


async def governed_with_parent(
    client: FakeAgentProtocolClient,
    *,
    cancel_ack_interval: float = 0.0,
    bounded: dict[str, int] | None = None,
) -> tuple[AsyncSubagentService, object, str, InMemoryAsyncSubagentAuthority]:
    """A governed service whose children are effects and reservations of a real in-memory run."""

    run_control, run_id = await admitted_parent(bounded=bounded)
    authority = InMemoryAsyncSubagentAuthority()
    service = AsyncSubagentService(
        InMemoryAsyncSubagentDetailRepository(),
        authority,
        DeepAgentsAsyncSubagentAdapter(
            secrets=SECRETS,
            request_scope="tenant-1",
            cancel_ack_interval_seconds=cancel_ack_interval,
        ),
        parent_effects=RunControlAsyncChildEffects(run_control, actor=actor()),  # type: ignore[arg-type]
        allow_new_spawns=True,
    )
    return service, run_control, run_id, authority


async def parent_budget_view(run_control: object, run_id: str, child_id: str) -> dict[str, object]:
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    effects = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    usage = budget.usage_records.get(async_child_usage_id(child_id))
    claim = effects.claims[async_child_effect_id(child_id)]
    return {
        "actual": {k: v for k, v in usage.actual_amounts.items() if v} if usage else None,
        "pending": dict(usage.pending_external_amounts) if usage else None,
        "outstanding": async_child_usage_id(child_id) in budget.outstanding_usage_ids,
        "effect_settled": claim.settlement is not None,
        # The claim turns terminal at settlement; until then the latest observation tells.
        "effect_disposition": (
            claim.observations[-1].disposition.value if claim.observations else "claimed"
        ),
    }


@pytest.mark.asyncio
async def test_orphaned_child_settles_pending_usage_up_to_its_budget_ceiling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-013 review B1: cancelled runs with unknown usage never settle as zero."""

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, run_control, run_id, _authority = await governed_with_parent(client)
    running = await service.spawn(spawn(run_id))
    child_id = running.child_execution_id
    client.add_foreign_run(child_id, child_id)
    await service.reconcile("tenant-1", child_id)
    orphaned = await service.reconcile_in_doubt(
        "tenant-1",
        child_id,
        "orphan_child",
        actor=reconciler(),
        decision_id="decision-orphan",
        reason="operator",
        decided_at=NOW,
    )
    assert orphaned.lifecycle == AsyncSubagentLifecycle.ORPHANED
    # A blocking child without a result is rejected (never admitted) before it settles.
    with pytest.raises(AsyncSubagentError, match="requires a typed result manifest"):
        await service.decide_result(
            "tenant-1", child_id, "admit", parent_open=True, current_generation=1, decided_at=NOW
        )
    await service.decide_result(
        "tenant-1", child_id, "reject", parent_open=True, current_generation=1, decided_at=NOW
    )
    link = await service.settle("tenant-1", child_id, "settlement:orphan", NOW)
    assert link.settled is False
    assert link.usage_disposition == "pending_usage"
    assert link.settlement_revision == 1
    view = await parent_budget_view(run_control, run_id, child_id)
    # Two cancelled runs of unknown usage: the pending ceiling is the child's budget limit.
    assert view == {
        "actual": {},
        "pending": {"tokens.total": 10},
        "outstanding": True,
        "effect_settled": False,
        "effect_disposition": "cancelled",
    }
    # N1: once the provider's usage is known, reconciliation settles the outstanding usage.
    settled = await service.reconcile_usage(
        "tenant-1",
        child_id,
        actor=reconciler(),
        run_usage={
            run_id_: AsyncSubagentUsage(
                provider_run_id=run_id_,
                attribution="provider_attributed",
                attributed_amounts={"tokens.total": amount},
            )
            for run_id_, amount in zip(
                [run["run_id"] for run in client.runs_of(child_id)], (2, 1), strict=True
            )
        },
        settlement_ref="settlement:orphan",
        reconciled_at=NOW,
    )
    assert settled.settled is True and settled.settlement_revision == 2
    view = await parent_budget_view(run_control, run_id, child_id)
    assert view["effect_settled"] is True and view["outstanding"] is False
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    assert budget.consumed["tokens.total"] == 3
    assert budget.pending_settlement["tokens.total"] == 0


@pytest.mark.asyncio
async def test_adopted_child_with_a_duplicate_keeps_the_duplicate_usage_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, run_control, run_id, _authority = await governed_with_parent(client)
    running = await service.spawn(spawn(run_id))
    child_id = running.child_execution_id
    client.add_foreign_run(child_id, child_id)
    await service.reconcile("tenant-1", child_id)
    await service.reconcile_in_doubt(
        "tenant-1",
        child_id,
        "adopt_provider_run",
        actor=reconciler(),
        decision_id="decision-adopt",
        run_id=running.provider_run_id,
        reason="operator",
        decided_at=NOW,
    )
    client.complete(child_id, "done", run_id=running.provider_run_id)
    completed = await service.reconcile("tenant-1", child_id)
    assert completed.lifecycle == AsyncSubagentLifecycle.COMPLETED
    await service.decide_result(
        "tenant-1", child_id, "admit", parent_open=True, current_generation=1, decided_at=NOW
    )
    link = await service.settle("tenant-1", child_id, "settlement:adopt", NOW)
    assert link.settled is False and link.usage_disposition == "pending_usage"
    view = await parent_budget_view(run_control, run_id, child_id)
    # The adopted run's 7 tokens are attributed; the duplicate's unknown usage is pending up
    # to the child's budget ceiling; the effect is not settled.
    assert view["actual"] == {"tokens.total": 7}
    assert view["pending"] == {"tokens.total": 10}
    assert view["outstanding"] is True and view["effect_settled"] is False


@pytest.mark.asyncio
async def test_cancelled_running_child_and_failed_child_leave_usage_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, run_control, run_id, _authority = await governed_with_parent(client)
    cancelled_child = await service.spawn(spawn(run_id))
    cancelled = await service.cancel(
        "tenant-1", cancelled_child.child_execution_id, "parent cancelled", NOW
    )
    assert cancelled.lifecycle == AsyncSubagentLifecycle.CANCELLED
    await service.decide_result(
        "tenant-1",
        cancelled_child.child_execution_id,
        "reject",
        parent_open=True,
        current_generation=1,
        decided_at=NOW,
    )
    link = await service.settle(
        "tenant-1", cancelled_child.child_execution_id, "settlement:cancelled", NOW
    )
    assert link.settled is False and link.usage_disposition == "pending_usage"
    view = await parent_budget_view(run_control, run_id, cancelled_child.child_execution_id)
    assert view["pending"] == {"tokens.total": 10} and view["effect_settled"] is False

    failed_request = spawn(run_id).model_copy(
        update={
            "idempotency_key": "spawn-failed",
            "reservation_id": "reservation-child-failed",
        }
    )
    failed_child = await service.spawn(failed_request)
    client.set_status(failed_child.child_execution_id, failed_child.provider_run_id or "", "error")
    failed = await service.reconcile("tenant-1", failed_child.child_execution_id)
    assert failed.lifecycle == AsyncSubagentLifecycle.FAILED
    await service.decide_result(
        "tenant-1",
        failed_child.child_execution_id,
        "reject",
        parent_open=True,
        current_generation=1,
        decided_at=NOW,
    )
    link = await service.settle(
        "tenant-1", failed_child.child_execution_id, "settlement:failed", NOW
    )
    assert link.settled is False and link.usage_disposition == "pending_usage"
    view = await parent_budget_view(run_control, run_id, failed_child.child_execution_id)
    assert view["pending"] == {"tokens.total": 10}
    assert view["effect_disposition"] == "failed" and view["effect_settled"] is False


@pytest.mark.asyncio
async def test_unacknowledged_cancel_stays_a_candidate_until_a_terminal_status_is_observed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-013 review N2."""

    client = FakeAgentProtocolClient(served=SERVED, cancel_sticks=True)
    install(monkeypatch, client)
    service, _run_control, run_id, authority = await governed_with_parent(client)
    running = await service.spawn(spawn(run_id))
    child_id = running.child_execution_id
    foreign = client.add_foreign_run(child_id, child_id)
    await service.reconcile("tenant-1", child_id)
    adopted = await service.reconcile_in_doubt(
        "tenant-1",
        child_id,
        "adopt_provider_run",
        actor=reconciler(),
        decision_id="decision-adopt",
        run_id=running.provider_run_id,
        reason="operator",
        decided_at=NOW,
    )
    assert adopted.provider_run_id == running.provider_run_id
    records = {
        r.provider_run_id: r for r in await authority.list_provider_runs("tenant-1", child_id)
    }
    assert records[foreign].disposition == "cancel_ambiguous"
    assert records[foreign].usage.attribution == "ambiguous"
    # The duplicate is still running on the provider: it remains a candidate, so the child is
    # in doubt again (a new incident revision) rather than silently bound.
    again = await service.reconcile("tenant-1", child_id)
    assert again.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
    assert again.in_doubt_reason == "multiple_provider_runs"
    assert authority.incidents[("tenant-1", child_id)].revision == 2
    # Once the provider shows the duplicate terminal, it is cancelled for good and excluded.
    client.set_status(child_id, foreign, "interrupted")
    bound = await service.reconcile("tenant-1", child_id)
    assert bound.lifecycle == AsyncSubagentLifecycle.RUNNING
    assert bound.provider_run_id == running.provider_run_id
    records = {
        r.provider_run_id: r for r in await authority.list_provider_runs("tenant-1", child_id)
    }
    assert records[foreign].disposition == "duplicate_cancelled"
    assert records[foreign].usage.attribution == "pending"


@pytest.mark.asyncio
async def test_in_doubt_child_whose_single_run_completed_resolves_its_incident_by_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-013 review N3."""

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, run_control, run_id, authority = await governed_with_parent(client)
    client.fail_after_create = ConnectionError("response lost")
    client.fail_list_after_create = ConnectionError("server unreachable")
    in_doubt = await service.spawn(spawn(run_id))
    child_id = in_doubt.child_execution_id
    assert in_doubt.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
    client.fail_list = None
    client.complete(child_id, "late but complete")
    completed = await service.reconcile("tenant-1", child_id)
    assert completed.lifecycle == AsyncSubagentLifecycle.COMPLETED
    incident = authority.incidents[("tenant-1", child_id)]
    assert incident.status == "resolved" and incident.resolution == "observation"
    records = await authority.list_provider_runs("tenant-1", child_id)
    assert [record.disposition for record in records] == ["bound"]
    assert records[0].usage.attribution == "provider_attributed"
    assert records[0].usage.attributed_amounts == {"tokens.total": 7}
    effects = await run_control.get_effects("tenant-1", run_id)  # type: ignore[attr-defined]
    claim = effects.claims[async_child_effect_id(child_id)]
    # Terminal observations are recorded; the claim's disposition turns terminal at settlement.
    assert claim.observations[-1].disposition.value == "succeeded"
    assert claim.settlement is None


@pytest.mark.asyncio
async def test_operator_decisions_require_the_privilege_and_are_exclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-013 review N5."""

    import asyncio

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, _run_control, run_id, authority = await governed_with_parent(client)
    running = await service.spawn(spawn(run_id))
    child_id = running.child_execution_id
    client.add_foreign_run(child_id, child_id)
    await service.reconcile("tenant-1", child_id)
    with pytest.raises(AsyncSubagentDecisionRejected, match="reconcile_async_child"):
        await service.reconcile_in_doubt(
            "tenant-1",
            child_id,
            "orphan_child",
            actor=actor(),
            decision_id="unprivileged",
            reason="no permission",
            decided_at=NOW,
        )
    assert "sdk.runs.cancel" not in client.calls
    results = await asyncio.gather(
        service.reconcile_in_doubt(
            "tenant-1",
            child_id,
            "adopt_provider_run",
            actor=reconciler(),
            decision_id="decision-a",
            run_id=running.provider_run_id,
            reason="operator a",
            decided_at=NOW,
        ),
        service.reconcile_in_doubt(
            "tenant-1",
            child_id,
            "orphan_child",
            actor=reconciler(),
            decision_id="decision-b",
            reason="operator b",
            decided_at=NOW,
        ),
        return_exceptions=True,
    )
    winners = [item for item in results if isinstance(item, AsyncSubagentExecution)]
    losers = [item for item in results if isinstance(item, AsyncSubagentDecisionRejected)]
    assert len(winners) == 1 and len(losers) == 1
    assert client.calls.count("sdk.runs.cancel") in {1, 2}
    held = authority.reconciliation_decisions[("tenant-1", child_id)]
    assert held[0] in {"decision-a", "decision-b"}
    incident = authority.incidents[("tenant-1", child_id)]
    assert incident.status == "resolved" and incident.decision_id == held[0]


def test_usage_is_attributed_only_when_every_ai_turn_is_reported() -> None:
    """RRM-013 review N6."""

    from mission_control.adapters.deep_agents.async_subagents import attribute_usage

    limits = {"tokens.total": 100, "model.turns": 4}
    stamped = [{"message_id": "a", "total_tokens": 5}]
    messages = [
        {"type": "human", "content": "x"},
        {"type": "ai", "id": "a", "content": "y"},
        {"type": "ai", "id": "b", "content": "z"},
    ]
    partial = attribute_usage("run", messages, limits, provider_usage=stamped)
    assert partial.attribution == "pending" and partial.pending_amounts == {"model.turns": 2}
    full = attribute_usage(
        "run",
        messages,
        limits,
        provider_usage=[*stamped, {"message_id": "b", "total_tokens": 6}],
    )
    assert full.attribution == "provider_attributed"
    assert full.attributed_amounts == {"tokens.total": 11, "model.turns": 2}
    none = attribute_usage("run", [{"type": "human", "content": "x"}], limits, provider_usage=[])
    assert none.attribution == "pending" and none.attributed_amounts == {}


@pytest.mark.asyncio
async def test_partly_reported_completed_child_keeps_the_unstamped_dimension_pending(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-013 re-review G1: a counted turn with unstamped tokens never drops `tokens.total`."""

    client = FakeAgentProtocolClient(served=SERVED, tokens_per_turn=None)
    install(monkeypatch, client)
    service, run_control, run_id, _authority = await governed_with_parent(
        client, bounded={"model.turns": 4}
    )
    limits = {"tokens.total": 10, "model.turns": 2}
    running = await service.spawn(spawn(run_id, limits=limits))
    child_id = running.child_execution_id
    client.complete(child_id, "done")
    completed = await service.reconcile("tenant-1", child_id)
    assert completed.result_manifest is not None
    usage = completed.result_manifest.usage
    assert usage.attribution == "pending" and usage.pending_amounts == {"model.turns": 1}
    # Domain: the unreported dimension is unknown, so it is pending at the child's ceiling.
    record = AsyncProviderRunRecord(
        child_execution_id=child_id,
        provider_thread_id=child_id,
        provider_run_id=running.provider_run_id,
        disposition="bound",
        provider_status="success",
        usage=usage,
        observed_at=NOW,
    )
    assert aggregate_child_usage((record,), limits) == (
        {},
        {"model.turns": 1, "tokens.total": 10},
    )
    await service.decide_result(
        "tenant-1", child_id, "admit", parent_open=True, current_generation=1, decided_at=NOW
    )
    link = await service.settle("tenant-1", child_id, "settlement:partial", NOW)
    assert link.settled is False and link.usage_disposition == "pending_usage"
    view = await parent_budget_view(run_control, run_id, child_id)
    assert view == {
        "actual": {},
        "pending": {"model.turns": 1, "tokens.total": 10},
        "outstanding": True,
        "effect_settled": False,
        "effect_disposition": "succeeded",
    }


@pytest.mark.asyncio
async def test_orphan_child_is_the_exit_under_a_served_graph_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-013 re-review G2: identity is verified for adopt only; orphan cancels every run."""

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, _details, authority = governed(client)
    running = await service.spawn(request())
    child_id = running.child_execution_id
    foreign = client.add_foreign_run(child_id, child_id)
    in_doubt = await service.reconcile("tenant-a", child_id)
    assert in_doubt.lifecycle == AsyncSubagentLifecycle.IN_DOUBT
    client.served = SERVED.model_copy(update={"graph_revision": "agent.research@9"})
    with pytest.raises(AsyncServedGraphMismatch):
        await service.reconcile_in_doubt(
            "tenant-a",
            child_id,
            "adopt_provider_run",
            actor=reconciler(),
            decision_id="decision-adopt",
            run_id=running.provider_run_id,
            reason="operator adopts under a mismatched server",
            decided_at=NOW,
        )
    assert "sdk.runs.cancel" not in client.calls
    assert authority.incidents[("tenant-a", child_id)].status == "operator_required"
    orphaned = await service.reconcile_in_doubt(
        "tenant-a",
        child_id,
        "orphan_child",
        actor=reconciler(),
        decision_id="decision-orphan",
        reason="operator orphans the child",
        decided_at=NOW,
    )
    assert orphaned.lifecycle == AsyncSubagentLifecycle.ORPHANED
    statuses = {run["run_id"]: run["status"] for run in client.runs_of(child_id)}
    assert statuses == {running.provider_run_id: "interrupted", foreign: "interrupted"}
    incident = authority.incidents[("tenant-a", child_id)]
    assert incident.status == "resolved" and incident.resolution == "orphan_child"
    assert {
        record.disposition
        for _scope, record in authority.provider_runs
        if record.child_execution_id == child_id
    } == {"orphaned_cancelled"}


@pytest.mark.asyncio
async def test_reconcile_usage_requires_the_privilege_and_consumes_attributed_overage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """RRM-013 re-review G3."""

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, run_control, run_id, _authority = await governed_with_parent(client)
    running = await service.spawn(spawn(run_id))
    child_id = running.child_execution_id
    client.add_foreign_run(child_id, child_id)
    await service.reconcile("tenant-1", child_id)
    await service.reconcile_in_doubt(
        "tenant-1",
        child_id,
        "orphan_child",
        actor=reconciler(),
        decision_id="decision-orphan",
        reason="operator",
        decided_at=NOW,
    )
    await service.decide_result(
        "tenant-1", child_id, "reject", parent_open=True, current_generation=1, decided_at=NOW
    )
    pending = await service.settle("tenant-1", child_id, "settlement:overage", NOW)
    assert pending.usage_disposition == "pending_usage" and pending.settlement_revision == 1
    run_ids = [run["run_id"] for run in client.runs_of(child_id)]
    run_usage = {
        run_id_: AsyncSubagentUsage(
            provider_run_id=run_id_,
            attribution="provider_attributed",
            attributed_amounts={"tokens.total": amount},
        )
        for run_id_, amount in zip(run_ids, (8, 5), strict=True)
    }
    # Any caller could otherwise assert attributed usage and settle the parent's effect.
    with pytest.raises(AsyncSubagentDecisionRejected, match="reconcile_async_child"):
        await service.reconcile_usage(
            "tenant-1",
            child_id,
            actor=actor(),
            run_usage=run_usage,
            settlement_ref="settlement:overage",
            reconciled_at=NOW,
        )
    view = await parent_budget_view(run_control, run_id, child_id)
    assert view["effect_settled"] is False and view["pending"] == {"tokens.total": 10}
    settled = await service.reconcile_usage(
        "tenant-1",
        child_id,
        actor=reconciler(),
        run_usage=run_usage,
        settlement_ref="settlement:overage",
        reconciled_at=NOW,
    )
    assert settled.settled is True and settled.settlement_revision == 2
    budget = await run_control.get_budget("tenant-1", run_id)  # type: ignore[attr-defined]
    # 13 attributed against a 10-token pending ceiling: 10 reconcile the pending amount, 3 are
    # consumed as overage; nothing is dropped and nothing is released.
    assert budget.consumed["tokens.total"] == 13
    assert budget.pending_settlement["tokens.total"] == 0
    settlement = budget.usage_settlements[f"{async_child_usage_id(child_id)}:settlement:2"]
    assert settlement.settled_amounts == {"tokens.total": 13}
    assert settlement.released_amounts == {}
    assert settlement.source_pending_amounts == {"tokens.total": 10}
    view = await parent_budget_view(run_control, run_id, child_id)
    assert view["effect_settled"] is True and view["outstanding"] is False


@pytest.mark.asyncio
async def test_cancel_transport_error_crosses_neutral_port_and_records_ambiguity(monkeypatch):
    import httpx

    from mission_control.application.ports.provider_errors import ProviderTransportError

    client = FakeAgentProtocolClient(served=SERVED)
    install(monkeypatch, client)
    service, details, _authority = governed(client)
    child = await service.spawn(request())

    async def unavailable(*_args, **_kwargs):
        raise httpx.ConnectError("offline fixture transport failed")

    monkeypatch.setattr(client.runs, "cancel", unavailable)
    with pytest.raises(ProviderTransportError):
        await service.cancel("tenant-a", child.child_execution_id, "parent stopped", NOW)
    link = await details.get_link("tenant-a", child.child_execution_id)
    assert link.cancellation_requested
    assert link.cancellation_receipt == "ambiguous"
    stored = await details.get_execution("tenant-a", child.child_execution_id)
    assert stored.lifecycle == AsyncSubagentLifecycle.RUNNING
