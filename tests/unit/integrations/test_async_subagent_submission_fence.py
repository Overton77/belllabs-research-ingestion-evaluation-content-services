"""REQ-CP-DA-008 submission fence, in_doubt and its exits, against the fake Agent Protocol.

Offline regression (RRM-013). The fake client stands in for the server so the fence, the
spawn-key lookup, the typed ambiguity and the operator decisions are deterministic; the live
proof is `tests/integration/agent_server/test_rrm_013_async_subagent_live.py`.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from app.application.async_subagents.service import (
    AsyncServedGraphMismatch,
    AsyncSubagentError,
    AsyncSubagentService,
    AsyncSubagentSubmissionInProgress,
    InMemoryAsyncSubagentAuthority,
    InMemoryAsyncSubagentDetailRepository,
)
from app.domain.operation_execution.async_subagent_reconciliation import (
    AsyncServedGraphIdentity,
    classify_async_children_for_fork,
)
from app.domain.operation_execution.contracts import AsyncSubagentLifecycle
from app.integrations.agents.deep_agents.async_subagents import DeepAgentsAsyncSubagentAdapter
from tests.acceptance.control_plane.test_wp_cp_045 import (
    GRAPH_BINDING_DIGEST,
    NOW,
    SERVED,
    contract,
    request,
)
from tests.fixtures.fake_agent_protocol import FakeAgentProtocolClient, install

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
    service, details, authority = governed(client)
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
            decision_id="decision-1",
            run_id="run-unknown",
            reason="operator",
            decided_at=NOW,
        )
    adopted = await service.reconcile_in_doubt(
        "tenant-a",
        child_id,
        "adopt_provider_run",
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
