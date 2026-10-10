"""MP-11 broker: bind before wait, bounded wait, revalidation, replay and recovery.

In-memory stores (FIXTURE semantics of ``adapters/postgres/approvals``; persistence and
concurrency are proven in ``tests/integration/postgres/test_mp11_*``). No provider is called:
the "native request" is the fixture request object a lane adapter would build.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from mission_control.application.execution.approvals_broker import ApprovalBroker
from mission_control.application.execution.approvals_memory import (
    InMemoryApprovalStore,
    StaticApprovalContext,
)
from mission_control.application.execution.stop_fence import InMemoryStopFenceRepository
from mission_control.application.human_tasks.memory import InMemoryHumanTaskRepository
from mission_control.application.human_tasks.service import HumanTaskService
from mission_control.domain.execution.approvals import NativeApprovalCorrelation
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.stop_fence import StopFence
from tests.fixtures.provider_frames import SCOPE
from tests.unit.approvals.fixtures import (
    OTHER_POLICY,
    POLICY,
    REVIEWER,
    RUN,
    Clock,
    answer,
    connection_scoped_request,
    elicitation_request,
    permission_request,
)

OWNER = ActorContext(actor_id=REVIEWER)


def _compose(
    connection_ref: str = "worker-a#1",
    store: InMemoryApprovalStore | None = None,
    context: StaticApprovalContext | None = None,
    fences: InMemoryStopFenceRepository | None = None,
) -> tuple[ApprovalBroker, HumanTaskService, InMemoryApprovalStore, StaticApprovalContext]:
    store = store or InMemoryApprovalStore()
    context = context or StaticApprovalContext(1, POLICY)
    broker = ApprovalBroker(
        store,
        store,
        probe=context,
        connection_ref=connection_ref,
        fences=fences,
        poll_seconds=0.01,
    )
    service = HumanTaskService(
        InMemoryHumanTaskRepository(),
        request_scope=SCOPE,
        approvals=store,
        approval_wake=broker.hub,
    )
    return broker, service, store, context


async def test_binding_is_persisted_before_the_wait_and_a_resolution_wakes_it() -> None:
    broker, service, store, _ = _compose()
    bound = await broker.bind(permission_request())
    # The durable task and its live correlation exist before anyone waits.
    stored = await store.get_task(SCOPE, bound.task.human_task_id)
    assert stored is not None and stored.lifecycle == "open"
    assert stored.packet.binding.schema_version == "mc.approval_binding.v1"
    assert (await store.get_correlation(SCOPE, bound.correlation.correlation_id)).state == "live"  # type: ignore[union-attr]

    waiter = asyncio.create_task(broker.wait(bound, wait_seconds=5))
    await asyncio.sleep(0.02)
    await service.resolve(bound.task.human_task_id, answer(bound.task), OWNER)
    outcome = await asyncio.wait_for(waiter, 2)
    assert outcome.status == "approved" and outcome.reply.action == "allow"
    closed = await store.get_correlation(SCOPE, bound.correlation.correlation_id)
    assert closed is not None and closed.state == "answered" and closed.reply == outcome.reply


async def test_callback_timeout_expires_the_correlation_not_the_task() -> None:
    broker, service, store, _ = _compose()
    bound = await broker.bind(permission_request())
    outcome = await broker.wait(bound, wait_seconds=0.05)
    assert outcome.status == "expired"
    assert outcome.reply.action == "deny" and outcome.reply.interrupt
    assert outcome.reply.reason == "wait_expired"
    task = await store.get_task(SCOPE, bound.task.human_task_id)
    assert task is not None and task.lifecycle == "open"  # timeout is never implicit approval

    # The human approves later; the expired native handle is never answered.
    await service.resolve(bound.task.human_task_id, answer(bound.task), OWNER)
    expired = await store.get_correlation(SCOPE, bound.correlation.correlation_id)
    assert expired is not None and expired.state == "expired"
    assert expired.reply is not None and expired.reply.reason == "wait_expired"

    # A fresh native request for the same call (reissue strategy) gets the recorded decision.
    again = await broker.bind(permission_request())
    assert again.task.human_task_id == bound.task.human_task_id
    assert again.immediate is not None and again.immediate.status == "approved"
    assert again.immediate.replayed
    replayed = await store.get_correlation(SCOPE, again.correlation.correlation_id)
    assert replayed is not None and replayed.replayed_from == again.task.resolution.resolution_ref  # type: ignore[union-attr]


async def test_restart_never_replays_approval_into_a_different_request_or_arguments() -> None:
    store = InMemoryApprovalStore()
    before, service, _, _ = _compose("worker-a#1", store)
    bound = await before.bind(connection_scoped_request("7"))
    await service.resolve(bound.task.human_task_id, answer(bound.task), OWNER)
    first = await before.wait(bound, wait_seconds=1)
    assert first.status == "approved"

    # Process restart: a new connection; the provider reuses request id "7" by coincidence.
    after, _, _, _ = _compose("worker-a#2", store)
    coincidence = await after.bind(connection_scoped_request("7"))
    assert coincidence.task.human_task_id != bound.task.human_task_id
    assert coincidence.task.lifecycle == "open" and coincidence.immediate is None

    # Modified arguments on the original stable call id: a new digest and a new review.
    stable, service2, _, _ = _compose("worker-a#2", store)
    original = await stable.bind(permission_request())
    await service2.resolve(original.task.human_task_id, answer(original.task), OWNER)
    edited = await stable.bind(
        permission_request(arguments={"command": "git push --force origin main", "timeout": 30})
    )
    assert edited.task.human_task_id != original.task.human_task_id
    assert edited.immediate is None and edited.task.lifecycle == "open"
    assert edited.task.packet.binding.input_digest != original.task.packet.binding.input_digest


async def test_recover_marks_lost_handles_and_follows_the_replay_strategy() -> None:
    store = InMemoryApprovalStore()
    crashed, _, _, _ = _compose("worker-a#1", store)
    reissue = await crashed.bind(permission_request())
    parked = await crashed.bind(connection_scoped_request("9"))
    restarted, service, _, _ = _compose("worker-a#2", store)
    items = await restarted.recover(SCOPE, reissue.task.packet.binding.harness_execution_id)
    assert {item.correlation.correlation_id for item in items} == {
        reissue.correlation.correlation_id,
        parked.correlation.correlation_id,
    }
    assert all(item.correlation.state == "lost" for item in items)
    actions = {item.correlation.correlation_id: item.action for item in items}
    assert actions[reissue.correlation.correlation_id] == "await_fresh_request"
    assert actions[parked.correlation.correlation_id] == "restart_at_safe_boundary"
    # The lost handle is never answered even after the human decides.
    await service.resolve(reissue.task.human_task_id, answer(reissue.task), OWNER)
    lost = await store.get_correlation(SCOPE, reissue.correlation.correlation_id)
    assert lost is not None and lost.state == "lost" and lost.reply is None
    # A second recover finds nothing live of another connection.
    assert await restarted.recover(SCOPE, reissue.task.packet.binding.harness_execution_id) == ()


async def test_stale_approval_after_compaction_policy_change_or_stop_fence() -> None:
    fences = InMemoryStopFenceRepository()
    broker, service, _store, context = _compose(fences=fences)
    bound = await broker.bind(permission_request())
    await service.resolve(bound.task.human_task_id, answer(bound.task), OWNER)
    context.state = context.state.model_copy(update={"generation": 2})  # compaction / transfer
    stale = await broker.wait(bound, wait_seconds=1)
    assert stale.status == "stale" and stale.reply.reason == "stale_generation"
    assert stale.reply.action == "deny"

    context.state = context.state.model_copy(
        update={"generation": 1, "policy_digest": OTHER_POLICY}
    )
    changed = await broker.bind(permission_request())
    assert changed.immediate is not None and changed.immediate.reply.reason == "policy_changed"

    context.state = context.state.model_copy(update={"policy_digest": POLICY})
    await fences.persist(
        StopFence(
            request_scope=SCOPE,
            run_id=RUN,
            generation=1,
            command_id="cancel-1",
            reason="operator stop",
            requested_at=datetime(2026, 10, 8, 12, 1, tzinfo=UTC),
        )
    )
    fenced = await broker.bind(permission_request())
    assert fenced.immediate is not None and fenced.immediate.status == "fenced"
    assert fenced.immediate.reply.reason == "stop_fenced"


async def test_cancel_path_and_a_superseding_delivery_close_the_old_handle() -> None:
    broker, service, _store, _ = _compose()
    first = await broker.bind(permission_request())
    second = await broker.bind(permission_request())  # the provider re-delivered the request
    assert first.task.human_task_id == second.task.human_task_id
    superseded = await broker.wait(first, wait_seconds=1)
    assert superseded.status == "superseded"
    cancelled = await service.cancel_run_approvals(RUN, actor_ref="operator:stop")
    assert [task.human_task_id for task in cancelled] == [first.task.human_task_id]
    outcome = await broker.wait(second, wait_seconds=1)
    assert outcome.status == "task_cancelled" and outcome.reply.reason == "task_cancelled"


async def test_not_replayable_strategy_and_deny_cancel_distinct_for_elicitation() -> None:
    broker, service, _, _ = _compose()
    bound = await broker.bind(
        permission_request(
            native=NativeApprovalCorrelation(tool_call_ref="toolu_park"),
            replay_strategy="park_for_reconciliation",
        )
    )
    await service.resolve(bound.task.human_task_id, answer(bound.task), OWNER)
    assert (await broker.wait(bound, wait_seconds=1)).status == "approved"
    again = await broker.bind(
        permission_request(
            native=NativeApprovalCorrelation(tool_call_ref="toolu_park"),
            replay_strategy="park_for_reconciliation",
        )
    )
    assert again.immediate is not None and again.immediate.status == "not_replayable"

    declined = await broker.bind(elicitation_request())
    await service.resolve(
        declined.task.human_task_id, answer(declined.task, decision="deny"), OWNER
    )
    decline = await broker.wait(declined, wait_seconds=1)
    dismissed = await broker.bind(
        elicitation_request(native=NativeApprovalCorrelation(native_request_ref="elicit-2"))
    )
    await service.resolve(
        dismissed.task.human_task_id, answer(dismissed.task, decision="cancel"), OWNER
    )
    cancel = await broker.wait(dismissed, wait_seconds=1)
    assert (decline.status, decline.reply.elicitation_action) == ("denied", "decline")
    assert (cancel.status, cancel.reply.elicitation_action) == ("cancelled", "cancel")


async def test_task_deadline_expire_policy_closes_the_task_without_approval() -> None:
    clock = Clock()
    store = InMemoryApprovalStore()
    context = StaticApprovalContext(1, POLICY)
    broker = ApprovalBroker(
        store, store, probe=context, connection_ref="worker-a#1", clock=clock, poll_seconds=0.01
    )
    bound = await broker.bind(permission_request(timeout_seconds=60, on_timeout="expire"))
    clock.advance(61)
    outcome = await broker.wait(bound, wait_seconds=1)
    assert outcome.status == "task_expired" and outcome.reply.action == "deny"
    task = await store.get_task(SCOPE, bound.task.human_task_id)
    assert task is not None and task.lifecycle == "expired" and task.resolution is None
