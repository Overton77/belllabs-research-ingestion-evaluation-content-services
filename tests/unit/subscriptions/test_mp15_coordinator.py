"""MP-15: bounded coordinator notifications, the durable inbox, causation bounds and prompts.

Unit level over FIXTURE stores (`coordinator_fakes`); the PostgreSQL proofs are in
`tests/integration/postgres/test_mp15_*`.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4, uuid5

import pytest

from mission_control.application.subscriptions.coordinator import (
    CoordinatorProfile,
    JournalEvent,
    NotificationKind,
    PlanInput,
    classify,
    extract_facts,
    notification_id,
    plan,
    prompt_request_id,
)
from mission_control.application.subscriptions.coordinator_service import (
    CoordinatorInboxService,
    CoordinatorRejected,
    CoordinatorSubscribeRequest,
)
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.policies.contracts import ActorContext
from tests.unit.subscriptions.coordinator_fakes import (
    MISSION,
    RUN,
    T0,
    FakeGateway,
    InMemoryInboxStore,
    journal_event,
)
from tests.unit.subscriptions.fakes import InMemoryStore

COORDINATOR = ActorContext(
    actor_id="coordinator-1",
    permissions=frozenset({"workflow_run.read", "workflow_run.control"}),
)
OTHER = ActorContext(actor_id="someone-else", permissions=frozenset({"workflow_run.read"}))
SUB = UUID("00000000-0000-7000-8000-0000000000c1")


class Clock:
    def __init__(self) -> None:
        self.now = T0 + timedelta(hours=1)

    def __call__(self) -> Any:
        return self.now


def classify_type(event_type: str, profile: CoordinatorProfile | None = None, **facts: Any):
    found = classify(
        JournalEvent(journal_event(1, event_type), facts), profile or CoordinatorProfile()
    )
    return None if found is None else (found.kind, found.actionable, found.public_event_type)


def test_default_filter_classifies_the_public_vocabulary_with_deltas_off():
    assert classify_type("human_task.created") == (
        NotificationKind.REVIEW_REQUIRED,
        True,
        "human_task.opened",
    )
    assert classify_type("human_task.opened")[0] is NotificationKind.REVIEW_REQUIRED  # type: ignore[index]
    assert classify_type("workflow_run.terminalize", terminal_outcome="completed") == (
        NotificationKind.TERMINAL_RESULT,
        True,
        "run.completed",
    )
    assert classify_type("attempt.completed", outcome="failed") == (
        NotificationKind.FAILED,
        True,
        "activation.completed",
    )
    assert classify_type("attempt.completed", outcome="succeeded") == (
        NotificationKind.CHILD_LIFECYCLE,
        False,
        "activation.completed",
    )
    assert classify_type("activation.lifecycle_changed", blocker="in_doubt")[0] is (  # type: ignore[index]
        NotificationKind.BLOCKED
    )
    assert classify_type("activation.lifecycle_changed")[0] is NotificationKind.PROGRESS  # type: ignore[index]
    assert classify_type("command.in_doubt")[0] is NotificationKind.BLOCKED  # type: ignore[index]
    assert classify_type("human_task.resolved")[0] is NotificationKind.REVIEW_CLOSED  # type: ignore[index]
    assert classify_type("workflow_run.record_output_evidence")[0] is (  # type: ignore[index]
        NotificationKind.ACCEPTED_OUTPUT
    )
    assert classify_type("workflow_run.register_async_child")[0] is (  # type: ignore[index]
        NotificationKind.CHILD_LIFECYCLE
    )
    # A wait is progress, never a review (SPEC-04: no set_wait -> human task alias).
    assert classify_type("workflow_run.set_wait")[0] is NotificationKind.PROGRESS  # type: ignore[index]
    # Token/tool deltas and per-turn bookkeeping are off by default ...
    for delta in (
        "tool_call.completed",
        "session.turn_started",
        "session.turn_completed",
        "session.usage_settled",
        "workflow_run.record_usage",
        "workflow_run.claim_effect",
    ):
        assert classify_type(delta) is None, delta
    # ... and only ever batched progress when a profile turns them on.
    on = CoordinatorProfile(include_deltas=True)
    assert classify_type("tool_call.completed", on)[0] is NotificationKind.PROGRESS  # type: ignore[index]
    # A profile that drops progress drops it entirely.
    quiet = CoordinatorProfile(kinds=(NotificationKind.REVIEW_REQUIRED,))
    assert classify_type("workflow_run.set_wait", quiet) is None
    assert classify_type("run.completed", quiet) is None


def test_profile_rejects_selecting_the_rate_limited_kind():
    with pytest.raises(ValueError):
        CoordinatorProfile(kinds=(NotificationKind.RATE_LIMITED,))
    with pytest.raises(ValueError):
        CoordinatorSubscribeRequest(
            target="run",
            target_id=RUN,
            profile=CoordinatorProfile(prompt_mode="queue_instruction"),
        )


def test_facts_are_whitelisted_scalars_never_bodies():
    facts = extract_facts(
        {
            "event_type": "human_task.created",
            "payload": {
                "human_task_id": "t-1",
                "kind": "human_gate:review",
                "permitted_decisions": ["approve", "deny"],
                "packet": {"body": "secret text"},
                "review_round": 2,
                "resolution": {"decision": "approve"},
            },
        }
    )
    assert facts == {
        "human_task_id": "t-1",
        "kind": "human_gate:review",
        "review_round": 2,
        "decision": "approve",
    }


def plan_input(events: list[JournalEvent], **overrides: Any) -> PlanInput:
    values: dict[str, Any] = {
        "subscription_id": SUB,
        "mission_id": MISSION,
        "profile": CoordinatorProfile(batch_window_seconds=60),
        "cursor_seq": 0,
        "next_inbox_seq": 1,
        "open": (),
        "recent_sealed": {},
        "depths": {},
        "events": tuple(events),
        "now": T0 + timedelta(seconds=10),
    }
    values.update(overrides)
    return PlanInput(**values)


def test_progress_is_batched_and_sealed_before_a_significant_event_of_the_same_run():
    events = [
        JournalEvent(journal_event(1, "workflow_run.start")),
        JournalEvent(journal_event(2, "activation.phase_changed")),
        JournalEvent(journal_event(3, "tool_call.completed")),  # delta: dropped
        JournalEvent(journal_event(4, "human_task.created"), {"human_task_id": "t-1"}),
        JournalEvent(journal_event(5, "workflow_run.set_wait")),
    ]
    result = plan(plan_input(events))
    sealed = sorted(result.sealed, key=lambda item: item.inbox_seq or 0)
    assert [(item.kind, item.inbox_seq) for item in sealed] == [
        (NotificationKind.PROGRESS, 1),
        (NotificationKind.REVIEW_REQUIRED, 2),
    ]
    progress, review = sealed
    assert progress.event_count == 2 and progress.counts == {
        "workflow_run.start": 1,
        "activation.phase_changed": 1,
    }
    assert review.actionable and review.facts == {"human_task_id": "t-1"}
    assert review.events[0].event_type == "human_task.opened"
    assert review.events[0].public_event_id is not None
    # The trailing progress stays open inside its window, then seals.
    (open_batch,) = [item for item in result.upserts if item.state == "open"]
    assert open_batch.seq_from == 5 and result.cursor_seq == 5
    later = plan(
        plan_input(
            [],
            cursor_seq=5,
            next_inbox_seq=3,
            open=(open_batch,),
            now=T0 + timedelta(minutes=5),
        )
    )
    assert [(item.notification_id, item.state, item.inbox_seq) for item in later.upserts] == [
        (open_batch.notification_id, "pending", 3)
    ]


def test_batches_are_bounded_by_size():
    events = [JournalEvent(journal_event(seq, "activation.phase_changed")) for seq in range(1, 8)]
    result = plan(plan_input(events, profile=CoordinatorProfile(batch_max_events=3)))
    assert [item.event_count for item in result.sealed] == [3, 3]
    assert [item.event_count for item in result.upserts if item.state == "open"] == [1]


def test_replanning_the_same_page_is_deterministic_and_events_are_deduped():
    events = [
        JournalEvent(journal_event(1, "human_task.created")),
        JournalEvent(journal_event(2, "workflow_run.terminalize")),
    ]
    first = plan(plan_input(events))
    again = plan(plan_input(events))
    assert [item.notification_id for item in first.upserts] == [
        item.notification_id for item in again.upserts
    ]
    assert first.sealed[0].notification_id == notification_id(
        SUB, NotificationKind.REVIEW_REQUIRED, events[0].envelope.event_id
    )
    # Events at or below the cursor are never planned twice.
    assert plan(plan_input(events, cursor_seq=2)).upserts == ()


def test_per_run_rate_cap_folds_overflow_into_one_summary_that_seals_when_the_window_rolls():
    profile = CoordinatorProfile(rate_limit_per_run=2, rate_window_seconds=60)
    events = [
        JournalEvent(journal_event(1, "human_task.created")),
        JournalEvent(journal_event(2, "workflow_run.record_output_evidence")),
        JournalEvent(journal_event(3, "command.in_doubt")),
        JournalEvent(journal_event(4, "attempt.completed"), {"outcome": "failed"}),
        JournalEvent(journal_event(5, "workflow_run.record_output_evidence")),
    ]
    result = plan(plan_input(events, profile=profile))
    assert [item.kind for item in result.sealed] == [
        NotificationKind.REVIEW_REQUIRED,
        NotificationKind.ACCEPTED_OUTPUT,
    ]
    (overflow,) = [item for item in result.upserts if item.state == "open"]
    assert overflow.kind is NotificationKind.RATE_LIMITED
    assert overflow.actionable  # something folded was actionable
    assert overflow.event_count == 3 and (overflow.seq_from, overflow.seq_to) == (3, 5)
    recent = {RUN: tuple(item.sealed_at for item in result.sealed)}  # type: ignore[misc]
    # Still inside the window: nothing more seals, however many events arrive.
    more = [JournalEvent(journal_event(6, "human_task.created"))]
    held = plan(
        plan_input(
            more,
            profile=profile,
            cursor_seq=5,
            next_inbox_seq=3,
            open=(overflow,),
            recent_sealed=recent,
        )
    )
    assert held.sealed == () and held.upserts[0].event_count == 4
    rolled = plan(
        plan_input(
            [],
            profile=profile,
            cursor_seq=6,
            next_inbox_seq=3,
            open=(held.upserts[0],),
            recent_sealed=recent,
            now=T0 + timedelta(minutes=5),
        )
    )
    assert [(item.kind, item.inbox_seq) for item in rolled.sealed] == [
        (NotificationKind.RATE_LIMITED, 3)
    ]


def test_events_caused_beyond_the_recursion_bound_are_suppressed_not_delivered():
    command = str(uuid4())
    events = [
        JournalEvent(journal_event(1, "human_task.created", causation_ref=command)),
        JournalEvent(journal_event(2, "activation.phase_changed", causation_ref=command)),
        JournalEvent(journal_event(3, "workflow_run.terminalize")),
    ]
    result = plan(plan_input(events, depths={command: 3}))
    suppressed = [item for item in result.upserts if item.state == "suppressed"]
    assert {item.suppressed_reason for item in suppressed} == {"recursion_bound"}
    assert {item.kind for item in suppressed} == {
        NotificationKind.REVIEW_REQUIRED,
        NotificationKind.PROGRESS,
    }
    assert all(item.inbox_seq is None for item in suppressed)
    assert [item.kind for item in result.sealed] == [NotificationKind.TERMINAL_RESULT]
    within = plan(plan_input(events[:1], depths={command: 2}))
    assert within.sealed[0].depth == 2 and within.sealed[0].causation_refs == (command,)


def service_with(
    *events: tuple[str, dict[str, Any]], gateway: FakeGateway | None = None, **kwargs: Any
) -> tuple[CoordinatorInboxService, InMemoryInboxStore, Clock]:
    subscriptions = InMemoryStore()
    inbox = InMemoryInboxStore(subscriptions)
    for seq, (event_type, facts) in enumerate(events, start=1):
        inbox.add(journal_event(seq, event_type), **facts)
    clock = Clock()
    return (
        CoordinatorInboxService(subscriptions, inbox, commands=gateway, clock=clock, **kwargs),
        inbox,
        clock,
    )


async def subscribe(
    service: CoordinatorInboxService, **overrides: Any
) -> Any:  # pragma: no cover - helper
    request = CoordinatorSubscribeRequest.model_validate(
        {"target": "run", "target_id": str(RUN), "after_seq": 0, **overrides}
    )
    return await service.subscribe(request, COORDINATOR)


async def test_offline_coordinator_recovers_each_pending_notification_once_by_id():
    service, inbox, clock = service_with(
        ("workflow_run.start", {}),
        ("human_task.created", {"human_task_id": "t-1"}),
        ("attempt.completed", {"outcome": "failed"}),
        ("workflow_run.terminalize", {"terminal_outcome": "failed"}),
    )
    created = await subscribe(service, profile={"batch_window_seconds": 0})
    sid = created.subscription_id
    # Nobody polled while the events committed; the inbox is materialized on demand.
    page = await service.poll(sid, COORDINATOR)
    kinds = [item.kind for item in page.notifications]
    assert kinds == [
        NotificationKind.PROGRESS,
        NotificationKind.REVIEW_REQUIRED,
        NotificationKind.FAILED,
        NotificationKind.TERMINAL_RESULT,
    ]
    ids = [item.notification_id for item in page.notifications]
    assert len(set(ids)) == 4 and page.acked_inbox_seq == 0 and not page.has_more
    # Polling again before acknowledging returns the same ids (nothing new is invented).
    assert [
        item.notification_id for item in (await service.poll(sid, COORDINATOR)).notifications
    ] == ids
    # Acknowledge out of order by id: the cursor moves only over a contiguous prefix.
    outcome = await service.ack(sid, COORDINATOR, notification_ids=(ids[1], ids[0]))
    assert set(outcome.acknowledged) == {ids[0], ids[1]} and outcome.acked_inbox_seq == 2
    again = await service.ack(sid, COORDINATOR, notification_ids=(ids[0], uuid4()))
    assert again.acknowledged == () and again.already == (ids[0],) and len(again.unknown) == 1
    remaining = await service.poll(sid, COORDINATOR)
    assert [item.notification_id for item in remaining.notifications] == ids[2:]
    await service.ack(sid, COORDINATOR, through_inbox_seq=4)
    # A restarted coordinator (new service over the same durable store) gets nothing twice.
    restarted = CoordinatorInboxService(inbox.subscriptions, inbox, clock=clock)
    final = await restarted.poll(sid, COORDINATOR)
    assert final.notifications == () and final.acked_inbox_seq == 4
    with pytest.raises(CoordinatorRejected) as refused:
        await service.ack(sid, COORDINATOR, through_inbox_seq=99)
    assert refused.value.code == "invalid"
    # Another actor sees the same answer as for an absent inbox.
    with pytest.raises(CoordinatorRejected) as hidden:
        await service.poll(sid, OTHER)
    assert hidden.value.code == "not_found"


async def test_one_approval_yields_exactly_one_actionable_notification_with_deltas_off():
    noise = [("tool_call.completed", {}), ("session.turn_completed", {})] * 20
    service, _inbox, clock = service_with(
        ("workflow_run.start", {}),
        *noise,
        ("activation.phase_changed", {"phase": "awaiting_human"}),
        ("human_task.created", {"human_task_id": "t-1", "kind": "human_gate:review"}),
        ("workflow_run.set_wait", {}),
        *noise,
    )
    created = await subscribe(service)
    page = await service.poll(created.subscription_id, COORDINATOR)
    actionable = [item for item in page.notifications if item.actionable]
    assert len(actionable) == 1
    (review,) = actionable
    assert review.kind is NotificationKind.REVIEW_REQUIRED
    assert review.facts["human_task_id"] == "t-1"
    # No notification covers a delta: only progress batches and the review exist.
    for item in page.notifications:
        assert not any(name.startswith(("tool_call.", "session.turn")) for name in item.counts)
    # The trailing progress batch seals after its window; still one actionable notification.
    clock.now += timedelta(minutes=5)
    later = await service.poll(created.subscription_id, COORDINATOR)
    assert [item.actionable for item in later.notifications].count(True) == 1


def command_request(run_id: str, version: int = 3) -> MissionCommandRequest:
    return MissionCommandRequest.model_validate(
        {
            "request_id": str(uuid4()),
            "expected_version": version,
            "expected_generation": 1,
            "target": {"kind": "run", "id": run_id},
            "kind": "queue_instruction",
            "payload": {"content": {"text": "react"}},
            "reason": "coordinator reacting to a notification",
        }
    )


async def test_notification_command_loop_hits_the_recursion_bound():
    gateway = FakeGateway()
    service, inbox, clock = service_with(("human_task.created", {}), gateway=gateway)
    created = await subscribe(
        service, profile={"batch_window_seconds": 0, "max_recursion_depth": 2}
    )
    sid = created.subscription_id
    seq = 1
    admitted = 0
    refusal = None
    for _round in range(10):
        page = await service.poll(sid, COORDINATOR)
        if not page.notifications:
            break
        for item in page.notifications:
            request = command_request("run-key-1")
            try:
                await service.command_from_notification(
                    sid, item.notification_id, "run-key-1", request, COORDINATOR
                )
            except CoordinatorRejected as rejected:
                refusal = rejected.code
                await service.ack(sid, COORDINATOR, notification_ids=(item.notification_id,))
                continue
            admitted += 1
            await service.ack(sid, COORDINATOR, notification_ids=(item.notification_id,))
            # The command's effect: a significant event caused by the request id (FIXTURE
            # standing in for the reducer journal).
            seq += 1
            inbox.add(
                journal_event(seq, "human_task.created", causation_ref=str(request.request_id))
            )
            clock.now += timedelta(seconds=1)
    assert admitted == 2  # depths 1 and 2; depth 3 is refused
    assert refusal == "recursion_bound"
    depths = sorted(record.depth for record in inbox.causations.values())
    assert depths == [1, 2]
    assert len(gateway.calls) == 2


async def test_triggered_commands_are_rate_capped_per_run():
    gateway = FakeGateway()
    events = [("human_task.created", {}) for _ in range(6)]
    service, _inbox, _clock = service_with(*events, gateway=gateway)
    created = await subscribe(
        service,
        profile={"batch_window_seconds": 0, "rate_limit_per_run": 3, "rate_window_seconds": 600},
    )
    sid = created.subscription_id
    page = await service.poll(sid, COORDINATOR)
    codes = []
    for item in page.notifications:
        try:
            await service.command_from_notification(
                sid, item.notification_id, "run-key-1", command_request("run-key-1"), COORDINATOR
            )
            codes.append("admitted")
        except CoordinatorRejected as rejected:
            codes.append(rejected.code)
    # Three notifications sealed individually, the rest folded into one rate_limited summary
    # (not sealed yet); triggered commands are capped at three per run and window.
    assert [item.kind for item in page.notifications] == [NotificationKind.REVIEW_REQUIRED] * 3
    assert codes == ["admitted"] * 3
    with pytest.raises(CoordinatorRejected) as capped:
        await service.command_from_notification(
            sid,
            page.notifications[0].notification_id,
            "run-key-1",
            command_request("run-key-1"),
            COORDINATOR,
        )
    assert capped.value.code == "rate_limited"


async def test_prompts_go_through_the_admitted_mailbox_once_per_notification():
    gateway = FakeGateway(stale_once=True)
    service, inbox, _clock = service_with(
        ("human_task.created", {"human_task_id": "t-1"}),
        ("workflow_run.start", {}),
        ("workflow_run.terminalize", {"terminal_outcome": "completed"}),
        gateway=gateway,
    )
    created = await subscribe(
        service,
        profile={"batch_window_seconds": 0, "prompt_mode": "queue_instruction"},
        coordinator_run_ref="coordinator-run",
    )
    sid = created.subscription_id
    report = await service.dispatch_prompts(sid)
    assert len(report.admitted) == 2 and report.failed == []
    # One stale-version rejection was retried at the new frontier under the same request id.
    ids = [request.request_id for _run, request, _actor in gateway.calls]
    assert ids[0] == ids[1]
    for run_id, request, actor in gateway.calls:
        assert run_id == "coordinator-run"
        assert request.kind == "queue_instruction"
        assert request.payload.boundary == "next_turn"  # type: ignore[union-attr]
        assert actor.actor_id == "coordinator-1"
        assert "coordinator-notification:" in request.reason
    review = next(item for item in inbox.rows.values() if item.kind.value == "review_required")
    assert prompt_request_id(sid, review.notification_id) in gateway.admitted
    text = gateway.admitted[prompt_request_id(sid, review.notification_id)].payload.content.text  # type: ignore[union-attr]
    assert str(review.notification_id) in text and "t-1" in text
    # A second pass prompts nothing new: one prompt per notification.
    again = await service.dispatch_prompts(sid)
    assert again.admitted == [] and again.replayed == []
    assert len(gateway.admitted) == 2
    # Causation is recorded with depth 1 for both prompts.
    assert sorted(record.depth for record in inbox.causations.values()) == [1, 1]


async def test_prompt_rate_cap_unsupported_semantics_and_off_mode():
    gateway = FakeGateway()
    events = [("human_task.created", {}) for _ in range(4)]
    service, _inbox, _clock = service_with(*events, gateway=gateway)
    created = await subscribe(
        service,
        profile={
            "batch_window_seconds": 0,
            "prompt_mode": "add_context",
            "prompt_limit_per_window": 2,
        },
        coordinator_run_ref="coordinator-run",
    )
    report = await service.dispatch_prompts(created.subscription_id)
    assert len(report.admitted) == 2 and report.deferred == 1
    unsupported_gateway = FakeGateway(lane_profile="cursor_cloud")
    service2, _inbox2, _clock2 = service_with(
        ("human_task.created", {}),
        gateway=unsupported_gateway,
        mailbox_semantics=lambda lane, kind: "unsupported",
    )
    created2 = await subscribe(
        service2,
        profile={"batch_window_seconds": 0, "prompt_mode": "queue_instruction"},
        coordinator_run_ref="coordinator-run",
    )
    skipped = await service2.dispatch_prompts(created2.subscription_id)
    assert [reason for _id, reason in skipped.skipped] == ["unsupported_semantics"]
    assert unsupported_gateway.calls == []
    quiet_gateway = FakeGateway()
    service3, _inbox3, _clock3 = service_with(("human_task.created", {}), gateway=quiet_gateway)
    created3 = await subscribe(service3)
    assert (await service3.dispatch_prompts(created3.subscription_id)).admitted == []
    assert quiet_gateway.calls == []


async def test_subscribe_guards_permissions_and_egress():
    service, _inbox, _clock = service_with()
    with pytest.raises(CoordinatorRejected) as no_read:
        await service.subscribe(
            CoordinatorSubscribeRequest(target="run", target_id=RUN),
            ActorContext(actor_id="x", permissions=frozenset()),
        )
    assert no_read.value.code == "unauthorized"
    with pytest.raises(CoordinatorRejected) as no_control:
        await service.subscribe(
            CoordinatorSubscribeRequest(
                target="run",
                target_id=RUN,
                profile=CoordinatorProfile(prompt_mode="queue_instruction"),
                coordinator_run_ref="coordinator-run",
            ),
            OTHER,
        )
    assert no_control.value.code == "unauthorized"
    with pytest.raises(CoordinatorRejected) as absent:
        await service.subscribe(
            CoordinatorSubscribeRequest(target="run", target_id=uuid5(RUN, "absent")), COORDINATOR
        )
    assert absent.value.code == "not_found"
