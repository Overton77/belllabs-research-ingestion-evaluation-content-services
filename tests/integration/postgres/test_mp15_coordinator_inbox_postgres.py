"""MP-15 coordinator inbox on a disposable PostgreSQL 17 (restricted runtime login, forced RLS).

The coordinator tables are a PROPOSED migration (0033, not allocated): each test applies
`PROPOSED_0033_DDL` to its own scratch database (created and dropped by `common_db`) on top
of the real 0001..0031 component, so the adapter is proven against the proposed DDL, never
against a shipped migration. Events come from real admissions, real MP-10 Human Gate tasks
(`human_task.created`) and real `queue_instruction` commands through `MissionControlService`
and the command mailbox. Token/tool delta events are appended through the canonical ledger
writer as a FIXTURE standing in for frame-derived facts (no provider runs here).

Proves:
- an offline coordinator recovers every pending notification once by id (two concurrent
  materializers, a restart, idempotent acknowledgement);
- one run requiring approval produces exactly one actionable notification while token and
  tool deltas stay off by default;
- notification-command feedback loops stop at the recursion bound (causation carried) and,
  where causation is lost, at the per-run rate cap;
- a coordinator prompt is admitted only through the coordinator's own mailbox, once per
  notification.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest
import pytest_asyncio

from mission_control.adapters.postgres.human_tasks.repository import PostgresHumanTaskRepository
from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.mailbox import PostgresCommandMailbox
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.adapters.postgres.subscriptions.coordinator_inbox import (
    PROPOSED_0033_DDL,
    PostgresCoordinatorInboxStore,
)
from mission_control.adapters.postgres.subscriptions.store import PostgresSubscriptionStore
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.missions.service import MissionControlService
from mission_control.application.subscriptions.aliases import derived_event_id
from mission_control.application.subscriptions.coordinator import (
    NotificationKind,
    prompt_request_id,
)
from mission_control.application.subscriptions.coordinator_service import (
    CoordinatorInboxService,
    CoordinatorRejected,
    CoordinatorSubscribeRequest,
)
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.policies.contracts import (
    ActorContext,
    DomainEventEnvelope,
    StartAction,
)
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.unit.human_tasks.fixtures import activation
from tests.unit.run_control.test_boundary_commands import TARGET
from tests.unit.run_control.test_run_control import actor, command, request, service

pytestmark = pytest.mark.common_db


def coordinator_actor() -> ActorContext:
    """The coordinator's own principal (the run-control unit actor, renamed)."""

    source = actor()
    return source.model_copy(
        update={
            "actor_id": "coordinator-1",
            "permissions": source.permissions | {"workflow_run.read", "workflow_run.control"},
        }
    )


COORDINATOR = coordinator_actor()


@pytest_asyncio.fixture
async def inbox_db(common_db: CommonDatabase) -> AsyncIterator[CommonDatabase]:  # noqa: F811
    """The scratch database plus the PROPOSED 0033 coordinator tables (test-only DDL)."""

    owner = await asyncpg.connect(common_db.owner_dsn)
    try:
        await owner.execute(PROPOSED_0033_DDL)
    finally:
        await owner.close()
    yield common_db


async def started_run(pool: asyncpg.Pool, db: CommonDatabase) -> tuple[str, UUID, UUID]:
    """A real admitted and started run: (run key, mission uuid, run uuid)."""

    scope = db.scope()
    authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
    assert admission.run_id is not None
    await authority.execute(
        command(admission.run_id, 1, "start", StartAction(execution_target=TARGET)).model_copy(
            update={"request_scope": scope}
        )
    )
    rows = await owner_fetch(
        db,
        "SELECT run_id, mission_id FROM mission_control.mission_run WHERE run_key = $1",
        admission.run_id,
    )
    return admission.run_id, rows[0]["mission_id"], rows[0]["run_id"]


async def owner_fetch(db: CommonDatabase, query: str, *args: Any) -> list[asyncpg.Record]:
    owner = await asyncpg.connect(db.owner_dsn)
    try:
        return list(await owner.fetch(query, *args))
    finally:
        await owner.close()


async def append_fixture_events(
    pool: asyncpg.Pool,
    scope: str,
    run_key: str,
    events: Sequence[tuple[str, dict[str, Any]]],
) -> None:
    """FIXTURE: journal events through the canonical ledger writer (frame-derived stand-ins)."""

    now = datetime.now(UTC)
    batch = uuid4()
    envelopes = tuple(
        DomainEventEnvelope(
            event_id=f"fixture:{batch}:{index}",
            event_type=event_type,
            aggregate_id=f"fixture:{batch}",
            aggregate_version=1,
            sequence=index + 1,
            occurred_at=now,
            recorded_at=now,
            actor=ActorContext(actor_id="fixture:frames"),
            correlation_id=str(batch),
            payload=payload,
        )
        for index, (event_type, payload) in enumerate(events)
    )
    async with pool.acquire() as connection, connection.transaction():
        args = await mc.begin(connection, scope)
        await mc.append_events(
            connection,
            args,
            run_key=run_key,
            commit_key=f"fixture:{batch}",
            expected_versions={},
            events=envelopes,
            actor_ref="fixture:frames",
        )


def inbox_service(pool: asyncpg.Pool, scope: str, **kwargs: Any) -> CoordinatorInboxService:
    return CoordinatorInboxService(
        PostgresSubscriptionStore(pool, scope), PostgresCoordinatorInboxStore(pool, scope), **kwargs
    )


async def subscribe(inboxes: CoordinatorInboxService, run_uuid: UUID, **overrides: Any) -> UUID:
    created = await inboxes.subscribe(
        CoordinatorSubscribeRequest.model_validate(
            {"target": "run", "target_id": str(run_uuid), "after_seq": 0, **overrides}
        ),
        COORDINATOR,
    )
    return created.subscription_id


async def test_offline_coordinator_recovers_pending_notifications_once_by_id(
    inbox_db: CommonDatabase,
) -> None:
    pool = await inbox_db.pool(max_size=6)
    try:
        scope = inbox_db.scope()
        run_key, _mission, run_uuid = await started_run(pool, inbox_db)
        sid = await subscribe(
            inbox_service(pool, scope), run_uuid, profile={"batch_window_seconds": 0}
        )
        # The coordinator is offline while work commits.
        task = await PostgresHumanTaskRepository(pool).open(
            activation(scope=scope, run_id=run_key), actor_ref="runtime"
        )
        await append_fixture_events(
            pool,
            scope,
            run_key,
            [
                ("tool_call.completed", {"status": "succeeded"}),
                ("attempt.completed", {"outcome": "failed", "failure_class": "provider"}),
            ],
        )
        # Two coordinator processes come back at once: the inbox row lock serializes them.
        first, second = inbox_service(pool, scope), inbox_service(pool, scope)
        await asyncio.gather(first.materialize(sid), second.materialize(sid))
        page = await first.poll(sid, COORDINATOR)
        kinds = [item.kind for item in page.notifications]
        assert kinds == [
            NotificationKind.PROGRESS,
            NotificationKind.REVIEW_REQUIRED,
            NotificationKind.FAILED,
        ]
        ids = [item.notification_id for item in page.notifications]
        assert [item.inbox_seq for item in page.notifications] == [1, 2, 3]
        assert page.notifications[1].facts["human_task_id"] == task.human_task_id
        # Every canonical event is covered at most once across delivered notifications.
        covered = [ref.event_id for item in page.notifications for ref in item.events]
        assert len(covered) == len(set(covered))
        rows = await owner_fetch(
            inbox_db,
            "SELECT count(*) AS n FROM mission_control.coordinator_notification "
            "WHERE subscription_id = $1",
            sid,
        )
        assert rows[0]["n"] == 3
        # Acknowledge out of order by id, twice: idempotent, forward only.
        outcome = await second.ack(sid, COORDINATOR, notification_ids=(ids[1],))
        assert outcome.acknowledged == (ids[1],) and outcome.acked_inbox_seq == 0
        again = await first.ack(sid, COORDINATOR, notification_ids=(ids[1], ids[0]))
        assert again.acknowledged == (ids[0],) and again.already == (ids[1],)
        assert again.acked_inbox_seq == 2
        assert [n.notification_id for n in (await second.poll(sid, COORDINATOR)).notifications] == [
            ids[2]
        ]
        await first.ack(sid, COORDINATOR, notification_ids=(ids[2],))
        # Restart: a new service over a new pool receives nothing a second time.
        restarted_pool = await inbox_db.pool(max_size=2)
        try:
            restarted = inbox_service(restarted_pool, scope)
            final = await restarted.poll(sid, COORDINATOR)
            assert final.notifications == () and final.acked_inbox_seq == 3
        finally:
            await restarted_pool.close()
        states = await owner_fetch(
            inbox_db,
            "SELECT state, acknowledged_by_actor_ref FROM mission_control.coordinator_notification "
            "WHERE subscription_id = $1 ORDER BY inbox_seq",
            sid,
        )
        assert [(row["state"], row["acknowledged_by_actor_ref"]) for row in states] == [
            ("acknowledged", "coordinator-1")
        ] * 3
        # Another tenant sees neither the inbox nor its notifications.
        other = inbox_service(pool, inbox_db.scope("tenant-2"))
        with pytest.raises(CoordinatorRejected) as hidden:
            await other.poll(sid, COORDINATOR)
        assert hidden.value.code == "not_found"
    finally:
        await pool.close()


async def test_one_run_requiring_approval_yields_one_actionable_notification_deltas_off(
    inbox_db: CommonDatabase,
) -> None:
    pool = await inbox_db.pool(max_size=4)
    try:
        scope = inbox_db.scope()
        run_key, _mission, run_uuid = await started_run(pool, inbox_db)
        inboxes = inbox_service(pool, scope)
        sid = await subscribe(inboxes, run_uuid)  # the default coordinator profile
        deltas = [
            ("tool_call.completed", {"status": "succeeded"}),
            ("session.turn_started", {"turn_ordinal": 1}),
            ("session.turn_completed", {"turn_ordinal": 1}),
            ("session.usage_settled", {"turn_ordinal": 1}),
        ] * 6
        await append_fixture_events(pool, scope, run_key, deltas)
        task = await PostgresHumanTaskRepository(pool).open(
            activation(scope=scope, run_id=run_key), actor_ref="runtime"
        )
        await append_fixture_events(pool, scope, run_key, deltas)
        page = await inboxes.poll(sid, COORDINATOR)
        actionable = [item for item in page.notifications if item.actionable]
        assert len(actionable) == 1
        (review,) = actionable
        assert review.kind is NotificationKind.REVIEW_REQUIRED
        canonical = await owner_fetch(
            inbox_db,
            "SELECT event_id FROM mission_control.mission_event WHERE event_type = $1",
            "human_task.created",
        )
        (ref,) = review.events
        assert ref.event_id == canonical[0]["event_id"]
        assert ref.event_type == "human_task.opened"
        assert ref.public_event_id == derived_event_id("human_task.opened", ref.event_id)
        assert review.facts["human_task_id"] == task.human_task_id
        for item in page.notifications:
            assert not any(
                name.startswith(("tool_call.", "session.turn", "session.usage"))
                for name in item.counts
            ), item.counts
        stored = await owner_fetch(
            inbox_db,
            "SELECT body::text AS body FROM mission_control.coordinator_notification "
            "WHERE subscription_id = $1",
            sid,
        )
        assert all("tool_call." not in row["body"] for row in stored)
        # Sealing the trailing progress batch later does not add a second actionable one.
        later = inbox_service(pool, scope, clock=lambda: datetime.now(UTC) + timedelta(hours=1))
        final = await later.poll(sid, COORDINATOR)
        assert [item.actionable for item in final.notifications].count(True) == 1
    finally:
        await pool.close()


def mission_service(pool: asyncpg.Pool, scope: str) -> tuple[MissionControlService, Any]:
    authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
    mailbox = MailboxDeliveryService(PostgresCommandMailbox(pool), authority)
    facade = MissionControlService(
        authority, BoundaryInterventionService(authority), request_scope=scope, mailbox=mailbox
    )
    return facade, mailbox


async def queue(facade: MissionControlService, run_key: str, text: str) -> MissionCommandRequest:
    inspection = await facade.inspect(run_key, COORDINATOR)
    return MissionCommandRequest.model_validate(
        {
            "request_id": str(uuid4()),
            "expected_version": inspection.version,
            "expected_generation": inspection.execution_generation,
            "target": {"kind": "run", "id": run_key},
            "kind": "queue_instruction",
            "payload": {"content": {"text": text}},
            "reason": "coordinator reacting to a notification",
        }
    )


async def test_notification_command_loop_stops_at_the_recursion_bound(
    inbox_db: CommonDatabase,
) -> None:
    pool = await inbox_db.pool(max_size=6)
    try:
        scope = inbox_db.scope()
        run_key, _mission, run_uuid = await started_run(pool, inbox_db)
        facade, mailbox = mission_service(pool, scope)
        inboxes = inbox_service(pool, scope, commands=facade)
        sid = await subscribe(
            inboxes,
            run_uuid,
            profile={"batch_window_seconds": 0, "max_recursion_depth": 2, "rate_limit_per_run": 50},
        )
        # A naive coordinator: every notification makes it queue another instruction.
        admitted: list[UUID] = []
        refusals: list[str] = []
        for _round in range(12):
            page = await inboxes.poll(sid, COORDINATOR)
            if not page.notifications:
                break
            for item in page.notifications:
                request_ = await queue(facade, run_key, f"react to {item.notification_id}")
                try:
                    await inboxes.command_from_notification(
                        sid, item.notification_id, run_key, request_, COORDINATOR
                    )
                    admitted.append(request_.request_id)
                except CoordinatorRejected as rejected:
                    refusals.append(rejected.code)
                await inboxes.ack(sid, COORDINATOR, notification_ids=(item.notification_id,))
        assert len(admitted) == 2 and refusals == ["recursion_bound"]
        causes = await owner_fetch(
            inbox_db,
            "SELECT command_request_id, depth, origin FROM mission_control.coordinator_causation "
            "ORDER BY depth",
        )
        assert [(row["depth"], row["origin"]) for row in causes] == [
            (1, "triggered"),
            (2, "triggered"),
        ]
        # Each admitted command's events carry its request id as causation.
        caused = await owner_fetch(
            inbox_db,
            "SELECT DISTINCT causation_ref FROM mission_control.mission_event "
            "WHERE event_type = 'command.queued'",
        )
        assert {row["causation_ref"] for row in caused} == {str(item) for item in admitted}
        entries = await mailbox.list_entries(scope, run_key)
        assert len(entries) == 2 and {entry.state for entry in entries} == {MailboxState.QUEUED}
        depths = await owner_fetch(
            inbox_db,
            "SELECT depth, state FROM mission_control.coordinator_notification "
            "WHERE subscription_id = $1 ORDER BY seq_from",
            sid,
        )
        assert [row["depth"] for row in depths] == [0, 1, 2]
    finally:
        await pool.close()


async def test_loop_without_causation_stops_at_the_per_run_rate_cap(
    inbox_db: CommonDatabase,
) -> None:
    pool = await inbox_db.pool(max_size=6)
    try:
        scope = inbox_db.scope()
        run_key, _mission, run_uuid = await started_run(pool, inbox_db)
        facade, mailbox = mission_service(pool, scope)
        inboxes = inbox_service(pool, scope, commands=facade)
        sid = await subscribe(
            inboxes,
            run_uuid,
            profile={
                "batch_window_seconds": 0,
                "rate_limit_per_run": 3,
                "rate_window_seconds": 3600,
            },
        )
        # The coordinator acts through the plain command path (causation lost downstream).
        commands = 0
        for _round in range(12):
            page = await inboxes.poll(sid, COORDINATOR)
            if not page.notifications:
                break
            for item in page.notifications:
                await facade.command(
                    run_key, await queue(facade, run_key, "react again"), COORDINATOR
                )
                commands += 1
                await inboxes.ack(sid, COORDINATOR, notification_ids=(item.notification_id,))
        assert commands == 3  # three sealed notifications per run and window, then silence
        rows = await owner_fetch(
            inbox_db,
            "SELECT kind, state FROM mission_control.coordinator_notification "
            "WHERE subscription_id = $1 ORDER BY opened_at, seq_from",
            sid,
        )
        delivered = [row for row in rows if row["state"] in {"pending", "acknowledged"}]
        assert len(delivered) == 3
        held = [row for row in rows if row["state"] == "open"]
        assert [row["kind"] for row in held] == ["rate_limited"]  # folded, not lost
        assert len(await mailbox.list_entries(scope, run_key)) == 3
        # When the window rolls the folded summary seals once (inspect on demand).
        later = inbox_service(pool, scope, clock=lambda: datetime.now(UTC) + timedelta(hours=2))
        rolled = await later.poll(sid, COORDINATOR)
        assert [item.kind for item in rolled.notifications] == [NotificationKind.RATE_LIMITED]
    finally:
        await pool.close()


async def test_coordinator_prompt_is_admitted_through_its_own_mailbox_once(
    inbox_db: CommonDatabase,
) -> None:
    pool = await inbox_db.pool(max_size=6)
    try:
        scope = inbox_db.scope()
        child_key, _child_mission, child_uuid = await started_run(pool, inbox_db)
        coordinator_key, _coordinator_mission, _coordinator_uuid = await started_run(pool, inbox_db)
        facade, mailbox = mission_service(pool, scope)
        inboxes = inbox_service(pool, scope, commands=facade)
        sid = await subscribe(
            inboxes,
            child_uuid,
            profile={"batch_window_seconds": 0, "prompt_mode": "queue_instruction"},
            coordinator_run_ref=coordinator_key,
        )
        task = await PostgresHumanTaskRepository(pool).open(
            activation(scope=scope, run_id=child_key), actor_ref="runtime"
        )
        report = await inboxes.dispatch_prompts(sid)
        assert len(report.admitted) == 1 and report.failed == []
        (notification_id,) = report.admitted
        (entry,) = await mailbox.list_entries(scope, coordinator_key)
        assert entry.state is MailboxState.QUEUED and entry.kind == "queue_instruction"
        assert entry.command_id == str(prompt_request_id(sid, notification_id))
        assert str(notification_id) in (entry.content_inline or "")
        assert task.human_task_id in (entry.content_inline or "")
        # The child run's mailbox is untouched: the prompt went to the coordinator's run.
        assert await mailbox.list_entries(scope, child_key) == ()
        # Re-dispatching (or a second process) never prompts the same notification twice.
        again = await inbox_service(pool, scope, commands=facade).dispatch_prompts(sid)
        assert again.admitted == [] and again.replayed == []
        assert len(await mailbox.list_entries(scope, coordinator_key)) == 1
        causes = await owner_fetch(
            inbox_db,
            "SELECT origin, depth, target_run_ref FROM mission_control.coordinator_causation",
        )
        assert [(row["origin"], row["depth"], row["target_run_ref"]) for row in causes] == [
            ("prompt", 1, coordinator_key)
        ]
        prompt = await owner_fetch(
            inbox_db,
            "SELECT prompt_state FROM mission_control.coordinator_notification "
            "WHERE notification_id = $1",
            notification_id,
        )
        assert prompt[0]["prompt_state"] == "admitted"
        # The coordinator reads it at its own next boundary (admitted mailbox semantics).
        delivered = await mailbox.deliver(
            scope,
            coordinator_key,
            delivery_key="coordinator-turn-1",
            family="GoalDirected",
            node_key="goal/executor",
            iteration_start=True,
            lane_profile="deep_agents",
        )
        assert [item.entry_id for item in delivered] == [entry.entry_id]
    finally:
        await pool.close()
