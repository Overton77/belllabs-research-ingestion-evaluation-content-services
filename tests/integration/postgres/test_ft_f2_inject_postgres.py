"""FT-F2 interrupt_and_inject on a disposable PostgreSQL 17 (forced RLS, restricted login).

An `interrupt_and_inject` admitted through the mission service writes a mailbox entry of its
own kind. A running turn's lane boundary claims only that kind (a queued instruction waits
for the next turn); an interrupted turn whose uncertain effects did not settle releases the
entry and records `command.in_doubt` with the pending effect ids; the replacement turn
consumes it once and records the semantics, cancelled and replacement turn refs and settled
effect ids; a lane reporting `unsupported` expires the entry (`unsupported_by_lane`) and
rejects the Command with a typed Delivery Report.
"""

from __future__ import annotations

import json
from uuid import uuid4

import asyncpg
import pytest

from mission_control.adapters.postgres.run_control.mailbox import PostgresCommandMailbox
from mission_control.adapters.postgres.run_control.run_control_repository import (
    PostgresRunControlRepository,
)
from mission_control.application.execution.boundary_interventions import BoundaryInterventionService
from mission_control.application.execution.mailbox import MailboxDeliveryService
from mission_control.application.missions.service import MissionControlService
from mission_control.domain.policies.contracts import StartAction
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.integration.postgres.test_command_mailbox_postgres import _request, operator
from tests.unit.run_control.test_boundary_commands import TARGET
from tests.unit.run_control.test_run_control import command, request, service

pytestmark = pytest.mark.common_db

INJECT = ("interrupt_and_inject",)


async def test_interrupt_and_inject_park_replace_and_unsupported_on_postgres(
    common_db: CommonDatabase,  # noqa: F811
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        scope = common_db.scope()
        authority, _ = service(PostgresRunControlRepository(pool))  # type: ignore[arg-type]
        admission = await authority.admit(request(request_scope=scope, request_id=str(uuid4())))
        assert admission.run_id is not None
        run_id = admission.run_id
        await authority.execute(
            command(run_id, 1, "start", StartAction(execution_target=TARGET)).model_copy(
                update={"request_scope": scope}
            )
        )
        store = PostgresCommandMailbox(pool)
        mailbox = MailboxDeliveryService(store, authority)
        facade = MissionControlService(
            authority, BoundaryInterventionService(authority), request_scope=scope, mailbox=mailbox
        )
        version = (await authority.get_run(scope, run_id)).version
        await facade.command(
            run_id,
            _request(run_id, version, "queue_instruction", {"content": {"text": "later"}}),
            operator(),
        )
        injected = await facade.command(
            run_id,
            _request(run_id, version, "interrupt_and_inject", {"content": {"text": "redirect"}}),
            operator(),
        )
        assert injected.delivery is not None
        assert [item.state.value for item in injected.delivery.receipts] == ["accepted", "queued"]
        entries = {item.kind: item for item in await store.list_entries(scope, run_id)}
        assert entries["interrupt_and_inject"].content_inline == "redirect"

        # The running turn's lane boundary takes only the injected entry.
        taken = await mailbox.deliver(
            scope,
            run_id,
            delivery_key="inject-1",
            family="GoalDirected",
            node_key="goal/executor",
            iteration_start=False,
            lane_profile="deep_agents",
            kinds=INJECT,
            cancelled_turn_ref="turn:1",
        )
        assert [item.kind for item in taken] == ["interrupt_and_inject"]
        # Uncertain effects did not settle: the entry returns to queued, no replacement.
        await mailbox.inject_parked(
            scope,
            run_id,
            delivery_key="inject-1",
            lane_profile="deep_agents",
            cancelled_turn_ref="turn:1",
            pending_effect_ids=("tool-effect:push",),
        )
        entries = {item.kind: item for item in await store.list_entries(scope, run_id)}
        assert entries["interrupt_and_inject"].state == MailboxState.QUEUED
        assert entries["queue_instruction"].state == MailboxState.QUEUED

        # After reconciliation, the next interrupt replaces the turn and consumes once.
        await mailbox.deliver(
            scope,
            run_id,
            delivery_key="inject-2",
            family="GoalDirected",
            node_key="goal/executor",
            iteration_start=False,
            lane_profile="deep_agents",
            kinds=INJECT,
            cancelled_turn_ref="turn:2",
        )
        for _ in range(2):  # a restarted worker replays the replacement start
            await mailbox.inject_replaced(
                scope,
                run_id,
                delivery_key="inject-2",
                lane_profile="deep_agents",
                delivered_semantics="cancel_and_replace",
                cancelled_turn_ref="turn:2",
                replacement_turn_ref="turn:3",
                settled_effect_ids=("tool-effect:push",),
            )
        await mailbox.turn_settled(
            scope,
            run_id,
            delivery_key="inject-2",
            lane_profile="deep_agents",
            succeeded=True,
            turn_ref="turn:3",
        )
        status = await authority.get_boundary_command(
            scope,
            run_id,
            injected.delivery.command.idempotency_issuer,
            injected.delivery.command.command_id,
        )
        assert status is not None
        assert [item.state.value for item in status.receipts] == [
            "accepted",
            "queued",
            "delivered",
            "observed",
            "applied",
        ]
        observed = status.receipts[3].delivery_report
        assert observed is not None
        assert observed.delivered_semantics == "cancel_and_replace"
        assert observed.native_refs.replacement_turn_ref == "turn:3"
        assert observed.settled_effect_ids == ("tool-effect:push",)
        entries = {item.kind: item for item in await store.list_entries(scope, run_id)}
        assert entries["interrupt_and_inject"].state == MailboxState.CONSUMED
        assert entries["queue_instruction"].state == MailboxState.QUEUED

        # A lane reporting `unsupported` expires the entry and rejects the Command.
        version = (await authority.get_run(scope, run_id)).version
        refused = await facade.command(
            run_id,
            _request(run_id, version, "interrupt_and_inject", {"content": {"text": "stop"}}),
            operator(),
        )
        assert refused.delivery is not None
        (pending,) = [
            item
            for item in await store.list_entries(scope, run_id)
            if item.command_id == refused.delivery.command.command_id
        ]
        await mailbox.inject_unsupported(scope, pending, lane_profile="cursor_cloud")
        expired = await store.expire(
            scope,
            run_id,
            entry_id=pending.entry_id,
            reason="unsupported_by_lane",
            now=pending.accepted_at,
        )
        assert expired is not None
        assert (expired.state, expired.expired_reason) == (
            MailboxState.EXPIRED,
            "unsupported_by_lane",
        )
        rejected = await authority.get_boundary_command(
            scope,
            run_id,
            refused.delivery.command.idempotency_issuer,
            refused.delivery.command.command_id,
        )
        assert rejected is not None
        assert [item.state.value for item in rejected.receipts] == [
            "accepted",
            "queued",
            "rejected",
        ]
        assert rejected.receipts[-1].rejection_reason == "not_applicable"

        owner = await asyncpg.connect(common_db.owner_dsn)
        try:
            in_doubt = await owner.fetch(
                "SELECT payload FROM mission_control.mission_event "
                "WHERE event_type = 'command.in_doubt' ORDER BY seq"
            )
            lifecycles = {
                row["command_id"]: row["lifecycle"]
                for row in await owner.fetch(
                    "SELECT command_id::text, lifecycle FROM mission_control.command "
                    "WHERE command_kind = 'interrupt_and_inject'"
                )
            }
        finally:
            await owner.close()
        (parked,) = in_doubt
        payload = parked["payload"]
        envelope = json.loads(payload) if isinstance(payload, str) else payload
        report = envelope["payload"]["delivery_report"]
        assert report["pending_effect_ids"] == ["tool-effect:push"]
        assert report["native_refs"]["cancelled_turn_ref"] == "turn:1"
        assert sorted(lifecycles.values()) == ["applied", "rejected"]
        # Another tenant reads nothing.
        assert await store.list_entries(common_db.scope("tenant-2"), run_id) == ()
    finally:
        await pool.close()
