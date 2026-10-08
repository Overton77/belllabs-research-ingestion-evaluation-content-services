"""FT-F1 command mailbox on a disposable PostgreSQL 17 (forced RLS, restricted runtime login).

A `queue_instruction` admitted through the mission service writes its mailbox entry, its
`accepted`/`queued` receipts and the `command.queued` mission event in the admitting
transaction; the boundary claim is idempotent per delivery key (even when empty); the turn
consumes once and settles `applied`; a cancel supersedes what is pending; rows are never
deleted and another tenant reads nothing.
"""

from __future__ import annotations

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
from mission_control.contracts.contracts import MissionCommandRequest
from mission_control.domain.policies.contracts import ActorContext, StartAction
from mission_control.domain.policies.mailbox import MailboxState
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.integration.postgres.runtime_common import common_db  # noqa: F401
from tests.unit.run_control.test_boundary_commands import TARGET
from tests.unit.run_control.test_run_control import actor, command, request, service

pytestmark = pytest.mark.common_db


def operator() -> ActorContext:
    source = actor()
    return source.model_copy(
        update={"permissions": source.permissions | {"workflow_run.read", "workflow_run.control"}}
    )


def _request(
    run_id: str, version: int, kind: str, payload: dict[str, object]
) -> MissionCommandRequest:
    return MissionCommandRequest.model_validate(
        {
            "request_id": str(uuid4()),
            "expected_version": version,
            "expected_generation": 1,
            "target": {"kind": "run", "id": run_id},
            "kind": kind,
            "payload": payload,
            "reason": "operator steering",
        }
    )


async def test_mailbox_admission_delivery_consumption_and_supersession_on_postgres(
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
        queued = _request(
            run_id, version, "queue_instruction", {"content": {"text": "cite the trial"}}
        )
        first = await facade.command(run_id, queued, operator())
        replay = await facade.command(run_id, queued, operator())
        assert replay.replay and replay.delivery == first.delivery
        assert first.delivery is not None
        assert [item.state.value for item in first.delivery.receipts] == ["accepted", "queued"]
        assert (await authority.get_run(scope, run_id)).version == version
        (entry,) = await store.list_entries(scope, run_id)
        assert (entry.state, entry.admission_sequence, entry.content_inline) == (
            MailboxState.QUEUED,
            1,
            "cite the trial",
        )
        # Claim idempotency per delivery key, even for an empty claim.
        empty = await mailbox.deliver(
            scope,
            run_id,
            delivery_key="verifier-key",
            family="GoalDirected",
            node_key="goal/verifier",
            iteration_start=True,
            lane_profile="deep_agents",
        )
        assert empty == ()
        delivered = await mailbox.deliver(
            scope,
            run_id,
            delivery_key="executor-key",
            family="GoalDirected",
            node_key="goal/executor",
            iteration_start=True,
            lane_profile="deep_agents",
        )
        assert [item.entry_id for item in delivered] == [entry.entry_id]
        await facade.command(
            run_id,
            _request(
                run_id,
                version,
                "add_context",
                {"content": {"text": "late note"}, "expand": "inline"},
            ),
            operator(),
        )
        again = await mailbox.deliver(
            scope,
            run_id,
            delivery_key="executor-key",
            family="GoalDirected",
            node_key="goal/executor",
            iteration_start=True,
            lane_profile="deep_agents",
        )
        assert [item.entry_id for item in again] == [entry.entry_id]
        assert (
            await mailbox.deliver(
                scope,
                run_id,
                delivery_key="verifier-key",
                family="GoalDirected",
                node_key="goal/executor",
                iteration_start=True,
                lane_profile="deep_agents",
            )
            == ()
        )
        for _ in range(2):  # a restarted worker starts the same turn again
            await mailbox.turn_started(
                scope, run_id, delivery_key="executor-key", lane_profile="deep_agents"
            )
        await mailbox.turn_settled(
            scope, run_id, delivery_key="executor-key", lane_profile="deep_agents", succeeded=True
        )
        status = await authority.get_boundary_command(
            scope, run_id, entry.command_issuer, entry.command_id
        )
        assert status is not None
        assert [item.state.value for item in status.receipts] == [
            "accepted",
            "queued",
            "delivered",
            "observed",
            "applied",
        ]
        # A cancel supersedes what is still pending (the late note); consumed stays consumed.
        version = (await authority.get_run(scope, run_id)).version
        cancel = await facade.command(run_id, _request(run_id, version, "cancel", {}), operator())
        entries = {item.entry_id: item for item in await store.list_entries(scope, run_id)}
        assert entries[entry.entry_id].state == MailboxState.CONSUMED
        (late,) = [item for item in entries.values() if item.entry_id != entry.entry_id]
        assert (late.state, late.superseded_by, late.expired_reason) == (
            MailboxState.SUPERSEDED,
            str(cancel.request_id),
            "superseded",
        )

        async with pool.acquire() as connection:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                async with connection.transaction():
                    await connection.execute("DELETE FROM mission_control.command_mailbox")
        owner = await asyncpg.connect(common_db.owner_dsn)
        try:
            rows = await owner.fetch(
                "SELECT c.lifecycle, d.delivery_semantics, d.requested_semantics, "
                "d.observed_outcome FROM mission_control.delivery_report d "
                "JOIN mission_control.command c ON c.command_id = d.command_id "
                "WHERE c.command_kind = 'queue_instruction' ORDER BY d.reported_at"
            )
            event_types = [
                row["event_type"]
                for row in await owner.fetch(
                    "SELECT event_type FROM mission_control.mission_event "
                    "WHERE event_type LIKE 'command.%' ORDER BY seq"
                )
            ]
            claims = await owner.fetchval(
                "SELECT count(*) FROM mission_control.command_mailbox_claim"
            )
        finally:
            await owner.close()
        assert {row["lifecycle"] for row in rows} == {"applied"}
        assert [row["observed_outcome"] for row in rows] == [
            "unknown",
            "delivered",
            "delivered",
            "applied",
        ]
        assert (
            rows[1]["delivery_semantics"]
            == rows[1]["requested_semantics"]
            == ("turn_boundary_guaranteed")
        )
        assert event_types == [
            "command.queued",  # the instruction
            "command.delivered",
            "command.queued",  # the late note
            "command.completed",  # the instruction, applied
            "command.completed",  # the late note, superseded by the cancel
        ]
        assert claims == 2
        # Another tenant reads nothing.
        assert await store.list_entries(common_db.scope("tenant-2"), run_id) == ()
    finally:
        await pool.close()
