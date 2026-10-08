"""FT-B4 on the common component: continuation transfers, sealed checkpoints and events.

Everything runs as the restricted runtime login under forced RLS: the
``continuation_transfer`` saga row (migration 0027, section B4), the sealed
``mc.continuation_checkpoint.v1`` in ``continuation_checkpoint`` with its
``checkpoint_validation`` verdict (0003), the continuation packet in ``context_selection``
and the ``session.*`` mission events written through the run-control reducer.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from mission_control.adapters.postgres.context.continuation_repository import (
    PostgresCheckpointRepository,
    PostgresContinuationRepository,
    PostgresRunIds,
)
from mission_control.adapters.postgres.context.selection_repository import (
    PostgresContextSelectionRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.application.context.continuation import (
    CheckpointReadService,
    ContinuationCommands,
    ContinuationRejected,
    ContinuationService,
    ContinuationTriggers,
    FrameHydrationConfirmation,
    FrameSessionLocator,
    HydrationReceipt,
    HydrationRequest,
    InMemoryMailbox,
    RunControlContinuationEvents,
    TransferStatus,
)
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.frames.writer import FrameWriter
from mission_control.domain.context.checkpoint import CheckpointIdentities
from mission_control.domain.context.packet import PacketScope
from mission_control.domain.policies.contracts import ActorContext
from mission_control.domain.policies.errors import IdempotencyConflict
from tests.fixtures.continuation import (
    FakeStaging,
    MemorySnapshots,
    NoArtifacts,
    StepClock,
    facts,
    seal_target,
    trigger,
)
from tests.fixtures.mission_control_common_db import CommonDatabase
from tests.fixtures.provider_frames import StepClock as FrameClock
from tests.fixtures.provider_frames import turn_observations
from tests.integration.postgres.frames_common import admit_unit_attempt
from tests.integration.postgres.runtime_common import common_db as common_db
from tests.integration.postgres.runtime_common import owner_rows
from tests.unit.run_control.test_run_control import ALL_PERMISSIONS, actor

pytestmark = pytest.mark.common_db


def continuation_actor() -> ActorContext:
    return actor().model_copy(
        update={
            "permissions": ALL_PERMISSIONS
            | {
                "workflow_run.record_continuation",
                "workflow_run.request_continuation",
                "workflow_run.read",
            }
        }
    )


class Hydrator:
    def __init__(self, target: str) -> None:
        self.target = target

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt:
        return HydrationReceipt(
            target_session_ref=self.target, restored=dict(request.snapshot.manifest)
        )


def _facts_for(db: CommonDatabase, run_key: str, unit_key: str, session: str) -> Any:
    parts = db.scope().split("/")
    return facts(
        scope=PacketScope(installation_id=parts[1], application_id=parts[2], tenant_id=parts[3]),
        identities=CheckpointIdentities(
            mission_id="mission-1",
            run_id=run_key,
            revision_id="rev-1",
            node_key="collect",
            activation_id="unit-collect-1",
            logical_execution_id=unit_key,
            source_agent_session_ref=session,
        ),
    )


@pytest.mark.asyncio
async def test_seal_and_transfer_persist_checkpoints_transfers_and_session_events(
    common_db: CommonDatabase,
) -> None:
    pool = await common_db.pool(max_size=4)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        scope = admitted.unit.request_scope
        session = f"thread:{admitted.unit.unit_key}"
        frames = PostgresFrameRepository(pool)
        handle = await frames.open_execution(admitted.start())
        await FrameWriter(frames, handle, clock=FrameClock()).write(turn_observations())

        staging = FakeStaging()
        transfers = PostgresContinuationRepository(pool)
        checkpoints = PostgresCheckpointRepository(pool)
        selections = PostgresContextSelectionRepository(pool)
        mailbox = InMemoryMailbox(pending={admitted.run_key: ["cmd-held-1"]})
        events = RunControlContinuationEvents(
            admitted.run_control, actor=continuation_actor(), clock=StepClock()
        )
        target_session = f"{session}~continuation~1"
        confirmation = FrameHydrationConfirmation(frames, PostgresRunIds(pool))
        service = ContinuationService(
            transfers=transfers,
            checkpoints=checkpoints,
            packets=ContextPackService(
                artifacts=NoArtifacts(), selections=selections, staging=staging
            ),
            packet_reader=selections,
            snapshots=MemorySnapshots(
                staging,
                {
                    session: {
                        "/inputs/sources/source_manifest.json": '{"records": 180}',
                        "/outputs/evidence_map.md": "# draft",
                    }
                },
            ),
            events=events,
            mailbox=mailbox,
            confirmation=confirmation,
            clock=StepClock(),
        )

        # The command path: request_continuation locates the live session from frames.
        commands = ContinuationCommands(
            ContinuationTriggers(transfers),
            FrameSessionLocator(frames, PostgresRunIds(pool)),
            request_scope=scope,
        )
        planned = await commands.plan(admitted.run_key, "cmd-continue-1", None)
        assert planned.lane_profile == "deep_agents"
        assert planned.source_session_ref == session
        assert planned.delivery == "turn_boundary_guaranteed"
        await commands.record(admitted.run_key, planned, "cmd-continue-1")
        await commands.record(admitted.run_key, planned, "cmd-continue-1")  # idempotent
        pending = await service.pending(scope, admitted.run_key)
        assert [item.transfer_id for item in pending] == [planned.transfer_id]

        sealed = await service.seal(
            planned.transfer_id,
            _facts_for(common_db, admitted.run_key, planned.logical_execution_id, session),
            seal_target(),
            request_scope=scope,
        )
        assert sealed.checkpoint is not None and sealed.checkpoint.valid
        assert sealed.transfer.status == TransferStatus.SEALED
        stored = await checkpoints.get(scope, admitted.run_key, sealed.checkpoint.checkpoint_id)
        assert stored is not None and stored.checkpoint == sealed.checkpoint
        assert await checkpoints.put(stored) == stored  # idempotent replay
        with pytest.raises(IdempotencyConflict):
            await checkpoints.put(
                stored.model_copy(
                    update={
                        "checkpoint": sealed.checkpoint.model_copy(
                            update={"checkpoint_digest": "sha256:" + "0" * 64}
                        )
                    }
                )
            )
        packet = await selections.get(sealed.packet.packet_id, request_scope=scope)
        assert packet is not None and packet.target.purpose.value == "continuation"

        # Hydration: session.transferred, release only after the target's session_init.
        outcome = await service.transfer(
            planned.transfer_id, Hydrator(target_session), request_scope=scope
        )
        assert outcome.transfer.status == TransferStatus.TRANSFERRED
        assert not outcome.transfer.released
        target_handle = await frames.open_execution(
            admitted.start(native_session_ref=target_session)
        )
        await FrameWriter(frames, target_handle, clock=FrameClock()).write(
            turn_observations("turn-2", tool_call="call-2")
        )
        released = await service.release_if_hydrated(planned.transfer_id, request_scope=scope)
        assert released.released
        assert mailbox.released[planned.transfer_id] == ("cmd-held-1",)

        rows = await owner_rows(
            common_db,
            """
            SELECT e.event_type, e.payload
            FROM mission_control.mission_event e
            JOIN mission_control.mission_run r
              ON r.installation_id = e.installation_id AND r.application_id = e.application_id
             AND r.tenant_id = e.tenant_id AND r.run_id = e.run_id
            WHERE r.run_key = $1 AND e.event_type LIKE 'session.%'
            ORDER BY e.seq
            """,
            admitted.run_key,
        )
        types = [row["event_type"] for row in rows]
        assert types == ["session.checkpoint_sealed", "session.transferred"]
        transferred = _payload(rows[-1]["payload"])
        assert transferred["source_session_ref"] == session
        assert transferred["target_session_ref"] == target_session
        assert transferred["checkpoint_digest"] == sealed.checkpoint.checkpoint_digest
        # A replayed event write records nothing new.
        await events.record(
            scope,
            admitted.run_key,
            service._action(released, "transferred", checkpoint=sealed.checkpoint),
        )
        again = await owner_rows(
            common_db,
            """
            SELECT count(*) AS n FROM mission_control.mission_event e
            JOIN mission_control.mission_run r
              ON r.installation_id = e.installation_id AND r.application_id = e.application_id
             AND r.tenant_id = e.tenant_id AND r.run_id = e.run_id
            WHERE r.run_key = $1 AND e.event_type LIKE 'session.%'
            """,
            admitted.run_key,
        )
        assert again[0]["n"] == 2

        validations = await owner_rows(
            common_db,
            """
            SELECT v.status, c.contract_version, c.manifest_digest
            FROM mission_control.checkpoint_validation v
            JOIN mission_control.continuation_checkpoint c
              ON c.installation_id = v.installation_id AND c.application_id = v.application_id
             AND c.tenant_id = v.tenant_id AND c.checkpoint_id = v.checkpoint_id
            WHERE c.contract_version = 'mc.continuation_checkpoint.v1'
            """,
        )
        assert [(row["status"], row["manifest_digest"]) for row in validations] == [
            ("valid", sealed.checkpoint.checkpoint_digest)
        ]

        # Reads (CLI `run checkpoint`, HTTP GET .../checkpoints) under the tenant scope.
        reads = CheckpointReadService(checkpoints, transfers, request_scope=scope)
        listed = await reads.list(admitted.run_key, actor=continuation_actor())
        assert [item.checkpoint_id for item in listed] == [sealed.checkpoint.checkpoint_id]
        assert listed[0].transfer_status == "transferred"
        other = CheckpointReadService(
            checkpoints, transfers, request_scope=common_db.scope("tenant-2")
        )
        await common_db.add_tenants(["tenant-2"])
        assert await other.list(admitted.run_key, actor=continuation_actor()) == ()
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_transfer_rows_are_compare_and_set(common_db: CommonDatabase) -> None:
    pool = await common_db.pool(max_size=2)
    try:
        admitted = await admit_unit_attempt(pool, common_db)
        scope = admitted.unit.request_scope
        transfers = PostgresContinuationRepository(pool)
        triggers = ContinuationTriggers(transfers, clock=StepClock())
        requested = await triggers.request(
            trigger(),
            request_scope=scope,
            run_key=admitted.run_key,
            activation_key=admitted.unit.unit_key,
            logical_execution_id=admitted.unit.unit_key,
            lane_profile="cursor_local",
            source_session_ref="agent-1",
        )
        assert requested.delivery == "wait_then_send"
        bumped = requested.model_copy(
            update={"status": TransferStatus.PARKED, "version": requested.version + 1}
        )
        await transfers.update(bumped, expected_version=requested.version)
        with pytest.raises(ContinuationRejected):
            await transfers.update(bumped, expected_version=requested.version)
        stored = await transfers.get(scope, requested.transfer_id)
        assert stored is not None and stored.status == TransferStatus.PARKED
        rows = await owner_rows(
            common_db,
            "SELECT status, version, delivery FROM mission_control.continuation_transfer",
        )
        assert [(row["status"], row["version"], row["delivery"]) for row in rows] == [
            ("parked", 2, "wait_then_send")
        ]
    finally:
        await pool.close()


def _payload(value: Any) -> dict[str, Any]:
    data = json.loads(value) if isinstance(value, str) else value
    return data.get("payload", data) if isinstance(data, dict) else {}
