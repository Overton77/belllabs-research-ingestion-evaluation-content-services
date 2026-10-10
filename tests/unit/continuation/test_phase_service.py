"""MP-12: the persisted phase machine in memory: crash at every phase, one activation,
matching digests, holds that survive a failed target, and the mailbox hold guard."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from mission_control.application.context.continuation import (
    ContinuationRejected,
    ContinuationTransfer,
    TransferStatus,
)
from mission_control.application.context.facts import FactsContext, OperationFactsCapture
from mission_control.application.context.hydrators import (
    LaneContinuationRegistration,
    LaneHydratorRegistry,
)
from mission_control.application.context.lane_continuation import (
    ContinuationHoldOracle,
    LaneContinuationCoordinator,
    LaneStateActivation,
)
from mission_control.application.context.phases import (
    ContinuationPhaseService,
    materialization_digest,
)
from mission_control.application.execution.harness.lane_turns import LaneExecutionIdentity
from mission_control.application.execution.harness.state import (
    InMemoryLaneExecutionStateStore,
    LaneExecutionUpdate,
)
from mission_control.application.execution.mailbox import (
    ContinuationMailboxHolds,
    InMemoryCommandMailbox,
    MailboxDeliveryService,
    MailboxHeld,
)
from mission_control.contracts.canonical import canonical_digest
from mission_control.domain.context.checkpoint import CHECKPOINT_INVALID
from mission_control.domain.context.phases import PHASE_ORDER, ContinuationPhase
from mission_control.domain.policies.mailbox import (
    MailboxEntry,
    content_digest,
    inline_content_ref,
)
from tests.fixtures.continuation import CONT_SCOPE, build_service, trigger
from tests.fixtures.lane_turns import cursor_operation
from tests.fixtures.mp12_lanes import Crashes, FakeHydrator

SOURCE = "agent-fake-1"
FILES = {
    SOURCE: {
        "/inputs/sources/source_manifest.json": '{"records": 180}',
        "/outputs/evidence_map.md": "# draft",
    }
}
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


@dataclass
class Wired:
    wired: dict[str, Any]
    phases: ContinuationPhaseService
    hydrator: FakeHydrator
    states: InMemoryLaneExecutionStateStore
    context: FactsContext
    run_key: str
    identity: LaneExecutionIdentity
    coordinator: LaneContinuationCoordinator

    @property
    def service(self) -> Any:
        return self.wired["service"]

    async def request(self, ref: str = "command://cmd-continue-1") -> ContinuationTransfer:
        return await self.service.request(
            trigger(ref=ref),
            request_scope=CONT_SCOPE,
            run_key=self.run_key,
            activation_key=self.identity.activation_key,
            logical_execution_id=self.identity.activation_key,
            lane_profile="cursor_local",
            source_session_ref=SOURCE,
        )

    async def drive(self, transfer_id: str) -> ContinuationTransfer:
        return await self.phases.drive(transfer_id, self.context, request_scope=CONT_SCOPE)

    async def advance(self, transfer_id: str) -> ContinuationTransfer:
        return await self.phases.advance(transfer_id, self.context, request_scope=CONT_SCOPE)


async def wire(
    *,
    crashes: Crashes | None = None,
    hydrator: FakeHydrator | None = None,
    pending: tuple[str, ...] = ("cmd-held-1", "cmd-held-2"),
) -> Wired:
    operation = cursor_operation()
    identity = LaneExecutionIdentity.of(operation, "cursor_local", 1)
    run_key = identity.run_key
    wired = build_service(session_files=FILES)
    wired["mailbox"].pending[run_key] = list(pending)
    hydrator = hydrator or FakeHydrator()
    states = InMemoryLaneExecutionStateStore()
    await states.record(
        CONT_SCOPE,
        identity.harness_execution_id,
        LaneExecutionUpdate(native_session_ref=SOURCE, native_turn_ref="run-fake-1"),
    )
    registry = LaneHydratorRegistry(
        {
            "cursor_local": LaneContinuationRegistration(
                "cursor_local", lambda scope: hydrator, snapshots=lambda scope: wired["snapshots"]
            )
        }
    )
    phases = ContinuationPhaseService(
        wired["service"],
        lanes=registry,
        facts=OperationFactsCapture(),
        activation=LaneStateActivation(states),
        after_persist=crashes,
    )
    context = FactsContext(
        operation=operation,
        harness_execution_id=str(identity.harness_execution_id),
        generation=1,
        turn_no=1,
        lane_state=await states.load(CONT_SCOPE, identity.harness_execution_id),
    )
    coordinator = LaneContinuationCoordinator(wired["store"])
    return Wired(wired, phases, hydrator, states, context, run_key, identity, coordinator)


def _digests(transfer: ContinuationTransfer) -> tuple[str | None, ...]:
    return (
        transfer.packet_digest,
        transfer.workspace_manifest_digest,
        transfer.materialization_digest,
        transfer.restored_manifest_digest,
    )


@pytest.mark.asyncio
async def test_phases_run_in_order_and_activation_records_matching_digests() -> None:
    w = await wire()
    transfer = await w.request()
    assert transfer.phase == ContinuationPhase.REQUESTED and not transfer.fencing
    activated = await w.drive(transfer.transfer_id)
    assert activated.activated and activated.released
    assert [item.phase for item in activated.phases] == list(PHASE_ORDER[1:])
    assert (activated.source_generation, activated.target_generation) == (1, 2)
    assert activated.target_turn_no == 2 and activated.target_session_ref == "agent-fake-2"
    # The digests the target received are the digests that were sealed and prepared.
    stored = await w.service.checkpoint(CONT_SCOPE, w.run_key, activated.checkpoint_id)
    packet = w.wired["selections"].packets[
        stored.checkpoint.context_packet_ref.removeprefix("context_packet:").rsplit("#", 1)[0]
    ]
    snapshot = w.wired["snapshots"].snapshots[activated.workspace_snapshot_ref]
    assert activated.packet_digest == packet.packet_digest
    assert activated.workspace_manifest_digest == snapshot.manifest_digest
    assert activated.materialization_digest == materialization_digest(packet, snapshot)
    assert activated.restored_manifest_digest == canonical_digest(
        dict(sorted(snapshot.manifest.items()))
    )
    assert stored.checkpoint.queued_commands == ("cmd-held-1", "cmd-held-2")
    assert stored.checkpoint.identities.source_agent_session_ref == SOURCE
    # Holds were released only after the activation; the lane state moved to the target.
    assert w.wired["mailbox"].released[transfer.transfer_id] == ("cmd-held-1", "cmd-held-2")
    state = await w.states.load(CONT_SCOPE, w.identity.harness_execution_id)
    assert state is not None and state.native_session_ref == "agent-fake-2"
    assert [item.event for item in w.wired["events"].actions] == [
        "checkpoint_sealed",
        "transferred",
    ]
    assert activated.ledger.transfers == 1
    assert len(w.hydrator.requests) == 1
    assert "purpose: continuation" in w.hydrator.requests[0].prompt_text
    # Idempotent: advancing an activated transfer changes nothing.
    again = await w.advance(transfer.transfer_id)
    assert again == activated


@pytest.mark.asyncio
@pytest.mark.parametrize("crash_at", list(PHASE_ORDER[1:]), ids=lambda phase: phase.value)
async def test_a_crash_after_any_persisted_phase_resumes_to_one_activated_target(
    crash_at: ContinuationPhase,
) -> None:
    crashes = Crashes(at={crash_at})
    w = await wire(crashes=crashes)
    transfer = await w.request()
    with pytest.raises(RuntimeError, match="worker lost"):
        await w.drive(transfer.transfer_id)
    stored = await w.service.transfer_store.get(CONT_SCOPE, transfer.transfer_id)
    assert stored is not None and stored.phase == crash_at, "the phase was persisted first"
    if crash_at in {ContinuationPhase.VERIFIED, ContinuationPhase.HYDRATED}:
        assert stored.fencing and not stored.released
    if crash_at == ContinuationPhase.ACTIVATED:
        assert stored.activated and not stored.released, "holds release after the record"
    activated = await w.drive(transfer.transfer_id)
    assert activated.activated and activated.released
    assert [item.phase for item in activated.phases] == list(PHASE_ORDER[1:]), "no repeats"
    assert len(w.hydrator.requests) == 1, "the recorded hydration is never repeated"
    assert all(item is not None for item in _digests(activated))
    siblings = await w.service.transfer_store.for_run(CONT_SCOPE, w.run_key)
    assert [item.activated for item in siblings] == [True]
    assert w.wired["mailbox"].released[transfer.transfer_id] == ("cmd-held-1", "cmd-held-2")


@pytest.mark.asyncio
async def test_a_hydration_receipt_lost_before_its_phase_persisted_is_redone_once() -> None:
    w = await wire(hydrator=FakeHydrator(fail_first=True))
    transfer = await w.request()
    with pytest.raises(ConnectionError):
        await w.drive(transfer.transfer_id)
    stored = await w.service.transfer_store.get(CONT_SCOPE, transfer.transfer_id)
    assert stored is not None and stored.phase == ContinuationPhase.TARGET_PREPARED
    activated = await w.drive(transfer.transfer_id)
    assert activated.activated and activated.target_session_ref == "agent-fake-3"
    assert len(w.hydrator.requests) == 2, "re-hydrated once; only the recorded target activates"


@pytest.mark.asyncio
async def test_a_failed_target_keeps_the_holds_and_lifts_the_fence() -> None:
    w = await wire(hydrator=FakeHydrator(corrupt=True))
    transfer = await w.request()
    with pytest.raises(ContinuationRejected) as rejected:
        await w.drive(transfer.transfer_id)
    assert rejected.value.code == CHECKPOINT_INVALID
    failed = await w.service.transfer_store.get(CONT_SCOPE, transfer.transfer_id)
    assert failed is not None
    assert (failed.status, failed.phase) == (TransferStatus.FAILED, ContinuationPhase.HYDRATED)
    assert failed.ended and not failed.fencing and not failed.released
    assert w.wired["mailbox"].released == {}, "a failed target consumes no held entry"
    state = await w.states.load(CONT_SCOPE, w.identity.harness_execution_id)
    assert state is not None and state.native_session_ref == SOURCE, "the source stays"
    assert [item.event for item in w.wired["events"].actions] == [
        "checkpoint_sealed",
        "continuation_failed",
    ]
    # The next transfer holds the same commands again and activates generation 2.
    w.hydrator.corrupt = False
    second = await w.request(ref="command://cmd-continue-2")
    activated = await w.drive(second.transfer_id)
    assert activated.activated and activated.held_command_ids == ("cmd-held-1", "cmd-held-2")
    assert (activated.source_generation, activated.target_generation) == (1, 2)
    assert w.wired["mailbox"].released == {second.transfer_id: ("cmd-held-1", "cmd-held-2")}


@pytest.mark.asyncio
async def test_two_verified_transfers_activate_exactly_one_target_generation() -> None:
    w = await wire()
    first = await w.request(ref="command://a")
    second = await w.request(ref="command://b")
    for transfer in (first, second):
        current = transfer
        while current.phase != ContinuationPhase.VERIFIED:
            current = await w.advance(current.transfer_id)
    winner = await w.advance(first.transfer_id)
    assert winner.activated
    with pytest.raises(ContinuationRejected) as conflict:
        await w.advance(second.transfer_id)
    assert conflict.value.code == "continuation_generation_conflict"
    loser = await w.service.transfer_store.get(CONT_SCOPE, second.transfer_id)
    assert loser is not None and loser.status == TransferStatus.FAILED and not loser.released
    assert w.wired["mailbox"].released == {first.transfer_id: ("cmd-held-1", "cmd-held-2")}
    activated = [
        item
        for item in await w.service.transfer_store.for_run(CONT_SCOPE, w.run_key)
        if item.activated
    ]
    assert [item.transfer_id for item in activated] == [first.transfer_id]


@pytest.mark.asyncio
async def test_the_coordinator_reads_pending_fencing_and_activated_turns() -> None:
    w = await wire()
    transfer = await w.request()
    heid = str(w.identity.harness_execution_id)
    pending = await w.coordinator.pending(CONT_SCOPE, w.run_key, source_session_ref=SOURCE)
    assert pending is not None and pending.transfer_id == transfer.transfer_id
    assert await w.coordinator.fencing(CONT_SCOPE, w.run_key, harness_execution_id=heid) is None
    frozen = await w.advance(transfer.transfer_id)
    assert frozen.phase == ContinuationPhase.FROZEN
    fencing = await w.coordinator.fencing(CONT_SCOPE, w.run_key, harness_execution_id=heid)
    assert fencing is not None and fencing.transfer_id == transfer.transfer_id
    assert await w.coordinator.pending(CONT_SCOPE, w.run_key, source_session_ref=SOURCE) is None
    oracle = ContinuationHoldOracle(w.wired["store"])
    assert await oracle.open_hold(CONT_SCOPE, w.run_key) == transfer.transfer_id
    activated = await w.drive(transfer.transfer_id)
    assert await w.coordinator.fencing(CONT_SCOPE, w.run_key, harness_execution_id=heid) is None
    assert await oracle.open_hold(CONT_SCOPE, w.run_key) is None
    found = await w.coordinator.activated_for_turn(
        CONT_SCOPE, w.run_key, harness_execution_id=heid, turn_no=2
    )
    assert found is not None and found.transfer_id == activated.transfer_id
    assert (
        await w.coordinator.activated_for_turn(
            CONT_SCOPE, w.run_key, harness_execution_id=heid, turn_no=3
        )
        is None
    )


# --- the durable mailbox: holds that survive a failed target -----------------------------------


def _entry(command_id: str, sequence: int) -> MailboxEntry:
    text = f"instruction {command_id}"
    digest = content_digest(text)
    return MailboxEntry(
        entry_id=f"entry-{command_id}",
        request_scope="tenant-1",
        run_id="run-1",
        command_id=command_id,
        command_issuer="operator",
        kind="queue_instruction",
        generation=1,
        boundary="next_turn",
        content_ref=inline_content_ref(digest),
        content_digest=digest,
        media_type="text/plain",
        content_bytes=len(text.encode("utf-8")),
        content_inline=text,
        admission_sequence=sequence,
        accepted_at=NOW,
    )


class _Holding:
    def __init__(self, transfer_id: str | None) -> None:
        self.transfer_id = transfer_id

    async def open_hold(self, request_scope: str, run_id: str) -> str | None:
        return self.transfer_id


class _NoReceipts:
    async def get_run(self, request_scope: str, run_id: str) -> Any:
        raise AssertionError("the boundary must not reach run control while held")

    async def get_boundary_command(self, *args: Any) -> Any:
        raise AssertionError("unreachable")

    async def record_boundary_receipt(self, *args: Any) -> Any:
        raise AssertionError("unreachable")


@pytest.mark.asyncio
async def test_held_entries_stay_queued_and_no_boundary_claims_them_while_holding() -> None:
    mailbox = InMemoryCommandMailbox()
    mailbox.insert_unlocked(_entry("cmd-a", 1))
    mailbox.insert_unlocked(_entry("cmd-b", 2))
    holds = ContinuationMailboxHolds(mailbox)
    assert await holds.hold("tenant-1", "run-1", "transfer-1") == ("cmd-a", "cmd-b")
    await holds.release("tenant-1", "run-1", "transfer-1", ("cmd-a", "cmd-b"))
    assert await holds.still_pending("tenant-1", "run-1") == ("cmd-a", "cmd-b"), "untouched"
    delivery = MailboxDeliveryService(mailbox, _NoReceipts(), holds=_Holding("transfer-1"))  # type: ignore[arg-type]
    with pytest.raises(MailboxHeld) as held:
        await delivery.deliver(
            "tenant-1",
            "run-1",
            delivery_key="op:1",
            family="goal_directed",
            node_key="goal",
            iteration_start=True,
            lane_profile="cursor_local",
        )
    assert (held.value.code, held.value.transfer_id) == ("continuation_holding", "transfer-1")
    entries = await mailbox.list_entries("tenant-1", "run-1")
    assert [item.state.value for item in entries] == ["queued", "queued"], "nothing was claimed"
