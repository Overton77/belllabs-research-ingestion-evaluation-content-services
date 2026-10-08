"""Continuation Service: trigger, seal and transfer one logical execution (SPEC-02, B4).

workflow-types/08 section 9, lane-neutral:

``safe boundary -> freeze new agent actions -> snapshot structured state and workspace ->
deterministic reduction -> semantic synthesis -> validate continuity -> seal checkpoint ->
provision fresh Session -> hydrate -> continuity check -> resume``

- :meth:`ContinuationService.request` records a trigger (context health, provider
  compaction frame, turn count, workflow boundary or a ``request_continuation`` command)
  with its lane delivery semantics; it is idempotent by trigger reference.
- :meth:`ContinuationService.seal` runs at the next safe boundary: governors, freeze
  (mailbox holds), workspace snapshot, deterministic reduction, optional admitted
  compactor under the failed-compaction policy, the ``purpose = continuation`` packet,
  validation, seal, ``session.checkpoint_sealed``.
- :meth:`ContinuationService.transfer` provisions and hydrates the fresh session through a
  lane's :class:`SessionHydrator` (Deep Agents here; Cursor is FT-G4), checks restored
  digests against the snapshot (``CHECKPOINT_INVALID``), writes ``session.transferred`` and
  releases held mailbox commands once the target's first ``session_init`` frame exists.

Every effect goes through a port; the in-memory implementations at the bottom serve unit
tests and offline harnesses, the Postgres ones live in ``adapters/postgres/context``.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal, Protocol
from uuid import UUID, uuid5

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from mission_control.application.context.pack_service import (
    ContextPackRejected,
    ContextPackService,
    ContinuationPackTarget,
)
from mission_control.contracts.canonical import canonical_digest
from mission_control.contracts.contracts import MissionControlRejected
from mission_control.domain.context.checkpoint import (
    CHECKPOINT_INVALID,
    CONTINUATION_GOVERNOR_EXHAUSTED,
    CheckpointBody,
    CheckpointInvalid,
    CompactionAttempt,
    CompactionFailurePolicy,
    CompactorKind,
    CompactorRef,
    CompactorSynthesis,
    ContinuationCheckpoint,
    ContinuationDelivery,
    ContinuationFacts,
    ContinuationGovernorPolicy,
    ContinuationLedger,
    ContinuationTrigger,
    ContinuationTriggerKind,
    FailureStep,
    context_packet_ref,
    continuation_delivery,
    evaluate_governors,
    next_failure_step,
    record_transfer,
    reduce_checkpoint,
    require_continuity,
    require_hydratable,
    seal_checkpoint,
    validate_checkpoint,
)
from mission_control.domain.context.packet import ContextPacket, LaneFileSupport, ModelBudgetProfile
from mission_control.domain.frames.contracts import FrameKind, ProviderFrame
from mission_control.domain.policies.contracts import (
    ActorContext,
    CommandResult,
    CommandStatus,
    LifecycleCommand,
    RecordContinuationAction,
    RequestContinuationAction,
    RunProjection,
)

_TRANSFER_NAMESPACE = UUID("8d3e6c7a-0b5f-4d3e-9a51-6c0de7c0a7b4")
CONTINUATION_ACTOR_REF = "mission-control:continuation"


def _utc_now() -> datetime:
    return datetime.now(UTC)


class _Record(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


# --------------------------------------------------------------------------------------
# Records
# --------------------------------------------------------------------------------------


class TransferStatus(StrEnum):
    REQUESTED = "requested"
    PARKED = "parked"
    """Sealing failed at least once; the source execution stays frozen (08 section 8)."""
    SEALED = "sealed"
    TRANSFERRED = "transferred"
    HUMAN_REVIEW = "human_review"
    FAILED = "failed"
    GOVERNOR_EXHAUSTED = "governor_exhausted"


TERMINAL_TRANSFER_STATUSES = frozenset(
    {TransferStatus.TRANSFERRED, TransferStatus.FAILED, TransferStatus.GOVERNOR_EXHAUSTED}
)


class ContinuationTransfer(_Record):
    """One continuation of one logical execution, from trigger to transfer."""

    transfer_id: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    run_key: str = Field(min_length=1)
    activation_key: str = Field(min_length=1)
    logical_execution_id: str = Field(min_length=1)
    lane_profile: str = Field(min_length=1)
    trigger: ContinuationTrigger
    delivery: ContinuationDelivery
    status: TransferStatus = TransferStatus.REQUESTED
    source_session_ref: str = Field(min_length=1)
    target_session_ref: str | None = None
    checkpoint_id: str | None = None
    held_command_ids: tuple[str, ...] = ()
    released: bool = False
    attempts: tuple[CompactionAttempt, ...] = ()
    ledger: ContinuationLedger = ContinuationLedger()
    """Cumulative continuation usage of the logical execution after this transfer."""
    failure_reason: str | None = None
    version: int = Field(default=1, ge=1)
    requested_at: AwareDatetime
    updated_at: AwareDatetime


def transfer_id_for(
    *, request_scope: str, run_key: str, logical_execution_id: str, trigger_ref: str
) -> str:
    """Deterministic transfer identity: one per (logical execution, trigger)."""

    return str(
        uuid5(
            _TRANSFER_NAMESPACE,
            "\x1f".join((request_scope, run_key, logical_execution_id, trigger_ref)),
        )
    )


class StoredCheckpoint(_Record):
    """A persisted checkpoint and the packet it names (for reads and hydration)."""

    request_scope: str
    run_key: str
    activation_key: str
    transfer_id: str | None = None
    execution_generation: int = Field(default=1, ge=1)
    checkpoint: ContinuationCheckpoint
    sealed_at: AwareDatetime


class WorkspaceSnapshot(_Record):
    """A frozen workspace: restorable path -> content digest, plus byte locators."""

    snapshot_ref: str = Field(min_length=1)
    manifest: Mapping[str, str] = Field(default_factory=dict)
    durable_refs: Mapping[str, str] = Field(default_factory=dict)
    """path -> ``<object_ref>#<sha256>:<size>`` (the verified durable input format)."""
    total_bytes: int = Field(default=0, ge=0)

    @property
    def manifest_digest(self) -> str:
        return canonical_digest(dict(sorted(self.manifest.items())))


class SealOutcome(_Record):
    transfer: ContinuationTransfer
    checkpoint: ContinuationCheckpoint | None = None
    packet: ContextPacket | None = None


class HydrationRequest(_Record):
    """What a lane needs to start the fresh session (lane-neutral; G4 consumes it)."""

    transfer_id: str
    request_scope: str
    run_key: str
    checkpoint: ContinuationCheckpoint
    packet: ContextPacket
    snapshot: WorkspaceSnapshot
    prompt_text: str
    """The ``admitted_input`` segment of the continuation packet (checkpoint fields inline)."""
    mission_files: Mapping[str, str] = Field(default_factory=dict)
    """``.mission/context.md`` and ``.mission/inputs.json`` of the continuation packet."""
    source_session_ref: str


class HydrationReceipt(_Record):
    target_session_ref: str = Field(min_length=1)
    restored: Mapping[str, str]
    """Restored path -> content digest, as the lane observed it after hydration."""
    native_identity: Mapping[str, str] = Field(default_factory=dict)


class TransferOutcome(_Record):
    transfer: ContinuationTransfer
    receipt: HydrationReceipt | None = None


class ContinuationRejected(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


# --------------------------------------------------------------------------------------
# Ports
# --------------------------------------------------------------------------------------


class ContinuationTransferRepository(Protocol):
    async def request(self, transfer: ContinuationTransfer) -> ContinuationTransfer:
        """Insert the requested transfer; a replay returns the stored row unchanged."""
        ...

    async def get(self, request_scope: str, transfer_id: str) -> ContinuationTransfer | None: ...

    async def update(
        self, transfer: ContinuationTransfer, *, expected_version: int
    ) -> ContinuationTransfer:
        """Compare-and-set on ``version``; raises :class:`ContinuationRejected` on conflict."""
        ...

    async def for_run(
        self, request_scope: str, run_key: str
    ) -> tuple[ContinuationTransfer, ...]: ...


class CheckpointRepository(Protocol):
    async def put(
        self,
        stored: StoredCheckpoint,
    ) -> StoredCheckpoint:
        """Persist a sealed checkpoint and its validation; idempotent by id and digest."""
        ...

    async def get(
        self, request_scope: str, run_key: str, checkpoint_id: str
    ) -> StoredCheckpoint | None: ...

    async def list(self, request_scope: str, run_key: str) -> tuple[StoredCheckpoint, ...]: ...


class PacketReader(Protocol):
    async def get(self, packet_id: str, *, request_scope: str) -> ContextPacket | None: ...


class WorkspaceSnapshotPort(Protocol):
    async def snapshot(
        self, *, request_scope: str, run_key: str, session_ref: str, roots: Sequence[str]
    ) -> WorkspaceSnapshot:
        """Freeze the session's workspace under ``roots`` (content addressed, verifiable)."""
        ...

    async def load(self, *, request_scope: str, snapshot_ref: str) -> WorkspaceSnapshot | None:
        """A frozen snapshot by reference, for hydration."""
        ...


class CompactorPort(Protocol):
    async def synthesize(
        self, facts: ContinuationFacts, *, compactor: CompactorRef, attempt: int
    ) -> CompactorSynthesis:
        """An admitted compacting agent's proposal (untrusted; validated before use)."""
        ...


class MailboxHoldPort(Protocol):
    """Pending commands are held during a transfer and released after hydration (08 §9)."""

    async def hold(self, request_scope: str, run_key: str, transfer_id: str) -> tuple[str, ...]: ...

    async def release(
        self, request_scope: str, run_key: str, transfer_id: str, command_ids: Sequence[str]
    ) -> None: ...


class ContinuationEventPort(Protocol):
    async def record(
        self, request_scope: str, run_key: str, action: RecordContinuationAction
    ) -> None: ...


class SessionHydrator(Protocol):
    """A lane's fresh-session provisioning from a checkpoint packet (SPEC-07 ``prepare``).

    Deep Agents: a new thread seeded with the checkpoint values and the restored files
    (``adapters/deep_agents/compaction.py``). Cursor (FT-G4): a new agent in a fresh
    workspace with the files and the git patch or branch.
    """

    async def hydrate(self, request: HydrationRequest) -> HydrationReceipt: ...


class HydrationConfirmation(Protocol):
    async def session_initialized(
        self, request_scope: str, run_key: str, target_session_ref: str
    ) -> bool:
        """True once the target session's first ``session_init`` frame is persisted."""
        ...


# --------------------------------------------------------------------------------------
# Service
# --------------------------------------------------------------------------------------


class SealTarget(_Record):
    """The fresh session's packet target (identity, budget profile and lane file support)."""

    revision_id: str = Field(min_length=1)
    node_key: str = Field(min_length=1)
    activation_id: str = Field(min_length=1)
    attempt_no: int = Field(ge=1)
    generation: int = Field(ge=0)
    profile: ModelBudgetProfile
    lane: LaneFileSupport = LaneFileSupport()
    mission_id: str | None = None


class ContinuationTriggers:
    """Records continuation triggers; the API (``request_continuation``) and the worker
    (context health, provider compaction frames, turn counts, boundaries) share it."""

    def __init__(
        self,
        transfers: ContinuationTransferRepository,
        *,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._transfers = transfers
        self._clock = clock

    async def request(
        self,
        trigger: ContinuationTrigger,
        *,
        request_scope: str,
        run_key: str,
        activation_key: str,
        logical_execution_id: str,
        lane_profile: str,
        source_session_ref: str,
    ) -> ContinuationTransfer:
        """Record a trigger; the family seals at its next safe boundary.

        Delivery is ``turn_boundary_guaranteed`` on Deep Agents and ``wait_then_send`` on
        Cursor; a lane without continuation delivery is rejected (``unsupported_control``).
        Idempotent by (logical execution, trigger reference).
        """

        try:
            delivery = continuation_delivery(lane_profile)
        except ValueError as error:
            raise ContinuationRejected("unsupported_control", str(error)) from error
        now = self._clock()
        transfer = ContinuationTransfer(
            transfer_id=transfer_id_for(
                request_scope=request_scope,
                run_key=run_key,
                logical_execution_id=logical_execution_id,
                trigger_ref=trigger.ref,
            ),
            request_scope=request_scope,
            run_key=run_key,
            activation_key=activation_key,
            logical_execution_id=logical_execution_id,
            lane_profile=lane_profile,
            trigger=trigger,
            delivery=delivery,
            source_session_ref=source_session_ref,
            ledger=await self.ledger(request_scope, run_key, logical_execution_id),
            requested_at=now,
            updated_at=now,
        )
        return await self._transfers.request(transfer)

    async def pending(self, request_scope: str, run_key: str) -> tuple[ContinuationTransfer, ...]:
        return tuple(
            item
            for item in await self._transfers.for_run(request_scope, run_key)
            if item.status in {TransferStatus.REQUESTED, TransferStatus.PARKED}
        )

    async def ledger(
        self, request_scope: str, run_key: str, logical_execution_id: str
    ) -> ContinuationLedger:
        """The cumulative continuation ledger after the last completed transfer."""

        prior = [
            item
            for item in await self._transfers.for_run(request_scope, run_key)
            if item.logical_execution_id == logical_execution_id
            and item.status == TransferStatus.TRANSFERRED
        ]
        if not prior:
            return ContinuationLedger()
        return max(prior, key=lambda item: item.ledger.transfers).ledger


class ContinuationService:
    def __init__(
        self,
        *,
        transfers: ContinuationTransferRepository,
        checkpoints: CheckpointRepository,
        packets: ContextPackService,
        packet_reader: PacketReader,
        snapshots: WorkspaceSnapshotPort,
        events: ContinuationEventPort,
        mailbox: MailboxHoldPort | None = None,
        compactor: CompactorPort | None = None,
        confirmation: HydrationConfirmation | None = None,
        failure_policy: CompactionFailurePolicy | None = None,
        governors: ContinuationGovernorPolicy | None = None,
        clock: Callable[[], datetime] = _utc_now,
        author: str = CONTINUATION_ACTOR_REF,
    ) -> None:
        self._transfers = transfers
        self._checkpoints = checkpoints
        self._packets = packets
        self._packet_reader = packet_reader
        self._snapshots = snapshots
        self._events = events
        self._mailbox = mailbox or NoMailbox()
        self._compactor = compactor
        self._confirmation = confirmation
        self._failure_policy = failure_policy or CompactionFailurePolicy()
        self._governors = governors or ContinuationGovernorPolicy()
        self._clock = clock
        self._author = author
        self._triggers = ContinuationTriggers(transfers, clock=clock)

    # -- trigger ------------------------------------------------------------------------

    async def request(
        self,
        trigger: ContinuationTrigger,
        *,
        request_scope: str,
        run_key: str,
        activation_key: str,
        logical_execution_id: str,
        lane_profile: str,
        source_session_ref: str,
    ) -> ContinuationTransfer:
        """Record a trigger (see :meth:`ContinuationTriggers.request`)."""

        return await self._triggers.request(
            trigger,
            request_scope=request_scope,
            run_key=run_key,
            activation_key=activation_key,
            logical_execution_id=logical_execution_id,
            lane_profile=lane_profile,
            source_session_ref=source_session_ref,
        )

    async def pending(self, request_scope: str, run_key: str) -> tuple[ContinuationTransfer, ...]:
        """Triggers awaiting a seal at the family's next safe boundary."""

        return await self._triggers.pending(request_scope, run_key)

    # -- seal -----------------------------------------------------------------------------

    async def seal(
        self, transfer_id: str, facts: ContinuationFacts, target: SealTarget, *, request_scope: str
    ) -> SealOutcome:
        """08 section 9 up to the seal, with 08 section 8 failure and 13 governors."""

        transfer = await self._require(request_scope, transfer_id)
        if transfer.status == TransferStatus.SEALED or transfer.status in (
            TERMINAL_TRANSFER_STATUSES
        ):
            stored = (
                await self._checkpoints.get(request_scope, transfer.run_key, transfer.checkpoint_id)
                if transfer.checkpoint_id
                else None
            )
            return SealOutcome(transfer=transfer, checkpoint=stored.checkpoint if stored else None)
        verdict = evaluate_governors(transfer.ledger, self._governors)
        if not verdict.allowed:
            return SealOutcome(
                transfer=await self._exhausted(transfer, ",".join(verdict.exhausted))
            )
        # Freeze new agent actions: hold every pending mailbox command for this transfer.
        held = await self._mailbox.hold(request_scope, transfer.run_key, transfer.transfer_id)
        snapshot = await self._snapshots.snapshot(
            request_scope=request_scope,
            run_key=transfer.run_key,
            session_ref=transfer.source_session_ref,
            roots=("/inputs", "/outputs", "/.mission"),
        )
        facts = facts.model_copy(
            update={
                "queued_command_ids": tuple(dict.fromkeys((*facts.queued_command_ids, *held))),
                "workspace_snapshot_ref": snapshot.snapshot_ref,
                "workspace_manifest": dict(snapshot.manifest),
                "governors_remaining": verdict.remaining,
            }
        )
        transfer = await self._save(transfer, held_command_ids=tuple(held))
        attempts = list(transfer.attempts)
        compactor = self._initial_compactor()
        while True:
            outcome = await self._attempt(transfer, facts, target, snapshot, compactor, attempts)
            if outcome is not None:
                return outcome
            step = next_failure_step(attempts, self._failure_policy)
            failures = sum(1 for item in attempts if item.result != "valid")
            ledger = transfer.ledger.model_copy(update={"failed_compactions": failures})
            transfer = await self._save(
                transfer, status=TransferStatus.PARKED, attempts=tuple(attempts), ledger=ledger
            )
            if not evaluate_governors(ledger, self._governors).allowed:
                return SealOutcome(
                    transfer=await self._exhausted(transfer, "max_failed_compactions")
                )
            if step == FailureStep.RETRY_COMPACTOR:
                continue
            if step == FailureStep.FALLBACK_COMPACTOR:
                assert self._failure_policy.fallback_compactor_ref is not None
                compactor = CompactorRef(
                    kind=CompactorKind.ADMITTED_AGENT,
                    ref=self._failure_policy.fallback_compactor_ref,
                )
                continue
            if step == FailureStep.HUMAN_REVIEW:
                return SealOutcome(
                    transfer=await self._save(
                        transfer,
                        status=TransferStatus.HUMAN_REVIEW,
                        failure_reason="checkpoint validation failed; human review required",
                    )
                )
            return SealOutcome(transfer=await self._fail(transfer, "no valid checkpoint"))

    def _initial_compactor(self) -> CompactorRef | None:
        return (
            CompactorRef(kind=CompactorKind.ADMITTED_AGENT, ref="mc.admitted_compactor")
            if self._compactor is not None
            else None
        )

    async def _attempt(
        self,
        transfer: ContinuationTransfer,
        facts: ContinuationFacts,
        target: SealTarget,
        snapshot: WorkspaceSnapshot,
        compactor: CompactorRef | None,
        attempts: list[CompactionAttempt],
    ) -> SealOutcome | None:
        """One compaction attempt; ``None`` means it failed and the policy decides next."""

        attempt_no = len(attempts) + 1
        synthesis: CompactorSynthesis | None = None
        used = compactor or CompactorRef(
            kind=CompactorKind.DETERMINISTIC, ref="mc.continuation_reducer/1"
        )
        if compactor is not None and self._compactor is not None:
            try:
                synthesis = await self._compactor.synthesize(
                    facts, compactor=compactor, attempt=attempt_no
                )
            except Exception:
                attempts.append(CompactionAttempt(compactor=used, result="error"))
                return None
        checkpoint_id = str(
            uuid5(_TRANSFER_NAMESPACE, f"{transfer.transfer_id}\x1fcheckpoint\x1f{attempt_no}")
        )
        sealed_at = self._clock()
        body = reduce_checkpoint(
            facts,
            checkpoint_id=checkpoint_id,
            context_packet_ref="context_packet:pending#sha256:" + "0" * 64,
            author=self._author,
            authored_at=sealed_at,
            synthesis=synthesis,
        )
        try:
            sealed_packet = await self._packets.pack_for_continuation(
                body,
                ContinuationPackTarget(
                    request_scope=transfer.request_scope,
                    run_id=transfer.run_key,
                    revision_id=target.revision_id,
                    node_key=target.node_key,
                    activation_id=target.activation_id,
                    attempt_no=target.attempt_no,
                    generation=target.generation,
                    sealed_at=sealed_at,
                    profile=target.profile,
                    workspace_manifest_digest=snapshot.manifest_digest,
                    workspace_bytes=snapshot.total_bytes,
                    lane=target.lane,
                    mission_id=target.mission_id,
                ),
            )
        except ContextPackRejected:
            attempts.append(CompactionAttempt(compactor=used, result="error"))
            return None
        body = body.model_copy(
            update={"context_packet_ref": context_packet_ref(sealed_packet.packet)}
        )
        verdict = validate_checkpoint(body, facts, sealed_packet.packet)
        checkpoint = seal_checkpoint(body, verdict)
        await self._checkpoints.put(
            StoredCheckpoint(
                request_scope=transfer.request_scope,
                run_key=transfer.run_key,
                activation_key=transfer.activation_key,
                transfer_id=transfer.transfer_id,
                execution_generation=max(target.generation, 1),
                checkpoint=checkpoint,
                sealed_at=sealed_at,
            )
        )
        attempts.append(CompactionAttempt(compactor=used, result=verdict.result))
        await self._events.record(
            transfer.request_scope,
            transfer.run_key,
            self._action(
                transfer,
                "checkpoint_sealed",
                checkpoint=checkpoint,
            ),
        )
        if not checkpoint.valid:
            return None
        sealed = await self._save(
            transfer,
            status=TransferStatus.SEALED,
            checkpoint_id=checkpoint.checkpoint_id,
            attempts=tuple(attempts),
        )
        return SealOutcome(transfer=sealed, checkpoint=checkpoint, packet=sealed_packet.packet)

    # -- transfer -------------------------------------------------------------------------

    async def transfer(
        self, transfer_id: str, hydrator: SessionHydrator, *, request_scope: str
    ) -> TransferOutcome:
        """Provision and hydrate the fresh session; never from an invalid checkpoint."""

        transfer = await self._require(request_scope, transfer_id)
        if transfer.status == TransferStatus.TRANSFERRED:
            return TransferOutcome(
                transfer=await self.release_if_hydrated(transfer_id, request_scope=request_scope)
            )
        if transfer.status != TransferStatus.SEALED or transfer.checkpoint_id is None:
            raise ContinuationRejected(
                CHECKPOINT_INVALID, f"transfer {transfer_id} has no sealed valid checkpoint"
            )
        stored = await self._checkpoints.get(
            request_scope, transfer.run_key, transfer.checkpoint_id
        )
        if stored is None:
            raise ContinuationRejected(CHECKPOINT_INVALID, "sealed checkpoint is missing")
        try:
            checkpoint = require_hydratable(stored.checkpoint)
        except CheckpointInvalid as error:
            raise ContinuationRejected(CHECKPOINT_INVALID, str(error)) from error
        packet_id = checkpoint.context_packet_ref.removeprefix("context_packet:").rsplit("#", 1)[0]
        packet = await self._packet_reader.get(packet_id, request_scope=request_scope)
        if packet is None or context_packet_ref(packet) != checkpoint.context_packet_ref:
            raise ContinuationRejected(CHECKPOINT_INVALID, "continuation packet is missing")
        snapshot = await self._snapshot_of(checkpoint, transfer)
        from mission_control.domain.context.render import (  # local: render is pure
            render_mission_files,
            render_prompt_segment,
        )

        receipt = await hydrator.hydrate(
            HydrationRequest(
                transfer_id=transfer.transfer_id,
                request_scope=request_scope,
                run_key=transfer.run_key,
                checkpoint=checkpoint,
                packet=packet,
                snapshot=snapshot,
                prompt_text=render_prompt_segment(packet).content,
                mission_files=render_mission_files(packet),
                source_session_ref=transfer.source_session_ref,
            )
        )
        restored_workspace = {
            path: digest
            for path, digest in receipt.restored.items()
            if not path.startswith("/.mission/")
        }
        expected_workspace = {
            path: digest
            for path, digest in snapshot.manifest.items()
            if not path.startswith("/.mission/")
        }
        try:
            require_continuity(expected_workspace, restored_workspace)
        except CheckpointInvalid as error:
            await self._fail(transfer, f"{CHECKPOINT_INVALID}: {error}")
            raise ContinuationRejected(CHECKPOINT_INVALID, str(error)) from error
        previous = await self._previous_checkpoint(transfer)
        ledger = record_transfer(transfer.ledger, previous=previous, current=checkpoint.body())
        transfer = await self._save(
            transfer,
            status=TransferStatus.TRANSFERRED,
            target_session_ref=receipt.target_session_ref,
            ledger=ledger,
        )
        await self._events.record(
            request_scope,
            transfer.run_key,
            self._action(transfer, "transferred", checkpoint=checkpoint),
        )
        transfer = await self.release_if_hydrated(transfer.transfer_id, request_scope=request_scope)
        return TransferOutcome(transfer=transfer, receipt=receipt)

    async def release_if_hydrated(
        self, transfer_id: str, *, request_scope: str
    ) -> ContinuationTransfer:
        """Release held commands once the target's first ``session_init`` frame exists."""

        transfer = await self._require(request_scope, transfer_id)
        if (
            transfer.status != TransferStatus.TRANSFERRED
            or transfer.released
            or transfer.target_session_ref is None
        ):
            return transfer
        if self._confirmation is not None and not await self._confirmation.session_initialized(
            request_scope, transfer.run_key, transfer.target_session_ref
        ):
            return transfer
        await self._mailbox.release(
            request_scope, transfer.run_key, transfer.transfer_id, transfer.held_command_ids
        )
        return await self._save(transfer, released=True)

    # -- reads -----------------------------------------------------------------------------

    async def checkpoints(self, request_scope: str, run_key: str) -> tuple[StoredCheckpoint, ...]:
        return await self._checkpoints.list(request_scope, run_key)

    async def checkpoint(
        self, request_scope: str, run_key: str, checkpoint_id: str
    ) -> StoredCheckpoint | None:
        return await self._checkpoints.get(request_scope, run_key, checkpoint_id)

    # -- helpers ---------------------------------------------------------------------------

    async def _require(self, request_scope: str, transfer_id: str) -> ContinuationTransfer:
        transfer = await self._transfers.get(request_scope, transfer_id)
        if transfer is None:
            raise ContinuationRejected("not_found", f"continuation transfer {transfer_id}")
        return transfer

    async def _previous_checkpoint(self, transfer: ContinuationTransfer) -> CheckpointBody | None:
        prior = [
            item
            for item in await self._transfers.for_run(transfer.request_scope, transfer.run_key)
            if item.logical_execution_id == transfer.logical_execution_id
            and item.status == TransferStatus.TRANSFERRED
            and item.checkpoint_id is not None
        ]
        if not prior:
            return None
        latest = max(prior, key=lambda item: item.ledger.transfers)
        assert latest.checkpoint_id is not None
        stored = await self._checkpoints.get(
            transfer.request_scope, transfer.run_key, latest.checkpoint_id
        )
        return stored.checkpoint.body() if stored else None

    async def _snapshot_of(
        self, checkpoint: ContinuationCheckpoint, transfer: ContinuationTransfer
    ) -> WorkspaceSnapshot:
        snapshot = await self._snapshots.load(
            request_scope=transfer.request_scope, snapshot_ref=checkpoint.workspace_snapshot_ref
        )
        if snapshot is None or snapshot.snapshot_ref != checkpoint.workspace_snapshot_ref:
            raise ContinuationRejected(CHECKPOINT_INVALID, "workspace snapshot is unavailable")
        return snapshot

    def _action(
        self,
        transfer: ContinuationTransfer,
        event: Literal["checkpoint_sealed", "transferred", "continuation_failed"],
        *,
        checkpoint: ContinuationCheckpoint | None = None,
        failure_reason: str | None = None,
    ) -> RecordContinuationAction:
        return RecordContinuationAction(
            event=event,
            transfer_id=transfer.transfer_id,
            activation_id=transfer.activation_key,
            logical_execution_id=transfer.logical_execution_id,
            lane_profile=transfer.lane_profile,
            trigger_kind=transfer.trigger.kind.value,
            source_session_ref=transfer.source_session_ref,
            target_session_ref=transfer.target_session_ref if event == "transferred" else None,
            checkpoint_id=checkpoint.checkpoint_id if checkpoint else None,
            checkpoint_digest=checkpoint.checkpoint_digest if checkpoint else None,
            context_packet_ref=checkpoint.context_packet_ref if checkpoint else None,
            validator_result=checkpoint.validator.result if checkpoint else None,
            failure_reason=failure_reason,
            released_command_ids=transfer.held_command_ids if event == "transferred" else (),
        )

    async def _save(
        self, transfer: ContinuationTransfer, **changes: object
    ) -> ContinuationTransfer:
        updated = ContinuationTransfer.model_validate(
            {
                **transfer.model_dump(mode="python"),
                **changes,
                "version": transfer.version + 1,
                "updated_at": self._clock(),
            }
        )
        return await self._transfers.update(updated, expected_version=transfer.version)

    async def _fail(self, transfer: ContinuationTransfer, reason: str) -> ContinuationTransfer:
        failed = await self._save(
            transfer, status=TransferStatus.FAILED, failure_reason=reason[:1024]
        )
        await self._events.record(
            failed.request_scope,
            failed.run_key,
            self._action(failed, "continuation_failed", failure_reason=reason[:1024]),
        )
        return failed

    async def _exhausted(self, transfer: ContinuationTransfer, which: str) -> ContinuationTransfer:
        reason = f"{CONTINUATION_GOVERNOR_EXHAUSTED}: {which}"
        exhausted = await self._save(
            transfer, status=TransferStatus.GOVERNOR_EXHAUSTED, failure_reason=reason[:1024]
        )
        await self._events.record(
            exhausted.request_scope,
            exhausted.run_key,
            self._action(exhausted, "continuation_failed", failure_reason=reason[:1024]),
        )
        return exhausted


# --------------------------------------------------------------------------------------
# Defaults and in-memory implementations (unit tests and offline harnesses)
# --------------------------------------------------------------------------------------


class NoMailbox:
    """No mailbox in this composition (FT-F1 provides the real one): nothing is held."""

    async def hold(self, request_scope: str, run_key: str, transfer_id: str) -> tuple[str, ...]:
        return ()

    async def release(
        self, request_scope: str, run_key: str, transfer_id: str, command_ids: Sequence[str]
    ) -> None:
        return None


@dataclass
class InMemoryMailbox:
    """Pending command ids per run; holds and releases are recorded for assertions."""

    pending: dict[str, list[str]] = field(default_factory=dict)
    held: dict[str, tuple[str, ...]] = field(default_factory=dict)
    released: dict[str, tuple[str, ...]] = field(default_factory=dict)

    async def hold(self, request_scope: str, run_key: str, transfer_id: str) -> tuple[str, ...]:
        ids = tuple(self.pending.get(run_key, ()))
        self.held[transfer_id] = ids
        return ids

    async def release(
        self, request_scope: str, run_key: str, transfer_id: str, command_ids: Sequence[str]
    ) -> None:
        self.released[transfer_id] = tuple(command_ids)


@dataclass
class InMemoryContinuationStore:
    """Transfers and checkpoints with the Postgres semantics (idempotent, CAS on version)."""

    transfers: dict[tuple[str, str], ContinuationTransfer] = field(default_factory=dict)
    checkpoints: dict[tuple[str, str, str], StoredCheckpoint] = field(default_factory=dict)

    async def request(self, transfer: ContinuationTransfer) -> ContinuationTransfer:
        key = (transfer.request_scope, transfer.transfer_id)
        return self.transfers.setdefault(key, transfer)

    async def get(self, request_scope: str, transfer_id: str) -> ContinuationTransfer | None:
        return self.transfers.get((request_scope, transfer_id))

    async def update(
        self, transfer: ContinuationTransfer, *, expected_version: int
    ) -> ContinuationTransfer:
        key = (transfer.request_scope, transfer.transfer_id)
        current = self.transfers.get(key)
        if current is None or current.version != expected_version:
            raise ContinuationRejected("stale_version", "continuation transfer changed")
        self.transfers[key] = transfer
        return transfer

    async def for_run(self, request_scope: str, run_key: str) -> tuple[ContinuationTransfer, ...]:
        return tuple(
            item
            for (scope, _id), item in sorted(self.transfers.items())
            if scope == request_scope and item.run_key == run_key
        )

    async def put(self, stored: StoredCheckpoint) -> StoredCheckpoint:
        key = (stored.request_scope, stored.run_key, stored.checkpoint.checkpoint_id)
        prior = self.checkpoints.get(key)
        if prior is not None:
            if prior.checkpoint.checkpoint_digest != stored.checkpoint.checkpoint_digest:
                raise ContinuationRejected("checkpoint_conflict", "checkpoint id reused")
            return prior
        self.checkpoints[key] = stored
        return stored

    async def get_checkpoint(
        self, request_scope: str, run_key: str, checkpoint_id: str
    ) -> StoredCheckpoint | None:
        return self.checkpoints.get((request_scope, run_key, checkpoint_id))

    async def list_checkpoints(
        self, request_scope: str, run_key: str
    ) -> tuple[StoredCheckpoint, ...]:
        return tuple(
            sorted(
                (
                    item
                    for (scope, run, _id), item in self.checkpoints.items()
                    if scope == request_scope and run == run_key
                ),
                key=lambda item: (item.sealed_at, item.checkpoint.checkpoint_id),
            )
        )


class InMemoryCheckpoints:
    """The :class:`CheckpointRepository` view of an :class:`InMemoryContinuationStore`."""

    def __init__(self, store: InMemoryContinuationStore) -> None:
        self._store = store

    async def put(self, stored: StoredCheckpoint) -> StoredCheckpoint:
        return await self._store.put(stored)

    async def get(
        self, request_scope: str, run_key: str, checkpoint_id: str
    ) -> StoredCheckpoint | None:
        return await self._store.get_checkpoint(request_scope, run_key, checkpoint_id)

    async def list(self, request_scope: str, run_key: str) -> tuple[StoredCheckpoint, ...]:
        return await self._store.list_checkpoints(request_scope, run_key)


@dataclass
class RecordingContinuationEvents:
    """Collects the continuation actions a service records (unit tests)."""

    actions: list[RecordContinuationAction] = field(default_factory=list)

    async def record(
        self, request_scope: str, run_key: str, action: RecordContinuationAction
    ) -> None:
        self.actions.append(action)


class ContinuationRunControl(Protocol):
    async def execute(self, command: LifecycleCommand) -> CommandResult: ...

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...

    async def get_command_result(
        self, request_scope: str, run_id: str, idempotency_issuer: str, command_id: str
    ) -> CommandResult | None: ...


class RunControlContinuationEvents:
    """Writes continuation facts as mission events through the run-control reducer.

    One command id per (transfer, event): a replay finds the stored result and records
    nothing new; a stale run version is never persisted (``record_continuation`` is a
    boundary fact kind), so the recorder rebinds the current version and retries.
    """

    ISSUER = "mission-control-continuation"

    def __init__(
        self,
        run_control: ContinuationRunControl,
        *,
        actor: ActorContext,
        clock: Callable[[], datetime] = _utc_now,
        attempts: int = 16,
    ) -> None:
        self._run_control = run_control
        self._actor = actor
        self._clock = clock
        self._attempts = attempts

    async def record(
        self, request_scope: str, run_key: str, action: RecordContinuationAction
    ) -> None:
        suffix = action.checkpoint_id or action.failure_reason or ""
        command_id = f"continuation:{action.transfer_id}:{action.event}:{suffix}"[:512]
        for _attempt in range(self._attempts):
            prior = await self._run_control.get_command_result(
                request_scope, run_key, self.ISSUER, command_id
            )
            if prior is not None and prior.status == CommandStatus.ACCEPTED:
                return
            run = await self._run_control.get_run(request_scope, run_key)
            result = await self._run_control.execute(
                LifecycleCommand(
                    command_id=command_id,
                    idempotency_issuer=self.ISSUER,
                    request_scope=request_scope,
                    run_id=run_key,
                    expected_run_version=run.version,
                    actor=self._actor,
                    action=action,
                    reason=f"continuation {action.event}",
                    evidence_refs=tuple(
                        ref
                        for ref in (
                            f"continuation_transfer:{action.transfer_id}",
                            f"checkpoint:{action.checkpoint_id}" if action.checkpoint_id else None,
                        )
                        if ref is not None
                    ),
                    occurred_at=self._clock(),
                    correlation_id=f"continuation:{action.transfer_id}",
                )
            )
            if result.status == CommandStatus.ACCEPTED:
                return
            if result.status == CommandStatus.STALE or result.reason_code == "stale_run_version":
                continue
            raise ContinuationRejected(
                result.reason_code, result.reason or "continuation event rejected"
            )
        raise ContinuationRejected(
            "continuation_event_contended", "run version kept changing while recording"
        )


@dataclass(frozen=True, slots=True)
class LocatedSession:
    activation_ref: str
    lane_profile: str
    native_session_ref: str


class SessionLocator(Protocol):
    async def current_session(
        self, request_scope: str, run_key: str, activation_ref: str | None
    ) -> LocatedSession | None:
        """The most recently observed native session of the activation (or of the run)."""
        ...


class FrameSessionLocator:
    """Locates the live session from the Native Event Store (latest frame wins)."""

    def __init__(
        self,
        frames: SessionInitFrames,
        run_ids: Callable[[str, str], Awaitable[UUID | None]],
    ) -> None:
        self._frames = frames
        self._run_ids = run_ids

    async def current_session(
        self, request_scope: str, run_key: str, activation_ref: str | None
    ) -> LocatedSession | None:
        run_id = await self._run_ids(request_scope, run_key)
        if run_id is None:
            return None
        frames = [
            frame
            for frame in await self._frames.frames_for_run(request_scope, run_id)
            if activation_ref is None or str(frame.activation_id) == activation_ref
        ]
        if not frames:
            return None
        latest = max(frames, key=lambda frame: (frame.observed_at, frame.arrival_ordinal))
        return LocatedSession(
            activation_ref=str(latest.activation_id),
            lane_profile=latest.lane_profile.value,
            native_session_ref=latest.native_session_ref,
        )


class ContinuationCommands:
    """``request_continuation`` on the command path (the ``ContinuationCommandPort``).

    ``plan`` resolves the session to continue and its lane delivery before the command is
    accepted; ``record`` writes the trigger after acceptance. The logical execution is the
    activation (intra-activation continuation keeps its identity, 08 section 10).
    """

    def __init__(
        self,
        triggers: ContinuationTriggers | ContinuationService,
        sessions: SessionLocator,
        *,
        request_scope: str,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self._service = triggers
        self._sessions = sessions
        self._scope = request_scope
        self._clock = clock

    async def plan(
        self, run_id: str, command_id: str, activation_id: str | None
    ) -> RequestContinuationAction:
        located = await self._sessions.current_session(self._scope, run_id, activation_id)
        if located is None:
            raise MissionControlRejected(
                "no_active_session", "no observed agent session to continue for this run"
            )
        try:
            delivery = continuation_delivery(located.lane_profile)
        except ValueError as error:
            raise MissionControlRejected("unsupported_control", str(error)) from error
        return RequestContinuationAction(
            transfer_id=transfer_id_for(
                request_scope=self._scope,
                run_key=run_id,
                logical_execution_id=located.activation_ref,
                trigger_ref=_command_trigger_ref(command_id),
            ),
            activation_id=located.activation_ref,
            logical_execution_id=located.activation_ref,
            lane_profile=located.lane_profile,
            source_session_ref=located.native_session_ref,
            delivery=delivery,
        )

    async def record(self, run_id: str, action: RequestContinuationAction, command_id: str) -> None:
        await self._service.request(
            ContinuationTrigger(
                kind=ContinuationTriggerKind.REQUEST_CONTINUATION,
                ref=_command_trigger_ref(command_id),
                observed_at=self._clock(),
            ),
            request_scope=self._scope,
            run_key=run_id,
            activation_key=action.activation_id,
            logical_execution_id=action.logical_execution_id,
            lane_profile=action.lane_profile,
            source_session_ref=action.source_session_ref,
        )


def _command_trigger_ref(command_id: str) -> str:
    return f"command://{command_id}"


class FrameHydrationConfirmation:
    """Hydration is confirmed by the target session's first persisted ``session_init``."""

    def __init__(
        self,
        frames: SessionInitFrames,
        run_ids: Callable[[str, str], Awaitable[UUID | None]],
    ) -> None:
        self._frames = frames
        self._run_ids = run_ids

    async def session_initialized(
        self, request_scope: str, run_key: str, target_session_ref: str
    ) -> bool:
        run_id = await self._run_ids(request_scope, run_key)
        if run_id is None:
            return False
        frames = await self._frames.frames_for_run(request_scope, run_id)
        return any(
            frame.native_session_ref == target_session_ref and frame.kind == FrameKind.SESSION_INIT
            for frame in frames
        )


class SessionInitFrames(Protocol):
    async def frames_for_run(
        self, request_scope: str, run_id: UUID, *, closing_only: bool = False, limit: int = 10_000
    ) -> tuple[ProviderFrame, ...]: ...


# --------------------------------------------------------------------------------------
# Reads (CLI `run checkpoint`, HTTP `GET .../runs/{id}/checkpoints[/{id}]`)
# --------------------------------------------------------------------------------------

REDACTION_THRESHOLD_BYTES = 4_096
READ_PERMISSION = "workflow_run.read"


class CheckpointReadDenied(PermissionError):
    pass


class CheckpointNotFound(LookupError):
    pass


class CheckpointView(_Record):
    """A checkpoint as operators read it; bodies above 4 KiB become digests."""

    schema_version: Literal["mc.checkpoint_view.v1"] = "mc.checkpoint_view.v1"
    run_id: str
    checkpoint_id: str
    checkpoint_digest: str
    validator_result: Literal["valid", "invalid"]
    transfer_id: str | None = None
    transfer_status: str | None = None
    sealed_at: AwareDatetime
    redacted_paths: tuple[str, ...] = ()
    checkpoint: dict[str, object]


def redact_large(
    value: object, *, limit: int = REDACTION_THRESHOLD_BYTES, path: str = ""
) -> tuple[object, tuple[str, ...]]:
    """Replace every string or list whose canonical JSON exceeds ``limit`` bytes by its
    digest and size; returns the redacted value and the redacted paths."""

    if isinstance(value, Mapping):
        redacted: dict[str, object] = {}
        paths: list[str] = []
        for key, item in value.items():
            child, child_paths = redact_large(
                item, limit=limit, path=f"{path}.{key}" if path else str(key)
            )
            redacted[str(key)] = child
            paths.extend(child_paths)
        return redacted, tuple(paths)
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    if isinstance(value, str | list | tuple) and len(encoded) > limit:
        digest = "sha256:" + hashlib.sha256(encoded).hexdigest()
        return (
            {
                "redacted": True,
                "digest": digest,
                "bytes": len(encoded),
                **({"items": len(value)} if isinstance(value, list | tuple) else {}),
            },
            (path,),
        )
    if isinstance(value, list | tuple):
        items: list[object] = []
        paths = []
        for index, item in enumerate(value):
            child, child_paths = redact_large(item, limit=limit, path=f"{path}[{index}]")
            items.append(child)
            paths.extend(child_paths)
        return items, tuple(paths)
    return value, ()


class CheckpointReadService:
    """Scoped reads of a run's continuation checkpoints (``workflow_run.read``)."""

    def __init__(
        self,
        checkpoints: CheckpointRepository,
        transfers: ContinuationTransferRepository,
        *,
        request_scope: str,
    ) -> None:
        self._checkpoints = checkpoints
        self._transfers = transfers
        self.request_scope = request_scope

    async def list(self, run_id: str, *, actor: ActorContext) -> tuple[CheckpointView, ...]:
        self._authorize(actor)
        transfers = {
            item.transfer_id: item
            for item in await self._transfers.for_run(self.request_scope, run_id)
        }
        return tuple(
            self._view(item, transfers.get(item.transfer_id or ""), full=False)
            for item in await self._checkpoints.list(self.request_scope, run_id)
        )

    async def get(
        self, run_id: str, checkpoint_id: str, *, actor: ActorContext, full: bool = False
    ) -> CheckpointView:
        self._authorize(actor)
        stored = await self._checkpoints.get(self.request_scope, run_id, checkpoint_id)
        if stored is None:
            raise CheckpointNotFound(checkpoint_id)
        transfer = (
            await self._transfers.get(self.request_scope, stored.transfer_id)
            if stored.transfer_id
            else None
        )
        return self._view(stored, transfer, full=full)

    @staticmethod
    def _authorize(actor: ActorContext) -> None:
        if READ_PERMISSION not in actor.permissions:
            raise CheckpointReadDenied("actor lacks workflow_run.read permission")

    @staticmethod
    def _view(
        stored: StoredCheckpoint, transfer: ContinuationTransfer | None, *, full: bool
    ) -> CheckpointView:
        body = stored.checkpoint.model_dump(mode="json")
        paths: tuple[str, ...] = ()
        if not full:
            redacted, paths = redact_large(body)
            assert isinstance(redacted, dict)
            body = redacted
        return CheckpointView(
            run_id=stored.run_key,
            checkpoint_id=stored.checkpoint.checkpoint_id,
            checkpoint_digest=stored.checkpoint.checkpoint_digest,
            validator_result=stored.checkpoint.validator.result,
            transfer_id=stored.transfer_id,
            transfer_status=transfer.status.value if transfer else None,
            sealed_at=stored.sealed_at,
            redacted_paths=paths,
            checkpoint=body,
        )
