"""The persisted continuation phase machine (MP-12; SPEC-01, ADR-0039).

``requested -> frozen -> snapshotted -> sealed -> target_prepared -> hydrated -> verified ->
activated``, each phase recorded on the ``ContinuationTransfer`` row before the next one
starts, so a worker lost at any point resumes from the last recorded phase and never
repeats an effect it recorded:

- **frozen** holds the pending mailbox commands and records the source generation; from
  here the lane refuses every new create/send on the source session (``fencing``);
- **snapshotted** freezes the workspace (content addressed; a repeated snapshot of an
  unchanged workspace yields the same reference);
- **sealed** captures the ledger facts and seals the checkpoint through
  :meth:`ContinuationService.seal` (its own failure policy and governors apply; a transfer
  that cannot seal ends here with its reason and releases the fence);
- **target_prepared** fixes the target generation and the materialization digest (packet
  digest, workspace manifest digest and the rendered ``.mission/`` files);
- **hydrated** provisions the fresh session through the lane's hydrator and records its
  identity and restored digests (status ``transferred``);
- **verified** compares packet, workspace and materialization digests with what the target
  received (``CHECKPOINT_INVALID`` fails the transfer; the holds are never released and the
  source is released from the fence);
- **activated** records the target as the execution's native session (superseding the
  source), the transfer in the governor ledger and ``session.transferred``, then releases
  the held commands. Exactly one transfer activates a target generation for one source
  generation (``decide_activation``).

``after_persist`` is a test seam for the crash drills: it runs after each phase is recorded
and before the next phase starts; production composes ``None``.
"""

from __future__ import annotations

import hashlib
from collections.abc import Awaitable, Callable, Mapping
from typing import ClassVar, Protocol

from mission_control.application.context.continuation import (
    ContinuationRejected,
    ContinuationService,
    ContinuationTransfer,
    HydrationRequest,
    SessionHydrator,
    TransferStatus,
    WorkspaceSnapshot,
    WorkspaceSnapshotPort,
)
from mission_control.application.context.facts import ContinuationFactsCapture, FactsContext
from mission_control.application.context.lane_support import LaneSessionActivation
from mission_control.contracts.canonical import canonical_digest
from mission_control.domain.context.checkpoint import (
    CHECKPOINT_INVALID,
    CheckpointInvalid,
    ContinuationCheckpoint,
    context_packet_ref,
    record_transfer,
    require_continuity,
    require_hydratable,
)
from mission_control.domain.context.packet import ContextPacket
from mission_control.domain.context.phases import (
    ActivationCandidate,
    ContinuationPhase,
    PhaseEntry,
    advance_phase,
    decide_activation,
    next_phase,
    target_generation_for,
)
from mission_control.domain.context.render import render_mission_files, render_prompt_segment

CONTINUATION_SNAPSHOT_ROOTS: tuple[str, ...] = ("/inputs", "/outputs", "/.mission")
AfterPersist = Callable[[ContinuationPhase, ContinuationTransfer], Awaitable[None]]


class PhaseLaneSupport(Protocol):
    """What one lane profile contributes to the phases (resolved per request scope)."""

    def hydrator(self, lane_profile: str, request_scope: str) -> SessionHydrator: ...

    def snapshots(self, lane_profile: str, request_scope: str) -> WorkspaceSnapshotPort | None: ...


class ContinuationPhaseService:
    def __init__(
        self,
        service: ContinuationService,
        *,
        lanes: PhaseLaneSupport,
        facts: ContinuationFactsCapture,
        activation: LaneSessionActivation | None = None,
        after_persist: AfterPersist | None = None,
    ) -> None:
        self._service = service
        self._lanes = lanes
        self._facts = facts
        self._activation = activation
        self._after_persist = after_persist

    # -- driving ---------------------------------------------------------------------------

    async def advance(
        self, transfer_id: str, context: FactsContext, *, request_scope: str
    ) -> ContinuationTransfer:
        """Enter the next phase once (idempotent); an ended transfer is returned as is."""

        transfer = await self._service.require(request_scope, transfer_id)
        if transfer.activated:
            return await self._release_after_activation(transfer)
        if transfer.ended:
            return transfer
        target = next_phase(transfer.phase)
        assert target is not None  # ACTIVATED was handled above
        step = self._STEPS[target]
        return await step(self, transfer, context)

    async def drive(
        self, transfer_id: str, context: FactsContext, *, request_scope: str
    ) -> ContinuationTransfer:
        """Every remaining phase in turn, until activated or ended."""

        transfer = await self._service.require(request_scope, transfer_id)
        while not transfer.ended:
            transfer = await self.advance(
                transfer.transfer_id, context, request_scope=request_scope
            )
        if transfer.activated and not transfer.released:
            # Activated before a crash cut the release off: release now (idempotent).
            transfer = await self.advance(
                transfer.transfer_id, context, request_scope=request_scope
            )
        return transfer

    # -- phases ----------------------------------------------------------------------------

    async def _freeze(
        self, transfer: ContinuationTransfer, context: FactsContext
    ) -> ContinuationTransfer:
        held = await self._service.mailbox.hold(
            transfer.request_scope, transfer.run_key, transfer.transfer_id
        )
        siblings = await self._siblings(transfer)
        activated = [item for item in siblings if item.activated]
        source_generation = 1 + len(activated)
        return await self._enter(
            transfer,
            ContinuationPhase.FROZEN,
            f"held {len(held)} command(s); source generation {source_generation}",
            held_command_ids=tuple(dict.fromkeys(held)),
            harness_execution_id=context.harness_execution_id,
            source_generation=source_generation,
        )

    async def _snapshot(
        self, transfer: ContinuationTransfer, context: FactsContext
    ) -> ContinuationTransfer:
        del context
        port = self._snapshots_for(transfer)
        snapshot = await port.snapshot(
            request_scope=transfer.request_scope,
            run_key=transfer.run_key,
            session_ref=transfer.source_session_ref,
            roots=CONTINUATION_SNAPSHOT_ROOTS,
        )
        return await self._enter(
            transfer,
            ContinuationPhase.SNAPSHOTTED,
            f"workspace {snapshot.snapshot_ref} ({len(snapshot.manifest)} file(s))",
            workspace_snapshot_ref=snapshot.snapshot_ref,
            workspace_manifest_digest=snapshot.manifest_digest,
        )

    async def _seal(
        self, transfer: ContinuationTransfer, context: FactsContext
    ) -> ContinuationTransfer:
        snapshot = await self._load_snapshot(transfer)
        captured = await self._facts.capture(
            transfer, context, request_scope=transfer.request_scope
        )
        outcome = await self._service.seal(
            transfer.transfer_id,
            captured.facts,
            captured.target,
            request_scope=transfer.request_scope,
            snapshot=snapshot,
            held=transfer.held_command_ids,
        )
        sealed = outcome.transfer
        if sealed.status != TransferStatus.SEALED or outcome.checkpoint is None:
            # Parked, failed, exhausted or sent to a human: the phase is not entered and the
            # status says why; an ended transfer releases the source from its fence.
            return sealed
        packet = outcome.packet
        digest = packet.packet_digest if packet is not None else None
        if digest is None:
            _, digest = outcome.checkpoint.context_packet_ref.removeprefix(
                "context_packet:"
            ).rsplit("#", 1)
        return await self._enter(
            sealed,
            ContinuationPhase.SEALED,
            f"checkpoint {outcome.checkpoint.checkpoint_id}",
            packet_digest=digest,
        )

    async def _prepare_target(
        self, transfer: ContinuationTransfer, context: FactsContext
    ) -> ContinuationTransfer:
        del context
        _checkpoint, packet, snapshot = await self._sealed_inputs(transfer)
        materialization = materialization_digest(packet, snapshot)
        return await self._enter(
            transfer,
            ContinuationPhase.TARGET_PREPARED,
            f"target generation {target_generation_for(transfer.source_generation)}",
            target_generation=target_generation_for(transfer.source_generation),
            materialization_digest=materialization,
            packet_digest=packet.packet_digest,
        )

    async def _hydrate(
        self, transfer: ContinuationTransfer, context: FactsContext
    ) -> ContinuationTransfer:
        del context
        checkpoint, packet, snapshot = await self._sealed_inputs(transfer)
        hydrator = self._lanes.hydrator(transfer.lane_profile, transfer.request_scope)
        receipt = await hydrator.hydrate(
            HydrationRequest(
                transfer_id=transfer.transfer_id,
                request_scope=transfer.request_scope,
                run_key=transfer.run_key,
                checkpoint=checkpoint,
                packet=packet,
                snapshot=snapshot,
                prompt_text=render_prompt_segment(packet).content,
                mission_files=render_mission_files(packet),
                source_session_ref=transfer.source_session_ref,
            )
        )
        restored = _workspace_only(receipt.restored)
        return await self._enter(
            transfer,
            ContinuationPhase.HYDRATED,
            f"target session {receipt.target_session_ref}",
            status=TransferStatus.TRANSFERRED,
            target_session_ref=receipt.target_session_ref,
            restored_manifest_digest=canonical_digest(restored),
        )

    async def _verify(
        self, transfer: ContinuationTransfer, context: FactsContext
    ) -> ContinuationTransfer:
        del context
        _checkpoint, packet, snapshot = await self._sealed_inputs(transfer)
        expected = _workspace_only(snapshot.manifest)
        findings: list[str] = []
        if transfer.restored_manifest_digest != canonical_digest(expected):
            findings.append("restored workspace digest differs from the snapshot manifest")
        if transfer.packet_digest != packet.packet_digest:
            findings.append("packet digest differs from the sealed checkpoint's packet")
        if transfer.materialization_digest != materialization_digest(packet, snapshot):
            findings.append("materialization digest differs from what was prepared")
        if transfer.workspace_manifest_digest != snapshot.manifest_digest:
            findings.append("workspace manifest digest differs from the snapshot")
        if findings:
            reason = f"{CHECKPOINT_INVALID}: " + "; ".join(findings)
            failed = await self._service.fail(transfer, reason)
            raise ContinuationRejected(CHECKPOINT_INVALID, failed.failure_reason or reason)
        return await self._enter(
            transfer,
            ContinuationPhase.VERIFIED,
            "packet, workspace and materialization digests match",
        )

    async def _activate(
        self, transfer: ContinuationTransfer, context: FactsContext
    ) -> ContinuationTransfer:
        siblings = await self._siblings(transfer)
        verdict = decide_activation(
            _candidate(transfer), tuple(_candidate(item) for item in siblings)
        )
        if not verdict.allowed:
            reason = (
                f"continuation_generation_conflict: {verdict.reason}"
                f" (winner {verdict.winner_transfer_id})"
            )
            await self._service.fail(transfer, reason)
            raise ContinuationRejected("continuation_generation_conflict", reason)
        checkpoint, _packet, _snapshot = await self._sealed_inputs(transfer)
        assert transfer.target_session_ref is not None and transfer.harness_execution_id
        if self._activation is not None:
            await self._activation.activate(
                transfer.request_scope,
                transfer.harness_execution_id,
                source_session_ref=transfer.source_session_ref,
                target_session_ref=transfer.target_session_ref,
            )
        previous = await self._service.previous_checkpoint(transfer)
        ledger = record_transfer(transfer.ledger, previous=previous, current=checkpoint.body())
        await self._service.record_transferred(transfer, checkpoint)
        activated = await self._enter(
            transfer,
            ContinuationPhase.ACTIVATED,
            f"generation {transfer.target_generation} active",
            ledger=ledger,
            target_turn_no=context.turn_no + 1,
            activated_at=self._service.now(),
        )
        return await self._release_after_activation(activated)

    async def _release_after_activation(
        self, transfer: ContinuationTransfer
    ) -> ContinuationTransfer:
        """Held commands are released only after the activation is recorded; a crash in
        between releases them on the next call."""

        if transfer.released:
            return transfer
        await self._service.mailbox.release(
            transfer.request_scope,
            transfer.run_key,
            transfer.transfer_id,
            transfer.held_command_ids,
        )
        return await self._service.save(transfer, released=True)

    _STEPS: ClassVar[
        dict[
            ContinuationPhase,
            Callable[
                [ContinuationPhaseService, ContinuationTransfer, FactsContext],
                Awaitable[ContinuationTransfer],
            ],
        ]
    ] = {
        ContinuationPhase.FROZEN: _freeze,
        ContinuationPhase.SNAPSHOTTED: _snapshot,
        ContinuationPhase.SEALED: _seal,
        ContinuationPhase.TARGET_PREPARED: _prepare_target,
        ContinuationPhase.HYDRATED: _hydrate,
        ContinuationPhase.VERIFIED: _verify,
        ContinuationPhase.ACTIVATED: _activate,
    }

    # -- helpers ---------------------------------------------------------------------------

    async def _enter(
        self,
        transfer: ContinuationTransfer,
        phase: ContinuationPhase,
        detail: str,
        **changes: object,
    ) -> ContinuationTransfer:
        entered = advance_phase(transfer.phase, phase)
        now = self._service.now()
        saved = await self._service.save(
            transfer,
            phase=entered,
            phases=(*transfer.phases, PhaseEntry(phase=entered, entered_at=now, detail=detail)),
            **changes,
        )
        if self._after_persist is not None:
            await self._after_persist(entered, saved)
        return saved

    async def _siblings(self, transfer: ContinuationTransfer) -> tuple[ContinuationTransfer, ...]:
        return tuple(
            item
            for item in await self._service.transfer_store.for_run(
                transfer.request_scope, transfer.run_key
            )
            if item.logical_execution_id == transfer.logical_execution_id
            and item.transfer_id != transfer.transfer_id
        )

    def _snapshots_for(self, transfer: ContinuationTransfer) -> WorkspaceSnapshotPort:
        port = self._lanes.snapshots(transfer.lane_profile, transfer.request_scope)
        return port if port is not None else self._service.snapshot_port

    async def _load_snapshot(self, transfer: ContinuationTransfer) -> WorkspaceSnapshot:
        if transfer.workspace_snapshot_ref is None:
            raise ContinuationRejected(CHECKPOINT_INVALID, "no workspace snapshot was recorded")
        snapshot = await self._snapshots_for(transfer).load(
            request_scope=transfer.request_scope, snapshot_ref=transfer.workspace_snapshot_ref
        )
        if snapshot is None or snapshot.snapshot_ref != transfer.workspace_snapshot_ref:
            raise ContinuationRejected(CHECKPOINT_INVALID, "workspace snapshot is unavailable")
        if snapshot.manifest_digest != transfer.workspace_manifest_digest:
            raise ContinuationRejected(
                CHECKPOINT_INVALID, "workspace snapshot manifest differs from the recorded digest"
            )
        return snapshot

    async def _sealed_inputs(
        self, transfer: ContinuationTransfer
    ) -> tuple[ContinuationCheckpoint, ContextPacket, WorkspaceSnapshot]:
        if transfer.checkpoint_id is None:
            raise ContinuationRejected(CHECKPOINT_INVALID, "no sealed checkpoint is recorded")
        stored = await self._service.checkpoint_store.get(
            transfer.request_scope, transfer.run_key, transfer.checkpoint_id
        )
        if stored is None:
            raise ContinuationRejected(CHECKPOINT_INVALID, "sealed checkpoint is missing")
        try:
            checkpoint = require_hydratable(stored.checkpoint)
        except CheckpointInvalid as error:
            raise ContinuationRejected(CHECKPOINT_INVALID, str(error)) from error
        packet_id = checkpoint.context_packet_ref.removeprefix("context_packet:").rsplit("#", 1)[0]
        packet = await self._service.packet_reader.get(
            packet_id, request_scope=transfer.request_scope
        )
        if packet is None or context_packet_ref(packet) != checkpoint.context_packet_ref:
            raise ContinuationRejected(CHECKPOINT_INVALID, "continuation packet is missing")
        snapshot = await self._load_snapshot(transfer)
        if snapshot.snapshot_ref != checkpoint.workspace_snapshot_ref:
            raise ContinuationRejected(
                CHECKPOINT_INVALID, "the checkpoint names another workspace snapshot"
            )
        return checkpoint, packet, snapshot


def materialization_digest(packet: ContextPacket, snapshot: WorkspaceSnapshot) -> str:
    """Exactly what the target receives: the packet, the restorable workspace and the
    rendered ``.mission/`` files (the prompt is rendered from the same packet)."""

    files = {
        name: "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()
        for name, text in sorted(render_mission_files(packet).items())
    }
    return canonical_digest(
        {
            "packet_digest": packet.packet_digest,
            "workspace_manifest_digest": snapshot.manifest_digest,
            "mission_files": files,
        }
    )


def verify_continuity(expected: dict[str, str], restored: dict[str, str]) -> None:
    """``require_continuity`` over the restorable (non ``.mission``) paths only."""

    require_continuity(_workspace_only(expected), _workspace_only(restored))


def _workspace_only(manifest: Mapping[str, str]) -> dict[str, str]:
    return {
        path: digest
        for path, digest in sorted(manifest.items())
        if not path.startswith("/.mission/")
    }


def _candidate(transfer: ContinuationTransfer) -> ActivationCandidate:
    return ActivationCandidate(
        transfer_id=transfer.transfer_id,
        source_generation=transfer.source_generation,
        target_generation=transfer.target_generation,
        phase=transfer.phase,
    )


__all__ = [
    "CONTINUATION_SNAPSHOT_ROOTS",
    "AfterPersist",
    "ContinuationPhaseService",
    "PhaseLaneSupport",
    "materialization_digest",
    "verify_continuity",
]
