"""Application-scoped adapters to the existing snapshot/fork/reconciliation sagas."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from mission_control.application.execution.mailbox import (
    MailboxContentRejected,
    mailbox_command_action,
)
from mission_control.application.execution.operations.unit_reconciliation import (
    UnitReconciliationService,
)
from mission_control.application.recovery.fork_seed import ForkSeedRejected, ForkSeedService
from mission_control.application.recovery.run_forks import (
    ADMIT_PERMISSION,
    FORK_PERMISSION,
    SNAPSHOT_PERMISSION,
    ForkCommand,
    ForkSnapshotNotFound,
    RunSnapshotService,
    SemanticForkService,
)
from mission_control.contracts.canonical import canonical_digest
from mission_control.contracts.contracts import MissionControlRejected
from mission_control.contracts.runtime_contracts import (
    ForkSeedView,
    MissionForkReceipt,
    MissionForkRequest,
    MissionReconciliationRequest,
    MissionSnapshotRequest,
)
from mission_control.domain.policies.contracts import ActorContext, CommandResult, LifecycleCommand
from mission_control.domain.policies.forks import RunSnapshotManifest


class MissionControlRuntimeService:
    def __init__(
        self,
        snapshot_service: RunSnapshotService,
        fork_service: SemanticForkService,
        *,
        request_scope: str,
        reconciliation_service: UnitReconciliationService | None = None,
        seeds: ForkSeedService | None = None,
    ) -> None:
        if not request_scope:
            raise ValueError("an authenticated application/tenant request scope is required")
        self._snapshots = snapshot_service
        self._forks = fork_service
        self._reconciliation = reconciliation_service
        self._scope = request_scope
        # FT-F4: the forked Run's own mailbox receives the Snapshot restore and the
        # instruction; without it a fork admits the Run only (the RRM-001 behavior).
        self._seeds = seeds

    @property
    def request_scope(self) -> str:
        return self._scope

    async def snapshot(
        self, run_id: str, request: MissionSnapshotRequest, actor: ActorContext
    ) -> RunSnapshotManifest:
        self._authorize(actor, SNAPSHOT_PERMISSION)
        request = MissionSnapshotRequest.model_validate(request.model_dump(mode="python"))
        return await self._snapshots.take(
            self._scope, run_id, expected_run_version=request.expected_version
        )

    async def get_snapshot(
        self, run_id: str, snapshot_id: str, actor: ActorContext
    ) -> RunSnapshotManifest:
        self._authorize(actor, SNAPSHOT_PERMISSION)
        snapshot = await self._snapshots.get(self._scope, snapshot_id)
        if snapshot.source_run_id != run_id:
            raise ForkSnapshotNotFound("run snapshot not found")
        return snapshot

    async def fork(
        self,
        run_id: str,
        request: MissionForkRequest,
        actor: ActorContext,
        *,
        sponsorship_refs: frozenset[str],
        approval_refs: frozenset[str],
    ) -> MissionForkReceipt:
        self._authorize(actor, FORK_PERMISSION, ADMIT_PERMISSION)
        request = MissionForkRequest.model_validate(request.model_dump(mode="python"))
        if request.sponsorship_ref not in sponsorship_refs:
            raise MissionControlRejected("unauthorized", "sponsorship was not granted")
        if not set(request.approval_refs) <= approval_refs:
            raise MissionControlRejected("unauthorized", "approval was not granted")
        if request.instruction is not None:
            if self._seeds is None:
                raise MissionControlRejected(
                    "unsupported_control", "a fork instruction requires a command mailbox"
                )
            self._authorize(actor, "workflow_run.control")
            try:  # Refuse oversized or mismatched content before anything is admitted.
                mailbox_command_action(
                    "queue_instruction", request.instruction, boundary="next_turn", generation=1
                )
            except MailboxContentRejected as error:
                raise MissionControlRejected(error.code, str(error)) from None
        # Existing fork storage keys on scope/request_id. Include actor and action so
        # distinct authenticated principals cannot collide on a caller-chosen UUID.
        scoped_id = str(
            uuid5(
                NAMESPACE_URL,
                json.dumps(
                    ["mc.runtime_fork.v1", self._scope, actor.actor_id, str(request.request_id)],
                    separators=(",", ":"),
                ),
            )
        )
        snapshot = await self._fork_snapshot(run_id, scoped_id, request)
        receipt = await self._forks.fork(
            ForkCommand(
                request_scope=self._scope,
                source_run_id=run_id,
                request_id=scoped_id,
                idempotency_key=scoped_id,
                snapshot_id=snapshot.snapshot_id,
                snapshot_digest=request.snapshot_digest or snapshot.snapshot_digest,
                changes=request.changes,
                invalidation_frontier=request.invalidation_frontier,
                cognitive_seed=request.cognitive_seed,
                baseline_reservations=request.baseline_reservations,
                sponsorship_ref=request.sponsorship_ref,
                approval_refs=request.approval_refs,
                actor=actor,
                reason=request.reason,
                requested_at=datetime.now(UTC),
            )
        )
        if self._seeds is None:
            return MissionForkReceipt(request_id=request.request_id, receipt=receipt)
        try:
            seed = await self._seeds.seed(
                receipt,
                snapshot,
                actor=actor,
                instruction=request.instruction,
                reason=request.reason,
            )
        except ForkSeedRejected as error:
            raise MissionControlRejected(error.code, str(error)) from None
        return MissionForkReceipt(
            request_id=request.request_id,
            receipt=receipt,
            seed=ForkSeedView(
                derived_run_id=seed.derived_run_id,
                snapshot_id=snapshot.snapshot_id,
                workspace_command_id=seed.workspace_command_id,
                instruction_command_id=seed.instruction_command_id,
            ),
        )

    async def _fork_snapshot(
        self, run_id: str, scoped_id: str, request: MissionForkRequest
    ) -> RunSnapshotManifest:
        """The Snapshot a fork starts from: the named one, the one an earlier attempt of the
        same request reserved (a retry never drifts to a newer Snapshot), or the latest safe
        Snapshot of the run (taken now when none was sealed; `CHECKPOINT_INVALID` if the run
        has no safe boundary)."""

        if request.snapshot_id is not None:
            snapshot = await self._snapshots.get(self._scope, request.snapshot_id)
            if snapshot.source_run_id != run_id:
                raise ForkSnapshotNotFound("run snapshot not found")
            return snapshot
        persisted = await self._forks.persisted_request(self._scope, scoped_id)
        if persisted is not None:
            return await self._snapshots.get(self._scope, persisted.snapshot_id)
        return await self._snapshots.latest_or_take(self._scope, run_id)

    async def reconcile(
        self, run_id: str, request: MissionReconciliationRequest, actor: ActorContext
    ) -> CommandResult:
        self._authorize(actor, "workflow_run.reconcile_unit")
        request = MissionReconciliationRequest.model_validate(request.model_dump(mode="python"))
        if self._reconciliation is None:
            raise MissionControlRejected(
                "unavailable", "application unit reconciliation is not configured"
            )
        command_id = str(request.request_id)
        return await self._reconciliation.reconcile_unit(
            LifecycleCommand(
                command_id=command_id,
                idempotency_issuer=json.dumps(
                    ["mc.unit_reconciliation.v1", actor.actor_id], separators=(",", ":")
                ),
                request_scope=self._scope,
                run_id=run_id,
                expected_run_version=request.expected_version,
                actor=actor,
                action=request.action,
                reason=request.reason,
                evidence_refs=(f"mc-request:{canonical_digest(request)}",),
                occurred_at=datetime.now(UTC),
                correlation_id=command_id,
            )
        )

    @staticmethod
    def _authorize(actor: ActorContext, *permissions: str) -> None:
        if not set(permissions) <= actor.permissions:
            raise MissionControlRejected("unauthorized", "actor lacks runtime control permission")
