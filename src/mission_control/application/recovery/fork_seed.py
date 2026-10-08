"""Seed a forked Run's first Context Packet (FT-F4; SPEC-06 "Fork", SPEC-02 workspace tier).

A fork never copies the source Run's mailbox, children or in-flight Commands
(REQ-CP-EXEC-012). What the derived Run receives is written to its *own* command mailbox,
as ordinary Commands admitted by the Reducer, after the fork saga admitted it:

1. the optional operator instruction, as the derived Run's first `queue_instruction`
   (boundary `next_turn`, attributed to the forking actor), and
2. the Snapshot restore, as a kernel-issued `add_context` with `expand: workspace`, which the
   first boundary seals into a `fork`-purpose packet as its single `workspace` item.

Both carry deterministic command ids derived from the fork request, so a retried fork
re-reads them instead of writing twice. Nothing is launched: `run start` stays separate.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5

from mission_control.application.execution.mailbox import mailbox_command_action
from mission_control.application.execution.service import RunControlService
from mission_control.contracts.contracts import ContentRef, InlineText
from mission_control.domain.policies.contracts import (
    ActorContext,
    AddContextAction,
    CommandResult,
    CommandStatus,
    LifecycleCommand,
    MailboxCommandAction,
)
from mission_control.domain.policies.forks import RunForkReceipt, RunSnapshotManifest

FORK_SEED_ACTOR_ID = "mission-control-fork-seed"
FORK_SEED_ACTOR = ActorContext(
    actor_id=FORK_SEED_ACTOR_ID, permissions=frozenset({"workflow_run.control"})
)
SNAPSHOT_MEDIA_TYPE = "application/x-mission-control-run-snapshot"
CONTROL_PERMISSION = "workflow_run.control"


class ForkSeedRejected(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class ForkSeed:
    """The derived Run's seeded Commands (their ids; receipts are in its command ledger)."""

    derived_run_id: str
    workspace_command_id: str
    instruction_command_id: str | None = None


def seed_command_id(request_scope: str, fork_request_id: str, part: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"mc.fork_seed.v1|{request_scope}|{fork_request_id}|{part}"))


def snapshot_restore_ref(snapshot: RunSnapshotManifest) -> str:
    """The lane-restorable Snapshot: its sandbox snapshot when one was captured, else the run
    snapshot itself (the materializer resolves either)."""

    return (
        snapshot.sandbox_snapshot_refs[0]
        if snapshot.sandbox_snapshot_refs
        else (f"run-snapshot://{snapshot.snapshot_id}")
    )


class ForkSeedService:
    def __init__(
        self,
        run_control: RunControlService,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._run_control = run_control
        self._clock = clock

    async def seed(
        self,
        receipt: RunForkReceipt,
        snapshot: RunSnapshotManifest,
        *,
        actor: ActorContext,
        instruction: ContentRef | InlineText | None,
        reason: str,
    ) -> ForkSeed:
        scope, run_id = receipt.request_scope, receipt.target_run_id
        instruction_id: str | None = None
        if instruction is not None:
            if CONTROL_PERMISSION not in actor.permissions:
                raise ForkSeedRejected(
                    "unauthorized", "a fork instruction needs workflow_run.control"
                )
            instruction_id = seed_command_id(scope, receipt.request_id, "instruction")
            content = instruction
            await self._admit(
                scope,
                run_id,
                command_id=instruction_id,
                issuer=json.dumps(["mc.fork_seed.v1", actor.actor_id], separators=(",", ":")),
                actor=actor,
                build=lambda generation: mailbox_command_action(
                    "queue_instruction", content, boundary="next_turn", generation=generation
                ),
                reason=reason,
                fork_request_id=receipt.request_id,
            )
        workspace_id = seed_command_id(scope, receipt.request_id, "workspace")
        restore = snapshot_restore_ref(snapshot)

        def workspace(generation: int) -> tuple[AddContextAction, None]:
            return (
                AddContextAction(
                    boundary="next_turn",
                    generation=generation,
                    content_ref=restore,
                    content_digest=snapshot.snapshot_digest,
                    media_type=SNAPSHOT_MEDIA_TYPE,
                    content_bytes=0,
                    expand="workspace",
                ),
                None,
            )

        await self._admit(
            scope,
            run_id,
            command_id=workspace_id,
            issuer=json.dumps(["mc.fork_seed.v1", FORK_SEED_ACTOR_ID], separators=(",", ":")),
            actor=FORK_SEED_ACTOR,
            build=workspace,
            reason=f"fork workspace restore from {snapshot.snapshot_id}"[:4096],
            fork_request_id=receipt.request_id,
        )
        return ForkSeed(
            derived_run_id=run_id,
            workspace_command_id=workspace_id,
            instruction_command_id=instruction_id,
        )

    async def _admit(
        self,
        request_scope: str,
        run_id: str,
        *,
        command_id: str,
        issuer: str,
        actor: ActorContext,
        build: Callable[[int], tuple[MailboxCommandAction, str | None]],
        reason: str,
        fork_request_id: str,
    ) -> CommandResult:
        for _attempt in range(4):
            prior = await self._run_control.get_command_result(
                request_scope, run_id, issuer, command_id
            )
            if prior is not None:
                if prior.status != CommandStatus.ACCEPTED:
                    raise ForkSeedRejected(
                        prior.reason_code, f"the forked run refused its seed: {prior.reason}"
                    )
                return prior
            projection = await self._run_control.get_run(request_scope, run_id)
            generation = (
                projection.execution_target.execution_generation
                if projection.execution_target is not None
                else 1
            )
            action, text = build(generation)
            result = await self._run_control.execute(
                LifecycleCommand(
                    command_id=command_id,
                    idempotency_issuer=issuer,
                    request_scope=request_scope,
                    run_id=run_id,
                    expected_run_version=projection.version,
                    actor=actor,
                    action=action,
                    reason=reason,
                    evidence_refs=(f"fork-request:{fork_request_id}",),
                    occurred_at=self._clock(),
                    correlation_id=f"fork:{fork_request_id}",
                ),
                mailbox_text=text,
            )
            if result.status == CommandStatus.ACCEPTED:
                return result
            if result.status == CommandStatus.REJECTED:
                raise ForkSeedRejected(
                    result.reason_code, f"the forked run refused its seed: {result.reason}"
                )
        raise ForkSeedRejected("stale_version", "the forked run kept moving while it was seeded")


__all__ = [
    "FORK_SEED_ACTOR",
    "ForkSeed",
    "ForkSeedRejected",
    "ForkSeedService",
    "seed_command_id",
    "snapshot_restore_ref",
]
