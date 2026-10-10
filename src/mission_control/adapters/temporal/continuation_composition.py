"""Production composition of Session Lane continuation (MP-12, SPEC-01, ADR-0039).

One worker builds one continuation stack over the common component: the persisted phase
machine (`ContinuationPhaseService`), the per-lane hydrators and snapshot ports
(`LaneHydratorRegistry`), the context-pressure coordinator `lane.turn` consults at a turn's
terminal frame, and the mailbox hold oracle that keeps held commands queued while a transfer
fences its source. The `continuation.*` activities are served on the cognitive queue beside
`lane.turn` (`OperationExecutionActivities(continuation=...)`), where the operation workflow
drives the phases behind `mp12-operation-continuation`.

Only lanes that register here can be continued. The service's own snapshot port refuses: a
lane without a registered snapshot port cannot be frozen, so a transfer for it fails typed
instead of inventing a workspace. Registration never marks a lane's continuation qualified.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

import asyncpg

from mission_control.adapters.postgres.context.continuation_repository import (
    PostgresCheckpointRepository,
    PostgresContinuationRepository,
    PostgresRunIds,
)
from mission_control.adapters.postgres.context.selection_repository import (
    PostgresContextSelectionRepository,
)
from mission_control.adapters.postgres.frames.repository import PostgresFrameRepository
from mission_control.adapters.postgres.run_control.mailbox import PostgresCommandMailbox
from mission_control.adapters.temporal.activities.continuation import ContinuationActivities
from mission_control.application.context.continuation import (
    ContinuationRejected,
    ContinuationService,
    FrameHydrationConfirmation,
    RunControlContinuationEvents,
    WorkspaceSnapshot,
)
from mission_control.application.context.facts import OperationFactsCapture
from mission_control.application.context.hydrators import (
    LaneContinuationRegistration,
    LaneHydratorRegistry,
)
from mission_control.application.context.lane_continuation import (
    ContinuationHoldOracle,
    LaneContinuationCoordinator,
    LaneStateActivation,
)
from mission_control.application.context.pack_service import ContextPackService
from mission_control.application.context.phases import ContinuationPhaseService
from mission_control.application.execution.harness.state import LaneExecutionStateStore
from mission_control.application.execution.mailbox import ContinuationMailboxHolds
from mission_control.application.execution.service import RunControlService
from mission_control.application.programs.service import orchestration_lifecycle_actor
from mission_control.domain.context.checkpoint import ContinuationGovernorPolicy
from mission_control.domain.context.pressure import ContextPressurePolicy
from mission_control.domain.policies.contracts import ActorContext

UNREGISTERED_SNAPSHOT = "continuation_snapshot_unregistered"


class UnregisteredLaneSnapshots:
    """The service-level snapshot port: every Session Lane snapshot comes from its own lane
    registration, so reaching this port means the lane cannot be continued."""

    async def snapshot(
        self, *, request_scope: str, run_key: str, session_ref: str, roots: Sequence[str]
    ) -> WorkspaceSnapshot:
        raise ContinuationRejected(
            UNREGISTERED_SNAPSHOT,
            f"run {run_key}: no lane registered a workspace snapshot port for this session",
        )

    async def load(self, *, request_scope: str, snapshot_ref: str) -> WorkspaceSnapshot | None:
        return None


def continuation_worker_actor() -> ActorContext:
    """The worker's lifecycle identity plus the continuation permissions it records with."""

    actor = orchestration_lifecycle_actor()
    return actor.model_copy(
        update={
            "permissions": actor.permissions
            | {
                "workflow_run.record_continuation",
                "workflow_run.request_continuation",
                "workflow_run.read",
            }
        }
    )


@dataclass(frozen=True)
class ContinuationSettings:
    """The worker's context-pressure policy and continuation governors (from `Settings`)."""

    soft_context_ratio: Decimal = Decimal("0.70")
    hard_context_ratio: Decimal = Decimal("0.85")
    reserve_ratio: Decimal = Decimal("0.15")
    max_session_turns: int | None = None
    max_transfers: int = 8
    max_compaction_failures: int = 2
    native_compaction: str = "preferred"

    def pressure(self) -> ContextPressurePolicy:
        return ContextPressurePolicy.model_validate(
            {
                "soft_context_ratio": self.soft_context_ratio,
                "hard_context_ratio": self.hard_context_ratio,
                "reserve_ratio": self.reserve_ratio,
                "max_session_turns": self.max_session_turns,
                "max_transfers": self.max_transfers,
                "max_compaction_failures": self.max_compaction_failures,
                "native_compaction": self.native_compaction,
            }
        )

    def governors(self) -> ContinuationGovernorPolicy:
        return ContinuationGovernorPolicy(
            max_transfers=self.max_transfers,
            max_failed_compactions=self.max_compaction_failures,
        )


@dataclass(frozen=True)
class LaneContinuationComposition:
    coordinator: LaneContinuationCoordinator
    activities: ContinuationActivities
    holds: ContinuationHoldOracle
    registry: LaneHydratorRegistry


def compose_lane_continuation(
    pool: asyncpg.Pool,
    run_control: RunControlService,
    *,
    packets: ContextPackService,
    states: LaneExecutionStateStore,
    frames: PostgresFrameRepository,
    registrations: Sequence[LaneContinuationRegistration],
    settings: ContinuationSettings | None = None,
) -> LaneContinuationComposition:
    """The continuation stack of one worker (see the module doc)."""

    policy = settings or ContinuationSettings()
    transfers = PostgresContinuationRepository(pool)
    checkpoints = PostgresCheckpointRepository(pool)
    selections = PostgresContextSelectionRepository(pool)
    registry = LaneHydratorRegistry(
        {registration.lane_profile: registration for registration in registrations}
    )
    actor = continuation_worker_actor()
    service = ContinuationService(
        transfers=transfers,
        checkpoints=checkpoints,
        packets=packets,
        packet_reader=selections,
        snapshots=UnregisteredLaneSnapshots(),
        events=RunControlContinuationEvents(run_control, actor=actor),
        mailbox=ContinuationMailboxHolds(PostgresCommandMailbox(pool)),
        confirmation=FrameHydrationConfirmation(frames, PostgresRunIds(pool)),
        governors=policy.governors(),
    )
    coordinator = LaneContinuationCoordinator(
        transfers,
        policy=policy.pressure(),
        triggers=service,
        checkpoints=checkpoints,
        packets=selections,
    )
    phases = ContinuationPhaseService(
        service,
        lanes=registry,
        facts=OperationFactsCapture(),
        activation=LaneStateActivation(states),
    )
    activities = ContinuationActivities(
        lambda _scope: service,
        registry,
        phases=lambda _scope: phases,
        coordinators=lambda _scope: coordinator,
        states=states,
        frames=frames,
    )
    return LaneContinuationComposition(
        coordinator=coordinator,
        activities=activities,
        holds=ContinuationHoldOracle(transfers),
        registry=registry,
    )


__all__ = [
    "UNREGISTERED_SNAPSHOT",
    "ContinuationSettings",
    "LaneContinuationComposition",
    "UnregisteredLaneSnapshots",
    "compose_lane_continuation",
    "continuation_worker_actor",
]
