"""Per-lane hydrator registry (MP-12): which lane hydrates a fresh session, and how.

The continuation activities resolve the lane's :class:`SessionHydrator` and its
:class:`WorkspaceSnapshotPort` through one registry keyed by lane profile. The Cursor
hydrators (``adapters/cursor/controls.py``) and the Deep Agents hydrator
(``adapters/deep_agents/compaction.py``) implement the protocol today; the Claude and Codex
lanes (MP-07/08) register theirs here. A profile without a registration is refused with the
typed ``unsupported_control`` rejection: no lane is ever hydrated by another lane's code.

Registrations are factories over the request scope, so a hydrator can bind scoped
repositories without the registry holding tenant state.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from mission_control.application.context.continuation import (
    ContinuationRejected,
    SessionHydrator,
    WorkspaceSnapshotPort,
)

HydratorFactory = Callable[[str], SessionHydrator]
SnapshotFactory = Callable[[str], WorkspaceSnapshotPort]


@dataclass(frozen=True)
class LaneContinuationRegistration:
    """One lane's continuation support: hydrator, optional snapshot port, qualification."""

    lane_profile: str
    hydrator: HydratorFactory
    snapshots: SnapshotFactory | None = None
    qualified: bool = False
    """Whether the lane's continuation (seal, hydrate, first turn) was live-qualified.
    Registering a hydrator never flips this; only recorded evidence does."""


class LaneHydratorRegistry:
    """Resolves the hydrator (and snapshot port) of a lane profile; raises for unknown ones.

    It satisfies the activities' ``LaneHydrators`` protocol (``registry(lane, scope)``).
    """

    def __init__(
        self, registrations: Mapping[str, LaneContinuationRegistration] | None = None
    ) -> None:
        self._registrations: dict[str, LaneContinuationRegistration] = dict(registrations or {})

    def register(self, registration: LaneContinuationRegistration) -> None:
        if registration.lane_profile in self._registrations:
            raise ValueError(
                f"lane profile {registration.lane_profile} already has a continuation hydrator"
            )
        self._registrations[registration.lane_profile] = registration

    def profiles(self) -> tuple[str, ...]:
        return tuple(sorted(self._registrations))

    def registration(self, lane_profile: str) -> LaneContinuationRegistration:
        try:
            return self._registrations[lane_profile]
        except KeyError:
            raise ContinuationRejected(
                "unsupported_control",
                f"lane profile {lane_profile} has no registered continuation hydrator",
            ) from None

    def hydrator(self, lane_profile: str, request_scope: str) -> SessionHydrator:
        return self.registration(lane_profile).hydrator(request_scope)

    def snapshots(self, lane_profile: str, request_scope: str) -> WorkspaceSnapshotPort | None:
        factory = self.registration(lane_profile).snapshots
        return None if factory is None else factory(request_scope)

    def __call__(self, lane_profile: str, request_scope: str) -> SessionHydrator:
        return self.hydrator(lane_profile, request_scope)


__all__ = [
    "HydratorFactory",
    "LaneContinuationRegistration",
    "LaneHydratorRegistry",
    "SnapshotFactory",
]
