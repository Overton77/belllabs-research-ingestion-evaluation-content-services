"""The worker-owned session manager (SPEC-01 "Session ownership and crash recovery"; MP-06).

One manager per worker process. It names the process (`owner_ref`: worker identity plus a
per-process instance, so a restarted process is a new owner), claims the fenced ownership of
each harness execution it drives, and keeps the live session handles between `lane.turn`
activities: ending an observation segment retains the session; only settlement releases it.
Activity cancellation is not provider cancellation; explicit lane cancel owns that.

Routing: a local session (`worker_hosted` placement) is reachable only from the process that
owns it. Control delivered to another worker while the owner's lease is live is rejected
(`SessionOwnedElsewhere`, retried by Temporal); after the lease expires the new worker takes
over with a higher epoch, which fences the old owner's journal writes and settlement but
cannot undo a tool the old process already started. Delivering control to the owner's own
task queue is a bootstrap delta (MP-06 handoff); this manager does not schedule anything.

The manager lives in the worker process; its restart semantics are explicit: a process
restart loses every retained local session (the persisted dispatch journal is the truth),
while a provider-hosted run continues and is reattached by native identity.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import UUID, uuid4

from mission_control.application.execution.harness.dispatch import (
    SessionOwnedElsewhere,
    SessionOwner,
)
from mission_control.application.execution.harness.state import LaneExecutionStateStore
from mission_control.domain.execution.lanes import SessionHandle, TurnHandle

LOCAL_PLACEMENT = "worker_hosted"


def default_owner_ref(worker_identity: str | None = None) -> str:
    base = worker_identity or f"{socket.gethostname()}:{os.getpid()}"
    return f"{base}#{uuid4().hex[:12]}"


@dataclass
class LiveSession:
    """A session this process keeps between segments (never a credential)."""

    owner: SessionOwner
    session: SessionHandle | None = None
    turn: TurnHandle | None = None
    segments: int = 0


SessionKey = tuple[str, UUID]


class WorkerSessionManager:
    def __init__(
        self,
        *,
        owner_ref: str | None = None,
        min_lease: timedelta = timedelta(seconds=10),
        lease_heartbeats: int = 2,
    ) -> None:
        if lease_heartbeats < 1:
            raise ValueError("a session lease spans at least one heartbeat timeout")
        self.owner_ref = owner_ref or default_owner_ref()
        self._min_lease = min_lease
        self._lease_heartbeats = lease_heartbeats
        self._live: dict[SessionKey, LiveSession] = {}

    def lease_for(self, heartbeat_timeout_s: int) -> timedelta:
        """`lease_heartbeats` (default two) heartbeat timeouts: a lost owner is detected by
        Temporal first, and the retry that follows waits at most the remaining heartbeats for
        the lease to expire (`MISSION_CONTROL_SESSION_LEASE_MIN_S` / `_HEARTBEATS`)."""

        return max(self._min_lease, timedelta(seconds=self._lease_heartbeats * heartbeat_timeout_s))

    async def claim(
        self,
        states: LaneExecutionStateStore,
        request_scope: str,
        harness_execution_id: UUID,
        *,
        generation: int,
        now: datetime,
        heartbeat_timeout_s: int,
    ) -> SessionOwner:
        owner = await states.claim_owner(
            request_scope,
            harness_execution_id,
            owner_ref=self.owner_ref,
            generation=generation,
            now=now,
            lease=self.lease_for(heartbeat_timeout_s),
        )
        key = (request_scope, harness_execution_id)
        live = self._live.get(key)
        if live is None or not live.owner.same_holder(owner):
            # A takeover (or a first claim) starts without any handle of an earlier owner.
            self._live[key] = LiveSession(owner=owner)
        else:
            live.owner = owner
        return owner

    def renewed(self, request_scope: str, harness_execution_id: UUID, owner: SessionOwner) -> None:
        live = self._live.get((request_scope, harness_execution_id))
        if live is not None and live.owner.same_holder(owner):
            live.owner = owner

    def retain(
        self,
        request_scope: str,
        harness_execution_id: UUID,
        owner: SessionOwner,
        *,
        session: SessionHandle | None,
        turn: TurnHandle | None,
    ) -> None:
        """A segment ended with the turn still running: keep the session alive here."""

        live = self._live.setdefault((request_scope, harness_execution_id), LiveSession(owner))
        live.owner, live.session, live.turn = owner, session, turn
        live.segments += 1

    def live(self, request_scope: str, harness_execution_id: UUID) -> LiveSession | None:
        return self._live.get((request_scope, harness_execution_id))

    def release(self, request_scope: str, harness_execution_id: UUID) -> None:
        self._live.pop((request_scope, harness_execution_id), None)

    def check_control(self, owner: SessionOwner | None, *, placement: str, now: datetime) -> None:
        """Reject control for a local session another live owner holds (wrong worker)."""

        if (
            owner is not None
            and placement == LOCAL_PLACEMENT
            and owner.owner_ref != self.owner_ref
            and not owner.expired(now)
        ):
            raise SessionOwnedElsewhere(owner.owner_ref, owner.lease_expires_at)


__all__ = [
    "LOCAL_PLACEMENT",
    "LiveSession",
    "WorkerSessionManager",
    "default_owner_ref",
]
