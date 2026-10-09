"""Chain outbox relay (SPEC-04 "Release through the outbox", ADR-0004, ADR-0031).

The chain reducer never calls Temporal: it writes ``mc.chain.start_run`` (and, for
``cancel_downstream``, ``mc.chain.cancel_run``) outbox intents in the ledger transaction that
released the link. :class:`ChainIntentRelay` leases due intents, delivers each through a port
and acknowledges it only after the delivery returned, so a crash between the two re-delivers.
Starting is idempotent: the production starter goes through the ordinary launch path, whose
submitter starts the run-derived root workflow id with ``USE_EXISTING`` and
``REJECT_DUPLICATE`` (a second delivery attaches to the workflow the first one started).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from mission_control.application.execution.run_launch import (
    RunLaunchRejected,
    RunLaunchRequest,
    RunLaunchService,
)
from mission_control.domain.composition.chain import ChainFamily
from mission_control.domain.policies.contracts import ActorContext

_LOG = logging.getLogger(__name__)

START_RUN_DESTINATION = "mc.chain.start_run"
CANCEL_RUN_DESTINATION = "mc.chain.cancel_run"
CHAIN_INTENT_SCHEMA: Final = "mc.chain_intent.v1"
CHAIN_LAUNCH_PERMISSION = "workflow_run.start"
MAX_DELIVERY_ATTEMPTS = 12


class ChainIntent(BaseModel):
    """One relay intent written by the chain reducer (``mc.chain_intent.v1``)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["mc.chain_intent.v1"] = CHAIN_INTENT_SCHEMA
    intent_kind: Literal["start_run", "cancel_run"]
    delivery_key: str = Field(min_length=1)
    request_scope: str = Field(min_length=1)
    chain_id: UUID
    mission_id: UUID
    run_id: UUID
    """``mission_run.run_id`` of the consumer."""
    run_key: str = Field(min_length=1)
    """The run-control identity the launch and commands address."""
    family: ChainFamily | None = None
    initial_goal: str | None = None
    link_ids: tuple[UUID, ...] = ()
    actor_ref: str = Field(min_length=1)
    """``chain:<chain_id>``: the launch is attributed to the chain, authorized at submit."""
    reason: str | None = None
    attempts: int = Field(default=0, ge=0)


@dataclass(frozen=True, slots=True)
class ChainStartReceipt:
    workflow_id: str
    temporal_run_id: str | None
    already_started: bool = False


class ChainIntentStore(Protocol):
    async def lease(
        self,
        request_scope: str,
        *,
        lease_owner: str,
        lease_until: datetime,
        now: datetime,
        limit: int,
    ) -> tuple[ChainIntent, ...]: ...

    async def mark_delivered(
        self, request_scope: str, delivery_key: str, *, lease_owner: str, delivered_at: datetime
    ) -> None: ...

    async def mark_failed(
        self,
        request_scope: str,
        delivery_key: str,
        *,
        lease_owner: str,
        retry_at: datetime,
        dead: bool,
    ) -> None: ...


class ChainRunStarter(Protocol):
    async def start(self, intent: ChainIntent) -> ChainStartReceipt: ...


class ChainRunCanceller(Protocol):
    async def cancel(self, intent: ChainIntent) -> None: ...


@dataclass(frozen=True, slots=True)
class ChainRelayReport:
    delivered: tuple[str, ...] = ()
    failed: tuple[str, ...] = ()
    dead: tuple[str, ...] = ()
    receipts: dict[str, ChainStartReceipt] = field(default_factory=dict)


class ChainIntentRelay:
    """At-least-once delivery of chain intents; acknowledgement follows delivery."""

    def __init__(
        self,
        *,
        store: ChainIntentStore,
        starter: ChainRunStarter,
        canceller: ChainRunCanceller | None = None,
        lease_owner: str = "chain-relay",
        lease_seconds: int = 60,
        base_backoff_seconds: float = 2.0,
        max_backoff_seconds: float = 900.0,
    ) -> None:
        self._store = store
        self._starter = starter
        self._canceller = canceller
        self._owner = lease_owner
        self._lease = timedelta(seconds=lease_seconds)
        self._base = base_backoff_seconds
        self._cap = max_backoff_seconds

    async def relay_once(
        self, request_scope: str, *, now: datetime, limit: int = 20
    ) -> ChainRelayReport:
        intents = await self._store.lease(
            request_scope,
            lease_owner=self._owner,
            lease_until=now + self._lease,
            now=now,
            limit=limit,
        )
        delivered: list[str] = []
        failed: list[str] = []
        dead: list[str] = []
        receipts: dict[str, ChainStartReceipt] = {}
        for intent in intents:
            try:
                if intent.intent_kind == "start_run":
                    receipts[intent.delivery_key] = await self._starter.start(intent)
                else:
                    if self._canceller is None:
                        raise RuntimeError("no chain run canceller is composed")
                    await self._canceller.cancel(intent)
            except Exception as error:  # delivery failures are retried, never dropped
                attempts = intent.attempts + 1
                is_dead = attempts >= MAX_DELIVERY_ATTEMPTS
                _LOG.warning(
                    "chain intent %s delivery failed (attempt %s): %s",
                    intent.delivery_key,
                    attempts,
                    error,
                )
                delay = min(self._cap, self._base * (2 ** min(attempts, 16)))
                await self._store.mark_failed(
                    request_scope,
                    intent.delivery_key,
                    lease_owner=self._owner,
                    retry_at=now + timedelta(seconds=delay),
                    dead=is_dead,
                )
                (dead if is_dead else failed).append(intent.delivery_key)
                continue
            await self._store.mark_delivered(
                request_scope, intent.delivery_key, lease_owner=self._owner, delivered_at=now
            )
            delivered.append(intent.delivery_key)
        return ChainRelayReport(
            delivered=tuple(delivered),
            failed=tuple(failed),
            dead=tuple(dead),
            receipts=receipts,
        )


class ChainRelayPump:
    """The relay as a bounded, cancellable process loop (MP-02: the worker runs it).

    Each pass leases at most ``limit`` due intents per tenant scope (row-level security binds
    one tenant per transaction); a scope whose pass fails is logged and retried on the next
    pass. The pump only delivers intents through the governed launch: Temporal stays the sole
    scheduler of the runs it starts.
    """

    def __init__(
        self,
        relay: ChainIntentRelay,
        request_scopes: Sequence[str],
        *,
        interval_seconds: float,
        limit: int = 20,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if interval_seconds <= 0 or limit < 1:
            raise ValueError("the chain relay pump needs a positive interval and batch limit")
        self._relay = relay
        self._scopes = tuple(request_scopes)
        self._interval = interval_seconds
        self._limit = limit
        self._clock = clock or (lambda: datetime.now(UTC))

    async def run_once(self) -> tuple[ChainRelayReport, ...]:
        reports: list[ChainRelayReport] = []
        for scope in self._scopes:
            try:
                reports.append(
                    await self._relay.relay_once(scope, now=self._clock(), limit=self._limit)
                )
            except Exception:  # a scope's outage never stops the other scopes or the worker
                _LOG.exception("chain relay pass failed for one tenant scope")
        return tuple(reports)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self.run_once()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=self._interval)


class ChainLaunchInputPort(Protocol):
    """The admitted consumer's family input (``StageGraphRunInput``/``GoalDirectedRunInput``
    as a mapping), prepared from the admitted run and its frozen semantic binding."""

    async def family_input(self, intent: ChainIntent) -> dict[str, Any]: ...


def chain_actor(intent: ChainIntent) -> ActorContext:
    return ActorContext(
        actor_id=intent.actor_ref,
        authority_refs=frozenset({f"authority:{intent.actor_ref}"}),
        permissions=frozenset({CHAIN_LAUNCH_PERMISSION}),
    )


class LaunchServiceChainStarter:
    """Start a released consumer through the ordinary governed launch (``RunLaunchService``).

    A re-delivery after the run already left ``pending`` is the idempotent case: the first
    delivery started it, so the intent is acknowledged as already started.
    """

    def __init__(self, *, inputs: ChainLaunchInputPort, launches: RunLaunchService) -> None:
        self._inputs = inputs
        self._launches = launches

    async def start(self, intent: ChainIntent) -> ChainStartReceipt:
        if intent.family is None:
            raise ValueError("a start_run intent names the consumer's family")
        payload = await self._inputs.family_input(intent)
        try:
            receipt = await self._launches.launch(
                RunLaunchRequest(
                    request_scope=intent.request_scope,
                    run_id=intent.run_key,
                    family=intent.family,
                    stagegraph=payload if intent.family == "StageGraph" else None,
                    goal_directed=payload if intent.family == "GoalDirected" else None,
                    mission_id=str(intent.mission_id),
                ),
                chain_actor(intent),
            )
        except RunLaunchRejected as error:
            if error.code == "run_not_pending":
                return ChainStartReceipt(workflow_id="", temporal_run_id=None, already_started=True)
            raise
        return ChainStartReceipt(
            workflow_id=receipt.workflow_id, temporal_run_id=receipt.temporal_run_id
        )
