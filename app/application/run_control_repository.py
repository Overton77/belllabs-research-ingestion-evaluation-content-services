from __future__ import annotations

import asyncio
from collections.abc import Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from app.domain.run_control.budget import roll_up_child_budget
from app.domain.run_control.contracts import (
    AdmissionDecision,
    BudgetLedgerEntry,
    BudgetState,
    CommandResult,
    CommandStatus,
    ConsumerApplyResult,
    ConsumerApplyStatus,
    ConsumerCursor,
    DomainEventEnvelope,
    LifecycleTransitionRecord,
    OutboxCursor,
    OutboxRecord,
    RunProjection,
)
from app.domain.run_control.errors import (
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)


@dataclass(frozen=True)
class AdmissionMutation:
    decision: AdmissionDecision
    projection: RunProjection | None = None
    budget: BudgetState | None = None
    transition: LifecycleTransitionRecord | None = None
    ledger_entries: tuple[BudgetLedgerEntry, ...] = ()
    events: tuple[DomainEventEnvelope, ...] = ()


@dataclass(frozen=True)
class CommandMutation:
    result: CommandResult
    request_scope: str
    expected_version: int
    projection: RunProjection | None = None
    budget: BudgetState | None = None
    transition: LifecycleTransitionRecord | None = None
    ledger_entries: tuple[BudgetLedgerEntry, ...] = ()
    events: tuple[DomainEventEnvelope, ...] = ()


class RunControlRepository(Protocol):
    async def get_admission_decision(
        self, request_scope: str, idempotency_issuer: str, request_id: str
    ) -> AdmissionDecision | None: ...

    async def commit_admission(self, mutation: AdmissionMutation) -> AdmissionDecision: ...

    async def get_command_result(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> CommandResult | None: ...

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection: ...

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState: ...

    async def commit_command(self, mutation: CommandMutation) -> CommandResult: ...

    async def list_transitions(
        self, request_scope: str, run_id: str
    ) -> tuple[LifecycleTransitionRecord, ...]: ...

    async def list_budget_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[BudgetLedgerEntry, ...]: ...

    async def list_outbox(
        self,
        request_scope: str,
        *,
        after: OutboxCursor | None = None,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]: ...

    async def mark_outbox_delivered(
        self, request_scope: str, event_id: str, delivered_at: datetime
    ) -> None: ...

    async def apply_consumer_event(
        self, request_scope: str, consumer_id: str, envelope: DomainEventEnvelope
    ) -> ConsumerApplyResult: ...


class InMemoryRunControlRepository:
    """Behavioral test adapter with the same atomic boundaries as PostgreSQL."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._admissions: dict[tuple[str, str, str], AdmissionDecision] = {}
        self._commands: dict[tuple[str, str, str], CommandResult] = {}
        self._runs: dict[str, RunProjection] = {}
        self._budgets: dict[str, BudgetState] = {}
        self._transitions: dict[str, list[LifecycleTransitionRecord]] = {}
        self._ledger: dict[str, list[BudgetLedgerEntry]] = {}
        self._outbox: dict[str, OutboxRecord] = {}
        self._next_outbox_position = 1
        self._cursors: dict[tuple[str, str], ConsumerCursor] = {}

    async def get_admission_decision(
        self, request_scope: str, idempotency_issuer: str, request_id: str
    ) -> AdmissionDecision | None:
        decision = self._admissions.get((request_scope, idempotency_issuer, request_id))
        return deepcopy(decision)

    async def commit_admission(self, mutation: AdmissionMutation) -> AdmissionDecision:
        key = (
            mutation.decision.request_scope,
            mutation.decision.idempotency_issuer,
            mutation.decision.request_id,
        )
        async with self._lock:
            prior = self._admissions.get(key)
            if prior is not None:
                if prior.request_fingerprint != mutation.decision.request_fingerprint:
                    raise IdempotencyConflict(
                        "run request identity was reused with a conflicting payload"
                    )
                return deepcopy(prior)
            if mutation.projection is not None:
                self._validate_accepted_admission(mutation)
                if mutation.projection.run_id in self._runs:
                    raise IdempotencyConflict("workflow run identity already exists")
            if mutation.projection is not None:
                assert mutation.budget is not None
                assert mutation.transition is not None
                run_id = mutation.projection.run_id
                self._apply_parent_rollup(
                    None,
                    mutation.budget,
                    idempotency_id=f"admission:{mutation.decision.request_id}",
                    occurred_at=mutation.decision.recorded_at,
                )
                self._runs[run_id] = deepcopy(mutation.projection)
                self._budgets[run_id] = deepcopy(mutation.budget)
                self._transitions[run_id] = [deepcopy(mutation.transition)]
                self._ledger[run_id] = list(deepcopy(mutation.ledger_entries))
                self._insert_events(mutation.events)
            self._admissions[key] = deepcopy(mutation.decision)
            return deepcopy(mutation.decision)

    async def get_command_result(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> CommandResult | None:
        self._require_scope(request_scope, run_id)
        return deepcopy(self._commands.get((run_id, idempotency_issuer, command_id)))

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection:
        self._require_scope(request_scope, run_id)
        try:
            return deepcopy(self._runs[run_id])
        except KeyError as exc:
            raise RunControlNotFound(f"workflow run not found: {run_id}") from exc

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState:
        self._require_scope(request_scope, run_id)
        try:
            return deepcopy(self._budgets[run_id])
        except KeyError as exc:
            raise RunControlNotFound(f"budget account not found for run: {run_id}") from exc

    async def commit_command(self, mutation: CommandMutation) -> CommandResult:
        key = (
            mutation.result.run_id,
            mutation.result.idempotency_issuer,
            mutation.result.command_id,
        )
        async with self._lock:
            prior = self._commands.get(key)
            if prior is not None:
                if prior.command_fingerprint != mutation.result.command_fingerprint:
                    raise IdempotencyConflict(
                        "lifecycle command identity was reused with a conflicting payload"
                    )
                return deepcopy(prior)
            current = self._runs.get(mutation.result.run_id)
            if current is None or current.request_scope != mutation.request_scope:
                raise RunControlNotFound(f"workflow run not found: {mutation.result.run_id}")
            if mutation.projection is None:
                raced = current.version != mutation.expected_version
                result = mutation.result.model_copy(
                    update={
                        "status": (
                            CommandStatus.STALE
                            if raced and mutation.result.status == CommandStatus.REJECTED
                            else mutation.result.status
                        ),
                        "resulting_run_version": current.version,
                        "phase": current.phase,
                        "terminal_outcome": current.terminal_outcome,
                        "reason_code": (
                            "stale_run_version"
                            if raced and mutation.result.status == CommandStatus.REJECTED
                            else mutation.result.reason_code
                        ),
                        "reason": (
                            "run advanced while the rejected command was being decided"
                            if raced and mutation.result.status == CommandStatus.REJECTED
                            else mutation.result.reason
                        ),
                    }
                )
                self._commands[key] = deepcopy(result)
                return deepcopy(result)
            if current.version != mutation.expected_version:
                raise RunVersionConflict(
                    f"expected version {mutation.expected_version}, "
                    f"current version is {current.version}"
                )
            if mutation.projection is not None:
                self._validate_accepted_command(mutation)
                assert mutation.budget is not None
                assert mutation.transition is not None
                self._apply_parent_rollup(
                    self._budgets[mutation.result.run_id],
                    mutation.budget,
                    idempotency_id=f"command:{mutation.result.command_id}",
                    occurred_at=mutation.result.recorded_at,
                )
                self._runs[mutation.result.run_id] = deepcopy(mutation.projection)
                self._budgets[mutation.result.run_id] = deepcopy(mutation.budget)
                self._transitions[mutation.result.run_id].append(deepcopy(mutation.transition))
                self._ledger[mutation.result.run_id].extend(deepcopy(mutation.ledger_entries))
                self._insert_events(mutation.events)
            self._commands[key] = deepcopy(mutation.result)
            return deepcopy(mutation.result)

    async def list_transitions(
        self, request_scope: str, run_id: str
    ) -> tuple[LifecycleTransitionRecord, ...]:
        self._require_scope(request_scope, run_id)
        return tuple(deepcopy(self._transitions[run_id]))

    async def list_budget_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[BudgetLedgerEntry, ...]:
        self._require_scope(request_scope, run_id)
        return tuple(deepcopy(self._ledger[run_id]))

    async def list_outbox(
        self,
        request_scope: str,
        *,
        after: OutboxCursor | None = None,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]:
        records = sorted(
            (
                item
                for item in self._outbox.values()
                if item.delivered_at is None
                and self._runs[item.envelope.aggregate_id].request_scope == request_scope
            ),
            key=lambda item: (item.cursor.position,),
        )
        if after is not None:
            records = [item for item in records if item.cursor.position > after.position]
        return tuple(deepcopy(records[:limit]))

    async def mark_outbox_delivered(
        self, request_scope: str, event_id: str, delivered_at: datetime
    ) -> None:
        async with self._lock:
            try:
                current = self._outbox[event_id]
            except KeyError as exc:
                raise RunControlNotFound(f"outbox event not found: {event_id}") from exc
            self._require_scope(request_scope, current.envelope.aggregate_id)
            self._outbox[event_id] = current.model_copy(
                update={
                    "delivery_attempts": current.delivery_attempts + 1,
                    "delivered_at": delivered_at,
                }
            )

    async def apply_consumer_event(
        self, request_scope: str, consumer_id: str, envelope: DomainEventEnvelope
    ) -> ConsumerApplyResult:
        authoritative = self._outbox.get(envelope.event_id)
        if authoritative is None or authoritative.envelope != envelope:
            raise RunControlNotFound(f"authoritative outbox event not found: {envelope.event_id}")
        self._require_scope(request_scope, envelope.aggregate_id)
        key = (consumer_id, envelope.aggregate_id)
        async with self._lock:
            cursor = self._cursors.get(
                key,
                ConsumerCursor(
                    consumer_id=consumer_id,
                    aggregate_id=envelope.aggregate_id,
                    last_aggregate_version=0,
                ),
            )
            same_version_next_sequence = (
                envelope.aggregate_version == cursor.last_aggregate_version
                and not cursor.last_version_final
                and envelope.sequence == cursor.last_sequence + 1
            )
            next_version_first_sequence = (
                envelope.aggregate_version == cursor.last_aggregate_version + 1
                and (cursor.last_aggregate_version == 0 or cursor.last_version_final)
                and envelope.sequence == 1
            )
            expected = (
                cursor.last_aggregate_version
                if cursor.last_sequence and same_version_next_sequence
                else cursor.last_aggregate_version + 1
            )
            already_applied = envelope.aggregate_version < cursor.last_aggregate_version or (
                envelope.aggregate_version == cursor.last_aggregate_version
                and envelope.sequence <= cursor.last_sequence
            )
            if already_applied:
                status = ConsumerApplyStatus.DUPLICATE
                next_cursor = cursor
            elif not (same_version_next_sequence or next_version_first_sequence):
                status = ConsumerApplyStatus.GAP
                next_cursor = cursor
            else:
                status = ConsumerApplyStatus.APPLIED
                next_cursor = cursor.model_copy(
                    update={
                        "last_aggregate_version": envelope.aggregate_version,
                        "last_sequence": envelope.sequence,
                        "last_version_final": envelope.is_version_final,
                    }
                )
                self._cursors[key] = next_cursor
            return ConsumerApplyResult(
                status=status,
                cursor=deepcopy(next_cursor),
                expected_version=expected,
                observed_version=envelope.aggregate_version,
            )

    def _require_scope(self, request_scope: str, run_id: str) -> None:
        projection = self._runs.get(run_id)
        if projection is None or projection.request_scope != request_scope:
            raise RunControlNotFound(f"workflow run not found: {run_id}")

    def _apply_parent_rollup(
        self,
        prior_child: BudgetState | None,
        child: BudgetState,
        *,
        idempotency_id: str,
        occurred_at: datetime,
    ) -> None:
        if child.parent_account_id is None:
            return
        parent_run_id = next(
            (
                run_id
                for run_id, budget in self._budgets.items()
                if budget.account_id == child.parent_account_id
            ),
            None,
        )
        if parent_run_id is None:
            raise RunControlNotFound(f"parent budget account not found: {child.parent_account_id}")
        parent = self._budgets[parent_run_id]
        updated, entries = roll_up_child_budget(
            parent,
            prior_child,
            child,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
        )
        self._apply_parent_rollup(
            parent,
            updated,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
        )
        self._budgets[parent_run_id] = updated
        self._ledger[parent_run_id].extend(entries)

    def _insert_events(self, events: Sequence[DomainEventEnvelope]) -> None:
        for event in events:
            prior = self._outbox.get(event.event_id)
            if prior is not None and prior.envelope != event:
                raise IdempotencyConflict(f"outbox event collision: {event.event_id}")
            self._outbox[event.event_id] = OutboxRecord(
                envelope=deepcopy(event),
                cursor=OutboxCursor(
                    position=self._next_outbox_position,
                    recorded_at=event.recorded_at,
                    aggregate_id=event.aggregate_id,
                    aggregate_version=event.aggregate_version,
                    sequence=event.sequence,
                ),
            )
            self._next_outbox_position += 1

    @staticmethod
    def _validate_accepted_admission(mutation: AdmissionMutation) -> None:
        if mutation.budget is None or mutation.transition is None or not mutation.events:
            raise ValueError("accepted admission must include all transactional effects")

    @staticmethod
    def _validate_accepted_command(mutation: CommandMutation) -> None:
        if mutation.budget is None or mutation.transition is None or not mutation.events:
            raise ValueError("accepted command must include all transactional effects")
