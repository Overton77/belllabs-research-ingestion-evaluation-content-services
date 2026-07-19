from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

import asyncpg

from app.application.run_control_repository import (
    AdmissionMutation,
    CommandMutation,
)
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

FailureHook = Callable[[str], Awaitable[None] | None]


class PostgresRunControlRepository:
    """Single-transaction PostgreSQL authority for run-control mutations."""

    def __init__(self, pool: asyncpg.Pool, *, before_commit: FailureHook | None = None) -> None:
        self._pool = pool
        self._before_commit = before_commit

    async def get_admission_decision(
        self, request_scope: str, idempotency_issuer: str, request_id: str
    ) -> AdmissionDecision | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT decision
                FROM belllabs_control.run_request_decisions
                WHERE request_scope = $1 AND idempotency_issuer = $2
                  AND request_id = $3
                """,
                request_scope,
                idempotency_issuer,
                request_id,
            )
        return AdmissionDecision.model_validate(_json(row["decision"])) if row else None

    async def commit_admission(self, mutation: AdmissionMutation) -> AdmissionDecision:
        decision = mutation.decision
        lock_key = (
            f"admission:{decision.request_scope}:"
            f"{decision.idempotency_issuer}:{decision.request_id}"
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, decision.request_scope)
            await _advisory_lock(connection, lock_key)
            prior = await connection.fetchrow(
                """
                SELECT request_fingerprint, decision
                FROM belllabs_control.run_request_decisions
                WHERE request_scope = $1 AND idempotency_issuer = $2
                  AND request_id = $3
                """,
                decision.request_scope,
                decision.idempotency_issuer,
                decision.request_id,
            )
            if prior:
                if prior["request_fingerprint"] != decision.request_fingerprint:
                    raise IdempotencyConflict(
                        "run request identity was reused with a conflicting payload"
                    )
                return AdmissionDecision.model_validate(_json(prior["decision"]))
            await connection.execute(
                """
                INSERT INTO belllabs_control.run_request_decisions
                    (request_scope, idempotency_issuer, request_id,
                     request_fingerprint, decision, recorded_at)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                """,
                decision.request_scope,
                decision.idempotency_issuer,
                decision.request_id,
                decision.request_fingerprint,
                _dump(decision),
                decision.recorded_at,
            )
            if mutation.projection is not None:
                if mutation.budget is None or mutation.transition is None or not mutation.events:
                    raise ValueError("accepted admission is missing transactional effects")
                await self._insert_run(connection, mutation.projection)
                await self._apply_parent_rollup(
                    connection,
                    None,
                    mutation.budget,
                    idempotency_id=f"admission:{decision.request_id}",
                    occurred_at=decision.recorded_at,
                )
                await self._insert_budget(connection, mutation.budget, decision.recorded_at)
                await self._insert_transition(connection, mutation.transition)
                await self._insert_ledger(connection, mutation.ledger_entries)
                await self._insert_events(connection, mutation.events)
            await self._inject("admission")
            return decision

    async def get_command_result(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> CommandResult | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT result
                FROM belllabs_control.lifecycle_command_results
                WHERE run_id = $1 AND idempotency_issuer = $2
                  AND command_id = $3
                """,
                run_id,
                idempotency_issuer,
                command_id,
            )
        return CommandResult.model_validate(_json(row["result"])) if row else None

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT projection FROM belllabs_control.workflow_runs
                WHERE run_id = $1 AND request_scope = $2
                """,
                run_id,
                request_scope,
            )
        if row is None:
            raise RunControlNotFound(f"workflow run not found: {run_id}")
        return RunProjection.model_validate(_json(row["projection"]))

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT account.state
                FROM belllabs_control.budget_accounts account
                JOIN belllabs_control.workflow_runs run USING (run_id)
                WHERE account.run_id = $1 AND run.request_scope = $2
                """,
                run_id,
                request_scope,
            )
        if row is None:
            raise RunControlNotFound(f"budget account not found for run: {run_id}")
        return BudgetState.model_validate(_json(row["state"]))

    async def commit_command(self, mutation: CommandMutation) -> CommandResult:
        result = mutation.result
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, mutation.request_scope)
            await _advisory_lock(connection, f"run:{result.run_id}")
            prior = await connection.fetchrow(
                """
                SELECT command_fingerprint, result
                FROM belllabs_control.lifecycle_command_results
                WHERE run_id = $1 AND idempotency_issuer = $2
                  AND command_id = $3
                """,
                result.run_id,
                result.idempotency_issuer,
                result.command_id,
            )
            if prior:
                if prior["command_fingerprint"] != result.command_fingerprint:
                    raise IdempotencyConflict(
                        "lifecycle command identity was reused with a conflicting payload"
                    )
                return CommandResult.model_validate(_json(prior["result"]))
            current = await connection.fetchrow(
                """
                SELECT version, projection
                FROM belllabs_control.workflow_runs
                WHERE run_id = $1
                FOR UPDATE
                """,
                result.run_id,
            )
            if current is None:
                raise RunControlNotFound(f"workflow run not found: {result.run_id}")
            current_projection = RunProjection.model_validate(_json(current["projection"]))
            current_version = current_projection.version
            if mutation.projection is None:
                raced = current_version != mutation.expected_version
                result = result.model_copy(
                    update={
                        "status": (
                            CommandStatus.STALE
                            if raced and result.status == CommandStatus.REJECTED
                            else result.status
                        ),
                        "resulting_run_version": current_projection.version,
                        "phase": current_projection.phase,
                        "terminal_outcome": current_projection.terminal_outcome,
                        "reason_code": (
                            "stale_run_version"
                            if raced and result.status == CommandStatus.REJECTED
                            else result.reason_code
                        ),
                        "reason": (
                            "run advanced while the rejected command was being decided"
                            if raced and result.status == CommandStatus.REJECTED
                            else result.reason
                        ),
                    }
                )
            if current_version != mutation.expected_version:
                if mutation.projection is None:
                    mutation = CommandMutation(
                        result=result,
                        request_scope=mutation.request_scope,
                        expected_version=current_version,
                    )
                else:
                    raise RunVersionConflict(
                        f"expected version {mutation.expected_version}, current version is "
                        f"{current_version}"
                    )
            await connection.execute(
                """
                INSERT INTO belllabs_control.lifecycle_command_results
                    (run_id, idempotency_issuer, command_id,
                     command_fingerprint, result, recorded_at)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                """,
                result.run_id,
                result.idempotency_issuer,
                result.command_id,
                result.command_fingerprint,
                _dump(result),
                result.recorded_at,
            )
            if mutation.projection is not None:
                if mutation.budget is None or mutation.transition is None or not mutation.events:
                    raise ValueError("accepted command is missing transactional effects")
                await connection.execute(
                    """
                    UPDATE belllabs_control.workflow_runs
                    SET version = $2, phase = $3, projection = $4::jsonb, updated_at = $5
                    WHERE run_id = $1
                    """,
                    mutation.projection.run_id,
                    mutation.projection.version,
                    mutation.projection.phase.value,
                    _dump(mutation.projection),
                    mutation.projection.updated_at,
                )
                prior_budget_raw = await connection.fetchval(
                    """
                    SELECT state FROM belllabs_control.budget_accounts
                    WHERE run_id = $1 FOR UPDATE
                    """,
                    mutation.projection.run_id,
                )
                prior_budget = BudgetState.model_validate(_json(prior_budget_raw))
                await self._apply_parent_rollup(
                    connection,
                    prior_budget,
                    mutation.budget,
                    idempotency_id=f"command:{result.command_id}",
                    occurred_at=result.recorded_at,
                )
                await connection.execute(
                    """
                    UPDATE belllabs_control.budget_accounts
                    SET state = $2::jsonb, updated_at = $3
                    WHERE run_id = $1
                    """,
                    mutation.projection.run_id,
                    _dump(mutation.budget),
                    mutation.projection.updated_at,
                )
                await self._insert_transition(connection, mutation.transition)
                await self._insert_ledger(connection, mutation.ledger_entries)
                await self._insert_events(connection, mutation.events)
            await self._inject("command")
            return result

    async def list_transitions(
        self, request_scope: str, run_id: str
    ) -> tuple[LifecycleTransitionRecord, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT transition
                FROM belllabs_control.lifecycle_transitions
                WHERE run_id = $1
                ORDER BY resulting_version
                """,
                run_id,
            )
        if not rows and not await self._run_exists(request_scope, run_id):
            raise RunControlNotFound(f"workflow run not found: {run_id}")
        return tuple(
            LifecycleTransitionRecord.model_validate(_json(row["transition"])) for row in rows
        )

    async def list_budget_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[BudgetLedgerEntry, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT entry
                FROM belllabs_control.budget_ledger
                WHERE run_id = $1
                ORDER BY occurred_at, entry_id
                """,
                run_id,
            )
        if not rows and not await self._run_exists(request_scope, run_id):
            raise RunControlNotFound(f"workflow run not found: {run_id}")
        return tuple(BudgetLedgerEntry.model_validate(_json(row["entry"])) for row in rows)

    async def list_outbox(
        self,
        request_scope: str,
        *,
        after: OutboxCursor | None = None,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            rows = await connection.fetch(
                """
                SELECT position, envelope, delivery_attempts, delivered_at
                FROM belllabs_control.outbox
                WHERE delivered_at IS NULL
                  AND ($1::bigint IS NULL OR position > $1)
                ORDER BY position
                LIMIT $2
                """,
                after.position if after else None,
                limit,
            )
        return tuple(_outbox_record(row) for row in rows)

    async def mark_outbox_delivered(
        self, request_scope: str, event_id: str, delivered_at: datetime
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            result = await connection.execute(
                """
                UPDATE belllabs_control.outbox
                SET delivery_attempts = delivery_attempts + 1, delivered_at = $2
                WHERE event_id = $1
                """,
                event_id,
                delivered_at,
            )
        if result == "UPDATE 0":
            raise RunControlNotFound(f"outbox event not found: {event_id}")

    async def apply_consumer_event(
        self, request_scope: str, consumer_id: str, envelope: DomainEventEnvelope
    ) -> ConsumerApplyResult:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            await _advisory_lock(connection, f"consumer:{consumer_id}:{envelope.aggregate_id}")
            authoritative = await connection.fetchval(
                "SELECT envelope FROM belllabs_control.outbox WHERE event_id = $1",
                envelope.event_id,
            )
            if (
                authoritative is None
                or DomainEventEnvelope.model_validate(_json(authoritative)) != envelope
            ):
                raise RunControlNotFound(
                    f"authoritative outbox event not found: {envelope.event_id}"
                )
            raw = await connection.fetchval(
                """
                SELECT cursor
                FROM belllabs_control.consumer_cursors
                WHERE consumer_id = $1 AND aggregate_id = $2
                FOR UPDATE
                """,
                consumer_id,
                envelope.aggregate_id,
            )
            cursor = (
                ConsumerCursor.model_validate(_json(raw))
                if raw is not None
                else ConsumerCursor(
                    consumer_id=consumer_id,
                    aggregate_id=envelope.aggregate_id,
                    last_aggregate_version=0,
                )
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
                if same_version_next_sequence
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
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.consumer_cursors
                        (consumer_id, aggregate_id, cursor)
                    VALUES ($1, $2, $3::jsonb)
                    ON CONFLICT (consumer_id, aggregate_id)
                    DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = clock_timestamp()
                    """,
                    consumer_id,
                    envelope.aggregate_id,
                    _dump(next_cursor),
                )
            return ConsumerApplyResult(
                status=status,
                cursor=next_cursor,
                expected_version=expected,
                observed_version=envelope.aggregate_version,
            )

    async def _insert_run(self, connection: asyncpg.Connection, projection: RunProjection) -> None:
        await connection.execute(
            """
            INSERT INTO belllabs_control.workflow_runs
                (run_id, request_scope, idempotency_issuer, request_id,
                 version, phase, projection, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            """,
            projection.run_id,
            projection.request_scope,
            projection.idempotency_issuer,
            projection.request_id,
            projection.version,
            projection.phase.value,
            _dump(projection),
            projection.updated_at,
        )

    async def _apply_parent_rollup(
        self,
        connection: asyncpg.Connection,
        prior_child: BudgetState | None,
        child: BudgetState,
        *,
        idempotency_id: str,
        occurred_at: datetime,
    ) -> None:
        if child.parent_account_id is None:
            return
        raw_parent = await connection.fetchval(
            """
            SELECT state FROM belllabs_control.budget_accounts
            WHERE account_id = $1
            FOR UPDATE
            """,
            child.parent_account_id,
        )
        if raw_parent is None:
            raise RunControlNotFound(f"parent budget account not found: {child.parent_account_id}")
        parent = BudgetState.model_validate(_json(raw_parent))
        updated, entries = roll_up_child_budget(
            parent,
            prior_child,
            child,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
        )
        await self._apply_parent_rollup(
            connection,
            parent,
            updated,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
        )
        await connection.execute(
            """
            UPDATE belllabs_control.budget_accounts
            SET state = $2::jsonb, updated_at = $3
            WHERE account_id = $1
            """,
            parent.account_id,
            _dump(updated),
            occurred_at,
        )
        await self._insert_ledger(connection, entries)

    async def _insert_budget(
        self, connection: asyncpg.Connection, budget: BudgetState, recorded_at: datetime
    ) -> None:
        await connection.execute(
            """
            INSERT INTO belllabs_control.budget_accounts
                (account_id, run_id, parent_account_id, state, updated_at)
            VALUES ($1, $2, $3, $4::jsonb, $5)
            """,
            budget.account_id,
            budget.run_id,
            budget.parent_account_id,
            _dump(budget),
            recorded_at,
        )

    async def _insert_transition(
        self, connection: asyncpg.Connection, transition: LifecycleTransitionRecord
    ) -> None:
        await connection.execute(
            """
            INSERT INTO belllabs_control.lifecycle_transitions
                (transition_id, run_id, command_id, prior_version, resulting_version,
                 transition, occurred_at)
            VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
            """,
            transition.transition_id,
            transition.run_id,
            transition.command_id,
            transition.prior_version,
            transition.resulting_version,
            _dump(transition),
            transition.occurred_at,
        )

    async def _insert_ledger(
        self,
        connection: asyncpg.Connection,
        entries: tuple[BudgetLedgerEntry, ...],
    ) -> None:
        for entry in entries:
            await connection.execute(
                """
                INSERT INTO belllabs_control.budget_ledger
                    (entry_id, account_id, run_id, idempotency_id, kind, entry, occurred_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                """,
                entry.entry_id,
                entry.account_id,
                entry.run_id,
                entry.idempotency_id,
                entry.kind.value,
                _dump(entry),
                entry.occurred_at,
            )

    async def _insert_events(
        self,
        connection: asyncpg.Connection,
        events: tuple[DomainEventEnvelope, ...],
    ) -> None:
        if not events:
            return
        await _advisory_lock(connection, "belllabs-control-outbox-order")
        position = int(
            await connection.fetchval(
                "SELECT COALESCE(MAX(position), 0) FROM belllabs_control.outbox"
            )
        )
        for event in events:
            position += 1
            await connection.execute(
                """
                INSERT INTO belllabs_control.outbox
                    (event_id, position, aggregate_id, aggregate_version, sequence, event_type,
                     envelope, recorded_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
                """,
                event.event_id,
                position,
                event.aggregate_id,
                event.aggregate_version,
                event.sequence,
                event.event_type,
                _dump(event),
                event.recorded_at,
            )

    async def _run_exists(self, request_scope: str, run_id: str) -> bool:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            return bool(
                await connection.fetchval(
                    "SELECT EXISTS (SELECT 1 FROM belllabs_control.workflow_runs "
                    "WHERE run_id = $1 AND request_scope = $2)",
                    run_id,
                    request_scope,
                )
            )

    async def _inject(self, boundary: str) -> None:
        if self._before_commit is None:
            return
        result = self._before_commit(boundary)
        if inspect.isawaitable(result):
            await result


async def _advisory_lock(connection: asyncpg.Connection, key: str) -> None:
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        key,
    )


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


def _dump(model: object) -> str:
    if not hasattr(model, "model_dump_json"):
        raise TypeError("PostgreSQL documents must be Pydantic contracts")
    return model.model_dump_json()


def _json(value: Any) -> object:
    return json.loads(value) if isinstance(value, str) else value


def _outbox_record(row: asyncpg.Record) -> OutboxRecord:
    envelope = DomainEventEnvelope.model_validate(_json(row["envelope"]))
    return OutboxRecord(
        envelope=envelope,
        cursor=OutboxCursor(
            position=row["position"],
            recorded_at=envelope.recorded_at,
            aggregate_id=envelope.aggregate_id,
            aggregate_version=envelope.aggregate_version,
            sequence=envelope.sequence,
        ),
        delivery_attempts=row["delivery_attempts"],
        delivered_at=row["delivered_at"],
    )
