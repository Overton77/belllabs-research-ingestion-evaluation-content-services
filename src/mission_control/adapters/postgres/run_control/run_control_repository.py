"""PostgreSQL run-control authority on the common mission_control component.

Admission writes the request receipt, mission, definition snapshot, revision #1, compiled
program, mission run, budget account, support effect ledger, ledger commit, contiguous
mission events and outbox rows in one transaction. Lifecycle commands, boundary commands
(``command`` + ``delivery_report``) and atomic family admission keep the transitional
idempotency, compare-and-swap and failure-injection semantics.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, TypeVar
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.application.execution.run_control_repository import (
    AdmissionMutation,
    CommandMutation,
    FamilyAdmissionCommit,
    _was_accepted,
    append_receipt,
    authority_state_digest,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.policies.budget import roll_up_child_budget
from mission_control.domain.policies.contracts import (
    CANCEL_SEQUENCE_SPACE,
    EXECUTION_SEQUENCE_SPACE,
    AdmissionDecision,
    BoundaryCommandReceipt,
    BoundaryCommandRecord,
    BoundaryCommandStatus,
    BudgetLedgerEntry,
    BudgetState,
    CommandResult,
    CommandStatus,
    ConsumerApplyResult,
    ConsumerApplyStatus,
    ConsumerCursor,
    DecisionStatus,
    DomainEventEnvelope,
    EffectLedgerEntry,
    EffectLedgerState,
    LifecycleTransitionRecord,
    OutboxCursor,
    OutboxRecord,
    RunProjection,
)
from mission_control.domain.policies.errors import (
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)
from mission_control.domain.policies.family_admission import (
    AtomicFamilyMutation,
    AuthorityStateConflict,
    FamilyAdmissionReceipt,
    FamilyVersionConflict,
)

FailureHook = Callable[[str], Awaitable[None] | None]
M = TypeVar("M", bound=AtomicFamilyMutation)

FAMILY_MUTATION_CONTRACT = "mc.family-mutation/1"
FAMILY_RECEIPT_CONTRACT = "mc.family-admission-receipt/1"
BOUNDARY_RECEIPT_SEMANTICS = "mc.boundary-receipt/1"


class PostgresRunControlRepository:
    """Single-transaction PostgreSQL authority for run-control mutations."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        family_writer_pool: asyncpg.Pool | None = None,
        before_commit: FailureHook | None = None,
    ) -> None:
        if family_writer_pool is pool:
            raise ValueError("family writer pool must be distinct from the normal application pool")
        self._pool = pool
        self._family_writer_pool = family_writer_pool
        self._before_commit = before_commit

    async def get_admission_decision(
        self, request_scope: str, idempotency_issuer: str, request_id: str
    ) -> AdmissionDecision | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await mc.receipt_row(
                connection,
                args,
                actor_ref=idempotency_issuer,
                action=mc.ADMIT_ACTION,
                request_key=request_id,
            )
        return AdmissionDecision.model_validate(mc.load(row["result"])) if row else None

    async def commit_admission(self, mutation: AdmissionMutation) -> AdmissionDecision:
        decision = mutation.decision
        lock_key = (
            f"admission:{decision.request_scope}:"
            f"{decision.idempotency_issuer}:{decision.request_id}"
        )
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, decision.request_scope)
            await mc.advisory_lock(connection, lock_key)
            prior = await mc.receipt_row(
                connection,
                args,
                actor_ref=decision.idempotency_issuer,
                action=mc.ADMIT_ACTION,
                request_key=decision.request_id,
            )
            if prior:
                if prior["payload_digest"] != decision.request_fingerprint:
                    raise IdempotencyConflict(
                        "run request identity was reused with a conflicting payload"
                    )
                return AdmissionDecision.model_validate(mc.load(prior["result"]))
            await mc.insert_receipt(
                connection,
                args,
                actor_ref=decision.idempotency_issuer,
                action=mc.ADMIT_ACTION,
                request_key=decision.request_id,
                payload_digest=decision.request_fingerprint,
                state="completed" if decision.status == DecisionStatus.ACCEPTED else "rejected",
                resource_ref=decision.run_id,
                result=decision.model_dump(mode="json"),
                recorded_at=decision.recorded_at,
            )
            if mutation.projection is not None:
                if (
                    mutation.budget is None
                    or mutation.effects is None
                    or mutation.transition is None
                    or not mutation.events
                ):
                    raise ValueError("accepted admission is missing transactional effects")
                actor = mutation.transition.actor.actor_id
                _mission_id, run_id = await mc.insert_admitted_run(
                    connection, args, mutation.projection, mutation.budget, actor
                )
                if mutation.budget.parent_account_id is not None:
                    await mc.lock_budget_chain(connection, args, mutation.budget.parent_account_id)
                await self._apply_parent_rollup(
                    connection,
                    args,
                    None,
                    mutation.budget,
                    idempotency_id=f"admission:{decision.request_id}",
                    occurred_at=decision.recorded_at,
                    actor_ref=actor,
                )
                await mc.insert_budget(
                    connection, args, mutation.budget, run_id, decision.recorded_at, actor
                )
                await mc.insert_effect_ledger(
                    connection, args, mutation.effects, decision.recorded_at, actor
                )
                commit_id = await mc.append_events(
                    connection,
                    args,
                    run_key=mutation.projection.run_id,
                    commit_key=mutation.transition.transition_id,
                    expected_versions={f"run:{mutation.projection.run_id}": 0},
                    events=mutation.events,
                    actor_ref=actor,
                )
                await mc.insert_transition(connection, args, mutation.transition, commit_id)
                await mc.insert_budget_entries(connection, args, mutation.ledger_entries, actor)
                await mc.insert_effect_entries(connection, args, mutation.effect_entries, actor)
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
            args = await mc.begin(connection, request_scope)
            if await _family_result_exists(
                connection, args, run_id, idempotency_issuer, command_id
            ):
                raise IdempotencyConflict(
                    "combined family command identity cannot be replayed as a plain command"
                )
            row = await mc.receipt_row(
                connection,
                args,
                actor_ref=idempotency_issuer,
                action=mc.LIFECYCLE_ACTION,
                request_key=mc.command_request_key(run_id, command_id),
            )
        return CommandResult.model_validate(mc.load(row["result"])) if row else None

    async def get_run(self, request_scope: str, run_id: str) -> RunProjection:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            row = await mc.run_row(connection, args, run_id)
        if row is None:
            raise RunControlNotFound(f"workflow run not found: {run_id}")
        return RunProjection.model_validate(mc.load(row["projection"]))

    async def get_budget(self, request_scope: str, run_id: str) -> BudgetState:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            state = await mc.budget_state(connection, args, run_key=run_id)
        if state is None:
            raise RunControlNotFound(f"budget account not found for run: {run_id}")
        return state

    async def get_effects(self, request_scope: str, run_id: str) -> EffectLedgerState:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            state = await mc.effect_state(connection, args, run_id)
        if state is None:
            raise RunControlNotFound(f"effect ledger not found for run: {run_id}")
        return state

    async def commit_command(self, mutation: CommandMutation) -> CommandResult:
        result = mutation.result
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, mutation.request_scope)
            await mc.advisory_lock(connection, f"run:{result.run_id}")
            request_key = mc.command_request_key(result.run_id, result.command_id)
            prior = await mc.receipt_row(
                connection,
                args,
                actor_ref=result.idempotency_issuer,
                action=mc.LIFECYCLE_ACTION,
                request_key=request_key,
            )
            if prior:
                if await _family_result_exists(
                    connection, args, result.run_id, result.idempotency_issuer, result.command_id
                ):
                    raise IdempotencyConflict(
                        "combined family command identity cannot be replayed as a plain command"
                    )
                if prior["payload_digest"] != result.command_fingerprint:
                    raise IdempotencyConflict(
                        "lifecycle command identity was reused with a conflicting payload"
                    )
                return CommandResult.model_validate(mc.load(prior["result"]))
            current = await mc.lock_mission_for_run(connection, args, result.run_id)
            current_projection = RunProjection.model_validate(mc.load(current["projection"]))
            current_version = current_projection.version
            prior_budget, prior_effects = await _locked_authority(connection, args, result.run_id)
            if mutation.expected_budget_digest != authority_state_digest(
                prior_budget
            ) or mutation.expected_effects_digest != authority_state_digest(prior_effects):
                raise AuthorityStateConflict(
                    "budget or effect authority changed while the command was being decided"
                )
            if mutation.projection is None:
                raced = current_version != mutation.expected_version
                if raced and mutation.boundary_commands and result.status == CommandStatus.ACCEPTED:
                    # A pending boundary acceptance binds the exact version it was validated
                    # against; the service re-decides it against the current projection.
                    raise RunVersionConflict(
                        f"expected version {mutation.expected_version}, "
                        f"current version is {current_version}"
                    )
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
                        expected_budget_digest=mutation.expected_budget_digest,
                        expected_effects_digest=mutation.expected_effects_digest,
                        boundary_commands=mutation.boundary_commands,
                        boundary_receipts=mutation.boundary_receipts,
                    )
                else:
                    raise RunVersionConflict(
                        f"expected version {mutation.expected_version}, current version is "
                        f"{current_version}"
                    )
            await _insert_command_result(connection, args, result, request_key)
            if mutation.projection is not None:
                await self._apply_transition(
                    connection,
                    args,
                    mutation,
                    prior_budget=prior_budget,
                    current_version=current_version,
                    idempotency_id=f"command:{result.command_id}",
                    occurred_at=result.recorded_at,
                )
            await self._record_boundary_commands(connection, args, mutation)
            await self._inject("command")
            return result

    # --- Boundary commands and receipts (RRM-007) -------------------------------------------

    async def get_boundary_command(
        self, request_scope: str, run_id: str, idempotency_issuer: str, command_id: str
    ) -> BoundaryCommandStatus | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            if await mc.run_row(connection, args, run_id) is None:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
            return await _boundary_command(connection, args, run_id, idempotency_issuer, command_id)

    async def list_boundary_commands(
        self, request_scope: str, run_id: str
    ) -> tuple[BoundaryCommandStatus, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            run = await mc.run_row(connection, args, run_id)
            if run is None:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
            rows = await connection.fetch(
                f"""
                SELECT payload->'record'->>'idempotency_issuer' AS idempotency_issuer,
                       payload->'record'->>'command_id' AS command_id
                FROM mission_control.command
                WHERE {SCOPE} AND run_id = $4 AND target_kind IS NOT NULL
                ORDER BY sequence_space, target_sequence, created_at,
                         payload->'record'->>'idempotency_issuer',
                         payload->'record'->>'command_id'
                """,
                *args,
                run["run_id"],
            )
            statuses = [
                await _boundary_command(
                    connection, args, run_id, row["idempotency_issuer"], row["command_id"]
                )
                for row in rows
            ]
        return tuple(status for status in statuses if status is not None)

    async def runs_with_pending_boundary_commands(
        self, request_scope: str, *, limit: int = 100
    ) -> tuple[str, ...]:
        # The latest receipt of an operator family command in the root's `execution`
        # sequence space, or of a cancel in its own `cancel` space (RRM-008), is still
        # `accepted`: inline delivery failed or never ran (`pending_delivery`); the relay
        # re-drives those runs in acceptance order.
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT run.run_key, min(c.created_at) AS first_accepted
                FROM mission_control.command c
                JOIN mission_control.mission_run run
                  ON run.installation_id = c.installation_id
                 AND run.application_id = c.application_id
                 AND run.tenant_id = c.tenant_id AND run.run_id = c.run_id
                WHERE {scoped("c")}
                  AND c.lifecycle = 'accepted'
                  AND c.target_kind IN ('root', 'family')
                  AND (
                    (c.command_kind IN ('pause', 'resume', 'satisfy_wait')
                     AND c.sequence_space = $4)
                    OR (c.command_kind = 'cancel' AND c.sequence_space = $6)
                  )
                GROUP BY run.run_key
                ORDER BY first_accepted
                LIMIT $5
                """,
                *args,
                EXECUTION_SEQUENCE_SPACE,
                limit,
                CANCEL_SEQUENCE_SPACE,
            )
        return tuple(str(row["run_key"]) for row in rows)

    async def record_boundary_receipt(
        self,
        request_scope: str,
        receipt: BoundaryCommandReceipt,
    ) -> BoundaryCommandStatus:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, f"run:{receipt.run_id}")
            status = await _boundary_command(
                connection,
                args,
                receipt.run_id,
                receipt.idempotency_issuer,
                receipt.command_id,
            )
            if status is None:
                raise RunControlNotFound(f"boundary command not found: {receipt.command_id}")
            updated = append_receipt(status, receipt)
            if len(updated.receipts) > len(status.receipts):
                await _append_receipt(connection, args, updated.receipts[-1])
            return updated

    async def _record_boundary_commands(
        self, connection: asyncpg.Connection, args: tuple[Any, ...], mutation: CommandMutation
    ) -> None:
        run_id = mutation.result.run_id
        new_ids = {
            (record.idempotency_issuer, record.command_id) for record in mutation.boundary_commands
        }
        run: asyncpg.Record | None = None
        for record in mutation.boundary_commands:
            if (
                await _boundary_command(
                    connection, args, run_id, record.idempotency_issuer, record.command_id
                )
                is not None
            ):
                raise IdempotencyConflict(f"boundary command already recorded: {record.command_id}")
            own = tuple(
                item
                for item in mutation.boundary_receipts
                if (item.idempotency_issuer, item.command_id)
                == (record.idempotency_issuer, record.command_id)
            )
            run = run or await mc.require_run(connection, args, run_id)
            sequence = record.target_sequence
            if sequence == 0 and record.target.kind != "run_control" and _was_accepted(own):
                sequence = await connection.fetchval(
                    f"""
                    SELECT coalesce(max(target_sequence), 0) + 1
                    FROM mission_control.command
                    WHERE {SCOPE} AND run_id = $4 AND sequence_space = $5
                    """,
                    *args,
                    run["run_id"],
                    record.target.sequence_space,
                )
            sequenced = record.model_copy(update={"target_sequence": sequence})
            # Validate the chain before writing it; the contract enforces the state machine.
            BoundaryCommandStatus(command=sequenced, receipts=own)
            await _insert_command(connection, args, run, sequenced, own)
        for item in mutation.boundary_receipts:
            if (item.idempotency_issuer, item.command_id) in new_ids:
                continue
            status = await _boundary_command(
                connection, args, run_id, item.idempotency_issuer, item.command_id
            )
            if status is None:
                raise RunControlNotFound(f"boundary command not found: {item.command_id}")
            updated = append_receipt(status, item)
            if len(updated.receipts) > len(status.receipts):
                await _append_receipt(connection, args, updated.receipts[-1])

    async def get_family_admission_receipt(
        self,
        request_scope: str,
        run_id: str,
        idempotency_issuer: str,
        command_id: str,
    ) -> FamilyAdmissionReceipt | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            if await mc.run_row(connection, args, run_id) is None:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
            raw = await connection.fetchval(
                f"""
                SELECT receipt FROM mission_control.family_admission_result
                WHERE {SCOPE} AND run_key = $4 AND idempotency_issuer = $5 AND command_key = $6
                """,
                *args,
                run_id,
                idempotency_issuer,
                command_id,
            )
        return FamilyAdmissionReceipt.model_validate(mc.load(raw)) if raw is not None else None

    async def get_family_head(
        self,
        request_scope: str,
        run_id: str,
        family_kind: str,
        mutation_type: type[M],
    ) -> M | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            if await mc.run_row(connection, args, run_id) is None:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
            raw = await connection.fetchval(
                f"""
                SELECT mutation FROM mission_control.family_admission_head
                WHERE {SCOPE} AND run_key = $4 AND family_kind = $5
                """,
                *args,
                run_id,
                family_kind,
            )
        return mutation_type.model_validate(mc.load(raw)) if raw is not None else None

    async def commit_family_admission(
        self, commit: FamilyAdmissionCommit
    ) -> FamilyAdmissionReceipt:
        commit.__post_init__()
        if self._family_writer_pool is None:
            raise RuntimeError(
                "atomic family admission requires a distinct family repository writer pool"
            )
        mutation = commit.family_mutation
        command = commit.command
        result = command.result
        async with self._family_writer_pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, mutation.request_scope)
            await mc.advisory_lock(connection, f"run:{mutation.run_id}")
            prior = await connection.fetchrow(
                f"""
                SELECT command_fingerprint, family_mutation_fingerprint, receipt
                FROM mission_control.family_admission_result
                WHERE {SCOPE} AND run_key = $4 AND idempotency_issuer = $5 AND command_key = $6
                """,
                *args,
                mutation.run_id,
                result.idempotency_issuer,
                result.command_id,
            )
            if prior:
                if (
                    prior["command_fingerprint"] != result.command_fingerprint
                    or prior["family_mutation_fingerprint"] != commit.family_mutation_fingerprint
                ):
                    raise IdempotencyConflict(
                        "family admission identity was reused with conflicting content"
                    )
                return FamilyAdmissionReceipt.model_validate(mc.load(prior["receipt"]))
            request_key = mc.command_request_key(mutation.run_id, result.command_id)
            plain = await mc.receipt_row(
                connection,
                args,
                actor_ref=result.idempotency_issuer,
                action=mc.LIFECYCLE_ACTION,
                request_key=request_key,
            )
            if plain is not None:
                raise IdempotencyConflict(
                    "plain lifecycle command identity cannot acquire a family mutation"
                )
            current = await mc.lock_mission_for_run(connection, args, mutation.run_id)
            current_projection = RunProjection.model_validate(mc.load(current["projection"]))
            current_version = current_projection.version
            prior_budget, prior_effects = await _locked_authority(connection, args, mutation.run_id)
            if command.expected_budget_digest != authority_state_digest(
                prior_budget
            ) or command.expected_effects_digest != authority_state_digest(prior_effects):
                raise AuthorityStateConflict(
                    "budget or effect authority changed while the command was being decided"
                )
            if command.projection is None:
                raced = current_version != command.expected_version
                result = result.model_copy(
                    update={
                        "status": (
                            CommandStatus.STALE
                            if raced and result.status == CommandStatus.REJECTED
                            else result.status
                        ),
                        "resulting_run_version": current_version,
                        "phase": current_projection.phase,
                        "terminal_outcome": current_projection.terminal_outcome,
                        "reason_code": (
                            "stale_run_version"
                            if raced and result.status == CommandStatus.REJECTED
                            else result.reason_code
                        ),
                    }
                )
                if result != commit.receipt.command_result:
                    raise RunVersionConflict("combined command result changed while committing")
            elif current_version != command.expected_version:
                raise RunVersionConflict(
                    f"expected version {command.expected_version}, current version is "
                    f"{current_version}"
                )

            accepted = commit.receipt.family_receipt is not None
            if accepted:
                collision = await connection.fetchrow(
                    f"""
                    SELECT mutation_fingerprint FROM mission_control.family_admission_journal
                    WHERE {SCOPE} AND run_key = $4 AND family_kind = $5 AND mutation_key = $6
                    """,
                    *args,
                    mutation.run_id,
                    mutation.family_kind,
                    mutation.mutation_id,
                )
                if collision is not None:
                    if collision["mutation_fingerprint"] == commit.family_mutation_fingerprint:
                        raise IdempotencyConflict(
                            "family mutation identity was reused by another command"
                        )
                    raise IdempotencyConflict(
                        "family mutation identity was reused with conflicting content"
                    )
                prior_head = await connection.fetchrow(
                    f"""
                    SELECT family_version FROM mission_control.family_admission_head
                    WHERE {SCOPE} AND run_key = $4 AND family_kind = $5
                    FOR UPDATE
                    """,
                    *args,
                    mutation.run_id,
                    mutation.family_kind,
                )
                current_family_version = prior_head["family_version"] if prior_head else 0
                if mutation.expected_family_version != current_family_version:
                    raise FamilyVersionConflict(
                        f"expected family version {mutation.expected_family_version}, "
                        f"current version is {current_family_version}"
                    )

            receipt_id = await _insert_command_result(connection, args, result, request_key)
            if command.projection is not None:
                await self._apply_transition(
                    connection,
                    args,
                    command,
                    prior_budget=prior_budget,
                    current_version=current_version,
                    idempotency_id=f"command:{result.command_id}",
                    occurred_at=result.recorded_at,
                )
            # The terminal receipts of a family-admitted terminalization.
            await self._record_boundary_commands(connection, args, command)
            await self._inject("family_admission.after_run_control")
            actor = result.idempotency_issuer
            if accepted:
                await connection.execute(
                    """
                    INSERT INTO mission_control.family_admission_journal (
                        installation_id, application_id, tenant_id, family_admission_journal_id,
                        run_key, family_kind, family_version, mutation_kind, mutation_key,
                        mutation_fingerprint, mutation_contract, mutation, decided_at, created_at,
                        created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $13,
                            $14)
                    """,
                    *args,
                    uuid7(),
                    mutation.run_id,
                    mutation.family_kind,
                    mutation.expected_family_version + 1,
                    mutation.mutation_kind,
                    mutation.mutation_id,
                    commit.family_mutation_fingerprint,
                    FAMILY_MUTATION_CONTRACT,
                    mc.dump(mutation),
                    mutation.decided_at,
                    actor,
                )
                head_written = await connection.execute(
                    """
                    INSERT INTO mission_control.family_admission_head (
                        installation_id, application_id, tenant_id, family_admission_head_id,
                        run_key, family_kind, family_version, mutation_fingerprint,
                        mutation_contract, mutation, updated_at, created_at, created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $11, $12)
                    ON CONFLICT (installation_id, application_id, tenant_id, run_key, family_kind)
                    DO UPDATE SET family_version = EXCLUDED.family_version,
                                  mutation_fingerprint = EXCLUDED.mutation_fingerprint,
                                  mutation = EXCLUDED.mutation,
                                  updated_at = EXCLUDED.updated_at
                    WHERE mission_control.family_admission_head.family_version
                          = EXCLUDED.family_version - 1
                    """,
                    *args,
                    uuid7(),
                    mutation.run_id,
                    mutation.family_kind,
                    mutation.expected_family_version + 1,
                    commit.family_mutation_fingerprint,
                    FAMILY_MUTATION_CONTRACT,
                    mc.dump(mutation),
                    mutation.decided_at,
                    actor,
                )
                if head_written != "INSERT 0 1":
                    raise FamilyVersionConflict("family head advanced concurrently")
            await connection.execute(
                """
                INSERT INTO mission_control.family_admission_result (
                    installation_id, application_id, tenant_id, family_admission_result_id,
                    run_key, idempotency_issuer, command_key, command_fingerprint,
                    family_mutation_fingerprint, request_receipt_id, receipt_contract, receipt,
                    recorded_at, created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb, $13, $13, $6)
                """,
                *args,
                uuid7(),
                mutation.run_id,
                result.idempotency_issuer,
                result.command_id,
                result.command_fingerprint,
                commit.family_mutation_fingerprint,
                receipt_id,
                FAMILY_RECEIPT_CONTRACT,
                mc.dump(commit.receipt),
                result.recorded_at,
            )
            await self._inject("family_admission.after_family")
            return commit.receipt

    async def list_transitions(
        self, request_scope: str, run_id: str
    ) -> tuple[LifecycleTransitionRecord, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT transition FROM mission_control.run_lifecycle_transition
                WHERE {SCOPE} AND run_key = $4
                ORDER BY resulting_version
                """,
                *args,
                run_id,
            )
            if not rows and await mc.run_row(connection, args, run_id) is None:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
        return tuple(
            LifecycleTransitionRecord.model_validate(mc.load(row["transition"])) for row in rows
        )

    async def list_budget_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[BudgetLedgerEntry, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            entries = await mc.list_budget_entries(connection, args, run_id)
            if not entries and await mc.run_row(connection, args, run_id) is None:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
        return tuple(entries)

    async def list_effect_ledger(
        self, request_scope: str, run_id: str
    ) -> tuple[EffectLedgerEntry, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT entry FROM mission_control.effect_ledger_entry
                WHERE {SCOPE} AND run_key = $4
                ORDER BY occurred_at, entry_key
                """,
                *args,
                run_id,
            )
            if not rows and await mc.run_row(connection, args, run_id) is None:
                raise RunControlNotFound(f"workflow run not found: {run_id}")
        return tuple(EffectLedgerEntry.model_validate(mc.load(row["entry"])) for row in rows)

    async def list_outbox(
        self,
        request_scope: str,
        *,
        after: OutboxCursor | None = None,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                SELECT global_position, payload, attempts, delivered_at
                FROM mission_control.outbox
                WHERE {SCOPE} AND destination_kind = $6
                  AND delivery_state IN ('pending', 'leased')
                  AND ($4::bigint IS NULL OR global_position > $4)
                ORDER BY global_position
                LIMIT $5
                """,
                *args,
                after.position if after else None,
                limit,
                mc.EVENT_DESTINATION,
            )
        return tuple(_outbox_record(row) for row in rows)

    async def lease_outbox(
        self,
        request_scope: str,
        *,
        lease_owner: str,
        lease_until: datetime,
        now: datetime,
        limit: int = 100,
    ) -> tuple[OutboxRecord, ...]:
        """Claim deliverable outbox rows with a fenced lease (``FOR UPDATE SKIP LOCKED``)."""

        if not lease_owner:
            raise ValueError("outbox lease owner cannot be empty")
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            rows = await connection.fetch(
                f"""
                WITH due AS (
                    SELECT outbox_id FROM mission_control.outbox
                    WHERE {SCOPE} AND destination_kind = $8
                      AND (delivery_state = 'pending'
                           OR (delivery_state = 'leased' AND lease_expires_at <= $6))
                      AND next_attempt_at <= $6
                    ORDER BY global_position
                    LIMIT $7
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE mission_control.outbox AS box
                SET delivery_state = 'leased', lease_owner = $4, lease_expires_at = $5,
                    version = box.version + 1
                FROM due
                WHERE {scoped("box")} AND box.outbox_id = due.outbox_id
                RETURNING box.global_position, box.payload, box.attempts, box.delivered_at
                """,
                *args,
                lease_owner,
                lease_until,
                now,
                limit,
                mc.EVENT_DESTINATION,
            )
        return tuple(_outbox_record(row) for row in sorted(rows, key=lambda r: r[0]))

    async def mark_outbox_delivered(
        self, request_scope: str, event_id: str, delivered_at: datetime
    ) -> None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            result = await connection.execute(
                f"""
                UPDATE mission_control.outbox
                SET attempts = attempts + 1, delivered_at = $5, delivery_state = 'delivered',
                    lease_owner = NULL, lease_expires_at = NULL, version = version + 1
                WHERE {SCOPE} AND delivery_key = $4
                """,
                *args,
                event_id,
                delivered_at,
            )
        if result == "UPDATE 0":
            raise RunControlNotFound(f"outbox event not found: {event_id}")

    async def apply_consumer_event(
        self, request_scope: str, consumer_id: str, envelope: DomainEventEnvelope
    ) -> ConsumerApplyResult:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            await mc.advisory_lock(connection, f"consumer:{consumer_id}:{envelope.aggregate_id}")
            authoritative = await connection.fetchval(
                f"SELECT payload FROM mission_control.outbox WHERE {SCOPE} AND delivery_key = $4",
                *args,
                envelope.event_id,
            )
            if (
                authoritative is None
                or DomainEventEnvelope.model_validate(mc.load(authoritative)) != envelope
            ):
                raise RunControlNotFound(
                    f"authoritative outbox event not found: {envelope.event_id}"
                )
            raw = await connection.fetchval(
                f"""
                SELECT cursor FROM mission_control.consumer_cursor
                WHERE {SCOPE} AND consumer_key = $4 AND aggregate_key = $5
                FOR UPDATE
                """,
                *args,
                consumer_id,
                envelope.aggregate_id,
            )
            cursor = (
                ConsumerCursor.model_validate(mc.load(raw))
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
                    INSERT INTO mission_control.consumer_cursor (
                        installation_id, application_id, tenant_id, consumer_cursor_id,
                        consumer_key, aggregate_key, cursor, updated_at, created_at,
                        created_by_actor_ref
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, clock_timestamp(),
                            clock_timestamp(), $5)
                    ON CONFLICT (installation_id, application_id, tenant_id, consumer_key,
                                 aggregate_key)
                    DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = clock_timestamp()
                    """,
                    *args,
                    uuid7(),
                    consumer_id,
                    envelope.aggregate_id,
                    mc.dump(next_cursor),
                )
            return ConsumerApplyResult(
                status=status,
                cursor=next_cursor,
                expected_version=expected,
                observed_version=envelope.aggregate_version,
            )

    async def _apply_transition(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        mutation: CommandMutation,
        *,
        prior_budget: BudgetState,
        current_version: int,
        idempotency_id: str,
        occurred_at: datetime,
    ) -> None:
        if (
            mutation.projection is None
            or mutation.budget is None
            or mutation.effects is None
            or mutation.transition is None
            or not mutation.events
        ):
            raise ValueError("accepted command is missing transactional effects")
        projection = mutation.projection
        actor = mutation.transition.actor.actor_id
        await mc.update_run(connection, args, projection, expected_version=current_version)
        await self._apply_parent_rollup(
            connection,
            args,
            prior_budget,
            mutation.budget,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
            actor_ref=actor,
        )
        await mc.update_budget(connection, args, mutation.budget, projection.updated_at)
        await mc.update_effect_ledger(connection, args, mutation.effects, projection.updated_at)
        commit_id = await mc.append_events(
            connection,
            args,
            run_key=projection.run_id,
            commit_key=mutation.transition.transition_id,
            expected_versions={f"run:{projection.run_id}": current_version},
            events=mutation.events,
            actor_ref=actor,
        )
        await mc.insert_transition(connection, args, mutation.transition, commit_id)
        await mc.insert_budget_entries(connection, args, mutation.ledger_entries, actor)
        await mc.insert_effect_entries(connection, args, mutation.effect_entries, actor)

    async def _apply_parent_rollup(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        prior_child: BudgetState | None,
        child: BudgetState,
        *,
        idempotency_id: str,
        occurred_at: datetime,
        actor_ref: str,
    ) -> None:
        if child.parent_account_id is None:
            return
        parent = await mc.budget_state(
            connection, args, account_key=child.parent_account_id, lock=True
        )
        if parent is None:
            raise RunControlNotFound(f"parent budget account not found: {child.parent_account_id}")
        updated, entries = roll_up_child_budget(
            parent,
            prior_child,
            child,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
        )
        await self._apply_parent_rollup(
            connection,
            args,
            parent,
            updated,
            idempotency_id=idempotency_id,
            occurred_at=occurred_at,
            actor_ref=actor_ref,
        )
        await mc.update_budget(connection, args, updated, occurred_at)
        await mc.insert_budget_entries(connection, args, entries, actor_ref)

    async def _inject(self, boundary: str) -> None:
        if self._before_commit is None:
            return
        result = self._before_commit(boundary)
        if inspect.isawaitable(result):
            await result


async def _locked_authority(
    connection: asyncpg.Connection, args: tuple[Any, ...], run_key: str
) -> tuple[BudgetState, EffectLedgerState]:
    """Lock the run's budget chain (sorted) and then its effect ledger."""

    account = await mc.budget_state(connection, args, run_key=run_key)
    if account is None:
        raise RunControlNotFound(f"budget account not found for run: {run_key}")
    await mc.lock_budget_chain(connection, args, account.account_id)
    budget = await mc.budget_state(connection, args, account_key=account.account_id)
    effects = await mc.effect_state(connection, args, run_key, lock=True)
    if budget is None or effects is None:
        raise RunControlNotFound(f"run authority not found: {run_key}")
    return budget, effects


async def _family_result_exists(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    run_key: str,
    idempotency_issuer: str,
    command_id: str,
) -> bool:
    return bool(
        await connection.fetchval(
            f"""
            SELECT EXISTS (
                SELECT 1 FROM mission_control.family_admission_result
                WHERE {SCOPE} AND run_key = $4 AND idempotency_issuer = $5 AND command_key = $6
            )
            """,
            *args,
            run_key,
            idempotency_issuer,
            command_id,
        )
    )


async def _insert_command_result(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    result: CommandResult,
    request_key: str,
) -> UUID:
    return await mc.insert_receipt(
        connection,
        args,
        actor_ref=result.idempotency_issuer,
        action=mc.LIFECYCLE_ACTION,
        request_key=request_key,
        payload_digest=result.command_fingerprint,
        state="completed" if result.status == CommandStatus.ACCEPTED else "rejected",
        resource_ref=result.run_id,
        result=result.model_dump(mode="json"),
        recorded_at=result.recorded_at,
    )


def _command_key(run_key: str, idempotency_issuer: str, command_id: str) -> str:
    return mc.json_key("boundary", run_key, idempotency_issuer, command_id)


async def _boundary_command(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    run_key: str,
    idempotency_issuer: str,
    command_id: str,
) -> BoundaryCommandStatus | None:
    row = await connection.fetchrow(
        f"""
        SELECT command_id, payload FROM mission_control.command
        WHERE {SCOPE} AND command_key = $4
        """,
        *args,
        _command_key(run_key, idempotency_issuer, command_id),
    )
    if row is None:
        return None
    payload = mc.load(row["payload"])
    reports = await connection.fetch(
        f"""
        SELECT detail FROM mission_control.delivery_report
        WHERE {SCOPE} AND command_id = $4
        ORDER BY (detail->>'ordinal')::integer
        """,
        *args,
        row["command_id"],
    )
    return BoundaryCommandStatus(
        command=BoundaryCommandRecord.model_validate(payload["record"]),
        receipts=(
            BoundaryCommandReceipt.model_validate(payload["initial_receipt"]),
            *(BoundaryCommandReceipt.model_validate(mc.load(item["detail"])) for item in reports),
        ),
    )


async def _insert_command(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    run: asyncpg.Record,
    record: BoundaryCommandRecord,
    receipts: tuple[BoundaryCommandReceipt, ...],
) -> None:
    """The accepted (or rejected-at-acceptance) command, then its later receipts."""

    first, *later = receipts
    command_id = uuid7()
    await connection.execute(
        """
        INSERT INTO mission_control.command (
            installation_id, application_id, tenant_id, command_id, command_key, mission_id,
            run_id, activation_id, subordinate_id, target_generation, target_version,
            command_kind, payload, payload_digest, payload_ref, deadline_at, lifecycle, outcome,
            requested_by_actor_ref, target_kind, target_ref, sequence_space, target_sequence,
            version, updated_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, NULL, $8, $9, $10, $11::jsonb, $12, NULL,
                NULL, $13, $14, $15, $16, $17, $18, $19, 1, $20, $20, $15)
        """,
        *args,
        command_id,
        _command_key(record.run_id, record.idempotency_issuer, record.command_id),
        run["mission_id"],
        run["run_id"],
        record.target.execution_generation,
        record.accepted_run_version,
        record.kind,
        mc.dump(
            {
                "record": record.model_dump(mode="json"),
                "initial_receipt": first.model_dump(mode="json"),
            }
        ),
        record.payload_digest,
        first.state.value,
        first.rejection_reason,
        record.actor_id,
        record.target.kind,
        record.target.target_ref,
        record.target.sequence_space,
        record.target_sequence,
        record.recorded_at,
    )
    for receipt in later:
        await _insert_report(connection, args, command_id, receipt)


async def _append_receipt(
    connection: asyncpg.Connection, args: tuple[Any, ...], receipt: BoundaryCommandReceipt
) -> None:
    command_id = await connection.fetchval(
        f"SELECT command_id FROM mission_control.command WHERE {SCOPE} AND command_key = $4",
        *args,
        _command_key(receipt.run_id, receipt.idempotency_issuer, receipt.command_id),
    )
    if command_id is None:
        raise RunControlNotFound(f"boundary command not found: {receipt.command_id}")
    await _insert_report(connection, args, command_id, receipt)


async def _insert_report(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    command_id: UUID,
    receipt: BoundaryCommandReceipt,
) -> None:
    """A delivery report per delivered/applied/rejected receipt; the command lifecycle
    advances in the same transaction. Accepted, delivered and applied stay distinct."""

    await connection.execute(
        """
        INSERT INTO mission_control.delivery_report (
            installation_id, application_id, tenant_id, delivery_report_id, command_id,
            report_key, delivery_semantics, reported_at, native_refs, observed_outcome, detail,
            created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $8, $12)
        """,
        *args,
        uuid7(),
        command_id,
        mc.json_key(
            "boundary-receipt",
            receipt.run_id,
            receipt.idempotency_issuer,
            receipt.command_id,
            receipt.ordinal,
        ),
        BOUNDARY_RECEIPT_SEMANTICS,
        receipt.recorded_at,
        [receipt.transport_ref] if receipt.transport_ref else [],
        receipt.state.value,
        mc.dump(receipt.model_dump(mode="json")),
        receipt.recorded_by,
    )
    await connection.execute(
        f"""
        UPDATE mission_control.command
        SET lifecycle = $5, outcome = COALESCE($6, outcome), version = version + 1,
            updated_at = $7
        WHERE {SCOPE} AND command_id = $4
        """,
        *args,
        command_id,
        receipt.state.value,
        receipt.rejection_reason,
        receipt.recorded_at,
    )


def _outbox_record(row: asyncpg.Record) -> OutboxRecord:
    envelope = DomainEventEnvelope.model_validate(mc.load(row["payload"]))
    return OutboxRecord(
        envelope=envelope,
        cursor=OutboxCursor(
            position=row["global_position"],
            recorded_at=envelope.recorded_at,
            aggregate_id=envelope.aggregate_id,
            aggregate_version=envelope.aggregate_version,
            sequence=envelope.sequence,
        ),
        delivery_attempts=row["attempts"],
        delivered_at=row["delivered_at"],
    )
