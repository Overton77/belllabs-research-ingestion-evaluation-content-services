from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg

from app.application.operations.operation_journal import (
    OperationJournalMutation,
    _is_journal_only_authority_mutation,
)
from app.domain.control_plane.canonical import sha256_digest
from app.domain.operation_execution.journal import (
    OperationClaimResult,
    OperationEffectClaim,
    OperationJournalSettlement,
    OperationTechnicalAttempt,
)
from app.domain.run_control.budget import roll_up_child_budget
from app.domain.run_control.contracts import (
    ApplyAuthorityBatchAction,
    BudgetLedgerEntry,
    BudgetState,
    RecordUsageAction,
    RunProjection,
)
from app.domain.run_control.errors import (
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)

FailureHook = Callable[[str], Awaitable[None] | None]


class PostgresAtomicOperationJournalRepository:
    """Commits claim, attempt, settlement, run, budget, lifecycle, and outbox atomically."""

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        before_commit: FailureHook | None = None,
    ) -> None:
        self._pool = pool
        self._before_commit = before_commit

    async def commit(
        self,
        mutation: OperationJournalMutation,
    ) -> OperationClaimResult:
        mutation.validate()
        claim = mutation.claim
        if claim.claim_mode != "active":
            return OperationClaimResult(
                status="shadow_denied",
                reason="shadow execution cannot acquire a consequential effect claim",
            )
        lock_key = (
            f"operation-effect:{claim.request_scope}:"
            f"{claim.operation_contract_digest}:{claim.idempotency_key}"
        )
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, mutation.request_scope)
            await _lock(connection, lock_key)
            prior_mutation = await connection.fetchrow(
                """
                SELECT m.effect_claim_id, m.mutation_digest, c.*
                FROM belllabs_control.operation_journal_mutations AS m
                JOIN belllabs_control.operation_effect_claims AS c
                  ON c.request_scope = m.request_scope
                 AND c.effect_claim_id = m.effect_claim_id
                WHERE m.request_scope = $1 AND m.mutation_id = $2
                """,
                mutation.request_scope,
                mutation.mutation_id,
            )
            if prior_mutation is not None:
                if (
                    prior_mutation["effect_claim_id"] != claim.effect_claim_id
                    or prior_mutation["mutation_digest"] != mutation.mutation_digest
                ):
                    raise IdempotencyConflict(
                        "operation journal mutation identity has conflicting intent"
                    )
                return OperationClaimResult(
                    status="existing",
                    claim=_claim_from_row(prior_mutation),
                    reason="same operation journal mutation already committed",
                )
            run_row = await connection.fetchrow(
                """
                SELECT version, projection
                FROM belllabs_control.workflow_runs
                WHERE request_scope = $1 AND run_id = $2
                FOR UPDATE
                """,
                mutation.request_scope,
                mutation.belllabs_run_id,
            )
            if run_row is None:
                raise RunControlNotFound(f"workflow run not found: {mutation.belllabs_run_id}")
            journal_only_settlement = _is_journal_only_authority_mutation(mutation)
            if run_row["version"] != mutation.expected_run_version and not (
                journal_only_settlement
                and run_row["version"] >= mutation.expected_run_version
            ):
                raise RunVersionConflict(
                    f"expected version {mutation.expected_run_version}, "
                    f"current version is {run_row['version']}"
                )
            if journal_only_settlement:
                assert mutation.authority_result is not None
                assert mutation.authority_command is not None
                persisted_authority = await connection.fetchrow(
                    """
                    SELECT command_fingerprint, result
                    FROM belllabs_control.lifecycle_command_results
                    WHERE run_id = $1 AND idempotency_issuer = $2 AND command_id = $3
                    """,
                    mutation.authority_result.run_id,
                    mutation.authority_result.idempotency_issuer,
                    mutation.authority_result.command_id,
                )
                if (
                    persisted_authority is None
                    or persisted_authority["command_fingerprint"]
                    != mutation.authority_result.command_fingerprint
                    or _json(persisted_authority["result"])
                    != mutation.authority_result.model_dump(mode="json")
                ):
                    raise IdempotencyConflict(
                        "journal settlement authority result is missing or unrelated"
                    )
                authority_evidence = await connection.fetchrow(
                    """
                    SELECT transition->>'command_id' AS transition_command_id,
                           outbox.event_type,
                           outbox.envelope
                    FROM belllabs_control.lifecycle_transitions AS transitions
                    JOIN belllabs_control.outbox AS outbox
                      ON outbox.aggregate_id = transitions.run_id
                     AND outbox.aggregate_version = transitions.resulting_version
                     AND outbox.sequence = 1
                    WHERE transitions.run_id = $1
                      AND transitions.resulting_version = $2
                    """,
                    mutation.authority_result.run_id,
                    mutation.authority_result.resulting_run_version,
                )
                action = mutation.authority_command.action
                expected_event_type = (
                    "workflow_run.apply_authority_batch"
                    if isinstance(action, ApplyAuthorityBatchAction)
                    else "workflow_run.record_usage"
                    if isinstance(action, RecordUsageAction)
                    else "workflow_run.claim_effect"
                )
                envelope = (
                    _json(authority_evidence["envelope"])
                    if authority_evidence is not None
                    else {}
                )
                if (
                    authority_evidence is None
                    or authority_evidence["transition_command_id"]
                    != mutation.authority_command.command_id
                    or authority_evidence["event_type"] != expected_event_type
                    or envelope.get("payload", {}).get("command_id")
                    != mutation.authority_command.command_id
                    or (
                        isinstance(action, ApplyAuthorityBatchAction)
                        and envelope.get("payload", {}).get("authority_batch_digest")
                        != sha256_digest(action)
                    )
                ):
                    raise IdempotencyConflict(
                        "journal settlement authority transition or event is unrelated"
                    )
            prior_row = await connection.fetchrow(
                """
                SELECT *
                FROM belllabs_control.operation_effect_claims
                WHERE request_scope = $1 AND operation_contract_digest = $2
                  AND idempotency_key = $3
                """,
                claim.request_scope,
                claim.operation_contract_digest,
                claim.idempotency_key,
            )
            existing = prior_row is not None
            persisted_claim: OperationEffectClaim | None = None
            if prior_row is not None:
                if (
                    prior_row["request_scope"] != claim.request_scope
                    or prior_row["belllabs_run_id"] != claim.belllabs_run_id
                    or prior_row["operation_contract_digest"] != claim.operation_contract_digest
                    or prior_row["idempotency_key"] != claim.idempotency_key
                    or prior_row["request_digest"] != claim.request_digest
                    or prior_row["semantic_binding_id"] != claim.semantic_binding_id
                    or prior_row["semantic_binding_digest"] != claim.semantic_binding_digest
                    or prior_row["semantic_attempt_key"] != claim.semantic_attempt_key
                    or prior_row["claim_mode"] != claim.claim_mode
                ):
                    raise IdempotencyConflict(
                        "effect claim key was reused with conflicting immutable intent"
                    )
                persisted_claim = _claim_from_row(prior_row)
                if prior_row["effect_claim_id"] != claim.effect_claim_id:
                    if _has_claim_children(mutation):
                        raise IdempotencyConflict(
                            "claim replay regenerated identity while carrying child mutations"
                        )
                    return OperationClaimResult(
                        status="existing",
                        claim=persisted_claim,
                        reason="same claim key and request digest already exists",
                    )
                if prior_row["status"] in {"settled", "cancelled"}:
                    prior_mutation = await connection.fetchrow(
                        """
                        SELECT effect_claim_id, mutation_digest
                        FROM belllabs_control.operation_journal_mutations
                        WHERE request_scope = $1 AND mutation_id = $2
                        """,
                        mutation.request_scope,
                        mutation.mutation_id,
                    )
                    if (
                        prior_mutation is not None
                        and prior_mutation["effect_claim_id"] == claim.effect_claim_id
                        and prior_mutation["mutation_digest"] == mutation.mutation_digest
                    ):
                        return OperationClaimResult(
                            status="existing",
                            claim=persisted_claim,
                            reason="same terminal journal mutation already committed",
                        )
                    raise IdempotencyConflict(
                        "terminal operation claim cannot accept another mutation"
                    )
            else:
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.operation_effect_claims (
                        effect_claim_id, request_scope, belllabs_run_id,
                        operation_contract_digest, idempotency_key, request_digest,
                        semantic_binding_id, semantic_binding_digest, semantic_attempt_key,
                        claim_mode, status, claimed_by, claimed_at,
                        heartbeat_at, lease_expires_at
                    )
                    VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                        $11, $12, $13, $14, $15
                    )
                    """,
                    claim.effect_claim_id,
                    claim.request_scope,
                    claim.belllabs_run_id,
                    claim.operation_contract_digest,
                    claim.idempotency_key,
                    claim.request_digest,
                    claim.semantic_binding_id,
                    claim.semantic_binding_digest,
                    claim.semantic_attempt_key,
                    claim.claim_mode,
                    claim.status.value,
                    claim.claimed_by,
                    claim.claimed_at,
                    claim.heartbeat_at,
                    claim.lease_expires_at,
                )
            mutation_inserted = await connection.fetchval(
                """
                INSERT INTO belllabs_control.operation_journal_mutations (
                    request_scope, mutation_id, effect_claim_id, mutation_digest,
                    mutation_payload, recorded_at
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                ON CONFLICT (request_scope, mutation_id) DO NOTHING
                RETURNING mutation_id
                """,
                mutation.request_scope,
                mutation.mutation_id,
                claim.effect_claim_id,
                mutation.mutation_digest,
                json.dumps({"schema_version": 1, "mutation_id": mutation.mutation_id}),
                claim.claimed_at,
            )
            if mutation_inserted is None:
                prior_mutation = await connection.fetchrow(
                    """
                    SELECT effect_claim_id, mutation_digest
                    FROM belllabs_control.operation_journal_mutations
                    WHERE request_scope = $1 AND mutation_id = $2
                    """,
                    mutation.request_scope,
                    mutation.mutation_id,
                )
                if (
                    prior_mutation is None
                    or prior_mutation["effect_claim_id"] != claim.effect_claim_id
                    or prior_mutation["mutation_digest"] != mutation.mutation_digest
                ):
                    raise IdempotencyConflict(
                        "operation journal mutation identity has conflicting intent"
                    )
                return OperationClaimResult(
                    status="existing",
                    claim=persisted_claim or claim,
                    reason="same operation journal mutation already committed",
                )
            await self._commit_attempt(connection, mutation)
            await self._commit_settlement(connection, mutation)
            await self._commit_run_control(
                connection,
                mutation,
                current_run=RunProjection.model_validate(_json(run_row["projection"])),
            )
            await self._inject("operation_journal")
            return OperationClaimResult(
                status="existing" if existing else "acquired",
                claim=persisted_claim or claim,
                reason=(
                    "same claim key and digest already exists"
                    if existing
                    else "consequential effect claim acquired"
                ),
            )

    async def get_claim(
        self,
        request_scope: str,
        effect_claim_id: str,
    ) -> OperationEffectClaim | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT *
                FROM belllabs_control.operation_effect_claims
                WHERE request_scope = $1 AND effect_claim_id = $2
                """,
                request_scope,
                effect_claim_id,
            )
        if row is None:
            return None
        return OperationEffectClaim(
            effect_claim_id=row["effect_claim_id"],
            request_scope=row["request_scope"],
            belllabs_run_id=row["belllabs_run_id"],
            operation_contract_digest=row["operation_contract_digest"],
            idempotency_key=row["idempotency_key"],
            request_digest=row["request_digest"],
            semantic_binding_id=row["semantic_binding_id"],
            semantic_binding_digest=row["semantic_binding_digest"],
            semantic_attempt_key=row["semantic_attempt_key"],
            claim_mode=row["claim_mode"],
            status=row["status"],
            claimed_by=row["claimed_by"],
            claimed_at=row["claimed_at"],
            heartbeat_at=row["heartbeat_at"],
            lease_expires_at=row["lease_expires_at"],
        )

    async def get_settlement(
        self,
        request_scope: str,
        effect_claim_id: str,
    ) -> OperationJournalSettlement | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT settlement_payload
                FROM belllabs_control.operation_settlements
                WHERE request_scope = $1 AND effect_claim_id = $2
                ORDER BY settlement_revision DESC
                LIMIT 1
                """,
                request_scope,
                effect_claim_id,
            )
        return OperationJournalSettlement.model_validate(_json(payload)) if payload else None

    async def _commit_attempt(
        self,
        connection: asyncpg.Connection,
        mutation: OperationJournalMutation,
    ) -> None:
        attempt = mutation.attempt
        if attempt is None:
            return
        prior = await connection.fetchrow(
            """
            SELECT *
            FROM belllabs_control.operation_execution_attempts
            WHERE request_scope = $1 AND effect_claim_id = $2 AND technical_attempt = $3
            """,
            attempt.request_scope,
            attempt.effect_claim_id,
            attempt.technical_attempt,
        )
        if prior is not None:
            persisted = OperationTechnicalAttempt(
                operation_attempt_id=prior["operation_attempt_id"],
                request_scope=prior["request_scope"],
                effect_claim_id=prior["effect_claim_id"],
                technical_attempt=prior["technical_attempt"],
                provider=prior["provider"],
                provider_attempt_id=prior["provider_attempt_id"],
                disposition=prior["disposition"],
                idempotency_supported=prior["idempotency_supported"],
                retry_class=prior["retry_class"],
                usage=_json(prior["usage_payload"]),
                started_at=prior["started_at"],
                finished_at=prior["finished_at"],
                failure_code=prior["failure_code"],
            )
            if persisted != attempt:
                raise IdempotencyConflict("technical operation attempt replay conflicts")
            return
        await connection.execute(
            """
            INSERT INTO belllabs_control.operation_execution_attempts (
                operation_attempt_id, request_scope, effect_claim_id,
                technical_attempt, provider, provider_attempt_id, disposition,
                idempotency_supported, retry_class, usage_payload,
                started_at, finished_at, failure_code
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12, $13
            )
            """,
            attempt.operation_attempt_id,
            attempt.request_scope,
            attempt.effect_claim_id,
            attempt.technical_attempt,
            attempt.provider,
            attempt.provider_attempt_id,
            attempt.disposition.value,
            attempt.idempotency_supported,
            attempt.retry_class,
            json.dumps(attempt.usage, sort_keys=True, separators=(",", ":")),
            attempt.started_at,
            attempt.finished_at,
            attempt.failure_code,
        )

    async def _commit_settlement(
        self,
        connection: asyncpg.Connection,
        mutation: OperationJournalMutation,
    ) -> None:
        settlement = mutation.settlement
        if settlement is None:
            return
        latest_payload = await connection.fetchval(
            """
            SELECT settlement_payload
            FROM belllabs_control.operation_settlements
            WHERE request_scope = $1 AND effect_claim_id = $2
            ORDER BY settlement_revision DESC
            LIMIT 1
            """,
            settlement.request_scope,
            settlement.effect_claim_id,
        )
        latest = (
            OperationJournalSettlement.model_validate(_json(latest_payload))
            if latest_payload is not None
            else None
        )
        if latest is None and settlement.settlement_revision != 1:
            raise IdempotencyConflict("initial operation settlement revision must be 1")
        if latest is not None:
            if settlement.settlement_revision == latest.settlement_revision:
                if settlement.settlement_digest != latest.settlement_digest:
                    raise IdempotencyConflict("operation settlement replay conflicts")
                return
            if (
                latest.status != "reconciliation_required"
                or settlement.settlement_revision != latest.settlement_revision + 1
                or settlement.settlement_id != latest.settlement_id
                or settlement.request_scope != latest.request_scope
                or settlement.effect_claim_id != latest.effect_claim_id
                or mutation.prior_settlement != latest
            ):
                raise IdempotencyConflict("operation settlement revision chain conflicts")
        prior = await connection.fetchrow(
            """
            SELECT settlement_id, settlement_digest
            FROM belllabs_control.operation_settlements
            WHERE request_scope = $1 AND effect_claim_id = $2
              AND settlement_revision = $3
            """,
            settlement.request_scope,
            settlement.effect_claim_id,
            settlement.settlement_revision,
        )
        if prior is not None:
            if (
                prior["settlement_id"] != settlement.settlement_id
                or prior["settlement_digest"] != settlement.settlement_digest
            ):
                raise IdempotencyConflict("operation settlement replay conflicts")
            return
        await connection.execute(
            """
            INSERT INTO belllabs_control.operation_settlements (
                settlement_id, request_scope, effect_claim_id, settlement_revision,
                settlement_digest, status, usage_payload, pending_external_usage_payload,
                result_manifest_ref, result_manifest_digest, result_manifest_size_bytes,
                failure_code, settlement_payload, settled_at
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb,
                $9, $10, $11, $12, $13::jsonb, $14
            )
            """,
            settlement.settlement_id,
            settlement.request_scope,
            settlement.effect_claim_id,
            settlement.settlement_revision,
            settlement.settlement_digest,
            settlement.status,
            json.dumps(settlement.usage, sort_keys=True, separators=(",", ":")),
            json.dumps(
                settlement.pending_external_usage,
                sort_keys=True,
                separators=(",", ":"),
            ),
            settlement.result_manifest_ref,
            settlement.result_manifest_digest,
            settlement.result_manifest_size_bytes,
            settlement.failure_code,
            _dump(settlement),
            settlement.settled_at,
        )
        await connection.execute(
            """
            UPDATE belllabs_control.operation_effect_claims
            SET status = $3, heartbeat_at = $4
            WHERE request_scope = $1 AND effect_claim_id = $2
            """,
            settlement.request_scope,
            settlement.effect_claim_id,
            (
                "reconciliation_required"
                if settlement.status == "reconciliation_required"
                else "cancelled"
                if settlement.status == "cancelled"
                else "settled"
            ),
            settlement.settled_at,
        )

    async def _commit_run_control(
        self,
        connection: asyncpg.Connection,
        mutation: OperationJournalMutation,
        *,
        current_run: RunProjection,
    ) -> None:
        if mutation.resulting_run is None:
            return
        assert mutation.resulting_budget is not None
        assert mutation.transition is not None
        assert mutation.command_result is not None
        if current_run.version == mutation.resulting_run.version:
            return
        if current_run.version != mutation.expected_run_version:
            raise RunVersionConflict(
                f"expected version {mutation.expected_run_version}, "
                f"current version is {current_run.version}"
            )
        prior_budget_payload = await connection.fetchval(
            """
            SELECT state
            FROM belllabs_control.budget_accounts
            WHERE run_id = $1
            FOR UPDATE
            """,
            mutation.belllabs_run_id,
        )
        if prior_budget_payload is None:
            raise RunControlNotFound(
                f"budget account not found for run: {mutation.belllabs_run_id}"
            )
        prior_budget = BudgetState.model_validate(_json(prior_budget_payload))
        prior_result = await connection.fetchrow(
            """
            SELECT command_fingerprint, result
            FROM belllabs_control.lifecycle_command_results
            WHERE run_id = $1 AND idempotency_issuer = $2 AND command_id = $3
            """,
            mutation.command_result.run_id,
            mutation.command_result.idempotency_issuer,
            mutation.command_result.command_id,
        )
        if prior_result is not None:
            if prior_result[
                "command_fingerprint"
            ] != mutation.command_result.command_fingerprint or _json(
                prior_result["result"]
            ) != mutation.command_result.model_dump(mode="json"):
                raise IdempotencyConflict("operation lifecycle command result collision")
        else:
            await connection.execute(
                """
                INSERT INTO belllabs_control.lifecycle_command_results (
                    run_id, idempotency_issuer, command_id,
                    command_fingerprint, result, recorded_at
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6)
                """,
                mutation.command_result.run_id,
                mutation.command_result.idempotency_issuer,
                mutation.command_result.command_id,
                mutation.command_result.command_fingerprint,
                _dump(mutation.command_result),
                mutation.command_result.recorded_at,
            )
        await self._apply_parent_rollup(
            connection,
            prior_budget,
            mutation.resulting_budget,
            idempotency_id=f"operation:{mutation.claim.effect_claim_id}",
            occurred_at=mutation.claim.claimed_at,
        )
        await connection.execute(
            """
            UPDATE belllabs_control.workflow_runs
            SET version = $2, phase = $3, projection = $4::jsonb, updated_at = $5
            WHERE run_id = $1
            """,
            mutation.resulting_run.run_id,
            mutation.resulting_run.version,
            mutation.resulting_run.phase.value,
            _dump(mutation.resulting_run),
            mutation.resulting_run.updated_at,
        )
        await connection.execute(
            """
            UPDATE belllabs_control.budget_accounts
            SET state = $2::jsonb, updated_at = $3
            WHERE run_id = $1
            """,
            mutation.belllabs_run_id,
            _dump(mutation.resulting_budget),
            mutation.resulting_run.updated_at,
        )
        prior_transition = await connection.fetchval(
            """
            SELECT transition
            FROM belllabs_control.lifecycle_transitions
            WHERE transition_id = $1
            """,
            mutation.transition.transition_id,
        )
        if prior_transition is not None:
            if _json(prior_transition) != mutation.transition.model_dump(mode="json"):
                raise IdempotencyConflict("operation lifecycle transition collision")
        else:
            await connection.execute(
                """
                INSERT INTO belllabs_control.lifecycle_transitions (
                    transition_id, run_id, command_id, prior_version,
                    resulting_version, transition, occurred_at
                )
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                """,
                mutation.transition.transition_id,
                mutation.transition.run_id,
                mutation.transition.command_id,
                mutation.transition.prior_version,
                mutation.transition.resulting_version,
                _dump(mutation.transition),
                mutation.transition.occurred_at,
            )
        await self._insert_ledger(connection, mutation.ledger_entries)
        for event in mutation.outbox_events:
            prior_event = await connection.fetchval(
                "SELECT envelope FROM belllabs_control.outbox WHERE event_id = $1",
                event.event_id,
            )
            if prior_event is not None:
                if _json(prior_event) != event.model_dump(mode="json"):
                    raise IdempotencyConflict("operation outbox event collision")
            else:
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.outbox (
                        event_id, aggregate_id, aggregate_version, sequence,
                        event_type, envelope, recorded_at
                    )
                    VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
                    """,
                    event.event_id,
                    event.aggregate_id,
                    event.aggregate_version,
                    event.sequence,
                    event.event_type,
                    _dump(event),
                    event.recorded_at,
                )

    async def _apply_parent_rollup(
        self,
        connection: asyncpg.Connection,
        prior_child: BudgetState,
        child: BudgetState,
        *,
        idempotency_id: str,
        occurred_at: Any,
    ) -> None:
        if child.parent_account_id is None:
            return
        parent_payload = await connection.fetchval(
            """
            SELECT state
            FROM belllabs_control.budget_accounts
            WHERE account_id = $1
            FOR UPDATE
            """,
            child.parent_account_id,
        )
        if parent_payload is None:
            raise RunControlNotFound(f"parent budget account not found: {child.parent_account_id}")
        parent = BudgetState.model_validate(_json(parent_payload))
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

    @staticmethod
    async def _insert_ledger(
        connection: asyncpg.Connection,
        entries: tuple[BudgetLedgerEntry, ...],
    ) -> None:
        for entry in entries:
            prior_entry = await connection.fetchval(
                "SELECT entry FROM belllabs_control.budget_ledger WHERE entry_id = $1",
                entry.entry_id,
            )
            if prior_entry is not None:
                if _json(prior_entry) != entry.model_dump(mode="json"):
                    raise IdempotencyConflict("operation budget ledger collision")
            else:
                await connection.execute(
                    """
                    INSERT INTO belllabs_control.budget_ledger (
                        entry_id, account_id, run_id, idempotency_id,
                        kind, entry, occurred_at
                    )
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

    async def _inject(self, boundary: str) -> None:
        if self._before_commit is None:
            return
        result = self._before_commit(boundary)
        if inspect.isawaitable(result):
            await result


def _has_claim_children(mutation: OperationJournalMutation) -> bool:
    return any(
        value is not None
        for value in (
            mutation.attempt,
            mutation.settlement,
            mutation.authority_command,
            mutation.authority_result,
            mutation.resulting_run,
            mutation.resulting_budget,
            mutation.transition,
        )
    ) or bool(mutation.ledger_entries or mutation.outbox_events)


def _claim_from_row(row: asyncpg.Record) -> OperationEffectClaim:
    return OperationEffectClaim(
        effect_claim_id=row["effect_claim_id"],
        request_scope=row["request_scope"],
        belllabs_run_id=row["belllabs_run_id"],
        operation_contract_digest=row["operation_contract_digest"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        semantic_binding_id=row["semantic_binding_id"],
        semantic_binding_digest=row["semantic_binding_digest"],
        semantic_attempt_key=row["semantic_attempt_key"],
        claim_mode=row["claim_mode"],
        status=row["status"],
        claimed_by=row["claimed_by"],
        claimed_at=row["claimed_at"],
        heartbeat_at=row["heartbeat_at"],
        lease_expires_at=row["lease_expires_at"],
    )


async def _set_scope(connection: asyncpg.Connection, request_scope: str) -> None:
    await connection.execute(
        "SELECT set_config('belllabs.request_scope', $1, true)",
        request_scope,
    )


async def _lock(connection: asyncpg.Connection, key: str) -> None:
    await connection.execute(
        "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
        key,
    )


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value
