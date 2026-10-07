"""Atomic operation journal on mission_control.

Claim-before-effect is the canonical ``operation_intent`` with its exact support claim;
technical attempts are observations; settlements keep their revision chain and exactly one
terminal settlement becomes the canonical ``operation_receipt``. Run, budget, lifecycle
receipt, transition, mission events and outbox commit in the same transaction.
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

import asyncpg

from mission_control.adapters.postgres.run_control import canonical as mc
from mission_control.adapters.postgres.run_control.canonical import SCOPE, scoped
from mission_control.application.execution.operations.operation_journal import (
    OperationJournalMutation,
    _is_journal_only_authority_mutation,
)
from mission_control.contracts.identities import uuid7
from mission_control.domain.authoring.canonical import sha256_digest, stored_payload_matches
from mission_control.domain.execution.journal import (
    OperationClaimResult,
    OperationEffectClaim,
    OperationJournalSettlement,
    OperationTechnicalAttempt,
)
from mission_control.domain.policies.budget import roll_up_child_budget
from mission_control.domain.policies.contracts import (
    ApplyAuthorityBatchAction,
    BudgetState,
    RecordUsageAction,
    RunProjection,
)
from mission_control.domain.policies.errors import (
    IdempotencyConflict,
    RunControlNotFound,
    RunVersionConflict,
)

FailureHook = Callable[[str], Awaitable[None] | None]

SETTLEMENT_CONTRACT = "mc.operation-settlement/1"
_INTENT_STATE = {
    "claimed": "claimed",
    "executing": "dispatched",
    "settled": "settled",
    "reconciliation_required": "ambiguous",
    "cancelled": "abandoned",
}
_RECEIPT_OUTCOME = {
    "completed": "applied",
    "failed": "failed",
    "cancelled": "cancelled",
    "timed_out": "timed_out",
}
_CLAIM_COLUMNS = """
    claim.claim_key, claim.run_key, claim.operation_contract_digest, claim.idempotency_key,
    claim.request_digest, claim.semantic_binding_key, claim.semantic_binding_digest,
    claim.semantic_attempt_key, claim.unit_key, claim.claim_mode, claim.status,
    claim.claimed_by, claim.claimed_at, claim.heartbeat_at, claim.lease_expires_at
"""


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
            args = await mc.begin(connection, mutation.request_scope)
            await mc.advisory_lock(connection, lock_key)
            prior_mutation = await connection.fetchrow(
                f"""
                SELECT m.claim_key AS mutation_claim_key, m.mutation_digest, {_CLAIM_COLUMNS}
                FROM mission_control.operation_journal_mutation AS m
                JOIN mission_control.operation_claim AS claim
                  ON claim.installation_id = m.installation_id
                 AND claim.application_id = m.application_id
                 AND claim.tenant_id = m.tenant_id AND claim.claim_key = m.claim_key
                WHERE {scoped("m")} AND m.mutation_key = $4
                """,
                *args,
                mutation.mutation_id,
            )
            if prior_mutation is not None:
                if (
                    prior_mutation["mutation_claim_key"] != claim.effect_claim_id
                    or prior_mutation["mutation_digest"] != mutation.mutation_digest
                ):
                    raise IdempotencyConflict(
                        "operation journal mutation identity has conflicting intent"
                    )
                return OperationClaimResult(
                    status="existing",
                    claim=_claim_from_row(prior_mutation, mutation.request_scope),
                    reason="same operation journal mutation already committed",
                )
            # Frozen lock order: mission, then run.
            run_row = await mc.lock_mission_for_run(connection, args, mutation.belllabs_run_id)
            run_version = int(run_row["version"])
            journal_only_settlement = _is_journal_only_authority_mutation(mutation)
            if run_version != mutation.expected_run_version and not (
                journal_only_settlement and run_version >= mutation.expected_run_version
            ):
                raise RunVersionConflict(
                    f"expected version {mutation.expected_run_version}, "
                    f"current version is {run_version}"
                )
            if journal_only_settlement:
                await _verify_journal_only_authority(connection, args, mutation)
            prior_row = await connection.fetchrow(
                f"""
                SELECT {_CLAIM_COLUMNS}
                FROM mission_control.operation_claim AS claim
                WHERE {scoped("claim")} AND claim.operation_contract_digest = $4
                  AND claim.idempotency_key = $5
                """,
                *args,
                claim.operation_contract_digest,
                claim.idempotency_key,
            )
            existing = prior_row is not None
            persisted_claim: OperationEffectClaim | None = None
            if prior_row is not None:
                if (
                    prior_row["run_key"] != claim.belllabs_run_id
                    or prior_row["operation_contract_digest"] != claim.operation_contract_digest
                    or prior_row["idempotency_key"] != claim.idempotency_key
                    or prior_row["request_digest"] != claim.request_digest
                    or prior_row["semantic_binding_key"] != claim.semantic_binding_id
                    or prior_row["semantic_binding_digest"] != claim.semantic_binding_digest
                    or prior_row["semantic_attempt_key"] != claim.semantic_attempt_key
                    or prior_row["unit_key"] != claim.unit_key
                    or prior_row["claim_mode"] != claim.claim_mode
                ):
                    raise IdempotencyConflict(
                        "effect claim key was reused with conflicting immutable intent"
                    )
                persisted_claim = _claim_from_row(prior_row, mutation.request_scope)
                if prior_row["claim_key"] != claim.effect_claim_id:
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
                    prior = await _mutation_row(connection, args, mutation.mutation_id)
                    if (
                        prior is not None
                        and prior["claim_key"] == claim.effect_claim_id
                        and prior["mutation_digest"] == mutation.mutation_digest
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
                await _insert_claim(connection, args, claim, run_row["run_id"])
            mutation_inserted = await connection.fetchval(
                """
                INSERT INTO mission_control.operation_journal_mutation (
                    installation_id, application_id, tenant_id, operation_journal_mutation_id,
                    mutation_key, claim_key, mutation_digest, mutation_payload, recorded_at,
                    created_at, created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $9, $10)
                ON CONFLICT (installation_id, application_id, tenant_id, mutation_key) DO NOTHING
                RETURNING mutation_key
                """,
                *args,
                uuid7(),
                mutation.mutation_id,
                claim.effect_claim_id,
                mutation.mutation_digest,
                json.dumps({"schema_version": 1, "mutation_id": mutation.mutation_id}),
                claim.claimed_at,
                claim.claimed_by,
            )
            if mutation_inserted is None:
                prior = await _mutation_row(connection, args, mutation.mutation_id)
                if (
                    prior is None
                    or prior["claim_key"] != claim.effect_claim_id
                    or prior["mutation_digest"] != mutation.mutation_digest
                ):
                    raise IdempotencyConflict(
                        "operation journal mutation identity has conflicting intent"
                    )
                return OperationClaimResult(
                    status="existing",
                    claim=persisted_claim or claim,
                    reason="same operation journal mutation already committed",
                )
            await self._commit_attempt(connection, args, mutation)
            await self._commit_settlement(connection, args, mutation)
            await self._commit_run_control(
                connection,
                args,
                mutation,
                current_run=RunProjection.model_validate(mc.load(run_row["projection"])),
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
            args = await mc.begin(connection, request_scope)
            row = await connection.fetchrow(
                f"""
                SELECT {_CLAIM_COLUMNS} FROM mission_control.operation_claim AS claim
                WHERE {scoped("claim")} AND claim.claim_key = $4
                """,
                *args,
                effect_claim_id,
            )
        return _claim_from_row(row, request_scope) if row is not None else None

    async def get_settlement(
        self,
        request_scope: str,
        effect_claim_id: str,
    ) -> OperationJournalSettlement | None:
        async with self._pool.acquire() as connection, connection.transaction():
            args = await mc.begin(connection, request_scope)
            payload = await _latest_settlement(connection, args, effect_claim_id)
        return OperationJournalSettlement.model_validate(mc.load(payload)) if payload else None

    async def _commit_attempt(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        mutation: OperationJournalMutation,
    ) -> None:
        attempt = mutation.attempt
        if attempt is None:
            return
        prior = await connection.fetchrow(
            f"""
            SELECT attempt_key, claim_key, technical_attempt, provider, provider_attempt_key,
                   disposition, idempotency_supported, retry_class, usage_payload, started_at,
                   finished_at, failure_code
            FROM mission_control.operation_technical_attempt
            WHERE {SCOPE} AND claim_key = $4 AND technical_attempt = $5
            """,
            *args,
            attempt.effect_claim_id,
            attempt.technical_attempt,
        )
        if prior is not None:
            persisted = OperationTechnicalAttempt(
                operation_attempt_id=prior["attempt_key"],
                request_scope=attempt.request_scope,
                effect_claim_id=prior["claim_key"],
                technical_attempt=prior["technical_attempt"],
                provider=prior["provider"],
                provider_attempt_id=prior["provider_attempt_key"],
                disposition=prior["disposition"],
                idempotency_supported=prior["idempotency_supported"],
                retry_class=prior["retry_class"],
                usage=mc.load(prior["usage_payload"]),
                started_at=prior["started_at"],
                finished_at=prior["finished_at"],
                failure_code=prior["failure_code"],
            )
            if persisted != attempt:
                raise IdempotencyConflict("technical operation attempt replay conflicts")
            return
        await connection.execute(
            """
            INSERT INTO mission_control.operation_technical_attempt (
                installation_id, application_id, tenant_id, operation_technical_attempt_id,
                attempt_key, claim_key, technical_attempt, provider, provider_attempt_key,
                disposition, idempotency_supported, retry_class, usage_payload, started_at,
                finished_at, failure_code, created_at, created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13::jsonb, $14, $15,
                    $16, $14, $8)
            """,
            *args,
            uuid7(),
            attempt.operation_attempt_id,
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
        args: tuple[Any, ...],
        mutation: OperationJournalMutation,
    ) -> None:
        settlement = mutation.settlement
        if settlement is None:
            return
        latest_payload = await _latest_settlement(connection, args, settlement.effect_claim_id)
        latest = (
            OperationJournalSettlement.model_validate(mc.load(latest_payload))
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
            f"""
            SELECT settlement_key, settlement_digest FROM mission_control.operation_settlement
            WHERE {SCOPE} AND claim_key = $4 AND settlement_revision = $5
            """,
            *args,
            settlement.effect_claim_id,
            settlement.settlement_revision,
        )
        if prior is not None:
            if (
                prior["settlement_key"] != settlement.settlement_id
                or prior["settlement_digest"] != settlement.settlement_digest
            ):
                raise IdempotencyConflict("operation settlement replay conflicts")
            return
        intent_id: UUID = await connection.fetchval(
            f"""
            SELECT operation_intent_id FROM mission_control.operation_claim
            WHERE {SCOPE} AND claim_key = $4
            """,
            *args,
            settlement.effect_claim_id,
        )
        receipt_id: UUID | None = None
        if settlement.status != "reconciliation_required":
            # Exactly one terminal settlement per claim becomes the operation receipt.
            receipt_id = uuid7()
            await connection.execute(
                """
                INSERT INTO mission_control.operation_receipt (
                    installation_id, application_id, tenant_id, operation_receipt_id,
                    operation_intent_id, receipt_contract, outcome, external_operation_ref,
                    output_refs, receipt_digest, reconciliation, recorded_at, created_at,
                    created_by_actor_ref
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10::jsonb, $11, $11, $12)
                """,
                *args,
                receipt_id,
                intent_id,
                SETTLEMENT_CONTRACT,
                _RECEIPT_OUTCOME[settlement.status],
                [settlement.result_manifest_ref] if settlement.result_manifest_ref else [],
                settlement.settlement_digest,
                mc.dump(
                    {
                        "settlement_id": settlement.settlement_id,
                        "settlement_revision": settlement.settlement_revision,
                        "failure_code": settlement.failure_code,
                    }
                ),
                settlement.settled_at,
                mc.WRITER_REF,
            )
        await connection.execute(
            """
            INSERT INTO mission_control.operation_settlement (
                installation_id, application_id, tenant_id, operation_settlement_id,
                settlement_key, claim_key, settlement_revision, settlement_digest, status,
                usage_payload, pending_external_usage_payload, result_manifest_ref,
                result_manifest_digest, result_manifest_size_bytes, failure_code,
                settlement_contract, settlement_payload, operation_receipt_id, settled_at,
                created_at, created_by_actor_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11::jsonb, $12, $13, $14,
                    $15, $16, $17::jsonb, $18, $19, $19, $20)
            """,
            *args,
            uuid7(),
            settlement.settlement_id,
            settlement.effect_claim_id,
            settlement.settlement_revision,
            settlement.settlement_digest,
            settlement.status,
            json.dumps(settlement.usage, sort_keys=True, separators=(",", ":")),
            json.dumps(settlement.pending_external_usage, sort_keys=True, separators=(",", ":")),
            settlement.result_manifest_ref,
            settlement.result_manifest_digest,
            settlement.result_manifest_size_bytes,
            settlement.failure_code,
            SETTLEMENT_CONTRACT,
            _dump(settlement),
            receipt_id,
            settlement.settled_at,
            mc.WRITER_REF,
        )
        status = (
            "reconciliation_required"
            if settlement.status == "reconciliation_required"
            else "cancelled"
            if settlement.status == "cancelled"
            else "settled"
        )
        await connection.execute(
            f"""
            UPDATE mission_control.operation_claim
            SET status = $5, heartbeat_at = $6, version = version + 1, updated_at = $6
            WHERE {SCOPE} AND claim_key = $4
            """,
            *args,
            settlement.effect_claim_id,
            status,
            settlement.settled_at,
        )
        await connection.execute(
            f"""
            UPDATE mission_control.operation_intent
            SET state = $5, version = version + 1, updated_at = $6
            WHERE {SCOPE} AND operation_intent_id = $4
            """,
            *args,
            intent_id,
            _INTENT_STATE[status],
            settlement.settled_at,
        )

    async def _commit_run_control(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
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
        account = await mc.budget_state(connection, args, run_key=mutation.belllabs_run_id)
        if account is None:
            raise RunControlNotFound(
                f"budget account not found for run: {mutation.belllabs_run_id}"
            )
        await mc.lock_budget_chain(connection, args, account.account_id)
        prior_budget = await mc.budget_state(connection, args, account_key=account.account_id)
        assert prior_budget is not None
        result = mutation.command_result
        request_key = mc.command_request_key(result.run_id, result.command_id)
        prior_result = await mc.receipt_row(
            connection,
            args,
            actor_ref=result.idempotency_issuer,
            action=mc.LIFECYCLE_ACTION,
            request_key=request_key,
        )
        if prior_result is not None:
            if prior_result["payload_digest"] != result.command_fingerprint or mc.load(
                prior_result["result"]
            ) != result.model_dump(mode="json"):
                raise IdempotencyConflict("operation lifecycle command result collision")
        else:
            await mc.insert_receipt(
                connection,
                args,
                actor_ref=result.idempotency_issuer,
                action=mc.LIFECYCLE_ACTION,
                request_key=request_key,
                payload_digest=result.command_fingerprint,
                state="completed" if result.status.value == "accepted" else "rejected",
                resource_ref=result.run_id,
                result=result.model_dump(mode="json"),
                recorded_at=result.recorded_at,
            )
        actor = mutation.transition.actor.actor_id
        await self._apply_parent_rollup(
            connection,
            args,
            prior_budget,
            mutation.resulting_budget,
            idempotency_id=f"operation:{mutation.claim.effect_claim_id}",
            occurred_at=mutation.claim.claimed_at,
            actor_ref=actor,
        )
        await mc.update_run(
            connection, args, mutation.resulting_run, expected_version=current_run.version
        )
        await mc.update_budget(
            connection, args, mutation.resulting_budget, mutation.resulting_run.updated_at
        )
        prior_transition = await mc.transition_payload(
            connection, args, mutation.transition.transition_id
        )
        commit_id = await mc.append_events(
            connection,
            args,
            run_key=mutation.belllabs_run_id,
            commit_key=f"operation:{mutation.mutation_id}",
            expected_versions={f"run:{mutation.belllabs_run_id}": current_run.version},
            events=mutation.outbox_events,
            actor_ref=actor,
        )
        if prior_transition is not None:
            if not stored_payload_matches(mc.load(prior_transition), mutation.transition):
                raise IdempotencyConflict("operation lifecycle transition collision")
        else:
            await mc.insert_transition(connection, args, mutation.transition, commit_id)
        await mc.insert_budget_entries(
            connection, args, mutation.ledger_entries, actor, replay_safe=True
        )

    async def _apply_parent_rollup(
        self,
        connection: asyncpg.Connection,
        args: tuple[Any, ...],
        prior_child: BudgetState,
        child: BudgetState,
        *,
        idempotency_id: str,
        occurred_at: Any,
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
        await mc.insert_budget_entries(connection, args, entries, actor_ref, replay_safe=True)

    async def _inject(self, boundary: str) -> None:
        if self._before_commit is None:
            return
        result = self._before_commit(boundary)
        if inspect.isawaitable(result):
            await result


async def _verify_journal_only_authority(
    connection: asyncpg.Connection, args: tuple[Any, ...], mutation: OperationJournalMutation
) -> None:
    assert mutation.authority_result is not None
    assert mutation.authority_command is not None
    result = mutation.authority_result
    persisted = await mc.receipt_row(
        connection,
        args,
        actor_ref=result.idempotency_issuer,
        action=mc.LIFECYCLE_ACTION,
        request_key=mc.command_request_key(result.run_id, result.command_id),
    )
    if (
        persisted is None
        or persisted["payload_digest"] != result.command_fingerprint
        or mc.load(persisted["result"]) != result.model_dump(mode="json")
    ):
        raise IdempotencyConflict("journal settlement authority result is missing or unrelated")
    evidence = await connection.fetchrow(
        f"""
        SELECT transitions.transition->>'command_id' AS transition_command_id,
               outbox.event_type, outbox.payload
        FROM mission_control.run_lifecycle_transition AS transitions
        JOIN mission_control.outbox AS outbox
          ON outbox.installation_id = transitions.installation_id
         AND outbox.application_id = transitions.application_id
         AND outbox.tenant_id = transitions.tenant_id
         AND outbox.aggregate_key = transitions.run_key
         AND outbox.aggregate_version = transitions.resulting_version
         AND outbox.aggregate_sequence = 1
        WHERE {scoped("transitions")} AND transitions.run_key = $4
          AND transitions.resulting_version = $5
        """,
        *args,
        result.run_id,
        result.resulting_run_version,
    )
    action = mutation.authority_command.action
    expected_event_type = (
        "workflow_run.apply_authority_batch"
        if isinstance(action, ApplyAuthorityBatchAction)
        else "workflow_run.record_usage"
        if isinstance(action, RecordUsageAction)
        else "workflow_run.claim_effect"
    )
    envelope = mc.load(evidence["payload"]) if evidence is not None else {}
    if (
        evidence is None
        or evidence["transition_command_id"] != mutation.authority_command.command_id
        or evidence["event_type"] != expected_event_type
        or envelope.get("payload", {}).get("command_id") != mutation.authority_command.command_id
        or (
            isinstance(action, ApplyAuthorityBatchAction)
            and envelope.get("payload", {}).get("authority_batch_digest") != sha256_digest(action)
        )
    ):
        raise IdempotencyConflict("journal settlement authority transition or event is unrelated")


async def _insert_claim(
    connection: asyncpg.Connection,
    args: tuple[Any, ...],
    claim: OperationEffectClaim,
    run_id: UUID,
) -> None:
    activation_id = None
    if claim.unit_key is not None:
        activation_id = await connection.fetchval(
            f"""
            SELECT activation_id FROM mission_control.activation
            WHERE {SCOPE} AND activation_key = $4
            """,
            *args,
            claim.unit_key,
        )
    intent_id = uuid7()
    await connection.execute(
        """
        INSERT INTO mission_control.operation_intent (
            installation_id, application_id, tenant_id, operation_intent_id, run_id,
            activation_id, attempt_id, action_ref, effect_key, input_digest, policy_digest,
            binding_digest, side_effect_class, reservation_ref, state, detail, version,
            updated_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, NULL, $7, $8, $9, NULL, $10, 'consequential', NULL,
                $11, $12::jsonb, 1, $13, $13, $14)
        """,
        *args,
        intent_id,
        run_id,
        activation_id,
        f"operation-contract:{claim.operation_contract_digest}",
        mc.json_key(claim.operation_contract_digest, claim.idempotency_key),
        claim.request_digest,
        claim.semantic_binding_digest,
        _INTENT_STATE[claim.status.value],
        _dump(claim),
        claim.claimed_at,
        claim.claimed_by,
    )
    await connection.execute(
        """
        INSERT INTO mission_control.operation_claim (
            installation_id, application_id, tenant_id, operation_claim_id, claim_key,
            operation_intent_id, run_key, operation_contract_digest, idempotency_key,
            request_digest, semantic_binding_key, semantic_binding_digest, semantic_attempt_key,
            unit_key, claim_mode, status, claimed_by, claimed_at, heartbeat_at, lease_expires_at,
            version, updated_at, created_at, created_by_actor_ref
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
                $18, $19, $20, 1, $18, $18, $17)
        """,
        *args,
        uuid7(),
        claim.effect_claim_id,
        intent_id,
        claim.belllabs_run_id,
        claim.operation_contract_digest,
        claim.idempotency_key,
        claim.request_digest,
        claim.semantic_binding_id,
        claim.semantic_binding_digest,
        claim.semantic_attempt_key,
        claim.unit_key,
        claim.claim_mode,
        claim.status.value,
        claim.claimed_by,
        claim.claimed_at,
        claim.heartbeat_at,
        claim.lease_expires_at,
    )


async def _mutation_row(
    connection: asyncpg.Connection, args: tuple[Any, ...], mutation_key: str
) -> asyncpg.Record | None:
    return await connection.fetchrow(
        f"""
        SELECT claim_key, mutation_digest FROM mission_control.operation_journal_mutation
        WHERE {SCOPE} AND mutation_key = $4
        """,
        *args,
        mutation_key,
    )


async def _latest_settlement(
    connection: asyncpg.Connection, args: tuple[Any, ...], claim_key: str
) -> Any:
    return await connection.fetchval(
        f"""
        SELECT settlement_payload FROM mission_control.operation_settlement
        WHERE {SCOPE} AND claim_key = $4
        ORDER BY settlement_revision DESC
        LIMIT 1
        """,
        *args,
        claim_key,
    )


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


def _claim_from_row(row: asyncpg.Record, request_scope: str) -> OperationEffectClaim:
    return OperationEffectClaim(
        effect_claim_id=row["claim_key"],
        request_scope=request_scope,
        belllabs_run_id=row["run_key"],
        operation_contract_digest=row["operation_contract_digest"],
        idempotency_key=row["idempotency_key"],
        request_digest=row["request_digest"],
        semantic_binding_id=row["semantic_binding_key"],
        semantic_binding_digest=row["semantic_binding_digest"],
        semantic_attempt_key=row["semantic_attempt_key"],
        unit_key=row["unit_key"],
        claim_mode=row["claim_mode"],
        status=row["status"],
        claimed_by=row["claimed_by"],
        claimed_at=row["claimed_at"],
        heartbeat_at=row["heartbeat_at"],
        lease_expires_at=row["lease_expires_at"],
    )


def _dump(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":"))
