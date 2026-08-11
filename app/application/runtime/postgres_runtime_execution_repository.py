from __future__ import annotations

import json
from typing import Any

import asyncpg
from pydantic import TypeAdapter

from app.application.runtime.runtime_execution_bindings import (
    RuntimeBindingConflict,
    RuntimeBindingReservation,
)
from app.domain.graph_runtime.contracts import (
    GraphExecutionSubmission,
    InterventionReceipt,
    RuntimeExecutionAttempt,
    RuntimeExecutionBinding,
    RuntimeExecutionProjection,
    RuntimeIntervention,
)
from app.domain.graph_runtime.identities import ExecutionEpochKey

INTERVENTION_ADAPTER: TypeAdapter[RuntimeIntervention] = TypeAdapter(RuntimeIntervention)


class PostgresRuntimeCoordinationRepository:
    """RLS-scoped async persistence for runtime bindings, attempts, commands, and tasks."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def create_binding(
        self,
        submission: GraphExecutionSubmission,
        binding: RuntimeExecutionBinding,
    ) -> RuntimeBindingReservation:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, binding.epoch.request_scope)
            await _lock(
                connection,
                f"runtime-binding:{binding.epoch.canonical_key}",
            )
            prior = await connection.fetchrow(
                """
                SELECT submission_digest, binding_payload
                FROM belllabs_control.runtime_execution_bindings
                WHERE request_scope = $1
                  AND (
                    submission_id = $2
                    OR submission_idempotency_key = $3
                    OR (belllabs_run_id = $4 AND execution_epoch = $5)
                  )
                FOR UPDATE
                """,
                binding.epoch.request_scope,
                submission.submission_id,
                submission.idempotency_key,
                binding.epoch.belllabs_run_id,
                binding.epoch.execution_epoch,
            )
            if prior is not None:
                persisted = RuntimeExecutionBinding.model_validate(_json(prior["binding_payload"]))
                if prior["submission_digest"] != submission.request_digest or persisted != binding:
                    raise RuntimeBindingConflict(
                        "runtime submission or epoch has conflicting immutable intent"
                    )
                return RuntimeBindingReservation(binding=persisted, created=False)
            deployment = binding.deployment
            thread = binding.agent_thread
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_execution_bindings (
                    binding_id, request_scope, belllabs_run_id, execution_epoch,
                    submission_id, submission_idempotency_key, submission_digest, run_plan_digest,
                    graph_assembly_digest, state_schema_digest, runtime_provider,
                    deployment_endpoint_id, deployment_revision, deployment_id,
                    assistant_id, graph_id, agent_server_thread_id,
                    status, active, version, binding_payload, created_at, updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    $12, $13, $14, $15, $16, $17, $18, $19, $20,
                    $21::jsonb, $22, $23
                )
                """,
                binding.binding_id,
                binding.epoch.request_scope,
                binding.epoch.belllabs_run_id,
                binding.epoch.execution_epoch,
                binding.submission_id,
                binding.submission_idempotency_key,
                binding.submission_digest,
                binding.run_plan_digest,
                binding.graph_assembly_digest,
                binding.state_schema_digest,
                binding.runtime_provider,
                deployment.deployment_endpoint_id if deployment else None,
                deployment.deployment_revision if deployment else None,
                deployment.deployment_id if deployment else None,
                deployment.assistant_id if deployment else None,
                binding.graph_id,
                thread.agent_server_thread_id if thread else None,
                binding.status.value,
                binding.active,
                binding.version,
                _dump(binding),
                binding.created_at,
                binding.updated_at,
            )
        return RuntimeBindingReservation(binding=binding, created=True)

    async def get_binding(
        self,
        epoch: ExecutionEpochKey,
    ) -> RuntimeExecutionBinding | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, epoch.request_scope)
            payload = await connection.fetchval(
                """
                SELECT binding_payload
                FROM belllabs_control.runtime_execution_bindings
                WHERE request_scope = $1 AND belllabs_run_id = $2
                  AND execution_epoch = $3
                """,
                epoch.request_scope,
                epoch.belllabs_run_id,
                epoch.execution_epoch,
            )
        return RuntimeExecutionBinding.model_validate(_json(payload)) if payload else None

    async def get_by_submission(
        self,
        request_scope: str,
        submission_id: str,
    ) -> RuntimeExecutionBinding | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            payload = await connection.fetchval(
                """
                SELECT binding_payload
                FROM belllabs_control.runtime_execution_bindings
                WHERE request_scope = $1 AND submission_id = $2
                """,
                request_scope,
                submission_id,
            )
        return RuntimeExecutionBinding.model_validate(_json(payload)) if payload else None

    async def append_attempt(
        self,
        attempt: RuntimeExecutionAttempt,
    ) -> RuntimeExecutionAttempt:
        scope = attempt.attempt_key.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(
                connection,
                f"runtime-attempt:{scope}:{attempt.binding_id}:"
                f"{attempt.attempt_key.runtime_attempt}",
            )
            prior = await connection.fetchrow(
                """
                SELECT provider_detail
                FROM belllabs_control.runtime_execution_attempts
                WHERE request_scope = $1 AND binding_id = $2 AND runtime_attempt = $3
                """,
                scope,
                attempt.binding_id,
                attempt.attempt_key.runtime_attempt,
            )
            if prior is not None:
                persisted = RuntimeExecutionAttempt.model_validate(
                    _json(prior["provider_detail"])["contract"]
                )
                if persisted != attempt:
                    raise RuntimeBindingConflict("runtime attempt identity has conflicting facts")
                return persisted
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_execution_attempts (
                    request_scope, binding_id, runtime_attempt, submission_id,
                    disposition, provider_request_digest, agent_server_run_id,
                    provider_detail, started_at, heartbeat_at, lease_expires_at,
                    finished_at, failure_code
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, $10, $11, $12, $13
                )
                """,
                scope,
                attempt.binding_id,
                attempt.attempt_key.runtime_attempt,
                attempt.attempt_key.submission_id,
                attempt.disposition.value,
                attempt.provider_request_digest,
                attempt.agent_run.agent_server_run_id if attempt.agent_run else None,
                _dump({"contract": attempt.model_dump(mode="json")}),
                attempt.started_at,
                attempt.heartbeat_at,
                attempt.lease_expires_at,
                attempt.finished_at,
                attempt.failure_code,
            )
        return attempt

    async def update_binding(
        self,
        binding: RuntimeExecutionBinding,
        *,
        expected_version: int,
    ) -> RuntimeExecutionBinding:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, binding.epoch.request_scope)
            prior_payload = await connection.fetchval(
                """
                SELECT binding_payload
                FROM belllabs_control.runtime_execution_bindings
                WHERE request_scope = $1 AND binding_id = $2
                FOR UPDATE
                """,
                binding.epoch.request_scope,
                binding.binding_id,
            )
            if prior_payload is None:
                raise LookupError("runtime binding not found")
            prior = RuntimeExecutionBinding.model_validate(_json(prior_payload))
            _validate_binding_update(prior, binding, expected_version)
            result = await connection.execute(
                """
                UPDATE belllabs_control.runtime_execution_bindings
                SET status = $3, active = $4, version = $5,
                    binding_payload = $6::jsonb, updated_at = $7
                WHERE request_scope = $1 AND binding_id = $2 AND version = $8
                """,
                binding.epoch.request_scope,
                binding.binding_id,
                binding.status.value,
                binding.active,
                binding.version,
                _dump(binding),
                binding.updated_at,
                expected_version,
            )
            if result == "UPDATE 0":
                raise RuntimeBindingConflict("runtime binding version is stale")
        return binding

    async def projection(
        self,
        epoch: ExecutionEpochKey,
    ) -> RuntimeExecutionProjection | None:
        binding = await self.get_binding(epoch)
        if binding is None:
            return None
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, epoch.request_scope)
            rows = await connection.fetch(
                """
                SELECT provider_detail
                FROM belllabs_control.runtime_execution_attempts
                WHERE request_scope = $1 AND binding_id = $2
                ORDER BY runtime_attempt
                """,
                epoch.request_scope,
                binding.binding_id,
            )
        attempts = tuple(
            RuntimeExecutionAttempt.model_validate(_json(row["provider_detail"])["contract"])
            for row in rows
        )
        return RuntimeExecutionProjection(binding=binding, attempts=attempts)

    async def record(
        self,
        intervention: RuntimeIntervention,
        receipt: InterventionReceipt,
    ) -> InterventionReceipt:
        scope = intervention.epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(connection, f"runtime-command:{scope}:{intervention.command_id}")
            prior = await connection.fetchrow(
                """
                SELECT command_id, request_digest, command_payload, receipt_payload
                FROM belllabs_control.runtime_intervention_commands
                WHERE request_scope = $1
                  AND (command_id = $2 OR idempotency_key = $3)
                FOR UPDATE
                """,
                scope,
                intervention.command_id,
                intervention.idempotency_key,
            )
            if prior is not None:
                persisted_command = _json(prior["command_payload"])
                if (
                    prior["command_id"] != intervention.command_id
                    or prior["request_digest"] != intervention.request_digest
                    or persisted_command != intervention.model_dump(mode="json")
                ):
                    raise RuntimeBindingConflict(
                        "intervention command identity has conflicting intent"
                    )
                if prior["receipt_payload"] is not None:
                    persisted_receipt = InterventionReceipt.model_validate(
                        _json(prior["receipt_payload"])
                    )
                    if persisted_receipt != receipt:
                        raise RuntimeBindingConflict(
                            "intervention receipt conflicts with prior completion"
                        )
                    return persisted_receipt
                await connection.execute(
                    """
                    UPDATE belllabs_control.runtime_intervention_commands
                    SET receipt_payload = $3::jsonb, status = $4, recorded_at = $5
                    WHERE request_scope = $1 AND command_id = $2
                    """,
                    scope,
                    intervention.command_id,
                    _dump(receipt),
                    receipt.status,
                    receipt.recorded_at,
                )
                return receipt
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_intervention_commands (
                    command_id, request_scope, binding_id, idempotency_key,
                    request_digest, intervention_kind, expected_belllabs_version,
                    expected_checkpoint_id, command_payload, receipt_payload,
                    status, requested_at, recorded_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10::jsonb,
                    $11, $12, $13
                )
                """,
                intervention.command_id,
                scope,
                receipt.binding_id,
                intervention.idempotency_key,
                intervention.request_digest,
                intervention.kind,
                intervention.expected_belllabs_version,
                (
                    intervention.expected_checkpoint.langgraph_checkpoint_id
                    if intervention.expected_checkpoint
                    else None
                ),
                _dump(intervention),
                _dump(receipt),
                receipt.status,
                intervention.requested_at,
                receipt.recorded_at,
            )
        return receipt

    async def reserve(
        self,
        intervention: RuntimeIntervention,
        *,
        binding_id: str,
    ) -> bool:
        scope = intervention.epoch.request_scope
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, scope)
            await _lock(
                connection,
                f"runtime-command:{scope}:{intervention.idempotency_key}",
            )
            prior = await connection.fetchrow(
                """
                SELECT command_id, request_digest, command_payload
                FROM belllabs_control.runtime_intervention_commands
                WHERE request_scope = $1
                  AND (command_id = $2 OR idempotency_key = $3)
                """,
                scope,
                intervention.command_id,
                intervention.idempotency_key,
            )
            if prior is not None:
                if (
                    prior["command_id"] != intervention.command_id
                    or prior["request_digest"] != intervention.request_digest
                    or _json(prior["command_payload"]) != intervention.model_dump(mode="json")
                ):
                    raise RuntimeBindingConflict(
                        "intervention idempotency identity has conflicting intent"
                    )
                return False
            await connection.execute(
                """
                INSERT INTO belllabs_control.runtime_intervention_commands (
                    command_id, request_scope, binding_id, idempotency_key,
                    request_digest, intervention_kind, expected_belllabs_version,
                    expected_checkpoint_id, command_payload, status, requested_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, 'pending', $10
                )
                """,
                intervention.command_id,
                scope,
                binding_id,
                intervention.idempotency_key,
                intervention.request_digest,
                intervention.kind,
                intervention.expected_belllabs_version,
                (
                    intervention.expected_checkpoint.langgraph_checkpoint_id
                    if intervention.expected_checkpoint
                    else None
                ),
                _dump(intervention),
                intervention.requested_at,
            )
        return True

    async def get_intervention(
        self,
        request_scope: str,
        command_id: str,
    ) -> tuple[RuntimeIntervention, InterventionReceipt] | None:
        async with self._pool.acquire() as connection, connection.transaction():
            await _set_scope(connection, request_scope)
            row = await connection.fetchrow(
                """
                SELECT binding_id, expected_belllabs_version, command_payload,
                       receipt_payload, requested_at
                FROM belllabs_control.runtime_intervention_commands
                WHERE request_scope = $1 AND command_id = $2
                """,
                request_scope,
                command_id,
            )
        if row is None:
            return None
        command = INTERVENTION_ADAPTER.validate_python(_json(row["command_payload"]))
        if row["receipt_payload"] is None:
            receipt = InterventionReceipt(
                command_id=command.command_id,
                status="reconciliation_required",
                binding_id=row["binding_id"],
                resulting_belllabs_version=row["expected_belllabs_version"],
                reason_code="provider_application_pending",
                recorded_at=row["requested_at"],
            )
        else:
            receipt = InterventionReceipt.model_validate(_json(row["receipt_payload"]))
        return (
            command,
            receipt,
        )


def _validate_binding_update(
    prior: RuntimeExecutionBinding,
    binding: RuntimeExecutionBinding,
    expected_version: int,
) -> None:
    if prior.version != expected_version or binding.version != expected_version + 1:
        raise RuntimeBindingConflict("runtime binding version is stale")
    immutable = {
        "status",
        "active",
        "version",
        "updated_at",
    }
    left = prior.model_dump(mode="json", exclude=immutable)
    right = binding.model_dump(mode="json", exclude=immutable)
    if left != right:
        raise RuntimeBindingConflict("runtime binding immutable identity changed")


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
